//! `Subscription` root — the client-facing `lnInvoicePaymentStatus*` ops.
//!
//! On subscribe the resolver emits the invoice's current status, then streams
//! later transitions until it is paid or expired. Events come from the
//! gateway's outbox via a shared [`OutboxFanout`] feed (plus `EventPublisher`
//! backfill), filtered to one invoice by `reference_id`. Repeated statuses are
//! suppressed; `PAID`/`EXPIRED` end the stream. The stream is resumable: a
//! `ResumeSequence` in request data replays only outbox rows past what the
//! client has already seen.
//!
//! Not wired here (Epic 5): the wallet-ownership check (this op is read-only)
//! and the real WebSocket transport — a reconnecting client's last-acked
//! sequence would arrive as `ResumeSequence`, as the synthetic test supplies
//! it. The GraphQL types match galoy exactly; see ADR-0008 for the full design.

use std::str::FromStr;

use async_graphql::{Context, Subscription};
use chrono::{DateTime, Utc};
use futures::Stream;
use lightning_invoice::Bolt11Invoice;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use ::tracing::{error, warn};

use super::types::{
    GraphqlError, InvoicePaymentStatus, LnInvoicePaymentStatusByHashInput,
    LnInvoicePaymentStatusByPaymentRequestInput, LnInvoicePaymentStatusInput,
    LnInvoicePaymentStatusPayload, LnPaymentPreImage, LnPaymentRequest,
    PaymentHash as GqlPaymentHash,
};
use crate::app::App;
use crate::invoice::entity::{Invoice, InvoiceState};
use crate::outbox::{GatewayDomainEvent, OutboxFanout, MAX_BACKFILL_EVENTS};
use crate::primitives::PaymentHash;

/// Per-subscriber payload buffer. A per-invoice stream emits at most a
/// handful of statuses (`PENDING` then `PAID`/`EXPIRED`), so this is small;
/// `send().await` provides backpressure if a subscriber stalls.
const STREAM_CHANNEL_SIZE: usize = 16;

/// Client's last-acknowledged outbox sequence, injected into request data.
/// Present → resume mode (gap-fill from here, no initial-status re-emit);
/// absent → fresh subscribe (initial status + live-tail). The synthetic
/// test supplies it directly; a future WS transport would set it from the
/// reconnecting client (Fact 4).
#[derive(Clone, Copy, Debug)]
pub struct ResumeSequence(pub i64);

pub struct Subscription;

#[Subscription]
impl Subscription {
    #[graphql(deprecation = "Deprecated in favor of lnInvoicePaymentStatusByPaymentRequest")]
    async fn ln_invoice_payment_status(
        &self,
        ctx: &Context<'_>,
        input: LnInvoicePaymentStatusInput,
    ) -> impl Stream<Item = LnInvoicePaymentStatusPayload> {
        build_status_stream(ctx, payment_hash_from_request(&input.payment_request.0))
    }

    async fn ln_invoice_payment_status_by_hash(
        &self,
        ctx: &Context<'_>,
        input: LnInvoicePaymentStatusByHashInput,
    ) -> impl Stream<Item = LnInvoicePaymentStatusPayload> {
        build_status_stream(ctx, Ok(input.payment_hash.0))
    }

    async fn ln_invoice_payment_status_by_payment_request(
        &self,
        ctx: &Context<'_>,
        input: LnInvoicePaymentStatusByPaymentRequestInput,
    ) -> impl Stream<Item = LnInvoicePaymentStatusPayload> {
        build_status_stream(ctx, payment_hash_from_request(&input.payment_request.0))
    }
}

/// Resolve `App` + `OutboxFanout` from request data and spawn the streaming
/// task; on a bad request or missing backend, yield exactly one
/// `{ errors }` payload then complete (galoy returns `{ errors }` on a bad
/// request — `ln-invoice-payment-status-by-hash.ts:46-48`).
fn build_status_stream(
    ctx: &Context<'_>,
    hash_result: Result<PaymentHash, String>,
) -> ReceiverStream<LnInvoicePaymentStatusPayload> {
    let (tx, rx) = mpsc::channel(STREAM_CHANNEL_SIZE);

    match (ctx.data::<App>(), ctx.data::<OutboxFanout>(), hash_result) {
        (Ok(app), Ok(fanout), Ok(payment_hash)) => {
            let resume = ctx.data_opt::<ResumeSequence>().map(|r| r.0);
            let app = app.clone();
            let fanout = fanout.clone();
            tokio::spawn(run_status_stream(app, fanout, payment_hash, resume, tx));
        }
        (_, _, Err(message)) => {
            let _ = tx.try_send(error_payload(message));
        }
        _ => {
            let _ = tx.try_send(error_payload(
                "subscription backend not configured".to_owned(),
            ));
        }
    }

    ReceiverStream::new(rx)
}

async fn run_status_stream(
    app: App,
    fanout: OutboxFanout,
    payment_hash: PaymentHash,
    resume: Option<i64>,
    tx: mpsc::Sender<LnInvoicePaymentStatusPayload>,
) {
    let cancel = fanout.cancel_token().clone();
    // Attach the live receiver BEFORE the initial read so a transition that
    // races the read buffers in the broadcast instead of being lost.
    let mut broadcast_rx = fanout.subscribe();
    let reference = payment_hash.to_hex();
    let mut emitter = StatusEmitter::default();
    let mut cursor: i64 = resume.unwrap_or(0);

    match resume {
        None => {
            let invoice = lookup_invoice(&app, payment_hash).await;
            let status = initial_status(
                invoice
                    .as_ref()
                    .map(|i| (i.state, i.expiry_at.into_inner())),
                Utc::now(),
            );
            if let Some(status) = emitter.next(status) {
                let payload = build_payload(status, payment_hash, invoice.as_ref());
                if tx.send(payload).await.is_err() {
                    return;
                }
            }
            if is_terminal(status) {
                return;
            }
        }
        Some(_) => {
            if let Flow::Stop = drain_backfill(
                &app,
                &fanout,
                payment_hash,
                &reference,
                &mut cursor,
                &mut emitter,
                &tx,
            )
            .await
            {
                return;
            }
            // Backfill drained no terminal event, but the invoice may already
            // be terminal with no further outbox row (settled/canceled at or
            // before the resume point, or `Open` past `expiry_at`, which emits
            // no row). Re-derive from the aggregate so the resumed stream
            // reports the outcome and completes instead of hanging on `recv`.
            let invoice = lookup_invoice(&app, payment_hash).await;
            let status = initial_status(
                invoice
                    .as_ref()
                    .map(|i| (i.state, i.expiry_at.into_inner())),
                Utc::now(),
            );
            if is_terminal(status) {
                if let Some(status) = emitter.next(status) {
                    let payload = build_payload(status, payment_hash, invoice.as_ref());
                    let _ = tx.send(payload).await;
                }
                return;
            }
        }
    }

    loop {
        tokio::select! {
            // Subscriber dropped the stream → tear down promptly.
            _ = tx.closed() => return,
            _ = cancel.cancelled() => return,
            recv = broadcast_rx.recv() => match recv {
                // The ingest task already read the row, so live events arrive
                // hydrated — filter by reference_id in memory, no DB read.
                Ok(event) => {
                    if event.sequence <= cursor {
                        continue;
                    }
                    cursor = event.sequence;
                    if event.reference_id == reference {
                        if let Flow::Stop = emit_for_event(
                            &app,
                            payment_hash,
                            event.domain_event,
                            &mut emitter,
                            &tx,
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        payment_hash = %payment_hash,
                        "subscription lagged broadcast buffer; re-backfilling from cursor"
                    );
                    if let Flow::Stop = drain_backfill(
                        &app,
                        &fanout,
                        payment_hash,
                        &reference,
                        &mut cursor,
                        &mut emitter,
                        &tx,
                    )
                    .await
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

/// `Stop` = terminal status emitted, subscriber gone, or a backfill read
/// failed — in every case the caller completes the stream.
enum Flow {
    Continue,
    Stop,
}

/// Page through `outbox_events WHERE sequence > *cursor`, advancing `*cursor`
/// to the highest sequence scanned. Rows matching this invoice's
/// `reference_id` are mapped → deduped → emitted. Used for the resume
/// gap-fill (AC7) and the `Lagged` re-backfill recovery (AC11).
async fn drain_backfill(
    app: &App,
    fanout: &OutboxFanout,
    payment_hash: PaymentHash,
    reference: &str,
    cursor: &mut i64,
    emitter: &mut StatusEmitter,
    tx: &mpsc::Sender<LnInvoicePaymentStatusPayload>,
) -> Flow {
    // Bound the gap-fill the same way the gRPC `subscription_loop` does
    // (`api/grpc/service.rs:82`): refuse an unbounded scan rather than page the
    // whole outbox, and tell the client to reconnect from a recent sequence.
    match fanout.publisher().count_after(*cursor).await {
        Ok(count) if count > MAX_BACKFILL_EVENTS => {
            warn!(
                count,
                max = MAX_BACKFILL_EVENTS,
                payment_hash = %payment_hash,
                "subscription backfill exceeds MAX_BACKFILL_EVENTS; refusing to page"
            );
            let _ = tx
                .send(error_payload(format!(
                    "too many events to backfill ({count}); reconnect from a more recent sequence"
                )))
                .await;
            return Flow::Stop;
        }
        Ok(_) => {}
        Err(e) => {
            error!(error = %e, payment_hash = %payment_hash, "subscription backfill count failed");
            return Flow::Stop;
        }
    }
    loop {
        let batch = match fanout.publisher().fetch_after_batch(*cursor).await {
            Ok(b) => b,
            Err(e) => {
                error!(error = %e, payment_hash = %payment_hash, "subscription backfill read failed");
                return Flow::Stop;
            }
        };
        if batch.is_empty() {
            return Flow::Continue;
        }
        for event in batch {
            *cursor = event.sequence;
            if event.reference_id != reference {
                continue;
            }
            if let Flow::Stop =
                emit_for_event(app, payment_hash, event.domain_event, emitter, tx).await
            {
                return Flow::Stop;
            }
        }
    }
}

/// Map one outbox event to a status, dedup against the last emitted, and
/// send the payload. Returns `Stop` when the status is terminal or the
/// subscriber has dropped the stream.
async fn emit_for_event(
    app: &App,
    payment_hash: PaymentHash,
    domain_event: GatewayDomainEvent,
    emitter: &mut StatusEmitter,
    tx: &mpsc::Sender<LnInvoicePaymentStatusPayload>,
) -> Flow {
    let Some(status) = status_from_event(domain_event) else {
        return Flow::Continue;
    };
    let Some(status) = emitter.next(status) else {
        return Flow::Continue;
    };
    // Only PAID carries the preimage + request; fetch the stored invoice
    // for them (AC5). The gateway-owned preimage is present from creation.
    let invoice = if status == InvoicePaymentStatus::Paid {
        lookup_invoice(app, payment_hash).await
    } else {
        None
    };
    if tx
        .send(build_payload(status, payment_hash, invoice.as_ref()))
        .await
        .is_err()
    {
        return Flow::Stop;
    }
    if is_terminal(status) {
        Flow::Stop
    } else {
        Flow::Continue
    }
}

async fn lookup_invoice(app: &App, payment_hash: PaymentHash) -> Option<Invoice> {
    match app
        .invoices()
        .maybe_find_by_payment_hash(&payment_hash)
        .await
        .map_err(crate::invoice::InvoiceError::from)
    {
        Ok(invoice) => invoice,
        Err(e) => {
            warn!(payment_hash = %payment_hash, error = %e, "subscription invoice lookup failed");
            None
        }
    }
}

/// Suppresses consecutive identical statuses (AC6): `next` returns `Some`
/// only when the status differs from the last emitted.
#[derive(Default)]
struct StatusEmitter {
    last: Option<InvoicePaymentStatus>,
}

impl StatusEmitter {
    fn next(&mut self, status: InvoicePaymentStatus) -> Option<InvoicePaymentStatus> {
        if self.last == Some(status) {
            None
        } else {
            self.last = Some(status);
            Some(status)
        }
    }
}

fn is_terminal(status: InvoicePaymentStatus) -> bool {
    matches!(
        status,
        InvoicePaymentStatus::Paid | InvoicePaymentStatus::Expired
    )
}

/// Current status from the stored aggregate (galoy emits this on subscribe).
/// `None` = no invoice for this hash → `EXPIRED` (galoy has no live invoice;
/// ADR-0008 Scope Q4). The authoritative status-mapping table lives in
/// ADR-0008.
fn initial_status(
    state_and_expiry: Option<(InvoiceState, DateTime<Utc>)>,
    now: DateTime<Utc>,
) -> InvoicePaymentStatus {
    use InvoicePaymentStatus as S;
    match state_and_expiry {
        None => S::Expired,
        Some((InvoiceState::Settled, _)) => S::Paid,
        Some((InvoiceState::Canceled, _)) => S::Expired,
        Some((InvoiceState::Held, _)) => S::Pending,
        Some((InvoiceState::Open, expiry_at)) => {
            if now >= expiry_at {
                S::Expired
            } else {
                S::Pending
            }
        }
    }
}

/// Map a live outbox domain event to a wire status. `None` for events that
/// don't bear on this invoice's receive status.
fn status_from_event(event: GatewayDomainEvent) -> Option<InvoicePaymentStatus> {
    use GatewayDomainEvent as E;
    use InvoicePaymentStatus as S;
    match event {
        E::LightningHtlcHeld => Some(S::Pending),
        E::LightningInvoiceSettled => Some(S::Paid),
        // Fact 5: an intraledger-settled invoice's PAID signal arrives as
        // this event, NOT `LightningInvoiceSettled` (Story 3.2 AC13).
        E::LightningIntraledgerTransferCompleted => Some(S::Paid),
        E::LightningInvoiceCanceled => Some(S::Expired),
        // Sender-side outgoing-payment events share the payment_hash
        // reference_id but don't describe THIS invoice's receive status.
        E::LightningPaymentInitiated
        | E::LightningPaymentCompleted
        | E::LightningPaymentFailed
        | E::LightningPaymentReversed => None,
    }
}

fn build_payload(
    status: InvoicePaymentStatus,
    payment_hash: PaymentHash,
    invoice: Option<&Invoice>,
) -> LnInvoicePaymentStatusPayload {
    let (payment_preimage, payment_request) = match (status, invoice) {
        (InvoicePaymentStatus::Paid, Some(inv)) => (
            Some(LnPaymentPreImage(inv.payment_preimage.to_hex())),
            Some(LnPaymentRequest(inv.bolt_invoice.as_str().to_owned())),
        ),
        _ => (None, None),
    };
    LnInvoicePaymentStatusPayload {
        errors: Vec::new(),
        payment_hash: Some(GqlPaymentHash(payment_hash)),
        payment_preimage,
        payment_request,
        status: Some(status),
    }
}

fn error_payload(message: String) -> LnInvoicePaymentStatusPayload {
    LnInvoicePaymentStatusPayload {
        errors: vec![GraphqlError::from_message(message)],
        payment_hash: None,
        payment_preimage: None,
        payment_request: None,
        status: None,
    }
}

/// Extract the payment hash from a BOLT11. Deliberately NOT
/// `decode::decode_bolt11` — that rejects expired invoices
/// (`app/decode.rs:21`), but a subscription to an expired payment_request
/// must still resolve the hash and report `EXPIRED` (AC4). Parsing alone
/// does not enforce expiry, so this tolerates it.
fn payment_hash_from_request(payment_request: &str) -> Result<PaymentHash, String> {
    let invoice = Bolt11Invoice::from_str(payment_request)
        .map_err(|e| format!("invalid lightning invoice: {e}"))?;
    let hash_slice: &[u8] = invoice.payment_hash().as_ref();
    let hash_bytes: [u8; 32] = hash_slice
        .try_into()
        .map_err(|_| "payment hash not 32 bytes".to_owned())?;
    Ok(PaymentHash::from(hash_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::secp256k1::{Secp256k1, SecretKey};
    use chrono::TimeZone;
    use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
    use std::time::Duration;

    fn at(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap()
    }

    // Every arm of `status_from_event` — hand-written match with no
    // type-system enforcement (same justification as
    // `outbox/entity.rs::domain_event_maps_to_standardized`). A swapped or
    // dropped arm would silently misreport invoice status. The
    // intraledger→PAID and canceled→EXPIRED arms are the regression-prone
    // ones (Fact 5; the `CANCELLED`-has-no-wire-value rule).
    #[test]
    fn status_from_event_covers_every_arm() {
        use GatewayDomainEvent as E;
        use InvoicePaymentStatus as S;
        assert_eq!(status_from_event(E::LightningHtlcHeld), Some(S::Pending));
        assert_eq!(status_from_event(E::LightningInvoiceSettled), Some(S::Paid));
        assert_eq!(
            status_from_event(E::LightningIntraledgerTransferCompleted),
            Some(S::Paid)
        );
        assert_eq!(
            status_from_event(E::LightningInvoiceCanceled),
            Some(S::Expired)
        );
        assert_eq!(status_from_event(E::LightningPaymentInitiated), None);
        assert_eq!(status_from_event(E::LightningPaymentCompleted), None);
        assert_eq!(status_from_event(E::LightningPaymentFailed), None);
        assert_eq!(status_from_event(E::LightningPaymentReversed), None);
    }

    #[test]
    fn initial_status_maps_state_and_expiry() {
        use InvoicePaymentStatus as S;
        let now = at(2026, 6, 16);
        let future = at(2026, 6, 17);
        let past = at(2026, 6, 15);
        assert_eq!(initial_status(None, now), S::Expired);
        assert_eq!(
            initial_status(Some((InvoiceState::Settled, future)), now),
            S::Paid
        );
        assert_eq!(
            initial_status(Some((InvoiceState::Canceled, future)), now),
            S::Expired
        );
        assert_eq!(
            initial_status(Some((InvoiceState::Held, future)), now),
            S::Pending
        );
        // Open before expiry → PENDING; Open at/after expiry → EXPIRED
        // (the on-subscribe expiry derivation, ADR-0008).
        assert_eq!(
            initial_status(Some((InvoiceState::Open, future)), now),
            S::Pending
        );
        assert_eq!(
            initial_status(Some((InvoiceState::Open, past)), now),
            S::Expired
        );
    }

    #[test]
    fn status_emitter_suppresses_consecutive_duplicates() {
        use InvoicePaymentStatus as S;
        let mut emitter = StatusEmitter::default();
        let emitted: Vec<_> = [S::Pending, S::Pending, S::Paid, S::Paid]
            .into_iter()
            .filter_map(|s| emitter.next(s))
            .collect();
        assert_eq!(emitted, vec![S::Pending, S::Paid]);
    }

    /// Build a signed regtest BOLT11 whose creation time + expiry are both
    /// in the past, so it is already expired.
    fn expired_bolt11(payment_hash_bytes: [u8; 32]) -> String {
        let private_key = SecretKey::from_slice(&[0x42; 32]).unwrap();
        let payment_hash = sha256::Hash::from_slice(&payment_hash_bytes).unwrap();
        let payment_secret = PaymentSecret([0x11; 32]);
        InvoiceBuilder::new(Currency::Regtest)
            .description("expiry-tolerant-test".into())
            .payment_hash(payment_hash)
            .payment_secret(payment_secret)
            .duration_since_epoch(Duration::from_secs(1))
            .expiry_time(Duration::from_secs(1))
            .min_final_cltv_expiry_delta(144)
            .amount_milli_satoshis(1_000)
            .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &private_key))
            .unwrap()
            .to_string()
    }

    // The regression this guards: an expired payment_request must still
    // resolve its hash (so the subscription can report EXPIRED), unlike
    // `decode_bolt11` which rejects expired invoices.
    #[test]
    fn payment_hash_from_request_tolerates_expiry() {
        let bolt11 = expired_bolt11([0xcd; 32]);
        let hash = payment_hash_from_request(&bolt11).expect("expired invoice still yields hash");
        assert_eq!(hash, PaymentHash::from([0xcd; 32]));
    }

    #[test]
    fn payment_hash_from_request_rejects_malformed() {
        assert!(payment_hash_from_request("not-a-bolt11").is_err());
    }
}
