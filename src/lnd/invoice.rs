//! `add_invoice` parameter + response types, plus the per-hash
//! `subscribe_invoice` listener.
//!
//! Per-hash `SubscribeSingleInvoice` is the only invoice-observation
//! path: LND's cluster-level `Lightning/SubscribeInvoices` drops
//! `Accepted` / `Canceled` events for backwards-compat. A listener is
//! spawned per invoice at `App::create_invoice` time + by the recovery
//! sweep at startup.

use std::time::Duration;

use ::tracing::{debug, info, warn};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic_lnd::lnrpc;
use tonic_lnd::tonic::Streaming;

use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Preimage};

use super::client::LndClient;
use super::error::LndError;

#[derive(Clone, Debug)]
pub struct AddInvoiceParams {
    pub amount_msat: MilliSatoshi,
    pub memo: Option<String>,
    pub expiry_seconds: u32,
}

#[derive(Clone, Debug)]
pub struct AddInvoiceResponse {
    /// LND-generated 32-byte payment-hash. Source of truth — never
    /// synthesize on the gateway side.
    pub payment_hash: PaymentHash,
    /// BOLT11 invoice string returned by LND.
    pub bolt_invoice: BoltInvoice,
}

/// Adapter-typed mirror of LND's `lnrpc::invoice::InvoiceState`. Kept
/// separate so the App layer never touches the prost-generated enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LndInvoiceState {
    Open,
    Accepted,
    Settled,
    Canceled,
}

/// One update emitted by the per-hash `subscribe_invoice` listener.
#[derive(Clone, Debug)]
pub struct InvoiceUpdate {
    pub payment_hash: PaymentHash,
    pub state: LndInvoiceState,
    /// Sum of `amt_msat` over `Accepted` HTLCs — the parked amount.
    pub htlc_amount_msat: MilliSatoshi,
    /// Present iff `state == Settled`.
    pub payment_preimage: Option<Preimage>,
}

/// Expected exit reason for `subscribe_invoice`. Any `Err` return is
/// instead the unexpected case, which the caller surfaces loudly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscribeInvoiceExit {
    /// Forwarded a terminal state (`Settled` or `Canceled`).
    Terminal,
    /// `cancel.cancelled()` fired.
    Cancelled,
}

/// Drive LND's per-hash `SubscribeSingleInvoice` stream until a
/// terminal state is forwarded or cancellation fires. Reconnects on
/// transient stream errors with a 2s backoff. In boot-stub mode,
/// returns `Ok(Cancelled)` without opening a stream.
pub async fn subscribe_invoice(
    lnd: LndClient,
    payment_hash: PaymentHash,
    tx: mpsc::Sender<InvoiceUpdate>,
    cancel: CancellationToken,
) -> Result<SubscribeInvoiceExit, LndError> {
    if !lnd.is_connected() {
        debug!(
            payment_hash = %payment_hash.to_hex(),
            "subscribe_invoice: boot_stub mode — awaiting cancellation"
        );
        cancel.cancelled().await;
        return Ok(SubscribeInvoiceExit::Cancelled);
    }

    loop {
        if cancel.is_cancelled() {
            return Ok(SubscribeInvoiceExit::Cancelled);
        }

        match lnd.subscribe_single_invoice_stream(payment_hash).await {
            Ok(stream) => {
                debug!(
                    payment_hash = %payment_hash.to_hex(),
                    "subscribe_invoice: stream opened"
                );
                let res = drive_stream(stream, payment_hash, &tx, &cancel).await;
                if cancel.is_cancelled() {
                    return Ok(SubscribeInvoiceExit::Cancelled);
                }
                match res {
                    Ok(Some(exit)) => return Ok(exit),
                    Ok(None) => {
                        warn!(
                            payment_hash = %payment_hash.to_hex(),
                            "subscribe_invoice: stream closed cleanly without terminal state; reconnecting"
                        );
                    }
                    Err(e) => {
                        warn!(
                            payment_hash = %payment_hash.to_hex(),
                            error = %e,
                            "subscribe_invoice: stream error; reconnecting"
                        );
                    }
                }
            }
            Err(e) => warn!(
                payment_hash = %payment_hash.to_hex(),
                error = %e,
                "subscribe_invoice: open failed; retrying"
            ),
        }

        tokio::select! {
            _ = cancel.cancelled() => return Ok(SubscribeInvoiceExit::Cancelled),
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}

/// Inner stream pump. Returns `Ok(Some(exit))` on terminal-state /
/// cancellation, `Ok(None)` on clean close without terminal state (the
/// outer loop reconnects), `Err(_)` on a transient LND error.
async fn drive_stream(
    mut stream: Streaming<lnrpc::Invoice>,
    payment_hash: PaymentHash,
    tx: &mpsc::Sender<InvoiceUpdate>,
    cancel: &CancellationToken,
) -> Result<Option<SubscribeInvoiceExit>, LndError> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(Some(SubscribeInvoiceExit::Cancelled)),
            msg = stream.message() => {
                let Some(invoice) = msg? else {
                    return Ok(None);
                };
                let update = match lnd_invoice_to_update(invoice) {
                    Ok(u) => u,
                    Err(e) => {
                        warn!(
                            payment_hash = %payment_hash.to_hex(),
                            error = %e,
                            "subscribe_invoice: unparsable LND invoice fields; skipping update"
                        );
                        continue;
                    }
                };
                let terminal = matches!(
                    update.state,
                    LndInvoiceState::Settled | LndInvoiceState::Canceled
                );
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(Some(SubscribeInvoiceExit::Cancelled)),
                    res = tx.send(update) => {
                        if res.is_err() {
                            info!(
                                payment_hash = %payment_hash.to_hex(),
                                "subscribe_invoice: receiver dropped; exiting stream loop"
                            );
                            return Ok(Some(SubscribeInvoiceExit::Cancelled));
                        }
                    }
                }
                if terminal {
                    return Ok(Some(SubscribeInvoiceExit::Terminal));
                }
            }
        }
    }
}

/// Map an `lnrpc::Invoice` to our adapter's `InvoiceUpdate`. An
/// unknown state enum or a malformed `r_hash` surfaces as
/// `LndError::InvalidResponse`.
pub(crate) fn lnd_invoice_to_update(invoice: lnrpc::Invoice) -> Result<InvoiceUpdate, LndError> {
    let r_hash_len = invoice.r_hash.len();
    let r_hash: [u8; 32] = invoice.r_hash.try_into().map_err(|_| {
        LndError::InvalidResponse(format!("r_hash length {r_hash_len}, expected 32"))
    })?;
    let payment_hash = PaymentHash::from(r_hash);

    let state = map_invoice_state(invoice.state)?;

    let htlc_amount_msat = sum_accepted_htlcs(&invoice.htlcs)?;

    let payment_preimage = parse_preimage(&invoice.r_preimage, &payment_hash);

    Ok(InvoiceUpdate {
        payment_hash,
        state,
        htlc_amount_msat,
        payment_preimage,
    })
}

fn map_invoice_state(value: i32) -> Result<LndInvoiceState, LndError> {
    use lnrpc::invoice::InvoiceState as S;
    let s = S::try_from(value)
        .map_err(|_| LndError::InvalidResponse(format!("unknown InvoiceState: {value}")))?;
    Ok(match s {
        S::Open => LndInvoiceState::Open,
        S::Accepted => LndInvoiceState::Accepted,
        S::Settled => LndInvoiceState::Settled,
        S::Canceled => LndInvoiceState::Canceled,
    })
}

fn sum_accepted_htlcs(htlcs: &[lnrpc::InvoiceHtlc]) -> Result<MilliSatoshi, LndError> {
    let accepted = lnrpc::InvoiceHtlcState::Accepted as i32;
    let total: u64 = htlcs
        .iter()
        .filter(|h| h.state == accepted)
        .map(|h| h.amt_msat)
        .try_fold(0u64, |acc, amt| acc.checked_add(amt))
        .ok_or_else(|| LndError::InvalidResponse("htlc amt_msat sum overflowed u64".to_owned()))?;
    Ok(MilliSatoshi::new(total))
}

/// `r_preimage` is 32 bytes when settled, empty otherwise. Any other
/// length is a wire anomaly — log and return `None`.
fn parse_preimage(bytes: &[u8], payment_hash: &PaymentHash) -> Option<Preimage> {
    if bytes.is_empty() {
        return None;
    }
    if bytes.len() != 32 {
        warn!(
            payment_hash = %payment_hash.to_hex(),
            preimage_len = bytes.len(),
            "subscribe_invoice: r_preimage wrong length (expected 32 bytes)"
        );
        return None;
    }
    let arr: [u8; 32] = bytes.try_into().ok()?;
    Some(Preimage::from(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lnd::client::{LndApi, MockLndApi};

    fn canned_response() -> AddInvoiceResponse {
        AddInvoiceResponse {
            payment_hash: PaymentHash::from([0xab; 32]),
            bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
        }
    }

    #[tokio::test]
    async fn mock_lnd_returns_canned_response() {
        let mut mock = MockLndApi::new();
        mock.expect_add_invoice()
            .times(1)
            .returning(|_| Box::pin(async { Ok(canned_response()) }));

        let resp = mock
            .add_invoice(AddInvoiceParams {
                amount_msat: MilliSatoshi::new(1_000_000),
                memo: Some("test".to_owned()),
                expiry_seconds: 3600,
            })
            .await
            .unwrap();

        assert_eq!(resp.payment_hash, PaymentHash::from([0xab; 32]));
        assert!(resp.bolt_invoice.as_str().starts_with("lnbc"));
    }

    fn invoice_with(state: lnrpc::invoice::InvoiceState, r_preimage: Vec<u8>) -> lnrpc::Invoice {
        lnrpc::Invoice {
            r_hash: vec![0xab; 32],
            state: state as i32,
            r_preimage,
            htlcs: vec![lnrpc::InvoiceHtlc {
                amt_msat: 1_000_000,
                state: lnrpc::InvoiceHtlcState::Accepted as i32,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn lnd_invoice_to_update_settled_carries_preimage() {
        let inv = invoice_with(lnrpc::invoice::InvoiceState::Settled, vec![0xee; 32]);
        let update = lnd_invoice_to_update(inv).unwrap();
        assert_eq!(update.state, LndInvoiceState::Settled);
        assert_eq!(update.payment_preimage, Some(Preimage::from([0xee; 32])));
        assert_eq!(update.htlc_amount_msat, MilliSatoshi::new(1_000_000));
    }

    #[test]
    fn lnd_invoice_to_update_accepted_omits_preimage() {
        // ACCEPTED state — preimage not yet released.
        let inv = invoice_with(lnrpc::invoice::InvoiceState::Accepted, Vec::new());
        let update = lnd_invoice_to_update(inv).unwrap();
        assert_eq!(update.state, LndInvoiceState::Accepted);
        assert_eq!(update.payment_preimage, None);
        assert_eq!(update.htlc_amount_msat, MilliSatoshi::new(1_000_000));
    }

    #[test]
    fn lnd_invoice_to_update_open_with_no_htlcs_is_zero() {
        let inv = lnrpc::Invoice {
            r_hash: vec![0xcd; 32],
            state: lnrpc::invoice::InvoiceState::Open as i32,
            ..Default::default()
        };
        let update = lnd_invoice_to_update(inv).unwrap();
        assert_eq!(update.state, LndInvoiceState::Open);
        assert_eq!(update.htlc_amount_msat, MilliSatoshi::ZERO);
    }

    #[test]
    fn lnd_invoice_to_update_canceled_no_preimage() {
        let inv = lnrpc::Invoice {
            r_hash: vec![0xef; 32],
            state: lnrpc::invoice::InvoiceState::Canceled as i32,
            ..Default::default()
        };
        let update = lnd_invoice_to_update(inv).unwrap();
        assert_eq!(update.state, LndInvoiceState::Canceled);
        assert_eq!(update.payment_preimage, None);
    }

    #[test]
    fn lnd_invoice_to_update_wrong_r_hash_length_errs() {
        let inv = lnrpc::Invoice {
            r_hash: vec![0x00; 31],
            state: lnrpc::invoice::InvoiceState::Open as i32,
            ..Default::default()
        };
        match lnd_invoice_to_update(inv) {
            Err(LndError::InvalidResponse(msg)) => assert!(msg.contains("r_hash length")),
            other => panic!("expected InvalidResponse, got {other:?}"),
        }
    }

    #[test]
    fn lnd_invoice_to_update_filters_non_accepted_htlcs_from_sum() {
        // A Settled invoice may still carry one ACCEPTED HTLC alongside
        // a SETTLED one; only the ACCEPTED amount contributes to
        // `htlc_amount_msat`.
        let inv = lnrpc::Invoice {
            r_hash: vec![0xab; 32],
            state: lnrpc::invoice::InvoiceState::Settled as i32,
            r_preimage: vec![0xee; 32],
            htlcs: vec![
                lnrpc::InvoiceHtlc {
                    amt_msat: 500_000,
                    state: lnrpc::InvoiceHtlcState::Accepted as i32,
                    ..Default::default()
                },
                lnrpc::InvoiceHtlc {
                    amt_msat: 700_000,
                    state: lnrpc::InvoiceHtlcState::Settled as i32,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let update = lnd_invoice_to_update(inv).unwrap();
        assert_eq!(update.htlc_amount_msat, MilliSatoshi::new(500_000));
    }

    #[test]
    fn lnd_invoice_to_update_htlc_sum_overflow_errs() {
        // Two ACCEPTED HTLCs whose amt_msat sum exceeds u64::MAX must
        // surface as InvalidResponse — not wrap or panic.
        let inv = lnrpc::Invoice {
            r_hash: vec![0xab; 32],
            state: lnrpc::invoice::InvoiceState::Accepted as i32,
            htlcs: vec![
                lnrpc::InvoiceHtlc {
                    amt_msat: u64::MAX,
                    state: lnrpc::InvoiceHtlcState::Accepted as i32,
                    ..Default::default()
                },
                lnrpc::InvoiceHtlc {
                    amt_msat: 1,
                    state: lnrpc::InvoiceHtlcState::Accepted as i32,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        match lnd_invoice_to_update(inv) {
            Err(LndError::InvalidResponse(msg)) => assert!(msg.contains("overflow")),
            other => panic!("expected InvalidResponse, got {other:?}"),
        }
    }
}
