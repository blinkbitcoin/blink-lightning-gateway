//! LND subscription consumers.
//!
//! `subscribe_payments` opens LND's `Router/TrackPayments` gRPC stream
//! and forwards every status transition as a `PaymentUpdate` into the
//! App layer via an `mpsc::Sender`. Mirrors `setupPaymentSubscribe` at
//! `blink/core/api/src/servers/trigger.ts:343-373`. The
//! invoice-subscription consumer (`subscribe_invoices`, mirroring
//! galoy's `setupInvoiceSubscribe`) lands in Story 2.3.
//!
//! Reconnect: on transient stream loss the loop sleeps 2s and reopens
//! the stream against the same LND. Cancellation (via `CancellationToken`)
//! short-circuits the sleep and exits cleanly. LND's `TrackPayments`
//! replays in-flight + terminal payments on reconnect, so missed
//! transitions during the gap are re-delivered.
//!
//! Boot-stub mode: when `LndClient::is_connected` is false the loop
//! never opens a stream; it awaits cancellation and returns Ok. This
//! preserves Story 2.2's behavior when the gateway is configured without
//! an `lnd:` block.

use std::time::Duration;

use ::tracing::{debug, info, warn};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic_lnd::lnrpc;
use tonic_lnd::tonic::Streaming;

use crate::payment::{FailureReason, Hop};
use crate::primitives::{MilliSatoshi, PaymentHash, Preimage};

use super::client::{lnd_payment_to_send_response, LndClient};
use super::error::LndError;
use super::payment::SendPaymentStatus;

/// One status update from LND's `Router/TrackPayments` stream. The App
/// layer's `handle_payment_update` consumes this.
#[derive(Clone, Debug)]
pub struct PaymentUpdate {
    pub payment_hash: PaymentHash,
    pub status: SendPaymentStatus,
    pub payment_preimage: Option<Preimage>,
    pub fees_paid_msat: MilliSatoshi,
    pub route_hops: Vec<Hop>,
    pub failure_reason: Option<FailureReason>,
}

/// Drive the LND `TrackPayments` stream until the cancellation token
/// fires. Each LND `lnrpc::Payment` is mapped to `PaymentUpdate` and
/// forwarded to `tx`. Transient stream errors trigger a 2-second
/// reconnect sleep, which the cancel token can interrupt.
///
/// Boot-stub mode: if `lnd` was constructed via `LndClient::boot_stub`,
/// the function logs a warning and awaits cancellation without opening
/// the stream — preserving the Story 2.2 default-binary behavior.
pub async fn subscribe_payments(
    lnd: LndClient,
    tx: mpsc::Sender<PaymentUpdate>,
    cancel: CancellationToken,
) -> Result<(), LndError> {
    if !lnd.is_connected() {
        debug!("subscribe_payments: boot_stub mode — awaiting cancellation");
        cancel.cancelled().await;
        warn!("subscribe_payments: cancelled (boot_stub mode; stream never opened)");
        return Ok(());
    }

    loop {
        if cancel.is_cancelled() {
            info!("subscribe_payments: cancelled before opening stream");
            return Ok(());
        }

        match lnd.track_payments_stream().await {
            Ok(stream) => {
                info!("subscribe_payments: stream opened to Router/TrackPayments");
                let res = drive_stream(stream, &tx, &cancel).await;
                if cancel.is_cancelled() {
                    info!("subscribe_payments: cancellation received; exiting");
                    return Ok(());
                }
                match res {
                    Ok(()) => warn!("subscribe_payments: stream closed cleanly; reconnecting"),
                    Err(e) => warn!(error = %e, "subscribe_payments: stream error; reconnecting"),
                }
            }
            Err(e) => warn!(error = %e, "subscribe_payments: open failed; retrying"),
        }

        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}

async fn drive_stream(
    mut stream: Streaming<lnrpc::Payment>,
    tx: &mpsc::Sender<PaymentUpdate>,
    cancel: &CancellationToken,
) -> Result<(), LndError> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            msg = stream.message() => {
                let Some(payment) = msg? else {
                    return Ok(());
                };
                let raw_hash = payment.payment_hash.clone();
                let raw_status = payment.status;
                let update = match payment_to_update(payment) {
                    Ok(u) => u,
                    Err(LndError::InvalidResponse(msg))
                        if msg.contains("PaymentStatus") =>
                    {
                        warn!(
                            payment_hash = %raw_hash,
                            status = raw_status,
                            error = %msg,
                            "subscribe_payments: unknown LND PaymentStatus; skipping update"
                        );
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            payment_hash = %raw_hash,
                            error = %e,
                            "subscribe_payments: unparsable LND payment fields; skipping update"
                        );
                        continue;
                    }
                };
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    res = tx.send(update) => {
                        if res.is_err() {
                            info!("subscribe_payments: receiver dropped; exiting stream loop");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

fn payment_to_update(payment: lnrpc::Payment) -> Result<PaymentUpdate, LndError> {
    let resp = lnd_payment_to_send_response(payment)?;
    Ok(PaymentUpdate {
        payment_hash: resp.payment_hash,
        status: resp.status,
        payment_preimage: resp.payment_preimage,
        fees_paid_msat: resp.fees_paid_msat,
        route_hops: resp.route_hops,
        failure_reason: resp.failure_reason,
    })
}
