//! Orphan-hold reconciliation (ADR-0003 §Consequences / AC10).
//!
//! A synchronous `AuthorizeSpend` posts a Cala hold *before* LND is called, so
//! a crash between the hold and the post-LND transition leaves the `Payment`
//! stranded `initiated` with a hold nothing settles or releases. Each stranded
//! intent's REAL outcome is confirmed at LND before acting: `Succeeded` →
//! settle, `Failed` → release the hold, `InFlight`/`NotFound` → leave for the
//! next tick. Terminal transitions reuse the shared
//! `commit_completed`/`commit_failed` writers, so they reconcile idempotently
//! with the live subscription.
//!
//! Using LND as source-of-truth is critical: a payment
//! can sit IN_FLIGHT past the idle threshold (lnd stuck-HTLC, lnd#7697) and
//! `handle_payment_update` no-ops on `InFlight`, so it is never moved out of
//! `initiated` — voiding its hold blind would lose funds if it later settles.

use std::time::Duration;

use chrono::Utc;

use crate::app::{App, AppError};
use crate::lnd::{LndError, SendPaymentStatus};
use crate::payment::{FailureReason, PaymentError, PaymentState};
use crate::primitives::{PaymentHash, Timestamp};

/// Per-payment LND lookup deadline. `TrackPaymentV2` is server-streaming and
/// the sweep processes intents sequentially, so a stalled stream must not hang
/// the whole tick — on elapse the intent is left for the next tick. Mirrors
/// blink-core's `TIMEOUT_PAYMENT` (45s non-regtest) around `getPayment`.
const LND_LOOKUP_TIMEOUT: Duration = Duration::from_secs(45);

impl App {
    /// Reconcile every payment stranded in `initiated` longer than `idle`
    /// against LND. Per-payment errors are logged and skipped — the next tick
    /// retries. Returns the count of intents driven to a terminal state.
    pub async fn sweep_orphan_holds(&self, idle: Duration, limit: i64) -> Result<usize, AppError> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(idle).unwrap_or_else(|_| chrono::Duration::minutes(10));
        let stranded = self.payments.list_stranded_initiated(cutoff, limit).await?;

        let mut reconciled = 0;
        for payment_hash in stranded {
            match self.reconcile_orphan_hold(payment_hash).await {
                Ok(true) => reconciled += 1,
                Ok(false) => {}
                Err(e) => ::tracing::error!(
                    payment_hash = %payment_hash.to_hex(),
                    error = %e,
                    "orphan-hold sweep: reconcile failed; leaving intent for the next tick"
                ),
            }
        }
        Ok(reconciled)
    }

    /// Reconcile one stranded `initiated` intent against LND's real outcome.
    /// `Ok(true)` if it was driven to a terminal state (settled or failed),
    /// `Ok(false)` if left untouched (raced out of `initiated`, still in-flight,
    /// or unknown to LND).
    async fn reconcile_orphan_hold(&self, payment_hash: PaymentHash) -> Result<bool, AppError> {
        let payment = self
            .payments
            .find_by_payment_hash(&payment_hash)
            .await
            .map_err(PaymentError::from)?;

        if payment.state != PaymentState::Initiated {
            // Raced with the subscription between query and load — already resolved.
            return Ok(false);
        }

        let amount_sat = payment.amount_msat.whole_sat() as i64;
        let wallet_id = payment.wallet_id;
        let now = Timestamp::now();

        // Confirm the REAL outcome at LND before touching the hold. Never decide
        // from time+state alone — a still-in-flight payment past the threshold
        // must not be voided. Bound the lookup so a stalled stream can't hang the
        // tick.
        let lookup = match tokio::time::timeout(
            LND_LOOKUP_TIMEOUT,
            self.lnd.lookup_payment(payment_hash),
        )
        .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(LndError::PaymentNotFound)) => {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %wallet_id,
                    "orphan-hold sweep: LND has no record of this payment; leaving initiated for the next tick / operator review"
                );
                return Ok(false);
            }
            Ok(Err(e)) => return Err(AppError::Lnd(e)),
            Err(_elapsed) => {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %wallet_id,
                    "orphan-hold sweep: LND payment lookup timed out; leaving initiated for the next tick"
                );
                return Ok(false);
            }
        };

        match lookup.status {
            SendPaymentStatus::InFlight => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %wallet_id,
                    "orphan-hold sweep: LND still reports in-flight; leaving the hold in place"
                );
                Ok(false)
            }
            SendPaymentStatus::Succeeded => {
                let preimage = lookup.payment_preimage.ok_or_else(|| {
                    AppError::Lnd(LndError::InvalidResponse(
                        "lookup_payment: Succeeded but payment_preimage missing".to_owned(),
                    ))
                })?;
                self.commit_completed(
                    payment,
                    payment_hash,
                    amount_sat,
                    preimage,
                    lookup.fees_paid_msat,
                    lookup.route_hops,
                    now,
                )
                .await?;
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %wallet_id,
                    "orphan-hold sweep: LND settled a stranded intent; reconciled to completed"
                );
                Ok(true)
            }
            SendPaymentStatus::Failed => {
                let reason = lookup.failure_reason.unwrap_or_else(|| {
                    FailureReason::Other("LND reported Failed with no reason".to_owned())
                });
                self.commit_failed(payment, payment_hash, amount_sat, reason, now)
                    .await?;
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %wallet_id,
                    "orphan-hold sweep: LND failed a stranded intent; reconciled to failed and released the hold"
                );
                Ok(true)
            }
        }
    }
}
