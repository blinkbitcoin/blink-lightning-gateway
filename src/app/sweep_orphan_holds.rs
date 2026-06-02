//! Orphan-hold reconciliation (ADR-0003 §Consequences / AC10).
//!
//! A synchronous `AuthorizeSpend` posts a Cala hold *before* LND is called.
//! If the gateway then crashes between the hold and the post-LND transition,
//! the `Payment` intent is left `initiated` with a hold that the normal
//! lifecycle never settles or releases. This sweep finds those stranded
//! intents and voids their holds directly via `VoidSpendAuthorization`.
//!
//! Safety: a payment that genuinely went in-flight is moved out of
//! `initiated` by the LND payment-subscription (`handle_payment_update`), so
//! a row still `initiated` past the idle threshold has no live HTLC. The
//! void runs BEFORE the terminal transition — if Symphony is unreachable the
//! payment stays `initiated` and the next tick retries (fail toward stuck
//! funds, never toward lost funds).

use std::time::Duration;

use chrono::Utc;

use crate::app::{App, AppError};
use crate::payment::{FailureReason, PaymentError, PaymentState};
use crate::primitives::{PaymentHash, Timestamp};

impl App {
    /// Void holds for every payment stranded in `initiated` longer than
    /// `idle`. Per-payment errors are logged and skipped — the next tick
    /// retries. Returns the count of holds voided this tick.
    pub async fn sweep_orphan_holds(&self, idle: Duration, limit: i64) -> Result<usize, AppError> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(idle).unwrap_or_else(|_| chrono::Duration::minutes(10));
        let stranded = self.payments.list_stranded_initiated(cutoff, limit).await?;

        let mut voided = 0;
        for payment_hash in stranded {
            match self.void_orphan_hold(payment_hash).await {
                Ok(true) => voided += 1,
                Ok(false) => {}
                Err(e) => ::tracing::error!(
                    payment_hash = %payment_hash.to_hex(),
                    error = %e,
                    "orphan-hold sweep: void failed; leaving intent for the next tick"
                ),
            }
        }
        Ok(voided)
    }

    /// Void one stranded hold, then fail the intent. Returns `Ok(false)` if
    /// the intent already left `initiated` between query and load (a race
    /// with the subscription handler).
    async fn void_orphan_hold(&self, payment_hash: PaymentHash) -> Result<bool, AppError> {
        let mut payment = self
            .payments
            .find_by_payment_hash(&payment_hash)
            .await
            .map_err(PaymentError::from)?;

        if payment.state != PaymentState::Initiated {
            return Ok(false);
        }

        // Void the hold FIRST (correlation_id == payment_hash, ADR-0002). The
        // gateway never recorded the authorization_id for a crash-stranded
        // intent, so it relies on Symphony's correlation_id-keyed idempotent
        // void. If this errors, return early — the intent stays `initiated`
        // and the next tick retries (the hold is NOT released, so funds are
        // never lost).
        self.symphony
            .void_spend_authorization(payment_hash.to_hex(), String::new())
            .await?;

        // Only now mark the intent terminal. No outbox event: the hold was
        // released by the direct void above, so re-emitting an
        // OutgoingPaymentFailed (whose Symphony handler would also release)
        // would double-handle.
        // The intent is `Initiated` (re-checked above), so both transitions
        // execute; the idempotent outcome is irrelevant here.
        let now = Timestamp::now();
        let _ = payment.mark_pending(now)?;
        let _ = payment.fail(
            FailureReason::Other("orphan-hold sweep: stranded intent, hold voided".to_owned()),
            now,
        )?;
        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        tx.commit().await?;

        ::tracing::warn!(
            payment_hash = %payment_hash.to_hex(),
            wallet_id = %payment.wallet_id,
            correlation_id = %payment_hash.to_hex(),
            "orphan-hold sweep: voided stranded hold and failed the intent"
        );
        Ok(true)
    }
}
