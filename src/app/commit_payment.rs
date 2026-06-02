//! Sole writers of the `OutgoingPaymentCompleted` / `OutgoingPaymentFailed`
//! outbox rows. Each settles/fails the `Payment` and publishes its terminal
//! event in one transaction. Called from the sync send path (`settle_inline` /
//! `fail_inline`, after `mark_pending`), the `TrackPayments` subscription
//! (`handle_payment_update`), and the orphan-hold reconciliation sweep — which
//! relies on the emitted `Failed` event to release the Symphony hold.

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::helpers::hops_to_json;
use crate::app::{App, AppError};
use crate::outbox::NewOutboxEvent;
use crate::payment::{FailureReason, Hop, Payment, PaymentError};
use crate::primitives::{MilliSatoshi, PaymentHash, Preimage, Timestamp};

impl App {
    /// Settle the payment (`(Initiated|Pending) → Completed`, idempotent on a
    /// duplicate `Completed`) and publish `OutgoingPaymentCompleted` in one tx.
    /// On a duplicate replay it returns without publishing.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_completed(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        amount_sat: i64,
        preimage: Preimage,
        fees_paid_msat: MilliSatoshi,
        route_hops: Vec<Hop>,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        match payment.settle(preimage, fees_paid_msat, route_hops.clone(), now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "settle ignored — duplicate SUCCEEDED replay",
                );
                return Ok(payment);
            }
        }

        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        let hops_json = hops_to_json(&route_hops);
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_payment_completed(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "payment_preimage": preimage.to_hex(),
                        "fees_paid_msat": fees_paid_msat.as_u64(),
                        "route_hops": hops_json,
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(payment)
    }

    /// Fail the payment (`(Initiated|Pending) → Failed`, idempotent on a
    /// duplicate `Failed`) and publish `OutgoingPaymentFailed` in one tx. On a
    /// duplicate replay it returns without publishing.
    pub(crate) async fn commit_failed(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        amount_sat: i64,
        failure_reason: FailureReason,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        let reason_detail = failure_reason.detail_str();
        match payment.fail(failure_reason, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fail ignored — duplicate FAILED replay",
                );
                return Ok(payment);
            }
        }

        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_payment_failed(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "failure_reason": reason_detail,
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(payment)
    }
}
