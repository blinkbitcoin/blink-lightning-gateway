//! Subscription-driven payment-update handler — dispatches LND
//! `Router/TrackPayments` updates through `App::handle_payment_update`
//! and the two `transition_to_completed` / `transition_to_failed`
//! helpers.

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::helpers::{hops_to_json, is_payment_not_found};
use crate::app::{App, AppError};
use crate::lnd::{PaymentUpdate, SendPaymentStatus};
use crate::outbox::NewOutboxEvent;
use crate::payment::{FailureReason, Hop, Payment, PaymentError};
use crate::primitives::{MilliSatoshi, PaymentHash, Preimage, Timestamp};

impl App {
    /// Subscription-driven update from LND's `Router/TrackPayments`
    /// stream. Idempotent against duplicates — the entity-level
    /// `Idempotent::AlreadyApplied` outcome short-circuits the transition
    /// helpers, and an `InvalidStateTransition` (genuine contradiction)
    /// is surfaced as an error rather than silently swallowed.
    ///
    /// `NotFound` is quiet-ignored: LND's `TrackPayments` replays
    /// in-flight + terminal payments on reconnect, and any payment that
    /// existed in LND before this gateway's first `send_payment` ran (or
    /// payments from a sibling gateway sharing the same LND) will fire
    /// an update against a `payment_hash` we have no row for.
    pub async fn handle_payment_update(&self, update: PaymentUpdate) -> Result<(), AppError> {
        let payment = match self
            .payments
            .find_by_payment_hash(&update.payment_hash)
            .await
        {
            Ok(p) => p,
            Err(e) if is_payment_not_found(&e) => {
                ::tracing::debug!(
                    payment_hash = %update.payment_hash.to_hex(),
                    "subscription update for unknown payment_hash; ignoring (likely sibling gateway or pre-existing LND payment)"
                );
                return Ok(());
            }
            Err(e) => return Err(PaymentError::from(e).into()),
        };

        let now = Timestamp::now();
        let amount_sat = payment.amount_msat.whole_sat() as i64;
        let payment_hash = payment.payment_hash;

        match update.status {
            SendPaymentStatus::InFlight => {
                // `IN_FLIGHT` is the synchronous-path's responsibility; the
                // subscription stream's at-least-once delivery means we may
                // see another one — no-op.
                Ok(())
            }
            SendPaymentStatus::Succeeded => {
                let preimage = update.payment_preimage.ok_or_else(|| {
                    AppError::Lnd(crate::lnd::LndError::InvalidResponse(
                        "Succeeded status but payment_preimage missing/malformed".to_owned(),
                    ))
                })?;
                match self
                    .transition_to_completed(
                        payment,
                        payment_hash,
                        amount_sat,
                        preimage,
                        update.fees_paid_msat,
                        update.route_hops,
                        now,
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    // Surface state-regression as an error — LND reporting
                    // SUCCEEDED for a payment we already marked Failed is
                    // a genuine contradiction worth investigating, not a
                    // duplicate replay (the entity-level idempotency
                    // guard would have caught a true duplicate first).
                    Err(e @ AppError::Payment(PaymentError::InvalidStateTransition { .. })) => {
                        ::tracing::error!(
                            payment_hash = %payment_hash.to_hex(),
                            error = %e,
                            "subscription Succeeded contradicts current state; surfacing"
                        );
                        Err(e)
                    }
                    Err(e) => Err(e),
                }
            }
            SendPaymentStatus::Failed => {
                let reason = update.failure_reason.unwrap_or_else(|| {
                    FailureReason::Other(
                        "LND TrackPayments emitted Failed with no reason".to_owned(),
                    )
                });
                match self
                    .transition_to_failed(payment, payment_hash, amount_sat, reason, now)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e @ AppError::Payment(PaymentError::InvalidStateTransition { .. })) => {
                        ::tracing::error!(
                            payment_hash = %payment_hash.to_hex(),
                            error = %e,
                            "subscription Failed contradicts current state; surfacing"
                        );
                        Err(e)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Subscription-driven settle (`Pending → Completed`). Used by
    /// `handle_payment_update` when LND's `TrackPayments` stream delivers
    /// a `Succeeded` for a payment already in Pending.
    #[allow(clippy::too_many_arguments)]
    async fn transition_to_completed(
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

    /// Subscription-driven fail (`Pending → Failed`).
    async fn transition_to_failed(
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
