//! `lnInvoicePaymentSend` use-case + the three transition helpers it
//! drives (`transition_to_pending`, `settle_inline`, `fail_inline`).

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::helpers::{is_concurrent_modification, lnd_error_to_failure_reason};
use crate::app::{decode, App, AppError, SendPaymentRequest};
use crate::fees::LnFees;
use crate::lnd::{SendPaymentParams, SendPaymentStatus};
use crate::outbox::NewOutboxEvent;
use crate::payment::{FailureReason, Hop, NewPayment, Payment, PaymentError};
use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId};
use crate::symphony::{
    is_authorize_unavailable, AccountKind, AccountRef, SymphonyAuthorizeRequest,
    SymphonyAuthorizeStatus,
};

impl App {
    /// `lnInvoicePaymentSend` use-case. ADR-0003 §5 ordering (mirrors galoy's
    /// `executePaymentViaLn`, `send-lightning.ts:725-820`): ownership → decode →
    /// persist intent → synchronous `authorize_spend` → LND → dispatch on status.
    ///
    /// Two invariants carry the design: the balance-gating Cala hold is created
    /// by `authorize_spend` BEFORE LND and fails closed (any auth error declines,
    /// LND never called); and the intent row is persisted before authorize, so no
    /// spend is authorized without a durable row for the orphan-hold sweep (AC10).
    pub async fn send_payment(&self, request: SendPaymentRequest) -> Result<Payment, AppError> {
        let now = Timestamp::now();

        self.check_wallet_ownership(&request.caller_auth, &request.wallet_id)
            .await?;

        let decoded = decode::decode_bolt11(&request.payment_request)?;

        // Story 2.2 drives only the amount-carrying path; the amountless
        // entrypoint (`lnNoAmountInvoicePaymentSend`) lands in Story 5.1.
        let amount_msat = decoded.amount_msat.ok_or(PaymentError::AmountRequired)?;

        // Fee policy.
        let max_fee_msat = LnFees::max_for(amount_msat);

        // Persist intent + emit OutgoingPaymentInitiated in one tx. Initiated
        // is emitted exactly ONCE here, so every terminal path (sync or
        // subscription) emits just its terminal event and Symphony's JOIN
        // always finds the Initiated row. This is only the outbox event — the
        // domain state stays `initiated` until LND's outcome is known (the
        // orphan-hold sweep's anchor), never moved to `pending` before LND.
        // UNIQUE-violation on `payment_hash` → `AlreadyPaid`.
        let payment_hash = decoded.payment_hash;
        let destination = decoded.destination.clone();
        let bolt_invoice = decoded.bolt_invoice.clone();
        let amount_sat = amount_msat.whole_sat() as i64;
        let new_payment = NewPayment::try_new(decoded, request.wallet_id, None, max_fee_msat, now)?;
        let mut tx = self.pool.begin().await?;
        let payment = self
            .payments
            .create_in_op(&mut tx, new_payment)
            .await
            .map_err(PaymentError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                self.build_initiated_outbox(
                    payment_hash,
                    request.wallet_id,
                    amount_sat,
                    max_fee_msat,
                    &destination,
                    &bolt_invoice,
                ),
            )
            .await?;
        tx.commit().await?;

        // Synchronous AuthorizeSpend (ADR-0003): the balance-gating hold is
        // created HERE, before LND, covering the worst-case `amount +
        // max_fee`. correlation_id == idempotency_key == payment_hash
        // so retries for the same hash resolve to one authorization.
        let hold_sat = MilliSatoshi::new(amount_msat.as_u64() + max_fee_msat.as_u64())
            .round_up_to_sat()
            .whole_sat();
        let symphony_resp = match self
            .symphony
            .authorize_spend(SymphonyAuthorizeRequest {
                correlation_id: payment_hash.to_hex(),
                account: AccountRef {
                    kind: AccountKind::WalletLiability,
                    id: request.wallet_id.to_string(),
                },
                sat_amount: hold_sat,
                idempotency_key: payment_hash.to_hex(),
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let reason = if is_authorize_unavailable(&e) {
                    FailureReason::Other(format!("Symphony service unavailable: {e}"))
                } else {
                    FailureReason::Other(format!("Symphony error: {e}"))
                };
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %request.wallet_id,
                    correlation_id = %payment_hash.to_hex(),
                    error = %e,
                    "AuthorizeSpend failed; declining payment fail-closed (LND not called)"
                );
                return self
                    .fail_inline(payment, payment_hash, amount_sat, reason, now)
                    .await;
            }
        };
        if matches!(symphony_resp.status, SymphonyAuthorizeStatus::Declined) {
            // Symphony declined: resolve to Failed. Initiated was already emitted
            // at create, so this just adds the terminal event.
            let reason = match symphony_resp.decline_reason {
                Some(crate::symphony::DeclineReason::InsufficientFunds) => {
                    FailureReason::InsufficientBalance
                }
                Some(other) => FailureReason::Other(format!("Symphony declined: {other:?}")),
                None => FailureReason::Other("Symphony declined: no reason".to_owned()),
            };
            return self
                .fail_inline(payment, payment_hash, amount_sat, reason, now)
                .await;
        }

        // 6. LND send. On error after persist, roll the row to Failed so
        //    Symphony can release the hold via OutgoingPaymentFailed.
        let lnd_resp = match self
            .lnd
            .send_payment(SendPaymentParams {
                bolt_invoice: bolt_invoice.clone(),
                max_fee_msat,
                timeout_seconds: 60,
            })
            .await
        {
            Ok(resp) => resp,
            Err(lnd_err) => {
                let reason = lnd_error_to_failure_reason(&lnd_err);
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    error = %lnd_err,
                    "LND send_payment errored after persist; rolling Payment to Failed"
                );
                return self
                    .fail_inline(payment, payment_hash, amount_sat, reason, now)
                    .await;
            }
        };

        // 7. Verify LND echoed the same payment_hash we submitted; a mismatch
        //    would bind our DB row to a different actual payment.
        if lnd_resp.payment_hash != payment_hash {
            ::tracing::error!(
                expected = %payment_hash.to_hex(),
                got = %lnd_resp.payment_hash.to_hex(),
                "LND returned mismatched payment_hash; failing the payment"
            );
            let reason = FailureReason::Other(format!(
                "LND payment_hash mismatch: expected {}, got {}",
                payment_hash.to_hex(),
                lnd_resp.payment_hash.to_hex()
            ));
            return self
                .fail_inline(payment, payment_hash, amount_sat, reason, now)
                .await;
        }

        // 8. Dispatch on LND's response status.
        match lnd_resp.status {
            SendPaymentStatus::InFlight => {
                self.transition_to_pending(payment, payment_hash, now).await
            }
            SendPaymentStatus::Succeeded => {
                let preimage = lnd_resp.payment_preimage.ok_or_else(|| {
                    AppError::Lnd(crate::lnd::LndError::InvalidResponse(
                        "Succeeded status but payment_preimage missing/malformed".to_owned(),
                    ))
                })?;
                self.settle_inline(
                    payment,
                    payment_hash,
                    amount_sat,
                    preimage,
                    lnd_resp.fees_paid_msat,
                    lnd_resp.route_hops,
                    now,
                )
                .await
            }
            SendPaymentStatus::Failed => {
                let reason = lnd_resp.failure_reason.unwrap_or_else(|| {
                    FailureReason::Other("LND returned Failed with no reason".to_owned())
                });
                self.fail_inline(payment, payment_hash, amount_sat, reason, now)
                    .await
            }
        }
    }

    /// `Initiated → Pending` for the sync `InFlight` path. State-only update —
    /// Initiated was already emitted at create, so no outbox write here. On
    /// `ConcurrentModification` (the subscription handler beat us to a terminal
    /// transition), reload and return the DB's state instead of erroring.
    async fn transition_to_pending(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        match payment.mark_pending(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "mark_pending ignored — duplicate IN_FLIGHT replay",
                );
                return Ok(payment);
            }
        }

        match self.payments.update(&mut payment).await {
            Ok(_) => Ok(payment),
            Err(e) if is_concurrent_modification(&e) => {
                let reloaded = self
                    .payments
                    .find_by_payment_hash(&payment_hash)
                    .await
                    .map_err(PaymentError::from)?;
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    state = %reloaded.state,
                    "transition_to_pending: concurrent modification; reloaded"
                );
                Ok(reloaded)
            }
            Err(e) => Err(PaymentError::from(e).into()),
        }
    }

    /// In-request settle for the sync `Succeeded` path: record the `Pending`
    /// step (LND's first message is already `Succeeded`, so the intent passes
    /// straight through `Pending`), then commit via [`App::commit_completed`]
    /// (the sole writer of `OutgoingPaymentCompleted`). Initiated was already
    /// emitted at create.
    #[allow(clippy::too_many_arguments)]
    async fn settle_inline(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        amount_sat: i64,
        preimage: Preimage,
        fees_paid_msat: MilliSatoshi,
        route_hops: Vec<Hop>,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        match payment.mark_pending(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "settle_inline: mark_pending ignored (duplicate); proceeding to settle"
                );
            }
        }
        self.commit_completed(
            payment,
            payment_hash,
            amount_sat,
            preimage,
            fees_paid_msat,
            route_hops,
            now,
        )
        .await
    }

    /// In-request fail for the sync failure paths (LND `Failed`, Symphony
    /// declined/errored, or `send_payment` errored after persist): record the
    /// `Pending` step, then commit via [`App::commit_failed`] (the sole writer
    /// of `OutgoingPaymentFailed`). Initiated was already emitted at create.
    async fn fail_inline(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        amount_sat: i64,
        failure_reason: FailureReason,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        match payment.mark_pending(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fail_inline: mark_pending ignored (duplicate); proceeding to fail"
                );
            }
        }
        self.commit_failed(payment, payment_hash, amount_sat, failure_reason, now)
            .await
    }

    fn build_initiated_outbox(
        &self,
        payment_hash: PaymentHash,
        wallet_id: WalletId,
        amount_sat: i64,
        max_fee_msat: MilliSatoshi,
        destination: &str,
        bolt_invoice: &BoltInvoice,
    ) -> NewOutboxEvent {
        NewOutboxEvent::for_lightning_payment_initiated(
            payment_hash.to_hex(),
            payment_hash.to_hex(),
            amount_sat,
            Utc::now(),
            serde_json::json!({
                "max_fee_msat": max_fee_msat.as_u64(),
                "destination": destination,
                "wallet_id": wallet_id.to_string(),
                "bolt_invoice": bolt_invoice.as_str(),
            }),
        )
    }
}
