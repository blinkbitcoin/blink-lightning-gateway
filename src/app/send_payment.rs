//! `lnInvoicePaymentSend` use-case + the three transition helpers it
//! drives (`transition_to_pending`, `complete_fast_settled`,
//! `complete_fast_failed`).

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::helpers::{
    hops_to_json, is_concurrent_modification, is_payment_hash_unique_violation,
    lnd_error_to_failure_reason,
};
use crate::app::{decode, App, AppError, SendPaymentRequest};
use crate::fees::LnFees;
use crate::lnd::{SendPaymentParams, SendPaymentStatus};
use crate::outbox::NewOutboxEvent;
use crate::payment::{FailureReason, Hop, NewPayment, Payment, PaymentError};
use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId};
use crate::symphony::{SymphonyAuthorizeRequest, SymphonyAuthorizeStatus};

impl App {
    /// `lnInvoicePaymentSend` use-case.
    ///
    /// Flow (mirrors galoy's `executePaymentViaLn` at
    /// `blink/core/api/src/app/payments/send-lightning.ts:725-820`):
    ///   1. (STUB) wallet-ownership check.
    ///   2. Decode the BOLT11 (pure-Rust via `lightning-invoice`).
    ///   3. Compute `max_fee_msat = LnFees::max_for(amount_msat)`.
    ///   4. Persist `NewPayment` + `Initiated` event (no outbox row yet —
    ///      `OutgoingPaymentInitiated` fires only after LND accepts the send).
    ///      A UNIQUE-violation on `payment_hash` here surfaces as
    ///      `PaymentError::AlreadyPaid` so the GraphQL resolver can return
    ///      `PaymentSendResult::AlreadyPaid` to the caller.
    ///   5. (STUB) `Symphony::authorize_spend`. Reordered AFTER persist so
    ///      a persist failure cannot leave Symphony holding an orphan
    ///      authorization.
    ///   6. Call LND `send_payment`. On error AFTER persist, transition
    ///      the row to `Failed` so it doesn't orphan in `Initiated` state
    ///      with no outbox row Symphony can use to release the hold.
    ///   7. Verify `lnd_resp.payment_hash` matches the decoded hash —
    ///      tonic stream reordering or LND bug could otherwise associate
    ///      the row with the wrong actual payment.
    ///   8. Dispatch by status:
    ///      - `InFlight` → mark `Pending`, publish `OutgoingPaymentInitiated`.
    ///      - `Succeeded` (fast-settle) → mark `Pending` + `Completed` in
    ///        one tx, publish both `OutgoingPaymentInitiated` and
    ///        `OutgoingPaymentCompleted` so Symphony's JOIN on
    ///        correlation_id always finds both rows.
    ///      - `Failed` (fast-fail) → mark `Pending` + `Failed` in one tx,
    ///        publish both `OutgoingPaymentInitiated` and
    ///        `OutgoingPaymentFailed`.
    pub async fn send_payment(&self, request: SendPaymentRequest) -> Result<Payment, AppError> {
        let now = Timestamp::now();

        // 1. STUB(story-2.5): wallet-ownership check.
        self.check_wallet_ownership(&request.wallet_id).await?;

        // 2. Decode the BOLT11.
        let decoded = decode::decode_bolt11(&request.payment_request)?;

        // Story 2.2 drives only the amount-carrying path; the amountless
        // App entrypoint (`lnNoAmountInvoicePaymentSend`, with a
        // caller-supplied amount) lands in Story 5.1.
        let amount_msat = decoded.amount_msat.ok_or(PaymentError::AmountRequired)?;

        // 3. Fee policy.
        let max_fee_msat = LnFees::max_for(amount_msat);

        // 4. Persist intent FIRST (was step 5 pre-review). A failure here
        //    short-circuits Symphony + LND, so the gateway never authorizes
        //    a spend it didn't durably record. UNIQUE-violation on
        //    `payment_hash` → `AlreadyPaid` (LN's own dedup invariant: the
        //    second attempt cannot move different money than the first).
        let payment_hash = decoded.payment_hash;
        let destination = decoded.destination.clone();
        let bolt_invoice = decoded.bolt_invoice.clone();
        let new_payment = NewPayment::try_new(decoded, request.wallet_id, None, max_fee_msat, now)?;
        let mut tx = self.pool.begin().await?;
        let payment = match self.payments.create_in_op(&mut tx, new_payment).await {
            Ok(p) => p,
            Err(e) => {
                if is_payment_hash_unique_violation(&e) {
                    return Err(AppError::Payment(PaymentError::AlreadyPaid {
                        payment_hash: payment_hash.to_hex(),
                    }));
                }
                return Err(PaymentError::from(e).into());
            }
        };
        tx.commit().await?;

        // 5. Symphony authorize. STUB(story-2.5): real
        //    `Symphony::authorize_spend` roundtrip lands in the cross-repo
        //    PR + Story 2.5. ADR-0003: when un-stubbed it MUST run
        //    synchronously and atomically (check + Cala hold) and fail
        //    closed. `correlation_id == idempotency_key == payment_hash`
        //    is deliberate (B6 review decision): LND's own payment-hash
        //    dedup makes `payment_hash` the canonical retry key for LN
        //    payments, so two attempts for the same hash MUST resolve to
        //    the same Symphony authorization decision.
        let symphony_resp = self
            .symphony
            .authorize_spend(SymphonyAuthorizeRequest {
                correlation_id: payment_hash.to_hex(),
                account_id: request.wallet_id.to_string(),
                sat_amount: amount_msat.whole_sat(),
                idempotency_key: payment_hash.to_hex(),
            })
            .await?;
        if matches!(symphony_resp.status, SymphonyAuthorizeStatus::Declined) {
            // Symphony declined: roll the row forward to Failed so it
            // doesn't orphan in Initiated. Outbox emits Initiated + Failed
            // together (fast-fail shape) so Symphony's downstream side
            // sees a complete lifecycle.
            let reason = match symphony_resp.decline_reason {
                Some(crate::symphony::DeclineReason::InsufficientFunds) => {
                    FailureReason::InsufficientBalance
                }
                Some(other) => FailureReason::Other(format!("Symphony declined: {other:?}")),
                None => FailureReason::Other("Symphony declined: no reason".to_owned()),
            };
            return self
                .complete_fast_failed(
                    payment,
                    payment_hash,
                    bolt_invoice,
                    destination,
                    request.wallet_id,
                    amount_msat,
                    max_fee_msat,
                    reason,
                    now,
                )
                .await;
        }

        // 6. LND send. On error AFTER persist (E2 orphan-recovery), roll
        //    the row to Failed so Symphony can release the hold via the
        //    OutgoingPaymentFailed handler.
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
                    .complete_fast_failed(
                        payment,
                        payment_hash,
                        bolt_invoice,
                        destination,
                        request.wallet_id,
                        amount_msat,
                        max_fee_msat,
                        reason,
                        now,
                    )
                    .await;
            }
        };

        // 7. Verify LND echoed back the same payment_hash we submitted.
        //    Mismatch would mean the gateway's DB row would not match the
        //    actual payment LND is tracking.
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
                .complete_fast_failed(
                    payment,
                    payment_hash,
                    bolt_invoice,
                    destination,
                    request.wallet_id,
                    amount_msat,
                    max_fee_msat,
                    reason,
                    now,
                )
                .await;
        }

        // 8. Dispatch on LND's response status.
        match lnd_resp.status {
            SendPaymentStatus::InFlight => {
                self.transition_to_pending(
                    payment,
                    payment_hash,
                    destination,
                    request.wallet_id,
                    amount_msat,
                    max_fee_msat,
                    bolt_invoice,
                    now,
                )
                .await
            }
            SendPaymentStatus::Succeeded => {
                let preimage = lnd_resp.payment_preimage.ok_or_else(|| {
                    AppError::Lnd(crate::lnd::LndError::InvalidResponse(
                        "Succeeded status but payment_preimage missing/malformed".to_owned(),
                    ))
                })?;
                self.complete_fast_settled(
                    payment,
                    payment_hash,
                    bolt_invoice,
                    destination,
                    request.wallet_id,
                    amount_msat,
                    max_fee_msat,
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
                self.complete_fast_failed(
                    payment,
                    payment_hash,
                    bolt_invoice,
                    destination,
                    request.wallet_id,
                    amount_msat,
                    max_fee_msat,
                    reason,
                    now,
                )
                .await
            }
        }
    }

    /// `Initiated → Pending` for the sync `InFlight` path. On
    /// `ConcurrentModification` (the subscription handler beat us to a
    /// terminal transition for the same payment), reload from the DB; if
    /// the projection has moved to a terminal state, return the reloaded
    /// row instead of erroring (the user will see Pending/Success).
    #[allow(clippy::too_many_arguments)]
    async fn transition_to_pending(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        destination: String,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        max_fee_msat: MilliSatoshi,
        bolt_invoice: BoltInvoice,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        match payment.mark_pending(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "mark_pending ignored — duplicate IN_FLIGHT replay",
                );
                return Ok(payment);
            }
        }

        let amount_sat = amount_msat.whole_sat() as i64;
        let mut tx = self.pool.begin().await?;
        match self.payments.update_in_op(&mut tx, &mut payment).await {
            Ok(_) => {}
            Err(e) if is_concurrent_modification(&e) => {
                // Subscription handler beat us to a terminal transition.
                // Drop our tx and reload; whatever state the DB now shows
                // is the source of truth.
                drop(tx);
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
                return Ok(reloaded);
            }
            Err(e) => return Err(PaymentError::from(e).into()),
        };
        self.outbox
            .publish_in_tx(
                &mut tx,
                self.build_initiated_outbox(
                    payment_hash,
                    wallet_id,
                    amount_sat,
                    max_fee_msat,
                    &destination,
                    &bolt_invoice,
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(payment)
    }

    /// Sync fast-settle: LND's first stream message is `Succeeded`.
    /// Emits both `OutgoingPaymentInitiated` and `OutgoingPaymentCompleted`
    /// in one transaction so Symphony's `LIGHTNING_PAYMENT_OUT` JOIN on
    /// correlation_id always finds the prior Initiated row.
    #[allow(clippy::too_many_arguments)]
    async fn complete_fast_settled(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        bolt_invoice: BoltInvoice,
        destination: String,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        max_fee_msat: MilliSatoshi,
        preimage: Preimage,
        fees_paid_msat: MilliSatoshi,
        route_hops: Vec<Hop>,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        // Queue Pending + Completed events in one go; update_in_op
        // persists both atomically.
        match payment.mark_pending(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fast_settled: mark_pending ignored (duplicate); proceeding to settle"
                );
            }
        }
        match payment.settle(preimage, fees_paid_msat, route_hops.clone(), now)? {
            Idempotent::Executed(()) => {}
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fast_settled: settle ignored — duplicate SUCCEEDED replay"
                );
                return Ok(payment);
            }
        }

        let amount_sat = amount_msat.whole_sat() as i64;
        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                self.build_initiated_outbox(
                    payment_hash,
                    wallet_id,
                    amount_sat,
                    max_fee_msat,
                    &destination,
                    &bolt_invoice,
                ),
            )
            .await?;
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

    /// Sync fast-fail: LND's first message is `Failed`, OR Symphony
    /// declined, OR `send_payment` itself errored after persist (E2
    /// orphan recovery). Emits both `OutgoingPaymentInitiated` and
    /// `OutgoingPaymentFailed` in one transaction.
    #[allow(clippy::too_many_arguments)]
    async fn complete_fast_failed(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        bolt_invoice: BoltInvoice,
        destination: String,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        max_fee_msat: MilliSatoshi,
        failure_reason: FailureReason,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        let reason_detail = failure_reason.detail_str();
        match payment.mark_pending(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fast_failed: mark_pending ignored (duplicate); proceeding to fail"
                );
            }
        }
        match payment.fail(failure_reason, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fast_failed: fail ignored — duplicate FAILED replay"
                );
                return Ok(payment);
            }
        }

        let amount_sat = amount_msat.whole_sat() as i64;
        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                self.build_initiated_outbox(
                    payment_hash,
                    wallet_id,
                    amount_sat,
                    max_fee_msat,
                    &destination,
                    &bolt_invoice,
                ),
            )
            .await?;
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
