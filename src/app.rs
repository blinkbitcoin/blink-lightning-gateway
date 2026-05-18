//! Application coordinator — single `App` struct (NOT folder of
//! per-aggregate services) per architecture L940 and ADR #1.
//!
//! Slice 1 carries `App::create_invoice` (inbound). Slice 2 adds the
//! outbound counterparts: `send_payment`, `fee_probe`, and the
//! subscription-driven `handle_payment_update`. All `impl App` methods
//! live in this file until it grows large enough to justify splitting.

use chrono::Utc;
use sqlx::PgPool;
use std::sync::Arc;

pub mod decode;
pub mod error;

pub use error::AppError;

use crate::fees::LnFees;
use crate::invoice::{Invoice, Invoices, NewInvoice};
use crate::lnd::{
    AddInvoiceParams, FeeProbeParams, LndApi, PaymentUpdate, SendPaymentParams, SendPaymentStatus,
};
use crate::outbox::{EventPublisher, NewOutboxEvent};
use crate::payment::{NewPayment, Payment, PaymentError, PaymentEvent, Payments};
use crate::primitives::{MilliSatoshi, PaymentHash, Timestamp, WalletId};
use crate::symphony::{SymphonyAuthorizeRequest, SymphonyAuthorizeStatus, SymphonyClient};

use es_entity::{EsEntity, Idempotent};

/// Operating mode. `DryRun` short-circuits LND + DB writes — useful for
/// FR2's eventual shadow-mode plumbing. Slice 1a only ever runs `Live`;
/// the variant exists so future shadow-mode work has a defined home.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Live,
    DryRun,
}

#[derive(Clone, Debug)]
pub struct NewInvoiceRequest {
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_seconds: u32,
    pub memo: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SendPaymentRequest {
    pub wallet_id: WalletId,
    pub payment_request: String,
    pub memo: Option<String>,
}

#[derive(Clone, Debug)]
pub struct FeeProbeRequest {
    pub wallet_id: WalletId,
    pub payment_request: String,
}

#[derive(Clone)]
pub struct App {
    invoices: Invoices,
    payments: Payments,
    lnd: Arc<dyn LndApi>,
    outbox: EventPublisher,
    symphony: Arc<dyn SymphonyClient>,
    pool: PgPool,
    mode: Mode,
}

impl App {
    pub fn new(
        pool: PgPool,
        lnd: Arc<dyn LndApi>,
        outbox: EventPublisher,
        symphony: Arc<dyn SymphonyClient>,
    ) -> Self {
        Self {
            invoices: Invoices::new(&pool),
            payments: Payments::new(&pool),
            lnd,
            outbox,
            symphony,
            pool,
            mode: Mode::Live,
        }
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `lnInvoiceCreate` use-case.
    pub async fn create_invoice(&self, request: NewInvoiceRequest) -> Result<Invoice, AppError> {
        let now = Timestamp::now();
        self.check_wallet_ownership(&request.wallet_id).await?;

        let lnd_resp = self
            .lnd
            .add_invoice(AddInvoiceParams {
                amount_msat: request.amount_msat,
                memo: request.memo,
                expiry_seconds: request.expiry_seconds,
            })
            .await?;

        let new_invoice = NewInvoice::try_new(
            lnd_resp.payment_hash,
            request.wallet_id,
            request.amount_msat,
            request.expiry_seconds,
            lnd_resp.bolt_invoice,
            now,
        )?;

        if matches!(self.mode, Mode::DryRun) {
            return Err(AppError::WalletOwnership(
                "DryRun mode not yet wired in slice 1a".to_owned(),
            ));
        }

        let mut tx = self.pool.begin().await?;
        let invoice = self
            .invoices
            .create_in_op(&mut tx, new_invoice)
            .await
            .map_err(crate::invoice::InvoiceError::from)?;
        tx.commit().await?;

        Ok(invoice)
    }

    /// `lnInvoicePaymentSend` use-case.
    ///
    /// Flow (mirrors galoy's `executePaymentViaLn` at
    /// `blink/core/api/src/app/payments/send-lightning.ts:725-820`):
    ///   1. (STUB) wallet-ownership check.
    ///   2. Decode the BOLT11 (pure-Rust via `lightning-invoice`).
    ///   3. Compute `max_fee_msat = LnFees::max_for(amount_msat)`.
    ///   4. (STUB) `Symphony::authorize_spend`.
    ///   5. Persist `NewPayment` + `Initiated` event in one tx (no outbox row yet —
    ///      `OutgoingPaymentInitiated` fires only after LND accepts as IN_FLIGHT).
    ///   6. Call LND `send_payment`.
    ///   7. Dispatch by status:
    ///      - `InFlight` → mark `Pending`, publish `OutgoingPaymentInitiated`.
    ///      - `Succeeded` → mark `Completed`, publish `OutgoingPaymentCompleted`.
    ///      - `Failed` → mark `Failed`, publish `OutgoingPaymentFailed`.
    pub async fn send_payment(&self, request: SendPaymentRequest) -> Result<Payment, AppError> {
        let now = Timestamp::now();

        // 1. STUB(story-2.5): wallet-ownership check.
        self.check_wallet_ownership(&request.wallet_id).await?;

        // 2. Decode the BOLT11.
        let decoded = decode::decode_bolt11(&request.payment_request)?;

        // Story 2.2 drives only the amount-carrying path; the amountless
        // App entrypoint (`lnNoAmountInvoicePaymentSend`, with a
        // caller-supplied amount) lands in Story 5.1. An amountless
        // invoice has no amount source here, so reject early with the
        // same error `NewPayment::try_new` would raise.
        let amount_msat = decoded.amount_msat.ok_or(PaymentError::AmountRequired)?;

        // 3. Fee policy.
        let max_fee_msat = LnFees::max_for(amount_msat);

        // 4. STUB(story-2.5): real Symphony::authorize_spend roundtrip lands
        //    in the cross-repo PR + Story 2.5. Slice-2 ships a stub that
        //    always returns Approved; surface the gRPC shape now so the
        //    un-stub is body-only later.
        //
        //    ADR-0003: this call is the gateway's `recordSendOffChain`
        //    equivalent — when un-stubbed in Story 2.5 it MUST run
        //    synchronously and atomically (check + Cala hold) BEFORE the
        //    LND call below, and fail closed. The request carries only
        //    rail-neutral fields; the card-specific `original_usd_cents`
        //    / `exchange_rate` / `merchant_info` are deliberately absent.
        let symphony_resp = self
            .symphony
            .authorize_spend(SymphonyAuthorizeRequest {
                correlation_id: decoded.payment_hash.to_hex(),
                account_id: request.wallet_id.to_string(),
                sat_amount: amount_msat.as_u64() / 1000,
                idempotency_key: decoded.payment_hash.to_hex(),
            })
            .await?;
        if matches!(symphony_resp.status, SymphonyAuthorizeStatus::Declined) {
            return Err(AppError::Symphony(
                crate::symphony::SymphonyError::Declined {
                    reason: symphony_resp.decline_reason.unwrap_or(
                        crate::symphony::DeclineReason::Other(
                            "no decline reason returned".to_owned(),
                        ),
                    ),
                },
            ));
        }

        // 5. Persist intent. `requested_amount_msat` is `None` — this is
        //    the amount-carrying path; the amount comes from the invoice.
        let payment_hash = decoded.payment_hash;
        let destination = decoded.destination.clone();
        let new_payment = NewPayment::try_new(decoded, request.wallet_id, None, max_fee_msat, now)?;
        let mut tx = self.pool.begin().await?;
        let payment = self
            .payments
            .create_in_op(&mut tx, new_payment)
            .await
            .map_err(PaymentError::from)?;
        tx.commit().await?;

        // 6. LND first, then DB update + outbox row. Order matches
        //    `App::create_invoice`'s LND-then-DB pattern.
        let lnd_resp = self
            .lnd
            .send_payment(SendPaymentParams {
                bolt_invoice: payment
                    .events()
                    .iter_all()
                    .find_map(|e| match e {
                        PaymentEvent::Initiated { bolt_invoice, .. } => Some(bolt_invoice.clone()),
                        _ => None,
                    })
                    .expect("initiated event present immediately after create_in_op"),
                max_fee_msat,
                timeout_seconds: 60,
            })
            .await?;

        // 7. Dispatch.
        let amount_sat = (amount_msat.as_u64() / 1000) as i64;
        match lnd_resp.status {
            SendPaymentStatus::InFlight => {
                self.transition_to_pending(
                    payment,
                    payment_hash,
                    destination,
                    amount_sat,
                    max_fee_msat,
                    now,
                )
                .await
            }
            SendPaymentStatus::Succeeded => {
                self.transition_to_completed(
                    payment,
                    payment_hash,
                    amount_sat,
                    lnd_resp.payment_preimage.ok_or_else(|| {
                        AppError::Lnd(crate::lnd::LndError::InvalidResponse(
                            "succeeded without preimage".to_owned(),
                        ))
                    })?,
                    lnd_resp.fees_paid_msat,
                    lnd_resp.route_hops,
                    now,
                )
                .await
            }
            SendPaymentStatus::Failed => {
                self.transition_to_failed(
                    payment,
                    payment_hash,
                    amount_sat,
                    lnd_resp
                        .failure_reason
                        .unwrap_or(crate::payment::FailureReason::Other(
                            "LND returned Failed with no reason".to_owned(),
                        )),
                    now,
                )
                .await
            }
        }
    }

    /// `lnInvoiceFeeProbe` use-case — straight-through; no DB writes,
    /// no outbox events.
    pub async fn fee_probe(&self, request: FeeProbeRequest) -> Result<MilliSatoshi, AppError> {
        self.check_wallet_ownership(&request.wallet_id).await?;
        let decoded = decode::decode_bolt11(&request.payment_request)?;
        let resp = self
            .lnd
            .fee_probe(FeeProbeParams {
                bolt_invoice: decoded.bolt_invoice,
            })
            .await?;
        Ok(resp.fee_msat)
    }

    /// Subscription-driven update from LND's `Router/TrackPayments`
    /// stream. Idempotent against duplicates — an already-`Completed`
    /// payment receiving another `Succeeded` event swallows the
    /// illegal-transition error and returns Ok(()).
    pub async fn handle_payment_update(&self, update: PaymentUpdate) -> Result<(), AppError> {
        let payment = self
            .payments
            .find_by_payment_hash(&update.payment_hash)
            .await
            .map_err(PaymentError::from)?;

        let now = Timestamp::now();
        let destination = payment
            .events()
            .iter_all()
            .find_map(|e| match e {
                PaymentEvent::Initiated { destination, .. } => Some(destination.clone()),
                _ => None,
            })
            .unwrap_or_default();
        let amount_sat = (payment.amount_msat.as_u64() / 1000) as i64;
        let max_fee_msat = payment.max_fee_msat;
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
                        "succeeded without preimage".to_owned(),
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
                    Err(AppError::Payment(PaymentError::InvalidStateTransition { .. })) => {
                        ::tracing::warn!(
                            payment_hash = %payment_hash,
                            "duplicate Succeeded for already-terminal payment; ignoring"
                        );
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            SendPaymentStatus::Failed => {
                let reason = update
                    .failure_reason
                    .unwrap_or(crate::payment::FailureReason::Other(
                        "no reason from LND".to_owned(),
                    ));
                match self
                    .transition_to_failed(payment, payment_hash, amount_sat, reason, now)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(AppError::Payment(PaymentError::InvalidStateTransition { .. })) => {
                        ::tracing::warn!(
                            payment_hash = %payment_hash,
                            "duplicate Failed for already-terminal payment; ignoring"
                        );
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
        }
        .map(|_| {
            // Silence the unused fields warning in the InFlight branch.
            let _ = destination;
            let _ = max_fee_msat;
        })
    }

    async fn transition_to_pending(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        destination: String,
        amount_sat: i64,
        max_fee_msat: MilliSatoshi,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        let events = match payment.mark_pending(now)? {
            Idempotent::Executed(events) => events,
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "mark_pending ignored — duplicate IN_FLIGHT replay",
                );
                return Ok(payment);
            }
        };
        payment.events_mut().extend(events);
        payment.state = crate::payment::PaymentState::Pending;

        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_payment_initiated(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "max_fee_msat": max_fee_msat.as_u64(),
                        "destination": destination,
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(payment)
    }

    #[allow(clippy::too_many_arguments)]
    async fn transition_to_completed(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        amount_sat: i64,
        preimage: crate::primitives::Preimage,
        fees_paid_msat: MilliSatoshi,
        route_hops: Vec<crate::payment::Hop>,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        let events = match payment.settle(preimage, fees_paid_msat, route_hops.clone(), now)? {
            Idempotent::Executed(events) => events,
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "settle ignored — duplicate SUCCEEDED replay",
                );
                return Ok(payment);
            }
        };
        payment.events_mut().extend(events);
        payment.state = crate::payment::PaymentState::Completed;

        let mut tx = self.pool.begin().await?;
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;
        let hops_json: Vec<_> = route_hops
            .iter()
            .map(|h| {
                serde_json::json!({
                    "pub_key": h.pub_key,
                    "channel_id": h.channel_id,
                    "fee_msat": h.fee_msat.as_u64(),
                    "amt_msat": h.amt_msat.as_u64(),
                })
            })
            .collect();
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

    async fn transition_to_failed(
        &self,
        mut payment: Payment,
        payment_hash: PaymentHash,
        amount_sat: i64,
        failure_reason: crate::payment::FailureReason,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        let reason_str = failure_reason.as_str().to_owned();
        let events = match payment.fail(failure_reason, now)? {
            Idempotent::Executed(events) => events,
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %payment.state,
                    "fail ignored — duplicate FAILED replay",
                );
                return Ok(payment);
            }
        };
        payment.events_mut().extend(events);
        payment.state = crate::payment::PaymentState::Failed;

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
                        "failure_reason": reason_str,
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(payment)
    }

    /// STUB(story-2.5): replace with Apollo Router entity sub-query + TTL
    /// cache.
    async fn check_wallet_ownership(&self, _wallet_id: &WalletId) -> Result<(), AppError> {
        Ok(())
    }
}
