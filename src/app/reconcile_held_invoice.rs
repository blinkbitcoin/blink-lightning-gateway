//! `App::reconcile_held_invoice` — periodic safety net for missed
//! subscription events.
//!
//! Branches on LND state:
//! - SETTLED — transition `Held → Settled` with LND's preimage; emit
//!   `LightningInvoiceSettled` at `held_amount_msat`.
//! - CANCELED — transition `Held → Canceled` with `CancelReason::Expired`;
//!   emit `LightningInvoiceCanceled` at `held_amount_msat`.
//! - ACCEPTED — still legitimately held; no-op (the auto-settle path
//!   hasn't run yet, OR Story 3.1's business gate is in progress, OR
//!   we're just between ticks).
//! - OPEN — unexpected (DB carries a `Held` event but LND says no HTLC
//!   parked); `warn!` and no-op.

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::helpers::is_invoice_not_found;
use crate::app::{App, AppError};
use crate::invoice::event::CancelReason;
use crate::invoice::{InvoiceError, InvoiceState};
use crate::lnd::LndInvoiceState;
use crate::outbox::NewOutboxEvent;
use crate::primitives::{PaymentHash, Timestamp};

impl App {
    pub async fn reconcile_held_invoice(&self, payment_hash: PaymentHash) -> Result<(), AppError> {
        let invoice = match self.invoices.find_by_payment_hash(&payment_hash).await {
            Ok(i) => i,
            Err(e) if is_invoice_not_found(&e) => {
                ::tracing::debug!(
                    payment_hash = %payment_hash.to_hex(),
                    "reconcile_held_invoice: invoice not found; ignoring"
                );
                return Ok(());
            }
            Err(e) => return Err(InvoiceError::from(e).into()),
        };

        if !matches!(invoice.state, InvoiceState::Held) {
            ::tracing::debug!(
                payment_hash = %payment_hash.to_hex(),
                current_state = %invoice.state,
                "reconcile_held_invoice: invoice no longer Held; skipping"
            );
            return Ok(());
        }

        let lnd_state = self.lnd.lookup_invoice(payment_hash).await?;

        match lnd_state.state {
            LndInvoiceState::Settled => {
                let preimage = lnd_state.payment_preimage.ok_or_else(|| {
                    AppError::Lnd(crate::lnd::LndError::InvalidResponse(
                        "lookup_invoice: SETTLED state but payment_preimage missing".to_owned(),
                    ))
                })?;
                self.reconcile_to_settled(invoice, payment_hash, preimage)
                    .await
            }
            LndInvoiceState::Canceled => self.reconcile_to_canceled(invoice, payment_hash).await,
            LndInvoiceState::Accepted => {
                ::tracing::trace!(
                    payment_hash = %payment_hash.to_hex(),
                    "reconcile_held_invoice: LND still ACCEPTED; no-op"
                );
                Ok(())
            }
            LndInvoiceState::Open => {
                // DB says Held → there's an `HtlcHeld` event in our log,
                // which only fires from `mark_held`, which only fires
                // from a real wire-side `Accepted`. LND saying OPEN
                // means either: (a) LND's invoice was canceled and then
                // re-issued under the same hash (it shouldn't permit
                // this — hashes are unique per `invoices`), or (b) state
                // diverged through a path we haven't accounted for.
                // Log and no-op; the operator will see this in metrics.
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    "reconcile_held_invoice: DB Held but LND OPEN; unexpected divergence"
                );
                Ok(())
            }
        }
    }

    /// Drive `Held → Settled` using LND-reported preimage. Mirrors
    /// `transition_to_invoice_settled` but factored here to keep the
    /// reconcile path self-contained — same outbox amount sourcing
    /// (AC12: `held_amount_msat`).
    async fn reconcile_to_settled(
        &self,
        mut invoice: crate::invoice::Invoice,
        payment_hash: PaymentHash,
        preimage: crate::primitives::Preimage,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        let now = Timestamp::now();
        match invoice.settle(preimage, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    "reconcile_to_settled: already applied; skipping outbox publish"
                );
                return Ok(());
            }
        }

        // `Invoice::settle` requires `Held` as source state, and the
        // outer `reconcile_held_invoice` only proceeds when `state == Held`
        // (which `mark_held` always pairs with `held_amount_msat = Some(..)`).
        let amount_sat = invoice
            .held_amount_msat
            .expect("reconcile_to_settled runs only when state == Held; mark_held sets held_amount_msat")
            .whole_sat() as i64;

        let mut tx = self.pool.begin().await?;
        self.invoices
            .update_in_op(&mut tx, &mut invoice)
            .await
            .map_err(InvoiceError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_invoice_settled(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "payment_preimage": preimage.to_hex(),
                        "wallet_id": wallet_id.to_string(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Drive `Held → Canceled` with `Expired` reason — LND's truth at
    /// the time of this poll. Same amount sourcing as the settled
    /// path: `held_amount_msat` (AC12 reconciliation invariant).
    async fn reconcile_to_canceled(
        &self,
        mut invoice: crate::invoice::Invoice,
        payment_hash: PaymentHash,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        let now = Timestamp::now();
        match invoice.cancel(CancelReason::Expired, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    "reconcile_to_canceled: already applied; skipping outbox publish"
                );
                return Ok(());
            }
        }

        let amount_sat = invoice
            .held_amount_msat
            .map(|m| m.whole_sat() as i64)
            .unwrap_or(0);

        let mut tx = self.pool.begin().await?;
        self.invoices
            .update_in_op(&mut tx, &mut invoice)
            .await
            .map_err(InvoiceError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_invoice_canceled(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "reason": CancelReason::Expired.as_str(),
                        "wallet_id": wallet_id.to_string(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
