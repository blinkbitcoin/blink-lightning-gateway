//! Sole writers of the `LightningInvoiceSettled` /
//! `LightningInvoiceCanceled` outbox rows. Called from
//! `settle_hold_invoice`, `reconcile_held_invoice`, and the Canceled
//! subscription arm. Outbox `amount_sat` always sources from LND's
//! `amt_paid_msat`; metadata carries `held_amount_msat` so Symphony
//! can offset its pending reservation regardless of MPP overpayment.

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::{App, AppError};
use crate::invoice::event::CancelReason;
use crate::invoice::{Invoice, InvoiceError};
use crate::outbox::NewOutboxEvent;
use crate::primitives::{MilliSatoshi, PaymentHash, Timestamp};

impl App {
    pub(crate) async fn commit_settle(
        &self,
        mut invoice: Invoice,
        payment_hash: PaymentHash,
        amt_paid_msat: MilliSatoshi,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        let now = Timestamp::now();
        match invoice.settle(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "commit_settle: already applied"
                );
                return Ok(());
            }
        }

        // settle() requires Held; mark_held always sets held_amount_msat.
        let held_amount_msat = invoice
            .held_amount_msat
            .expect("settle requires Held; mark_held sets held_amount_msat")
            .as_u64();
        let amount_sat = amt_paid_msat.whole_sat() as i64;

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
                        "wallet_id": wallet_id.to_string(),
                        "amt_paid_msat": amt_paid_msat.as_u64(),
                        "held_amount_msat": held_amount_msat,
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn commit_cancel(
        &self,
        mut invoice: Invoice,
        payment_hash: PaymentHash,
        reason: CancelReason,
        amt_paid_msat: MilliSatoshi,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        let now = Timestamp::now();
        let reason_for_outbox = reason.clone();
        match invoice.cancel(reason, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "commit_cancel: already applied"
                );
                return Ok(());
            }
        }

        // None = Open → Canceled (BOLT11 expiry, no pending booked).
        // Some(x) = Held → Canceled (Symphony releases x msat pending).
        let held_amount_msat = invoice.held_amount_msat.map(|m| m.as_u64());
        let amount_sat = amt_paid_msat.whole_sat() as i64;

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
                        "reason": reason_for_outbox.as_str(),
                        "wallet_id": wallet_id.to_string(),
                        "amt_paid_msat": amt_paid_msat.as_u64(),
                        "held_amount_msat": held_amount_msat,
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
