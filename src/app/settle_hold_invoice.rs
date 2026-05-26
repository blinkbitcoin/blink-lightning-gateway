//! `App::settle_hold_invoice` — gateway-side HOLD-invoice settle.
//!
//! Story 2.4: drives LND's `Invoices/SettleInvoice` with the
//! gateway-owned preimage hydrated off the `Invoice` aggregate, then
//! transitions `Held → Settled` and books the `LightningInvoiceSettled`
//! outbox row in one DB transaction.

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::helpers::is_invoice_not_found;
use crate::app::{App, AppError};
use crate::invoice::InvoiceError;
use crate::outbox::NewOutboxEvent;
use crate::primitives::{PaymentHash, Timestamp};

impl App {
    /// Release the preimage on a held HODL invoice. Called automatically
    /// by `handle_invoice_update`'s `Accepted` arm after the business
    /// gate (stubbed in Story 2.4); Story 3.1 introduces gated callers.
    ///
    /// Idempotent in two layers: a `NotFound` lookup is a quiet skip
    /// (absorbs create/listener races); `Idempotent::AlreadyApplied`
    /// from `Invoice::settle` short-circuits before LND is called a
    /// second time (matters when the subscription path observed the
    /// settle first and ran the projection update).
    pub async fn settle_hold_invoice(&self, payment_hash: PaymentHash) -> Result<(), AppError> {
        let invoice = match self.invoices.find_by_payment_hash(&payment_hash).await {
            Ok(i) => i,
            Err(e) if is_invoice_not_found(&e) => {
                ::tracing::debug!(
                    payment_hash = %payment_hash.to_hex(),
                    "settle_hold_invoice: invoice not found; ignoring"
                );
                return Ok(());
            }
            Err(e) => return Err(InvoiceError::from(e).into()),
        };

        let preimage = invoice.payment_preimage;
        let wallet_id = invoice.wallet_id;
        let now = Timestamp::now();

        // Call LND first: only after `SettleInvoice` succeeds is it safe
        // to flip the projection to `Settled`. A wire failure here leaves
        // the invoice in `Held`, where the subscription path will pick up
        // the settle (if LND ultimately accepted it) or the
        // `invoice_expiry_sweep` will eventually cancel it.
        self.lnd.settle_invoice(preimage).await?;

        let mut invoice = invoice;
        match invoice.settle(preimage, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "settle_hold_invoice: settle already applied; skipping outbox publish"
                );
                return Ok(());
            }
        }

        // AC12: clearing event echoes the persisted parked amount so the
        // outbox pending layer reconciles with the credit booked at Held.
        let amount_sat = invoice
            .held_amount_msat
            .map(|m| m.whole_sat() as i64)
            .unwrap_or_else(|| {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    "settle_hold_invoice: held_amount_msat absent; emitting amount_sat=0"
                );
                0
            });

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
}
