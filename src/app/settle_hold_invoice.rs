//! Gateway-side HOLD-invoice settle. Mirrors blink-core's
//! `update-single-pending-invoice.ts`: LookupInvoice → branch on LND
//! truth → SettleInvoice (only when still Accepted) → commit at LND's
//! `amt_paid_msat`. Sole writer of the Settled outbox row in the
//! happy path; the subscription `Settled` echo is observation-only.

use crate::app::helpers::is_invoice_not_found;
use crate::app::{App, AppError};
use crate::invoice::event::CancelReason;
use crate::invoice::{InvoiceError, InvoiceState};
use crate::lnd::LndInvoiceState;
use crate::primitives::PaymentHash;

impl App {
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

        if !matches!(invoice.state, InvoiceState::Held) {
            ::tracing::info!(
                payment_hash = %payment_hash.to_hex(),
                current_state = %invoice.state,
                "settle_hold_invoice: not in Held state; skipping"
            );
            return Ok(());
        }

        let lnd_state = self.lnd.lookup_invoice(payment_hash).await?;

        match lnd_state.state {
            LndInvoiceState::Accepted => {
                self.lnd.settle_invoice(invoice.payment_preimage).await?;
                self.commit_settle(invoice, payment_hash, lnd_state.amt_paid_msat)
                    .await
            }
            LndInvoiceState::Settled => {
                // LND already settled (concurrent caller, operator action);
                // skip the RPC, just commit the projection + outbox.
                self.commit_settle(invoice, payment_hash, lnd_state.amt_paid_msat)
                    .await
            }
            LndInvoiceState::Canceled => {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    "settle_hold_invoice: DB Held but LND CANCELED; committing cancel"
                );
                self.commit_cancel(
                    invoice,
                    payment_hash,
                    CancelReason::Expired,
                    lnd_state.amt_paid_msat,
                )
                .await
            }
            LndInvoiceState::Open => {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    "settle_hold_invoice: DB Held but LND OPEN; unexpected divergence"
                );
                Ok(())
            }
        }
    }
}
