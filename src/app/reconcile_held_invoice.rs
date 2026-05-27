//! Safety net for missed subscription events: every 5 min the
//! reconciliation sweep calls this for each Held invoice. LookupInvoice
//! → branch on LND state → commit_settle / commit_cancel / no-op.
//! Mirrors blink-core's `update-single-pending-invoice.ts` (ADR-0004).

use crate::app::helpers::is_invoice_not_found;
use crate::app::{App, AppError};
use crate::invoice::event::CancelReason;
use crate::invoice::{InvoiceError, InvoiceState};
use crate::lnd::LndInvoiceState;
use crate::primitives::PaymentHash;

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
                self.commit_settle(invoice, payment_hash, lnd_state.amt_paid_msat)
                    .await
            }
            LndInvoiceState::Canceled => {
                self.commit_cancel(
                    invoice,
                    payment_hash,
                    CancelReason::Expired,
                    lnd_state.amt_paid_msat,
                )
                .await
            }
            LndInvoiceState::Accepted => {
                self.lnd.settle_invoice(invoice.payment_preimage).await?;
                self.commit_settle(invoice, payment_hash, lnd_state.amt_paid_msat)
                    .await
            }
            LndInvoiceState::Open => {
                // DB Held → HtlcHeld event exists (mark_held was called
                // from a real wire Accepted). LND OPEN means divergence.
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    "reconcile_held_invoice: DB Held but LND OPEN; unexpected divergence"
                );
                Ok(())
            }
        }
    }
}
