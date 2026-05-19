//! `lnInvoiceCreate` use-case.

use crate::app::{App, AppError, Mode, NewInvoiceRequest};
use crate::invoice::{Invoice, NewInvoice};
use crate::lnd::AddInvoiceParams;
use crate::primitives::Timestamp;

impl App {
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

        let invoice = self
            .invoices
            .create(new_invoice)
            .await
            .map_err(crate::invoice::InvoiceError::from)?;

        // Spawn the per-hash `subscribe_invoice` listener
        self.invoice_dispatcher
            .spawn_listener_for(invoice.payment_hash);

        Ok(invoice)
    }
}
