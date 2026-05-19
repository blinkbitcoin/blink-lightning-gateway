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

        let mut tx = self.pool.begin().await?;
        let invoice = self
            .invoices
            .create_in_op(&mut tx, new_invoice)
            .await
            .map_err(crate::invoice::InvoiceError::from)?;
        tx.commit().await?;

        // Story 2.3: spawn the per-hash `subscribe_invoice` listener now
        // that the row + event are durable. Idempotent against double-
        // spawn (the recovery sweep may have already covered this hash
        // if create_invoice ran during shutdown). Spawn failure logs at
        // WARN but does NOT fail the mutation — the listener becomes a
        // recovery-sweep target on next restart.
        self.invoice_dispatcher
            .spawn_listener_for(invoice.payment_hash);

        Ok(invoice)
    }
}
