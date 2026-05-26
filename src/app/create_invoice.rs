//! `lnInvoiceCreate` use-case: every gateway invoice is a
//! HODL invoice — the gateway generates the 32-byte preimage,
//! derives `payment_hash = SHA256(preimage)`, and calls LND's
//! `Invoices/AddHoldInvoice`.

use crate::app::{App, AppError, Mode, NewInvoiceRequest};
use crate::invoice::{Invoice, NewInvoice};
use crate::lnd::AddHoldInvoiceParams;
use crate::primitives::{Preimage, Timestamp};

impl App {
    pub async fn create_invoice(&self, request: NewInvoiceRequest) -> Result<Invoice, AppError> {
        let now = Timestamp::now();
        self.check_wallet_ownership(&request.wallet_id).await?;

        // Gateway-owned preimage + derived payment_hash
        let payment_preimage = Preimage::generate();
        let payment_hash = payment_preimage.payment_hash();

        let lnd_resp = self
            .lnd
            .add_hold_invoice(AddHoldInvoiceParams {
                payment_hash,
                amount_msat: Some(request.amount_msat),
                memo: request.memo,
                expiry_seconds: request.expiry_seconds,
            })
            .await?;

        let new_invoice = NewInvoice::try_new(
            payment_hash,
            payment_preimage,
            request.wallet_id,
            Some(request.amount_msat),
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
