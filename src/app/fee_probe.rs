//! `lnInvoiceFeeProbe` use-case — straight-through, no DB writes,
//! no outbox events.

use crate::app::{decode, App, AppError, FeeProbeRequest};
use crate::lnd::FeeProbeParams;
use crate::primitives::MilliSatoshi;

impl App {
    /// `lnInvoiceFeeProbe` use-case.
    pub async fn fee_probe(&self, request: FeeProbeRequest) -> Result<MilliSatoshi, AppError> {
        self.check_wallet_ownership(&request.caller_auth, &request.wallet_id)
            .await?;
        let decoded = decode::decode_bolt11(&request.payment_request)?;
        let resp = self
            .lnd
            .fee_probe(FeeProbeParams {
                bolt_invoice: decoded.bolt_invoice,
            })
            .await?;
        Ok(resp.fee_msat)
    }
}
