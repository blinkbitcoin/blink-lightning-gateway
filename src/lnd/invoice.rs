//! `add_invoice` parameter + response types. Mirrors the shape of LND's
//! `AddInvoice` RPC request/response.

use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash};

#[derive(Clone, Debug)]
pub struct AddInvoiceParams {
    pub amount_msat: MilliSatoshi,
    pub memo: Option<String>,
    pub expiry_seconds: u32,
}

#[derive(Clone, Debug)]
pub struct AddInvoiceResponse {
    /// LND-generated 32-byte payment-hash. Source of truth — never
    /// synthesize on the gateway side.
    pub payment_hash: PaymentHash,
    /// BOLT11 invoice string returned by LND.
    pub bolt_invoice: BoltInvoice,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lnd::client::{LndApi, MockLndApi};

    fn canned_response() -> AddInvoiceResponse {
        AddInvoiceResponse {
            payment_hash: PaymentHash::from([0xab; 32]),
            bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
        }
    }

    #[tokio::test]
    async fn mock_lnd_returns_canned_response() {
        let mut mock = MockLndApi::new();
        mock.expect_add_invoice()
            .times(1)
            .returning(|_| Box::pin(async { Ok(canned_response()) }));

        let resp = mock
            .add_invoice(AddInvoiceParams {
                amount_msat: MilliSatoshi::new(1_000_000),
                memo: Some("test".to_owned()),
                expiry_seconds: 3600,
            })
            .await
            .unwrap();

        assert_eq!(resp.payment_hash, PaymentHash::from([0xab; 32]));
        assert!(resp.bolt_invoice.as_str().starts_with("lnbc"));
    }
}
