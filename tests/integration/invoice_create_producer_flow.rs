//! Slice 1a closing test — producer-flow E2E.
//!
//! GraphQL `lnInvoiceCreate` mutation → `App::create_invoice` → stub LND →
//! atomic DB tx wrapping `Invoice` projection-row + event-rows. Asserts
//! the response shape and the two DB tables (`invoices`, `invoice_events`).
//!
//! No outbox-event assertion: invoice creation does not fire a
//! standardized event on Symphony's stream — Story 2.3 wires the LND
//! `subscribe_invoices` adapter and emits the right events keyed off
//! real wire callbacks.

use std::sync::Arc;

use async_graphql::Value;
use async_trait::async_trait;
use serde_json::json;

use blink_lightning_gateway::api::graphql::{build_schema, GatewaySchema};
use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher};
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::primitives::{BoltInvoice, PaymentHash, Preimage};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};

use crate::common::TestDatabase;

/// Hand-written stub LND. Integration tests are a separate compilation
/// unit, so they can't see the lib's `MockLndApi` (gated on lib `cfg(test)`).
struct CannedLnd {
    canned_bolt_invoice: &'static str,
}

#[async_trait]
impl LndApi for CannedLnd {
    async fn add_hold_invoice(
        &self,
        _params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        Ok(AddHoldInvoiceResponse {
            bolt_invoice: BoltInvoice::new(self.canned_bolt_invoice),
        })
    }

    async fn settle_invoice(&self, _preimage: Preimage) -> Result<(), LndError> {
        Ok(())
    }

    async fn cancel_invoice(&self, _payment_hash: PaymentHash) -> Result<(), LndError> {
        Ok(())
    }

    async fn lookup_invoice(&self, _payment_hash: PaymentHash) -> Result<InvoiceUpdate, LndError> {
        Err(LndError::Stub)
    }

    async fn lookup_payment(
        &self,
        _payment_hash: PaymentHash,
    ) -> Result<SendPaymentResponse, LndError> {
        Err(LndError::Stub)
    }

    async fn send_payment(
        &self,
        _params: SendPaymentParams,
    ) -> Result<SendPaymentResponse, LndError> {
        Err(LndError::Stub)
    }

    async fn fee_probe(&self, _params: FeeProbeParams) -> Result<FeeProbeResponse, LndError> {
        Err(LndError::Stub)
    }
}

fn build_test_schema(app: App) -> GatewaySchema {
    build_schema(app)
}

const MUTATION: &str = r#"
    mutation Create($amount: SatAmount!, $walletId: WalletId!) {
        lnInvoiceCreate(input: { amount: $amount, walletId: $walletId }) {
            invoice {
                paymentHash
                paymentRequest
                satoshis
            }
            errors {
                message
            }
        }
    }
"#;

#[tokio::test]
async fn ln_invoice_create_persists_invoice_and_event_rows() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd: Arc<dyn LndApi> = Arc::new(CannedLnd {
        canned_bolt_invoice: "lnbc10n1pj1234testbolt11invoice",
    });
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let app = App::new(
        pool.clone(),
        lnd,
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );
    let schema = build_test_schema(app);

    let wallet_id = "11111111-1111-1111-1111-111111111111";

    let request =
        async_graphql::Request::new(MUTATION).variables(async_graphql::Variables::from_value(
            Value::from_json(json!({
                "amount": 1000,
                "walletId": wallet_id,
            }))
            .unwrap(),
        ));
    let response = schema.execute(request).await;

    assert!(
        response.errors.is_empty(),
        "GraphQL execution errors: {:?}",
        response.errors
    );

    let data = response.data.into_json().unwrap();
    let payload = data.get("lnInvoiceCreate").expect("payload");
    let resolver_errors = payload.get("errors").unwrap().as_array().unwrap();
    assert!(
        resolver_errors.is_empty(),
        "resolver errors: {resolver_errors:?}"
    );
    let invoice = payload.get("invoice").unwrap();
    // Story 2.4: hash is gateway-derived from a random preimage, no longer
    // canned by LND. Verify shape (64-char hex) rather than a fixed value.
    let payment_hash = invoice.get("paymentHash").unwrap().as_str().unwrap();
    assert_eq!(payment_hash.len(), 64, "payment_hash hex length");
    assert!(
        payment_hash.chars().all(|c| c.is_ascii_hexdigit()),
        "payment_hash hex form"
    );
    let payment_request = invoice.get("paymentRequest").unwrap().as_str().unwrap();
    assert!(payment_request.starts_with("lnbc"));
    assert_eq!(invoice.get("satoshis").unwrap().as_u64().unwrap(), 1000);

    // DB state assertions.
    let (invoices_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM invoices")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(invoices_count, 1);

    let (event_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM invoice_events WHERE event->>'type' = 'created'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(event_count, 1);
}
