//! `App::create_invoice` service-level coverage: success path + error
//! propagation. Booted Postgres lives under `tests/` per the workspace
//! convention.

use std::sync::Arc;

use async_trait::async_trait;

use blink_lightning_gateway::app::{App, AppError, InvoiceUpdateDispatcher, NewInvoiceRequest};
use blink_lightning_gateway::lnd::{
    AddInvoiceParams, AddInvoiceResponse, FeeProbeParams, FeeProbeResponse, LndApi, LndError,
    SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, WalletId};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};
use uuid::Uuid;

use crate::common::TestDatabase;

/// Integration tests can't see the lib's `MockLndApi` (gated on lib
/// `cfg(test)`). The trait has one method, so a hand-written stub is
/// trivial.
struct CannedOkLnd;

#[async_trait]
impl LndApi for CannedOkLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        Ok(AddInvoiceResponse {
            payment_hash: PaymentHash::from([0xab; 32]),
            bolt_invoice: BoltInvoice::new("lnbc10n1pj..."),
        })
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

struct CannedErrLnd;

#[async_trait]
impl LndApi for CannedErrLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
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

fn ok_request() -> NewInvoiceRequest {
    NewInvoiceRequest {
        wallet_id: WalletId::from(Uuid::now_v7()),
        amount_msat: MilliSatoshi::new(1_000_000),
        expiry_seconds: 3600,
        memo: Some("test".to_owned()),
    }
}

#[tokio::test]
async fn create_invoice_persists_invoice_and_event_rows() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        Arc::new(CannedOkLnd),
        outbox,
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );

    let invoice = app.create_invoice(ok_request()).await.expect("create");
    assert_eq!(invoice.payment_hash, PaymentHash::from([0xab; 32]));
    assert_eq!(invoice.amount_msat, MilliSatoshi::new(1_000_000));

    let invoices_count: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM invoices"#)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(invoices_count.0, 1);

    let event_count: (i64,) =
        sqlx::query_as(r#"SELECT COUNT(*) FROM invoice_events WHERE event->>'type' = 'created'"#)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(event_count.0, 1);
}

#[tokio::test]
async fn create_invoice_propagates_invoice_error() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        Arc::new(CannedOkLnd),
        outbox,
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );
    let mut bad = ok_request();
    // Zero amount is the only condition that surfaces as `InvoiceError`
    // through `try_new`. Out-of-range expiry would be silently coerced
    // to the 4-hour default (matches blink-core), so it doesn't error.
    bad.amount_msat = MilliSatoshi::ZERO;
    let err = app.create_invoice(bad).await.unwrap_err();
    assert!(matches!(err, AppError::Invoice(_)));
}

#[tokio::test]
async fn create_invoice_propagates_lnd_error() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        Arc::new(CannedErrLnd),
        outbox,
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );
    let err = app.create_invoice(ok_request()).await.unwrap_err();
    assert!(matches!(err, AppError::Lnd(_)));
}
