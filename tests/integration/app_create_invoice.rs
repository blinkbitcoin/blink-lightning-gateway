//! `App::create_invoice` service-level coverage: success path + error
//! propagation. Booted Postgres lives under `tests/` per the workspace
//! convention.

use std::sync::Arc;

use async_trait::async_trait;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres as PgImage;

use blink_ln_gateway::app::{App, AppError, NewInvoiceRequest};
use blink_ln_gateway::lnd::{AddInvoiceParams, AddInvoiceResponse, LndApi, LndError};
use blink_ln_gateway::outbox::GatewayEventType;
use blink_ln_gateway::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, WalletId};

async fn boot_pg() -> (testcontainers::ContainerAsync<PgImage>, PgPool) {
    let container = PgImage::default().start().await.expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect pg");
    sqlx::migrate!().run(&pool).await.expect("migrate");
    (container, pool)
}

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
}

struct CannedErrLnd;

#[async_trait]
impl LndApi for CannedErrLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        Err(LndError::Stub)
    }
}

fn ok_request() -> NewInvoiceRequest {
    NewInvoiceRequest {
        wallet_id: WalletId::new(),
        amount_msat: MilliSatoshi::new(1_000_000),
        expiry_seconds: 3600,
        memo: Some("test".to_owned()),
    }
}

#[tokio::test]
#[serial]
async fn create_invoice_persists_invoice_event_and_outbox_rows() {
    let (_pg, pool) = boot_pg().await;
    let app = App::new(pool.clone(), Arc::new(CannedOkLnd));

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

    let outbox_row: (String, String, i64, String) = sqlx::query_as(
        r#"SELECT event_type, domain_event_type, sat_amount, currency FROM outbox_events"#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        outbox_row.0,
        GatewayEventType::IncomingPaymentPending.as_str()
    );
    assert_eq!(outbox_row.1, "lightning_invoice_created");
    assert_eq!(outbox_row.2, 1000); // 1_000_000 msat / 1000
    assert_eq!(outbox_row.3, "BTC");
}

#[tokio::test]
#[serial]
async fn create_invoice_propagates_invoice_error() {
    let (_pg, pool) = boot_pg().await;
    let app = App::new(pool, Arc::new(CannedOkLnd));
    let mut bad = ok_request();
    // Zero amount is the only condition that surfaces as `InvoiceError`
    // through `try_new`. Out-of-range expiry would be silently coerced
    // to the 4-hour default (matches blink-core), so it doesn't error.
    bad.amount_msat = MilliSatoshi::ZERO;
    let err = app.create_invoice(bad).await.unwrap_err();
    assert!(matches!(err, AppError::Invoice(_)));
}

#[tokio::test]
#[serial]
async fn create_invoice_propagates_lnd_error() {
    let (_pg, pool) = boot_pg().await;
    let app = App::new(pool, Arc::new(CannedErrLnd));
    let err = app.create_invoice(ok_request()).await.unwrap_err();
    assert!(matches!(err, AppError::Lnd(_)));
}
