//! Slice 1a closing test — producer-flow E2E.
//!
//! GraphQL `lnInvoiceCreate` mutation → `App::create_invoice` → stub LND →
//! atomic DB tx wrapping `Invoice` persist + outbox publish → pg_notify on
//! `gateway_events`. Asserts every DB table state + the notification
//! payload.
//!
//! Story 1.5 adds `tests/invoice_consumer_flow.rs` for the gRPC →
//! Symphony-stub → Cala-mock half. Both halves together close the C2-Discovery
//! hypothesis on a mock stack.

use std::sync::Arc;
use std::time::Duration;

use async_graphql::Value;
use async_trait::async_trait;
use serde_json::json;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres as PgImage;

use blink_ln_gateway::api::graphql::{build_schema, GatewaySchema};
use blink_ln_gateway::app::App;
use blink_ln_gateway::lnd::{AddInvoiceParams, AddInvoiceResponse, LndApi, LndError};
use blink_ln_gateway::primitives::{BoltInvoice, PaymentHash};

/// Hand-written stub LND. Integration tests are a separate compilation
/// unit, so they can't see the lib's `MockLndApi` (gated on lib `cfg(test)`).
/// The trait has one method, so this is trivial — see Story 1.4 Dev Notes.
struct CannedLnd {
    canned_payment_hash: [u8; 32],
    canned_bolt_invoice: &'static str,
}

#[async_trait]
impl LndApi for CannedLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        Ok(AddInvoiceResponse {
            payment_hash: PaymentHash::from(self.canned_payment_hash),
            bolt_invoice: BoltInvoice::new(self.canned_bolt_invoice),
        })
    }
}

async fn boot_pg() -> (
    testcontainers::ContainerAsync<PgImage>,
    PgPool,
    String, /* postgres URL */
) {
    // Inline the testcontainers retry pattern (Story 1.3 anti-pattern note:
    // do not introduce `tests/common/mod.rs` until ≥2 integration tests
    // exist; Story 1.5 adds the second test, which is when the shared
    // fixture becomes warranted).
    let mut last_err: Option<String> = None;
    for attempt in 1..=3 {
        match PgImage::default().start().await {
            Ok(container) => {
                let port = container.get_host_port_ipv4(5432).await.expect("port");
                let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

                let mut pool_err: Option<String> = None;
                for pool_attempt in 1..=5 {
                    match PgPoolOptions::new().max_connections(4).connect(&url).await {
                        Ok(pool) => {
                            sqlx::migrate!().run(&pool).await.expect("migrate");
                            return (container, pool, url);
                        }
                        Err(e) => {
                            pool_err = Some(format!("attempt {pool_attempt}: {e}"));
                            tokio::time::sleep(Duration::from_millis(500 * pool_attempt)).await;
                        }
                    }
                }
                panic!("pool connect failed: {pool_err:?}");
            }
            Err(e) => {
                last_err = Some(format!("attempt {attempt}: {e}"));
                tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
            }
        }
    }
    panic!("container start failed: {last_err:?}");
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
#[serial]
async fn ln_invoice_create_persists_three_tables_and_fires_pg_notify() {
    let (_pg, pool, url) = boot_pg().await;

    // Spin up an inline LISTEN connection BEFORE the mutation so we don't
    // race the trigger.
    let (listen_client, mut listen_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("listen connect");
    let (notif_tx, mut notif_rx) =
        tokio::sync::mpsc::unbounded_channel::<tokio_postgres::Notification>();
    let driver = tokio::spawn(async move {
        use std::future::poll_fn;
        use tokio_postgres::AsyncMessage;
        loop {
            let msg = poll_fn(|cx| listen_conn.poll_message(cx)).await;
            match msg {
                Some(Ok(AsyncMessage::Notification(n))) => {
                    let _ = notif_tx.send(n);
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    });
    listen_client
        .batch_execute("LISTEN gateway_events;")
        .await
        .expect("LISTEN");

    let lnd: Arc<dyn LndApi> = Arc::new(CannedLnd {
        canned_payment_hash: [0xaa; 32],
        canned_bolt_invoice: "lnbc10n1pj1234testbolt11invoice",
    });
    let app = App::new(pool.clone(), lnd);
    let schema = build_test_schema(app);

    // Use a stable WalletId for assertion stability.
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
    assert_eq!(
        invoice.get("paymentHash").unwrap().as_str().unwrap(),
        "aa".repeat(32)
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

    let (outbox_count, outbox_event_type, outbox_currency, outbox_sat): (i64, String, String, i64) =
        sqlx::query_as(
            r#"
            SELECT
                COUNT(*),
                COALESCE(MAX(event_type), ''),
                COALESCE(MAX(currency), ''),
                COALESCE(MAX(sat_amount), 0)
            FROM outbox_events
            "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outbox_count, 1);
    assert_eq!(outbox_event_type, "INCOMING_PAYMENT_PENDING");
    assert_eq!(outbox_currency, "BTC");
    assert_eq!(outbox_sat, 1000);

    // pg_notify fired with the row's sequence as payload.
    let notif = tokio::time::timeout(Duration::from_secs(5), notif_rx.recv())
        .await
        .expect("pg_notify within 5s")
        .expect("notification");
    assert_eq!(notif.channel(), "gateway_events");
    let payload_seq: i64 = notif.payload().parse().expect("numeric payload");
    let (db_seq,): (i64,) = sqlx::query_as("SELECT sequence FROM outbox_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(payload_seq, db_seq);

    drop(listen_client);
    let _ = tokio::time::timeout(Duration::from_millis(100), driver).await;
}
