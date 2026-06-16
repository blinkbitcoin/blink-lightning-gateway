//! Synthetic E2E for the `lnInvoicePaymentStatus*` subscription (Slice 6,
//! ADR-0008). No WebSocket transport — drives `schema.execute_stream`
//! directly (Fact 4). Invoice transitions are published as outbox rows (the
//! method AC14 calls out); the fanout's single `LISTEN` broadcasts them to
//! the per-invoice subscriber stream. Each test owns its own testcontainers
//! Postgres (`TestDatabase`); no `#[serial]`.

use std::sync::Arc;
use std::time::Duration;

use async_graphql::Request;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use blink_lightning_gateway::api::graphql::{
    build_schema_with_fanout, GatewaySchema, ResumeSequence,
};
use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher, NewInvoiceRequest};
use blink_lightning_gateway::invoice::entity::NewInvoice;
use blink_lightning_gateway::invoice::Invoices;
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, LndInvoiceState, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::{
    EventPublisher, ListenConnection, NewOutboxEvent, OutboxFanout,
};
use blink_lightning_gateway::primitives::{
    BoltInvoice, InvoiceId, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};

use crate::common::{CannedWalletOwnership, TestDatabase};

/// LND stub: `add_hold_invoice` echoes a canned BOLT11 so `create_invoice`
/// completes; everything else returns enough to settle the HODL path.
struct CannedLnd;

#[async_trait]
impl LndApi for CannedLnd {
    async fn add_hold_invoice(
        &self,
        _params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        Ok(AddHoldInvoiceResponse {
            bolt_invoice: BoltInvoice::new("lnbc10n1pj..."),
        })
    }
    async fn settle_invoice(&self, _preimage: Preimage) -> Result<(), LndError> {
        Ok(())
    }
    async fn cancel_invoice(&self, _payment_hash: PaymentHash) -> Result<(), LndError> {
        Ok(())
    }
    async fn lookup_invoice(&self, payment_hash: PaymentHash) -> Result<InvoiceUpdate, LndError> {
        Ok(InvoiceUpdate {
            payment_hash,
            state: LndInvoiceState::Accepted,
            amt_paid_msat: MilliSatoshi::new(1_000_000),
            payment_preimage: None,
        })
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

fn build_app(pool: sqlx::PgPool) -> App {
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    App::new(
        pool.clone(),
        Arc::new(CannedLnd),
        EventPublisher::new(&pool),
        symphony,
        CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    )
}

/// `app` + a fanout-backed schema. Sleeps briefly so the fanout's single
/// `LISTEN gateway_events` registers before any outbox row is published
/// (the same 200ms the gRPC subscriber test uses).
async fn setup(db: &TestDatabase) -> (App, GatewaySchema) {
    let app = build_app(db.pool.clone());
    let fanout = OutboxFanout::start(
        EventPublisher::new(&db.pool),
        ListenConnection::new(db.url.clone()),
        CancellationToken::new(),
    )
    .expect("fanout starts");
    let schema = build_schema_with_fanout(app.clone(), fanout);
    tokio::time::sleep(Duration::from_millis(200)).await;
    (app, schema)
}

fn invoice_request(wallet: WalletId) -> NewInvoiceRequest {
    NewInvoiceRequest {
        caller_auth: Default::default(),
        wallet_id: wallet,
        amount_msat: MilliSatoshi::new(1_000_000),
        expiry_seconds: 3600,
        memo: Some("sub-test".to_owned()),
        external_id: None,
    }
}

fn by_hash_query(hash: &PaymentHash) -> String {
    format!(
        r#"subscription {{ lnInvoicePaymentStatusByHash(input: {{ paymentHash: "{}" }}) {{ status paymentHash paymentPreimage errors {{ message }} }} }}"#,
        hash.to_hex()
    )
}

/// Pull one subscription payload (the `lnInvoicePaymentStatusByHash` field)
/// out of a streamed `Response`, asserting no top-level GraphQL errors.
fn next_payload(resp: async_graphql::Response) -> serde_json::Value {
    assert!(
        resp.errors.is_empty(),
        "unexpected GraphQL errors: {:?}",
        resp.errors
    );
    let json = resp.data.into_json().expect("response data → json");
    json["lnInvoicePaymentStatusByHash"].clone()
}

/// Publish one outbox row in its own transaction; the commit fires
/// `pg_notify('gateway_events', sequence)`. Returns the assigned sequence.
async fn publish(pool: &sqlx::PgPool, event: NewOutboxEvent) -> i64 {
    let publisher = EventPublisher::new(pool);
    let mut tx = pool.begin().await.expect("begin tx");
    let seq = publisher
        .publish_in_tx(&mut tx, event)
        .await
        .expect("publish outbox row");
    tx.commit().await.expect("commit");
    seq
}

fn settled_event(hash: &PaymentHash) -> NewOutboxEvent {
    NewOutboxEvent::for_lightning_invoice_settled(
        hash.to_hex(),
        hash.to_hex(),
        1000,
        Utc::now(),
        serde_json::json!({}),
    )
}

async fn next_within(
    stream: &mut (impl StreamExt<Item = async_graphql::Response> + Unpin),
) -> async_graphql::Response {
    tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("payload within 10s")
        .expect("stream yielded a payload")
}

// AC14(a)+(b): Open → first payload PENDING; a published Settled row →
// PAID (with preimage), in order; then the stream completes (terminal).
#[tokio::test]
async fn open_then_settled_streams_pending_then_paid() {
    let db = TestDatabase::new().await.expect("test db");
    let (app, schema) = setup(&db).await;

    let wallet = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create");
    let hash = invoice.payment_hash;

    let mut stream = schema.execute_stream(Request::new(by_hash_query(&hash)));

    let pending = next_payload(next_within(&mut stream).await);
    assert_eq!(pending["status"], serde_json::json!("PENDING"));
    assert_eq!(pending["paymentHash"], serde_json::json!(hash.to_hex()));

    // Polling PENDING proves the live receiver is attached; publish now.
    publish(&db.pool, settled_event(&hash)).await;

    let paid = next_payload(next_within(&mut stream).await);
    assert_eq!(paid["status"], serde_json::json!("PAID"));
    assert!(
        paid["paymentPreimage"].is_string(),
        "PAID payload carries a preimage, got {paid}"
    );

    // Terminal: the stream completes after PAID.
    let end = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("stream completes within 10s");
    assert!(
        end.is_none(),
        "stream should complete after PAID, got {end:?}"
    );
}

// AC14(c): subscribing to an already-Settled invoice yields exactly one
// PAID (initial-status path) and completes — no hang waiting for events.
#[tokio::test]
async fn already_settled_emits_single_paid() {
    let db = TestDatabase::new().await.expect("test db");
    let (app, schema) = setup(&db).await;

    let wallet = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create");
    let hash = invoice.payment_hash;

    // Accepted auto-settles under the HODL substrate (Story 2.4):
    // Open → Held → Settled.
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: hash,
        state: LndInvoiceState::Accepted,
        amt_paid_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: None,
    })
    .await
    .expect("settle");

    let mut stream = schema.execute_stream(Request::new(by_hash_query(&hash)));

    let paid = next_payload(next_within(&mut stream).await);
    assert_eq!(paid["status"], serde_json::json!("PAID"));

    let end = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("stream completes within 10s");
    assert!(end.is_none(), "single PAID then complete, got {end:?}");
}

// AC14(d) — cancel path: Open → PENDING, a LightningInvoiceCanceled row →
// EXPIRED, then complete.
#[tokio::test]
async fn canceled_row_streams_expired() {
    let db = TestDatabase::new().await.expect("test db");
    let (app, schema) = setup(&db).await;

    let wallet = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create");
    let hash = invoice.payment_hash;

    let mut stream = schema.execute_stream(Request::new(by_hash_query(&hash)));

    let pending = next_payload(next_within(&mut stream).await);
    assert_eq!(pending["status"], serde_json::json!("PENDING"));

    publish(
        &db.pool,
        NewOutboxEvent::for_lightning_invoice_canceled(
            hash.to_hex(),
            hash.to_hex(),
            0,
            Utc::now(),
            serde_json::json!({}),
        ),
    )
    .await;

    let expired = next_payload(next_within(&mut stream).await);
    assert_eq!(expired["status"], serde_json::json!("EXPIRED"));

    let end = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("stream completes within 10s");
    assert!(
        end.is_none(),
        "stream should complete after EXPIRED, got {end:?}"
    );
}

// AC14(d) — expiry path: an Open invoice already past `expiry_at` yields a
// single EXPIRED on subscribe (the on-subscribe `now >= expiry_at`
// derivation, ADR-0008), then completes.
#[tokio::test]
async fn open_past_expiry_emits_expired() {
    let db = TestDatabase::new().await.expect("test db");
    let (_app, schema) = setup(&db).await;

    // `NewInvoice::try_new` always sets a future expiry, so seed the row
    // directly with a past `expiry_at`.
    let preimage = Preimage::from([0x55; 32]);
    let hash = preimage.payment_hash();
    let new = NewInvoice {
        id: InvoiceId::new(),
        payment_hash: hash,
        payment_preimage: preimage,
        wallet_id: WalletId::from(Uuid::now_v7()),
        amount_msat: Some(MilliSatoshi::new(1_000_000)),
        expiry_at: Timestamp::from(Utc::now() - chrono::Duration::seconds(3600)),
        bolt_invoice: BoltInvoice::new("lnbc-expired"),
        external_id: "ext-expired".to_owned(),
        created_at: Timestamp::now(),
    };
    Invoices::new(&db.pool)
        .create(new)
        .await
        .expect("seed invoice");

    let mut stream = schema.execute_stream(Request::new(by_hash_query(&hash)));

    let expired = next_payload(next_within(&mut stream).await);
    assert_eq!(expired["status"], serde_json::json!("EXPIRED"));

    let end = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("stream completes within 10s");
    assert!(end.is_none(), "single EXPIRED then complete, got {end:?}");
}

// AC15 (Fact 5): an intraledger-settled invoice's PAID signal arrives as
// LightningIntraledgerTransferCompleted, NOT LightningInvoiceSettled. Guards
// the regression where the mapping handles only LightningInvoiceSettled and
// silently never reports an intraledger-settled invoice as paid.
#[tokio::test]
async fn intraledger_transfer_completed_streams_paid() {
    let db = TestDatabase::new().await.expect("test db");
    let (app, schema) = setup(&db).await;

    let wallet = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create");
    let hash = invoice.payment_hash;

    let mut stream = schema.execute_stream(Request::new(by_hash_query(&hash)));

    let pending = next_payload(next_within(&mut stream).await);
    assert_eq!(pending["status"], serde_json::json!("PENDING"));

    publish(
        &db.pool,
        NewOutboxEvent::for_lightning_intraledger_transfer_completed(
            hash.to_hex(),
            hash.to_hex(),
            1000,
            Utc::now(),
            serde_json::json!({ "intraledger": true }),
        ),
    )
    .await;

    let paid = next_payload(next_within(&mut stream).await);
    assert_eq!(paid["status"], serde_json::json!("PAID"));
}

// AC14(e) / AC7: reconnection. With Held(seq1) + Settled(seq2) already
// landed, a resume from sequence 0 backfills the full [PENDING, PAID]; a
// resume from seq1 backfills only [PAID] — the already-acked PENDING is not
// re-delivered (no duplicate) and the PAID is not missed (no gap).
#[tokio::test]
async fn resume_from_sequence_backfills_without_duplicate_or_gap() {
    let db = TestDatabase::new().await.expect("test db");
    let (app, schema) = setup(&db).await;

    let wallet = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create");
    let hash = invoice.payment_hash;

    let held_seq = publish(
        &db.pool,
        NewOutboxEvent::for_lightning_htlc_held(
            hash.to_hex(),
            hash.to_hex(),
            1000,
            Utc::now(),
            serde_json::json!({}),
        ),
    )
    .await;
    publish(&db.pool, settled_event(&hash)).await;

    // Resume from 0 → full history backfilled, in order, then complete.
    let mut from_zero =
        schema.execute_stream(Request::new(by_hash_query(&hash)).data(ResumeSequence(0)));
    assert_eq!(
        next_payload(next_within(&mut from_zero).await)["status"],
        serde_json::json!("PENDING")
    );
    assert_eq!(
        next_payload(next_within(&mut from_zero).await)["status"],
        serde_json::json!("PAID")
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), from_zero.next())
            .await
            .expect("completes")
            .is_none(),
        "resume-from-0 completes after PAID"
    );

    // Resume past the Held → only PAID is delivered (no duplicate PENDING).
    let mut from_held =
        schema.execute_stream(Request::new(by_hash_query(&hash)).data(ResumeSequence(held_seq)));
    let first = next_payload(next_within(&mut from_held).await);
    assert_eq!(
        first["status"],
        serde_json::json!("PAID"),
        "resume past Held skips the already-acked PENDING"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), from_held.next())
            .await
            .expect("completes")
            .is_none(),
        "resume-from-held completes after PAID"
    );
}
