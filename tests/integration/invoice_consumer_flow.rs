//! Slice 1b closing test — consumer-flow E2E.
//!
//! Inserts a `LightningInvoiceSettled` outbox row directly via
//! `EventPublisher::publish_in_tx` (the production trigger, LND
//! `subscribe_invoices` `is_confirmed`, lands in Story 2.3 — this
//! test drives the publisher directly to demonstrate the pipeline
//! without depending on the not-yet-implemented adapter):
//! `outbox_events` row → pg_notify → `LightningPaymentGatewayService::SubscribeEvents`
//! gRPC stream → in-process Symphony stub → Cala-mock journal entry.
//! Asserts the proto fields on the wire and the entry the stub records
//! into the Cala mock (template = `LIGHTNING_INVOICE_SETTLED`).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serial_test::serial;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Server};

use blink_lightning_gateway::api::grpc::LightningPaymentGatewayService;
use blink_lightning_gateway::lightning_payment_gateway::{
    amount as proto_amount, lightning_payment_gateway_client::LightningPaymentGatewayClient,
    lightning_payment_gateway_server::LightningPaymentGatewayServer, GatewayEventType,
    SubscribeEventsRequest,
};
use blink_lightning_gateway::outbox::{EventPublisher, NewOutboxEvent};

use crate::common::TestDatabase;

/// One Cala journal-entry record that the stub captures for a streamed
/// event.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CalaMockEntry {
    template_name: &'static str,
    correlation_id: String,
    reference_id: String,
    amount_sat: u64,
}

#[derive(Clone, Default)]
struct CalaMock {
    entries: Arc<tokio::sync::Mutex<Vec<CalaMockEntry>>>,
}

impl CalaMock {
    async fn record(&self, entry: CalaMockEntry) {
        self.entries.lock().await.push(entry);
    }

    async fn snapshot(&self) -> Vec<CalaMockEntry> {
        self.entries.lock().await.clone()
    }
}

/// In-process Symphony stub. The cross-repo Symphony PR carries the real
/// `LIGHTNING_INVOICE_SETTLED` template; this test mirrors its
/// template-selection shape without depending on the not-yet-merged PR
/// (per Story 1.5 AC7).
async fn run_symphony_stub(
    mut stream: tonic::Streaming<blink_lightning_gateway::lightning_payment_gateway::PaymentEvent>,
    cala: CalaMock,
    expected: usize,
) {
    use futures::StreamExt;

    let mut consumed = 0usize;
    while consumed < expected {
        let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream message within 5s");
        let event = match msg {
            Some(Ok(e)) => e,
            Some(Err(s)) => panic!("stream error: {s:?}"),
            None => panic!("stream ended with {consumed} consumed (expected {expected})"),
        };

        let template_name = template_name_for(event.event_type);

        let amount_sat = match event.amount.as_ref().and_then(|a| a.value.as_ref()) {
            Some(proto_amount::Value::Sats(s)) => *s,
            other => panic!("unexpected amount variant: {other:?}"),
        };

        cala.record(CalaMockEntry {
            template_name,
            correlation_id: event.correlation_id,
            reference_id: event.reference_id,
            amount_sat,
        })
        .await;

        consumed += 1;
    }
}

/// Template selection from `event_type`. Slice 1 emits
/// `LightningInvoiceSettled` only, which maps to
/// `INCOMING_PAYMENT_CONFIRMED` → `LIGHTNING_INVOICE_SETTLED` template.
/// Other variants (`IncomingPaymentPending` → HOLD HTLC encumbrance,
/// `IncomingPaymentCanceled` → HOLD release) land alongside Story 2.3 /
/// 2.4. HTLC-bearing variants surface as a panic so the test fails
/// loudly if a future change accidentally widens the producer side.
fn template_name_for(event_type: i32) -> &'static str {
    match GatewayEventType::try_from(event_type) {
        Ok(GatewayEventType::IncomingPaymentConfirmed) => "LIGHTNING_INVOICE_SETTLED",
        Ok(GatewayEventType::IncomingPaymentPending) => "LIGHTNING_INVOICE_PENDING",
        Ok(GatewayEventType::IncomingPaymentCanceled) => "LIGHTNING_INVOICE_CANCELED",
        other => panic!("unsupported event_type for Slice 1: {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn outbox_row_streams_through_grpc_into_cala_mock() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let cancel_token = CancellationToken::new();
    let svc =
        LightningPaymentGatewayService::new(pool.clone(), db.url.clone(), cancel_token.clone())
            .expect("svc");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_token = cancel_token.clone();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(LightningPaymentGatewayServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async move {
                server_token.cancelled().await;
            })
            .await
            .expect("server")
    });

    let endpoint = format!("http://{addr}");
    let channel = Channel::from_shared(endpoint)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = LightningPaymentGatewayClient::new(channel);

    // Subscribe FIRST so LISTEN registers before the producer fires
    // pg_notify. The 5s timeout in the subscription_loop handles the
    // small startup delay.
    let response = client
        .subscribe_events(SubscribeEventsRequest { after_sequence: 0 })
        .await
        .expect("subscribe");
    let stream = response.into_inner();

    // Give the subscription_loop a moment to register LISTEN. Without
    // this, the producer can fire pg_notify before LISTEN is up; the
    // backfill catches the row anyway, but the live-stream path is what
    // we actually want to exercise.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Insert one outbox row via the publisher directly. Production
    // trigger (LND subscribe_invoices is_confirmed → App::handle_invoice_update)
    // lands in Story 2.3.
    let publisher = EventPublisher::new(&pool);
    let mut tx = pool.begin().await.unwrap();
    let payment_hash_hex = "aa".repeat(32);
    let _seq = publisher
        .publish_in_tx(
            &mut tx,
            NewOutboxEvent::for_lightning_invoice_settled(
                payment_hash_hex.clone(),
                payment_hash_hex.clone(),
                1000,
                Utc::now(),
                serde_json::json!({
                    "bolt_invoice": "lnbc10n1pj1234testbolt11invoice",
                    "payment_hash": payment_hash_hex,
                }),
            ),
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let cala = CalaMock::default();
    let stub_handle = tokio::spawn(run_symphony_stub(stream, cala.clone(), 1));

    tokio::time::timeout(Duration::from_secs(10), stub_handle)
        .await
        .expect("stub completes within 10s")
        .expect("stub task did not panic");

    let entries = cala.snapshot().await;
    assert_eq!(entries.len(), 1, "expected one Cala journal entry");
    let entry = &entries[0];
    assert_eq!(entry.template_name, "LIGHTNING_INVOICE_SETTLED");
    assert_eq!(entry.correlation_id, payment_hash_hex);
    assert_eq!(entry.reference_id, payment_hash_hex);
    assert_eq!(entry.amount_sat, 1000);

    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}
