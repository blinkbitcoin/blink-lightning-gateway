//! Integration coverage for `App::handle_invoice_update`: drives synthetic
//! `InvoiceUpdate` values, asserts entity transitions + outbox rows +
//! the gRPC subscriber pipeline. Wire-format mapping coverage lives
//! in `src/lnd/invoice.rs` unit tests.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Server};

use blink_lightning_gateway::api::grpc::LightningPaymentGatewayService;
use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher, NewInvoiceRequest};
use blink_lightning_gateway::invoice::entity::{InvoiceState, NewInvoice};
use blink_lightning_gateway::invoice::{InvoiceError, Invoices};
use blink_lightning_gateway::job::invoice_subscription_recovery_sweep::run_invoice_subscription_recovery_sweep;
use blink_lightning_gateway::lightning_payment_gateway::{
    amount as proto_amount, lightning_payment_gateway_client::LightningPaymentGatewayClient,
    lightning_payment_gateway_server::LightningPaymentGatewayServer, GatewayEventType,
    SubscribeEventsRequest,
};
use blink_lightning_gateway::lnd::{
    AddInvoiceParams, AddInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate, LndApi,
    LndError, LndInvoiceState, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};
use uuid::Uuid;

use crate::common::TestDatabase;

const PH_A: [u8; 32] = [0x0a; 32];
const PH_B: [u8; 32] = [0x0b; 32];
const PH_C: [u8; 32] = [0x0c; 32];
const PH_D: [u8; 32] = [0x0d; 32];

/// LND stub. `add_invoice` pops the next canned hash so each
/// `create_invoice` call gets a distinct one.
struct CannedLnd {
    canned_hashes: AsyncMutex<Vec<[u8; 32]>>,
}

impl CannedLnd {
    fn new(hashes: Vec<[u8; 32]>) -> Self {
        Self {
            canned_hashes: AsyncMutex::new(hashes),
        }
    }
}

#[async_trait]
impl LndApi for CannedLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        let mut guard = self.canned_hashes.lock().await;
        let bytes = guard
            .pop()
            .expect("CannedLnd: ran out of canned payment_hashes");
        Ok(AddInvoiceResponse {
            payment_hash: PaymentHash::from(bytes),
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct CalaMockEntry {
    template_name: &'static str,
    payment_hash: String,
    amount_sat: u64,
}

#[derive(Clone, Default)]
struct CalaMock {
    entries: Arc<AsyncMutex<Vec<CalaMockEntry>>>,
}

impl CalaMock {
    async fn record(&self, entry: CalaMockEntry) {
        self.entries.lock().await.push(entry);
    }
    async fn snapshot(&self) -> Vec<CalaMockEntry> {
        self.entries.lock().await.clone()
    }
}

fn template_name_for(event_type: i32) -> &'static str {
    match GatewayEventType::try_from(event_type) {
        Ok(GatewayEventType::IncomingPaymentConfirmed) => "LIGHTNING_INVOICE_SETTLED",
        Ok(GatewayEventType::IncomingPaymentPending) => "LIGHTNING_INVOICE_PENDING",
        Ok(GatewayEventType::IncomingPaymentCanceled) => "LIGHTNING_INVOICE_CANCELED",
        other => panic!("unsupported event_type: {other:?}"),
    }
}

async fn run_symphony_stub(
    mut stream: tonic::Streaming<blink_lightning_gateway::lightning_payment_gateway::PaymentEvent>,
    cala: CalaMock,
    expected: usize,
) {
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
            payment_hash: event.reference_id,
            amount_sat,
        })
        .await;
        consumed += 1;
    }
}

fn invoice_request(wallet: WalletId) -> NewInvoiceRequest {
    NewInvoiceRequest {
        wallet_id: wallet,
        amount_msat: MilliSatoshi::new(1_000_000),
        expiry_seconds: 3600,
        memo: Some("test".to_owned()),
    }
}

/// Walks all four `LndInvoiceState` arms plus the idempotent-replay
/// and contradictory-transition cases, then asserts the produced
/// outbox rows stream through gRPC into the Symphony stub.
#[tokio::test]
async fn incoming_invoice_subscription_drives_full_lifecycle() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    // Canned hashes pop LIFO — listed reverse of consumption order.
    let canned = CannedLnd::new(vec![PH_D, PH_C, PH_B, PH_A]);
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        Arc::new(canned),
        outbox,
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );

    // Step 1 — create invoices.
    let wallet = WalletId::from(Uuid::now_v7());
    let inv_a = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create A");
    assert_eq!(inv_a.payment_hash, PaymentHash::from(PH_A));
    assert_eq!(inv_a.state, InvoiceState::Open);

    let inv_b = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create B");
    assert_eq!(inv_b.payment_hash, PaymentHash::from(PH_B));

    let inv_c = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create C");
    assert_eq!(inv_c.payment_hash, PaymentHash::from(PH_C));

    let inv_d = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create D");
    assert_eq!(inv_d.payment_hash, PaymentHash::from(PH_D));

    // Subscribe before any outbox row fires.
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
    let response = client
        .subscribe_events(SubscribeEventsRequest { after_sequence: 0 })
        .await
        .expect("subscribe");
    let stream = response.into_inner();
    // Brief delay so subscription_loop registers LISTEN.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Step 2 — Settled for A.
    let preimage_a = Preimage::from([0xee; 32]);
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: PaymentHash::from(PH_A),
        state: LndInvoiceState::Settled,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: Some(preimage_a),
    })
    .await
    .expect("settle A");
    assert_state(&pool, &PaymentHash::from(PH_A), "settled").await;
    assert_outbox(
        &pool,
        "lightning_invoice_settled",
        "INCOMING_PAYMENT_CONFIRMED",
        1,
    )
    .await;

    // Step 3 — Accepted for B (HTLC arrived).
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: PaymentHash::from(PH_B),
        state: LndInvoiceState::Accepted,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: None,
    })
    .await
    .expect("hold B");
    assert_state(&pool, &PaymentHash::from(PH_B), "held").await;
    assert_outbox(&pool, "lightning_htlc_held", "INCOMING_PAYMENT_PENDING", 1).await;

    // Step 4 — Canceled for C.
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: PaymentHash::from(PH_C),
        state: LndInvoiceState::Canceled,
        htlc_amount_msat: MilliSatoshi::ZERO,
        payment_preimage: None,
    })
    .await
    .expect("cancel C");
    assert_state(&pool, &PaymentHash::from(PH_C), "canceled").await;
    assert_outbox(
        &pool,
        "lightning_invoice_canceled",
        "INCOMING_PAYMENT_CANCELED",
        1,
    )
    .await;

    // Step 5 — Duplicate Settled → idempotent; no new outbox row.
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: PaymentHash::from(PH_A),
        state: LndInvoiceState::Settled,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: Some(preimage_a),
    })
    .await
    .expect("duplicate settle is Ok(())");
    assert_outbox_total(&pool, 3).await;

    // Step 6 — Canceled after Settled → InvalidStateTransition.
    let err = app
        .handle_invoice_update(InvoiceUpdate {
            payment_hash: PaymentHash::from(PH_A),
            state: LndInvoiceState::Canceled,
            htlc_amount_msat: MilliSatoshi::ZERO,
            payment_preimage: None,
        })
        .await
        .expect_err("Canceled after Settled MUST surface");
    match err {
        blink_lightning_gateway::app::AppError::Invoice(InvoiceError::InvalidStateTransition {
            attempted: "cancel",
            ..
        }) => {}
        other => panic!("expected InvalidStateTransition(cancel), got {other:?}"),
    }
    assert_outbox_total(&pool, 3).await;

    // Step 7 — Open → Held → Settled (HOLD lifecycle).
    let preimage_d = Preimage::from([0xfa; 32]);
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: PaymentHash::from(PH_D),
        state: LndInvoiceState::Accepted,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: None,
    })
    .await
    .expect("hold D");
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: PaymentHash::from(PH_D),
        state: LndInvoiceState::Settled,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: Some(preimage_d),
    })
    .await
    .expect("settle D");
    assert_state(&pool, &PaymentHash::from(PH_D), "settled").await;
    assert_outbox_total(&pool, 5).await;
    assert_outbox(&pool, "lightning_htlc_held", "INCOMING_PAYMENT_PENDING", 2).await;
    assert_outbox(
        &pool,
        "lightning_invoice_settled",
        "INCOMING_PAYMENT_CONFIRMED",
        2,
    )
    .await;

    // Step 8 — consumer pipeline: 5 outbox rows → 5 CalaMock entries.
    let cala = CalaMock::default();
    let stub_handle = tokio::spawn(run_symphony_stub(stream, cala.clone(), 5));
    tokio::time::timeout(Duration::from_secs(15), stub_handle)
        .await
        .expect("stub completes within 15s")
        .expect("stub task did not panic");

    let entries = cala.snapshot().await;
    assert_eq!(entries.len(), 5, "expected 5 Cala mock entries");

    let count =
        |name: &str| -> usize { entries.iter().filter(|e| e.template_name == name).count() };
    assert_eq!(count("LIGHTNING_INVOICE_SETTLED"), 2);
    assert_eq!(count("LIGHTNING_INVOICE_PENDING"), 2);
    assert_eq!(count("LIGHTNING_INVOICE_CANCELED"), 1);

    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

/// `run_invoice_subscription_recovery_sweep` must ask for a listener on
/// every `Open` / `Held` invoice and skip terminal ones. Driven against
/// a recording dispatcher so the assertion observes the real sweep, not
/// a re-implementation of it.
#[tokio::test]
async fn recovery_sweep_spawns_listener_for_open_and_held_only() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let invoices_repo = Invoices::new(&pool);
    let open_hash = PaymentHash::from([0x11; 32]);
    let held_hash = PaymentHash::from([0x22; 32]);
    let settled_hash = PaymentHash::from([0x33; 32]);

    // Open — never paid.
    invoices_repo
        .create(seed_invoice(open_hash, "lnbc-open"))
        .await
        .unwrap();

    // Held — an HTLC is parked.
    let mut held = invoices_repo
        .create(seed_invoice(held_hash, "lnbc-held"))
        .await
        .unwrap();
    let _ = held
        .mark_held(MilliSatoshi::new(1_000_000), Timestamp::now())
        .unwrap();
    invoices_repo.update(&mut held).await.unwrap();

    // Settled — terminal; the sweep must skip it.
    let mut settled = invoices_repo
        .create(seed_invoice(settled_hash, "lnbc-settled"))
        .await
        .unwrap();
    let _ = settled
        .settle(Preimage::from([0xee; 32]), Timestamp::now())
        .unwrap();
    invoices_repo.update(&mut settled).await.unwrap();

    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        Arc::new(CannedLnd::new(Vec::new())),
        EventPublisher::new(&pool),
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );

    // Drive the real sweep; the recording dispatcher captures every
    // hash it asked to subscribe.
    let dispatcher = InvoiceUpdateDispatcher::recording_for_test();
    run_invoice_subscription_recovery_sweep(app, dispatcher.clone())
        .await
        .unwrap();

    let recorded = dispatcher.recorded();
    assert_eq!(
        recorded.len(),
        2,
        "one spawn per open/held invoice, no duplicates"
    );
    let recorded: std::collections::HashSet<_> = recorded.into_iter().collect();
    assert_eq!(
        recorded,
        std::collections::HashSet::from([open_hash, held_hash]),
        "recovery sweep must subscribe every open/held invoice and skip terminal ones"
    );
}

/// `NewInvoice` with a throwaway wallet/amount for seeding sweep rows.
fn seed_invoice(payment_hash: PaymentHash, bolt: &str) -> NewInvoice {
    NewInvoice::try_new(
        payment_hash,
        WalletId::from(Uuid::now_v7()),
        MilliSatoshi::new(1_000_000),
        3600,
        BoltInvoice::new(bolt),
        Timestamp::now(),
    )
    .expect("valid NewInvoice")
}

async fn assert_state(pool: &sqlx::PgPool, payment_hash: &PaymentHash, expected: &str) {
    let row: (String,) = sqlx::query_as(r#"SELECT state FROM invoices WHERE payment_hash = $1"#)
        .bind(payment_hash.as_bytes().as_slice())
        .fetch_one(pool)
        .await
        .expect("state row");
    assert_eq!(row.0, expected, "state for {payment_hash}");
}

async fn assert_outbox(
    pool: &sqlx::PgPool,
    domain_event_type: &str,
    event_type: &str,
    expected: i64,
) {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*) FROM outbox_events WHERE domain_event_type = $1 AND event_type = $2"#,
    )
    .bind(domain_event_type)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("outbox count");
    assert_eq!(
        row.0, expected,
        "outbox count for ({domain_event_type}, {event_type})"
    );
}

async fn assert_outbox_total(pool: &sqlx::PgPool, expected: i64) {
    let row: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM outbox_events"#)
        .fetch_one(pool)
        .await
        .expect("outbox total");
    assert_eq!(row.0, expected, "outbox row total");
}
