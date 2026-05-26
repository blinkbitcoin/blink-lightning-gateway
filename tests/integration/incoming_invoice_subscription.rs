//! Integration coverage for `App::handle_invoice_update`: drives synthetic
//! `InvoiceUpdate` values, asserts entity transitions + outbox rows +
//! the gRPC subscriber pipeline. Wire-format mapping coverage lives
//! in `src/lnd/invoice.rs` unit tests.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Server};

use blink_lightning_gateway::api::grpc::LightningPaymentGatewayService;
use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher, NewInvoiceRequest};
use blink_lightning_gateway::invoice::entity::NewInvoice;
use blink_lightning_gateway::invoice::Invoices;
use blink_lightning_gateway::job::invoice_subscription_recovery_sweep::run_invoice_subscription_recovery_sweep;
use blink_lightning_gateway::lightning_payment_gateway::{
    amount as proto_amount, lightning_payment_gateway_client::LightningPaymentGatewayClient,
    lightning_payment_gateway_server::LightningPaymentGatewayServer, GatewayEventType,
    SubscribeEventsRequest,
};
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, LndInvoiceState, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};
use uuid::Uuid;

use crate::common::TestDatabase;

/// LND stub. `add_hold_invoice` echoes back the gateway-supplied hash —
/// Story 2.4 made the hash an INPUT — and records the calls so tests
/// can assert the gateway issued the right RPC. `settle_invoice` /
/// `cancel_invoice` record their arguments and succeed.
struct CannedLnd {
    add_calls: AsyncMutex<Vec<PaymentHash>>,
    settle_calls: StdMutex<Vec<Preimage>>,
    cancel_calls: StdMutex<Vec<PaymentHash>>,
}

impl CannedLnd {
    fn new() -> Self {
        Self {
            add_calls: AsyncMutex::new(Vec::new()),
            settle_calls: StdMutex::new(Vec::new()),
            cancel_calls: StdMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LndApi for CannedLnd {
    async fn add_hold_invoice(
        &self,
        params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        self.add_calls.lock().await.push(params.payment_hash);
        Ok(AddHoldInvoiceResponse {
            bolt_invoice: BoltInvoice::new("lnbc10n1pj..."),
        })
    }

    async fn settle_invoice(&self, preimage: Preimage) -> Result<(), LndError> {
        self.settle_calls
            .lock()
            .expect("settle_calls lock")
            .push(preimage);
        Ok(())
    }

    async fn cancel_invoice(&self, payment_hash: PaymentHash) -> Result<(), LndError> {
        self.cancel_calls
            .lock()
            .expect("cancel_calls lock")
            .push(payment_hash);
        Ok(())
    }

    async fn lookup_invoice(&self, _payment_hash: PaymentHash) -> Result<InvoiceUpdate, LndError> {
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

/// Walk the LND invoice state machine through `handle_invoice_update` and
/// assert: entity transitions land, outbox rows fire, the gRPC subscriber
/// pipeline drains them. Story 2.4 changes the Accepted arm into an
/// auto-settle, so an Accepted observation now produces BOTH a Held and
/// a Settled outbox row.
#[tokio::test]
async fn incoming_invoice_subscription_drives_full_lifecycle() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd = Arc::new(CannedLnd::new());
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        lnd.clone(),
        outbox,
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );

    // Step 1 — create three invoices. Hashes are gateway-derived from
    // randomly generated preimages; capture them off the returned
    // `Invoice` so the synthetic wire events can reference them.
    // (Prior to the Story-2.4 settle-source-state tightening, this test
    // also exercised a fourth invoice for an `Open → Settled` direct
    // path; that path is now structurally impossible under the HODL
    // substrate — LND only fires Settled in response to our own
    // SettleInvoice call, which is gated on `state == Held`.)
    let wallet = WalletId::from(Uuid::now_v7());
    let inv_b = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create B");
    let inv_c = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create C");
    let inv_d = app
        .create_invoice(invoice_request(wallet))
        .await
        .expect("create D");

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

    // Step 2 — Accepted for B → auto-settles per the new Story 2.4 flow.
    // Two outbox rows (Held + Settled), state = Settled.
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv_b.payment_hash,
        state: LndInvoiceState::Accepted,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: None,
    })
    .await
    .expect("hold + auto-settle B");
    assert_state(&pool, &inv_b.payment_hash, "settled").await;
    assert_outbox(&pool, "lightning_htlc_held", "INCOMING_PAYMENT_PENDING", 1).await;
    assert_outbox(
        &pool,
        "lightning_invoice_settled",
        "INCOMING_PAYMENT_CONFIRMED",
        1,
    )
    .await;

    // Step 3 — Canceled (Open → Canceled) for C. Single Canceled outbox
    // row at amount=0 (never-held discriminator from AC12).
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv_c.payment_hash,
        state: LndInvoiceState::Canceled,
        htlc_amount_msat: MilliSatoshi::ZERO,
        payment_preimage: None,
    })
    .await
    .expect("cancel C");
    assert_state(&pool, &inv_c.payment_hash, "canceled").await;
    assert_outbox(
        &pool,
        "lightning_invoice_canceled",
        "INCOMING_PAYMENT_CANCELED",
        1,
    )
    .await;

    // Step 4 — Accepted for D auto-settles (2 rows), then a wire Settled
    // is idempotent (0 rows). The Accepted arm's auto-settle goes via the
    // `LndApi::settle_invoice` mock — the test stub records the call.
    // Also covers the crash-recovery safety net: LND echoes back Settled
    // after our SettleInvoice call; the Settled arm must no-op.
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv_d.payment_hash,
        state: LndInvoiceState::Accepted,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: None,
    })
    .await
    .expect("hold + auto-settle D");
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv_d.payment_hash,
        state: LndInvoiceState::Settled,
        htlc_amount_msat: MilliSatoshi::new(1_000_000),
        payment_preimage: Some(inv_d.payment_preimage),
    })
    .await
    .expect("idempotent settle D");
    assert_state(&pool, &inv_d.payment_hash, "settled").await;
    assert_outbox_total(&pool, 5).await;
    assert_outbox(&pool, "lightning_htlc_held", "INCOMING_PAYMENT_PENDING", 2).await;
    assert_outbox(
        &pool,
        "lightning_invoice_settled",
        "INCOMING_PAYMENT_CONFIRMED",
        2,
    )
    .await;

    // The auto-settle path called LND `SettleInvoice` once for each of B
    // and D — verifies the new wiring actually runs.
    let settle_count = lnd.settle_calls.lock().expect("settle_calls lock").len();
    assert_eq!(settle_count, 2, "LND SettleInvoice should fire for B + D");

    // Step 5 — consumer pipeline: 5 outbox rows → 5 CalaMock entries.
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
    let pre_open = Preimage::from([0x11; 32]);
    let pre_held = Preimage::from([0x22; 32]);
    let pre_settled = Preimage::from([0x33; 32]);
    let open_hash = pre_open.payment_hash();
    let held_hash = pre_held.payment_hash();

    // Open — never paid.
    invoices_repo
        .create(seed_invoice(pre_open, "lnbc-open"))
        .await
        .unwrap();

    // Held — an HTLC is parked.
    let mut held = invoices_repo
        .create(seed_invoice(pre_held, "lnbc-held"))
        .await
        .unwrap();
    let _ = held
        .mark_held(MilliSatoshi::new(1_000_000), Timestamp::now())
        .unwrap();
    invoices_repo.update(&mut held).await.unwrap();

    // Settled — terminal; the sweep must skip it. Under the HODL
    // substrate, `Invoice::settle` requires `Held` as source state, so
    // push through `mark_held` first to mirror the production path.
    let mut settled = invoices_repo
        .create(seed_invoice(pre_settled, "lnbc-settled"))
        .await
        .unwrap();
    let _ = settled
        .mark_held(MilliSatoshi::new(1_000_000), Timestamp::now())
        .unwrap();
    let _ = settled.settle(pre_settled, Timestamp::now()).unwrap();
    invoices_repo.update(&mut settled).await.unwrap();

    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        Arc::new(CannedLnd::new()),
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

/// `NewInvoice` seeded for sweep coverage. The preimage carries the hash
/// so the test can assert on its derivation if needed.
fn seed_invoice(preimage: Preimage, bolt: &str) -> NewInvoice {
    NewInvoice::try_new(
        preimage.payment_hash(),
        preimage,
        WalletId::from(Uuid::now_v7()),
        Some(MilliSatoshi::new(1_000_000)),
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
