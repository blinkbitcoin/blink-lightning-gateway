//! Slice 2 closing test — outbound payment happy path end-to-end.
//!
//! GraphQL `lnInvoicePaymentSend` → `App::send_payment` → stub LND
//! `send_payment` returns `IN_FLIGHT` → atomic DB tx wrapping
//! `Payment` projection + `Pending` event + `OutgoingPaymentInitiated`
//! outbox row → gRPC `SubscribeEvents` stream → in-process Symphony
//! stub → Cala-mock journal entries for both PENDING (hold) and
//! SETTLED (final + implicit reimbursement).
//!
//! Then the LND subscription handler fires: `app.handle_payment_update`
//! with `SUCCEEDED` → `Completed` event + `OutgoingPaymentCompleted`
//! outbox row → second stream message → second Cala-mock entry.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitcoin::hashes::{sha256, Hash};
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Server};

use blink_lightning_gateway::api::graphql::{build_schema, GatewaySchema};
use blink_lightning_gateway::api::grpc::LightningPaymentGatewayService;
use blink_lightning_gateway::app::App;
use blink_lightning_gateway::lightning_payment_gateway::{
    amount as proto_amount, lightning_payment_gateway_client::LightningPaymentGatewayClient,
    lightning_payment_gateway_server::LightningPaymentGatewayServer, GatewayEventType,
    SubscribeEventsRequest,
};
use blink_lightning_gateway::lnd::{
    AddInvoiceParams, AddInvoiceResponse, FeeProbeParams, FeeProbeResponse, LndApi, LndError,
    PaymentUpdate, SendPaymentParams, SendPaymentResponse, SendPaymentStatus,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::payment::{FailureReason, Hop, Payments};
use blink_lightning_gateway::primitives::{MilliSatoshi, PaymentHash, Preimage};
use blink_lightning_gateway::symphony::{
    DeclineReason, SymphonyAuthorizeRequest, SymphonyAuthorizeResponse, SymphonyAuthorizeStatus,
    SymphonyClient, SymphonyError,
};

use crate::common::TestDatabase;

const TEST_AMOUNT_MSAT: u64 = 100_000_000; // 100k sats; well above the 1-sat fee floor.
const PAYMENT_HASH_BYTES: [u8; 32] = [0xcc; 32];
const PREIMAGE_BYTES: [u8; 32] = [0xdd; 32];
const TEST_FEES_PAID_MSAT: u64 = 200_000; // < LnFees::max_for(100k sats) = 500k msat.

/// Build a valid BOLT11 invoice for the test using lightning-invoice's
/// `InvoiceBuilder`. Payment hash is fixed (`PAYMENT_HASH_BYTES`) so
/// the stub LND and the `handle_payment_update` call agree on which
/// payment to look up.
fn make_test_bolt11() -> String {
    let private_key = SecretKey::from_slice(&[0x42; 32]).unwrap();
    let payment_hash = sha256::Hash::from_slice(&PAYMENT_HASH_BYTES).unwrap();
    let payment_secret = PaymentSecret([0x11; 32]);

    // Use the current system time + a 1h expiry so the `would_expire`
    // guard in `decode_bolt11` (Story 2.2 review fix) does not reject
    // the test invoice as expired.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    InvoiceBuilder::new(Currency::Regtest)
        .description("ln-gateway slice 2 test".into())
        .payment_hash(payment_hash)
        .payment_secret(payment_secret)
        .amount_milli_satoshis(TEST_AMOUNT_MSAT)
        .duration_since_epoch(now)
        .expiry_time(std::time::Duration::from_secs(3600))
        .min_final_cltv_expiry_delta(144)
        .build_signed(|hash| Secp256k1::new().sign_ecdsa_recoverable(hash, &private_key))
        .unwrap()
        .to_string()
}

struct CannedLnd;

#[async_trait]
impl LndApi for CannedLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        Err(LndError::Stub)
    }

    async fn send_payment(
        &self,
        _params: SendPaymentParams,
    ) -> Result<SendPaymentResponse, LndError> {
        Ok(SendPaymentResponse {
            payment_hash: PaymentHash::from(PAYMENT_HASH_BYTES),
            payment_preimage: None,
            status: SendPaymentStatus::InFlight,
            fees_paid_msat: MilliSatoshi::ZERO,
            route_hops: Vec::new(),
            failure_reason: None,
        })
    }

    async fn fee_probe(&self, _params: FeeProbeParams) -> Result<FeeProbeResponse, LndError> {
        Err(LndError::Stub)
    }
}

#[derive(Default)]
struct CannedSymphonyClient;

#[tonic::async_trait]
impl SymphonyClient for CannedSymphonyClient {
    async fn authorize_spend(
        &self,
        request: SymphonyAuthorizeRequest,
    ) -> Result<SymphonyAuthorizeResponse, SymphonyError> {
        Ok(SymphonyAuthorizeResponse {
            status: SymphonyAuthorizeStatus::Approved,
            authorization_id: Some(request.correlation_id),
            decline_reason: None::<DeclineReason>,
        })
    }
}

fn build_test_schema(app: App) -> GatewaySchema {
    build_schema(app)
}

const MUTATION: &str = r#"
    mutation Send($input: LnInvoicePaymentInput!) {
        lnInvoicePaymentSend(input: $input) {
            status
            errors { message }
            transaction { id }
        }
    }
"#;

/// One Cala mock entry. The two outbound-payment templates emit
/// asymmetric amount params (hold vs. settled), so this is a wider
/// shape than Slice 1's CalaMockEntry.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CalaMockEntry {
    template_name: &'static str,
    /// PENDING-layer hold amount: `amount + max_fee_msat` for both
    /// initiated and out templates.
    amount_held_msat: u64,
    /// SETTLED-layer post amount: `amount + actual_fee_msat` (only
    /// meaningful for `LIGHTNING_PAYMENT_OUT`; the initiated template
    /// posts no SETTLED leg).
    amount_settled_msat: Option<u64>,
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

/// Consume `expected` messages off the SubscribeEvents stream. Dispatch
/// each by `event_type` and record a `CalaMockEntry` reflecting which
/// Symphony template would have fired. Asymmetric amounts (hold vs.
/// settled) come from `gateway_metadata` per the AC13 / AC14 contract.
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

        let amount_sat = match event.amount.as_ref().and_then(|a| a.value.as_ref()) {
            Some(proto_amount::Value::Sats(s)) => *s,
            other => panic!("unexpected amount variant: {other:?}"),
        };
        let metadata: serde_json::Value =
            serde_json::from_str(&event.gateway_metadata).expect("gateway_metadata parses");

        let entry = match GatewayEventType::try_from(event.event_type) {
            Ok(GatewayEventType::OutgoingPaymentInitiated) => {
                let max_fee = metadata
                    .get("max_fee_msat")
                    .and_then(|v| v.as_u64())
                    .expect("max_fee_msat in metadata");
                CalaMockEntry {
                    template_name: "LIGHTNING_PAYMENT_INITIATED",
                    amount_held_msat: amount_sat * 1000 + max_fee,
                    amount_settled_msat: None,
                }
            }
            Ok(GatewayEventType::OutgoingPaymentCompleted) => {
                // Stub shortcut: Symphony's real handler joins COMPLETED
                // back to INITIATED via correlation_id to read max_fee;
                // we reach into recorded CalaMock entries instead.
                let fees_paid = metadata
                    .get("fees_paid_msat")
                    .and_then(|v| v.as_u64())
                    .expect("fees_paid_msat in metadata");
                let amount_msat = amount_sat * 1000;
                let prior_held = {
                    let entries = cala.entries.lock().await;
                    entries
                        .iter()
                        .find(|e| e.template_name == "LIGHTNING_PAYMENT_INITIATED")
                        .map(|e| e.amount_held_msat)
                        .expect("prior INITIATED entry recorded")
                };
                CalaMockEntry {
                    template_name: "LIGHTNING_PAYMENT_OUT",
                    amount_held_msat: prior_held,
                    amount_settled_msat: Some(amount_msat + fees_paid),
                }
            }
            other => panic!("unexpected event_type for Slice 2: {other:?}"),
        };

        cala.record(entry).await;
        consumed += 1;
    }
}

#[tokio::test]
async fn payment_send_happy_path_drives_outbox_into_symphony_stub() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd: Arc<dyn LndApi> = Arc::new(CannedLnd);
    let outbox = EventPublisher::new(&pool);
    let symphony_for_app: Arc<dyn SymphonyClient> = Arc::new(CannedSymphonyClient);

    let app = App::new(pool.clone(), lnd, outbox, symphony_for_app);
    let schema = build_test_schema(app.clone());

    // ── Stand up the gRPC server before triggering the producer so the
    //    subscription can register LISTEN ahead of the pg_notify fire.
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
    // Small grace for LISTEN to register before the producer fires.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Producer: send GraphQL mutation.
    let bolt11 = make_test_bolt11();
    let wallet_id = "11111111-1111-1111-1111-111111111111";
    let request =
        async_graphql::Request::new(MUTATION).variables(async_graphql::Variables::from_value(
            async_graphql::Value::from_json(serde_json::json!({
                "input": {
                    "walletId": wallet_id,
                    "paymentRequest": bolt11,
                }
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
    let payload = data.get("lnInvoicePaymentSend").expect("payload");
    let resolver_errors = payload.get("errors").unwrap().as_array().unwrap();
    assert!(
        resolver_errors.is_empty(),
        "resolver errors: {resolver_errors:?}"
    );
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("PENDING")
    );

    // ── DB state after send.
    let (payments_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM payments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(payments_count, 1);
    let (state,): (String,) = sqlx::query_as("SELECT state FROM payments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "pending");
    let (event_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM payment_events WHERE event_type IN ('initiated','pending')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 2);
    let (outbox_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'OUTGOING_PAYMENT_INITIATED'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox_count, 1);

    // BOLT11 amount actually decoded — pins the msat unit semantics
    // through GraphQL → entity → projection.
    let payments_repo = Payments::new(&pool);
    let persisted = payments_repo
        .find_by_payment_hash(&PaymentHash::from(PAYMENT_HASH_BYTES))
        .await
        .expect("find");
    assert_eq!(persisted.amount_msat, MilliSatoshi::new(TEST_AMOUNT_MSAT));

    // ── Simulate the LND subscription firing SUCCEEDED.
    app.handle_payment_update(PaymentUpdate {
        payment_hash: PaymentHash::from(PAYMENT_HASH_BYTES),
        status: SendPaymentStatus::Succeeded,
        payment_preimage: Some(Preimage::from(PREIMAGE_BYTES)),
        fees_paid_msat: MilliSatoshi::new(TEST_FEES_PAID_MSAT),
        route_hops: Vec::<Hop>::new(),
        failure_reason: None::<FailureReason>,
    })
    .await
    .expect("handle_payment_update");

    // ── DB state after settlement.
    let (state,): (String,) = sqlx::query_as("SELECT state FROM payments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "completed");
    let (event_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM payment_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 3);
    let (outbox_completed,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'OUTGOING_PAYMENT_COMPLETED'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox_completed, 1);

    // Preimage round-tripped from handle_payment_update → settle event
    // → reload via try_from_events.
    let settled = payments_repo
        .find_by_payment_hash(&PaymentHash::from(PAYMENT_HASH_BYTES))
        .await
        .expect("find");
    assert_eq!(
        settled.payment_preimage,
        Some(Preimage::from(PREIMAGE_BYTES))
    );

    // ── Consumer half: drain 2 stream messages into the in-process
    //    Symphony stub + Cala mock.
    let cala = CalaMock::default();
    let stub_handle = tokio::spawn(run_symphony_stub(stream, cala.clone(), 2));
    tokio::time::timeout(Duration::from_secs(10), stub_handle)
        .await
        .expect("stub completes within 10s")
        .expect("stub task did not panic");

    let entries = cala.snapshot().await;
    assert_eq!(entries.len(), 2, "expected two Cala entries");
    assert_eq!(entries[0].template_name, "LIGHTNING_PAYMENT_INITIATED");
    assert_eq!(entries[1].template_name, "LIGHTNING_PAYMENT_OUT");

    // Asymmetric-amounts reimbursement contract: held = amount + max_fee
    // (PENDING-layer), settled = amount + fees_paid (SETTLED-layer); the
    // delta is the implicit refund. With the msat-policy fix, `amount`
    // is ceiling-rounded to whole sats at decode, so for the test's
    // whole-sat `TEST_AMOUNT_MSAT` (100k sats = 100_000_000 msat) the
    // outbox `amount_sat` field round-trips losslessly.
    //
    // These assertions use absolute expected values (not arithmetic that
    // mirrors what the test rig just wrote) so a regression in the
    // metadata's `max_fee_msat` source — say, the resolver setting it
    // from the wrong place — would actually fail the test rather than
    // tautologically re-deriving the same value.
    const EXPECTED_MAX_FEE_MSAT: u64 = 500_000; // 0.5% of 100k sat = 500 sat = 500_000 msat
    const EXPECTED_AMOUNT_MSAT: u64 = TEST_AMOUNT_MSAT;
    assert_eq!(
        blink_lightning_gateway::fees::LnFees::max_for(MilliSatoshi::new(TEST_AMOUNT_MSAT))
            .as_u64(),
        EXPECTED_MAX_FEE_MSAT,
        "LnFees::max_for contract check"
    );
    assert_eq!(
        entries[0].amount_held_msat,
        EXPECTED_AMOUNT_MSAT + EXPECTED_MAX_FEE_MSAT,
        "INITIATED hold = amount + max_fee"
    );
    assert_eq!(
        entries[1].amount_held_msat,
        EXPECTED_AMOUNT_MSAT + EXPECTED_MAX_FEE_MSAT,
        "OUT hold carries the prior max_fee"
    );
    assert_eq!(
        entries[1].amount_settled_msat,
        Some(EXPECTED_AMOUNT_MSAT + TEST_FEES_PAID_MSAT),
        "OUT settled = amount + actual fee (implicit refund of max_fee - actual_fee)"
    );

    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}
