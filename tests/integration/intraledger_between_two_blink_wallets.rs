//! Slice 5 closing test — intraledger payment between two Blink wallets,
//! end-to-end (ADR-0007).
//!
//! Recipient invoice created via `App::create_invoice`; a second wallet pays
//! it via `App::send_payment`. A canned Symphony returns `Approved` and
//! captures the request. Asserts the transfer routes through the intraledger
//! path (no LND send), the `AuthorizeSpend` request shape (zero-fee + generic
//! `gateway_metadata`), the `Payment` going straight to `Completed`, the
//! recipient invoice reaching `Settled` with its LND invoice canceled, and the
//! single reporting-only outbox event — and NO incoming-confirmed accounting row.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bitcoin::hashes::{sha256, Hash};
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};

use blink_lightning_gateway::app::{
    App, AppError, InvoiceUpdateDispatcher, NewInvoiceRequest, SendPaymentRequest,
};
use blink_lightning_gateway::invoice::InvoiceState;
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::payment::{PaymentError, PaymentState, Payments};
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, WalletId,
};
use blink_lightning_gateway::symphony::{AccountKind, SymphonyClient};

use crate::common::{CannedWalletOwnership, RecordingSymphony, TestDatabase};
use uuid::Uuid;

const TEST_AMOUNT_MSAT: u64 = 100_000_000; // 100k sats
const TEST_AMOUNT_SAT: u64 = 100_000;

/// Build a real signed regtest BOLT11 encoding `payment_hash` + `amount_msat`,
/// so the invoice `create_invoice` stores actually decodes back to the
/// gateway-derived payment_hash (intraledger detection turns on that match).
fn build_bolt11_for(payment_hash: PaymentHash, amount_msat: u64) -> BoltInvoice {
    let private_key = SecretKey::from_slice(&[0x42; 32]).unwrap();
    let ph = sha256::Hash::from_slice(payment_hash.as_bytes()).unwrap();
    let payment_secret = PaymentSecret([0x11; 32]);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let signed = InvoiceBuilder::new(Currency::Regtest)
        .description("intraledger e2e".into())
        .payment_hash(ph)
        .payment_secret(payment_secret)
        .amount_milli_satoshis(amount_msat)
        .duration_since_epoch(now)
        .expiry_time(Duration::from_secs(3600))
        .min_final_cltv_expiry_delta(144)
        .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &private_key))
        .unwrap();
    BoltInvoice::new(signed.to_string())
}

/// LND stub that (a) issues a decodable BOLT11 on `add_hold_invoice`, (b)
/// records whether `send_payment` was ever called (must stay false on the
/// intraledger path), and (c) records the hashes passed to `cancel_invoice`.
#[derive(Clone, Default)]
struct IntraledgerLnd {
    send_called: Arc<AtomicBool>,
    canceled: Arc<Mutex<Vec<PaymentHash>>>,
}

#[async_trait]
impl LndApi for IntraledgerLnd {
    async fn add_hold_invoice(
        &self,
        params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        let amount_msat = params.amount_msat.map(|a| a.as_u64()).unwrap_or_default();
        Ok(AddHoldInvoiceResponse {
            bolt_invoice: build_bolt11_for(params.payment_hash, amount_msat),
        })
    }
    async fn settle_invoice(&self, _preimage: Preimage) -> Result<(), LndError> {
        Err(LndError::Stub)
    }
    async fn cancel_invoice(&self, payment_hash: PaymentHash) -> Result<(), LndError> {
        self.canceled.lock().unwrap().push(payment_hash);
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
        self.send_called.store(true, Ordering::SeqCst);
        Err(LndError::Stub)
    }
    async fn fee_probe(&self, _params: FeeProbeParams) -> Result<FeeProbeResponse, LndError> {
        Err(LndError::Stub)
    }
}

fn build_app(pool: sqlx::PgPool, lnd: Arc<dyn LndApi>, symphony: Arc<dyn SymphonyClient>) -> App {
    App::new(
        pool.clone(),
        lnd,
        EventPublisher::new(&pool),
        symphony,
        CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    )
}

fn invoice_req(wallet_id: WalletId) -> NewInvoiceRequest {
    NewInvoiceRequest {
        caller_auth: Default::default(),
        wallet_id,
        amount_msat: MilliSatoshi::new(TEST_AMOUNT_MSAT),
        expiry_seconds: 3600,
        memo: None,
        external_id: None,
    }
}

fn send_req(wallet_id: WalletId, payment_request: String) -> SendPaymentRequest {
    SendPaymentRequest {
        caller_auth: Default::default(),
        wallet_id,
        payment_request,
        memo: None,
    }
}

#[tokio::test]
async fn intraledger_between_two_blink_wallets_end_to_end() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let lnd = IntraledgerLnd::default();
    let send_called = lnd.send_called.clone();
    let canceled = lnd.canceled.clone();
    let (symphony, captured) = RecordingSymphony::approving();
    let app = build_app(pool.clone(), Arc::new(lnd), symphony);

    let sender = WalletId::from(Uuid::now_v7());
    let recipient = WalletId::from(Uuid::now_v7());

    // Recipient creates an invoice; sender pays it.
    let recipient_invoice = app
        .create_invoice(invoice_req(recipient))
        .await
        .expect("create recipient invoice");
    let payment_hash = recipient_invoice.payment_hash;

    let payment = app
        .send_payment(send_req(
            sender,
            recipient_invoice.bolt_invoice.as_str().to_owned(),
        ))
        .await
        .expect("intraledger send");

    // (a) Routed through the intraledger path — LND send was NEVER called.
    assert!(
        !send_called.load(Ordering::SeqCst),
        "LND send_payment MUST NOT be called on the intraledger path"
    );

    // (b) AuthorizeSpend called once, zero-fee (sat_amount == amount), with the
    //     generic gateway_metadata carrying intraledger + recipient_wallet_id.
    let requests = captured.lock().await;
    assert_eq!(requests.len(), 1, "exactly one AuthorizeSpend");
    let req = &requests[0];
    assert_eq!(req.correlation_id, payment_hash.to_hex());
    assert_eq!(req.account.kind, AccountKind::WalletLiability);
    assert_eq!(req.account.id, sender.to_string());
    assert_eq!(
        req.sat_amount, TEST_AMOUNT_SAT,
        "sat_amount is the bare amount — zero fee, no +max_fee"
    );
    assert_eq!(
        req.gateway_metadata
            .get("intraledger")
            .and_then(|v| v.as_bool()),
        Some(true),
        "gateway_metadata.intraledger must be true"
    );
    assert_eq!(
        req.gateway_metadata
            .get("recipient_wallet_id")
            .and_then(|v| v.as_str()),
        Some(recipient.to_string().as_str()),
        "gateway_metadata.recipient_wallet_id must be the recipient"
    );
    drop(requests);

    // (c) Payment is Completed and never went through pending.
    assert_eq!(payment.state, PaymentState::Completed);
    let reloaded = Payments::new(&pool)
        .find_by_payment_hash(&payment_hash)
        .await
        .expect("reload payment");
    assert_eq!(reloaded.state, PaymentState::Completed);
    let (pending_events,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM payment_events WHERE event_type = 'pending'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        pending_events, 0,
        "intraledger payment never enters pending"
    );

    // (d) Recipient invoice is Settled, and its LND invoice was canceled.
    let settled_invoice = app
        .invoices()
        .find_by_payment_hash(&payment_hash)
        .await
        .expect("reload invoice");
    assert_eq!(settled_invoice.state, InvoiceState::Settled);
    // Clone out so the std MutexGuard drops at end-of-statement (no guard held
    // across the awaits below).
    let canceled_hashes = canceled.lock().unwrap().clone();
    assert_eq!(
        canceled_hashes.as_slice(),
        &[payment_hash],
        "cancel_invoice called exactly once for the recipient invoice"
    );

    // (e) Exactly one outbox row: the reporting-only intraledger event mapping
    //     to OUTGOING_PAYMENT_COMPLETED, with intraledger + both wallet IDs.
    //     NO incoming-confirmed accounting row (that would double-credit).
    let (total,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(total, 1, "exactly one outbox row");
    let (domain_event, event_type, metadata): (String, String, serde_json::Value) =
        sqlx::query_as("SELECT domain_event_type, event_type, gateway_metadata FROM outbox_events")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(domain_event, "lightning_intraledger_transfer_completed");
    assert_eq!(event_type, "OUTGOING_PAYMENT_COMPLETED");
    assert_eq!(
        metadata.get("intraledger").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        metadata.get("sender_wallet_id").and_then(|v| v.as_str()),
        Some(sender.to_string().as_str())
    );
    assert_eq!(
        metadata.get("recipient_wallet_id").and_then(|v| v.as_str()),
        Some(recipient.to_string().as_str())
    );
    let (incoming_rows,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM outbox_events \
         WHERE event_type = 'INCOMING_PAYMENT_CONFIRMED' \
            OR domain_event_type = 'lightning_invoice_settled'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        incoming_rows, 0,
        "no incoming-confirmed accounting row — the credit leg is in the synchronous journal"
    );
}

#[tokio::test]
async fn intraledger_self_payment_is_rejected() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let lnd = IntraledgerLnd::default();
    let send_called = lnd.send_called.clone();
    let (symphony, captured) = RecordingSymphony::approving();
    let app = build_app(pool.clone(), Arc::new(lnd), symphony);

    let wallet = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_req(wallet))
        .await
        .expect("create invoice");

    let err = app
        .send_payment(send_req(wallet, invoice.bolt_invoice.as_str().to_owned()))
        .await
        .expect_err("self-payment must be rejected");

    assert!(
        matches!(err, AppError::Payment(PaymentError::SelfPayment)),
        "expected SelfPayment, got {err:?}"
    );
    assert_eq!(
        tonic::Status::from(err).code(),
        tonic::Code::InvalidArgument,
        "self-payment maps to invalid_argument"
    );
    assert!(
        !send_called.load(Ordering::SeqCst),
        "LND must never be called on a self-payment"
    );
    assert!(
        captured.lock().await.is_empty(),
        "AuthorizeSpend must not run for a self-payment"
    );
}

#[tokio::test]
async fn intraledger_already_settled_recipient_is_rejected() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let lnd = IntraledgerLnd::default();
    let send_called = lnd.send_called.clone();
    let (symphony, _captured) = RecordingSymphony::approving();
    let app = build_app(pool.clone(), Arc::new(lnd), symphony);

    let recipient = WalletId::from(Uuid::now_v7());
    let invoice = app
        .create_invoice(invoice_req(recipient))
        .await
        .expect("create invoice");
    let bolt = invoice.bolt_invoice.as_str().to_owned();

    // First transfer settles the recipient invoice.
    app.send_payment(send_req(WalletId::from(Uuid::now_v7()), bolt.clone()))
        .await
        .expect("first intraledger send");

    // A second pay against the now-Settled invoice is rejected as AlreadyPaid
    // by the recipient-state guard — before any new spend is authorized.
    let err = app
        .send_payment(send_req(WalletId::from(Uuid::now_v7()), bolt))
        .await
        .expect_err("paying an already-settled invoice must be rejected");

    assert!(
        matches!(err, AppError::Payment(PaymentError::AlreadyPaid { .. })),
        "expected AlreadyPaid, got {err:?}"
    );
    assert!(
        !send_called.load(Ordering::SeqCst),
        "LND must never be called on the intraledger path"
    );
}

#[tokio::test]
async fn intraledger_authorize_declined_fails_closed() {
    // AC7 fail-closed: a Declined AuthorizeSpend must leave the world untouched
    // — LND never called (no send, no cancel), recipient invoice still Open, no
    // Payment recorded, no outbox event. The only state change is the (declined)
    // authorize attempt itself.
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let lnd = IntraledgerLnd::default();
    let send_called = lnd.send_called.clone();
    let canceled = lnd.canceled.clone();
    let (symphony, captured) = RecordingSymphony::declining();
    let app = build_app(pool.clone(), Arc::new(lnd), symphony);

    let sender = WalletId::from(Uuid::now_v7());
    let recipient = WalletId::from(Uuid::now_v7());

    let recipient_invoice = app
        .create_invoice(invoice_req(recipient))
        .await
        .expect("create recipient invoice");
    let payment_hash = recipient_invoice.payment_hash;

    let err = app
        .send_payment(send_req(
            sender,
            recipient_invoice.bolt_invoice.as_str().to_owned(),
        ))
        .await
        .expect_err("declined AuthorizeSpend must fail the transfer");
    assert!(
        matches!(err, AppError::Symphony(_)),
        "expected a Symphony decline error, got {err:?}"
    );

    // The authorize attempt happened exactly once and was the ONLY side effect.
    assert_eq!(captured.lock().await.len(), 1, "one AuthorizeSpend attempt");
    assert!(
        !send_called.load(Ordering::SeqCst),
        "LND send_payment must not be called when the spend is declined"
    );
    assert!(
        canceled.lock().unwrap().is_empty(),
        "recipient LND invoice must not be canceled when the spend is declined"
    );

    // Recipient invoice stays Open; no Payment intent; no outbox event.
    let invoice = app
        .invoices()
        .find_by_payment_hash(&payment_hash)
        .await
        .expect("reload invoice");
    assert_eq!(
        invoice.state,
        InvoiceState::Open,
        "recipient invoice must stay Open on a declined transfer"
    );
    let (payment_event_rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM payment_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        payment_event_rows, 0,
        "no Payment recorded on a declined transfer"
    );
    let (outbox_rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outbox_rows, 0, "no outbox event on a declined transfer");
}
