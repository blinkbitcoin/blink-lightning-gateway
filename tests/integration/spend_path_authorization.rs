//! Story 3.1 spend-path coverage (AC21): the three behavioral guarantees
//! the un-stub introduces.
//!
//! (a) fail-closed — Symphony unreachable → payment declined → LND NEVER
//!     called (the invariant ADR-0003 exists to protect).
//! (b) wallet-ownership mismatch → `permission_denied`, LND never called.
//! (c) duplicate `(wallet_id, external_id)` → `DuplicateExternalId`, fails
//!     loudly (does not return the existing invoice).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bitcoin::hashes::{sha256, Hash};
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};

use blink_lightning_gateway::app::{
    App, AppError, InvoiceUpdateDispatcher, NewInvoiceRequest, SendPaymentRequest,
};
use blink_lightning_gateway::invoice::InvoiceError;
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::payment::PaymentState;
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, WalletId,
};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};

use crate::common::{CannedWalletOwnership, TestDatabase};
use uuid::Uuid;

const TEST_AMOUNT_MSAT: u64 = 100_000_000;

fn make_test_bolt11() -> String {
    let private_key = SecretKey::from_slice(&[0x42; 32]).unwrap();
    let payment_hash = sha256::Hash::from_slice(&[0xcc; 32]).unwrap();
    let payment_secret = PaymentSecret([0x11; 32]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    InvoiceBuilder::new(Currency::Regtest)
        .description("ln-gateway 3.1 spend-path test".into())
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

/// LND stub that records whether `send_payment` was ever called — the
/// fail-closed assertion turns on it staying `false`.
#[derive(Clone, Default)]
struct RecordingLnd {
    send_called: Arc<AtomicBool>,
}

#[async_trait]
impl LndApi for RecordingLnd {
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
    async fn lookup_invoice(&self, _payment_hash: PaymentHash) -> Result<InvoiceUpdate, LndError> {
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

fn build_app(
    pool: sqlx::PgPool,
    lnd: Arc<dyn LndApi>,
    symphony: Arc<dyn SymphonyClient>,
    ownership: Arc<dyn blink_lightning_gateway::wallet::WalletOwnershipChecker>,
) -> App {
    App::new(
        pool.clone(),
        lnd,
        EventPublisher::new(&pool),
        symphony,
        ownership,
        InvoiceUpdateDispatcher::for_test(),
    )
}

#[tokio::test]
async fn symphony_unreachable_declines_and_never_calls_lnd() {
    let db = TestDatabase::new().await.expect("test db");
    let lnd = RecordingLnd::default();
    let send_called = lnd.send_called.clone();
    // boot_stub Symphony fails closed (Unavailable) on every authorize.
    let app = build_app(
        db.pool.clone(),
        Arc::new(lnd),
        Arc::new(LightningSymphonyClient::boot_stub()),
        CannedWalletOwnership::allow(),
    );

    let payment = app
        .send_payment(SendPaymentRequest {
            caller_auth: Default::default(),
            wallet_id: WalletId::from(Uuid::now_v7()),
            payment_request: make_test_bolt11(),
            memo: None,
        })
        .await
        .expect("fail-closed routes through fail_inline (Ok, Failed)");

    assert_eq!(
        payment.state,
        PaymentState::Failed,
        "AuthorizeSpend transport error must decline the payment"
    );
    assert!(
        !send_called.load(Ordering::SeqCst),
        "LND send_payment MUST NOT be called when authorization fails closed"
    );
}

#[tokio::test]
async fn wallet_ownership_mismatch_is_permission_denied_and_never_calls_lnd() {
    let db = TestDatabase::new().await.expect("test db");
    let lnd = RecordingLnd::default();
    let send_called = lnd.send_called.clone();
    let app = build_app(
        db.pool.clone(),
        Arc::new(lnd),
        Arc::new(LightningSymphonyClient::boot_stub()),
        CannedWalletOwnership::deny(),
    );

    let err = app
        .send_payment(SendPaymentRequest {
            caller_auth: Default::default(),
            wallet_id: WalletId::from(Uuid::now_v7()),
            payment_request: make_test_bolt11(),
            memo: None,
        })
        .await
        .expect_err("ownership mismatch must error");

    assert!(matches!(err, AppError::WalletOwnership(_)));
    assert_eq!(
        tonic::Status::from(err).code(),
        tonic::Code::PermissionDenied,
        "wallet-ownership denial maps to permission_denied"
    );
    assert!(
        !send_called.load(Ordering::SeqCst),
        "ownership is the first gate — LND must never be reached"
    );
}

#[tokio::test]
async fn duplicate_external_id_on_same_wallet_fails_loudly() {
    let db = TestDatabase::new().await.expect("test db");
    let app = build_app(
        db.pool.clone(),
        Arc::new(RecordingLnd::default()),
        Arc::new(LightningSymphonyClient::boot_stub()),
        CannedWalletOwnership::allow(),
    );

    let wallet_id = WalletId::from(Uuid::now_v7());
    let req = |external_id: &str| NewInvoiceRequest {
        caller_auth: Default::default(),
        wallet_id,
        amount_msat: MilliSatoshi::new(1_000_000),
        expiry_seconds: 3600,
        memo: None,
        external_id: Some(external_id.to_owned()),
    };

    app.create_invoice(req("dup-ext"))
        .await
        .expect("first create");

    // Second create with the same (wallet_id, external_id) — a fresh preimage
    // means a distinct payment_hash, so the ONLY collision is external_id.
    let err = app
        .create_invoice(req("dup-ext"))
        .await
        .expect_err("duplicate external_id must fail loudly");

    assert!(
        matches!(
            err,
            AppError::Invoice(InvoiceError::DuplicateExternalId { .. })
        ),
        "expected DuplicateExternalId, got {err:?}"
    );
}
