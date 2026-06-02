//! Orphan-hold reconciliation sweep coverage.
//!
//! The sweep must confirm a stranded `initiated` payment's REAL outcome at LND
//! before touching its hold (interim guard for blink-ln-gateway-tie). These
//! tests pin each branch — and especially that a still-in-flight payment is
//! NOT voided (the money-loss regression: handle_payment_update no-ops on
//! InFlight, so a crash-stranded live HTLC stays `initiated`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher};
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, SendPaymentParams, SendPaymentResponse, SendPaymentStatus,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::payment::entity::{DecodedInvoice, NewPayment};
use blink_lightning_gateway::payment::{FailureReason, PaymentState, Payments};
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};
use blink_lightning_gateway::symphony::{
    SymphonyAuthorizeRequest, SymphonyAuthorizeResponse, SymphonyAuthorizeStatus, SymphonyClient,
    SymphonyError,
};
use sqlx::PgPool;

use crate::common::{CannedWalletOwnership, TestDatabase};

const PAYMENT_HASH_BYTES: [u8; 32] = [0xcc; 32];
const PREIMAGE_BYTES: [u8; 32] = [0xdd; 32];

/// LND stub whose `lookup_payment` returns a canned outcome; every other
/// method is `Stub` (the sweep only calls `lookup_payment`).
struct LookupLnd {
    behavior: LookupBehavior,
}

#[derive(Clone, Copy)]
enum LookupBehavior {
    Status(SendPaymentStatus),
    NotFound,
}

#[async_trait]
impl LndApi for LookupLnd {
    async fn add_hold_invoice(
        &self,
        _params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        Err(LndError::Stub)
    }

    async fn settle_invoice(&self, _preimage: Preimage) -> Result<(), LndError> {
        Err(LndError::Stub)
    }

    async fn cancel_invoice(&self, _payment_hash: PaymentHash) -> Result<(), LndError> {
        Err(LndError::Stub)
    }

    async fn lookup_invoice(&self, _payment_hash: PaymentHash) -> Result<InvoiceUpdate, LndError> {
        Err(LndError::Stub)
    }

    async fn lookup_payment(
        &self,
        payment_hash: PaymentHash,
    ) -> Result<SendPaymentResponse, LndError> {
        match self.behavior {
            LookupBehavior::NotFound => Err(LndError::PaymentNotFound),
            LookupBehavior::Status(status) => Ok(SendPaymentResponse {
                payment_hash,
                payment_preimage: matches!(status, SendPaymentStatus::Succeeded)
                    .then(|| Preimage::from(PREIMAGE_BYTES)),
                status,
                fees_paid_msat: MilliSatoshi::new(1_000),
                route_hops: Vec::new(),
                failure_reason: matches!(status, SendPaymentStatus::Failed)
                    .then(|| FailureReason::NoRoute),
            }),
        }
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

/// Trivial Symphony stub — required by `App::new` but never called by the
/// reconcile path (the hold is released/settled via the outbox event).
struct StubSymphony;

#[tonic::async_trait]
impl SymphonyClient for StubSymphony {
    async fn authorize_spend(
        &self,
        request: SymphonyAuthorizeRequest,
    ) -> Result<SymphonyAuthorizeResponse, SymphonyError> {
        Ok(SymphonyAuthorizeResponse {
            status: SymphonyAuthorizeStatus::Approved,
            authorization_id: Some(request.correlation_id),
            decline_reason: None,
        })
    }

    async fn void_spend_authorization(
        &self,
        _correlation_id: String,
        _authorization_id: String,
    ) -> Result<(), SymphonyError> {
        Ok(())
    }
}

fn build_app(pool: PgPool, lnd: Arc<dyn LndApi>) -> App {
    App::new(
        pool.clone(),
        lnd,
        EventPublisher::new(&pool),
        Arc::new(StubSymphony),
        CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    )
}

/// Persist a payment in `initiated` (as `send_payment` would, minus the
/// outbox row) and backdate it past any idle cutoff so the sweep selects it.
async fn seed_stranded_initiated(pool: &PgPool) {
    let payments = Payments::new(pool);
    let decoded = DecodedInvoice {
        payment_hash: PaymentHash::from(PAYMENT_HASH_BYTES),
        destination: "02abc".to_owned(),
        amount_msat: Some(MilliSatoshi::new(1_000_000)),
        bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
    };
    let new = NewPayment::try_new(
        decoded,
        WalletId::from(Uuid::now_v7()),
        None,
        MilliSatoshi::new(5_000),
        Timestamp::now(),
    )
    .expect("valid new payment");
    let created = payments.create(new).await.expect("create");
    assert_eq!(created.state, PaymentState::Initiated);

    // The test DB holds exactly this one payment, so a blanket backdate is safe.
    sqlx::query("UPDATE payments SET created_at = NOW() - INTERVAL '1 hour'")
        .execute(pool)
        .await
        .expect("backdate created_at");
}

async fn payment_state(pool: &PgPool) -> String {
    let (state,): (String,) = sqlx::query_as("SELECT state FROM payments")
        .fetch_one(pool)
        .await
        .unwrap();
    state
}

async fn outbox_count_of(pool: &PgPool, event_type: &str) -> i64 {
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM outbox_events WHERE event_type = $1")
            .bind(event_type)
            .fetch_one(pool)
            .await
            .unwrap();
    count
}

async fn terminal_outbox_count(pool: &PgPool) -> i64 {
    outbox_count_of(pool, "OUTGOING_PAYMENT_COMPLETED").await
        + outbox_count_of(pool, "OUTGOING_PAYMENT_FAILED").await
}

#[tokio::test]
async fn inflight_payment_is_left_untouched() {
    // The regression guard: a stranded intent whose HTLC LND still reports
    // in-flight must NOT be reconciled — voiding its hold here is the
    // money-loss bug. Leave it `initiated` with no terminal event.
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    seed_stranded_initiated(&pool).await;

    let lnd: Arc<dyn LndApi> = Arc::new(LookupLnd {
        behavior: LookupBehavior::Status(SendPaymentStatus::InFlight),
    });
    let app = build_app(pool.clone(), lnd);

    let reconciled = app
        .sweep_orphan_holds(Duration::from_secs(0), 100)
        .await
        .expect("sweep");

    assert_eq!(reconciled, 0, "in-flight payment must not be reconciled");
    assert_eq!(payment_state(&pool).await, "initiated");
    assert_eq!(
        terminal_outbox_count(&pool).await,
        0,
        "no terminal outbox event for an in-flight payment"
    );
}

#[tokio::test]
async fn notfound_payment_is_left_for_review() {
    // LND has no record (never received it). Still don't void blindly —
    // leave it for the next tick / operator review.
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    seed_stranded_initiated(&pool).await;

    let lnd: Arc<dyn LndApi> = Arc::new(LookupLnd {
        behavior: LookupBehavior::NotFound,
    });
    let app = build_app(pool.clone(), lnd);

    let reconciled = app
        .sweep_orphan_holds(Duration::from_secs(0), 100)
        .await
        .expect("sweep");

    assert_eq!(reconciled, 0, "NotFound payment must be left untouched");
    assert_eq!(payment_state(&pool).await, "initiated");
    assert_eq!(terminal_outbox_count(&pool).await, 0);
}

#[tokio::test]
async fn succeeded_payment_is_settled() {
    // LND confirms the stranded intent actually settled → reconcile to
    // completed and emit OutgoingPaymentCompleted (Symphony settles the hold).
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    seed_stranded_initiated(&pool).await;

    let lnd: Arc<dyn LndApi> = Arc::new(LookupLnd {
        behavior: LookupBehavior::Status(SendPaymentStatus::Succeeded),
    });
    let app = build_app(pool.clone(), lnd);

    let reconciled = app
        .sweep_orphan_holds(Duration::from_secs(0), 100)
        .await
        .expect("sweep");

    assert_eq!(reconciled, 1);
    assert_eq!(payment_state(&pool).await, "completed");
    assert_eq!(
        outbox_count_of(&pool, "OUTGOING_PAYMENT_COMPLETED").await,
        1,
        "settled reconcile emits exactly one Completed event"
    );
    assert_eq!(outbox_count_of(&pool, "OUTGOING_PAYMENT_FAILED").await, 0);
}

#[tokio::test]
async fn failed_payment_releases_the_hold_via_outbox() {
    // LND confirms the stranded intent failed → reconcile to failed and emit
    // OutgoingPaymentFailed so Symphony releases the hold (no sync void).
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    seed_stranded_initiated(&pool).await;

    let lnd: Arc<dyn LndApi> = Arc::new(LookupLnd {
        behavior: LookupBehavior::Status(SendPaymentStatus::Failed),
    });
    let app = build_app(pool.clone(), lnd);

    let reconciled = app
        .sweep_orphan_holds(Duration::from_secs(0), 100)
        .await
        .expect("sweep");

    assert_eq!(reconciled, 1);
    assert_eq!(payment_state(&pool).await, "failed");
    assert_eq!(
        outbox_count_of(&pool, "OUTGOING_PAYMENT_FAILED").await,
        1,
        "failed reconcile emits exactly one Failed event"
    );
    assert_eq!(
        outbox_count_of(&pool, "OUTGOING_PAYMENT_COMPLETED").await,
        0
    );
}
