//! `App::reconcile_held_invoice` E2E.
//!
//! Seed a Held invoice, then for each branch of LND's possible state
//! drive `reconcile_held_invoice` (the same code path the 5-min
//! `invoice_reconciliation_sweep` job runs each tick) and assert the
//! gateway projection + outbox reflect LND's truth.
//!
//! Three cases:
//!   1. DB Held + LND SETTLED  → projection transitions Held→Settled;
//!      outbox `amount_sat` = `lnd_state.amt_paid_msat` (LND-truth);
//!      `gateway_metadata.held_amount_msat` carries the original
//!      reservation for Symphony's pending-layer offset.
//!      LND `SettleInvoice` is NOT called.
//!   2. DB Held + LND CANCELED → projection transitions Held→Canceled;
//!      outbox `amount_sat` = `lnd_state.amt_paid_msat` (typically 0
//!      for canceled invoices); `gateway_metadata.held_amount_msat`
//!      carries the original reservation. LND `CancelInvoice` is NOT
//!      called.
//!   3. DB Held + LND ACCEPTED → reconcile drives SettleInvoice +
//!      commit_settle (blink-core parity — self-heal a transient
//!      SettleInvoice failure from the original Accepted-arm).

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;

use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher};
use blink_lightning_gateway::invoice::entity::NewInvoice;
use blink_lightning_gateway::invoice::Invoices;
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

/// LND stub returning a configurable `LookupInvoice` result. Each test
/// case constructs one with the LND-side state it wants to simulate.
struct LookupStubLnd {
    lookup_result: InvoiceUpdate,
    settle_calls: StdMutex<Vec<Preimage>>,
    cancel_calls: StdMutex<Vec<PaymentHash>>,
}

impl LookupStubLnd {
    fn new(lookup_result: InvoiceUpdate) -> Self {
        Self {
            lookup_result,
            settle_calls: StdMutex::new(Vec::new()),
            cancel_calls: StdMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LndApi for LookupStubLnd {
    async fn add_hold_invoice(
        &self,
        _params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        Err(LndError::Stub)
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
        Ok(self.lookup_result.clone())
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

/// Helpers: seed a `Held` invoice in the DB so each test starts in the
/// same state. Returns (App, payment_hash, parked_amount).
async fn seed_held_invoice(
    pool: &sqlx::PgPool,
    lnd: Arc<LookupStubLnd>,
) -> (App, PaymentHash, MilliSatoshi) {
    let invoices_repo = Invoices::new(pool);
    let outbox = EventPublisher::new(pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let app = App::new(
        pool.clone(),
        lnd,
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );

    let preimage = Preimage::generate();
    let payment_hash = preimage.payment_hash();
    let parked = MilliSatoshi::new(750_000);
    let new = NewInvoice::try_new(
        payment_hash,
        preimage,
        WalletId::from(Uuid::now_v7()),
        Some(parked),
        3600,
        BoltInvoice::new("lnbc7500n1pj..."),
        "ext-id".to_owned(),
        Timestamp::now(),
    )
    .expect("valid new invoice");
    let mut invoice = invoices_repo.create(new).await.expect("create");
    let _ = invoice
        .mark_held(parked, Timestamp::now())
        .expect("mark_held");
    invoices_repo.update(&mut invoice).await.expect("update");

    (app, payment_hash, parked)
}

#[tokio::test]
async fn reconcile_held_to_settled_when_lnd_says_settled() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    // Simulate the MPP-overpayment / late-shard scenario: LND's
    // settled amount (amt_paid_msat) is larger than what was originally
    // parked at HOLD time. The outbox `amount_sat` MUST track LND's
    // truth; the metadata MUST carry the original reservation.
    let preimage_for_lookup = Preimage::from([0xee; 32]);
    let final_settled = MilliSatoshi::new(850_000); // > parked (750_000)
    let lnd_result_placeholder = InvoiceUpdate {
        payment_hash: PaymentHash::from([0u8; 32]),
        state: LndInvoiceState::Settled,
        amt_paid_msat: final_settled,
        payment_preimage: Some(preimage_for_lookup),
    };
    let lnd = Arc::new(LookupStubLnd::new(lnd_result_placeholder));
    let (_, payment_hash, parked) = seed_held_invoice(&pool, lnd.clone()).await;

    // Re-build the LND result with the actual seeded payment_hash so
    // the LookupInvoice path returns LND's "truth" for the right hash.
    let lookup_result = InvoiceUpdate {
        payment_hash,
        state: LndInvoiceState::Settled,
        amt_paid_msat: final_settled,
        payment_preimage: Some(preimage_for_lookup),
    };
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let lnd = Arc::new(LookupStubLnd::new(lookup_result));
    let app = App::new(
        pool.clone(),
        lnd.clone(),
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );

    app.reconcile_held_invoice(payment_hash)
        .await
        .expect("reconcile");

    // Projection moved to Settled.
    let row: (String,) = sqlx::query_as(r#"SELECT state FROM invoices WHERE payment_hash = $1"#)
        .bind(payment_hash.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("state row");
    assert_eq!(row.0, "settled");

    // Outbox Settled row: `amount_sat` reflects LND's `amt_paid_msat`
    // (the final settled amount, possibly > original parked due to
    // late MPP shards). `held_amount_msat` in metadata preserves the
    // original reservation so Symphony can offset the pending.
    let outbox_rows: Vec<(String, String, i64, serde_json::Value)> = sqlx::query_as(
        r#"SELECT domain_event_type, event_type, amount_sat, gateway_metadata
           FROM outbox_events
           WHERE reference_id = $1
           ORDER BY sequence"#,
    )
    .bind(payment_hash.to_hex())
    .fetch_all(&pool)
    .await
    .expect("outbox rows");
    assert_eq!(outbox_rows.len(), 1);
    assert_eq!(outbox_rows[0].0, "lightning_invoice_settled");
    assert_eq!(outbox_rows[0].1, "INCOMING_PAYMENT_CONFIRMED");
    // LND-fresh, NOT parked.
    assert_eq!(outbox_rows[0].2, final_settled.whole_sat() as i64);
    // Metadata mirrors blink-core's pattern: amt_paid_msat for the
    // settled amount, held_amount_msat for the original reservation.
    let meta = &outbox_rows[0].3;
    assert_eq!(
        meta.get("amt_paid_msat").and_then(|v| v.as_u64()),
        Some(final_settled.as_u64()),
        "metadata.amt_paid_msat MUST equal LND's final settled amount"
    );
    assert_eq!(
        meta.get("held_amount_msat").and_then(|v| v.as_u64()),
        Some(parked.as_u64()),
        "metadata.held_amount_msat MUST equal the originally-parked reservation"
    );

    // Reconcile does NOT call settle_invoice on LND — LND already
    // settled (which is why we're catching up via lookup). Calling
    // again would be a wasted RPC.
    assert!(
        lnd.settle_calls
            .lock()
            .expect("settle_calls lock")
            .is_empty(),
        "reconcile-to-settled path MUST NOT call settle_invoice (LND already settled)"
    );
    assert!(
        lnd.cancel_calls
            .lock()
            .expect("cancel_calls lock")
            .is_empty(),
        "reconcile-to-settled path MUST NOT call cancel_invoice"
    );
}

#[tokio::test]
async fn reconcile_held_to_canceled_when_lnd_says_canceled() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd_result_placeholder = InvoiceUpdate {
        payment_hash: PaymentHash::from([0u8; 32]),
        state: LndInvoiceState::Canceled,
        amt_paid_msat: MilliSatoshi::ZERO,
        payment_preimage: None,
    };
    let lnd = Arc::new(LookupStubLnd::new(lnd_result_placeholder));
    let (_, payment_hash, parked) = seed_held_invoice(&pool, lnd.clone()).await;

    // Rebuild LND with the real seeded hash.
    let lookup_result = InvoiceUpdate {
        payment_hash,
        state: LndInvoiceState::Canceled,
        amt_paid_msat: MilliSatoshi::ZERO,
        payment_preimage: None,
    };
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let lnd = Arc::new(LookupStubLnd::new(lookup_result));
    let app = App::new(
        pool.clone(),
        lnd.clone(),
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );

    app.reconcile_held_invoice(payment_hash)
        .await
        .expect("reconcile");

    let row: (String,) = sqlx::query_as(r#"SELECT state FROM invoices WHERE payment_hash = $1"#)
        .bind(payment_hash.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("state row");
    assert_eq!(row.0, "canceled");

    // Outbox Canceled row: `amount_sat` reflects LND's `amt_paid_msat`
    // (0 for a CANCELED invoice — HTLCs were released, not settled).
    // `held_amount_msat` in metadata preserves the original
    // reservation so Symphony's pending-layer release offsets it.
    let outbox_rows: Vec<(String, String, i64, serde_json::Value)> = sqlx::query_as(
        r#"SELECT domain_event_type, event_type, amount_sat, gateway_metadata
           FROM outbox_events
           WHERE reference_id = $1
           ORDER BY sequence"#,
    )
    .bind(payment_hash.to_hex())
    .fetch_all(&pool)
    .await
    .expect("outbox rows");
    assert_eq!(outbox_rows.len(), 1);
    assert_eq!(outbox_rows[0].0, "lightning_invoice_canceled");
    assert_eq!(outbox_rows[0].1, "INCOMING_PAYMENT_CANCELED");
    assert_eq!(outbox_rows[0].2, 0, "LND-canceled has amt_paid_msat=0");
    let meta = &outbox_rows[0].3;
    assert_eq!(
        meta.get("amt_paid_msat").and_then(|v| v.as_u64()),
        Some(0),
        "metadata.amt_paid_msat MUST equal LND's amt_paid_msat at cancel time"
    );
    assert_eq!(
        meta.get("held_amount_msat").and_then(|v| v.as_u64()),
        Some(parked.as_u64()),
        "metadata.held_amount_msat MUST preserve the originally-parked reservation"
    );

    assert!(
        lnd.cancel_calls
            .lock()
            .expect("cancel_calls lock")
            .is_empty(),
        "reconcile-to-canceled MUST NOT call cancel_invoice (LND already canceled)"
    );
}

#[tokio::test]
async fn reconcile_held_drives_settle_when_lnd_says_accepted() {
    // Blink-core parity: reconcile re-drives SettleInvoice + commit_settle
    // when LND is still ACCEPTED. Mirrors the cron-retry path in
    // `update-single-pending-invoice.ts` — a transient SettleInvoice
    // failure from the Accepted-arm self-heals on the next reconcile tick.
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd_result_placeholder = InvoiceUpdate {
        payment_hash: PaymentHash::from([0u8; 32]),
        state: LndInvoiceState::Accepted,
        amt_paid_msat: MilliSatoshi::new(750_000),
        payment_preimage: None,
    };
    let lnd = Arc::new(LookupStubLnd::new(lnd_result_placeholder));
    let (_, payment_hash, parked) = seed_held_invoice(&pool, lnd.clone()).await;

    let lookup_result = InvoiceUpdate {
        payment_hash,
        state: LndInvoiceState::Accepted,
        amt_paid_msat: parked,
        payment_preimage: None,
    };
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let lnd = Arc::new(LookupStubLnd::new(lookup_result));
    let app = App::new(
        pool.clone(),
        lnd.clone(),
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );

    app.reconcile_held_invoice(payment_hash)
        .await
        .expect("reconcile");

    let row: (String,) = sqlx::query_as(r#"SELECT state FROM invoices WHERE payment_hash = $1"#)
        .bind(payment_hash.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("state row");
    assert_eq!(row.0, "settled");

    // One Settled outbox row at LND's amt_paid_msat; metadata carries
    // held_amount_msat for Symphony's pending offset.
    let outbox_rows: Vec<(String, String, i64, serde_json::Value)> = sqlx::query_as(
        r#"SELECT domain_event_type, event_type, amount_sat, gateway_metadata
           FROM outbox_events
           WHERE reference_id = $1
           ORDER BY sequence"#,
    )
    .bind(payment_hash.to_hex())
    .fetch_all(&pool)
    .await
    .expect("outbox rows");
    assert_eq!(outbox_rows.len(), 1);
    assert_eq!(outbox_rows[0].0, "lightning_invoice_settled");
    assert_eq!(outbox_rows[0].1, "INCOMING_PAYMENT_CONFIRMED");
    assert_eq!(outbox_rows[0].2, parked.whole_sat() as i64);
    let meta = &outbox_rows[0].3;
    assert_eq!(
        meta.get("amt_paid_msat").and_then(|v| v.as_u64()),
        Some(parked.as_u64())
    );
    assert_eq!(
        meta.get("held_amount_msat").and_then(|v| v.as_u64()),
        Some(parked.as_u64())
    );

    // settle_invoice fired exactly once.
    let settle_calls = lnd.settle_calls.lock().expect("settle_calls lock");
    assert_eq!(settle_calls.len(), 1);
    drop(settle_calls);
    assert!(lnd
        .cancel_calls
        .lock()
        .expect("cancel_calls lock")
        .is_empty());
}
