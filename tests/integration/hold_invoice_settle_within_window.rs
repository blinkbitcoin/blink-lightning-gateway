//! HOLD-invoice settle E2E. Accepted → `transition_to_held` (Held
//! outbox) → `settle_hold_invoice` (LookupInvoice + SettleInvoice +
//! commit_settle) writes the Settled outbox and projection in one
//! path (blink-core parity). The subsequent synthetic Settled echo
//! must be a pure no-op.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;

use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher, NewInvoiceRequest};
use blink_lightning_gateway::invoice::InvoiceState;
use blink_lightning_gateway::lnd::{
    AddHoldInvoiceParams, AddHoldInvoiceResponse, FeeProbeParams, FeeProbeResponse, InvoiceUpdate,
    LndApi, LndError, LndInvoiceState, SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, WalletId,
};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};
use uuid::Uuid;

use crate::common::TestDatabase;

/// LND stub that records `add_hold_invoice` / `settle_invoice` /
/// `cancel_invoice` calls so the test can assert they fired.
struct RecordingLnd {
    add_calls: StdMutex<Vec<PaymentHash>>,
    settle_calls: StdMutex<Vec<Preimage>>,
    cancel_calls: StdMutex<Vec<PaymentHash>>,
}

impl RecordingLnd {
    fn new() -> Self {
        Self {
            add_calls: StdMutex::new(Vec::new()),
            settle_calls: StdMutex::new(Vec::new()),
            cancel_calls: StdMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LndApi for RecordingLnd {
    async fn add_hold_invoice(
        &self,
        params: AddHoldInvoiceParams,
    ) -> Result<AddHoldInvoiceResponse, LndError> {
        self.add_calls
            .lock()
            .expect("add_calls lock")
            .push(params.payment_hash);
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

    async fn lookup_invoice(&self, payment_hash: PaymentHash) -> Result<InvoiceUpdate, LndError> {
        // Canned ACCEPTED for the happy-path settle_hold_invoice flow.
        Ok(InvoiceUpdate {
            payment_hash,
            state: LndInvoiceState::Accepted,
            amt_paid_msat: MilliSatoshi::new(500_000),
            payment_preimage: None,
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

#[tokio::test]
async fn hold_invoice_settle_within_window_e2e() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd = Arc::new(RecordingLnd::new());
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let app = App::new(
        pool.clone(),
        lnd.clone(),
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );

    // Step 1 — issue the HODL invoice. The hash is gateway-derived from
    // a randomly generated preimage; the stub records the hash so we can
    // verify the gateway issued the right RPC.
    let wallet_id = WalletId::from(Uuid::now_v7());
    let inv = app
        .create_invoice(NewInvoiceRequest {
            caller_auth: Default::default(),
            wallet_id,
            amount_msat: MilliSatoshi::new(500_000),
            expiry_seconds: 3600,
            memo: Some("hold-settle-test".to_owned()),
            external_id: None,
        })
        .await
        .expect("create HODL invoice");
    assert_eq!(inv.state, InvoiceState::Open);
    let add_calls = lnd.add_calls.lock().expect("add_calls lock");
    assert_eq!(add_calls.len(), 1);
    assert_eq!(add_calls[0], inv.payment_hash);
    drop(add_calls);

    // Step 2 — synthetic Accepted. transition_to_held writes Held;
    // settle_hold_invoice runs LookupInvoice + SettleInvoice + commit_settle
    // so the Settled row + projection land in the same path.
    let parked = MilliSatoshi::new(500_000);
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv.payment_hash,
        state: LndInvoiceState::Accepted,
        amt_paid_msat: parked,
        payment_preimage: None,
    })
    .await
    .expect("accepted");

    let settle_calls = lnd.settle_calls.lock().expect("settle_calls lock");
    assert_eq!(settle_calls.len(), 1);
    assert_eq!(settle_calls[0], inv.payment_preimage);
    drop(settle_calls);
    assert!(lnd
        .cancel_calls
        .lock()
        .expect("cancel_calls lock")
        .is_empty());

    let row: (String,) = sqlx::query_as(r#"SELECT state FROM invoices WHERE payment_hash = $1"#)
        .bind(inv.payment_hash.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("state row");
    assert_eq!(row.0, "settled");

    let outbox_rows: Vec<(String, String, i64, serde_json::Value)> = sqlx::query_as(
        r#"SELECT domain_event_type, event_type, amount_sat, gateway_metadata
           FROM outbox_events
           WHERE reference_id = $1
           ORDER BY sequence"#,
    )
    .bind(inv.payment_hash.to_hex())
    .fetch_all(&pool)
    .await
    .expect("outbox rows");
    assert_eq!(outbox_rows.len(), 2);
    assert_eq!(outbox_rows[0].0, "lightning_htlc_held");
    assert_eq!(outbox_rows[0].1, "INCOMING_PAYMENT_PENDING");
    assert_eq!(outbox_rows[1].0, "lightning_invoice_settled");
    assert_eq!(outbox_rows[1].1, "INCOMING_PAYMENT_CONFIRMED");
    assert_eq!(outbox_rows[0].2, parked.whole_sat() as i64);
    assert_eq!(outbox_rows[1].2, parked.whole_sat() as i64);

    let settled_meta = &outbox_rows[1].3;
    assert_eq!(
        settled_meta.get("amt_paid_msat").and_then(|v| v.as_u64()),
        Some(parked.as_u64())
    );
    assert_eq!(
        settled_meta
            .get("held_amount_msat")
            .and_then(|v| v.as_u64()),
        Some(parked.as_u64())
    );

    // Step 3 — Settled wire event is observation-only; no extra outbox
    // row, no extra SettleInvoice.
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv.payment_hash,
        state: LndInvoiceState::Settled,
        amt_paid_msat: parked,
        payment_preimage: Some(inv.payment_preimage),
    })
    .await
    .expect("settled echo is no-op");
    let post_count: (i64,) =
        sqlx::query_as(r#"SELECT COUNT(*) FROM outbox_events WHERE reference_id = $1"#)
            .bind(inv.payment_hash.to_hex())
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(post_count.0, 2);
    assert_eq!(lnd.settle_calls.lock().expect("settle_calls lock").len(), 1);
}
