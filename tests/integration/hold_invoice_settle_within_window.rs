//! Story 2.4 AC17 — HOLD invoice settle-within-window E2E.
//!
//! `App::create_invoice` issues a HODL invoice → drive a synthetic
//! `Accepted` `InvoiceUpdate` through `App::handle_invoice_update` →
//! the Accepted arm runs `transition_to_held` (commits `LightningHtlcHeld`
//! outbox row) then the auto-settle runs (`App::settle_hold_invoice` →
//! `LndApi::settle_invoice` → commits `LightningInvoiceSettled` outbox row).
//! Asserts the Held outbox amount equals the Settled outbox amount
//! (AC12 pending-layer reconciliation).

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

#[tokio::test]
async fn hold_invoice_settle_within_window_e2e() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();

    let lnd = Arc::new(RecordingLnd::new());
    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(
        pool.clone(),
        lnd.clone(),
        outbox,
        symphony,
        InvoiceUpdateDispatcher::for_test(),
    );

    // Step 1 — issue the HODL invoice. The hash is gateway-derived from
    // a randomly generated preimage; the stub records the hash so we can
    // verify the gateway issued the right RPC.
    let wallet_id = WalletId::from(Uuid::now_v7());
    let inv = app
        .create_invoice(NewInvoiceRequest {
            wallet_id,
            amount_msat: MilliSatoshi::new(500_000),
            expiry_seconds: 3600,
            memo: Some("hold-settle-test".to_owned()),
        })
        .await
        .expect("create HODL invoice");
    assert_eq!(inv.state, InvoiceState::Open);
    let add_calls = lnd.add_calls.lock().expect("add_calls lock");
    assert_eq!(add_calls.len(), 1);
    assert_eq!(add_calls[0], inv.payment_hash);
    drop(add_calls);

    // Step 2 — synthetic Accepted update. The Accepted arm runs
    // `transition_to_held` (commits LightningHtlcHeld outbox row), then
    // the stubbed business gate passes, then `settle_hold_invoice` calls
    // LND `settle_invoice` and commits the LightningInvoiceSettled row.
    let htlc_amount = MilliSatoshi::new(500_000);
    app.handle_invoice_update(InvoiceUpdate {
        payment_hash: inv.payment_hash,
        state: LndInvoiceState::Accepted,
        htlc_amount_msat: htlc_amount,
        payment_preimage: None,
    })
    .await
    .expect("accepted → auto-settle");

    // LND `SettleInvoice` was called exactly once with the gateway-owned
    // preimage. Catches the regression where the Accepted arm transitions
    // Held but never actually drives the LND-side settle.
    let settle_calls = lnd.settle_calls.lock().expect("settle_calls lock");
    assert_eq!(
        settle_calls.len(),
        1,
        "LndApi::settle_invoice must fire after auto-settle"
    );
    assert_eq!(settle_calls[0], inv.payment_preimage);
    drop(settle_calls);
    assert!(
        lnd.cancel_calls
            .lock()
            .expect("cancel_calls lock")
            .is_empty(),
        "auto-settle path MUST NOT call cancel_invoice"
    );

    // Step 3 — projection is Settled; pending layer reconciles.
    let row: (String,) = sqlx::query_as(r#"SELECT state FROM invoices WHERE payment_hash = $1"#)
        .bind(inv.payment_hash.as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("state row");
    assert_eq!(row.0, "settled");

    let outbox_rows: Vec<(String, String, i64)> = sqlx::query_as(
        r#"SELECT domain_event_type, event_type, amount_sat
           FROM outbox_events
           WHERE reference_id = $1
           ORDER BY sequence"#,
    )
    .bind(inv.payment_hash.to_hex())
    .fetch_all(&pool)
    .await
    .expect("outbox rows");
    assert_eq!(
        outbox_rows.len(),
        2,
        "Held + Settled outbox rows fired by auto-settle"
    );
    assert_eq!(outbox_rows[0].0, "lightning_htlc_held");
    assert_eq!(outbox_rows[0].1, "INCOMING_PAYMENT_PENDING");
    assert_eq!(outbox_rows[1].0, "lightning_invoice_settled");
    assert_eq!(outbox_rows[1].1, "INCOMING_PAYMENT_CONFIRMED");

    // AC12: the amount that books the pending credit at Held equals the
    // amount that clears it at Settled. Both come from the persisted
    // `held_amount_msat`, so they MUST be identical.
    assert_eq!(
        outbox_rows[0].2, outbox_rows[1].2,
        "Held outbox amount must equal Settled outbox amount (pending-layer reconciliation)"
    );
    assert_eq!(outbox_rows[0].2, htlc_amount.whole_sat() as i64);
}
