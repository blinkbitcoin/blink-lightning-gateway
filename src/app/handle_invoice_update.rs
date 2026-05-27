//! `App::handle_invoice_update` + `InvoiceUpdateDispatcher`. The
//! dispatcher coordinates per-hash `subscribe_invoice` listeners;
//! `handle_invoice_update` applies one update from a listener.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use es_entity::Idempotent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app::helpers::is_invoice_not_found;
use crate::app::{App, AppError};
use crate::invoice::event::CancelReason;
use crate::invoice::{Invoice, InvoiceError};
use crate::lnd::{
    subscribe_invoice, InvoiceUpdate, LndClient, LndInvoiceState, SubscribeInvoiceExit,
};
use crate::outbox::NewOutboxEvent;
use crate::primitives::{MilliSatoshi, PaymentHash, Timestamp};

/// Coordinates per-hash `subscribe_invoice` listeners.
#[derive(Clone)]
pub struct InvoiceUpdateDispatcher {
    mode: Mode,
}

/// `Live` in production; the test variants stand in without an LND
/// connection — `NoOp` for tests that drive `handle_invoice_update`
/// synthetically, `Recording` for tests that need to observe which
/// hashes a caller asked to subscribe.
#[derive(Clone)]
enum Mode {
    Live(Arc<InvoiceUpdateDispatcherInner>),
    NoOp,
    Recording(Arc<Mutex<Vec<PaymentHash>>>),
}

struct InvoiceUpdateDispatcherInner {
    lnd: LndClient,
    tx: mpsc::Sender<InvoiceUpdate>,
    cancel: CancellationToken,
    active: Mutex<HashSet<PaymentHash>>,
}

impl InvoiceUpdateDispatcher {
    pub fn new(lnd: LndClient, tx: mpsc::Sender<InvoiceUpdate>, cancel: CancellationToken) -> Self {
        Self {
            mode: Mode::Live(Arc::new(InvoiceUpdateDispatcherInner {
                lnd,
                tx,
                cancel,
                active: Mutex::new(HashSet::new()),
            })),
        }
    }

    /// No-op dispatcher for tests that drive `handle_invoice_update`
    /// synthetically.
    pub fn for_test() -> Self {
        Self { mode: Mode::NoOp }
    }

    /// Recording dispatcher for tests: `spawn_listener_for` records the
    /// hash instead of spawning a listener, so a test can drive the real
    /// `run_invoice_subscription_recovery_sweep` and assert which
    /// invoices it asked to subscribe (read back via `recorded`).
    pub fn recording_for_test() -> Self {
        Self {
            mode: Mode::Recording(Arc::new(Mutex::new(Vec::new()))),
        }
    }

    /// Hashes passed to `spawn_listener_for`, in call order. Always empty
    /// unless this dispatcher was built via `recording_for_test`.
    pub fn recorded(&self) -> Vec<PaymentHash> {
        match &self.mode {
            Mode::Recording(recorded_hashes) => match recorded_hashes.lock() {
                Ok(guard) => guard.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            },
            _ => Vec::new(),
        }
    }

    /// Idempotent — duplicate calls for the same hash are a no-op.
    /// Unexpected listener exits surface as `tracing::error!` so
    /// silent-death failure modes are visible.
    pub fn spawn_listener_for(&self, payment_hash: PaymentHash) {
        let inner = match &self.mode {
            Mode::Live(inner) => inner,
            Mode::NoOp => {
                ::tracing::debug!(
                    payment_hash = %payment_hash.to_hex(),
                    "spawn_listener_for skipped — for_test dispatcher"
                );
                return;
            }
            Mode::Recording(recorded_hashes) => {
                let mut guard = match recorded_hashes.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.push(payment_hash);
                return;
            }
        };

        {
            // A panicking listener task must not lock the dispatcher out of subsequent spawns
            let mut guard = match inner.active.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if !guard.insert(payment_hash) {
                ::tracing::debug!(
                    payment_hash = %payment_hash.to_hex(),
                    "spawn_listener_for: listener already active; no-op"
                );
                return;
            }
        }

        let lnd = inner.lnd.clone();
        let tx = inner.tx.clone();
        let cancel = inner.cancel.clone();
        let inner_for_cleanup = inner.clone();
        tokio::spawn(async move {
            let result = subscribe_invoice(lnd, payment_hash, tx, cancel).await;
            match result {
                Ok(SubscribeInvoiceExit::Terminal) => {
                    ::tracing::debug!(
                        payment_hash = %payment_hash.to_hex(),
                        "per-hash invoice listener exited at terminal state"
                    );
                }
                Ok(SubscribeInvoiceExit::Cancelled) => {
                    ::tracing::debug!(
                        payment_hash = %payment_hash.to_hex(),
                        "per-hash invoice listener exited on cancellation"
                    );
                }
                Err(e) => {
                    ::tracing::error!(
                        payment_hash = %payment_hash.to_hex(),
                        error = %e,
                        "per-hash invoice listener exited unexpectedly"
                    );
                }
            }
            // Release the slot so the recovery sweep can respawn if needed.
            let mut guard = match inner_for_cleanup.active.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.remove(&payment_hash);
        });
    }
}

impl App {
    /// Apply one update from a per-hash listener.
    ///
    /// Concurrency: the read-modify-write here is safe only because
    /// the single invoice-update consumer task (`src/cli.rs`) processes
    /// updates serially — sharding it requires `SELECT ... FOR UPDATE`
    /// to avoid duplicate `invoice_events.sequence` writes.
    pub async fn handle_invoice_update(&self, update: InvoiceUpdate) -> Result<(), AppError> {
        let invoice = match self
            .invoices
            .find_by_payment_hash(&update.payment_hash)
            .await
        {
            Ok(i) => i,
            Err(e) if is_invoice_not_found(&e) => {
                ::tracing::debug!(
                    payment_hash = %update.payment_hash.to_hex(),
                    "invoice subscription update for unknown payment_hash; ignoring"
                );
                return Ok(());
            }
            Err(e) => return Err(InvoiceError::from(e).into()),
        };

        let now = Timestamp::now();
        let payment_hash = invoice.payment_hash;

        match update.state {
            LndInvoiceState::Open => {
                // No-op by design. `SubscribeSingleInvoice` emits the
                // current state once on subscribe, then forward
                // transitions only
                ::tracing::trace!(payment_hash = %payment_hash.to_hex(), "Open state; no-op");
                Ok(())
            }
            LndInvoiceState::Accepted => {
                self.transition_to_held(invoice, payment_hash, update.amt_paid_msat, now)
                    .await?;
                // STUB(story-3.1): business gate (wallet-ownership /
                // price-lock checks) — always passes for now. Story 2.4's
                // HODL substrate auto-settles every accepted HTLC so the
                // gateway preserves "regular invoice" UX on top of the
                // HODL path. galoy's `handleHeldInvoice` is the model.
                if let Err(e) = self.settle_hold_invoice(payment_hash).await {
                    ::tracing::error!(
                        payment_hash = %payment_hash.to_hex(),
                        error = %e,
                        "settle_hold_invoice after Held transition failed; \
                         invoice_reconciliation_sweep will apply LND's truth on next tick"
                    );
                }
                Ok(())
            }
            LndInvoiceState::Settled => {
                ::tracing::trace!(payment_hash = %payment_hash.to_hex(), "Settled; teardown only");
                Ok(())
            }
            LndInvoiceState::Canceled => {
                self.commit_cancel(
                    invoice,
                    payment_hash,
                    CancelReason::Expired,
                    update.amt_paid_msat,
                )
                .await
            }
        }
    }

    /// `Open → Held` on LND `Accepted`.
    async fn transition_to_held(
        &self,
        mut invoice: Invoice,
        payment_hash: PaymentHash,
        amt_paid_msat: MilliSatoshi,
        now: Timestamp,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        match invoice.mark_held(amt_paid_msat, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "mark_held ignored — duplicate replay"
                );
                return Ok(());
            }
        }

        let amount_sat = amt_paid_msat.whole_sat() as i64;
        let mut tx = self.pool.begin().await?;
        self.invoices
            .update_in_op(&mut tx, &mut invoice)
            .await
            .map_err(InvoiceError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_htlc_held(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "amt_paid_msat": amt_paid_msat.as_u64(),
                        "wallet_id": wallet_id.to_string(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
