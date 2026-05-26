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
use crate::primitives::{MilliSatoshi, PaymentHash, Preimage, Timestamp};

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
    /// Apply one update from a per-hash listener. Quiet-ignores
    /// `NotFound` to absorb the create / listener-spawn race; dispatches
    /// each `LndInvoiceState` to its transition helper.
    ///
    /// Concurrency: the invoice is read here, before the transition
    /// helper opens its transaction. That read-modify-write is safe only
    /// because the single invoice-update consumer task (`src/cli.rs`)
    /// processes updates one at a time — two concurrent
    /// `handle_invoice_update` calls for one `payment_hash` would compute
    /// the same next event-log `sequence` and the second commit would
    /// violate the `invoice_events` primary key. If that consumer is ever
    /// sharded, the transition path needs row-level locking
    /// (`SELECT ... FOR UPDATE`).
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
                // transitions only — and LND's invoice state machine has
                // no `Accepted -> Open` (nor `Settled -> Open`) edge, so
                // `Open` can only ever arrive while the row is still
                // `Open`. There is no `Held -> Open` regression to catch.
                ::tracing::trace!(payment_hash = %payment_hash.to_hex(), "Open state; no-op");
                Ok(())
            }
            LndInvoiceState::Accepted => {
                self.transition_to_held(invoice, payment_hash, update.htlc_amount_msat, now)
                    .await?;
                // STUB(story-2.5): business gate (wallet-ownership /
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
                let preimage = update.payment_preimage.ok_or_else(|| {
                    AppError::Lnd(crate::lnd::LndError::InvalidResponse(
                        "Settled state but payment_preimage missing".to_owned(),
                    ))
                })?;
                self.transition_to_invoice_settled(invoice, payment_hash, preimage, now)
                    .await
            }
            LndInvoiceState::Canceled => {
                // Subscription path only observes LND's auto-cancel on
                // timeout; explicit-cancel commands will fire `Manual`
                // through a separate method.
                self.transition_to_invoice_canceled(
                    invoice,
                    payment_hash,
                    CancelReason::Expired,
                    now,
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
        htlc_amount_msat: MilliSatoshi,
        now: Timestamp,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        match invoice.mark_held(htlc_amount_msat, now)? {
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

        // The Held outbox event books a pending credit at the
        // *parked HTLC amount*. The persisted `held_amount_msat` (set
        // by `mark_held`) is what Settled / Canceled later echo, so the
        // pending layer reconciles. For a fixed-amount invoice this
        // equals `invoice.amount_msat`; for an amountless invoice it's
        // the only correct source.
        let amount_sat = htlc_amount_msat.whole_sat() as i64;
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
                        "htlc_amount_msat": htlc_amount_msat.as_u64(),
                        "wallet_id": wallet_id.to_string(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// `(Open|Held) → Settled` on LND `is_confirmed`.
    async fn transition_to_invoice_settled(
        &self,
        mut invoice: Invoice,
        payment_hash: PaymentHash,
        preimage: Preimage,
        now: Timestamp,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        match invoice.settle(preimage, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "settle ignored — duplicate replay"
                );
                return Ok(());
            }
        }

        // Settled events echo the persisted `held_amount_msat`
        // so Symphony's pending-layer release matches the credit the
        // `LightningHtlcHeld` event booked. For Open → Settled (no Held
        // ever happened — should not occur on the HODL substrate but
        // guarded against) fall through to 0 with a `warn!`.
        let amount_sat = invoice
            .held_amount_msat
            .map(|m| m.whole_sat() as i64)
            .unwrap_or_else(|| {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    "settled invoice has no held_amount_msat; emitting amount_sat=0"
                );
                0
            });
        let mut tx = self.pool.begin().await?;
        self.invoices
            .update_in_op(&mut tx, &mut invoice)
            .await
            .map_err(InvoiceError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_invoice_settled(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "payment_preimage": preimage.to_hex(),
                        "wallet_id": wallet_id.to_string(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// `(Open|Held) → Canceled` on LND `is_canceled`.
    async fn transition_to_invoice_canceled(
        &self,
        mut invoice: Invoice,
        payment_hash: PaymentHash,
        reason: CancelReason,
        now: Timestamp,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        // Clone so the outbox metadata can reference it after
        // `cancel()` consumes the original.
        let reason_for_outbox = reason.clone();
        match invoice.cancel(reason, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "cancel ignored — duplicate replay"
                );
                return Ok(());
            }
        }

        // Clearing the pending layer requires emitting the same
        // amount the Held event booked. `held_amount_msat.is_some()` is
        // exactly the "was it ever held" discriminator: for a never-held
        // (Open → Canceled) invoice no pending entry was booked.
        let amount_sat = invoice
            .held_amount_msat
            .map(|m| m.whole_sat() as i64)
            .unwrap_or(0);
        let mut tx = self.pool.begin().await?;
        self.invoices
            .update_in_op(&mut tx, &mut invoice)
            .await
            .map_err(InvoiceError::from)?;
        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_invoice_canceled(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "reason": reason_for_outbox.as_str(),
                        "wallet_id": wallet_id.to_string(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
