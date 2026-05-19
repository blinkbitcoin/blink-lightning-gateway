//! Subscription-driven invoice-update handler — dispatches LND
//! per-hash `SubscribeSingleInvoice` updates through
//! `App::handle_invoice_update` and three transition helpers
//! (`transition_to_held`, `transition_to_invoice_settled`,
//! `transition_to_invoice_canceled`).
//!
//! Also home to the `InvoiceUpdateDispatcher` — owns the shared
//! `mpsc::Sender<InvoiceUpdate>`, the `LndClient`, the
//! `CancellationToken`, and a `HashSet<PaymentHash>` of currently-active
//! per-hash listeners. `spawn_listener_for` is idempotent: calling it
//! twice for the same `payment_hash` is a no-op. The spawn wrapper
//! implements AC9: a per-hash listener that exits via any path OTHER
//! than `cancel.cancelled()` or terminal-state forwarding logs
//! `tracing::error!`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use es_entity::Idempotent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app::helpers::is_es_not_found;
use crate::app::{App, AppError};
use crate::invoice::event::CancelReason;
use crate::invoice::{Invoice, InvoiceError};
use crate::lnd::{
    subscribe_invoice, InvoiceUpdate, LndClient, LndInvoiceState, SubscribeInvoiceExit,
};
use crate::outbox::NewOutboxEvent;
use crate::primitives::{MilliSatoshi, PaymentHash, Preimage, Timestamp};

/// Coordinates per-hash `subscribe_invoice` listeners. Constructed
/// once in `cli::run_cmd` and shared (via `Clone`) into `App::new` so
/// `App::create_invoice` can spawn a listener immediately after the
/// LND `add_invoice` ack.
#[derive(Clone)]
pub struct InvoiceUpdateDispatcher {
    inner: Option<Arc<InvoiceUpdateDispatcherInner>>,
}

struct InvoiceUpdateDispatcherInner {
    lnd: LndClient,
    tx: mpsc::Sender<InvoiceUpdate>,
    cancel: CancellationToken,
    active: Mutex<HashSet<PaymentHash>>,
}

impl InvoiceUpdateDispatcher {
    /// Real dispatcher: spawns per-hash `subscribe_invoice` listeners.
    pub fn new(lnd: LndClient, tx: mpsc::Sender<InvoiceUpdate>, cancel: CancellationToken) -> Self {
        Self {
            inner: Some(Arc::new(InvoiceUpdateDispatcherInner {
                lnd,
                tx,
                cancel,
                active: Mutex::new(HashSet::new()),
            })),
        }
    }

    /// Test-only constructor whose `spawn_listener_for` is a no-op. Used
    /// by integration tests that drive `handle_invoice_update`
    /// synthetically without real per-hash listeners.
    pub fn for_test() -> Self {
        Self { inner: None }
    }

    /// Spawn a per-hash listener for `payment_hash`. Idempotent: calling
    /// twice for the same hash is a debug-log + no-op. The spawned task
    /// implements AC9: any exit path other than terminal-state or
    /// cancellation logs `tracing::error!`.
    pub fn spawn_listener_for(&self, payment_hash: PaymentHash) {
        let Some(inner) = self.inner.as_ref() else {
            ::tracing::debug!(
                payment_hash = %payment_hash.to_hex(),
                "spawn_listener_for skipped — for_test dispatcher"
            );
            return;
        };

        {
            let mut guard = match inner.active.lock() {
                Ok(g) => g,
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
                        "per-hash invoice listener exited cleanly at terminal state"
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
                        "per-hash invoice listener exited unexpectedly; \
                         this invoice will not have a live observer until next restart's recovery sweep"
                    );
                }
            }
            // Always release the slot — listener can be respawned by the
            // recovery sweep if it died unexpectedly.
            let mut guard = match inner_for_cleanup.active.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.remove(&payment_hash);
        });
    }
}

impl App {
    /// Handle one `InvoiceUpdate` from a per-hash `subscribe_invoice`
    /// listener. Dispatches on `LndInvoiceState` to the matching
    /// transition helper. All work happens in one DB transaction so the
    /// projection update and the outbox row land together.
    ///
    /// `NotFound` is quiet-ignored: per-hash listeners spawned by
    /// `App::create_invoice` always run after the create's tx commits,
    /// but the recovery sweep + transient races can in principle deliver
    /// an update for an unknown payment_hash.
    pub async fn handle_invoice_update(&self, update: InvoiceUpdate) -> Result<(), AppError> {
        let invoice = match self
            .invoices
            .find_by_payment_hash(&update.payment_hash)
            .await
        {
            Ok(i) => i,
            Err(e) if is_es_not_found(&e) => {
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
                ::tracing::trace!(
                    payment_hash = %payment_hash.to_hex(),
                    "initial Open state from per-hash listener; row already in Pending"
                );
                Ok(())
            }
            LndInvoiceState::Accepted => {
                self.transition_to_held(invoice, payment_hash, update.htlc_amount_msat, now)
                    .await
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
                // Story 2.3's subscription path only ever sees Canceled
                // from LND's auto-cancel on timeout. Story 2.4 will
                // wire `CancelReason::Manual` through a separate
                // explicit-cancel App method.
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

    /// `Pending → Held` on LND `Accepted`.
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
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "mark_held ignored — duplicate Accepted replay"
                );
                return Ok(());
            }
        }

        let amount_sat = invoice.amount_msat.whole_sat() as i64;
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

    /// `(Pending|Held) → Settled` on LND `Settled` (`is_confirmed`).
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
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "settle ignored — duplicate Settled replay"
                );
                return Ok(());
            }
        }

        let amount_sat = invoice.amount_msat.whole_sat() as i64;
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

    /// `(Pending|Held) → Canceled` on LND `Canceled` (`is_canceled`).
    async fn transition_to_invoice_canceled(
        &self,
        mut invoice: Invoice,
        payment_hash: PaymentHash,
        reason: CancelReason,
        now: Timestamp,
    ) -> Result<(), AppError> {
        let wallet_id = invoice.wallet_id;
        let reason_for_outbox = reason.clone();
        match invoice.cancel(reason, now)? {
            Idempotent::Executed(()) => {}
            Idempotent::Ignored => {
                ::tracing::info!(
                    payment_hash = %payment_hash.to_hex(),
                    current_state = %invoice.state,
                    "cancel ignored — duplicate Canceled replay"
                );
                return Ok(());
            }
        }

        let amount_sat = invoice.amount_msat.whole_sat() as i64;
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
