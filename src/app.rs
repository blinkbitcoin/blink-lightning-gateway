//! Application coordinator — single `App` struct (NOT folder of
//! per-aggregate services) per architecture L940 and ADR #1.
//!
//! Per-use-case methods live in dedicated `src/app/<use_case>.rs`
//! sibling files via `impl crate::app::App { ... }` blocks; this file
//! holds only the struct, its constructor, the request types, and the
//! per-use-case module declarations.

use sqlx::PgPool;
use std::sync::Arc;

pub mod create_invoice;
pub mod decode;
pub mod error;
pub mod fee_probe;
pub mod handle_invoice_update;
pub mod handle_payment_update;
pub mod helpers;
pub mod send_payment;

pub use error::AppError;
pub use handle_invoice_update::InvoiceUpdateDispatcher;

use crate::invoice::Invoices;
use crate::lnd::LndApi;
use crate::outbox::EventPublisher;
use crate::payment::Payments;
use crate::primitives::{MilliSatoshi, WalletId};
use crate::symphony::SymphonyClient;

/// Operating mode. `DryRun` short-circuits LND + DB writes — useful for
/// FR2's eventual shadow-mode plumbing. Slice 1a only ever runs `Live`;
/// the variant exists so future shadow-mode work has a defined home.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Live,
    DryRun,
}

#[derive(Clone, Debug)]
pub struct NewInvoiceRequest {
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_seconds: u32,
    pub memo: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SendPaymentRequest {
    pub wallet_id: WalletId,
    pub payment_request: String,
    pub memo: Option<String>,
}

#[derive(Clone, Debug)]
pub struct FeeProbeRequest {
    pub wallet_id: WalletId,
    pub payment_request: String,
}

#[derive(Clone)]
pub struct App {
    pub(crate) invoices: Invoices,
    pub(crate) payments: Payments,
    pub(crate) lnd: Arc<dyn LndApi>,
    pub(crate) outbox: EventPublisher,
    pub(crate) symphony: Arc<dyn SymphonyClient>,
    pub(crate) pool: PgPool,
    pub(crate) mode: Mode,
    pub(crate) invoice_dispatcher: InvoiceUpdateDispatcher,
}

impl App {
    pub fn new(
        pool: PgPool,
        lnd: Arc<dyn LndApi>,
        outbox: EventPublisher,
        symphony: Arc<dyn SymphonyClient>,
        invoice_dispatcher: InvoiceUpdateDispatcher,
    ) -> Self {
        Self {
            invoices: Invoices::new(&pool),
            payments: Payments::new(&pool),
            lnd,
            outbox,
            symphony,
            pool,
            mode: Mode::Live,
            invoice_dispatcher,
        }
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Accessor used by `job::invoice_subscription_recovery_sweep`. The
    /// repo is event-sourced and effectively read-only from outside the
    /// per-use-case files; exposing it as `&Invoices` is safer than
    /// duplicating the sweep logic onto `App`.
    pub fn invoices(&self) -> &Invoices {
        &self.invoices
    }
}
