//! Application coordinator — single `App` struct (NOT folder of
//! per-aggregate services) per architecture L940 and ADR #1.
//!
//! Slice 1a only carries `App::create_invoice` (the GraphQL
//! `lnInvoiceCreate` mutation routes here). Later slices add more
//! request-driven use-cases (`send_payment`, `fee_probe`, ...) and
//! background-driven ones (HTLC settlement reconciliation in Story 2.2,
//! payment finalization in Story 2.1). All `impl App` methods live in
//! this file until it grows large enough to justify splitting.
//!
//! `error::AppError` permits `anyhow::Error` at this boundary; layers
//! below it use typed `thiserror` errors.

use sqlx::PgPool;
use std::sync::Arc;

pub mod error;

pub use error::AppError;

use crate::invoice::{Invoice, Invoices, NewInvoice};
use crate::lnd::{AddInvoiceParams, LndApi};
use crate::primitives::{MilliSatoshi, Timestamp, WalletId};

/// Operating mode. `DryRun` short-circuits LND + DB writes — useful for
/// FR2's eventual shadow-mode plumbing. Slice 1a only ever runs `Live`;
/// the variant exists so future shadow-mode work has a defined home.
///
/// STUB(future-c2-shadow-mode): dry-run plumbing extends here when
/// shadow-mode lands per FR2/FR3 (PRD).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Live,
    DryRun,
}

/// Request shape for `App::create_invoice`. Carries the inputs the
/// GraphQL resolver collected; the App fills in `payment_hash` /
/// `bolt_invoice` from LND's `add_invoice` response.
#[derive(Clone, Debug)]
pub struct NewInvoiceRequest {
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_seconds: u32,
    pub memo: Option<String>,
}

/// Application coordinator. Holds the repos + adapter handles + pool. One
/// struct per the architecture's single-coordinator rule.
#[derive(Clone)]
pub struct App {
    invoices: Invoices,
    lnd: Arc<dyn LndApi>,
    pool: PgPool,
    mode: Mode,
}

impl App {
    pub fn new(pool: PgPool, lnd: Arc<dyn LndApi>) -> Self {
        Self {
            invoices: Invoices::new(&pool),
            lnd,
            pool,
            mode: Mode::Live,
        }
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `lnInvoiceCreate` use-case. Wires the slice top-to-bottom:
    ///   1. (STUB) wallet-ownership check.
    ///   2. LND `add_invoice` (source of truth for `payment_hash` +
    ///      `bolt_invoice`).
    ///   3. Pure entity command `Invoice::create`.
    ///   4. DB transaction wrapping the projection-row + events insert.
    ///   5. Return the hydrated invoice.
    ///
    /// Order of (2) before (4) matches galoy's `addInvoiceForSelfForBtcWallet`
    /// at `blink/core/api/src/app/wallets/add-invoice-for-wallet.ts:55-82`.
    /// Failure mode "LND succeeds, DB fails" leaves an orphan invoice in
    /// LND with no DB record.
    /// KNOWN-ISSUE(story-4.3): orphan-invoice sweep job lands in chaos tests.
    ///
    /// No outbox event fires on creation — blink-core doesn't broadcast
    /// invoice-creation either. The outbox row + standardized event for
    /// incoming-payment lifecycle land in Story 2.3 (the LND
    /// `subscribe_invoices` adapter + handler), keyed off real wire
    /// events (`is_held`, `is_confirmed`, `is_canceled`).
    pub async fn create_invoice(&self, request: NewInvoiceRequest) -> Result<Invoice, AppError> {
        let now = Timestamp::now();

        // 1. STUB(epic-5.2): wallet-ownership check. Replace with Apollo
        //    Router entity sub-query + TTL cache (architecture's recommended
        //    path per L109; ADR to be filed in Story 5.2).
        self.check_wallet_ownership(&request.wallet_id).await?;

        // 2. LND first (source of truth for payment_hash / bolt_invoice).
        // `request.memo` moves into `AddInvoiceParams` here; LND encodes it
        // into the BOLT11 `d` field of the returned bolt_invoice. We don't
        // persist memo separately — same as blink-core (its
        // walletInvoiceSchema has `paymentRequest` only, no `memo` column).
        let lnd_resp = self
            .lnd
            .add_invoice(AddInvoiceParams {
                amount_msat: request.amount_msat,
                memo: request.memo,
                expiry_seconds: request.expiry_seconds,
            })
            .await?;

        // 3. Validate inputs and build the post-validation `NewInvoice`. Only
        // a zero amount surfaces as Err; out-of-range expiry is coerced to
        // the BTC default (4h), matching blink-core.
        let new_invoice = NewInvoice::try_new(
            lnd_resp.payment_hash,
            request.wallet_id,
            request.amount_msat,
            request.expiry_seconds,
            lnd_resp.bolt_invoice,
            now,
        )?;

        if matches!(self.mode, Mode::DryRun) {
            // STUB(future-c2-shadow-mode): in shadow mode we'd return a
            // synthesized Invoice without persisting. Slice 1a never
            // reaches here; explicit error so misconfig is loud.
            return Err(AppError::WalletOwnership(
                "DryRun mode not yet wired in slice 1a".to_owned(),
            ));
        }

        // 4. Atomic projection-row + event-rows insert in one transaction.
        // `create_in_op` returns the hydrated `Invoice`; no separate
        // find_by_payment_hash round-trip needed.
        let mut tx = self.pool.begin().await?;
        let invoice = self
            .invoices
            .create_in_op(&mut tx, new_invoice)
            .await
            .map_err(crate::invoice::InvoiceError::from)?;
        tx.commit().await?;

        Ok(invoice)
    }

    /// STUB(epic-5.2): replace with Apollo Router entity sub-query + TTL
    /// cache (architecture's recommended path per L109).
    async fn check_wallet_ownership(&self, _wallet_id: &WalletId) -> Result<(), AppError> {
        Ok(())
    }
}
