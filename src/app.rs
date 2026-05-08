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
use crate::outbox::{EventPublisher, NewOutboxEvent};
use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Timestamp, WalletId};

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
    outbox: EventPublisher,
    lnd: Arc<dyn LndApi>,
    pool: PgPool,
    mode: Mode,
}

impl App {
    pub fn new(pool: PgPool, lnd: Arc<dyn LndApi>) -> Self {
        Self {
            invoices: Invoices::new(&pool),
            outbox: EventPublisher::new(&pool),
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
    ///   4. DB transaction wrapping the projection-row + events insert and
    ///      the outbox publish atomically.
    ///   5. Return the hydrated invoice.
    ///
    /// Order of (2) before (4) matches galoy's `addInvoiceForSelfForBtcWallet`
    /// at `blink/core/api/src/app/wallets/add-invoice-for-wallet.ts:55-82`.
    /// Failure mode "LND succeeds, DB fails" leaves an orphan invoice in
    /// LND with no DB record.
    /// KNOWN-ISSUE(story-4.3): orphan-invoice sweep job lands in chaos tests.
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
            lnd_resp.bolt_invoice.clone(),
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

        // 4. Atomic projection-row + event-rows + outbox-row insert in one
        // transaction. `create_in_op` returns the hydrated `Invoice`; no
        // separate find_by_payment_hash round-trip needed.
        let mut tx = self.pool.begin().await?;
        let invoice = self
            .invoices
            .create_in_op(&mut tx, new_invoice)
            .await
            .map_err(crate::invoice::InvoiceError::from)?;

        let outbox_event = build_invoice_created_outbox_event(
            &lnd_resp.payment_hash,
            request.amount_msat,
            now,
            &lnd_resp.bolt_invoice,
        )?;
        self.outbox.publish_in_tx(&mut tx, outbox_event).await?;
        tx.commit().await?;

        Ok(invoice)
    }

    /// STUB(epic-5.2): replace with Apollo Router entity sub-query + TTL
    /// cache (architecture's recommended path per L109).
    async fn check_wallet_ownership(&self, _wallet_id: &WalletId) -> Result<(), AppError> {
        Ok(())
    }
}

fn build_invoice_created_outbox_event(
    payment_hash: &PaymentHash,
    amount_msat: MilliSatoshi,
    now: Timestamp,
    bolt_invoice: &BoltInvoice,
) -> Result<NewOutboxEvent, AppError> {
    let metadata = serde_json::json!({
        "bolt_invoice": bolt_invoice.as_str(),
        "payment_hash": payment_hash.to_hex(),
    });
    let sat_amount: i64 = (amount_msat.as_u64() / 1000)
        .try_into()
        .map_err(|_| AppError::WalletOwnership("amount too large".to_owned()))?;
    Ok(NewOutboxEvent::for_lightning_invoice_created(
        InvoiceEventCorrelation::for_request(payment_hash).0,
        payment_hash.to_hex(),
        sat_amount,
        now.into_inner(),
        metadata,
    ))
}

// Internal helper that's nominally just the correlation_id stringification;
// kept here so the App's outbox event always has a deterministic
// payment_hash-keyed correlation that downstream Symphony can match against.
struct InvoiceEventCorrelation(String);
impl InvoiceEventCorrelation {
    fn for_request(payment_hash: &PaymentHash) -> Self {
        Self(payment_hash.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lnd::client::MockLndApi;
    use crate::lnd::AddInvoiceResponse;
    use crate::outbox::GatewayEventType;
    use crate::primitives::WalletId;
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres as PgImage;

    async fn boot_pg() -> (testcontainers::ContainerAsync<PgImage>, PgPool) {
        let container = PgImage::default().start().await.expect("start pg");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect pg");
        sqlx::migrate!().run(&pool).await.expect("migrate");
        (container, pool)
    }

    fn mock_lnd_returning_canned() -> Arc<dyn LndApi> {
        let mut mock = MockLndApi::new();
        mock.expect_add_invoice().returning(|_| {
            Box::pin(async {
                Ok(AddInvoiceResponse {
                    payment_hash: PaymentHash::from([0xab; 32]),
                    bolt_invoice: BoltInvoice::new("lnbc10n1pj..."),
                })
            })
        });
        Arc::new(mock)
    }

    fn ok_request() -> NewInvoiceRequest {
        NewInvoiceRequest {
            wallet_id: WalletId::new(),
            amount_msat: MilliSatoshi::new(1_000_000),
            expiry_seconds: 3600,
            memo: Some("test".to_owned()),
        }
    }

    #[tokio::test]
    #[serial]
    async fn create_invoice_persists_invoice_event_and_outbox_rows() {
        let (_pg, pool) = boot_pg().await;
        let app = App::new(pool.clone(), mock_lnd_returning_canned());

        let invoice = app.create_invoice(ok_request()).await.expect("create");
        assert_eq!(invoice.payment_hash, PaymentHash::from([0xab; 32]));
        assert_eq!(invoice.amount_msat, MilliSatoshi::new(1_000_000));

        let invoices_count: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM invoices"#)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(invoices_count.0, 1);

        let event_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM invoice_events WHERE event->>'type' = 'created'"#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count.0, 1);

        let outbox_row: (String, String, i64, String) = sqlx::query_as(
            r#"SELECT event_type, domain_event_type, sat_amount, currency FROM outbox_events"#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            outbox_row.0,
            GatewayEventType::IncomingPaymentPending.as_str()
        );
        assert_eq!(outbox_row.1, "lightning_invoice_created");
        assert_eq!(outbox_row.2, 1000); // 1_000_000 msat / 1000
        assert_eq!(outbox_row.3, "BTC");
    }

    #[tokio::test]
    #[serial]
    async fn create_invoice_propagates_invoice_error() {
        let (_pg, pool) = boot_pg().await;
        let app = App::new(pool, mock_lnd_returning_canned());
        let mut bad = ok_request();
        // Zero amount is the only condition that surfaces as `InvoiceError`
        // through `try_new`. Out-of-range expiry would be silently coerced
        // to the 4-hour default (matches blink-core), so it doesn't error.
        bad.amount_msat = MilliSatoshi::ZERO;
        let err = app.create_invoice(bad).await.unwrap_err();
        assert!(matches!(err, AppError::Invoice(_)));
    }

    #[tokio::test]
    #[serial]
    async fn create_invoice_propagates_lnd_error() {
        let (_pg, pool) = boot_pg().await;
        let mut mock = MockLndApi::new();
        mock.expect_add_invoice()
            .returning(|_| Box::pin(async { Err(crate::lnd::LndError::Stub) }));
        let app = App::new(pool, Arc::new(mock));
        let err = app.create_invoice(ok_request()).await.unwrap_err();
        assert!(matches!(err, AppError::Lnd(_)));
    }
}
