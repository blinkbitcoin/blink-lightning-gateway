//! `Invoices` — `EsRepo`-derived repository for the Invoice aggregate.
//!
//! `#[derive(EsRepo)]` generates `create` / `create_in_op`, `find_by_id` /
//! `maybe_find_by_id` / `find_by_id_in_op`, `find_by_payment_hash` /
//! `maybe_find_by_payment_hash`, `maybe_find_by_wallet_id` /
//! `list_for_wallet_id` (cursor-paginated), `update` / `update_in_op`, and
//! the internal `persist_events` driver. The macro reads the column list
//! below to emit the projection-row INSERT in `create_in_op` and the
//! UPDATE in `update_in_op`. See blink-card/src/authorization/repo.rs for
//! the same shape.

// `EsEntity` and `EsEvent` are imported because the `EsRepo` derive's
// expansion calls `Invoice::events()` (provided by `EsEntity`) and
// `<InvoiceEvent as EsEvent>::event_context()`. Both traits look unused at
// first glance — they're consumed inside the macro output.
use es_entity::EsRepo;
#[allow(unused_imports)]
use es_entity::{EsEntity, EsEvent};
use sqlx::PgPool;

use super::entity::Invoice;
use super::event::InvoiceEvent;
use crate::primitives::{InvoiceId, MilliSatoshi, PaymentHash, Timestamp, WalletId};

#[derive(EsRepo, Clone)]
#[es_repo(
    entity = "Invoice",
    columns(
        payment_hash(ty = "PaymentHash", update(persist = false)),
        wallet_id(ty = "WalletId", list_for, update(persist = false)),
        amount_msat(ty = "MilliSatoshi", find_by = false, update(persist = false)),
        expiry_at(ty = "Timestamp", find_by = false, update(persist = false)),
        state(
            ty = "String",
            find_by = false,
            create(accessor = "state_str()"),
            update(accessor = "state_str()"),
        ),
    )
)]
pub struct Invoices {
    pool: PgPool,
}

impl Invoices {
    pub fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }

    /// Return every invoice in a non-terminal state — used by Story 2.3's
    /// `invoice_subscription_recovery_sweep` to re-spawn per-hash
    /// listeners after a gateway restart. Hand-rolled rather than
    /// reaching for the cursor-paginated `list_*` methods (those don't
    /// accept a state filter and the open-invoice set is small in dev
    /// scope).
    pub async fn list_open_invoices(&self) -> Result<Vec<Invoice>, super::InvoiceError> {
        let rows = sqlx::query!(r#"SELECT id FROM invoices WHERE state IN ('pending', 'held')"#)
            .fetch_all(&self.pool)
            .await
            .map_err(es_entity::EsRepoError::Sqlx)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let invoice = self.find_by_id(InvoiceId::from(row.id)).await?;
            out.push(invoice);
        }
        Ok(out)
    }
}
