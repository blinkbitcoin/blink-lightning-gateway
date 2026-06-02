//! `Payments` — `EsRepo`-derived repository for the Payment aggregate.
//!
//! Same shape as `Invoices`. `#[derive(EsRepo)]` emits the projection
//! INSERT in `create_in_op` and UPDATE in `update_in_op` from the
//! column list below.

use chrono::{DateTime, Utc};
use es_entity::EsRepo;
#[allow(unused_imports)]
use es_entity::{EsEntity, EsEvent};
use sqlx::PgPool;

use super::entity::Payment;
use super::event::PaymentEvent;
use crate::primitives::{MilliSatoshi, PaymentHash, PaymentId, WalletId};

#[derive(EsRepo, Clone)]
#[es_repo(
    entity = "Payment",
    columns(
        payment_hash(ty = "PaymentHash", update(persist = false)),
        wallet_id(ty = "WalletId", list_for, update(persist = false)),
        amount_msat(ty = "MilliSatoshi", find_by = false, update(persist = false)),
        max_fee_msat(ty = "MilliSatoshi", find_by = false, update(persist = false)),
        state(
            ty = "String",
            find_by = false,
            create(accessor = "state_str()"),
            update(accessor = "state_str()"),
        ),
    )
)]
pub struct Payments {
    pool: PgPool,
}

impl Payments {
    pub fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }

    /// Payment intents stranded in `initiated` past `older_than` — the
    /// orphan-hold sweep anchor (ADR-0003 / AC10). A payment only stays
    /// `initiated` if the gateway crashed between persisting the intent and
    /// the post-LND transition; the real LND payment-subscription moves
    /// genuinely in-flight payments to `pending` independently, so a row
    /// still `initiated` past the idle threshold has no live HTLC and its
    /// hold is safe to void.
    pub async fn list_stranded_initiated(
        &self,
        older_than: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<PaymentHash>, super::PaymentError> {
        let rows = sqlx::query!(
            r#"SELECT payment_hash as "payment_hash: PaymentHash"
               FROM payments
               WHERE state = 'initiated' AND created_at < $1
               LIMIT $2"#,
            older_than,
            limit,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.payment_hash).collect())
    }
}
