//! `Payments` — `EsRepo`-derived repository for the Payment aggregate.
//!
//! Same shape as `Invoices`. `#[derive(EsRepo)]` emits the projection
//! INSERT in `create_in_op` and UPDATE in `update_in_op` from the
//! column list below.

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
}
