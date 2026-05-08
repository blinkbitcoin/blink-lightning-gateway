//! `InvoiceError` — typed domain errors for the Invoice aggregate.
//!
//! `thiserror` per architecture L564-582. Variants only as needed by
//! Slice 1a; Story 2.2 (HOLD lifecycle) extends with `AlreadySettled`,
//! `AlreadyCancelled`, etc. as those state transitions land.

use thiserror::Error;

use crate::primitives::PaymentHash;

#[derive(Debug, Error)]
pub enum InvoiceError {
    #[error("invoice amount must be > 0 msat")]
    InvalidAmount,

    #[error("invoice not found for payment_hash {0}")]
    NotFound(PaymentHash),

    // `EsRepoError` already wraps `sqlx::Error`, `EsEntityError`, and
    // `CursorDestructureError` internally — no need for separate variants
    // for those.
    #[error(transparent)]
    EsRepo(#[from] es_entity::EsRepoError),
}
