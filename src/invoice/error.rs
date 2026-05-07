//! `InvoiceError` — typed domain errors for the Invoice aggregate.
//!
//! `thiserror` per architecture L564-582. Variants only as needed by Slice 1a;
//! Story 2.2 (HOLD lifecycle) extends with `AlreadySettled`,
//! `AlreadyCancelled`, etc. as those state transitions land.

use thiserror::Error;

use crate::primitives::PaymentHash;

#[derive(Debug, Error)]
pub enum InvoiceError {
    #[error("invoice expiry must be in 60..=86400 seconds; got {0}")]
    InvalidExpiry(u32),

    #[error("invoice amount must be > 0 msat")]
    InvalidAmount,

    #[error("invoice not found for payment_hash {0}")]
    NotFound(PaymentHash),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error(transparent)]
    EsEntity(#[from] es_entity::EsEntityError),

    #[error("invoice events row decode failed: {0}")]
    EventDecode(#[from] serde_json::Error),
}
