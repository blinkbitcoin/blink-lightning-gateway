//! `InvoiceError` — typed domain errors for the Invoice aggregate.
//!
//! `thiserror` per architecture L564-582. Story 2.3 adds
//! `InvalidStateTransition` mirroring `PaymentError::InvalidStateTransition`'s
//! shape — used by the new `mark_held` / `settle` / `cancel` command
//! methods when LND wire events contradict the projected state.

use thiserror::Error;

use crate::primitives::PaymentHash;

use super::entity::InvoiceState;

#[derive(Debug, Error)]
pub enum InvoiceError {
    #[error("invoice amount must be > 0 msat")]
    InvalidAmount,

    #[error("invoice not found for payment_hash {0}")]
    NotFound(PaymentHash),

    #[error("invalid state transition from {from} attempting {attempted}")]
    InvalidStateTransition {
        from: InvoiceState,
        attempted: &'static str,
    },

    // `EsRepoError` already wraps `sqlx::Error`, `EsEntityError`, and
    // `CursorDestructureError` internally — no need for separate variants
    // for those.
    #[error(transparent)]
    EsRepo(#[from] es_entity::EsRepoError),
}
