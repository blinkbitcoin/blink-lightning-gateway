//! `InvoiceError` — typed domain errors for the Invoice aggregate.
//!
//! `thiserror` per architecture L564-582. Story 2.3 adds
//! `InvalidStateTransition` mirroring `PaymentError::InvalidStateTransition`'s
//! shape — used by the new `mark_held` / `settle` / `cancel` command
//! methods when LND wire events contradict the projected state.

use thiserror::Error;

use super::entity::InvoiceState;
use super::repo::{InvoiceCreateError, InvoiceFindError, InvoiceModifyError, InvoiceQueryError};
use crate::primitives::PaymentHash;

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

    #[error(transparent)]
    InvoiceCreate(#[from] InvoiceCreateError),
    #[error(transparent)]
    InvoiceModify(#[from] InvoiceModifyError),
    #[error(transparent)]
    InvoiceFind(#[from] InvoiceFindError),
    #[error(transparent)]
    InvoiceQuery(#[from] InvoiceQueryError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}
