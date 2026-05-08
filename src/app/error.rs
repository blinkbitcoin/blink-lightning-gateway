//! `AppError` — application-service error type. `anyhow::Error` is
//! permitted at this boundary per ADR #1; typed variants are preferred for
//! predictable matching at gRPC/GraphQL surfaces. The gRPC `Status` mapping
//! lands in Story 1.5 at `src/api/error.rs`.

use thiserror::Error;

use crate::invoice::InvoiceError;
use crate::lnd::LndError;
use crate::outbox::OutboxError;

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Invoice(#[from] InvoiceError),

    #[error(transparent)]
    Lnd(#[from] LndError),

    #[error(transparent)]
    Outbox(#[from] OutboxError),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("wallet ownership check failed: {0}")]
    WalletOwnership(String),
}
