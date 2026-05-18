//! `AppError` — application-service error type. `anyhow::Error` is
//! permitted at this boundary per ADR #1; typed variants are preferred
//! for predictable matching at gRPC/GraphQL surfaces. The gRPC `Status`
//! mapping lives at `src/api/error.rs`.

use thiserror::Error;

use crate::invoice::InvoiceError;
use crate::lnd::LndError;
use crate::outbox::OutboxError;
use crate::payment::PaymentError;
use crate::symphony::SymphonyError;

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Invoice(#[from] InvoiceError),

    #[error(transparent)]
    Payment(#[from] PaymentError),

    #[error(transparent)]
    Lnd(#[from] LndError),

    #[error(transparent)]
    Outbox(#[from] OutboxError),

    #[error(transparent)]
    Symphony(#[from] SymphonyError),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("invalid bolt invoice: {0}")]
    InvalidBoltInvoice(String),

    #[error("wallet ownership check failed: {0}")]
    WalletOwnership(String),
}
