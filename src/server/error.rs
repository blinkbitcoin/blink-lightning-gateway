//! Errors surfaced by the server-bootstrap layer.

use thiserror::Error;

use crate::outbox::OutboxError;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("listener bind error: {0}")]
    Bind(#[from] std::io::Error),

    #[error("outbox subsystem error: {0}")]
    Outbox(#[from] OutboxError),
}
