//! `SymphonyError` — typed errors for the Symphony-as-client adapter.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SymphonyError {
    #[error("symphony gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("symphony gRPC status: {0}")]
    Status(#[from] tonic::Status),

    #[error("symphony declined: {reason:?}")]
    Declined {
        reason: super::client::DeclineReason,
    },

    #[error("symphony adapter is stubbed; real handshake lands in story 2.5")]
    Stub,
}
