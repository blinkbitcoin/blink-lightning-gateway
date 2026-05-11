//! `OutboxError` — typed errors for the outbox publisher and listener.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("unknown gateway domain-event type: {0}")]
    UnknownEventType(String),

    #[error("outbox metadata serialization failed: {0}")]
    Metadata(#[from] serde_json::Error),

    #[error("LISTEN connection lost")]
    ListenDisconnected,

    #[error("outbox listener configuration error: {0}")]
    Configuration(String),
}
