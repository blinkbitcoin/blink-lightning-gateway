//! `OutboxError` — typed errors for the outbox publisher path.
//!
//! The `ListenDisconnected` variant lands in Story 1.5 alongside
//! `listen_connection.rs`; Slice 1a only writes, never listens (tests open a
//! throwaway `tokio_postgres::LISTEN` inline).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("unknown gateway domain-event type: {0}")]
    UnknownEventType(String),

    #[error("outbox metadata serialization failed: {0}")]
    Metadata(#[from] serde_json::Error),
}
