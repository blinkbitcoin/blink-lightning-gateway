//! Layer 1 — request-level idempotency.
//!
//! STUB(epic-5.2): un-stub three-layer idempotency to use the
//! `idempotency_keys` table (per architecture L622-630). Schema already
//! exists from `migrations/<TS>_idempotency_stubs.sql`.

use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub Uuid);

impl IdempotencyKey {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for IdempotencyKey {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub enum IdempotencyOutcome {
    /// Fresh request — proceed with processing.
    NotSeen,
    /// Replay — return the prior response without re-processing.
    Seen { prior_response: serde_json::Value },
}

#[derive(Debug, Error)]
pub enum IdempotencyError {
    #[error("idempotency hash mismatch for replayed key")]
    HashMismatch,

    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

/// Repository wrapper for the `idempotency_keys` table.
///
/// STUB(epic-5.2): real implementation hashes the request, looks up the
/// row, returns `Seen { prior_response }` on hit (or `NotSeen` on miss),
/// and rejects mismatched hashes. Slice 1a returns `NotSeen` unconditionally.
#[derive(Clone, Debug, Default)]
pub struct IdempotencyKeys;

impl IdempotencyKeys {
    pub fn new() -> Self {
        Self
    }

    /// STUB(epic-5.2): always `Ok(NotSeen)`.
    pub async fn check_or_record(
        &self,
        _key: &IdempotencyKey,
        _request_hash: &[u8],
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        Ok(IdempotencyOutcome::NotSeen)
    }
}
