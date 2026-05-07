//! Layer 2 — event-level idempotency.
//!
//! STUB(epic-5.2): un-stub to use the `processed_events` table. Schema
//! already exists from `migrations/<TS>_idempotency_stubs.up.sql`. The
//! real impl checks `(gateway_id, sequence)` membership and records on
//! first-process so replays are no-ops.

use super::request::{IdempotencyError, IdempotencyOutcome};

#[derive(Clone, Debug, Default)]
pub struct ProcessedEvents;

impl ProcessedEvents {
    pub fn new() -> Self {
        Self
    }

    /// STUB(epic-5.2): always `Ok(NotSeen)`. Real impl checks the
    /// `processed_events(gateway_id, sequence)` PK.
    pub async fn dedupe(
        &self,
        _gateway_id: &str,
        _sequence: i64,
    ) -> Result<IdempotencyOutcome, IdempotencyError> {
        Ok(IdempotencyOutcome::NotSeen)
    }
}
