//! Three-layer idempotency surface (architecture L622-630):
//!
//! 1. `request` — request-level: hash the inbound request, replay returns
//!    the cached response.
//! 2. `event` — event-level: consumer-side dedup of `(gateway_id, sequence)`.
//! 3. `correlation` — correlation-id propagation from inbound request through
//!    outbox event into Cala.
//!
//! Slice 1a stubs all three with identity (`Ok(NotSeen)`) so the
//! `App::create_invoice` use-case has the right shape. Real implementations
//! land in Story 5.2 (un-stubs) — the schema is already in place from
//! `migrations/<TS>_idempotency_stubs.up.sql`.

pub mod correlation;
pub mod event;
pub mod request;

pub use correlation::CorrelationId;
pub use event::ProcessedEvents;
pub use request::{IdempotencyError, IdempotencyKey, IdempotencyKeys, IdempotencyOutcome};
