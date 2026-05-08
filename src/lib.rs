//! blink-ln-gateway library crate.
//!
//! Bria-style flat per-bounded-context layout. See ADR #1 (filed by Story 1.3
//! at `_bmad-output/decisions/0001-ddd-bria-pattern-fidelity.md`) for the full
//! rationale. Module bodies land in their respective slice stories
//! (Epic 2 onwards); this scaffold only declares them.

// Bounded contexts (event-sourced via es-entity)
//
// `profile/` was REJECTED per architecture.md L183 (gaps table) — no tenant
// model in C2-Discovery scope; Profile would land if/when external trust
// boundaries arrive (C2-Production or beyond). The architecture's L824 tree
// listing was stale; the rejection wins.
pub mod htlc;
pub mod invoice;
pub mod payment;

// Domain modules shared by every bounded context (invoice, payment, htlc
// all use these — value objects and fee math).
pub mod fees;
pub mod primitives;

// Infrastructure shared by every bounded context (idempotency keys,
// background-job runner, pg_notify event outbox).
pub mod idempotency;
pub mod job;
pub mod outbox;

// External-system adapters (NO `cala` adapter — gateway never talks to Cala
// directly per ADR #2)
pub mod lnd;
pub mod symphony;

// Inbound API
pub mod api;

// Application coordinator (single coordinator module, NOT folder of services
// per architecture and ADR #1)
pub mod app;

// Server / lifecycle / dev
pub mod cli;
pub mod config;
pub mod health;
pub mod server;

// Top-level scaffold (next to `lib.rs` and `main.rs`)
pub mod dev_constants;
pub mod scope;
pub mod tracing;
