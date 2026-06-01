//! blink-lightning-gateway library crate.

pub mod invoice;
pub mod payment;

// Domain modules shared by every bounded context (invoice, payment both use
// these — value objects and fee math).
pub mod fees;
pub mod primitives;

// Infrastructure shared by every bounded context (background-job runner,
// pg_notify event outbox).
pub mod job;
pub mod outbox;

// External-system adapters (NO `cala` adapter — gateway never talks to Cala
// directly per ADR #2)
pub mod lnd;
pub mod symphony;

// Cross-subgraph wallet-ownership validation .Not a local Wallet
// projection — the gateway does not own the Wallet aggregate.
pub mod wallet;

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

// Generated proto modules. Compiled by `build.rs` from the files in
// `proto/` and dropped into `OUT_DIR`; we re-export them at crate root so
// downstream call sites read `crate::lightning_payment_gateway::*` and
// match the import shape in blink-card.
pub mod lightning_payment_gateway {
    tonic::include_proto!("lightning_payment_gateway");
}

pub mod symphony_proto {
    tonic::include_proto!("symphony");
}
