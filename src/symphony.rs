//! Symphony-as-client adapter. Reserved for the synchronous gRPC calls
//! the gateway will make TO Symphony — first inhabitant lands in
//! Story 2.2 (`authorize_spend`-equivalent before LND `send_payment`).
//! See `client.rs` for the placeholder note.
//!
//! The gateway-as-server bootstrap (gRPC listen socket, transport config,
//! tonic-health, graceful drain) does NOT live here — it belongs in
//! `src/server/` per architecture L190 + epics.md L148, and lands with
//! Story 2.1.

pub mod client;
pub mod config;

pub use config::SymphonyConfig;
