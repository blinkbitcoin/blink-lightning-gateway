//! Symphony adapter. Per architecture line 946, every adapter has `client.rs` + `config.rs` + `error.rs`. Plus `grpc.rs` for the tonic transport (placeholder mTLS per ADR #4 — non-decision per NFR5). Real implementation lands in Story 1.4.

pub mod client;
pub mod config;
pub mod error;
pub mod grpc;
