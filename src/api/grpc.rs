//! gRPC inbound surface.
//!
//! - `service.rs`: `LightningPaymentGatewayService` — the server-streaming
//!   `SubscribeEvents` RPC Symphony consumes.
//! - `error.rs`: central `From<...> for tonic::Status` mapping. Per
//!   CLAUDE.md no other module constructs `tonic::Status` directly.

mod error;
mod service;

pub use service::LightningPaymentGatewayService;
