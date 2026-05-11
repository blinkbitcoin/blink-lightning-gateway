//! Inbound API surface.
//!
//! - `graphql/` (Story 1.4): the federation v2 subgraph hosting
//!   `lnInvoiceCreate` (and, in Story 5.1, the rest of the 27 LN ops).
//! - `grpc.rs` (Story 1.5): `LightningPaymentGatewayService`, the
//!   server-streaming `SubscribeEvents` RPC Symphony consumes.
//! - `error.rs` (Story 1.5): central `From<...> for tonic::Status`
//!   mapping. Per CLAUDE.md no other module constructs `tonic::Status`
//!   directly.

pub mod error;
pub mod graphql;
pub mod grpc;
