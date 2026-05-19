//! Inbound API surface.
//!
//! - `graphql/` (Story 1.4): the federation v2 subgraph hosting
//!   `lnInvoiceCreate` (and, in Story 5.1, the rest of the 27 LN ops).
//! - `grpc/` (Story 1.5): `LightningPaymentGatewayService` + central
//!   `tonic::Status` mapping. The `SubscribeEvents` RPC Symphony
//!   consumes lives here.

pub mod graphql;
pub mod grpc;
