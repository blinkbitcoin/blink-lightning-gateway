//! Inbound API surface. Slice 1a hosts the GraphQL federation v2 subgraph
//! with `lnInvoiceCreate` only. The gRPC `SubscribeEvents` server arrives
//! in Story 1.5 (consumer flow) along with `src/api/error.rs`'s central
//! `tonic::Status` mapping.

pub mod graphql;
