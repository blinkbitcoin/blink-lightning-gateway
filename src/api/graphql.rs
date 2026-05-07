//! GraphQL subgraph (federation v2 via async-graphql 7.0). Slice 1a hosts
//! the `lnInvoiceCreate` mutation only; the rest of the 27-op surface
//! arrives via Story 5.1's cookie-cutter expansion. Resolver routes to the
//! `App` coordinator with no business logic in the resolver itself
//! (architecture L348).

pub mod mutation;
pub mod query;
pub mod schema;
pub mod types;

pub use mutation::Mutation;
pub use query::Query;
pub use schema::{build_schema, GatewaySchema};
