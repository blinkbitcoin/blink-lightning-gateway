//! GraphQL subgraph (federation v2 via async-graphql 7.0). Slice 1a only
//! implements `lnInvoiceCreate`; the other 26 operations land in Story
//! 5.1. Resolvers hold no business logic — they only call into `App`
//! (architecture L348).

pub mod mutation;
pub mod query;
pub mod schema;
pub mod types;

pub use mutation::Mutation;
pub use query::Query;
pub use schema::{build_schema, GatewaySchema};
