//! GraphQL subgraph (federation v2 via async-graphql 7.0). Resolvers hold
//! no business logic — they only call into `App` (architecture L348).
//! Mutations/queries are request/response; the `Subscription` root
//! (Slice 6) streams `lnInvoicePaymentStatus*`. Slice 1a's `lnInvoiceCreate`
//! plus the remaining ops land across Stories 2.x–5.1.

pub mod mutation;
pub mod query;
pub mod schema;
pub mod subscription;
pub mod types;

pub use mutation::Mutation;
pub use query::Query;
pub use schema::{build_schema, build_schema_with_fanout, GatewaySchema};
pub use subscription::{ResumeSequence, Subscription};
