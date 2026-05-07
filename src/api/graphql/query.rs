//! GraphQL `Query` root. Federation v2's `_service { sdl }` field is
//! auto-handled by async-graphql 7.0's federation feature; Slice 1a has no
//! other queries. The empty `Query` struct is required to satisfy
//! `Schema<Query, Mutation, Subscription>`'s type slot.

use async_graphql::Object;

pub struct Query;

#[Object]
impl Query {
    /// Health probe — always `"ok"`. Federation `_service` is generated
    /// automatically; this gives a non-federation client a smoke point.
    async fn ping(&self) -> &'static str {
        "ok"
    }
}
