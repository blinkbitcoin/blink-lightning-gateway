//! Federation v2 subgraph schema assembly. Wires `Query` + `Mutation` +
//! injects `App` as schema data so resolvers can access it via
//! `ctx.data::<App>()`.

use async_graphql::{EmptySubscription, Schema};

use super::{Mutation, Query};
use crate::app::App;

pub type GatewaySchema = Schema<Query, Mutation, EmptySubscription>;

pub fn build_schema(app: App) -> GatewaySchema {
    Schema::build(Query, Mutation, EmptySubscription)
        .enable_federation()
        .data(app)
        .finish()
}
