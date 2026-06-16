//! Federation v2 subgraph schema assembly. Wires `Query` + `Mutation` +
//! `Subscription` and injects `App` (and, for the live server, the
//! `OutboxFanout`) as schema data so resolvers read them via
//! `ctx.data::<...>()`.

use async_graphql::{Schema, SchemaBuilder};

use super::{Mutation, Query, Subscription};
use crate::app::App;
use crate::outbox::OutboxFanout;

pub type GatewaySchema = Schema<Query, Mutation, Subscription>;

fn schema_builder(app: App) -> SchemaBuilder<Query, Mutation, Subscription> {
    Schema::build(Query, Mutation, Subscription)
        .enable_federation()
        .data(app)
}

/// Schema without a fanout — for `write_sdl` (no resolver fires) and any
/// caller that does not exercise subscriptions. The 3 subscription ops are
/// still present in the SDL because `Subscription` is in the type params.
pub fn build_schema(app: App) -> GatewaySchema {
    schema_builder(app).finish()
}

/// Schema with the `OutboxFanout` injected so subscription resolvers can
/// live-tail the outbox. Used by `run_graphql_server` and the synthetic E2E.
pub fn build_schema_with_fanout(app: App, fanout: OutboxFanout) -> GatewaySchema {
    schema_builder(app).data(fanout).finish()
}
