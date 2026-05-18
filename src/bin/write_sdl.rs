//! SDL printer for the GraphQL subgraph. Run via `cargo run --bin
//! write_sdl > schema.graphql` and diff the `lnInvoiceCreate`-relevant
//! lines against
//! `blink/core/api/src/graphql/public/schema.graphql:633-672,972` per
//! Story 1.4 AC9'. Federation composition CI gate lands in Story 5.3.
//!
//! Slice 1a builds the schema with a stub `App` so the SDL output is a
//! pure schema dump, free of any runtime concerns. The pool used here is
//! never connected — `App` only reaches it through resolvers that this bin
//! never executes.

use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

use async_trait::async_trait;
use blink_lightning_gateway::api::graphql::build_schema;
use blink_lightning_gateway::app::App;
use blink_lightning_gateway::lnd::{
    AddInvoiceParams, AddInvoiceResponse, FeeProbeParams, FeeProbeResponse, LndApi, LndError,
    SendPaymentParams, SendPaymentResponse,
};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};

struct StubLnd;

#[async_trait]
impl LndApi for StubLnd {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        Err(LndError::Stub)
    }

    async fn send_payment(
        &self,
        _params: SendPaymentParams,
    ) -> Result<SendPaymentResponse, LndError> {
        Err(LndError::Stub)
    }

    async fn fee_probe(&self, _params: FeeProbeParams) -> Result<FeeProbeResponse, LndError> {
        Err(LndError::Stub)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Lazy pool — never connects, never queries. The schema build only
    // calls `Schema::data(app).finish()`; no resolver fires here.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://stub:stub@localhost/stub")
        .expect("connect_lazy");

    let outbox = EventPublisher::new(&pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::new(""));
    let app = App::new(pool, Arc::new(StubLnd), outbox, symphony);
    let schema = build_schema(app);
    println!("{}", schema.sdl());
}
