//! GraphQL subgraph server bootstrap (federation v2 via `async-graphql` +
//! `axum`).
//!
//! Slice 1's surface only hosts `lnInvoiceCreate` (and the type system
//! the schema needs). Apollo Router JWT validation middleware is NOT
//! wired here — `config.jwks_url` is carried in the YAML so Story 2.2
//! can adopt it without re-touching this file.

use std::net::SocketAddr;

use async_graphql_axum::GraphQL;
use axum::{routing::post_service, Router};
use tokio_util::sync::CancellationToken;

use ::tracing::info;

use crate::api::graphql::build_schema;
use crate::app::App;
use crate::server::config::SubgraphServerConfig;
use crate::server::error::ServerError;

pub async fn run_graphql_server(
    config: SubgraphServerConfig,
    app: App,
    cancel: CancellationToken,
) -> Result<(), ServerError> {
    let schema = build_schema(app);
    let router = Router::new().route("/graphql", post_service(GraphQL::new(schema)));

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(
        port = config.port,
        jwks_url = %config.jwks_url,
        "starting GraphQL subgraph server"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;

    info!("GraphQL subgraph server exited");
    Ok(())
}
