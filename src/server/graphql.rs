//! GraphQL subgraph server.
//!
//! The POST handler validates the caller JWT against the JWKS and injects a
//! [`CallerAuth`] on success.

use std::net::SocketAddr;
use std::sync::Arc;

use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::http::{header::AUTHORIZATION, HeaderMap};
use axum::{routing::post, Extension, Router};
use tokio_util::sync::CancellationToken;

use ::tracing::{info, warn};

use crate::api::graphql::{build_schema, GatewaySchema};
use crate::app::App;
use crate::server::config::SubgraphServerConfig;
use crate::server::error::ServerError;
use crate::server::jwks::RemoteJwksDecoder;
use crate::wallet::CallerAuth;

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

async fn graphql_handler(
    Extension(schema): Extension<GatewaySchema>,
    Extension(decoder): Extension<Arc<RemoteJwksDecoder>>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut request = req.into_inner();

    if let Some(token) = bearer_token(&headers) {
        match decoder.decode(token) {
            Ok(claims) => {
                request = request.data(CallerAuth::new(token.to_owned(), claims.sub));
            }
            // Invalid token: don't trust it — proceed without CallerAuth so
            // wallet-targeted ops fail closed (a valid token is required).
            Err(e) => warn!(error = %e, "caller JWT failed validation; proceeding unauthenticated"),
        }
    }

    schema.execute(request).await.into()
}

pub async fn run_graphql_server(
    config: SubgraphServerConfig,
    app: App,
    cancel: CancellationToken,
) -> Result<(), ServerError> {
    let schema = build_schema(app);

    let decoder = Arc::new(RemoteJwksDecoder::new(config.jwks_url.clone()));
    {
        let decoder = decoder.clone();
        tokio::spawn(async move { decoder.refresh_keys_periodically().await });
    }

    let router = Router::new()
        .route("/graphql", post(graphql_handler))
        .layer(Extension(schema))
        .layer(Extension(decoder));

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
