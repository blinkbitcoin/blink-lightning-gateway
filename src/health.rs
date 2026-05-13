//! HTTP health probes for Kubernetes liveness / readiness checks.
//!
//! Three routes:
//!   - `GET /health/startup` — returns 200 once Postgres responds to a
//!     `SELECT 1` within 2s, else 503.
//!   - `GET /health/live`    — always 200 once the server is bound.
//!   - `GET /health/ready`   — same Postgres ping as startup.
//!
//! The gRPC `grpc.health.v1.Health/Check` service (different audience —
//! Symphony's gRPC client uses it) is registered in `src/server/grpc.rs`
//! via `tonic_health`. The HTTP probes here are what Kubernetes reads;
//! the gRPC health is what Symphony reads.
//!
//! The function signature deviates from AC10's literal
//! `(pool, port, health_reporter)` shape — the `tonic_health` reporter
//! is owned by the supervisor in `src/cli.rs::run_cmd` so the SIGTERM
//! ordering (flip gRPC `set_not_serving` → grace sleep → cancel token)
//! lives in one place. Passing the reporter into both `health::run` and
//! the supervisor would split the flip responsibility and re-introduce
//! the ordering ambiguity AC11 exists to remove.

use std::net::SocketAddr;
use std::time::Duration;

use axum::{http::StatusCode, routing::get, Router};
use sqlx::PgPool;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use ::tracing::{error, info};

use crate::server::error::ServerError;

const POSTGRES_PROBE_TIMEOUT: Duration = Duration::from_millis(2000);

async fn check_postgres(pool: &PgPool) -> StatusCode {
    match timeout(
        POSTGRES_PROBE_TIMEOUT,
        sqlx::query("SELECT 1").fetch_one(pool),
    )
    .await
    {
        Ok(Ok(_)) => StatusCode::OK,
        Ok(Err(e)) => {
            error!(error = %e, "health: Postgres probe failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
        Err(_) => {
            error!("health: Postgres probe timed out");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

pub async fn run(pool: PgPool, port: u16, cancel: CancellationToken) -> Result<(), ServerError> {
    let startup_pool = pool.clone();
    let ready_pool = pool.clone();

    let router = Router::new()
        .route(
            "/health/startup",
            get(move || {
                let pool = startup_pool.clone();
                async move { check_postgres(&pool).await }
            }),
        )
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route(
            "/health/ready",
            get(move || {
                let pool = ready_pool.clone();
                async move { check_postgres(&pool).await }
            }),
        );

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
    info!(port, "starting HTTP health probe server");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;

    info!("HTTP health probe server exited");
    Ok(())
}
