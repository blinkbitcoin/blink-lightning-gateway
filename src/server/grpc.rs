//! gRPC server bootstrap.
//!
//! Adapts `blink-card/src/server/mod.rs:222-281` with two deliberate
//! changes that close Story-1.5-review gaps:
//!   1. `serve_with_shutdown(addr, cancel.cancelled())` so the tonic
//!      listener stops accepting new connections when `cancel` fires
//!      (blink-card uses `serve(addr)` and only the outbox subscriber
//!      loops see cancel — the listener keeps accepting).
//!   2. The `tonic_health::HealthReporter` + `HealthServer` are owned
//!      by the caller (`src/cli.rs::run_cmd`). The caller constructs
//!      the (reporter, service) pair, calls `set_serving::<...>()`,
//!      passes the `HealthServer` here for `add_service`, and keeps
//!      the reporter so the SIGTERM handler can flip `set_not_serving`
//!      ahead of the cancel.

use std::net::SocketAddr;
use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::HealthService;

use ::tracing::info;

use crate::api::grpc::LightningPaymentGatewayService;
use crate::lightning_payment_gateway::lightning_payment_gateway_server::LightningPaymentGatewayServer;
use crate::server::config::GrpcServerConfig;
use crate::server::error::ServerError;

pub async fn run_grpc_server(
    config: GrpcServerConfig,
    pool: PgPool,
    cancel: CancellationToken,
    health_server: HealthServer<HealthService>,
) -> Result<(), ServerError> {
    let service =
        LightningPaymentGatewayService::new(pool, config.pg_config.clone(), cancel.clone())?;
    let gateway_server = LightningPaymentGatewayServer::new(service);

    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
    info!(
        port = config.port,
        keepalive_interval_secs = config.keepalive_interval_secs,
        keepalive_timeout_secs = config.keepalive_timeout_secs,
        "starting gRPC server"
    );

    Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(config.keepalive_interval_secs)))
        .http2_keepalive_timeout(Some(Duration::from_secs(config.keepalive_timeout_secs)))
        .add_service(health_server)
        .add_service(gateway_server)
        .serve_with_shutdown(addr, cancel.cancelled())
        .await?;

    info!("gRPC server exited");
    Ok(())
}
