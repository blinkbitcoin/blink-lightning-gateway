//! Binary entrypoint: clap-based CLI + supervisor that boots the gRPC,
//! GraphQL, and HTTP health servers and coordinates graceful shutdown.
//!
//! Shape mirrors `blink-card/src/cli/mod.rs:1-238`:
//!   - clap derive `Cli` with `--config` + `--pg-con` + an optional
//!     `migrate` subcommand.
//!   - The default (no subcommand) runs `run_cmd`, which spawns each
//!     server task and uses an `mpsc::channel(1)` to capture the first
//!     exit reason — "first task to exit wins" pattern.
//!
//! Two deliberate divergences from blink-card:
//!   1. The gateway adds an explicit SIGTERM-aware shutdown handler
//!      that flips `tonic_health::HealthReporter::set_not_serving`
//!      BEFORE cancelling the shutdown token, with a configurable grace
//!      sleep between them. blink-card relies on the LB removing the
//!      pod from rotation fast enough; the gateway hosts long-lived
//!      `SubscribeEvents` streams, so the ordering matters.
//!   2. The CLI has no `RAIN_API_KEY`/`VISA_API_KEY`/`BLINK_CORE_API_KEY`
//!      env-var plumbing. The gateway talks to no third-party services
//!      directly; Symphony does that on its own behalf. Future stories
//!      add per-adapter flags as real upstreams arrive.

pub mod config;
pub mod db;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};

use ::tracing::{info, warn};

use crate::api::grpc::LightningPaymentGatewayService;
use crate::app::App;
use crate::cli::config::{Config, EnvOverride};
use crate::lightning_payment_gateway::lightning_payment_gateway_server::LightningPaymentGatewayServer;
use crate::lnd::{LndApi, LndClient, LndConfig};

#[derive(Parser)]
#[clap(long_about = None)]
struct Cli {
    #[clap(
        short,
        long,
        env = "BLINK_LIGHTNING_GATEWAY_CONFIG",
        default_value = "ln-gateway.yml",
        value_name = "FILE"
    )]
    config: PathBuf,

    #[clap(long, env = "PG_CON")]
    pg_con: String,

    #[clap(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run database migrations and exit.
    Migrate,
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = Config::from_path(
        &cli.config,
        EnvOverride {
            pg_con: cli.pg_con.clone(),
        },
    )?;

    if let Some(tracing_config) = &config.tracing {
        crate::tracing::init_tracer(tracing_config.clone())?;
    } else {
        crate::tracing::init_fmt_subscriber()?;
        info!("No otel endpoint configured; using fmt tracing-subscriber.");
    }

    match cli.command {
        Some(Commands::Migrate) => migrate_cmd(config).await,
        None => run_cmd(config).await,
    }
}

async fn migrate_cmd(config: Config) -> anyhow::Result<()> {
    info!("Running database migrations...");
    db::run_migrations(&config.db).await?;
    info!("Database migrations completed successfully.");
    Ok(())
}

async fn run_cmd(config: Config) -> anyhow::Result<()> {
    let pool = db::init_pool(&config.db).await?;

    // Boot stub for the LND adapter. The gRPC `SubscribeEvents` surface
    // (Symphony's audience) and the HTTP health probes do not depend on
    // LND, so the gateway is fully serviceable for those audiences. The
    // GraphQL `lnInvoiceCreate` mutation will return `LndError::Stub`
    // until Story 2.2 wires the real tonic-channel-backed connection.
    let lnd: Arc<dyn LndApi> = Arc::new(LndClient::boot_stub(LndConfig::stub()));
    warn!(
        "LndClient is a boot stub — lnInvoiceCreate will return LndError::Stub \
         until Story 2.2 wires real LND."
    );

    let app = App::new(pool.clone(), lnd);

    let cancel = CancellationToken::new();
    let (send, mut receive) = tokio::sync::mpsc::channel::<anyhow::Result<()>>(1);
    let mut handles = Vec::new();

    // Construct the tonic-health reporter + server pair once. The
    // reporter handle is shared between the gRPC server (which carries
    // the `HealthServer` service) and the SIGTERM handler (which flips
    // the registered services to `NotServing` ahead of the token
    // cancel). Initial state is `Serving` for the gateway service.
    let health_reporter = HealthReporter::new();
    health_reporter
        .set_serving::<LightningPaymentGatewayServer<LightningPaymentGatewayService>>()
        .await;
    let health_service = HealthService::from_health_reporter(health_reporter.clone());
    let health_server = HealthServer::new(health_service);

    // gRPC server task.
    {
        let mut grpc_config = config.grpc_server.clone();
        grpc_config.pg_config = config.db.pg_con.clone();
        let send = send.clone();
        let pool = pool.clone();
        let cancel = cancel.clone();
        info!("Starting blink-lightning-gateway gRPC server");
        handles.push(tokio::spawn(async move {
            let result = crate::server::run_grpc_server(grpc_config, pool, cancel, health_server)
                .await
                .map_err(anyhow::Error::from)
                .context("gRPC server error");
            let _ = send.try_send(result);
        }));
    }

    // GraphQL subgraph task.
    {
        let send = send.clone();
        let app = app.clone();
        let cancel = cancel.clone();
        let graphql_config = config.subgraph_server.clone();
        info!("Starting blink-lightning-gateway GraphQL subgraph server");
        handles.push(tokio::spawn(async move {
            let result = crate::server::run_graphql_server(graphql_config, app, cancel)
                .await
                .map_err(anyhow::Error::from)
                .context("GraphQL subgraph server error");
            let _ = send.try_send(result);
        }));
    }

    // HTTP health probe task.
    {
        let send = send.clone();
        let pool = pool.clone();
        let cancel = cancel.clone();
        let port = config.health_server.port;
        info!("Starting blink-lightning-gateway HTTP health server");
        handles.push(tokio::spawn(async move {
            let result = crate::health::run(pool, port, cancel)
                .await
                .map_err(anyhow::Error::from)
                .context("HTTP health server error");
            let _ = send.try_send(result);
        }));
    }

    // SIGTERM / SIGINT handler: flips gRPC health to NotServing, sleeps
    // the configured grace window, then cancels the shutdown token. The
    // ordering matches Kubernetes' expectation that a pod marked
    // NotServing first stops receiving new traffic and then has its
    // in-flight work drained. See AC11 in story 2.1 for the rationale.
    {
        let send = send.clone();
        let cancel = cancel.clone();
        let reporter = health_reporter.clone();
        let grace = Duration::from_secs(config.grpc_server.shutdown_grace_secs);
        handles.push(tokio::spawn(async move {
            wait_for_shutdown_signal().await;
            info!("Shutdown signal received; flipping gRPC health to NotServing");
            reporter
                .set_not_serving::<LightningPaymentGatewayServer<LightningPaymentGatewayService>>()
                .await;
            info!(
                grace_secs = grace.as_secs(),
                "Draining: sleeping grace window before cancelling token"
            );
            tokio::time::sleep(grace).await;
            info!("Grace window elapsed; cancelling shutdown token");
            cancel.cancel();
            let _ = send.try_send(Ok(()));
        }));
    }
    drop(send);

    // Supervisor: the first task to post an exit reason wins. After
    // capturing the reason, cancel the token (a no-op if the signal
    // handler already cancelled) and abort the remaining handles.
    let reason = receive
        .recv()
        .await
        .expect("supervisor channel closed without a message");
    cancel.cancel();
    for handle in handles {
        handle.abort();
    }
    reason
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl_c handler");
        info!("ctrl_c received");
    }
}
