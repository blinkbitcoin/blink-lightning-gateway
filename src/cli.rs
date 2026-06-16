//! Binary entrypoint: clap-based CLI + supervisor that boots the gRPC,
//! GraphQL, and HTTP health servers and coordinates graceful shutdown.

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

use ::tracing::{error, info, warn};

use job::{JobId, JobSvcConfig, Jobs};

use crate::api::grpc::LightningPaymentGatewayService;
use crate::app::{App, InvoiceUpdateDispatcher};
use crate::cli::config::{Config, EnvOverride};
use crate::job::invoice_reconciliation_sweep::InvoiceReconciliationSweepInitializer;
use crate::job::invoice_subscription_recovery_sweep::InvoiceSubscriptionRecoverySweepInitializer;
use crate::job::orphan_hold_sweep::OrphanHoldSweepInitializer;
use crate::lightning_payment_gateway::lightning_payment_gateway_server::LightningPaymentGatewayServer;
use crate::lnd::{subscribe_payments, InvoiceUpdate, LndApi, LndClient, LndConfig};
use crate::outbox::EventPublisher;
use crate::symphony::{LightningSymphonyClient, SymphonyClient};
use crate::wallet::{ApolloRouterOwnershipChecker, WalletOwnershipChecker};

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

fn report_exit(send: &tokio::sync::mpsc::Sender<anyhow::Result<()>>, result: anyhow::Result<()>) {
    use tokio::sync::mpsc::error::TrySendError;
    match send.try_send(result) {
        Ok(()) => {}
        Err(TrySendError::Full(dropped)) => warn!(
            dropped = ?dropped,
            "supervisor channel full; dropped exit reason"
        ),
        Err(TrySendError::Closed(_)) => {}
    }
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.pg_con.trim().is_empty() {
        anyhow::bail!("PG_CON / --pg-con must not be empty");
    }

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

    // LND adapter. When `config.lnd` is `Some`, open a real mTLS+macaroon
    // tonic channel; when `None`, fall back to the boot stub so the gRPC
    // `SubscribeEvents` surface and HTTP health probes stay serviceable
    // for audiences that don't depend on LND.
    let lnd_client = match &config.lnd {
        Some(lnd_cfg) => match LndClient::connect(lnd_cfg.clone()).await {
            Ok(client) => {
                info!(address = %lnd_cfg.address, "Connected to LND");
                client
            }
            Err(e) => {
                anyhow::bail!("failed to connect to LND at {}: {e}", lnd_cfg.address);
            }
        },
        None => {
            warn!(
                "No `lnd:` block in config; using LndClient boot stub. \
                 lnInvoiceCreate / lnInvoicePaymentSend will return LndError::Stub."
            );
            LndClient::boot_stub(LndConfig::stub())
        }
    };
    let lnd: Arc<dyn LndApi> = Arc::new(lnd_client.clone());

    let outbox = EventPublisher::new(&pool);

    // Symphony spend-authorization client. A gateway that cannot
    // reach its accounting system must NOT boot into a silently-approving
    // state, so an unset/invalid endpoint is a hard boot failure.
    // The channel connects lazily — a Symphony outage at runtime declines
    // payments via the AuthorizeSpend fail-closed path.
    config
        .symphony
        .validate()
        .map_err(|e| anyhow::anyhow!("symphony config invalid: {e}"))?;
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::connect_lazy(
        &config.symphony.grpc_endpoint,
    )?);

    // Cross-subgraph wallet-ownership checker
    let ownership: Arc<dyn WalletOwnershipChecker> =
        Arc::new(ApolloRouterOwnershipChecker::new(&config.wallet_ownership));

    let cancel = CancellationToken::new();

    // invoice-update dispatcher + consumer task. The dispatcher owns
    // the LND handle + the shared mpsc Sender; threaded into `App::new`
    // so `App::create_invoice` spawns a per-hash `subscribe_invoice`
    // listener at invoice-creation time, and the recovery sweep re-spawns
    // listeners for any open invoice at boot.
    let (invoice_update_tx, mut invoice_update_rx) =
        tokio::sync::mpsc::channel::<InvoiceUpdate>(64);
    let invoice_dispatcher =
        InvoiceUpdateDispatcher::new(lnd_client.clone(), invoice_update_tx, cancel.clone());

    let app = App::new(
        pool.clone(),
        lnd,
        outbox,
        symphony,
        ownership,
        invoice_dispatcher.clone(),
    );

    let jobs_config = JobSvcConfig::builder()
        .pool(pool.clone())
        .exec_migrations(false)
        .build()
        .map_err(|e| anyhow::anyhow!("build JobSvcConfig: {e}"))?;
    let mut jobs = Jobs::init(jobs_config).await?;

    // Initialize scheduled jobs.
    let (recovery_sweep_spawner, reconciliation_sweep_spawner, orphan_hold_sweep_spawner) =
        if lnd_client.is_connected() {
            let recovery = jobs.add_initializer(InvoiceSubscriptionRecoverySweepInitializer::new(
                app.clone(),
                invoice_dispatcher.clone(),
            ));
            let reconciliation =
                jobs.add_initializer(InvoiceReconciliationSweepInitializer::new(app.clone()));
            let orphan_hold = jobs.add_initializer(OrphanHoldSweepInitializer::new(app.clone()));
            (Some(recovery), Some(reconciliation), Some(orphan_hold))
        } else {
            warn!(
                "LND boot-stub mode: skipping invoice_subscription_recovery_sweep \
                 + invoice_reconciliation_sweep + orphan_hold_sweep registration"
            );
            (None, None, None)
        };

    jobs.start_poll().await.context("Jobs::start_poll")?;

    if let Some(spawner) = recovery_sweep_spawner {
        spawner
            .spawn_unique(JobId::new(), ())
            .await
            .context("spawn_unique invoice_subscription_recovery_sweep")?;
    }
    if let Some(spawner) = reconciliation_sweep_spawner {
        spawner
            .spawn_unique(JobId::new(), ())
            .await
            .context("spawn_unique invoice_reconciliation_sweep")?;
    }
    if let Some(spawner) = orphan_hold_sweep_spawner {
        spawner
            .spawn_unique(JobId::new(), ())
            .await
            .context("spawn_unique orphan_hold_sweep")?;
    }

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
            report_exit(&send, result);
        }));
    }

    // GraphQL subgraph task.
    {
        let send = send.clone();
        let app = app.clone();
        let cancel = cancel.clone();
        let mut graphql_config = config.subgraph_server.clone();
        graphql_config.pg_config = config.db.pg_con.clone();
        info!("Starting blink-lightning-gateway GraphQL subgraph server");
        handles.push(tokio::spawn(async move {
            let result = crate::server::run_graphql_server(graphql_config, app, cancel)
                .await
                .map_err(anyhow::Error::from)
                .context("GraphQL subgraph server error");
            report_exit(&send, result);
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
            report_exit(&send, result);
        }));
    }

    // Drains InvoiceUpdates from the per-hash listeners into
    // `handle_invoice_update`. Supervisor-tracked: a panic, or every
    // listener Sender dropping, surfaces as a supervised exit so the
    // binary restarts rather than going silently dark.
    {
        let consumer_cancel = cancel.clone();
        let app_for_consumer = app.clone();
        let send = send.clone();
        info!("Starting invoice-update consumer task");
        handles.push(tokio::spawn(async move {
            let outcome: anyhow::Result<()> = loop {
                tokio::select! {
                    _ = consumer_cancel.cancelled() => break Ok(()),
                    update = invoice_update_rx.recv() => {
                        match update {
                            Some(update) => {
                                if let Err(e) =
                                    app_for_consumer.handle_invoice_update(update).await
                                {
                                    error!(error = %e, "handle_invoice_update returned error");
                                }
                            }
                            None => break Err(anyhow::anyhow!(
                                "invoice-update consumer exited: every listener Sender dropped"
                            )),
                        }
                    }
                }
            };
            // Cancellation is the expected exit; anything else means the
            // subsystem went dark.
            let result = if consumer_cancel.is_cancelled() {
                Ok(())
            } else {
                outcome
            };
            report_exit(&send, result);
        }));
    }

    // LND payment-subscription task. Forwards
    // `Router/TrackPayments` events into `App::handle_payment_update`.
    // The producer reports its exit reason to the supervisor channel
    // (matching the gRPC/GraphQL/health tasks) so the supervisor knows
    // when the subscription has gone dark — without that signal the
    // gateway could keep serving `lnInvoicePaymentSend` while no
    // subscription reconciles payments to terminal state.
    {
        let cancel = cancel.clone();
        let send = send.clone();
        let app = app.clone();
        let lnd_for_sub = lnd_client.clone();
        let (update_tx, mut update_rx) = tokio::sync::mpsc::channel(64);
        let cancel_for_sub = cancel.clone();
        info!("Starting LND payment-subscription task");
        handles.push(tokio::spawn(async move {
            let consumer_cancel = cancel_for_sub.clone();
            let app_for_consumer = app.clone();
            let consumer = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = consumer_cancel.cancelled() => return,
                        update = update_rx.recv() => {
                            let Some(update) = update else { return; };
                            if let Err(e) = app_for_consumer.handle_payment_update(update).await {
                                warn!(error = %e, "handle_payment_update returned error");
                            }
                        }
                    }
                }
            });
            let producer_result = subscribe_payments(lnd_for_sub, update_tx, cancel_for_sub).await;
            let _ = consumer.await;
            // Translate the producer's outcome into the supervisor's
            // channel. Cancellation is the expected exit; any other
            // outcome — even Ok(()) — flags the subscription has gone
            // dark and the binary should restart so kube reconnects.
            let result =
                if cancel.is_cancelled() {
                    Ok(())
                } else {
                    match producer_result {
                        Ok(()) => Err(anyhow::anyhow!(
                            "LND payment-subscription producer exited without cancellation"
                        )),
                        Err(e) => Err(anyhow::Error::from(e)
                            .context("LND payment-subscription producer error")),
                    }
                };
            report_exit(&send, result);
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
            let result: anyhow::Result<()> = async {
                wait_for_shutdown_signal()
                    .await
                    .context("install shutdown signal handler")?;
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
                Ok(())
            }
            .await;
            report_exit(&send, result);
        }));
    }
    drop(send);

    // Supervisor: the first task to post an exit reason wins. After
    // capturing the reason, cancel the token (a no-op if the signal
    // handler already cancelled) and let the remaining tasks drain
    // within `drain_deadline`. Abort only if a task does not exit
    // within that window — `handle.abort()` short-circuits tonic's
    // and axum's graceful-shutdown drain.
    let reason = match receive.recv().await {
        Some(r) => r,
        None => anyhow::bail!("all server tasks exited without reporting status"),
    };
    cancel.cancel();
    let drain_deadline =
        Duration::from_secs(config.grpc_server.shutdown_grace_secs) + Duration::from_secs(5);
    for mut handle in handles {
        let abort = handle.abort_handle();
        if tokio::time::timeout(drain_deadline, &mut handle)
            .await
            .is_err()
        {
            warn!(
                deadline_secs = drain_deadline.as_secs(),
                "server task did not drain within deadline; aborting"
            );
            abort.abort();
        }
    }

    // Explicit graceful shutdown of the job poller
    if let Err(e) = jobs.shutdown().await {
        warn!(error = %e, "Jobs::shutdown returned error");
    }

    reason
}

async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("install ctrl_c handler")?;
        info!("ctrl_c received");
        Ok(())
    }
}
