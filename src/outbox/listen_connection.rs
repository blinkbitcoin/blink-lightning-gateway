//! Long-lived `LISTEN gateway_events` connection with exponential reconnect.
//!
//! Ported from `blink-card/src/outbox/listen_connection.rs:1-195` modulo:
//!   - the LISTEN channel name (`gateway_events`, not `card_events`),
//!   - the `OutboxError::ListenDisconnected` variant lives in this gateway's
//!     error enum.
//!
//! Used exclusively by `crate::api::grpc::LightningPaymentGatewayService`'s
//! `subscription_loop`. Each gRPC `SubscribeEvents` invocation gets its own
//! `ListenConnection` and the consumer drives it to completion.
//!
//! `tokio_postgres::NoTls` is intentional: the LISTEN connection is a
//! separate Postgres session (sqlx's `runtime-tokio-rustls` only governs
//! sqlx pool sessions). Internal cluster traffic uses network-level trust
//! per ADR #4. If `pg_config` carries `sslmode=require`/`verify-...`, the
//! constructor logs an error and the connect call will fail at runtime —
//! deliberate so misconfiguration surfaces loudly instead of silently
//! falling back to plaintext.

use ::tracing::{error, info, warn};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use super::OutboxError;

const BASE_BACKOFF_MS: u64 = 1000;
const MAX_BACKOFF_MS: u64 = 30_000;

#[derive(Clone)]
pub struct ListenConnection {
    pg_config: String,
}

impl ListenConnection {
    pub fn new(pg_config: String) -> Self {
        let lower = pg_config.to_lowercase();
        if lower.contains("sslmode=require") || lower.contains("sslmode=verify") {
            error!(
                "LISTEN connection uses NoTls connector but pg_config requires TLS. \
                 Connection will fail. Remove sslmode or use network-level encryption."
            );
        } else if lower.contains("sslmode=prefer") {
            warn!(
                "LISTEN connection will not use TLS despite sslmode=prefer. \
                 Consider using network-level encryption (e.g., service mesh)."
            );
        }
        Self { pg_config }
    }

    pub fn validate(&self) -> Result<(), OutboxError> {
        if self.pg_config.is_empty() {
            return Err(OutboxError::Configuration(
                "pg_config is empty - LISTEN connection requires valid PostgreSQL connection string"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Spawn the connection task and return the receiver side of an
    /// unbounded channel of `Result<sequence, OutboxError>`. Spawning is
    /// eager so LISTEN is registered before the consumer's backfill begins;
    /// notifications that arrive during backfill buffer in the channel and
    /// the post-backfill loop drains them with the dedupe-on-sequence
    /// check.
    pub fn start_listening(
        &self,
        cancel_token: CancellationToken,
    ) -> mpsc::UnboundedReceiver<Result<i64, OutboxError>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let pg_config = self.pg_config.clone();

        tokio::spawn(async move {
            Self::connection_loop(pg_config, tx, cancel_token).await;
        });

        rx
    }

    async fn connection_loop(
        pg_config: String,
        tx: mpsc::UnboundedSender<Result<i64, OutboxError>>,
        cancel_token: CancellationToken,
    ) {
        let mut backoff_ms = BASE_BACKOFF_MS;

        loop {
            if cancel_token.is_cancelled() {
                info!("ListenConnection: cancellation requested, stopping");
                break;
            }

            let connect_result = tokio_postgres::connect(&pg_config, NoTls).await;

            let (client, mut connection) = match connect_result {
                Ok(conn) => {
                    backoff_ms = BASE_BACKOFF_MS;
                    conn
                }
                Err(e) => {
                    error!(error = %e, backoff_ms, "Failed to connect to PostgreSQL for LISTEN");
                    if tx.send(Err(OutboxError::ListenDisconnected)).is_err() {
                        info!("ListenConnection: receiver dropped, stopping");
                        return;
                    }
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    continue;
                }
            };

            let (notify_tx, mut notify_rx) = mpsc::unbounded_channel();

            let cancel_clone = cancel_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => {
                            break;
                        }
                        msg = std::future::poll_fn(|cx| connection.poll_message(cx)) => {
                            match msg {
                                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                                    if notify_tx.send(n).is_err() {
                                        break;
                                    }
                                }
                                Some(Ok(_)) => {}
                                Some(Err(e)) => {
                                    error!(error = %e, "Connection error");
                                    break;
                                }
                                None => {
                                    info!("Connection closed");
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            if let Err(e) = client.execute("LISTEN gateway_events", &[]).await {
                error!(error = %e, "Failed to execute LISTEN command");
                if tx.send(Err(OutboxError::ListenDisconnected)).is_err() {
                    return;
                }
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                continue;
            }

            info!("LISTEN gateway_events started successfully");

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("ListenConnection: cancellation requested during notification loop");
                        return;
                    }
                    notification = notify_rx.recv() => {
                        match notification {
                            Some(n) => {
                                match n.payload().parse::<i64>() {
                                    Ok(sequence) => {
                                        if tx.send(Ok(sequence)).is_err() {
                                            info!("ListenConnection: receiver dropped, stopping");
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        warn!(payload = %n.payload(), error = %e, "Failed to parse notification payload as sequence");
                                    }
                                }
                            }
                            None => {
                                warn!("Notification channel closed, reconnecting");
                                if tx.send(Err(OutboxError::ListenDisconnected)).is_err() {
                                    return;
                                }
                                break;
                            }
                        }
                    }
                }
            }

            sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
        }
    }
}
