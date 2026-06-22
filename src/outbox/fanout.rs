//! In-process broadcast fanout over the `gateway_events` LISTEN channel.
//!
//! One ingest task drives a single [`ListenConnection`], reads each notified
//! row ONCE, and broadcasts the hydrated event (behind an `Arc`) to N
//! concurrent GraphQL subscribers — so they share ONE Postgres `LISTEN` AND
//! one read per event, instead of each opening its own `LISTEN` and re-reading
//! every row (bria's `Outbox` shape, `../bria/src/outbox/mod.rs`). Each
//! subscriber filters the broadcast by `reference_id` in memory (no live-path
//! read) and falls back to [`EventPublisher`] backfill only for resume and
//! lag recovery — see `crate::api::graphql::subscription`. A subscriber that
//! overflows the broadcast buffer (`Lagged`) re-backfills from its cursor, the
//! same recovery the gRPC `subscription_loop` does on a LISTEN drop.

use std::sync::Arc;

use ::tracing::{error, info, warn};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::{EventPublisher, ListenConnection, OutboxError, OutboxEvent};

/// Broadcast buffer depth. Matched to the gRPC path's `CHANNEL_SIZE`
/// (`api/grpc/service.rs:25`) so the lag threshold is the same across both
/// streaming surfaces.
const BROADCAST_CAPACITY: usize = 1000;

/// Shared handle injected into the GraphQL schema as `.data(...)`. Cloning
/// is cheap (an `EventPublisher` pool handle + a broadcast `Sender` + the
/// cancellation token); every clone fans out from the same ingest task. The
/// broadcast carries `Arc<OutboxEvent>` so fan-out to N receivers is a
/// refcount bump, not N deep clones.
#[derive(Clone)]
pub struct OutboxFanout {
    publisher: EventPublisher,
    sender: broadcast::Sender<Arc<OutboxEvent>>,
    cancel: CancellationToken,
}

impl OutboxFanout {
    /// Build the fanout and spawn its single ingest task. The task lives
    /// until `cancel` fires; subscribers attach via [`Self::subscribe`].
    /// Errors if `listen_conn` has no usable connection string.
    pub fn start(
        publisher: EventPublisher,
        listen_conn: ListenConnection,
        cancel: CancellationToken,
    ) -> Result<Self, OutboxError> {
        listen_conn.validate()?;
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);

        let ingest_publisher = publisher.clone();
        let ingest_sender = sender.clone();
        let ingest_cancel = cancel.clone();
        tokio::spawn(async move {
            Self::ingest_loop(listen_conn, ingest_publisher, ingest_sender, ingest_cancel).await;
        });

        Ok(Self {
            publisher,
            sender,
            cancel,
        })
    }

    /// Backfill primitives (`fetch_after_batch` / `find_by_sequence` /
    /// `count_after`) for a subscriber's gap-fill and resume reads.
    pub fn publisher(&self) -> &EventPublisher {
        &self.publisher
    }

    /// Process-wide shutdown signal — per-subscriber streams watch it so
    /// they tear down on server shutdown even while idle on `recv`.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Attach a new live-tail receiver. A subscriber that lags past
    /// `BROADCAST_CAPACITY` sees `RecvError::Lagged` and recovers by
    /// re-backfilling from its cursor.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<OutboxEvent>> {
        self.sender.subscribe()
    }

    async fn ingest_loop(
        listen_conn: ListenConnection,
        publisher: EventPublisher,
        sender: broadcast::Sender<Arc<OutboxEvent>>,
        cancel: CancellationToken,
    ) {
        let mut notifications = listen_conn.start_listening(cancel.clone());
        info!("OutboxFanout ingest task started");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("OutboxFanout ingest: cancellation requested, stopping");
                    return;
                }
                notification = notifications.recv() => {
                    match notification {
                        Some(Ok(sequence)) => {
                            // Read the row ONCE here and broadcast it, so N
                            // subscribers cost one DB read per event, not N.
                            // Subscribers filter by `reference_id` in memory.
                            match publisher.find_by_sequence(sequence).await {
                                Ok(Some(event)) => {
                                    // No active receivers → `SendError`; harmless.
                                    // The row is durable in `outbox_events`, so a
                                    // subscriber that attaches later backfills it.
                                    let _ = sender.send(Arc::new(event));
                                }
                                // `pg_notify` fires after commit so the row should
                                // be visible; a transient miss self-heals via a
                                // subscriber's `Lagged`/resume re-backfill.
                                Ok(None) => warn!(
                                    sequence,
                                    "OutboxFanout ingest: event not found for notification"
                                ),
                                Err(e) => error!(
                                    sequence,
                                    error = %e,
                                    "OutboxFanout ingest: failed to fetch event"
                                ),
                            }
                        }
                        // `start_listening` reconnects internally with backoff; any
                        // sequences missed during the gap are recovered by a
                        // subscriber's `Lagged`/cursor re-backfill (the same shape
                        // the gRPC loop uses on `ListenDisconnected`).
                        Some(Err(OutboxError::ListenDisconnected)) => {
                            warn!("OutboxFanout ingest: LISTEN connection lost; reconnecting");
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "OutboxFanout ingest: notification error");
                        }
                        None => {
                            info!("OutboxFanout ingest: notification stream ended, stopping");
                            return;
                        }
                    }
                }
            }
        }
    }
}
