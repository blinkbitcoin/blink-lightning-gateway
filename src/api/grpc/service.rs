//! `LightningPaymentGatewayService` implements the
//! `LightningPaymentGateway` tonic trait. Single RPC: `SubscribeEvents`.
//!
//! Per subscriber:
//!   1. Start LISTEN immediately so notifications buffer while backfill
//!      is in flight.
//!   2. Walk `outbox_events WHERE sequence > after_sequence` in pages.
//!   3. Switch to live notifications, deduping any sequence already
//!      covered by backfill.
//!   4. On LISTEN reconnect, re-backfill — capped by
//!      `MAX_BACKFILL_EVENTS`.

use ::tracing::{error, info, instrument, warn};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::lightning_payment_gateway::lightning_payment_gateway_server::LightningPaymentGateway;
use crate::lightning_payment_gateway::{PaymentEvent, SubscribeEventsRequest};
use crate::outbox::{EventPublisher, ListenConnection, OutboxError, MAX_BACKFILL_EVENTS};

const CHANNEL_SIZE: usize = 1000;

/// If a single message can't drain within this window, treat the
/// stream as dead and tear down.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Inject the current OpenTelemetry trace context into the proto
/// event's JSON metadata so consumers can stitch their spans into one
/// distributed trace.
///
/// STUB(epic-2): identity function for now — `src/tracing.rs` is a
/// placeholder until Epic 2 wires OpenTelemetry. The call site stays
/// so the upgrade is just filling in the helper.
fn inject_trace_context(event: PaymentEvent) -> PaymentEvent {
    event
}

#[derive(Clone)]
pub struct LightningPaymentGatewayService {
    outbox: EventPublisher,
    listen_conn: ListenConnection,
    cancel_token: CancellationToken,
}

impl LightningPaymentGatewayService {
    /// `pg_url` is passed in separately from `pool` because sqlx's
    /// pool doesn't expose its config and the LISTEN side uses a
    /// different driver (`tokio_postgres`, `NoTls`). `cancel_token`
    /// is the process-wide shutdown signal; subscribers tear down
    /// when it fires.
    pub fn new(
        pool: PgPool,
        pg_url: String,
        cancel_token: CancellationToken,
    ) -> Result<Self, OutboxError> {
        let listen_conn = ListenConnection::new(pg_url);
        listen_conn.validate()?;
        Ok(Self {
            outbox: EventPublisher::new(&pool),
            listen_conn,
            cancel_token,
        })
    }

    #[instrument(name = "outbox.subscribe", skip(self), fields(after_sequence))]
    pub async fn subscribe(
        self: Arc<Self>,
        after_sequence: u64,
    ) -> Result<mpsc::Receiver<Result<PaymentEvent, Status>>, Status> {
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);

        let count = self
            .outbox
            .count_after(after_sequence as i64)
            .await
            .map_err(Status::from)?;

        if count > MAX_BACKFILL_EVENTS {
            return Err(Status::resource_exhausted(format!(
                "Backfill would require {count} events, max is {MAX_BACKFILL_EVENTS}. \
                 Consider subscribing from a more recent sequence.",
            )));
        }

        let this = self.clone();
        tokio::spawn(async move {
            this.subscription_loop(after_sequence as i64, tx).await;
        });

        Ok(rx)
    }

    #[instrument(
        name = "outbox.subscription_loop",
        skip(self, tx),
        fields(after_sequence)
    )]
    async fn subscription_loop(
        &self,
        after_sequence: i64,
        tx: mpsc::Sender<Result<PaymentEvent, Status>>,
    ) {
        let mut last_sent_sequence = after_sequence;

        // Start LISTEN before backfill so notifications buffer in the
        // channel while we walk history. The live-stream branch below
        // dedupes anything already covered against `last_sent_sequence`.
        let mut notifications = self.listen_conn.start_listening(self.cancel_token.clone());

        loop {
            if self.cancel_token.is_cancelled() {
                info!("LightningPaymentGatewayService: cancellation during backfill");
                return;
            }

            match self.outbox.fetch_after_batch(last_sent_sequence).await {
                Ok(events) => {
                    if events.is_empty() {
                        break;
                    }

                    for event in events {
                        let proto = inject_trace_context(event.to_proto());
                        last_sent_sequence = event.sequence;

                        match tokio::time::timeout(SEND_TIMEOUT, tx.send(Ok(proto))).await {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => {
                                warn!(
                                    sequence = last_sent_sequence,
                                    "Subscriber disconnected during backfill, closing stream"
                                );
                                return;
                            }
                            Err(_) => {
                                warn!(
                                    sequence = last_sent_sequence,
                                    "Send timeout during backfill, closing stream"
                                );
                                return;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to fetch backfill events");
                    let _ = tx.send(Err(Status::from(e))).await;
                    return;
                }
            }
        }

        info!(
            last_sent_sequence,
            "Backfill complete, switching to live stream"
        );

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("LightningPaymentGatewayService: cancellation requested");
                    return;
                }
                notification = notifications.recv() => {
                    match notification {
                        Some(Ok(sequence)) => {
                            if sequence <= last_sent_sequence {
                                continue;
                            }

                            match self.outbox.find_by_sequence(sequence).await {
                                Ok(Some(event)) => {
                                    let event_type_str = event.event_type.as_str();
                                    let correlation_id = event.correlation_id.clone();

                                    // Root span per event (`parent: None`)
                                    // so each event downstream gets its
                                    // own distributed trace.
                                    let span = ::tracing::info_span!(
                                        parent: None,
                                        "outbox.stream_event",
                                        sequence,
                                        event_type = event_type_str,
                                        correlation_id = %correlation_id,
                                    );
                                    let proto = span.in_scope(|| {
                                        info!("Streaming event to subscriber");
                                        inject_trace_context(event.to_proto())
                                    });
                                    last_sent_sequence = sequence;

                                    match tokio::time::timeout(SEND_TIMEOUT, tx.send(Ok(proto))).await {
                                        Ok(Ok(())) => {}
                                        Ok(Err(_)) => {
                                            warn!(sequence, "Subscriber disconnected, closing stream");
                                            return;
                                        }
                                        Err(_) => {
                                            warn!(sequence, "Send timeout, closing stream");
                                            return;
                                        }
                                    }
                                }
                                Ok(None) => {
                                    warn!(sequence, "Event not found for notification");
                                }
                                Err(e) => {
                                    error!(sequence, error = %e, "Failed to fetch event");
                                }
                            }
                        }
                        Some(Err(OutboxError::ListenDisconnected)) => {
                            warn!(
                                "LISTEN connection lost, re-backfilling from sequence {}",
                                last_sent_sequence
                            );

                            match self.outbox.count_after(last_sent_sequence).await {
                                Ok(count) if count > MAX_BACKFILL_EVENTS => {
                                    error!(
                                        count,
                                        max = MAX_BACKFILL_EVENTS,
                                        last_sent_sequence,
                                        "Re-backfill would exceed limit, closing stream"
                                    );
                                    let _ = tx
                                        .send(Err(Status::resource_exhausted(format!(
                                            "Re-backfill would require {count} events, max is {MAX_BACKFILL_EVENTS}",
                                        ))))
                                        .await;
                                    return;
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to count events for re-backfill limit check");
                                    let _ = tx.send(Err(Status::from(e))).await;
                                    return;
                                }
                                Ok(_) => {}
                            }

                            loop {
                                if self.cancel_token.is_cancelled() {
                                    return;
                                }
                                match self.outbox.fetch_after_batch(last_sent_sequence).await {
                                    Ok(events) => {
                                        if events.is_empty() {
                                            break;
                                        }
                                        for event in events {
                                            let proto = inject_trace_context(event.to_proto());
                                            last_sent_sequence = event.sequence;
                                            match tokio::time::timeout(SEND_TIMEOUT, tx.send(Ok(proto))).await {
                                                Ok(Ok(())) => {}
                                                Ok(Err(_)) => {
                                                    warn!(
                                                        sequence = last_sent_sequence,
                                                        "Subscriber disconnected during re-backfill, closing stream"
                                                    );
                                                    return;
                                                }
                                                Err(_) => {
                                                    warn!(
                                                        sequence = last_sent_sequence,
                                                        "Send timeout during re-backfill, closing stream"
                                                    );
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to fetch events during re-backfill");
                                        let _ = tx.send(Err(Status::from(e))).await;
                                        return;
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "Notification error");
                        }
                        None => {
                            info!("Notification stream ended");
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl LightningPaymentGateway for LightningPaymentGatewayService {
    type SubscribeEventsStream =
        tokio_stream::wrappers::ReceiverStream<Result<PaymentEvent, Status>>;

    #[instrument(
        name = "lightning_payment_gateway.subscribe_events",
        skip(self, request),
        fields(after_sequence)
    )]
    async fn subscribe_events(
        &self,
        request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let req = request.into_inner();
        let after_sequence = req.after_sequence;
        ::tracing::Span::current().record("after_sequence", after_sequence);

        info!(after_sequence, "New subscriber connected");

        let rx = Arc::new(self.clone()).subscribe(after_sequence).await?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(stream))
    }
}
