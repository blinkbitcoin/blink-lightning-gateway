//! `EventPublisher::publish_in_tx` — the slice's only outbox-write path.

use sqlx::{PgPool, Postgres, Transaction};

use super::{
    entity::{NewOutboxEvent, OutboxEvent},
    error::OutboxError,
};

const BACKFILL_BATCH_SIZE: i64 = 1000;
pub const MAX_BACKFILL_EVENTS: i64 = 100_000;

#[derive(Clone, Debug)]
pub struct EventPublisher {
    pool: PgPool,
}

impl EventPublisher {
    pub fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert one outbox row inside the caller's transaction. Returns the
    /// freshly assigned `BIGSERIAL` sequence.
    pub async fn publish_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event: NewOutboxEvent,
    ) -> Result<i64, OutboxError> {
        let event_type = event.domain_event.to_standardized();
        let row = sqlx::query!(
            r#"
            INSERT INTO outbox_events (
                correlation_id,
                domain_event_type,
                event_type,
                reference_id,
                amount_sat,
                timestamp,
                gateway_metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING sequence
            "#,
            event.correlation_id,
            event.domain_event.as_str(),
            event_type.as_str(),
            event.reference_id,
            event.amount_sat,
            event.timestamp,
            event.gateway_metadata,
        )
        .fetch_one(&mut **tx)
        .await?;

        Ok(row.sequence)
    }

    /// Single-row read by sequence. Used by both tests and the gRPC
    /// `subscription_loop` after a `gateway_events` notification fires.
    pub async fn find_by_sequence(
        &self,
        sequence: i64,
    ) -> Result<Option<OutboxEvent>, OutboxError> {
        let row = sqlx::query!(
            r#"
            SELECT
                sequence,
                correlation_id,
                domain_event_type,
                event_type,
                reference_id,
                amount_sat,
                timestamp,
                gateway_metadata
            FROM outbox_events
            WHERE sequence = $1
            "#,
            sequence,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            None => None,
            Some(r) => Some(OutboxEvent {
                sequence: r.sequence,
                correlation_id: r.correlation_id,
                domain_event: r.domain_event_type.parse()?,
                event_type: r.event_type.parse()?,
                reference_id: r.reference_id,
                amount_sat: r.amount_sat,
                timestamp: r.timestamp,
                gateway_metadata: r.gateway_metadata,
            }),
        })
    }

    /// Backfill batch read: every row with `sequence > after_sequence`,
    /// ordered ascending, up to `BACKFILL_BATCH_SIZE`. Subscription loop
    /// pages through these until it gets an empty page, then switches to
    /// LISTEN-driven streaming.
    pub async fn fetch_after_batch(
        &self,
        after_sequence: i64,
    ) -> Result<Vec<OutboxEvent>, OutboxError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                sequence,
                correlation_id,
                domain_event_type,
                event_type,
                reference_id,
                amount_sat,
                timestamp,
                gateway_metadata
            FROM outbox_events
            WHERE sequence > $1
            ORDER BY sequence
            LIMIT $2
            "#,
            after_sequence,
            BACKFILL_BATCH_SIZE,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|r| {
                Ok(OutboxEvent {
                    sequence: r.sequence,
                    correlation_id: r.correlation_id,
                    domain_event: r.domain_event_type.parse()?,
                    event_type: r.event_type.parse()?,
                    reference_id: r.reference_id,
                    amount_sat: r.amount_sat,
                    timestamp: r.timestamp,
                    gateway_metadata: r.gateway_metadata,
                })
            })
            .collect()
    }

    /// Count rows with `sequence > after_sequence`. Used by the subscription
    /// loop's `MAX_BACKFILL_EVENTS` guard rail before opening the stream
    /// and again before each re-backfill after a LISTEN reconnect.
    pub async fn count_after(&self, after_sequence: i64) -> Result<i64, OutboxError> {
        let count = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!" FROM outbox_events WHERE sequence > $1"#,
            after_sequence,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }
}
