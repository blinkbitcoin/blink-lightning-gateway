//! `EventPublisher::publish_in_tx` — the slice's only outbox-write path.
//!
//! Re-derived from `blink-card/src/outbox/repository.rs:64-100` (`insert_in_tx`)
//! minus the `webhook_id`-based `ON CONFLICT` (LN has no webhook ingress —
//! see `migrations/<TS>_outbox_events.sql` for the rationale). The pg
//! trigger `outbox_events_notify` fires `pg_notify('gateway_events',
//! sequence::text)` after every insert.

use sqlx::{PgPool, Postgres, Transaction};

use super::{
    entity::{NewOutboxEvent, OutboxEvent},
    error::OutboxError,
};

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
                sat_amount,
                currency,
                timestamp,
                gateway_metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING sequence
            "#,
            event.correlation_id,
            event.domain_event.as_str(),
            event_type.as_str(),
            event.reference_id,
            event.sat_amount,
            event.currency,
            event.timestamp,
            event.gateway_metadata,
        )
        .fetch_one(&mut **tx)
        .await?;

        Ok(row.sequence)
    }

    /// Read-back helper; primarily for tests + Story 1.5's
    /// `subscription_loop` will use a similar `fetch_after_batch` pattern.
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
                sat_amount,
                currency,
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
                sat_amount: r.sat_amount,
                currency: r.currency,
                timestamp: r.timestamp,
                gateway_metadata: r.gateway_metadata,
            }),
        })
    }
}
