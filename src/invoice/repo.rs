//! `Invoices` — concrete `Pool<Postgres>`-holding repository for the Invoice
//! aggregate. No trait abstraction over the pool (architecture L700).
//!
//! Two write/read entry points for Slice 1a:
//! - `persist_in_tx` — atomic projection-row + events insert inside a tx.
//! - `find_by_payment_hash` — load projection + replay events to hydrate.

use es_entity::{EntityEvents, EsEvent, GenericEvent, TryFromEvents};
use sqlx::{Pool, Postgres, Transaction};

use super::{
    entity::{Invoice, NewInvoice},
    error::InvoiceError,
};
use crate::primitives::{InvoiceId, PaymentHash, Timestamp};

#[derive(Clone, Debug)]
pub struct Invoices {
    pool: Pool<Postgres>,
}

impl Invoices {
    pub fn new(pool: &Pool<Postgres>) -> Self {
        Self { pool: pool.clone() }
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// Validates `params` via `Invoice::create`, then inserts the projection
    /// row + every emitted event inside the caller's transaction. The caller
    /// commits.
    ///
    /// Returns the freshly-generated `InvoiceId`.
    pub async fn persist_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        params: NewInvoice,
        now: Timestamp,
    ) -> Result<InvoiceId, InvoiceError> {
        let events = Invoice::create(params, now)?;
        // Extract canonical fields from the first (Created) event for the
        // projection-row insert. Slice 1a only emits `Created`, so destructure
        // it directly.
        let (id, payment_hash, wallet_id, amount_msat, expiry_at, created_at) = match &events[0] {
            super::event::InvoiceEvent::Created {
                id,
                payment_hash,
                wallet_id,
                amount_msat,
                expiry_at,
                created_at,
                ..
            } => (
                *id,
                *payment_hash,
                *wallet_id,
                *amount_msat,
                *expiry_at,
                *created_at,
            ),
        };

        sqlx::query!(
            r#"INSERT INTO invoices (id, payment_hash, wallet_id, amount_msat, expiry_at, state, created_at)
               VALUES ($1, $2, $3, $4, $5, 'pending', $6)"#,
            uuid::Uuid::from(id),
            payment_hash as PaymentHash,
            uuid::Uuid::from(wallet_id),
            amount_msat as crate::primitives::MilliSatoshi,
            expiry_at.into_inner(),
            created_at.into_inner(),
        )
        .execute(&mut **tx)
        .await?;

        for (sequence, event) in events.iter().enumerate() {
            let payload = serde_json::to_value(event)?;
            sqlx::query!(
                r#"INSERT INTO invoice_events (id, sequence, event)
                   VALUES ($1, $2, $3)"#,
                uuid::Uuid::from(id),
                (sequence + 1) as i32,
                payload,
            )
            .execute(&mut **tx)
            .await?;
        }

        Ok(id)
    }

    /// Loads the projection + replays the event log to hydrate.
    pub async fn find_by_payment_hash(
        &self,
        payment_hash: &PaymentHash,
    ) -> Result<Invoice, InvoiceError> {
        let row = sqlx::query!(
            r#"SELECT id FROM invoices WHERE payment_hash = $1"#,
            payment_hash as &PaymentHash,
        )
        .fetch_optional(&self.pool)
        .await?;

        let id = match row {
            Some(r) => InvoiceId::from(r.id),
            None => return Err(InvoiceError::NotFound(*payment_hash)),
        };

        let events = sqlx::query!(
            r#"SELECT id, sequence, event, recorded_at
               FROM invoice_events
               WHERE id = $1
               ORDER BY sequence"#,
            uuid::Uuid::from(id),
        )
        .fetch_all(&self.pool)
        .await?;

        let generic: Vec<GenericEvent<<super::event::InvoiceEvent as EsEvent>::EntityId>> = events
            .into_iter()
            .map(|r| GenericEvent {
                entity_id: InvoiceId::from(r.id),
                sequence: r.sequence,
                event: r.event,
                context: None,
                recorded_at: r.recorded_at,
            })
            .collect();

        let entity_events = build_entity_events(id, generic)?;
        Ok(Invoice::try_from_events(entity_events)?)
    }
}

/// Hand-roll `EntityEvents` from `GenericEvent<InvoiceId>` rows. The es-entity
/// `EntityEvents::load_first` helper exists, but it expects entity ids to be
/// grouped + needs an `EsEntity` callback; for a single-id load by
/// payment_hash this hand-roll is simpler and uses the public surface only.
fn build_entity_events(
    id: InvoiceId,
    rows: Vec<GenericEvent<InvoiceId>>,
) -> Result<EntityEvents<super::event::InvoiceEvent>, InvoiceError> {
    let mut acc = EntityEvents::init(id, std::iter::empty::<super::event::InvoiceEvent>());
    // Replay events in chronological order via `extend` (treats them as
    // "new" then we never persist again).
    let parsed: Result<Vec<super::event::InvoiceEvent>, _> = rows
        .into_iter()
        .map(|row| serde_json::from_value::<super::event::InvoiceEvent>(row.event))
        .collect();
    acc.extend(parsed?);
    Ok(acc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{BoltInvoice, MilliSatoshi, WalletId};
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres as PgImage;

    async fn boot_pg() -> (testcontainers::ContainerAsync<PgImage>, Pool<Postgres>) {
        let container = PgImage::default().start().await.expect("start pg");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect pg");
        sqlx::migrate!().run(&pool).await.expect("migrate");
        (container, pool)
    }

    fn ok_new_invoice() -> NewInvoice {
        NewInvoice {
            payment_hash: PaymentHash::from([0xaa; 32]),
            wallet_id: WalletId::new(),
            amount_msat: MilliSatoshi::new(1_000_000),
            expiry_seconds: 3600,
            memo: Some("test".to_owned()),
            bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
        }
    }

    #[tokio::test]
    #[serial]
    async fn persist_then_find_by_payment_hash_round_trips() {
        let (_pg, pool) = boot_pg().await;
        let invoices = Invoices::new(&pool);

        let now = Timestamp::now();
        let mut tx = pool.begin().await.unwrap();
        let id = invoices
            .persist_in_tx(&mut tx, ok_new_invoice(), now)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let inv = invoices
            .find_by_payment_hash(&PaymentHash::from([0xaa; 32]))
            .await
            .unwrap();
        assert_eq!(inv.id, id);
        assert_eq!(inv.amount_msat, MilliSatoshi::new(1_000_000));
    }

    #[tokio::test]
    #[serial]
    async fn find_by_payment_hash_missing_returns_not_found() {
        let (_pg, pool) = boot_pg().await;
        let invoices = Invoices::new(&pool);
        let err = invoices
            .find_by_payment_hash(&PaymentHash::from([0xff; 32]))
            .await
            .unwrap_err();
        assert!(matches!(err, InvoiceError::NotFound(_)));
    }

    #[tokio::test]
    #[serial]
    async fn persist_writes_one_invoices_row_and_one_event_row() {
        let (_pg, pool) = boot_pg().await;
        let invoices = Invoices::new(&pool);
        let mut tx = pool.begin().await.unwrap();
        invoices
            .persist_in_tx(&mut tx, ok_new_invoice(), Timestamp::now())
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let count_invoices: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM invoices"#)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count_invoices.0, 1);

        let count_events: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM invoice_events"#)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count_events.0, 1);
    }
}
