//! `Invoices` — `EsRepo`-derived repository for the Invoice aggregate.
//!
//! `#[derive(EsRepo)]` generates `create` / `create_in_op`, `find_by_id` /
//! `maybe_find_by_id` / `find_by_id_in_op`, `find_by_payment_hash` /
//! `maybe_find_by_payment_hash`, `maybe_find_by_wallet_id` /
//! `list_for_wallet_id` (cursor-paginated), `update` / `update_in_op`, and
//! the internal `persist_events` driver. The macro reads the column list
//! below to emit the projection-row INSERT in `create_in_op` and the
//! UPDATE in `update_in_op`. See blink-card/src/authorization/repo.rs for
//! the same shape.

// `EsEntity` and `EsEvent` are imported because the `EsRepo` derive's
// expansion calls `Invoice::events()` (provided by `EsEntity`) and
// `<InvoiceEvent as EsEvent>::event_context()`. Both traits look unused at
// first glance — they're consumed inside the macro output.
use es_entity::EsRepo;
#[allow(unused_imports)]
use es_entity::{EsEntity, EsEvent};
use sqlx::PgPool;

use super::entity::Invoice;
use super::event::InvoiceEvent;
use crate::primitives::{InvoiceId, MilliSatoshi, PaymentHash, Timestamp, WalletId};

#[derive(EsRepo, Clone)]
#[es_repo(
    entity = "Invoice",
    columns(
        payment_hash(ty = "PaymentHash", update(persist = false)),
        wallet_id(ty = "WalletId", list_for, update(persist = false)),
        amount_msat(ty = "MilliSatoshi", find_by = false, update(persist = false)),
        expiry_at(ty = "Timestamp", find_by = false, update(persist = false)),
        state(
            ty = "String",
            find_by = false,
            create(accessor = "state_str()"),
            update(accessor = "state_str()"),
        ),
    )
)]
pub struct Invoices {
    pool: PgPool,
}

impl Invoices {
    pub fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invoice::entity::NewInvoice;
    use crate::invoice::error::InvoiceError;
    use crate::primitives::{BoltInvoice, MilliSatoshi, Timestamp};
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Pool;
    use sqlx::Postgres;
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
        NewInvoice::try_new(
            PaymentHash::from([0xaa; 32]),
            WalletId::new(),
            MilliSatoshi::new(1_000_000),
            3600,
            BoltInvoice::new("lnbc1u1pj..."),
            Timestamp::now(),
        )
        .expect("valid")
    }

    #[tokio::test]
    #[serial]
    async fn create_then_find_by_payment_hash() {
        let (_pg, pool) = boot_pg().await;
        let invoices = Invoices::new(&pool);

        let new = ok_new_invoice();
        let expected_id = new.id;
        let created = invoices.create(new).await.expect("create");
        assert_eq!(created.id, expected_id);

        let found = invoices
            .find_by_payment_hash(&PaymentHash::from([0xaa; 32]))
            .await
            .expect("find");
        assert_eq!(found.id, expected_id);
        assert_eq!(found.amount_msat, MilliSatoshi::new(1_000_000));
    }

    #[tokio::test]
    #[serial]
    async fn maybe_find_by_payment_hash_missing_returns_none() {
        let (_pg, pool) = boot_pg().await;
        let invoices = Invoices::new(&pool);
        let res = invoices
            .maybe_find_by_payment_hash(&PaymentHash::from([0xff; 32]))
            .await
            .expect("ok");
        assert!(res.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn create_writes_one_invoices_row_and_one_event_row() {
        let (_pg, pool) = boot_pg().await;
        let invoices = Invoices::new(&pool);
        let _ = invoices.create(ok_new_invoice()).await.expect("create");

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

        let event_type: (String,) =
            sqlx::query_as(r#"SELECT event_type FROM invoice_events LIMIT 1"#)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(event_type.0, "created");
    }

    #[tokio::test]
    #[serial]
    async fn try_new_validation_fails_before_db_write() {
        let (_pg, _pool) = boot_pg().await;
        // Zero amount is the only condition `try_new` rejects with an Err.
        // Out-of-range expiry is silently coerced to the BTC default
        // (4 hours) per blink-core's behavior, so it doesn't surface here.
        let err = NewInvoice::try_new(
            PaymentHash::from([0xaa; 32]),
            WalletId::new(),
            MilliSatoshi::ZERO,
            3600,
            BoltInvoice::new("lnbc..."),
            Timestamp::now(),
        )
        .unwrap_err();
        assert!(matches!(err, InvoiceError::InvalidAmount));
    }
}
