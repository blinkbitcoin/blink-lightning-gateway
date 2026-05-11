//! `Invoices` repo coverage: round-trips against Postgres. The
//! `try_new` validation path is covered by the pure entity tests in
//! `src/invoice/entity.rs` (`try_new_rejects_zero_amount`), so it
//! doesn't need its own DB-bound test here.

use serial_test::serial;

use blink_lightning_gateway::invoice::entity::NewInvoice;
use blink_lightning_gateway::invoice::Invoices;
use blink_lightning_gateway::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Timestamp, WalletId};

use crate::common::TestDatabase;

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
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
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
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
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
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
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

    let event_type: (String,) = sqlx::query_as(r#"SELECT event_type FROM invoice_events LIMIT 1"#)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_type.0, "created");
}
