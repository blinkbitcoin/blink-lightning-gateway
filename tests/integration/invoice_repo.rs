//! `Invoices` repo coverage: round-trips against Postgres. The
//! `try_new` validation path is covered by the pure entity tests in
//! `src/invoice/entity.rs`, so it doesn't need its own DB-bound test
//! here.

use blink_lightning_gateway::invoice::entity::{InvoiceState, NewInvoice};
use blink_lightning_gateway::invoice::Invoices;
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};
use uuid::Uuid;

use crate::common::TestDatabase;

fn ok_new_invoice() -> NewInvoice {
    let preimage = Preimage::from([0xee; 32]);
    NewInvoice::try_new(
        preimage.payment_hash(),
        preimage,
        WalletId::from(Uuid::now_v7()),
        Some(MilliSatoshi::new(1_000_000)),
        3600,
        BoltInvoice::new("lnbc1u1pj..."),
        Timestamp::now(),
    )
    .expect("valid")
}

#[tokio::test]
async fn create_then_find_by_payment_hash() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let invoices = Invoices::new(&pool);

    let new = ok_new_invoice();
    let expected_id = new.id;
    let expected_hash = new.payment_hash;
    let created = invoices.create(new).await.expect("create");
    assert_eq!(created.id, expected_id);

    let found = invoices
        .find_by_payment_hash(&expected_hash)
        .await
        .expect("find");
    assert_eq!(found.id, expected_id);
    assert_eq!(found.amount_msat, Some(MilliSatoshi::new(1_000_000)));
}

#[tokio::test]
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

#[tokio::test]
async fn list_open_invoices_returns_open_and_held_only() {
    // Seed three invoices, transition two to terminal states; assert
    // `list_open_invoices` returns the Open one + a Held one only.
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let invoices = Invoices::new(&pool);

    let pre_open = Preimage::from([0x01; 32]);
    let open_invoice = invoices
        .create(
            NewInvoice::try_new(
                pre_open.payment_hash(),
                pre_open,
                WalletId::from(Uuid::now_v7()),
                Some(MilliSatoshi::new(1_000_000)),
                3600,
                BoltInvoice::new("lnbc1u1pj..."),
                Timestamp::now(),
            )
            .unwrap(),
        )
        .await
        .expect("create open");

    let pre_held = Preimage::from([0x02; 32]);
    let mut held = invoices
        .create(
            NewInvoice::try_new(
                pre_held.payment_hash(),
                pre_held,
                WalletId::from(Uuid::now_v7()),
                Some(MilliSatoshi::new(2_000_000)),
                3600,
                BoltInvoice::new("lnbc2u1pj..."),
                Timestamp::now(),
            )
            .unwrap(),
        )
        .await
        .expect("create held");
    let _ = held
        .mark_held(MilliSatoshi::new(2_000_000), Timestamp::now())
        .unwrap();
    invoices.update(&mut held).await.expect("update to held");

    let pre_settled = Preimage::from([0x03; 32]);
    let mut settled = invoices
        .create(
            NewInvoice::try_new(
                pre_settled.payment_hash(),
                pre_settled,
                WalletId::from(Uuid::now_v7()),
                Some(MilliSatoshi::new(3_000_000)),
                3600,
                BoltInvoice::new("lnbc3u1pj..."),
                Timestamp::now(),
            )
            .unwrap(),
        )
        .await
        .expect("create settled");
    let _ = settled.settle(pre_settled, Timestamp::now()).unwrap();
    invoices
        .update(&mut settled)
        .await
        .expect("update to settled");

    let open = invoices.list_open_invoices().await.expect("list");
    let ids: Vec<_> = open.iter().map(|i| i.id).collect();
    assert!(
        ids.contains(&open_invoice.id),
        "open invoice expected in list"
    );
    assert!(ids.contains(&held.id), "held invoice expected in list");
    assert!(
        !ids.contains(&settled.id),
        "settled invoice MUST NOT appear in list"
    );
    assert_eq!(open.len(), 2);
}

#[tokio::test]
async fn invoice_settled_event_hydrates_payment_preimage() {
    // Persist a Created event (preimage on Created per AC3), transition to
    // Settled, reload — assert the `payment_preimage` field round-trips
    // through the event log. Guards `try_from_events`'s preimage-from-
    // Created fold and the `settle` debug_assert that the wire preimage
    // matches the Created one.
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let invoices = Invoices::new(&pool);

    let preimage = Preimage::from([0xab; 32]);
    let payment_hash = preimage.payment_hash();
    let new = NewInvoice::try_new(
        payment_hash,
        preimage,
        WalletId::from(Uuid::now_v7()),
        Some(MilliSatoshi::new(1_000_000)),
        3600,
        BoltInvoice::new("lnbc1u1pj..."),
        Timestamp::now(),
    )
    .unwrap();
    let mut invoice = invoices.create(new).await.expect("create");

    let _ = invoice.settle(preimage, Timestamp::now()).unwrap();
    invoices.update(&mut invoice).await.expect("update");

    let reloaded = invoices
        .find_by_payment_hash(&payment_hash)
        .await
        .expect("find");
    assert_eq!(reloaded.state, InvoiceState::Settled);
    assert_eq!(reloaded.payment_preimage, preimage);
}
