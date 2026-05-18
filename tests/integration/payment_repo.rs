//! `Payments` repo coverage: round-trips against Postgres

use es_entity::{EsEntity, Idempotent};

use blink_lightning_gateway::payment::entity::{DecodedInvoice, NewPayment};
use blink_lightning_gateway::payment::{Hop, PaymentState, Payments};
use blink_lightning_gateway::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};
use uuid::Uuid;

use crate::common::TestDatabase;

fn ok_new_payment() -> NewPayment {
    let decoded = DecodedInvoice {
        payment_hash: PaymentHash::from([0xcc; 32]),
        destination: "02abc".to_owned(),
        amount_msat: Some(MilliSatoshi::new(1_000_000)),
        bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
    };
    NewPayment::try_new(
        decoded,
        WalletId::from(Uuid::now_v7()),
        None,
        MilliSatoshi::new(5_000),
        Timestamp::now(),
    )
    .expect("valid")
}

#[tokio::test]
async fn create_then_find_by_payment_hash() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let payments = Payments::new(&pool);

    let new = ok_new_payment();
    let expected_id = new.id;
    let created = payments.create(new).await.expect("create");
    assert_eq!(created.id, expected_id);
    assert_eq!(created.state, PaymentState::Initiated);

    let found = payments
        .find_by_payment_hash(&PaymentHash::from([0xcc; 32]))
        .await
        .expect("find");
    assert_eq!(found.id, expected_id);
    assert_eq!(found.max_fee_msat, MilliSatoshi::new(5_000));
}

#[tokio::test]
async fn update_in_op_persists_pending_event_and_state_transition() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let payments = Payments::new(&pool);

    let mut payment = payments.create(ok_new_payment()).await.expect("create");

    // Mark pending in one transaction.
    let mut tx = pool.begin().await.unwrap();
    let events = match payment.mark_pending(Timestamp::now()).expect("pending") {
        Idempotent::Executed(events) => events,
        Idempotent::Ignored => panic!("first mark_pending should execute"),
    };
    payment.events_mut().extend(events);
    payment.state = PaymentState::Pending;
    payments
        .update_in_op(&mut tx, &mut payment)
        .await
        .expect("update");
    tx.commit().await.unwrap();

    let reloaded = payments
        .find_by_payment_hash(&PaymentHash::from([0xcc; 32]))
        .await
        .expect("find");
    assert_eq!(reloaded.state, PaymentState::Pending);

    // settle.
    let mut tx = pool.begin().await.unwrap();
    let events = match reloaded
        .settle(
            Preimage::from([0xdd; 32]),
            MilliSatoshi::new(200),
            Vec::<Hop>::new(),
            Timestamp::now(),
        )
        .expect("settle")
    {
        Idempotent::Executed(events) => events,
        Idempotent::Ignored => panic!("first settle should execute"),
    };
    let mut after_settle = reloaded;
    after_settle.events_mut().extend(events);
    after_settle.state = PaymentState::Completed;
    payments
        .update_in_op(&mut tx, &mut after_settle)
        .await
        .expect("update");
    tx.commit().await.unwrap();

    let reloaded = payments
        .find_by_payment_hash(&PaymentHash::from([0xcc; 32]))
        .await
        .expect("find");
    assert_eq!(reloaded.state, PaymentState::Completed);
    assert_eq!(reloaded.fees_paid_msat, Some(MilliSatoshi::new(200)));
}

#[tokio::test]
async fn create_writes_one_payments_row_and_one_event_row() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let payments = Payments::new(&pool);
    let _ = payments.create(ok_new_payment()).await.expect("create");

    let count_payments: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM payments"#)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count_payments.0, 1);

    let count_events: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM payment_events"#)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count_events.0, 1);

    let event_type: (String,) = sqlx::query_as(r#"SELECT event_type FROM payment_events LIMIT 1"#)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_type.0, "initiated");
}

#[tokio::test]
async fn maybe_find_by_payment_hash_missing_returns_none() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let payments = Payments::new(&pool);
    let res = payments
        .maybe_find_by_payment_hash(&PaymentHash::from([0xff; 32]))
        .await
        .expect("ok");
    assert!(res.is_none());
}
