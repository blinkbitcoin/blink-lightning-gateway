//! Producer-side outbox tests: write path + pg_notify wire format.
//!
//! The trigger event used here is `LightningInvoiceSettled` — its
//! production source (LND `subscribe_invoices` `is_confirmed` callback)
//! lands in Story 2.3, but `EventPublisher::publish_in_tx` already
//! accepts the variant, so these tests exercise the publisher
//! in isolation against testcontainers Postgres.

use std::time::Duration;

use chrono::Utc;
use serial_test::serial;

use blink_lightning_gateway::outbox::{
    EventPublisher, GatewayDomainEvent, GatewayEventType, NewOutboxEvent,
};

use crate::common::TestDatabase;

fn invoice_settled_event() -> NewOutboxEvent {
    NewOutboxEvent::for_lightning_invoice_settled(
        "corr-1",
        "aa".repeat(32),
        1000,
        Utc::now(),
        serde_json::json!({"bolt_invoice": "lnbc1u..."}),
    )
}

#[tokio::test]
#[serial]
async fn publish_in_tx_writes_one_row_with_correct_event_type() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let pub_ = EventPublisher::new(&pool);

    let mut tx = pool.begin().await.unwrap();
    let seq = pub_
        .publish_in_tx(&mut tx, invoice_settled_event())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let row = pub_.find_by_sequence(seq).await.unwrap().expect("row");
    assert_eq!(row.event_type, GatewayEventType::IncomingPaymentConfirmed);
    assert_eq!(
        row.domain_event,
        GatewayDomainEvent::LightningInvoiceSettled
    );
    assert_eq!(row.amount_sat, 1000);
    assert_eq!(row.reference_id, "aa".repeat(32));
}

#[tokio::test]
#[serial]
async fn sequence_is_monotonic_across_writes() {
    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let pub_ = EventPublisher::new(&pool);

    let mut s = Vec::new();
    for _ in 0..5 {
        let mut tx = pool.begin().await.unwrap();
        s.push(
            pub_.publish_in_tx(&mut tx, invoice_settled_event())
                .await
                .unwrap(),
        );
        tx.commit().await.unwrap();
    }
    for w in s.windows(2) {
        assert!(w[1] > w[0], "sequence must monotonically increase");
    }
}

#[tokio::test]
#[serial]
async fn pg_notify_fires_on_gateway_events_channel() {
    use std::future::poll_fn;
    use tokio_postgres::AsyncMessage;

    let db = TestDatabase::new().await.expect("test db");
    let pool = db.pool.clone();
    let pub_ = EventPublisher::new(&pool);

    let (client, mut conn) = tokio_postgres::connect(&db.url, tokio_postgres::NoTls)
        .await
        .expect("listen connect");
    let (notif_tx, mut notif_rx) =
        tokio::sync::mpsc::unbounded_channel::<tokio_postgres::Notification>();
    let driver = tokio::spawn(async move {
        loop {
            let msg = poll_fn(|cx| conn.poll_message(cx)).await;
            match msg {
                Some(Ok(AsyncMessage::Notification(n))) => {
                    let _ = notif_tx.send(n);
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    });

    client
        .batch_execute("LISTEN gateway_events;")
        .await
        .expect("LISTEN");

    let publish_seq = {
        let mut tx = pool.begin().await.unwrap();
        let s = pub_
            .publish_in_tx(&mut tx, invoice_settled_event())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        s
    };

    let notif = tokio::time::timeout(Duration::from_secs(5), notif_rx.recv())
        .await
        .expect("pg_notify within 5s")
        .expect("notification");

    assert_eq!(notif.channel(), "gateway_events");
    assert_eq!(notif.payload(), publish_seq.to_string());

    drop(client);
    let _ = tokio::time::timeout(Duration::from_millis(100), driver).await;
}
