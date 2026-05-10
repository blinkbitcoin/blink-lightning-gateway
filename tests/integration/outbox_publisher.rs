//! Producer-side outbox tests: write path + pg_notify wire format.
//!
//! Booted Postgres via testcontainers — kept under `tests/` per the
//! workspace convention that DB-bound tests don't live inline in
//! `src/`.

use std::time::Duration;

use chrono::Utc;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres as PgImage;

use blink_ln_gateway::outbox::{
    EventPublisher, GatewayDomainEvent, GatewayEventType, NewOutboxEvent,
};

async fn boot_pg() -> (
    testcontainers::ContainerAsync<PgImage>,
    PgPool,
    String, /* postgres URL */
) {
    let container = PgImage::default().start().await.expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect pg");
    sqlx::migrate!().run(&pool).await.expect("migrate");
    (container, pool, url)
}

fn invoice_created_event() -> NewOutboxEvent {
    NewOutboxEvent::for_lightning_invoice_created(
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
    let (_pg, pool, _url) = boot_pg().await;
    let pub_ = EventPublisher::new(&pool);

    let mut tx = pool.begin().await.unwrap();
    let seq = pub_
        .publish_in_tx(&mut tx, invoice_created_event())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let row = pub_.find_by_sequence(seq).await.unwrap().expect("row");
    assert_eq!(row.event_type, GatewayEventType::IncomingPaymentPending);
    assert_eq!(
        row.domain_event,
        GatewayDomainEvent::LightningInvoiceCreated
    );
    assert_eq!(row.sat_amount, 1000);
    assert_eq!(row.reference_id, "aa".repeat(32));
    assert_eq!(row.currency, "BTC");
}

#[tokio::test]
#[serial]
async fn sequence_is_monotonic_across_writes() {
    let (_pg, pool, _url) = boot_pg().await;
    let pub_ = EventPublisher::new(&pool);

    let mut s = Vec::new();
    for _ in 0..5 {
        let mut tx = pool.begin().await.unwrap();
        s.push(
            pub_.publish_in_tx(&mut tx, invoice_created_event())
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
    // Throwaway inline LISTEN connection. Story 1.5 ports the
    // production-grade `listen_connection.rs` with backoff/cancellation;
    // here we drive `tokio_postgres::Connection::poll_message` directly
    // via `std::future::poll_fn` to capture `AsyncMessage::Notification`s.
    use std::future::poll_fn;
    use tokio_postgres::AsyncMessage;

    let (_pg, pool, url) = boot_pg().await;
    let pub_ = EventPublisher::new(&pool);

    let (client, mut conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
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
            .publish_in_tx(&mut tx, invoice_created_event())
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
