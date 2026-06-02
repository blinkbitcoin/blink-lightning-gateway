//! Story 2.1 AC11 — server lifecycle test.
//!
//! Spawns `run_grpc_server` + `run_graphql_server` + `health::run`
//! against a testcontainers Postgres, opens an in-flight
//! `SubscribeEvents` stream, cancels the shutdown token, and asserts:
//!   1. `/health/ready` returned 200 while the server was up.
//!   2. The GraphQL subgraph answered an introspection POST with 200
//!      while the server was up (init smoke — proves the GraphQL
//!      handler is wired, not just any axum listener).
//!   3. The in-flight `SubscribeEvents` stream completes cleanly when
//!      the cancel fires (the consumer-side `subscription_loop` sees
//!      `cancel.cancelled()` and tears down without panicking the
//!      tonic server task).
//!   4. After the cancel, both the gRPC and the GraphQL listeners stop
//!      accepting new TCP connections.
//!   5. All three server tasks join cleanly (no panic, no error
//!      returned).
//!
//! SIGTERM trapping in a test is awkward (the binary's `run_cmd` owns
//! the signal handler), so the test triggers shutdown by cancelling
//! the token directly. The grace-sleep step lives in `run_cmd`; here
//! we exercise the listener + subscriber tear-down only.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};

use blink_lightning_gateway::api::grpc::LightningPaymentGatewayService;
use blink_lightning_gateway::app::{App, InvoiceUpdateDispatcher};
use blink_lightning_gateway::health;
use blink_lightning_gateway::lightning_payment_gateway::{
    lightning_payment_gateway_client::LightningPaymentGatewayClient,
    lightning_payment_gateway_server::LightningPaymentGatewayServer, SubscribeEventsRequest,
};
use blink_lightning_gateway::lnd::{LndApi, LndClient, LndConfig};
use blink_lightning_gateway::outbox::EventPublisher;
use blink_lightning_gateway::server::{
    run_graphql_server, run_grpc_server, GrpcServerConfig, SubgraphServerConfig,
};
use blink_lightning_gateway::symphony::{LightningSymphonyClient, SymphonyClient};

use crate::common::TestDatabase;

async fn pick_free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port for free-port lookup");
    let port = listener
        .local_addr()
        .expect("local_addr on ephemeral listener")
        .port();
    drop(listener);
    port
}

async fn wait_for_tcp(host: &str, port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect((host, port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not start accepting TCP on {host}:{port}");
}

async fn http_request_status(host: &str, port: u16, request: &str) -> u16 {
    let mut stream = tokio::net::TcpStream::connect((host, port))
        .await
        .expect("HTTP probe: TCP connect failed");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("HTTP probe: write_all failed");
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut buf))
        .await
        .expect("HTTP probe: read_to_end timed out — server hung")
        .expect("HTTP probe: read_to_end failed");
    let body = std::str::from_utf8(&buf).unwrap_or("");
    body.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

async fn http_get_status(host: &str, port: u16, path: &str) -> u16 {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    http_request_status(host, port, &request).await
}

async fn http_post_json_status(host: &str, port: u16, path: &str, body: &str) -> u16 {
    let request = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        len = body.len()
    );
    http_request_status(host, port, &request).await
}

#[tokio::test]
async fn server_lifecycle_drains_in_flight_subscribers_on_cancel() {
    let db = TestDatabase::new().await.expect("test db");

    let grpc_port = pick_free_port().await;
    let health_port = pick_free_port().await;
    let graphql_port = pick_free_port().await;

    let cancel = CancellationToken::new();

    let reporter = HealthReporter::new();
    reporter
        .set_serving::<LightningPaymentGatewayServer<LightningPaymentGatewayService>>()
        .await;
    let health_service = HealthService::from_health_reporter(reporter.clone());
    let health_server = HealthServer::new(health_service);

    let grpc_config = GrpcServerConfig {
        port: grpc_port,
        pg_config: db.url.clone(),
        ..GrpcServerConfig::default()
    };
    let grpc_pool = db.pool.clone();
    let grpc_cancel = cancel.clone();
    let grpc_handle = tokio::spawn(async move {
        run_grpc_server(grpc_config, grpc_pool, grpc_cancel, health_server).await
    });

    let health_pool = db.pool.clone();
    let health_cancel = cancel.clone();
    let http_handle =
        tokio::spawn(async move { health::run(health_pool, health_port, health_cancel).await });

    // GraphQL subgraph server. Boot-stub the LND adapter the same way
    // `src/cli.rs::run_cmd` does — the test only exercises init +
    // shutdown, not the `lnInvoiceCreate` happy path.
    let lnd: Arc<dyn LndApi> = Arc::new(LndClient::boot_stub(LndConfig::stub()));
    let outbox = EventPublisher::new(&db.pool);
    let symphony: Arc<dyn SymphonyClient> = Arc::new(LightningSymphonyClient::boot_stub());
    let app = App::new(
        db.pool.clone(),
        lnd,
        outbox,
        symphony,
        crate::common::CannedWalletOwnership::allow(),
        InvoiceUpdateDispatcher::for_test(),
    );
    let graphql_config = SubgraphServerConfig {
        port: graphql_port,
        ..SubgraphServerConfig::default()
    };
    let graphql_cancel = cancel.clone();
    let graphql_handle =
        tokio::spawn(async move { run_graphql_server(graphql_config, app, graphql_cancel).await });

    wait_for_tcp("127.0.0.1", grpc_port).await;
    wait_for_tcp("127.0.0.1", health_port).await;
    wait_for_tcp("127.0.0.1", graphql_port).await;

    // 1. /health/ready returns 200 while servers are up.
    let status = http_get_status("127.0.0.1", health_port, "/health/ready").await;
    assert_eq!(status, 200, "/health/ready should return 200 while serving");

    // 2. GraphQL subgraph answers an introspection POST. Proves the
    //    `post_service(GraphQL::new(schema))` handler is actually
    //    wired — a bare axum listener would 404.
    let graphql_status = http_post_json_status(
        "127.0.0.1",
        graphql_port,
        "/graphql",
        r#"{"query":"{__typename}"}"#,
    )
    .await;
    assert_eq!(
        graphql_status, 200,
        "/graphql should return 200 for an introspection POST"
    );

    // 3. Open an in-flight SubscribeEvents stream.
    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let channel = Channel::from_shared(endpoint.clone())
        .expect("endpoint")
        .connect()
        .await
        .expect("gRPC channel connect");
    let mut client = LightningPaymentGatewayClient::new(channel);
    let response = client
        .subscribe_events(SubscribeEventsRequest { after_sequence: 0 })
        .await
        .expect("subscribe_events");
    let mut stream = response.into_inner();

    // Give the subscription_loop a moment to register LISTEN. Without
    // this delay the cancel can fire while the loop is still starting,
    // which is a separate teardown path we are not exercising here.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. Cancel the shutdown token (the SIGTERM-equivalent step from
    //    AC11; the grace-sleep step is exercised by the supervisor
    //    in `src/cli.rs::run_cmd`, not here).
    cancel.cancel();

    // 5. The in-flight stream completes cleanly. Tonic represents a
    //    server-side drop as either `None` (clean EOF when the server
    //    drops its `tx` half) or `Some(Err(Status::cancelled |
    //    Status::unavailable))` depending on whether the cancel is
    //    observed inside the outbound stream wrap or at the transport
    //    layer. Both are valid graceful-drain outcomes.
    use tokio_stream::StreamExt;
    let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream completes within 5s of cancel");
    match next {
        None => {}
        Some(Err(ref status))
            if matches!(
                status.code(),
                tonic::Code::Cancelled | tonic::Code::Unavailable
            ) => {}
        other => panic!(
            "in-flight subscribe_events stream should close with None or \
             Cancelled/Unavailable on cancel, got: {other:?}"
        ),
    }

    // 6. All three server tasks join cleanly.
    let grpc_result = tokio::time::timeout(Duration::from_secs(5), grpc_handle)
        .await
        .expect("grpc server task joined within 5s of cancel")
        .expect("grpc server task did not panic");
    assert!(
        grpc_result.is_ok(),
        "run_grpc_server returned an error: {grpc_result:?}"
    );

    let http_result = tokio::time::timeout(Duration::from_secs(5), http_handle)
        .await
        .expect("health http task joined within 5s of cancel")
        .expect("health http task did not panic");
    assert!(
        http_result.is_ok(),
        "health::run returned an error: {http_result:?}"
    );

    let graphql_result = tokio::time::timeout(Duration::from_secs(5), graphql_handle)
        .await
        .expect("graphql server task joined within 5s of cancel")
        .expect("graphql server task did not panic");
    assert!(
        graphql_result.is_ok(),
        "run_graphql_server returned an error: {graphql_result:?}"
    );

    // 7. After the servers have exited, both the gRPC and GraphQL
    //    ports no longer accept TCP connections.
    let conn_after_shutdown_grpc = tokio::net::TcpStream::connect(("127.0.0.1", grpc_port)).await;
    assert!(
        conn_after_shutdown_grpc.is_err(),
        "gRPC port should refuse connections after server shutdown"
    );
    let conn_after_shutdown_graphql =
        tokio::net::TcpStream::connect(("127.0.0.1", graphql_port)).await;
    assert!(
        conn_after_shutdown_graphql.is_err(),
        "GraphQL port should refuse connections after server shutdown"
    );
}
