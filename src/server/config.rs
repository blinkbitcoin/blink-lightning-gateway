//! Server-side config types

use serde::{Deserialize, Serialize};

/// Configuration for the federation v2 GraphQL subgraph server.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubgraphServerConfig {
    #[serde(default = "default_graphql_port")]
    pub port: u16,
    #[serde(default = "default_jwks_url")]
    pub jwks_url: String,
    /// PostgreSQL connection string for the `OutboxFanout`'s single
    /// `LISTEN gateway_events` ingest (used by the `lnInvoicePaymentStatus*`
    /// subscriptions). The CLI copies `db.pg_con` into this field before
    /// passing to `run_graphql_server`, the same way `GrpcServerConfig` is
    /// handled — sqlx's pool doesn't expose its config and the LISTEN side
    /// uses `tokio_postgres`.
    #[serde(default)]
    pub pg_config: String,
}

impl Default for SubgraphServerConfig {
    fn default() -> Self {
        Self {
            port: default_graphql_port(),
            jwks_url: default_jwks_url(),
            pg_config: String::new(),
        }
    }
}

/// Configuration for the gRPC server hosting
/// `lightning_payment_gateway.LightningPaymentGateway/SubscribeEvents`
/// and the `grpc.health.v1.Health` service.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcServerConfig {
    #[serde(default = "default_grpc_port")]
    pub port: u16,
    /// PostgreSQL connection string for the long-lived `LISTEN
    /// gateway_events` connection used by `subscription_loop`. The CLI
    /// copies `db.pg_con` into this field before passing to `run_grpc_server`.
    #[serde(default)]
    pub pg_config: String,
    /// HTTP/2 keepalive interval — sends PING frames to detect dead
    /// connections.
    #[serde(default = "default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
    /// HTTP/2 keepalive timeout — connection closed if no PONG within
    /// this window.
    #[serde(default = "default_keepalive_timeout_secs")]
    pub keepalive_timeout_secs: u64,
    /// Seconds the supervisor sleeps between flipping the gRPC health
    /// service to `NotServing` and cancelling the shutdown token. The
    /// grace window lets Kubernetes observe the NotServing flip and
    /// remove the pod from the gRPC LB endpoints before this pod
    /// refuses connections (see AC11 in story 2.1).
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            port: default_grpc_port(),
            pg_config: String::new(),
            keepalive_interval_secs: default_keepalive_interval_secs(),
            keepalive_timeout_secs: default_keepalive_timeout_secs(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
        }
    }
}

/// Configuration for the HTTP health probe server.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthServerConfig {
    #[serde(default = "default_health_port")]
    pub port: u16,
}

impl Default for HealthServerConfig {
    fn default() -> Self {
        Self {
            port: default_health_port(),
        }
    }
}

fn default_graphql_port() -> u16 {
    6691
}

fn default_grpc_port() -> u16 {
    6690
}

fn default_jwks_url() -> String {
    "http://localhost:4456/.well-known/jwks.json".to_owned()
}

fn default_health_port() -> u16 {
    8080
}

fn default_keepalive_interval_secs() -> u64 {
    30
}

fn default_keepalive_timeout_secs() -> u64 {
    10
}

fn default_shutdown_grace_secs() -> u64 {
    5
}
