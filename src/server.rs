//! Server lifecycle module (gRPC + GraphQL listeners; graceful shutdown
//! coordination lives in `src/cli.rs::run_cmd`, which owns the
//! `tonic_health::HealthReporter` and the SIGTERM-aware ordering of
//! `set_not_serving` → grace sleep → token cancel).

pub mod config;
pub mod error;
pub mod graphql;
pub mod grpc;
pub mod jwks;

pub use config::{GrpcServerConfig, HealthServerConfig, SubgraphServerConfig};
pub use error::ServerError;
pub use graphql::run_graphql_server;
pub use grpc::run_grpc_server;
pub use jwks::{JwtClaims, RemoteJwksDecoder};
