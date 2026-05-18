//! Root `Config` for the gateway binary. Mirrors
//!
//! Every field uses `#[serde(default)]` so a partial `ln-gateway.yml`
//! (or an empty file) still produces a valid `Config`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::db::DbConfig;
use crate::lnd::LndConfig;
use crate::server::config::{GrpcServerConfig, HealthServerConfig, SubgraphServerConfig};
use crate::symphony::SymphonyConfig;
use crate::tracing::TracingConfig;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub db: DbConfig,
    #[serde(default)]
    pub subgraph_server: SubgraphServerConfig,
    #[serde(default)]
    pub grpc_server: GrpcServerConfig,
    #[serde(default)]
    pub health_server: HealthServerConfig,
    #[serde(default)]
    pub symphony: SymphonyConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lnd: Option<LndConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracing: Option<TracingConfig>,
}

pub struct EnvOverride {
    pub pg_con: String,
}

impl Config {
    pub fn from_path(
        path: impl AsRef<Path>,
        EnvOverride { pg_con }: EnvOverride,
    ) -> anyhow::Result<Self> {
        let body = std::fs::read_to_string(path).context("Couldn't read config file")?;
        let mut config: Config = if body.trim().is_empty() {
            Config::default()
        } else {
            serde_yaml::from_str(&body).context("Couldn't parse config file")?
        };
        config.db.pg_con = pg_con;
        Ok(config)
    }
}
