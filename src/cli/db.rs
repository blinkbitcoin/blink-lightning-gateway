//! Postgres pool initialisation + migration helper

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    /// Postgres connection string. Always env-overridden via `PG_CON` /
    /// `--pg-con`; never read from YAML. `#[serde(skip)]` keeps it out
    /// of the YAML schema and out of `Config::default()` round-trips.
    #[serde(skip)]
    pub pg_con: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            pg_con: String::new(),
            pool_size: default_pool_size(),
        }
    }
}

fn default_pool_size() -> u32 {
    20
}

pub async fn run_migrations(config: &DbConfig) -> anyhow::Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&config.pg_con)
        .await?;

    sqlx::migrate!().run(&pool).await?;
    pool.close().await;
    Ok(())
}

pub async fn init_pool(config: &DbConfig) -> anyhow::Result<sqlx::PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.pool_size)
        .connect(&config.pg_con)
        .await?;

    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}
