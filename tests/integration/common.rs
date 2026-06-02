//! Shared test fixtures for the integration suite.
//!
//! `TestDatabase` boots a Postgres testcontainer with retry logic on both
//! container startup and pool connection — both can fail under parallel
//! test load when Docker is slow to map ports or accept connections.

use std::sync::Arc;
use std::time::Duration;

use blink_lightning_gateway::primitives::WalletId;
use blink_lightning_gateway::wallet::{CallerAuth, WalletOwnershipChecker, WalletOwnershipError};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres as PgImage;

/// Hand-written `WalletOwnershipChecker` stub — the integration suite can't
/// see the lib's `mockall::automock` mocks (gated on lib `cfg(test)`), so we
/// hand-roll one per the CLAUDE.md convention. `allow` approves every check
/// (the default for tests that aren't exercising ownership); `deny` rejects.
pub struct CannedWalletOwnership {
    allow: bool,
}

impl CannedWalletOwnership {
    pub fn allow() -> Arc<dyn WalletOwnershipChecker> {
        Arc::new(Self { allow: true })
    }

    pub fn deny() -> Arc<dyn WalletOwnershipChecker> {
        Arc::new(Self { allow: false })
    }
}

#[tonic::async_trait]
impl WalletOwnershipChecker for CannedWalletOwnership {
    async fn check(
        &self,
        _caller: &CallerAuth,
        wallet_id: &WalletId,
    ) -> Result<(), WalletOwnershipError> {
        if self.allow {
            Ok(())
        } else {
            Err(WalletOwnershipError::NotOwned(*wallet_id))
        }
    }
}

const CONTAINER_START_MAX_RETRIES: u32 = 3;
const POOL_CONNECT_MAX_RETRIES: u32 = 5;
const RETRY_BASE_DELAY_MS: u64 = 500;

/// Test database with a running Postgres container, a connected pool, and
/// the connection URL. The container is held inside the struct so it stays
/// alive for the test's lifetime.
pub struct TestDatabase {
    pub pool: PgPool,
    /// Postgres connection URL — exposed so tests can open a separate
    /// `tokio_postgres` connection for LISTEN/NOTIFY.
    pub url: String,
    _container: ContainerAsync<PgImage>,
}

impl TestDatabase {
    pub async fn new() -> anyhow::Result<Self> {
        let (container, url) = Self::start_container_with_retry()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let pool = Self::connect_with_retry(&url)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        sqlx::migrate!().run(&pool).await?;

        Ok(Self {
            pool,
            url,
            _container: container,
        })
    }

    async fn start_container_with_retry() -> Result<(ContainerAsync<PgImage>, String), String> {
        let mut last_error = String::new();

        for attempt in 1..=CONTAINER_START_MAX_RETRIES {
            match PgImage::default().start().await {
                Ok(container) => match container.get_host_port_ipv4(5432).await {
                    Ok(port) => {
                        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
                        return Ok((container, url));
                    }
                    Err(e) => last_error = e.to_string(),
                },
                Err(e) => last_error = e.to_string(),
            }

            if attempt < CONTAINER_START_MAX_RETRIES {
                let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * attempt as u64);
                eprintln!(
                    "Container startup attempt {attempt}/{CONTAINER_START_MAX_RETRIES} failed: \
                     {last_error}. Retrying in {delay:?}..."
                );
                tokio::time::sleep(delay).await;
            }
        }

        Err(format!(
            "Failed to start container after {CONTAINER_START_MAX_RETRIES} attempts. \
             Last error: {last_error}"
        ))
    }

    async fn connect_with_retry(url: &str) -> Result<PgPool, String> {
        let mut last_error = String::new();

        for attempt in 1..=POOL_CONNECT_MAX_RETRIES {
            match PgPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(10))
                .connect(url)
                .await
            {
                Ok(pool) => return Ok(pool),
                Err(e) => {
                    last_error = e.to_string();
                    if attempt < POOL_CONNECT_MAX_RETRIES {
                        let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * attempt as u64);
                        eprintln!(
                            "Pool connect attempt {attempt}/{POOL_CONNECT_MAX_RETRIES} failed: \
                             {last_error}. Retrying in {delay:?}..."
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(format!(
            "Failed to connect after {POOL_CONNECT_MAX_RETRIES} attempts. \
             Last error: {last_error}"
        ))
    }
}
