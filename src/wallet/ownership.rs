//! Cross-subgraph wallet-ownership validation.
//!
//! blink-core owns the `Wallet` aggregate, so the gateway gates a
//! wallet-targeted op by asking Apollo Router — as the caller — which wallets
//! it owns (`me { defaultAccount { wallets { id } } }`) and checking the
//! target is in that set. Fail-closed: unreachable Router, GraphQL error, or
//! missing caller all DENY. Per-`sub` TTL cache amortizes repeated checks.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{instrument, warn};

use crate::primitives::WalletId;

/// Caller identity threaded from the GraphQL context. `jwt` is forwarded to
/// the Router (the auth authority); `sub` keys the cache only. Empty → the
/// request is unauthenticated and every check fails closed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CallerAuth {
    pub jwt: String,
    pub sub: String,
}

impl CallerAuth {
    pub fn new(jwt: impl Into<String>, sub: impl Into<String>) -> Self {
        Self {
            jwt: jwt.into(),
            sub: sub.into(),
        }
    }

    fn is_authenticated(&self) -> bool {
        !self.jwt.is_empty() && !self.sub.is_empty()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WalletOwnershipConfig {
    /// Apollo Router endpoint the membership query is issued against. Empty
    /// → `boot_stub` (fail-closed on every check).
    #[serde(default)]
    pub router_endpoint: String,
    /// TTL of the per-caller membership cache, seconds.
    #[serde(default = "default_cache_secs")]
    pub cache_secs: u64,
}

impl Default for WalletOwnershipConfig {
    fn default() -> Self {
        Self {
            router_endpoint: String::new(),
            cache_secs: default_cache_secs(),
        }
    }
}

fn default_cache_secs() -> u64 {
    60
}

/// Soft cap: when the cache reaches this many entries, `store` drops expired
/// ones before inserting. Bounds a long-running pod's cache to ≈ callers
/// active within the TTL (expiry is otherwise lazy — `cached` never evicts),
/// rather than growing with every distinct caller ever seen.
const CACHE_PRUNE_THRESHOLD: usize = 10_000;

#[derive(Debug, Error)]
pub enum WalletOwnershipError {
    #[error("wallet {0} is not owned by the caller")]
    NotOwned(WalletId),
    #[error("caller is not authenticated")]
    Unauthenticated,
    #[error("wallet-ownership router unreachable: {0}")]
    Unreachable(String),
    #[error("wallet-ownership query returned an error: {0}")]
    QueryError(String),
}

#[tonic::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait WalletOwnershipChecker: Send + Sync {
    /// `Ok(())` iff the caller owns `wallet_id`. Any other outcome is an
    /// `Err` that maps to `permission_denied` (fail-closed).
    async fn check(
        &self,
        caller: &CallerAuth,
        wallet_id: &WalletId,
    ) -> Result<(), WalletOwnershipError>;
}

const MEMBERSHIP_QUERY: &str = "query { me { defaultAccount { wallets { id } } } }";

#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    data: Option<MeData>,
    #[serde(default)]
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Deserialize)]
struct GraphQlError {
    message: String,
}

#[derive(Deserialize)]
struct MeData {
    #[serde(default)]
    me: Option<Me>,
}

#[derive(Deserialize)]
struct Me {
    #[serde(rename = "defaultAccount", default)]
    default_account: Option<DefaultAccount>,
}

#[derive(Deserialize)]
struct DefaultAccount {
    #[serde(default)]
    wallets: Vec<WalletRef>,
}

#[derive(Deserialize)]
struct WalletRef {
    id: String,
}

/// Apollo-Router-fronted checker with a TTL membership cache. `BootStub`
/// (empty endpoint) denies every check.
pub struct ApolloRouterOwnershipChecker {
    mode: Mode,
    ttl: Duration,
    // sub → (owned wallet-id set, cached_at). Mutex is fine — the locked
    // section is just a HashMap get/insert; the HTTP call is outside it.
    cache: Mutex<std::collections::HashMap<String, (HashSet<String>, Instant)>>,
}

enum Mode {
    Real {
        http: reqwest::Client,
        endpoint: String,
    },
    BootStub,
}

impl ApolloRouterOwnershipChecker {
    /// Build a real checker against `endpoint`. Falls back to `boot_stub`
    /// when `endpoint` is empty (ADR-0006 §Consequences).
    pub fn new(config: &WalletOwnershipConfig) -> Self {
        let ttl = Duration::from_secs(config.cache_secs);
        if config.router_endpoint.trim().is_empty() {
            warn!("walletOwnership.router_endpoint empty; using fail-closed boot stub");
            return Self::boot_stub(ttl);
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            mode: Mode::Real {
                http,
                endpoint: config.router_endpoint.clone(),
            },
            ttl,
            cache: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn boot_stub(ttl: Duration) -> Self {
        Self {
            mode: Mode::BootStub,
            ttl,
            cache: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Cache lookup. Returns the owned-set if a non-expired entry exists.
    fn cached(&self, sub: &str) -> Option<HashSet<String>> {
        let cache = self.cache.lock().expect("ownership cache poisoned");
        cache.get(sub).and_then(|(set, at)| {
            if at.elapsed() < self.ttl {
                Some(set.clone())
            } else {
                None
            }
        })
    }

    fn store(&self, sub: &str, set: HashSet<String>) {
        let mut cache = self.cache.lock().expect("ownership cache poisoned");
        // Opportunistic GC: only entries refreshed by a returning caller are
        // otherwise reclaimed, so sweep expired ones once the map grows past
        // the soft cap (keeps a long-lived pod bounded by active callers).
        if cache.len() >= CACHE_PRUNE_THRESHOLD {
            cache.retain(|_, (_, at)| at.elapsed() < self.ttl);
        }
        cache.insert(sub.to_owned(), (set, Instant::now()));
    }

    /// One membership round trip. Any transport/HTTP/GraphQL failure is an
    /// `Err` (the caller denies fail-closed).
    async fn fetch_owned_wallets(
        &self,
        caller: &CallerAuth,
    ) -> Result<HashSet<String>, WalletOwnershipError> {
        let (http, endpoint) = match &self.mode {
            Mode::Real { http, endpoint } => (http, endpoint),
            Mode::BootStub => {
                return Err(WalletOwnershipError::Unreachable(
                    "wallet-ownership checker not configured (boot stub)".to_owned(),
                ))
            }
        };

        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", caller.jwt))
            .map_err(|_| WalletOwnershipError::QueryError("invalid bearer token".to_owned()))?;
        bearer.set_sensitive(true);

        let resp = http
            .post(endpoint)
            .header(AUTHORIZATION, bearer)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .json(&serde_json::json!({ "query": MEMBERSHIP_QUERY }))
            .send()
            .await
            .map_err(|e| WalletOwnershipError::Unreachable(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(WalletOwnershipError::QueryError(format!(
                "router returned HTTP {}",
                resp.status()
            )));
        }

        let envelope: Envelope = resp
            .json()
            .await
            .map_err(|e| WalletOwnershipError::QueryError(format!("decode: {e}")))?;

        if let Some(errors) = envelope.errors {
            if let Some(first) = errors.into_iter().next() {
                return Err(WalletOwnershipError::QueryError(first.message));
            }
        }

        let wallets = envelope
            .data
            .and_then(|d| d.me)
            .and_then(|m| m.default_account)
            .map(|a| a.wallets)
            .ok_or_else(|| {
                WalletOwnershipError::QueryError("me.defaultAccount missing".to_owned())
            })?;

        Ok(wallets.into_iter().map(|w| w.id).collect())
    }
}

#[tonic::async_trait]
impl WalletOwnershipChecker for ApolloRouterOwnershipChecker {
    #[instrument(skip(self, caller), fields(wallet_id = %wallet_id))]
    async fn check(
        &self,
        caller: &CallerAuth,
        wallet_id: &WalletId,
    ) -> Result<(), WalletOwnershipError> {
        if !caller.is_authenticated() {
            return Err(WalletOwnershipError::Unauthenticated);
        }
        let target = wallet_id.to_string();

        if let Some(set) = self.cached(&caller.sub) {
            return if set.contains(&target) {
                Ok(())
            } else {
                Err(WalletOwnershipError::NotOwned(*wallet_id))
            };
        }

        let owned = self.fetch_owned_wallets(caller).await?;
        let result = if owned.contains(&target) {
            Ok(())
        } else {
            Err(WalletOwnershipError::NotOwned(*wallet_id))
        };
        // Cache the set, not the decision, so another wallet for the same
        // caller within the TTL is a hit.
        self.store(&caller.sub, owned);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn wallet() -> WalletId {
        WalletId::from(Uuid::now_v7())
    }

    #[tokio::test]
    async fn unauthenticated_caller_denies() {
        // Fail-closed: a request with no caller identity must deny before
        // any network call (guards the empty-jwt path).
        let checker = ApolloRouterOwnershipChecker::boot_stub(Duration::from_secs(60));
        let err = checker
            .check(&CallerAuth::default(), &wallet())
            .await
            .unwrap_err();
        assert!(matches!(err, WalletOwnershipError::Unauthenticated));
    }

    #[tokio::test]
    async fn boot_stub_denies_authenticated_caller() {
        // An empty router endpoint must still deny (fail-closed), even for a
        // well-formed caller — never silently approve.
        let checker = ApolloRouterOwnershipChecker::boot_stub(Duration::from_secs(60));
        let err = checker
            .check(&CallerAuth::new("jwt", "sub-1"), &wallet())
            .await
            .unwrap_err();
        assert!(matches!(err, WalletOwnershipError::Unreachable(_)));
    }

    #[test]
    fn store_prunes_expired_entries_past_the_cap() {
        // Guards the unbounded-growth fix: expiry is lazy (`cached` never
        // evicts), so without the prune-on-store the map would grow with every
        // distinct caller forever. With a 0s TTL every entry is expired, so
        // once the soft cap is crossed `store` must sweep the map back down
        // instead of letting it climb past the threshold.
        let checker = ApolloRouterOwnershipChecker::boot_stub(Duration::from_secs(0));
        for i in 0..=CACHE_PRUNE_THRESHOLD {
            checker.store(&format!("sub-{i}"), HashSet::new());
        }
        let len = checker.cache.lock().unwrap().len();
        assert!(
            len <= CACHE_PRUNE_THRESHOLD,
            "cache grew past the cap ({len} entries) — prune-on-store regressed"
        );
    }
}
