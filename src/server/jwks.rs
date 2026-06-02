//! Remote-JWKS JWT validation for the GraphQL subgraph. Fetch the JWKS
//! once + refresh periodically, cache the RSA decoding keys, and verify the
//! caller JWT's RS256 signature + `exp` on each request. The validated
//! `sub` keys the wallet-ownership membership cache; the raw token is
//! forwarded to Apollo Router (the auth authority for the membership query).

use std::sync::RwLock;
use std::time::Duration;

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;

/// JWKS key cache refresh interval (matches blink-card's 30 min).
const REFRESH_INTERVAL: Duration = Duration::from_secs(1800);

const FETCH_RETRY_COUNT: usize = 10;
const FETCH_RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// Claims the gateway reads off a validated caller JWT. Only `sub` is
/// needed (the membership-cache key); `exp` is validated by `jsonwebtoken`
/// but not surfaced.
#[derive(Debug, Clone, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
}

#[derive(Debug, Error)]
pub enum JwksError {
    #[error("jwt validation failed: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("jwks fetch failed: {0}")]
    FetchFailed(String),
    #[error("no jwks key available")]
    NoKeyAvailable,
}

type KeysCache = RwLock<Vec<(Option<String>, DecodingKey)>>;

pub struct RemoteJwksDecoder {
    jwks_url: String,
    keys_cache: KeysCache,
    validation: Validation,
    client: reqwest::Client,
}

impl RemoteJwksDecoder {
    pub fn new(jwks_url: String) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;
        Self {
            jwks_url,
            keys_cache: RwLock::new(Vec::new()),
            validation,
            client: reqwest::Client::new(),
        }
    }

    async fn fetch_jwks(&self) -> Result<Vec<(Option<String>, DecodingKey)>, JwksError> {
        let mut last_error = None;
        for attempt in 1..=FETCH_RETRY_COUNT {
            match self.client.get(&self.jwks_url).send().await {
                Ok(response) => {
                    let jwks: Jwks = response
                        .json()
                        .await
                        .map_err(|e| JwksError::FetchFailed(e.to_string()))?;
                    return Ok(jwks
                        .keys
                        .into_iter()
                        .filter_map(|jwk| {
                            DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
                                .ok()
                                .map(|key| (jwk.kid, key))
                        })
                        .collect());
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    if attempt < FETCH_RETRY_COUNT {
                        tokio::time::sleep(FETCH_RETRY_BACKOFF).await;
                    }
                }
            }
        }
        Err(JwksError::FetchFailed(last_error.unwrap_or_else(|| {
            "jwks fetch retries exhausted".to_owned()
        })))
    }

    pub async fn refresh_keys(&self) -> Result<(), JwksError> {
        let keys = self.fetch_jwks().await?;
        *self.keys_cache.write().expect("jwks cache poisoned") = keys;
        Ok(())
    }

    /// Background loop: refresh now, then every `REFRESH_INTERVAL`. A refresh
    /// that fails even after its inner fetch-retry budget logs and waits for the
    /// next tick (existing keys stay live).
    pub async fn refresh_keys_periodically(&self) {
        loop {
            if let Err(e) = self.refresh_keys().await {
                warn!(error = %e, "JWKS key refresh failed; will retry");
            }
            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    }

    /// Verify signature + `exp` and return the claims. Tries the key matching
    /// the token's `kid` first, then falls back to every cached key (handles
    /// rotation), matching blink-card.
    pub fn decode(&self, token: &str) -> Result<JwtClaims, JwksError> {
        let header = decode_header(token)?;
        let keys = self.keys_cache.read().expect("jwks cache poisoned");

        if let Some(target_kid) = &header.kid {
            if let Some((_, key)) = keys
                .iter()
                .find(|(kid, _)| kid.as_deref() == Some(target_kid.as_str()))
            {
                return Ok(decode::<JwtClaims>(token, key, &self.validation)?.claims);
            }
        }

        let mut last_err = JwksError::NoKeyAvailable;
        for (_, key) in keys.iter() {
            match decode::<JwtClaims>(token, key, &self.validation) {
                Ok(data) => return Ok(data.claims),
                Err(e) => last_err = e.into(),
            }
        }
        Err(last_err)
    }
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: Option<String>,
    n: String,
    e: String,
}
