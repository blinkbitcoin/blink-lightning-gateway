//! `SymphonyClient` — the gateway's synchronous gRPC handshake with
//! Symphony before LND `send_payment`.
//!
//! Slice 2 lands the trait + a stub `LightningSymphonyClient` that
//! always returns `Approved`. The real gRPC roundtrip against
//! Symphony's `LightningAuthorizationService` (a new RPC; today's
//! Symphony only carries `CardAuthorizationService`) lands in the
//! cross-repo PR (Story 2.2 AC14) and is wired on the gateway side
//! by Story 3.1 per ADR-0001's stub-un-stub schedule.
//!
//! Trait shape mirrors `blink-card/src/symphony/client.rs:24-110`, but
//! the request message is deliberately NOT a copy of blink-card's —
//! per ADR-0003 the spend-gate is gateway-agnostic and carries only
//! rail-neutral fields. blink-card's `original_usd_cents` /
//! `exchange_rate` / `merchant_info` / request-side `authorization_id`
//! are fiat/card-specific and are left behind (CLAUDE.md: "blink-card
//! → leave behind card-specific code"). The typed `AccountRef` from
//! ADR-0003 §4 lands with the Story 3.1 un-stub; Slice 2 keeps a plain
//! `account_id` string on the stub path.

use serde::{Deserialize, Serialize};

use super::error::SymphonyError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SymphonyAuthorizeStatus {
    Approved,
    Declined,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclineReason {
    InsufficientFunds,
    AccountFrozen,
    AmountExceedsLimit,
    Other(String),
}

/// Gateway-agnostic spend-authorization request (ADR-0003). Only
/// rail-neutral fields: who, how much, and the dedup/trace handles.
#[derive(Clone, Debug)]
pub struct SymphonyAuthorizeRequest {
    /// Correlation id for tracing (== `payment_hash` hex, per ADR-0002).
    pub correlation_id: String,
    /// Account the spend is gated against. Slice 2 passes the
    /// `wallet_id` string; ADR-0003's typed `AccountRef { kind, id }`
    /// replaces this in the Story 3.1 un-stub.
    pub account_id: String,
    /// Amount to authorize, in satoshis.
    pub sat_amount: u64,
    /// Idempotency key (== `payment_hash` hex, per ADR-0002).
    pub idempotency_key: String,
}

#[derive(Clone, Debug)]
pub struct SymphonyAuthorizeResponse {
    pub status: SymphonyAuthorizeStatus,
    pub authorization_id: Option<String>,
    pub decline_reason: Option<DeclineReason>,
}

#[tonic::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait SymphonyClient: Send + Sync {
    async fn authorize_spend(
        &self,
        request: SymphonyAuthorizeRequest,
    ) -> Result<SymphonyAuthorizeResponse, SymphonyError>;
}

/// STUB(story-3.1): always returns `Approved`. The real synchronous
/// gRPC handshake to Symphony's `LightningAuthorizationService` lands
/// in the cross-repo Symphony PR (Story 2.2 AC14); the gateway-side
/// wiring lands in Story 3.1. Loud in code so reviewers know what is
/// deferred and what is wired.
#[derive(Clone, Debug, Default)]
pub struct LightningSymphonyClient {
    #[allow(dead_code)]
    endpoint: String,
}

impl LightningSymphonyClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

#[tonic::async_trait]
impl SymphonyClient for LightningSymphonyClient {
    async fn authorize_spend(
        &self,
        request: SymphonyAuthorizeRequest,
    ) -> Result<SymphonyAuthorizeResponse, SymphonyError> {
        ::tracing::debug!(
            correlation_id = %request.correlation_id,
            sat_amount = request.sat_amount,
            "STUB(story-3.1): authorize_spend always-approved"
        );
        Ok(SymphonyAuthorizeResponse {
            status: SymphonyAuthorizeStatus::Approved,
            authorization_id: Some(request.correlation_id),
            decline_reason: None,
        })
    }
}
