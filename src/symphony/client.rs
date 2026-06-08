//! `SymphonyClient` — synchronous spend-authorization handshake with
//! Symphony before LND `send_payment` (ADR-0003). Requests carry only
//! rail-neutral fields; blink-card's fiat/card fields are left behind.
//!
//! `LightningSymphonyClient` wraps the generated client over a lazily-
//! connected channel; a connect failure surfaces as `Unavailable`, which
//! `is_authorize_unavailable` turns into a fail-closed decline (never LND).

use serde::{Deserialize, Serialize};
use tonic::transport::Channel;
use tonic::{Code, Status};

use super::error::SymphonyError;
use crate::symphony_proto as proto;
use crate::symphony_proto::spend_authorization_service_client::SpendAuthorizationServiceClient;

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

/// Which Cala account the spend is gated against (ADR-0003 §4). Symphony
/// owns the account-naming scheme from `kind`; the LN path always uses
/// `WalletLiability`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountKind {
    WalletLiability,
    CardCollateral,
}

impl AccountKind {
    fn to_proto(self) -> proto::AccountKind {
        match self {
            Self::WalletLiability => proto::AccountKind::WalletLiability,
            Self::CardCollateral => proto::AccountKind::CardCollateral,
        }
    }
}

/// Typed account reference (ADR-0003 §4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRef {
    pub kind: AccountKind,
    pub id: String,
}

/// Gateway-agnostic spend-authorization request (ADR-0003): who, how much,
/// and the dedup/trace handles.
#[derive(Clone, Debug)]
pub struct SymphonyAuthorizeRequest {
    /// Trace correlation (== `payment_hash` hex, ADR-0002).
    pub correlation_id: String,
    pub account: AccountRef,
    /// Amount to authorize, satoshis. LN hold: `amount + max_fee`.
    /// Intraledger settle-inline: `amount` only (zero-fee, ADR-0007).
    pub sat_amount: u64,
    /// Idempotency key (== `payment_hash` hex, ADR-0002).
    pub idempotency_key: String,
    /// Generic, gateway-agnostic metadata Symphony interprets (ADR-0007).
    /// Empty (`{}`) for the default check-and-hold LN path; the intraledger
    /// path sets `{"intraledger":true,"recipient_wallet_id":...}` to request
    /// the settle-inline two-leg transfer. Kept rail-neutral on purpose — no
    /// LN-specific typed fields on the shared spend primitive.
    pub gateway_metadata: serde_json::Value,
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
    /// Synchronous atomic check + Cala hold (ADR-0003 §Decision-2). A
    /// transport error (Symphony unreachable, WIP handler's `unimplemented`)
    /// is the caller's signal to fail closed via `is_authorize_unavailable`.
    async fn authorize_spend(
        &self,
        request: SymphonyAuthorizeRequest,
    ) -> Result<SymphonyAuthorizeResponse, SymphonyError>;

    /// Void a previously-posted hold (orphan-hold reconciliation sweep,
    /// ADR-0003 §Consequences / AC10).
    async fn void_spend_authorization(
        &self,
        correlation_id: String,
        authorization_id: String,
    ) -> Result<(), SymphonyError>;
}

/// True for a transport / availability failure (unreachable, deadline, the
/// WIP handler's `unimplemented`) rather than a clean `Declined`. On true the
/// spend path fails closed — decline, never call LND. Mirrors blink-card's
/// `orchestrator_unavailable` (`blink-card/src/symphony/error.rs:37-55`).
pub fn is_authorize_unavailable(err: &SymphonyError) -> bool {
    match err {
        // Eager-connect path (unused with connect_lazy).
        SymphonyError::Transport(_) => true,
        SymphonyError::Status(status) => matches!(
            status.code(),
            Code::Unavailable
                | Code::Unknown
                | Code::DeadlineExceeded
                | Code::Cancelled
                // WIP Cala-template handler returns `Unimplemented` until
                // sign-off (AC6); treat as unavailable → fail-closed decline.
                | Code::Unimplemented
        ),
        // A clean `Declined` is a decision, not an availability failure.
        SymphonyError::Declined { .. } => false,
        // Boot-time config error — never reaches the runtime call path.
        SymphonyError::Config(_) => false,
    }
}

fn map_decline_reason(code: i32, message: &str) -> DeclineReason {
    use proto::SpendDeclineReason as R;
    let detail = |fallback: &str| {
        if message.is_empty() {
            fallback.to_owned()
        } else {
            message.to_owned()
        }
    };
    match R::try_from(code).unwrap_or(R::SpendDeclineUnspecified) {
        R::SpendDeclineInsufficientBalance => DeclineReason::InsufficientFunds,
        R::SpendDeclineAccountFrozen => DeclineReason::AccountFrozen,
        R::SpendDeclineAmountExceedsLimit => DeclineReason::AmountExceedsLimit,
        R::SpendDeclineAccountNotFound => DeclineReason::Other(detail("account not found")),
        R::SpendDeclineInternalError => DeclineReason::Other(detail("symphony internal error")),
        R::SpendDeclineUnspecified => DeclineReason::Other(detail("unspecified decline")),
    }
}

/// Real client over `SpendAuthorizationServiceClient`. `BootStub` is the
/// fail-closed fallback for a missing endpoint — every call returns
/// `Unavailable` so `is_authorize_unavailable` declines (production tightens
/// this to a hard boot bail in `cli.rs`).
#[derive(Clone)]
pub struct LightningSymphonyClient {
    mode: ClientMode,
}

#[derive(Clone)]
enum ClientMode {
    Real(SpendAuthorizationServiceClient<Channel>),
    BootStub,
}

impl LightningSymphonyClient {
    /// Build a client over a lazily-connected channel.
    pub fn connect_lazy(endpoint: &str) -> Result<Self, SymphonyError> {
        let channel = Channel::from_shared(endpoint.to_owned())
            .map_err(|e| SymphonyError::Config(format!("invalid grpc_endpoint: {e}")))?
            .connect_lazy();
        Ok(Self {
            mode: ClientMode::Real(SpendAuthorizationServiceClient::new(channel)),
        })
    }

    /// Fail-closed stub used when no endpoint is configured. Every call
    /// returns `Unavailable`.
    pub fn boot_stub() -> Self {
        Self {
            mode: ClientMode::BootStub,
        }
    }

    fn client(&self) -> Result<SpendAuthorizationServiceClient<Channel>, SymphonyError> {
        match &self.mode {
            ClientMode::Real(c) => Ok(c.clone()),
            ClientMode::BootStub => Err(SymphonyError::Status(Status::unavailable(
                "symphony spend-authorization client not configured (boot stub); failing closed",
            ))),
        }
    }
}

#[tonic::async_trait]
impl SymphonyClient for LightningSymphonyClient {
    async fn authorize_spend(
        &self,
        request: SymphonyAuthorizeRequest,
    ) -> Result<SymphonyAuthorizeResponse, SymphonyError> {
        let mut client = self.client()?;
        let proto_req = proto::SpendAuthorizeRequest {
            correlation_id: request.correlation_id,
            account: Some(proto::AccountRef {
                kind: request.account.kind.to_proto() as i32,
                id: request.account.id,
            }),
            sat_amount: request.sat_amount as i64,
            idempotency_key: request.idempotency_key,
            // Serialize the rail-neutral bag to a JSON-object string. A
            // serialization failure falls back to `{}` — never a panic.
            gateway_metadata: serde_json::to_string(&request.gateway_metadata)
                .unwrap_or_else(|_| "{}".to_owned()),
        };
        let resp = client.authorize_spend(proto_req).await?.into_inner();
        if resp.authorized {
            Ok(SymphonyAuthorizeResponse {
                status: SymphonyAuthorizeStatus::Approved,
                authorization_id: (!resp.authorization_id.is_empty())
                    .then_some(resp.authorization_id),
                decline_reason: None,
            })
        } else {
            Ok(SymphonyAuthorizeResponse {
                status: SymphonyAuthorizeStatus::Declined,
                authorization_id: (!resp.authorization_id.is_empty())
                    .then_some(resp.authorization_id),
                decline_reason: Some(map_decline_reason(
                    resp.decline_reason,
                    &resp.decline_message,
                )),
            })
        }
    }

    async fn void_spend_authorization(
        &self,
        correlation_id: String,
        authorization_id: String,
    ) -> Result<(), SymphonyError> {
        let mut client = self.client()?;
        client
            .void_spend_authorization(proto::SpendVoidRequest {
                correlation_id,
                authorization_id,
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_status_is_fail_closed() {
        let e = SymphonyError::Status(Status::unavailable("down"));
        assert!(is_authorize_unavailable(&e));
    }

    #[test]
    fn deadline_and_unimplemented_are_fail_closed() {
        // `Unimplemented` covers the WIP Cala-template handler (AC6): the
        // gateway must decline fail-closed, not surface a hard error.
        assert!(is_authorize_unavailable(&SymphonyError::Status(
            Status::deadline_exceeded("slow")
        )));
        assert!(is_authorize_unavailable(&SymphonyError::Status(
            Status::unimplemented("wip")
        )));
    }

    #[test]
    fn declined_is_not_unavailable() {
        // A clean Declined is a decision — it must NOT be reclassified as a
        // transport failure (which would mask insufficient-balance as infra).
        let e = SymphonyError::Declined {
            reason: DeclineReason::InsufficientFunds,
        };
        assert!(!is_authorize_unavailable(&e));
    }

    #[test]
    fn invalid_argument_is_not_unavailable() {
        // A genuine contract error fails the payment closed upstream, but the
        // classifier must not call it "unavailable".
        let e = SymphonyError::Status(Status::invalid_argument("bad"));
        assert!(!is_authorize_unavailable(&e));
    }

    #[test]
    fn decline_reason_maps_by_proto_code() {
        // Guards the hand-written proto-code → domain mapping; a swapped arm
        // would mislabel an insufficient-balance decline.
        assert_eq!(
            map_decline_reason(
                proto::SpendDeclineReason::SpendDeclineInsufficientBalance as i32,
                ""
            ),
            DeclineReason::InsufficientFunds
        );
        assert_eq!(
            map_decline_reason(
                proto::SpendDeclineReason::SpendDeclineAccountFrozen as i32,
                ""
            ),
            DeclineReason::AccountFrozen
        );
        assert_eq!(
            map_decline_reason(
                proto::SpendDeclineReason::SpendDeclineAmountExceedsLimit as i32,
                ""
            ),
            DeclineReason::AmountExceedsLimit
        );
        // Unmapped reasons carry the server message when present.
        assert_eq!(
            map_decline_reason(
                proto::SpendDeclineReason::SpendDeclineAccountNotFound as i32,
                "no such wallet"
            ),
            DeclineReason::Other("no such wallet".to_owned())
        );
    }

    #[test]
    fn account_kind_maps_to_proto() {
        // The LN spend path always uses WalletLiability; a cross-wire to
        // CardCollateral would route the hold to the wrong Cala account.
        assert_eq!(
            AccountKind::WalletLiability.to_proto(),
            proto::AccountKind::WalletLiability
        );
        assert_eq!(
            AccountKind::CardCollateral.to_proto(),
            proto::AccountKind::CardCollateral
        );
    }

    #[tokio::test]
    async fn boot_stub_fails_closed() {
        let client = LightningSymphonyClient::boot_stub();
        let err = client
            .authorize_spend(SymphonyAuthorizeRequest {
                correlation_id: "c".to_owned(),
                account: AccountRef {
                    kind: AccountKind::WalletLiability,
                    id: "w".to_owned(),
                },
                sat_amount: 1,
                idempotency_key: "c".to_owned(),
                gateway_metadata: serde_json::json!({}),
            })
            .await
            .unwrap_err();
        assert!(is_authorize_unavailable(&err));
    }
}
