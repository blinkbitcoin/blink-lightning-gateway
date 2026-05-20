//! `LndClient` and the `LndApi` trait.
//!
//! ## Why a trait at the adapter boundary
//!
//! The architecture rejects trait abstractions in repos and adapters
//! (architecture L700) because they invite premature inversion. The
//! `LndApi` trait is a **deliberate exception bounded to the test-mocking
//! surface**: gRPC mocks via `wiremock` would require hand-encoded protobuf
//! payloads (fragile against any proto-schema drift), so the idiomatic Rust
//! pattern is a thin trait at the adapter boundary, mocked via `mockall`.
//! No code outside `src/lnd/` and `src/app/` should reach for this trait —
//! domain code calls into App, App calls `LndApi` — that's the layering.
//!
//! ## Runtime modes
//!
//! `LndClient` has two construction paths:
//!   - `connect(config)` — opens an mTLS+macaroon `tonic_lnd::Client`
//!     against a real LND instance. Every `LndApi` call routes through
//!     the real gRPC.
//!   - `boot_stub(config)` — leaves the inner client unset; every
//!     `LndApi` call returns `LndError::Stub`. Used by `cli::run_cmd`
//!     when the YAML has no `lnd:` block, and by tests that don't drive
//!     real LND.
//!
//! `subscribe_payments` (in `subscription.rs`) calls
//! `track_payments_stream` directly on the concrete `LndClient`; the
//! `LndApi` trait does not include streaming methods because the
//! integration suite drives `App::handle_payment_update` synthetically
//! (per Story 2.2 AC15 step 7) rather than threading a mock stream
//! through the trait.

use async_trait::async_trait;
use tonic_lnd::tonic::Streaming;
use tonic_lnd::{invoicesrpc, lnrpc, routerrpc};
use tracing::warn;

use crate::payment::{FailureReason, Hop};
use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Preimage, Pubkey};

use super::{
    config::LndConfig,
    error::LndError,
    invoice::{AddInvoiceParams, AddInvoiceResponse},
    payment::{
        FeeProbeParams, FeeProbeResponse, SendPaymentParams, SendPaymentResponse, SendPaymentStatus,
    },
};

/// Adapter contract the `App` coordinator + tests speak to. The
/// `mockall::automock` attribute generates `MockLndApi` for the lib's own
/// `cfg(test)` blocks. Integration tests in `tests/` (separate compilation
/// unit, no `cfg(test)` for the lib) hand-write a tiny stub impl.
#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait LndApi: Send + Sync {
    async fn add_invoice(&self, params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError>;

    async fn send_payment(
        &self,
        params: SendPaymentParams,
    ) -> Result<SendPaymentResponse, LndError>;

    async fn fee_probe(&self, params: FeeProbeParams) -> Result<FeeProbeResponse, LndError>;
}

/// Real LND adapter. `inner` is `Some` iff `connect` succeeded; `None`
/// in `boot_stub` mode (every RPC returns `LndError::Stub`).
#[derive(Clone)]
pub struct LndClient {
    #[allow(dead_code)]
    config: LndConfig,
    inner: Option<tonic_lnd::Client>,
}

impl std::fmt::Debug for LndClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = if self.inner.is_some() {
            "connected"
        } else {
            "boot_stub"
        };
        f.debug_struct("LndClient")
            .field("config", &self.config)
            .field("inner", &state)
            .finish()
    }
}

impl LndClient {
    /// Open an mTLS + macaroon channel to LND. `config.address` must
    /// start with `https://`. Reads `cert_path` + `macaroon_path` off
    /// disk; failures surface as `LndError::Connect`.
    pub async fn connect(config: LndConfig) -> Result<Self, LndError> {
        let address = config.address.clone();
        let cert_path = config.cert_path.clone();
        let macaroon_path = config.macaroon_path.clone();
        let inner = tonic_lnd::connect(address, cert_path, macaroon_path)
            .await
            .map_err(|e| LndError::Connect(format!("{e}")))?;
        Ok(Self {
            config,
            inner: Some(inner),
        })
    }

    /// Construct without attempting to connect. The binary entrypoint
    /// uses this when no `lnd:` block is in the YAML, so the gateway
    /// boots the gRPC + GraphQL + health surfaces with no LND upstream;
    /// every `LndApi` call returns `LndError::Stub`.
    pub fn boot_stub(config: LndConfig) -> Self {
        Self {
            config,
            inner: None,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.inner.is_some()
    }

    fn require_inner(&self) -> Result<tonic_lnd::Client, LndError> {
        self.inner.clone().ok_or(LndError::Stub)
    }

    /// Open the `Router/TrackPayments` stream. Used by `subscribe_payments`
    /// in `subscription.rs`. Returns `LndError::Stub` if this client was
    /// constructed via `boot_stub`.
    pub async fn track_payments_stream(&self) -> Result<Streaming<lnrpc::Payment>, LndError> {
        let mut inner = self.require_inner()?;
        let stream = inner
            .router()
            .track_payments(routerrpc::TrackPaymentsRequest {
                no_inflight_updates: false,
            })
            .await?
            .into_inner();
        Ok(stream)
    }

    /// Open the per-hash `Invoices/SubscribeSingleInvoice` stream.
    /// `SubscribeSingleInvoice` always emits the current invoice state
    /// on subscribe (per `invoices.proto:31-35`) — that's what makes
    /// the recovery sweep work for invoices that transitioned during outage.
    pub async fn subscribe_single_invoice_stream(
        &self,
        payment_hash: PaymentHash,
    ) -> Result<Streaming<lnrpc::Invoice>, LndError> {
        let mut inner = self.require_inner()?;
        let stream = inner
            .invoices()
            .subscribe_single_invoice(invoicesrpc::SubscribeSingleInvoiceRequest {
                r_hash: payment_hash.as_bytes().to_vec(),
            })
            .await?
            .into_inner();
        Ok(stream)
    }
}

#[async_trait]
impl LndApi for LndClient {
    async fn add_invoice(&self, params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        let mut inner = self.require_inner()?;
        let value_msat: i64 =
            params.amount_msat.as_u64().try_into().map_err(|_| {
                LndError::InvalidResponse("amount_msat exceeds i64::MAX".to_owned())
            })?;
        let invoice = lnrpc::Invoice {
            memo: params.memo.unwrap_or_default(),
            value_msat,
            expiry: i64::from(params.expiry_seconds),
            ..Default::default()
        };
        let resp = inner.lightning().add_invoice(invoice).await?.into_inner();
        let len = resp.r_hash.len();
        let r_hash: [u8; 32] = resp
            .r_hash
            .try_into()
            .map_err(|_| LndError::InvalidResponse(format!("r_hash length {len}, expected 32")))?;
        Ok(AddInvoiceResponse {
            payment_hash: PaymentHash::from(r_hash),
            bolt_invoice: BoltInvoice::new(resp.payment_request),
        })
    }

    async fn send_payment(
        &self,
        params: SendPaymentParams,
    ) -> Result<SendPaymentResponse, LndError> {
        let mut inner = self.require_inner()?;
        let fee_limit_msat: i64 =
            params.max_fee_msat.as_u64().try_into().map_err(|_| {
                LndError::InvalidResponse("max_fee_msat exceeds i64::MAX".to_owned())
            })?;
        let request = routerrpc::SendPaymentRequest {
            payment_request: params.bolt_invoice.into_inner(),
            fee_limit_msat,
            timeout_seconds: params.timeout_seconds as i32,
            no_inflight_updates: false,
            ..Default::default()
        };
        let mut stream = inner.router().send_payment_v2(request).await?.into_inner();
        // `SendPaymentV2` keeps the stream open and emits one Payment per
        // status transition. The first message is the decision we surface
        // synchronously (INITIATED/IN_FLIGHT, or immediate SUCCEEDED/FAILED).
        // Later transitions arrive via the separate `TrackPayments`
        // subscription, so we drop this stream after the first message —
        // tonic's Drop sends a cancel, which LND honors.
        let payment = stream.message().await?.ok_or_else(|| {
            LndError::InvalidResponse(
                "SendPaymentV2 stream closed without yielding a Payment".to_owned(),
            )
        })?;
        lnd_payment_to_send_response(payment)
    }

    async fn fee_probe(&self, params: FeeProbeParams) -> Result<FeeProbeResponse, LndError> {
        let mut inner = self.require_inner()?;
        let req = routerrpc::RouteFeeRequest {
            payment_request: params.bolt_invoice.into_inner(),
            ..Default::default()
        };
        let resp = inner.router().estimate_route_fee(req).await?.into_inner();
        if resp.failure_reason != lnrpc::PaymentFailureReason::FailureReasonNone as i32 {
            return Err(map_failure_reason_to_lnd_error(resp.failure_reason));
        }
        let fee_msat = MilliSatoshi::try_from(resp.routing_fee_msat).map_err(|e| {
            LndError::InvalidResponse(format!("estimate_route_fee.routing_fee_msat: {e}"))
        })?;
        // `time_lock_delay` is measured in blocks; surfaced through the
        // proto-equivalent `expiry_seconds` slot. Callers (App::fee_probe)
        // read only `fee_msat`.
        let expiry_seconds = u32::try_from(resp.time_lock_delay).unwrap_or(0);
        Ok(FeeProbeResponse {
            fee_msat,
            expiry_seconds,
        })
    }
}

/// Map an `lnrpc::Payment` (from `SendPaymentV2` or `TrackPayments`) to
/// our adapter's `SendPaymentResponse`. Public to `super` so
/// `subscription.rs` reuses it.
pub(super) fn lnd_payment_to_send_response(
    payment: lnrpc::Payment,
) -> Result<SendPaymentResponse, LndError> {
    let payment_hash = parse_hex_payment_hash(&payment.payment_hash)?;
    let payment_preimage = parse_preimage(&payment.payment_preimage, &payment.payment_hash);
    let status = map_payment_status(payment.status)?;
    // LND has been observed to set `fee_msat` to negative (-1) on early
    // Failed payments before fee accounting finalizes. Treat as 0 with a
    // warn so the state transition still completes — the `status` field
    // is the authoritative outcome, not `fee_msat`.
    //
    // Ceiling-round to whole-sat to mirror blink-core's `safe_fee`
    // (`Math.ceil(fee_mtokens / 1000)` inside the `lightning` npm lib).
    let fees_paid_msat = MilliSatoshi::try_from(payment.fee_msat)
        .unwrap_or_else(|e| {
            warn!(
                payment_hash = %payment.payment_hash,
                fee_msat = payment.fee_msat,
                error = %e,
                "LND payment.fee_msat invalid; defaulting to ZERO"
            );
            MilliSatoshi::ZERO
        })
        .round_up_to_sat();
    let route_hops = first_route_hops(&payment.htlcs, &payment.payment_hash);
    let failure_reason = if status == SendPaymentStatus::Failed {
        Some(map_failure_reason_to_typed(payment.failure_reason))
    } else {
        None
    };
    Ok(SendPaymentResponse {
        payment_hash,
        payment_preimage,
        status,
        fees_paid_msat,
        route_hops,
        failure_reason,
    })
}

fn parse_hex_payment_hash(s: &str) -> Result<PaymentHash, LndError> {
    s.parse()
        .map_err(|e| LndError::InvalidResponse(format!("payment.payment_hash: {e}")))
}

/// Parse LND `Payment.payment_preimage`. Empty string → `None` (in-flight).
/// 64-char hex → `Some(Preimage)`. Anything else is a wire-level anomaly:
/// returns `None` and warns with the offending bytes so the caller can
/// distinguish "absent" from "garbage" (a `Succeeded` payment with `None`
/// here is a hard error at the call site).
fn parse_preimage(s: &str, payment_hash_hex: &str) -> Option<Preimage> {
    if s.is_empty() {
        return None;
    }
    if s.len() != 64 {
        warn!(
            payment_hash = %payment_hash_hex,
            preimage_len = s.len(),
            "LND payment.payment_preimage wrong length (expected 64 hex chars)"
        );
        return None;
    }
    match s.parse() {
        Ok(p) => Some(p),
        Err(e) => {
            warn!(
                payment_hash = %payment_hash_hex,
                error = %e,
                preimage_hex = %s,
                "LND payment.payment_preimage not hex"
            );
            None
        }
    }
}

fn map_payment_status(value: i32) -> Result<SendPaymentStatus, LndError> {
    use lnrpc::payment::PaymentStatus;
    let status = PaymentStatus::try_from(value)
        .map_err(|_| LndError::InvalidResponse(format!("unknown PaymentStatus: {value}")))?;
    Ok(match status {
        PaymentStatus::Initiated | PaymentStatus::InFlight => SendPaymentStatus::InFlight,
        PaymentStatus::Succeeded => SendPaymentStatus::Succeeded,
        PaymentStatus::Failed => SendPaymentStatus::Failed,
        PaymentStatus::Unknown => {
            return Err(LndError::InvalidResponse(
                "LND returned PaymentStatus::Unknown".to_owned(),
            ));
        }
    })
}

fn map_failure_reason_to_lnd_error(value: i32) -> LndError {
    use lnrpc::PaymentFailureReason as P;
    match P::try_from(value).unwrap_or(P::FailureReasonError) {
        P::FailureReasonTimeout => LndError::PaymentTimeout,
        P::FailureReasonNoRoute => LndError::NoRoute,
        P::FailureReasonIncorrectPaymentDetails => LndError::IncorrectPaymentDetails,
        other => LndError::InvalidResponse(format!("LND failure reason: {other:?}")),
    }
}

fn map_failure_reason_to_typed(value: i32) -> FailureReason {
    use lnrpc::PaymentFailureReason as P;
    match P::try_from(value).unwrap_or(P::FailureReasonError) {
        P::FailureReasonNone => FailureReason::Other("none".to_owned()),
        P::FailureReasonTimeout => FailureReason::Timeout,
        P::FailureReasonNoRoute => FailureReason::NoRoute,
        P::FailureReasonInsufficientBalance => FailureReason::InsufficientBalance,
        P::FailureReasonIncorrectPaymentDetails => FailureReason::IncorrectPaymentDetails,
        P::FailureReasonError => FailureReason::Other("LND failure_reason: error".to_owned()),
        P::FailureReasonCanceled => FailureReason::Other("canceled".to_owned()),
    }
}

fn first_route_hops(htlcs: &[lnrpc::HtlcAttempt], payment_hash_hex: &str) -> Vec<Hop> {
    htlcs
        .iter()
        .find_map(|h| h.route.as_ref())
        .map(|route| {
            route
                .hops
                .iter()
                .filter_map(|hop| {
                    // Skip hops with unparsable pubkeys rather than storing
                    // garbage; route_hops is advisory and a missing hop is
                    // less harmful than one with a bogus identity.
                    let pub_key = match hop.pub_key.parse::<Pubkey>() {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(
                                payment_hash = %payment_hash_hex,
                                error = %e,
                                pubkey_hex = %hop.pub_key,
                                "LND hop.pub_key not parseable; skipping hop"
                            );
                            return None;
                        }
                    };
                    Some(Hop {
                        pub_key,
                        channel_id: hop.chan_id,
                        // Ceiling-round to whole-sat for consistency with the
                        // top-level `fees_paid_msat` invariant — hops are
                        // advisory but the unit should match downstream
                        // consumers' sat-resolution expectations.
                        fee_msat: MilliSatoshi::try_from(hop.fee_msat)
                            .unwrap_or(MilliSatoshi::ZERO)
                            .round_up_to_sat(),
                        amt_msat: MilliSatoshi::try_from(hop.amt_to_forward_msat)
                            .unwrap_or(MilliSatoshi::ZERO)
                            .round_up_to_sat(),
                        expiry: hop.expiry,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}
