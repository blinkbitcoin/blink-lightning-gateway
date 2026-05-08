//! GraphQL scalar/type/interface definitions for the `lnInvoiceCreate` op.
//! Slice 1a only carries what the mutation needs; sibling ops add their own
//! types as they land. SDL is checked byte-against-galoy in Story 5.3 (CI
//! gate); for now the manual diff is recorded in this story's Completion
//! Notes.

use async_graphql::{
    InputObject, InputValueError, InputValueResult, Object, Scalar, ScalarType, SimpleObject, Value,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash as DomainPaymentHash, WalletId as DomainWalletId,
};

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

/// Galoy parses `SatAmount` from JSON Number (positive integer); we accept
/// String too for the test rig that sends `amount: "1000"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SatAmount(pub u64);

#[Scalar(name = "SatAmount")]
impl ScalarType for SatAmount {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::Number(n) => n
                .as_u64()
                .ok_or_else(|| InputValueError::custom("SatAmount must be a non-negative integer"))
                .map(SatAmount),
            Value::String(s) => s
                .parse::<u64>()
                .map(SatAmount)
                .map_err(|e| InputValueError::custom(format!("invalid SatAmount: {e}"))),
            other => Err(InputValueError::expected_type(other)),
        }
    }

    fn to_value(&self) -> Value {
        Value::Number(self.0.into())
    }
}

impl SatAmount {
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn to_msat(self) -> MilliSatoshi {
        MilliSatoshi::new(self.0 * 1000)
    }
}

/// Expiry in minutes (galoy: `Minutes`). Range 1..=1440 (24h).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Minutes(pub u32);

#[Scalar(name = "Minutes")]
impl ScalarType for Minutes {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::Number(n) => n
                .as_u64()
                .and_then(|v| u32::try_from(v).ok())
                .ok_or_else(|| InputValueError::custom("Minutes out of range"))
                .map(Minutes),
            Value::String(s) => s
                .parse::<u32>()
                .map(Minutes)
                .map_err(|e| InputValueError::custom(format!("invalid Minutes: {e}"))),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::Number(self.0.into())
    }
}

/// External reference id supplied by the client (galoy: `TxExternalId`).
/// Slice 1a accepts and ignores; full implementation lands later.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxExternalId(pub String);

#[Scalar(name = "TxExternalId")]
impl ScalarType for TxExternalId {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(TxExternalId(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

/// Memo (galoy: `Memo`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memo(pub String);

#[Scalar(name = "Memo")]
impl ScalarType for Memo {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(Memo(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

/// `WalletId` — UUID. Wraps the domain's `WalletId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletId(pub DomainWalletId);

#[Scalar(name = "WalletId")]
impl ScalarType for WalletId {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => DomainWalletId::from_str(&s)
                .map(WalletId)
                .map_err(|e| InputValueError::custom(format!("invalid WalletId: {e}"))),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<WalletId> for DomainWalletId {
    fn from(w: WalletId) -> Self {
        w.0
    }
}

/// `PaymentHash` — 64-char hex. Wraps the domain `PaymentHash`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaymentHash(pub DomainPaymentHash);

#[Scalar(name = "PaymentHash")]
impl ScalarType for PaymentHash {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => DomainPaymentHash::from_str(&s)
                .map(PaymentHash)
                .map_err(|e| InputValueError::custom(format!("invalid PaymentHash: {e}"))),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/// `LnPaymentRequest` — opaque BOLT11.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LnPaymentRequest(pub String);

#[Scalar(name = "LnPaymentRequest")]
impl ScalarType for LnPaymentRequest {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(LnPaymentRequest(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

impl From<BoltInvoice> for LnPaymentRequest {
    fn from(b: BoltInvoice) -> Self {
        Self(b.into_inner())
    }
}

/// `LnPaymentSecret` — 32-byte secret as hex. Slice 1a hard-codes empty;
/// real value comes from LND's `add_invoice` `payment_addr` field once
/// wired (Story 1.6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LnPaymentSecret(pub String);

#[Scalar(name = "LnPaymentSecret")]
impl ScalarType for LnPaymentSecret {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(LnPaymentSecret(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

// ---------------------------------------------------------------------------
// Input + Payload + LnInvoice
// ---------------------------------------------------------------------------

#[derive(InputObject)]
pub struct LnInvoiceCreateInput {
    /// Amount in satoshis.
    pub amount: SatAmount,
    /// Optional invoice expiration time in minutes.
    pub expires_in: Option<Minutes>,
    pub external_id: Option<TxExternalId>,
    /// Optional memo for the lightning invoice.
    pub memo: Option<Memo>,
    /// Wallet ID for a BTC wallet belonging to the current account.
    pub wallet_id: WalletId,
}

/// Concrete error shape returned in `LnInvoicePayload.errors`. Galoy's
/// schema declares an `interface Error` with multiple concrete impls;
/// Story 5.1 adds the interface and per-error-class types when it builds
/// out the remaining 26 operations. Slice 1a returns only the generic
/// message.
#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "GraphqlError")]
pub struct GraphqlError {
    pub message: String,
}

impl GraphqlError {
    pub fn from_message(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
        }
    }
}

#[derive(SimpleObject)]
pub struct LnInvoice {
    pub payment_hash: PaymentHash,
    pub payment_request: LnPaymentRequest,
    pub payment_secret: LnPaymentSecret,
    pub satoshis: SatAmount,
}

pub struct LnInvoicePayload {
    pub errors: Vec<GraphqlError>,
    pub invoice: Option<LnInvoice>,
}

#[Object]
impl LnInvoicePayload {
    async fn errors(&self) -> &[GraphqlError] {
        &self.errors
    }
    async fn invoice(&self) -> Option<&LnInvoice> {
        self.invoice.as_ref()
    }
}
