//! GraphQL scalar/type/interface definitions for the `lnInvoiceCreate` op.
//! Slice 1a only carries what the mutation needs; sibling ops add their own
//! types as they land. SDL is checked byte-against-galoy in Story 5.3 (CI
//! gate); for now the manual diff is recorded in this story's Completion
//! Notes.

use async_graphql::{
    Enum, InputObject, InputValueError, InputValueResult, Object, Scalar, ScalarType, SimpleObject,
    Value,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash as DomainPaymentHash, WalletId as DomainWalletId,
};

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

/// Non-negative integer amount in satoshis. Accepts JSON Number or String.
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

/// Duration in minutes.
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

/// Client-supplied external reference id for the transaction.
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

/// Free-form memo string.
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

/// UUID identifying a wallet.
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

/// 32-byte SHA-256 payment hash, hex-encoded (64 lowercase chars).
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

/// BOLT11 invoice string.
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

/// BOLT11 payment secret (32-byte `payment_addr`, hex-encoded).
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

/// Error returned in a payload's `errors` array.
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

// ---------------------------------------------------------------------------
// Outbound payment types (Slice 2)
// ---------------------------------------------------------------------------

/// Outcome of a payment-send mutation.
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "PaymentSendResult")]
pub enum PaymentSendResult {
    AlreadyPaid,
    Failure,
    Pending,
    Success,
}

#[derive(InputObject)]
#[graphql(name = "LnInvoicePaymentInput")]
pub struct LnInvoicePaymentInput {
    pub memo: Option<Memo>,
    pub payment_request: LnPaymentRequest,
    pub wallet_id: WalletId,
}

#[derive(InputObject)]
#[graphql(name = "LnInvoiceFeeProbeInput")]
pub struct LnInvoiceFeeProbeInput {
    pub payment_request: LnPaymentRequest,
    pub wallet_id: WalletId,
}

/// Settled transaction record.
#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "Transaction")]
pub struct Transaction {
    pub id: String,
}

pub struct PaymentSendPayload {
    pub errors: Vec<GraphqlError>,
    pub status: Option<PaymentSendResult>,
    pub transaction: Option<Transaction>,
}

#[Object]
impl PaymentSendPayload {
    async fn errors(&self) -> &[GraphqlError] {
        &self.errors
    }
    async fn status(&self) -> Option<PaymentSendResult> {
        self.status
    }
    async fn transaction(&self) -> Option<&Transaction> {
        self.transaction.as_ref()
    }
}

pub struct SatAmountPayload {
    pub amount: Option<SatAmount>,
    pub errors: Vec<GraphqlError>,
}

#[Object]
impl SatAmountPayload {
    async fn amount(&self) -> Option<SatAmount> {
        self.amount
    }
    async fn errors(&self) -> &[GraphqlError] {
        &self.errors
    }
}

// ---------------------------------------------------------------------------
// Invoice payment-status subscription types (Slice 6) — galoy parity.
// SDL must match `blink/core/api/src/graphql/public/schema.graphql`
// byte-for-byte (Story 5.1 diffs against galoy's SDL): enum :597-601,
// inputs :694-704, payload :706-712. No `CANCELLED` value — an LN
// cancellation surfaces on the wire as `EXPIRED` (ADR-0008).
// ---------------------------------------------------------------------------

/// Wire status of an invoice. `Canceled` (gateway-internal) maps to
/// `EXPIRED`; there is deliberately no `CANCELLED` value (galoy parity).
/// async-graphql screaming-snake-cases the variants, so these render as
/// `EXPIRED | PAID | PENDING`.
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "InvoicePaymentStatus")]
pub enum InvoicePaymentStatus {
    Expired,
    Paid,
    Pending,
}

/// BOLT11 payment preimage (32-byte preimage, hex-encoded). Modeled on
/// the `LnPaymentSecret` scalar; galoy's `paymentPreimage` field type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LnPaymentPreImage(pub String);

#[Scalar(name = "LnPaymentPreImage")]
impl ScalarType for LnPaymentPreImage {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(LnPaymentPreImage(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

#[derive(InputObject)]
#[graphql(name = "LnInvoicePaymentStatusByHashInput")]
pub struct LnInvoicePaymentStatusByHashInput {
    pub payment_hash: PaymentHash,
}

#[derive(InputObject)]
#[graphql(name = "LnInvoicePaymentStatusByPaymentRequestInput")]
pub struct LnInvoicePaymentStatusByPaymentRequestInput {
    pub payment_request: LnPaymentRequest,
}

#[derive(InputObject)]
#[graphql(name = "LnInvoicePaymentStatusInput")]
pub struct LnInvoicePaymentStatusInput {
    pub payment_request: LnPaymentRequest,
}

/// All fields `Option` exactly as galoy (`schema.graphql:706-712`).
pub struct LnInvoicePaymentStatusPayload {
    pub errors: Vec<GraphqlError>,
    pub payment_hash: Option<PaymentHash>,
    pub payment_preimage: Option<LnPaymentPreImage>,
    pub payment_request: Option<LnPaymentRequest>,
    pub status: Option<InvoicePaymentStatus>,
}

#[Object(name = "LnInvoicePaymentStatusPayload")]
impl LnInvoicePaymentStatusPayload {
    async fn errors(&self) -> &[GraphqlError] {
        &self.errors
    }
    async fn payment_hash(&self) -> Option<&PaymentHash> {
        self.payment_hash.as_ref()
    }
    async fn payment_preimage(&self) -> Option<&LnPaymentPreImage> {
        self.payment_preimage.as_ref()
    }
    async fn payment_request(&self) -> Option<&LnPaymentRequest> {
        self.payment_request.as_ref()
    }
    async fn status(&self) -> Option<InvoicePaymentStatus> {
        self.status
    }
}
