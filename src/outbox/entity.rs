//! Outbox event types — the bridge between the gateway's typed domain
//! events and the standardized 8-event vocabulary Symphony consumes
//! (architecture L1042-1052).
//!
//! `GatewayEventType` here is hand-written because Story 1.4 does NOT carry
//! proto codegen (deferred to 1.5). Story 1.5's
//! `proto/lightning_payment_gateway.proto` will generate a structurally
//! identical enum; at that point the two can be unified or interop'd via
//! `From`. The values + integer tags here MUST match the proto enum so the
//! string-form column in `outbox_events.event_type` lines up across the
//! transition.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use super::error::OutboxError;

/// Standardized 8-event vocabulary — same shape as
/// `blink-card/proto/card_payment_gateway.proto::GatewayEventType`. Field
/// tags 1..=8 are reserved by the proto; do not renumber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GatewayEventType {
    OutgoingPaymentInitiated,
    OutgoingPaymentCompleted,
    OutgoingPaymentFailed,
    OutgoingPaymentReversed,
    IncomingPaymentReceived,
    IncomingPaymentPending,
    IncomingPaymentConfirmed,
    IncomingPaymentCanceled,
}

impl GatewayEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OutgoingPaymentInitiated => "OUTGOING_PAYMENT_INITIATED",
            Self::OutgoingPaymentCompleted => "OUTGOING_PAYMENT_COMPLETED",
            Self::OutgoingPaymentFailed => "OUTGOING_PAYMENT_FAILED",
            Self::OutgoingPaymentReversed => "OUTGOING_PAYMENT_REVERSED",
            Self::IncomingPaymentReceived => "INCOMING_PAYMENT_RECEIVED",
            Self::IncomingPaymentPending => "INCOMING_PAYMENT_PENDING",
            Self::IncomingPaymentConfirmed => "INCOMING_PAYMENT_CONFIRMED",
            Self::IncomingPaymentCanceled => "INCOMING_PAYMENT_CANCELED",
        }
    }
}

impl fmt::Display for GatewayEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GatewayEventType {
    type Err = OutboxError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "OUTGOING_PAYMENT_INITIATED" => Ok(Self::OutgoingPaymentInitiated),
            "OUTGOING_PAYMENT_COMPLETED" => Ok(Self::OutgoingPaymentCompleted),
            "OUTGOING_PAYMENT_FAILED" => Ok(Self::OutgoingPaymentFailed),
            "OUTGOING_PAYMENT_REVERSED" => Ok(Self::OutgoingPaymentReversed),
            "INCOMING_PAYMENT_RECEIVED" => Ok(Self::IncomingPaymentReceived),
            "INCOMING_PAYMENT_PENDING" => Ok(Self::IncomingPaymentPending),
            "INCOMING_PAYMENT_CONFIRMED" => Ok(Self::IncomingPaymentConfirmed),
            "INCOMING_PAYMENT_CANCELED" => Ok(Self::IncomingPaymentCanceled),
            other => Err(OutboxError::UnknownEventType(other.to_owned())),
        }
    }
}

/// Gateway-specific domain events — what actually happened in our domain
/// (e.g. `LightningInvoiceCreated`). `to_standardized()` maps each onto the
/// 8-event vocabulary for the consumer-side stream.
///
/// Slice 1a only emits `LightningInvoiceCreated`. Other variants
/// (`LightningInvoiceSettled`, `LightningPaymentInitiated`, etc.) land
/// alongside their owning slices.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayDomainEvent {
    LightningInvoiceCreated,
}

impl GatewayDomainEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LightningInvoiceCreated => "lightning_invoice_created",
        }
    }

    /// Map to the standardized 8-event vocabulary. The choice for
    /// `LightningInvoiceCreated` → `IncomingPaymentPending` reflects that
    /// invoice creation registers an *intent* to receive a payment that has
    /// not yet been confirmed; ADR #2 (Story 1.5) confirms or revises this.
    pub fn to_standardized(self) -> GatewayEventType {
        match self {
            Self::LightningInvoiceCreated => GatewayEventType::IncomingPaymentPending,
        }
    }
}

impl fmt::Display for GatewayDomainEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GatewayDomainEvent {
    type Err = OutboxError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lightning_invoice_created" => Ok(Self::LightningInvoiceCreated),
            other => Err(OutboxError::UnknownEventType(other.to_owned())),
        }
    }
}

/// Persisted outbox event — fully hydrated row from `outbox_events`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxEvent {
    pub sequence: i64,
    pub correlation_id: String,
    pub domain_event: GatewayDomainEvent,
    pub event_type: GatewayEventType,
    pub reference_id: String,
    pub sat_amount: i64,
    pub currency: String,
    pub timestamp: DateTime<Utc>,
    pub gateway_metadata: serde_json::Value,
}

/// Caller-provided fields for a new outbox row. The publisher derives
/// `event_type` from `domain_event.to_standardized()` and `sequence` from
/// the `BIGSERIAL` column at insert time, so they're absent here.
#[derive(Clone, Debug)]
pub struct NewOutboxEvent {
    pub correlation_id: String,
    pub domain_event: GatewayDomainEvent,
    pub reference_id: String,
    pub sat_amount: i64,
    pub currency: String,
    pub timestamp: DateTime<Utc>,
    pub gateway_metadata: serde_json::Value,
}

impl NewOutboxEvent {
    /// Convenience constructor for the common Slice-1a case: a freshly
    /// created LN invoice. Fills in `domain_event` and the BTC currency
    /// default.
    pub fn for_lightning_invoice_created(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        sat_amount: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            domain_event: GatewayDomainEvent::LightningInvoiceCreated,
            reference_id: payment_hash_hex.into(),
            sat_amount,
            currency: "BTC".to_owned(),
            timestamp,
            gateway_metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lightning_invoice_created_maps_to_incoming_pending() {
        assert_eq!(
            GatewayDomainEvent::LightningInvoiceCreated.to_standardized(),
            GatewayEventType::IncomingPaymentPending
        );
    }

    #[test]
    fn event_type_string_round_trip() {
        let t = GatewayEventType::IncomingPaymentPending;
        let s = t.to_string();
        assert_eq!(s, "INCOMING_PAYMENT_PENDING");
        let back: GatewayEventType = s.parse().unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn domain_event_string_round_trip() {
        let d = GatewayDomainEvent::LightningInvoiceCreated;
        let s = d.to_string();
        assert_eq!(s, "lightning_invoice_created");
        let back: GatewayDomainEvent = s.parse().unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn unknown_event_type_returns_typed_error() {
        let err = "WAT".parse::<GatewayEventType>().unwrap_err();
        assert!(matches!(err, OutboxError::UnknownEventType(_)));
    }
}
