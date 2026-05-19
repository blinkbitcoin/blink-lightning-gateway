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
use crate::lightning_payment_gateway as proto;

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

    /// Map this hand-written enum onto the prost-generated proto enum.
    /// Both enums carry identical SCREAMING_SNAKE_CASE variant names and
    /// integer tags 1..=8, so the variants line up 1:1. The hand-written
    /// copy stays so domain code does not have to import the
    /// prost-generated proto module just to log or pattern-match an
    /// event type.
    pub fn to_proto(self) -> proto::GatewayEventType {
        match self {
            Self::OutgoingPaymentInitiated => proto::GatewayEventType::OutgoingPaymentInitiated,
            Self::OutgoingPaymentCompleted => proto::GatewayEventType::OutgoingPaymentCompleted,
            Self::OutgoingPaymentFailed => proto::GatewayEventType::OutgoingPaymentFailed,
            Self::OutgoingPaymentReversed => proto::GatewayEventType::OutgoingPaymentReversed,
            Self::IncomingPaymentReceived => proto::GatewayEventType::IncomingPaymentReceived,
            Self::IncomingPaymentPending => proto::GatewayEventType::IncomingPaymentPending,
            Self::IncomingPaymentConfirmed => proto::GatewayEventType::IncomingPaymentConfirmed,
            Self::IncomingPaymentCanceled => proto::GatewayEventType::IncomingPaymentCanceled,
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

/// Gateway-specific domain events.
///
/// `LightningInvoiceSettled` (Story 1.5 / Story 2.3 production trigger
/// via the per-hash `subscribe_invoice` listener) names the wire event
/// `is_confirmed`. Synonym: "Confirmed" — kept here for grep-ability.
/// `LightningHtlcHeld` (Story 2.3) names `is_held`; maps to
/// `IncomingPaymentPending`. `LightningInvoiceCanceled` (Story 2.3)
/// names `is_canceled`; maps to `IncomingPaymentCanceled` and runs
/// without a Cala template until Story 2.4 authors
/// `LIGHTNING_INVOICE_CANCELED`.
/// `LightningPaymentInitiated`/`Completed`/`Failed` (Story 2.2) handle
/// the symmetric outbound flow; `LightningPaymentReversed` is reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayDomainEvent {
    LightningInvoiceSettled,
    LightningHtlcHeld,
    LightningInvoiceCanceled,
    LightningPaymentInitiated,
    LightningPaymentCompleted,
    LightningPaymentFailed,
    LightningPaymentReversed,
}

impl GatewayDomainEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LightningInvoiceSettled => "lightning_invoice_settled",
            Self::LightningHtlcHeld => "lightning_htlc_held",
            Self::LightningInvoiceCanceled => "lightning_invoice_canceled",
            Self::LightningPaymentInitiated => "lightning_payment_initiated",
            Self::LightningPaymentCompleted => "lightning_payment_completed",
            Self::LightningPaymentFailed => "lightning_payment_failed",
            Self::LightningPaymentReversed => "lightning_payment_reversed",
        }
    }

    /// Map to the standardized 8-event vocabulary.
    pub fn to_standardized(self) -> GatewayEventType {
        match self {
            Self::LightningInvoiceSettled => GatewayEventType::IncomingPaymentConfirmed,
            Self::LightningHtlcHeld => GatewayEventType::IncomingPaymentPending,
            Self::LightningInvoiceCanceled => GatewayEventType::IncomingPaymentCanceled,
            Self::LightningPaymentInitiated => GatewayEventType::OutgoingPaymentInitiated,
            Self::LightningPaymentCompleted => GatewayEventType::OutgoingPaymentCompleted,
            Self::LightningPaymentFailed => GatewayEventType::OutgoingPaymentFailed,
            Self::LightningPaymentReversed => GatewayEventType::OutgoingPaymentReversed,
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
            "lightning_invoice_settled" => Ok(Self::LightningInvoiceSettled),
            "lightning_htlc_held" => Ok(Self::LightningHtlcHeld),
            "lightning_invoice_canceled" => Ok(Self::LightningInvoiceCanceled),
            "lightning_payment_initiated" => Ok(Self::LightningPaymentInitiated),
            "lightning_payment_completed" => Ok(Self::LightningPaymentCompleted),
            "lightning_payment_failed" => Ok(Self::LightningPaymentFailed),
            "lightning_payment_reversed" => Ok(Self::LightningPaymentReversed),
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
    pub amount_sat: i64,
    pub timestamp: DateTime<Utc>,
    pub gateway_metadata: serde_json::Value,
}

impl OutboxEvent {
    /// Project this row onto the wire-level `PaymentEvent` proto. Mirrors
    /// `blink-card/src/outbox/entity.rs:28-61` (`to_proto`) — same shape so
    /// Symphony's existing consumer-side `GatewayEventSource` decode path
    /// works against this gateway too.
    pub fn to_proto(&self) -> proto::PaymentEvent {
        debug_assert!(self.sequence > 0, "Sequence must be positive");
        debug_assert!(
            self.amount_sat >= 0,
            "amount_sat must be non-negative, got: {}",
            self.amount_sat
        );

        // Defense in depth: a corrupt negative `amount_sat` is logged and
        // clamped to 0. Casting a negative `i64` to `u64` would silently
        // wrap to a near-`u64::MAX` value, which would propagate downstream.
        let safe_amount_sat = if self.amount_sat >= 0 {
            self.amount_sat as u64
        } else {
            ::tracing::error!(
                sequence = self.sequence,
                amount_sat = self.amount_sat,
                "Negative amount_sat detected - clamping to 0"
            );
            0
        };

        proto::PaymentEvent {
            sequence: self.sequence as u64,
            correlation_id: self.correlation_id.clone(),
            event_type: self.event_type.to_proto() as i32,
            reference_id: self.reference_id.clone(),
            amount: Some(proto::Amount {
                value: Some(proto::amount::Value::Sats(safe_amount_sat)),
            }),
            timestamp_ms: self.timestamp.timestamp_millis(),
            gateway_metadata: serde_json::to_string(&self.gateway_metadata)
                .unwrap_or_else(|_| "{}".to_string()),
        }
    }
}

/// Caller-provided fields for a new outbox row. The publisher derives
/// `event_type` from `domain_event.to_standardized()` and `sequence` from
/// the `BIGSERIAL` column at insert time, so they're absent here.
#[derive(Clone, Debug)]
pub struct NewOutboxEvent {
    pub correlation_id: String,
    pub domain_event: GatewayDomainEvent,
    pub reference_id: String,
    pub amount_sat: i64,
    pub timestamp: DateTime<Utc>,
    pub gateway_metadata: serde_json::Value,
}

impl NewOutboxEvent {
    fn new(
        domain_event: GatewayDomainEvent,
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            domain_event,
            reference_id: payment_hash_hex.into(),
            amount_sat,
            timestamp,
            gateway_metadata,
        }
    }

    /// Construct a `LightningInvoiceSettled` outbox row. Production
    /// trigger (LND `subscribe_invoices` `is_confirmed` callback) lands
    /// in Story 2.3; until then this constructor is exercised only by
    /// integration tests that demonstrate the outbox → gRPC pipeline.
    pub fn for_lightning_invoice_settled(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self::new(
            GatewayDomainEvent::LightningInvoiceSettled,
            correlation_id,
            payment_hash_hex,
            amount_sat,
            timestamp,
            gateway_metadata,
        )
    }

    /// Construct a `LightningPaymentInitiated` outbox row. Fires when
    /// LND accepts the outbound payment as `IN_FLIGHT`. The Symphony
    /// `LIGHTNING_PAYMENT_INITIATED` template consumes this and posts
    /// the PENDING-layer hold for `amount + max_fee` against the
    /// sender's wallet. `gateway_metadata` MUST include
    /// `max_fee_msat` so the SETTLED-layer release at completion time
    /// can compute the asymmetric reimbursement.
    pub fn for_lightning_payment_initiated(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self::new(
            GatewayDomainEvent::LightningPaymentInitiated,
            correlation_id,
            payment_hash_hex,
            amount_sat,
            timestamp,
            gateway_metadata,
        )
    }

    /// Construct a `LightningPaymentCompleted` outbox row. Fires when
    /// LND's payment-subscription stream reports `SUCCEEDED`. The
    /// Symphony `LIGHTNING_PAYMENT_OUT` template releases the
    /// PENDING-layer hold (sized at `amount + max_fee`) and posts the
    /// SETTLED-layer final debit (sized at `amount + actual_fee`).
    /// `gateway_metadata.fees_paid_msat` is load-bearing.
    pub fn for_lightning_payment_completed(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self::new(
            GatewayDomainEvent::LightningPaymentCompleted,
            correlation_id,
            payment_hash_hex,
            amount_sat,
            timestamp,
            gateway_metadata,
        )
    }

    /// Construct a `LightningPaymentFailed` outbox row. Fires when LND
    /// rejects the payment (immediately at `send_payment` time, or
    /// later via the `TrackPayments` stream). The Symphony
    /// `LIGHTNING_PAYMENT_OUT_FAILED` template releases the
    /// PENDING-layer hold; no SETTLED entries.
    pub fn for_lightning_payment_failed(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self::new(
            GatewayDomainEvent::LightningPaymentFailed,
            correlation_id,
            payment_hash_hex,
            amount_sat,
            timestamp,
            gateway_metadata,
        )
    }

    /// Construct a `LightningHtlcHeld` outbox row. Fires when LND's
    /// per-hash `SubscribeSingleInvoice` reports `Accepted` (an HTLC
    /// parked on a HOLD invoice). Maps to `IncomingPaymentPending`; the
    /// Symphony `LIGHTNING_INVOICE_PENDING` template posts the
    /// PENDING-layer hold for the held amount.
    pub fn for_lightning_htlc_held(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self::new(
            GatewayDomainEvent::LightningHtlcHeld,
            correlation_id,
            payment_hash_hex,
            amount_sat,
            timestamp,
            gateway_metadata,
        )
    }

    /// Construct a `LightningInvoiceCanceled` outbox row. Fires when
    /// LND emits `is_canceled`. Maps to `IncomingPaymentCanceled`. The
    /// `LIGHTNING_INVOICE_CANCELED` Cala template is deferred to
    /// Story 2.4; the outbox row still emits so a future Symphony-side
    /// handler-routing arm can consume it.
    pub fn for_lightning_invoice_canceled(
        correlation_id: impl Into<String>,
        payment_hash_hex: impl Into<String>,
        amount_sat: i64,
        timestamp: DateTime<Utc>,
        gateway_metadata: serde_json::Value,
    ) -> Self {
        Self::new(
            GatewayDomainEvent::LightningInvoiceCanceled,
            correlation_id,
            payment_hash_hex,
            amount_sat,
            timestamp,
            gateway_metadata,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Guards every arm of `GatewayDomainEvent::to_standardized`. The
    // mapping is hand-written match arms with no type-system enforcement,
    // so a swapped variant (e.g. Completed → Failed) would silently
    // misroute Symphony templates. Cheap to extend when a variant lands.
    #[test]
    fn domain_event_maps_to_standardized() {
        use GatewayDomainEvent as D;
        use GatewayEventType as T;
        let cases = [
            (D::LightningInvoiceSettled, T::IncomingPaymentConfirmed),
            (D::LightningHtlcHeld, T::IncomingPaymentPending),
            (D::LightningInvoiceCanceled, T::IncomingPaymentCanceled),
            (D::LightningPaymentInitiated, T::OutgoingPaymentInitiated),
            (D::LightningPaymentCompleted, T::OutgoingPaymentCompleted),
            (D::LightningPaymentFailed, T::OutgoingPaymentFailed),
            (D::LightningPaymentReversed, T::OutgoingPaymentReversed),
        ];
        for (domain, expected) in cases {
            assert_eq!(domain.to_standardized(), expected, "domain={domain:?}");
        }
    }

    #[test]
    fn event_type_string_round_trip() {
        let t = GatewayEventType::IncomingPaymentConfirmed;
        let s = t.to_string();
        assert_eq!(s, "INCOMING_PAYMENT_CONFIRMED");
        let back: GatewayEventType = s.parse().unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn domain_event_string_round_trip() {
        for variant in [
            GatewayDomainEvent::LightningInvoiceSettled,
            GatewayDomainEvent::LightningHtlcHeld,
            GatewayDomainEvent::LightningInvoiceCanceled,
            GatewayDomainEvent::LightningPaymentInitiated,
            GatewayDomainEvent::LightningPaymentCompleted,
            GatewayDomainEvent::LightningPaymentFailed,
            GatewayDomainEvent::LightningPaymentReversed,
        ] {
            let s = variant.to_string();
            let back: GatewayDomainEvent = s.parse().unwrap();
            assert_eq!(back, variant, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn unknown_event_type_returns_typed_error() {
        let err = "WAT".parse::<GatewayEventType>().unwrap_err();
        assert!(matches!(err, OutboxError::UnknownEventType(_)));
    }
}
