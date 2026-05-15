//! `PaymentEvent` — event-sourced changes on the Payment aggregate.
//!
//! Five variants mapping to the standardized 8-event vocabulary's
//! `Outgoing*` variants. `Reversed` is reserved for the rare path where
//! LND reports success but the ledger detects a discrepancy; Slice 2
//! does not exercise that transition but the variant exists so the
//! Symphony `LIGHTNING_PAYMENT_OUT` template handler can be authored
//! against the full 4-state vocabulary.
//!
//! No per-event `id` field — `EntityEvents::id()` carries it (same
//! choice as `InvoiceEvent`).

use es_entity::EsEvent;
use serde::{Deserialize, Serialize};

use crate::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, PaymentId, Preimage, Timestamp, WalletId,
};

/// Reason an outbound payment failed. Mirrors LND's
/// `lnrpc.PaymentFailureReason` enum with an `Other(String)` escape
/// hatch for variants this gateway hasn't enumerated yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum FailureReason {
    Timeout,
    NoRoute,
    InsufficientBalance,
    IncorrectPaymentDetails,
    Other(String),
}

impl FailureReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Timeout => "Timeout",
            Self::NoRoute => "NoRoute",
            Self::InsufficientBalance => "InsufficientBalance",
            Self::IncorrectPaymentDetails => "IncorrectPaymentDetails",
            Self::Other(_) => "Other",
        }
    }
}

/// One hop on the route LND eventually used. Flat struct mirroring
/// LND's `lnrpc.Hop` proto. `pub_key` carried as hex String here
/// (rather than a typed `Pubkey`) because routing carries no
/// per-hop pubkey validation requirements at the gateway boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hop {
    pub pub_key: String,
    pub channel_id: u64,
    pub fee_msat: MilliSatoshi,
    pub amt_msat: MilliSatoshi,
    pub expiry: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, EsEvent)]
#[serde(tag = "type", rename_all = "snake_case")]
#[es_event(id = "PaymentId")]
pub enum PaymentEvent {
    Initiated {
        payment_hash: PaymentHash,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        max_fee_msat: MilliSatoshi,
        bolt_invoice: BoltInvoice,
        destination: String,
        initiated_at: Timestamp,
    },
    /// Fired when LND returns `IN_FLIGHT` from `SendPaymentV2`. No
    /// `payment_preimage` yet — the preimage arrives with the terminal
    /// `Completed` event over the payment-subscription stream.
    Pending { sent_at: Timestamp },
    Completed {
        settled_at: Timestamp,
        payment_preimage: Preimage,
        fees_paid_msat: MilliSatoshi,
        route_hops: Vec<Hop>,
    },
    Failed {
        failed_at: Timestamp,
        failure_reason: FailureReason,
    },
    /// Reserved for the rare path where LND reports success but the
    /// ledger detects a discrepancy. Slice 2 does NOT fire this; the
    /// variant exists so Symphony's `LIGHTNING_PAYMENT_OUT` template
    /// handler can pattern-match the full 4-state vocabulary.
    Reversed {
        reversed_at: Timestamp,
        reason: String,
    },
}
