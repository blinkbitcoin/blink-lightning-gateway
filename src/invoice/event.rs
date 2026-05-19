//! `InvoiceEvent` — event-sourced changes on the Invoice aggregate.
//!
//! Slice 1a carries `Created`. Story 2.3 adds `HtlcHeld`, `Settled`,
//! and `Canceled` for the inbound subscription lifecycle observed via
//! LND's per-hash `SubscribeSingleInvoice`. Story 2.4 will DRIVE
//! `Held → Settled` via the explicit-cancel / settle-hold-invoice
//! command paths; Story 2.3 only observes regular invoices.
//!
//! Note: there is no per-event `id` field — `EntityEvents::id()` already
//! carries the entity id, and the es-entity event log table joins on
//! `invoice_events.id`. Storing it on the payload as well would be a
//! redundant write.

use es_entity::EsEvent;
use serde::{Deserialize, Serialize};

use crate::primitives::{
    BoltInvoice, InvoiceId, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};

/// Reason an inbound invoice was canceled. Story 2.3's subscription
/// path only ever fires `Expired` (LND auto-cancels unheld invoices on
/// timeout). `Manual` is reserved for Story 2.4's explicit-cancel
/// command path; `Other(String)` is the escape hatch for variants LND
/// may emit that we haven't enumerated yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum CancelReason {
    Expired,
    Manual,
    Other(String),
}

impl CancelReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Expired => "Expired",
            Self::Manual => "Manual",
            Self::Other(_) => "Other",
        }
    }
}

// `memo` intentionally not stored as a separate field here — it survives
// inside `bolt_invoice` (BOLT11's `d` field), and that's where blink-core
// keeps it too (the MongoDB `walletInvoiceSchema` has `paymentRequest`
// only, no `memo`/`description` column at
// `blink/core/api/src/services/mongoose/schema.ts:97-99`). Keeping it
// separate here would be a privacy regression vs. blink-core.
#[derive(Clone, Debug, Serialize, Deserialize, EsEvent)]
#[serde(tag = "type", rename_all = "snake_case")]
#[es_event(id = "InvoiceId")]
pub enum InvoiceEvent {
    Created {
        payment_hash: PaymentHash,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        expiry_at: Timestamp,
        bolt_invoice: BoltInvoice,
        created_at: Timestamp,
    },
    /// LND `SubscribeSingleInvoice` reported `Accepted` — an HTLC
    /// (or set of HTLCs for MPP) is parked on a HOLD invoice. For
    /// Story 2.3's regular-invoice path the field equals the original
    /// invoice amount; HOLD's MPP case in Story 2.4 may differ.
    HtlcHeld {
        held_at: Timestamp,
        htlc_amount_msat: MilliSatoshi,
    },
    /// LND reported `Settled` (`is_confirmed`) — the preimage has been
    /// released. Drives the standardized `IncomingPaymentConfirmed`
    /// outbox row.
    Settled {
        settled_at: Timestamp,
        payment_preimage: Preimage,
    },
    /// LND reported `Canceled` (`is_canceled`). Story 2.3 only ever
    /// fires `CancelReason::Expired`; Story 2.4 wires `Manual`.
    Canceled {
        canceled_at: Timestamp,
        reason: CancelReason,
    },
}
