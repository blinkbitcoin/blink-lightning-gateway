//! `InvoiceEvent` — event-sourced changes on the Invoice aggregate.
//!
//! Story 2.4 makes every gateway invoice a HODL invoice: `Created`
//! carries the gateway-owned `payment_preimage` and an optional
//! `amount_msat` (amountless invoices source the settled amount from
//! received HTLCs). The DRIVE side (`settle_hold_invoice`,
//! `reconcile_held_invoice`) lands in the app module; this file is
//! the aggregate event vocabulary only.
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

/// Reason an inbound invoice was canceled
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CancelReason {
    Expired,
}

impl CancelReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Expired => "Expired",
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
        payment_preimage: Preimage,
        wallet_id: WalletId,
        amount_msat: Option<MilliSatoshi>,
        expiry_at: Timestamp,
        bolt_invoice: BoltInvoice,
        external_id: String,
        created_at: Timestamp,
    },
    HtlcHeld {
        held_at: Timestamp,
        htlc_amount_msat: MilliSatoshi,
    },
    Settled {
        settled_at: Timestamp,
    },
    Canceled {
        canceled_at: Timestamp,
        reason: CancelReason,
    },
}
