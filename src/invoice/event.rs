//! `InvoiceEvent` — event-sourced changes on the Invoice aggregate.
//!
//! Slice 1a carries only `Created`. `Settled`, `Cancelled`, and HOLD-state
//! transitions land in Story 2.2.
//!
//! Note: there is no per-event `id` field — `EntityEvents::id()` already
//! carries the entity id, and the es-entity event log table joins on
//! `invoice_events.id`. Storing it on the payload as well would be a
//! redundant write.

use es_entity::EsEvent;
use serde::{Deserialize, Serialize};

use crate::primitives::{BoltInvoice, InvoiceId, MilliSatoshi, PaymentHash, Timestamp, WalletId};

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
}
