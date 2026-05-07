//! `InvoiceEvent` — event-sourced changes on the Invoice aggregate.
//!
//! Slice 1a carries only `Created`. `Settled`, `Cancelled`, and HOLD-state
//! transitions land in Story 2.2.

use es_entity::EsEvent;
use serde::{Deserialize, Serialize};

use crate::primitives::{BoltInvoice, InvoiceId, MilliSatoshi, PaymentHash, Timestamp, WalletId};

#[derive(Clone, Debug, Serialize, Deserialize, EsEvent)]
#[serde(tag = "type", rename_all = "snake_case")]
#[es_event(id = "InvoiceId")]
pub enum InvoiceEvent {
    Created {
        id: InvoiceId,
        payment_hash: PaymentHash,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        expiry_at: Timestamp,
        memo: Option<String>,
        bolt_invoice: BoltInvoice,
        created_at: Timestamp,
    },
}
