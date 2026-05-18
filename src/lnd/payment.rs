//! `send_payment` + `fee_probe` parameter and response types for the
//! `LndApi` trait. The trait body itself lives in `client.rs` — this
//! file just defines the data types it traffics in. The
//! `lnrpc::Payment` → `SendPaymentResponse` mapping helper lives in
//! `client.rs::lnd_payment_to_send_response`.

use serde::{Deserialize, Serialize};

use crate::payment::{FailureReason, Hop};
use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash, Preimage};

#[derive(Clone, Debug)]
pub struct SendPaymentParams {
    pub bolt_invoice: BoltInvoice,
    pub max_fee_msat: MilliSatoshi,
    pub timeout_seconds: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendPaymentStatus {
    InFlight,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug)]
pub struct SendPaymentResponse {
    pub payment_hash: PaymentHash,
    pub payment_preimage: Option<Preimage>,
    pub status: SendPaymentStatus,
    pub fees_paid_msat: MilliSatoshi,
    pub route_hops: Vec<Hop>,
    pub failure_reason: Option<FailureReason>,
}

#[derive(Clone, Debug)]
pub struct FeeProbeParams {
    pub bolt_invoice: BoltInvoice,
}

#[derive(Clone, Debug)]
pub struct FeeProbeResponse {
    pub fee_msat: MilliSatoshi,
    pub expiry_seconds: u32,
}
