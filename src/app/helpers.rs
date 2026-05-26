//! Shared helpers reachable from every per-use-case `impl App` block.
//!
//! Free functions are crate-visible. `check_wallet_ownership` is a
//! method on `App` (still stubbed for Story 3.1) but lives here so all
//! per-use-case files can call it without each owning its own copy.

use crate::app::{App, AppError};
use crate::invoice::repo::InvoiceFindError;
use crate::lnd::LndError;
use crate::payment::repo::{PaymentFindError, PaymentModifyError};
use crate::payment::{FailureReason, Hop};
use crate::primitives::WalletId;

/// Detect concurrent-modification on a Payment modify. Used by the sync
/// `send_payment` path to retry once when the subscription handler beats
/// us to the projection update for the same payment.
pub(crate) fn is_concurrent_modification(err: &PaymentModifyError) -> bool {
    matches!(err, PaymentModifyError::ConcurrentModification)
}

pub(crate) fn is_payment_not_found(err: &PaymentFindError) -> bool {
    matches!(err, PaymentFindError::NotFound { .. })
}

pub(crate) fn is_invoice_not_found(err: &InvoiceFindError) -> bool {
    matches!(err, InvoiceFindError::NotFound { .. })
}

pub(crate) fn hops_to_json(route_hops: &[Hop]) -> Vec<serde_json::Value> {
    route_hops
        .iter()
        .map(|h| {
            serde_json::json!({
                "pub_key": h.pub_key.to_hex(),
                "channel_id": h.channel_id,
                "fee_msat": h.fee_msat.as_u64(),
                "amt_msat": h.amt_msat.as_u64(),
            })
        })
        .collect()
}

/// Map an `LndError` from a synchronous `send_payment` call to a typed
/// `FailureReason` for the orphan-recovery `Failed` transition.
pub(crate) fn lnd_error_to_failure_reason(err: &LndError) -> FailureReason {
    match err {
        LndError::PaymentTimeout => FailureReason::Timeout,
        LndError::NoRoute => FailureReason::NoRoute,
        LndError::IncorrectPaymentDetails => FailureReason::IncorrectPaymentDetails,
        other => FailureReason::Other(format!("LND error: {other}")),
    }
}

impl App {
    /// STUB(story-3.1): replace with Apollo Router entity sub-query + TTL
    /// cache.
    pub(crate) async fn check_wallet_ownership(
        &self,
        _wallet_id: &WalletId,
    ) -> Result<(), AppError> {
        Ok(())
    }
}
