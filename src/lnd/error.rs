//! `LndError` — typed errors for the LND adapter. Maps tonic transport
//! failures + LND-specific decoding issues into named variants.
//!
//! `Rpc` wraps `tonic_lnd::tonic::Status` (the `tonic 0.13` version that
//! `fedimint-tonic-lnd 0.4` re-exports), not the workspace's `tonic 0.14`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LndError {
    #[error("LND connection failed: {0}")]
    Connect(String),

    /// Boxed because `tonic::Status` is ~176 bytes and clippy's
    /// `result_large_err` triggers across every `Result<_, LndError>`
    /// return path otherwise.
    #[error("LND RPC returned status: {0}")]
    Rpc(Box<tonic_lnd::tonic::Status>),

    #[error("LND adapter is stubbed; real wiring is exercised through `tilt up`")]
    Stub,

    #[error("invalid LND response: {0}")]
    InvalidResponse(String),

    #[error("LND has no record of this payment")]
    PaymentNotFound,

    // Slice-2 outbound-payment failure-mode variants, mapping LND's
    // `lnrpc.PaymentFailureReason` enum onto rust-side typed errors.
    #[error("payment timed out before LND found a route")]
    PaymentTimeout,

    #[error("no route found to destination")]
    NoRoute,

    #[error("destination rejected payment details")]
    IncorrectPaymentDetails,
}

impl From<tonic_lnd::tonic::Status> for LndError {
    fn from(status: tonic_lnd::tonic::Status) -> Self {
        LndError::Rpc(Box::new(status))
    }
}
