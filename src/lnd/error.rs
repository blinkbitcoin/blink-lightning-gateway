//! `LndError` — typed errors for the LND adapter. Maps tonic transport
//! failures + LND-specific decoding issues into named variants. Slice 1a
//! exercises only `Stub`/`Connect`; later slices that wire real LND through
//! `lnd_grpc_rust` extend with `Rpc(tonic::Status)`, `DecodeInvoice`, etc.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LndError {
    #[error("LND connection failed: {0}")]
    Connect(String),

    #[error("LND RPC returned status: {0}")]
    Rpc(#[from] tonic::Status),

    #[error(
        "LND adapter is stubbed in story 1.4; real wiring lands in story 1.6 (Tilt local stack)"
    )]
    Stub,

    #[error("invalid LND response: {0}")]
    InvalidResponse(String),
}
