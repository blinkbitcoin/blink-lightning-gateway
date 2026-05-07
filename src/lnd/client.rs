//! `LndClient` and the `LndApi` trait.
//!
//! ## Why a trait at the adapter boundary
//!
//! The architecture rejects trait abstractions in repos and adapters
//! (architecture L700) because they invite premature inversion. The
//! `LndApi` trait is a **deliberate exception bounded to the test-mocking
//! surface**: gRPC mocks via `wiremock` would require hand-encoded protobuf
//! payloads (fragile against any proto-schema drift), so the idiomatic Rust
//! pattern is a thin trait at the adapter boundary, mocked via `mockall`.
//! No code outside `src/lnd/` and `src/app/` should reach for this trait —
//! domain code calls into App, App calls `LndApi` — that's the layering.
//!
//! ## Slice 1a stub
//!
//! Story 1.4 (this story) defines the trait + a stub `LndClient` that
//! returns `LndError::Stub` on every method. The real wiring against
//! `lnd_grpc_rust` (or whichever LND crate the workspace settles on —
//! `lnd_grpc_client` referenced in the architecture L235 does not exist on
//! crates.io as of 2026-05-07; closest are `lnd_grpc_rust = "2.15"` and
//! `fedimint-tonic-lnd = "0.4"`) lands in Story 1.6 alongside the Tilt
//! local stack that actually runs LND. The producer integration test for
//! 1.4 uses `MockLndApi` exclusively.

use async_trait::async_trait;

use super::{
    config::LndConfig,
    error::LndError,
    invoice::{AddInvoiceParams, AddInvoiceResponse},
};

/// Adapter contract the `App` coordinator + tests speak to. The
/// `mockall::automock` attribute generates `MockLndApi` for the lib's own
/// `cfg(test)` blocks. Integration tests in `tests/` (separate compilation
/// unit, no `cfg(test)` for the lib) hand-write a tiny stub impl — the
/// trait has one method, so the duplication is tiny.
#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait LndApi: Send + Sync {
    async fn add_invoice(&self, params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError>;
}

/// Slice-1a stub. Real wiring lands in Story 1.6.
#[derive(Clone, Debug)]
pub struct LndClient {
    #[allow(dead_code)] // used once real LND lands
    config: LndConfig,
}

impl LndClient {
    /// Slice-1a stub: returns `LndError::Stub` immediately. Story 1.6
    /// replaces this with a real tonic-channel-backed connection.
    pub async fn connect(config: LndConfig) -> Result<Self, LndError> {
        let _ = config;
        Err(LndError::Stub)
    }
}

#[async_trait]
impl LndApi for LndClient {
    async fn add_invoice(&self, _params: AddInvoiceParams) -> Result<AddInvoiceResponse, LndError> {
        Err(LndError::Stub)
    }
}
