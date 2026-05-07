//! LND adapter. Per architecture L946, every adapter has `client.rs` +
//! `config.rs` + `error.rs`. Slice-specific files (`invoice.rs` here;
//! `payment.rs`, `htlc.rs`, `subscription.rs` in 2.1, 3.1, etc.) land
//! alongside the slice that needs them.

pub mod client;
pub mod config;
pub mod error;
pub mod invoice;

pub use client::{LndApi, LndClient};
pub use config::LndConfig;
pub use error::LndError;
pub use invoice::{AddInvoiceParams, AddInvoiceResponse};
