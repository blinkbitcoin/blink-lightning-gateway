//! LND adapter. Per architecture line 946, every adapter has `client.rs` + `config.rs` + `error.rs`. Slice-specific files (`invoice.rs`, `payment.rs`, `htlc.rs`, `subscription.rs`) land in their respective slice stories (1.4, 2.1, 3.1).

pub mod client;
pub mod config;
pub mod error;
