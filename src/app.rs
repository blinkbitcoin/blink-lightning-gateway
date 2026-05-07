//! Application coordinator — single `App` struct (NOT folder of
//! per-aggregate services) per architecture L940 and ADR #1. Children:
//! `inbound` (use-cases driven by inbound API), `outbound` (use-cases
//! driven by background work — empty in Slice 1a), `error` (`AppError`,
//! `anyhow::Error` permitted at this boundary).

pub mod error;
pub mod inbound;
pub mod outbound;

pub use error::AppError;
pub use inbound::{App, Mode, NewInvoiceRequest};
