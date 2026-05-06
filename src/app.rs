//! Application coordinator — single `App` struct (NOT folder of per-aggregate services) per architecture line 940 and ADR #1. Children: `inbound` (use-cases driven by inbound API), `outbound` (use-cases driven by background work), `error` (`ServiceError` — anyhow-permitted boundary). Real implementation lands in Story 1.4.

pub mod error;
pub mod inbound;
pub mod outbound;
