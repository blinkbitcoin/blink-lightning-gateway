//! OpenTelemetry init slot. Detail in Epic 2.
//!
//! NOTE: this module is shadowed by the external `tracing` crate. Inside this
//! file, `use tracing::*;` would resolve to `crate::tracing` (i.e., self).
//! When the external crate is needed, write `use ::tracing::{info, warn, ...};`
//! (absolute leading `::`) instead.
