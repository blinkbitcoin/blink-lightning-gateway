//! Layer 3 — correlation-id propagation.
//!
//! `CorrelationId` is a UUID v7 generated at the inbound API surface and
//! threaded through the App use-case → outbox event → Symphony stream →
//! Cala journal. Slice 1a generates them via `CorrelationId::new()`; full
//! tracing-context binding (matching architecture L632-664 structured
//! field requirements) is stable.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CorrelationId(Uuid);

impl CorrelationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for CorrelationId {
    fn from(u: Uuid) -> Self {
        Self(u)
    }
}
