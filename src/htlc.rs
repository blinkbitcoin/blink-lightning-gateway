//! HtlcAttempt aggregate (per ADR #1). DDD shape: entity / repo / event / error. Real implementation lands in Story 3.1 (Slice 4 — MPP).

pub mod entity;
pub mod error;
pub mod event;
pub mod repo;
