//! Payment aggregate (per ADR #1). DDD shape: entity / repo / event / error. Real implementation lands in Story 2.1 (Slice 2 — payment send).

pub mod entity;
pub mod error;
pub mod event;
pub mod repo;
