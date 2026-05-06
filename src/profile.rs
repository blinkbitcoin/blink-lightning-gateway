//! Profile bounded context — API-consumer identity (auth scope, tenant, per-caller idempotency).
//!
//! NOTE: architecture.md is internally inconsistent on this module. L824 lists `profile/` as a
//! real bounded context with `entity.rs` / `repo.rs` / `event.rs` / `error.rs`. The gaps table
//! at L183 says "REJECTED on second look. `src/profile/` removed from structure." Deepening
//! kept here pending HN's resolution; if rejected, drop the module + its children + the
//! `pub mod profile;` declaration in `src/lib.rs`.

pub mod entity;
pub mod error;
pub mod event;
pub mod repo;
