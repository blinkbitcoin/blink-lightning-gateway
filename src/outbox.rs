//! Transactional outbox pattern for the gateway. `EventPublisher::publish_in_tx`
//! writes one row to `outbox_events` inside the caller's transaction and the
//! Postgres `pg_notify('gateway_events', ...)` trigger fires on commit.
//!
//! Story 1.4 (this story) lands the producer-side: the `entity` types,
//! `publisher`, and the `error` type. The LISTEN connection
//! (`listen_connection.rs`) and gRPC `subscription_loop` consumer arrive in
//! Story 1.5.

pub mod entity;
pub mod error;
pub mod fanout;
pub mod listen_connection;
pub mod publisher;

pub use entity::{GatewayDomainEvent, GatewayEventType, NewOutboxEvent, OutboxEvent};
pub use error::OutboxError;
pub use fanout::OutboxFanout;
pub use listen_connection::ListenConnection;
pub use publisher::{EventPublisher, MAX_BACKFILL_EVENTS};
