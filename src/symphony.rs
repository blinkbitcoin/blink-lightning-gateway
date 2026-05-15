//! Symphony-as-client adapter. The synchronous gRPC handshake the
//! gateway makes TO Symphony before LND `send_payment`. Trait +
//! stub-client + config land here in Story 2.2; real wiring is
//! deferred to Story 2.5 per ADR-0001 stub schedule.

pub mod client;
pub mod config;
pub mod error;

pub use client::{
    DeclineReason, LightningSymphonyClient, SymphonyAuthorizeRequest, SymphonyAuthorizeResponse,
    SymphonyAuthorizeStatus, SymphonyClient,
};
pub use config::SymphonyConfig;
pub use error::SymphonyError;
