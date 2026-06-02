//! Symphony-as-client adapter. The synchronous gRPC handshake the
//! gateway makes to Symphony's `SpendAuthorizationService` before LND
//! `send_payment`.

pub mod client;
pub mod config;
pub mod error;

pub use client::{
    is_authorize_unavailable, AccountKind, AccountRef, DeclineReason, LightningSymphonyClient,
    SymphonyAuthorizeRequest, SymphonyAuthorizeResponse, SymphonyAuthorizeStatus, SymphonyClient,
};
pub use config::SymphonyConfig;
pub use error::SymphonyError;
