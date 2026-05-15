//! Payment aggregate. DDD shape: entity / repo / event / error.

pub mod entity;
pub mod error;
pub mod event;
pub mod repo;

pub use entity::{DecodedInvoice, NewPayment, Payment, PaymentState};
pub use error::PaymentError;
pub use event::{FailureReason, Hop, PaymentEvent};
pub use repo::Payments;
