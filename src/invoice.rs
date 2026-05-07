//! Invoice aggregate (per ADR #1). DDD shape: entity / repo / event / error.

pub mod entity;
pub mod error;
pub mod event;
pub mod repo;

pub use entity::{Invoice, InvoiceState, NewInvoice, NewInvoiceEvents};
pub use error::InvoiceError;
pub use event::InvoiceEvent;
pub use repo::Invoices;
