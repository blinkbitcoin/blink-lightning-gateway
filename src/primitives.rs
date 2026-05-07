//! Value objects shared across bounded contexts (PaymentHash, MilliSatoshi,
//! BoltInvoice, Preimage, Timestamp, ids). One newtype per concept; no shared
//! "smart string" type. Each newtype derives `serde` (transparent) +
//! `sqlx::Type` so it round-trips through the wire and the DB without
//! callers reaching past the wrapper.

pub mod bolt_invoice;
pub mod ids;
pub mod milli_satoshi;
pub mod payment_hash;
pub mod preimage;
pub mod timestamp;

pub use bolt_invoice::BoltInvoice;
pub use ids::{AccountId, InvoiceId, WalletId};
pub use milli_satoshi::{MilliSatoshi, MilliSatoshiError, Satoshis};
pub use payment_hash::{PaymentHash, PaymentHashError};
pub use preimage::{Preimage, PreimageError};
pub use timestamp::Timestamp;
