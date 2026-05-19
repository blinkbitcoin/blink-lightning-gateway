//! Value objects shared across bounded contexts (PaymentHash, MilliSatoshi,
//! BoltInvoice, Preimage, Timestamp, ids). One newtype per concept; no shared
//! "smart string" type. Each newtype derives `serde` (transparent) +
//! `sqlx::Type`, so the same `PaymentHash` value (for example) can be
//! serialized to JSON, sent over the wire, written to Postgres, read back,
//! and deserialized — all without callers ever extracting the inner bytes
//! and reconstructing the wrapper themselves.

pub mod bolt_invoice;
pub mod ids;
pub mod milli_satoshi;
pub mod payment_hash;
pub mod preimage;
pub mod pubkey;
pub mod timestamp;

pub use bolt_invoice::BoltInvoice;
pub use ids::{InvoiceId, PaymentId, WalletId};
pub use milli_satoshi::{MilliSatoshi, MilliSatoshiError, Satoshis};
pub use payment_hash::{PaymentHash, PaymentHashError};
pub use preimage::{Preimage, PreimageError};
pub use pubkey::{Pubkey, PubkeyError};
pub use timestamp::Timestamp;
