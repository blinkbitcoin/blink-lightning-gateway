//! `Preimage` — 32-byte secret revealed when an LN invoice settles. The
//! `payment_hash` (`PaymentHash`) is `SHA256(preimage)`.
//!
//! Wire form: 64-char lowercase hex. DB form: `BYTEA`. Slice 1 does not
//! settle invoices; this type lands here so Story 2.2 (HOLD invoice settle)
//! has the type ready.

use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Decode, Encode, Postgres, Type};
use std::fmt;
use std::str::FromStr;

use super::PaymentHash;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Preimage([u8; 32]);

impl Preimage {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Generate a cryptographically-secure-random 32-byte preimage
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Derive `payment_hash = SHA256(preimage)`
    pub fn payment_hash(&self) -> PaymentHash {
        let mut hasher = Sha256::new();
        hasher.update(self.0);
        let digest: [u8; 32] = hasher.finalize().into();
        PaymentHash::from(digest)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PreimageError {
    #[error("preimage hex decode failed: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("preimage must be exactly 32 bytes, got {0}")]
    InvalidLength(usize),
}

impl fmt::Display for Preimage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Preimage {
    // Display secret in debug as well — never logged in production paths,
    // and full hex is necessary when developers manually inspect failed tests.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Preimage({})", self.to_hex())
    }
}

impl FromStr for Preimage {
    type Err = PreimageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;
        let len = bytes.len();
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| PreimageError::InvalidLength(len))?;
        Ok(Self(arr))
    }
}

impl From<[u8; 32]> for Preimage {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl Serialize for Preimage {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Preimage {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Use `String` (not `&str`) so we accept owned strings out of
        // `serde_json::Value::String`, which has no borrowed view.
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Type<Postgres> for Preimage {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Vec<u8> as Type<Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Vec<u8> as Type<Postgres>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Postgres> for Preimage {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let v = self.0.to_vec();
        <Vec<u8> as Encode<Postgres>>::encode(v, buf)
    }
}

impl<'r> Decode<'r, Postgres> for Preimage {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <Vec<u8> as Decode<Postgres>>::decode(value)?;
        let len = bytes.len();
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| Box::new(PreimageError::InvalidLength(len)) as sqlx::error::BoxDynError)?;
        Ok(Self(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_hex() {
        let raw = [0x42; 32];
        let p = Preimage::from(raw);
        let s = p.to_string();
        assert_eq!(s, "42".repeat(32));
        let parsed: Preimage = s.parse().unwrap();
        assert_eq!(parsed, p);
    }

    #[test]
    fn from_str_rejects_short() {
        let s = "ab".repeat(31);
        let err = Preimage::from_str(&s).unwrap_err();
        assert!(matches!(err, PreimageError::InvalidLength(_)));
    }

    #[test]
    fn payment_hash_is_sha256_of_preimage() {
        // Catches accidental algorithm or endianness changes in
        // `Preimage::payment_hash` — the gateway-owned hash must match
        // what LND will compute when verifying the HODL invoice.
        let p = Preimage::from([0u8; 32]);
        // SHA-256 of 32 zero bytes:
        let expected_hex = "66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925";
        assert_eq!(p.payment_hash().to_hex(), expected_hex);
    }

    #[test]
    fn generate_produces_distinct_preimages() {
        // Smoke-check the entropy source is at least not constant.
        let a = Preimage::generate();
        let b = Preimage::generate();
        assert_ne!(a, b);
    }
}
