//! `PaymentHash` — 32-byte SHA-256 hash returned by LND on `add_invoice`.
//!
//! Wire form: 64-char lowercase hex. DB form: `BYTEA`. Round-trip preserves
//! the underlying 32 bytes exactly.

use serde::{Deserialize, Serialize};
use sqlx::{Decode, Encode, Postgres, Type};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PaymentHash([u8; 32]);

impl PaymentHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
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
pub enum PaymentHashError {
    #[error("payment hash hex decode failed: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("payment hash must be exactly 32 bytes, got {0}")]
    InvalidLength(usize),
}

impl fmt::Display for PaymentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for PaymentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PaymentHash({})", self.to_hex())
    }
}

impl FromStr for PaymentHash {
    type Err = PaymentHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;
        let len = bytes.len();
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| PaymentHashError::InvalidLength(len))?;
        Ok(Self(arr))
    }
}

impl From<[u8; 32]> for PaymentHash {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl Serialize for PaymentHash {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for PaymentHash {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Use `String` (not `&str`) so we accept owned strings out of
        // `serde_json::Value::String`, which has no borrowed view.
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Type<Postgres> for PaymentHash {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Vec<u8> as Type<Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Vec<u8> as Type<Postgres>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Postgres> for PaymentHash {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let v = self.0.to_vec();
        <Vec<u8> as Encode<Postgres>>::encode(v, buf)
    }
}

impl<'r> Decode<'r, Postgres> for PaymentHash {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <Vec<u8> as Decode<Postgres>>::decode(value)?;
        let len = bytes.len();
        let arr: [u8; 32] = bytes.try_into().map_err(|_| {
            Box::new(PaymentHashError::InvalidLength(len)) as sqlx::error::BoxDynError
        })?;
        Ok(Self(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_hex() {
        let raw = [0xab; 32];
        let h = PaymentHash::from(raw);
        let s = h.to_string();
        assert_eq!(s, "ab".repeat(32));
        let parsed: PaymentHash = s.parse().unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn from_str_rejects_short() {
        let s = "ab".repeat(31);
        let err = PaymentHash::from_str(&s).unwrap_err();
        assert!(matches!(err, PaymentHashError::InvalidLength(_)));
    }

    #[test]
    fn from_str_rejects_invalid_hex() {
        let s = "z".repeat(64);
        let err = PaymentHash::from_str(&s).unwrap_err();
        assert!(matches!(err, PaymentHashError::Hex(_)));
    }

    #[test]
    fn serde_round_trip() {
        let h = PaymentHash::from([0xcd; 32]);
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, format!("\"{}\"", "cd".repeat(32)));
        let back: PaymentHash = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);
    }
}
