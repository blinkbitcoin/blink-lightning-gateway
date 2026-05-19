//! `Pubkey` — compressed secp256k1 public key (33 bytes).
//!
//! Wire form: 66-char lowercase hex. DB form: `BYTEA`. Round-trip
//! preserves the underlying bytes. Mirrors blink-core's branded
//! `Pubkey` string type (regex-validated at user-input boundaries; see
//! `blink/core/api/src/domain/bitcoin/lightning/index.ts:26-32`),
//! upgraded to a Rust newtype so the compiler enforces non-mixability
//! with other strings.

use serde::{Deserialize, Serialize};
use sqlx::{Decode, Encode, Postgres, Type};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pubkey([u8; 33]);

impl Pubkey {
    pub const fn new(bytes: [u8; 33]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 33] {
        &self.0
    }

    pub fn into_bytes(self) -> [u8; 33] {
        self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PubkeyError {
    #[error("pubkey hex decode failed: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("pubkey must be exactly 33 bytes (66 hex chars), got {0}")]
    InvalidLength(usize),
}

impl fmt::Display for Pubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Pubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pubkey({})", self.to_hex())
    }
}

impl FromStr for Pubkey {
    type Err = PubkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;
        let len = bytes.len();
        let arr: [u8; 33] = bytes
            .try_into()
            .map_err(|_| PubkeyError::InvalidLength(len))?;
        Ok(Self(arr))
    }
}

impl From<[u8; 33]> for Pubkey {
    fn from(bytes: [u8; 33]) -> Self {
        Self(bytes)
    }
}

impl Serialize for Pubkey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Pubkey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Type<Postgres> for Pubkey {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Vec<u8> as Type<Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Vec<u8> as Type<Postgres>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Postgres> for Pubkey {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let v = self.0.to_vec();
        <Vec<u8> as Encode<Postgres>>::encode(v, buf)
    }
}

impl<'r> Decode<'r, Postgres> for Pubkey {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <Vec<u8> as Decode<Postgres>>::decode(value)?;
        let len = bytes.len();
        let arr: [u8; 33] = bytes
            .try_into()
            .map_err(|_| Box::new(PubkeyError::InvalidLength(len)) as sqlx::error::BoxDynError)?;
        Ok(Self(arr))
    }
}

impl sqlx::postgres::PgHasArrayType for Pubkey {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <Vec<u8> as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_hex() {
        let raw = [0xab; 33];
        let p = Pubkey::from(raw);
        let s = p.to_string();
        assert_eq!(s, "ab".repeat(33));
        let parsed: Pubkey = s.parse().unwrap();
        assert_eq!(parsed, p);
    }

    #[test]
    fn from_str_rejects_short() {
        let s = "ab".repeat(32);
        let err = Pubkey::from_str(&s).unwrap_err();
        assert!(matches!(err, PubkeyError::InvalidLength(_)));
    }

    #[test]
    fn from_str_rejects_invalid_hex() {
        let s = "z".repeat(66);
        let err = Pubkey::from_str(&s).unwrap_err();
        assert!(matches!(err, PubkeyError::Hex(_)));
    }

    #[test]
    fn serde_round_trip() {
        let p = Pubkey::from([0xcd; 33]);
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, format!("\"{}\"", "cd".repeat(33)));
        let back: Pubkey = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
    }
}
