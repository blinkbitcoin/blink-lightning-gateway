//! `MilliSatoshi(u64)` and `Satoshis(u64)` — LN's two amount units.
//!
//! Why u64 (not `rust_decimal` like bria): millisat is integer-only by
//! design (LN's smallest unit), and 21M BTC × 100M sat × 1000 msat = 2.1×10^18
//! msat, well below `u64::MAX` (1.8×10^19). bria's pattern is fine for
//! on-chain outputs that mix sats with fractional fee-rate calculations;
//! gateway never needs that.
//!
//! Postgres encoding: `BIGINT` (signed i64). The DB schema declares
//! `amount_msat BIGINT NOT NULL`. Encode/Decode bounds-check on the
//! u64↔i64 boundary; anything past `i64::MAX` is rejected.

use serde::{Deserialize, Serialize};
use sqlx::{Decode, Encode, Postgres, Type};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MilliSatoshi(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Satoshis(u64);

#[derive(Debug, thiserror::Error)]
pub enum MilliSatoshiError {
    #[error("milli-satoshi value must be non-negative; got {0}")]
    Negative(i64),
    #[error("milli-satoshi value exceeds i64::MAX (cannot encode as Postgres BIGINT)")]
    Overflow,
}

impl MilliSatoshi {
    pub const ZERO: MilliSatoshi = MilliSatoshi(0);

    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Ceiling-round to whole-sat (output is always a multiple of 1000).
    /// Called at BOLT11 decode so `Payment.amount_msat` only ever holds
    /// whole-sat values — downstream sat conversions are then lossless.
    /// Mirrors blink-core's `safe_tokens` ceiling.
    pub const fn round_up_to_sat(self) -> Self {
        Self(self.0.div_ceil(1000) * 1000)
    }

    /// Truncating divide to whole satoshis. Only safe to call when the
    /// invariant "self is a multiple of 1000" holds — i.e. after a
    /// `round_up_to_sat`.
    pub const fn whole_sat(self) -> u64 {
        self.0 / 1000
    }
}

impl fmt::Display for MilliSatoshi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} msat", self.0)
    }
}

impl From<u64> for MilliSatoshi {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl TryFrom<i64> for MilliSatoshi {
    type Error = MilliSatoshiError;
    fn try_from(v: i64) -> Result<Self, Self::Error> {
        if v < 0 {
            Err(MilliSatoshiError::Negative(v))
        } else {
            Ok(Self(v as u64))
        }
    }
}

impl Satoshis {
    pub const ZERO: Satoshis = Satoshis(0);

    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Convert to milli-satoshi. Bitcoin's max supply (21M BTC = 2.1×10^15
    /// sat) × 1000 = 2.1×10^18, well below `u64::MAX`. For amounts that
    /// could legitimately exceed that range, `to_msat` is incorrect to use.
    pub const fn to_msat(self) -> MilliSatoshi {
        MilliSatoshi(self.0 * 1000)
    }
}

impl fmt::Display for Satoshis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} sat", self.0)
    }
}

impl From<u64> for Satoshis {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl Type<Postgres> for MilliSatoshi {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as Type<Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i64 as Type<Postgres>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Postgres> for MilliSatoshi {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let v: i64 = self
            .0
            .try_into()
            .map_err(|_| Box::new(MilliSatoshiError::Overflow) as sqlx::error::BoxDynError)?;
        <i64 as Encode<Postgres>>::encode(v, buf)
    }
}

impl<'r> Decode<'r, Postgres> for MilliSatoshi {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let v: i64 = <i64 as Decode<Postgres>>::decode(value)?;
        if v < 0 {
            return Err(Box::new(MilliSatoshiError::Negative(v)) as sqlx::error::BoxDynError);
        }
        Ok(Self(v as u64))
    }
}

// Required by `EsRepo`'s auto-generated `create_all_in_op`, which binds a
// `Vec<&MilliSatoshi>` for batch UNNEST inserts.
impl sqlx::postgres::PgHasArrayType for MilliSatoshi {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn satoshis_to_msat() {
        assert_eq!(Satoshis::new(1).to_msat(), MilliSatoshi::new(1000));
        assert_eq!(Satoshis::new(0).to_msat(), MilliSatoshi::ZERO);
    }

    #[test]
    fn try_from_negative_i64_rejects() {
        let err = MilliSatoshi::try_from(-1_i64).unwrap_err();
        assert!(matches!(err, MilliSatoshiError::Negative(-1)));
    }

    #[test]
    fn try_from_zero_i64_ok() {
        let m = MilliSatoshi::try_from(0_i64).unwrap();
        assert_eq!(m, MilliSatoshi::ZERO);
    }

    #[test]
    fn display_formats_with_unit() {
        assert_eq!(MilliSatoshi::new(1500).to_string(), "1500 msat");
        assert_eq!(Satoshis::new(42).to_string(), "42 sat");
    }

    #[test]
    fn serde_round_trip() {
        let m = MilliSatoshi::new(123_456);
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(json, "123456");
        let back: MilliSatoshi = serde_json::from_str(&json).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn round_up_to_sat_rounds_up_sub_sat() {
        // Sub-sat msat round up to the next whole sat (1000 msat).
        assert_eq!(
            MilliSatoshi::new(1).round_up_to_sat(),
            MilliSatoshi::new(1000)
        );
        assert_eq!(
            MilliSatoshi::new(999).round_up_to_sat(),
            MilliSatoshi::new(1000)
        );
        assert_eq!(
            MilliSatoshi::new(1001).round_up_to_sat(),
            MilliSatoshi::new(2000)
        );
    }

    #[test]
    fn round_up_to_sat_preserves_whole_sat() {
        // Whole-sat msat values are unchanged.
        assert_eq!(MilliSatoshi::ZERO.round_up_to_sat(), MilliSatoshi::ZERO);
        assert_eq!(
            MilliSatoshi::new(1000).round_up_to_sat(),
            MilliSatoshi::new(1000)
        );
        assert_eq!(
            MilliSatoshi::new(100_000_000).round_up_to_sat(),
            MilliSatoshi::new(100_000_000)
        );
    }

    #[test]
    fn whole_sat_truncates() {
        // Only valid post-`round_up_to_sat`. Documents the lossy nature
        // of using it on a non-whole-sat value.
        assert_eq!(MilliSatoshi::new(1000).whole_sat(), 1);
        assert_eq!(MilliSatoshi::new(999).whole_sat(), 0);
    }
}
