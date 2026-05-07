//! `Timestamp` — UTC datetime wrapper. Thin newtype around
//! `chrono::DateTime<Utc>` so entity command methods take `now: Timestamp`
//! as a parameter (per architecture L501) instead of calling
//! `chrono::Utc::now()` from inside pure code.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    pub fn new(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }

    pub fn now() -> Self {
        Self(Utc::now())
    }

    pub fn into_inner(self) -> DateTime<Utc> {
        self.0
    }

    pub fn as_inner(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_rfc3339())
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_utc() {
        let ts = Timestamp::now();
        assert_eq!(ts.as_inner().timezone(), Utc);
    }

    #[test]
    fn serde_round_trip() {
        let dt = DateTime::parse_from_rfc3339("2026-05-07T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let ts = Timestamp::new(dt);
        let json = serde_json::to_string(&ts).unwrap();
        let back: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ts);
    }
}
