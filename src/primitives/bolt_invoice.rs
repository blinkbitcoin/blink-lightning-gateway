//! `BoltInvoice` — opaque BOLT11 string returned by LND `add_invoice`. The
//! gateway does NOT parse or validate the inner BOLT11 — that's LND's job.

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct BoltInvoice(String);

impl BoltInvoice {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for BoltInvoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for BoltInvoice {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for BoltInvoice {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_string() {
        let s = "lnbc1500u1pwvr...".to_owned();
        let b = BoltInvoice::new(s.clone());
        assert_eq!(b.as_str(), s);
        assert_eq!(b.to_string(), s);
    }

    #[test]
    fn serde_round_trip() {
        let b = BoltInvoice::new("lnbc100");
        let json = serde_json::to_string(&b).unwrap();
        assert_eq!(json, "\"lnbc100\"");
        let back: BoltInvoice = serde_json::from_str(&json).unwrap();
        assert_eq!(back, b);
    }
}
