//! `LndConfig` — connection settings for the LND gRPC client. YAML-mappable;
//! `cert_path` and `macaroon_path` are file paths read at boot, never
//! committed.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LndConfig {
    /// `host:port` form, e.g. `lnd1:10009`.
    pub address: String,
    /// Path to the TLS cert file LND signs its gRPC server with.
    pub cert_path: PathBuf,
    /// Path to the LND admin macaroon. Should be read-only on disk in
    /// production.
    pub macaroon_path: PathBuf,
}

impl LndConfig {
    /// Convenience for tests — points nowhere real and is rejected by the
    /// stub `connect`.
    pub fn stub() -> Self {
        Self {
            address: "stub:0".to_owned(),
            cert_path: PathBuf::from("/dev/null"),
            macaroon_path: PathBuf::from("/dev/null"),
        }
    }
}
