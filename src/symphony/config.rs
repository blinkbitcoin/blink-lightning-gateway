//! Symphony adapter config.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SymphonyConfig {
    /// gRPC endpoint Symphony serves
    /// `LightningAuthorizationService` on. Defaults to empty so a
    /// `symphony: {}` block from older YAML still parses; the binary
    /// validates non-empty on boot (`SymphonyConfig::validate`).
    #[serde(default)]
    pub grpc_endpoint: String,
}

impl SymphonyConfig {
    /// Returns `Ok(())` if the config is usable. Boot-time validation
    /// path — the binary rejects an empty `grpc_endpoint` early rather
    /// than letting an obscure connection error surface at first call.
    pub fn validate(&self) -> Result<(), String> {
        if self.grpc_endpoint.trim().is_empty() {
            return Err(
                "symphony.grpc_endpoint must be set (e.g. http://symphony:6580); empty value rejected"
                    .to_owned(),
            );
        }
        Ok(())
    }
}
