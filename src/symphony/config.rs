//! Symphony adapter config.
//!
//! Empty for now — the first field lands with Story 2.2's
//! `authorize_spend`-equivalent (analogous to
//! `blink-card/src/symphony/config.rs::endpoint`). Existing as an empty
//! struct so that `Config.symphony` is wired and `ln-gateway.yml`'s
//! `symphony: {}` block parses without changes when Story 2.2 starts
//! populating it.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SymphonyConfig {}
