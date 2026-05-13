//! OpenTelemetry init slot.
//!
//! NOTE: this module is shadowed by the external `tracing` crate. Inside
//! this file, `use tracing::*;` would resolve to `crate::tracing` (i.e.,
//! self). When the external crate is needed, write
//! `use ::tracing::{info, warn, ...};` (absolute leading `::`) instead.
//!
//! Story 2.1 lands `TracingConfig` so `Config.tracing` is wired and
//! `ln-gateway.yml`'s commented-out `tracing:` block parses without code
//! changes. The OTLP exporter wiring itself is deferred — calling
//! `init_tracer` today logs a warning and falls back to the JSON-formatted
//! `tracing-subscriber` path. When a later Epic 2 story adds the
//! `opentelemetry-otlp` + `tracing-opentelemetry` deps, this function
//! becomes the real init body.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TracingConfig {
    pub exporter_otlp_url: String,
    pub service_name: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            exporter_otlp_url: "http://localhost:4317".to_owned(),
            service_name: "blink-lightning-gateway-dev".to_owned(),
        }
    }
}

pub fn init_tracer(config: TracingConfig) -> anyhow::Result<()> {
    eprintln!(
        "WARNING: otel tracing init is not yet wired. \
         Configured exporter={} service={}. Falling back to the fmt subscriber.",
        config.exporter_otlp_url, config.service_name
    );
    init_fmt_subscriber()
}

pub fn init_fmt_subscriber() -> anyhow::Result<()> {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("blink_lightning_gateway=debug,info")),
        )
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to init tracing subscriber: {e}"))
}
