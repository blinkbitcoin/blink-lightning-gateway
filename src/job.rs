//! Background jobs and recovery sweeps.
//!
//! Story 2.3 introduces the module with the
//! `invoice_subscription_recovery_sweep` — spawns per-hash
//! `subscribe_invoice` listeners for every invoice in `Pending` or
//! `Held` after a gateway restart. Future jobs (invoice-expiry sweep,
//! HTLC-expiry sweep, stuck-payment retry) land alongside per
//! architecture L848-854.
//!
//! `sqlxmq` is reserved for jobs that genuinely need durable
//! persistence + retry-on-failure (e.g. Symphony's outbox-dispatch
//! loop). The recovery sweep is fire-and-forget at boot time and uses
//! plain `await` from `cli::run_cmd` instead.

pub mod invoice_subscription_recovery_sweep;
