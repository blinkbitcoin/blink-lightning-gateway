//! `invoice_reconciliation_sweep` — periodic safety net for missed
//! per-hash subscription events.
//!
//! For every `Held` invoice in the DB, calls LND's
//! `LookupInvoice` via `App::reconcile_held_invoice` and applies LND's
//! actual state. Replaces the earlier 10s `invoice_expiry_sweep` that
//! enforced a gateway-owned `hold_timeout_at` deadline (removed —
//! cancellation is now LND-driven via BOLT11 `expiry` + `holdexpirydelta`).

use std::error::Error;
use std::time::Duration;

use async_trait::async_trait;
use job::{CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType};

use crate::app::App;

/// Matches current blink-core's 5-minute `watchHeldInvoices` cadence.
pub const RECONCILIATION_SWEEP_INTERVAL: Duration = Duration::from_secs(300);

/// Per-tick cap. Bounds one tick's wall-clock; the next reschedule
/// picks up any remainder.
const RECONCILIATION_SWEEP_LIMIT: i64 = 1000;

#[derive(Clone)]
pub struct InvoiceReconciliationSweepInitializer {
    app: App,
}

impl InvoiceReconciliationSweepInitializer {
    pub fn new(app: App) -> Self {
        Self { app }
    }
}

impl JobInitializer for InvoiceReconciliationSweepInitializer {
    type Config = ();

    fn job_type(&self) -> JobType {
        JobType::new("invoice_reconciliation_sweep")
    }

    fn init(
        &self,
        _job: &Job,
        _spawner: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
        Ok(Box::new(InvoiceReconciliationSweepRunner {
            app: self.app.clone(),
        }))
    }
}

struct InvoiceReconciliationSweepRunner {
    app: App,
}

#[async_trait]
impl JobRunner for InvoiceReconciliationSweepRunner {
    async fn run(&self, _current_job: CurrentJob) -> Result<JobCompletion, Box<dyn Error>> {
        let held = self
            .app
            .invoices()
            .list_held(RECONCILIATION_SWEEP_LIMIT)
            .await?;
        for payment_hash in held {
            if let Err(e) = self.app.reconcile_held_invoice(payment_hash).await {
                // Per-hash error → log + continue. The sweep is
                // idempotent at every layer; the next tick will retry
                // any row that's still Held.
                ::tracing::error!(
                    payment_hash = %payment_hash.to_hex(),
                    error = %e,
                    "invoice_reconciliation_sweep: reconcile_held_invoice returned error; continuing"
                );
            }
        }
        Ok(JobCompletion::RescheduleIn(RECONCILIATION_SWEEP_INTERVAL))
    }
}
