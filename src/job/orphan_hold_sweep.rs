//! `orphan_hold_sweep` — voids Cala holds whose payment never proceeded
//! (ADR-0003 §Consequences / AC10).
//!
//! `job` recurring singleton, 5-minute reschedule. Each tick voids holds for
//! `Payment` intents stranded in `initiated` longer than the 10-minute idle
//! threshold. Cadence matches `invoice_reconciliation_sweep`.

use std::error::Error;
use std::time::Duration;

use async_trait::async_trait;
use job::{CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType};

use crate::app::App;

/// Reschedule cadence (matches `invoice_reconciliation_sweep`).
pub const ORPHAN_HOLD_SWEEP_INTERVAL: Duration = Duration::from_secs(300);

/// A payment must be stranded at least this long before its hold is voided —
/// gives the LND payment-subscription time to reconcile a genuinely
/// in-flight payment out of `initiated` first.
const ORPHAN_HOLD_IDLE_THRESHOLD: Duration = Duration::from_secs(600);

/// Per-tick cap; the next reschedule picks up any remainder.
const ORPHAN_HOLD_SWEEP_LIMIT: i64 = 1000;

#[derive(Clone)]
pub struct OrphanHoldSweepInitializer {
    app: App,
}

impl OrphanHoldSweepInitializer {
    pub fn new(app: App) -> Self {
        Self { app }
    }
}

impl JobInitializer for OrphanHoldSweepInitializer {
    type Config = ();

    fn job_type(&self) -> JobType {
        JobType::new("orphan_hold_sweep")
    }

    fn init(
        &self,
        _job: &Job,
        _spawner: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
        Ok(Box::new(OrphanHoldSweepRunner {
            app: self.app.clone(),
        }))
    }
}

struct OrphanHoldSweepRunner {
    app: App,
}

#[async_trait]
impl JobRunner for OrphanHoldSweepRunner {
    async fn run(&self, _current_job: CurrentJob) -> Result<JobCompletion, Box<dyn Error>> {
        let voided = self
            .app
            .sweep_orphan_holds(ORPHAN_HOLD_IDLE_THRESHOLD, ORPHAN_HOLD_SWEEP_LIMIT)
            .await?;
        if voided > 0 {
            ::tracing::info!(voided, "orphan_hold_sweep: voided stranded holds");
        }
        Ok(JobCompletion::RescheduleIn(ORPHAN_HOLD_SWEEP_INTERVAL))
    }
}
