//! `orphan_hold_sweep` — reconciles Cala holds whose payment never proceeded
//! (ADR-0003 §Consequences / AC10).
//!
//! `job` recurring singleton, twice-daily reschedule. Each tick reconciles
//! `Payment` intents stranded in `initiated` longer than the 10-minute idle
//! threshold against LND's real outcome (settle / release / leave) — never a
//! blind void. The live `TrackPayments` subscription is the primary resolver;
//! this is the straggler backstop (mirrors blink-core's daily reconciliation
//! cron, run a touch more often — and safe at this cadence only because the
//! action is LND-confirmed, not a time/state heuristic).

use std::error::Error;
use std::time::Duration;

use async_trait::async_trait;
use job::{CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType};

use crate::app::App;

/// Reschedule cadence — twice daily. Reschedules 12h after each completion
/// (a backstop behind the real-time subscription; blink-core runs the
/// equivalent reconciliation once daily).
pub const ORPHAN_HOLD_SWEEP_INTERVAL: Duration = Duration::from_secs(12 * 60 * 60);

/// A payment must be stranded at least this long before the sweep reconciles
/// it against LND — gives the synchronous send path time to finish and the
/// subscription time to deliver a terminal event first, so the sweep only
/// touches genuinely stuck intents.
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
        let reconciled = self
            .app
            .sweep_orphan_holds(ORPHAN_HOLD_IDLE_THRESHOLD, ORPHAN_HOLD_SWEEP_LIMIT)
            .await?;
        if reconciled > 0 {
            ::tracing::info!(
                reconciled,
                "orphan_hold_sweep: reconciled stranded intents against LND"
            );
        }
        Ok(JobCompletion::RescheduleIn(ORPHAN_HOLD_SWEEP_INTERVAL))
    }
}
