//! Boot-time sweep: spawn a per-hash `subscribe_invoice` listener for
//! every open invoice.
//!
//! This catches up transitions missed during an outage:
//! `SubscribeSingleInvoice` emits the current invoice state on
//! subscribe, so a listener spawned at recovery sees (and applies) any
//! transition that happened while the gateway was down.
//!
//! Registered with the `Jobs` service as a `JobCompletion::Complete`
//! single-shot job and kicked once at boot

use std::error::Error;

use ::tracing::info;
use async_trait::async_trait;
use job::{CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType};

use crate::app::{App, InvoiceUpdateDispatcher};

/// Spawn a per-hash listener for every `Open` / `Held` invoice.
/// Fire-and-forget — returns once every spawn has been issued.
pub async fn run_invoice_subscription_recovery_sweep(
    app: App,
    dispatcher: InvoiceUpdateDispatcher,
) -> Result<(), anyhow::Error> {
    let open = app.invoices().list_open_invoices().await?;
    info!(
        count = open.len(),
        "invoice_subscription_recovery_sweep: spawning per-hash listeners for open invoices"
    );
    for invoice in open {
        dispatcher.spawn_listener_for(invoice.payment_hash);
    }
    Ok(())
}

#[derive(Clone)]
pub struct InvoiceSubscriptionRecoverySweepInitializer {
    app: App,
    dispatcher: InvoiceUpdateDispatcher,
}

impl InvoiceSubscriptionRecoverySweepInitializer {
    pub fn new(app: App, dispatcher: InvoiceUpdateDispatcher) -> Self {
        Self { app, dispatcher }
    }
}

impl JobInitializer for InvoiceSubscriptionRecoverySweepInitializer {
    type Config = ();

    fn job_type(&self) -> JobType {
        JobType::new("invoice_subscription_recovery_sweep")
    }

    fn init(
        &self,
        _job: &Job,
        _spawner: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
        Ok(Box::new(InvoiceSubscriptionRecoverySweepRunner {
            app: self.app.clone(),
            dispatcher: self.dispatcher.clone(),
        }))
    }
}

struct InvoiceSubscriptionRecoverySweepRunner {
    app: App,
    dispatcher: InvoiceUpdateDispatcher,
}

#[async_trait]
impl JobRunner for InvoiceSubscriptionRecoverySweepRunner {
    async fn run(&self, _current_job: CurrentJob) -> Result<JobCompletion, Box<dyn Error>> {
        run_invoice_subscription_recovery_sweep(self.app.clone(), self.dispatcher.clone()).await?;
        Ok(JobCompletion::Complete)
    }
}
