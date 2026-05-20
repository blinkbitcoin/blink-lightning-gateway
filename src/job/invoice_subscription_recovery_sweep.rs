//! Boot-time sweep: spawn a per-hash `subscribe_invoice` listener for
//! every open invoice.
//!
//! This catches up transitions missed during an outage:
//! `SubscribeSingleInvoice` emits the current invoice state on
//! subscribe, so a listener spawned at recovery sees (and applies) any
//! transition that happened while the gateway was down.

use ::tracing::info;

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
