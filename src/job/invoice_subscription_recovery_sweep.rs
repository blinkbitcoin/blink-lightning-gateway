//! Boot-time sweep: spawn per-hash `subscribe_invoice` listeners for
//! every invoice in a non-terminal state.
//!
//! Why this catches up missed transitions during outage:
//! `SubscribeSingleInvoice` always emits the current invoice state
//! immediately on subscribe (proto doc `invoices.proto:31-35`,
//! verified at `invoices/invoiceregistry.go::deliverSingleBacklogEvents`).
//! If an invoice transitioned `Pending → Held` (or any other
//! transition) while the gateway was down, the per-hash listener
//! spawned at recovery sees the current state on its first emission,
//! forwards it through the mpsc, and `App::handle_invoice_update`
//! performs the right transition.
//!
//! Mirrors `setupListenersForExistingHodlInvoices` at
//! `blink/core/api/src/servers/trigger.ts:277-308`, with one key
//! difference: this gateway enumerates via `Invoices::list_open_invoices`
//! (DB-side) since the DB has the canonical open-invoice set for this
//! gateway's creations. galoy's reference enumerates via LND.

use ::tracing::info;

use crate::app::{App, InvoiceUpdateDispatcher};

/// Spawn a per-hash listener for every invoice currently in `Pending`
/// or `Held`. Fire-and-forget — returns `Ok(())` after every spawn has
/// been issued.
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
