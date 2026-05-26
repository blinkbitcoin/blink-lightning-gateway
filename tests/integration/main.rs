// Integration test umbrella. One `cargo test --test integration` binary
// pulls in every file in this folder via `mod`. Adding a new integration
// test = drop a file here and add one `mod` line.

mod common;

mod app_create_invoice;
mod hold_invoice_reconciliation;
mod hold_invoice_settle_within_window;
mod incoming_invoice_subscription;
mod invoice_consumer_flow;
mod invoice_create_producer_flow;
mod invoice_repo;
mod outbox_publisher;
mod payment_repo;
mod payment_send_happy_path;
mod server_lifecycle;
