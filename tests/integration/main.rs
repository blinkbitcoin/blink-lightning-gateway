// Integration test umbrella. One `cargo test --test integration` binary
// pulls in every file in this folder via `mod`. Adding a new integration
// test = drop a file here and add one `mod` line.

mod app_create_invoice;
mod invoice_create_producer_flow;
mod invoice_repo;
mod outbox_publisher;
