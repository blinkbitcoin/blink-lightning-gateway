//! Dev seed — provision wallet-liability Cala accounts (ADR-0007).
//!
//! Calls Symphony's `EnsureWalletLiabilityAccount` (idempotent create-or-return)
//! for each given wallet ID, so both the sender's and recipient's
//! `wl:{wallet_id}` accounts exist before an intraledger transfer's credit leg
//! posts. The in-repo intraledger integration test does NOT need this (it uses
//! a canned `Approved` Symphony); this is for the Tilt/manual dev stack running
//! a real Symphony.
//!
//! Run:
//!   cargo run --example seed_test_wallets -- <wallet_id> [<wallet_id> ...]
//!   SYMPHONY_GRPC_ENDPOINT=http://localhost:6700 cargo run --example seed_test_wallets -- <uuid> <uuid>
//!
//! Endpoint defaults to Symphony's dev gRPC port (`dev/symphony-dev.yml`:
//! `grpc.port = 6700`). As a Tilt hook, run it as a local_resource that depends
//! on the Symphony resource and passes the dev wallet IDs.

use blink_lightning_gateway::symphony_proto::{
    spend_authorization_service_client::SpendAuthorizationServiceClient,
    EnsureWalletLiabilityAccountRequest,
};

const DEFAULT_ENDPOINT: &str = "http://localhost:6700";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wallet_ids: Vec<String> = std::env::args().skip(1).collect();
    if wallet_ids.is_empty() {
        eprintln!(
            "usage: cargo run --example seed_test_wallets -- <wallet_id> [<wallet_id> ...]\n\
             (set SYMPHONY_GRPC_ENDPOINT to override the default {DEFAULT_ENDPOINT})"
        );
        std::process::exit(2);
    }

    let endpoint =
        std::env::var("SYMPHONY_GRPC_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
    println!("seeding wallet-liability accounts via {endpoint}");

    let mut client = SpendAuthorizationServiceClient::connect(endpoint).await?;

    for wallet_id in wallet_ids {
        let resp = client
            .ensure_wallet_liability_account(EnsureWalletLiabilityAccountRequest {
                // Fresh trace id per call (this is provisioning, not a spend).
                correlation_id: uuid::Uuid::now_v7().to_string(),
                wallet_id: wallet_id.clone(),
            })
            .await?
            .into_inner();
        println!(
            "  ok  wallet_id={wallet_id}  account_code={}  account_id={}",
            resp.account_code, resp.account_id
        );
    }

    println!("done");
    Ok(())
}
