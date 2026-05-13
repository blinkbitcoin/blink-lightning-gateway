#[tokio::main]
async fn main() -> anyhow::Result<()> {
    blink_lightning_gateway::cli::run().await
}
