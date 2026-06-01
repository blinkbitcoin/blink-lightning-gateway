fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .type_attribute(
            "lightning_payment_gateway.GatewayEventType",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&["proto/lightning_payment_gateway.proto"], &["proto"])?;

    // Client-only build of Symphony's `SpendAuthorizationService` (ADR-0003).
    // `build_server(false)` — the gateway never serves this; it only calls it.
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["proto/spend_authorization.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/lightning_payment_gateway.proto");
    println!("cargo:rerun-if-changed=proto/spend_authorization.proto");

    Ok(())
}
