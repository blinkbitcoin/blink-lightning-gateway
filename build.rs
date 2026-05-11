fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .type_attribute(
            "lightning_payment_gateway.GatewayEventType",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&["proto/lightning_payment_gateway.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/lightning_payment_gateway.proto");

    Ok(())
}
