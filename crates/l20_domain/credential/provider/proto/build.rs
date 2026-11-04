fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");

    let provider_dir = proto_root.join("credential/provider/v2");
    let provider_file = provider_dir.join("intermediate_key_broker.proto");
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure(),
        &[provider_file],
        &[provider_dir],
    )?;

    Ok(())
}
