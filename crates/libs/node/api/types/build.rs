fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let common_file = proto_root.join("node/api/v1/common.proto");

    // common.proto has no imports from other packages, so no extern_paths needed.
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure(),
        &[common_file],
        &[proto_root],
    )?;

    Ok(())
}
