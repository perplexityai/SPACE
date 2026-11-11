fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let common = root.join("node/api/v1/common.proto");
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure().build_server(false),
        &[common],
        &[root],
    )?;
    Ok(())
}
