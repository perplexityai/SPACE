fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");

    let gtw_file = proto_root.join("net/gateway/v1/gateway.proto");
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure()
            .extern_path(".space.net.policy.v1", "::pplx_space_net_common::proto"),
        &[gtw_file],
        &[proto_root],
    )?;

    Ok(())
}
