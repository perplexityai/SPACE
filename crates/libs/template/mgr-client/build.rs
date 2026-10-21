fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../proto");
    let proto_file = proto_root.join("template/manager/v1/manager.proto");
    let builder = tonic_prost_build::configure()
        .extern_path(
            ".space.sandbox.manager.v1",
            "::pplx_space_sbx_mgr_protocol::proto",
        )
        .extern_path(".space.net.policy.v1", "::pplx_space_net_common::proto")
        .extern_path(
            ".space.execution.v1",
            "::pplx_space_execution_common::proto",
        );
    pplx_space_common_proto_build::compile_protos(builder, &[proto_file], &[proto_root])?;
    Ok(())
}
