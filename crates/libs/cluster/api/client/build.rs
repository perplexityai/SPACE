fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

    let cluster_file = proto_root.join("cluster/api/v1/cluster.proto");

    let raw_descriptor_path = out_dir.join("cluster_api_descriptor_raw.bin");

    let builder = tonic_prost_build::configure()
        // Common node types (NodeRole, FacetRef, NodeTemplateBuild, etc.)
        // are imported via node/api/v1/common.proto — map them to the
        // existing generated types to avoid duplicating them.
        .extern_path(".space.node.api.v1", "::pplx_space_node_api_types::proto")
        .extern_path(
            ".space.execution.v1",
            "::pplx_space_execution_common::proto",
        )
        .extern_path(
            ".space.sandbox.manager.v1",
            "::pplx_space_sbx_mgr_client::proto",
        )
        .extern_path(
            ".space.template.manager.v1",
            "::pplx_space_template_mgr_client::proto",
        )
        .extern_path(".space.net.policy.v1", "::pplx_space_net_common::proto");
    pplx_space_common_proto_build::compile_protos_with_descriptor_set(
        builder,
        &raw_descriptor_path,
        &[cluster_file],
        &[proto_root],
    )?;

    strip_to_service(
        &raw_descriptor_path,
        &out_dir.join("cluster_api_v2_descriptor.bin"),
        "ClusterApi",
    )?;

    Ok(())
}

fn strip_to_service(
    raw_path: &std::path::Path,
    out_path: &std::path::Path,
    keep_service: &str,
) -> anyhow::Result<()> {
    use prost::Message;

    let raw_bytes = std::fs::read(raw_path)?;
    let mut fds = prost_types::FileDescriptorSet::decode(raw_bytes.as_slice())?;

    for file in &mut fds.file {
        file.service
            .retain(|s| s.name.as_deref() == Some(keep_service));
    }

    std::fs::write(out_path, fds.encode_to_vec())?;
    Ok(())
}
