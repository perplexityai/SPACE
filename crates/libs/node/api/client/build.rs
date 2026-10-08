fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

    let common_file = proto_root.join("node/api/v1/common.proto");
    let worker_file = proto_root.join("node/api/v1/worker.proto");
    let builder_file = proto_root.join("node/api/v1/builder.proto");
    let activation_file = proto_root.join("node/api/v1/activation.proto");
    let cluster_file = proto_root.join("node/api/v1/cluster.proto");

    let raw_descriptor_path = out_dir.join("node_api_descriptor_raw.bin");

    let builder = tonic_prost_build::configure()
        .extern_path(
            ".space.sandbox.manager.v1",
            "::pplx_space_sbx_mgr_client::proto",
        )
        .extern_path(
            ".space.template.manager.v1",
            "::pplx_space_template_mgr_client::proto",
        )
        .extern_path(".build.manager.v1", "::pplx_space_build_mgr_client::proto")
        .extern_path(".space.net.policy.v1", "::pplx_space_net_common::proto")
        .extern_path(
            ".space.net.gateway.v1",
            "::pplx_space_net_gateway_client::proto",
        )
        .extern_path(
            ".space.spaced.operations.v1",
            "::pplx_space_sbx_mgr_client::operations",
        )
        .extern_path(".space.node.v1", "::pplx_space_node_mgr_client::proto")
        .extern_path(
            ".space.execution.v1",
            "::pplx_space_execution_common::proto",
        );
    pplx_space_common_proto_build::compile_protos_with_descriptor_set(
        builder,
        &raw_descriptor_path,
        &[
            common_file,
            worker_file,
            builder_file,
            cluster_file,
            activation_file,
        ],
        &[proto_root],
    )?;

    strip_to_service(
        &raw_descriptor_path,
        &out_dir.join("worker_descriptor.bin"),
        "WorkerApi",
    )?;
    strip_to_service(
        &raw_descriptor_path,
        &out_dir.join("builder_descriptor.bin"),
        "BuilderApi",
    )?;
    strip_to_service(
        &raw_descriptor_path,
        &out_dir.join("cluster_descriptor.bin"),
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
