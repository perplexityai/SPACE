fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let out_dir = std::env::var("OUT_DIR")?;
    let descriptor_path = std::path::Path::new(&out_dir).join("sandbox_manager_descriptor.bin");

    let mgr_dir = proto_root.join("sandbox/manager/v1");
    let mgr_file = mgr_dir.join("manager.proto");
    let proto_dirs = [
        proto_root,
        mgr_dir,
        pplx_space_spaced_build::protobuf_include_dir()?,
    ];
    let builder = tonic_prost_build::configure()
        .enum_attribute(
            ".space.sandbox.manager.v1.ForkDiskStorageClass",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .field_attribute(
            ".space.sandbox.manager.v1.ForkDiskStorageClass.FORK_DISK_STORAGE_CLASS_UNSPECIFIED",
            "#[serde(skip)]",
        )
        .field_attribute(
            ".space.sandbox.manager.v1.ForkDiskStorageClass.FORK_DISK_STORAGE_CLASS_STANDARD",
            "#[serde(rename = \"STANDARD\")]",
        )
        .field_attribute(
            ".space.sandbox.manager.v1.ForkDiskStorageClass.FORK_DISK_STORAGE_CLASS_GLACIER_INSTANT_RETRIEVAL",
            "#[serde(rename = \"GLACIER_IR\")]",
        )
        .extern_path(
            ".space.spaced.update.v1",
            "::pplx_space_spaced_guest_protocol::proto::update",
        )
        .extern_path(
            ".space.spaced.operations.v1",
            "::pplx_space_spaced_guest_protocol::proto::operations",
        )
        .extern_path(
            ".space.execution.v1",
            "::pplx_space_execution_common::proto",
        )
        .extern_path(".space.net.policy.v1", "::pplx_space_net_common::proto")
        .extern_path(
            ".space.net.gateway.v1",
            "::pplx_space_net_gateway_protocol::proto",
        );
    pplx_space_common_proto_build::compile_protos_with_descriptor_set(
        builder,
        &descriptor_path,
        &[mgr_file],
        &proto_dirs,
    )?;

    Ok(())
}
