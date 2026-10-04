fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../proto");
    let shared_includes = [
        proto_root.clone(),
        pplx_space_spaced_build::protobuf_include_dir()?,
    ];

    let activity_proto = proto_root.join("spaced/activity/v1/activity.proto");
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure(),
        std::slice::from_ref(&activity_proto),
        &shared_includes,
    )?;

    let operations_proto = proto_root.join("spaced/operations/v1/operations.proto");
    let operations_builder = tonic_prost_build::configure()
        .type_attribute(
            ".space.spaced.operations.v1.SendInputRequest",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .skip_debug([".space.spaced.operations.v1.SendInputRequest"])
        // File bytes relay through several hops; as `Bytes`, a decoded chunk
        // is a slice of the received buffer instead of a new copy.
        .bytes(".space.spaced.operations.v1.FileChunk.data");
    pplx_space_common_proto_build::compile_protos(
        operations_builder,
        std::slice::from_ref(&operations_proto),
        &shared_includes,
    )?;

    let dependent_protos = [
        proto_root.join("spaced/host/v1/host.proto"),
        proto_root.join("spaced/guest/v1/guest.proto"),
        proto_root.join("spaced/bridge/guest/v1/guest.proto"),
        proto_root.join("spaced/bridge/host/v1/host.proto"),
        proto_root.join("spaced/host_control/v1/host_control.proto"),
    ];
    let dependent_builder = tonic_prost_build::configure()
        .extern_path(".space.spaced.activity.v1", "crate::proto::activity")
        .extern_path(".space.spaced.operations.v1", "crate::proto::operations")
        .extern_path(".space.net.policy.v1", "::pplx_space_net_common::proto")
        .extern_path(
            ".space.credential.manager.v1.ResolvedNetworkCredential",
            "::pplx_space_credential_injection_core::proto::ResolvedNetworkCredential",
        );
    pplx_space_common_proto_build::compile_protos(
        dependent_builder,
        &dependent_protos,
        &shared_includes,
    )?;
    // Separate output prevents generation of imported bridge services from
    // overwriting the canonical bridge module while using its existing types.
    let worker_output = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("worker");
    std::fs::create_dir_all(&worker_output)?;
    let worker_builder = tonic_prost_build::configure()
        .out_dir(worker_output)
        .extern_path(".space.spaced.operations.v1", "crate::proto::operations")
        .extern_path(
            ".space.spaced.bridge.guest.v1",
            "crate::proto::bridge_guest",
        );
    pplx_space_common_proto_build::compile_protos(
        worker_builder,
        &[proto_root.join("spaced/worker/v1/worker.proto")],
        &shared_includes,
    )?;
    Ok(())
}
