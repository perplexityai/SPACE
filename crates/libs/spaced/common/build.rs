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

    let dependent_protos = [
        proto_root.join("spaced/host/v1/host.proto"),
        proto_root.join("spaced/guest/v1/guest.proto"),
        proto_root.join("spaced/bridge/host/v1/host.proto"),
        proto_root.join("spaced/host_control/v1/host_control.proto"),
    ];
    let dependent_builder = tonic_prost_build::configure()
        .extern_path(".space.spaced.activity.v1", "crate::proto::activity")
        .extern_path(
            ".space.spaced.operations.v1",
            "::pplx_space_spaced_guest_protocol::proto::operations",
        )
        .extern_path(
            ".space.spaced.bridge.guest.v1",
            "::pplx_space_spaced_guest_protocol::proto::bridge_guest",
        )
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
    Ok(())
}
