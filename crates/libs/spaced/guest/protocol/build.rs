fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let shared_includes = [
        proto_root.clone(),
        pplx_space_spaced_build::protobuf_include_dir()?,
    ];
    let operations_proto = proto_root.join("spaced/operations/v1/operations.proto");
    let operations_builder = tonic_prost_build::configure()
        .type_attribute(
            ".space.spaced.operations.v1.SendInputRequest",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .skip_debug([".space.spaced.operations.v1.SendInputRequest"])
        .bytes(".space.spaced.operations.v1.FileChunk.data");
    pplx_space_common_proto_build::compile_protos(
        operations_builder,
        std::slice::from_ref(&operations_proto),
        &shared_includes,
    )?;

    let builder = tonic_prost_build::configure()
        .extern_path(".space.spaced.operations.v1", "crate::proto::operations");
    pplx_space_common_proto_build::compile_protos(
        builder,
        &[
            proto_root.join("spaced/bridge/guest/v1/guest.proto"),
            proto_root.join("spaced/worker/v1/worker.proto"),
        ],
        &shared_includes,
    )?;
    Ok(())
}
