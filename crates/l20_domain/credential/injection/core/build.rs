fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let file = proto_root.join("credential/manager/v1/network_credential.proto");

    let builder = tonic_prost_build::configure()
        .skip_debug([".space.credential.manager.v1.ResolvedNetworkCredential"])
        .type_attribute(
            ".space.credential.manager.v1.ResolvedNetworkCredential",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        );
    pplx_space_common_proto_build::compile_protos(builder, &[file], &[proto_root])?;
    Ok(())
}
