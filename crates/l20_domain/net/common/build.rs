fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../proto");
    let policy_v1_file = proto_root.join("net/policy/v1/policy.proto");

    let builder = tonic_prost_build::configure()
        .type_attribute(
            ".space.net.policy.v1.NetworkPolicyV1",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            ".space.net.policy.v1.NetworkCredentialRuleV1",
            "#[derive(Eq, Hash)]",
        );
    pplx_space_common_proto_build::compile_protos(builder, &[policy_v1_file], &[proto_root])?;

    Ok(())
}
