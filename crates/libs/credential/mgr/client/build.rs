fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");

    let cred_file = proto_root.join("credential/manager/v1/credential_manager.proto");
    let builder = tonic_prost_build::configure()
        .type_attribute(
            ".space.credential.manager.v1.X509SigningCredentials",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.ExchangeTokenRequest",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.ExchangeTokenResponse",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.AwsCredentials",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.IssueDekResponse",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.UnwrapDekResponse",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.EncryptRequest",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.DecryptResponse",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.GetOrIssueSandboxVolumeDekResponse",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .type_attribute(
            ".space.credential.manager.v1.ResolveNetworkCredentialsResponse",
            "#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]",
        )
        .skip_debug([
            ".space.credential.manager.v1.X509SigningCredentials",
            ".space.credential.manager.v1.ExchangeTokenRequest",
            ".space.credential.manager.v1.ExchangeTokenResponse",
            ".space.credential.manager.v1.AwsCredentials",
            ".space.credential.manager.v1.IssueDekResponse",
            ".space.credential.manager.v1.UnwrapDekResponse",
            ".space.credential.manager.v1.EncryptRequest",
            ".space.credential.manager.v1.DecryptResponse",
            ".space.credential.manager.v1.GetOrIssueSandboxVolumeDekResponse",
            ".space.credential.manager.v1.ResolveNetworkCredentialsResponse",
        ])
        .extern_path(
            ".space.credential.manager.v1.ResolvedNetworkCredential",
            "::pplx_space_credential_injection_core::proto::ResolvedNetworkCredential",
        );
    pplx_space_common_proto_build::compile_protos(builder, &[cred_file], &[proto_root])?;

    Ok(())
}
