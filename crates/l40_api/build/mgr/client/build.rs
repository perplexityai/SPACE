fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");
    let proto_file = proto_root.join("build/manager/v1/manager.proto");
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure().skip_debug([
            ".build.manager.v1.Recipe.SetRuntime",
            ".build.manager.v1.Recipe.RuntimeApp",
            ".build.manager.v1.Recipe.RuntimeApp.ExecReady",
        ]),
        &[proto_file],
        &[proto_root],
    )?;
    Ok(())
}
