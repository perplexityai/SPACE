fn main() -> anyhow::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = std::path::Path::new(&manifest_dir).join("../../../../../proto");

    let vol_dir = proto_root.join("volume/manager/v1");
    let vol_file = vol_dir.join("volume_manager.proto");
    pplx_space_common_proto_build::compile_protos(
        tonic_prost_build::configure().type_attribute(
            ".space.volume.manager.v1.CopyObjectRequest",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        ),
        &[vol_file],
        &[vol_dir],
    )?;

    Ok(())
}
