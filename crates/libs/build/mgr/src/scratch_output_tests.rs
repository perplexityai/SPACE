use super::*;

#[test]
fn scratch_output_permission_targets_skip_registered_digest_dirs() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let output_dir = tempdir.path().join("seed");
    fs::create_dir(&output_dir).expect("create output dir");
    fs::write(output_dir.join("rootfs.ext4"), b"rootfs").expect("write rootfs");
    fs::write(output_dir.join("metadata.json"), b"{}").expect("write metadata");
    let digest_dir = output_dir.join("sha256:old-build");
    fs::create_dir(&digest_dir).expect("create digest dir");
    fs::write(digest_dir.join("rootfs.ext4"), b"old rootfs").expect("write digest rootfs");

    let targets = scratch_output_permission_targets(&output_dir).expect("targets");

    assert_eq!(
        targets,
        vec![
            output_dir.clone(),
            output_dir.join("rootfs.ext4"),
            output_dir.join("metadata.json"),
        ]
    );
}
