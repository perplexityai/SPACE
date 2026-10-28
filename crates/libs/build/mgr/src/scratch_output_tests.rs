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

#[tokio::test]
async fn installed_guest_versions_come_from_both_binaries_not_the_builder() {
    let directory = tempfile::tempdir().unwrap();
    for (service_version, expected) in [("9.4.2", Some("9.4.2")), ("9.4.3", None)] {
        for (name, version) in [("spaced", "9.4.2"), ("spaced-svc", service_version)] {
            let path = directory.path().join(name);
            fs::write(
                &path,
                format!("#!/bin/sh\nprintf '%s\\n' '{name} {version}'\n"),
            )
            .unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        assert_eq!(
            installed_guest_binary_version(directory.path())
                .await
                .ok()
                .as_deref(),
            expected
        );
    }
}
