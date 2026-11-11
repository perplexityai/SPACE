use super::*;

#[test]
fn staging_dir_name_carries_required_prefix() {
    // template-manager's RegisterTemplate validation requires this exact
    // prefix; drift here fails every registration.
    assert_eq!(
        build_staging_dir_name(&"019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap()),
        "build.importing.019d84bd-280d-7331-99c6-b63377f3a3a2"
    );
}

#[test]
fn staging_guard_removes_dir_on_drop() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join("build.importing.job-1");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("rootfs.ext4"), b"partial").unwrap();

    drop(BuildStagingGuard::new(
        staging.to_string_lossy().into_owned(),
    ));
    assert!(!staging.exists(), "armed guard must reap the staging dir");

    // A guard over a dir that never materialized is a quiet no-op.
    drop(BuildStagingGuard::new(
        temp.path()
            .join("build.importing.never")
            .to_string_lossy()
            .into_owned(),
    ));
}
