use super::*;

#[test]
fn participant_rejects_symlink_lock_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("host-file");
    let capability = dir.path().join("capability.lock");
    std::fs::write(&target, b"host state").unwrap();
    std::os::unix::fs::symlink(&target, &capability).unwrap();

    let result = DependencyDrainParticipant::register(
        &capability,
        dir.path().join("admission.lock"),
        dir.path().join("work.lock"),
    );

    assert!(
        matches!(result, Err(DependencyDrainError::Open { .. })),
        "dependency-drain locks must never follow a symlink: {result:?}"
    );
}
