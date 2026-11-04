use super::*;

#[test]
fn live_pid_symlink_requires_a_live_numeric_target() {
    let directory = tempfile::tempdir().unwrap();
    let live_target = directory.path().join("4242");
    let malformed_target = directory.path().join("not-a-pid");
    let zero_target = directory.path().join("0");
    std::fs::write(&live_target, b"").unwrap();
    std::fs::write(&malformed_target, b"").unwrap();
    std::fs::write(&zero_target, b"").unwrap();
    let link = directory.path().join("sidecar.pid");

    for (target, expected) in [
        (&live_target, Some(4242)),
        (&malformed_target, None),
        (&zero_target, None),
        (&directory.path().join("4243"), None),
    ] {
        std::os::unix::fs::symlink(target, &link).unwrap();
        assert_eq!(live_pid_from_symlink(&link), expected);
        std::fs::remove_file(&link).unwrap();
    }
    std::fs::write(&link, b"4242").unwrap();
    assert_eq!(live_pid_from_symlink(&link), None);
    std::fs::remove_file(&link).unwrap();
    assert_eq!(live_pid_from_symlink(&link), None);
}
