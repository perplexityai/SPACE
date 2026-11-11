use super::*;

#[test]
fn host_path_rejects_relative() {
    let fs = SandboxFs::new(PathBuf::from("/tmp/sim-fs"));
    let err = fs.host_path("sbx-1", "relative/path").unwrap_err();
    assert!(matches!(err, FsError::NotAbsolute(_)));
}

#[test]
fn host_path_rejects_traversal() {
    let fs = SandboxFs::new(PathBuf::from("/tmp/sim-fs"));
    for evil in ["/..", "/foo/../bar", "/./foo", "/../etc/passwd"] {
        let err = fs.host_path("sbx-1", evil).unwrap_err();
        assert!(
            matches!(err, FsError::UnsafePath(_)),
            "expected UnsafePath for {evil}"
        );
    }
}

#[test]
fn host_path_joins_under_sandbox_root() {
    let fs = SandboxFs::new(PathBuf::from("/tmp/sim-fs"));
    let host = fs.host_path("sbx-1", "/home/user/file.txt").unwrap();
    assert_eq!(host, PathBuf::from("/tmp/sim-fs/sbx-1/home/user/file.txt"));
}

#[test]
fn host_path_rejects_unsafe_sandbox_ids() {
    let fs = SandboxFs::new(PathBuf::from("/tmp/sim-fs"));
    for evil in ["", "..", ".", "../escape", "a/b", "abs\\path", "\0bad"] {
        let err = fs.host_path(evil, "/x").unwrap_err();
        assert!(
            matches!(err, FsError::UnsafeSandboxId(_)),
            "expected UnsafeSandboxId for {evil:?}"
        );
    }
}
