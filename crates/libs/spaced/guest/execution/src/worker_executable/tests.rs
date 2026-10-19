use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn rejects_missing_relative_and_non_executable_files() {
    assert!(matches!(
        WorkerExecutable::open("worker"),
        Err(OpenWorkerExecutableError::RelativePath)
    ));
    let directory = tempfile::tempdir().expect("directory");
    assert!(matches!(
        WorkerExecutable::open(directory.path().join("missing")),
        Err(OpenWorkerExecutableError::Io(_))
    ));
    assert!(matches!(
        WorkerExecutable::open(directory.path()),
        Err(OpenWorkerExecutableError::InvalidExecutable)
    ));
    let path = directory.path().join("worker");
    std::fs::write(&path, b"\x7fELF").expect("non-executable file");
    assert!(matches!(
        WorkerExecutable::open(&path),
        Err(OpenWorkerExecutableError::InvalidExecutable)
    ));
    std::fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("script");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).expect("permissions");
    assert!(matches!(
        WorkerExecutable::open(&path),
        Err(OpenWorkerExecutableError::InvalidExecutable)
    ));
    std::fs::remove_file(&path).expect("remove script");
    nix::unistd::mkfifo(&path, nix::sys::stat::Mode::S_IRWXU).expect("FIFO");
    assert!(matches!(
        WorkerExecutable::open(&path),
        Err(OpenWorkerExecutableError::InvalidExecutable)
    ));
}
