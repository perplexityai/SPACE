use super::*;

const SANDBOX: &str = "01a0b0c9-b8a3-7473-9c12-3821a701bc28";

#[test]
fn an_armed_marker_fires_once_and_is_consumed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(SANDBOX), b"").unwrap();

    assert!(consume_fault_marker(dir.path(), SANDBOX));
    assert!(
        !dir.path().join(SANDBOX).exists(),
        "firing consumes the marker"
    );
    assert!(
        !consume_fault_marker(dir.path(), SANDBOX),
        "a consumed marker never fires again"
    );
}

#[test]
fn a_marker_arms_only_its_own_sandbox() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(SANDBOX), b"").unwrap();

    assert!(!consume_fault_marker(
        dir.path(),
        "01a0b0c9-b8a3-7473-9c12-000000000000"
    ));
    assert!(
        dir.path().join(SANDBOX).exists(),
        "another sandbox leaves the marker armed"
    );
}

#[test]
fn an_absent_directory_fires_nothing() {
    let dir = tempfile::tempdir().unwrap();
    assert!(!consume_fault_marker(&dir.path().join("missing"), SANDBOX));
}

#[test]
fn an_id_that_is_not_one_plain_path_component_never_fires() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("faults");
    std::fs::create_dir(&dir).unwrap();
    // A file outside the fault directory that a traversal would reach.
    std::fs::write(root.path().join("outside"), b"").unwrap();
    std::fs::write(dir.join(".hidden"), b"").unwrap();

    for id in ["", "../outside", ".hidden", "a/b", "a\\b", ".."] {
        assert!(!consume_fault_marker(&dir, id), "{id:?} must not fire");
    }
    assert!(root.path().join("outside").exists());
    assert!(dir.join(".hidden").exists());
}

#[test]
fn the_injected_failure_is_a_resource_refusal_with_a_named_cause() {
    // The node's rollback path treats this like a disk-admission refusal; the
    // message lets a smoke tell the injected failure from a real one.
    let status = tonic::Status::resource_exhausted(INJECTED_FAULT_MESSAGE);
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("test hook"));
    assert!(RESTORE_PREPARE_FAULT_DIR_ENV.starts_with("SPACE_SBXMGR_TEST_"));
}

/// The environment through [`pplx_space_common_core::env::Environment`],
/// so no test mutates the process environment.
struct FaultDir(std::path::PathBuf);

impl pplx_space_common_core::env::Environment for FaultDir {
    fn var_os(&self, name: &str) -> Option<std::ffi::OsString> {
        (name == RESTORE_PREPARE_FAULT_DIR_ENV).then(|| self.0.clone().into_os_string())
    }
}

struct NoVars;

impl pplx_space_common_core::env::Environment for NoVars {
    fn var_os(&self, _name: &str) -> Option<std::ffi::OsString> {
        None
    }
}

#[test]
fn the_hook_directory_arms_one_fault_for_its_sandbox() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(SANDBOX), b"").unwrap();
    let env = FaultDir(dir.path().to_path_buf());

    let status = take_restore_prepare_fault_in(&env, SANDBOX).expect("the armed marker fires");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(status.message(), INJECTED_FAULT_MESSAGE);
    assert!(
        take_restore_prepare_fault_in(&env, SANDBOX).is_none(),
        "the fault fires once"
    );
}

#[test]
fn without_the_hook_directory_nothing_is_read() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(SANDBOX), b"").unwrap();

    assert!(take_restore_prepare_fault_in(&NoVars, SANDBOX).is_none());
    assert!(
        dir.path().join(SANDBOX).exists(),
        "an unset hook leaves every marker alone"
    );
}

/// Release builds compile no fault at all: the restore path's entry point is
/// a constant `None`, whatever the environment holds.
#[cfg(not(feature = "test-hooks"))]
#[test]
fn without_test_hooks_no_fault_is_ever_taken() {
    assert!(take_restore_prepare_fault(SANDBOX).is_none());
}
