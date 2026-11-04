use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};

#[test]
fn execution_store_recovers_atomic_writes_and_rejects_unsafe_entries() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("executions");
    let store = ExecutionStore::at(root.clone());
    let sandbox = SandboxId::new();
    let first = SandboxActivationAttemptId::new();
    let second = SandboxActivationAttemptId::new();
    assert_eq!(store.read(sandbox).unwrap(), None);
    store.write(sandbox, first).unwrap();
    let successor = ExecutionStore::at(root.clone());
    assert_eq!(successor.read(sandbox).unwrap(), Some(first));
    fs::write(root.join(format!(".{sandbox}.new")), "interrupted").unwrap();
    successor.write(sandbox, second).unwrap();
    assert_eq!(store.read(sandbox).unwrap(), Some(second));
    let path = root.join(sandbox.to_string());
    for unsafe_entry in ["malformed", "symlink", "hardlink", "fifo"] {
        fs::remove_file(&path).unwrap();
        match unsafe_entry {
            "malformed" => fs::write(&path, "not an identity").unwrap(),
            "symlink" => symlink(temp.path().join("absent"), &path).unwrap(),
            "hardlink" => {
                let source = temp.path().join("source");
                fs::write(&source, second.to_string()).unwrap();
                fs::hard_link(source, &path).unwrap();
            }
            "fifo" => {
                nix::unistd::mkfifo(
                    &path,
                    nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(store.read(sandbox).is_err(), "{unsafe_entry}");
    }
    store.remove(sandbox).unwrap();
    store.remove(sandbox).unwrap();
    assert_eq!(store.read(sandbox).unwrap(), None);
    fs::set_permissions(&root, fs::Permissions::from_mode(0o770)).unwrap();
    store.read(sandbox).unwrap_err();
}

#[test]
fn cpu_budget_survives_restart_and_lower_retry_but_not_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("executions");
    let store = ExecutionStore::at(root.clone());
    let sandbox = SandboxId::new();
    let first = SandboxActivationAttemptId::new();
    let second = SandboxActivationAttemptId::new();
    let count = |value| VcpuCount::try_from_u32(value).unwrap();
    store.write(sandbox, first).unwrap();
    assert_eq!(store.cpu_budget(sandbox, first).unwrap(), None);
    store.record_cpu_budget(sandbox, first, count(4)).unwrap();
    for requested in [4, 1, 4, 8] {
        assert_eq!(
            store
                .record_cpu_budget(sandbox, first, count(requested))
                .is_ok(),
            requested <= 4
        );
        assert_eq!(store.cpu_budget(sandbox, first).unwrap(), Some(count(4)));
    }
    let restarted = ExecutionStore::at(root.clone());
    assert_eq!(
        restarted.cpu_budget(sandbox, first).unwrap(),
        Some(count(4))
    );
    restarted.write(sandbox, second).unwrap();
    assert_eq!(restarted.cpu_budget(sandbox, second).unwrap(), None);
    assert!(
        restarted
            .record_cpu_budget(sandbox, first, count(4))
            .is_err()
    );
    // An interrupted staging write is replaced without authorizing its contents.
    fs::write(root.join(format!(".{sandbox}.cpu.new")), b"interrupted").unwrap();
    restarted
        .record_cpu_budget(sandbox, second, count(2))
        .unwrap();
    assert_eq!(restarted.cpu_budget(sandbox, first).unwrap(), None);
    assert_eq!(
        restarted.cpu_budget(sandbox, second).unwrap(),
        Some(count(2))
    );
    let path = root.join(format!("{sandbox}.cpu"));
    for corruption in [
        b"bad".to_vec(),
        [
            b"CPU9".as_slice(),
            second.to_string().as_bytes(),
            &2_u32.to_be_bytes(),
        ]
        .concat(),
        [
            b"CPU1".as_slice(),
            second.to_string().as_bytes(),
            &0_u32.to_be_bytes(),
        ]
        .concat(),
    ] {
        fs::write(&path, corruption).unwrap();
        assert_eq!(
            restarted.cpu_budget(sandbox, second).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
    let staging = root.join(format!(".{sandbox}.cpu.new"));
    fs::write(&staging, b"interrupted").unwrap();
    restarted.remove(sandbox).unwrap();
    assert!(!path.exists());
    assert!(!staging.exists());
    assert_eq!(restarted.read(sandbox).unwrap(), None);
}
