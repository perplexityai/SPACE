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

#[test]
fn device_intent_survives_lost_replies_and_rejects_changed_or_corrupt_assignments() {
    use pplx_space_execution_common::{devices::decode_execution, proto};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("executions");
    let store = ExecutionStore::at(root.clone());
    let sandbox = SandboxId::new();
    let execution = SandboxActivationAttemptId::new();
    let input = proto::DeviceExecutionInput {
        sandbox_id: sandbox.to_string(),
        requirements_digest: format!("sha256:{}", "ab".repeat(32)),
        assignments: vec![proto::ExclusiveDeviceAssignment {
            lease_id: SandboxActivationAttemptId::new().to_string(),
            parent_id: SandboxActivationAttemptId::new().to_string(),
            profile: "compute".into(),
            host_device_ids: vec!["pci:0000:01:00.0".into()],
        }],
    };
    let devices = decode_execution("", Some(&input)).unwrap().1.unwrap();
    store.write(sandbox, execution).unwrap();
    store
        .check_device_adoption(sandbox, execution, None)
        .unwrap();
    store
        .check_device_adoption(sandbox, execution, Some(&devices))
        .unwrap_err();
    store
        .record_device_intent(sandbox, execution, Some(&devices))
        .unwrap();
    let restarted = ExecutionStore::at(root.clone());
    restarted
        .check_device_adoption(sandbox, execution, Some(&devices))
        .unwrap();
    restarted
        .record_device_intent(sandbox, execution, Some(&devices))
        .unwrap();
    restarted
        .check_device_adoption(sandbox, execution, None)
        .unwrap_err();
    restarted
        .record_device_intent(sandbox, execution, None)
        .unwrap_err();
    let mut different = input.clone();
    different.assignments[0].parent_id = SandboxActivationAttemptId::new().to_string();
    let different = decode_execution("", Some(&different)).unwrap().1.unwrap();
    restarted
        .record_device_intent(sandbox, execution, Some(&different))
        .unwrap_err();

    let path = root.join(format!("{sandbox}.devices"));
    let valid: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    for case in 0..4 {
        let mut bad = valid.clone();
        match case {
            0 => bad["format"] = "device_execution_v9".into(),
            1 => bad["unknown"] = true.into(),
            2 => bad["devices"]["assignments"] = serde_json::json!([]),
            3 => {
                let duplicate = bad["devices"]["assignments"][0].clone();
                bad["devices"]["assignments"]
                    .as_array_mut()
                    .unwrap()
                    .push(duplicate);
            }
            _ => unreachable!(),
        }
        fs::write(&path, serde_json::to_vec(&bad).unwrap()).unwrap();
        restarted
            .check_device_adoption(sandbox, execution, Some(&devices))
            .unwrap_err();
    }
    fs::write(&path, serde_json::to_vec(&valid).unwrap()).unwrap();
    let successor = SandboxActivationAttemptId::new();
    restarted.write(sandbox, successor).unwrap();
    restarted
        .check_device_adoption(sandbox, successor, Some(&devices))
        .unwrap_err();
    // Replacement is published only after the service proves quiescence. It
    // does not erase the node-manager's independent device ownership history.
    restarted
        .record_device_intent(sandbox, successor, None)
        .unwrap();
    restarted
        .check_device_adoption(sandbox, successor, None)
        .unwrap();
    restarted
        .record_device_intent(sandbox, successor, Some(&devices))
        .unwrap_err();
    restarted.remove(sandbox).unwrap();
    assert!(!path.exists());
}

#[test]
fn stop_fences_device_launches_even_before_intent_and_across_interrupted_binding() {
    use pplx_space_execution_common::{devices::decode_execution, proto};
    for has_intent in [false, true] {
        for interrupted_bind in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("executions");
            let store = ExecutionStore::at(root.clone());
            let sandbox = SandboxId::new();
            let previous = SandboxActivationAttemptId::new();
            let stopped = if interrupted_bind {
                SandboxActivationAttemptId::new()
            } else {
                previous
            };
            let input = proto::DeviceExecutionInput {
                sandbox_id: sandbox.to_string(),
                requirements_digest: format!("sha256:{}", "ab".repeat(32)),
                assignments: vec![proto::ExclusiveDeviceAssignment {
                    lease_id: SandboxActivationAttemptId::new().to_string(),
                    parent_id: SandboxActivationAttemptId::new().to_string(),
                    profile: "compute".into(),
                    host_device_ids: vec!["pci:0000:01:00.0".into()],
                }],
            };
            let devices = decode_execution("", Some(&input)).unwrap().1.unwrap();
            store.write(sandbox, previous).unwrap();
            if has_intent {
                store
                    .record_device_intent(sandbox, previous, Some(&devices))
                    .unwrap();
            }
            store.stop_device_execution(sandbox, stopped).unwrap();
            let restarted = ExecutionStore::at(root);
            for execution in [previous, stopped] {
                // Model completing the bind after a lost stop reply. Neither
                // the previous nor stopped activation may regain device access.
                restarted.write(sandbox, execution).unwrap();
                restarted
                    .check_device_adoption(sandbox, execution, Some(&devices))
                    .unwrap_err();
                restarted
                    .record_device_intent(sandbox, execution, Some(&devices))
                    .unwrap_err();
                if has_intent {
                    restarted
                        .record_device_intent(sandbox, execution, None)
                        .unwrap_err();
                } else {
                    restarted
                        .record_device_intent(sandbox, execution, None)
                        .unwrap();
                }
                restarted
                    .record_device_intent(sandbox, execution, Some(&devices))
                    .unwrap_err();
            }
            let next = SandboxActivationAttemptId::new();
            restarted.write(sandbox, next).unwrap();
            restarted
                .record_device_intent(sandbox, next, Some(&devices))
                .unwrap();
        }
    }
}
