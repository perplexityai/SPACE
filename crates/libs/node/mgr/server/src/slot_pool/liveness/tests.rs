use super::*;

#[test]
fn reclamation_requires_all_independent_evidence_to_be_gone() {
    use LeaseLiveness::{Gone, Live, Unknown};
    // Sidecar cgroup, VM cgroup, tracked PID, fc.pid: exhaust all 81 rows.
    for a in [Gone, Live, Unknown] {
        for b in [Gone, Live, Unknown] {
            for c in [Gone, Live, Unknown] {
                for d in [Gone, Live, Unknown] {
                    let evidence = [a, b, c, d];
                    let expected = evidence
                        .iter()
                        .copied()
                        .max_by_key(|value| match value {
                            Gone => 0,
                            Unknown => 1,
                            Live => 2,
                        })
                        .unwrap();
                    assert_eq!(LeaseLiveness::combine(evidence), expected, "{evidence:?}");
                }
            }
        }
    }
    for (kind, expected) in [
        (std::io::ErrorKind::NotFound, Gone),
        (std::io::ErrorKind::PermissionDenied, Unknown),
        (std::io::ErrorKind::InvalidData, Unknown),
        (std::io::ErrorKind::Other, Unknown),
    ] {
        assert_eq!(LeaseLiveness::observed(Err(kind.into())), expected);
    }
}

#[test]
fn pid_evidence_checks_ownership_zombies_and_unreadable_state() {
    use LeaseLiveness::{Gone, Live, Unknown};
    let root = tempfile::tempdir().unwrap();
    let proc_dir = root.path();
    for (state, group, cmdline, expected) in [
        ("S", "0::/space/sandbox-a", "worker", Live),
        ("S", "0::/space/sbx-sandbox-a/child", "worker", Live),
        ("S", "0::/space/sandbox-ab", "worker", Gone),
        ("S", "0::/unrelated", "worker", Gone),
        ("S", "0::/unrelated", "worker\0sandbox-a\0", Live),
        ("Z", "0::/space/sandbox-a", "worker", Gone),
        ("X", "0::/space/sandbox-a", "worker", Gone),
        ("?", "0::/space/sandbox-a", "worker", Unknown),
        ("S", "malformed", "worker", Unknown),
    ] {
        std::fs::write(proc_dir.join("status"), format!("State:\t{state}\n")).unwrap();
        std::fs::write(proc_dir.join("cgroup"), group).unwrap();
        std::fs::write(proc_dir.join("cmdline"), cmdline).unwrap();
        assert_eq!(
            LeaseLiveness::observed(process_owns_sandbox(proc_dir, "sandbox-a")),
            expected,
            "{state}/{group}"
        );
    }
    std::fs::write(proc_dir.join("status"), "State:\tS\n").unwrap();
    std::fs::remove_file(proc_dir.join("cgroup")).unwrap();
    assert_eq!(
        LeaseLiveness::observed(process_owns_sandbox(proc_dir, "sandbox-a")),
        Unknown
    );
    std::fs::create_dir(proc_dir.join("cgroup")).unwrap();
    assert_eq!(
        LeaseLiveness::observed(process_owns_sandbox(proc_dir, "sandbox-a")),
        Unknown
    );
    assert_eq!(
        LeaseLiveness::observed(process_owns_sandbox(&proc_dir.join("absent"), "sandbox-a")),
        Gone
    );
}

#[test]
fn namespace_evidence_matches_members_of_the_slot_namespace_only() {
    use LeaseLiveness::{Gone, Live, Unknown};
    let root = tempfile::tempdir().unwrap();
    let proc_root = root.path().join("proc");
    // Namespace identity is the inode a mount and `/proc/<pid>/ns/net` share.
    let slot_ns = root.path().join("slot-ns");
    let other_ns = root.path().join("other-ns");
    std::fs::write(&slot_ns, b"").unwrap();
    std::fs::write(&other_ns, b"").unwrap();
    let member = |pid: &str, namespace: &Path| {
        std::fs::create_dir_all(proc_root.join(pid).join("ns")).unwrap();
        std::os::unix::fs::symlink(namespace, proc_root.join(pid).join("ns/net")).unwrap();
    };
    member("10", &other_ns);
    // A zombie or exiting process has no namespace links left.
    std::fs::create_dir_all(proc_root.join("11")).unwrap();
    std::fs::create_dir_all(proc_root.join("self")).unwrap();
    assert_eq!(netns_occupancy(&proc_root, &slot_ns), Gone);
    assert_eq!(
        netns_occupancy(&proc_root, &root.path().join("absent-ns")),
        Gone
    );
    assert_eq!(
        netns_occupancy(&root.path().join("absent-proc"), &slot_ns),
        Unknown
    );

    // A thread of an exited leader still inside the namespace holds it.
    std::fs::create_dir_all(proc_root.join("11/task/13/ns")).unwrap();
    std::os::unix::fs::symlink(&slot_ns, proc_root.join("11/task/13/ns/net")).unwrap();
    assert_eq!(netns_occupancy(&proc_root, &slot_ns), Live);
    std::fs::remove_dir_all(proc_root.join("11/task")).unwrap();

    member("12", &slot_ns);
    assert_eq!(netns_occupancy(&proc_root, &slot_ns), Live);
}

#[test]
fn released_lease_ignores_the_successor_but_not_its_own_runtime() {
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    set_test_sandbox_runtime_evidence(&sandbox_id, LeaseLiveness::Live);
    // A process that carries the sandbox ID, like a launched sidecar does.
    let mut runtime = std::process::Command::new("sh")
        .args(["-c", "while :; do sleep 1; done"])
        .arg(&sandbox_id)
        // Its `sleep` children outlive the kill below; keep them off the
        // test's output so they do not hold it open.
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let cmdline = format!("/proc/{}/cmdline", runtime.id());
    let give_up = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !std::fs::read(&cmdline)
        .unwrap_or_default()
        .split(|byte| *byte == 0)
        .any(|arg| arg == sandbox_id.as_bytes())
    {
        assert!(
            std::time::Instant::now() < give_up,
            "{cmdline} has no marker"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let released = SlotInfo {
        sandbox_id: sandbox_id.clone(),
        slot: 0,
        generation: 1,
        sandbox_uid: net::sandbox_uid_for_slot(0),
        pid: None,
        pod_netns: None,
    };
    let successor = SlotInfo {
        slot: 1,
        sandbox_uid: net::sandbox_uid_for_slot(1),
        ..released.clone()
    };
    let tracked = SlotInfo {
        pid: Some(runtime.id()),
        ..released.clone()
    };

    // Without a newer lease the sandbox-wide evidence stays authoritative,
    // and the current lease on the same slot is this lease, not a successor.
    assert_eq!(
        released_lease_liveness(&released, None),
        LeaseLiveness::Live
    );
    assert_eq!(
        released_lease_liveness(&released, Some(&released)),
        LeaseLiveness::Live
    );
    assert_eq!(
        released_lease_liveness(&released, Some(&successor)),
        LeaseLiveness::Gone
    );
    // The released lease's own process still holds the reset.
    assert_eq!(
        released_lease_liveness(&tracked, Some(&successor)),
        LeaseLiveness::Live
    );
    // Unless the successor now tracks that PID as its own.
    let successor_owns_pid = SlotInfo {
        pid: tracked.pid,
        ..successor.clone()
    };
    assert_eq!(
        released_lease_liveness(&tracked, Some(&successor_owns_pid)),
        LeaseLiveness::Gone
    );
    let _ = runtime.kill();
    let _ = runtime.wait();
}
