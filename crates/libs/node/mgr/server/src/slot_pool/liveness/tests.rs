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
                    let sources = [
                        EvidenceSource::SidecarCgroup,
                        EvidenceSource::VmCgroup,
                        EvidenceSource::TrackedPid,
                        EvidenceSource::FcPid,
                    ];
                    let blocker = RuntimeBlocker::first(
                        std::iter::zip(evidence, sources)
                            .map(|(liveness, source)| RuntimeBlocker::new(liveness, source, None)),
                    );
                    assert_eq!(
                        blocker.map_or(Gone, |blocker| blocker.liveness),
                        expected,
                        "{evidence:?}"
                    );
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

/// A `/proc` tree whose namespaces are plain files: a task is a member of
/// the namespace its `ns/net` link resolves to.
struct FakeHost {
    root: tempfile::TempDir,
    proc_root: PathBuf,
}

/// A thread's ID, `State` and namespace; an exited thread has none.
type FakeThread<'a> = (u32, &'a str, Option<&'a str>);

const VM_CGROUP: &str = "0::/space/sandbox-a\n";
const SIDECAR_CGROUP: &str = "0::/space/sbx-sandbox-a/gateway\n";
const OTHER_CGROUP: &str = "0::/kubepods/pod-a/container-a\n";

impl FakeHost {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let proc_root = root.path().join("proc");
        std::fs::create_dir_all(proc_root.join("self")).unwrap();
        Self { root, proc_root }
    }

    fn proc_root(&self) -> &Path {
        &self.proc_root
    }

    fn namespace(&self, name: &str) -> NamespaceId {
        let path = self.root.path().join(name);
        if !path.exists() {
            std::fs::write(&path, b"").unwrap();
        }
        let metadata = std::fs::metadata(path).unwrap();
        (metadata.dev(), metadata.ino())
    }

    /// A process running as `uid` in `cgroup`. Its leader is the thread
    /// whose ID is `pid`.
    fn process(&self, pid: u32, uid: u32, cgroup: &str, threads: &[FakeThread<'_>]) {
        let process = self.proc_root().join(pid.to_string());
        std::fs::create_dir_all(&process).unwrap();
        let state = |state: &str| format!("State:\t{state}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n");
        let leader = threads
            .iter()
            .find(|(tid, ..)| *tid == pid)
            .map_or("S", |(_, state, _)| state);
        std::fs::write(process.join("status"), state(leader)).unwrap();
        std::fs::write(process.join("cgroup"), cgroup).unwrap();
        for (tid, thread_state, namespace) in threads {
            let task = process.join("task").join(tid.to_string());
            std::fs::create_dir_all(&task).unwrap();
            std::fs::write(task.join("status"), state(thread_state)).unwrap();
            if let Some(namespace) = namespace {
                self.namespace(namespace);
                std::fs::create_dir_all(task.join("ns")).unwrap();
                std::os::unix::fs::symlink(self.root.path().join(namespace), task.join("ns/net"))
                    .unwrap();
            }
        }
    }

    fn remove(&self, pid: u32) {
        std::fs::remove_dir_all(self.proc_root().join(pid.to_string())).unwrap();
    }

    fn view(&self, namespace: Option<NamespaceId>) -> SlotHost<'_> {
        SlotHost {
            proc_root: &self.proc_root,
            namespace: Ok(namespace),
            sandbox_cgroups: Ok(vec![CgroupMembers {
                populated: true,
                pids: vec![20],
            }]),
        }
    }
}

fn released_and_successor() -> (SlotInfo, SlotInfo) {
    let released = SlotInfo {
        sandbox_id: "sandbox-a".to_string(),
        slot: 3,
        generation: 1,
        sandbox_uid: net::sandbox_uid_for_slot(3),
        pid: None,
        pod_netns: None,
    };
    let successor = SlotInfo {
        slot: 4,
        generation: 2,
        sandbox_uid: net::sandbox_uid_for_slot(4),
        ..released.clone()
    };
    (released, successor)
}

fn gateway_uid(info: &SlotInfo) -> u32 {
    pplx_space_common_core::host::linux::gateway::gateway_uid(
        pplx_space_common_core::types::LinuxUid::new(info.sandbox_uid),
    )
    .unwrap()
    .get()
}

/// A host with node-manager, the successor's VM and an exited process.
fn host_with_successor_running(successor: &SlotInfo) -> FakeHost {
    let host = FakeHost::new();
    host.process(
        10,
        0,
        OTHER_CGROUP,
        &[(10, "S", Some("host-ns")), (11, "S", Some("host-ns"))],
    );
    host.process(
        20,
        successor.sandbox_uid,
        VM_CGROUP,
        &[
            (20, "S", Some("successor-ns")),
            (21, "S", Some("successor-ns")),
        ],
    );
    std::fs::create_dir_all(host.proc_root().join("30")).unwrap();
    host
}

#[test]
fn released_slot_is_live_while_any_thread_remains_in_its_namespace() {
    use EvidenceSource::NetnsMember;
    use LeaseLiveness::Live;
    let (released, successor) = released_and_successor();
    let host = host_with_successor_running(&successor);
    let view = host.view(Some(host.namespace("slot-ns")));
    assert_eq!(released_lease_runtime_with(&released, &view), None);

    // A root jailer that has called `setns`, from a thread other than its
    // leader, before it drops to the slot's UID.
    host.process(
        40,
        0,
        OTHER_CGROUP,
        &[(40, "S", Some("host-ns")), (41, "S", Some("slot-ns"))],
    );
    assert_eq!(
        released_lease_runtime_with(&released, &view),
        RuntimeBlocker::new(Live, NetnsMember, Some(40))
    );
    host.remove(40);

    // vCPU threads still tearing the guest down after their leader exited.
    host.process(
        50,
        0,
        VM_CGROUP,
        &[(50, "Z", None), (51, "S", Some("slot-ns"))],
    );
    assert_eq!(
        released_lease_runtime_with(&released, &view),
        RuntimeBlocker::new(Live, NetnsMember, Some(50))
    );
    host.remove(50);
    assert_eq!(released_lease_runtime_with(&released, &view), None);
}

#[test]
fn released_slot_without_its_namespace_mount_is_judged_by_its_uids() {
    use EvidenceSource::{NetnsUnverifiable, ProcessUnreadable, SlotUid};
    use LeaseLiveness::{Live, Unknown};
    let (released, successor) = released_and_successor();
    let host = host_with_successor_running(&successor);
    // An earlier attempt of the reset deleted the mount, then failed.
    let view = host.view(None);
    assert_eq!(released_lease_runtime_with(&released, &view), None);

    // The released lease's VM or gateway, left in the now-anonymous
    // namespace, runs as its slot's UID in the sandbox's cgroups.
    for (uid, cgroup) in [
        (released.sandbox_uid, VM_CGROUP),
        (gateway_uid(&released), SIDECAR_CGROUP),
    ] {
        host.process(60, uid, cgroup, &[(60, "S", Some("anonymous-ns"))]);
        assert_eq!(
            released_lease_runtime_with(&released, &view),
            RuntimeBlocker::new(Live, SlotUid, Some(60))
        );
        host.remove(60);
    }

    // Any other workload on the host may run at a UID in the slot range.
    host.process(
        61,
        released.sandbox_uid,
        OTHER_CGROUP,
        &[(61, "S", Some("pod-ns"))],
    );
    assert_eq!(released_lease_runtime_with(&released, &view), None);
    host.remove(61);

    // An exited, unreaped process is not a survivor, unless one of its
    // threads still runs.
    host.process(62, released.sandbox_uid, VM_CGROUP, &[(62, "Z", None)]);
    assert_eq!(released_lease_runtime_with(&released, &view), None);
    host.process(
        62,
        released.sandbox_uid,
        VM_CGROUP,
        &[(62, "Z", None), (63, "S", Some("anonymous-ns"))],
    );
    assert_eq!(
        released_lease_runtime_with(&released, &view),
        RuntimeBlocker::new(Live, SlotUid, Some(62))
    );
    host.remove(62);

    // A process whose credentials cannot be read is not proof of absence.
    std::fs::create_dir_all(host.proc_root().join("70/status")).unwrap();
    assert_eq!(
        released_lease_runtime_with(&released, &view),
        RuntimeBlocker::new(Unknown, ProcessUnreadable, Some(70))
    );
    host.remove(70);

    let unverifiable = SlotHost {
        namespace: Err(std::io::ErrorKind::PermissionDenied.into()),
        ..host.view(None)
    };
    assert_eq!(
        released_lease_runtime_with(&released, &unverifiable),
        RuntimeBlocker::new(Unknown, NetnsUnverifiable, None)
    );
}

#[test]
fn released_slot_is_never_gone_without_a_complete_pid_view() {
    use EvidenceSource::{NetnsMember, PidNamespaceIncomplete};
    use LeaseLiveness::{Live, Unknown};
    let (released, successor) = released_and_successor();
    let host = host_with_successor_running(&successor);
    let slot_ns = host.namespace("slot-ns");
    let members = |populated, pids| Ok(vec![CgroupMembers { populated, pids }]);
    let empty = SlotHost {
        sandbox_cgroups: members(false, vec![]),
        ..host.view(Some(slot_ns))
    };
    assert_eq!(released_lease_runtime_with(&released, &empty), None);
    for sandbox_cgroups in [
        // The kernel lists a cgroup member outside this PID namespace as 0.
        members(true, vec![20, 0]),
        // It skips an exiting leader, which can still hold the namespace.
        members(true, vec![]),
        Err(std::io::ErrorKind::PermissionDenied.into()),
    ] {
        let view = SlotHost {
            sandbox_cgroups,
            ..host.view(Some(slot_ns))
        };
        assert_eq!(
            released_lease_runtime_with(&released, &view),
            RuntimeBlocker::new(Unknown, PidNamespaceIncomplete, None)
        );
        // A member it can see is still decisive.
        host.process(40, 0, OTHER_CGROUP, &[(40, "S", Some("slot-ns"))]);
        assert_eq!(
            released_lease_runtime_with(&released, &view),
            RuntimeBlocker::new(Live, NetnsMember, Some(40))
        );
        host.remove(40);
    }
}

#[test]
fn a_tracked_pid_counts_only_while_it_is_the_released_slots_process() {
    use EvidenceSource::TrackedPid;
    use LeaseLiveness::Live;
    let (released, successor) = released_and_successor();
    let host = host_with_successor_running(&successor);
    let view = host.view(Some(host.namespace("slot-ns")));
    let tracked = SlotInfo {
        pid: Some(80),
        ..released.clone()
    };
    // The released lease's gateway, outside any SPACE namespace.
    host.process(
        80,
        gateway_uid(&released),
        SIDECAR_CGROUP,
        &[(80, "S", Some("host-ns"))],
    );
    assert_eq!(
        released_lease_runtime_with(&tracked, &view),
        RuntimeBlocker::new(Live, TrackedPid, Some(80))
    );
    // The successor's Firecracker has since been given the same PID.
    host.remove(80);
    host.process(
        80,
        successor.sandbox_uid,
        VM_CGROUP,
        &[(80, "S", Some("successor-ns"))],
    );
    assert_eq!(released_lease_runtime_with(&tracked, &view), None);
}

#[test]
fn released_lease_without_a_successor_keeps_sandbox_wide_evidence() {
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    set_test_runtime_evidence(
        &sandbox_id,
        TestRuntimeEvidence {
            sandbox: LeaseLiveness::Live,
            slot: None,
        },
    );
    let (released, successor) = released_and_successor();
    let released = SlotInfo {
        sandbox_id: sandbox_id.clone(),
        ..released
    };
    let successor = SlotInfo {
        sandbox_id,
        ..successor
    };
    let live = RuntimeBlocker::new(LeaseLiveness::Live, EvidenceSource::SidecarCgroup, None);
    assert_eq!(released_lease_runtime(&released, None), live);
    // The current lease on the same slot is this lease, not a successor.
    assert_eq!(released_lease_runtime(&released, Some(&released)), live);
    assert_eq!(released_lease_runtime(&released, Some(&successor)), None);
}

#[cfg(target_os = "linux")]
#[test]
fn only_an_nsfs_inode_identifies_the_slot_namespace() {
    let proc_root = Path::new("/proc");
    let own = std::fs::metadata("/proc/self/ns/net").unwrap();
    assert_eq!(
        mounted_namespace(proc_root, Path::new("/proc/self/ns/net")).unwrap(),
        Some((own.dev(), own.ino()))
    );
    // The file an unmount leaves behind when its unlink fails.
    let placeholder = tempfile::NamedTempFile::new().unwrap();
    assert_eq!(
        mounted_namespace(proc_root, placeholder.path()).unwrap(),
        None
    );
    let absent = placeholder.path().with_extension("absent");
    assert_eq!(mounted_namespace(proc_root, &absent).unwrap(), None);
}
