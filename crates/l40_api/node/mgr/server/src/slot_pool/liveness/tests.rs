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
