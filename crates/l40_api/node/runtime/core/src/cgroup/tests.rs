use super::*;

#[test]
fn parent_memory_budget_is_bounded_by_host_and_logical_capacity() {
    for (child_mib, slots, host_mib, percent, expected_mib) in [
        (
            8320,
            16,
            65_536,
            DEFAULT_PARENT_MEMORY_MAX_HOST_PERCENT,
            58_982,
        ),
        (8320, 16, 65_536, 80, 52_428),
        (8320, 16, 65_536, 100, 65_536),
        (8320, 16, 65_536, 1, 655),
        (1024, 2, 65_536, 90, 2048),
        // A large child must not raise the cap above the host budget.
        (8320, 16, 1024, 90, 921),
    ] {
        let limit = ParentMemoryLimit::for_host(child_mib, slots, host_mib, percent).unwrap();
        assert_eq!(limit.memory_max_mib, expected_mib);
        assert_eq!(limit.memory_max, (expected_mib * MIB_BYTES).to_string());
    }
}

#[test]
fn parent_memory_rejects_invalid_percent_zero_budget_and_overflow() {
    for percent in [0, 101, u32::MAX] {
        assert!(matches!(
            ParentMemoryLimit::for_host(8320, 16, 65_536, percent),
            Err(ParentMemoryLimitError::InvalidPercent(_))
        ));
    }
    for (child, slots, host) in [
        (8320, 0, 65_536),
        (8320, 16, 0),
        (8320, 16, 1),
        (u64::MAX, 16, 65_536),
        (8320, 16, u64::MAX),
    ] {
        assert!(matches!(
            ParentMemoryLimit::for_host(child, slots, host, 90),
            Err(ParentMemoryLimitError::InvalidBudget)
        ));
    }
    assert!(matches!(
        ParentMemoryLimit::for_host(DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB, 16, 65_536, 90),
        Err(ParentMemoryLimitError::InvalidChildLimit)
    ));
}

#[test]
fn parent_setup_enforces_configured_cap_without_usage_floor_or_child_changes() {
    let root = tempfile::tempdir().unwrap();
    for (file, value) in [
        ("memory.oom.group", "1"),
        ("memory.high", "1"),
        ("memory.swap.max", "max"),
        ("memory.max", "max"),
        ("memory.current", "17179869184"),
    ] {
        fs::write(root.path().join(file), value).unwrap();
    }
    let child = root.path().join("child");
    fs::create_dir(&child).unwrap();
    fs::write(child.join("memory.max"), "536870912").unwrap();

    // Repeating startup is idempotent, even when current usage exceeds the cap.
    for _ in 0..2 {
        let limit = ParentMemoryLimit::for_host(1024, 2, 65_536, 90).unwrap();
        apply_parent_memory_limit_at(root.path(), limit).unwrap();
        for (file, expected) in [
            ("memory.oom.group", "0"),
            ("memory.high", "max"),
            ("memory.swap.max", "0"),
            ("memory.max", "2147483648"),
            ("memory.current", "17179869184"),
        ] {
            assert_eq!(
                fs::read_to_string(root.path().join(file)).unwrap(),
                expected
            );
        }
        assert_eq!(
            fs::read_to_string(child.join("memory.max")).unwrap(),
            "536870912"
        );
    }
}

#[test]
fn parent_setup_reports_partial_write_failure() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("memory.high"), "1").unwrap();
    fs::create_dir(root.path().join("memory.max")).unwrap();
    let limit = ParentMemoryLimit::for_host(1024, 2, 65_536, 90).unwrap();
    assert!(matches!(
        apply_parent_memory_limit_at(root.path(), limit),
        Err(ParentMemoryLimitError::Write(
            CgroupUpdateError::Write { .. }
        ))
    ));
    // No rollback to group-wide OOM killing after the hard-cap write fails.
    assert_eq!(
        fs::read_to_string(root.path().join("memory.oom.group")).unwrap(),
        "0"
    );
    // Do not remove an existing throttle if installing the hard cap failed.
    assert_eq!(
        fs::read_to_string(root.path().join("memory.high")).unwrap(),
        "1"
    );
}

#[test]
fn termination_waits_for_known_quiescence_and_removal() {
    // Synthetic filesystem observations: the async supervisor owns timing.
    let root = tempfile::tempdir().unwrap();
    let paths = [root.path().join("sidecar"), root.path().join("vm")];
    for path in &paths {
        fs::create_dir(path).unwrap();
        fs::write(path.join("cgroup.kill"), "").unwrap();
        fs::write(path.join("cgroup.events"), "populated 1\n").unwrap();
    }
    for _ in 0..60 {
        assert!(matches!(
            try_terminate_cgroup_paths(&paths).unwrap(),
            CgroupRemovalOutcome::Busy {
                observation: CgroupBusyObservation::Populated { .. },
                ..
            }
        ));
    }
    for path in &paths {
        assert_eq!(fs::read_to_string(path.join("cgroup.kill")).unwrap(), "1");
    }
    fs::remove_dir_all(&paths[0]).unwrap();
    assert!(matches!(
        try_terminate_cgroup_paths(&paths).unwrap(),
        CgroupRemovalOutcome::Busy { .. }
    ));
    fs::remove_dir_all(&paths[1]).unwrap();
    assert!(
        matches!(
            try_terminate_cgroup_paths(&paths).unwrap(),
            CgroupRemovalOutcome::Removed(_)
        ),
        "repeated Stop is idempotent"
    );
}

#[test]
fn termination_never_treats_unknown_population_as_empty() {
    for events in [Some("populated unknown\n"), None] {
        let root = tempfile::tempdir().unwrap();
        if let Some(events) = events {
            fs::write(root.path().join("cgroup.events"), events).unwrap();
        }
        try_terminate_cgroup_paths(&[root.path().to_owned()]).unwrap_err();
        assert!(root.path().exists());
    }
}

#[test]
fn cgroup_kill_request_writes_kernel_trigger_when_available() {
    let root = tempfile::tempdir().unwrap();
    let kill_file = root.path().join("cgroup.kill");
    fs::write(&kill_file, "").unwrap();

    assert!(request_cgroup_kill(root.path()));
    assert_eq!(fs::read_to_string(kill_file).unwrap(), "1");
}

#[test]
fn cgroup_kill_request_falls_back_when_kernel_trigger_is_absent() {
    let root = tempfile::tempdir().unwrap();

    assert!(!request_cgroup_kill(root.path()));
}

#[test]
fn empty_cgroup_removal_completes_without_retry() {
    let root = tempfile::tempdir().unwrap();
    let cgroup_path = root.path().join("sandbox");
    fs::create_dir(&cgroup_path).unwrap();

    let report = remove_cgroup_path(&cgroup_path).unwrap();

    assert!(report.present);
    assert!(report.removed);
    assert_eq!(report.attempts, 1);
    assert_eq!(report.cgroup_kill_requests, 0);
    assert_eq!(report.killed_pids, 0);
    assert!(!cgroup_path.exists());
}

#[test]
fn removal_recurses_into_child_cgroups() {
    // A jailer-created nested cgroup leaves child directories under the
    // sandbox cgroup. A flat rmdir of the parent fails while children exist;
    // removal must descend and remove them bottom-up first.
    let root = tempfile::tempdir().unwrap();
    let cgroup_path = root.path().join("sandbox");
    fs::create_dir(&cgroup_path).unwrap();
    let child = cgroup_path.join("child");
    fs::create_dir(&child).unwrap();
    fs::create_dir(child.join("grandchild")).unwrap();

    let report = remove_cgroup_path(&cgroup_path)
        .expect("removal should recurse into child cgroups and succeed");

    assert!(report.removed);
    assert_eq!(report.child_cgroups_removed, 2);
    assert!(!cgroup_path.exists());
}

#[test]
fn populated_flag_reflects_cgroup_events() {
    let root = tempfile::tempdir().unwrap();
    let cgroup_path = root.path().join("sandbox");
    assert!(cgroup_is_quiescent(&cgroup_path).unwrap());
    fs::create_dir(&cgroup_path).unwrap();

    // Legacy best-effort enumeration ignores missing events; mutation authority must not.
    assert_eq!(
        cgroup_is_quiescent(&cgroup_path).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(!cgroup_populated(&cgroup_path));
    assert_eq!(
        read_cgroup_populated(&cgroup_path).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );

    fs::write(cgroup_path.join("cgroup.events"), "populated 1\nfrozen 0\n").unwrap();
    assert!(cgroup_populated(&cgroup_path));
    assert!(read_cgroup_populated(&cgroup_path).unwrap());
    assert!(!cgroup_is_quiescent(&cgroup_path).unwrap());

    fs::write(cgroup_path.join("cgroup.events"), "populated 0\nfrozen 0\n").unwrap();
    assert!(!cgroup_populated(&cgroup_path));
    assert!(!read_cgroup_populated(&cgroup_path).unwrap());
    assert!(cgroup_is_quiescent(&cgroup_path).unwrap());
    fs::write(cgroup_path.join("cgroup.events"), "populated unknown\n").unwrap();
    assert_eq!(
        cgroup_is_quiescent(&cgroup_path).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(
        read_cgroup_populated(&cgroup_path).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn reclaim_never_removes_a_populated_cgroup() {
    // Safety invariant: a populated (live) cgroup must be left untouched, so
    // a racing or mis-classified live sandbox is never disturbed.
    let root = tempfile::tempdir().unwrap();
    let cgroup_path = root.path().join("019efbf4-975e-7673-a2a4-4a7b9ad2e63a");
    fs::create_dir(&cgroup_path).unwrap();
    fs::write(cgroup_path.join("cgroup.kill"), "").unwrap();
    fs::write(cgroup_path.join("cgroup.events"), "populated 1\nfrozen 0\n").unwrap();

    assert_eq!(
        reclaim_unpopulated_cgroup(&cgroup_path).unwrap(),
        OrphanReclaimOutcome::Populated
    );
    assert!(cgroup_path.exists(), "populated cgroup must not be removed");
    assert_eq!(
        fs::read_to_string(cgroup_path.join("cgroup.kill")).unwrap(),
        "",
        "a shared live cgroup must not be signalled"
    );
}

#[test]
fn reclaim_removes_unpopulated_cgroup_with_children() {
    let root = tempfile::tempdir().unwrap();
    let cgroup_path = root.path().join("sandbox");
    fs::create_dir(&cgroup_path).unwrap();
    // No cgroup.events file => unpopulated. (On a real cgroupfs the kernel
    // reclaims control files atomically on rmdir; a regular fs does not, so
    // the unit test must not leave stray files in the directory.)
    fs::create_dir(cgroup_path.join("child")).unwrap();

    assert_eq!(
        reclaim_unpopulated_cgroup(&cgroup_path).unwrap(),
        OrphanReclaimOutcome::Reclaimed
    );
    assert!(!cgroup_path.exists());
}

#[test]
fn reclaim_reports_absent_when_already_gone() {
    let root = tempfile::tempdir().unwrap();
    let cgroup_path = root.path().join("missing");
    assert_eq!(
        reclaim_unpopulated_cgroup(&cgroup_path).unwrap(),
        OrphanReclaimOutcome::Absent
    );
}

#[test]
fn list_sandbox_cgroup_dirs_classifies_naming() {
    let root = tempfile::tempdir().unwrap();
    let id = "019efbf4-975e-7673-a2a4-4a7b9ad2e63a";
    fs::create_dir(root.path().join(format!("sbx-{id}"))).unwrap();
    fs::create_dir(root.path().join(id)).unwrap();
    fs::create_dir(root.path().join("notasandbox")).unwrap(); // no '-' => ignored
    // Dashed, >8 chars, but NOT a valid SandboxId: a non-sandbox cgroup
    // under the SPACE parent must never be matched by the destructive sweep.
    fs::create_dir(root.path().join("warm-pool")).unwrap();
    fs::create_dir(root.path().join("sbx-not-a-uuid")).unwrap();
    fs::write(root.path().join("cgroup.procs"), "").unwrap(); // a file => ignored

    let dirs = list_sandbox_cgroup_dirs_in(root.path());
    let ids: Vec<&String> = dirs.iter().map(|(id, _)| id).collect();
    assert_eq!(ids.iter().filter(|i| i.as_str() == id).count(), 2);
    assert!(!ids.iter().any(|i| i.as_str() == "notasandbox"));
    assert!(
        !ids.iter().any(|i| i.as_str() == "warm-pool"),
        "dashed non-sandbox cgroup must not be matched by the reclaim sweep"
    );
    assert!(
        !ids.iter().any(|i| i.as_str() == "not-a-uuid"),
        "sbx-<non-uuid> must not be matched by the reclaim sweep"
    );
}

#[test]
fn fallible_process_reader_preserves_cgroup_read_errors() {
    let root = tempfile::tempdir().unwrap();
    let error = cgroup_process_ids_result(&root.path().join("missing"))
        .expect_err("attestation must distinguish an unreadable cgroup from empty membership");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn invalid_combined_update_leaves_all_cgroup_limits_unchanged() {
    let root = tempfile::tempdir().unwrap();
    for name in ["cpu.max", "memory.max", "pids.max"] {
        fs::write(root.path().join(name), "original").unwrap();
    }
    for (cpu, memory, pids) in [
        (Some(0), Some(512), Some(64)),
        (Some(2), Some(0), Some(64)),
        (Some(2), Some(u64::MAX), Some(64)),
        (Some(2), Some(512), Some(0)),
    ] {
        assert!(matches!(
            update_cgroup_at(root.path(), cpu, memory, pids),
            Err(CgroupUpdateError::Invalid(_))
        ));
        for name in ["cpu.max", "memory.max", "pids.max"] {
            assert_eq!(
                fs::read_to_string(root.path().join(name)).unwrap(),
                "original"
            );
        }
    }
}

#[test]
fn cpu_budget_comes_from_resolved_launch_limits() {
    for requested in [None, Some(1), Some(8)] {
        let limits = CgroupLimits::from_request(requested, None, None);
        assert_eq!(
            limits.cpu_budget().unwrap().get().get(),
            requested.unwrap_or(DEFAULT_VCPU_COUNT)
        );
    }
    for raw in ["max 100000", "0 100000", "100000 0", "150000 100000", "bad"] {
        let limits = CgroupLimits {
            cpu_max: raw.into(),
            ..Default::default()
        };
        assert!(limits.cpu_budget().is_err(), "{raw}");
    }
}

#[test]
fn legacy_cpu_updates_only_reduce_the_current_rate() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cpu.max");
    for (current, requested, expected) in [
        ("800000 100000", 8, true),
        ("800000 100000", 4, true),
        ("200000 100000", 4, false),
        ("300000 200000", 1, true),
        ("300000 200000", 2, false),
        ("max 100000", 8, true),
    ] {
        std::fs::write(&path, current).unwrap();
        assert_eq!(
            cpu_quota_is_nonincreasing_at(directory.path(), requested).unwrap(),
            expected,
            "{current}, {requested}"
        );
    }
    for invalid in ["", "1 0", "max 0", "bogus 100000", "100000 100000 extra"] {
        std::fs::write(&path, invalid).unwrap();
        assert!(
            cpu_quota_is_nonincreasing_at(directory.path(), 1).is_err(),
            "{invalid}"
        );
    }
    std::fs::remove_file(path).unwrap();
    cpu_quota_is_nonincreasing_at(directory.path(), 1).unwrap_err();
}
