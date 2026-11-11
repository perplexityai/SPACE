use super::*;

fn roots(dir: &Path) -> [PathBuf; 6] {
    let roots = ["netns", "cgroup", "run", "cache", "proc", "gateway"].map(|name| dir.join(name));
    for root in &roots {
        std::fs::create_dir(root).unwrap();
    }
    roots
}

#[test]
fn paged_walk_is_complete_bounded_and_never_claims_failed_evidence_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let namespaces = roots.first().unwrap();
    let expected: HashSet<_> = (0..600).map(|n| format!("sns-{n:06}")).collect();
    for name in &expected {
        std::fs::write(namespaces.join(name), []).unwrap();
    }
    let cache = roots.get(3).unwrap();
    for shared in ["shared", ".fork-copy-staging", "templates"] {
        std::fs::create_dir(cache.join(shared)).unwrap();
    }
    let orphan = SandboxId::new().to_string();
    std::fs::create_dir(cache.join(&orphan)).unwrap();
    let expected_runs: HashSet<_> = [2, 5]
        .map(|index| {
            let path = roots.get(index).unwrap().join(&orphan);
            std::fs::create_dir(&path).unwrap();
            path.display().to_string()
        })
        .into_iter()
        .collect();
    let registry = OrphanInventory::default();
    let mut token = String::new();
    let mut seen = HashSet::new();
    let mut seen_cache = Vec::new();
    let mut seen_runs = HashSet::new();
    let mut pages = 0;
    loop {
        let page = registry
            .page_with(
                ListOrphanCandidatesRequest { page_token: token },
                &[],
                &roots,
                || Ok(vec![]),
            )
            .unwrap();
        assert!(
            page.orphaned_namespaces.len()
                + page.orphaned_cache_dirs.len()
                + page.orphaned_run_dirs.len()
                <= PAGE_ENTRIES
        );
        assert!(page.scan_started_at_unix_ms > 0);
        seen.extend(page.orphaned_namespaces);
        seen_cache.extend(page.orphaned_cache_dirs);
        seen_runs.extend(page.orphaned_run_dirs);
        pages += 1;
        token = page.next_page_token;
        if token.is_empty() {
            assert!(page.complete);
            break;
        }
        assert!(!page.complete);
    }
    assert!(pages > 1);
    assert_eq!(seen, expected);
    assert_eq!(seen_cache, [cache.join(orphan).display().to_string()]);
    assert_eq!(seen_runs, expected_runs);
    assert!(registry.0.lock().unwrap().is_empty());

    let mut missing_roots = roots.clone();
    *missing_roots.first_mut().unwrap() = dir.path().join("missing-netns");
    let page = registry
        .page_with(
            ListOrphanCandidatesRequest {
                page_token: String::new(),
            },
            &[],
            &missing_roots,
            || Ok(vec![]),
        )
        .unwrap();
    assert!(!page.complete);
    assert!(page.next_page_token.is_empty());
    assert!(!page.warnings.is_empty());
    assert_eq!(page.orphaned_cache_dirs.len(), 1);
}

#[test]
fn cursor_limits_expiry_and_replay_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    for n in 0..600 {
        std::fs::write(roots.first().unwrap().join(format!("sns-{n}")), []).unwrap();
    }
    let registry = OrphanInventory::default();
    let read = |token: String| {
        registry.page_with(
            ListOrphanCandidatesRequest { page_token: token },
            &[],
            &roots,
            || Ok(vec![]),
        )
    };
    let first = read(String::new()).unwrap();
    let second = read(first.next_page_token.clone()).unwrap();
    assert_eq!(
        read(first.next_page_token).unwrap_err().code(),
        tonic::Code::Aborted
    );
    for _ in 1..MAX_SCANS {
        read(String::new()).unwrap();
    }
    // Force the abandoned continuation to be the LRU without wall-clock waits.
    let id: Uuid = second
        .next_page_token
        .split_once(':')
        .unwrap()
        .0
        .parse()
        .unwrap();
    registry.0.lock().unwrap().get_mut(&id).unwrap().touched =
        Instant::now() - Duration::from_secs(1);
    let replacement = read(String::new()).unwrap();
    assert!(!replacement.next_page_token.is_empty());
    assert_eq!(registry.0.lock().unwrap().len(), MAX_SCANS);
    assert_eq!(
        read(second.next_page_token).unwrap_err().code(),
        tonic::Code::Aborted
    );
    for scan in registry.0.lock().unwrap().values_mut() {
        scan.touched = Instant::now() - CURSOR_IDLE_TTL;
    }
    assert_eq!(
        read(replacement.next_page_token).unwrap_err().code(),
        tonic::Code::Aborted
    );
    assert!(registry.0.lock().unwrap().is_empty());
}

#[test]
fn process_evidence_preserves_tracked_ancestry_and_unreadable_is_not_absence() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let processes = roots.get(4).unwrap();
    let tracked = SandboxId::new().to_string();
    let orphan = SandboxId::new().to_string();
    for (pid, cgroup) in [
        (101, format!("0::/space/sbx-{tracked}/child\n").into_bytes()),
        (102, format!("0::/space/{orphan}\n").into_bytes()),
        (103, vec![255]),
    ] {
        let process = processes.join(pid.to_string());
        std::fs::create_dir(&process).unwrap();
        std::fs::write(process.join("comm"), "firecracker\n").unwrap();
        std::fs::write(process.join("cgroup"), cgroup).unwrap();
    }
    // execveat on older kernels can leave a numeric comm. The executable
    // identity still identifies the VMM; cgroup ancestry protects live leases.
    #[cfg(unix)]
    for (pid, owner) in [(104, &orphan), (105, &tracked)] {
        let process = processes.join(pid.to_string());
        std::fs::create_dir(&process).unwrap();
        std::fs::write(process.join("comm"), "7\n").unwrap();
        std::os::unix::fs::symlink("/runtime/bin/cloud-hypervisor", process.join("exe")).unwrap();
        std::fs::write(
            process.join("cgroup"),
            format!("0::/space/sbx-{owner}/vm\n"),
        )
        .unwrap();
    }
    let evidence = Evidence {
        sandboxes: HashSet::from([tracked]),
        slots: HashSet::new(),
        pids: HashSet::new(),
    };
    let page = Scan::new().page(&evidence, &roots, || Ok(vec![]));
    let mut expected = vec![format!("102:{orphan}")];
    #[cfg(unix)]
    expected.push(format!("104:{orphan}"));
    let mut found = page.orphaned_processes;
    found.sort();
    assert_eq!(found, expected);
    assert!(!page.complete);
    assert!(page.next_page_token.is_empty());
    assert!(!page.warnings.is_empty());
}
