use super::*;

fn reading(at: Instant, usage_usec: u64) -> Reading {
    Reading {
        identity: Identity {
            device: 1,
            inode: 2,
            runtime_pid: 123,
            execution_id: None,
        },
        at,
        usage_usec,
        budget: Budget::parse("200000 100000").expect("two cores"),
    }
}

// These tests exercise accounting and continuity, not emitted telemetry.
#[test]
fn interval_accounting_uses_actual_elapsed_time_and_current_budget() {
    let start = Instant::now();
    for (seconds, used, budget, expected_cores, expected_percent) in [
        (30, 12_000_000, "200000 100000", 0.4, Some(20.0)),
        (40, 40_000_000, "200000 100000", 1.0, Some(50.0)),
        (30, 0, "200000 100000", 0.0, Some(0.0)),
        (30, 60_600_000, "200000 100000", 2.02, Some(101.0)),
        (30, 15_000_000, "50000 100000", 0.5, Some(100.0)),
        (30, 12_000_000, "max 100000", 0.4, None),
    ] {
        let mut first = reading(start, 900);
        first.budget = Budget::parse(budget).expect("valid budget");
        let mut last = first.clone();
        last.at += Duration::from_secs(seconds);
        last.usage_usec += used;
        let mut baseline = Baseline::default();
        assert!(baseline.observe(Some(first)).is_none());
        let usage = baseline.observe(Some(last)).expect("continuous interval");
        assert!((usage.cores - expected_cores).abs() < 1e-9);
        match (usage.pct_of_requested, expected_percent) {
            (Some(actual), Some(expected)) => assert!((actual - expected).abs() < 1e-9),
            (None, None) => {}
            other => panic!("unexpected percentage: {other:?}"),
        }
    }
}

#[test]
fn discontinuities_rebaseline_instead_of_joining_different_intervals() {
    let start = Instant::now();
    let first = reading(start, 100_000_000);
    let next = reading(start + Duration::from_secs(30), 112_000_000);
    let mut cases = vec![("missing reading", None)];
    let mut changed = next.clone();
    changed.usage_usec = 1;
    cases.push(("counter reset", Some(changed)));
    let mut changed = next.clone();
    changed.identity.runtime_pid += 1;
    cases.push(("replacement process", Some(changed)));
    let mut changed = next.clone();
    changed.identity.inode += 1;
    cases.push(("replacement cgroup", Some(changed)));
    let mut changed = next.clone();
    changed.identity.execution_id = Some(SandboxActivationAttemptId::new());
    cases.push(("replacement execution", Some(changed)));
    let mut changed = next.clone();
    changed.budget = Budget::parse("400000 100000").expect("four cores");
    cases.push(("resize", Some(changed)));
    for (label, offset) in [
        ("short window", 1),
        ("long window", 120),
        ("same instant", 0),
    ] {
        let mut changed = next.clone();
        changed.at = start + Duration::from_secs(offset);
        cases.push((label, Some(changed)));
    }
    for (label, changed) in cases {
        let mut baseline = Baseline::default();
        baseline.observe(Some(first.clone()));
        assert!(baseline.observe(changed.clone()).is_none(), "{label}");
        let mut recovery = changed.unwrap_or_else(|| next.clone());
        recovery.at += Duration::from_secs(30);
        recovery.usage_usec += 12_000_000;
        if label == "missing reading" {
            assert!(baseline.observe(Some(recovery.clone())).is_none());
            recovery.at += Duration::from_secs(30);
            recovery.usage_usec += 12_000_000;
        }
        let usage = baseline.observe(Some(recovery)).expect(label);
        assert!((usage.cores - 0.4).abs() < 1e-9, "{label}");
    }
}

#[tokio::test]
async fn cgroup_reader_requires_valid_accounting_and_uses_the_vm_directory() {
    let root = tempfile::tempdir().expect("temporary directory");
    let id = SandboxId::new();
    let info = SidecarInfo {
        sandbox_id: id.to_string(),
        template: "seed".to_string(),
        guest_image: Default::default(),
        node_execution_id: None,
        uid: None,
        runtime_pid: Some(123),
        spaced_host_pid: None,
        netgtw_pid: None,
        vsock_uds_path: None,
        process_env: Default::default(),
        network_tx_bytes: 0,
        network_rx_bytes: 0,
    };
    let reader = Reader::new();
    let path = pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path_in(
        root.path(),
        &id.to_string(),
    );
    std::fs::create_dir(&path).expect("VM cgroup");
    // The sidecar cgroup deliberately has unrelated, valid values.
    let sidecar = root.path().join(format!("sbx-{id}"));
    std::fs::create_dir(&sidecar).expect("sidecar cgroup");
    std::fs::write(sidecar.join("cpu.stat"), "usage_usec 999999\n").expect("sidecar counter");
    std::fs::write(sidecar.join("cpu.max"), "100000 100000\n").expect("sidecar quota");
    for (stat, max, expected) in [
        ("usage_usec 42\nuser_usec 20\n", "200000 100000\n", Some(42)),
        ("usage_usec 0\n", "max 100000\n", Some(0)),
        ("usage_usec 42\n", "0 100000\n", None),
        ("usage_usec 42\n", "max 0\n", None),
        ("usage_usec 42\n", "200000 100000 extra\n", None),
        ("usage_usec 42\n", "garbage\n", None),
        ("user_usec 42\n", "200000 100000\n", None),
        ("usage_usec -1\n", "200000 100000\n", None),
        ("usage_usec 1\nusage_usec 2\n", "200000 100000\n", None),
        ("usage_usec 42 extra\n", "200000 100000\n", None),
    ] {
        std::fs::write(path.join("cpu.stat"), stat).expect("counter");
        std::fs::write(path.join("cpu.max"), max).expect("quota");
        assert_eq!(
            reader
                .read(root.path(), &info, Some(SandboxState::Started))
                .await
                .map(|r| r.usage_usec),
            expected
        );
    }
    std::fs::remove_file(path.join("cpu.stat")).expect("remove counter");
    assert!(
        reader
            .read(root.path(), &info, Some(SandboxState::Started))
            .await
            .is_none()
    );
    std::fs::write(path.join("cpu.stat"), "usage_usec 42\n").expect("restore counter");
    for lifecycle in [
        None,
        Some(SandboxState::Paused),
        Some(SandboxState::Quarantined),
    ] {
        assert!(reader.read(root.path(), &info, lifecycle).await.is_none());
    }
}

#[tokio::test]
async fn accepted_cpu_windows_respect_lifecycle_edges_without_affecting_health() {
    use super::super::{
        ComposedProbeResult, HealthLifecycleEpochs, HealthProbeOutcome,
        apply_composed_health_result,
        tests::{sample, test_health},
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let id = SandboxId::new().to_string();
    let state = Arc::new(Mutex::new(HashMap::from([(id.clone(), test_health())])));
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let edges = HealthLifecycleEpochs {
        state: Arc::clone(&state),
    };
    let start = Instant::now();
    let apply = |seconds, epoch, lifecycle, has_reading: bool| {
        let state = Arc::clone(&state);
        let db = db.clone();
        let id = id.clone();
        async move {
            apply_composed_health_result(
                &state,
                &db,
                Some(Ok((
                    id,
                    Some(lifecycle),
                    epoch,
                    ComposedProbeResult {
                        template: "seed".to_string(),
                        observations: std::array::from_fn(|_| sample(HealthProbeOutcome::Success)),
                        guest_status: None,
                        memory_used_bytes: None,
                        cpu_reading: has_reading.then(|| {
                            reading(start + Duration::from_secs(seconds), seconds * 400_000)
                        }),
                    },
                ))),
            )
            .await
        }
    };
    assert!(
        apply(0, 0, SandboxState::Started, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_none()
    );
    assert!(
        apply(30, 0, SandboxState::Started, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_some()
    );
    edges.record_edge(&id).await;
    assert!(apply(60, 0, SandboxState::Started, true).await.is_none());
    assert!(
        apply(60, 1, SandboxState::Paused, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_none()
    );
    edges.record_edge(&id).await;
    assert!(
        apply(90, 2, SandboxState::Started, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_none()
    );
    assert!(
        apply(120, 2, SandboxState::Started, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_some()
    );
    let before = state.lock().await[&id].health_status;
    assert!(
        apply(150, 2, SandboxState::Started, false)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_none()
    );
    assert_eq!(state.lock().await[&id].health_status, before);
    assert!(
        apply(180, 2, SandboxState::Started, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_none()
    );
    assert!(
        apply(210, 2, SandboxState::Started, true)
            .await
            .expect("accepted probe")
            .cpu_usage
            .is_some()
    );
    state.lock().await.remove(&id);
    assert!(apply(240, 2, SandboxState::Started, true).await.is_none());
}
