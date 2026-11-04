use super::*;

#[test]
fn configuration_precedence_and_owned_sections() {
    for (cli, env, file, expected) in [
        (Some(4), Some(3), Some(2), 4),
        (None, Some(3), Some(2), 3),
        (None, None, Some(2), 2),
        (None, None, None, 1),
        (Some(0), Some(3), Some(2), 0),
    ] {
        assert_eq!(setting("test", cli, env, file, 1), expected);
    }
    for section in [
        "network_pool",
        "sandbox_cgroup",
        "memory_admission",
        "disk_io_reservation",
    ] {
        toml::from_str::<NodeConfig>(&format!("[{section}]\nmisspelled = 1")).unwrap_err();
    }
    // The shared file contains sections owned by other daemons.
    let config: NodeConfig = toml::from_str(
        "[other_service]\nnew_setting = true\n[network_pool]\ntarget = 3\nretain_ready = 2\nmin_ready = 1",
    ).unwrap();
    assert_eq!(config.network_pool.target_ready_slots, Some(3));
    assert_eq!(config.network_pool.retained_ready_slots, Some(2));
    assert_eq!(config.network_pool.min_ready_slots, Some(1));
    toml::from_str::<NodeConfig>("[network_pool]\nmax_slots = -1").unwrap_err();
    toml::from_str::<NodeConfig>("[network_pool]\ntarget = 1\ntarget_ready_slots = 2").unwrap_err();
}

// Invoked in a separate test process by the handoff test; ordinary suite
// runs have no probe path and do nothing. Uses the real flock boundary.
#[test]
fn handoff_mutex_probe() {
    let Ok(path) = std::env::var("SPACE_NODEMGR_TEST_MUTEX") else {
        return;
    };
    let expect_busy = std::env::var("SPACE_NODEMGR_TEST_MUTEX_BUSY").unwrap() == "1";
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(ServiceMutex::acquire(
        path,
        CancellationToken::new(),
        Duration::from_millis(1),
        Some(Duration::from_millis(20)),
    ));
    if expect_busy {
        assert!(matches!(
            result,
            Err(pplx_space_common_svc::service_mutex::AcquireError::Timeout { .. })
        ));
    } else {
        assert!(
            result.is_ok(),
            "successor must acquire after complete drain: {result:?}"
        );
    }
}

#[tokio::test]
async fn drain_deadline_retains_mutex_until_owned_child_exits() {
    let path = std::env::temp_dir().join(format!(
        "space-nodemgr-handoff-{}-{}.lock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mutex = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(1),
        None,
    )
    .await
    .unwrap();
    let tracker = TaskTracker::new();
    // cat exits only once this test closes stdin. No shell grandchildren and
    // no sleep-based guess about whether the owned child is still running.
    let mut child = std::process::Command::new("/bin/cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let child_input = child.stdin.take().unwrap();
    tracker.spawn_blocking(move || {
        assert!(child.wait().unwrap().success());
    });
    tracker.close();
    let drain = tokio::spawn(async move {
        drain_before_handoff(tracker.wait(), Duration::ZERO).await;
        drop(mutex);
    });
    let mut drain = Box::pin(drain);
    tokio::time::timeout(Duration::from_millis(30), &mut drain)
        .await
        .expect_err("drain deadline must retain ownership");
    let probe = |busy: bool| {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "run::tests::handoff_mutex_probe", "--nocapture"])
            .env("SPACE_NODEMGR_TEST_MUTEX", &path)
            .env(
                "SPACE_NODEMGR_TEST_MUTEX_BUSY",
                if busy { "1" } else { "0" },
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    };
    probe(true);
    drop(child_input);
    drain.await.unwrap();
    probe(false);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn runtime_supervisor_activation_is_opt_in() {
    assert!(!runtime_supervisor_activation_enabled(None));
    assert!(!runtime_supervisor_activation_enabled(Some("false")));
    assert!(runtime_supervisor_activation_enabled(Some("true")));
}

#[test]
fn service_port_ingress_isolation_auto_uses_ingress_sharing_signal() {
    assert_eq!(
        resolve_service_port_ingress_isolation(Some("auto"), true).unwrap(),
        ServicePortIngressIsolation::SharedHost
    );
    assert_eq!(
        resolve_service_port_ingress_isolation(Some("auto"), false).unwrap(),
        ServicePortIngressIsolation::ExclusiveHost
    );
    assert_eq!(
        resolve_service_port_ingress_isolation(None, true).unwrap(),
        ServicePortIngressIsolation::SharedHost
    );
    assert_eq!(
        resolve_service_port_ingress_isolation(None, false).unwrap(),
        ServicePortIngressIsolation::ExclusiveHost
    );
}

#[test]
fn service_port_ingress_isolation_explicit_values_override_auto() {
    assert_eq!(
        resolve_service_port_ingress_isolation(Some("shared_host"), false).unwrap(),
        ServicePortIngressIsolation::SharedHost
    );
    assert_eq!(
        resolve_service_port_ingress_isolation(Some("exclusive_host"), true).unwrap(),
        ServicePortIngressIsolation::ExclusiveHost
    );
    resolve_service_port_ingress_isolation(Some("k8s"), true).unwrap_err();
}
