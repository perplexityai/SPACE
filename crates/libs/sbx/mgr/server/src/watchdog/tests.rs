use super::*;
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, ExecutionGeneration, NodeId, PodUid, RouteGeneration, SandboxId,
    SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
    SandboxRuntimeLabel, SpaceApiRouteIdentity,
};

#[tokio::test]
async fn gateway_metric_registry_forgets_retired_builds() {
    let state = Arc::new(Mutex::new(HashMap::new()));
    let mut published = HashSet::new();
    let mut published_inventory = HashSet::new();
    for build in 0..100 {
        let mut health = test_health();
        health.netgtw_alive = true;
        health.netgtw_git_sha = Some(format!("build-{build}"));
        state.lock().await.insert("sandbox".to_owned(), health);
        emit_net_gateway_live_gauges(&state, &[], &mut published, &mut published_inventory).await;
        assert_eq!(published.len(), 1);
        assert!(published.contains(&(format!("build-{build}"), NetGatewayLifecycle::Unknown)));
    }
    state.lock().await.clear();
    emit_net_gateway_live_gauges(&state, &[], &mut published, &mut published_inventory).await;
    assert!(published.is_empty());
}

fn insert_test_sandbox(conn: &crate::db::Connection, id: &str, state: crate::state::SandboxState) {
    let uuid8 = id
        .parse::<SandboxId>()
        .map(SandboxRuntimeLabel::from_sandbox_id)
        .unwrap_or_else(|_| {
            SandboxRuntimeLabel::parse(id).expect("test sandbox id must parse as runtime label")
        });
    let user =
        pplx_space_common_core::types::SandboxOwner::parse("user").expect("test owner must parse");
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id,
            uuid8: &uuid8,
            state,
            template: "seed",
            template_hash: Some("hash"),
            user: &user,
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &BTreeMap::new(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .expect("test sandbox insert must succeed");
}

pub(super) fn test_health() -> SandboxHealth {
    SandboxHealth {
        guest_uptime_secs: 10,
        guest_healthy: true,
        guest_ready: false,
        consecutive_guest_probe_failures: 0,
        runtime_alive: true,
        vsock_raw_ping_ok: false,
        spaced_host_alive: true,
        netgtw_alive: false,
        network_tx_bytes: 0,
        network_rx_bytes: 0,
        last_guest_ready_at: None,
        last_guest_heartbeat_at: None,
        spaced_version: None,
        netgtw_git_sha: None,
        netgtw_stats_pid: None,
        netgtw_tls_interception_mode: NetGatewayTlsInterceptionMode::Unknown,
        netgtw_identity_profile_version: None,
        netgtw_route_verification_key_generation: None,
        netgtw_route_verification_window: NetGatewayRouteVerificationWindow::new(Instant::now(), 0),
        health_status: ComposedHealthStatus::Initializing,
        health_observed_at: None,
        health_components: HealthComponents::default(),
        lifecycle_epoch: 0,
        next_health_probe_at: Instant::now(),
        health_probe_sequence: 0,
        last_console_tail_at: None,
        node_lease_backoff: None,
    }
}

fn test_route_identity() -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        route_generation: Some(RouteGeneration::new(42).expect("route generation parses")),
        current_node_id: Some(NodeId::new()),
        substrate_kind: Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes),
        execution_generation: Some(ExecutionGeneration::new(7).expect("generation parses")),
        pod_uid: Some(PodUid::parse("pod-uid-42").expect("pod uid parses")),
        ..Default::default()
    }
}

fn set_runtime_fields(
    conn: &crate::db::Connection,
    id: &str,
    space_api_route_identity: SpaceApiRouteIdentity,
) {
    crate::db::update_runtime_fields(
        conn,
        id.parse().unwrap(),
        crate::db::RuntimeFields {
            uid: 20_001,
            slot: 1,
            guest_cid: 42,
            pid: 30_001,
            spaced_host_pid: Some(30_002),
            netgtw_pid: 30_003,
            vsock_uds_path: "/tmp/vsock.sock",
            api_socket_path: "/tmp/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity,
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        },
    )
    .expect("runtime fields update");
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|position| args.get(position + 1))
        .map(String::as_str)
}

fn restart_test_info(sandbox_id: &str) -> SidecarInfo {
    SidecarInfo {
        guest_image: Default::default(),
        node_execution_id: None,
        sandbox_id: sandbox_id.to_string(),
        uid: Some(20_001),
        runtime_pid: Some(30_001),
        spaced_host_pid: Some(30_002),
        netgtw_pid: Some(30_003),
        vsock_uds_path: Some("/tmp/vsock.sock".to_string()),
        process_env: BTreeMap::new(),
        network_tx_bytes: 0,
        network_rx_bytes: 0,
    }
}

#[test]
fn sidecar_restart_queue_deduplicates_and_releases_after_completion() {
    let now = tokio::time::Instant::now();
    let mut queue = SidecarRestartQueue::default();
    let spaced = SidecarRestartRequest::SpacedHost(restart_test_info("sandbox-a"));
    let spaced_key = spaced.key();
    assert!(queue.schedule(spaced, now));
    assert!(!queue.schedule(
        SidecarRestartRequest::SpacedHost(restart_test_info("sandbox-a")),
        now
    ));
    assert!(queue.schedule(
        SidecarRestartRequest::NetGateway(restart_test_info("sandbox-a")),
        now
    ));

    assert_eq!(queue.pop_front().expect("spaced request").key(), spaced_key);
    assert_eq!(
        queue.pop_front().expect("gateway request").key().kind,
        SidecarKind::NetGateway
    );
    assert!(queue.pop_front().is_none());
    assert!(!queue.schedule(
        SidecarRestartRequest::SpacedHost(restart_test_info("sandbox-a")),
        now
    ));

    queue.complete(&spaced_key);
    assert!(queue.schedule(
        SidecarRestartRequest::SpacedHost(restart_test_info("sandbox-a")),
        now
    ));
}

#[test]
fn sidecar_restart_queue_prunes_stale_pending_generation() {
    let now = tokio::time::Instant::now();
    let mut queue = SidecarRestartQueue::default();
    let stale = SidecarRestartRequest::SpacedHost(restart_test_info("sandbox-a"));
    let key = stale.key();
    assert!(queue.schedule(stale, now));

    queue.retain_pending(|_| false);

    assert!(queue.pending.is_empty());
    assert!(!queue.scheduled.contains_key(&key));
    assert!(queue.schedule(
        SidecarRestartRequest::SpacedHost(restart_test_info("sandbox-a")),
        now
    ));
}

#[test]
fn sidecar_liveness_requires_the_committed_pid() {
    assert!(!sidecar_pid_is_committed(None, None));
    assert!(!sidecar_pid_is_committed(None, Some(42)));
    assert!(!sidecar_pid_is_committed(Some(41), Some(42)));
    assert!(sidecar_pid_is_committed(Some(42), Some(42)));
}

#[test]
fn net_gateway_relaunch_args_include_current_route_identity() {
    let id = "0199c1e0-0000-7000-8000-000000000002";
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, id, crate::state::SandboxState::Started);
    let identity = test_route_identity();
    set_runtime_fields(&conn, id, identity.clone());

    let db = crate::db::SharedConnection::new(conn);
    let flags = pplx_space_feature_flags::FeatureFlagClient::disabled();
    let args = net_gateway_relaunch_args(&db, id, &flags).expect("relaunch args build");

    assert!(!has_flag(&args, "--tls-interception-mode"));
    assert_eq!(flag_value(&args, "--route-generation"), Some("42"));
    assert_eq!(
        flag_value(&args, "--current-node-id"),
        identity
            .current_node_id
            .map(|node_id| node_id.to_string())
            .as_deref()
    );
    assert_eq!(flag_value(&args, "--execution-generation"), Some("7"));
    assert_eq!(flag_value(&args, "--pod-uid"), Some("pod-uid-42"));
    assert!(!has_flag(&args, "--no-identity"));
}

#[test]
fn net_gateway_relaunch_args_keep_pool_gateway_no_identity() {
    let id = "0199c1e0-0000-7000-8000-000000000003";
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, id, crate::state::SandboxState::Started);
    conn.execute(
        "UPDATE sandboxes SET pool_template = 'seed' WHERE id = ?1",
        crate::db::fixture::params![id],
    )
    .expect("pool template update");
    set_runtime_fields(&conn, id, test_route_identity());

    let db = crate::db::SharedConnection::new(conn);
    let flags = pplx_space_feature_flags::FeatureFlagClient::disabled();
    let args = net_gateway_relaunch_args(&db, id, &flags).expect("relaunch args build");

    assert!(!has_flag(&args, "--tls-interception-mode"));
    assert!(has_flag(&args, "--no-identity"));
    for flag in [
        "--route-generation",
        "--current-node-id",
        "--execution-generation",
        "--pod-uid",
    ] {
        assert!(!has_flag(&args, flag));
    }
}

// Regression (review-pointed): after a deliberate gateway cycle, the
// watchdog cache kept advertising the new process under the old process'
// build sha until the next staggered stats probe (~30s), so an immediate
// `cycle-net-gateway --stale` rerun would re-select and re-cycle the
// freshly relaunched gateway. Every relaunch path funnels through
// `relaunch_netgtw`, which pairs the relaunch with this clear; the clear
// itself is exercised here (the full relaunch needs a live node-manager).
#[tokio::test]
async fn clear_netgtw_probe_state_drops_cached_process_observations() {
    let conn = crate::db::test_conn();
    let watchdog = SandboxWatchdog::new_for_tests(
        crate::db::SharedConnection::new(conn),
        NetworkPolicyReconcileQueue::default(),
        CancellationToken::new(),
        &TaskTracker::new(),
    );
    watchdog
        .add_sandbox(SidecarInfo {
            guest_image: Default::default(),
            node_execution_id: None,
            sandbox_id: "0199c1e0-0000-7000-8000-000000000001".to_string(),
            uid: Some(20001),
            runtime_pid: None,
            spaced_host_pid: None,
            netgtw_pid: None,
            vsock_uds_path: None,
            process_env: BTreeMap::new(),
            network_tx_bytes: 0,
            network_rx_bytes: 0,
        })
        .await;
    {
        let mut map = watchdog.state.lock().await;
        map.get_mut("0199c1e0-0000-7000-8000-000000000001")
            .expect("sandbox health entry must exist")
            .netgtw_git_sha = Some("sha-old".to_string());
        let health = map
            .get_mut("0199c1e0-0000-7000-8000-000000000001")
            .expect("sandbox health entry must exist");
        health.netgtw_stats_pid = Some(30_003);
        health.netgtw_tls_interception_mode = NetGatewayTlsInterceptionMode::On;
        health.netgtw_identity_profile_version = Some(1);
        health.netgtw_route_verification_key_generation = Some(7);
    }

    {
        let mut state = watchdog.state.lock().await;
        clear_netgtw_probe_state(&mut state, "0199c1e0-0000-7000-8000-000000000001");
    }

    let health = watchdog
        .get_health("0199c1e0-0000-7000-8000-000000000001")
        .await
        .expect("sandbox health entry must exist");
    assert_eq!(
        health.netgtw_git_sha, None,
        "relaunched gateway must not keep advertising the old process' sha"
    );
    assert_eq!(health.netgtw_stats_pid, None);
    assert_eq!(
        health.netgtw_tls_interception_mode,
        NetGatewayTlsInterceptionMode::Unknown
    );
    assert_eq!(health.netgtw_identity_profile_version, None);
    assert_eq!(health.netgtw_route_verification_key_generation, None);
}

async fn watchdog_with_committed_gateway(
    sandbox_id: &str,
) -> (SandboxWatchdog, tokio_util::task::TaskTracker) {
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, sandbox_id, crate::state::SandboxState::Started);
    set_runtime_fields(&conn, sandbox_id, test_route_identity());
    let db = crate::db::SharedConnection::new(conn);
    let tracker = tokio_util::task::TaskTracker::new();
    let watchdog = SandboxWatchdog::new_for_tests(
        db.clone(),
        NetworkPolicyReconcileQueue::default(),
        CancellationToken::new(),
        &tracker,
    );
    watchdog.add_sandbox(restart_test_info(sandbox_id)).await;
    {
        let mut state = watchdog.state.lock().await;
        let health = state.get_mut(sandbox_id).expect("sandbox health");
        health.runtime_alive = true;
        health.netgtw_git_sha = Some("sha-old".to_string());
        health.netgtw_identity_profile_version = Some(1);
        health.netgtw_route_verification_key_generation = Some(7);
    }
    (watchdog, tracker)
}

#[tokio::test]
async fn cancelled_gateway_commit_changes_neither_authority_nor_probe_state() {
    let sandbox_id = "0199c1e0-0000-7000-8000-000000000004";
    let (watchdog, _tracker) = watchdog_with_committed_gateway(sandbox_id).await;
    let health_guard = watchdog.state.lock().await;

    let result = tokio::time::timeout(
        Duration::from_millis(10),
        commit_netgtw_replacement(
            sandbox_id,
            20_001,
            Some(30_003),
            40_003,
            &watchdog.db,
            &watchdog.sidecars,
            &watchdog.state,
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "commit must remain cancellable before SQLite"
    );
    assert_eq!(
        health_guard
            .get(sandbox_id)
            .expect("sandbox health")
            .netgtw_git_sha
            .as_deref(),
        Some("sha-old")
    );
    drop(health_guard);
    let row = crate::db::get_sandbox(&watchdog.db.lock().unwrap(), sandbox_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.netgtw_pid, Some(30_003));
    assert_eq!(
        watchdog
            .sidecar_info_for_test(sandbox_id)
            .await
            .unwrap()
            .netgtw_pid,
        Some(30_003)
    );
}

#[tokio::test]
async fn successful_gateway_commit_updates_authority_and_clears_probe_state() {
    let sandbox_id = "0199c1e0-0000-7000-8000-000000000005";
    let (watchdog, _tracker) = watchdog_with_committed_gateway(sandbox_id).await;

    commit_netgtw_replacement(
        sandbox_id,
        20_001,
        Some(30_003),
        40_003,
        &watchdog.db,
        &watchdog.sidecars,
        &watchdog.state,
    )
    .await
    .expect("replacement commit");

    let row = crate::db::get_sandbox(&watchdog.db.lock().unwrap(), sandbox_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.netgtw_pid, Some(40_003));
    assert_eq!(
        watchdog
            .sidecar_info_for_test(sandbox_id)
            .await
            .unwrap()
            .netgtw_pid,
        Some(40_003)
    );
    let health = watchdog.get_health(sandbox_id).await.unwrap();
    assert_eq!(health.netgtw_git_sha, None);
    assert_eq!(health.netgtw_identity_profile_version, None);
    assert_eq!(health.netgtw_route_verification_key_generation, None);
}

#[tokio::test]
async fn rejected_gateway_commit_preserves_probe_state() {
    let sandbox_id = "0199c1e0-0000-7000-8000-000000000006";
    let (watchdog, _tracker) = watchdog_with_committed_gateway(sandbox_id).await;
    watchdog
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET netgtw_pid = ?1 WHERE id = ?2",
            crate::db::fixture::params![50_003, sandbox_id],
        )
        .unwrap();

    commit_netgtw_replacement(
        sandbox_id,
        20_001,
        Some(30_003),
        40_003,
        &watchdog.db,
        &watchdog.sidecars,
        &watchdog.state,
    )
    .await
    .expect_err("newer durable authority must reject stale commit");

    assert_eq!(
        watchdog
            .get_health(sandbox_id)
            .await
            .unwrap()
            .netgtw_git_sha
            .as_deref(),
        Some("sha-old")
    );
}

fn test_watchdog_with_row(sandbox_id: &str, state: crate::state::SandboxState) -> SandboxWatchdog {
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, sandbox_id, state);
    SandboxWatchdog::new_for_tests(
        crate::db::SharedConnection::new(conn),
        NetworkPolicyReconcileQueue::default(),
        CancellationToken::new(),
        &TaskTracker::new(),
    )
}

/// Lay out `<run_dir>/<id>/fc.pid` the way a running sandbox looks. The
/// symlink target only needs to exist for `fc.pid` to read as alive.
fn write_run_dir_fc_pid(run_dir: &Path, sandbox_id: &str, alive: bool) {
    let sandbox_run_dir = run_dir.join(sandbox_id);
    std::fs::create_dir_all(&sandbox_run_dir).expect("sandbox run dir must be creatable");
    let target = run_dir.join(format!("{sandbox_id}.proc-stub"));
    if alive {
        std::fs::write(&target, b"").expect("fc.pid stub target must be writable");
    }
    std::os::unix::fs::symlink(&target, sandbox_run_dir.join("fc.pid"))
        .expect("fc.pid symlink must be creatable");
}

// Recovery must be gated on run-dir liveness alone. Gating it on
// anything under the cache dir regresses to the failure this pins down:
// per-sandbox cache subtrees are not guaranteed readable by the service
// user, `Path::exists()` reads "permission denied" as "absent", and a
// running VM skipped here stays unmonitored (never staleness-probed,
// invisible to `cycle-net-gateway --stale`) until the next restart.
#[tokio::test]
async fn recover_all_monitors_running_vm_without_consulting_cache_dir() {
    let sandbox_id = SandboxId::new().to_string();
    let watchdog = test_watchdog_with_row(&sandbox_id, crate::state::SandboxState::Started);
    let run_dir = tempfile::tempdir().expect("temp run dir must be creatable");
    write_run_dir_fc_pid(run_dir.path(), &sandbox_id, true);

    watchdog.recover_all_in(run_dir.path()).await;

    assert!(
        watchdog.get_health(&sandbox_id).await.is_some(),
        "running VM with a DB row must be re-registered for monitoring \
         regardless of cache-dir readability"
    );
}

#[tokio::test]
async fn recover_all_skips_dead_vm() {
    let sandbox_id = SandboxId::new().to_string();
    let watchdog = test_watchdog_with_row(&sandbox_id, crate::state::SandboxState::Started);
    let run_dir = tempfile::tempdir().expect("temp run dir must be creatable");
    write_run_dir_fc_pid(run_dir.path(), &sandbox_id, false);

    watchdog.recover_all_in(run_dir.path()).await;

    assert!(
        watchdog.get_health(&sandbox_id).await.is_none(),
        "a sandbox whose fc.pid does not resolve has no VM to monitor"
    );
}

#[tokio::test]
async fn recover_all_skips_running_vm_without_db_row() {
    let sandbox_id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    let watchdog = SandboxWatchdog::new_for_tests(
        crate::db::SharedConnection::new(conn),
        NetworkPolicyReconcileQueue::default(),
        CancellationToken::new(),
        &TaskTracker::new(),
    );
    let run_dir = tempfile::tempdir().expect("temp run dir must be creatable");
    write_run_dir_fc_pid(run_dir.path(), &sandbox_id, true);

    watchdog.recover_all_in(run_dir.path()).await;

    assert!(
        watchdog.get_health(&sandbox_id).await.is_none(),
        "a run-dir entry with no DB row is orphan evidence, not a monitoring target"
    );
}

#[test]
fn net_gateway_route_verification_counts_prove_applied_generation() {
    let now = Instant::now();
    let aged = now - NET_GATEWAY_ROUTE_VERIFICATION_GRACE;
    let mut healths = [
        SandboxHealth {
            netgtw_alive: true,
            netgtw_identity_profile_version: Some(1),
            netgtw_route_verification_key_generation: Some(7),
            netgtw_route_verification_window: NetGatewayRouteVerificationWindow::new(aged, 7),
            ..test_health()
        },
        SandboxHealth {
            netgtw_alive: true,
            netgtw_identity_profile_version: Some(1),
            netgtw_route_verification_key_generation: Some(6),
            netgtw_route_verification_window: NetGatewayRouteVerificationWindow::new(aged, 7),
            ..test_health()
        },
        SandboxHealth {
            netgtw_alive: true,
            netgtw_identity_profile_version: Some(0),
            netgtw_route_verification_key_generation: Some(0),
            netgtw_route_verification_window: NetGatewayRouteVerificationWindow::new(aged, 7),
            ..test_health()
        },
        SandboxHealth {
            netgtw_alive: true,
            netgtw_route_verification_window: NetGatewayRouteVerificationWindow::new(aged, 7),
            ..test_health()
        },
        SandboxHealth {
            netgtw_alive: true,
            ..test_health()
        },
        SandboxHealth {
            netgtw_alive: true,
            netgtw_identity_profile_version: Some(1),
            netgtw_route_verification_key_generation: Some(6),
            ..test_health()
        },
        SandboxHealth {
            netgtw_alive: false,
            netgtw_identity_profile_version: Some(1),
            netgtw_route_verification_key_generation: Some(6),
            ..test_health()
        },
    ];

    let counts = net_gateway_route_verification_counts(healths.iter_mut(), 7, now);
    assert_eq!(
        counts.get(&NetGatewayRouteVerificationStatus::Matched),
        Some(&1)
    );
    assert_eq!(
        counts.get(&NetGatewayRouteVerificationStatus::Mismatched),
        Some(&1)
    );
    assert_eq!(
        counts.get(&NetGatewayRouteVerificationStatus::Unsupported),
        Some(&1)
    );
    assert_eq!(
        counts.get(&NetGatewayRouteVerificationStatus::Unknown),
        Some(&1)
    );
    assert_eq!(
        counts.get(&NetGatewayRouteVerificationStatus::Probing),
        Some(&1)
    );
    assert_eq!(
        counts.get(&NetGatewayRouteVerificationStatus::Converging),
        Some(&1)
    );
    assert_eq!(counts.values().sum::<usize>(), 6);

    let disabled_counts = net_gateway_route_verification_counts(healths.iter_mut(), 0, now);
    assert_eq!(
        disabled_counts.get(&NetGatewayRouteVerificationStatus::Disabled),
        Some(&6)
    );
    assert_eq!(disabled_counts.len(), 1);
}

#[test]
fn gateway_tls_interception_mode_keeps_old_and_future_values_unknown() {
    use pplx_space_net_common::proto::TlsInterception as ProtoMode;

    for (value, expected) in [
        (0, NetGatewayTlsInterceptionMode::Unknown),
        (ProtoMode::Off as i32, NetGatewayTlsInterceptionMode::Off),
        (ProtoMode::On as i32, NetGatewayTlsInterceptionMode::On),
        (i32::MAX, NetGatewayTlsInterceptionMode::Unknown),
    ] {
        assert_eq!(net_gateway_tls_interception_mode(value), expected);
    }
}

fn gateway_stats_observation(
    build_git_sha: &str,
    tls_interception_mode: NetGatewayTlsInterceptionMode,
    counter: u64,
) -> NetGatewayStatsObservation {
    NetGatewayStatsObservation {
        network_tx_bytes: counter,
        network_rx_bytes: counter + 1,
        build_git_sha: build_git_sha.to_string(),
        tls_interception_mode,
        identity_profile_version: 1,
        route_verification_key_generation: counter,
    }
}

async fn observe_live_gateway(watchdog: &SandboxWatchdog, sandbox_id: &str, pid: u32) -> bool {
    observe_authoritative_net_gateway_process(
        &watchdog.state,
        &watchdog.sidecars,
        sandbox_id,
        Some(pid),
        Some(pid),
    )
    .await
}

#[tokio::test]
async fn gateway_stats_are_fenced_to_the_observed_process() {
    const SANDBOX_ID: &str = "0199c1e0-0000-7000-8000-000000000007";
    const OLD_PID: u32 = 30_003;
    const NEW_PID: u32 = 40_003;

    let (watchdog, _tracker) = watchdog_with_committed_gateway(SANDBOX_ID).await;
    assert!(
        watchdog
            .get_health(SANDBOX_ID)
            .await
            .expect("sandbox health")
            .runtime_alive,
        "the normal observation fixture must represent a live VM"
    );
    assert!(observe_live_gateway(&watchdog, SANDBOX_ID, OLD_PID).await);
    {
        let mut state = watchdog.state.lock().await;
        let health = state.get_mut(SANDBOX_ID).expect("sandbox health");
        assert_eq!(
            health.apply_net_gateway_stats(
                OLD_PID,
                gateway_stats_observation("cached-sha", NetGatewayTlsInterceptionMode::On, 10,),
            ),
            Some((10, 11))
        );
    }

    assert!(observe_live_gateway(&watchdog, SANDBOX_ID, OLD_PID).await);
    assert_eq!(
        watchdog
            .get_health(SANDBOX_ID)
            .await
            .expect("sandbox health")
            .netgtw_git_sha
            .as_deref(),
        Some("cached-sha")
    );

    commit_netgtw_replacement(
        SANDBOX_ID,
        20_001,
        Some(OLD_PID),
        NEW_PID,
        &watchdog.db,
        &watchdog.sidecars,
        &watchdog.state,
    )
    .await
    .expect("replacement commit");

    assert!(
        !observe_live_gateway(&watchdog, SANDBOX_ID, OLD_PID).await,
        "a delayed observer must not restore the replaced PID's fence"
    );
    {
        let mut state = watchdog.state.lock().await;
        let health = state.get_mut(SANDBOX_ID).expect("sandbox health");
        assert_eq!(health.netgtw_stats_pid, None);
        assert_eq!(
            health.apply_net_gateway_stats(
                OLD_PID,
                gateway_stats_observation("stale-sha", NetGatewayTlsInterceptionMode::On, 30,),
            ),
            None
        );
        assert_eq!(health.netgtw_git_sha, None);
    }

    assert!(observe_live_gateway(&watchdog, SANDBOX_ID, NEW_PID).await);
    let mut state = watchdog.state.lock().await;
    let health = state.get_mut(SANDBOX_ID).expect("sandbox health");
    assert_eq!(
        health.apply_net_gateway_stats(
            NEW_PID,
            gateway_stats_observation("current-sha", NetGatewayTlsInterceptionMode::Off, 50),
        ),
        Some((50, 51))
    );
    assert_eq!(health.netgtw_git_sha.as_deref(), Some("current-sha"));
    assert_eq!(
        health.netgtw_tls_interception_mode,
        NetGatewayTlsInterceptionMode::Off
    );
    health.runtime_alive = false;
    drop(state);

    assert!(
        observe_live_gateway(&watchdog, SANDBOX_ID, NEW_PID).await,
        "the final probe retains matching PID authority after VM death"
    );
    let health = watchdog
        .get_health(SANDBOX_ID)
        .await
        .expect("sandbox health");
    assert!(
        !health.netgtw_alive,
        "a matching gateway PID must not resurrect liveness after VM death"
    );
    assert_eq!(health.netgtw_stats_pid, Some(NEW_PID));
}

#[test]
fn gateway_inventory_counts_every_live_process_with_explicit_unknowns() {
    let mut on = test_health();
    on.netgtw_alive = true;
    on.netgtw_git_sha = Some("gateway-a".to_string());
    on.netgtw_tls_interception_mode = NetGatewayTlsInterceptionMode::On;
    let mut on_row = health_row("on", crate::state::SandboxState::Started);
    on_row.template_content_hash = Some("sha256:template-a".to_string());

    let mut off = test_health();
    off.netgtw_alive = true;
    off.netgtw_git_sha = Some(String::new());
    off.netgtw_tls_interception_mode = NetGatewayTlsInterceptionMode::Off;
    let mut off_missing_row = health_row("off-missing", crate::state::SandboxState::Paused);
    off_missing_row.template_content_hash = None;
    let mut off_empty_row = health_row("off-empty", crate::state::SandboxState::Paused);
    off_empty_row.template_content_hash = Some(String::new());

    let mut unknown = test_health();
    unknown.netgtw_alive = true;
    let mut dead = test_health();
    dead.netgtw_tls_interception_mode = NetGatewayTlsInterceptionMode::On;

    let counts = net_gateway_inventory_counts(
        [
            (&on, Some(&on_row)),
            (&off, Some(&off_missing_row)),
            (&off, Some(&off_empty_row)),
            (&unknown, None),
            (&dead, Some(&on_row)),
        ]
        .into_iter(),
    );
    let count =
        |tls_interception_mode, gateway_sha: &str, template_content_hash: &str, lifecycle| {
            counts
                .get(&NetGatewayInventoryBucket {
                    tls_interception_mode,
                    gateway_sha: gateway_sha.to_string(),
                    template_content_hash: template_content_hash.to_string(),
                    lifecycle,
                })
                .copied()
        };

    assert_eq!(counts.values().sum::<usize>(), 4);
    assert_eq!(counts.len(), 3);
    assert_eq!(
        count(
            NetGatewayTlsInterceptionMode::On,
            "gateway-a",
            "sha256:template-a",
            NetGatewayLifecycle::Known(crate::state::SandboxState::Started),
        ),
        Some(1)
    );
    assert_eq!(
        count(
            NetGatewayTlsInterceptionMode::Off,
            "unknown",
            "unknown",
            NetGatewayLifecycle::Known(crate::state::SandboxState::Paused),
        ),
        Some(2)
    );
    assert_eq!(
        count(
            NetGatewayTlsInterceptionMode::Unknown,
            "unknown",
            "unknown",
            NetGatewayLifecycle::Unknown,
        ),
        Some(1)
    );
}

fn health_row(id: &str, state: crate::state::SandboxState) -> SandboxGuestHealthRow {
    SandboxGuestHealthRow {
        id: id.to_string(),
        state,
        template_content_hash: Some("hash".to_string()),
    }
}

#[test]
fn guest_health_classifier_uses_firecracker_and_guest_path() {
    let healthy = test_health();
    assert_eq!(
        classify_guest_health(GuestHealthProbe::from(&healthy)),
        GuestHealthCategory::Healthy
    );

    let mut unreachable = test_health();
    unreachable.runtime_alive = false;
    assert_eq!(
        classify_guest_health(GuestHealthProbe::from(&unreachable)),
        GuestHealthCategory::Unreachable
    );

    let mut guest_unhealthy = test_health();
    guest_unhealthy.guest_healthy = false;
    assert_eq!(
        classify_guest_health(GuestHealthProbe::from(&guest_unhealthy)),
        GuestHealthCategory::Degraded
    );

    let mut spaced_host_down = test_health();
    spaced_host_down.spaced_host_alive = false;
    assert_eq!(
        classify_guest_health(GuestHealthProbe::from(&spaced_host_down)),
        GuestHealthCategory::Degraded
    );

    let mut repeated_probe_failures = test_health();
    repeated_probe_failures.consecutive_guest_probe_failures = GUEST_HEALTH_FAILURE_THRESHOLD;
    assert_eq!(
        classify_guest_health(GuestHealthProbe::from(&repeated_probe_failures)),
        GuestHealthCategory::Degraded
    );
}

#[test]
fn guest_health_counts_bucket_rows_by_lifecycle_and_health() {
    let rows = vec![
        health_row("started-healthy", crate::state::SandboxState::Started),
        health_row("started-degraded", crate::state::SandboxState::Started),
        health_row("paused-unreachable", crate::state::SandboxState::Paused),
        health_row("started-missing", crate::state::SandboxState::Started),
        health_row("stopped-ignored", crate::state::SandboxState::Stopped),
    ];
    let mut health = HashMap::from([("started-healthy".to_string(), test_health())]);
    let mut degraded = test_health();
    degraded.guest_healthy = false;
    health.insert("started-degraded".to_string(), degraded);
    let mut unreachable = test_health();
    unreachable.runtime_alive = false;
    health.insert("paused-unreachable".to_string(), unreachable);
    let health_snapshot = health
        .iter()
        .map(|(sandbox_id, health)| (sandbox_id.clone(), HealthGaugeSnapshot::from(health)))
        .collect();

    let counts = guest_health_gauge_counts(&rows, &health_snapshot, |sandbox_id| {
        assert_eq!(sandbox_id, "started-missing");
        GuestHealthProbe {
            runtime_alive: true,
            guest_healthy: false,
            spaced_host_alive: true,
            consecutive_guest_probe_failures: GUEST_HEALTH_FAILURE_THRESHOLD,
        }
    });

    assert_eq!(
        counts.get(&("started", GuestHealthCategory::Healthy)),
        Some(&1)
    );
    assert_eq!(
        counts.get(&("started", GuestHealthCategory::Degraded)),
        Some(&2)
    );
    assert_eq!(
        counts.get(&("paused", GuestHealthCategory::Unreachable)),
        Some(&1)
    );
    assert!(!counts.contains_key(&("stopped", GuestHealthCategory::Healthy)));
}

#[test]
fn guest_health_gauge_values_zero_fill_known_live_buckets() {
    let counts = HashMap::from([(("started", GuestHealthCategory::Healthy), 2)]);

    let values = guest_health_gauge_values(&counts);

    assert_eq!(
        values.len(),
        GUEST_HEALTH_LIFECYCLES.len() * GUEST_HEALTH_CATEGORIES.len()
    );
    assert!(values.contains(&GuestHealthGaugeValue {
        lifecycle: "started",
        health: "healthy",
        count: 2,
    }));
    assert!(values.contains(&GuestHealthGaugeValue {
        lifecycle: "paused",
        health: "unreachable",
        count: 0,
    }));
}

pub(super) fn sample(outcome: HealthProbeOutcome) -> HealthObservation {
    HealthObservation {
        observed_at: SystemTime::now(),
        latency_micros: 10,
        outcome,
        detail: String::new(),
        firecracker_state: None,
        lifecycle_epoch: 0,
    }
}

#[test]
fn component_history_is_bounded_to_five_newest_samples() {
    let mut components = HealthComponents::default();
    for latency_micros in 0..7 {
        let mut observation = sample(HealthProbeOutcome::Success);
        observation.latency_micros = latency_micros;
        components.push(HealthComponent::Firecracker, observation);
    }

    let values = components
        .get(HealthComponent::Firecracker)
        .observations
        .iter()
        .map(|observation| observation.latency_micros)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![2, 3, 4, 5, 6]);
}

#[test]
fn component_health_requires_two_failures_and_two_successes() {
    let mut health = test_health();
    health.health_observed_at = Some(SystemTime::now());
    let component = HealthComponent::SpacedGuest;
    let lifecycle = Some(crate::state::SandboxState::Started);

    health
        .health_components
        .push(component, sample(HealthProbeOutcome::Timeout));
    update_component_classification(&mut health, component, lifecycle);
    assert!(!health.component_is_degraded(component));

    health
        .health_components
        .push(component, sample(HealthProbeOutcome::Unavailable));
    update_component_classification(&mut health, component, lifecycle);
    assert!(health.component_is_degraded(component));

    health
        .health_components
        .push(component, sample(HealthProbeOutcome::Success));
    update_component_classification(&mut health, component, lifecycle);
    assert!(health.component_is_degraded(component));

    health
        .health_components
        .push(component, sample(HealthProbeOutcome::Success));
    update_component_classification(&mut health, component, lifecycle);
    assert!(!health.component_is_degraded(component));
}

#[test]
fn pause_failed_recovery_requires_two_matching_firecracker_states() {
    let observation = |state| {
        let mut observation = sample(HealthProbeOutcome::Success);
        observation.firecracker_state = Some(state);
        observation
    };
    let mut health = test_health();
    health
        .health_components
        .push(HealthComponent::Firecracker, observation(VmState::Running));
    assert_eq!(pause_failed_recovery_target(&health, 0), None);
    health
        .health_components
        .push(HealthComponent::Firecracker, observation(VmState::Paused));
    assert_eq!(pause_failed_recovery_target(&health, 0), None);
    health
        .health_components
        .push(HealthComponent::Firecracker, observation(VmState::Paused));
    assert_eq!(
        pause_failed_recovery_target(&health, 0),
        Some(crate::db::PauseFailedRecoveryTarget::Paused)
    );
}

async fn pause_failed_recovery_fixture(
    firecracker_state: VmState,
) -> (
    crate::db::SharedConnection,
    Arc<SnapshotScheduler>,
    AutoLifecycleCoordinator,
    HealthLifecycleEpochs,
    SandboxId,
) {
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox(
        &conn,
        &sandbox_id_string,
        crate::state::SandboxState::Pausing,
    );
    assert!(
        crate::db::mark_quarantined_from_state(
            &conn,
            &sandbox_id_string,
            crate::state::SandboxState::Pausing,
            SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::PauseFailed,
            "pause response was ambiguous",
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .expect("pause-failed quarantine must be recorded")
    );
    let db = crate::db::SharedConnection::new(conn);
    let auto_lifecycle = AutoLifecycleCoordinator::default();
    let lifecycle_epochs = HealthLifecycleEpochs::default();
    let snapshot_scheduler = Arc::new(SnapshotScheduler::new(Arc::new(
        crate::snapshot_scheduler::CheckpointContext {
            db: db.clone(),
            auto_lifecycle: auto_lifecycle.clone(),
            health_lifecycle: lifecycle_epochs.clone(),
            node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
        },
    )));
    snapshot_scheduler
        .add_sandbox(sandbox_id_string.clone(), "seed".to_string())
        .await;
    let mut health = test_health();
    for _ in 0..HEALTH_SUCCESS_THRESHOLD {
        let mut observation = sample(HealthProbeOutcome::Success);
        observation.firecracker_state = Some(firecracker_state);
        health
            .health_components
            .push(HealthComponent::Firecracker, observation);
    }
    lifecycle_epochs
        .state
        .lock()
        .await
        .insert(sandbox_id_string, health);
    (
        db,
        snapshot_scheduler,
        auto_lifecycle,
        lifecycle_epochs,
        sandbox_id,
    )
}

#[tokio::test]
async fn pause_failed_recovery_applies_node_side_effects() {
    let (db, snapshot_scheduler, auto_lifecycle, lifecycle_epochs, sandbox_id) =
        pause_failed_recovery_fixture(VmState::Paused).await;
    reconcile_pause_failed_quarantine(
        &db,
        snapshot_scheduler.as_ref(),
        &auto_lifecycle,
        &lifecycle_epochs,
        &sandbox_id,
        0,
    )
    .await;

    let sandbox_id = sandbox_id.to_string();
    {
        let db = db.lock().expect("database mutex must not be poisoned");
        assert_eq!(
            crate::db::get_sandbox(&db, &sandbox_id)
                .expect("sandbox state must be readable")
                .expect("sandbox must exist")
                .state,
            crate::state::SandboxState::Paused
        );
    }
    assert!(!snapshot_scheduler.is_active(&sandbox_id).await);
    assert_eq!(
        lifecycle_epochs
            .state
            .lock()
            .await
            .get(&sandbox_id)
            .expect("sandbox health must remain registered")
            .lifecycle_epoch,
        1
    );
}

#[tokio::test]
async fn pause_failed_recovery_revalidates_epoch_after_artifact_lock() {
    let (db, snapshot_scheduler, auto_lifecycle, lifecycle_epochs, sandbox_id) =
        pause_failed_recovery_fixture(VmState::Paused).await;
    let sandbox_id_string = sandbox_id.to_string();
    let artifact_guard = auto_lifecycle
        .artifact_lock(&sandbox_id_string)
        .lock_owned()
        .await;
    let recovery = {
        let db = db.clone();
        let snapshot_scheduler = snapshot_scheduler.clone();
        let auto_lifecycle = auto_lifecycle.clone();
        let lifecycle_epochs = lifecycle_epochs.clone();
        tokio::spawn(async move {
            reconcile_pause_failed_quarantine(
                &db,
                snapshot_scheduler.as_ref(),
                &auto_lifecycle,
                &lifecycle_epochs,
                &sandbox_id,
                0,
            )
            .await;
        })
    };
    tokio::task::yield_now().await;
    lifecycle_epochs.record_edge(&sandbox_id_string).await;
    drop(artifact_guard);
    recovery.await.expect("recovery task must complete");

    {
        let db = db.lock().expect("database mutex must not be poisoned");
        assert_eq!(
            crate::db::get_sandbox(&db, &sandbox_id_string)
                .expect("sandbox state must be readable")
                .expect("sandbox must exist")
                .state,
            crate::state::SandboxState::Quarantined
        );
    }
    assert!(snapshot_scheduler.is_active(&sandbox_id_string).await);
}

#[test]
fn firecracker_api_failure_does_not_change_process_liveness() {
    let mut health = test_health();

    update_legacy_health_fields(
        &mut health,
        HealthComponent::Firecracker,
        HealthProbeOutcome::Timeout,
    );

    assert!(
        health.runtime_alive,
        "API reachability must not become destructive process-liveness evidence"
    );
}

#[test]
fn paused_guest_observation_preserves_last_vsock_result() {
    let mut health = test_health();
    health.vsock_raw_ping_ok = true;
    health.consecutive_guest_probe_failures = 2;

    update_legacy_health_fields(
        &mut health,
        HealthComponent::SpacedGuest,
        HealthProbeOutcome::NotApplicable,
    );

    assert!(health.vsock_raw_ping_ok);
    assert_eq!(health.consecutive_guest_probe_failures, 0);
}

#[tokio::test]
async fn lifecycle_edge_preserves_status_and_discards_in_flight_probe() {
    let sandbox_id = SandboxId::new().to_string();
    let mut health = test_health();
    health.health_status = ComposedHealthStatus::Healthy;
    health.health_observed_at = Some(SystemTime::now());
    health.consecutive_guest_probe_failures = 2;
    let mut paused = sample(HealthProbeOutcome::Success);
    paused.firecracker_state = Some(VmState::Paused);
    health
        .health_components
        .push(HealthComponent::Firecracker, paused);

    let state = Arc::new(Mutex::new(HashMap::from([(sandbox_id.clone(), health)])));
    HealthLifecycleEpochs {
        state: Arc::clone(&state),
    }
    .record_edge(&sandbox_id)
    .await;

    {
        let map = state.lock().await;
        let health = map.get(&sandbox_id).expect("sandbox health exists");
        assert_eq!(health.lifecycle_epoch, 1);
        assert_eq!(health.consecutive_guest_probe_failures, 0);
        assert_eq!(health.current_status(), ComposedHealthStatus::Healthy);
        assert_eq!(health.cached_firecracker_state(), None);
    }

    let conn = crate::db::test_conn();
    let db = crate::db::SharedConnection::new(conn);
    let observations = std::array::from_fn(|_| sample(HealthProbeOutcome::Success));
    apply_composed_health_result(
        &state,
        &db,
        Some(Ok((
            sandbox_id.clone(),
            Some(crate::state::SandboxState::Started),
            0,
            ComposedProbeResult {
                observations,
                guest_status: None,
            },
        ))),
    )
    .await;

    let map = state.lock().await;
    let health = map.get(&sandbox_id).expect("sandbox health exists");
    assert_eq!(health.current_status(), ComposedHealthStatus::Healthy);
    assert!(
        HealthComponent::ALL.into_iter().all(|component| health
            .health_components
            .get(component)
            .observations
            .iter()
            .all(|observation| observation.lifecycle_epoch == 0)),
        "the stale probe must not append observations in the new epoch"
    );
}

#[tokio::test]
async fn missing_guest_uptime_preserves_last_observation() {
    let sandbox_id = SandboxId::new().to_string();
    let mut health = test_health();
    health.guest_ready = true;
    let state = Arc::new(Mutex::new(HashMap::from([(sandbox_id.clone(), health)])));
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, &sandbox_id, crate::state::SandboxState::Started);
    crate::db::update_guest_uptime(&conn, &sandbox_id, 10).expect("initial uptime persists");
    let db = crate::db::SharedConnection::new(conn);

    apply_composed_health_result(
        &state,
        &db,
        Some(Ok((
            sandbox_id.clone(),
            Some(crate::state::SandboxState::Started),
            0,
            ComposedProbeResult {
                observations: std::array::from_fn(|_| sample(HealthProbeOutcome::Success)),
                guest_status: Some(pplx_space_spaced_host_client::proto::GetStatusResponse {
                    ready: false,
                    guest_uptime: None,
                    host_version: None,
                    guest_version: None,
                }),
            },
        ))),
    )
    .await;

    {
        let health = state.lock().await;
        let health = health.get(&sandbox_id).expect("sandbox health exists");
        assert_eq!(health.guest_uptime_secs, 10);
        assert!(!health.guest_ready);
    }
    assert_eq!(
        crate::db::get_sandbox(&db.lock().expect("database lock"), &sandbox_id)
            .expect("sandbox lookup succeeds")
            .expect("sandbox exists")
            .guest_uptime_secs,
        Some(10)
    );
}

#[test]
fn inapplicable_guest_components_do_not_block_composed_health() {
    for (lifecycle, without_agent) in [
        (crate::state::SandboxState::Paused, false),
        (crate::state::SandboxState::Started, true),
    ] {
        let mut health = test_health();
        health.health_status = ComposedHealthStatus::Initializing;
        health.health_observed_at = Some(SystemTime::now());
        for component in HealthComponent::ALL {
            let inapplicable = component == HealthComponent::SpacedGuest
                || (without_agent && component == HealthComponent::SpacedHost);
            let outcome = if inapplicable {
                HealthProbeOutcome::NotApplicable
            } else {
                HealthProbeOutcome::Success
            };
            health.health_components.push(component, sample(outcome));
            health.health_components.push(component, sample(outcome));
            update_component_classification(&mut health, component, Some(lifecycle));
        }
        assert_eq!(
            classify_composed_health(&health, Some(lifecycle)),
            ComposedHealthStatus::Healthy,
            "{lifecycle:?} without_agent={without_agent}"
        );
        // A previous execution's exemption is not evidence for the new one.
        health.lifecycle_epoch += 1;
        assert_eq!(
            classify_composed_health(&health, Some(lifecycle)),
            ComposedHealthStatus::Initializing
        );
    }
}

#[test]
fn spaced_host_restart_args_include_required_host_control_socket() {
    let sandbox_id = "sbx-123";
    let vsock_uds_path = format!("{RUN_DIR}/{sandbox_id}/root/v.sock");

    let args = spaced_host_restart_args(sandbox_id, vsock_uds_path);

    assert_eq!(
        args,
        vec![
            "--socket".to_string(),
            pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
            "--sandbox-id".to_string(),
            sandbox_id.to_string(),
            "--vsock-uds".to_string(),
            format!("{RUN_DIR}/{sandbox_id}/root/v.sock"),
            "--host-control-socket".to_string(),
            pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET.to_string(),
            "--boot-type".to_string(),
            "resume".to_string(),
        ]
    );
}

#[tokio::test]
async fn successful_health_probes_leave_failed_lifecycle_unchanged() {
    let sandbox_id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, &sandbox_id, crate::state::SandboxState::Failed);
    conn.execute(
        "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
        crate::db::fixture::params![
            "guest bridge health probe failed after 3 consecutive failures: timeout",
            &sandbox_id
        ],
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);
    let state = Arc::new(Mutex::new(HashMap::from([(
        sandbox_id.clone(),
        test_health(),
    )])));
    for _ in 0..HEALTH_SUCCESS_THRESHOLD {
        let mut firecracker = sample(HealthProbeOutcome::Success);
        firecracker.firecracker_state = Some(VmState::Running);
        apply_composed_health_result(
            &state,
            &db,
            Some(Ok((
                sandbox_id.clone(),
                Some(crate::state::SandboxState::Failed),
                0,
                ComposedProbeResult {
                    observations: [
                        firecracker,
                        sample(HealthProbeOutcome::Success),
                        sample(HealthProbeOutcome::Success),
                        sample(HealthProbeOutcome::Success),
                    ],
                    guest_status: Some(pplx_space_spaced_host_client::proto::GetStatusResponse {
                        ready: true,
                        guest_uptime: None,
                        host_version: None,
                        guest_version: None,
                    }),
                },
            ))),
        )
        .await;
    }
    let row = crate::db::get_sandbox(&db.lock().unwrap(), &sandbox_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, crate::state::SandboxState::Failed);
    assert!(row.failure_reason.is_some());
}

/// Sidecar-restart dependencies for one monitored sandbox whose runtime is
/// alive and whose committed net-gateway is gone.
struct DeadGatewayFixture {
    sandbox_id: &'static str,
    db: crate::db::SharedConnection,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    state: Arc<Mutex<HashMap<String, SandboxHealth>>>,
    node_manager: crate::node_manager_transport::fake::ServedNodeManager,
    queue: SidecarRestartQueue,
    spaced_host_dead: bool,
}

/// How long each fake relaunch takes on the paused clock, so a failure is
/// processed after the sweep that scheduled it, as in a real Launch call.
const FAKE_LAUNCH_LATENCY: Duration = Duration::from_millis(100);

impl DeadGatewayFixture {
    fn new(
        sandbox_id: &'static str,
        launch: impl Fn() -> Result<u32, tonic::Status> + Send + Sync + 'static,
    ) -> Self {
        let conn = crate::db::test_conn();
        insert_test_sandbox(&conn, sandbox_id, crate::state::SandboxState::Started);
        set_runtime_fields(&conn, sandbox_id, test_route_identity());
        let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
            .unary(
                "Launch",
                move |_: pplx_space_node_mgr_client::proto::LaunchRequest| {
                    launch().map(|pid| pplx_space_node_mgr_client::proto::LaunchResponse { pid })
                },
            )
            .serve();
        Self {
            sandbox_id,
            db: crate::db::SharedConnection::new(conn),
            sidecars: Arc::new(Mutex::new(HashMap::from([(
                sandbox_id.to_owned(),
                restart_test_info(sandbox_id),
            )]))),
            state: Arc::new(Mutex::new(HashMap::from([(
                sandbox_id.to_owned(),
                test_health(),
            )]))),
            node_manager,
            queue: SidecarRestartQueue::default(),
            spaced_host_dead: false,
        }
    }

    /// The sweep observation of this sandbox. The `/run` and `/proc` liveness
    /// probes are replaced by one in which the runtime is alive, the
    /// spaced-host is committed unless `spaced_host_dead`, and the
    /// net-gateway is not committed until its pid changes from the original.
    fn observe(&self, info: &SidecarInfo) -> SidecarSweepObservation {
        let netgtw_alive = info.netgtw_pid != Some(30_003);
        SidecarSweepObservation {
            runtime_alive: true,
            spaced_host_live_pid: info.spaced_host_pid,
            netgtw_live_pid: info.netgtw_pid.filter(|_| netgtw_alive),
            spaced_host_alive: !self.spaced_host_dead,
            netgtw_alive,
        }
    }

    /// One sidecar sweep for this sandbox: its health record and its restart
    /// pass. Returns whether it called node-manager's Launch.
    async fn sweep(&mut self) -> bool {
        let sweep_at = tokio::time::Instant::now();
        let launches_before = self.launches();
        let info = self.sidecars.lock().await[self.sandbox_id].clone();
        let observation = self.observe(&info);
        let backoff = record_sidecar_sweep(&self.state, &info, &observation).await;
        schedule_sidecar_restarts(&mut self.queue, &info, &observation, backoff, sweep_at);
        while let Some(request) = self.queue.pop_front() {
            let outcome = self.run(request).await;
            finish_sidecar_restart(&mut self.queue, &self.sidecars, &self.state, outcome).await;
        }
        self.launches() > launches_before
    }

    async fn run(&self, request: SidecarRestartRequest) -> SidecarRestartOutcome {
        let result = run_sidecar_restart(
            request.clone(),
            self.db.clone(),
            Arc::clone(&self.sidecars),
            NetworkPolicyReconcileQueue::default(),
            &self.state,
            &pplx_space_feature_flags::FeatureFlagClient::disabled(),
            &self.node_manager.connector(),
        )
        .await;
        tokio::time::advance(FAKE_LAUNCH_LATENCY).await;
        SidecarRestartOutcome { request, result }
    }

    /// Sweep every `SIDECAR_CHECK_INTERVAL` for `sweeps` ticks and return the
    /// paused-clock offsets, from the first sweep, of each sweep that called
    /// Launch.
    async fn sweep_for(&mut self, sweeps: u32) -> Vec<Duration> {
        let start = tokio::time::Instant::now();
        let mut launched_at = Vec::new();
        for tick in 1..=sweeps {
            let sweep_offset = start.elapsed();
            if self.sweep().await {
                launched_at.push(sweep_offset);
            }
            let next_sweep = start + SIDECAR_CHECK_INTERVAL * tick;
            tokio::time::advance(next_sweep - tokio::time::Instant::now()).await;
        }
        launched_at
    }

    fn launches(&self) -> usize {
        self.node_manager
            .calls()
            .iter()
            .filter(|method| *method == "Launch")
            .count()
    }

    async fn backoff(&self) -> Option<NodeLeaseBackoff> {
        self.state.lock().await[self.sandbox_id].node_lease_backoff
    }

    async fn apply_failed_gateway_probe(&self) {
        let mut firecracker = sample(HealthProbeOutcome::Success);
        firecracker.firecracker_state = Some(VmState::Running);
        let mut net_gateway = sample(HealthProbeOutcome::Unavailable);
        net_gateway.detail = "transport error".to_owned();
        apply_composed_health_result(
            &self.state,
            &self.db,
            Some(Ok((
                self.sandbox_id.to_owned(),
                Some(crate::state::SandboxState::Started),
                0,
                ComposedProbeResult {
                    observations: [
                        firecracker,
                        sample(HealthProbeOutcome::Success),
                        sample(HealthProbeOutcome::Success),
                        net_gateway,
                    ],
                    guest_status: None,
                },
            ))),
        )
        .await;
    }

    async fn health(&self) -> crate::proto::GetSandboxHealthResponse {
        self.state.lock().await[self.sandbox_id].to_proto(self.sandbox_id)
    }

    async fn last_net_gateway_detail(&self) -> String {
        net_gateway_details(&self.health().await)
            .pop()
            .expect("a net-gateway observation")
    }
}

fn net_gateway_details(health: &crate::proto::GetSandboxHealthResponse) -> Vec<String> {
    health
        .components
        .iter()
        .find(|component| component.component == HealthComponent::NetGateway.as_proto())
        .expect("net-gateway component")
        .observations
        .iter()
        .map(|observation| observation.detail.clone())
        .collect()
}

fn sandbox_not_allocated() -> Result<u32, tonic::Status> {
    Err(tonic::Status::not_found("sandbox not allocated"))
}

fn labelled_probe_detail(probe_detail: &str) -> String {
    format!("{NODE_LEASE_MISSING_HEALTH_DETAIL}; probe: {probe_detail}")
}

#[tokio::test(start_paused = true)]
async fn missing_node_lease_backs_off_net_gateway_relaunches() {
    let mut fixture = DeadGatewayFixture::new(
        "0199c1e0-0000-7000-8000-000000000007",
        sandbox_not_allocated,
    );

    let launched_at = fixture.sweep_for(200).await;

    let secs = |secs| Duration::from_secs(secs);
    assert_eq!(
        launched_at,
        [0, 5, 15, 35, 75, 155, 315, 615, 915].map(secs),
        "relaunches must double their spacing from the scheduling sweep up to the cap \
         instead of running every sweep"
    );
    let backoff = fixture.backoff().await.expect("backoff stays armed");
    assert_eq!(backoff.failures, 9);
    assert_eq!(backoff.delay, NODE_LEASE_MISSING_BACKOFF_MAX);
}

#[tokio::test(start_paused = true)]
async fn missing_node_lease_backs_off_both_sidecars_once_per_sweep() {
    let mut fixture = DeadGatewayFixture::new(
        "0199c1e0-0000-7000-8000-000000000010",
        sandbox_not_allocated,
    );
    fixture.spaced_host_dead = true;

    let launched_at = fixture.sweep_for(8).await;

    assert_eq!(
        launched_at,
        [0, 5, 15, 35].map(Duration::from_secs),
        "both sidecars' failures in one sweep count as one backoff step"
    );
    assert_eq!(fixture.launches(), 8);
    assert_eq!(fixture.backoff().await.expect("backoff").failures, 4);
}

#[tokio::test(start_paused = true)]
async fn missing_node_lease_names_the_reason_in_net_gateway_health() {
    let mut fixture = DeadGatewayFixture::new(
        "0199c1e0-0000-7000-8000-000000000008",
        sandbox_not_allocated,
    );
    assert!(fixture.sweep().await);

    for _ in 0..HEALTH_FAILURE_THRESHOLD {
        fixture.apply_failed_gateway_probe().await;
    }

    let health = fixture.health().await;
    assert_eq!(health.status, ComposedHealthStatus::Degraded.as_proto());
    assert_eq!(
        health.degraded_components,
        [HealthComponent::NetGateway.as_proto()]
    );
    assert_eq!(
        net_gateway_details(&health),
        vec![labelled_probe_detail("transport error"); HEALTH_FAILURE_THRESHOLD]
    );
}

#[test]
fn node_lease_label_needs_a_live_runtime_and_a_down_sidecar() {
    let mut health = test_health();
    health.node_lease_backoff = Some(NodeLeaseBackoff::new(tokio::time::Instant::now()));
    health.spaced_host_alive = false;
    assert!(node_lease_missing_explains(
        HealthComponent::NetGateway,
        &health
    ));
    assert!(node_lease_missing_explains(
        HealthComponent::SpacedHost,
        &health
    ));
    assert!(!node_lease_missing_explains(
        HealthComponent::Firecracker,
        &health
    ));

    let mut gateway_running = test_health();
    gateway_running.node_lease_backoff = health.node_lease_backoff;
    gateway_running.netgtw_alive = true;
    assert!(!node_lease_missing_explains(
        HealthComponent::NetGateway,
        &gateway_running
    ));
    assert!(
        !node_lease_missing_explains(HealthComponent::SpacedHost, &gateway_running),
        "a running spaced-host is not explained by a missing lease"
    );

    let mut runtime_dead = test_health();
    runtime_dead.node_lease_backoff = health.node_lease_backoff;
    runtime_dead.runtime_alive = false;
    assert!(!node_lease_missing_explains(
        HealthComponent::NetGateway,
        &runtime_dead
    ));
}

#[tokio::test(start_paused = true)]
async fn other_launch_failure_clears_the_node_lease_health_label() {
    let lease_missing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let launch_lease = Arc::clone(&lease_missing);
    let mut fixture = DeadGatewayFixture::new("0199c1e0-0000-7000-8000-00000000000b", move || {
        if launch_lease.load(Ordering::SeqCst) {
            sandbox_not_allocated()
        } else {
            Err(tonic::Status::internal("runtime-supervisor launch failed"))
        }
    });
    assert!(fixture.sweep().await);
    fixture.apply_failed_gateway_probe().await;
    assert_eq!(
        fixture.last_net_gateway_detail().await,
        labelled_probe_detail("transport error")
    );

    lease_missing.store(false, Ordering::SeqCst);
    assert_eq!(fixture.sweep_for(2).await.len(), 1);
    fixture.apply_failed_gateway_probe().await;

    assert_eq!(
        fixture.last_net_gateway_detail().await,
        "transport error",
        "a relaunch that failed for another reason removes the lease evidence"
    );
    assert_eq!(
        fixture.sweep_for(3).await.len(),
        3,
        "other failures retry every sweep"
    );
}

#[tokio::test(start_paused = true)]
async fn successful_relaunch_ends_node_lease_backoff() {
    let lease_restored = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let launch_lease = Arc::clone(&lease_restored);
    let mut fixture = DeadGatewayFixture::new("0199c1e0-0000-7000-8000-000000000009", move || {
        if launch_lease.load(Ordering::SeqCst) {
            Ok(40_003)
        } else {
            sandbox_not_allocated()
        }
    });
    assert_eq!(fixture.sweep_for(7).await.len(), 3);
    assert!(fixture.backoff().await.is_some());

    lease_restored.store(true, Ordering::SeqCst);
    // The next attempt waits out the current backoff, then succeeds.
    let launched_at = fixture.sweep_for(8).await;

    assert_eq!(launched_at.len(), 1);
    assert!(fixture.backoff().await.is_none());
    assert_eq!(
        fixture.sidecars.lock().await[fixture.sandbox_id].netgtw_pid,
        Some(40_003)
    );
    fixture.apply_failed_gateway_probe().await;
    assert_eq!(
        fixture.last_net_gateway_detail().await,
        "transport error",
        "a failed probe after the lease returns reports its own error"
    );
}

#[tokio::test(start_paused = true)]
async fn sidecars_restored_elsewhere_end_node_lease_backoff_at_the_sweep() {
    let mut fixture = DeadGatewayFixture::new(
        "0199c1e0-0000-7000-8000-00000000000c",
        sandbox_not_allocated,
    );
    assert!(fixture.sweep().await);
    assert!(fixture.backoff().await.is_some());

    // Another actor, such as a lifecycle relaunch, commits a new gateway.
    fixture
        .sidecars
        .lock()
        .await
        .get_mut(fixture.sandbox_id)
        .expect("monitored sandbox")
        .netgtw_pid = Some(40_004);
    tokio::time::advance(SIDECAR_CHECK_INTERVAL).await;

    assert!(!fixture.sweep().await, "nothing is left to relaunch");
    assert!(fixture.backoff().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn stale_restart_outcome_leaves_a_reregistered_sandbox_without_backoff() {
    let fixture = DeadGatewayFixture::new(
        "0199c1e0-0000-7000-8000-00000000000d",
        sandbox_not_allocated,
    );
    let mut queue = SidecarRestartQueue::default();
    let info = fixture.sidecars.lock().await[fixture.sandbox_id].clone();
    let request = SidecarRestartRequest::NetGateway(info.clone());
    assert!(queue.schedule(request.clone(), tokio::time::Instant::now()));
    let outcome = fixture.run(request).await;
    assert!(matches!(
        outcome.result,
        Err(SidecarRestartFailure::NodeLeaseMissing(_))
    ));

    // The sandbox leaves the watchdog and registers again with a new gateway
    // while the old registration's relaunch is in flight.
    fixture.sidecars.lock().await.insert(
        fixture.sandbox_id.to_owned(),
        SidecarInfo {
            netgtw_pid: Some(40_005),
            ..info
        },
    );
    fixture
        .state
        .lock()
        .await
        .insert(fixture.sandbox_id.to_owned(), test_health());
    finish_sidecar_restart(&mut queue, &fixture.sidecars, &fixture.state, outcome).await;

    assert!(fixture.backoff().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn other_launch_failures_retry_every_sweep_without_backoff() {
    let mut fixture = DeadGatewayFixture::new("0199c1e0-0000-7000-8000-00000000000a", || {
        Err(tonic::Status::unavailable(
            "runtime-supervisor launch failed",
        ))
    });

    assert_eq!(fixture.sweep_for(6).await.len(), 6);
    assert!(fixture.backoff().await.is_none());
}
