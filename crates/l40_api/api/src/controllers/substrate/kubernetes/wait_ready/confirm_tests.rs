use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    CallObserver, ScriptedWorkerConfig, kube_status, pod_reply, scripted_kube_client,
    start_scripted_worker,
};
use crate::controllers::substrate::kubernetes::tests::{
    insert_k8s_execution, insert_test_sandbox_row, k8s_node_with_internal_ip, node_api_pod_on_node,
    sandbox_pod_on_node, substrate_with_inventory_caches_and_pool,
};
use hyper::http::{Method, StatusCode};
use k8s_openapi::api::core::v1::PodCondition;
use pplx_space_sql_locator::sql;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[tokio::test(flavor = "current_thread")]
async fn post_ip_healthy_reflector_deletion_and_db_retirement_fail_fast_without_timeout()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // --- Subcase A: healthy reflector observes pod Delete mid-probe -> 1 GET confirms 404,
    // retires execution tracking, and cancels the hanging TCP probe immediately. ---
    let deleted_sbx = SandboxId::new();
    let deleted_pod_name = format!("space-sbx-{deleted_sbx}-g1");
    insert_test_sandbox_row(&db.pool, deleted_sbx, "creating").await?;
    insert_k8s_execution(
        &db.pool,
        deleted_sbx,
        1,
        &deleted_pod_name,
        "uid-del",
        None,
        true,
    )
    .await?;

    let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let blackhole_port = blackhole.local_addr()?.port();
    let (probe_tx, mut probe_rx) = tokio::sync::mpsc::channel::<()>(16);
    let _hold_socket = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = blackhole.accept().await {
            let _ = probe_tx.try_send(());
            held.push(stream);
        }
    });

    let gets = Arc::new(AtomicUsize::new(0));
    let gets_for_client = Arc::clone(&gets);
    let expected_path = format!("/api/v1/namespaces/space/pods/{deleted_pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            gets_for_client.fetch_add(1, Ordering::SeqCst);
            return kube_status(StatusCode::NOT_FOUND, "NotFound");
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
    });

    let mut running_pod =
        (*sandbox_pod_on_node(&deleted_pod_name, "worker-a", 1, "Running")).clone();
    running_pod.metadata.namespace = Some("space".to_string());
    running_pod.metadata.uid = Some("uid-del".to_string());
    running_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = blackhole_port;
    substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (substrate, mut feed) = substrate.with_pod_cache_feed(vec![running_pod.clone()]);

    while probe_rx.try_recv().is_ok() {}
    let delete_driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), probe_rx.recv())
            .await
            .expect("Subcase A proxy probe must connect before injecting pod deletion")
            .expect("blackhole accept loop open");
        feed.delete(running_pod);
        (feed, probe_rx)
    });

    let started = Instant::now();
    let err = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            deleted_sbx,
            PodProxyWaitTarget::new(&deleted_pod_name, Some("uid-del"), 1),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect_err("deleted pod must fail fast during post-IP proxy wait");
    let (_feed, mut probe_rx) = delete_driver.await?;
    assert!(
        matches!(err, SubstrateError::Failed { .. }),
        "expected retired Failed error instead of ProxyUnreachable, got {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "healthy reflector deletion must cancel probe promptly: {:?}",
        started.elapsed()
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "exactly one confirmation GET should verify cached deletion"
    );
    assert!(
        substrate
            .store
            .active_execution_status(deleted_sbx)
            .await?
            .is_none(),
        "confirmed 404 must retire active execution tracking"
    );

    // --- Subcase B: active execution retired in Postgres while pod remains in pod_store ->
    // fails fast with zero apiserver GETs instead of looping until ProxyUnreachable. ---
    let retired_sbx = SandboxId::new();
    let retired_pod_name = format!("space-sbx-{retired_sbx}-g1");
    insert_test_sandbox_row(&db.pool, retired_sbx, "started").await?;
    insert_k8s_execution(
        &db.pool,
        retired_sbx,
        1,
        &retired_pod_name,
        "uid-ret",
        None,
        true,
    )
    .await?;

    let probe_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let closed_port = probe_listener.local_addr()?.port();
    let zero_gets = Arc::new(AtomicUsize::new(0));
    let zero_gets_reply = Arc::clone(&zero_gets);
    let (zero_get_client, _log2) = scripted_kube_client(move |request| {
        if request.method == Method::GET {
            zero_gets_reply.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet")
    });
    let mut cached_pod =
        (*sandbox_pod_on_node(&retired_pod_name, "worker-a", 1, "Running")).clone();
    cached_pod.metadata.namespace = Some("space".to_string());
    cached_pod.metadata.uid = Some("uid-ret".to_string());
    cached_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut retired_substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    retired_substrate.k8s.client = zero_get_client;
    retired_substrate.k8s.sandbox_proxy_port = closed_port;
    retired_substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    retired_substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (retired_substrate, _retired_feed) =
        retired_substrate.with_pod_cache_feed(vec![cached_pod]);

    let pool_for_retire = db.pool.clone();
    let retire_driver = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(2), probe_listener.accept())
            .await
            .expect("Subcase B proxy probe must connect before retiring execution")?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
        ))
        .bind(retired_sbx)
        .execute(&pool_for_retire)
        .await?;
        pplx_space_test_env::TestResult::Ok((stream, probe_listener))
    });

    let started = Instant::now();
    let err = retired_substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            retired_sbx,
            PodProxyWaitTarget::new(&retired_pod_name, Some("uid-ret"), 1),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect_err("retired DB row must abort post-IP proxy loop");
    let _held_probe_sockets = retire_driver.await??;
    assert!(matches!(err, SubstrateError::Failed { .. }), "got {err:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(zero_gets.load(Ordering::SeqCst), 0);

    // --- Subcase C: confirmation GET after reflector-observed deletion fails with 429 ->
    // propagates Unavailable immediately even while shared pod reflector stays healthy. ---
    let throttled_sbx = SandboxId::new();
    let throttled_pod_name = format!("space-sbx-{throttled_sbx}-g1");
    insert_test_sandbox_row(&db.pool, throttled_sbx, "creating").await?;
    insert_k8s_execution(
        &db.pool,
        throttled_sbx,
        1,
        &throttled_pod_name,
        "uid-thr",
        None,
        true,
    )
    .await?;

    let expected_thr_path = format!("/api/v1/namespaces/space/pods/{throttled_pod_name}");
    let (thr_client, _thr_log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_thr_path {
            return kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests");
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
    });
    let mut thr_pod = (*sandbox_pod_on_node(&throttled_pod_name, "worker-a", 1, "Running")).clone();
    thr_pod.metadata.namespace = Some("space".to_string());
    thr_pod.metadata.uid = Some("uid-thr".to_string());
    thr_pod.status.get_or_insert_with(Default::default).pod_ip = Some("127.0.0.1".to_string());

    let mut thr_substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    thr_substrate.k8s.client = thr_client;
    thr_substrate.k8s.sandbox_proxy_port = blackhole_port;
    thr_substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    thr_substrate.budgets.readiness_pod_get = Duration::from_millis(80);
    thr_substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (thr_substrate, mut thr_feed) = thr_substrate.with_pod_cache_feed(vec![thr_pod.clone()]);

    while probe_rx.try_recv().is_ok() {}
    let thr_delete_driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), probe_rx.recv())
            .await
            .expect("Subcase C proxy probe must connect before injecting pod deletion")
            .expect("blackhole accept loop open");
        thr_feed.delete(thr_pod);
        (thr_feed, probe_rx)
    });
    let started_thr = Instant::now();
    let thr_err = thr_substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            throttled_sbx,
            PodProxyWaitTarget::new(&throttled_pod_name, Some("uid-thr"), 1),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect_err("throttled confirmation GET must propagate Unavailable");
    let (_thr_feed, mut probe_rx) = thr_delete_driver.await?;
    assert!(
        matches!(thr_err, SubstrateError::Unavailable { .. }),
        "expected Unavailable from required confirmation GET, got {thr_err:?}"
    );
    assert!(
        started_thr.elapsed() < Duration::from_secs(1),
        "throttled confirmation GET must fail fast instead of stalling: {:?}",
        started_thr.elapsed()
    );

    // --- Subcase D: post-IP pod transitions to Failed on healthy reflector -> 1 GET confirms
    // terminal phase and fails fast instead of waiting until ProxyUnreachable. ---
    let failed_sbx = SandboxId::new();
    let failed_pod_name = format!("space-sbx-{failed_sbx}-g1");
    insert_test_sandbox_row(&db.pool, failed_sbx, "creating").await?;
    insert_k8s_execution(
        &db.pool,
        failed_sbx,
        1,
        &failed_pod_name,
        "uid-fail",
        None,
        true,
    )
    .await?;

    let mut failed_pod = (*sandbox_pod_on_node(&failed_pod_name, "worker-a", 1, "Failed")).clone();
    failed_pod.metadata.namespace = Some("space".to_string());
    failed_pod.metadata.uid = Some("uid-fail".to_string());
    failed_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());
    let failed_pod_for_reply = failed_pod.clone();

    let fail_gets = Arc::new(AtomicUsize::new(0));
    let fail_gets_reply = Arc::clone(&fail_gets);
    let expected_fail_path = format!("/api/v1/namespaces/space/pods/{failed_pod_name}");
    let (fail_client, _fail_log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_fail_path {
            fail_gets_reply.fetch_add(1, Ordering::SeqCst);
            return pod_reply(&failed_pod_for_reply);
        }
        if request.method == Method::DELETE && request.path == expected_fail_path {
            return kube_status(StatusCode::NOT_FOUND, "NotFound");
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
    });
    let mut initial_running =
        (*sandbox_pod_on_node(&failed_pod_name, "worker-a", 1, "Running")).clone();
    initial_running.metadata.namespace = Some("space".to_string());
    initial_running.metadata.uid = Some("uid-fail".to_string());
    initial_running
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut fail_substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    fail_substrate.k8s.client = fail_client;
    fail_substrate.k8s.sandbox_proxy_port = blackhole_port;
    fail_substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    fail_substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (fail_substrate, mut fail_feed) = fail_substrate.with_pod_cache_feed(vec![initial_running]);

    while probe_rx.try_recv().is_ok() {}
    let fail_driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), probe_rx.recv())
            .await
            .expect("Subcase D proxy probe must connect before injecting Failed pod")
            .expect("blackhole accept loop open");
        fail_feed.apply(failed_pod);
        fail_feed
    });
    let started_fail = Instant::now();
    let fail_err = fail_substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            failed_sbx,
            PodProxyWaitTarget::new(&failed_pod_name, Some("uid-fail"), 1),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect_err("Failed pod must fail fast during post-IP proxy wait");
    let _fail_feed = fail_driver.await?;
    assert!(
        matches!(fail_err, SubstrateError::Failed { ref message } if message.contains("exited or disappeared before its proxy became reachable")),
        "expected retired Failed error instead of UnexpectedRequest or ProxyUnreachable, got {fail_err:?}"
    );
    assert!(
        fail_substrate
            .store
            .active_execution_status(failed_sbx)
            .await?
            .is_none()
    );
    assert!(
        started_fail.elapsed() < Duration::from_secs(1),
        "post-IP terminal transition must fail fast: {:?}",
        started_fail.elapsed()
    );
    assert_eq!(fail_gets.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wake_joiner_preserves_live_owner_replacement_and_follows_next_generation()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let g1_pod_name = format!("space-sbx-{sandbox_id}-g1");
    let g2_pod_name = format!("space-sbx-{sandbox_id}-g2");
    insert_test_sandbox_row(&db.pool, sandbox_id, "restoring").await?;
    // Seed `g1` with an existing `pod_ip` so a joiner cannot short-circuit on stale `g1.pod_ip`.
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        &g1_pod_name,
        "uid-g1",
        Some("127.0.0.1"),
        true,
    )
    .await?;
    let owner_attempt = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .bind(owner_attempt)
    .execute(&db.pool)
    .await?;

    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".into(),
        "fc".into(),
        "root".into(),
        None,
        false,
    )
    .with_allocation(2, "uid-g2", "127.0.0.1");
    record.status = "running".into();
    worker.sim.insert_new(record).await?;

    // Gate proxy connections behind `proxy_open` so `g2` publishes `status.podIP` >100ms
    // before its proxy port accepts traffic (spanning multiple 50ms `follow_owner` ticks).
    let proxy_open = Arc::new(AtomicBool::new(false));
    let proxy_open_for_gate = Arc::clone(&proxy_open);
    let gated_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let gated_port = gated_listener.local_addr()?.port();
    let worker_port = worker.port;
    let _gate_task = tokio::spawn(async move {
        while let Ok((mut inbound, _)) = gated_listener.accept().await {
            if !proxy_open_for_gate.load(Ordering::SeqCst) {
                drop(inbound);
                continue;
            }
            tokio::spawn(async move {
                if let Ok(mut upstream) =
                    tokio::net::TcpStream::connect(("127.0.0.1", worker_port)).await
                {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut upstream).await;
                }
            });
        }
    });

    let mut unpinned_unsched_g1 =
        (*sandbox_pod_on_node(&g1_pod_name, "worker-sim-node", 1, "Pending")).clone();
    unpinned_unsched_g1.metadata.namespace = Some("space".to_string());
    unpinned_unsched_g1.metadata.uid = Some("uid-g1".to_string());
    unpinned_unsched_g1
        .spec
        .as_mut()
        .expect("pod spec")
        .node_name = None;
    unpinned_unsched_g1
        .status
        .get_or_insert_with(Default::default)
        .conditions = Some(vec![PodCondition {
        type_: "PodScheduled".to_string(),
        status: "False".to_string(),
        reason: Some("Unschedulable".to_string()),
        message: Some("0/1 nodes are available: source node cordoned".to_string()),
        ..Default::default()
    }]);

    let mut unsched_g1 = unpinned_unsched_g1.clone();
    unsched_g1.spec.as_mut().expect("pod spec").affinity =
        Some(k8s_openapi::api::core::v1::Affinity {
            node_affinity: Some(k8s_openapi::api::core::v1::NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(
                    k8s_openapi::api::core::v1::NodeSelector {
                        node_selector_terms: vec![k8s_openapi::api::core::v1::NodeSelectorTerm {
                            match_fields: Some(vec![
                                k8s_openapi::api::core::v1::NodeSelectorRequirement {
                                    key: "metadata.name".to_string(),
                                    operator: "In".to_string(),
                                    values: Some(vec!["worker-sim-node".to_string()]),
                                },
                            ]),
                            ..Default::default()
                        }],
                    },
                ),
                ..Default::default()
            }),
            ..Default::default()
        });

    let mut deleting_g1 =
        (*sandbox_pod_on_node(&g1_pod_name, "worker-sim-node", 1, "Running")).clone();
    deleting_g1.metadata.namespace = Some("space".to_string());
    deleting_g1.metadata.uid = Some("uid-g1".to_string());
    deleting_g1.metadata.deletion_timestamp = Some(
        k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(k8s_openapi::jiff::Timestamp::now()),
    );
    deleting_g1
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut failed_g1 =
        (*sandbox_pod_on_node(&g1_pod_name, "worker-sim-node", 1, "Failed")).clone();
    failed_g1.metadata.namespace = Some("space".to_string());
    failed_g1.metadata.uid = Some("uid-g1".to_string());

    let mut running_g2 =
        (*sandbox_pod_on_node(&g2_pod_name, "worker-sim-node", 2, "Running")).clone();
    running_g2.metadata.namespace = Some("space".to_string());
    running_g2.metadata.uid = Some("uid-g2".to_string());
    running_g2
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut worker_pod = node_api_pod_on_node("space-worker-node-a", "worker-sim-node");
    worker_pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        gated_port.to_string(),
    )]));
    worker_pod.status.as_mut().expect("worker status").pod_ip = Some("127.0.0.1".to_string());

    let (client, _log) = scripted_kube_client(|_| kube_status(StatusCode::NOT_FOUND, "NotFound"));
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![worker_pod],
        vec![k8s_node_with_internal_ip("worker-sim-node", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = gated_port;
    let (substrate, mut feed) = substrate.with_pod_cache_feed(vec![unsched_g1]);

    // While the wake owner's activation attempt is live, a joiner must neither
    // surface `Unschedulable`, resolve a `Deleting` predecessor, nor retire `g1` on 404/terminal.
    let join_target = PodProxyWaitTarget::new(&g1_pod_name, None, 1);
    let deadline = Instant::now() + Duration::from_secs(2);
    let out_unsched = substrate
        .resolve_pod_ip_for_readiness(sandbox_id, &join_target, deadline)
        .await?;
    assert!(out_unsched.execution_in_flight && !out_unsched.terminal_or_absent_retired);

    // A non-source-pinned Unschedulable pod has no in-request re-drive path, so
    // a joiner/retry must still surface `Unschedulable` immediately.
    feed.apply(unpinned_unsched_g1);
    let err_unpinned = substrate
        .resolve_pod_ip_for_readiness(sandbox_id, &join_target, deadline)
        .await
        .expect_err("unpinned Unschedulable pod must fail fast");
    assert!(matches!(err_unpinned, SubstrateError::Unschedulable { .. }));

    feed.apply(deleting_g1.clone());
    let out_deleting = substrate
        .resolve_pod_ip_for_readiness(sandbox_id, &join_target, deadline)
        .await?;
    assert!(
        out_deleting.execution_in_flight
            && out_deleting.resolved.is_none()
            && !out_deleting.terminal_or_absent_retired
    );

    feed.apply(failed_g1.clone());
    let out_terminal = substrate
        .resolve_pod_ip_for_readiness(sandbox_id, &join_target, deadline)
        .await?;
    assert!(out_terminal.execution_in_flight && !out_terminal.terminal_or_absent_retired);

    let out_absent = substrate
        .resolve_pod_ip_for_readiness_uncached(sandbox_id, &join_target, deadline)
        .await?;
    assert!(out_absent.execution_in_flight && !out_absent.terminal_or_absent_retired);
    assert_eq!(
        substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .expect("g1 must remain active for live owner")
            .generation,
        1
    );

    // Start from `deleting_g1` (with `g1.pod_ip == Some("127.0.0.1")`) and drive a live
    // `ensure_active_execution_pod_proxy_reachable` joiner (`false`) while the owner
    // replaces `g1` with `g2` whose proxy port opens 120ms after `status.podIP`.
    feed.apply(deleting_g1.clone());
    let pool_for_owner = db.pool.clone();
    let g2_pod_name_for_owner = g2_pod_name.clone();
    let proxy_open_for_owner = Arc::clone(&proxy_open);
    let proxy_entered = Arc::clone(&substrate.readiness_test_hooks.proxy_wait_entered);
    let proxy_started_at = Arc::clone(&substrate.readiness_test_hooks.proxy_wait_started_at);
    let owner = tokio::spawn(async move {
        let mut entered = std::pin::pin!(proxy_entered.notified());
        entered.as_mut().enable();
        if proxy_started_at
            .lock()
            .expect("proxy_wait_started_at")
            .is_none()
        {
            tokio::time::timeout(Duration::from_secs(2), entered)
                .await
                .expect("joiner must observe g1 and enter proxy readiness before g2 replacement");
        }
        feed.delete(deleting_g1);
        let mut tx = pool_for_owner.begin().await.expect("begin owner tx");
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .execute(&mut *tx)
        .await
        .expect("retire g1");
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, 2, $2, 'space', 'uid-g2', 'running', true)"
        ))
        .bind(sandbox_id)
        .bind(&g2_pod_name_for_owner)
        .execute(&mut *tx)
        .await
        .expect("insert g2");
        tx.commit().await.expect("commit g1 -> g2 transition");
        feed.apply(running_g2);
        tokio::time::sleep(Duration::from_millis(120)).await;
        proxy_open_for_owner.store(true, Ordering::SeqCst);
        feed
    });

    substrate
        .ensure_active_execution_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            sandbox_id,
            false,
            Instant::now() + Duration::from_secs(2),
        )
        .await?;
    assert!(
        proxy_open.load(Ordering::SeqCst),
        "joiner must wait until g2's proxy port actually opens, not short-circuit on g2.pod_ip"
    );
    let _feed = owner.await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn read_scheduled_create_pod_rejects_mismatched_uid_on_fallback_get()
-> pplx_space_test_env::TestResult {
    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?.port()
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy(&format!("postgres://unused@127.0.0.1:{closed_port}/unused"))?;
    let pod_name = "space-sbx-scheduled-uid-g1";
    let mut replacement = (*sandbox_pod_on_node(pod_name, "worker-a", 1, "Running")).clone();
    replacement.metadata.namespace = Some("space".to_string());
    replacement.metadata.uid = Some("uid-replacement".to_string());

    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let reply_pod = replacement.clone();
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            return pod_reply(&reply_pod);
        }
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        pool,
    );
    substrate.k8s.client = client;
    substrate.budgets.reflector_wait = Duration::from_millis(5);
    let (substrate, _feed) = substrate.with_pod_cache_feed(vec![]);

    let mismatch = substrate
        .read_scheduled_create_pod(pod_name, Some("uid-original"), "test create pod")
        .await?;
    assert!(
        matches!(
            mismatch,
            crate::controllers::substrate::kubernetes::pod_ops::ScheduledCreatePodRead::SupersededUid
        ),
        "fallback GET with mismatched pod UID must report SupersededUid: {mismatch:?}"
    );

    let crate::controllers::substrate::kubernetes::pod_ops::ScheduledCreatePodRead::Matched(
        matched,
    ) = substrate
        .read_scheduled_create_pod(pod_name, Some("uid-replacement"), "test create pod")
        .await?
    else {
        panic!("matching UID must return ScheduledCreatePodRead::Matched");
    };
    assert_eq!(matched.uid(), Some("uid-replacement"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn degraded_post_ip_fallback_refreshes_resolved_uid_on_same_ip()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "restoring").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, &pod_name, "uid-1", None, true).await?;

    let (first_probe_tx, first_probe_rx) = tokio::sync::oneshot::channel();
    let first_probe_tx = Arc::new(std::sync::Mutex::new(Some(first_probe_tx)));
    let observe_first_probe: CallObserver = Arc::new(move || {
        if let Some(tx) = first_probe_tx.lock().expect("first_probe_tx lock").take() {
            let _ = tx.send(());
        }
        Box::pin(std::future::ready(()))
    });
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        observers: std::collections::HashMap::from([(
            "WorkerApi/SandboxState",
            observe_first_probe,
        )]),
        ..ScriptedWorkerConfig::new([])
    })
    .await?;
    let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".into(),
        "fc".into(),
        "root".into(),
        None,
        false,
    )
    .with_allocation(1, "uid-2", "127.0.0.1");
    record.status = "running".into();
    worker.sim.insert_new(record).await?;

    let mut pod_uid1 = (*sandbox_pod_on_node(&pod_name, "worker-sim-node", 1, "Running")).clone();
    pod_uid1.metadata.namespace = Some("space".to_string());
    pod_uid1.metadata.uid = Some("uid-1".to_string());
    pod_uid1.status.get_or_insert_with(Default::default).pod_ip = Some("127.0.0.1".to_string());

    let mut pod_uid2 = pod_uid1.clone();
    pod_uid2.metadata.uid = Some("uid-2".to_string());
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            return pod_reply(&pod_uid2);
        }
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });

    let mut worker_pod = node_api_pod_on_node("space-worker-node-a", "worker-sim-node");
    worker_pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        worker.port.to_string(),
    )]));
    worker_pod.status.as_mut().expect("worker status").pod_ip = Some("127.0.0.1".to_string());

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![worker_pod],
        vec![k8s_node_with_internal_ip("worker-sim-node", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![pod_uid1]);
    substrate.budgets.readiness_quiet_reflector = Duration::from_millis(20);

    // Simulate owner re-driving generation 1 onto `uid-2` with the same `127.0.0.1`
    // right as the joiner's first post-IP worker probe runs with `uid-1`.
    let pool_for_owner = db.pool.clone();
    let fault_task = tokio::spawn(async move {
        let _ = first_probe_rx.await;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET pod_uid = 'uid-2' WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .execute(&pool_for_owner)
        .await
        .expect("update pod_uid to uid-2");
        feed.mark_watch_error();
        feed
    });

    let (ip, binding) = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            sandbox_id,
            PodProxyWaitTarget::new(&pod_name, None, 1),
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    assert_eq!(ip.to_string(), "127.0.0.1");
    assert_eq!(binding.execution_object_id, "uid-2");
    let _feed = fault_task.await?;
    Ok(())
}

/// When a UID-unpinned joiner resolves `uid-1` at `127.0.0.1`, a same-IP
/// replacement `uid-2` arrives in `pod_store` before the owner updates
/// Postgres (so the post-IP loop rejects `Arc<pod_uid2>` against `uid-1`), and
/// the owner then updates Postgres to `uid-2` without another reflector event,
/// the post-IP loop must re-evaluate `Arc<pod_uid2>` on the new execution-row
/// stamp and adopt `uid-2` with zero uncached `GET`s.
#[tokio::test(flavor = "current_thread")]
async fn same_ip_replacement_arriving_before_post_ip_init_adopts_on_healthy_reflector()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "restoring").await?;
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        &pod_name,
        "uid-1",
        Some("127.0.0.1"),
        true,
    )
    .await?;

    let mut pod_uid1 = (*sandbox_pod_on_node(&pod_name, "worker-sim-node", 1, "Running")).clone();
    pod_uid1.metadata.namespace = Some("space".to_string());
    pod_uid1.metadata.uid = Some("uid-1".to_string());
    pod_uid1.status.get_or_insert_with(Default::default).pod_ip = Some("127.0.0.1".to_string());

    let mut pod_uid2 = pod_uid1.clone();
    pod_uid2.metadata.uid = Some("uid-2".to_string());

    let feed_slot: Arc<
        std::sync::Mutex<
            Option<crate::controllers::substrate::kubernetes::test_substrate::PodCacheFeed>,
        >,
    > = Arc::default();
    let feed_for_hook = Arc::clone(&feed_slot);
    let pool_for_observer = db.pool.clone();
    let probe_calls = Arc::new(AtomicUsize::new(0));
    let probe_calls_observer = Arc::clone(&probe_calls);
    let pod_uid2_for_hook = pod_uid2.clone();
    let observe_probes: CallObserver = Arc::new(move || {
        let call = probe_calls_observer.fetch_add(1, Ordering::SeqCst) + 1;
        let pool = pool_for_observer.clone();
        Box::pin(async move {
            if call == 2 {
                // Probe 1 failed against `uid-1`, and post-probe 1 already
                // rejected the pre-cached `Arc<pod_uid2>` on `uid-1`'s row
                // stamp; on probe 2 advance Postgres to `uid-2` WITHOUT
                // touching `feed` so the exact same `Arc<pod_uid2>` must be
                // re-evaluated after probe 2.
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                     SET pod_uid = 'uid-2',
                         updated_at = GREATEST(NOW(), updated_at + INTERVAL '1 microsecond')
                     WHERE sandbox_id = $1 AND generation = 1"
                ))
                .bind(sandbox_id)
                .execute(&pool)
                .await
                .expect("update pod_uid to uid-2");
            }
        })
    });
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        observers: std::collections::HashMap::from([("WorkerApi/SandboxState", observe_probes)]),
        ..ScriptedWorkerConfig::new([])
    })
    .await?;
    let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".into(),
        "fc".into(),
        "root".into(),
        None,
        false,
    )
    .with_allocation(1, "uid-2", "127.0.0.1");
    record.status = "running".into();
    worker.sim.insert_new(record).await?;

    let gets = Arc::new(AtomicUsize::new(0));
    let gets_reply = Arc::clone(&gets);
    let (client, _log) = scripted_kube_client(move |req| {
        if req.method == Method::GET {
            gets_reply.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });

    let mut worker_pod = node_api_pod_on_node("space-worker-node-a", "worker-sim-node");
    worker_pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        worker.port.to_string(),
    )]));
    worker_pod.status.as_mut().expect("worker status").pod_ip = Some("127.0.0.1".to_string());

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![worker_pod],
        vec![k8s_node_with_internal_ip("worker-sim-node", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (substrate, feed) = substrate.with_pod_cache_feed(vec![pod_uid1]);
    *feed_slot.lock().expect("feed mutex") = Some(feed);
    *substrate
        .readiness_test_hooks
        .after_ip_assigned
        .lock()
        .expect("after_ip_assigned mutex") = Some(Box::new(move || {
        // Deliver `pod_uid2` into `pod_store` after IP assignment resolves
        // `uid-1` and BEFORE `handled_post_ip_view` initializes at the start of
        // the proxy probe loop, exercising `pod.uid() == resolved_uid`.
        feed_for_hook
            .lock()
            .expect("feed mutex")
            .as_mut()
            .expect("feed installed")
            .apply(pod_uid2_for_hook);
    }));

    let (ip, binding) = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            sandbox_id,
            PodProxyWaitTarget::new(&pod_name, None, 1),
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    assert_eq!(ip.to_string(), "127.0.0.1");
    assert_eq!(binding.execution_object_id, "uid-2");
    assert!(
        probe_calls.load(Ordering::SeqCst) >= 3,
        "must reject uid-2 on probe 1, observe DB advance on probe 2, and bind uid-2 on probe 3"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "healthy reflector must adopt pre-cached same-IP replacement without any uncached GET"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn published_confirmation_verifies_ip_and_fences_pre_ip_deletion_and_reset_epochs()
-> pplx_space_test_env::TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // 1. `record_confirmed(Published)` rejects mismatched `row.pod_ip` and
    // persists `pod_ip` when `row.pod_ip` is still NULL.
    let sbx_mismatch = SandboxId::new();
    let pod_mismatch = format!("space-sbx-{sbx_mismatch}-g1");
    insert_test_sandbox_row(&db.pool, sbx_mismatch, "started").await?;
    insert_k8s_execution(
        &db.pool,
        sbx_mismatch,
        1,
        &pod_mismatch,
        "uid-ip-check",
        Some("10.244.1.10"),
        true,
    )
    .await?;

    let sbx_null_ip = SandboxId::new();
    let pod_null_ip = format!("space-sbx-{sbx_null_ip}-g1");
    insert_test_sandbox_row(&db.pool, sbx_null_ip, "creating").await?;
    insert_k8s_execution(
        &db.pool,
        sbx_null_ip,
        1,
        &pod_null_ip,
        "uid-ip-null",
        None,
        true,
    )
    .await?;

    let substrate = substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    let (substrate, _feed) = substrate.with_pod_cache_feed(vec![]);
    let deadline = Instant::now() + Duration::from_secs(2);

    assert!(
        !substrate
            .record_confirmed(
                sbx_mismatch,
                (PodIp::parse("10.244.1.99")?, PodIpSource::Published),
                &PodProxyWaitTarget::new(&pod_mismatch, Some("uid-ip-check"), 1),
                None,
                deadline,
            )
            .await?,
        "pinned Published confirmation must reject an IP different from row.pod_ip"
    );
    assert!(
        !substrate
            .record_confirmed(
                sbx_mismatch,
                (PodIp::parse("10.244.1.99")?, PodIpSource::Published),
                &PodProxyWaitTarget::new(&pod_mismatch, None, 1),
                None,
                deadline,
            )
            .await?,
        "unpinned Published confirmation must reject an IP different from row.pod_ip"
    );
    assert!(
        substrate
            .record_confirmed(
                sbx_null_ip,
                (PodIp::parse("10.244.1.20")?, PodIpSource::Published),
                &PodProxyWaitTarget::new(&pod_null_ip, Some("uid-ip-null"), 1),
                None,
                deadline,
            )
            .await?,
        "pinned Published confirmation must persist pod_ip when row.pod_ip is NULL"
    );
    let persisted_ip: Option<String> = sqlx::query_scalar(sql!(
        "SELECT host(pod_ip) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sbx_null_ip)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(persisted_ip.as_deref(), Some("10.244.1.20"));

    // 2. `repair_creating_pod_metadata` and `resolve_pod_ip_for_readiness_uncached`
    // refuse to repair or adopt a `creating` row both when `creating_repair_epoch`
    // is `None` (a concurrent reset completed before readiness ever observed the row)
    // and when `reset_to_creating` advanced `updated_at` past the observed epoch.
    let sbx_reset = SandboxId::new();
    let pod_reset = format!("space-sbx-{sbx_reset}-g1");
    insert_test_sandbox_row(&db.pool, sbx_reset, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 1, $2, 'space', 'creating', true)"
    ))
    .bind(sbx_reset)
    .bind(&pod_reset)
    .execute(&db.pool)
    .await?;
    let mut lingering_old_pod =
        (*sandbox_pod_on_node(&pod_reset, "worker-a", 1, "Running")).clone();
    lingering_old_pod.metadata.namespace = Some("space".to_string());
    lingering_old_pod.metadata.uid = Some("uid-stale-epoch".to_string());
    lingering_old_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.1.88".to_string());
    let expected_reset_path = format!("/api/v1/namespaces/space/pods/{pod_reset}");
    let (reset_client, _reset_log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == expected_reset_path {
            return crate::controllers::substrate::kubernetes::scripted_test_doubles::pod_reply(
                &lingering_old_pod,
            );
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedResetRequest")
    });
    let mut reset_substrate =
        substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    reset_substrate.k8s.client = reset_client;
    let (reset_substrate, _reset_feed) = reset_substrate.with_pod_cache_feed(vec![]);

    let unrepairable_target = PodProxyWaitTarget::new(&pod_reset, Some("uid-stale-epoch"), 1);
    assert!(
        !reset_substrate
            .repair_creating_pod_metadata(
                sbx_reset,
                &unrepairable_target,
                "uid-stale-epoch",
                Some(None),
                deadline,
            )
            .await?,
        "waiter without creating_repair_epoch must never repair a reset creating row"
    );
    let pre_reset_out = reset_substrate
        .resolve_pod_ip_for_readiness_uncached(sbx_reset, &unrepairable_target, deadline)
        .await?;
    assert!(
        pre_reset_out.resolved.is_none() && !pre_reset_out.execution_in_flight,
        "waiter whose row reset before readiness started must step aside instead of adopting lingering pod"
    );

    let initial_row = reset_substrate
        .store
        .active_execution_status(sbx_reset)
        .await?
        .expect("initial creating row");
    assert_eq!(initial_row.pod_uid, None);
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = GREATEST(NOW(), updated_at + INTERVAL '1 microsecond')
         WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sbx_reset)
    .execute(&db.pool)
    .await?;
    let stale_epoch_target = PodProxyWaitTarget::new(&pod_reset, Some("uid-stale-epoch"), 1)
        .with_creating_repair_epoch(Some(initial_row.updated_at));
    assert!(
        !reset_substrate
            .repair_creating_pod_metadata(
                sbx_reset,
                &stale_epoch_target,
                "uid-stale-epoch",
                Some(None),
                deadline,
            )
            .await?,
        "stale creating_repair_epoch must not overwrite a reset creating row"
    );
    let stale_epoch_out = reset_substrate
        .resolve_pod_ip_for_readiness_uncached(sbx_reset, &stale_epoch_target, deadline)
        .await?;
    assert!(stale_epoch_out.resolved.is_none() && !stale_epoch_out.execution_in_flight);

    // 3. Pre-IP healthy-reflector `Pending` deletion and DB retirement fail fast
    // instead of stalling until `PodNeverReady`.
    let sbx_pre_del = SandboxId::new();
    let pod_pre_del = format!("space-sbx-{sbx_pre_del}-g1");
    insert_test_sandbox_row(&db.pool, sbx_pre_del, "creating").await?;
    insert_k8s_execution(
        &db.pool,
        sbx_pre_del,
        1,
        &pod_pre_del,
        "uid-pre-del",
        None,
        true,
    )
    .await?;
    let pre_gets = Arc::new(AtomicUsize::new(0));
    let pre_gets_reply = Arc::clone(&pre_gets);
    let expected_pre_path = format!("/api/v1/namespaces/space/pods/{pod_pre_del}");
    let (pre_client, _pre_log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == expected_pre_path {
            pre_gets_reply.fetch_add(1, Ordering::SeqCst);
            return kube_status(StatusCode::NOT_FOUND, "NotFound");
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedPreIpRequest")
    });
    let mut pending_pod = (*sandbox_pod_on_node(&pod_pre_del, "worker-a", 1, "Pending")).clone();
    pending_pod.metadata.namespace = Some("space".to_string());
    pending_pod.metadata.uid = Some("uid-pre-del".to_string());
    pending_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;

    let mut pre_substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    pre_substrate.k8s.client = pre_client;
    pre_substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(5);
    pre_substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (pre_substrate, mut pre_feed) =
        pre_substrate.with_pod_cache_feed(vec![pending_pod.clone()]);
    let iterations = Arc::clone(&pre_substrate.readiness_test_hooks.ip_wait_iterations);
    let delete_pending = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), async {
            while iterations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pre-IP readiness loop must enter first iteration within 2s");
        pre_feed.delete(pending_pod);
        pre_feed
    });
    let started = Instant::now();
    let err = pre_substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sbx_pre_del,
            PodProxyWaitTarget::new(&pod_pre_del, Some("uid-pre-del"), 1),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect_err("deleted Pending pod must fail fast");
    let _pre_feed = delete_pending.await?;
    assert!(matches!(err, SubstrateError::Failed { .. }), "got {err:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(pre_gets.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn post_ip_joiner_switches_to_scheduled_pending_replacement_via_recorded_ip()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        kube_status, scripted_kube_client,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
        substrate_with_inventory_caches_and_pool,
    };
    use axum::http::StatusCode;
    use pplx_space_sql_locator::sql;
    use std::sync::atomic::{AtomicUsize, Ordering};

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let worker = pplx_space_node_api_sim::SimWorkerHarness::builder("worker-a")
        .start()
        .await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;

    let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".into(),
        "fc".into(),
        "root".into(),
        None,
        false,
    )
    .with_allocation(1, "uid-b", "127.0.0.1");
    record.status = "running".into();
    worker.state().insert_new(record).await?;

    let stale_ip = if tokio::net::TcpListener::bind(("[::1]", 0)).await.is_ok() {
        "::1"
    } else {
        "127.0.0.2"
    };
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        &pod_name,
        "uid-a",
        Some(stale_ip),
        true,
    )
    .await?;

    let mut pod_a = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    pod_a.metadata.namespace = Some("space".to_string());
    pod_a.metadata.uid = Some("uid-a".to_string());
    pod_a.status.get_or_insert_with(Default::default).pod_ip = Some(stale_ip.to_string());

    let mut pending_pod_b = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Pending")).clone();
    pending_pod_b.metadata.namespace = Some("space".to_string());
    pending_pod_b.metadata.uid = Some("uid-b".to_string());
    pending_pod_b
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;

    let zero_gets = Arc::new(AtomicUsize::new(0));
    let zero_gets_c = Arc::clone(&zero_gets);
    let (client, _log) = scripted_kube_client(move |_req| {
        zero_gets_c.fetch_add(1, Ordering::SeqCst);
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet")
    });
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port();
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (substrate, mut feed) = substrate.with_pod_cache_feed(vec![pod_a]);

    let pool_for_owner = db.pool.clone();
    let after_ip_notify = Arc::new(tokio::sync::Notify::new());
    let after_ip_notify_hook = Arc::clone(&after_ip_notify);
    *substrate
        .readiness_test_hooks
        .after_ip_assigned
        .lock()
        .expect("after_ip_assigned") = Some(Box::new(move || {
        after_ip_notify_hook.notify_one();
    }));

    let owner = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), after_ip_notify.notified())
            .await
            .expect("joiner must enter post-IP wait on pod_a before replacement");
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET pod_uid = 'uid-b', pod_ip = '127.0.0.1', state = 'running', updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .execute(&pool_for_owner)
        .await
        .expect("persist replacement uid-b + NodeReported pod_ip");
        feed.apply(pending_pod_b);
        feed
    });

    let (resolved_ip, binding) = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            sandbox_id,
            PodProxyWaitTarget::new(&pod_name, None, 1),
            Instant::now() + Duration::from_secs(2),
        )
        .await?;
    let _feed = owner.await?;
    assert_eq!(resolved_ip, PodIp::parse("127.0.0.1")?);
    assert_eq!(binding.execution_object_id, "uid-b");
    assert_eq!(zero_gets.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recovering_row_never_short_circuits_and_unpinned_pending_joiner_retires_with_zero_gets()
-> pplx_space_test_env::TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // Subcase A: `state = 'recovering'` with a stale `pod_ip` must NOT
    // short-circuit `ensure_active_execution_pod_proxy_reachable(..., true)`.
    let rec_sbx = SandboxId::new();
    let rec_pod = format!("space-sbx-{rec_sbx}-g1");
    insert_test_sandbox_row(&db.pool, rec_sbx, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-dead', '127.0.0.1', 'recovering', true)"
    ))
    .bind(rec_sbx)
    .bind(&rec_pod)
    .execute(&db.pool)
    .await?;
    let (client_rec, _log_rec) = scripted_kube_client(|_req| {
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet")
    });
    let mut sub_rec = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    sub_rec.k8s.client = client_rec;
    sub_rec.k8s.sandbox_proxy_port = 1;
    sub_rec.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let mut dead_pod = (*sandbox_pod_on_node(&rec_pod, "worker-a", 1, "Pending")).clone();
    dead_pod.metadata.namespace = Some("space".to_string());
    dead_pod.metadata.uid = Some("uid-dead".to_string());
    dead_pod.status.get_or_insert_with(Default::default).pod_ip = None;
    let (sub_rec, _feed_rec) = sub_rec.with_pod_cache_feed(vec![dead_pod]);
    let rec_err = sub_rec
        .ensure_active_execution_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            rec_sbx,
            true,
            Instant::now() + Duration::from_millis(60),
        )
        .await
        .expect_err("recovering row with stale pod_ip must not short-circuit Ok(())");
    assert!(
        matches!(rec_err, SubstrateError::Timeout { .. }),
        "got {rec_err:?}"
    );

    // Subcase B: unpinned joiner on a cached `Pending` pod under a healthy
    // reflector emits zero GETs across `readiness_quiet_reflector` and fails
    // fast when the execution row is retired.
    let join_sbx = SandboxId::new();
    let join_pod = format!("space-sbx-{join_sbx}-g1");
    insert_test_sandbox_row(&db.pool, join_sbx, "restoring").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active, created_at)
         VALUES ($1, 1, $2, 'space', NULL, 'creating', true, NOW() - INTERVAL '10 minutes')"
    ))
    .bind(join_sbx)
    .bind(&join_pod)
    .execute(&db.pool)
    .await?;
    let gets = Arc::new(AtomicUsize::new(0));
    let gets_c = Arc::clone(&gets);
    let (client_join, _log_join) = scripted_kube_client(move |_req| {
        gets_c.fetch_add(1, Ordering::SeqCst);
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });
    let mut sub_join = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    sub_join.k8s.client = client_join;
    *sub_join
        .readiness_test_hooks
        .fallback_jitter
        .lock()
        .expect("fallback_jitter") = Some(0.5);
    let mut cached_pending = (*sandbox_pod_on_node(&join_pod, "worker-a", 1, "Pending")).clone();
    cached_pending.metadata.namespace = Some("space".to_string());
    cached_pending.metadata.uid = Some("uid-join".to_string());
    cached_pending
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;
    let mut heartbeat = (*sandbox_pod_on_node("space-sbx-hb-g1", "worker-a", 1, "Pending")).clone();
    heartbeat.metadata.namespace = Some("space".to_string());
    heartbeat.metadata.uid = Some("uid-hb".to_string());

    // Drive reflector freshness with a paused Tokio clock while holding one
    // blocking task so real Postgres socket round-trips never auto-advance time.
    let (release_clock, clock_released) = std::sync::mpsc::channel::<()>();
    let clock_hold = tokio::task::spawn_blocking(move || while clock_released.recv().is_ok() {});
    tokio::time::pause();

    let (mut sub_join, mut feed_join) = sub_join.with_pod_cache_feed(vec![cached_pending]);
    sub_join.budgets.readiness_quiet_reflector = Duration::from_millis(60);
    let join_iters = Arc::clone(&sub_join.readiness_test_hooks.ip_wait_iterations);
    let pool_for_join = db.pool.clone();
    let started_join = Instant::now();
    let deadline_join = started_join + Duration::from_secs(5);
    let join_fut = sub_join.wait_for_pod_proxy_reachable(
        crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
        join_sbx,
        PodProxyWaitTarget::new(&join_pod, None, 1),
        deadline_join,
    );
    let driver_fut = async {
        for step in 0..=1_000 {
            if join_iters.load(Ordering::SeqCst) > 0 {
                break;
            }
            if step == 1_000 {
                return Err("joiner never entered wait_for_pod_ip_assignment".into());
            }
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(1))).await?;
            tokio::task::yield_now().await;
        }
        // Step controlled time across `125ms > readiness_quiet_reflector` (`60ms`)
        // with heartbeats before each step so `now - last_delivery <= 20ms`.
        for _ in 0..7 {
            feed_join.apply(heartbeat.clone());
            tokio::time::advance(Duration::from_millis(20)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gets.load(Ordering::SeqCst),
            0,
            "healthy reflector heartbeats must suppress fallback GETs past readiness_quiet_reflector"
        );
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
        ))
        .bind(join_sbx)
        .execute(&pool_for_join)
        .await?;
        // Keep reflector heartbeats fresh until `JOINER_CACHED_PENDING_EXECUTION_RECHECK`
        // (`200ms`) wakes the joiner and observes the retired DB row.
        for _ in 0..50 {
            feed_join.apply(heartbeat.clone());
            tokio::time::advance(Duration::from_millis(20)).await;
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(1))).await?;
            tokio::task::yield_now().await;
        }
        Err::<(), Box<dyn std::error::Error + Send + Sync>>(
            "unpinned Pending joiner did not observe retired DB row within heartbeat bound".into(),
        )
    };
    let outcome = tokio::select! {
        res = join_fut => Ok(res),
        driver_res = driver_fut => Err(driver_res),
    };
    let elapsed_join = started_join.elapsed();
    drop(release_clock);
    clock_hold.await?;
    let join_res = match outcome {
        Ok(res) => res,
        Err(driver_res) => {
            driver_res?;
            unreachable!("driver_fut always returns Err when join_fut has not finished");
        }
    };
    let join_err = join_res.expect_err("retired DB row must abort unpinned Pending joiner");
    assert!(
        matches!(join_err, SubstrateError::Failed { .. }),
        "got {join_err:?}"
    );
    assert!(elapsed_join < Duration::from_secs(1));
    assert_eq!(gets.load(Ordering::SeqCst), 0);
    Ok(())
}
