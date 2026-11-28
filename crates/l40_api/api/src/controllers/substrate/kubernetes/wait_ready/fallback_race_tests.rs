use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    KubeReply, kube_status, pod_reply, scripted_kube_client, scripted_kube_client_async,
};
use crate::controllers::substrate::kubernetes::tests::{
    insert_k8s_execution, insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
    substrate_with_inventory_caches_and_pool,
};
use hyper::http::{Method, StatusCode};
use pplx_space_sql_locator::sql;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A UID-pinned readiness waiter superseded by a newer live attempt (`uid-new`
/// or `claim_pod_materialization` reset) must fail with `SubstrateError::Unavailable`
/// rather than terminal `Failed` (which aborts retryable callers) and without
/// setting `is_nodemgr_unreachable` (which triggers replacement recovery).
#[tokio::test(flavor = "current_thread")]
async fn superseded_proxy_wait_returns_unavailable_not_failed_or_nodemgr_unreachable()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, &pod_name, "uid-new", None, true).await?;

    let (client, _log) = scripted_kube_client(|_req| {
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });
    let mut pod_new = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Pending")).clone();
    pod_new.metadata.namespace = Some("space".to_string());
    pod_new.metadata.uid = Some("uid-new".to_string());
    pod_new.status.get_or_insert_with(Default::default).pod_ip = None;

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (substrate, _feed) = substrate.with_pod_cache_feed(vec![pod_new]);

    // Pre-IP UID-pinned waiter superseded by `uid-new`.
    let pre_err = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("exec"),
            sandbox_id,
            PodProxyWaitTarget::new(&pod_name, Some("uid-old"), 1).abort_on_uid_change(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .expect_err("superseded pre-IP waiter must fail fast");
    assert!(
        matches!(pre_err, SubstrateError::Unavailable { .. }),
        "expected Unavailable on live supersession, got {pre_err:?}"
    );
    assert!(!is_nodemgr_unreachable(&pre_err));

    // Post-IP UID-pinned waiter superseded mid-probe by `claim_pod_materialization`.
    let reset_sbx = SandboxId::new();
    let reset_pod = format!("space-sbx-{reset_sbx}-g1");
    insert_test_sandbox_row(&db.pool, reset_sbx, "started").await?;
    insert_k8s_execution(
        &db.pool,
        reset_sbx,
        1,
        &reset_pod,
        "uid-old",
        Some("127.0.0.1"),
        true,
    )
    .await?;
    let probe_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let stalled_port = probe_listener.local_addr()?.port();
    let (client2, _log2) = scripted_kube_client(|_req| {
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });
    let mut running_old = (*sandbox_pod_on_node(&reset_pod, "worker-a", 1, "Running")).clone();
    running_old.metadata.namespace = Some("space".to_string());
    running_old.metadata.uid = Some("uid-old".to_string());
    running_old
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut worker_a2 = k8s_node_with_internal_ip("worker-a", "127.0.0.1");
    worker_a2.metadata.uid = Some("worker-a-uid".to_string());
    let mut sub2 =
        substrate_with_inventory_caches_and_pool(vec![], vec![worker_a2], db.pool.clone());
    sub2.k8s.client = client2;
    sub2.k8s.sandbox_proxy_port = stalled_port;
    let (mut sub2, _feed2) = sub2.with_pod_cache_feed(vec![running_old]);
    sub2.budgets.readiness_quiet_reflector = Duration::from_secs(60);

    let pool_for_reset = db.pool.clone();
    let reset_driver = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(2), probe_listener.accept())
            .await
            .expect("proxy probe connects")?;
        let mut tx = pool_for_reset.begin().await?;
        assert!(
            pplx_space_sandbox_store::k8s_executions::pod_placement::claim_pod_materialization(
                &mut *tx, reset_sbx, 1,
            )
            .await?
        );
        tx.commit().await?;
        pplx_space_test_env::TestResult::Ok((stream, probe_listener))
    });

    let post_err = sub2
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("exec"),
            reset_sbx,
            PodProxyWaitTarget::new(&reset_pod, Some("uid-old"), 1).abort_on_uid_change(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .expect_err("superseded post-IP waiter must fail fast");
    let _held = reset_driver.await??;
    assert!(
        matches!(post_err, SubstrateError::Unavailable { .. }),
        "expected Unavailable on post-IP reset supersession, got {post_err:?}"
    );
    assert!(!is_nodemgr_unreachable(&post_err));
    Ok(())
}

/// If `probe_pod_proxy` returns `true` while a reflector-loss confirmation GET
/// (`confirms_loss() == true`) is already in flight, `wait_for_pod_proxy_reachable`
/// must resolve that pending loss check before recording a confirmed IP or
/// returning a binding.
#[tokio::test(flavor = "current_thread")]
async fn pending_post_ip_loss_check_aborts_binding_when_probe_connects_concurrently()
-> pplx_space_test_env::TestResult {
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
    insert_k8s_execution(&db.pool, sandbox_id, 1, &pod_name, "uid-loss", None, true).await?;
    crate::test_support::kubernetes::insert_kubernetes_resident(
        worker.state(),
        sandbox_id,
        "running",
        None,
    )
    .await?;

    // Gate a TCP proxy in front of `worker` so we hold the probe connection
    // until the loss-confirmation GET is in flight, then forward the probe so
    // `probe_pod_proxy` succeeds while the `404` GET is still held.
    let gate_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let gate_port = gate_listener.local_addr()?.port();
    let worker_port = worker.port();
    let probe_connected = Arc::new(tokio::sync::Notify::new());
    let probe_connected_proxy = Arc::clone(&probe_connected);
    let forward_probe = Arc::new(tokio::sync::Notify::new());
    let forward_probe_proxy = Arc::clone(&forward_probe);
    let _proxy = tokio::spawn(async move {
        if let Ok((mut inbound, _)) = gate_listener.accept().await {
            probe_connected_proxy.notify_one();
            forward_probe_proxy.notified().await;
            if let Ok(mut outbound) =
                tokio::net::TcpStream::connect(("127.0.0.1", worker_port)).await
            {
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            }
        }
    });

    let get_entered = Arc::new(tokio::sync::Notify::new());
    let get_entered_c = Arc::clone(&get_entered);
    let release_get = Arc::new(tokio::sync::Notify::new());
    let release_get_c = Arc::clone(&release_get);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client_async(move |req| {
        let entered = Arc::clone(&get_entered_c);
        let release = Arc::clone(&release_get_c);
        let matches_pod = req.method == Method::GET && req.path == expected_path;
        async move {
            if matches_pod {
                entered.notify_one();
                release.notified().await;
                kube_status(StatusCode::NOT_FOUND, "NotFound")
            } else {
                kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
            }
        }
    });

    let mut running_pod = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    running_pod.metadata.namespace = Some("space".to_string());
    running_pod.metadata.uid = Some("uid-loss".to_string());
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
    substrate.k8s.sandbox_proxy_port = gate_port;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![running_pod.clone()]);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let awaiting_loss_confirm = Arc::clone(
        &substrate
            .readiness_test_hooks
            .awaiting_post_ip_loss_confirmation,
    );

    let driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), probe_connected.notified())
            .await
            .expect("probe connects to gate");
        let mut get_wait = std::pin::pin!(get_entered.notified());
        get_wait.as_mut().enable();
        let mut loss_confirm_wait = std::pin::pin!(awaiting_loss_confirm.notified());
        loss_confirm_wait.as_mut().enable();
        feed.delete(running_pod);
        tokio::time::timeout(Duration::from_secs(2), get_wait)
            .await
            .expect("reflector deletion launches loss-confirmation GET");
        forward_probe.notify_one();
        tokio::time::timeout(Duration::from_secs(2), loss_confirm_wait)
            .await
            .expect("probe success awaits in-flight loss-confirmation GET");
        release_get.notify_one();
        feed
    });

    let err = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            PodProxyWaitTarget::new(&pod_name, Some("uid-loss"), 1),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .expect_err("404 loss confirmation must prevent binding");
    let _feed = driver.await?;
    assert!(matches!(err, SubstrateError::Failed { .. }), "got {err:?}");
    assert!(
        substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none()
    );
    Ok(())
}

/// An older in-flight degraded GET that returns `Pending` (inconclusive) while
/// `pod_store` has already moved to `Failed` must NOT mark the `Failed` view as
/// `rejected_view`; the next pass must verify and retire the terminal pod.
/// Also verifies `joiner_missing_pod_recheck_at` does not wake-churn while an
/// uncached fallback GET is already in flight.
#[tokio::test(flavor = "current_thread")]
async fn inconclusive_in_flight_fallback_keeps_terminal_view_and_suppresses_joiner_wake_churn()
-> pplx_space_test_env::TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // Part 1: older `Pending` GET completes after `pod_store` turns `Failed` ->
    // `Failed` view is still verified on the next iteration and retired fast.
    let sbx = SandboxId::new();
    let pod_name = format!("space-sbx-{sbx}-g1");
    insert_test_sandbox_row(&db.pool, sbx, "creating").await?;
    insert_k8s_execution(&db.pool, sbx, 1, &pod_name, "uid-term", None, true).await?;

    let mut pending_pod = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Pending")).clone();
    pending_pod.metadata.namespace = Some("space".to_string());
    pending_pod.metadata.uid = Some("uid-term".to_string());
    {
        let status = pending_pod.status.get_or_insert_with(Default::default);
        status.pod_ip = None;
        status.pod_ips = None;
    }
    let mut failed_pod = pending_pod.clone();
    failed_pod.status.get_or_insert_with(Default::default).phase = Some("Failed".to_string());

    let get_count = Arc::new(AtomicUsize::new(0));
    let get_count_c = Arc::clone(&get_count);
    let deleted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let deleted_c = Arc::clone(&deleted);
    let first_get_entered = Arc::new(tokio::sync::Notify::new());
    let first_get_entered_c = Arc::clone(&first_get_entered);
    let release_first_get = Arc::new(tokio::sync::Notify::new());
    let release_first_get_c = Arc::clone(&release_first_get);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let pending_for_get = pending_pod.clone();
    let failed_for_get = failed_pod.clone();
    let (client, _log) = scripted_kube_client_async(move |req| {
        let count = Arc::clone(&get_count_c);
        let deleted = Arc::clone(&deleted_c);
        let entered = Arc::clone(&first_get_entered_c);
        let release = Arc::clone(&release_first_get_c);
        let p = pending_for_get.clone();
        let f = failed_for_get.clone();
        let path = expected_path.clone();
        async move {
            if req.path == path && req.method == Method::DELETE {
                deleted.store(true, Ordering::SeqCst);
                return kube_status(StatusCode::OK, "Deleted");
            }
            if req.path == path && req.method == Method::GET {
                if deleted.load(Ordering::SeqCst) {
                    return kube_status(StatusCode::NOT_FOUND, "NotFound");
                }
                let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    entered.notify_one();
                    release.notified().await;
                    return pod_reply(&p);
                }
                return pod_reply(&f);
            }
            kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
        }
    });

    let mut worker_a = k8s_node_with_internal_ip("worker-a", "10.0.1.1");
    worker_a.metadata.uid = Some("worker-a-uid".to_string());
    let mut substrate =
        substrate_with_inventory_caches_and_pool(vec![], vec![worker_a], db.pool.clone());
    substrate.k8s.client = client;
    substrate.k8s.k8s_blocking_delete_timeout = Duration::from_millis(10);
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![pending_pod]);
    substrate.budgets.readiness_quiet_reflector = Duration::from_millis(20);
    feed.mark_watch_error();

    let awaiting_terminal = Arc::clone(
        &substrate
            .readiness_test_hooks
            .awaiting_in_flight_terminal_fallback,
    );
    let driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), first_get_entered.notified())
            .await
            .expect("degraded GET starts");
        let mut terminal_wait = std::pin::pin!(awaiting_terminal.notified());
        terminal_wait.as_mut().enable();
        // `PodCacheFeed::apply` clears `watch_failing` via `SandboxPodWriter`
        // and delivers `Failed` before the older `Pending` GET completes.
        feed.apply(failed_pod);
        tokio::time::timeout(Duration::from_secs(2), terminal_wait)
            .await
            .expect("terminal reflector update awaits in-flight fallback GET");
        release_first_get.notify_one();
        feed
    });

    let started = Instant::now();
    let err = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sbx,
            PodProxyWaitTarget::new(&pod_name, Some("uid-term"), 1),
            Instant::now() + Duration::from_millis(450),
        )
        .await
        .expect_err("terminal pod must still be verified and retired");
    let _feed = driver.await?;
    assert!(matches!(err, SubstrateError::Failed { .. }), "got {err:?}");
    assert!(started.elapsed() < Duration::from_millis(350));
    assert_eq!(get_count.load(Ordering::SeqCst), 2);

    // Part 2: unpinned joiner with missing pod does not re-wake on
    // `joiner_missing_pod_recheck_at` while `pending_fallback` is in flight.
    let join_sbx = SandboxId::new();
    let join_pod = format!("space-sbx-{join_sbx}-g1");
    insert_test_sandbox_row(&db.pool, join_sbx, "restoring").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active, created_at, updated_at)
         VALUES ($1, 1, $2, 'space', NULL, 'creating', true, NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')"
    ))
    .bind(join_sbx)
    .bind(&join_pod)
    .execute(&db.pool)
    .await?;
    let hang_get_entered = Arc::new(tokio::sync::Notify::new());
    let hang_get_entered_c = Arc::clone(&hang_get_entered);
    let (hang_client, _hang_log) = scripted_kube_client(move |_req| {
        hang_get_entered_c.notify_waiters();
        KubeReply::Hang
    });
    let mut sub_hang = substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    sub_hang.k8s.client = hang_client;
    let (mut sub_hang, mut hang_feed) = sub_hang.with_pod_cache_feed(vec![]);
    sub_hang.budgets.readiness_quiet_reflector = Duration::from_millis(25);
    sub_hang.budgets.readiness_pod_get = Duration::from_secs(2);
    hang_feed.mark_watch_error();
    let iters = Arc::clone(&sub_hang.readiness_test_hooks.ip_wait_iterations);
    let mut hang_entered_wait = std::pin::pin!(hang_get_entered.notified());
    hang_entered_wait.as_mut().enable();
    let (hang_res, ()) = tokio::join!(
        sub_hang.wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            join_sbx,
            PodProxyWaitTarget::new(&join_pod, None, 1),
            Instant::now() + Duration::from_millis(260),
        ),
        async {
            tokio::time::timeout(Duration::from_secs(2), hang_entered_wait)
                .await
                .expect("degraded fallback GET starts before deadline");
        }
    );
    assert!(
        matches!(hang_res, Err(SubstrateError::Timeout { .. })),
        "expected readiness timeout while fallback GET hangs, got {hang_res:?}"
    );
    let observed_iters = iters.load(Ordering::SeqCst);
    assert!(
        observed_iters <= 2,
        "pending_fallback must suppress joiner_missing_pod_recheck_at wake churn (got {observed_iters} iterations)"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fenced_metadata_placement_and_non_readiness_terminal_uid_mismatch_regressions()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::SandboxPodView;
    use crate::controllers::substrate::kubernetes::store::{
        FencedPodMetadataOutcome, PlacementRetryDisposition, PodMetadataFence,
    };
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // 1. `persist_pod_metadata_if_unchanged` and `remember_pod_node_placement_fenced`
    // refuse to overwrite a row once `pod_uid` advances or a same-generation reset
    // cleared `pod_uid`/`pod_ip` while preserving `pod_materialized_at`.
    let sbx = SandboxId::new();
    let pod_name = format!("space-sbx-{sbx}-g1");
    insert_test_sandbox_row(&db.pool, sbx, "started").await?;
    insert_k8s_execution(
        &db.pool,
        sbx,
        1,
        &pod_name,
        "uid-2",
        Some("10.244.0.22"),
        true,
    )
    .await?;
    let mut node_a = k8s_node_with_internal_ip("node-a", "10.0.1.1");
    node_a.metadata.uid = Some("node-a-uid".to_string());
    let mut node_b = k8s_node_with_internal_ip("node-b", "10.0.1.2");
    node_b.metadata.uid = Some("node-b-uid".to_string());
    let (substrate, _feed) =
        substrate_with_inventory_caches_and_pool(vec![], vec![node_a, node_b], db.pool.clone())
            .with_pod_cache_feed(vec![]);
    let stale_meta = substrate
        .store
        .persist_pod_metadata_if_unchanged(
            sbx,
            1,
            PodMetadataFence {
                expected_pod_uid: Some("uid-1"),
                expected_pod_ip: Some("10.244.0.11"),
                expected_pod_materialized_at_set: true,
            },
            Some("uid-1"),
            Some("10.244.0.11"),
            None,
        )
        .await?;
    assert_eq!(stale_meta, FencedPodMetadataOutcome::Superseded);
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = NULL, pod_ip = NULL, pod_materialized_at = NOW() WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sbx)
    .execute(&db.pool)
    .await?;
    let pre_reset_meta = substrate
        .store
        .persist_pod_metadata_if_unchanged(
            sbx,
            1,
            PodMetadataFence {
                expected_pod_uid: None,
                expected_pod_ip: None,
                expected_pod_materialized_at_set: false,
            },
            Some("uid-1"),
            Some("10.244.0.11"),
            None,
        )
        .await?;
    assert_eq!(pre_reset_meta, FencedPodMetadataOutcome::Superseded);
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = 'uid-2', pod_ip = '10.244.0.22'::inet WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sbx)
    .execute(&db.pool)
    .await?;
    let mut stale_pod_on_node_a = (*sandbox_pod_on_node(&pod_name, "node-a", 1, "Running")).clone();
    stale_pod_on_node_a.metadata.namespace = Some("space".to_string());
    stale_pod_on_node_a.metadata.uid = Some("uid-1".to_string());
    let fenced_placement = substrate
        .remember_pod_node_placement_fenced(
            sbx,
            1,
            Some("uid-1"),
            &SandboxPodView::from(stale_pod_on_node_a),
            PlacementRetryDisposition::RetryConverges,
        )
        .await?;
    assert!(!fenced_placement.matched_execution);

    // Deleting unsettled `Pending` pod must not surface a recorded IP.
    let mut deleting_pending = (*sandbox_pod_on_node(&pod_name, "node-b", 1, "Pending")).clone();
    deleting_pending.metadata.namespace = Some("space".to_string());
    deleting_pending.metadata.uid = Some("uid-2".to_string());
    deleting_pending.metadata.deletion_timestamp = Some(
        k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(k8s_openapi::jiff::Timestamp::now()),
    );
    deleting_pending
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;
    let deleting_check = substrate
        .inspect_active_execution_for_pod(
            sbx,
            &PodProxyWaitTarget::new(&pod_name, None, 1),
            &SandboxPodView::from(deleting_pending),
        )
        .await?;
    assert_eq!(deleting_check.recorded_ip, None);

    // 2. Non-readiness `resolve_pod_ip` seeing terminal `uid-1` while Postgres
    // records `uid-2` must NOT delete `pod_name` or retire `uid-2`, and
    // `resolve_pod_ip_for_readiness_uncached` stops after 1 uncached GET on a
    // fenced retry instead of spinning a second uncached GET immediately.
    let mut terminal_uid1 = (*sandbox_pod_on_node(&pod_name, "node-b", 1, "Failed")).clone();
    terminal_uid1.metadata.namespace = Some("space".to_string());
    terminal_uid1.metadata.uid = Some("uid-1".to_string());
    let mut running_uid2 = (*sandbox_pod_on_node(&pod_name, "node-b", 1, "Running")).clone();
    running_uid2.metadata.namespace = Some("space".to_string());
    running_uid2.metadata.uid = Some("uid-2".to_string());
    running_uid2
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.22".into());
    let (deletes, gets) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (deletes_c, gets_c) = (Arc::clone(&deletes), Arc::clone(&gets));
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (terminal_for_get, running_for_get) = (terminal_uid1.clone(), running_uid2);
    let (client, _log) = scripted_kube_client(move |req| {
        if req.path == expected_path && req.method == Method::DELETE {
            deletes_c.fetch_add(1, Ordering::SeqCst);
            return kube_status(StatusCode::OK, "Deleted");
        }
        if req.path == expected_path && req.method == Method::GET {
            return if gets_c.fetch_add(1, Ordering::SeqCst) == 0 {
                pod_reply(&terminal_for_get)
            } else {
                pod_reply(&running_for_get)
            };
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
    });
    let mut sub2 = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("node-b", "10.0.1.2")],
        db.pool.clone(),
    );
    sub2.k8s.client = client;
    let (sub2, _feed2) = sub2.with_pod_cache_feed(vec![terminal_uid1]);
    let readiness_retry = sub2
        .resolve_pod_ip_for_readiness_uncached(
            sbx,
            &PodProxyWaitTarget::new(&pod_name, None, 1),
            Instant::now() + Duration::from_secs(2),
        )
        .await?;
    assert!(readiness_retry.used_uncached_get);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "force_uncached readiness must return after 1 GET so ReadinessFallback paces retries"
    );
    assert_eq!(deletes.load(Ordering::SeqCst), 0);
    assert_eq!(
        sub2.resolve_pod_ip(sbx).await?.expect("uid-2").to_string(),
        "10.244.0.22"
    );
    assert_eq!(deletes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pinned_wait_rechecks_cached_uid_mismatch_when_db_advances_without_new_pod_events()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sbx = SandboxId::new();
    let pod_name = format!("space-sbx-{sbx}-g1");
    insert_test_sandbox_row(&db.pool, sbx, "started").await?;
    insert_k8s_execution(&db.pool, sbx, 1, &pod_name, "uid-a", None, true).await?;

    let mut cached_b = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Pending")).clone();
    cached_b.metadata.namespace = Some("space".to_string());
    cached_b.metadata.uid = Some("uid-b".to_string());

    let gets = Arc::new(AtomicUsize::new(0));
    let gets_c = Arc::clone(&gets);
    let (client, _log) = scripted_kube_client(move |_req| {
        gets_c.fetch_add(1, Ordering::SeqCst);
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet")
    });
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (mut substrate, _feed) = substrate.with_pod_cache_feed(vec![cached_b]);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);

    let pool_bg = db.pool.clone();
    let ip_iters = Arc::clone(&substrate.readiness_test_hooks.ip_wait_iterations);
    let updater = tokio::spawn(async move {
        let sync_deadline = Instant::now() + Duration::from_secs(1);
        while ip_iters.load(Ordering::SeqCst) == 0 {
            if Instant::now() >= sync_deadline {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    "wait_for_pod_ip_assignment never entered its cached-mismatch wait loop".into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET pod_uid = 'uid-b', updated_at = NOW() WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sbx)
        .execute(&pool_bg)
        .await?;
        Ok(())
    });

    let started = Instant::now();
    let err = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("exec"),
            sbx,
            PodProxyWaitTarget::new(&pod_name, Some("uid-a"), 1).abort_on_uid_change(),
            Instant::now() + Duration::from_millis(600),
        )
        .await
        .expect_err("pinned wait must abort once Postgres advances to uid-b");
    updater.await??;
    assert!(
        matches!(err, SubstrateError::Unavailable { .. }),
        "expected retryable Unavailable on superseded UID, got {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "mismatched cached pod recheck must fire before deadline, elapsed {:?}",
        started.elapsed()
    );
    assert_eq!(gets.load(Ordering::SeqCst), 0);
    Ok(())
}

/// An unpinned wake joiner's `follow_owner` poll (`match_recorded_pod_uid =
/// false`) treats a transient `active_execution_status` Postgres lock/query
/// failure as inconclusive rather than aborting a viable proxy wait.
#[tokio::test(flavor = "current_thread")]
async fn wake_joiner_follow_owner_survives_transient_status_db_error()
-> pplx_space_test_env::TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let worker = pplx_space_node_api_sim::SimWorkerHarness::builder("worker-a")
        .start()
        .await?;
    let sbx = SandboxId::new();
    let pod_name = format!("space-sbx-{sbx}-g1");
    insert_test_sandbox_row(&db.pool, sbx, "started").await?;
    insert_k8s_execution(&db.pool, sbx, 1, &pod_name, "uid-wake", None, true).await?;
    crate::test_support::kubernetes::insert_kubernetes_resident(
        worker.state(),
        sbx,
        "running",
        None,
    )
    .await?;

    let fast_lock_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '15ms'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with((*db.pool.connect_options()).clone())
        .await?;
    let fast_lock_pool_sync = fast_lock_pool.clone();

    let mut running = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    running.metadata.namespace = Some("space".to_string());
    running.metadata.uid = Some("uid-wake".to_string());
    running.status.get_or_insert_with(Default::default).pod_ip = Some("127.0.0.1".to_string());

    let (client, _log) = scripted_kube_client(|_req| {
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet")
    });
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        fast_lock_pool,
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port();
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![]);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);

    let ip_iters = Arc::clone(&substrate.readiness_test_hooks.ip_wait_iterations);
    let follow_err_notify = Arc::clone(&substrate.readiness_test_hooks.follow_owner_status_error);
    let lock_pool = db.pool.clone();
    let driver = async move {
        let sync_deadline = Instant::now() + Duration::from_secs(1);
        while ip_iters.load(Ordering::SeqCst) == 0 {
            if Instant::now() >= sync_deadline {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    "joiner never entered wait_for_pod_ip_assignment".into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Queue behind iteration 1's `joiner_missing_pod_recheck_at` status read
        // so `wait_for_pod_ip_assignment` is parked in `updates.recv()` for 60s
        // while `follow_owner` alone polls `active_execution_status` at 50ms.
        drop(fast_lock_pool_sync.acquire().await?);
        let err_observed = follow_err_notify.notified();
        tokio::pin!(err_observed);
        err_observed.as_mut().enable();
        let mut tx = lock_pool.begin().await?;
        sqlx::query("LOCK TABLE sandbox_k8s_executions IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await?;
        tokio::time::timeout(Duration::from_secs(1), err_observed)
            .await
            .map_err(|_| "follow_owner never hit transient active_execution_status error")?;
        tx.rollback().await?;
        feed.apply(running);
        Ok(())
    };

    let (wait_res, driver_res) = tokio::join!(
        substrate.ensure_active_execution_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            sbx,
            false,
            Instant::now() + Duration::from_secs(2),
        ),
        driver,
    );
    driver_res?;
    wait_res?;
    Ok(())
}

/// Regression: `record_confirmed(PodIpSource::Published)` retries transient
/// `active_execution_status` Postgres errors within `deadline` (and returns
/// `Ok(false)` once `deadline` elapses) instead of aborting a healthy proxy-confirmed
/// create/wake wait with `SubstrateError::Database`.
#[tokio::test(flavor = "current_thread")]
async fn record_confirmed_published_retries_transient_status_db_error()
-> pplx_space_test_env::TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sbx = SandboxId::new();
    let pod_name = format!("space-sbx-{sbx}-g1");
    insert_test_sandbox_row(&db.pool, sbx, "started").await?;
    insert_k8s_execution(&db.pool, sbx, 1, &pod_name, "uid-pub", None, true).await?;

    let fast_lock_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '15ms'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with((*db.pool.connect_options()).clone())
        .await?;

    let substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        fast_lock_pool,
    );
    let target = PodProxyWaitTarget::new(&pod_name, Some("uid-pub"), 1);
    let endpoint = (PodIp::parse("127.0.0.1")?, PodIpSource::Published);

    let mut tx = db.pool.begin().await?;
    sqlx::query("LOCK TABLE sandbox_k8s_executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    // When `deadline` has already elapsed after the transient DB error, return
    // `Ok(false)` so the caller converts deadline expiry cleanly instead of 500ing.
    let expired = substrate
        .record_confirmed(sbx, endpoint, &target, None, Instant::now())
        .await?;
    assert!(!expired);

    let release = async move {
        // Hold past `15ms` `lock_timeout` so the first status query fails, then
        // release before the `50ms` retry poll.
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.rollback().await?;
        Ok::<(), sqlx::Error>(())
    };
    let (confirmed, release_res) = tokio::join!(
        substrate.record_confirmed(
            sbx,
            endpoint,
            &target,
            None,
            Instant::now() + Duration::from_secs(2),
        ),
        release,
    );
    release_res?;
    assert!(confirmed?);
    let row = substrate
        .store
        .active_execution_status(sbx)
        .await?
        .ok_or("active execution row present")?;
    assert_eq!(row.pod_ip.as_deref(), Some("127.0.0.1"));
    Ok(())
}
