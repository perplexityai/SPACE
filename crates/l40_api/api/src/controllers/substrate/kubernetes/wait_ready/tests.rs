use super::*;
use crate::controllers::substrate::kubernetes::tests::{
    sandbox_pod_on_node, substrate_with_inventory_caches_and_pool,
};
use pplx_space_sbx_mgr_client::proto::SandboxInfo;
use sqlx::postgres::PgPoolOptions;

fn info(sandbox_id: SandboxId, generation: i64, pod_uid: &str, pod_ip: &str) -> SandboxInfo {
    SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        execution_generation: Some(generation),
        pod_uid: Some(pod_uid.to_owned()),
        external_address: Some(pod_ip.to_owned()),
        ..Default::default()
    }
}

#[test]
fn only_the_observed_executions_address_is_taken() {
    let sandbox_id = SandboxId::new();
    let other_sandbox = SandboxId::new();
    let reported = vec![
        info(other_sandbox, 1, "pod-a", "10.0.0.1"),
        info(sandbox_id, 1, "pod-old", "10.0.0.2"),
        info(sandbox_id, 1, "pod-a", "10.0.0.3"),
    ];
    let taken = |reported, generation, pod_uid| {
        node_reported_address(reported, sandbox_id, generation, pod_uid)
    };

    assert_eq!(
        taken(reported.clone(), 1, "pod-a"),
        Some(PodIp::parse("10.0.0.3").unwrap())
    );
    assert_eq!(taken(reported.clone(), 1, "pod-new"), None);
    assert_eq!(taken(reported, 2, "pod-a"), None);
    assert_eq!(
        taken(vec![info(sandbox_id, 1, "pod-a", "not-an-ip")], 1, "pod-a"),
        None
    );
}

/// The readiness probe accepts only a node serving the target execution
/// at the probed address. For a kubelet-published address, fields an older
/// node omits are not compared; a node-reported one needs them all.
#[test]
fn a_probe_accepts_only_the_target_served_at_its_address() {
    let sandbox_id = SandboxId::new();
    let probed = PodIp::parse("10.0.0.3").unwrap();
    let target = PodProxyWaitTarget::new("pod", Some("pod-a"), 1);
    let degraded = SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        ..Default::default()
    };
    for source in [PodIpSource::Published, PodIpSource::NodeReported] {
        let serves =
            |info: SandboxInfo| serves_target(vec![info], sandbox_id, &target, probed, source);
        assert!(serves(info(sandbox_id, 1, "pod-a", "10.0.0.3")));
        // A recreated runtime sandbox's node reports its new allocation.
        assert!(!serves(info(sandbox_id, 1, "pod-a", "10.0.0.4")));
        assert!(!serves(info(sandbox_id, 1, "pod-b", "10.0.0.3")));
        assert!(!serves(info(sandbox_id, 2, "pod-a", "10.0.0.3")));
        assert!(!serves(info(SandboxId::new(), 1, "pod-a", "10.0.0.3")));
        assert!(!serves_target(
            Vec::new(),
            sandbox_id,
            &target,
            probed,
            source
        ));
        assert_eq!(serves(degraded.clone()), source == PodIpSource::Published);
    }
}

/// A teardown acts on a runtime its worker names by identity, skips it
/// only when the worker reports another execution, and does not read
/// missing fields or a moved address as either.
#[test]
fn a_runtime_answer_skips_only_a_worker_that_reports_another_runtime() {
    let sandbox_id = SandboxId::new();
    let probed = PodIp::parse("10.0.0.3").unwrap();
    let target = PodProxyWaitTarget::new("pod", Some("pod-a"), 1);
    let answer = |info: SandboxInfo| runtime_answer(vec![info], sandbox_id, &target, probed);
    assert_eq!(
        answer(info(sandbox_id, 1, "pod-a", "10.0.0.3")),
        RuntimeAnswer::Serves
    );
    // A restarted sandbox manager no longer reports the address.
    let restarted = SandboxInfo {
        external_address: None,
        ..info(sandbox_id, 1, "pod-a", "10.0.0.3")
    };
    assert_eq!(answer(restarted), RuntimeAnswer::Serves);
    for elsewhere in [
        info(sandbox_id, 1, "pod-b", "10.0.0.3"),
        info(sandbox_id, 2, "pod-a", "10.0.0.3"),
        info(SandboxId::new(), 1, "pod-a", "10.0.0.3"),
    ] {
        assert_eq!(answer(elsewhere), RuntimeAnswer::Elsewhere);
    }
    // The target's execution at another address is still a runtime to
    // clean up, so the teardown waits for an address it can confirm.
    let moved = info(sandbox_id, 1, "pod-a", "10.0.0.4");
    let unidentified_moved = SandboxInfo {
        pod_uid: None,
        ..info(sandbox_id, 1, "pod-a", "10.0.0.4")
    };
    let degraded = SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        ..Default::default()
    };
    for unconfirmed in [moved, unidentified_moved, degraded] {
        assert_eq!(answer(unconfirmed), RuntimeAnswer::Unconfirmed);
    }
}

/// Healthy reflectors and brief reconnect blips never trigger fallback reads;
/// sustained watch faults and watch silence stagger long-waiting creates across
/// per-waiter jitter windows and pace subsequent reads from completion.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn readiness_fallback_fires_only_when_reflector_is_degraded_and_paces_after_completion() {
    let quiet = Duration::from_secs(5);
    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener.local_addr().expect("local_addr").port()
    };
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy(&format!("postgres://unused@127.0.0.1:{closed_port}/unused"))
        .expect("lazy pool builds without connecting");
    let (mut substrate, mut feed) =
        substrate_with_inventory_caches_and_pool(Vec::new(), Vec::new(), pool)
            .with_pod_cache_feed(Vec::new());
    substrate.budgets.readiness_quiet_reflector = quiet;

    let start = Instant::now();
    let mut early = ReadinessFallback::with_jitter(start, quiet, 0.0);
    let late = ReadinessFallback::with_jitter(start, quiet, 1.0);

    // Even right after waiter creation (`t = 100ms`, well before the `4s..5s`
    // initial silence floor), a brief watch fault (`< 500ms`) reconnects with
    // zero fallback `GET`s while a sustained fault fires at `faulted_at +
    // 500ms..2.0s` without waiting out `initial_silence_floor`.
    tokio::time::advance(Duration::from_millis(100)).await;
    feed.mark_watch_error();
    tokio::time::advance(Duration::from_millis(400)).await;
    assert!(!early.due(Instant::now(), &substrate) && !late.due(Instant::now(), &substrate));
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(
        early.due(Instant::now(), &substrate) && !late.due(Instant::now(), &substrate),
        "early-waiter watch fault must become due after 500ms grace without waiting 4s..5s"
    );
    feed.relist(Vec::new());
    assert!(substrate.pod_reflector_watch_healthy_at(Instant::now()));

    // Advance past `5s` while healthy so both waiters are post-`initial_silence_floor`.
    for _ in 0..3 {
        feed.apply(
            (*sandbox_pod_on_node("space-sbx-heartbeat-g1", "node-a", 1, "Running")).clone(),
        );
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(substrate.pod_reflector_watch_healthy_at(Instant::now()));
        assert!(!early.due(Instant::now(), &substrate));
        assert!(!late.due(Instant::now(), &substrate));
    }

    // Silence reaching `quiet` makes `early` (`jitter = 0.0`, `5.0s`) due while
    // `late` (`jitter = 1.0`, `6.0s`) stays staggered for another second.
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(!substrate.pod_reflector_watch_healthy_at(Instant::now()));
    assert!(early.due(Instant::now(), &substrate));
    assert!(!late.due(Instant::now(), &substrate));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(late.due(Instant::now(), &substrate));

    // Pacing starts when the read completes, not when it started, so a 4s
    // apiserver response still leaves a full jittered `quiet` gap afterward.
    tokio::time::advance(Duration::from_secs(4)).await;
    early.record_completed_with_jitter(Instant::now(), 0.0);
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(!early.due(Instant::now(), &substrate));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(early.due(Instant::now(), &substrate));

    // Once healthy deliveries resume, a brief watch error (`400ms` < `500ms`
    // grace) that reconnects via `InitDone` emits zero fallback reads, whereas
    // a sustained watch fault staggers `early` (`500ms`) and `late` (`2.0s`).
    feed.apply((*sandbox_pod_on_node("space-sbx-recovered-g1", "node-a", 1, "Running")).clone());
    assert!(!early.due(Instant::now(), &substrate));
    feed.mark_watch_error();
    tokio::time::advance(Duration::from_millis(400)).await;
    assert!(
        !early.due(Instant::now(), &substrate) && !late.due(Instant::now(), &substrate),
        "brief reconnect / 410 Gone re-list inside fault grace must not herd GETs"
    );
    feed.relist(Vec::new());
    assert!(substrate.pod_reflector_watch_healthy_at(Instant::now()));

    feed.mark_watch_error();
    tokio::time::advance(Duration::from_millis(500)).await;
    assert!(early.due(Instant::now(), &substrate));
    assert!(!late.due(Instant::now(), &substrate));
    tokio::time::advance(Duration::from_millis(1500)).await;
    assert!(late.due(Instant::now(), &substrate));
}

/// During an async paginated reflector re-list (`Init` -> `InitApply` ->
/// `InitDone`), `InitApply` buffers privately without marking the watch stream
/// faulted or triggering fallback `GET`s (even on a waiter older than `quiet`),
/// and `InitDone` wakes `wait_for_pod_ip_assignment` immediately once the
/// rebuilt snapshot swaps into the store.
#[tokio::test(flavor = "current_thread")]
async fn wait_for_pod_ip_assignment_wakes_on_relist_init_done() -> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        kube_status, scripted_kube_client,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_k8s_execution, insert_test_sandbox_row, k8s_node_with_internal_ip,
    };
    use hyper::http::{Method, StatusCode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_k8s_execution(&db.pool, sandbox_id, 1, &pod_name, "relist-uid", None, true).await?;

    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET {
            reply_gets.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });

    let mut pending = (*sandbox_pod_on_node(&pod_name, "node-a", 1, "Pending")).clone();
    pending.metadata.uid = Some("relist-uid".to_string());
    pending.status.get_or_insert_with(Default::default).pod_ip = None;

    let other = (*sandbox_pod_on_node("space-sbx-other-g1", "node-a", 1, "Running")).clone();

    let mut running = (*sandbox_pod_on_node(&pod_name, "node-a", 1, "Running")).clone();
    running.metadata.uid = Some("relist-uid".to_string());
    running.status.get_or_insert_with(Default::default).pod_ip = Some("10.0.0.77".to_string());

    let mut substrate = substrate_with_inventory_caches_and_pool(
        Vec::new(),
        vec![k8s_node_with_internal_ip("node-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![pending]);
    let quiet = Duration::from_secs(30);
    substrate.budgets.readiness_quiet_reflector = quiet;

    let now = Instant::now();
    let deadline = now + Duration::from_secs(2);
    // Simulate a waiter that has already waited past its initial `next_allowed`
    // floor so a false-degraded `Init`..`InitDone` window would fire a `GET`.
    let mut fallback = ReadinessFallback::with_jitter(now - quiet, quiet, 0.0);
    let target = PodProxyWaitTarget::new(&pod_name, Some("relist-uid"), 1);

    let relist = async move {
        feed.wait_for_subscriber().await;
        feed.begin_relist();
        tokio::task::yield_now().await;
        feed.relist_apply(other);
        tokio::task::yield_now().await;
        feed.relist_apply(running);
        tokio::task::yield_now().await;
        feed.finish_relist();
    };
    let (assigned, ()) = tokio::join!(
        substrate.wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            &target,
            deadline,
            &mut fallback,
        ),
        relist
    );
    let (ip, source) = assigned?.expect("InitDone wakeup resolves the swapped-in pod IP");
    assert_eq!(ip, PodIp::parse("10.0.0.77")?);
    assert_eq!(source, PodIpSource::Published);
    assert_eq!(gets.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Two degraded-fallback invariants in `wait_for_pod_ip_assignment`:
/// 1. A UID-unpinned joiner (`target.pod_uid = None`) rejects a cached
///    predecessor pod under the same name without clobbering the replacement
///    execution's node placement or stalling `ReadinessFallback`, then resolves
///    the replacement pod on its paced fallback `GET`.
/// 2. A UID-pinned create/restore (`target.pod_uid = Some(...)`) whose
///    degraded-mode fallback `GET` observes a scheduled `Pending` pod before
///    kubelet publishes `podIP` forwards that view to `observed`, waking
///    `node_reported_pod_ip` immediately in a single `GET`.
#[tokio::test(flavor = "current_thread")]
async fn unpinned_joiner_rejects_predecessor_and_pinned_wait_wakes_node_report_on_fallback_get()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        KubeReply, ScriptedWorkerConfig, kube_status, scripted_kube_client, start_scripted_worker,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_k8s_execution, insert_test_sandbox_row, k8s_node_with_internal_ip,
        node_api_pod_on_node,
    };
    use hyper::http::{Method, StatusCode};
    use pplx_space_sql_locator::sql;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // --- Part 1: UID-unpinned joiner with cached predecessor pod ---
    let join_sandbox = SandboxId::new();
    insert_test_sandbox_row(&db.pool, join_sandbox, "restoring").await?;
    let join_pod_name = format!("space-sbx-{join_sandbox}-g1");
    insert_k8s_execution(
        &db.pool,
        join_sandbox,
        1,
        &join_pod_name,
        "uid-new",
        None,
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET k8s_node_name = 'worker-sim-node'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(join_sandbox)
    .execute(&db.pool)
    .await?;

    // --- Part 2: UID-pinned create with scheduled Pending pod on fallback GET ---
    let pinned_sandbox = SandboxId::new();
    insert_test_sandbox_row(&db.pool, pinned_sandbox, "creating").await?;
    let pinned_pod_name = format!("space-sbx-{pinned_sandbox}-g1");
    insert_k8s_execution(
        &db.pool,
        pinned_sandbox,
        1,
        &pinned_pod_name,
        "uid-pinned",
        None,
        true,
    )
    .await?;

    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
        pinned_sandbox.to_string(),
        "seed".into(),
        "fc".into(),
        "root".into(),
        None,
        false,
    )
    .with_allocation(1, "uid-pinned", "10.0.0.88");
    record.status = "running".into();
    worker
        .sim
        .insert_new(record)
        .await
        .expect("worker records pinned execution");

    let mut join_replacement =
        (*sandbox_pod_on_node(&join_pod_name, "worker-sim-node", 1, "Running")).clone();
    join_replacement.metadata.uid = Some("uid-new".to_string());
    join_replacement
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.0.0.79".to_string());

    let mut pinned_pending =
        (*sandbox_pod_on_node(&pinned_pod_name, "worker-sim-node", 1, "Pending")).clone();
    pinned_pending.metadata.uid = Some("uid-pinned".to_string());
    pinned_pending
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;

    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let join_path = format!("/api/v1/namespaces/space/pods/{join_pod_name}");
    let pinned_path = format!("/api/v1/namespaces/space/pods/{pinned_pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == join_path {
            reply_gets.fetch_add(1, Ordering::SeqCst);
            return KubeReply::Json(
                StatusCode::OK,
                serde_json::to_value(&join_replacement).unwrap(),
            );
        }
        if request.method == Method::GET && request.path == pinned_path {
            reply_gets.fetch_add(1, Ordering::SeqCst);
            return KubeReply::Json(
                StatusCode::OK,
                serde_json::to_value(&pinned_pending).unwrap(),
            );
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRequest")
    });

    // Seed both a nonterminal (`Pending` on `gone-node`) and a terminal
    // (`Failed` on `gone-node`) predecessor check: start with `Pending` on
    // `gone-node` to prove unfenced placement persistence is blocked, then
    // transition to `Failed` to prove a rejected terminal predecessor does not
    // stall `ReadinessFallback` when the reflector faults.
    let mut predecessor_pending =
        (*sandbox_pod_on_node(&join_pod_name, "gone-node", 1, "Pending")).clone();
    predecessor_pending.metadata.uid = Some("uid-old".to_string());
    let mut predecessor_terminal =
        (*sandbox_pod_on_node(&join_pod_name, "gone-node", 1, "Failed")).clone();
    predecessor_terminal.metadata.uid = Some("uid-old".to_string());
    predecessor_terminal
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.0.0.11".to_string());

    let mut worker_pod = node_api_pod_on_node("space-worker-node-a", "worker-sim-node");
    worker_pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        worker.port.to_string(),
    )]));
    worker_pod
        .status
        .as_mut()
        .expect("node-api pod has status")
        .pod_ip = Some("127.0.0.1".to_string());

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![worker_pod],
        vec![k8s_node_with_internal_ip("worker-sim-node", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![predecessor_pending]);
    let quiet = Duration::from_millis(80);
    substrate.budgets.readiness_quiet_reflector = quiet;

    let fault = async move {
        feed.wait_for_subscriber().await;
        feed.apply(predecessor_terminal);
        tokio::task::yield_now().await;
        feed.mark_watch_error();
    };
    let now = Instant::now();
    let mut join_fallback = ReadinessFallback::with_jitter(now - quiet, quiet, 0.0);
    let join_target = PodProxyWaitTarget::new(&join_pod_name, None, 1);
    let (join_assigned, ()) = tokio::join!(
        substrate.wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            join_sandbox,
            &join_target,
            now + Duration::from_secs(2),
            &mut join_fallback,
        ),
        fault
    );
    assert_eq!(
        join_assigned?.expect("joiner resolves replacement pod via paced fallback GET"),
        (PodIp::parse("10.0.0.79")?, PodIpSource::Published)
    );
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    let row = substrate
        .store
        .active_execution_status(join_sandbox)
        .await?
        .expect("active execution row remains intact");
    assert_eq!(row.k8s_node_name.as_deref(), Some("worker-sim-node"));

    // Part 2: UID-pinned wait wakes `node_reported_pod_ip` from fallback `GET`.
    let now = Instant::now();
    let mut pinned_fallback = ReadinessFallback::with_jitter(now - quiet, quiet, 0.0);
    let pinned_target = PodProxyWaitTarget::new(&pinned_pod_name, Some("uid-pinned"), 1);
    let pinned_assigned = substrate
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            pinned_sandbox,
            &pinned_target,
            now + Duration::from_secs(2),
            &mut pinned_fallback,
        )
        .await?
        .expect("fallback GET scheduled_pod wakes node_reported_pod_ip");
    assert_eq!(
        pinned_assigned,
        (PodIp::parse("10.0.0.88")?, PodIpSource::NodeReported)
    );
    assert_eq!(gets.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn owning_readiness_wait_repairs_unpersisted_post_create_metadata()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        ScriptedWorkerConfig, kube_status, scripted_kube_client, start_scripted_worker,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, node_api_pod_on_node,
        sandbox_pod_on_node, substrate_with_inventory_caches_and_pool,
    };
    use hyper::http::StatusCode;
    use pplx_space_sql_locator::sql;
    use std::collections::BTreeMap;

    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // Seed two `creating` executions whose post-POST `persist_pod_metadata`
    // failed transiently (`state = 'creating'`, `pod_uid = NULL`, `pod_ip = NULL`):
    // 1. `published_sbx`: pod already has `status.podIP = 127.0.0.1` (`Published`).
    // 2. `node_reported_sbx`: pod is scheduled `Pending` with `podIP = None` while
    //    its worker node already serves `(sandbox_id, generation = 1, "uid-node", "127.0.0.1")`.
    let published_sbx = SandboxId::new();
    let published_pod_name = format!("space-sbx-{published_sbx}-g1");
    let node_reported_sbx = SandboxId::new();
    let node_reported_pod_name = format!("space-sbx-{node_reported_sbx}-g1");
    let mut creating_epochs = std::collections::HashMap::new();
    for (sbx, pod_name) in [
        (published_sbx, &published_pod_name),
        (node_reported_sbx, &node_reported_pod_name),
    ] {
        insert_test_sandbox_row(&db.pool, sbx, "creating").await?;
        let epoch: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, $2, 'space', 'creating', true)
             RETURNING updated_at"
        ))
        .bind(sbx)
        .bind(pod_name)
        .fetch_one(&db.pool)
        .await?;
        creating_epochs.insert(sbx, epoch);
    }

    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    for (sbx, uid) in [(published_sbx, "uid-pub"), (node_reported_sbx, "uid-node")] {
        let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
            sbx.to_string(),
            "seed".into(),
            "fc".into(),
            "root".into(),
            None,
            false,
        )
        .with_allocation(1, uid, "127.0.0.1");
        record.status = "running".into();
        worker
            .sim
            .insert_new(record)
            .await
            .expect("worker records execution");
    }

    let mut published_pod =
        (*sandbox_pod_on_node(&published_pod_name, "worker-sim-node", 1, "Running")).clone();
    published_pod.metadata.uid = Some("uid-pub".to_string());
    published_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("127.0.0.1".to_string());

    let mut pending_pod =
        (*sandbox_pod_on_node(&node_reported_pod_name, "worker-sim-node", 1, "Pending")).clone();
    pending_pod.metadata.uid = Some("uid-node".to_string());
    pending_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;

    let mut worker_pod = node_api_pod_on_node("space-worker-node-a", "worker-sim-node");
    worker_pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        worker.port.to_string(),
    )]));
    worker_pod
        .status
        .as_mut()
        .expect("node-api pod has status")
        .pod_ip = Some("127.0.0.1".to_string());

    let (client, _log) =
        scripted_kube_client(|_| kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet"));
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![worker_pod],
        vec![k8s_node_with_internal_ip("worker-sim-node", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    let (substrate, _feed) = substrate.with_pod_cache_feed(vec![published_pod, pending_pod]);

    // A joiner (`pod_uid: None`) still treats an unrecorded `creating` row as in-flight.
    let joiner_out = substrate
        .resolve_pod_ip_for_readiness(
            published_sbx,
            &PodProxyWaitTarget::new(&published_pod_name, None, 1),
            Instant::now() + Duration::from_secs(2),
        )
        .await?;
    assert!(joiner_out.resolved.is_none());

    // Owning UID-pinned waits repair both `Published` and `NodeReported` rows to `running`.
    let localhost = PodIp::parse("127.0.0.1")?;
    for (sbx, pod_name, uid) in [
        (published_sbx, &published_pod_name, "uid-pub"),
        (node_reported_sbx, &node_reported_pod_name, "uid-node"),
    ] {
        let (ip, _binding) = substrate
            .wait_for_pod_proxy_reachable(
                crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
                sbx,
                PodProxyWaitTarget::new(pod_name, Some(uid), 1)
                    .with_creating_repair_epoch(creating_epochs.get(&sbx).copied()),
                Instant::now() + Duration::from_secs(2),
            )
            .await?;
        assert_eq!(ip, localhost);
        let row = substrate
            .store
            .active_execution_status(sbx)
            .await?
            .expect("repaired execution row");
        assert_eq!(row.state, ActiveExecutionState::Running);
        assert_eq!(row.pod_uid.as_ref().map(|u| u.as_str()), Some(uid));
        assert_eq!(row.pod_ip.as_deref(), Some("127.0.0.1"));
        assert_eq!(substrate.resolve_pod_ip(sbx).await?, Some(localhost));
    }

    // Verify `repair_creating_pod_metadata` preserves `sandboxes.activating_client_id`
    // on a reflector cache miss by querying the apiserver instead of treating
    // the missing cache entry as an unlabeled pod, and refuses to overwrite the
    // sandbox row's client attribution when the pod has already disappeared.
    let cache_miss_sbx = SandboxId::new();
    let cache_miss_pod_name = format!("space-sbx-{cache_miss_sbx}-g1");
    let vanished_sbx = SandboxId::new();
    let vanished_pod_name = format!("space-sbx-{vanished_sbx}-g1");
    for (sbx, pod_name, prior_client) in [
        (cache_miss_sbx, &cache_miss_pod_name, "prior-client"),
        (vanished_sbx, &vanished_pod_name, "keep-client"),
    ] {
        insert_test_sandbox_row(&db.pool, sbx, "creating").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET activating_client_id = $2 WHERE id = $1"
        ))
        .bind(sbx)
        .bind(prior_client)
        .execute(&db.pool)
        .await?;
        let epoch: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, $2, 'space', 'creating', true)
             RETURNING updated_at"
        ))
        .bind(sbx)
        .bind(pod_name)
        .fetch_one(&db.pool)
        .await?;
        creating_epochs.insert(sbx, epoch);
    }

    let mut uncached_pod =
        (*sandbox_pod_on_node(&cache_miss_pod_name, "worker-sim-node", 1, "Pending")).clone();
    uncached_pod.metadata.uid = Some("uid-miss".to_string());
    uncached_pod
        .metadata
        .labels
        .get_or_insert_with(Default::default)
        .insert(
            pplx_space_sandbox_pod::SANDBOX_CLIENT_ID_LABEL.to_string(),
            "uncached-pod-client".to_string(),
        );
    let expected_uncached_path = format!("/api/v1/namespaces/space/pods/{cache_miss_pod_name}");
    let (miss_client, _miss_log) = scripted_kube_client(move |request| {
        if request.path == expected_uncached_path {
            crate::controllers::substrate::kubernetes::scripted_test_doubles::pod_reply(
                &uncached_pod,
            )
        } else {
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
    });
    let mut miss_substrate =
        substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    miss_substrate.k8s.client = miss_client;
    let (miss_substrate, _empty_feed) = miss_substrate.with_pod_cache_feed(vec![]);

    assert!(
        miss_substrate
            .repair_creating_pod_metadata(
                cache_miss_sbx,
                &PodProxyWaitTarget::new(&cache_miss_pod_name, Some("uid-miss"), 1)
                    .with_creating_repair_epoch(creating_epochs.get(&cache_miss_sbx).copied()),
                "uid-miss",
                None,
                Instant::now() + Duration::from_secs(2),
            )
            .await?
    );
    let repaired_client: Option<String> = sqlx::query_scalar(sql!(
        "SELECT activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(cache_miss_sbx)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(repaired_client.as_deref(), Some("uncached-pod-client"));

    assert!(
        !miss_substrate
            .repair_creating_pod_metadata(
                vanished_sbx,
                &PodProxyWaitTarget::new(&vanished_pod_name, Some("uid-vanished"), 1)
                    .with_creating_repair_epoch(creating_epochs.get(&vanished_sbx).copied()),
                "uid-vanished",
                None,
                Instant::now() + Duration::from_secs(2),
            )
            .await?
    );
    let preserved_client: Option<String> = sqlx::query_scalar(sql!(
        "SELECT activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(vanished_sbx)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(preserved_client.as_deref(), Some("keep-client"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_degraded_fallback_get_yields_to_reflector_recovery()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::scripted_kube_client_async;
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
        substrate_with_inventory_caches_and_pool,
    };
    use axum::http::StatusCode;
    use pplx_space_sql_locator::sql;

    struct NotifyOnDrop(Option<Arc<tokio::sync::Notify>>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(notify) = self.0.take() {
                notify.notify_one();
            }
        }
    }

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    let settled_sbx = SandboxId::new();
    let settled_pod_name = format!("space-sbx-{settled_sbx}-g1");
    let pending_sbx = SandboxId::new();
    let pending_pod_name = format!("space-sbx-{pending_sbx}-g1");
    for (sbx, pod_name, uid) in [
        (settled_sbx, &settled_pod_name, "uid-settled"),
        (pending_sbx, &pending_pod_name, "uid-pending"),
    ] {
        insert_test_sandbox_row(&db.pool, sbx, "creating").await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, 1, $2, 'space', $3, 'creating', true)"
        ))
        .bind(sbx)
        .bind(pod_name)
        .bind(uid)
        .execute(&db.pool)
        .await?;
    }

    let get_started = Arc::new(tokio::sync::Notify::new());
    let case2_get_dropped = Arc::new(tokio::sync::Notify::new());
    let get_started_for_client = Arc::clone(&get_started);
    let case2_dropped_for_client = Arc::clone(&case2_get_dropped);
    let pending_path = format!("/api/v1/namespaces/space/pods/{pending_pod_name}");
    let (client, log) = scripted_kube_client_async(move |req| {
        get_started_for_client.notify_one();
        let drop_notify = (req.path == pending_path).then(|| Arc::clone(&case2_dropped_for_client));
        async move {
            let _on_drop = NotifyOnDrop(drop_notify);
            std::future::pending::<
                crate::controllers::substrate::kubernetes::scripted_test_doubles::KubeReply,
            >()
            .await;
            crate::controllers::substrate::kubernetes::scripted_test_doubles::kube_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unreachable",
            )
        }
    });
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![]);
    let quiet = Duration::from_millis(1200);
    substrate.budgets.readiness_pod_get = Duration::from_millis(500);
    substrate.budgets.readiness_quiet_reflector = quiet;

    // Case 1: degraded-mode fallback GET hangs, then the reflector recovers
    // with a settled `Running` pod before `readiness_pod_get` expires. The wait
    // resolves from the cache immediately instead of blocking on the hung GET.
    feed.mark_watch_error();
    let mut settled_pod =
        (*sandbox_pod_on_node(&settled_pod_name, "worker-a", 1, "Running")).clone();
    settled_pod.metadata.namespace = Some("space".to_string());
    settled_pod.metadata.uid = Some("uid-settled".to_string());
    settled_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.42".to_string());

    let get_started_case1 = Arc::clone(&get_started);
    let feed_task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), get_started_case1.notified())
            .await
            .expect("Case 1 degraded fallback GET must start");
        feed.relist(vec![settled_pod]);
        (feed, Instant::now())
    });

    let started = Instant::now();
    let mut fallback = ReadinessFallback::with_jitter(started - quiet, quiet, 0.0);
    let resolved = substrate
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            settled_sbx,
            &PodProxyWaitTarget::new(&settled_pod_name, Some("uid-settled"), 1),
            Instant::now() + Duration::from_secs(3),
            &mut fallback,
        )
        .await?;
    let (mut feed, relisted_at) = feed_task.await?;
    let post_relist = relisted_at.elapsed();
    assert_eq!(
        resolved,
        Some((PodIp::parse("10.244.0.42")?, PodIpSource::Published))
    );
    assert!(
        post_relist < Duration::from_millis(250),
        "reflector recovery must resolve without waiting on the 500ms stalled fallback GET: {post_relist:?}",
    );
    assert_eq!(
        log.lock().expect("kube log").len(),
        1,
        "degraded fallback GET was started once before reflector recovery pre-empted it"
    );

    // Case 2: degraded-mode fallback GET hangs, the reflector recovers with an
    // unsettled `Pending` pod before `readiness_pod_get` expires, the stale
    // degraded GET times out with `Unavailable` while the reflector is healthy,
    // and a later reflector `Running` event completes the wait cleanly.
    substrate.budgets.readiness_pod_get = Duration::from_millis(80);
    feed.mark_watch_error();
    let mut pending_pod =
        (*sandbox_pod_on_node(&pending_pod_name, "worker-a", 1, "Pending")).clone();
    pending_pod.metadata.namespace = Some("space".to_string());
    pending_pod.metadata.uid = Some("uid-pending".to_string());
    pending_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = None;
    let mut running_pod = pending_pod.clone();
    let running_status = running_pod.status.get_or_insert_with(Default::default);
    running_status.phase = Some("Running".to_string());
    running_status.pod_ip = Some("10.244.0.43".to_string());

    let get_started_case2 = Arc::clone(&get_started);
    let suppressed_case2 = Arc::clone(
        &substrate
            .readiness_test_hooks
            .suppressed_healthy_fallback_error,
    );
    let feed_task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), get_started_case2.notified())
            .await
            .expect("Case 2 degraded fallback GET must start");
        let mut suppressed_wait = std::pin::pin!(suppressed_case2.notified());
        suppressed_wait.as_mut().enable();
        feed.relist(vec![pending_pod]);
        tokio::time::timeout(Duration::from_secs(2), case2_get_dropped.notified())
            .await
            .expect("Case 2 hung fallback GET must be dropped when readiness_pod_get expires");
        tokio::time::timeout(Duration::from_secs(2), suppressed_wait)
            .await
            .expect("InFlightFallback::finish must suppress Unavailable once reflector recovered");
        feed.apply(running_pod);
        feed
    });

    let now = Instant::now();
    let mut fallback = ReadinessFallback::with_jitter(now - quiet, quiet, 0.0);
    let resolved = substrate
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            pending_sbx,
            &PodProxyWaitTarget::new(&pending_pod_name, Some("uid-pending"), 1),
            Instant::now() + Duration::from_secs(3),
            &mut fallback,
        )
        .await?;
    let _feed = feed_task.await?;
    assert_eq!(
        resolved,
        Some((PodIp::parse("10.244.0.43")?, PodIpSource::Published))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pending_fallback_does_not_overlap_terminal_confirmation_get()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        kube_status, pod_reply, scripted_kube_client_async,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
        substrate_with_inventory_caches_and_pool,
    };
    use axum::http::{Method, StatusCode};
    use pplx_space_sql_locator::sql;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-terminal-overlap', 'creating', true)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;

    let mut failed_pod = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Failed")).clone();
    failed_pod.metadata.namespace = Some("space".to_string());
    failed_pod.metadata.uid = Some("uid-terminal-overlap".to_string());

    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let readiness_gets = Arc::new(AtomicUsize::new(0));
    let deleted = Arc::new(AtomicBool::new(false));
    let get_started = Arc::new(tokio::sync::Notify::new());
    let release_get = Arc::new(tokio::sync::Notify::new());

    let in_flight_c = Arc::clone(&in_flight);
    let max_in_flight_c = Arc::clone(&max_in_flight);
    let readiness_gets_c = Arc::clone(&readiness_gets);
    let deleted_c = Arc::clone(&deleted);
    let get_started_c = Arc::clone(&get_started);
    let release_get_c = Arc::clone(&release_get);
    let failed_for_reply = failed_pod.clone();
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");

    let (client, _log) = scripted_kube_client_async(move |request| {
        let in_flight = Arc::clone(&in_flight_c);
        let max_in_flight = Arc::clone(&max_in_flight_c);
        let readiness_gets = Arc::clone(&readiness_gets_c);
        let deleted = Arc::clone(&deleted_c);
        let get_started = Arc::clone(&get_started_c);
        let release_get = Arc::clone(&release_get_c);
        let failed_for_reply = failed_for_reply.clone();
        let expected_path = expected_path.clone();
        async move {
            match (request.method, request.path == expected_path) {
                (Method::GET, true) if deleted.load(Ordering::SeqCst) => {
                    kube_status(StatusCode::NOT_FOUND, "NotFound")
                }
                (Method::GET, true) => {
                    readiness_gets.fetch_add(1, Ordering::SeqCst);
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(current, Ordering::SeqCst);
                    get_started.notify_one();
                    release_get.notified().await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    pod_reply(&failed_for_reply)
                }
                (Method::DELETE, true) => {
                    deleted.store(true, Ordering::SeqCst);
                    kube_status(StatusCode::OK, "Success")
                }
                _ => kube_status(StatusCode::NOT_FOUND, "NotFound"),
            }
        }
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.k8s_blocking_delete_timeout = Duration::from_millis(10);
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![]);
    let quiet = Duration::from_millis(600);
    substrate.budgets.readiness_quiet_reflector = quiet;
    feed.mark_watch_error();

    let release_for_driver = Arc::clone(&release_get);
    let max_for_driver = Arc::clone(&max_in_flight);
    let awaiting_terminal = Arc::clone(
        &substrate
            .readiness_test_hooks
            .awaiting_in_flight_terminal_fallback,
    );
    let driver = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), get_started.notified())
            .await
            .expect("initial degraded fallback GET must start");
        let mut terminal_wait = std::pin::pin!(awaiting_terminal.notified());
        terminal_wait.as_mut().enable();
        feed.apply(failed_pod);
        tokio::time::timeout(Duration::from_secs(2), terminal_wait)
            .await
            .expect("terminal reflector update must await the already-running fallback GET");
        assert_eq!(
            max_for_driver.load(Ordering::SeqCst),
            1,
            "terminal reflector update must not launch a concurrent second GET"
        );
        release_for_driver.notify_waiters();
        feed
    });

    let now = Instant::now();
    let mut fallback = ReadinessFallback::with_jitter(now - quiet, quiet, 0.0);
    let resolved = substrate
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            &PodProxyWaitTarget::new(&pod_name, Some("uid-terminal-overlap"), 1),
            Instant::now() + Duration::from_millis(450),
            &mut fallback,
        )
        .await;
    let _feed = driver.await?;
    assert!(
        matches!(resolved, Err(SubstrateError::Failed { .. })),
        "confirmed terminal pod must fail fast once retired instead of waiting to deadline: {resolved:?}"
    );
    assert!(
        now.elapsed() < Duration::from_millis(300),
        "confirmed terminal retirement must return before the 450ms phase deadline"
    );
    assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
    assert_eq!(
        readiness_gets.load(Ordering::SeqCst),
        1,
        "already-running fallback GET retired the terminal pod without a duplicate readiness GET"
    );
    assert!(
        substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none(),
        "confirmed terminal pod must retire the active execution row"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn post_ip_degraded_fallback_bypasses_stale_settled_cache_view()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        kube_status, pod_reply, scripted_kube_client,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
        substrate_with_inventory_caches_and_pool,
    };
    use axum::http::{Method, StatusCode};
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
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-post-ip', 'creating', true)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;
    crate::test_support::kubernetes::insert_kubernetes_resident(
        worker.state(),
        sandbox_id,
        "running",
        None,
    )
    .await?;

    // Seed `pod_store` with an already-settled pod on an alternate loopback IP,
    // where a silent TCP listener holds accepted sockets open without speaking
    // HTTP/2 (so `stale_ip` never completes a proxy handshake), while the
    // apiserver serves the updated `127.0.0.1` allocation after the reflector
    // watch faults. Running with a generous `2s` deadline keeps CI runners
    // deterministic while proving `pending_post_ip_fallback` bypasses the stale
    // cached view once (`get_calls == 1`) and switches to `127.0.0.1`.
    let (stale_ip, listener) = match tokio::net::TcpListener::bind(("[::1]", worker.port())).await {
        Ok(listener) => ("::1", listener),
        Err(_) => (
            "127.0.0.2",
            tokio::net::TcpListener::bind(("127.0.0.2", worker.port()))
                .await
                .expect("bind stalled loopback listener on worker port"),
        ),
    };
    let _hold_stalled = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    let mut stale_pod = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    stale_pod.metadata.namespace = Some("space".to_string());
    stale_pod.metadata.uid = Some("uid-post-ip".to_string());
    stale_pod.status.get_or_insert_with(Default::default).pod_ip = Some(stale_ip.to_string());

    let mut fresh_pod = stale_pod.clone();
    fresh_pod.status.get_or_insert_with(Default::default).pod_ip = Some("127.0.0.1".to_string());

    let get_calls = Arc::new(AtomicUsize::new(0));
    let get_calls_c = Arc::clone(&get_calls);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            get_calls_c.fetch_add(1, Ordering::SeqCst);
            // Return the replacement IP ONLY in the GET response while leaving
            // `stale_pod` untouched in `pod_store`.
            pod_reply(&fresh_pod)
        } else {
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port();
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![stale_pod]);
    // Control follow-up GET eligibility separately: `mark_watch_error()` makes
    // the first degraded GET eligible immediately, while `readiness_quiet_reflector`
    // + fixed jitter holds any second GET past the wait deadline.
    substrate.budgets.readiness_quiet_reflector = Duration::from_millis(350);
    *substrate
        .readiness_test_hooks
        .fallback_jitter
        .lock()
        .expect("fallback_jitter") = Some(0.5);
    feed.mark_watch_error();

    let (resolved_ip, binding) = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            PodProxyWaitTarget::new(&pod_name, Some("uid-post-ip"), 1),
            Instant::now() + Duration::from_millis(250),
        )
        .await?;
    let _keep_feed = feed;
    assert_eq!(resolved_ip, PodIp::parse("127.0.0.1")?);
    assert_eq!(binding.execution_object_id, "uid-post-ip");
    assert_eq!(
        get_calls.load(Ordering::SeqCst),
        1,
        "post-IP degraded fallback must bypass stale cached view once and switch to 127.0.0.1"
    );
    let cached_after = substrate
        .pod_store
        .get(&kube::runtime::reflector::ObjectRef::new(&pod_name).within("space"))
        .expect("stale pod remains in reflector cache");
    assert_eq!(
        cached_after.pod_ip(),
        Some(stale_ip),
        "reflector cache must still hold stale_ip so only the first GET response could supply 127.0.0.1"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn post_ip_degraded_fallback_retirement_cancels_probe_and_refuses_binding()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        kube_status, scripted_kube_client,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
        substrate_with_inventory_caches_and_pool,
    };
    use axum::http::{Method, StatusCode};
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
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-retire', '127.0.0.1', 'running', true)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;
    crate::test_support::kubernetes::insert_kubernetes_resident(
        worker.state(),
        sandbox_id,
        "running",
        None,
    )
    .await?;

    let (stale_ip, listener) = match tokio::net::TcpListener::bind(("[::1]", worker.port())).await {
        Ok(listener) => ("::1", listener),
        Err(_) => (
            "127.0.0.2",
            tokio::net::TcpListener::bind(("127.0.0.2", worker.port()))
                .await
                .expect("bind stalled loopback listener on worker port"),
        ),
    };
    let _hold_stalled = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });

    let mut cached_pod = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    cached_pod.metadata.namespace = Some("space".to_string());
    cached_pod.metadata.uid = Some("uid-retire".to_string());
    cached_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some(stale_ip.to_string());

    let get_calls = Arc::new(AtomicUsize::new(0));
    let get_calls_c = Arc::clone(&get_calls);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            get_calls_c.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "127.0.0.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port();
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![cached_pod]);
    substrate.budgets.readiness_quiet_reflector = Duration::from_millis(40);
    feed.mark_watch_error();

    let target = PodProxyWaitTarget::new(&pod_name, Some("uid-retire"), 1);
    let started_at = Instant::now();
    let err = substrate
        .wait_for_pod_proxy_reachable(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            target,
            Instant::now() + Duration::from_millis(400),
        )
        .await
        .expect_err("post-IP fallback retirement must fail readiness rather than binding");
    assert!(
        matches!(err, SubstrateError::Failed { .. }),
        "retired execution must fail readiness immediately with Failed, got {err:?}"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(250),
        "post-IP retirement must cancel the stalled probe immediately, took {:?}",
        started_at.elapsed()
    );
    assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    assert!(
        substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .is_none(),
        "404 fallback must have retired the active execution"
    );
    assert!(
        !substrate
            .record_confirmed(
                sandbox_id,
                (PodIp::parse("127.0.0.1")?, PodIpSource::Published),
                &target,
                None,
                Instant::now() + Duration::from_secs(1),
            )
            .await?,
        "record_confirmed must reject a Published IP once the execution row is retired"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn disappearing_settled_pod_clears_joiner_recheck_without_busy_spin()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, k8s_node_with_internal_ip, sandbox_pod_on_node,
        substrate_with_inventory_caches_and_pool,
    };
    use pplx_space_sql_locator::sql;
    use std::sync::atomic::Ordering;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 1, $2, 'space', 'creating', true)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;

    let mut initial_pod = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    initial_pod.metadata.namespace = Some("space".to_string());
    initial_pod.metadata.uid = Some("uid-joiner-1".to_string());
    initial_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.51".to_string());
    let mut replacement_pod = initial_pod.clone();
    replacement_pod.metadata.uid = Some("uid-joiner-2".to_string());
    replacement_pod
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.52".to_string());

    let substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(vec![initial_pod.clone()]);
    let quiet = Duration::from_secs(5);
    substrate.budgets.readiness_quiet_reflector = quiet;
    let recheck_armed = Arc::clone(&substrate.readiness_test_hooks.recheck_armed);
    let iterations = Arc::clone(&substrate.readiness_test_hooks.ip_wait_iterations);
    let mut armed = std::pin::pin!(recheck_armed.notified());
    armed.as_mut().enable();

    let pool_for_driver = db.pool.clone();
    let (armed_tx, armed_rx) = tokio::sync::oneshot::channel::<()>();
    let recheck_waiter = async move {
        tokio::time::timeout(Duration::from_secs(2), armed)
            .await
            .expect("joiner must arm recheck_at on initial creating pod");
        let _ = armed_tx.send(());
    };
    let driver = tokio::spawn(async move {
        armed_rx.await.expect("recheck_at armed");
        feed.delete(initial_pod);
        tokio::task::yield_now().await;
        let before_window = iterations.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(280)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let spins = iterations.load(Ordering::SeqCst) - before_window;
        assert!(
            spins <= 2,
            "stale recheck_at after pod disappearance must not busy-spin: {spins} iterations"
        );
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = 'uid-joiner-2', state = 'created'
             WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .execute(&pool_for_driver)
        .await
        .expect("record replacement pod uid");
        feed.apply(replacement_pod);
        feed
    });

    let target = PodProxyWaitTarget::new(&pod_name, None, 1);
    let mut fallback = ReadinessFallback::with_jitter(Instant::now(), quiet, 0.0);
    let (resolved, ()) = tokio::join!(
        substrate.wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            &target,
            Instant::now() + Duration::from_secs(2),
            &mut fallback,
        ),
        recheck_waiter,
    );
    let resolved = resolved?;
    let _feed = driver.await?;
    assert_eq!(
        resolved,
        Some((PodIp::parse("10.244.0.52")?, PodIpSource::Published))
    );
    Ok(())
}
