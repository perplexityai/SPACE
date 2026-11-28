use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    kube_status, pod_reply, scripted_kube_client,
};
use crate::controllers::substrate::kubernetes::tests::{
    PooledSlotPod, insert_k8s_execution, insert_test_sandbox_row, k8s_node_with_internal_ip,
    pool_claim_fixture, sandbox_pod_on_node, substrate_with_inventory_caches_and_pool,
};
use crate::controllers::substrate::{AdmittedPoolClaim, AdmittedPoolClaimLookup, SubstrateAdapter};
use hyper::http::{Method, StatusCode};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};

fn pending_pod(pod_name: &str) -> Pod {
    let mut pod = (*sandbox_pod_on_node(pod_name, "node-a", 1, "Pending")).clone();
    let status = pod.status.get_or_insert_with(Default::default);
    status.phase = Some("Pending".to_string());
    status.pod_ip = None;
    pod
}

fn running_pod_with_ip(pod_name: &str, ip: &str) -> Pod {
    let mut pod = (*sandbox_pod_on_node(pod_name, "node-a", 1, "Running")).clone();
    let status = pod.status.get_or_insert_with(Default::default);
    status.phase = Some("Running".to_string());
    status.pod_ip = Some(ip.to_string());
    pod
}

fn pooled_pod(pod_name: &str, phase: &str, pod_ip: Option<&str>, progress: Option<&str>) -> Pod {
    let mut annotations = std::collections::BTreeMap::new();
    if let Some(progress) = progress {
        annotations.insert("progress".to_string(), progress.to_string());
    }
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("space".to_string()),
            uid: Some("pool-pod-uid".to_string()),
            annotations: (!annotations.is_empty()).then_some(annotations),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::core::v1::PodSpec {
            node_name: Some("in-set-worker".to_string()),
            ..Default::default()
        }),
        status: Some(k8s_openapi::api::core::v1::PodStatus {
            phase: Some(phase.to_string()),
            pod_ip: pod_ip.map(str::to_owned),
            ..Default::default()
        }),
    }
}

/// While the shared pod reflector is healthy, an unsettled (`Pending`) pod in
/// the cache is returned directly without an apiserver `GET`. Once the
/// reflector watch faults, the same unsettled cache entry falls back to a
/// single apiserver `GET`.
#[tokio::test(flavor = "current_thread")]
async fn read_sandbox_pod_trusts_pending_cache_entry_when_reflector_is_healthy() -> TestResult {
    let pod_name = "space-sbx-pending-g1";
    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            reply_gets.fetch_add(1, Ordering::SeqCst);
            pod_reply(&running_pod_with_ip(pod_name, "10.0.0.42"))
        } else {
            kube_status(StatusCode::BAD_REQUEST, "BadRequest")
        }
    });

    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?.port()
    };
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy(&format!("postgres://unused@127.0.0.1:{closed_port}/unused"))
        .expect("lazy pool builds without connecting");
    let mut substrate = substrate_with_inventory_caches_and_pool(Vec::new(), Vec::new(), pool);
    substrate.k8s.client = client;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(Vec::new());
    substrate.budgets.reflector_wait = Duration::from_millis(10);
    feed.apply(pending_pod(pod_name));

    let cached = substrate
        .read_sandbox_pod(pod_name)
        .await?
        .expect("pending pod is cached");
    assert_eq!(cached.pod_ip(), None);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "healthy reflector must not issue an uncached GET for a cached Pending pod"
    );

    feed.mark_watch_error();
    let fetched = substrate
        .read_sandbox_pod(pod_name)
        .await?
        .expect("degraded reflector falls back to apiserver GET");
    assert_eq!(fetched.pod_ip(), Some("10.0.0.42"));
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "degraded reflector issues exactly one fallback GET"
    );
    Ok(())
}

/// When an execution's pod is absent from both the reflector cache and the
/// apiserver, `resolve_pod_instance` confirms absence with a single uncached
/// `GET` rather than issuing a second back-to-back `GET` on the `None` branch.
#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_instance_confirms_missing_pod_with_single_uncached_get() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        &pod_name,
        "pod-uid-1",
        Some("10.0.0.9"),
        true,
    )
    .await?;

    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            reply_gets.fetch_add(1, Ordering::SeqCst);
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        } else {
            kube_status(StatusCode::BAD_REQUEST, "BadRequest")
        }
    });

    let mut substrate =
        substrate_with_inventory_caches_and_pool(Vec::new(), Vec::new(), db.pool.clone());
    substrate.k8s.client = client;
    let (mut substrate, _feed) = substrate.with_pod_cache_feed(Vec::new());
    substrate.budgets.reflector_wait = Duration::from_millis(10);

    let resolved = substrate.resolve_pod_instance(sandbox_id).await?;
    assert!(resolved.is_none());
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "missing pod resolution must verify 404 once, not re-GET on the None arm"
    );
    Ok(())
}

/// When the cluster controller promotes a warm-pool slot to `ready` slightly
/// before this API replica's reflector applies the `Pending` -> `Running`
/// watch event, `claim_pooled_execution` waits on `pod_events` for the running
/// snapshot rather than invalidating the healthy slot, while still avoiding
/// any uncached apiserver `GET` under a healthy reflector.
#[tokio::test(flavor = "current_thread")]
async fn claim_pooled_execution_waits_for_lagging_reflector_without_uncached_get() -> TestResult {
    let mut fixture = pool_claim_fixture(PooledSlotPod {
        phase: "Pending",
        node_name: Some("in-set-worker"),
        nodes: &[("in-set-worker", Some("workers"))],
    })
    .await?;

    let pod_name = "space-sbx-pooled-g1";
    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            reply_gets.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });
    fixture.substrate.k8s.client = client;
    fixture.substrate.budgets.reflector_wait = Duration::from_millis(200);

    let (substrate, mut feed) = fixture.substrate.with_pod_cache_feed(Vec::new());
    fixture.substrate = substrate;
    let mut pre_bind_unschedulable = pooled_pod(pod_name, "Pending", None, None);
    pre_bind_unschedulable
        .status
        .get_or_insert_with(Default::default)
        .conditions = Some(vec![k8s_openapi::api::core::v1::PodCondition {
        type_: "PodScheduled".to_string(),
        status: "False".to_string(),
        reason: Some("Unschedulable".to_string()),
        message: Some("0/1 nodes available: scale-up in progress".to_string()),
        ..Default::default()
    }]);
    feed.apply(pre_bind_unschedulable.clone());

    let deliver = async move {
        feed.wait_for_subscriber().await;
        // Intermediate Pending update (including pre-bind Unschedulable) before
        // Running must not cut the wait short or invalidate the ready slot.
        feed.apply(pooled_pod(pod_name, "Pending", None, Some("1")));
        tokio::task::yield_now().await;
        feed.apply(pooled_pod(pod_name, "Running", Some("10.90.0.7"), None));
        feed
    };

    let (attempt, mut feed) = {
        let request = fixture.request();
        tokio::join!(fixture.substrate.claim_pooled_execution(&request), deliver,)
    };
    let attempt = attempt?;
    let claimed = attempt
        .claim
        .expect("inter-reflector lag from Pending to Running must adopt the ready slot instead of invalidating it");
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "healthy reflector catch-up must not issue an uncached apiserver GET"
    );

    // If the reflector still shows `Pending` (including a lagging pre-bind
    // `Unschedulable` condition from before the slot reached `ready`) past
    // `reflector_wait` while healthy, an admitted lookup keeps the claim
    // (`Transient`) rather than invalidating the slot, and a fresh claim
    // releases the slot back to `ready` rather than invalidating it — both
    // without uncached `GET`s.
    fixture.substrate.budgets.reflector_wait = Duration::from_millis(15);
    let mut lagging_unschedulable = pre_bind_unschedulable;
    lagging_unschedulable.metadata.resource_version = Some("2".to_string());
    feed.apply(lagging_unschedulable);

    let request = fixture.request();
    let admitted_lookup = fixture
        .substrate
        .lookup_admitted_pooled_execution(
            &request,
            AdmittedPoolClaim {
                claim_id: claimed.claim_id,
                pool_slot_id: fixture.slot_id,
            },
            claimed.sandbox_id,
        )
        .await?;
    assert!(
        matches!(admitted_lookup, AdmittedPoolClaimLookup::Transient),
        "lagging healthy reflector on an admitted slot must report Transient so retry adopts the same slot"
    );
    assert_eq!(
        fixture.slot_disposition().await?,
        ("claimed".to_string(), Some(claimed.claim_id)),
        "admitted lookup must keep its claim rather than invalidating the warm slot"
    );

    // Release the claim back to `ready` and verify `claim_pooled_execution`
    // also releases (never invalidates) when `Pending` outlasts `reflector_wait`.
    crate::controllers::substrate::kubernetes::pool::release_claimed_pool_slot(
        &fixture.db.pool,
        &claimed,
    )
    .await?;
    let over_budget_attempt = fixture.substrate.claim_pooled_execution(&request).await?;
    assert!(
        over_budget_attempt.claim.is_none(),
        "claim_pooled_execution must fall back to cold when Pending outlasts reflector_wait"
    );
    assert_eq!(
        fixture.slot_disposition().await?,
        ("ready".to_string(), None),
        "claim_pooled_execution must release the warm slot back to ready rather than invalidating it"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "healthy reflector must still avoid uncached GETs when Pending outlasts reflector_wait"
    );

    // Degraded fallback returning a transient 5xx on an admitted lookup must
    // also preserve the claim (`Transient`) rather than invalidating the slot.
    feed.apply(pooled_pod(pod_name, "Running", Some("10.90.0.7"), None));
    let reclaimed = fixture
        .substrate
        .claim_pooled_execution(&request)
        .await?
        .claim
        .expect("running slot reclaims cleanly");
    gets.store(0, Ordering::SeqCst);
    feed.apply(pooled_pod(pod_name, "Pending", None, Some("3")));
    feed.mark_watch_error();
    let degraded_transient = fixture
        .substrate
        .lookup_admitted_pooled_execution(
            &request,
            AdmittedPoolClaim {
                claim_id: reclaimed.claim_id,
                pool_slot_id: fixture.slot_id,
            },
            reclaimed.sandbox_id,
        )
        .await?;
    assert!(
        matches!(degraded_transient, AdmittedPoolClaimLookup::Transient),
        "degraded transient 5xx must keep the admitted claim for retry"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "degraded lookup must issue one fallback GET that receives the scripted 5xx"
    );
    assert_eq!(
        fixture.slot_disposition().await?,
        ("claimed".to_string(), Some(reclaimed.claim_id)),
        "transient apiserver read failure must not invalidate the warm slot"
    );

    // Conversely, an `Unknown` cached snapshot on a healthy reflector is
    // settled-unusable and invalidates the slot without issuing an uncached `GET`.
    gets.store(0, Ordering::SeqCst);
    feed.apply(pooled_pod(pod_name, "Unknown", None, Some("4")));
    let unknown_lookup = fixture
        .substrate
        .lookup_admitted_pooled_execution(
            &request,
            AdmittedPoolClaim {
                claim_id: reclaimed.claim_id,
                pool_slot_id: fixture.slot_id,
            },
            reclaimed.sandbox_id,
        )
        .await?;
    assert!(
        matches!(unknown_lookup, AdmittedPoolClaimLookup::ReleasePending),
        "Unknown phase is settled-unusable and must invalidate the slot"
    );
    assert_eq!(
        fixture.slot_disposition().await?,
        ("claimed".to_string(), None),
        "unusable Unknown warm-pool pod must clear claim_id so the stale-claimed sweeper reaps it"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "Unknown phase is settled in cache and needs no uncached GET"
    );
    Ok(())
}

/// After a same-name pod replacement updates `sandbox_k8s_executions.pod_uid`
/// to the replacement UID, a lagging reflector snapshot that still holds a
/// settled (`Unschedulable`) predecessor UID must not trigger an uncached
/// `GET` on either `resolve_pod_ip_for_readiness` or `resolve_pod_instance`,
/// nor overwrite `row.pod_uid` back to the deleted predecessor; both resolve
/// cleanly from `pod_events` once the replacement UID arrives.
#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_instance_ignores_lagging_predecessor_uid_after_same_name_replacement()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_k8s_execution(&db.pool, sandbox_id, 1, &pod_name, "pod-uid-2", None, true).await?;

    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET && request.path == expected_path {
            reply_gets.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        Vec::new(),
        vec![k8s_node_with_internal_ip("node-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(Vec::new());
    substrate.budgets.reflector_wait = Duration::from_millis(200);

    let mut predecessor = pending_pod(&pod_name);
    predecessor.metadata.uid = Some("pod-uid-1".to_string());
    predecessor
        .status
        .get_or_insert_with(Default::default)
        .conditions = Some(vec![k8s_openapi::api::core::v1::PodCondition {
        type_: "PodScheduled".to_string(),
        status: "False".to_string(),
        reason: Some("Unschedulable".to_string()),
        message: Some("0/1 nodes available".to_string()),
        ..Default::default()
    }]);
    feed.apply(predecessor.clone());

    let readiness = substrate
        .resolve_pod_ip_for_readiness(
            sandbox_id,
            &PodProxyWaitTarget::new(&pod_name, None, 1),
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    assert!(
        readiness.resolved.is_none(),
        "stale predecessor UID in a healthy reflector must yield None without failing Unschedulable"
    );
    assert!(
        !readiness.cache_matches_execution && !readiness.used_uncached_get,
        "lagging predecessor UID is marked non-authoritative without an uncached GET"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "readiness lookup with lagging predecessor UID on a healthy reflector must not issue an uncached GET"
    );

    let mut replacement = running_pod_with_ip(&pod_name, "10.0.0.22");
    replacement.metadata.uid = Some("pod-uid-2".to_string());

    let deliver = async move {
        feed.wait_for_subscriber().await;
        feed.delete(predecessor);
        tokio::task::yield_now().await;
        feed.apply(replacement);
    };

    let (resolved, ()) = tokio::join!(substrate.resolve_pod_instance(sandbox_id), deliver);
    let resolved = resolved?.expect("replacement pod resolves once reflector catches up");
    assert_eq!(
        resolved.pod_uid.as_ref().map(PodUid::as_str),
        Some("pod-uid-2")
    );
    assert_eq!(resolved.ip, PodIp::parse("10.0.0.22")?);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "reflector catch-up to replacement UID must not issue an uncached GET"
    );

    let row = substrate
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("active execution row remains persisted");
    assert_eq!(row.pod_uid.as_ref().map(PodUid::as_str), Some("pod-uid-2"));
    Ok(())
}

/// Started-sandbox recovery keeps `sandbox_k8s_executions.state = 'recovering'`
/// across `ensure_pod_exists` and proxy-reachability readiness so external
/// route lookups (`resolve_pod_ip`) stay fenced off, while
/// `resolve_pod_ip_for_readiness` and `record_confirmed_pod_ip` both bind the
/// recovery pod's IP without promoting `state` out of `recovering`.
#[tokio::test(flavor = "current_thread")]
async fn recovering_execution_resolves_pod_ip_for_readiness_while_staying_hidden_externally()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
         VALUES ($1, 1, $2, 'space', 'recovering-uid', 'recovering', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;

    let gets = Arc::new(AtomicUsize::new(0));
    let reply_gets = Arc::clone(&gets);
    let (client, _log) = scripted_kube_client(move |request| {
        if request.method == Method::GET {
            reply_gets.fetch_add(1, Ordering::SeqCst);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        Vec::new(),
        vec![k8s_node_with_internal_ip("node-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (substrate, mut feed) = substrate.with_pod_cache_feed(Vec::new());

    let node_ip = PodIp::parse("10.0.0.54")?;
    assert!(
        substrate
            .store
            .record_confirmed_pod_ip(sandbox_id, 1, "recovering-uid", node_ip)
            .await?
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_ip = NULL WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut pod = running_pod_with_ip(&pod_name, "10.0.0.55");
    pod.metadata.uid = Some("recovering-uid".to_string());
    feed.apply(pod);

    assert_eq!(
        substrate.resolve_pod_ip(sandbox_id).await?,
        None,
        "external route lookups must continue hiding in-progress recovering executions"
    );

    let resolved = substrate
        .resolve_pod_ip_for_readiness(
            sandbox_id,
            &PodProxyWaitTarget::new(&pod_name, Some("recovering-uid"), 1),
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    assert_eq!(
        resolved.resolved,
        Some((PodIp::parse("10.0.0.55")?, PodIpSource::Published))
    );
    assert_eq!(gets.load(Ordering::SeqCst), 0);

    let row = substrate
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("recovering execution row remains active");
    assert_eq!(row.state, ActiveExecutionState::Recovering);
    assert_eq!(row.pod_ip.as_deref(), Some("10.0.0.55"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unsettled_pod_resolves_recorded_node_ip_and_fences_predecessor_creating_view()
-> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::wait_ready::ReadinessFallback;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // Part 1: While `state = 'creating'` and `pod_uid IS NULL`, a UID-unpinned
    // joiner must neither persist a predecessor pod's node placement nor
    // deadlock on a cached `Failed` predecessor when the reflector watch faults.
    let sandbox_creating = SandboxId::new();
    let pod_creating = format!("space-sbx-{sandbox_creating}-g2");
    insert_test_sandbox_row(&db.pool, sandbox_creating, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 2, $2, 'space', 'creating', true)"
    ))
    .bind(sandbox_creating)
    .bind(&pod_creating)
    .execute(&db.pool)
    .await?;

    let mut running_predecessor =
        (*sandbox_pod_on_node(&pod_creating, "gone-node", 1, "Running")).clone();
    running_predecessor.metadata.namespace = Some("space".to_string());
    running_predecessor.metadata.uid = Some("uid-old".to_string());
    running_predecessor
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.11".to_string());
    let mut failed_predecessor = running_predecessor.clone();
    failed_predecessor
        .status
        .get_or_insert_with(Default::default)
        .phase = Some("Failed".to_string());

    let mut replacement = (*sandbox_pod_on_node(&pod_creating, "worker-b", 2, "Running")).clone();
    replacement.metadata.namespace = Some("space".to_string());
    replacement.metadata.uid = Some("uid-new".to_string());
    replacement
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.99".to_string());
    let replacement_for_get = replacement.clone();
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_creating}");

    let (client, _log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == expected_path {
            return pod_reply(&replacement_for_get);
        }
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });
    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![
            k8s_node_with_internal_ip("gone-node", "10.0.1.9"),
            k8s_node_with_internal_ip("worker-b", "10.0.1.2"),
        ],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (mut substrate, mut feed) =
        substrate.with_pod_cache_feed(vec![running_predecessor.clone()]);
    let quiet = Duration::from_millis(80);
    substrate.budgets.readiness_quiet_reflector = quiet;

    let joiner_target = PodProxyWaitTarget::new(&pod_creating, None, 2);
    let deadline = Instant::now() + Duration::from_secs(2);

    // Deterministically verify at the owning resolver layer that neither a
    // `Running` nor a `Failed` predecessor pod persists `gone-node` placement
    // while `state = 'creating'` and `pod_uid IS NULL`.
    for predecessor in [&running_predecessor, &failed_predecessor] {
        feed.apply(predecessor.clone());
        let check = substrate
            .inspect_active_execution_for_pod(
                sandbox_creating,
                &joiner_target,
                &SandboxPodView::from(predecessor.clone()),
            )
            .await?;
        assert!(
            check.placement_fence_uid.is_none(),
            "UID-unpinned joiner must not report a matching recorded UID while pod_uid IS NULL"
        );
        let pre_uid = substrate
            .resolve_pod_ip_for_readiness(sandbox_creating, &joiner_target, deadline)
            .await?;
        assert!(pre_uid.resolved.is_none());
        assert!(pre_uid.execution_in_flight);
        let recorded_node: Option<uuid::Uuid> =
            sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id = $1"))
                .bind(sandbox_creating.as_uuid())
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(
            recorded_node, None,
            "UID-unpinned joiner must not persist predecessor node placement while pod_uid IS NULL"
        );
    }

    // With `failed_predecessor` still cached and the reflector faulted, once the
    // owner records `uid-new`, `wait_for_pod_ip_assignment` must not deadlock on
    // the cached `Failed` predecessor and must resolve the replacement IP.
    feed.mark_watch_error();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET pod_uid = 'uid-new', state = 'created'
         WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox_creating)
    .execute(&db.pool)
    .await?;

    let mut fallback = ReadinessFallback::with_jitter(Instant::now() - quiet, quiet, 0.0);
    let resolved = substrate
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_creating,
            &joiner_target,
            Instant::now() + Duration::from_secs(1),
            &mut fallback,
        )
        .await?;
    assert_eq!(
        resolved,
        Some((PodIp::parse("10.244.0.99")?, PodIpSource::Published))
    );

    // Part 2: An unsettled `Pending` pod (`status.podIP = None`) resolves an
    // already-confirmed `NodeReported` `row.pod_ip` immediately on a pinned wait,
    // and at the start-wait recheck on a UID-unpinned joiner, with zero GETs.
    let sandbox_node_ip = SandboxId::new();
    let pod_node_ip = format!("space-sbx-{sandbox_node_ip}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_node_ip, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-node-ip', '10.244.0.77', 'created', true)"
    ))
    .bind(sandbox_node_ip)
    .bind(&pod_node_ip)
    .execute(&db.pool)
    .await?;

    let mut pending_pod = (*sandbox_pod_on_node(&pod_node_ip, "worker-b", 1, "Pending")).clone();
    pending_pod.metadata.namespace = Some("space".to_string());
    pending_pod.metadata.uid = Some("uid-node-ip".to_string());

    let gets = Arc::new(AtomicUsize::new(0));
    let gets_for_client = Arc::clone(&gets);
    let (zero_get_client, _) = scripted_kube_client(move |_req| {
        gets_for_client.fetch_add(1, Ordering::SeqCst);
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedGet")
    });
    let mut substrate_node = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-b", "10.0.1.2")],
        db.pool.clone(),
    );
    substrate_node.k8s.client = zero_get_client;
    let (substrate_node, _feed_node) = substrate_node.with_pod_cache_feed(vec![pending_pod]);

    let mut pinned_fallback =
        ReadinessFallback::with_jitter(Instant::now(), Duration::from_secs(5), 0.0);
    let pinned = substrate_node
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_node_ip,
            &PodProxyWaitTarget::new(&pod_node_ip, Some("uid-node-ip"), 1),
            Instant::now() + Duration::from_millis(300),
            &mut pinned_fallback,
        )
        .await?;
    assert_eq!(
        pinned,
        Some((PodIp::parse("10.244.0.77")?, PodIpSource::NodeReported))
    );

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_ip = NULL WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_node_ip)
    .execute(&db.pool)
    .await?;
    let recheck_armed = Arc::clone(&substrate_node.readiness_test_hooks.recheck_armed);
    let mut armed = std::pin::pin!(recheck_armed.notified());
    armed.as_mut().enable();
    let pool_for_joiner = db.pool.clone();
    let mut joiner_fallback =
        ReadinessFallback::with_jitter(Instant::now(), Duration::from_secs(5), 0.0);
    let joiner_node_target = PodProxyWaitTarget::new(&pod_node_ip, None, 1);
    let (joined, ()) = tokio::join!(
        substrate_node.wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
            sandbox_node_ip,
            &joiner_node_target,
            Instant::now() + Duration::from_secs(2),
            &mut joiner_fallback,
        ),
        async {
            tokio::time::timeout(Duration::from_secs(2), armed)
                .await
                .expect("joiner must arm recheck timer before owner records node IP");
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                 SET pod_ip = '10.244.0.78'
                 WHERE sandbox_id = $1 AND generation = 1"
            ))
            .bind(sandbox_node_ip)
            .execute(&pool_for_joiner)
            .await
            .expect("record owner-confirmed node IP");
        }
    );
    let joined = joined?;
    assert_eq!(
        joined,
        Some((PodIp::parse("10.244.0.78")?, PodIpSource::NodeReported))
    );
    assert_eq!(gets.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reachable_pod_ip_and_joined_create_fall_back_past_unsettled_or_unscheduled_cache()
-> pplx_space_test_env::TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "running").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-reachable', '10.244.0.88', 'created', true)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;

    let mut predecessor_cached =
        (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone();
    predecessor_cached.metadata.namespace = Some("space".to_string());
    predecessor_cached.metadata.uid = Some("uid-stale".to_string());
    predecessor_cached
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.10".to_string());

    let mut settled_apiserver = running_pod_with_ip(&pod_name, "10.244.0.88");
    settled_apiserver.metadata.uid = Some("uid-reachable".to_string());
    settled_apiserver
        .spec
        .get_or_insert_with(Default::default)
        .node_name = Some("worker-a".to_string());
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let gets = Arc::new(AtomicUsize::new(0));
    let gets_for_client = Arc::clone(&gets);
    let (client, _log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == expected_path {
            gets_for_client.fetch_add(1, Ordering::SeqCst);
            return pod_reply(&settled_apiserver);
        }
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("worker-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.budgets.reflector_wait = Duration::from_millis(20);
    let (substrate, mut feed) = substrate.with_pod_cache_feed(vec![predecessor_cached]);
    feed.mark_watch_error();
    substrate
        .remember_pod_node_placement(
            sandbox_id,
            &SandboxPodView::from(
                (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Running")).clone(),
            ),
            PlacementRetryDisposition::RetryDuplicatesExecution,
        )
        .await?;

    let resolved = substrate.resolve_reachable_pod_ip(sandbox_id).await?;
    assert_eq!(resolved, Some(PodIp::parse("10.244.0.88")?));
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "degraded predecessor cache entry must fall back to apiserver GET"
    );

    // And when `row.pod_ip` is still NULL, `resolve_reachable_pod_ip` returns
    // `None` immediately without issuing a wasted apiserver GET.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_ip = NULL WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    assert_eq!(substrate.resolve_reachable_pod_ip(sandbox_id).await?, None);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "unassigned row.pod_ip must short-circuit without an apiserver GET"
    );
    Ok(())
}

/// Cache-mode `resolve_pod_ip_for_readiness` must stay strictly cache-only when
/// the reflector is degraded and `pod_store` holds an unsettled `Pending` pod
/// (leaving degraded `GET`s to `resolve_pod_ip_for_readiness_uncached`), and
/// terminal confirmation `GET`s must reject a replacement pod with a different
/// UID rather than adopting its placement or IP for an execution still naming
/// the old UID.
#[tokio::test(flavor = "current_thread")]
async fn readiness_cache_mode_skips_unpaced_degraded_gets_and_fences_terminal_confirm_uid()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{sandbox_id}-g1");
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, &pod_name, "uid-a", None, true).await?;

    let mut pending_a = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Pending")).clone();
    pending_a.metadata.namespace = Some("space".to_string());
    pending_a.metadata.uid = Some("uid-a".to_string());
    pending_a.status.get_or_insert_with(Default::default).pod_ip = None;

    let mut failed_a = pending_a.clone();
    failed_a.status.get_or_insert_with(Default::default).phase = Some("Failed".to_string());

    let mut replacement_b = (*sandbox_pod_on_node(&pod_name, "worker-b", 1, "Running")).clone();
    replacement_b.metadata.namespace = Some("space".to_string());
    replacement_b.metadata.uid = Some("uid-b".to_string());
    replacement_b
        .status
        .get_or_insert_with(Default::default)
        .pod_ip = Some("10.244.0.88".to_string());
    let replacement_for_get = replacement_b.clone();

    let gets = Arc::new(AtomicUsize::new(0));
    let gets_for_client = Arc::clone(&gets);
    let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
    let (client, _log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == expected_path {
            gets_for_client.fetch_add(1, Ordering::SeqCst);
            return pod_reply(&replacement_for_get);
        }
        kube_status(StatusCode::NOT_FOUND, "NotFound")
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![
            k8s_node_with_internal_ip("worker-a", "10.0.1.1"),
            k8s_node_with_internal_ip("worker-b", "10.0.1.2"),
        ],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (substrate, mut feed) = substrate.with_pod_cache_feed(vec![pending_a]);
    feed.mark_watch_error();

    let pinned_target = PodProxyWaitTarget::new(&pod_name, Some("uid-a"), 1);
    let unpinned_target = PodProxyWaitTarget::new(&pod_name, None, 1);
    let deadline = Instant::now() + Duration::from_secs(2);

    // 1. Degraded reflector + unsettled `Pending` pod in cache: cache-mode
    // `resolve_pod_ip_for_readiness` must issue 0 uncached GETs.
    let cache_only = substrate
        .resolve_pod_ip_for_readiness(sandbox_id, &pinned_target, deadline)
        .await?;
    assert!(cache_only.resolved.is_none());
    assert!(!cache_only.used_uncached_get);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "cache-mode readiness resolve must not bypass ReadinessFallback pacing"
    );

    // 2. Cached terminal `uid-a` + confirmation GET returning `Running` `uid-b`:
    // both pinned and unpinned readiness resolves must reject `uid-b` without
    // persisting `worker-b` placement or `uid-b` metadata while `row.pod_uid` is `uid-a`.
    feed.apply(failed_a);
    for target in [&pinned_target, &unpinned_target] {
        let outcome = substrate
            .resolve_pod_ip_for_readiness(sandbox_id, target, deadline)
            .await?;
        assert!(outcome.resolved.is_none());
        assert!(outcome.used_uncached_get);
        assert!(!outcome.cache_matches_execution);
    }
    assert_eq!(gets.load(Ordering::SeqCst), 2);
    let (recorded_uid, recorded_ip): (Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT pod_uid, host(pod_ip) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(recorded_uid.as_deref(), Some("uid-a"));
    assert_eq!(recorded_ip, None);
    let recorded_node: Option<uuid::Uuid> =
        sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id.as_uuid())
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(recorded_node, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_fences_stale_restore_gets_absent_retirement_and_reset_creating_rows()
-> TestResult {
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::scripted_kube_client_async;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let deadline = Instant::now() + Duration::from_secs(2);

    // 1. Expired `restoring` + `creating` row: cache-mode `resolve_pod_ip_for_readiness`
    // must NOT fire an uncached `GET` (preserving `ReadinessFallback` pacing), while
    // `resolve_pod_ip_for_readiness_uncached` verifies once and fences retirement on
    // `observed_pod_uid`.
    let restore_sbx = SandboxId::new();
    let restore_pod = format!("space-sbx-{restore_sbx}-g1");
    insert_test_sandbox_row(&db.pool, restore_sbx, "restoring").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active, updated_at)
         VALUES ($1, 1, $2, 'space', 'creating', true, NOW() - INTERVAL '10 minutes')"
    ))
    .bind(restore_sbx)
    .bind(&restore_pod)
    .execute(&db.pool)
    .await?;

    let mut cached_restore_pod = running_pod_with_ip(&restore_pod, "10.244.0.91");
    cached_restore_pod.metadata.namespace = Some("space".to_string());
    cached_restore_pod.metadata.uid = Some("uid-stale-restore".to_string());

    let gets = Arc::new(AtomicUsize::new(0));
    let gets_for_client = Arc::clone(&gets);
    let pool_for_client = db.pool.clone();
    let restore_path = format!("/api/v1/namespaces/space/pods/{restore_pod}");
    let (client, _) = scripted_kube_client_async(move |req| {
        let gets_for_client = Arc::clone(&gets_for_client);
        let pool_for_client = pool_for_client.clone();
        let is_restore = req.method == Method::GET && req.path == restore_path;
        async move {
            let call = gets_for_client.fetch_add(1, Ordering::SeqCst);
            if is_restore && call == 0 {
                // Concurrent materializer records `uid-live` while the 404 GET is in flight:
                // observed `pod_uid = None` must NOT retire `uid-live`.
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET pod_uid = 'uid-live', state = 'created', pod_materialized_at = NOW()
                      WHERE sandbox_id = $1 AND generation = 1"
                ))
                .bind(restore_sbx)
                .execute(&pool_for_client)
                .await
                .expect("record concurrent uid-live");
            } else if is_restore && call == 1 {
                // Concurrent waiter already retired the row while the second 404 GET ran:
                // `terminal_or_absent_retired` must still report `true`.
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET active = FALSE, state = 'forgotten'
                      WHERE sandbox_id = $1 AND generation = 1"
                ))
                .bind(restore_sbx)
                .execute(&pool_for_client)
                .await
                .expect("retire row concurrently");
            }
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
    });

    let mut substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("node-a", "10.0.1.1")],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    let (substrate, _feed) = substrate.with_pod_cache_feed(vec![cached_restore_pod]);

    let joiner_target = PodProxyWaitTarget::new(&restore_pod, None, 1);
    let cached = substrate
        .resolve_pod_ip_for_readiness(restore_sbx, &joiner_target, deadline)
        .await?;
    assert!(cached.resolved.is_none());
    assert!(!cached.used_uncached_get);
    assert!(cached.execution_in_flight);
    assert_eq!(gets.load(Ordering::SeqCst), 0);

    let uncached_raced = substrate
        .resolve_pod_ip_for_readiness_uncached(restore_sbx, &joiner_target, deadline)
        .await?;
    assert!(uncached_raced.used_uncached_get);
    assert!(
        !uncached_raced.terminal_or_absent_retired,
        "404 on observed pod_uid=None must not retire or claim retired once uid-live was recorded"
    );
    let active_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(restore_sbx)
    .fetch_optional(&db.pool)
    .await?
    .flatten();
    assert_eq!(active_uid.as_deref(), Some("uid-live"));

    let uncached_already_retired = substrate
        .resolve_pod_ip_for_readiness_uncached(restore_sbx, &joiner_target, deadline)
        .await?;
    assert!(
        uncached_already_retired.terminal_or_absent_retired,
        "concurrently retired generation must still surface terminal_or_absent_retired"
    );

    // 2. Stale UID-pinned waiter after `claim_pod_materialization` resets a previously
    // materialized row (`pod_materialized_at IS NOT NULL`, `state = 'creating'`,
    // `pod_uid = NULL`): must NOT repair `uid-stale` or persist placement/IP.
    let reset_sbx = SandboxId::new();
    let reset_pod_name = format!("space-sbx-{reset_sbx}-g1");
    insert_test_sandbox_row(&db.pool, reset_sbx, "started").await?;
    insert_k8s_execution(
        &db.pool,
        reset_sbx,
        1,
        &reset_pod_name,
        "uid-stale",
        Some("10.244.0.10"),
        true,
    )
    .await?;
    let mut tx = db.pool.begin().await?;
    assert!(
        pplx_space_sandbox_store::k8s_executions::pod_placement::claim_pod_materialization(
            &mut *tx, reset_sbx, 1,
        )
        .await?
    );
    tx.commit().await?;

    let mut stale_running = running_pod_with_ip(&reset_pod_name, "10.244.0.10");
    stale_running.metadata.namespace = Some("space".to_string());
    stale_running.metadata.uid = Some("uid-stale".to_string());
    let stale_for_get = stale_running.clone();
    let reset_path = format!("/api/v1/namespaces/space/pods/{reset_pod_name}");
    let (reset_client, _reset_log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == reset_path {
            return pod_reply(&stale_for_get);
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedResetRequest")
    });
    let mut reset_substrate = substrate_with_inventory_caches_and_pool(
        vec![],
        vec![k8s_node_with_internal_ip("node-a", "10.0.1.1")],
        db.pool.clone(),
    );
    reset_substrate.k8s.client = reset_client;
    let (reset_substrate, _reset_feed) = reset_substrate.with_pod_cache_feed(vec![stale_running]);

    let stale_outcome = reset_substrate
        .resolve_pod_ip_for_readiness(
            reset_sbx,
            &PodProxyWaitTarget::new(&reset_pod_name, Some("uid-stale"), 1),
            deadline,
        )
        .await?;
    assert!(stale_outcome.resolved.is_none());
    assert!(!stale_outcome.execution_in_flight);

    // Even when `sandboxes.lifecycle = 'restoring'` and `updated_at` has aged
    // past the stale-restore threshold while `pod_materialized_at IS NOT NULL`,
    // an uncached readiness check for an unpinned joiner must NOT adopt the
    // lingering former pod `uid-stale` into the reset `creating` row.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(reset_sbx)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET updated_at = NOW() - INTERVAL '10 minutes',
             activation_attempt_expires_at = NOW() - INTERVAL '1 minute'
         WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(reset_sbx)
    .execute(&db.pool)
    .await?;
    let joiner_reset_outcome = reset_substrate
        .resolve_pod_ip_for_readiness_uncached(
            reset_sbx,
            &PodProxyWaitTarget::new(&reset_pod_name, None, 1),
            deadline,
        )
        .await?;
    assert!(
        joiner_reset_outcome.resolved.is_none(),
        "reset creating row with pod_materialized_at set must never adopt a lingering former pod"
    );
    assert!(joiner_reset_outcome.execution_in_flight);
    let (state, uid, ip): (String, Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT state, pod_uid, host(pod_ip) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(reset_sbx)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(state, "creating");
    assert_eq!(uid, None);
    assert_eq!(ip, None);

    // 3. Abandoned stale `creating` restore row (`pod_materialized_at IS NULL`,
    // `pod_uid IS NULL`, expired attempt lease, apiserver returns 404) waited on
    // by an unpinned joiner while the shared reflector is healthy retires the
    // wedged row promptly instead of stalling until `deadline`.
    let abandoned_sbx = SandboxId::new();
    let abandoned_pod = format!("space-sbx-{abandoned_sbx}-g1");
    insert_test_sandbox_row(&db.pool, abandoned_sbx, "creating").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(abandoned_sbx)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active,
              created_at, updated_at, activation_attempt_id, activation_attempt_expires_at)
         VALUES ($1, 1, $2, 'space', 'creating', true,
                 NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes',
                 $3, NOW() - INTERVAL '1 minute')"
    ))
    .bind(abandoned_sbx)
    .bind(&abandoned_pod)
    .bind(uuid::Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let abandoned_path = format!("/api/v1/namespaces/space/pods/{abandoned_pod}");
    let (abandoned_client, _abandoned_log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == abandoned_path {
            return kube_status(StatusCode::NOT_FOUND, "NotFound");
        }
        kube_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            "UnexpectedAbandonedRequest",
        )
    });
    let mut abandoned_substrate =
        substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    abandoned_substrate.k8s.client = abandoned_client;
    abandoned_substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let (abandoned_substrate, _abandoned_feed) = abandoned_substrate.with_pod_cache_feed(vec![]);
    let mut healthy_fallback =
        crate::controllers::substrate::kubernetes::wait_ready::ReadinessFallback::with_jitter(
            Instant::now(),
            Duration::from_secs(60),
            0.0,
        );
    let started_abandoned = Instant::now();
    let abandoned_err = abandoned_substrate
        .wait_for_pod_ip_assignment(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            abandoned_sbx,
            &PodProxyWaitTarget::new(&abandoned_pod, None, 1),
            Instant::now() + Duration::from_secs(5),
            &mut healthy_fallback,
        )
        .await
        .expect_err("abandoned stale restore creating row must retire and fail fast");
    assert!(matches!(abandoned_err, SubstrateError::Failed { .. }));
    assert!(
        started_abandoned.elapsed() < Duration::from_secs(1),
        "healthy-reflector joiner must retire abandoned restore row promptly"
    );
    assert!(
        abandoned_substrate
            .store
            .active_execution_status(abandoned_sbx)
            .await?
            .is_none()
    );
    Ok(())
}

/// Terminal pod cleanup in `resolve_pod_ip_for_readiness` carries UID
/// preconditions on `DELETE` and fences `forget_pod_tracking` against the
/// observed `activation_attempt_id` so a concurrent same-generation redrive's
/// replacement pod and DB row are never torn down.
#[tokio::test(flavor = "current_thread")]
async fn terminal_cleanup_fences_delete_uid_and_attempt_against_concurrent_redrive()
-> pplx_space_test_env::TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let deadline = Instant::now() + Duration::from_secs(2);

    // Case A: DELETE carries `Preconditions.uid = "uid-a"` and receives `409 Conflict`
    // after a concurrent same-generation redrive recreated `pod_name` with `uid-b`.
    let conflict_sbx = SandboxId::new();
    let conflict_pod = format!("space-sbx-{conflict_sbx}-g1");
    insert_test_sandbox_row(&db.pool, conflict_sbx, "creating").await?;
    let attempt_a = uuid::Uuid::now_v7();
    let attempt_b = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
              activation_attempt_id, activation_attempt_expires_at)
         VALUES ($1, 1, $2, 'space', 'uid-a', 'created', true, $3, NOW() - INTERVAL '1 minute')"
    ))
    .bind(conflict_sbx)
    .bind(&conflict_pod)
    .bind(attempt_a)
    .execute(&db.pool)
    .await?;

    let mut failed_a = running_pod_with_ip(&conflict_pod, "10.244.0.50");
    failed_a.metadata.namespace = Some("space".to_string());
    failed_a.metadata.uid = Some("uid-a".to_string());
    failed_a.status.as_mut().expect("status").phase = Some("Failed".to_string());
    let failed_a_for_get = failed_a.clone();
    let delete_bodies: Arc<std::sync::Mutex<Vec<Option<serde_json::Value>>>> = Arc::default();
    let delete_bodies_c = Arc::clone(&delete_bodies);
    let pool_for_redrive = db.pool.clone();
    let conflict_path = format!("/api/v1/namespaces/space/pods/{conflict_pod}");
    let (conflict_client, _conflict_log) = scripted_kube_client(move |req| {
        if req.path == conflict_path && req.method == Method::GET {
            return pod_reply(&failed_a_for_get);
        }
        if req.path == conflict_path && req.method == Method::DELETE {
            delete_bodies_c
                .lock()
                .expect("delete bodies")
                .push(req.body.clone());
            return kube_status(StatusCode::CONFLICT, "Conflict");
        }
        kube_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            "UnexpectedConflictRequest",
        )
    });
    let mut conflict_substrate =
        substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    conflict_substrate.k8s.client = conflict_client;
    let (conflict_substrate, _conflict_feed) =
        conflict_substrate.with_pod_cache_feed(vec![failed_a]);

    let conflict_out = conflict_substrate
        .resolve_pod_ip_for_readiness(
            conflict_sbx,
            &PodProxyWaitTarget::new(&conflict_pod, Some("uid-a"), 1),
            deadline,
        )
        .await?;
    assert!(!conflict_out.terminal_or_absent_retired);
    let bodies = delete_bodies.lock().expect("delete bodies").clone();
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0]
            .as_ref()
            .and_then(|v| v.pointer("/preconditions/uid"))
            .and_then(serde_json::Value::as_str),
        Some("uid-a"),
        "terminal cleanup DELETE must carry UID precondition for uid-a, got: {:?}",
        bodies[0]
    );
    let surviving_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(conflict_sbx)
    .fetch_one(&pool_for_redrive)
    .await?;
    assert_eq!(
        surviving_uid.as_deref(),
        Some("uid-a"),
        "409 Conflict on UID-preconditioned DELETE must leave DB tracking intact"
    );

    // Case B: DELETE returns `404 NotFound` after a concurrent same-generation
    // redrive has already advanced the row from `attempt_a` (`uid-old`) to
    // `attempt_b` (`uid-b`). `delete_pod_and_forget_tracking` must NOT retire
    // the new attempt's execution row.
    let race_sbx = SandboxId::new();
    let race_pod = format!("space-sbx-{race_sbx}-g1");
    insert_test_sandbox_row(&db.pool, race_sbx, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
              activation_attempt_id, activation_attempt_expires_at)
         VALUES ($1, 1, $2, 'space', 'uid-b', 'created', true, $3, NOW() + INTERVAL '5 minutes')"
    ))
    .bind(race_sbx)
    .bind(&race_pod)
    .bind(attempt_b)
    .execute(&db.pool)
    .await?;
    let race_path = format!("/api/v1/namespaces/space/pods/{race_pod}");
    let (race_client, _race_log) = scripted_kube_client(move |req| {
        if req.path == race_path && req.method == Method::DELETE {
            return kube_status(StatusCode::NOT_FOUND, "NotFound");
        }
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedRaceRequest")
    });
    let mut race_substrate =
        substrate_with_inventory_caches_and_pool(vec![], vec![], db.pool.clone());
    race_substrate.k8s.client = race_client;
    let (race_substrate, _race_feed) = race_substrate.with_pod_cache_feed(vec![]);
    let retired = race_substrate
        .delete_pod_and_forget_tracking(
            super::super::TerminalPodCleanup {
                sandbox_id: race_sbx,
                pod_name: &race_pod,
                generation: 1,
                expected_delete_uid: Some("uid-b"),
                activation_attempt_id: Some(
                    pplx_space_sandbox_model::SandboxActivationAttemptId::from_uuid(attempt_a),
                ),
                pod_fence: ObservedAttemptPodFence::exact_uid(Some("uid-b")),
            },
            PodTrackingCleanupCause::MissingPodObservation,
        )
        .await?;
    assert!(!retired);
    let (row_uid, row_state): (Option<String>, String) = sqlx::query_as(sql!(
        "SELECT pod_uid, state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(race_sbx)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(row_uid.as_deref(), Some("uid-b"));
    assert_eq!(row_state, "created");
    Ok(())
}

/// Non-readiness `resolve_pod_ip` trusts a matching `Pending` pod in the
/// reflector cache when `row.pod_ip` is `None` and the reflector is healthy,
/// returning `Ok(None)` with zero uncached `GET`s.
#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_ip_trusts_healthy_cached_pending_pod_without_uncached_get() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sbx = SandboxId::new();
    let pod_name = format!("space-sbx-{sbx}-g1");
    insert_test_sandbox_row(&db.pool, sbx, "creating").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', 'uid-pending-nonready', NULL, 'created', true)"
    ))
    .bind(sbx)
    .bind(&pod_name)
    .execute(&db.pool)
    .await?;

    let mut pending = (*sandbox_pod_on_node(&pod_name, "worker-a", 1, "Pending")).clone();
    pending.metadata.namespace = Some("space".to_string());
    pending.metadata.uid = Some("uid-pending-nonready".to_string());

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
    let (mut substrate, _feed) = substrate.with_pod_cache_feed(vec![pending]);
    substrate.budgets.reflector_wait = Duration::from_millis(15);

    let resolved = substrate.resolve_pod_ip(sbx).await?;
    assert_eq!(resolved, None);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "healthy reflector must not issue an uncached GET for a cached Pending pod on non-readiness resolve_pod_ip"
    );
    Ok(())
}
