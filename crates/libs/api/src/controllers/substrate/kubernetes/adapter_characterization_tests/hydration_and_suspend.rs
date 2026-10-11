//! `pod_ops::hydrate_restored_execution` and `suspend_execution`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId};
use pplx_space_sbx_mgr_client::proto as sbx;

use super::super::pod_ops::ExecutionHydrationOutcome;
use super::super::scripted_test_doubles::{
    KubeReply, KubeRequest, ScriptedReply, ScriptedWorkerConfig, kube_status, pod_path, pod_reply,
    requests_matching, scripted_kube_client, scripted_kube_client_async, start_scripted_worker,
};
use super::super::tests::{
    deleted_pod_kubernetes_client, insert_test_sandbox_row, node_api_pod_on_node,
    substrate_with_store_and_client, substrate_with_store_client_proxy_port_and_pod,
};
use super::*;
use crate::controllers::substrate::PublishedLocalArtifact;

/// Long enough that a correct suspend retirement never misses it on a slow
/// runner; the doubles answer the DELETE and the absence check immediately.
const BLOCKING_DELETE_TIMEOUT: Duration = Duration::from_millis(500);

// ── hydrate_restored_execution ──────────────────────────────────────────────

const HYDRATE_POD: &str = "space-sbx-hydrate-g2";

#[derive(Debug, Clone, Copy)]
enum HydrationPod {
    /// The apiserver has no pod at the execution's name.
    Absent,
    /// A Running pod with an IP whose annotations name this execution.
    Matching,
    /// A Running pod with an IP whose generation annotation is a predecessor's.
    PriorGeneration,
    /// The apiserver rejects the read.
    Forbidden,
}

/// A substrate whose reflector already holds a stale, settled pod at the
/// execution's name, so only an authoritative read sees the apiserver's pod.
fn substrate_with_stale_cached_pod(
    pool: &PgPool,
    client: Client,
    sandbox_id: SandboxId,
) -> KubernetesSubstrate {
    substrate_with_store_client_proxy_port_and_pod(
        pool.clone(),
        client,
        DEFAULT_SANDBOX_PROXY_PORT,
        Some(sandbox_pod(
            HYDRATE_POD,
            "stale-cached-uid",
            sandbox_id,
            2,
            "Running",
            Some("192.0.2.50"),
        )),
    )
}

/// Each row: the execution state before hydration, what the apiserver holds,
/// the outcome (`None` for an error), and the execution row afterwards as
/// `(state, pod_uid, pod_ip)`. Callers only act on `Err` versus the outcome,
/// so the error's variant is not pinned.
#[rstest::rstest]
#[case::matching_pod_hydrates_unroutable_row(
    "creating",
    HydrationPod::Matching,
    Some(ExecutionHydrationOutcome::Hydrated),
    ("running", Some("hydrate-uid"), Some("192.0.2.9"))
)]
#[case::recovering_row_records_ip_but_stays_unroutable(
    "recovering",
    HydrationPod::Matching,
    Some(ExecutionHydrationOutcome::NotRoutable),
    ("recovering", Some("hydrate-uid"), Some("192.0.2.9"))
)]
#[case::absent_pod_writes_nothing(
    "creating",
    HydrationPod::Absent,
    Some(ExecutionHydrationOutcome::NotRoutable),
    ("creating", None, None)
)]
#[case::predecessor_pod_writes_nothing(
    "creating",
    HydrationPod::PriorGeneration,
    Some(ExecutionHydrationOutcome::NotRoutable),
    ("creating", None, None)
)]
#[case::apiserver_error_is_an_error_not_unroutable(
    "creating",
    HydrationPod::Forbidden,
    None,
    ("creating", None, None)
)]
#[tokio::test(flavor = "current_thread")]
async fn hydrate_restored_execution_copies_only_the_apiservers_verified_pod(
    #[case] initial_state: &str,
    #[case] apiserver: HydrationPod,
    #[case] expected: Option<ExecutionHydrationOutcome>,
    #[case] expected_row: (&str, Option<&str>, Option<&str>),
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "restoring").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            generation: 2,
            pod_uid: None,
            pod_ip: None,
            state: initial_state,
            ..ExecutionSeed::running(HYDRATE_POD, "unused")
        },
    )
    .await?;
    let (client, _log) = scripted_kube_client(move |request| {
        assert_eq!(request.method, Method::GET, "hydration only reads");
        assert_eq!(request.path, pod_path(HYDRATE_POD));
        match apiserver {
            HydrationPod::Absent => kube_status(StatusCode::NOT_FOUND, "NotFound"),
            HydrationPod::Matching => pod_reply(&sandbox_pod(
                HYDRATE_POD,
                "hydrate-uid",
                sandbox_id,
                2,
                "Running",
                Some("192.0.2.9"),
            )),
            HydrationPod::PriorGeneration => pod_reply(&sandbox_pod(
                HYDRATE_POD,
                "hydrate-uid",
                sandbox_id,
                1,
                "Running",
                Some("192.0.2.9"),
            )),
            HydrationPod::Forbidden => kube_status(StatusCode::FORBIDDEN, "Forbidden"),
        }
    });
    let substrate = substrate_with_stale_cached_pod(&db.pool, client, sandbox_id);

    let outcome = substrate
        .hydrate_restored_execution(sandbox_id, Duration::from_secs(5))
        .await;

    assert_eq!(outcome.as_ref().ok().copied(), expected, "{outcome:?}");
    let (state, active, pod_uid, pod_ip) = execution_row(&db.pool, sandbox_id, 2).await?;
    assert!(active);
    assert_eq!(
        (state.as_str(), pod_uid.as_deref(), pod_ip.as_deref()),
        expected_row
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn hydrate_restored_execution_skips_the_apiserver_without_an_unroutable_execution()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let (client, log) = scripted_kube_client(|_| kube_status(StatusCode::FORBIDDEN, "Forbidden"));
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);

    // No execution row at all.
    let unknown = SandboxId::new();
    insert_test_sandbox_row(&db.pool, unknown, "restoring").await?;
    assert_eq!(
        substrate
            .hydrate_restored_execution(unknown, Duration::from_secs(5))
            .await?,
        ExecutionHydrationOutcome::NotRoutable
    );

    // An execution that is already `running` with a pod IP.
    let routable = SandboxId::new();
    insert_test_sandbox_row(&db.pool, routable, "restoring").await?;
    insert_execution(
        &db.pool,
        routable,
        ExecutionSeed::running("space-sbx-routable-g1", "routable-uid"),
    )
    .await?;
    assert_eq!(
        substrate
            .hydrate_restored_execution(routable, Duration::from_secs(5))
            .await?,
        ExecutionHydrationOutcome::AlreadyRoutable
    );

    assert!(
        log.lock().unwrap().is_empty(),
        "neither case may consult the apiserver"
    );
    Ok(())
}

/// A read that outlives the budget is an error the caller defers on, not
/// `NotRoutable`, and writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_restored_execution_fails_on_a_wedged_apiserver() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "restoring").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            generation: 2,
            pod_uid: None,
            pod_ip: None,
            state: "creating",
            ..ExecutionSeed::running(HYDRATE_POD, "unused")
        },
    )
    .await?;
    let (client, _log) = scripted_kube_client(|_| KubeReply::Hang);
    let substrate = substrate_with_stale_cached_pod(&db.pool, client, sandbox_id);

    let outcome = substrate
        .hydrate_restored_execution(sandbox_id, Duration::from_millis(50))
        .await;

    assert!(outcome.is_err(), "{outcome:?}");
    let (state, _, pod_uid, pod_ip) = execution_row(&db.pool, sandbox_id, 2).await?;
    assert_eq!((state.as_str(), pod_uid, pod_ip), ("creating", None, None));
    Ok(())
}

/// An execution retired between the apiserver read and the write is not
/// hydrated: the generation-guarded write matches no active row.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_restored_execution_does_not_hydrate_an_execution_retired_mid_read() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-retired-mid-read-g1";
    insert_test_sandbox_row(&db.pool, sandbox_id, "restoring").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            pod_uid: None,
            pod_ip: None,
            state: "creating",
            ..ExecutionSeed::running(pod_name, "unused")
        },
    )
    .await?;
    let pod = sandbox_pod(
        pod_name,
        "retired-mid-read-uid",
        sandbox_id,
        1,
        "Running",
        Some("192.0.2.9"),
    );
    let retire_pool = db.pool.clone();
    // The apiserver answers only after a concurrent teardown retired the row.
    let (client, _log) = scripted_kube_client_async(move |_request| {
        let retire_pool = retire_pool.clone();
        let pod = pod.clone();
        async move {
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(&retire_pool)
            .await
            .expect("retire the execution mid-read");
            pod_reply(&pod)
        }
    });
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);

    let outcome = substrate
        .hydrate_restored_execution(sandbox_id, Duration::from_secs(5))
        .await?;

    assert_eq!(outcome, ExecutionHydrationOutcome::NotRoutable);
    let (state, active, pod_uid, pod_ip) = execution_row(&db.pool, sandbox_id, 1).await?;
    assert!(!active);
    assert_eq!((state.as_str(), pod_uid, pod_ip), ("creating", None, None));
    Ok(())
}

// ── suspend_execution ───────────────────────────────────────────────────────

fn delete_precondition_uid(request: &KubeRequest) -> Option<&str> {
    request
        .body
        .as_ref()?
        .pointer("/preconditions/uid")
        .and_then(serde_json::Value::as_str)
}

/// The sandbox's current suspend artifact, if any.
async fn suspend_artifact_id(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<Uuid>, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes
          WHERE id = $1 AND current_local_artifact_origin = 'suspend'"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?
    .flatten())
}

/// A ready worker node-api pod on [`WORKER_NODE`] that advertises `port`.
fn node_api_pod(port: u16) -> Pod {
    let mut pod = node_api_pod_on_node("space-worker-node-a", WORKER_NODE);
    pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        port.to_string(),
    )]));
    pod.status.as_mut().expect("node-api pod has status").pod_ip = Some("127.0.0.1".to_string());
    assert_eq!(
        super::super::caches::pod_node_api_port(&pod),
        Some(port),
        "the node-api pod must advertise its port the way the substrate reads it"
    );
    pod
}

/// A substrate that reaches `worker_port` only through the port its node-api
/// pod advertises. The configured fallback port is closed on loopback, so a
/// regression that ignores the advertised port fails to connect instead of
/// reaching whatever listens on this host's default node-api port.
fn substrate_with_node_api_on(
    pool: &PgPool,
    client: Client,
    worker_port: u16,
) -> KubernetesSubstrate {
    let mut substrate = substrate_with_store_client_proxy_port_and_pod(
        pool.clone(),
        client,
        DEFAULT_SANDBOX_PROXY_PORT,
        Some(node_api_pod(worker_port)),
    );
    substrate.k8s.node_api_port = 1;
    substrate
}

#[derive(Debug, Clone, Copy)]
enum SuspendRefusal {
    NoExecution,
    ExecutionWithoutPodUid,
    SandboxNotSuspendable,
}

/// A suspend refused before it claims ownership is retryable and leaves the
/// pod, the active execution, and the sandbox lifecycle as they were.
#[rstest::rstest]
#[case::no_active_execution(SuspendRefusal::NoExecution)]
#[case::execution_without_pod_uid(SuspendRefusal::ExecutionWithoutPodUid)]
#[case::sandbox_not_started_or_paused(SuspendRefusal::SandboxNotSuspendable)]
#[tokio::test(flavor = "current_thread")]
async fn suspend_execution_refuses_before_claiming_without_retiring_the_pod(
    #[case] refusal: SuspendRefusal,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-suspend-refusal-g1";
    let lifecycle = match refusal {
        SuspendRefusal::SandboxNotSuspendable => "stopping",
        _ => "started",
    };
    insert_test_sandbox_row(&db.pool, sandbox_id, lifecycle).await?;
    let seed = ExecutionSeed::running(pod_name, "suspend-refusal-uid");
    let seed = match refusal {
        SuspendRefusal::NoExecution => None,
        SuspendRefusal::ExecutionWithoutPodUid => Some(ExecutionSeed {
            pod_uid: None,
            ..seed
        }),
        SuspendRefusal::SandboxNotSuspendable => Some(seed),
    };
    if let Some(seed) = seed {
        insert_execution(&db.pool, sandbox_id, seed).await?;
    }
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);

    let outcome = substrate.suspend_execution_tracked(sandbox_id).await;

    assert!(
        matches!(outcome, Err(SubstrateError::Unavailable { .. })),
        "{refusal:?}: expected a retryable refusal, got {outcome:?}"
    );
    assert!(
        log.lock().unwrap().is_empty(),
        "{refusal:?}: no pod may be retired"
    );
    let expected_active = i64::from(!matches!(refusal, SuspendRefusal::NoExecution));
    assert_eq!(
        active_execution_count(&db.pool, sandbox_id).await?,
        expected_active
    );
    assert_eq!(
        sandbox_lifecycle(&db.pool, sandbox_id).await?,
        lifecycle,
        "{refusal:?}: the refusal must not claim the suspend"
    );
    Ok(())
}

/// A suspend whose artifact is already committed resumes pod retirement
/// without asking the node again, and returns the committed artifact.
#[tokio::test(flavor = "current_thread")]
async fn suspend_execution_resumes_retirement_of_a_committed_suspend() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-committed-suspend-g1";
    let artifact = PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    insert_test_sandbox_row(&db.pool, sandbox_id, "suspending").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact.id.as_uuid())
    .execute(&db.pool)
    .await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            // No node: reaching the node would fail this test.
            k8s_node_name: None,
            state: "suspended",
            ..ExecutionSeed::running(pod_name, "committed-suspend-uid")
        },
    )
    .await?;
    // The double asserts every DELETE carries this UID precondition.
    let delete_attempts = Arc::new(AtomicUsize::new(0));
    let mut substrate = substrate_with_store_and_client(
        db.pool.clone(),
        deleted_pod_kubernetes_client(
            pod_name,
            "committed-suspend-uid",
            Arc::clone(&delete_attempts),
        ),
    );
    substrate.k8s.k8s_blocking_delete_timeout = BLOCKING_DELETE_TIMEOUT;

    let returned = substrate.suspend_execution_tracked(sandbox_id).await?;

    assert_eq!(returned, Some(artifact));
    assert!(delete_attempts.load(Ordering::SeqCst) >= 1);
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 0);
    Ok(())
}

/// The node's durable generation suspend artifact is committed to the sandbox
/// before the exact pod is deleted, and the sandbox ends `suspended` on that
/// artifact.
#[tokio::test(flavor = "current_thread")]
async fn suspend_execution_commits_the_nodes_artifact_before_retiring_the_pod() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-node-suspend-g1";
    let node_artifact = LocalArtifactId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running(pod_name, "node-suspend-uid"),
    )
    .await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        "WorkerApi/PrepareTeardownSuspend",
        ScriptedReply::message(&sbx::PrepareTeardownSuspendResponse {
            outcome: sbx::TeardownSuspendOutcome::Durable as i32,
            reason: None,
            published_artifact: Some(sbx::PublishedArtifact {
                local_artifact_id: node_artifact.to_string(),
                artifact_kind: sbx::ArtifactKind::Full as i32,
                origin: sbx::ArtifactOrigin::Suspend as i32,
                snapshot_name: None,
            }),
        }),
    )]))
    .await?;
    // The sandbox's suspend artifact as the apiserver sees the pod DELETE.
    let artifact_at_delete: Arc<Mutex<Vec<Option<Uuid>>>> = Arc::default();
    let observed = Arc::clone(&artifact_at_delete);
    let observe_pool = db.pool.clone();
    let (client, log) = scripted_kube_client_async(move |request| {
        let observed = Arc::clone(&observed);
        let observe_pool = observe_pool.clone();
        async move {
            if request.path != pod_path(pod_name) {
                return kube_status(StatusCode::BAD_REQUEST, "BadRequest");
            }
            if request.method == Method::DELETE {
                let artifact = suspend_artifact_id(&observe_pool, sandbox_id)
                    .await
                    .expect("read the suspend artifact at DELETE");
                observed.lock().unwrap().push(artifact);
                return kube_status(StatusCode::OK, "Success");
            }
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
    });
    let mut substrate = substrate_with_node_api_on(&db.pool, client, worker.port);
    substrate.k8s.k8s_blocking_delete_timeout = BLOCKING_DELETE_TIMEOUT;

    let returned = substrate
        .suspend_execution_tracked(sandbox_id)
        .await?
        .expect("Kubernetes suspend always returns its artifact");

    let at_delete = artifact_at_delete.lock().unwrap().clone();
    assert!(!at_delete.is_empty(), "the pod must be deleted");
    assert!(
        at_delete
            .iter()
            .all(|artifact| *artifact == Some(node_artifact.as_uuid())),
        "the node's artifact must be committed before every pod DELETE: {at_delete:?}"
    );
    assert!(
        requests_matching(&log, &Method::DELETE)
            .iter()
            .all(|request| delete_precondition_uid(request) == Some("node-suspend-uid")),
        "every DELETE must carry the execution's pod UID precondition"
    );
    assert_eq!(returned.id, node_artifact);
    assert_eq!(returned.kind, ArtifactKind::Full);
    assert_eq!(
        suspend_artifact_id(&db.pool, sandbox_id).await?,
        Some(node_artifact.as_uuid())
    );
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 0);
    Ok(())
}

/// A node that refuses the generation-bound suspend RPC fails the suspend as
/// retryable and leaves the pod and the claimed execution in place. The
/// mapping of each `PrepareTeardownSuspend` outcome is pinned one layer down
/// by `adapter::tests::generation_suspend_polling_preserves_success_and_terminal_failure`.
#[tokio::test(flavor = "current_thread")]
async fn suspend_execution_keeps_the_pod_when_the_node_refuses() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-node-refuses-suspend-g1";
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running(pod_name, "node-refuses-uid"),
    )
    .await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        "WorkerApi/PrepareTeardownSuspend",
        ScriptedReply::Status(tonic::Status::not_found("sandbox record no longer exists")),
    )]))
    .await?;
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let substrate = substrate_with_node_api_on(&db.pool, client, worker.port);

    let outcome = substrate.suspend_execution_tracked(sandbox_id).await;

    assert!(
        matches!(outcome, Err(SubstrateError::Unavailable { .. })),
        "expected a retryable refusal, got {outcome:?}"
    );
    assert!(
        !worker
            .received("WorkerApi/PrepareTeardownSuspend")
            .is_empty(),
        "the refusal must come from the node"
    );
    assert!(log.lock().unwrap().is_empty(), "no pod may be retired");
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
    Ok(())
}
