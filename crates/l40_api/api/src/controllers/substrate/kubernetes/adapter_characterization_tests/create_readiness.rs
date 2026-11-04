//! `create_execution` after its pod exists: the arms that run when the
//! readiness wait fails, and a create that joins an activation already in
//! flight.
//!
//! The substrate's [`SubstrateBudgets`] are shortened so the outer readiness
//! budget and the preserve decision expire after a second, while Postgres,
//! the scripted apiserver and the scripted worker stay real.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use futures_util::FutureExt as _;
use k8s_openapi::api::core::v1::{Node, NodeAddress, NodeStatus};
use pplx_space_common_core::types::{
    NodeId, SandboxActivationAttemptId, SandboxTransitionTargetState,
};
use sqlx::{Postgres, Transaction};

use super::super::scripted_test_doubles::{
    CallObserver, KubeReply, KubeRequest, ScriptedReply, ScriptedWorker, ScriptedWorkerConfig,
    kube_status, pod_path, pod_reply, requests_matching, scripted_kube_client_async,
    start_scripted_worker,
};
use super::super::tests::{insert_test_sandbox_row, substrate_with_inventory_caches_and_pool};
use super::*;

const SPACED_WAIT_READY: &str = "WorkerApi/SpacedWaitReady";
const PODS_PATH: &str = "/api/v1/namespaces/space/pods";
/// A node the apiserver names but the substrate's node cache does not hold.
const UNCACHED_NODE: &str = "node-uncached";

/// Shortened create budgets: the inner readiness phases get 500 ms and the
/// outer budget fires 500 ms later. A readiness wait parked on a pod read
/// that never answers reaches that read after the 250 ms reflector wait, well
/// inside the outer budget even on a loaded runner.
fn test_create_budgets() -> SubstrateBudgets {
    SubstrateBudgets {
        create_ready: Duration::from_secs(1),
        create_ready_phase_margin: Duration::from_millis(500),
        preserve_decision: Duration::from_secs(1),
        ..SubstrateBudgets::DEFAULT
    }
}

/// The production create budgets with a shortened preserve decision, for
/// cases that end on their own phase well inside the create budget.
fn phase_ending_budgets() -> SubstrateBudgets {
    SubstrateBudgets {
        preserve_decision: Duration::from_secs(1),
        ..SubstrateBudgets::DEFAULT
    }
}

/// The worker node, cached with the UID and address placement records.
fn worker_node() -> Node {
    Node {
        metadata: kube::api::ObjectMeta {
            name: Some(WORKER_NODE.to_string()),
            uid: Some(format!("{WORKER_NODE}-uid")),
            ..Default::default()
        },
        status: Some(NodeStatus {
            addresses: Some(vec![NodeAddress {
                type_: "InternalIP".to_string(),
                address: "127.0.0.1".to_string(),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn worker_node_id() -> NodeId {
    space_k8s_node_id(WORKER_NODE, SpaceNodeRole::Worker.as_str())
}

/// A substrate whose created pods are reachable on `worker`, with only
/// [`WORKER_NODE`] in its node cache.
fn substrate_on(pool: PgPool, client: Client, worker: &ScriptedWorker) -> KubernetesSubstrate {
    let mut substrate =
        substrate_with_inventory_caches_and_pool(Vec::new(), vec![worker_node()], pool);
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    substrate.budgets = test_create_budgets();
    substrate
}

/// A running pod for `sandbox_id`'s first generation on `node`, at the
/// loopback address its worker listens on.
fn running_pod(pod_name: &str, sandbox_id: SandboxId, node: Option<&str>) -> Pod {
    let mut pod = sandbox_pod(
        pod_name,
        &format!("{pod_name}-uid"),
        sandbox_id,
        1,
        "Running",
        Some("127.0.0.1"),
    );
    pod.spec.as_mut().expect("sandbox pod has a spec").node_name = node.map(str::to_string);
    pod
}

fn requested_pod_name(request: &KubeRequest) -> String {
    request
        .body
        .as_ref()
        .and_then(|pod| pod.pointer("/metadata/name"))
        .and_then(serde_json::Value::as_str)
        .expect("a pod create names its pod")
        .to_string()
}

async fn active_attempt(pool: &PgPool, sandbox_id: SandboxId) -> Result<Option<Uuid>, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

// ── readiness failure ───────────────────────────────────────────────────────

/// How the readiness wait of a create whose pod was accepted fails.
#[derive(Debug, Clone, Copy)]
enum ReadinessFailure {
    /// Readiness's first pod read never answers. No readiness phase bounds
    /// that read, so only the outer create budget ends the wait, with a
    /// timeout that names no phase.
    OuterBudget,
    /// Every guest probe answers gRPC `Unavailable` until the command-ready
    /// phase times out.
    NodeMgrUnavailable,
    /// The first execution read loses a lock race: a table lock is held from
    /// the pod create until the cleanup delete arrives.
    LockContention,
}

/// What the apiserver reports for the pod once readiness has failed.
#[derive(Debug, Clone, Copy)]
enum PodAfterFailure {
    /// Scheduled on the cached worker node, its proxy still reachable.
    ScheduledReachable,
    /// Scheduled on a node missing from the cache whose apiserver read never
    /// answers, so the preserve decision cannot finish.
    ScheduledOnUncachedNode,
    /// Marked for deletion.
    Terminating,
    /// Gone.
    Absent,
}

#[derive(Debug, Clone, Copy)]
enum CleanupDelete {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Copy)]
enum ExpectedEnd {
    /// Returned as started on the worker node, without readiness; the pod
    /// and its execution are kept.
    PreservedOnNode,
    /// The pod is deleted, its execution retired, and the error returned.
    TornDown(ExpectedError),
    /// Nothing is deleted or retired: cleanup is left to reconciliation, and
    /// the error returned.
    Deferred(ExpectedError),
}

#[derive(Debug, Clone, Copy)]
enum ExpectedError {
    /// The outer budget's own timeout, which carries no phase.
    TimeoutWithoutPhase,
    /// The command-ready phase's timeout.
    GuestNotReadyTimeout,
    /// Still retryable: the cleanup delete was accepted.
    LockContention,
    /// Demoted from contention: nothing is removing the pod, so a retry would
    /// orphan it.
    Failed,
}

impl ExpectedError {
    fn matches(self, error: &SubstrateError) -> bool {
        match self {
            Self::TimeoutWithoutPhase => {
                matches!(error, SubstrateError::Timeout { phase: None, .. })
            }
            Self::GuestNotReadyTimeout => matches!(
                error,
                SubstrateError::Timeout {
                    phase: Some(WaitReadyTimeoutPhase::GuestNotReady),
                    ..
                }
            ),
            Self::LockContention => matches!(error, SubstrateError::LockContention { .. }),
            Self::Failed => matches!(error, SubstrateError::Failed { .. }),
        }
    }
}

/// Apiserver state the scripted replies read and the test's observers change.
#[derive(Default)]
struct ApiserverState {
    pod_name: Mutex<Option<String>>,
    readiness_failed: AtomicBool,
    /// Held from the pod create to the cleanup delete in the contention case.
    table_lock: tokio::sync::Mutex<Option<Transaction<'static, Postgres>>>,
}

impl ApiserverState {
    async fn release_table_lock(&self) {
        if let Some(lock) = self.table_lock.lock().await.take() {
            lock.rollback()
                .await
                .expect("release the execution table lock");
        }
    }
}

/// A create whose readiness wait fails gives back exactly what the failure
/// arm owns: a pod that is scheduled and reachable when the outer budget
/// expires is kept and returned as started; one that is terminating, or whose
/// preserve decision outlives its budget, is deleted with its execution
/// retired; one that vanished after the wait timed out, on the outer budget or
/// after node-mgr probes answered `Unavailable`, is left, with its claim, to
/// reconciliation. A contended read after the pod exists stays retryable only
/// when the cleanup delete was accepted.
#[rstest::rstest]
#[case::outer_budget_preserves_a_reachable_pod(
    ReadinessFailure::OuterBudget,
    PodAfterFailure::ScheduledReachable,
    CleanupDelete::Accepted,
    ExpectedEnd::PreservedOnNode
)]
#[case::outer_budget_tears_down_a_terminating_pod(
    ReadinessFailure::OuterBudget,
    PodAfterFailure::Terminating,
    CleanupDelete::Accepted,
    ExpectedEnd::TornDown(ExpectedError::TimeoutWithoutPhase)
)]
#[case::slow_preserve_decision_tears_down(
    ReadinessFailure::OuterBudget,
    PodAfterFailure::ScheduledOnUncachedNode,
    CleanupDelete::Accepted,
    ExpectedEnd::TornDown(ExpectedError::TimeoutWithoutPhase)
)]
#[case::outer_budget_defers_cleanup_of_a_vanished_pod(
    ReadinessFailure::OuterBudget,
    PodAfterFailure::Absent,
    CleanupDelete::Accepted,
    ExpectedEnd::Deferred(ExpectedError::TimeoutWithoutPhase)
)]
#[case::unreachable_node_mgr_defers_cleanup_of_a_vanished_pod(
    ReadinessFailure::NodeMgrUnavailable,
    PodAfterFailure::Absent,
    CleanupDelete::Accepted,
    ExpectedEnd::Deferred(ExpectedError::GuestNotReadyTimeout)
)]
#[case::contention_with_accepted_cleanup_stays_retryable(
    ReadinessFailure::LockContention,
    PodAfterFailure::ScheduledReachable,
    CleanupDelete::Accepted,
    ExpectedEnd::TornDown(ExpectedError::LockContention)
)]
#[case::contention_with_rejected_cleanup_is_demoted(
    ReadinessFailure::LockContention,
    PodAfterFailure::ScheduledReachable,
    CleanupDelete::Rejected,
    ExpectedEnd::TornDown(ExpectedError::Failed)
)]
#[tokio::test(flavor = "current_thread")]
async fn create_readiness_failure_keeps_or_releases_only_what_its_arm_owns(
    #[case] failure: ReadinessFailure,
    #[case] pod_after_failure: PodAfterFailure,
    #[case] cleanup_delete: CleanupDelete,
    #[case] expected: ExpectedEnd,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;

    let state = Arc::new(ApiserverState::default());
    let probe_reply = match failure {
        ReadinessFailure::NodeMgrUnavailable => {
            ScriptedReply::Status(tonic::Status::unavailable("node-mgr unreachable"))
        }
        // Readiness fails before any probe.
        ReadinessFailure::OuterBudget | ReadinessFailure::LockContention => ScriptedReply::Hang,
    };
    let observed = Arc::clone(&state);
    let readiness_failed: CallObserver = Arc::new(move || {
        observed.readiness_failed.store(true, Ordering::SeqCst);
        async {}.boxed()
    });
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        observers: [(SPACED_WAIT_READY, readiness_failed)].into(),
        ..ScriptedWorkerConfig::new([(SPACED_WAIT_READY, probe_reply)])
    })
    .await?;

    let reply_state = Arc::clone(&state);
    let lock_pool = db.pool.clone();
    let (client, log) = scripted_kube_client_async(move |request| {
        let state = Arc::clone(&reply_state);
        let lock_pool = lock_pool.clone();
        async move {
            let pod_name = state.pod_name.lock().unwrap().clone();
            let at_pod = pod_name.as_deref().map(pod_path);
            match request.method {
                Method::POST if request.path == PODS_PATH => {
                    let pod_name = requested_pod_name(&request);
                    *state.pod_name.lock().unwrap() = Some(pod_name.clone());
                    if let ReadinessFailure::LockContention = failure {
                        let mut lock = lock_pool.begin().await.expect("begin the table lock");
                        sqlx::query(sql!(
                            "LOCK TABLE sandbox_k8s_executions IN ACCESS EXCLUSIVE MODE"
                        ))
                        .execute(&mut *lock)
                        .await
                        .expect("lock the execution table");
                        *state.table_lock.lock().await = Some(lock);
                    }
                    // Recording the pod's UID and IP moves the execution out
                    // of `creating`, so readiness reads the pod rather than
                    // watching it. No node yet: placement is recorded only
                    // after readiness.
                    pod_reply(&running_pod(&pod_name, sandbox_id, None))
                }
                Method::GET if Some(&request.path) == at_pod.as_ref() => {
                    let pod_name = pod_name.expect("the pod was created");
                    // Readiness makes the first read of the pod; the reads
                    // after it see the pod as it is once readiness failed.
                    let first_read = !state.readiness_failed.load(Ordering::SeqCst);
                    match failure {
                        ReadinessFailure::OuterBudget if first_read => {
                            state.readiness_failed.store(true, Ordering::SeqCst);
                            return KubeReply::Hang;
                        }
                        _ => {}
                    }
                    let after_failure = state.readiness_failed.load(Ordering::SeqCst);
                    match (after_failure, pod_after_failure) {
                        (true, PodAfterFailure::Absent) => {
                            kube_status(StatusCode::NOT_FOUND, "NotFound")
                        }
                        (true, PodAfterFailure::Terminating) => {
                            let mut pod = running_pod(&pod_name, sandbox_id, Some(WORKER_NODE));
                            pod.metadata.deletion_timestamp =
                                Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                                    jiff::Timestamp::now(),
                                ));
                            pod_reply(&pod)
                        }
                        (true, PodAfterFailure::ScheduledOnUncachedNode) => {
                            pod_reply(&running_pod(&pod_name, sandbox_id, Some(UNCACHED_NODE)))
                        }
                        _ => pod_reply(&running_pod(&pod_name, sandbox_id, Some(WORKER_NODE))),
                    }
                }
                Method::GET if request.path == format!("/api/v1/nodes/{UNCACHED_NODE}") => {
                    KubeReply::Hang
                }
                Method::DELETE if Some(&request.path) == at_pod.as_ref() => {
                    state.release_table_lock().await;
                    match cleanup_delete {
                        CleanupDelete::Accepted => kube_status(StatusCode::OK, "Success"),
                        CleanupDelete::Rejected => {
                            kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
                        }
                    }
                }
                _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
            }
        }
    });
    let pool = match failure {
        // Bound lock waits, as an application login's role-level
        // `lock_timeout` does, so the held table lock aborts the read.
        ReadinessFailure::LockContention => {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .after_connect(|conn, _| {
                    Box::pin(async move {
                        sqlx::query("SET lock_timeout = '100ms'")
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect(&db.database_url)
                .await?
        }
        ReadinessFailure::OuterBudget | ReadinessFailure::NodeMgrUnavailable => db.pool.clone(),
    };
    let mut substrate = substrate_on(pool, client, &worker);
    match failure {
        // A data-plane timeout far past the budget, so only the outer budget
        // can end the wait.
        ReadinessFailure::OuterBudget => {
            substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(60);
        }
        // These end on their own phase; the create budget stays out of reach.
        ReadinessFailure::NodeMgrUnavailable => {
            substrate.budgets = phase_ending_budgets();
            substrate.k8s.create_command_ready_timeout = Duration::from_millis(500);
        }
        ReadinessFailure::LockContention => {
            substrate.budgets = phase_ending_budgets();
        }
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    request.wait_until_ready = true;

    let started = Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(10), substrate.create_execution(request))
            .await
            .map_err(|_| format!("{failure:?}/{pod_after_failure:?}: the create must end"))?;
    let elapsed = started.elapsed();
    state.release_table_lock().await;

    assert!(
        elapsed < PRESERVE_DECISION_BUDGET,
        "{failure:?}/{pod_after_failure:?}: the shortened budgets must end the create, took \
         {elapsed:?}"
    );
    assert!(
        substrate.task_tracker.is_empty(),
        "no cleanup is left running in the background"
    );
    let pod_name = state
        .pod_name
        .lock()
        .unwrap()
        .clone()
        .expect("the pod was created");
    let deletes = requests_matching(&log, &Method::DELETE);
    let active = active_execution_count(&db.pool, sandbox_id).await?;
    match expected {
        ExpectedEnd::PreservedOnNode => {
            let outcome =
                outcome.map_err(|error| format!("expected a preserved create: {error:?}"))?;
            assert_eq!(outcome.initial_lifecycle, "started");
            assert_eq!(outcome.initial_placement, "on_node");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(!outcome.command_ready, "readiness was never observed");
            assert!(outcome.binding.is_none());
            assert!(
                deletes.is_empty(),
                "the preserved pod must not be deleted: {deletes:?}"
            );
            assert_eq!(active, 1, "the preserved pod's execution stays tracked");
            let (state, _, pod_uid, pod_ip) = execution_row(&db.pool, sandbox_id, 1).await?;
            assert_eq!(
                (state.as_str(), pod_uid, pod_ip.as_deref()),
                (
                    "running",
                    Some(format!("{pod_name}-uid")),
                    Some("127.0.0.1")
                )
            );
            assert_eq!(substrate.cancel_tracker.current_count(), 0);
        }
        ExpectedEnd::TornDown(error) => {
            let outcome = outcome.err().ok_or("expected the create to fail")?;
            assert!(
                error.matches(&outcome),
                "expected {error:?}, got {outcome:?}"
            );
            let [delete] = deletes.as_slice() else {
                panic!("expected one pod delete, got {deletes:?}");
            };
            assert_eq!(delete.path, pod_path(&pod_name));
            assert_eq!(active, 0, "the execution must be retired");
            assert_eq!(
                substrate.cancel_tracker.current_count(),
                0,
                "a synchronous cleanup is not a cancellation"
            );
        }
        ExpectedEnd::Deferred(error) => {
            let outcome = outcome.err().ok_or("expected the create to fail")?;
            assert!(
                error.matches(&outcome),
                "expected {error:?}, got {outcome:?}"
            );
            assert!(deletes.is_empty(), "nothing may be deleted: {deletes:?}");
            assert_eq!(
                active, 1,
                "the claim is left for the orphaned-sandbox reconciler"
            );
            assert_eq!(
                substrate.cancel_tracker.current_count(),
                1,
                "the deferred cleanup still counts toward cancel-storm backpressure"
            );
        }
    }
    Ok(())
}

// ── joined create ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum OwnerPod {
    /// The owner's pod is running and its proxy reachable.
    Reachable,
    /// The owner's pod never gets an IP.
    NeverAssigned,
}

/// A create that finds another create's activation in flight joins it rather
/// than creating a pod: it returns the owner's pod once that pod is
/// reachable, without a lease of its own, and when the owner's pod never
/// becomes reachable it times out within the create budget, leaving the
/// owner's execution and activation untouched.
#[rstest::rstest]
#[case::returns_the_owners_reachable_pod(OwnerPod::Reachable)]
#[case::times_out_without_disturbing_the_owner(OwnerPod::NeverAssigned)]
#[tokio::test(flavor = "current_thread")]
async fn joined_create_waits_on_the_owners_pod(#[case] owner_pod: OwnerPod) -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let pod_name: Arc<Mutex<Option<String>>> = Arc::default();
    let reply_pod_name = Arc::clone(&pod_name);
    let (client, log) = scripted_kube_client_async(move |request| {
        let pod_name = reply_pod_name.lock().unwrap().clone();
        async move {
            match (request.method, pod_name) {
                (Method::GET, Some(pod_name)) if request.path == pod_path(&pod_name) => {
                    match owner_pod {
                        OwnerPod::Reachable => {
                            pod_reply(&running_pod(&pod_name, sandbox_id, Some(WORKER_NODE)))
                        }
                        OwnerPod::NeverAssigned => pod_reply(&sandbox_pod(
                            &pod_name,
                            &format!("{pod_name}-uid"),
                            sandbox_id,
                            1,
                            "Pending",
                            None,
                        )),
                    }
                }
                // The per-pod watch never reports an IP.
                (Method::GET, _) if request.path == PODS_PATH => KubeReply::Hang,
                _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
            }
        }
    });
    let mut substrate = substrate_on(db.pool.clone(), client, &worker);
    // A phase timeout far past the budget, so only the create budget can end
    // the joined wait.
    substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(60);

    // Another create owns the activation.
    let owner_attempt = SandboxActivationAttemptId::new();
    let owner = substrate
        .store
        .begin_activation_execution_row(
            sandbox_id,
            owner_attempt,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    *pod_name.lock().unwrap() = Some(owner.pod_name.clone());
    if let OwnerPod::Reachable = owner_pod {
        substrate
            .store
            .persist_pod_metadata(
                sandbox_id,
                owner.generation,
                Some(&format!("{}-uid", owner.pod_name)),
                Some("127.0.0.1"),
            )
            .await?;
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        substrate.create_execution(inputs.create_request(sandbox_id, &template_ref)),
    )
    .await
    .map_err(|_| format!("{owner_pod:?}: the joined create must end"))?;

    assert!(
        requests_matching(&log, &Method::POST).is_empty()
            && requests_matching(&log, &Method::DELETE).is_empty(),
        "{owner_pod:?}: a joined create neither creates nor deletes a pod"
    );
    assert_eq!(
        active_attempt(&db.pool, sandbox_id).await?,
        Some(owner_attempt.as_uuid()),
        "{owner_pod:?}: the owner keeps its activation"
    );
    assert!(
        substrate.task_tracker.is_empty(),
        "{owner_pod:?}: a joined create leaves nothing running in the background"
    );
    match owner_pod {
        OwnerPod::Reachable => {
            let outcome =
                outcome.map_err(|error| format!("expected a joined create: {error:?}"))?;
            assert!(
                outcome.lease_completion.is_none(),
                "the lease is the owner's"
            );
            assert_eq!(outcome.initial_lifecycle, "started");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(outcome.command_ready);
            let binding = outcome
                .binding
                .ok_or("a joined create returns the owner's binding")?;
            assert_eq!(
                binding.execution_object_id,
                format!("{}-uid", owner.pod_name)
            );
            assert_eq!(binding.execution_generation, 1);
        }
        OwnerPod::NeverAssigned => {
            let outcome = outcome
                .err()
                .ok_or("expected the joined create to time out")?;
            assert!(
                matches!(
                    outcome,
                    SubstrateError::Timeout {
                        phase: Some(WaitReadyTimeoutPhase::PodNeverReady),
                        ..
                    }
                ),
                "{outcome:?}"
            );
            let elapsed = started.elapsed();
            assert!(
                elapsed < Duration::from_secs(5),
                "the create budget, not the data-plane timeout, must end the wait: {elapsed:?}"
            );
            assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
        }
    }
    Ok(())
}
