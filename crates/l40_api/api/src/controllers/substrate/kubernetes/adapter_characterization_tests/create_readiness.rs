//! `create_execution` after its pod exists: the arms that run when the
//! readiness wait fails, and a create that joins an activation already in
//! flight.
//!
//! The substrate's [`SubstrateBudgets`] are shortened so the readiness waits
//! and preserve decisions finish quickly without changing Postgres or the
//! scripted apiserver and worker.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use futures_util::FutureExt as _;
use k8s_openapi::api::core::v1::{Node, NodeAddress, NodeStatus};
use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxTransitionTargetState};
use sqlx::{Postgres, Transaction};

use super::super::scripted_test_doubles::{
    CallObserver, KubeReply, KubeRequest, ScriptedReply, ScriptedWorker, ScriptedWorkerConfig,
    kube_status, pod_path, pod_reply, requests_matching, scripted_kube_client,
    scripted_kube_client_async, start_scripted_worker,
};
use super::super::tests::{insert_test_sandbox_row, substrate_with_inventory_caches_and_pool};
use super::*;

const SPACED_WAIT_READY: &str = "WorkerApi/SpacedWaitReady";
const PODS_PATH: &str = "/api/v1/namespaces/space/pods";
/// A node the apiserver names but the substrate's node cache does not hold.
const UNCACHED_NODE: &str = "node-uncached";

/// The shortened preserve decision. A preservable pod's decision is a
/// reflector miss, one apiserver GET and a node-cache lookup, all in process.
const TEST_PRESERVE_DECISION: Duration = Duration::from_millis(500);

/// Shortened create budgets over the test substrate's `base`: the inner
/// readiness phases get 300 ms and the outer budget fires 300 ms later. A
/// readiness wait parked on node discovery reaches it after the test
/// substrate's short reflector wait, well inside the readiness phase.
fn test_create_budgets(base: SubstrateBudgets) -> SubstrateBudgets {
    SubstrateBudgets {
        create_ready: Duration::from_millis(600),
        create_ready_phase_margin: Duration::from_millis(300),
        preserve_decision: TEST_PRESERVE_DECISION,
        ..base
    }
}

/// The production create budgets with a shortened preserve decision, for
/// cases that end on their own phase well inside the create budget.
fn phase_ending_budgets(base: SubstrateBudgets) -> SubstrateBudgets {
    SubstrateBudgets {
        create_ready: SubstrateBudgets::DEFAULT.create_ready,
        create_ready_phase_margin: SubstrateBudgets::DEFAULT.create_ready_phase_margin,
        preserve_decision: TEST_PRESERVE_DECISION,
        ..base
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
    substrate.budgets = test_create_budgets(substrate.budgets);
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

/// [`running_pod`] before kubelet has started it and published its IP.
fn starting_pod(pod_name: &str, sandbox_id: SandboxId, node: Option<&str>) -> Pod {
    let mut pod = running_pod(pod_name, sandbox_id, node);
    let status = pod.status.as_mut().expect("sandbox pod has a status");
    status.phase = Some("Pending".to_string());
    status.pod_ip = None;
    pod
}

/// Where a create's pod is when its readiness wait runs, and what the
/// apiserver answers when a single-shot fallback read runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PodSource {
    /// The shared reflector reports the pod running with an IP.
    ReflectorEvent,
    /// The reflector reports the pod bound to its node, whose sandbox
    /// manager reports the pod's address before kubelet publishes it.
    NodeReportsFirst,
    /// The reflector stays healthy and delivers updates for other pods before
    /// delivering this pod after the quiet-reflector window; zero fallback
    /// GETs may run.
    HealthyBusyReflectorSkipsOneShot,
    /// The reflector delivers zero events during `readiness_quiet_reflector`;
    /// the single quiet-reflector one-shot GET finds the pod running.
    QuietReflectorOneShot,
    /// The reflector observes the pod `Pending` and then removes it before it
    /// settles; the single disappearance one-shot GET finds it running.
    DisappearedFromCacheOneShot,
    /// The quiet-reflector one-shot GET hangs until the IP phase deadline
    /// preempts it.
    ReadsStall,
    /// The reflector reports the pod unschedulable.
    Unschedulable,
    /// The reflector reports the pod `Pending` and then deleted; the single
    /// disappearance one-shot GET finds `404 NotFound` and the wait times out.
    Deleted,
    /// The quiet-reflector one-shot GET hits `429 TooManyRequests` and fails
    /// fast with `503 Unavailable` instead of burning the create budget.
    OverloadFailsFast,
}

const TEST_QUIET_REFLECTOR: Duration = Duration::from_millis(80);
const TEST_DATA_PLANE_READY: Duration = Duration::from_millis(450);

/// A create waits for its pod's IP through the process's shared sandbox-pod
/// reflector without opening a per-pod watch or running a GET poll loop.
#[rstest::rstest]
#[case::a_reflector_update_binds_the_pod(PodSource::ReflectorEvent)]
#[case::the_node_reported_address_binds_the_pod(PodSource::NodeReportsFirst)]
#[case::active_reflector_traffic_suppresses_quiet_one_shot(
    PodSource::HealthyBusyReflectorSkipsOneShot
)]
#[case::a_quiet_reflector_uses_one_bounded_get(PodSource::QuietReflectorOneShot)]
#[case::a_vanished_cached_pod_uses_one_bounded_get(PodSource::DisappearedFromCacheOneShot)]
#[case::the_phase_deadline_preempts_a_stalled_read(PodSource::ReadsStall)]
#[case::an_unschedulable_pod_fails_the_create(PodSource::Unschedulable)]
#[case::a_deleted_pod_times_out_after_one_check(PodSource::Deleted)]
#[case::apiserver_throttling_fails_fast_as_unavailable(PodSource::OverloadFailsFast)]
#[tokio::test(flavor = "current_thread")]
async fn create_learns_its_pod_ip_without_a_watch_or_poll_of_its_own(
    #[case] source: PodSource,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let pod_name = Arc::new(Mutex::new(String::new()));
    let reply_pod_name = Arc::clone(&pod_name);
    let created_at: Arc<Mutex<Option<Instant>>> = Arc::default();
    let reply_created_at = Arc::clone(&created_at);
    let pod_reads: Arc<Mutex<Vec<Instant>>> = Arc::default();
    let reply_pod_reads = Arc::clone(&pod_reads);
    let (client, log) = scripted_kube_client(move |request| {
        let mut pod_name = reply_pod_name.lock().unwrap();
        match request.method {
            Method::POST if request.path == PODS_PATH => {
                *pod_name = requested_pod_name(request);
                *reply_created_at.lock().unwrap() = Some(Instant::now());
                pod_reply(&starting_pod(&pod_name, sandbox_id, None))
            }
            Method::GET if request.path == pod_path(&pod_name) => {
                reply_pod_reads.lock().unwrap().push(Instant::now());
                match source {
                    // Armed to succeed so a regression that calls GET on the
                    // reflector-driven cases is caught by the `reads` count.
                    PodSource::ReflectorEvent
                    | PodSource::NodeReportsFirst
                    | PodSource::HealthyBusyReflectorSkipsOneShot
                    | PodSource::QuietReflectorOneShot
                    | PodSource::DisappearedFromCacheOneShot => {
                        pod_reply(&running_pod(&pod_name, sandbox_id, Some(WORKER_NODE)))
                    }
                    PodSource::ReadsStall
                        if reply_created_at
                            .lock()
                            .unwrap()
                            .is_some_and(|at| at.elapsed() < TEST_DATA_PLANE_READY) =>
                    {
                        KubeReply::Hang
                    }
                    PodSource::OverloadFailsFast
                        if reply_created_at
                            .lock()
                            .unwrap()
                            .is_some_and(|at| at.elapsed() < TEST_DATA_PLANE_READY) =>
                    {
                        kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests")
                    }
                    PodSource::ReadsStall
                    | PodSource::OverloadFailsFast
                    | PodSource::Unschedulable
                    | PodSource::Deleted => kube_status(StatusCode::NOT_FOUND, "NotFound"),
                }
            }
            Method::DELETE if request.path == pod_path(&pod_name) => {
                kube_status(StatusCode::OK, "Success")
            }
            _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
        }
    });
    let node_api_pods = if source == PodSource::NodeReportsFirst {
        vec![super::hydration_and_suspend::node_api_pod(worker.port)]
    } else {
        Vec::new()
    };
    let mut substrate = substrate_with_inventory_caches_and_pool(
        node_api_pods,
        vec![worker_node()],
        db.pool.clone(),
    );
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(Vec::new());
    substrate.budgets = phase_ending_budgets(substrate.budgets);
    substrate.budgets.readiness_quiet_reflector = TEST_QUIET_REFLECTOR;
    substrate.budgets.readiness_pod_get = if source == PodSource::ReadsStall {
        // Longer than the phase has left, so the phase deadline preempts it.
        Duration::from_secs(1)
    } else {
        Duration::from_millis(100)
    };
    substrate.k8s.create_data_plane_ready_timeout = TEST_DATA_PLANE_READY;
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    request.wait_until_ready = false;

    let reflector = async {
        let name = loop {
            let name = pod_name.lock().unwrap().clone();
            if !name.is_empty() {
                break name;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let pending = starting_pod(&name, sandbox_id, Some(WORKER_NODE));
        match source {
            PodSource::ReflectorEvent => {
                feed.apply(pending);
                tokio::time::sleep(Duration::from_millis(25)).await;
                feed.apply(running_pod(&name, sandbox_id, Some(WORKER_NODE)));
            }
            PodSource::NodeReportsFirst => {
                let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
                    sandbox_id.to_string(),
                    "seed".into(),
                    "fc".into(),
                    "root".into(),
                    None,
                    false,
                )
                .with_allocation(1, &format!("{name}-uid"), "127.0.0.1");
                record.status = "running".into();
                worker
                    .sim
                    .insert_new(record)
                    .await
                    .expect("the node records the sandbox");
                feed.apply(pending);
            }
            PodSource::HealthyBusyReflectorSkipsOneShot => {
                // Deliver another pod's update inside the quiet window so the
                // reflector proves liveness, then deliver our pod well past
                // `TEST_QUIET_REFLECTOR`.
                feed.apply(starting_pod(
                    "space-sbx-other-pod",
                    SandboxId::new(),
                    Some(WORKER_NODE),
                ));
                tokio::time::sleep(TEST_QUIET_REFLECTOR + Duration::from_millis(80)).await;
                feed.apply(running_pod(&name, sandbox_id, Some(WORKER_NODE)));
            }
            PodSource::DisappearedFromCacheOneShot | PodSource::Deleted => {
                feed.apply(pending.clone());
                tokio::time::sleep(Duration::from_millis(25)).await;
                feed.delete(pending);
            }
            PodSource::Unschedulable => {
                let mut pod = starting_pod(&name, sandbox_id, None);
                pod.status.as_mut().expect("pod has a status").conditions =
                    Some(vec![k8s_openapi::api::core::v1::PodCondition {
                        type_: "PodScheduled".into(),
                        status: "False".into(),
                        reason: Some("Unschedulable".into()),
                        message: Some("0/2 nodes are available".into()),
                        ..Default::default()
                    }]);
                feed.apply(pod);
            }
            PodSource::QuietReflectorOneShot
            | PodSource::ReadsStall
            | PodSource::OverloadFailsFast => {}
        }
    };
    let (outcome, ()) = tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::future::join(Box::pin(substrate.create_execution(request)), reflector),
    )
    .await
    .map_err(|_| "the create must end within its budgets")?;

    let created_at = created_at.lock().unwrap().ok_or("the pod was created")?;
    let reads: Vec<Duration> = pod_reads
        .lock()
        .unwrap()
        .iter()
        .map(|read| read.saturating_duration_since(created_at))
        .collect();
    let watches: Vec<_> = requests_matching(&log, &Method::GET)
        .into_iter()
        .filter(|request| request.path == PODS_PATH)
        .collect();
    assert!(
        watches.is_empty(),
        "{source:?}: no per-create pod watch: {watches:?}"
    );
    let readiness_reads: Vec<Duration> = reads
        .iter()
        .copied()
        .filter(|read| *read < TEST_DATA_PLANE_READY)
        .collect();

    match source {
        PodSource::ReflectorEvent | PodSource::HealthyBusyReflectorSkipsOneShot => {
            let outcome = outcome?;
            assert!(outcome.binding.is_some(), "{source:?}: reflector binds pod");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(
                reads.is_empty(),
                "{source:?}: healthy reflector must issue zero pod GETs: {reads:?}"
            );
        }
        PodSource::NodeReportsFirst => {
            let outcome = outcome?;
            assert!(outcome.binding.is_some(), "node address binds pod");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(reads.is_empty(), "zero pod GETs: {reads:?}");
            let recorded: Option<String> = sqlx::query_scalar(sql!(
                "SELECT host(pod_ip) FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
            assert_eq!(recorded.as_deref(), Some("127.0.0.1"));
        }
        PodSource::QuietReflectorOneShot => {
            let outcome = outcome?;
            assert!(outcome.binding.is_some());
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            let first = *reads.first().ok_or("quiet-reflector one-shot must read")?;
            assert!(
                first >= TEST_QUIET_REFLECTOR,
                "quiet one-shot waits out readiness_quiet_reflector: {reads:?}"
            );
            // One readiness fallback GET, plus at most one post-readiness
            // placement read when `pod_store` still lacks the pod.
            assert!(
                reads.len() <= 2,
                "single-shot fallback, not a poll: {reads:?}"
            );
        }
        PodSource::DisappearedFromCacheOneShot => {
            let outcome = outcome?;
            assert!(outcome.binding.is_some());
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(reads.len() <= 2, "single disappearance check: {reads:?}");
        }
        PodSource::ReadsStall | PodSource::Deleted => {
            let error = outcome.err().ok_or("the pod never gets an IP")?;
            assert!(
                matches!(
                    error,
                    SubstrateError::Timeout {
                        phase: Some(WaitReadyTimeoutPhase::PodNeverReady),
                        ..
                    }
                ),
                "{source:?}: expected PodNeverReady timeout, got {error:?}"
            );
            assert_eq!(
                readiness_reads.len(),
                1,
                "{source:?}: single-shot fallback issues exactly one readiness GET: {reads:?}"
            );
        }
        PodSource::Unschedulable => {
            let error = outcome
                .err()
                .ok_or("an unschedulable pod fails the create")?;
            assert!(
                matches!(error, SubstrateError::Unschedulable { .. }),
                "expected Unschedulable, got {error:?}"
            );
            assert!(
                readiness_reads.is_empty(),
                "unschedulable verdict comes from reflector with zero readiness GETs: {reads:?}"
            );
        }
        PodSource::OverloadFailsFast => {
            let error = outcome
                .err()
                .ok_or("throttled fallback GET must fail fast")?;
            assert!(
                matches!(error, SubstrateError::Unavailable { .. }),
                "expected Unavailable on 429, got {error:?}"
            );
            assert_eq!(
                readiness_reads.len(),
                1,
                "overload fails fast on the single fallback GET: {reads:?}"
            );
            let deletes = requests_matching(&log, &Method::DELETE);
            assert_eq!(
                deletes.len(),
                1,
                "apiserver 429 must tear down the created pod instead of deferring as node-mgr unreachable"
            );
        }
    }
    Ok(())
}

// ── readiness failure ───────────────────────────────────────────────────────

/// How the readiness wait of a create whose pod was accepted fails.
#[derive(Debug, Clone, Copy)]
enum ReadinessFailure {
    /// Discovering the assigned node on a cache miss never answers. The IP
    /// readiness phase's deadline ends that wait, before the outer create
    /// budget would.
    NodeDiscoveryHangs,
    /// Every guest probe answers gRPC `Unavailable` until the command-ready
    /// phase times out.
    NodeMgrUnavailable,
    /// The guest never answers, and the create asked to wait for it far less
    /// than the substrate's own command-ready bound.
    GuestPastItsRequestedWait,
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
    /// The IP readiness phase's timeout.
    PodNeverReadyTimeout,
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
            Self::PodNeverReadyTimeout => matches!(
                error,
                SubstrateError::Timeout {
                    phase: Some(WaitReadyTimeoutPhase::PodNeverReady),
                    ..
                }
            ),
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
/// arm owns: a pod that is scheduled and reachable when the wait times out is
/// kept and returned as started; one that is terminating, or whose preserve
/// decision outlives its budget, is deleted with its execution retired; one
/// that vanished after the wait timed out, on a hung node discovery or after
/// node-mgr probes answered `Unavailable`, is left, with its claim, to
/// reconciliation. A contended read after the pod exists stays retryable only
/// when the cleanup delete was accepted.
#[rstest::rstest]
#[case::hung_node_discovery_preserves_a_reachable_pod(
    ReadinessFailure::NodeDiscoveryHangs,
    PodAfterFailure::ScheduledReachable,
    CleanupDelete::Accepted,
    ExpectedEnd::PreservedOnNode
)]
#[case::hung_node_discovery_tears_down_a_terminating_pod(
    ReadinessFailure::NodeDiscoveryHangs,
    PodAfterFailure::Terminating,
    CleanupDelete::Accepted,
    ExpectedEnd::TornDown(ExpectedError::PodNeverReadyTimeout)
)]
#[case::slow_preserve_decision_tears_down(
    ReadinessFailure::NodeDiscoveryHangs,
    PodAfterFailure::ScheduledOnUncachedNode,
    CleanupDelete::Accepted,
    ExpectedEnd::TornDown(ExpectedError::PodNeverReadyTimeout)
)]
#[case::hung_node_discovery_defers_cleanup_of_a_vanished_pod(
    ReadinessFailure::NodeDiscoveryHangs,
    PodAfterFailure::Absent,
    CleanupDelete::Accepted,
    ExpectedEnd::Deferred(ExpectedError::PodNeverReadyTimeout)
)]
#[case::a_create_waits_for_its_guest_only_as_long_as_it_asked(
    ReadinessFailure::GuestPastItsRequestedWait,
    PodAfterFailure::ScheduledReachable,
    CleanupDelete::Accepted,
    ExpectedEnd::PreservedOnNode
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
        // Readiness fails before any probe, or no probe answers.
        ReadinessFailure::NodeDiscoveryHangs
        | ReadinessFailure::LockContention
        | ReadinessFailure::GuestPastItsRequestedWait => ScriptedReply::Hang,
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
                    // of `creating`, so readiness can resolve the pod. No
                    // node yet: placement is recorded only after readiness.
                    pod_reply(&running_pod(&pod_name, sandbox_id, None))
                }
                Method::GET if Some(&request.path) == at_pod.as_ref() => {
                    let pod_name = pod_name.expect("the pod was created");
                    // Readiness makes the first read of the pod; the reads
                    // after it see the pod as it is once readiness failed.
                    let first_read = !state.readiness_failed.load(Ordering::SeqCst);
                    match failure {
                        ReadinessFailure::NodeDiscoveryHangs if first_read => {
                            state.readiness_failed.store(true, Ordering::SeqCst);
                            return pod_reply(&running_pod(
                                &pod_name,
                                sandbox_id,
                                Some(UNCACHED_NODE),
                            ));
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
        ReadinessFailure::NodeDiscoveryHangs
        | ReadinessFailure::NodeMgrUnavailable
        | ReadinessFailure::GuestPastItsRequestedWait => db.pool.clone(),
    };
    let mut substrate = substrate_on(pool, client, &worker);
    match failure {
        // A data-plane timeout far past the budget, so the IP phase ends on
        // the create budget's phase deadline.
        ReadinessFailure::NodeDiscoveryHangs => {
            substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(60);
        }
        // These end on their own phase; the create budget stays out of reach.
        ReadinessFailure::NodeMgrUnavailable => {
            substrate.budgets = phase_ending_budgets(substrate.budgets);
            substrate.k8s.create_command_ready_timeout = Duration::from_millis(500);
        }
        ReadinessFailure::LockContention => {
            substrate.budgets = phase_ending_budgets(substrate.budgets);
        }
        // Only the create's own, shorter wait can end it inside the test's
        // bound.
        ReadinessFailure::GuestPastItsRequestedWait => {
            substrate.budgets = phase_ending_budgets(substrate.budgets);
            substrate.k8s.create_command_ready_timeout = Duration::from_secs(60);
        }
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    request.wait_until_ready = true;
    if let ReadinessFailure::GuestPastItsRequestedWait = failure {
        request.command_ready_wait = Some(Duration::from_millis(200));
    }

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
    if let ReadinessFailure::GuestPastItsRequestedWait = failure {
        // A guest probe that outlived the create's 200 ms wait would hold it
        // for a whole probe timeout.
        assert!(
            elapsed < START_WAIT_COMMAND_PROBE_TIMEOUT,
            "the create's own wait ends it: {elapsed:?}"
        );
    }
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

// ── node refusal ────────────────────────────────────────────────────────────

const EVENTS_PATH: &str = "/api/v1/namespaces/space/events";

/// Kubelet's event for a sandbox whose disk is smaller than its template's
/// rootfs, as the shim and node-manager word it.
const INVALID_DISK_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = \
    InvalidArgument desc = failed to start sandbox \"0123\": failed to create containerd task: \
    failed to create shim task: sbx-manager Wake [space-err=InvalidArgument]: prepare sandbox \
    failed: code: 'Client specified an invalid argument', message: \"rootfs_size_mb=300 is \
    smaller than cloned rootfs size 451 MiB\"";

/// A refusal another attempt can get past.
const RETRYABLE_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = Unavailable \
    desc = failed to create shim task: sbx-manager Wake [space-err=Unavailable]: node-manager \
    is restarting";

const MEMORY_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = ResourceExhausted \
    desc = sbx-manager Wake [space-err=ResourceExhausted]: network allocate failed: \
    node memory admission refused: insufficient node memory";
const QUEUE_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = ResourceExhausted \
    desc = sbx-manager Wake [space-err=ResourceExhausted]: lifecycle queue full";

/// What the node records for the create's pod sandbox, and how the apiserver
/// answers the create's watch of those events.
#[derive(Debug, Clone, Copy, PartialEq)]
enum NodeRefusal {
    /// The node refuses the request as invalid.
    Invalid,
    /// The node refuses for a reason another attempt can get past.
    Retryable,
    /// The node refuses the request as invalid, and the apiserver throttles
    /// the first watch of the pod's events.
    InvalidAfterThrottledWatch,
    Memory,
    MemoryWithFlagOff,
    MemoryWithFlagUnset,
    QueueFull,
}

/// Invalid requests and opt-in memory refusals fail with the node's reason,
/// delete the pod and retire its execution. Other refusals leave kubelet to
/// retry until the readiness deadline. Events come from a pod-scoped watch,
/// including after the apiserver throttles it.
#[rstest::rstest]
#[case::invalid_request_fails_the_create(NodeRefusal::Invalid)]
#[case::other_refusals_wait_for_the_deadline(NodeRefusal::Retryable)]
#[case::a_throttled_watch_is_watched_again(NodeRefusal::InvalidAfterThrottledWatch)]
#[case::memory_refusal_is_retryable(NodeRefusal::Memory)]
#[case::memory_refusal_waits_with_flag_off(NodeRefusal::MemoryWithFlagOff)]
#[case::memory_refusal_waits_by_default(NodeRefusal::MemoryWithFlagUnset)]
#[case::queue_full_keeps_kubelet_retry(NodeRefusal::QueueFull)]
#[tokio::test(flavor = "current_thread")]
async fn create_handles_node_refusal(#[case] refusal: NodeRefusal) -> TestResult {
    let enabled = if matches!(
        refusal,
        NodeRefusal::Invalid | NodeRefusal::MemoryWithFlagOff
    ) {
        "false"
    } else {
        "true"
    };
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files_and(&[(
        "SPACE_CREATE_FAIL_FAST_ON_NODE_REJECTION",
        (refusal != NodeRefusal::MemoryWithFlagUnset).then(|| std::ffi::OsStr::new(enabled)),
    )]) {
        return Ok(());
    }
    let fails_fast = matches!(
        refusal,
        NodeRefusal::Invalid | NodeRefusal::InvalidAfterThrottledWatch | NodeRefusal::Memory
    );
    let message = match refusal {
        NodeRefusal::Invalid | NodeRefusal::InvalidAfterThrottledWatch => INVALID_DISK_REFUSAL,
        NodeRefusal::Memory | NodeRefusal::MemoryWithFlagOff | NodeRefusal::MemoryWithFlagUnset => {
            MEMORY_REFUSAL
        }
        NodeRefusal::QueueFull => QUEUE_REFUSAL,
        NodeRefusal::Retryable => RETRYABLE_REFUSAL,
    };
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        SPACED_WAIT_READY,
        ScriptedReply::Hang,
    )]))
    .await?;

    let pod_name: Arc<Mutex<Option<String>>> = Arc::default();
    let event_watches = Arc::new(AtomicUsize::new(0));
    let reply_pod_name = Arc::clone(&pod_name);
    let reply_event_watches = Arc::clone(&event_watches);
    let (client, log) = scripted_kube_client_async(move |request| {
        let pod_name = Arc::clone(&reply_pod_name);
        let event_watches = Arc::clone(&reply_event_watches);
        async move {
            let name = pod_name.lock().unwrap().clone();
            let pending = |name: &str| {
                pod_reply(&sandbox_pod(
                    name,
                    &format!("{name}-uid"),
                    sandbox_id,
                    1,
                    "Pending",
                    None,
                ))
            };
            match request.method {
                Method::POST if request.path == PODS_PATH => {
                    let name = requested_pod_name(&request);
                    *pod_name.lock().unwrap() = Some(name.clone());
                    pending(&name)
                }
                Method::GET if name.as_deref().map(pod_path).as_ref() == Some(&request.path) => {
                    pending(name.as_deref().expect("the pod was created"))
                }
                Method::GET if request.path == EVENTS_PATH => {
                    let name = name.expect("events are watched after the pod is created");
                    let watch = event_watches.fetch_add(1, Ordering::SeqCst);
                    if refusal == NodeRefusal::InvalidAfterThrottledWatch && watch == 0 {
                        return kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests");
                    }
                    KubeReply::WatchEvents(vec![serde_json::json!({
                        "type": "ADDED",
                        "object": {
                            "apiVersion": "v1",
                            "kind": "Event",
                            "metadata": { "name": format!("{name}.1"), "namespace": "space" },
                            "involvedObject": {
                                "kind": "Pod",
                                "name": name,
                                "uid": format!("{name}-uid"),
                            },
                            "reason": "FailedCreatePodSandBox",
                            "message": message,
                            "type": "Warning",
                        },
                    })])
                }
                Method::DELETE if name.as_deref().map(pod_path).as_ref() == Some(&request.path) => {
                    kube_status(StatusCode::OK, "Success")
                }
                _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
            }
        }
    });
    let mut substrate = substrate_on(db.pool.clone(), client, &worker);
    substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(60);
    if fails_fast {
        // Only the refusal can end the wait inside the test's bound.
        substrate.budgets = phase_ending_budgets(substrate.budgets);
    } else {
        substrate.budgets.create_ready = Duration::from_millis(500);
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        substrate.create_execution(inputs.create_request(sandbox_id, &template_ref)),
    )
    .await
    .map_err(|_| "the create must end")?;
    let elapsed = started.elapsed();
    let error = outcome.err().ok_or("the pod never got an IP")?;
    let pod_name = pod_name
        .lock()
        .unwrap()
        .clone()
        .expect("the pod was created");
    let event_reads: Vec<_> = requests_matching(&log, &Method::GET)
        .into_iter()
        .filter(|request| request.path == EVENTS_PATH)
        .collect();
    assert!(!event_reads.is_empty(), "the wait reads its pod's events");
    for read in &event_reads {
        let query = read.query.as_deref().unwrap_or_default();
        assert!(
            query.contains("watch=true")
                && query.contains("resourceVersion=0")
                && query.contains(&format!("{pod_name}-uid")),
            "each read is a watch of the pod's events from the apiserver's cache: {query}"
        );
    }
    if !fails_fast {
        assert!(
            matches!(error, SubstrateError::Timeout { .. }),
            "expected the readiness deadline, got {error:?}"
        );
        return Ok(());
    }

    match (&error, refusal) {
        (SubstrateError::Unavailable { message }, NodeRefusal::Memory) => {
            assert!(message.contains("node memory admission refused"));
        }
        (
            SubstrateError::InvalidRequest { message },
            NodeRefusal::Invalid | NodeRefusal::InvalidAfterThrottledWatch,
        ) => {
            assert!(message.contains("smaller than cloned rootfs size 451 MiB"));
        }
        _ => panic!("expected the node's refusal, got {error:?}"),
    }
    // Far inside the production readiness budget, with room for a throttled
    // watch's re-watch delay and a loaded runner. The watch assertion above,
    // not this bound, is what rules out polling.
    assert!(
        elapsed < Duration::from_secs(3),
        "the refusal, not the readiness budget, ends the create: {elapsed:?}"
    );
    let deletes = requests_matching(&log, &Method::DELETE);
    let [delete] = deletes.as_slice() else {
        panic!("expected one pod delete, got {deletes:?}");
    };
    assert_eq!(delete.path, pod_path(&pod_name));
    assert_eq!(
        active_execution_count(&db.pool, sandbox_id).await?,
        0,
        "the execution must be retired"
    );
    Ok(())
}

// ── joined create ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum OwnerPod {
    /// The owner's pod is running and its proxy reachable.
    Reachable,
    /// The owner's pod is reachable, but its guest never answers a create
    /// that waits for it.
    ReachableGuestSilent,
    /// The owner's pod never gets an IP.
    NeverAssigned,
}

/// A create that finds another create's activation in flight joins it rather
/// than creating a pod: it returns the owner's pod once that pod is
/// reachable, without a lease of its own, and when the owner's pod never
/// becomes reachable it times out within the create budget, leaving the
/// owner's execution and activation untouched. A create that waits for the
/// guest, and whose guest does not answer in time, returns the owner's
/// reachable pod as started but not command-ready rather than failing; its
/// caller would delete the sandbox the owner is starting on a failure.
#[rstest::rstest]
#[case::returns_the_owners_reachable_pod(OwnerPod::Reachable)]
#[case::keeps_the_owners_pod_when_its_guest_is_slow(OwnerPod::ReachableGuestSilent)]
#[case::times_out_without_disturbing_the_owner(OwnerPod::NeverAssigned)]
#[tokio::test(flavor = "current_thread")]
async fn joined_create_waits_on_the_owners_pod(#[case] owner_pod: OwnerPod) -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let worker = match owner_pod {
        OwnerPod::ReachableGuestSilent => {
            start_scripted_worker(ScriptedWorkerConfig::new([(
                SPACED_WAIT_READY,
                ScriptedReply::Hang,
            )]))
            .await?
        }
        OwnerPod::Reachable | OwnerPod::NeverAssigned => {
            start_scripted_worker(ScriptedWorkerConfig::new([])).await?
        }
    };
    let pod_name: Arc<Mutex<Option<String>>> = Arc::default();
    let reply_pod_name = Arc::clone(&pod_name);
    let (client, log) = scripted_kube_client_async(move |request| {
        let pod_name = reply_pod_name.lock().unwrap().clone();
        async move {
            match (request.method, pod_name) {
                (Method::GET, Some(pod_name)) if request.path == pod_path(&pod_name) => {
                    match owner_pod {
                        OwnerPod::Reachable | OwnerPod::ReachableGuestSilent => {
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
    if let OwnerPod::Reachable | OwnerPod::ReachableGuestSilent = owner_pod {
        substrate
            .store
            .persist_pod_metadata(
                sandbox_id,
                owner.generation,
                Some(&format!("{}-uid", owner.pod_name)),
                Some("127.0.0.1"),
                None,
            )
            .await?;
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    if let OwnerPod::ReachableGuestSilent = owner_pod {
        request.wait_until_ready = true;
        request.command_ready_wait = Some(Duration::from_millis(200));
    }

    let started = Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(10), substrate.create_execution(request))
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
        OwnerPod::ReachableGuestSilent => {
            let outcome = outcome
                .map_err(|error| format!("expected the owner's pod, not yet ready: {error:?}"))?;
            assert!(
                outcome.lease_completion.is_none(),
                "the lease is the owner's"
            );
            assert_eq!(outcome.initial_lifecycle, "started");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(!outcome.command_ready, "the guest never answered");
            assert!(outcome.binding.is_none());
            assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
            // A guest probe that outlived the create's 200 ms wait would hold
            // it for a whole probe timeout.
            let elapsed = started.elapsed();
            assert!(
                elapsed < START_WAIT_COMMAND_PROBE_TIMEOUT,
                "the create's own wait ends it: {elapsed:?}"
            );
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
