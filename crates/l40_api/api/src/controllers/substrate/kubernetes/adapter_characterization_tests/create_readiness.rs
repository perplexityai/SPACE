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
/// apiserver answers a direct read of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PodSource {
    /// The shared reflector reports the pod running with an IP.
    ReflectorEvent,
    /// The reflector reports the pod bound to its node, whose sandbox
    /// manager reports the pod's address before kubelet publishes it.
    NodeReportsFirst,
    /// The reflector stays healthy (other pods keep updating) while this pod
    /// stays `Pending` without an IP until the phase deadline expires.
    HealthyReflectorPendingUntilTimeout,
    /// The reflector delivers nothing at all, as a stalled watch does; a
    /// degraded-mode fallback read finds the pod running.
    ReflectorStalls,
    /// The reflector's watch stream faults right after delivering another pod's
    /// event; degraded-mode fallback reads the pod without waiting for silence.
    ReflectorWatchFailing,
    /// The reflector is silent and fallback reads find the pod still `Pending`.
    NeverAssigned,
    /// The reflector is silent, `readiness_pod_get` exceeds the phase budget,
    /// and the phase deadline cuts the stalled read.
    ReadsStall,
    /// The reflector is silent and the fallback read exceeds `readiness_pod_get`
    /// before the phase deadline, surfacing `503 Unavailable`.
    FallbackReadTimesOut,
    /// The reflector is silent and `kube-apiserver` throttles (`429`) the
    /// fallback read, surfacing `503 Unavailable`.
    FallbackReadThrottled,
    /// The reflector is silent and `kube-apiserver` returns `503` on the
    /// fallback read, surfacing `503 Unavailable` without a duplicate cleanup GET.
    FallbackReadServerError,
    /// The reflector reports the pod unschedulable.
    Unschedulable,
    /// The reflector reports the pod deleted while staying healthy; readiness
    /// confirms 404 once and fails fast instead of waiting out the phase deadline.
    Deleted,
}

/// How long the reflector may go without a watch delivery before readiness
/// treats it as degraded and allows paced fallback reads.
const TEST_QUIET_REFLECTOR: Duration = Duration::from_millis(180);

/// The IP phase's deadline in [`create_learns_its_pod_ip_without_a_watch_or_poll_of_its_own`]:
/// long enough to observe multiple degraded-reflector intervals while keeping
/// every test case well below the 2-second CI test budget.
const TEST_DATA_PLANE_READY: Duration = Duration::from_millis(750);

/// A create waits for its pod's IP through the process's shared sandbox-pod
/// reflector with zero per-create watches and zero uncached `GET`s while the
/// reflector is healthy — even when the pod stays `Pending` all the way to the
/// readiness timeout. Only when the reflector is silent past
/// `readiness_quiet_reflector` or its watch has faulted does the wait issue
/// paced degraded-mode `GET`s, failing fast with `Unavailable` on apiserver
/// read timeouts, `429` throttling, or `5xx` errors.
#[rstest::rstest]
#[case::a_reflector_update_binds_the_pod(PodSource::ReflectorEvent)]
#[case::the_node_reported_address_binds_the_pod(PodSource::NodeReportsFirst)]
#[case::a_healthy_reflector_emits_zero_gets_even_when_pending_times_out(
    PodSource::HealthyReflectorPendingUntilTimeout
)]
#[case::a_stalled_reflector_is_covered_by_a_paced_fallback_read(PodSource::ReflectorStalls)]
#[case::a_failing_watch_triggers_degraded_fallback_read(PodSource::ReflectorWatchFailing)]
#[case::degraded_fallback_reads_are_paced_when_never_assigned(PodSource::NeverAssigned)]
#[case::the_phase_deadline_preempts_a_stalled_read(PodSource::ReadsStall)]
#[case::a_timed_out_fallback_read_fails_fast_as_unavailable(PodSource::FallbackReadTimesOut)]
#[case::a_throttled_fallback_read_fails_fast_as_unavailable(PodSource::FallbackReadThrottled)]
#[case::a_5xx_fallback_read_fails_fast_as_unavailable(PodSource::FallbackReadServerError)]
#[case::an_unschedulable_pod_fails_the_create(PodSource::Unschedulable)]
#[case::a_deleted_pod_confirms_once_and_fails_fast(PodSource::Deleted)]
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
    // The node's report reaches the wait through its node-api pod.
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
    let proxy_wait_started_at = Arc::clone(&substrate.readiness_test_hooks.proxy_wait_started_at);
    let proxy_wait_finished_at = Arc::clone(&substrate.readiness_test_hooks.proxy_wait_finished_at);
    let reply_proxy_wait_started = Arc::clone(&proxy_wait_started_at);
    let ip_wait_iterations = Arc::clone(&substrate.readiness_test_hooks.ip_wait_iterations);
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
                    PodSource::ReflectorStalls | PodSource::ReflectorWatchFailing => {
                        pod_reply(&running_pod(&pod_name, sandbox_id, Some(WORKER_NODE)))
                    }
                    PodSource::HealthyReflectorPendingUntilTimeout | PodSource::NeverAssigned => {
                        pod_reply(&starting_pod(&pod_name, sandbox_id, Some(WORKER_NODE)))
                    }
                    // Only the readiness wait's reads stall; the failed
                    // create's teardown finds the pod gone.
                    PodSource::ReadsStall
                        if reply_proxy_wait_started
                            .lock()
                            .unwrap()
                            .or_else(|| *reply_created_at.lock().unwrap())
                            .is_some_and(|at| at.elapsed() < TEST_DATA_PLANE_READY) =>
                    {
                        KubeReply::Hang
                    }
                    PodSource::ReadsStall => kube_status(StatusCode::NOT_FOUND, "NotFound"),
                    PodSource::FallbackReadTimesOut => KubeReply::Hang,
                    PodSource::FallbackReadThrottled => {
                        kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests")
                    }
                    PodSource::FallbackReadServerError => {
                        kube_status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable")
                    }
                    PodSource::ReflectorEvent
                    | PodSource::NodeReportsFirst
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
    substrate.k8s.client = client;
    substrate.k8s.sandbox_proxy_port = worker.port;
    let (mut substrate, mut feed) = substrate.with_pod_cache_feed(Vec::new());
    substrate.budgets = phase_ending_budgets(substrate.budgets);
    substrate.budgets.readiness_pod_get = if source == PodSource::ReadsStall {
        // Longer than the phase has left, so the deadline cuts the read.
        Duration::from_secs(2)
    } else {
        Duration::from_millis(100)
    };
    substrate.budgets.readiness_quiet_reflector = TEST_QUIET_REFLECTOR;
    *substrate
        .readiness_test_hooks
        .fallback_jitter
        .lock()
        .unwrap() = Some(0.5);
    let data_plane_ready = TEST_DATA_PLANE_READY;
    substrate.k8s.create_data_plane_ready_timeout = data_plane_ready;
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    request.wait_until_ready = false;

    // Stands in for the reflector's watch: reports the created pod's
    // progress once the create's wait is under way.
    let reflector = async {
        let name = loop {
            let name = pod_name.lock().unwrap().clone();
            if !name.is_empty() {
                break name;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let pending = starting_pod(&name, sandbox_id, Some(WORKER_NODE));
        let other = starting_pod("space-sbx-other-g1", SandboxId::new(), Some(WORKER_NODE));
        let mut watch_faulted_at = None;
        match source {
            PodSource::ReflectorEvent => {
                feed.apply(pending);
                tokio::time::sleep(Duration::from_millis(30)).await;
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
            PodSource::Deleted => {
                while proxy_wait_started_at.lock().unwrap().is_none()
                    && proxy_wait_finished_at.lock().unwrap().is_none()
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                if proxy_wait_finished_at.lock().unwrap().is_none() {
                    feed.apply(pending.clone());
                    let seen_iter = ip_wait_iterations.load(Ordering::SeqCst);
                    while ip_wait_iterations.load(Ordering::SeqCst) <= seen_iter
                        && proxy_wait_finished_at.lock().unwrap().is_none()
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    if proxy_wait_finished_at.lock().unwrap().is_none() {
                        feed.delete(pending);
                    }
                }
            }
            PodSource::HealthyReflectorPendingUntilTimeout => {
                feed.apply(pending);
                while proxy_wait_finished_at.lock().unwrap().is_none() {
                    feed.apply(other.clone());
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            }
            PodSource::ReflectorWatchFailing => {
                while proxy_wait_started_at.lock().unwrap().is_none()
                    && proxy_wait_finished_at.lock().unwrap().is_none()
                {
                    feed.apply(other.clone());
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                // Keep unrelated watch deliveries flowing past `TEST_QUIET_REFLECTOR`,
                // then stamp one more delivery immediately before faulting the
                // watch so a silence-only fallback from that delivery would have
                // to wait at least another `0.8 * TEST_QUIET_REFLECTOR` (`144ms`).
                for _ in 0..4 {
                    if proxy_wait_finished_at.lock().unwrap().is_some() {
                        break;
                    }
                    feed.apply(other.clone());
                    tokio::time::sleep(Duration::from_millis(55)).await;
                }
                if proxy_wait_finished_at.lock().unwrap().is_none() {
                    feed.apply(other);
                    watch_faulted_at = Some(Instant::now());
                    feed.mark_watch_error();
                }
            }
            PodSource::ReflectorStalls
            | PodSource::NeverAssigned
            | PodSource::ReadsStall
            | PodSource::FallbackReadTimesOut
            | PodSource::FallbackReadThrottled
            | PodSource::FallbackReadServerError => {}
        }
        watch_faulted_at
    };
    let finished_for_create = Arc::clone(&proxy_wait_finished_at);
    let create_fut = async move {
        let out = substrate.create_execution(request).await;
        let mut slot = finished_for_create.lock().unwrap();
        if slot.is_none() {
            *slot = Some(Instant::now());
        }
        out
    };
    let started_at = Instant::now();
    let (outcome, watch_faulted_at) = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join(Box::pin(create_fut), reflector),
    )
    .await
    .map_err(|_| "the create must end within its budgets")?;
    let total_elapsed = started_at.elapsed();

    let created_at = created_at.lock().unwrap().ok_or("the pod was created")?;
    let phase_anchor = proxy_wait_started_at.lock().unwrap().unwrap_or(created_at);
    let phase_elapsed = proxy_wait_finished_at
        .lock()
        .unwrap()
        .map_or(total_elapsed, |end| {
            end.saturating_duration_since(phase_anchor)
        });
    let raw_reads: Vec<Instant> = pod_reads.lock().unwrap().clone();
    let reads: Vec<Duration> = raw_reads
        .iter()
        .map(|read| read.saturating_duration_since(created_at))
        .collect();
    let phase_reads: Vec<Duration> = raw_reads
        .iter()
        .map(|read| read.saturating_duration_since(phase_anchor))
        .collect();
    let watches: Vec<_> = requests_matching(&log, &Method::GET)
        .into_iter()
        .filter(|request| request.path == PODS_PATH)
        .collect();
    assert!(
        watches.is_empty(),
        "{source:?}: no per-create pod watch: {watches:?}"
    );
    match source {
        PodSource::ReflectorEvent => {
            let outcome = outcome?;
            assert!(
                outcome.binding.is_some(),
                "the reflector's IP binds the pod"
            );
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert_eq!(reads, Vec::<Duration>::new(), "no per-create pod read");
        }
        PodSource::NodeReportsFirst => {
            let outcome = outcome?;
            assert!(
                outcome.binding.is_some(),
                "the node's address binds the pod"
            );
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert_eq!(reads, Vec::<Duration>::new(), "no per-create pod read");
            let recorded: Option<String> = sqlx::query_scalar(sql!(
                "SELECT host(pod_ip) FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
            assert_eq!(
                recorded.as_deref(),
                Some("127.0.0.1"),
                "the confirmed node address is recorded"
            );
        }
        PodSource::HealthyReflectorPendingUntilTimeout => {
            let error = outcome.err().ok_or("pending pod times out")?;
            assert!(
                matches!(
                    error,
                    SubstrateError::Timeout {
                        phase: Some(WaitReadyTimeoutPhase::PodNeverReady),
                        ..
                    }
                ),
                "expected PodNeverReady timeout: {error:?}"
            );
            assert_eq!(
                reads,
                Vec::<Duration>::new(),
                "healthy reflector must emit zero uncached GETs across readiness and cleanup"
            );
            let deletes = requests_matching(&log, &Method::DELETE);
            assert_eq!(
                deletes.len(),
                1,
                "timed-out pod still cached as Pending is cleaned up via DELETE"
            );
        }
        PodSource::ReflectorStalls => {
            let outcome = outcome?;
            assert!(
                outcome.binding.is_some(),
                "degraded fallback read binds the pod"
            );
            let wait_started_at = proxy_wait_started_at
                .lock()
                .unwrap()
                .ok_or("readiness entry timestamp recorded")?;
            let first = raw_reads
                .first()
                .ok_or("silent reflector reads the pod")?
                .saturating_duration_since(wait_started_at);
            assert!(
                first >= TEST_QUIET_REFLECTOR - Duration::from_millis(15)
                    && first < TEST_QUIET_REFLECTOR + Duration::from_millis(250),
                "a silent reflector waits out the quiet interval from readiness entry before reading: first={first:?}, phase_reads={phase_reads:?}"
            );
            assert!(
                reads.len() <= 2,
                "paced fallback read, not a poll: {reads:?}"
            );
        }
        PodSource::ReflectorWatchFailing => {
            let outcome = outcome?;
            assert!(
                outcome.binding.is_some(),
                "faulted watch fallback read binds the pod"
            );
            let faulted_at = watch_faulted_at.ok_or("watch fault timestamp recorded")?;
            let first_instant = *raw_reads.first().ok_or("faulted watch reads the pod")?;
            assert!(
                first_instant >= faulted_at,
                "healthy watch must emit zero GETs before mark_watch_error(): {reads:?}"
            );
            let fault_delay = first_instant.saturating_duration_since(faulted_at);
            assert!(
                fault_delay < Duration::from_millis(120),
                "watch error must trigger fallback promptly without waiting another quiet window ({TEST_QUIET_REFLECTOR:?}): fault_delay={fault_delay:?}, reads={reads:?}"
            );
            assert!(
                reads.len() <= 2,
                "paced fallback read, not a poll: {reads:?}"
            );
        }
        PodSource::NeverAssigned | PodSource::ReadsStall => {
            let error = outcome.err().ok_or("the pod never gets an IP")?;
            assert!(
                matches!(
                    error,
                    SubstrateError::Timeout {
                        phase: Some(WaitReadyTimeoutPhase::PodNeverReady),
                        ..
                    }
                ),
                "{source:?}: expected the IP phase to expire: {error:?}"
            );
            let readiness_reads: Vec<_> = phase_reads
                .iter()
                .filter(|read| **read < data_plane_ready)
                .collect();
            let expected = if source == PodSource::ReadsStall {
                1..=1
            } else {
                2..=5
            };
            assert!(
                expected.contains(&readiness_reads.len()),
                "{source:?}: degraded fallback reads are paced, never polled: {reads:?}"
            );
            if source == PodSource::ReadsStall {
                assert!(
                    phase_elapsed < Duration::from_millis(1500),
                    "phase deadline must preempt the 2s stalled GET: {phase_elapsed:?}"
                );
            } else {
                let min_gap = TEST_QUIET_REFLECTOR
                    .mul_f64(0.8)
                    .saturating_sub(Duration::from_millis(40));
                assert!(
                    readiness_reads
                        .windows(2)
                        .all(|pair| pair[1].saturating_sub(*pair[0]) >= min_gap),
                    "consecutive fallback GETs must respect quiet-reflector spacing ({min_gap:?}): {readiness_reads:?}"
                );
            }
        }
        PodSource::FallbackReadTimesOut
        | PodSource::FallbackReadThrottled
        | PodSource::FallbackReadServerError => {
            let error = outcome
                .err()
                .ok_or("apiserver failure on fallback read fails fast")?;
            assert!(
                matches!(error, SubstrateError::Unavailable { .. }),
                "{source:?}: expected retryable 503 Unavailable, got: {error:?}"
            );
            assert_eq!(
                reads.len(),
                1,
                "{source:?}: must fail fast after one fallback GET with no duplicate cleanup GET: {reads:?}"
            );
            assert!(
                phase_elapsed < data_plane_ready,
                "{source:?}: must fail fast before phase deadline ({phase_elapsed:?} vs {data_plane_ready:?})"
            );
            let deletes = requests_matching(&log, &Method::DELETE);
            assert_eq!(deletes.len(), 1, "{source:?}: cleans up created pod");
        }
        PodSource::Unschedulable => {
            let error = outcome
                .err()
                .ok_or("an unschedulable pod fails the create")?;
            assert!(
                matches!(error, SubstrateError::Unschedulable { .. }),
                "expected the scheduler's verdict: {error:?}"
            );
            assert_eq!(
                reads,
                Vec::<Duration>::new(),
                "the verdict comes from the reflector without an uncached GET"
            );
        }
        PodSource::Deleted => {
            let error = outcome.err().ok_or("a deleted pod fails the create")?;
            assert!(
                matches!(error, SubstrateError::Failed { .. }),
                "expected deleted pod to fail fast instead of timing out: {error:?}"
            );
            assert!(
                phase_elapsed < data_plane_ready,
                "deleted pod must fail fast before phase deadline ({phase_elapsed:?} vs {data_plane_ready:?})"
            );
            assert_eq!(
                reads.len(),
                1,
                "healthy reflector confirms 404 once on pod deletion: {reads:?}"
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
/// answers the process's watch of those events.
#[derive(Debug, Clone, Copy, PartialEq)]
enum NodeRefusal {
    /// The node refuses the request as invalid.
    Invalid,
    /// The node refuses for a reason another attempt can get past.
    Retryable,
    /// The node refuses the request as invalid, and the apiserver throttles
    /// the first watch of the namespace's refusals.
    InvalidAfterThrottledWatch,
    Memory,
    MemoryWithFlagOff,
    MemoryWithFlagUnset,
    QueueFull,
}

/// Invalid requests and opt-in memory refusals fail with the node's reason,
/// delete the pod and retire its execution. Other refusals leave kubelet to
/// retry until the readiness deadline. Events come from the process's one
/// watch of the namespace's refusals, including after the apiserver throttles
/// it, and the create reads none of its own.
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
                    let watching = request
                        .query
                        .as_deref()
                        .is_some_and(|query| query.contains("watch=true"));
                    if !watching {
                        return KubeReply::Json(
                            StatusCode::OK,
                            serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "EventList",
                                "metadata": { "resourceVersion": "1" },
                                "items": [],
                            }),
                        );
                    }
                    let watch = event_watches.fetch_add(1, Ordering::SeqCst);
                    if refusal == NodeRefusal::InvalidAfterThrottledWatch && watch == 0 {
                        return kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests");
                    }
                    let first_refusal_watch =
                        usize::from(refusal == NodeRefusal::InvalidAfterThrottledWatch);
                    if watch > first_refusal_watch {
                        return KubeReply::Hang;
                    }
                    // The watch opened before the create: the node's refusal
                    // arrives on it once the pod exists.
                    let name = loop {
                        if let Some(name) = pod_name.lock().unwrap().clone() {
                            break name;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    };
                    KubeReply::WatchEvents(vec![serde_json::json!({
                        "type": "ADDED",
                        "object": {
                            "apiVersion": "v1",
                            "kind": "Event",
                            "metadata": {
                                "name": format!("{name}.1"),
                                "namespace": "space",
                                "resourceVersion": "2",
                            },
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
    let mut substrate =
        substrate_on(db.pool.clone(), client, &worker).with_pod_sandbox_rejection_watch();
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
    substrate.shutdown_token.cancel();
    let event_reads: Vec<_> = requests_matching(&log, &Method::GET)
        .into_iter()
        .filter(|request| request.path == EVENTS_PATH)
        .collect();
    assert!(!event_reads.is_empty(), "the process watches refusals");
    for read in &event_reads {
        let query = read.query.as_deref().unwrap_or_default();
        assert!(
            query.contains("fieldSelector=reason%3DFailedCreatePodSandBox")
                && !query.contains(&pod_name),
            "every read is the process's watch, none the create's own: {query}"
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
    // watch's backoff and a loaded runner. The read assertion above, not this
    // bound, is what rules out polling.
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

mod joined;
