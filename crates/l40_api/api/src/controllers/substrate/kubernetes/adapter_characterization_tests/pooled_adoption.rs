//! `prepare_pooled_execution` and the per-sandbox setup it applies through
//! `apply_adopted_per_sandbox_setup`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use pplx_space_common_core::types::PodUid;
use pplx_space_sbx_mgr_client::proto as sbx;
use prost::Message as _;

use super::super::scripted_test_doubles::{
    CallObserver, ScriptedReply, ScriptedWorker, ScriptedWorkerConfig, kube_status,
    scripted_kube_client, start_scripted_worker,
};
use super::super::tests::{
    insert_test_sandbox_row, substrate_with_store_client_proxy_port_and_pod,
    substrate_with_unreachable_store,
};
use super::super::wait_ready::ConnectedCommandReadiness;
use super::*;
use crate::controllers::sandbox::pooled_prepare_error_is_reclaimable;
use crate::controllers::substrate::{PooledExecutionClaim, PooledExecutionClaimRequest};

const POOLED_POD_NAME: &str = "space-sbx-pooled-g1";
const POOLED_POD_UID: &str = "pooled-pod-uid";

const UPDATE: &str = "WorkerApi/UpdateSandbox";
const SET_NETWORK_POLICY: &str = "WorkerApi/SetSandboxNetworkPolicy";
const SET_AUTO_LIFECYCLE: &str = "WorkerApi/SetSandboxAutoLifecyclePolicy";
const WAKE: &str = "WorkerApiInternal/WakeSandbox";
/// The per-sandbox setup steps, in the order adoption applies them.
const SETUP_STEPS: [&str; 3] = [UPDATE, SET_NETWORK_POLICY, SET_AUTO_LIFECYCLE];

/// Zero-hint rate limits that spend the node-api client's whole retry count.
const RATE_LIMITS: usize =
    pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY.max_retries;

/// The status the node-api client returns when its own budget for `command`
/// runs out.
fn client_deadline(command: &str) -> tonic::Status {
    tonic::Status::deadline_exceeded(format!(
        "worker-node-api/{command} deadline exceeded while running RPC"
    ))
}

struct PooledInputs {
    create: CreateInputs,
    claim: PooledExecutionClaim,
}

impl PooledInputs {
    fn new() -> Self {
        let mut create = CreateInputs::new();
        create
            .env
            .insert("SANDBOX_TYPE".to_string(), "example".to_string());
        Self {
            create,
            claim: PooledExecutionClaim {
                claim_id: Uuid::now_v7(),
                pool_slot_id: Some(Uuid::now_v7()),
                sandbox_id: SandboxId::new(),
                content_hash: "sha256:seed".to_string(),
                pod_name: POOLED_POD_NAME.to_string(),
                pod_uid: Some(PodUid::parse(POOLED_POD_UID).expect("static pod uid parses")),
                execution_generation: 1,
                pod_ip: "127.0.0.1".to_string(),
                vcpu: None,
                memory_mib: None,
                disk_mib: None,
                disk_io_limits: Default::default(),
                active_node_id: None,
                route_identity: None,
            },
        }
    }

    fn request(&self) -> PooledExecutionClaimRequest<'_> {
        PooledExecutionClaimRequest {
            metric_identity: SandboxApiMetricIdentity::native("create"),
            actor: &self.create.actor,
            template: self.create.template.as_str(),
            content_hash: Some("sha256:seed"),
            template_release_id: None,
            runtime: "fc",
            vcpu: None,
            memory_mib: None,
            env: &self.create.env,
            auto_lifecycle: &self.create.auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 3,
            template_network_policy: &self.create.template_policy,
            template_network_policy_generation: 7,
            volume_encryption: false,
        }
    }

    /// The route-adoption Wake acknowledgement for this claim: its slot's
    /// route identity, optionally carrying the worker's node id.
    fn route_adoption_ack(&self, current_node_id: Option<&str>) -> ScriptedReply {
        let claim = &self.claim;
        ScriptedReply::message(&sbx::WakeResponse {
            sandbox_id: claim.sandbox_id.to_string(),
            transition: sbx::WakeTransition::Noop as i32,
            route_identity: Some(sbx::SpaceApiRouteIdentity {
                sandbox_id: None,
                route_generation: Some(1),
                current_node_id: current_node_id.map(str::to_string),
                execution_generation: Some(1),
                pod_uid: Some(POOLED_POD_UID.to_string()),
                activation_attempt_id: claim.pool_slot_id.map(|id| id.to_string()),
                substrate_kind: sbx::SandboxSubstrateKind::Kubernetes as i32,
            }),
        })
    }

    /// Every setup step accepted, and the route-adoption Wake acknowledged
    /// with the worker's node id.
    fn accepted(&self) -> Vec<(&'static str, ScriptedReply)> {
        vec![
            (
                UPDATE,
                ScriptedReply::message(&sbx::UpdateResponse::default()),
            ),
            (
                SET_NETWORK_POLICY,
                network_policy_confirmed(&self.create.template_policy),
            ),
            (
                SET_AUTO_LIFECYCLE,
                ScriptedReply::message(&sbx::SetAutoLifecyclePolicyResponse::default()),
            ),
            (
                WAKE,
                self.route_adoption_ack(Some(&Uuid::now_v7().to_string())),
            ),
        ]
    }
}

fn network_policy_confirmed(policy: &NetworkPolicyV1) -> ScriptedReply {
    ScriptedReply::message(&sbx::SetNetworkPolicyResponse {
        effective_policy: Some(pplx_space_net_common::proto::NetworkPolicyV1::from(policy)),
        ..Default::default()
    })
}

/// Replace the scripted reply for `method`.
fn with_reply(
    mut replies: Vec<(&'static str, ScriptedReply)>,
    method: &'static str,
    reply: ScriptedReply,
) -> Vec<(&'static str, ScriptedReply)> {
    replies.retain(|(scripted, _)| *scripted != method);
    replies.push((method, reply));
    replies
}

/// The readiness budget [`prepare`] hands the adoption.
const PREPARE_TIMEOUT: Duration = Duration::from_secs(10);

async fn prepare(
    substrate: &KubernetesSubstrate,
    inputs: &PooledInputs,
    wait_until_ready: bool,
) -> Result<(), ControllerError> {
    substrate
        .prepare_pooled_execution(
            SandboxApiMetricIdentity::native("create"),
            &inputs.request(),
            &inputs.claim,
            &inputs.create.auto_lifecycle,
            wait_until_ready,
            PREPARE_TIMEOUT,
        )
        .await
}

/// A substrate that reaches `worker` as the claimed pod and whose store dials
/// only a closed loopback port and fails fast, for paths that must not touch
/// the database.
fn substrate_without_store(worker: &ScriptedWorker) -> KubernetesSubstrate {
    let mut substrate = substrate_with_unreachable_store();
    substrate.k8s.sandbox_proxy_port = worker.port;
    substrate
}

fn assert_setup_precedes_wake(worker: &ScriptedWorker) {
    let wake = worker.first_call(WAKE).expect("the adoption must Wake");
    for step in SETUP_STEPS {
        let setup = worker
            .first_call(step)
            .unwrap_or_else(|| panic!("{step} must run"));
        assert!(setup < wake, "{step} must reach the node before the Wake");
    }
}

/// A pooled adoption applies the adopter's identity, the request-resolved
/// network policy, and the auto-lifecycle policy to the booted pod, all before
/// it adopts the route identity with a Wake.
#[tokio::test(flavor = "current_thread")]
async fn pooled_adoption_applies_per_sandbox_setup_before_route_adoption() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let inputs = PooledInputs::new();
    let worker = start_scripted_worker(ScriptedWorkerConfig::new(inputs.accepted())).await?;

    prepare(&substrate_without_store(&worker), &inputs, false).await?;

    assert_setup_precedes_wake(&worker);
    let [update] = worker
        .received(UPDATE)
        .try_into()
        .map_err(|_| "one update")?;
    let update = sbx::UpdateRequest::decode(update)?;
    assert_eq!(update.sandbox_id, inputs.claim.sandbox_id.to_string());
    assert_eq!(
        update.expected_current_owner.as_deref(),
        Some(pplx_space_common_core::types::ROOT_SANDBOX_OWNER.as_str()),
        "the owner flip is conditional on the pool's root owner"
    );
    assert_eq!(
        update.owner.as_deref(),
        Some(inputs.create.actor.sandbox_owner.as_str())
    );
    assert!(update.replace_process_env);
    assert_eq!(
        update.process_env.get("SANDBOX_TYPE").map(String::as_str),
        Some("example")
    );

    let [policy] = worker
        .received(SET_NETWORK_POLICY)
        .try_into()
        .map_err(|_| "one policy update")?;
    let policy = sbx::SetNetworkPolicyRequest::decode(policy)?;
    assert!(policy.inherit_template, "no override inherits the template");
    assert_eq!(
        policy.policy_generation, 0,
        "an inherited adoption keeps the pod's own generation counter"
    );
    assert!(
        policy.sync_to_gateway,
        "the gateway must confirm before wake"
    );

    let [lifecycle] = worker
        .received(SET_AUTO_LIFECYCLE)
        .try_into()
        .map_err(|_| "one lifecycle update")?;
    let lifecycle = sbx::SetAutoLifecyclePolicyRequest::decode(lifecycle)?;
    assert_eq!(lifecycle.sandbox_id, inputs.claim.sandbox_id.to_string());
    assert_eq!(
        lifecycle.authority,
        pplx_space_net_common::proto::PolicyAuthority::Cluster as i32
    );
    Ok(())
}

#[derive(Debug)]
enum SetupFailure {
    Update(tonic::Status),
    NetworkPolicy(tonic::Status),
    /// The node rate-limits the policy update with a zero retry hint until
    /// the client's retry count is spent, then fails it with this status.
    NetworkPolicyAfterRateLimits(tonic::Status),
    /// The gateway confirmed a policy other than the request-resolved one.
    NetworkPolicyMismatch,
    AutoLifecycle(tonic::Status),
}

/// Each failed setup step stops the adoption: no later step runs and no Wake
/// is sent, even though the node would acknowledge one. The status, and
/// whether the create path's classifier reads the error as reclaimable, are
/// what its in-place retry and re-claim decisions act on.
#[rstest::rstest]
#[case::owner_changed(
    SetupFailure::Update(tonic::Status::failed_precondition("sandbox owner changed")),
    StatusCode::CONFLICT,
    false
)]
#[case::update_deadline(
    SetupFailure::Update(client_deadline("update_sandbox")),
    StatusCode::SERVICE_UNAVAILABLE,
    true
)]
// What node-api relays when its sandbox-manager connection closes under a
// queued request. Without its local "transport error" source the retrying
// client does not replay it.
#[case::update_cancelled(
    SetupFailure::Update(tonic::Status::cancelled("operation was canceled")),
    StatusCode::REQUEST_TIMEOUT,
    false
)]
#[case::update_internal(
    SetupFailure::Update(tonic::Status::internal("sandbox update failed")),
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[case::policy_conflict(
    SetupFailure::NetworkPolicy(tonic::Status::failed_precondition("policy generation is stale")),
    StatusCode::CONFLICT,
    false
)]
#[case::policy_rejected(
    SetupFailure::NetworkPolicy(tonic::Status::invalid_argument("policy is invalid")),
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
// Zero-hint rate limits replay immediately but count against the client's
// retry limit, so the `Unavailable` that follows them comes back unreplayed
// well inside the setup budget.
#[case::policy_unavailable_after_rate_limits(
    SetupFailure::NetworkPolicyAfterRateLimits(tonic::Status::unavailable(
        "synchronous network policy update requires a live net-gateway"
    )),
    StatusCode::SERVICE_UNAVAILABLE,
    true
)]
#[case::policy_mismatch(
    SetupFailure::NetworkPolicyMismatch,
    StatusCode::SERVICE_UNAVAILABLE,
    false
)]
#[case::auto_lifecycle_deadline(
    SetupFailure::AutoLifecycle(client_deadline("set_sandbox_auto_lifecycle_policy")),
    StatusCode::SERVICE_UNAVAILABLE,
    true
)]
#[tokio::test(flavor = "current_thread")]
async fn pooled_adoption_stops_at_a_failed_setup_step(
    #[case] failure: SetupFailure,
    #[case] expected_status: StatusCode,
    #[case] reclaimable: bool,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let inputs = PooledInputs::new();
    assert_ne!(
        inputs.create.template_policy,
        NetworkPolicyV1::allow_all(),
        "the mismatch case needs a policy distinct from the request's"
    );
    let (failed_step, reply) = match &failure {
        SetupFailure::Update(status) => (UPDATE, ScriptedReply::Status(status.clone())),
        SetupFailure::NetworkPolicy(status) => {
            (SET_NETWORK_POLICY, ScriptedReply::Status(status.clone()))
        }
        SetupFailure::NetworkPolicyAfterRateLimits(status) => {
            let rate_limited = pplx_space_common_grpc::rate_limited_status(
                "UDS gRPC request-rate limit exceeded; retry_after_ms=0",
                Some(std::time::Duration::ZERO),
            );
            let replies = std::iter::repeat_n(ScriptedReply::Status(rate_limited), RATE_LIMITS)
                .chain([ScriptedReply::Status(status.clone())])
                .collect();
            (SET_NETWORK_POLICY, ScriptedReply::Sequence(replies))
        }
        SetupFailure::NetworkPolicyMismatch => (
            SET_NETWORK_POLICY,
            network_policy_confirmed(&NetworkPolicyV1::allow_all()),
        ),
        SetupFailure::AutoLifecycle(status) => {
            (SET_AUTO_LIFECYCLE, ScriptedReply::Status(status.clone()))
        }
    };
    let worker = start_scripted_worker(ScriptedWorkerConfig::new(with_reply(
        inputs.accepted(),
        failed_step,
        reply,
    )))
    .await?;

    let error = prepare(&substrate_without_store(&worker), &inputs, false)
        .await
        .expect_err("a failed setup step must fail the adoption");

    assert_eq!(error.status(), expected_status, "{failure:?}: {error:?}");
    assert_eq!(
        pooled_prepare_error_is_reclaimable(&error),
        reclaimable,
        "{failure:?}: reclaimable: {error}"
    );
    assert!(
        !worker.received(failed_step).is_empty(),
        "{failure:?}: the failing step must have been reached"
    );
    if let SetupFailure::NetworkPolicyAfterRateLimits(_) = &failure {
        assert_eq!(
            worker.received(SET_NETWORK_POLICY).len(),
            RATE_LIMITS + 1,
            "every rate limit was replayed and the final status was not"
        );
    }
    let later_steps = SETUP_STEPS
        .iter()
        .skip_while(|step| **step != failed_step)
        .skip(1);
    for later_step in later_steps.chain([&WAKE]) {
        assert!(
            worker.received(later_step).is_empty(),
            "{failure:?}: {later_step} must not run after a failed step"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum RouteAdoption {
    Succeeds,
    Fails,
    FailsReclaimably,
}

/// Route adoption succeeds only on a Wake acknowledgement. The acknowledged
/// identity must carry the node id when the worker advertises split operation
/// receipts; an older worker may omit it.
#[rstest::rstest]
#[case::capable_worker_must_report_its_node(true, false, RouteAdoption::Fails)]
#[case::older_worker_may_omit_its_node(false, false, RouteAdoption::Succeeds)]
#[case::wake_deadline_is_reclaimable(false, true, RouteAdoption::FailsReclaimably)]
#[tokio::test(flavor = "current_thread")]
async fn pooled_route_adoption_requires_a_complete_acknowledgement(
    #[case] advertise_operation_receipts: bool,
    #[case] wake_times_out: bool,
    #[case] expected: RouteAdoption,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let inputs = PooledInputs::new();
    let wake = if wake_times_out {
        ScriptedReply::Status(client_deadline("wake_sandbox"))
    } else {
        inputs.route_adoption_ack(None)
    };
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        advertise_operation_receipts,
        ..ScriptedWorkerConfig::new(with_reply(inputs.accepted(), WAKE, wake))
    })
    .await?;

    let outcome = prepare(&substrate_without_store(&worker), &inputs, false).await;

    match (expected, &outcome) {
        (RouteAdoption::Succeeds, Ok(())) => {}
        (RouteAdoption::Fails, Err(error)) => {
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error:?}");
            assert!(
                !pooled_prepare_error_is_reclaimable(error),
                "the create path must not reclaim: {error}"
            );
        }
        (RouteAdoption::FailsReclaimably, Err(error)) => {
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error:?}");
            assert!(
                pooled_prepare_error_is_reclaimable(error),
                "the create path must reclaim: {error}"
            );
        }
        (expected, outcome) => panic!("expected {expected:?}, got {outcome:?}"),
    }
    assert!(!worker.received(WAKE).is_empty(), "the Wake must be sent");
    Ok(())
}

/// The claimed warm-pool execution row as the pool claim binds it: generation
/// 1, `running` on [`WORKER_NODE`], routed by `route_activation_id` (the pool
/// slot id; `None` for a row bound before route ids were recorded).
async fn insert_claimed_pool_execution(
    pool: &PgPool,
    claim: &PooledExecutionClaim,
    route_activation_id: Option<Uuid>,
) -> TestResult {
    insert_test_sandbox_row(pool, claim.sandbox_id, "started").await?;
    insert_k8s_execution(
        pool,
        claim.sandbox_id,
        1,
        &claim.pod_name,
        POOLED_POD_UID,
        Some(&claim.pod_ip),
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET k8s_node_name = $2, route_activation_id = $3
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(claim.sandbox_id)
    .bind(WORKER_NODE)
    .bind(route_activation_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `(activation_attempt_id, route_activation_id)` of an execution.
type ActivationIds = (Option<Uuid>, Option<Uuid>);

/// The [`ActivationIds`] of the active execution.
async fn activation_ids(pool: &PgPool, sandbox_id: SandboxId) -> Result<ActivationIds, BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT activation_attempt_id, route_activation_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Started adoption leaves the route-adoption Wake to the worker's own
/// identity echo, so only the setup steps are scripted.
fn started_setup(inputs: &PooledInputs) -> Vec<(&'static str, ScriptedReply)> {
    let mut replies = inputs.accepted();
    replies.retain(|(method, _)| *method != WAKE);
    replies
}

fn substrate_with_claimed_pod_store(pool: &PgPool, worker: &ScriptedWorker) -> KubernetesSubstrate {
    let (client, _log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    substrate_with_store_client_proxy_port_and_pod(pool.clone(), client, worker.port, None)
}

/// A started adoption (`wait_until_ready = true`) refuses a worker that does
/// not advertise split operation receipts before applying any setup.
#[tokio::test(flavor = "current_thread")]
async fn started_pooled_adoption_refuses_a_worker_without_split_receipts() -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let inputs = PooledInputs::new();
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        advertise_operation_receipts: false,
        ..ScriptedWorkerConfig::new(started_setup(&inputs))
    })
    .await?;

    let mut substrate = substrate_without_store(&worker);
    substrate.budgets.pooled_split_receipt_wait = Duration::from_millis(300);

    let started = Instant::now();
    let outcome = prepare(&substrate, &inputs, true).await;

    assert!(outcome.is_err(), "an incapable worker must be refused");
    assert!(
        started.elapsed() < adapter::WARM_POOL_SETUP_RPC_TIMEOUT,
        "the substrate's split-receipt budget, not the production one, must end the wait"
    );
    let receipt_checks = worker
        .received("Health/Check")
        .into_iter()
        .filter_map(|message| tonic_health::pb::HealthCheckRequest::decode(message).ok())
        .filter(|check| {
            check.service == pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE
        })
        .count();
    assert!(
        receipt_checks > 0,
        "the refusal must follow the worker's NOT_SERVING receipt capability"
    );
    for method in SETUP_STEPS.into_iter().chain([WAKE]) {
        assert!(worker.received(method).is_empty(), "{method} must not run");
    }
    Ok(())
}

/// A started adoption that reaches command readiness releases the activation
/// attempt it took, including after a Wake that first reported the runtime
/// not ready; one whose Wake failed after being sent keeps it, because the
/// node may still be acting on that Wake. Setup always precedes the Wake.
#[rstest::rstest]
#[case::ready_releases_the_attempt(1, None, Ok(()))]
#[case::not_ready_wake_is_polled_until_ready(2, None, Ok(()))]
#[case::sent_wake_failure_keeps_the_attempt(
    1,
    Some(tonic::Status::invalid_argument("wake request rejected")),
    Err(StatusCode::CONFLICT)
)]
#[tokio::test(flavor = "current_thread")]
async fn started_pooled_adoption_releases_its_attempt_only_when_no_wake_may_be_pending(
    #[case] wake_succeeds_from_attempt: usize,
    #[case] wake_failure: Option<tonic::Status>,
    #[case] expected: Result<(), StatusCode>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let inputs = PooledInputs::new();
    insert_claimed_pool_execution(&db.pool, &inputs.claim, inputs.claim.pool_slot_id).await?;
    let mut replies = started_setup(&inputs);
    if let Some(status) = wake_failure {
        replies.push((WAKE, ScriptedReply::Status(status)));
    }
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        wake_succeeds_from_attempt,
        ..ScriptedWorkerConfig::new(replies)
    })
    .await?;

    let outcome = prepare(
        &substrate_with_claimed_pod_store(&db.pool, &worker),
        &inputs,
        true,
    )
    .await;

    assert_eq!(
        outcome.as_ref().map_err(ControllerError::status).copied(),
        expected,
        "{outcome:?}"
    );
    assert_setup_precedes_wake(&worker);
    let (attempt, route) = activation_ids(&db.pool, inputs.claim.sandbox_id).await?;
    assert_eq!(
        attempt.is_none(),
        expected.is_ok(),
        "attempt after {expected:?}: {attempt:?}"
    );
    assert_eq!(
        route, inputs.claim.pool_slot_id,
        "the pool slot keeps routing the execution"
    );
    Ok(())
}

/// A claimed execution bound before route ids were recorded gets the
/// adoption's activation attempt published as its route only after the node
/// acknowledged the Wake, which in turn lets that attempt be released.
#[tokio::test(flavor = "current_thread")]
async fn started_pooled_adoption_publishes_a_legacy_route_after_the_wake_ack() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let inputs = PooledInputs::new();
    insert_claimed_pool_execution(&db.pool, &inputs.claim, None).await?;
    // The execution's activation ids as each Wake reaches the node.
    let ids_at_wake: Arc<Mutex<Vec<ActivationIds>>> = Arc::default();
    let observed = Arc::clone(&ids_at_wake);
    let observe_pool = db.pool.clone();
    let sandbox_id = inputs.claim.sandbox_id;
    let observe_ids: CallObserver = Arc::new(move || {
        let observed = Arc::clone(&observed);
        let observe_pool = observe_pool.clone();
        Box::pin(async move {
            let ids = activation_ids(&observe_pool, sandbox_id)
                .await
                .expect("read the activation ids at Wake");
            observed.lock().unwrap().push(ids);
        })
    });
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        observers: HashMap::from([(WAKE, observe_ids)]),
        ..ScriptedWorkerConfig::new(started_setup(&inputs))
    })
    .await?;

    prepare(
        &substrate_with_claimed_pod_store(&db.pool, &worker),
        &inputs,
        true,
    )
    .await?;

    let at_wake = ids_at_wake.lock().unwrap().clone();
    let [(Some(wake_attempt), route_at_wake)] = at_wake[..] else {
        panic!("expected one Wake under an activation attempt: {at_wake:?}");
    };
    assert_eq!(
        route_at_wake, None,
        "the route must stay unpublished until the Wake is acknowledged"
    );
    let (attempt, route) = activation_ids(&db.pool, sandbox_id).await?;
    assert_eq!(
        route,
        Some(wake_attempt),
        "the acknowledged Wake's activation attempt must be published as the route"
    );
    assert_eq!(attempt, None, "the finished attempt must be released");
    Ok(())
}

const SPACED_WAIT_READY: &str = "WorkerApi/SpacedWaitReady";

/// A started adoption whose guest never reports command readiness after the
/// pool's Wake gives up at the adoption's own readiness cap, not at the end of
/// the create's readiness budget, so a fallthrough create reaches its cold
/// create without that stall. The timeout stays non-reclaimable.
#[tokio::test(flavor = "current_thread")]
async fn started_pooled_adoption_stops_waiting_for_a_never_ready_guest_at_its_cap() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let inputs = PooledInputs::new();
    insert_claimed_pool_execution(&db.pool, &inputs.claim, inputs.claim.pool_slot_id).await?;
    // The node holds every probe for a guest that is not ready.
    let worker = start_scripted_worker(ScriptedWorkerConfig::new(with_reply(
        started_setup(&inputs),
        SPACED_WAIT_READY,
        ScriptedReply::Hang,
    )))
    .await?;
    let mut substrate = substrate_with_claimed_pod_store(&db.pool, &worker);
    substrate.budgets.pooled_command_ready = Duration::from_millis(300);

    let started = Instant::now();
    let outcome = prepare(&substrate, &inputs, true).await;

    let Err(error) = outcome else {
        panic!("a guest that never reports ready must fail the adoption");
    };
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error:?}");
    assert!(
        !pooled_prepare_error_is_reclaimable(&error),
        "the create path must not spend another readiness wait on this pod: {error}"
    );
    assert!(
        started.elapsed() < PREPARE_TIMEOUT,
        "the adoption's readiness cap, not the create's budget, must end the wait"
    );
    assert!(!worker.received(SPACED_WAIT_READY).is_empty());
    Ok(())
}

/// How long the node holds the first readiness probe after the pool's Wake,
/// for a guest still slow to report ready.
#[derive(Debug, Clone, Copy)]
enum SlowFirstProbe {
    /// The guest reports ready late in the node's wait, before the client's
    /// deadline.
    AnswersReady,
    /// The guest is still not ready when the client's deadline ends the
    /// probe. The node keeps waiting, as sbx-mgr's retried host-agent wait
    /// does.
    OutlivesTheClientDeadline,
}

/// The node waits for the guest for the time a readiness probe asks for,
/// after connecting to the guest's host agent, so a guest slow after the
/// pool's Wake holds the first probe past that wait. The readiness poll waits
/// out a late answer, and keeps polling to its own deadline when the client's
/// deadline ends the probe, instead of failing the adoption on that one probe.
#[rstest::rstest]
#[case::answers_ready(SlowFirstProbe::AnswersReady, 1)]
#[case::outlives_the_client_deadline(SlowFirstProbe::OutlivesTheClientDeadline, 2)]
#[tokio::test(flavor = "current_thread")]
async fn pooled_readiness_poll_outlasts_a_slow_first_probe(
    #[case] first_probe: SlowFirstProbe,
    #[case] expected_probes: usize,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let client_deadline = START_WAIT_COMMAND_PROBE_TIMEOUT + START_WAIT_COMMAND_PROBE_CLIENT_MARGIN;
    let held_for = match first_probe {
        // Past the wait the probe asks for, as connecting to the host agent
        // adds.
        SlowFirstProbe::AnswersReady => {
            START_WAIT_COMMAND_PROBE_TIMEOUT + Duration::from_millis(500)
        }
        SlowFirstProbe::OutlivesTheClientDeadline => client_deadline + Duration::from_secs(1),
    };
    let ready = ScriptedReply::message(&sbx::SpacedWaitReadyResponse::default());
    let (probe_sent, mut probes) = tokio::sync::mpsc::unbounded_channel();
    let observe_probe: CallObserver = Arc::new(move || {
        let _ = probe_sent.send(());
        Box::pin(std::future::ready(()))
    });
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        observers: HashMap::from([(SPACED_WAIT_READY, observe_probe)]),
        ..ScriptedWorkerConfig::new([(
            SPACED_WAIT_READY,
            ScriptedReply::Sequence(Arc::from([
                ScriptedReply::Delayed(held_for, Arc::new(ready.clone())),
                ready,
            ])),
        )])
    })
    .await?;
    let substrate = substrate_without_store(&worker);
    let inputs = PooledInputs::new();
    let endpoint =
        KubernetesSubstrate::endpoint_for_pod_ip(PodIp::parse(&inputs.claim.pod_ip)?, worker.port)?;
    let mut node_api = NodeApiClient::with_service_name("worker-node-api", endpoint.clone());
    let binding = ExecutionBinding {
        execution_object_id: inputs.claim.pod_name.clone(),
        runtime_endpoint: endpoint,
        execution_generation: inputs.claim.execution_generation,
    };
    // Move the clock only through the `advance` calls below. A paused clock
    // still auto-advances while the runtime waits on I/O, unless a blocking
    // task is running, and could jump to the client's deadline while a reply
    // is in flight.
    let (release_clock, clock_released) = std::sync::mpsc::channel::<()>();
    // Blocks until `release_clock` is dropped.
    let clock_hold = tokio::task::spawn_blocking(move || while clock_released.recv().is_ok() {});
    tokio::time::pause();

    let poll_returned = std::cell::Cell::new(false);
    let (ready_binding, advanced) = tokio::join!(
        async {
            let ready_binding = Box::pin(substrate.poll_connected_client_until_command_ready(
                &mut node_api,
                inputs.claim.sandbox_id,
                SandboxApiMetricIdentity::native("create"),
                Duration::from_secs(10),
                binding,
                None,
            ))
            .await;
            poll_returned.set(true);
            ready_binding
        },
        async {
            // The node's delay and the client's deadline both start as the
            // first probe arrives. Timer deadlines round up to the next
            // millisecond.
            probes
                .recv()
                .await
                .ok_or("the first probe must reach the node")?;
            tokio::time::advance(held_for.min(client_deadline) + Duration::from_millis(1)).await;
            // A poll that has its answer returns on the reply alone. Move the
            // clock again only once real time shows the poll still waiting,
            // then step through its backoff until the next probe reaches the
            // node or the poll returns. Stepping while a reply travels could
            // reach the client's deadline and end the probe it answers.
            let grace_ends = std::time::Instant::now() + std::time::Duration::from_secs(1);
            for _ in 0..1_000 {
                if poll_returned.get() || probes.try_recv().is_ok() {
                    break;
                }
                tokio::task::spawn_blocking(|| {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                })
                .await?;
                if std::time::Instant::now() >= grace_ends {
                    tokio::time::advance(Duration::from_millis(50)).await;
                }
            }
            Ok::<_, BoxError>(())
        },
    );
    advanced?;
    let ready_binding = ready_binding?;
    drop(release_clock);
    clock_hold.await?;

    assert_eq!(ready_binding.execution_object_id, inputs.claim.pod_name);
    assert_eq!(worker.received(SPACED_WAIT_READY).len(), expected_probes);
    Ok(())
}

/// Every readiness probe after the pool's Wake comes back at once with a
/// status the poll treats as not ready yet, and the last backoff ends at the
/// readiness deadline. The poll gives up there and reports what the node
/// answered: a probe started with no budget left would end before it reached
/// the node, and its `DeadlineExceeded` would stand in for that answer.
#[tokio::test(flavor = "current_thread")]
async fn pooled_readiness_poll_reports_the_nodes_last_answer_at_the_deadline() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let not_started = tonic::Status::failed_precondition("sandbox not started");
    let (probe_sent, mut probes_at) = tokio::sync::mpsc::unbounded_channel();
    let observe_probe: CallObserver = Arc::new(move || {
        let _ = probe_sent.send(Instant::now());
        Box::pin(std::future::ready(()))
    });
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        observers: HashMap::from([(SPACED_WAIT_READY, observe_probe)]),
        ..ScriptedWorkerConfig::new([(SPACED_WAIT_READY, ScriptedReply::Status(not_started))])
    })
    .await?;
    let inputs = PooledInputs::new();
    let endpoint =
        KubernetesSubstrate::endpoint_for_pod_ip(PodIp::parse(&inputs.claim.pod_ip)?, worker.port)?;
    let mut node_api = NodeApiClient::with_service_name("worker-node-api", endpoint);
    // Room for a second probe but not for the backoff after it, which ends
    // at the deadline.
    let ready_timeout = START_WAIT_FAST_POLL_INTERVAL + Duration::from_millis(200);

    let deadline = Instant::now() + ready_timeout;
    let outcome = Box::pin(
        KubernetesSubstrate::probe_connected_client_until_command_ready(
            &mut node_api,
            inputs.claim.sandbox_id,
            SandboxApiMetricIdentity::native("create"),
            ready_timeout,
        ),
    )
    .await?;

    let ConnectedCommandReadiness::TimedOut(probes) = outcome else {
        panic!("a guest that never reports ready must time out");
    };
    let last_status = probes
        .last_status
        .ok_or("the probes' answers must be reported")?;
    assert_eq!(
        (last_status.code(), last_status.message()),
        (tonic::Code::FailedPrecondition, "sandbox not started"),
        "the timeout must report the node's last answer"
    );
    assert_eq!(
        probes.deadline_exceeded, 0,
        "no probe ran out of time; the node answered each at once"
    );
    assert!(!probes.saw_unavailable);
    let mut probe_count = 0;
    while let Ok(probe_at) = probes_at.try_recv() {
        assert!(probe_at < deadline, "a probe started after the deadline");
        probe_count += 1;
    }
    assert_eq!(probe_count, worker.received(SPACED_WAIT_READY).len());
    assert!(probe_count > 0);
    Ok(())
}
