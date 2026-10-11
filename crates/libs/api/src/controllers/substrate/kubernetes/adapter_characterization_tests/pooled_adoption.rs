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
            Duration::from_secs(10),
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
#[case::update_cancelled(
    SetupFailure::Update(tonic::Status::cancelled("Timeout expired")),
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
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let inputs = PooledInputs::new();
    let worker = start_scripted_worker(ScriptedWorkerConfig {
        advertise_operation_receipts: false,
        ..ScriptedWorkerConfig::new(started_setup(&inputs))
    })
    .await?;

    let outcome = prepare(&substrate_without_store(&worker), &inputs, true).await;

    assert!(outcome.is_err(), "an incapable worker must be refused");
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
