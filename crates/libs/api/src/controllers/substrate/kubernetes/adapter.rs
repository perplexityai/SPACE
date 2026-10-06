use pplx_space_common_core::types::SandboxId;

use super::per_sandbox_setup::{NetworkPolicySetup, NodeFilesystemSetup, PerSandboxSetup};
use super::pool::pooled_route_activation_id;
use super::store::{ActiveExecutionStatusRow, PlacementRetryDisposition, SuspendExecutionClaim};
use super::support::*;
use super::wait_ready::{CommandReadyPod, PodProxyWaitTarget};
use super::*;
use crate::controllers::db_contention_error;
use crate::controllers::substrate::DbContext;
use crate::controllers::substrate::PooledClaimRefusal;
use crate::controllers::substrate::{
    AdmissionPoolClaim, AdmittedPoolClaim, AdmittedPoolClaimLookup, CreateBootMode,
    PooledExecutionClaim, PooledExecutionClaimAttempt, PooledExecutionClaimRequest,
    PooledPrepareFailureCleanup, PublishedLocalArtifact, create_wait_should_preserve_node_work,
    enqueue_artifact_backup_with_client,
};
use crate::models::{ClusterPoolConfigRequest, ClusterPoolConfigResponse};
use crate::node_group_placement::{
    NodeGroupPlacementResolveError, PlacementSubject, ResolvePath as NodeGroupResolvePath,
    ResolvedPlacement,
};
use pplx_space_cluster_pool::{OpenPoolSlotDeleteLock, WarmPoolStore};
use pplx_space_common_core::{
    sql,
    types::{
        ArtifactKind, AutoLifecyclePolicy, BackupSource, ExecutionGeneration, NodeAddress, NodeId,
        NodeManagedBy as SpaceNodeManagedBy, PodUid, PolicyAuthority, SandboxLifecycle,
        SandboxTransitionTargetState,
    },
};
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
};
use rand::RngExt;
use sqlx::{Postgres, Transaction};

const MAX_STALE_POOL_CLAIMS_PER_REQUEST: usize = 3;
const NODE_API_ENDPOINT_RESOLVE_TIMEOUT: Duration = Duration::from_secs(15);
const NODE_API_ENDPOINT_RESOLVE_RETRY_INTERVAL: Duration = Duration::from_millis(250);
pub(super) const WARM_POOL_SETUP_RPC_TIMEOUT: Duration = Duration::from_secs(5);
// Planned teardown waits for the exact snapshot upload to become remotely
// durable before retiring the pod. Full-codec artifacts can outlive the
// generic node-RPC budget, so this path needs its own outer deadline.
const TEARDOWN_SUSPEND_DURABILITY_BUDGET: Duration = Duration::from_secs(300);
const MAX_LOGGED_SUSPEND_PENDING_REASON_BYTES: usize = 512;
const MAX_SUSPEND_PENDING_REASON_LOGS: u8 = 3;
const SUSPEND_PENDING_REASON_LOG_INTERVAL: Duration = Duration::from_secs(30);
const NETWORK_SLOT_CAPACITY_UNAVAILABLE_REASON: &str = "network_slot_capacity_unavailable";
const SUSPEND_TRIGGER_METADATA: &str = "x-space-suspend-trigger";

pub(super) fn pooled_route_identity_adoption_request(
    claim: &PooledExecutionClaim,
) -> Result<pplx_space_sbx_mgr_client::proto::WakeRequest, ControllerError> {
    let pool_slot_id = pooled_route_activation_id(claim)?;
    pooled_wake_request(
        claim,
        pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(pool_slot_id),
        pplx_space_sbx_mgr_client::proto::WakeTargetState::Paused,
    )
}

fn pooled_wake_request(
    claim: &PooledExecutionClaim,
    activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    target_state: pplx_space_sbx_mgr_client::proto::WakeTargetState,
) -> Result<pplx_space_sbx_mgr_client::proto::WakeRequest, ControllerError> {
    let pod_uid = claim.pod_uid.as_ref().ok_or_else(|| {
        ControllerError::internal("Kubernetes pooled claim is missing its pod UID route")
    })?;
    Ok(pplx_space_sbx_mgr_client::proto::WakeRequest {
        implicit_wake_source: 0,
        sandbox_id: claim.sandbox_id.to_string(),
        blocking: true,
        start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
            activation_auto_lifecycle: None,
            expected_execution_generation: None,
            operation_activation_attempt_id: None,
            vcpu: claim.vcpu,
            memory_mib: claim.memory_mib,
            netns_path: None,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            route_identity: Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                sandbox_id: None,
                route_generation: Some(claim.execution_generation),
                current_node_id: claim.active_node_id.map(|node_id| node_id.to_string()),
                execution_generation: i64::try_from(claim.execution_generation).ok(),
                pod_uid: Some(pod_uid.to_string()),
                activation_attempt_id: Some(activation_attempt_id.to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes
                    as i32,
            }),
        }),
        // A ready pool member is Paused, so this adopts route identity without
        // resuming it. If the invariant is violated and the VM is already
        // Started, Wake preserves Started rather than pausing a live VM.
        target_state: target_state as i32,
    })
}

async fn wait_for_pooled_split_receipt_capability(
    node_api: &mut NodeApiClient,
    deadline: Instant,
) -> Result<(), ControllerError> {
    let mut last_error = "worker did not advertise split operation receipts".to_string();
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(ControllerError::service_unavailable(format!(
                "pooled sandbox operation-receipt capability was not ready before split activation: {last_error}"
            )));
        };
        match node_api
            .health_service_serving(
                pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                remaining.min(START_WAIT_CONNECT_TIMEOUT),
            )
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                last_error =
                    "worker node-api reported operation receipts as NOT_SERVING".to_string();
            }
            Err(error) => last_error = error.to_string(),
        }
        if tokio::time::timeout_at(deadline, tokio::time::sleep(START_WAIT_FAST_POLL_INTERVAL))
            .await
            .is_err()
        {
            return Err(ControllerError::service_unavailable(format!(
                "pooled sandbox operation-receipt capability was not ready before split activation: {last_error}"
            )));
        }
    }
}

async fn await_suspend_continuation<T, F>(
    task_tracker: &TaskTracker,
    continuation: F,
) -> Result<T, SubstrateError>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, SubstrateError>> + Send + 'static,
{
    task_tracker
        .spawn(continuation.in_current_span())
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("sandbox suspend task stopped unexpectedly: {error}"),
        })?
}

impl KubernetesSubstrate {
    /// Keep generation-bound suspend finalization alive after its request
    /// waiter disconnects. The tracked task also remains part of graceful
    /// service shutdown, matching the detached restore-leader path.
    pub(in crate::controllers::substrate) async fn suspend_execution_tracked(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        let substrate = self.clone();
        await_suspend_continuation(&self.task_tracker, async move {
            <KubernetesSubstrate as SubstrateAdapter>::suspend_execution(&substrate, sandbox_id)
                .await
        })
        .await
    }
}

async fn await_generation_suspend(
    client: &mut RuntimeClient,
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
    endpoint_addr: &str,
    deadline: Instant,
) -> Result<PublishedLocalArtifact, SubstrateError> {
    poll_generation_suspend(
        |request| {
            let mut client = client.clone();
            async move { client.prepare_teardown_suspend(request).await }
        },
        sandbox_id,
        execution_generation,
        endpoint_addr,
        deadline,
    )
    .await
}

async fn poll_generation_suspend<F, Fut>(
    mut prepare: F,
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
    endpoint_addr: &str,
    deadline: Instant,
) -> Result<PublishedLocalArtifact, SubstrateError>
where
    F: FnMut(
        tonic::Request<pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest>,
    ) -> Fut,
    Fut: std::future::Future<
            Output = Result<
                tonic::Response<pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse>,
                tonic::Status,
            >,
        >,
{
    let poll_started_at = Instant::now();
    let poll_budget = deadline.saturating_duration_since(poll_started_at);
    let mut pending_responses = 0_u64;
    let mut reasonless_pending_responses = 0_u64;
    let mut retryable_errors = 0_u64;
    let mut last_pending_reason = None;
    let mut last_reasoned_pending_reason = None;
    let mut last_retryable_code = None;
    let mut pending_reason_logs = 0_u8;
    let mut last_logged_pending_reason = None;
    let mut next_pending_reason_log_at = poll_started_at;
    let result = async {
        loop {
            let mut request = tonic::Request::new(
                pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_generation: execution_generation.get(),
                    action: pplx_space_sbx_mgr_client::proto::TeardownSuspendAction::BeginOrJoin as i32,
                },
            );
            request.metadata_mut().insert(
                SUSPEND_TRIGGER_METADATA,
                tonic::metadata::MetadataValue::from_static("user"),
            );
            let response = match tokio::time::timeout_at(
                deadline,
                prepare(request),
            )
            .await
            .map_err(|_| SubstrateError::Timeout {
                message: format!(
                    "generation-bound suspend for sandbox {sandbox_id} generation {execution_generation} did not become durable within {}s",
                    TEARDOWN_SUSPEND_DURABILITY_BUDGET.as_secs(),
                ),
                phase: None,
            })? {
                Ok(response) => response.into_inner(),
                Err(error) if generation_suspend_rpc_should_retry(&error) => {
                    retryable_errors += 1;
                    last_retryable_code = Some(error.code());
                    tracing::debug!(
                        %sandbox_id,
                        %execution_generation,
                        node_api_endpoint = %endpoint_addr,
                        code = %error.code(),
                        error = %error,
                        "retrying generation-bound suspend observation after transient node-api failure"
                    );
                    tokio::time::sleep_until(
                        (Instant::now() + Duration::from_millis(250)).min(deadline),
                    )
                    .await;
                    continue;
                }
                Err(error) => {
                    return Err(SubstrateError::Unavailable {
                        message: format!(
                            "node-api generation-bound suspend at {endpoint_addr} failed: {error}"
                        ),
                    });
                }
            };
            let outcome =
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::try_from(response.outcome)
                    .unwrap_or(pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Unspecified);
            match outcome {
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Durable => {
                    return published_generation_suspend_artifact(response, endpoint_addr);
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Pending => {
                    pending_responses += 1;
                    // Keep bounded operator diagnostics out of client errors and
                    // SuspendFailed events. A reasonless tail describes the latest
                    // observation without erasing earlier reasoned Pending history.
                    last_pending_reason = response.reason.map(|reason| {
                        pplx_space_common_svc::bootstrap::truncate_utf8_for_log(
                            &reason,
                            MAX_LOGGED_SUSPEND_PENDING_REASON_BYTES,
                        ).value
                    });
                    if let Some(reason) = &last_pending_reason {
                        last_reasoned_pending_reason = Some(reason.clone());
                    } else {
                        reasonless_pending_responses += 1;
                    }
                    // Suppress ordinary reasonless waits. Once a reason appears,
                    // expose changes (including recovery to None) with both a
                    // cooldown and a hard cap, even if the node keeps oscillating.
                    let now = Instant::now();
                    if pending_reason_logs < MAX_SUSPEND_PENDING_REASON_LOGS
                        && now >= next_pending_reason_log_at
                        && (pending_reason_logs > 0 || last_pending_reason.is_some())
                        && (pending_reason_logs == 0
                            || last_pending_reason != last_logged_pending_reason)
                    {
                        tracing::info!(
                            %sandbox_id,
                            %execution_generation,
                            node_api_endpoint = endpoint_addr,
                            poll_elapsed_ms = duration_millis_to_u64(poll_started_at.elapsed()),
                            pending_responses,
                            ?last_pending_reason,
                            "generation-bound suspend pending observation changed"
                        );
                        pending_reason_logs += 1;
                        last_logged_pending_reason = last_pending_reason.clone();
                        next_pending_reason_log_at = now + SUSPEND_PENDING_REASON_LOG_INTERVAL;
                    }
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Superseded => {
                    return Err(SubstrateError::Unavailable {
                        message: response.reason.unwrap_or_else(|| {
                            format!(
                                "execution generation {execution_generation} no longer owns sandbox {sandbox_id}"
                            )
                        }),
                    });
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Deleted => {
                    return Err(SubstrateError::Unavailable {
                        message: response.reason.unwrap_or_else(|| {
                            format!("sandbox {sandbox_id} was deleted before suspend completed")
                        }),
                    });
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Unspecified => {
                    return Err(SubstrateError::Failed {
                        message: format!(
                            "node-api at {endpoint_addr} returned an invalid generation-bound suspend outcome"
                        ),
                    });
                }
            }
            tokio::time::sleep_until((Instant::now() + Duration::from_millis(250)).min(deadline)).await;
        }
    }
    .await;
    if let Err(error) = &result {
        // One summary for every unsuccessful exit, including terminal RPC and
        // artifact-validation failures; never one event per observation.
        tracing::warn!(
            %sandbox_id,
            %execution_generation,
            node_api_endpoint = endpoint_addr,
            poll_elapsed_ms = duration_millis_to_u64(poll_started_at.elapsed()),
            poll_budget_ms = duration_millis_to_u64(poll_budget),
            pending_responses,
            reasonless_pending_responses,
            retryable_errors,
            ?last_pending_reason,
            ?last_reasoned_pending_reason,
            ?last_retryable_code,
            %error,
            "generation-bound suspend polling failed"
        );
    }
    result
}

pub(super) fn generation_suspend_rpc_should_retry(error: &tonic::Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    ) || pplx_space_common_grpc::is_temporary_tcp_failure(error)
}

pub(super) fn published_generation_suspend_artifact(
    response: pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse,
    endpoint_addr: &str,
) -> Result<PublishedLocalArtifact, SubstrateError> {
    let artifact = response
        .published_artifact
        .ok_or_else(|| SubstrateError::Failed {
            message: format!(
                "node-api at {endpoint_addr} reported durable generation suspend without an artifact"
            ),
        })?;
    let origin = pplx_space_sbx_mgr_client::proto::ArtifactOrigin::try_from(artifact.origin)
        .unwrap_or(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Unspecified);
    if origin != pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend {
        return Err(SubstrateError::Failed {
            message: format!(
                "node-api at {endpoint_addr} reported durable generation suspend with {origin:?} artifact origin"
            ),
        });
    }
    PublishedLocalArtifact::from_published(
        Some(artifact),
        "generation-bound suspend",
        ArtifactKind::Full,
    )
}

pub(super) fn suspend_pod_deletion_confirmation(
    outcome: K8sBlockingDeleteOutcome,
    pod_name: &str,
    timeout_secs: u64,
) -> Result<(), SubstrateError> {
    match outcome {
        K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound => Ok(()),
        K8sBlockingDeleteOutcome::TimedOut => Err(SubstrateError::Timeout {
            message: format!(
                "timed out after {timeout_secs}s waiting for Kubernetes sandbox pod {pod_name} to be absent after suspend"
            ),
            phase: None,
        }),
        K8sBlockingDeleteOutcome::ObserveError => Err(SubstrateError::Unavailable {
            message: format!(
                "could not confirm Kubernetes sandbox pod {pod_name} is absent after suspend"
            ),
        }),
        K8sBlockingDeleteOutcome::PreconditionFailed => Err(SubstrateError::Unavailable {
            message: format!("Kubernetes sandbox pod {pod_name} changed before suspend retirement"),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NetworkSlotCapacityPreflight {
    Available,
    Full,
    Unknown,
}

/// The address a fenced teardown may send its in-VM stop to, derived from the
/// execution row and the pod the fence check already verified.
///
/// This exists because `resolve_pod_ip` is not read-only: on an absent or
/// terminal pod it deletes the object and releases generation tracking, and on
/// a metadata drift it persists replacement identity. Calling it from a fenced
/// teardown lets those mutations run against whatever is current *before* the
/// fence's own preconditioned delete gets a say — the exact ordering the fence
/// exists to prevent. It also asks the database and the apiserver for a row
/// and a pod the fence check just read.
///
/// So the fenced path derives the address itself, from values already in hand,
/// applying the same filters `resolve_pod_ip` applies and none of its writes.
/// Every filter resolves to "no runtime to stop", which is always safe: the
/// teardown proceeds to the pod delete instead of talking to a VM.
fn fenced_runtime_pod_ip(
    lifecycle: &str,
    state: ActiveExecutionState,
    live_pod: Option<&Pod>,
) -> Option<PodIp> {
    if is_suspended_lifecycle(lifecycle) || execution_pod_lookup_in_flight(state) {
        return None;
    }
    let live_pod = live_pod?;
    if live_pod.metadata.deletion_timestamp.is_some()
        || terminal_pod_phase(live_pod.status.as_ref()).is_some()
        || pod_unschedulable_message(live_pod.status.as_ref()).is_some()
    {
        return None;
    }
    assigned_pod_ip(live_pod.status.as_ref()).ok().flatten()
}

/// Project the world as it is now into the same shape the fence carries, so
/// the two can be compared as whole values rather than field by field. One
/// constructor, so the comparison and every database guard read the same
/// fields.
fn current_execution_identity(
    active: &ActiveExecutionStatusRow,
    live_pod: Option<&Pod>,
) -> ExecutionIdentity {
    ExecutionIdentity {
        pod_name: PodName::parse(&active.pod_name)
            // The column is written by `sandbox_pod_name`, so this cannot
            // normally fail; a value the newtype rejects is schema drift, and
            // an unparseable name matches no fence, which fails closed.
            .unwrap_or_else(|_| PodName::parse("space-sbx-unparseable").expect("static pod name")),
        generation: Some(active.generation),
        activation_attempt_id: active.activation_attempt_id,
        recorded: RecordedExecution::Read {
            pod_uid: active.pod_uid.clone(),
            route_activation_id: active.route_activation_id,
        },
        pod: match live_pod {
            None => ObservedPod::Absent,
            Some(pod) => match pod
                .metadata
                .uid
                .as_deref()
                .and_then(|uid| PodUid::parse(uid).ok())
            {
                Some(pod_uid) => ObservedPod::Seen {
                    pod_uid,
                    resource_version: pod
                        .metadata
                        .resource_version
                        .as_deref()
                        .and_then(|version| PodResourceVersion::parse(version).ok()),
                    terminal: terminal_pod_phase(pod.status.as_ref()).is_some(),
                },
                // A live object we cannot identify is not something any fence
                // can be proven against.
                None => ObservedPod::Unobserved,
            },
        },
    }
}

/// What a passing pod-identity fence check proved, carried forward so the
/// teardown's destructive steps are pinned to the execution the caller
/// observed instead of re-asking what is active by the time each one runs.
/// How current the pod read behind a fence check has to be.
///
/// The teardown path may trust the reflector for a pod it will not stop,
/// because its own delete carries the identity as a precondition and so
/// discovers a stale read before mutating anything. A caller-side probe has no
/// such backstop: its answer authorizes a state transition that commits
/// *before* any delete runs, so a cached pod that the apiserver has already
/// replaced would approve a fence the transition then cannot be followed
/// through on. These probes are rare — only revert and reap candidates reach
/// one — so the extra read is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FenceReadFreshness {
    CacheIsEnough,
    MustBeAuthoritative,
}

#[derive(Debug)]
struct FencedTeardown {
    /// The identity the fence matched, as the *row* holds it. Carried whole so
    /// both database guards bind the same predicate, and so the UID they bind
    /// is the row's — not the live pod's, which is absent precisely when the
    /// cleanup is most needed.
    identity: ExecutionIdentity,
    /// The address the in-VM stop may use, derived from the pod the fence
    /// verified rather than from whatever is active at stop time. `None` means
    /// there is no runtime to stop and the teardown goes straight to the pod
    /// delete.
    runtime_pod_ip: Option<PodIp>,
    /// The pod the check *proved* is there, if any — which is what the delete
    /// carries as its precondition.
    ///
    /// Read from the object the comparison just accepted rather than from the
    /// caller's fence, because those differ exactly where it matters: the
    /// filtered reflector drops terminal pods, so a terminal pod always
    /// arrives behind a name-only fence, and taking the precondition from the
    /// fence would leave it undeletable forever. Deriving it here also keeps
    /// "do we have an identity to delete under" from being a second, separate
    /// judgement that can disagree with the fence check.
    verified_pod_uid: Option<PodUid>,
}

enum PodFenceCheck {
    /// Boxed: this variant is much larger than the other two, and the enum is
    /// returned by value from every fence check.
    Holds(Box<FencedTeardown>),
    /// No active execution at all. The retire's goal is already met — another
    /// teardown or a suspend finalizer got there first — so this is success,
    /// not supersession. Calling it a mismatch would make a compensating
    /// caller undo a transition that was correct, which is the opposite of
    /// what the fence is for. Same reading as
    /// [`Self::classify_lost_tracking_release`]'s `None` arm.
    AlreadyRetired,
    Mismatch(PodFenceMismatch),
}

fn pooled_metadata_update_error_is_reclaimable(status: &tonic::Status) -> bool {
    status.code() == Code::DeadlineExceeded
        || pplx_space_common_grpc::is_temporary_tcp_failure(status)
}

fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|status| status.conditions.as_deref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Ready")
        })
        .map(|condition| condition.status == "True")
        .unwrap_or(false)
}

pub(super) fn node_schedulable(node: &Node) -> bool {
    !node
        .spec
        .as_ref()
        .and_then(|spec| spec.unschedulable)
        .unwrap_or(false)
        && node_ready(node)
}

/// True when a node is not hard-blocked from new sandbox placement. Mirrors the
/// required node anti-affinity that `build_sandbox_pod` stamps on every sandbox
/// pod (`NotIn` the blocked value): a node labelled
/// `ssd-placement-blocked=true` is unschedulable for sandboxes, so it must not
/// count toward eligible start capacity. A missing label or any other value
/// leaves the node eligible — matching `NotIn`, which is satisfied by nodes
/// that lack the key.
pub(super) fn node_not_placement_blocked(node: &Node) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|labels| {
            labels.get(pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_LABEL)
        })
        .map(|value| value != pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_VALUE)
        .unwrap_or(true)
}

/// True when a node's node-local SPACE service pods (node-ctl,
/// volume-manager, …) are known **not** to be `Ready` — the cluster-singleton
/// writer has labelled it `node-services-ready=false`.
///
/// The inverse of the soft `NotIn` preference `build_sandbox_pod` stamps: a
/// missing label or any other value reads as ready, so an unlabeled node (no
/// writer deployed, node just joined) is never treated as degraded.
pub(super) fn node_services_not_ready(node: &Node) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|labels| {
            labels.get(pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL)
        })
        .is_some_and(|value| {
            value == pplx_space_common_core::kubernetes::NODE_SERVICES_NOT_READY_VALUE
        })
}

fn quantity_as_u64(
    quantity: &k8s_openapi::apimachinery::pkg::api::resource::Quantity,
) -> Option<u64> {
    quantity.0.parse::<u64>().ok()
}

fn node_network_slot_capacity(node: &Node) -> Option<u64> {
    let status = node.status.as_ref()?;
    let advertised = status
        .capacity
        .as_ref()
        .and_then(|capacity| {
            capacity.get(pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE)
        })
        .and_then(quantity_as_u64)?;
    if advertised == 0 {
        return None;
    }

    status
        .allocatable
        .as_ref()
        .and_then(|allocatable| {
            allocatable.get(pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE)
        })
        .and_then(quantity_as_u64)
        .or(Some(advertised))
}

fn active_pod_network_slot_request(pod: &Pod) -> u64 {
    if pod.metadata.deletion_timestamp.is_some()
        || terminal_pod_phase(pod.status.as_ref()).is_some()
    {
        return 0;
    }

    pod.spec
        .as_ref()
        .map(|spec| {
            spec.containers
                .iter()
                .filter_map(|container| {
                    container
                        .resources
                        .as_ref()
                        .and_then(|resources| resources.requests.as_ref())
                        .and_then(|requests| {
                            requests.get(
                                pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE,
                            )
                        })
                        .and_then(quantity_as_u64)
                })
                .sum()
        })
        .unwrap_or(0)
}

pub(super) fn node_network_slot_available(node: &Node, pods: &[std::sync::Arc<Pod>]) -> bool {
    let Some(node_name) = node.metadata.name.as_deref() else {
        return false;
    };
    let Some(capacity) = node_network_slot_capacity(node) else {
        return false;
    };
    let requested: u64 = pods
        .iter()
        .filter(|pod| {
            pod.spec.as_ref().and_then(|spec| spec.node_name.as_deref()) == Some(node_name)
        })
        .map(|pod| active_pod_network_slot_request(pod))
        .sum();
    requested < capacity
}

pub(super) fn network_slot_capacity_preflight(
    nodes: &[std::sync::Arc<Node>],
    pods: &[std::sync::Arc<Pod>],
    config: &SandboxPodConfigValues,
) -> NetworkSlotCapacityPreflight {
    let selector = config.base_node_selector();
    let mut saw_capacity_node = false;
    for node in nodes {
        let Some(node_name) = node.metadata.name.as_deref() else {
            continue;
        };
        if !node_schedulable(node) || !selector.matches(node) {
            continue;
        }
        let Some(capacity) = node_network_slot_capacity(node) else {
            continue;
        };
        saw_capacity_node = true;

        let requested: u64 = pods
            .iter()
            .filter(|pod| {
                pod.spec.as_ref().and_then(|spec| spec.node_name.as_deref()) == Some(node_name)
            })
            .map(|pod| active_pod_network_slot_request(pod))
            .sum();

        if requested < capacity {
            return NetworkSlotCapacityPreflight::Available;
        }
    }

    if saw_capacity_node {
        NetworkSlotCapacityPreflight::Full
    } else {
        NetworkSlotCapacityPreflight::Unknown
    }
}

impl KubernetesSubstrate {
    /// Whether an admitted warm pod's node is somewhere this sandbox may run,
    /// by the same rules `claim_pooled_execution` admits a claim under. Only
    /// the outcomes that claim path *adopts* answer `true`; every outcome it
    /// declines, refuses, or cannot decide answers `false`. Metrics stay with
    /// the ordinary claim path, which owns the placement-admission series.
    async fn admitted_pool_pod_placement_admits(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        pod: Option<&Pod>,
    ) -> bool {
        let resolved = match self
            .node_group_placement
            .resolve(
                NodeGroupResolvePath::PoolClaim,
                PlacementSubject::Sandbox(request.actor.org_id),
                request.template,
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                tracing::info!(
                    template = request.template,
                    %error,
                    "could not resolve node-group placement for an admitted warm-pool slot"
                );
                return false;
            }
        };
        match resolved.placement() {
            None => match resolved {
                ResolvedPlacement::Unprovisioned => matches!(
                    pooled_pod_node_group(&self.k8s.caches, pod),
                    PooledPodNodeGroup::Unlabeled
                ),
                ResolvedPlacement::Disabled | ResolvedPlacement::Placed(_) => true,
            },
            Some(placement) => {
                placement.admits_shared_capacity()
                    && matches!(
                        pooled_pod_node_admission(&self.k8s.caches, placement, pod),
                        PooledPodAdmission::Admitted
                    )
            }
        }
    }
}

impl KubernetesSubstrate {
    async fn apply_pooled_execution_auto_lifecycle_with_client(
        client: &mut RetryingWorkerApiClient<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        client
            .set_sandbox_auto_lifecycle_policy(
                pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyRequest {
                    sandbox_id: claim.sandbox_id.to_string(),
                    policy: Some(crate::controllers::auto_lifecycle::policy_to_proto(policy)),
                    authority: PolicyAuthority::Cluster.as_proto_i32(),
                },
                WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .await
            .map_err(|err| {
                ControllerError::service_unavailable(format!(
                    "pooled sandbox data-plane auto lifecycle policy update failed: {err}"
                ))
            })?;

        Ok(())
    }

    /// Apply every per-sandbox setup step to an already-booted warm-pool pod via
    /// gRPC, mirroring what a cold create bakes into the pod spec.
    ///
    /// This is the warm-pool consumer of [`PerSandboxSetup`]; the cold consumer
    /// is [`PerSandboxSetup::into_cold_pod_metadata`]. `setup` is consumed by an
    /// exhaustive destructure so a new setup field fails to compile here until it
    /// is handled — the structural guarantee that adoption can't silently skip a
    /// cold-path setup step.
    async fn apply_adopted_per_sandbox_setup(
        &self,
        client: &mut NodeApiClient,
        claim: &PooledExecutionClaim,
        setup: PerSandboxSetup,
        network_policy_generation: u64,
    ) -> Result<(), ControllerError> {
        let mut client = RetryingWorkerApiClient::new(client);
        let sandbox_id = claim.sandbox_id;
        let PerSandboxSetup {
            sandbox_owner,
            user_id,
            org_id,
            process_env,
            auto_lifecycle,
            network_policy,
            node_fs,
            // Always false for adoption (pool members are plaintext by
            // construction and encrypted creates bypass the pool); nothing to
            // apply to the already-booted pod.
            volume_encryption: _,
            disk_io_limits: _,
        } = setup;
        // The pool pod's user_id/org_id annotations carry the pool's identity;
        // adoption conveys the adopter's identity through the owner update below
        // (the owner encodes the user). Those pod annotations are cold-boot
        // metadata and are not rewritten here. If they ever become load-bearing
        // post-adoption, patch them alongside the network-policy annotation.
        let _ = (user_id, org_id);
        // Node-fs permissions can't be applied from the API process (node-mgr is
        // only reachable via sbx-mgr). They are ensured eagerly by sbx-mgr's
        // shared resume path before the VM resumes (`resume_after_wake_lock`),
        // which every adoption wake funnels through. Listed here so a future
        // node-fs requirement is surfaced at this destructure. (Irrefutable
        // today; adding a `NodeFilesystemSetup` variant makes this a compile
        // error until handled.)
        let NodeFilesystemSetup::EnsureArtifactDirPermissions = node_fs;
        let policy_proto =
            super::super::override_request_policy(network_policy.override_policy.as_ref());

        // Step: sandbox metadata. Flip the sandbox from the pool's root owner
        // to the adopter and replace process env before wake.
        let env_count = process_env.as_map().len();
        let process_env = std::collections::HashMap::<String, String>::from(process_env);
        client
            .update_sandbox(
                pplx_space_sbx_mgr_client::proto::UpdateRequest {
                    sandbox_id: claim.sandbox_id.to_string(),
                    vcpu: None,
                    memory_mib: None,
                    pids_max: None,
                    expected_current_owner: Some(
                        pplx_space_common_core::types::ROOT_SANDBOX_OWNER
                            .as_str()
                            .to_string(),
                    ),
                    owner: Some(sandbox_owner.as_str().to_string()),
                    process_env,
                    replace_process_env: true,
                },
                WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.pool.update_sandbox_metadata.client",
                sandbox_id = %sandbox_id,
                env_count,
            ))
            .await
            .map_err(|err| {
                if err.code() == Code::FailedPrecondition {
                    ControllerError::conflict(format!(
                        "pooled sandbox metadata update failed: {err}"
                    ))
                } else if pooled_metadata_update_error_is_reclaimable(&err) {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox metadata update failed: {err}"
                    ))
                } else if err.code() == Code::Cancelled {
                    ControllerError::new(
                        axum::http::StatusCode::REQUEST_TIMEOUT,
                        format!("pooled sandbox metadata update failed: {err}"),
                    )
                } else {
                    ControllerError::internal(format!(
                        "pooled sandbox metadata update failed: {err}"
                    ))
                }
            })?;

        // Step: network policy. Apply the effective policy to sbx-mgr's local DB
        // before waking the sandbox.
        let NetworkPolicySetup {
            override_policy,
            effective_policy,
            authority,
        } = network_policy;
        // This closes the window where a sandbox
        // adopted with a non-default policy (e.g. allow_internet_access=false)
        // could wake up enforcing the pool's original default policy and rely on
        // the periodic reconciler to correct it asynchronously.
        //
        // Why sbx-mgr can target this sandbox as a non-pool network identity:
        // K8s pool pods are created by the containerd shim calling sbx-mgr
        // Create without setting pool_template, so pool_template = NULL in
        // sbx-mgr's SQLite from the start (unlike direct pool sandboxes, which
        // use AdoptPoolSandbox to clear a real pool_template marker). The
        // sbx-mgr reconcile-enqueue gate also requires netgtw_pid IS NOT NULL;
        // sync_to_gateway below verifies the live gateway and fails if it cannot
        // confirm the policy.
        //
        // sync_to_gateway: true makes sbx-mgr call refresh_network_policy on a
        // live net-gateway before returning, rather than enqueuing the background
        // reconciler. This closes the window entirely: the gateway confirms the
        // new policy before wake_sandbox is called. If the gateway is not live,
        // sbx-mgr returns Unavailable and the create path rolls back this claimed
        // pool pod instead of waking it with an unconfirmed policy.
        let inherit_template = override_policy.is_none();
        let set_policy_response = client
            .set_sandbox_network_policy(
                pplx_space_sbx_mgr_client::proto::SetNetworkPolicyRequest {
                    sandbox_id: claim.sandbox_id.to_string(),
                    policy: policy_proto,
                    inherit_template,
                    policy_generation: network_policy_generation,
                    authority: authority.as_proto_i32(),
                    sync_to_gateway: true,
                },
                WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.pool.set_network_policy.client",
                sandbox_id = %sandbox_id,
            ))
            .await
            .map_err(|err| {
                if err.code() == Code::FailedPrecondition {
                    ControllerError::conflict(format!(
                        "pooled sandbox network policy update conflicted: {err}"
                    ))
                } else if err.code() == Code::Unavailable {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox data-plane network policy update failed: {err}"
                    ))
                } else {
                    ControllerError::internal(format!(
                        "pooled sandbox network policy update failed: {err}"
                    ))
                }
            })?;

        let set_policy_response = set_policy_response.into_inner();

        // A successful sync response only proves *a* policy was confirmed on the
        // gateway — verify it's the one the API resolved for this request, not a
        // stale policy sbx-mgr resolved from its own (possibly lagging) template
        // cache or a skipped update. Compare the confirmed effective policy value
        // against the API-resolved effective policy; on mismatch, roll back the
        // claim rather than wake under the wrong policy.
        let confirmed_effective_policy = set_policy_response
            .effective_policy
            .as_ref()
            .map(pplx_space_net_common::NetworkPolicyV1::try_from)
            .transpose()
            .map_err(|err| {
                ControllerError::internal(format!(
                    "pooled sandbox returned an undecodable effective network policy: {err}"
                ))
            })?;
        if confirmed_effective_policy.as_ref() != Some(&effective_policy) {
            return Err(ControllerError::service_unavailable(
                "pooled sandbox gateway confirmed a network policy that differs from the \
                 request-resolved policy (sbx-mgr template cache may be stale); rolling back",
            ));
        }

        // Patch the pod annotation with the explicit override so that if sbx-mgr
        // ever restarts and re-reads the pod spec, the shim passes the correct
        // policy to sbx-mgr's Create.
        //
        // K8s pool pods have no network-policy annotation at creation time (the
        // pool pod builder uses SandboxPodRequest::Pooled which omits the runtime
        // annotation block). Only write the annotation for an explicit override;
        // leave it absent for template inheritance so a future shim Create sees nil
        // NetworkPolicy → sbx-mgr inherits the template default rather than locking
        // in the current template value as a per-sandbox override. This is
        // best-effort and fire-and-forget — sbx-mgr's DB already holds the correct
        // policy from the preceding set_sandbox_network_policy call.
        if let Some(override_policy) = override_policy.as_ref() {
            use kube::api::{Patch, PatchParams};
            match serde_json::to_string(override_policy) {
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %claim.pod_name,
                        %err,
                        "pool adoption: failed to serialize network policy override for pod annotation"
                    );
                }
                Ok(policy_json) if ensure_network_policy_annotation_size(&policy_json).is_err() => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %claim.pod_name,
                        "pool adoption: network policy override too large for pod annotation, skipping"
                    );
                }
                Ok(policy_json) => {
                    let patch = serde_json::json!({
                        "metadata": {
                            "annotations": {
                                SANDBOX_NETWORK_POLICY_ANNOTATION: policy_json,
                                SANDBOX_NETWORK_POLICY_GENERATION_ANNOTATION: network_policy_generation.to_string(),
                                SANDBOX_NETWORK_POLICY_AUTHORITY_ANNOTATION: authority.to_string(),
                            }
                        }
                    });
                    let pods: Api<Pod> =
                        Api::namespaced(self.k8s.client.clone(), &self.k8s.sandbox_namespace);
                    let pod_name = claim.pod_name.clone();
                    let sandbox_id_bg = sandbox_id;
                    tokio::spawn(async move {
                        if let Err(err) = pods
                            .patch(&pod_name, &PatchParams::default(), &Patch::Merge(&patch))
                            .await
                        {
                            tracing::warn!(
                                sandbox_id = %sandbox_id_bg,
                                pod_name = %pod_name,
                                %err,
                                "pool adoption: failed to update pod network policy annotation"
                            );
                        }
                    });
                }
            }
        }

        // Step: auto-lifecycle. Set before any wake so the idle policy is live
        // the moment the VM resumes.
        Self::apply_pooled_execution_auto_lifecycle_with_client(
            &mut client,
            claim,
            &auto_lifecycle,
        )
        .instrument(tracing::info_span!(
            "substrate.k8s.pool.set_auto_lifecycle_policy.client",
            sandbox_id = %sandbox_id,
        ))
        .await?;

        Ok(())
    }

    /// Resolve the node a slow-create pod can be preserved on, or `None` if it
    /// isn't usefully preservable. Used by the slow-create preserve path to
    /// decide whether returning the sandbox as `started` would actually let a
    /// client retry converge.
    ///
    /// Preservable requires the execution to have advanced past `creating`,
    /// carry a persisted pod IP, be scheduled, be non-terminal/non-deleting,
    /// AND have a sandbox proxy that currently accepts TCP. The proxy check is
    /// the load-bearing one: the execution flips to `state = 'running'` the
    /// moment a pod IP is assigned — *before* the proxy ever accepts a
    /// connection — so "has an IP" alone does not mean the runtime is
    /// reachable. Without the live proxy probe, a create whose proxy never came
    /// up (guest wedged, node-mgr stuck) would be marked the non-sweepable
    /// `started` state and returned as Running forever, since there is no later
    /// readiness finalizer. Requiring a live proxy means a preserved create can
    /// actually be reached by the client's data-plane retry; anything else
    /// falls through to teardown, exactly as before this change.
    async fn preservable_create_node(
        &self,
        sandbox_uuid: SandboxId,
        pod_name: &str,
    ) -> Option<NodeId> {
        let row = self
            .store
            .active_execution_status(sandbox_uuid)
            .await
            .ok()
            .flatten()?;
        if execution_pod_create_in_flight(row.state) || row.pod_ip.is_none() {
            return None;
        }
        let pod = match self.lookup_sandbox_pod(pod_name) {
            // Trust the cached pod only when it shows BOTH a node assignment
            // and a pod IP. The reflector can observe the scheduling update
            // (`nodeName`) before the later `podIP` assignment; an IP-lagged
            // cached pod would make `assigned_pod_ip` below return `None` and
            // wrongly drop a reachable slow-create to teardown even though the
            // per-pod watch already persisted the IP (`row.pod_ip`, checked
            // above). Fall through to the authoritative GET in that window.
            Some(pod)
                if assigned_pod_node_name(&pod).is_some()
                    && assigned_pod_ip(pod.status.as_ref())
                        .ok()
                        .flatten()
                        .is_some() =>
            {
                pod
            }
            _ => fetch_sandbox_pod_uncached(&self.k8s, pod_name)
                .await
                .ok()
                .flatten()?,
        };
        assigned_pod_node_name(&pod)?;
        if pod.metadata.deletion_timestamp.is_some()
            || terminal_pod_phase(pod.status.as_ref()).is_some()
        {
            return None;
        }
        // Live evidence the runtime is reachable — not merely that an IP was
        // assigned. Same probe + timeout the create proxy-reachable loop uses.
        let pod_ip = assigned_pod_ip(pod.status.as_ref()).ok().flatten()?;
        if !Self::pod_proxy_accepts_tcp(
            pod_ip,
            self.k8s.sandbox_proxy_port,
            START_WAIT_CONNECT_TIMEOUT,
        )
        .await
        {
            return None;
        }
        self.remember_pod_node_placement(
            sandbox_uuid,
            &pod,
            PlacementRetryDisposition::RetryDuplicatesExecution,
        )
        .await
        .ok()
        .flatten()
    }

    /// Check a caller's pod-identity fence before anything destructive runs.
    ///
    /// Two questions, two authorities. The control plane answers "is the
    /// fenced pod still the active execution?" — a successor generation lives
    /// under a different pod name, so no precondition attached to the observed
    /// pod would ever see it. The apiserver answers "is the object at that
    /// name still the one the caller saw?" via UID and, when supplied,
    /// resourceVersion.
    ///
    /// Running this before the in-VM stop is the point: the fence exists to
    /// keep a retire off a sandbox that recovered, and by the time the pod
    /// delete carries its precondition the VM has already been stopped and its
    /// disk backed up. The delete's precondition still matters — it closes the
    /// gap between this check and the delete — but it cannot be the only gate.
    ///
    /// A passing check hands back the facts every later step is pinned to, so
    /// none of them has to re-ask "what is active now?" and get the successor
    /// as an answer.
    async fn check_pod_fence(
        &self,
        sandbox_id: SandboxId,
        fence: &PodIdentityFence,
        freshness: FenceReadFreshness,
    ) -> Result<PodFenceCheck, SubstrateError> {
        // One read answers "is the fenced pod still the active execution?" and
        // supplies the generation and runtime the rest of the teardown needs.
        let Some(active) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(PodFenceCheck::AlreadyRetired);
        };
        let live_pod = self
            .read_fenced_pod(&active.lifecycle, active.state, &fence.pod_name, freshness)
            .await?;
        // One comparison, over the whole identity. Every earlier bug on this
        // path was a site that compared a different subset of these fields, or
        // read one of them being absent as "no expectation here".
        let current = current_execution_identity(&active, live_pod.as_ref());
        if let Err(mismatch) = fence.observed().reconcile(&current) {
            if mismatch == PodFenceMismatch::PodIdentityUnverified
                && let Some(observed_uid) = live_pod
                    .as_ref()
                    .and_then(|pod| pod.metadata.uid.as_deref())
            {
                self.hydrate_materialized_pod_uid(
                    sandbox_id,
                    &current_execution_identity(&active, live_pod.as_ref()),
                    observed_uid,
                )
                .await;
            }
            return Ok(PodFenceCheck::Mismatch(mismatch));
        }
        Ok(PodFenceCheck::Holds(Box::new(FencedTeardown {
            verified_pod_uid: match &current.pod {
                ObservedPod::Seen { pod_uid, .. } => Some(pod_uid.clone()),
                ObservedPod::Absent | ObservedPod::Unobserved => None,
            },
            identity: current_execution_identity(&active, live_pod.as_ref()),
            runtime_pod_ip: fenced_runtime_pod_ip(
                &active.lifecycle,
                active.state,
                live_pod.as_ref(),
            ),
        })))
    }

    /// The pod object the fence decision is made against.
    ///
    /// The reflector is the cheap read every hot path uses, and a fence check
    /// runs over a candidate set large enough that adding an apiserver GET per
    /// candidate is its own outage risk. For a decision that concludes "no
    /// runtime to stop" the cache is good enough: the only destructive step
    /// left is the delete, and the apiserver evaluates that precondition
    /// itself.
    ///
    /// It is not good enough when the decision is about to authorize a
    /// blocking in-VM stop. A lagging cache can still match a fence the
    /// apiserver would reject — a same-name replacement, or the same pod
    /// having moved on — and by the time the delete says so the VM is already
    /// stopped. So a cached object that *would* authorize a stop is re-read
    /// authoritatively, which bounds the extra GET to candidates that actually
    /// have a live runtime. A cache miss is re-read for the same reason it
    /// always was: treating "not cached" as "no pod" would silently pass the
    /// fence.
    async fn read_fenced_pod(
        &self,
        lifecycle: &str,
        state: ActiveExecutionState,
        pod_name: &PodName,
        freshness: FenceReadFreshness,
    ) -> Result<Option<Pod>, SubstrateError> {
        let cached = self.lookup_sandbox_pod(pod_name.as_str());
        let authorizes_stop = cached
            .as_ref()
            .is_some_and(|pod| fenced_runtime_pod_ip(lifecycle, state, Some(pod)).is_some());
        if cached.is_some() && !authorizes_stop && freshness == FenceReadFreshness::CacheIsEnough {
            return Ok(cached);
        }
        fetch_sandbox_pod_uncached(&self.k8s, pod_name.as_str())
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("failed to read fenced sandbox pod: {err}"),
            })
    }

    /// Record the UID of a live pod that no fence can describe.
    ///
    /// A name-only fence refuses a live pod, which is correct but not by
    /// itself convergent: if pod creation succeeded and the process died
    /// before persisting the pod's metadata, nothing else ever writes that
    /// UID, so every later candidate carries the same unusable fence and the
    /// execution is never cleaned up — worse than the unfenced retire this
    /// replaced, which at least made progress.
    ///
    /// Writing the UID here is the create path's missed write, made
    /// generation-scoped so it cannot land on a successor. It authorizes
    /// nothing on its own: this call is refusing the teardown, and the *next*
    /// candidate select carries the UID into a fence that can be enforced.
    async fn hydrate_materialized_pod_uid(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        observed_uid: &str,
    ) {
        match self
            .store
            .backfill_missing_pod_uid(sandbox_id, identity, observed_uid)
            .await
        {
            Ok(_) => tracing::info!(
                sandbox_id = %sandbox_id,
                generation = ?identity.generation,
                "recorded the pod uid a name-only fence could not verify; the next sweep \
                 can fence on it"
            ),
            Err(error) => tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = ?identity.generation,
                %error,
                "failed to record the pod uid a name-only fence could not verify; cleanup \
                 will keep deferring until it lands"
            ),
        }
    }

    /// Shared body of [`SubstrateAdapter::delete_execution`] and
    /// [`SubstrateAdapter::retire_execution`]; `mode` decides whether the
    /// in-VM cleanup also destroys node-local sandbox state, and `fence` pins
    /// the whole teardown to one observed pod identity (see
    /// [`Self::check_pod_fence`]).
    async fn teardown_execution(
        &self,
        sandbox_id: SandboxId,
        mode: ExecutionTeardown,
        fence: &RetireFence,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let pod_fence = match fence {
            RetireFence::Unobserved => None,
            RetireFence::NoExecution => {
                // Having observed no execution authorizes exactly one thing:
                // clearing the placement state such a sandbox is left holding.
                // Not the in-VM stop, and not the tracked-pod sweep — both act
                // on objects this caller never saw, and the pod list would
                // include a successor's.
                return self.retire_with_no_active_execution(sandbox_id, mode).await;
            }
            RetireFence::Pod(pod_fence) => Some(pod_fence),
        };
        let fenced = match pod_fence {
            Some(pod_fence) => match self
                .check_pod_fence(sandbox_id, pod_fence, FenceReadFreshness::CacheIsEnough)
                .await?
            {
                PodFenceCheck::AlreadyRetired => {
                    return self.retire_with_no_active_execution(sandbox_id, mode).await;
                }
                PodFenceCheck::Mismatch(mismatch) => {
                    return Ok(self.refuse_fenced_teardown(
                        sandbox_id,
                        pod_fence,
                        mismatch,
                        TeardownProgress::NothingMutated,
                    ));
                }
                PodFenceCheck::Holds(fenced) => Some(fenced),
            },
            None => None,
        };
        // A fenced teardown already has its address, derived from the pod the
        // fence verified. The unfenced path resolves one now: best-effort, and
        // never letting a missing runtime block the teardown. When the tracked
        // pod has nothing to talk to — no IP yet, or `Pending+Unschedulable` —
        // `resolve_pod_ip` returns `Unavailable`; treat that as "no live
        // runtime" and fall through to the pod-delete loop instead of
        // short-circuiting. Genuine infra faults still propagate.
        let pod_ip = match &fenced {
            Some(fenced) => fenced.runtime_pod_ip,
            None => match self.resolve_pod_ip(sandbox_uuid).await {
                Ok(pod_ip) => pod_ip,
                Err(err) if delete_can_skip_runtime_cleanup(&err) => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        reason = %err,
                        "tracked sandbox pod has no reachable runtime; skipping in-VM cleanup and deleting pod object"
                    );
                    None
                }
                Err(err) => return Err(err),
            },
        };
        // Tracks whether anything has actually been done yet, so a refusal
        // arriving later can tell a compensating caller whether undoing its
        // own transition is still honest.
        let mut progress = TeardownProgress::NothingMutated;
        if let Some(pod_ip) = pod_ip {
            // Retire preserves the stopped local disk by stopping the runtime
            // and enqueueing an explicit current-instance backup before pod
            // tracking is forgotten. Once tracking is inactive, the
            // failed-cleanup retry scan no longer has a source execution to
            // address; abort on stop/backup failure so the retry loop keeps the
            // pod and execution row visible. Delete goes straight to sbx-mgr's
            // guarded delete: a pre-delete Stop would turn an in-flight
            // `Suspending` row into `Stopping` and erase the publication fence.
            let endpoint_addr = format!("{pod_ip}:{}", self.k8s.sandbox_proxy_port);
            let client = Self::pod_worker_client(
                pod_ip,
                self.k8s.sandbox_proxy_port,
                POD_WORKER_CONNECT_TIMEOUT,
            )
            .await;
            if let Some(mut client) = client {
                if mode.sends_sandbox_stop() {
                    let stop_result = client
                        .stop_sandbox(pplx_space_sbx_mgr_client::proto::StopRequest {
                            sandbox_id: sandbox_id.to_string(),
                            template_filter: None,
                            blocking: true,
                            immediate_kill: false,
                        })
                        .await;
                    if let Err(err) = stop_result {
                        return Err(SubstrateError::Failed {
                            message: format!(
                                "in-VM stop failed during execution retire; keeping execution tracked for retry: {err}"
                            ),
                        });
                    }
                    // Past this point the sandbox is stopped whatever any
                    // later fence check says, so a refusal from here on must
                    // not read as "nothing happened".
                    progress = TeardownProgress::RuntimeStopped;
                    match enqueue_artifact_backup_with_client(
                        &mut client,
                        &endpoint_addr,
                        sandbox_id,
                        ArtifactKind::Disk,
                        BackupSource::CurrentInstance,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(err) if retire_can_skip_backup_enqueue_error(&err) => {
                            tracing::info!(
                                sandbox_id = %sandbox_id,
                                error = %err,
                                "stopped-disk backup skipped during execution retire because \
                                 sbx-mgr has no sandbox row or terminally refused the backup"
                            );
                        }
                        Err(err) => {
                            return Err(SubstrateError::Failed {
                                message: format!(
                                    "stopped-disk backup enqueue failed during execution retire; keeping execution tracked for retry: {err}"
                                ),
                            });
                        }
                    }
                }
                if mode.sends_sandbox_delete()
                    && let Err(error) = client
                        .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                            sandbox_id: sandbox_id.to_string(),
                            template_filter: None,
                            blocking: true,
                        })
                        .await
                {
                    match classify_sandbox_delete_rpc_error(&error) {
                        SandboxDeleteRpcErrorDisposition::DeferTeardown => {
                            return Err(SubstrateError::SuspendInFlight {
                                message: format!(
                                    "sandbox {sandbox_id} node-local delete deferred while suspend publication is pending: {error}"
                                ),
                            });
                        }
                        SandboxDeleteRpcErrorDisposition::RetainedFilesystemOnlyCopy => {
                            return Err(SubstrateError::RetainedFilesystemOnlyCopy {
                                message: format!(
                                    "sandbox {sandbox_id} node-local delete refused because the retained filesystem may be the only copy: {error}"
                                ),
                            });
                        }
                        SandboxDeleteRpcErrorDisposition::ContinueTeardown => {
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                error = %error,
                                "best-effort sandbox delete RPC failed during execution teardown"
                            );
                        }
                    }
                }
            } else if mode.requires_stop_success() {
                return Err(SubstrateError::Failed {
                    message: "pod worker unreachable for in-VM stop during execution retire; \
                              keeping execution tracked for retry"
                        .to_string(),
                });
            }
        }

        let placement_clock = if mode.preserves_placement_freshness() {
            PlacementClock::Preserve
        } else {
            PlacementClock::Bump
        };

        // A fenced teardown deletes exactly the pod the caller observed, under
        // its precondition, and nothing else. The unfenced loop below sweeps
        // every *tracked* name, which on a race includes a successor
        // generation's pod — a name the caller never saw and no precondition
        // attached to the observed pod could protect.
        if let Some((fence, fenced)) = pod_fence.zip(fenced.as_ref()) {
            // The check proved no pod, which means it held because the pod was
            // *absent* — and between that check and here it can materialize.
            // There is nothing to precondition on, so there is nothing safe to
            // delete: an unconditional delete would remove a live pod nobody
            // identified. Releasing tracking would be no
            // better — a materialized pod with no active execution row is an
            // orphan the failed-cleanup pass can no longer collect — so absence
            // is re-proven authoritatively first, as late as possible.
            if fenced.verified_pod_uid.is_none()
                && let Some(materialized) =
                    fetch_sandbox_pod_uncached(&self.k8s, fence.pod_name.as_str())
                        .await
                        .map_err(|err| SubstrateError::Failed {
                            message: format!("failed to re-read an unfenceable sandbox pod: {err}"),
                        })?
            {
                if let Some(observed_uid) = materialized.metadata.uid.as_deref() {
                    self.hydrate_materialized_pod_uid(sandbox_id, &fenced.identity, observed_uid)
                        .await;
                }
                return Ok(self.refuse_fenced_teardown(
                    sandbox_id,
                    fence,
                    PodFenceMismatch::PodIdentityUnverified,
                    progress,
                ));
            }
            if let Some(pod_uid) = fenced.verified_pod_uid.as_ref() {
                match delete_sandbox_pod_fenced(
                    &self.k8s,
                    &fence.pod_name,
                    pod_uid,
                    fence.resource_version.as_ref(),
                )
                .await
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to delete fenced sandbox pod: {err}"),
                })? {
                    // A delete that removed the object is destructive too, so
                    // a refusal from here on must not report otherwise. Never
                    // downgrades a stop that already ran.
                    FencedPodDeleteOutcome::Deleted => {
                        if progress == TeardownProgress::NothingMutated {
                            progress = TeardownProgress::PodDeleted;
                        }
                    }
                    FencedPodDeleteOutcome::NotFound => {}
                    FencedPodDeleteOutcome::PreconditionFailed => {
                        return Ok(self.refuse_fenced_teardown(
                            sandbox_id,
                            fence,
                            self.classify_delete_precondition_rejection(pod_uid, &fence.pod_name)
                                .await,
                            progress,
                        ));
                    }
                }
            }
            // Strictly-older generations are provably not the successor, and
            // nothing else reliably collects them once this sandbox leaves the
            // failed-cleanup scan. The successor's own generation is excluded
            // by the bound, not by a name comparison.
            self.delete_tracked_pods(
                self.store
                    .tracked_execution_pod_names_before_generation(
                        sandbox_id,
                        fenced.identity.generation.unwrap_or(i64::MIN),
                    )
                    .await?,
            )
            .await?;

            // Generation-guarded, so a successor installed while this teardown
            // ran keeps its execution row instead of being retired out from
            // under itself. The guard is a CAS, which makes this the one step
            // that closes its own window rather than narrowing it.
            if !self
                .store
                .forget_fenced_pod_tracking(sandbox_uuid, &fenced.identity, placement_clock)
                .await?
            {
                return self
                    .classify_lost_tracking_release(sandbox_id, fence, progress)
                    .await;
            }
            return Ok(RetireExecutionOutcome::Retired);
        }

        self.delete_tracked_pods(self.store.tracked_execution_pod_names(sandbox_id).await?)
            .await?;

        self.store
            .forget_pod_tracking(sandbox_uuid, placement_clock)
            .await?;

        Ok(RetireExecutionOutcome::Retired)
    }

    /// Idempotent best-effort delete of each tracked pod name. An already-gone
    /// pod is the expected case, not a failure.
    async fn delete_tracked_pods(&self, pod_names: Vec<String>) -> Result<(), SubstrateError> {
        for pod_name in pod_names {
            if let Err(err) = delete_sandbox_pod(&self.k8s, &pod_name).await
                && !is_not_found_error(&err)
            {
                return Err(SubstrateError::Failed {
                    message: format!("failed to delete sandbox pod {pod_name}: {err}"),
                });
            }
        }
        Ok(())
    }

    /// Decide what a lost tracking-release CAS means.
    ///
    /// Losing it is not automatically a supersession. The teardown's own
    /// `resolve_pod_ip` releases tracking when it finds the pod
    /// authoritatively absent, so an ordinary already-gone pod reaches the CAS
    /// with nothing left to claim. Reporting that as a defer would tell the
    /// sweeper nothing was torn down while the execution had in fact just been
    /// retired, and meter a pod-identity race that never happened. Only an
    /// execution that is *active under another generation* is a supersession.
    async fn classify_lost_tracking_release(
        &self,
        sandbox_id: SandboxId,
        fence: &PodIdentityFence,
        progress: TeardownProgress,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        match self.store.active_execution_status(sandbox_id).await? {
            None => Ok(RetireExecutionOutcome::Retired),
            Some(_) => Ok(self.refuse_fenced_teardown(
                sandbox_id,
                fence,
                PodFenceMismatch::ExecutionSuperseded,
                progress,
            )),
        }
    }

    /// Finish a teardown that found no active execution.
    ///
    /// Both ways of arriving here — a caller that observed no execution, and a
    /// fence whose row went inactive since — leave the same two jobs, and must
    /// do both. Pods recorded on *inactive* rows, then the placement state the
    /// sandbox is left holding; without the second a revert can leave a
    /// dormant `suspended` row still marked as placed, holding a pool slot. No successor can own one, because a
    /// successor's pod is on the active row, and nothing else retries them:
    /// the failed-cleanup pass selects active rows only. That is where a
    /// create cleanup leaves an orphan when it marks its row inactive and then
    /// fails its best-effort delete, and an orphan pod holds a VM and node
    /// capacity until the node is recycled.
    ///
    /// One function so the two paths cannot answer this differently.
    async fn retire_with_no_active_execution(
        &self,
        sandbox_id: SandboxId,
        mode: ExecutionTeardown,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        // Pods first, tracking second. The rows naming these pods are what a
        // later attempt would read to find them again, so clearing tracking
        // ahead of a delete that then fails strands the pod with nothing left
        // pointing at it — the failed-cleanup pass selects active rows only.
        // Deleting first is safe in the other direction: a successor's pod is
        // recorded on the *active* row, so nothing here can be one, even if an
        // execution appears while this runs.
        let inactive = self.store.inactive_execution_pod_names(sandbox_id).await?;
        if !inactive.is_empty() {
            self.delete_tracked_pods(inactive).await?;
        }
        // Then the placement state the sandbox is left holding. The clear
        // re-proves the absence inside its own lock, so an execution created
        // since claims nothing — and its refusal is the one way this path
        // still reports a mismatch.
        let placement_clock = if mode.preserves_placement_freshness() {
            PlacementClock::Preserve
        } else {
            PlacementClock::Bump
        };
        if !self
            .store
            .forget_pod_tracking_if_no_active_execution(sandbox_id, placement_clock)
            .await?
        {
            tracing::info!(
                sandbox_id = %sandbox_id,
                "an execution appeared while retiring a sandbox that had none; \
                 refusing the teardown"
            );
            return Ok(RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionMissing,
                progress: TeardownProgress::NothingMutated,
            });
        }
        Ok(RetireExecutionOutcome::Retired)
    }

    /// Turn the apiserver's one 409 into the two things it can actually mean.
    ///
    /// A preconditioned delete is rejected both when the pod the caller
    /// observed simply moved — a Pending pod coming up changes its
    /// resourceVersion — and when a different pod now holds that name. Those
    /// call for opposite decisions from a caller that has already stopped a
    /// runtime and is deciding whether to undo its own state transition: the
    /// first says the sandbox it stopped is the live one, the second says it
    /// stopped a predecessor. Picking either as the default is wrong half the
    /// time, so read the object and answer with the unambiguous mismatch. The
    /// read is authoritative and only happens on a 409, which is rare.
    async fn classify_delete_precondition_rejection(
        &self,
        fenced_uid: &PodUid,
        pod_name: &PodName,
    ) -> PodFenceMismatch {
        match fetch_sandbox_pod_uncached(&self.k8s, pod_name.as_str()).await {
            // Gone between the rejection and this read: whatever the 409 was
            // about, the object the caller wanted removed is not there. Report
            // it as the pod having changed, which is the conservative half.
            Ok(None) => PodFenceMismatch::PodUidChanged,
            Ok(Some(pod)) => match pod.metadata.uid.as_deref() {
                Some(uid) if uid == fenced_uid.as_str() => {
                    PodFenceMismatch::PodResourceVersionChanged
                }
                Some(_) => PodFenceMismatch::PodUidChanged,
                // An object with no UID is not something to reason about.
                None => PodFenceMismatch::PodIdentityUnverified,
            },
            Err(err) => {
                tracing::info!(
                    pod_name = %pod_name,
                    error = %err,
                    "could not read the pod behind a rejected delete precondition; \
                     treating it as replaced, which is the half that defers"
                );
                PodFenceMismatch::DeletePreconditionRejected
            }
        }
    }

    /// Log and report a teardown the fence refused. Every refusal path funnels
    /// through here so they all leave the same trace and none can drift into
    /// reporting a teardown that did not happen.
    fn refuse_fenced_teardown(
        &self,
        sandbox_id: SandboxId,
        fence: &PodIdentityFence,
        mismatch: PodFenceMismatch,
        progress: TeardownProgress,
    ) -> RetireExecutionOutcome {
        tracing::info!(
            sandbox_id = %sandbox_id,
            pod_name = %fence.pod_name,
            mismatch = %mismatch,
            progress = ?progress,
            "pod-identity fence rejected an execution teardown; the sandbox changed \
             since the caller observed it, so nothing further was torn down"
        );
        RetireExecutionOutcome::PodIdentityChanged { mismatch, progress }
    }

    pub(in crate::controllers::substrate) fn try_resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        let k8s_node_name = target
            .substrate_node_id
            .ok_or_else(|| SubstrateError::NotFound {
                message: "k8s node row missing substrate_node_id".to_string(),
            })?;

        let pod = self
            .k8s
            .caches
            .ready_node_api_pod_on(k8s_node_name, target.role.as_str())
            .ok_or_else(|| SubstrateError::Unavailable {
                message: format!(
                    "no ready node-api pod for role {} on k8s node {k8s_node_name}",
                    target.role
                ),
            })?;

        let raw_ip = caches::pod_ip(&pod).ok_or_else(|| SubstrateError::Unavailable {
            message: format!("node-api pod on k8s node {k8s_node_name} has no pod IP yet"),
        })?;

        let ip: IpAddr = raw_ip.parse().map_err(|err| SubstrateError::Failed {
            message: format!(
                "node-api pod on k8s node {k8s_node_name} has invalid pod IP {raw_ip}: {err}"
            ),
        })?;
        // Prefer the port advertised by the node-api pod itself so worker and
        // builder DaemonSets configured with different ports each get dialed
        // correctly. Fall back to the configured global default when a pod is
        // missing the annotation and legacy named port.
        let port = caches::pod_node_api_port(&pod).unwrap_or(self.k8s.node_api_port);
        Ok(SocketAddr::new(ip, port).to_string())
    }

    pub(super) async fn finish_suspend_handoff(
        &self,
        sandbox_id: SandboxId,
        expected_generation: ExecutionGeneration,
        expected_pod_uid: PodUid,
        pod_name: String,
        node_suspend_artifact: Option<PublishedLocalArtifact>,
    ) -> Result<PublishedLocalArtifact, SubstrateError> {
        let persisted_suspend_artifact = self
            .store
            .committed_suspend_artifact(sandbox_id, expected_generation, &expected_pod_uid)
            .await?;
        let published_artifact = match (persisted_suspend_artifact, node_suspend_artifact) {
            (Some(persisted), Some(from_node)) if persisted != from_node => {
                return Err(SubstrateError::Unavailable {
                    message: "Kubernetes suspend artifact changed before pod retirement"
                        .to_string(),
                });
            }
            (Some(persisted), _) => persisted,
            (None, Some(from_node)) => {
                self.store
                    .persist_or_accept_suspend_commit(
                        sandbox_id,
                        expected_generation,
                        &expected_pod_uid,
                        from_node,
                    )
                    .await?;
                from_node
            }
            (None, None) => {
                return Err(SubstrateError::Unavailable {
                    message: "Kubernetes suspend commit was not persisted".to_string(),
                });
            }
        };

        match self
            .k8s_blocking_delete_sandbox_pod_if_uid(&pod_name, &expected_pod_uid)
            .await
        {
            Ok(K8sBlockingDeleteOutcome::Deleted) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    "deleted Kubernetes sandbox pod after suspend"
                );
            }
            Ok(K8sBlockingDeleteOutcome::NotFound) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    "Kubernetes sandbox pod was already gone after suspend"
                );
            }
            Ok(outcome) => {
                let error = suspend_pod_deletion_confirmation(
                    outcome,
                    &pod_name,
                    self.k8s.k8s_blocking_delete_timeout.as_secs(),
                )
                .expect_err("unconfirmed pod deletion cannot finalize suspend");
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    delete_outcome = outcome.as_str(),
                    error = %error,
                    "pod deletion is unconfirmed after suspend; keeping the active execution"
                );
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::SuspendFailed,
                    SandboxEventSource::Substrate,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .pod_name(pod_name.clone())
                        .error(error.to_string()),
                );
                return Err(error);
            }
            Err(err) => {
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::SuspendFailed,
                    SandboxEventSource::Substrate,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .pod_name(pod_name.clone())
                        .error(err.to_string()),
                );
                return Err(SubstrateError::Failed {
                    message: format!(
                        "failed to delete suspended sandbox pod before marking suspended: {err}"
                    ),
                });
            }
        }

        self.store
            .finish_suspended_execution(
                sandbox_id,
                expected_generation,
                &expected_pod_uid,
                published_artifact,
            )
            .await?;

        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::SuspendCompleted,
            SandboxEventSource::Substrate,
        )
        .emit_with(SandboxEventDetails::default().pod_name(pod_name));

        Ok(published_artifact)
    }
}

impl SubstrateAdapter for KubernetesSubstrate {
    fn managed_by(&self) -> &'static str {
        "k8s"
    }

    fn supports_node_pool_placement(&self) -> bool {
        true
    }

    async fn create_execution<'a>(
        &'a self,
        request: CreateExecutionRequest<'a>,
    ) -> Result<CreateExecutionOutcome, SubstrateError> {
        let sandbox_id = request.sandbox_id;
        let sandbox_uuid = sandbox_id;

        // Cascade-amplifier backpressure: if recent cancel rate
        // exceeds the configured budget, reject new creates with 503 so we
        // don't pile more cleanup load onto a node-mgr that is already
        // saturated. Returning early here also short-circuits the DB write
        // in `ensure_active_execution_row`.
        if self.cancel_tracker.is_over_budget() {
            // Rejection is recorded as a tagged `sandbox.api.create.error`
            // with `reason:rejected_cancel_storm` — alerts can filter on
            // the tag without us also emitting a parallel counter.
            pplx_space_metrics::sandbox::api::create_step_error(
                sandbox_uuid,
                request.metric_identity,
                "fc",
                Some(request.template_ref.template.as_str()),
                "execution_create",
                "rejected_cancel_storm",
                None,
            );
            tracing::warn!(
                sandbox_id = %sandbox_id,
                cancel_count = self.cancel_tracker.current_count(),
                budget = self.cancel_tracker.budget_per_sec(),
                "create_execution rejected: cancel-storm backpressure active"
            );
            return Err(SubstrateError::Unavailable {
                message: "create_execution rejected: cancel-storm backpressure active".to_string(),
            });
        }

        match network_slot_capacity_preflight(
            &self.k8s.caches.nodes.state(),
            &self.pod_store.state(),
            &self.k8s.sandbox_pod_config,
        ) {
            NetworkSlotCapacityPreflight::Available => {}
            NetworkSlotCapacityPreflight::Unknown => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "create_execution proceeding with unknown Kubernetes network-slot capacity"
                );
            }
            NetworkSlotCapacityPreflight::Full => {
                pplx_space_metrics::sandbox::api::create_step_error(
                    sandbox_uuid,
                    request.metric_identity,
                    "fc",
                    Some(request.template_ref.template.as_str()),
                    "execution_create",
                    NETWORK_SLOT_CAPACITY_UNAVAILABLE_REASON,
                    None,
                );
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "create_execution rejected: cached Kubernetes network-slot capacity is full"
                );
                return Err(SubstrateError::Unavailable {
                    message:
                        "create_execution rejected: no Kubernetes network-slot capacity available"
                            .to_string(),
                });
            }
        }

        // Resolved from the `templates` row and the `node_groups` rows, for the
        // requesting org — on create the actor *is* the owner, and the cold pod
        // metadata stamps the same org onto the pod, which the builder
        // cross-checks against this placement. Both pod-request branches below
        // render it; neither may guess it, so a resolution failure fails the
        // create.
        //
        // Ahead of `begin_activation_execution_row`, and that ordering is the
        // point: this is the one create-path step that can fail on catalog
        // state rather than on Kubernetes, and every failure exit *after* the
        // activation row has to retire pod tracking and book an
        // `execution_create` step error. A `?` here would do neither, leaving
        // tracking bound on an attempt that never reached a pod and dropping
        // the create out of the step funnel entirely. Resolving first means the
        // failure happens while there is still nothing to unwind — and it also
        // keeps the database read out of the `execution_create` latency, which
        // measures the Kubernetes create.
        //
        // Everything it needs is in hand at function entry, so the hoist costs
        // nothing.
        let node_group_placement = self
            .node_group_placement
            .resolve(
                NodeGroupResolvePath::Create,
                PlacementSubject::Sandbox(request.actor.org_id),
                request.template_ref.template.as_str(),
            )
            .await?;

        let requested_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let execution = match self
            .store
            .begin_activation_execution_row(
                sandbox_id,
                requested_attempt_id,
                SandboxTransitionTargetState::Started,
            )
            .await
        {
            Ok(execution) => execution,
            Err(SubstrateError::ActivationInFlight {
                same_target: true,
                owner_attempt_id,
                ..
            }) => {
                let fence = self
                    .store
                    .reconcile_activation_fence(
                        sandbox_id,
                        requested_attempt_id,
                        Some(owner_attempt_id),
                        SandboxTransitionTargetState::Started,
                    )
                    .await?;
                let execution = fence.execution;
                let (_, phase_deadline) = ready_wait_bounds(super::CREATE_READY_BUDGET);
                let (pod_ip, binding) = self
                    .wait_for_pod_proxy_reachable(
                        request.metric_identity,
                        sandbox_id,
                        PodProxyWaitTarget {
                            pod_name: &execution.pod_name,
                            pod_uid: execution.pod_uid.as_ref().map(PodUid::as_str),
                            generation: execution.generation,
                        },
                        "create_join",
                        phase_deadline,
                    )
                    .await?;
                let binding = if request.wait_until_ready {
                    self.poll_until_command_ready(
                        sandbox_id,
                        request.metric_identity,
                        self.k8s
                            .create_command_ready_timeout
                            .min(remaining_ready_window(phase_deadline)),
                        CommandReadyPod::PreResolved(pod_ip),
                        Some(request.template_ref.template.as_str()),
                    )
                    .await?
                } else {
                    binding
                };
                let joined_pod = match self.lookup_sandbox_pod(&execution.pod_name) {
                    Some(pod) => Some(pod),
                    None => fetch_sandbox_pod_uncached(&self.k8s, &execution.pod_name)
                        .await
                        .map_err(|error| SubstrateError::Failed {
                            message: format!("failed to query joined create pod: {error}"),
                        })?,
                }
                .ok_or_else(|| SubstrateError::Unavailable {
                    message: "joined create reached readiness but its pod disappeared".to_string(),
                })?;
                let active_node_id = self
                    .remember_pod_node_placement(
                        sandbox_uuid,
                        &joined_pod,
                        PlacementRetryDisposition::RetryDuplicatesExecution,
                    )
                    .await?
                    .ok_or_else(|| SubstrateError::Unavailable {
                        message: "joined create pod has no registered node placement".to_string(),
                    })?;
                return Ok(CreateExecutionOutcome {
                    lease_completion: None,
                    sandbox_id,
                    binding: Some(binding),
                    initial_lifecycle: "started",
                    initial_placement: "on_node",
                    active_node_id: Some(active_node_id),
                    command_ready: true,
                });
            }
            Err(error) => return Err(error),
        };
        let activation_attempt_id =
            execution
                .activation_attempt_id
                .ok_or_else(|| SubstrateError::Failed {
                    message: "activation execution has no attempt fence".to_string(),
                })?;
        let pod_name = execution.pod_name.clone();

        let mut cancel_guard = CancelGuard::new(
            sandbox_id,
            pod_name.clone(),
            execution.generation,
            activation_attempt_id,
            Arc::clone(&self.cancel_tracker),
        );

        // Resolve the per-sandbox setup once through the shared routine, then
        // encode it into the pod spec. Warm-pool adoption resolves the same
        // setup from its claim request and applies it via gRPC; see
        // `per_sandbox_setup` for the compile-time guarantee that both paths
        // stay in sync.
        //
        // Cold derives the network-policy annotation generation from the
        // template counter on inherit; adoption deliberately uses 0 instead (it
        // adopts an already-booted pod whose generation counter is independent),
        // so the wire generation is computed per-path rather than shared.
        let cold_network_policy_generation = if request.network_policy_override.is_some() {
            request.network_policy_generation
        } else {
            request.template_network_policy_generation
        };
        let mut setup = PerSandboxSetup::from_create_request(&request);
        setup.auto_lifecycle = self
            .store
            .auto_lifecycle_policy_after_claim(sandbox_id)
            .await?;
        let cold_metadata = setup.into_cold_pod_metadata(cold_network_policy_generation)?;

        let pod_create_started_at = Instant::now();
        let common = SandboxPodCommon {
            pod_name: &pod_name,
            sandbox_id: sandbox_uuid,
            execution_generation: execution.generation,
            activation_attempt_id: execution.bootstrap_route_activation_id(),
            template: request.template_ref.template.as_str(),
            content_hash: request.template_ref.content_hash,
            env: request.env,
            traceparent: pplx_space_observability::trace_propagation::current_traceparent(),
        };
        let runtime = cold_metadata.runtime_metadata(request.resources);
        let restore_source_sandbox_id: String;
        let pod_request = match request.restore_from.as_ref() {
            Some(restore) => {
                let storage_uri = restore.storage_uri.ok_or_else(|| SubstrateError::Failed {
                    message: "fork disk restore requires an exact artifact storage URI".to_string(),
                })?;
                restore_source_sandbox_id = restore.source_sandbox_id.to_string();
                SandboxPodRequest::Restore {
                    artifact_kind: pplx_space_common_core::types::ArtifactKind::Disk,
                    common,
                    runtime,
                    suspend_restore_storage_uri: Some(storage_uri),
                    suspend_restore_source_node: None,
                    fork_disk_restore: Some(Box::new(ForkDiskRestorePodContext {
                        source_sandbox_id: &restore_source_sandbox_id,
                        artifact_snapshot_id: restore.snapshot_id,
                        target_owner: request.actor.sandbox_owner.as_str(),
                    })),
                    direct_node_name: None,
                    preferred_node_name: None,
                    restore_node_shortlist: Vec::new(),
                    extra_tolerations: &[],
                    extra_required_affinity: None,
                    // A fork restore creates a *new* sandbox from a snapshot,
                    // so the set is the target org's for the target template —
                    // resolved above with the rest of the create path. It
                    // carries no source-node pin (`direct_node_name: None`),
                    // so kube-scheduler places it inside the set.
                    node_group_placement: node_group_placement.placement(),
                }
            }
            None => SandboxPodRequest::OnDemand {
                common,
                runtime,
                // Create-time isolation pool pin, straight from the request; the
                // PodSpec gets a nodeSelector + toleration. Not persisted.
                node_pool: request.node_pool,
                node_group_placement: node_group_placement.placement(),
            },
        };
        let pod = match create_sandbox_pod(&self.k8s, pod_request)
            .instrument(tracing::info_span!(
                "substrate.k8s.create_pod",
                sandbox_id = %sandbox_id,
                pod_name = %pod_name,
                namespace = %self.k8s.sandbox_namespace,
                template = %request.template_ref.template,
            ))
            .await
        {
            Ok(pod) => {
                // Pod exists — arm the guard so cancellation triggers cleanup.
                let pod_uid = pod
                    .metadata
                    .uid
                    .as_deref()
                    .and_then(|uid| PodUid::parse(uid).ok());
                cancel_guard.arm(self.k8s.clone(), self.store.clone(), pod_uid);
                pod
            }
            Err(err) => {
                pplx_space_metrics::sandbox::api::create_step_error(
                    sandbox_uuid,
                    request.metric_identity,
                    "fc",
                    Some(request.template_ref.template.as_str()),
                    "execution_create",
                    "kubernetes_api",
                    None,
                );
                if let Err(forget_err) = self
                    .store
                    .forget_pod_tracking_if_generation_and_attempt(
                        sandbox_uuid,
                        execution.generation,
                        activation_attempt_id,
                    )
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = execution.generation,
                        error = %forget_err,
                        "failed to forget Kubernetes pod tracking after pod create failure"
                    );
                }
                return Err(SubstrateError::Failed {
                    message: format!("failed to create sandbox pod: {err}"),
                });
            }
        };

        pplx_space_metrics::sandbox::api::create_step_latency(
            sandbox_uuid,
            pplx_space_metrics::sandbox::api::CreateStep {
                identity: request.metric_identity,
                backend: "fc",
                template: Some(request.template_ref.template.as_str()),
                step: "execution_create",
                result: "success",
                started_at: pod_create_started_at.into(),
                phase: None,
            },
        );

        let mut active_node_id = self
            .remember_pod_node_placement(
                sandbox_uuid,
                &pod,
                PlacementRetryDisposition::RetryDuplicatesExecution,
            )
            .await?;
        let pod_uid = pod.metadata.uid.clone();
        let pod_status = pod.status.as_ref();
        let pod_ip = assigned_pod_ip(pod_status)?;
        let phase = normalize_pod_phase(
            pod_status.and_then(|status| status.phase.clone()),
            "pending",
        );

        let pod_ip_string = pod_ip.map(|ip| ip.to_string());
        match self
            .store
            .persist_pod_metadata(
                sandbox_uuid,
                execution.generation,
                pod_uid.as_deref(),
                pod_ip_string.as_deref(),
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = execution.generation,
                "pod metadata write no-oped after pod create: the execution was \
                 retired or replaced mid-flight"
            ),
            Err(err) => tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = execution.generation,
                error = %err,
                "failed to persist Kubernetes pod metadata after pod create"
            ),
        }

        let wait_until_ready = request.wait_until_ready;
        // Cap the combined readiness wait at the ALB-aware budget. The two
        // sub-waits below run sequentially and their per-phase timeouts can
        // sum past the upstream gateway ceiling; this outer bound guarantees
        // the create returns before the gateway would cut the connection.
        // The inner waits clamp to `phase_deadline` so their phase-tagged
        // timeouts fire first; the outer timeout is the backstop for the
        // stretches between them.
        let (create_budget, phase_deadline) = ready_wait_bounds(super::CREATE_READY_BUDGET);
        let select_outcome = match tokio::time::timeout(create_budget, async {
            let (pod_ip, binding) = self
                .wait_for_pod_proxy_reachable(
                    request.metric_identity,
                    sandbox_id,
                    PodProxyWaitTarget {
                        pod_name: &pod_name,
                        pod_uid: pod_uid.as_deref(),
                        generation: execution.generation,
                    },
                    "create",
                    phase_deadline,
                )
                .instrument(tracing::info_span!(
                    "substrate.k8s.wait_pod_proxy_reachable",
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    namespace = %self.k8s.sandbox_namespace,
                ))
                .await?;
            if wait_until_ready {
                // Command-ready keeps its own (shorter) bound inside the
                // outer ALB budget — intentionally distinct from it. The
                // pod proxy is already reachable here, so the guest agent
                // normally comes up in seconds; the cap stops a single
                // create from pinning on a wedged guest. When it does
                // fire, the outer match preserves the (scheduled,
                // running) pod and returns the sandbox as
                // `started`/`command_ready=false` — the client's first
                // data-plane op then gets the SDK-recoverable
                // `sandbox_timeout` (the guest is briefly unreachable)
                // and converges on retry. So a guest that needs longer
                // than this cap still heals; we don't hold the create
                // call open for the full budget waiting on it.
                self.poll_until_command_ready(
                    sandbox_id,
                    request.metric_identity,
                    self.k8s
                        .create_command_ready_timeout
                        .min(remaining_ready_window(phase_deadline)),
                    CommandReadyPod::PreResolved(pod_ip),
                    Some(request.template_ref.template.as_str()),
                )
                .await
            } else {
                Ok(binding)
            }
        })
        .await
        {
            Ok(outcome) => outcome,
            // Neutral message: whether the work is preserved (pod left running,
            // retry the same sandbox) or torn down (pod never scheduled) is
            // decided by the preserve check below, so don't promise either here.
            Err(_elapsed) => Err(SubstrateError::Timeout {
                message: format!(
                    "sandbox create did not reach readiness within {}s",
                    super::CREATE_READY_BUDGET.as_secs()
                ),
                phase: None,
            }),
        };
        let binding = match select_outcome {
            Ok(binding) => binding,
            Err(err) => {
                // Slow-but-healthy create: the pod is still coming up. Only
                // preservable if it's far enough along that a client retry can
                // reach it (execution past `creating`, with a persisted IP) —
                // then leave the pod running (no delete, no `forget_pod_tracking`)
                // and report the sandbox as `started` on its node, NOT
                // `creating` (which the transient-lifecycle sweeper would age
                // out and delete). `command_ready: false` lets the caller emit
                // `create_pending` and skip the binding it never got. If the
                // pod isn't preservable (no IP/state still `creating`, terminal,
                // or unscheduled), fall through to teardown.
                // Bound the preserve decision itself: it runs inside the
                // post-budget response margin and can fall through to an
                // unbounded apiserver GET on a cache miss. A slow decision
                // degrades to teardown (`Err(Elapsed)` / `Ok(None)` both fall
                // through) rather than overrunning the margin and re-tripping
                // the latency cliff.
                if create_wait_should_preserve_node_work(&err)
                    && let Ok(Some(node_id)) = tokio::time::timeout(
                        super::PRESERVE_DECISION_BUDGET,
                        self.preservable_create_node(sandbox_uuid, &pod_name),
                    )
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        "sandbox create exhausted the ALB-aware readiness budget but the pod is \
                         scheduled and running; returning it as started-on-node so the node \
                         converges and the client's data-plane retry reaches it"
                    );
                    cancel_guard.defuse();
                    self.store
                        .release_activation_attempt_best_effort(
                            sandbox_id,
                            execution.generation,
                            activation_attempt_id,
                        )
                        .await;
                    return Ok(CreateExecutionOutcome {
                        lease_completion: None,
                        sandbox_id,
                        binding: None,
                        initial_lifecycle: "started",
                        initial_placement: "on_node",
                        active_node_id: Some(node_id),
                        command_ready: false,
                    });
                }
                // Cascade-amplifier short-circuit: when the failure looks
                // like node-mgr is unreachable *and* the pod is
                // authoritatively absent (cache + apiserver GET both say
                // it's gone), defer cleanup to the orphaned-sandbox
                // reconciler. `read_sandbox_pod` falls back to a real GET
                // on cache misses, so a reflector-lag false-miss can't
                // strand the row in `creating` — the reconciler's
                // `OrphanKind::Normal` path skips Present pods, and we
                // only want to defer when there's nothing for it to miss.
                // Otherwise fall through to the synchronous delete and
                // let the downstream cascade controls (sbx-mgr cleanup
                // cap, node-mgr per-method limiter) absorb the load.
                //
                // The outer ALB-budget timeout synthesizes a generic
                // `Timeout` that carries no node-mgr signal, so during a
                // node-mgr brownout `is_nodemgr_unreachable` would miss it and
                // re-enter the synchronous delete that feeds the brownout.
                // Treat a budget timeout with an authoritatively-absent pod the
                // same way: defer to the reconciler — there's nothing to delete
                // anyway, and it keeps the cascade protection intact.
                let err_defers_cleanup =
                    is_nodemgr_unreachable(&err) || matches!(err, SubstrateError::Timeout { .. });
                let pod_authoritatively_missing = err_defers_cleanup
                    && matches!(self.read_sandbox_pod(&pod_name).await, Ok(None));
                if pod_authoritatively_missing {
                    cancel_guard.mark_cause(CancelCause::NodeMgrUnreachable);
                    return Err(err);
                }
                // Cleanup runs after the budget already expired, but it is NOT
                // response-margin-critical: the binding upstream cut is the
                // node-RPC channel timeout (which doesn't bound this kube
                // DELETE), and the ALB idle timeout is far higher (see
                // `ALB_IDLE_TIMEOUT_SECS`). So let the delete complete — an
                // unbounded delete is what actually removes the pod, whereas
                // abandoning it on a timeout would forget tracking + delete the
                // row and leave the pod running with nothing to GC it.
                // `Ok` here means the apiserver *accepted* a graceful delete
                // (`DeleteParams::default()`), not that the pod is gone: it
                // sets `deletionTimestamp` and the VM runs through its grace
                // period, staying Terminating indefinitely if the node is
                // unreachable — which is one of the conditions that reach this
                // arm. So this is "cleanup was handed off", which is the most
                // this call can establish; the orphan reconciler owns the rest.
                // The distinction matters because it is the demotion's input.
                let pod_cleanup_accepted = match delete_sandbox_pod(&self.k8s, &pod_name).await {
                    Ok(()) => true,
                    // `delete_sandbox_pod` propagates the raw apiserver error,
                    // so an already-absent pod arrives here as 404. That is the
                    // most confirmed removal there is — treating it as
                    // unconfirmed would demote the common benign race.
                    Err(delete_err) if is_not_found_error(&delete_err) => true,
                    Err(delete_err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            error = %delete_err,
                            "failed to delete sandbox pod during creation cleanup — pod may be orphaned"
                        );
                        false
                    }
                };
                if let Err(forget_err) = self
                    .store
                    .forget_pod_tracking_if_generation_and_attempt(
                        sandbox_uuid,
                        execution.generation,
                        activation_attempt_id,
                    )
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = execution.generation,
                        error = %forget_err,
                        "failed to forget Kubernetes pod tracking after creation cleanup"
                    );
                }
                cancel_guard.defuse();
                // The pod delete is best-effort and only logs. When it was not
                // even accepted, nothing is removing this pod, so `err` must
                // not tell the caller to retry: `create` mints a fresh
                // `SandboxId`, so the retry buys a second sandbox and leaves
                // this one orphaned with no cleanup pending. An accepted
                // delete puts the pod on a path to removal and the row is
                // deleted below, so a contention abort stays retryable there —
                // which is the common case and the one the bound needs.
                if pod_cleanup_accepted {
                    return Err(err);
                }
                return Err(err.demoted_after_side_effect());
            }
        };

        let initial_placement = "on_node";
        let initial_lifecycle = match phase.as_str() {
            "failed" => "failed",
            _ => "started",
        };

        // The reflector watch and the per-pod watch used by
        // `wait_for_pod_proxy_reachable` are independent streams, so the
        // reflector cache may briefly lag the scheduler-binding MODIFIED
        // event. We are at the post-readiness step, so the pod must have
        // been bound to a node by now; if the cache still shows it as
        // unscheduled, that is definitive evidence of a stale cache and
        // we must fall back to an authoritative GET. Without this check,
        // a stale cache hit would leave `active_node_id` as `None` and the
        // caller's `UPDATE sandboxes SET active_node_id = $3` would clear
        // the placement that the watch already persisted to the DB.
        let post_create_pod = match self.lookup_sandbox_pod(&pod_name) {
            Some(pod) if assigned_pod_node_name(&pod).is_some() => Some(pod),
            _ => fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                .await
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to query created sandbox pod placement: {err}"),
                })?,
        };
        if let Some(pod) = post_create_pod
            && let Some(node_id) = self
                .remember_pod_node_placement(
                    sandbox_uuid,
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?
        {
            active_node_id = Some(node_id);
        }

        cancel_guard.defuse();
        self.store
            .release_activation_attempt_best_effort(
                sandbox_id,
                execution.generation,
                activation_attempt_id,
            )
            .await;
        Ok(CreateExecutionOutcome {
            lease_completion: Some(super::super::ActivationLeaseCompletion {
                generation: execution.generation,
                attempt_id: activation_attempt_id,
            }),
            sandbox_id,
            binding: Some(binding),
            initial_lifecycle,
            initial_placement,
            active_node_id,
            command_ready: true,
        })
    }

    async fn complete_activation_lease(
        &self,
        sandbox_id: SandboxId,
        completion: Option<super::super::ActivationLeaseCompletion>,
    ) {
        if let Some(completion) = completion {
            self.store
                .release_finished_activation_attempt_best_effort(
                    sandbox_id,
                    completion.generation,
                    completion.attempt_id,
                )
                .await;
        }
    }

    async fn delete_execution(&self, sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        // Unfenced: a delete is the caller asking for the sandbox to be gone
        // whatever pod currently backs it, so there is no observed identity to
        // pin to and the teardown can only report `Retired`.
        self.teardown_execution(
            sandbox_id,
            ExecutionTeardown::Delete,
            &RetireFence::Unobserved,
        )
        .await
        .map(|_| ())
    }

    /// Answered entirely from the sandbox-pod reflector — the same cache every
    /// hot read uses — so a caller can snapshot a whole candidate batch for
    /// the cost of a map lookup each. A pod missing from the cache is omitted
    /// rather than fetched: the caller wanted a cheap observation, and a
    /// per-candidate apiserver GET is exactly what it was avoiding.
    async fn observe_pod_identities(
        &self,
        pod_names: &[PodName],
    ) -> std::collections::HashMap<PodName, ObservedPodIdentity> {
        pod_names
            .iter()
            .filter_map(|pod_name| {
                let pod = self.lookup_sandbox_pod(pod_name.as_str())?;
                let pod_uid = pod
                    .metadata
                    .uid
                    .as_deref()
                    .and_then(|uid| PodUid::parse(uid).ok())?;
                let resource_version = pod
                    .metadata
                    .resource_version
                    .as_deref()
                    .and_then(|version| PodResourceVersion::parse(version).ok());
                Some((
                    pod_name.clone(),
                    ObservedPodIdentity {
                        pod_uid,
                        resource_version,
                    },
                ))
            })
            .collect()
    }

    async fn retire_execution(
        &self,
        request: RetireExecutionRequest,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        self.teardown_execution(
            request.sandbox_id,
            ExecutionTeardown::Retire,
            &request.fence,
        )
        .await
    }

    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, SubstrateError> {
        match fence {
            // Nothing was observed, so there is nothing that could have
            // changed underneath the caller.
            RetireFence::Unobserved => Ok(None),
            RetireFence::NoExecution => Ok(self
                .store
                .active_execution_status(sandbox_id)
                .await?
                .map(|_| PodFenceMismatch::ExecutionMissing)),
            RetireFence::Pod(pod_fence) => {
                match self
                    .check_pod_fence(
                        sandbox_id,
                        pod_fence,
                        FenceReadFreshness::MustBeAuthoritative,
                    )
                    .await?
                {
                    // Nothing to refuse either way: the retire will hold, or
                    // it has already happened.
                    PodFenceCheck::Holds(_) | PodFenceCheck::AlreadyRetired => Ok(None),
                    PodFenceCheck::Mismatch(mismatch) => Ok(Some(mismatch)),
                }
            }
        }
    }

    async fn delete_observed_execution(
        &self,
        sandbox_id: SandboxId,
        target: &NodeApiTarget<'_>,
    ) -> Result<(), SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .db_context("failed to start pool slot delete transaction")?;
        let slot = match WarmPoolStore::lock_open_slot_for_delete_by_reserved_sandbox_id(
            &mut tx,
            sandbox_uuid,
        )
        .await
        .db_context("failed to lock observed pool slot for delete")?
        {
            OpenPoolSlotDeleteLock::Locked(slot) => slot,
            OpenPoolSlotDeleteLock::NotFound => {
                return super::super::delete_observed_execution_via_node_api(
                    self, sandbox_id, target,
                )
                .await;
            }
            OpenPoolSlotDeleteLock::NotOpen { state } => {
                return Err(SubstrateError::Conflict {
                    message: format!(
                        "observed Kubernetes pool slot is {state}; delete the adopted sandbox instead"
                    ),
                });
            }
            OpenPoolSlotDeleteLock::Busy => {
                return Err(SubstrateError::Conflict {
                    message: "observed Kubernetes pool slot is busy; retry delete".to_string(),
                });
            }
        };

        let pods: Api<Pod> = Api::namespaced(self.k8s.client.clone(), &slot.pod_namespace);
        if let Err(err) = pods.delete(&slot.pod_name, &DeleteParams::default()).await
            && !is_not_found_error(&err)
        {
            return Err(SubstrateError::Failed {
                message: format!("failed to delete observed pool slot pod: {err}"),
            });
        }

        // Classified even though the pod above is already gone: unlike create,
        // this path is idempotent under retry — the re-run's `pods.delete`
        // answers NotFound, which the guard above tolerates, and the row delete
        // converges to the same absent state. The lock and the commit around it
        // are already classified, so leaving this one statement out is the hole
        // that reports an abort here as permanent while its neighbours in the
        // same transaction report it as retryable.
        let deleted = WarmPoolStore::delete_locked_open_slot(&mut tx, slot.id)
            .await
            .db_context("failed to delete observed pool slot row")?;
        if !deleted {
            return Err(SubstrateError::Conflict {
                message: "observed Kubernetes pool slot changed before delete".to_string(),
            });
        }
        tx.commit()
            .await
            .db_context("failed to commit observed pool slot delete")?;

        tracing::info!(
            sandbox_id = %sandbox_id,
            pool_slot_id = %slot.id,
            pod_name = %slot.pod_name,
            pod_namespace = %slot.pod_namespace,
            "deleted observed Kubernetes pool slot"
        );

        Ok(())
    }

    async fn start_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        if !options.blocking {
            return Err(SubstrateError::Conflict {
                message: "kubernetes substrate requires blocking=true for start".to_string(),
            });
        }
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        if !is_recoverable_live_lifecycle(&sandbox.lifecycle) {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox cannot be started while lifecycle is {}",
                    sandbox.lifecycle
                ),
            });
        }
        // `start` on a `suspended`/`restoring` sandbox IS a restore: it loads a
        // snapshot and may leave live node work behind. Route it through the
        // shared wake/restore machinery (`wake_execution_inner` via
        // `wake_execution_with_metric_identity`) so it claims `suspended ->
        // restoring` + a restore lease, bounds the wait below the channel
        // timeout, and reverts/releases under the unifying invariant. Without
        // this, a timed-out start would leave the row `suspended` with a live
        // restore pod — swept by neither the orphaned-sandbox nor the
        // transient-lifecycle sweeper — stranding the pod and making retries see
        // a dormant sandbox. Non-restore starts (e.g. `stopped`) keep the inline
        // path below; they never claim `restoring`, so there's no orphan to
        // guard against.
        if matches!(
            normalize_lifecycle(&sandbox.lifecycle).as_str(),
            "suspended" | "restoring"
        ) {
            return self
                .wake_execution_with_metric_identity(
                    &sandbox,
                    options.ready_timeout,
                    metric_identity,
                    charge,
                )
                .await;
        }
        // The controller may have deferred admission from a stale restore-
        // shaped snapshot. If the refreshed row now takes the cold-start path,
        // spend that deferred charge before creating an execution.
        charge.charge_cold_activation().await?;
        // Only non-restore lifecycles (`stopped`, etc.) reach here — the
        // `suspended`/`restoring` cases delegated to the restore machinery
        // above — so this resolves to `None` (no snapshot to restore from).
        let restore_plan = if matches!(
            normalize_lifecycle(&sandbox.lifecycle).as_str(),
            "suspended" | "restoring"
        ) {
            Some(self.suspend_restore_plan(&sandbox, false).await?)
        } else {
            None
        };
        // Bound the entire blocking start phase — pod (re)creation, the
        // blocking `wake_sandbox` RPC, and command-ready polling — below the
        // node-RPC channel timeout, exactly like `wake_execution_inner`.
        // Without this the blocking wake (governed only by the 120s endpoint
        // timeout) could ride up to the channel cut and surface as the opaque
        // `Cancelled` cliff this change exists to prevent; the clamp makes a
        // slow start return a structured, retryable `Timeout` instead.
        // The command-ready poll clamps to `phase_deadline` so its
        // phase-tagged timeout fires before this budget's generic one; the
        // backstop sits one phase margin above the caller's requested wait
        // (capped at `CREATE_READY_BUDGET`), so the caller's effective
        // readiness wait is never shortened by the margin.
        let (wait_budget, phase_deadline) = ready_wait_bounds(options.ready_timeout);
        let requested_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let activation_assignment = ActivationAttemptAssignment::new(
            requested_attempt_id,
            SandboxTransitionTargetState::Started,
        );
        let wait = async {
            // Boxed: `ensure_pod_exists` is a large future, and inlining it in
            // this async block overflows the test thread's stack.
            let (activation_attempt_id, joined_attempt_id) = match Box::pin(
                self.ensure_pod_exists(
                    &sandbox,
                    None,
                    restore_plan
                        .as_ref()
                        .and_then(|plan| plan.source_node_name.as_deref()),
                    restore_plan
                        .as_ref()
                        .and_then(|plan| plan.storage_uri.as_deref()),
                    PodMaterializationOwnership::new(
                        None,
                        // Non-restore start path holds no restore lease.
                        None,
                        Some(activation_assignment.clone()),
                    ),
                ),
            )
            .await
            {
                Ok(()) => (activation_assignment.assigned_or_requested(), None),
                Err(SubstrateError::ActivationInFlight {
                    same_target: true,
                    owner_attempt_id,
                    ..
                }) => (requested_attempt_id, Some(owner_attempt_id)),
                Err(error) => {
                    // Materialization can acquire ownership before placement
                    // is available. No Wake was dispatched, so a retry must
                    // re-enter recovery instead of joining this abandoned lease.
                    if let Some((attempt_id, generation)) = activation_assignment.assigned() {
                        self.store
                            .release_finished_activation_attempt_best_effort(
                                sandbox_id, generation, attempt_id,
                            )
                            .await;
                    }
                    return Err(error);
                }
            };
            let fence = self
                .store
                .reconcile_activation_fence(
                    sandbox_id,
                    activation_attempt_id,
                    joined_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await?;
            let fence_ownership = fence.ownership();
            let owned_attempt = fence
                .owned_attempt_id()
                .map(|attempt_id| (attempt_id, fence.execution.generation));
            let active_execution = fence.execution;
            let wake_action = activation_wake_action(
                &active_execution,
                fence_ownership,
                SandboxLifecycle::Started,
            )?;
            let restore_storage_uri = restore_plan
                .as_ref()
                .and_then(|plan| plan.storage_uri.as_deref());
            SandboxEvent::new(
                sandbox_id,
                SandboxEventType::ExecutionEnsured,
                SandboxEventSource::Substrate,
            )
            .emit_with(sandbox_template_event_details(
                &sandbox,
                restore_storage_uri,
                SandboxEventDetails::default()
                    .operation("start")
                    .lifecycle_state(sandbox.lifecycle.as_str())
                    .bool_field(
                        SandboxEventBoolField::RestoreStorageUriPresent,
                        restore_storage_uri.is_some(),
                    ),
            ));
            let emit_wake_requested = || {
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::WakeRequested,
                    SandboxEventSource::Substrate,
                )
                .emit_with(sandbox_template_event_details(
                    &sandbox,
                    restore_storage_uri,
                    SandboxEventDetails::default()
                        .operation("start")
                        .bool_field(SandboxEventBoolField::HasNetnsPath, false)
                        .bool_field(
                            SandboxEventBoolField::RestoreStorageUriPresent,
                            restore_storage_uri.is_some(),
                        ),
                ));
            };
            let wake_result = self
                .execute_activation_wake_action(
                    sandbox_id,
                    wake_action,
                    phase_deadline,
                    |route_identity, activation_policy| {
                        pplx_space_sbx_mgr_client::proto::WakeRequest {
                            implicit_wake_source: 0,
                            sandbox_id: sandbox_id.to_string(),
                            blocking: true,
                            start_context: Some(
                                pplx_space_sbx_mgr_client::proto::WakeStartContext {
                                    activation_auto_lifecycle: Some(activation_policy),
                                    expected_execution_generation: route_identity
                                        .execution_generation,
                                    operation_activation_attempt_id: active_execution
                                        .activation_attempt_id
                                        .map(|id| id.to_string()),
                                    vcpu: sandbox_vcpu_option(&sandbox),
                                    memory_mib: sandbox_memory_mib_option(&sandbox),
                                    netns_path: None,
                                    suspend_restore_storage_uri: restore_plan
                                        .as_ref()
                                        .and_then(|plan| plan.storage_uri.clone()),
                                    suspend_restore_source_node: restore_plan
                                        .as_ref()
                                        .and_then(|plan| plan.source_node_name.clone()),
                                    route_identity: Some(wake_request_route_identity(
                                        route_identity,
                                    )),
                                },
                            ),
                            target_state: 0,
                        }
                    },
                    emit_wake_requested,
                )
                .await;
            let (wake_outcome, route_identity) = match wake_result {
                Ok(outcome) => outcome,
                Err(failure) => {
                    if failure.operation_release_is_safe()
                        && let Some((attempt_id, generation)) = owned_attempt
                    {
                        self.store
                            .release_finished_activation_attempt_best_effort(
                                sandbox_id, generation, attempt_id,
                            )
                            .await;
                    }
                    let dispatched = failure.was_dispatched();
                    let error = failure.into_error();
                    if dispatched {
                        SandboxEvent::new(
                            sandbox_id,
                            SandboxEventType::WakeFailed,
                            SandboxEventSource::Substrate,
                        )
                        .emit_with(sandbox_template_event_details(
                            &sandbox,
                            restore_storage_uri,
                            SandboxEventDetails::default()
                                .operation("start")
                                .error(error.to_string()),
                        ));
                    }
                    return Err(error);
                }
            };
            let emit_wake_completed = |readiness_fallback: bool| {
                let mut details = SandboxEventDetails::default().operation("start");
                if readiness_fallback {
                    details = details.reason("resident_route_unchanged_readiness_fallback");
                }
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::WakeCompleted,
                    SandboxEventSource::Substrate,
                )
                .emit_with(sandbox_template_event_details(
                    &sandbox,
                    restore_storage_uri,
                    details,
                ));
            };
            if wake_outcome.dispatch.was_dispatched() && wake_outcome.landed {
                emit_wake_completed(false);
            }
            let binding = self
                .poll_until_command_ready(
                    sandbox_id,
                    metric_identity,
                    options
                        .ready_timeout
                        .min(remaining_ready_window(phase_deadline)),
                    match route_identity.as_ref() {
                        Some(route_identity) => CommandReadyPod::SelectedRoute {
                            route_identity,
                            pod_ip: wake_outcome.pod_ip,
                        },
                        None => CommandReadyPod::ResolveLegacy,
                    },
                    None,
                )
                .await?;

            if wake_outcome.dispatch.was_dispatched() && !wake_outcome.landed {
                emit_wake_completed(true);
            }

            Ok((binding, owned_attempt))
        };
        let result = match tokio::time::timeout(wait_budget, wait).await {
            Ok(inner) => inner,
            Err(_elapsed) => Err(SubstrateError::Timeout {
                message: format!(
                    "sandbox start did not reach readiness within {}s; the node is still \
                     starting — retry the same sandbox to converge",
                    wait_budget.as_secs()
                ),
                phase: None,
            }),
        };
        if let Ok((_, Some((activation_attempt_id, generation)))) = &result {
            self.store
                .release_activation_attempt_best_effort(
                    sandbox_id,
                    *generation,
                    *activation_attempt_id,
                )
                .await;
        }
        result.map(|(binding, owned_attempt)| {
            let mut activation = ExecutionActivation::owned(binding);
            activation.lease_completion = owned_attempt.map(|(attempt_id, generation)| {
                super::super::ActivationLeaseCompletion {
                    generation,
                    attempt_id,
                }
            });
            activation
        })
    }

    async fn restore_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ActivationCompletion, SubstrateError> {
        // User-API restore is always to `Started` — the HTTP wake/restore
        // contract returns a running sandbox. Only the drain pipeline's
        // `restore_evacuated_execution` opts into `Paused`.
        self.restore_execution_inner(
            metric_identity,
            sandbox_id,
            options,
            None,
            pplx_space_common_core::types::SandboxLifecycle::Started,
            charge,
        )
        .await
    }

    async fn suspend_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        let suspend_deadline = Instant::now() + TEARDOWN_SUSPEND_DURABILITY_BUDGET;
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::SuspendRequested,
            SandboxEventSource::Substrate,
        )
        .emit_with(SandboxEventDetails::default());
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox is not assigned to a pod".to_string(),
            })?;
        let expected_generation =
            ExecutionGeneration::new(execution.generation).map_err(|error| {
                SubstrateError::Failed {
                    message: format!("invalid active Kubernetes execution generation: {error}"),
                }
            })?;
        let expected_pod_uid =
            execution
                .pod_uid
                .clone()
                .ok_or_else(|| SubstrateError::Unavailable {
                    message: "sandbox pod has no Kubernetes UID for suspend retirement".to_string(),
                })?;
        let suspend_claim = self
            .store
            .claim_suspend_execution(sandbox_id, expected_generation, &expected_pod_uid)
            .await?;
        tracing::info!(
            %sandbox_id,
            generation = %expected_generation,
            claim = %suspend_claim,
            "claimed generation-bound Kubernetes suspend"
        );
        let committed_suspend_artifact = match suspend_claim {
            SuspendExecutionClaim::Completed(artifact) => Some(artifact),
            SuspendExecutionClaim::Claimed | SuspendExecutionClaim::Existing => {
                self.store
                    .committed_suspend_artifact(sandbox_id, expected_generation, &expected_pod_uid)
                    .await?
            }
        };
        let node_suspend_artifact = if committed_suspend_artifact.is_some() {
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = %expected_generation,
                "resuming pod retirement for a committed Kubernetes suspend"
            );
            None
        } else {
            let k8s_node_name =
                execution
                    .k8s_node_name
                    .as_deref()
                    .ok_or_else(|| SubstrateError::Unavailable {
                        message: "sandbox pod is not assigned to a Kubernetes node".to_string(),
                    })?;
            let target = NodeApiTarget {
                managed_by: SpaceNodeManagedBy::Kubernetes,
                role: SpaceNodeRole::Worker,
                direct_endpoint: None,
                substrate_node_id: Some(k8s_node_name),
            };
            let suspend_path_started_at = Instant::now();
            let endpoint_addr = match tokio::time::timeout_at(
                suspend_deadline,
                self.resolve_node_api_endpoint(&target),
            )
            .await
            {
                Err(_) => {
                    return Err(SubstrateError::Timeout {
                        message: format!(
                            "timed out resolving node-api for generation-bound suspend of sandbox {sandbox_id}"
                        ),
                        phase: None,
                    });
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        k8s_node_name,
                        elapsed_ms = duration_millis_to_u64(suspend_path_started_at.elapsed()),
                        error = %err,
                        "failed to resolve node-api endpoint for Kubernetes sandbox suspend"
                    );
                    return Err(err);
                }
                Ok(Ok(endpoint_addr)) => endpoint_addr,
            };
            let endpoint = match node_api_endpoint(
                &endpoint_addr,
                POD_WORKER_CONNECT_TIMEOUT,
                NODE_RPC_TIMEOUT,
            ) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        k8s_node_name,
                        node_api_endpoint = %endpoint_addr,
                        elapsed_ms = duration_millis_to_u64(suspend_path_started_at.elapsed()),
                        error = %err,
                        "invalid node-api endpoint for Kubernetes sandbox suspend"
                    );
                    return Err(err);
                }
            };
            let mut client = match tokio::time::timeout_at(
                suspend_deadline,
                connect_worker(endpoint),
            )
            .await
            {
                Err(_) => {
                    return Err(SubstrateError::Timeout {
                        message: format!(
                            "timed out connecting node-api for generation-bound suspend of sandbox {sandbox_id}"
                        ),
                        phase: None,
                    });
                }
                Ok(Ok(client)) => client,
                Ok(Err(err)) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        k8s_node_name,
                        node_api_endpoint = %endpoint_addr,
                        elapsed_ms = duration_millis_to_u64(suspend_path_started_at.elapsed()),
                        error = %err,
                        "failed to connect node-api for Kubernetes sandbox suspend"
                    );
                    return Err(SubstrateError::Unavailable {
                        message: format!("node-api not reachable at {endpoint_addr}: {err}"),
                    });
                }
            };

            let suspend_rpc_started_at = Instant::now();
            let published_artifact = match await_generation_suspend(
                &mut client,
                sandbox_id,
                expected_generation,
                &endpoint_addr,
                suspend_deadline,
            )
            .await
            {
                Ok(published_artifact) => published_artifact,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = %expected_generation,
                        k8s_node_name,
                        node_api_endpoint = %endpoint_addr,
                        elapsed_ms = duration_millis_to_u64(suspend_rpc_started_at.elapsed()),
                        error = %error,
                        "generation-bound Kubernetes sandbox suspend did not become durable"
                    );
                    SandboxEvent::new(
                        sandbox_id,
                        SandboxEventType::SuspendFailed,
                        SandboxEventSource::Substrate,
                    )
                    .emit_with(SandboxEventDetails::default().error(error.to_string()));
                    return Err(error);
                }
            };
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = %expected_generation,
                k8s_node_name,
                node_api_endpoint = %endpoint_addr,
                local_artifact_id = %published_artifact.id,
                elapsed_ms = duration_millis_to_u64(suspend_rpc_started_at.elapsed()),
                "generation-bound Kubernetes sandbox suspend became durable"
            );
            Some(published_artifact)
        };

        self.finish_suspend_handoff(
            sandbox_id,
            expected_generation,
            expected_pod_uid,
            execution.pod_name,
            node_suspend_artifact,
        )
        .await
        .map(Some)
    }

    async fn wake_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        wake_activity: Option<ExecutionActivityKind>,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        self.wake_execution_with_metric_identity_and_activity(
            sandbox,
            timeout,
            metric_identity,
            charge,
            wake_activity,
        )
        .await
    }

    async fn runtime_client(
        &self,
        sandbox_id: SandboxId,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        let sandbox_before_resolution = self
            .store
            .require_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        if let Some(pod_ip) = self
            .resolve_pod_ip(sandbox_id)
            .await
            .map_err(ControllerError::from)?
            && let Ok(client) = self.worker_client_for_pod_ip(pod_ip).await
        {
            return Ok(client);
        }

        let sandbox = self
            .store
            .require_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        if normalize_lifecycle(&sandbox_before_resolution.lifecycle) == "suspending"
            && is_suspended_lifecycle(&sandbox.lifecycle)
        {
            let binding = self
                .wake_execution_with_metric_identity(
                    &sandbox,
                    Duration::from_secs(120),
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                        "runtime_client",
                    ),
                    charge,
                )
                .await
                .map_err(ControllerError::from)?;
            return connect_worker(binding.binding.runtime_endpoint).await;
        }
        if is_wake_recovery_lifecycle(&sandbox.lifecycle)
            && !is_dormant_lifecycle(&sandbox.lifecycle)
        {
            let binding = self
                .ensure_execution_ready_for_access(
                    &sandbox,
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                        "runtime_client",
                    ),
                    Duration::from_secs(120),
                    charge,
                    None,
                )
                .await?;
            return connect_worker(binding.runtime_endpoint).await;
        }

        Err(ControllerError::service_unavailable(
            "sandbox is not assigned to a node-api",
        ))
    }

    async fn reachable_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        let Some(pod_ip) = self
            .resolve_pod_ip(sandbox_id)
            .await
            .map_err(ControllerError::from)?
        else {
            return Ok(None);
        };

        match self.worker_client_for_pod_ip(pod_ip).await {
            Ok(client) => Ok(Some(client)),
            Err(_) => Ok(None),
        }
    }

    async fn probe_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        let Some(pod_ip) = self
            .resolve_reachable_pod_ip(sandbox_id)
            .await
            .map_err(ControllerError::from)?
        else {
            return Ok(None);
        };

        match self.worker_client_for_pod_ip(pod_ip).await {
            Ok(client) => Ok(Some(client)),
            Err(_) => Ok(None),
        }
    }

    async fn access_runtime_client(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
        timeout: Duration,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        if is_suspended_lifecycle(&sandbox.lifecycle) {
            Self::should_allow_access_recovery(sandbox, activity)?;
            let binding = self
                .wake_execution_with_metric_identity(
                    sandbox,
                    timeout,
                    metric_identity,
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
                .map_err(ControllerError::from)?;
            return connect_worker(binding.binding.runtime_endpoint).await;
        }

        if let Some(pod_ip) = self
            .resolve_pod_ip(sandbox.id)
            .await
            .map_err(ControllerError::from)?
        {
            // A preserved slow create was reported `started` before its guest
            // command API was confirmed ready. Force a (bounded) command-ready
            // confirmation on first access so a data-plane op can't reach a
            // not-yet-ready guest, then clear the marker so subsequent accesses
            // take the plain fast path. On timeout the `SubstrateError::Timeout`
            // surfaces as the SDK-recoverable `sandbox_timeout`. Normal
            // (non-pending) sandboxes skip this entirely — no added latency.
            if sandbox.command_ready_pending {
                let sandbox_id = sandbox.id;
                self.poll_until_command_ready(
                    sandbox_id,
                    metric_identity,
                    timeout,
                    CommandReadyPod::PreResolved(pod_ip),
                    None,
                )
                .await
                .map_err(ControllerError::from)?;
                // Clearing the marker is the access side of a CAS against the
                // command-ready reaper: if it didn't claim the row, the reaper
                // may have flipped the sandbox to `failed` and started tearing
                // the pod down between our readiness probe and here. Re-read the
                // lifecycle; if it's no longer `started`, don't hand back a
                // client to a doomed pod — surface a retryable error instead. A
                // `false` with the row still `started` is a benign concurrent
                // clear, so we proceed.
                let claimed = self
                    .store
                    .clear_command_ready_pending(sandbox.id)
                    .await
                    .map_err(ControllerError::from)?;
                if !claimed {
                    let current = self
                        .store
                        .require_sandbox(sandbox_id)
                        .await
                        .map_err(ControllerError::from)?;
                    if normalize_lifecycle(&current.lifecycle) != "started" {
                        return Err(ControllerError::from(SubstrateError::Conflict {
                            message: format!(
                                "sandbox was concurrently reaped while never command-ready \
                                 (now {}); create a new sandbox",
                                current.lifecycle
                            ),
                        }));
                    }
                }
            }
            if let Ok(client) = self.worker_client_for_pod_ip(pod_ip).await {
                return Ok(client);
            }
        }

        let sandbox = self
            .store
            .require_sandbox(sandbox.id)
            .await
            .map_err(ControllerError::from)?;
        Self::should_allow_access_recovery(&sandbox, activity)?;
        let binding = self
            .ensure_execution_ready_for_access(&sandbox, metric_identity, timeout, charge, None)
            .await?;
        connect_worker(binding.runtime_endpoint).await
    }

    async fn sandbox_ip(&self, sandbox_id: SandboxId) -> Result<Option<String>, ControllerError> {
        self.resolve_pod_ip(sandbox_id)
            .await
            .map_err(ControllerError::from)
            .map(|pod_ip| pod_ip.map(|ip| ip.to_string()))
    }

    async fn access_sandbox_ip(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<Option<String>, ControllerError> {
        let sandbox_id = sandbox.id;
        let restore_plan = if is_suspended_lifecycle(&sandbox.lifecycle) {
            Some(
                self.suspend_restore_plan(sandbox, false)
                    .await
                    .map_err(ControllerError::from)?,
            )
        } else {
            None
        };
        if let Some(pod_ip) = self
            .resolve_pod_ip(sandbox.id)
            .await
            .map_err(ControllerError::from)?
        {
            if !is_dormant_lifecycle(&sandbox.lifecycle) {
                return Ok(Some(pod_ip.to_string()));
            }
            Self::should_allow_access_recovery(sandbox, ExecutionActivityKind::Access)?;
            if let Some(execution) = self
                .store
                .active_execution_status(sandbox.id)
                .await
                .map_err(ControllerError::from)?
            {
                let requested_attempt_id =
                    pplx_space_common_core::types::SandboxActivationAttemptId::new();
                let (activation_attempt_id, joined_attempt_id) = match self
                    .store
                    .assign_activation_attempt(
                        sandbox_id,
                        execution.generation,
                        requested_attempt_id,
                        SandboxTransitionTargetState::Started,
                    )
                    .await
                {
                    Ok(assigned_attempt_id) => (assigned_attempt_id, None),
                    Err(SubstrateError::ActivationInFlight {
                        same_target: true,
                        owner_attempt_id,
                        ..
                    }) => (requested_attempt_id, Some(owner_attempt_id)),
                    Err(error) => return Err(ControllerError::from(error)),
                };
                let fence = self
                    .store
                    .reconcile_activation_fence(
                        sandbox_id,
                        activation_attempt_id,
                        joined_attempt_id,
                        SandboxTransitionTargetState::Started,
                    )
                    .await
                    .map_err(ControllerError::from)?;
                let fence_ownership = fence.ownership();
                let owned_attempt_id = fence.owned_attempt_id();
                let execution_generation = fence.execution.generation;
                let wake_action = activation_wake_action(
                    &fence.execution,
                    fence_ownership,
                    SandboxLifecycle::Started,
                )
                .map_err(ControllerError::from)?;
                let (_, phase_deadline) = ready_wait_bounds(timeout);
                let wake_result = self
                    .execute_activation_wake_action(
                        sandbox_id,
                        wake_action,
                        phase_deadline,
                        |route_identity, activation_policy| {
                            pplx_space_sbx_mgr_client::proto::WakeRequest {
                                implicit_wake_source: 0,
                                sandbox_id: sandbox.id.to_string(),
                                blocking: true,
                                start_context: Some(
                                    pplx_space_sbx_mgr_client::proto::WakeStartContext {
                                        activation_auto_lifecycle: Some(activation_policy),
                                        expected_execution_generation: route_identity
                                            .execution_generation,
                                        operation_activation_attempt_id: fence
                                            .execution
                                            .activation_attempt_id
                                            .map(|id| id.to_string()),
                                        vcpu: sandbox_vcpu_option(sandbox),
                                        memory_mib: sandbox_memory_mib_option(sandbox),
                                        netns_path: None,
                                        suspend_restore_storage_uri: restore_plan
                                            .as_ref()
                                            .and_then(|plan| plan.storage_uri.clone()),
                                        suspend_restore_source_node: restore_plan
                                            .as_ref()
                                            .and_then(|plan| plan.source_node_name.clone()),
                                        route_identity: Some(wake_request_route_identity(
                                            route_identity,
                                        )),
                                    },
                                ),
                                target_state: 0,
                            }
                        },
                        || {},
                    )
                    .await;
                let (wake_outcome, route_identity) = match wake_result {
                    Ok(outcome) => outcome,
                    Err(failure) => {
                        if failure.operation_release_is_safe()
                            && let Some(activation_attempt_id) = owned_attempt_id
                        {
                            self.store
                                .release_activation_attempt_best_effort(
                                    sandbox_id,
                                    execution_generation,
                                    activation_attempt_id,
                                )
                                .await;
                        }
                        return Err(ControllerError::from(failure.into_error()));
                    }
                };
                if let Err(error) = self
                    .poll_until_command_ready(
                        sandbox_id,
                        metric_identity,
                        remaining_ready_window(phase_deadline),
                        match route_identity.as_ref() {
                            Some(route_identity) => CommandReadyPod::SelectedRoute {
                                route_identity,
                                pod_ip: wake_outcome.pod_ip,
                            },
                            None => CommandReadyPod::ResolveLegacy,
                        },
                        None,
                    )
                    .await
                {
                    if wake_outcome.dispatch.operation_release_is_safe()
                        && let Some(activation_attempt_id) = owned_attempt_id
                    {
                        self.store
                            .release_activation_attempt_best_effort(
                                sandbox_id,
                                execution_generation,
                                activation_attempt_id,
                            )
                            .await;
                    }
                    return Err(ControllerError::from(error));
                }
                if let Some(activation_attempt_id) = owned_attempt_id {
                    self.store
                        .release_activation_attempt_best_effort(
                            sandbox_id,
                            execution_generation,
                            activation_attempt_id,
                        )
                        .await;
                }
                return match route_identity.as_ref() {
                    Some(route_identity) => self
                        .selected_wake_pod_ip(sandbox_id, route_identity)
                        .await
                        .map_err(ControllerError::from)
                        .map(|pod_ip| pod_ip.map(|ip| ip.to_string())),
                    None => self
                        .resolve_pod_ip(sandbox.id)
                        .await
                        .map_err(ControllerError::from)
                        .map(|pod_ip| pod_ip.map(|ip| ip.to_string())),
                };
            }
            tracing::info!(
                sandbox_id = %sandbox_id,
                "ignored stale dormant pod projection without an active execution"
            );
        }

        Self::should_allow_access_recovery(sandbox, ExecutionActivityKind::Access)?;
        // No source-node pin on the access path (see
        // `ensure_execution_ready_for_access`): the restore runs cross-node via
        // the storage URI so the started-recovery re-drive stays fenceable.
        self.ensure_execution_ready_for_access(
            sandbox,
            metric_identity,
            timeout,
            charge,
            restore_plan
                .as_ref()
                .and_then(|plan| plan.storage_uri.as_deref()),
        )
        .await?;
        self.resolve_pod_ip(sandbox.id)
            .await
            .map_err(ControllerError::from)
            .map(|pod_ip| pod_ip.map(|ip| ip.to_string()))
    }

    async fn builder_client(&self) -> Result<BuilderClient, ControllerError> {
        self.builder_node_client().await
    }

    async fn builder_client_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<BuilderClient, ControllerError> {
        let node_name = sqlx::query_scalar::<_, String>(sql!(
            "SELECT nk.k8s_node_name
             FROM nodes n
             JOIN node_k8s nk ON nk.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = 'k8s'
               AND n.role = 'builder'
             LIMIT 1"
        ))
        .bind(node_id.as_uuid())
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| {
            db_contention_error(&format!("failed to load builder node {node_id}"), &error)
        })?
        .ok_or_else(|| {
            ControllerError::service_unavailable(format!(
                "builder node {node_id} is not registered"
            ))
        })?;
        let target = NodeApiTarget {
            managed_by: SpaceNodeManagedBy::Kubernetes,
            role: SpaceNodeRole::Builder,
            direct_endpoint: None,
            substrate_node_id: Some(&node_name),
        };
        let address = self
            .resolve_node_api_endpoint(&target)
            .await
            .map_err(ControllerError::from)?;
        self.builder_node_client_at(&address).await
    }

    fn pool_eligible_for_request(
        &self,
        boot_mode: CreateBootMode,
        backend: &str,
        wait_until_ready: bool,
        disk_mib: Option<u32>,
        file_changes_enabled: bool,
        implicit_wake_enabled: bool,
    ) -> bool {
        pool::pool_eligible_for_request(
            boot_mode,
            backend,
            wait_until_ready,
            disk_mib,
            file_changes_enabled,
            implicit_wake_enabled,
        )
    }

    fn pool_supports_request_env(&self) -> bool {
        true
    }

    async fn claim_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        // A pooled pod is built for no org and therefore carries no node-group
        // boundary — and adoption applies the per-sandbox setup to that
        // already-running pod over gRPC, so no second pod build ever renders
        // one. Nothing downstream re-checks placement either: a successful
        // adoption returns before `create_execution`, where the create path's
        // own resolver lives. So the claim itself has to decide, and the
        // decision is about the **node the slot is already on**, because that
        // node is where the sandbox will run.
        //
        // Resolved lazily, inside the loop and memoized: a template with no
        // warm pool at all never reaches a claim, and paying a catalog read on
        // every pool-eligible create to answer a question that cannot arise
        // would tax the common path for the rare one.
        //
        // Every refusal below picks one of two ways to give the slot up, and
        // the choice is a policy knob with a real cost on each side:
        //
        // * `release` returns the slot to the pool with `ready_at` untouched.
        //   The pool keeps its capacity — but the claim orders by `ready_at
        //   ASC`, so a slot released for a reason that will still hold next
        //   time is handed straight back to the next create, forever. That is
        //   a tarpit: the pool's hit rate sits at zero with every pod healthy.
        // * `invalidate` takes the slot out of the pool for the reconciler to
        //   replace. It ends the tarpit — but it destroys a warm pod, and if
        //   the reason was momentary the pool pays a create to rebuild what it
        //   already had.
        //
        // So the rule is: release when the refusal is about *this create* and
        // the slot is still good for the next one; invalidate when the refusal
        // is evidence about *the slot*. Each arm says which side it is on.
        //
        // One arm is neither, and is the deliberate exception: `Unknown` is
        // about our *ability to read* the node, which says nothing about either
        // the create or the slot. It releases, because the common cause is a
        // node cache still filling on a freshly rolled replica — invalidating
        // there would destroy warm pods on every deploy, per replica. The cost
        // is that a permanently unreadable node (a reflector task that panicked
        // and left a readable stale store) re-offers the same slot to every
        // create. That is a pre-existing reflector-lifecycle gap rather than a
        // reason to invert this knob, and `nodes_ready()` cannot bound it
        // because `ReflectorReady::transition_to` is a one-way latch that keeps
        // reporting ready after the writer drops.
        let mut placement: Option<ResolvedPlacement> = None;
        let mut attempted_pool_shape = None;
        for attempt in 1..=MAX_STALE_POOL_CLAIMS_PER_REQUEST {
            let claim_attempt = pool::claim_pooled_execution(
                self.store.pool(),
                request,
                &self.k8s.sandbox_namespace,
            )
            .await?;
            attempted_pool_shape = claim_attempt.pool_shape;
            let Some(claim) = claim_attempt.claim else {
                return Ok(claim_attempt);
            };

            let pod = match self.read_sandbox_pod(&claim.pod_name).await {
                Ok(pod) => pod,
                Err(error) => {
                    pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                    tracing::warn!(
                        claim_id = %claim.claim_id,
                        sandbox_id = %claim.sandbox_id,
                        pod_name = %claim.pod_name,
                        attempt,
                        %error,
                        "failed to validate claimed Kubernetes warm-pool pod; released claim and falling back"
                    );
                    return Ok(PooledExecutionClaimAttempt::missed(
                        claim_attempt.pool_shape,
                    ));
                }
            };
            // Usability first, and this order matters. A pod that is gone,
            // replaced, or finished makes every node question unanswerable —
            // and it is the *stale claim* path, so the slot is invalidated and
            // can never be handed to another caller. Asking the candidate set
            // first would let an unreadable node turn a dead pod's slot into a
            // `release`, which puts it back at the front of the queue for every
            // create that follows.
            if !pooled_claim_pod_is_usable(&claim, pod.as_ref()) {
                tracing::warn!(
                    claim_id = %claim.claim_id,
                    sandbox_id = %claim.sandbox_id,
                    pod_name = %claim.pod_name,
                    attempt,
                    "invalidating stale Kubernetes warm-pool claim"
                );
                pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
                continue;
            }

            let resolved = match placement.as_ref() {
                Some(resolved) => resolved,
                None => {
                    let resolved = self
                        .node_group_placement
                        .resolve(
                            NodeGroupResolvePath::PoolClaim,
                            PlacementSubject::Sandbox(request.actor.org_id),
                            request.template,
                        )
                        .await;
                    match resolved {
                        Ok(resolved) => placement.insert(resolved),
                        // Nothing can be shown to satisfy a boundary that
                        // cannot be computed, so this claim can't be admitted.
                        // It reports a *miss*, not an error: the cold create
                        // resolves the same candidate set on `path:create` and
                        // fails there, which is where the user-facing mapping
                        // and the operator's metric both live. Returning `Err`
                        // here instead would abort the create inside the claim
                        // step, book it as `warm_pool_claim/claim_failed`, and
                        // read to an operator as warm-pool trouble rather than
                        // a placement refusal — and it would turn a momentary
                        // catalog read failure into a failed create that a
                        // cold build would have served.
                        Err(error) => {
                            // Which disposition the rule above picks turns on
                            // the same question the dedicated-only arm asks —
                            // can a pooled pod host this template at all? —
                            // and here the caller's own error already answers
                            // it.
                            //
                            // `NoCompatibleNodeGroup` from a caller that *has*
                            // an org can only mean an org-specific template: a
                            // generic one with no compatible group reports
                            // `Unprovisioned` instead, because the `Generic`
                            // arm of the compatibility relation does not read
                            // the caller. An org-specific template is
                            // unhostable by every org-less pod, so the config
                            // is dead to every caller, its refill is suppressed
                            // for the same reason, and its slots sit on shared
                            // workers holding a private rootfs. Invalidate —
                            // that is evidence about the slot.
                            //
                            // A reaped `templates` row is not that. It is a
                            // control-plane inconsistency an operator repairs
                            // by re-creating the row, after which these slots
                            // are usable again, so release and keep the capacity.
                            // Same for a catalog blip, which is evidence about
                            // nothing.
                            let dead_config = matches!(
                                error,
                                NodeGroupPlacementResolveError::NoCompatibleNodeGroup { .. }
                            );
                            // Each standing condition is reported as a refusal
                            // of the class the *cold path* would have produced
                            // for it, so a strict `boot_mode=pool` create — the
                            // one mode with no cold path to fall through to —
                            // gets the same answer either way instead of a
                            // retry instruction it can never satisfy.
                            //
                            // `NoCompatibleNodeGroup` maps to `Conflict` there,
                            // so it is a 409 here. `TemplateAbsent` and
                            // `Invariant` map to `Failed`, so they are 500s:
                            // a reaped row is a control-plane fault an operator
                            // repairs, and answering it 409 would take the only
                            // signal of it out of 5xx alerting — while
                            // answering 503 would tell the client to retry what
                            // only an operator can fix. Only `Catalog` is
                            // genuinely momentary, and only there is the
                            // ordinary retryable miss the right answer, because
                            // a retry really can clear it.
                            let refusal = match &error {
                                NodeGroupPlacementResolveError::NoCompatibleNodeGroup {
                                    ..
                                } => Some(PooledClaimRefusal::PlacementIncompatible {
                                    message: error.to_string(),
                                }),
                                NodeGroupPlacementResolveError::TemplateAbsent { .. }
                                | NodeGroupPlacementResolveError::Invariant(_) => {
                                    Some(PooledClaimRefusal::PlacementUnresolvable {
                                        message: error.to_string(),
                                    })
                                }
                                NodeGroupPlacementResolveError::Catalog(_) => None,
                            };
                            if dead_config {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::OrgSpecificTemplate,
                                );
                                tracing::info!(
                                    claim_id = %claim.claim_id,
                                    template = request.template,
                                    %error,
                                    "invalidating a warm-pool slot whose template no pooled \
                                     pod may host; this config drains and does not refill"
                                );
                                pool::invalidate_claimed_pool_slot(self.store.pool(), &claim)
                                    .await?;
                            } else {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unresolved,
                                );
                                tracing::info!(
                                    claim_id = %claim.claim_id,
                                    template = request.template,
                                    %error,
                                    "declining the warm pool: this sandbox's node-group \
                                     candidate set could not be resolved; released the claim \
                                     and falling through to a cold create unless the request \
                                     pinned boot_mode=pool, which has no cold path"
                                );
                                pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                            }
                            return Ok(match refusal {
                                Some(refusal) => PooledExecutionClaimAttempt::refused(
                                    claim_attempt.pool_shape,
                                    refusal,
                                ),
                                None => {
                                    PooledExecutionClaimAttempt::missed(claim_attempt.pool_shape)
                                }
                            });
                        }
                    }
                }
            };

            match resolved.placement() {
                // Gate off, or no generic group on this cluster: the pod
                // carries no boundary and neither does the sandbox.
                //
                // The two are booked differently. With the gate off there is no
                // placement decision to record and the series stays silent.
                // Unprovisioned *is* a decision — the gate is on and the answer
                // was "nothing to enforce" — and recording it is what keeps
                // this counter a total on a cluster that reached stage 3 before
                // stage 0, where `admitted` is necessarily zero while adoption
                // works perfectly well.
                None => {
                    if matches!(resolved, ResolvedPlacement::Unprovisioned) {
                        // `Unprovisioned` means no *generic* group admits this
                        // template — not that the cluster has no groups. A
                        // dedicated group can exist without one (nothing orders
                        // `create_node_group`), and in that state this sandbox
                        // renders no boundary while some nodes belong to
                        // another org's hardware.
                        //
                        // Rendering nothing there is pre-existing behavior;
                        // a generic group must be provisioned before dedicated
                        // groups. Adopting a pod already sitting on such a node
                        // would hand this sandbox capacity
                        // reserved for another org with no check at all, since
                        // there is no candidate set here to check against.
                        //
                        // So a labeled node is disqualifying on this arm. With
                        // no generic group provisioned, a node carrying a
                        // group label belongs to a dedicated group by
                        // elimination — the only writer is the labeler and the
                        // only groups are dedicated ones — so the slot is
                        // released and the sandbox builds cold. On a genuinely
                        // unprovisioned cluster no node is labeled and nothing
                        // changes.
                        match pooled_pod_node_group(&self.k8s.caches, pod.as_ref()) {
                            PooledPodNodeGroup::Labeled(group) => {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::UnprovisionedLabeledNode,
                                );
                                tracing::warn!(
                                    claim_id = %claim.claim_id,
                                    sandbox_id = %claim.sandbox_id,
                                    pod_name = %claim.pod_name,
                                    node_group = %group,
                                    "declining a warm-pool slot whose node belongs to a node \
                                     group while no generic group admits this template; a \
                                     dedicated group exists without a default generic one"
                                );
                                pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                                return Ok(PooledExecutionClaimAttempt::missed(
                                    claim_attempt.pool_shape,
                                ));
                            }
                            // Cannot be shown safe, so it is not adopted. This
                            // arm has no candidate set to test membership
                            // against, so an unreadable node is the one case
                            // where "adopt anyway" would rest on nothing at
                            // all — a cold reflector would hand this sandbox a
                            // pod that may be sitting on another org's
                            // dedicated hardware.
                            PooledPodNodeGroup::Unreadable => {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unknown,
                                );
                                tracing::info!(
                                    claim_id = %claim.claim_id,
                                    pod_name = %claim.pod_name,
                                    "could not read the claimed warm pod's node while no generic \
                                     group admits this template; released the claim and \
                                     building cold rather than adopting unverified"
                                );
                                pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                                return Ok(PooledExecutionClaimAttempt::missed(
                                    claim_attempt.pool_shape,
                                ));
                            }
                            // Read, and in no group: the genuinely
                            // unprovisioned cluster, where there is nothing to
                            // enforce and nothing to be outside of.
                            PooledPodNodeGroup::Unlabeled => {}
                        }
                        pplx_space_metrics::node_group_placement::pool_claim_admission(
                            pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unconstrained,
                        );
                    }
                    return Ok(PooledExecutionClaimAttempt {
                        claim: Some(claim),
                        pool_shape: claim_attempt.pool_shape,
                        refusal: None,
                    });
                }
                Some(placement) => {
                    // Dedicated-only set: every candidate is this caller's own
                    // hardware, and a pooled pod lands on shared capacity by
                    // construction, so no slot can satisfy *this* sandbox.
                    // Answered from the set alone, without reading a node.
                    //
                    // How to give the slot up then depends on a question the
                    // caller's set cannot answer: can the pool host this
                    // template for *anybody*? Two different states reach here.
                    //
                    // * An **org-specific** template. No org-less pod may ever
                    //   host it, so every slot under the config is dead to
                    //   every caller, and `reconcile_config`'s own org-less
                    //   resolve fails and suppresses refill. Invalidate: the
                    //   pool drains to zero, and the private rootfs those pods
                    //   pulled stops sitting on shared workers.
                    // * A **generic** template on a cluster that has a
                    //   dedicated group and no generic group. This caller owns
                    //   dedicated capacity so its set is all its own, but an
                    //   org-less resolve of the same template reports
                    //   `Unprovisioned` — `Ok`, not `Err` — so refill keeps
                    //   running and a caller without dedicated capacity adopts
                    //   these slots unconstrained. Release: invalidating would
                    //   destroy a usable pod once per create, forever, and the
                    //   reconciler would rebuild it just as fast.
                    //
                    // So ask the pool's own question directly rather than infer
                    // it. One extra catalog read, on an arm that returns.
                    if !placement.admits_shared_capacity() {
                        let poolable = self
                            .node_group_placement
                            .resolve(
                                NodeGroupResolvePath::PoolClaim,
                                PlacementSubject::WarmPool,
                                request.template,
                            )
                            .await;
                        // Three outcomes, not a bool. The refusal itself is
                        // the same in all of them — this sandbox's set is its
                        // own dedicated hardware and no pooled pod runs there,
                        // which no retry changes — but what it says about the
                        // *config*, and therefore what the slot and the caller
                        // are told, differs:
                        //
                        // Note the deliberate asymmetry with the arm above,
                        // which withholds a refusal for a `Catalog` blip alone
                        // — a reaped row and a predicate disagreement carry
                        // `PlacementUnresolvable` there. It is not an
                        // inconsistency to harmonize: up there the sandbox's own
                        // set is unknown, so a momentary read failure
                        // establishes nothing about whether a pooled pod could
                        // ever serve it; down here the set resolved and is
                        // dedicated-only, which settles that on its own
                        // whatever this second read says — so every outcome
                        // below refuses, and only the slot's fate varies.
                        //
                        // * `Ok(_)` — the pool can host this template for
                        //   somebody (an `Unprovisioned` cluster, or a generic
                        //   set this caller is simply not in). Release, so
                        //   those callers keep the slot.
                        // * `Err(NoCompatibleNodeGroup)` — org-specific
                        //   template, the one error that proves it: a generic
                        //   template with no groups at all reports
                        //   `Unprovisioned`. Nobody can adopt any slot under
                        //   this config, so invalidate.
                        // * `Err(_)` — a reaped row or a catalog blip on this
                        //   second read. It proves nothing about the config, so
                        //   release; and it must not borrow either of the other
                        //   two messages, both of which state a cluster fact as
                        //   certain. The first read succeeded, so this is the
                        //   only place that failure is newly reachable.
                        // The message is built as a base plus an optional
                        // cause, rather than three independent strings. The
                        // base is derived from the sandbox's own resolved set,
                        // so it is true in every arm; a cause is appended only
                        // where this second read actually established one. A
                        // fourth variant added later therefore falls back to
                        // saying less, not to asserting something false — which
                        // is the failure the previous shape had.
                        let base = format!(
                            "no warm-pool pod runs on capacity that sandbox template '{}' may \
                             use",
                            request.template
                        );
                        let (verdict, cause, invalidate) = match &poolable {
                            // `Unprovisioned` is the only outcome that
                            // establishes the cluster has no shared group for
                            // this template. `Placed` reaching here means a
                            // generic group appeared between the claim's two
                            // reads — the race the `caller_dedicated_only`
                            // catalog entry names — and asserting the cluster
                            // fact there would state something the second read
                            // just disproved. Same verdict, since the slot's
                            // fate is the same; no cause, because none was
                            // established for *this* create.
                            Ok(ResolvedPlacement::Unprovisioned) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::CallerDedicatedOnly,
                                Some(
                                    "this cluster has no shared node group it may use, so only \
                                     capacity dedicated to this organization is eligible",
                                ),
                                false,
                            ),
                            Ok(ResolvedPlacement::Disabled | ResolvedPlacement::Placed(_)) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::CallerDedicatedOnly,
                                None,
                                false,
                            ),
                            Err(NodeGroupPlacementResolveError::NoCompatibleNodeGroup {
                                ..
                            }) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::OrgSpecificTemplate,
                                Some(
                                    "the template may run only on node groups dedicated to its \
                                     organization",
                                ),
                                true,
                            ),
                            Err(_) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unresolved,
                                None,
                                false,
                            ),
                        };
                        let message = match cause {
                            Some(cause) => format!("{base}: {cause}"),
                            None => base,
                        };
                        pplx_space_metrics::node_group_placement::pool_claim_admission(verdict);
                        if invalidate {
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                template = request.template,
                                "invalidating a warm-pool slot whose template no pooled pod \
                                 may host; this config drains and does not refill"
                            );
                            pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
                        } else {
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                template = request.template,
                                %verdict,
                                "declining the warm pool: this sandbox admits only dedicated \
                                 capacity, and nothing proves the slot is unusable by another \
                                 caller, so it goes back"
                            );
                            pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                        }
                        return Ok(PooledExecutionClaimAttempt::refused(
                            claim_attempt.pool_shape,
                            PooledClaimRefusal::PlacementIncompatible { message },
                        ));
                    }
                    match pooled_pod_node_admission(&self.k8s.caches, placement, pod.as_ref()) {
                        PooledPodAdmission::Admitted => {
                            pplx_space_metrics::node_group_placement::pool_claim_admission(pplx_space_metrics::node_group_placement::PoolClaimAdmission::Admitted);
                            return Ok(PooledExecutionClaimAttempt {
                                claim: Some(claim),
                                pool_shape: claim_attempt.pool_shape,
                                refusal: None,
                            });
                        }
                        // The node's own label puts it outside the set — a
                        // group reassigned under resident slots. Take the slot
                        // *out* of the pool rather than releasing it: `release`
                        // leaves `ready_at` untouched and the claim orders by
                        // `ready_at ASC`, so releasing would hand the same slot
                        // to the next create forever and the pool's hit rate
                        // would sit at zero with every pod healthy. Invalidating
                        // lets the reconciler replace it somewhere admissible,
                        // and also stops a warm pod squatting on hardware that
                        // now belongs to one org.
                        PooledPodAdmission::OutsideSet => {
                            pplx_space_metrics::node_group_placement::pool_claim_admission(pplx_space_metrics::node_group_placement::PoolClaimAdmission::OutsideSet);
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                sandbox_id = %claim.sandbox_id,
                                pod_name = %claim.pod_name,
                                attempt,
                                "invalidating a warm-pool slot whose node is outside this \
                                 sandbox's node-group candidate set"
                            );
                            pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
                            continue;
                        }
                        // The node is unlabeled: refuse this adoption, but
                        // leave the pod alone. The label is the labeler's to
                        // write and it repairs on its next tick, so the slot is
                        // usable again shortly — whereas invalidating would
                        // turn a label write into fleet-wide pool loss. The
                        // brief tarpit that a release causes here (`ready_at`
                        // is untouched, so this slot is re-picked first) is
                        // bounded by that tick, and every isolation guarantee
                        // still holds because the sandbox builds cold.
                        PooledPodAdmission::LabelAbsent => {
                            pplx_space_metrics::node_group_placement::pool_claim_admission(
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::LabelAbsent,
                            );
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                sandbox_id = %claim.sandbox_id,
                                pod_name = %claim.pod_name,
                                attempt,
                                "declining a warm-pool slot whose node carries no node-group \
                                 label; released the claim for the labeler to repair and \
                                 falling through to a cold create"
                            );
                            pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                            return Ok(PooledExecutionClaimAttempt::missed(
                                claim_attempt.pool_shape,
                            ));
                        }
                        // Not evidence against the slot, so leave it in the
                        // pool and build cold.
                        PooledPodAdmission::Unknown => {
                            // Booked before the release, so a release failure
                            // propagates with the verdict already recorded.
                            // Every arm here does the same, which makes this
                            // counter a total of the *placement* decisions —
                            // not of claims: an unusable pod, an unreadable
                            // pod, and a gate-off adoption all pass through
                            // without a verdict, because none of them is a
                            // placement decision.
                            pplx_space_metrics::node_group_placement::pool_claim_admission(pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unknown);
                            pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                pod_name = %claim.pod_name,
                                "could not read the claimed warm pod's node group; released \
                                 the claim and falling through to a cold create unless the \
                                 request pinned boot_mode=pool, which has no cold path"
                            );
                            return Ok(PooledExecutionClaimAttempt::missed(
                                claim_attempt.pool_shape,
                            ));
                        }
                    }
                }
            }
        }

        tracing::warn!(
            limit = MAX_STALE_POOL_CLAIMS_PER_REQUEST,
            "stopped claiming Kubernetes warm-pool slots after discarding unusable candidates"
        );
        Ok(PooledExecutionClaimAttempt::missed(attempted_pool_shape))
    }

    async fn lookup_admitted_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        admitted: AdmittedPoolClaim,
        sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        let lookup = pool::lookup_admitted_pooled_execution(
            self.store.pool(),
            request,
            admitted,
            sandbox_id,
            &self.k8s.sandbox_namespace,
        )
        .await?;
        let AdmittedPoolClaimLookup::Adoptable { claim, pool_shape } = lookup else {
            return Ok(lookup);
        };

        // The same pod and placement checks `claim_pooled_execution` makes,
        // with one difference in how a slot is given up. The ordinary claim
        // may `release` a slot that is fine for another caller; this slot's
        // reserved ID is already this create's public ID, so returning it to
        // the inventory would let another create bind the same ID. Every
        // refusal here invalidates instead: the slot leaves the inventory, the
        // stale-claimed sweep collects its pod, and this create builds cold.
        let usable = match self.read_sandbox_pod(&claim.pod_name).await {
            Ok(pod) => {
                pooled_claim_pod_is_usable(&claim, pod.as_ref())
                    && self
                        .admitted_pool_pod_placement_admits(request, pod.as_ref())
                        .await
            }
            Err(error) => {
                tracing::warn!(
                    claim_id = %claim.claim_id,
                    sandbox_id = %claim.sandbox_id,
                    pod_name = %claim.pod_name,
                    %error,
                    "failed to read the admitted warm-pool pod; not adopting it"
                );
                false
            }
        };
        if !usable {
            tracing::info!(
                claim_id = %claim.claim_id,
                sandbox_id = %claim.sandbox_id,
                pod_name = %claim.pod_name,
                "invalidating an admitted warm-pool slot this create will not adopt"
            );
            pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
            return Ok(AdmittedPoolClaimLookup::ReleasePending);
        }
        Ok(AdmittedPoolClaimLookup::Adoptable { claim, pool_shape })
    }

    async fn skip_admitted_pool_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), ControllerError> {
        self.store
            .retire_pool_reserved_generation(sandbox_id)
            .await
            .map_err(ControllerError::from)
    }

    async fn admitted_pool_cleanup_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, ControllerError> {
        pool::admitted_pool_cleanup_pending(self.store.pool(), sandbox_id).await
    }

    async fn claim_admission_pool_slot(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
    ) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
        pool::claim_admission_pool_slot(tx, template_release_id, &self.k8s.sandbox_namespace).await
    }

    async fn persist_pooled_execution_claim(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        pool::persist_pooled_execution_claim(tx, request, claim).await
    }

    async fn rollback_pooled_execution_claim(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        pool::invalidate_claimed_pool_slot(self.store.pool(), claim)
            .await
            .map_err(|error| {
                ControllerError::internal(format!(
                    "pool claim leaked for pod {}: failed to invalidate claimed pool slot: {error}",
                    claim.pod_name
                ))
            })
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        pool::mark_claimed_pool_slot_cleanup_pending(self.store.pool(), claim).await?;
        Ok(PooledPrepareFailureCleanup::DeferredToReconciler)
    }

    async fn apply_pooled_execution_auto_lifecycle(
        &self,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        let pod_ip = PodIp::parse(&claim.pod_ip).map_err(ControllerError::from)?;
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        let mut node_api = NodeApiClient::with_service_name("worker-node-api", endpoint);

        let mut client = RetryingWorkerApiClient::new(&mut node_api);
        Self::apply_pooled_execution_auto_lifecycle_with_client(&mut client, claim, policy).await
    }

    async fn prepare_pooled_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
        wait_until_ready: bool,
        timeout: Duration,
    ) -> Result<(), ControllerError> {
        let sandbox_id = claim.sandbox_id;
        let pod_ip = PodIp::parse(&claim.pod_ip).map_err(ControllerError::from)?;
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        let mut node_api = NodeApiClient::with_service_name("worker-node-api", endpoint);
        let require_pooled_adoption_node_id = if wait_until_ready {
            wait_for_pooled_split_receipt_capability(
                &mut node_api,
                Instant::now() + WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .await?;
            true
        } else {
            // Manager-local Kubernetes node hydration and exact operation
            // receipts shipped behind the same prerequisite. Keep the landed
            // mixed-version contract for non-starting adoption: an older or
            // rolled-back manager may omit only its locally-owned node ID,
            // while any node ID it does return must still match the claim.
            node_api
                .health_service_serving(
                    pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                    WARM_POOL_SETUP_RPC_TIMEOUT,
                )
                .await
                .map_err(|error| {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox route-identity capability check failed: {error}"
                    ))
                })?
        };

        // Resolve the per-sandbox setup through the same shared routine the cold
        // create path uses, then apply every step to the already-booted pool pod
        // via gRPC. `apply_adopted_per_sandbox_setup` consumes the resolved setup
        // by exhaustive destructure, so a new cold-path setup step can't silently
        // skip pooled sandboxes. See `per_sandbox_setup`.
        let mut setup = PerSandboxSetup::from_claim_request(request, claim);
        // Pool ownership precedes this policy capture; the create request may
        // still contain the policy from before the claim.
        setup.auto_lifecycle = policy.clone();
        // Cold bakes the inherit-template network-policy generation from the
        // template counter into the pod annotation; adoption deliberately uses 0
        // so the adopted pod keeps its own independent generation counter (a later
        // user override carries the API's Postgres generation and must not be
        // skipped if the template counter happened to be higher). The override
        // case uses the request generation in both paths.
        let network_policy_generation = if request.network_policy_override.is_some() {
            request.network_policy_generation
        } else {
            0
        };
        self.apply_adopted_per_sandbox_setup(
            &mut node_api,
            claim,
            setup,
            network_policy_generation,
        )
        .await?;

        if !wait_until_ready {
            let mut internal_client = RetryingWorkerApiInternalClient::new(&mut node_api);
            let mut wake_request = pooled_route_identity_adoption_request(claim)?;
            let expected_route_identity = wake_request
                .start_context
                .as_ref()
                .and_then(|context| context.route_identity.as_ref())
                .expect("pooled route adoption request always has a route identity")
                .clone();
            wake_request
                .start_context
                .as_mut()
                .expect("pooled route adoption request always has start context")
                .route_identity = Some(wake_request_route_identity(&expected_route_identity));
            let response = internal_client
                .wake_sandbox(wake_request, WARM_POOL_SETUP_RPC_TIMEOUT)
                .instrument(tracing::info_span!(
                    "substrate.k8s.pool.adopt_route_identity.client",
                    sandbox_id = %sandbox_id,
                ))
                .await
                .map_err(|err| {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox route identity adoption failed: {err}"
                    ))
                })?;
            validate_wake_route_identity_with_node_requirement(
                response.get_ref(),
                &claim.sandbox_id.to_string(),
                &expected_route_identity,
                require_pooled_adoption_node_id,
            )
            .map_err(ControllerError::from)?;
        } else {
            let requested_attempt_id =
                pplx_space_common_core::types::SandboxActivationAttemptId::new();
            let execution_generation = i64::try_from(claim.execution_generation)
                .map_err(|_| ControllerError::internal("pool execution generation exceeds i64"))?;
            let (activation_attempt_id, joined_attempt_id) = match self
                .store
                .assign_activation_attempt(
                    sandbox_id,
                    execution_generation,
                    requested_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await
            {
                Ok(assigned_attempt_id) => (assigned_attempt_id, None),
                Err(SubstrateError::ActivationInFlight {
                    same_target: true,
                    owner_attempt_id,
                    ..
                }) => (requested_attempt_id, Some(owner_attempt_id)),
                Err(error) => return Err(ControllerError::from(error)),
            };
            let fence = self
                .store
                .reconcile_activation_fence(
                    sandbox_id,
                    activation_attempt_id,
                    joined_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await
                .map_err(ControllerError::from)?;
            let fence_ownership = fence.ownership();
            let owned_attempt_id = fence.owned_attempt_id();
            let mut wake_may_have_landed = false;
            let wake_and_ready = async {
                // Node-fs artifact-dir permissions are ensured eagerly by
                // sbx-mgr's shared resume path (`resume_after_wake_lock`)
                // before the VM resumes — API cannot reach node-mgr directly.
                // Once the pool slot is bound, PostgreSQL is authoritative:
                // during the consumer-first rollout the old producer may
                // still rotate this value away from the original slot ID.
                let wake_deadline = Instant::now() + timeout;
                let wake_action = activation_wake_action(
                    &fence.execution,
                    fence_ownership,
                    SandboxLifecycle::Started,
                )
                .map_err(ControllerError::from)?;
                if let ActivationWakeAction::Execute {
                    route_identity: expected_route_identity,
                    policy,
                    require_split_receipt_capability,
                    publish_legacy_route_after_ack,
                    ..
                } = wake_action
                {
                    // Per-sandbox setup can outlive a manager restart. Recheck
                    // the dynamically bridged capability immediately before
                    // emitting the split A/B request; the pre-setup check
                    // prevents partial setup against an incompatible node,
                    // while this check closes that restart window.
                    if require_split_receipt_capability {
                        wait_for_pooled_split_receipt_capability(
                            &mut node_api,
                            wake_deadline,
                        )
                        .await?;
                    }
                    let mut wake_request = pooled_wake_request(
                        claim,
                        pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
                            pooled_route_activation_id(claim)?,
                        ),
                        pplx_space_sbx_mgr_client::proto::WakeTargetState::Unspecified,
                    )?;
                    wake_request
                        .start_context
                        .as_mut()
                        .expect("pooled Wake request always has start context")
                        .route_identity =
                        Some(wake_request_route_identity(&expected_route_identity));
                    wake_request
                        .start_context
                        .as_mut()
                        .expect("pooled Wake context")
                        .operation_activation_attempt_id = fence
                        .execution
                        .activation_attempt_id
                        .map(|id| id.to_string());

                    let wake_started_at = Instant::now();
                    let mut internal_client = RetryingWorkerApiInternalClient::new(&mut node_api);
                    loop {
                        let Some(remaining) = wake_deadline.checked_duration_since(Instant::now())
                        else {
                            return Err(ControllerError::service_unavailable(
                                "pooled sandbox Wake did not complete within the readiness budget",
                            ));
                        };
                        // Polling the RPC is the point of no return: any
                        // transport error after this may hide a manager-side
                        // lifecycle transition or delayed completion receipt.
                        wake_may_have_landed = true;
                        let response = internal_client
                            .wake_sandbox(wake_request.clone(), remaining.min(NODE_RPC_TIMEOUT))
                            .instrument(tracing::info_span!(
                                "substrate.k8s.pool.wake_sandbox.client",
                                sandbox_id = %sandbox_id,
                            ))
                            .await;
                        match response {
                            Ok(response) => {
                                validate_wake_route_identity_if_present(
                                    response.get_ref(),
                                    &claim.sandbox_id.to_string(),
                                    &expected_route_identity,
                                    policy.requires_route_identity(),
                                )
                                .map_err(ControllerError::from)?;
                                break;
                            }
                            Err(error) => match classify_wake_rpc_error(&error) {
                                WakeRpcErrorDisposition::ReadinessPoll(_) => {
                                    let delay = start_wait_poll_interval(wake_started_at);
                                    if tokio::time::timeout_at(
                                        wake_deadline,
                                        tokio::time::sleep(delay),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        return Err(ControllerError::service_unavailable(format!(
                                            "pooled sandbox Wake did not complete within the readiness budget: {error}"
                                        )));
                                    }
                                }
                                WakeRpcErrorDisposition::Fail => {
                                    return Err(ControllerError::conflict(format!(
                                        "sandbox wake failed: {error}"
                                    )));
                                }
                            },
                        }
                    }
                    if let Some(route_activation_id) = publish_legacy_route_after_ack {
                        let expected_pod_uid = expected_route_identity
                            .pod_uid
                            .as_deref()
                            .ok_or_else(|| {
                                ControllerError::internal(
                                    "acknowledged pooled legacy route has no Pod UID",
                                )
                            })?;
                        self.store
                            .publish_legacy_route_identity_after_ack(
                                sandbox_id,
                                fence.execution.generation,
                                route_activation_id,
                                expected_pod_uid,
                            )
                            .await
                            .map_err(ControllerError::from)?;
                    }
                }
                self.poll_connected_client_until_command_ready(
                    &mut node_api,
                    sandbox_id,
                    metric_identity,
                    wake_deadline
                        .checked_duration_since(Instant::now())
                        .unwrap_or_default(),
                    ExecutionBinding {
                        execution_object_id: claim.pod_name.clone(),
                        runtime_endpoint: Self::endpoint_for_pod_ip(
                            pod_ip,
                            self.k8s.sandbox_proxy_port,
                        )
                        .map_err(ControllerError::from)?,
                        execution_generation: claim.execution_generation,
                    },
                    None,
                )
                .await
                .map_err(ControllerError::from)
            }
            .await;
            if let Some(activation_attempt_id) = owned_attempt_id
                && (wake_and_ready.is_ok() || !wake_may_have_landed)
            {
                self.store
                    .release_activation_attempt_best_effort(
                        sandbox_id,
                        fence.execution.generation,
                        activation_attempt_id,
                    )
                    .await;
            }
            wake_and_ready?;
        }

        Ok(())
    }

    async fn list_pool_configs(&self) -> Result<Vec<ClusterPoolConfigResponse>, ControllerError> {
        pool::list_pool_configs(&self.store).await
    }

    async fn upsert_pool_config(
        &self,
        request: ClusterPoolConfigRequest,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        pool::upsert_pool_config(&self.store, request).await
    }

    async fn disable_pool_config(
        &self,
        id: Uuid,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        pool::disable_pool_config(&self.store, id).await
    }

    async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        let started_at = Instant::now();
        loop {
            match self.try_resolve_node_api_endpoint(target) {
                Ok(endpoint) => return Ok(endpoint),
                Err(err)
                    if matches!(err, SubstrateError::Unavailable { .. })
                        && started_at.elapsed() < NODE_API_ENDPOINT_RESOLVE_TIMEOUT =>
                {
                    sleep(NODE_API_ENDPOINT_RESOLVE_RETRY_INTERVAL).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn select_ready_worker_node_api_target(
        &self,
    ) -> Result<OwnedNodeApiTarget, SubstrateError> {
        let candidates = self
            .list_inventory_nodes(Some(SpaceNodeRole::Worker.as_str()))
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter(|node| {
                node.state
                    .as_deref()
                    .and_then(|state| state.parse::<NodeLifecycleState>().ok())
                    == Some(NodeLifecycleState::Ready)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(SubstrateError::Unavailable {
                message: "no ready Kubernetes worker node-api pods".to_string(),
            });
        }
        let index = rand::rng().random_range(0..candidates.len());
        let candidate = candidates
            .get(index)
            .ok_or_else(|| SubstrateError::Failed {
                message: "ready Kubernetes worker selection produced an invalid index".to_string(),
            })?;
        Ok(OwnedNodeApiTarget {
            managed_by: candidate.managed_by,
            role: candidate.role,
            direct_endpoint: candidate.address.as_ref().map(ToString::to_string),
            substrate_node_id: candidate
                .substrate_node_id
                .clone()
                .or_else(|| candidate.name.clone()),
        })
    }

    async fn list_inventory_nodes(
        &self,
        role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        let pods = self.k8s.caches.node_api_pods.state();
        let mut candidates = Vec::with_capacity(pods.len());
        for pod in pods {
            let Some(app_name) = caches::pod_app_name(&pod) else {
                continue;
            };
            let Some(role) = caches::role_from_app_name(app_name) else {
                continue;
            };
            let role = SpaceNodeRole::parse(role).map_err(|err| SubstrateError::Failed {
                message: format!("node-api pod advertised invalid role {role:?}: {err}"),
            })?;
            if let Some(filter) = role_filter
                && filter != role.as_str()
            {
                continue;
            }
            let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) else {
                continue;
            };
            let Some(k8s_node) = self.k8s.caches.node_by_name(node_name) else {
                // Node hasn't been seen by the Node reflector yet; skip
                // rather than fabricate an ID. Will appear once the watch
                // catches up.
                continue;
            };
            let Some(uid_str) = k8s_node.metadata.uid.as_deref() else {
                continue;
            };
            let id = space_k8s_node_id(node_name, role.as_str());
            let Some(raw_address) = k8s_node_internal_ip(&k8s_node) else {
                tracing::warn!(
                    k8s_node_name = %node_name,
                    role = %role,
                    "skipping Kubernetes node inventory row without InternalIP"
                );
                continue;
            };
            let address = match raw_address.parse::<NodeAddress>() {
                Ok(address) => address,
                Err(err) => {
                    tracing::warn!(
                        k8s_node_name = %node_name,
                        role = %role,
                        raw_address = %raw_address,
                        %err,
                        "skipping Kubernetes node inventory row with invalid InternalIP"
                    );
                    continue;
                }
            };
            // The inventory listing derives state from node-api *pod*
            // readiness, which lags Kubernetes Node readiness during a
            // node-api rollout. It is not the lifecycle owner, so it persists
            // with `InsertOnly` — it seeds the state of a brand-new node row
            // but must never overwrite a live node's `kube_node_sync`-owned
            // state, or an unready pod on a Ready node would oscillate it back
            // to `registered`. The returned entry still reports the observed
            // pod-readiness state.
            let observed_state = NodeLifecycleState::from_ready(caches::pod_is_ready(&pod));
            match self
                .store
                .persist_k8s_node_inventory(
                    node_name,
                    uid_str,
                    id,
                    role,
                    Some(raw_address.as_str()),
                    NodeStateWrite::InsertOnly(observed_state),
                )
                .await
            {
                Ok(_) => {}
                Err(SubstrateError::NodeTombstoned { message }) => {
                    tracing::info!(
                        k8s_node_name = %node_name,
                        role = %role,
                        reason = %message,
                        "skipping tombstoned kube node from inventory listing"
                    );
                    continue;
                }
                Err(err) => return Err(err),
            }
            let created_at = pod
                .metadata
                .creation_timestamp
                .as_ref()
                .and_then(|t| OffsetDateTime::from_unix_timestamp_nanos(t.0.as_nanosecond()).ok())
                .unwrap_or_else(OffsetDateTime::now_utc);
            candidates.push(NodeInventoryEntry {
                id,
                name: Some(node_name.to_string()),
                address: Some(address),
                role,
                managed_by: SpaceNodeManagedBy::Kubernetes,
                state: Some(observed_state.as_str().to_string()),
                last_heartbeat: None,
                created_at,
                substrate_node_id: Some(node_name.to_string()),
            });
        }
        Ok(Some(dedupe_inventory_entries(candidates)))
    }
}

#[cfg(test)]
mod tests;
