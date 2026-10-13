use super::store::{ActivationFenceOwnership, ActiveExecutionStatusRow};
use super::*;
use crate::controllers::substrate::PooledExecutionClaim;
use pplx_space_common_core::types::{NodeId, PodUid, SandboxActivationAttemptId, SandboxLifecycle};
use uuid::Uuid;

pub(super) fn k8s_execution_route_identity(
    execution: &ActiveExecutionStatusRow,
) -> Result<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity, SubstrateError> {
    let route_generation =
        u64::try_from(execution.generation).map_err(|error| SubstrateError::Conflict {
            message: format!(
                "Kubernetes execution has invalid route generation {}: {error}",
                execution.generation
            ),
        })?;
    let Some(route_activation_id) = execution
        .route_activation_id
        .or(execution.activation_attempt_id)
    else {
        return Err(SubstrateError::Unavailable {
            message:
                "Kubernetes execution has no published route identity; retry after materialization"
                    .to_string(),
        });
    };
    let pod_uid = execution
        .pod_uid
        .as_ref()
        .ok_or_else(|| SubstrateError::Unavailable {
            message: "Kubernetes execution has no pod UID route; retry after materialization"
                .to_string(),
        })?;
    Ok(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
        sandbox_id: None,
        route_generation: Some(route_generation),
        current_node_id: execution.current_node_id.map(|node_id| node_id.to_string()),
        execution_generation: Some(execution.generation),
        pod_uid: Some(pod_uid.to_string()),
        activation_attempt_id: Some(route_activation_id.to_string()),
        substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes as i32,
    })
}

/// The target node is runtime-owned. Omit it from Wake so sandbox-manager
/// resolves and validates its own local node, while retaining the
/// control-plane observation in the expected acknowledgement when available.
pub(super) fn wake_request_route_identity(
    expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
) -> pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
    let mut requested = expected.clone();
    requested.current_node_id = None;
    requested
}

#[cfg(test)]
pub(super) fn validate_wake_route_identity(
    response: &pplx_space_sbx_mgr_client::proto::WakeResponse,
    expected_sandbox_id: &str,
    expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
) -> Result<(), SubstrateError> {
    validate_wake_route_identity_with_node_requirement(
        response,
        expected_sandbox_id,
        expected,
        true,
    )
}

/// During mixed-version operation, an old or rolled-back worker can acknowledge
/// every established route dimension while omitting its locally-owned node ID.
/// Only that absence is compatible: a present node must still equal the claim.
pub(super) fn validate_wake_route_identity_with_node_requirement(
    response: &pplx_space_sbx_mgr_client::proto::WakeResponse,
    expected_sandbox_id: &str,
    expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
    require_current_node_id: bool,
) -> Result<(), SubstrateError> {
    let actual = response
        .route_identity
        .as_ref()
        .ok_or_else(|| SubstrateError::Unavailable {
            message: "sandbox-manager Wake response omitted the authoritative route identity"
                .to_string(),
        })?;
    let uuid_strings_equal = |actual: &str, expected: &str| {
        matches!(
            (Uuid::parse_str(actual), Uuid::parse_str(expected)),
            (Ok(actual), Ok(expected)) if actual == expected
        )
    };
    let optional_uuid_strings_equal = |actual: Option<&str>, expected: Option<&str>| {
        actual
            .zip(expected)
            .is_some_and(|(actual, expected)| uuid_strings_equal(actual, expected))
    };
    let pod_uids_equal = |actual: Option<&str>, expected: Option<&str>| {
        actual
            .and_then(|actual| PodUid::parse(actual).ok())
            .zip(expected.and_then(|expected| PodUid::parse(expected).ok()))
            .is_some_and(|(actual, expected)| actual == expected)
    };
    let mismatch = [
        (
            !uuid_strings_equal(&response.sandbox_id, expected_sandbox_id),
            "sandbox_id",
        ),
        (
            actual
                .sandbox_id
                .as_deref()
                .is_some_and(|bound| !uuid_strings_equal(bound, expected_sandbox_id)),
            "route_identity.sandbox_id",
        ),
        (
            actual.route_generation != expected.route_generation,
            "route_generation",
        ),
        (
            match actual.current_node_id.as_deref() {
                Some(actual_node_id) => {
                    let actual_node_id = Uuid::parse_str(actual_node_id);
                    let expected_node_id = expected
                        .current_node_id
                        .as_deref()
                        .map(Uuid::parse_str)
                        .transpose();
                    match (actual_node_id, expected_node_id) {
                        (Ok(actual_node_id), Ok(Some(expected_node_id))) => {
                            actual_node_id != expected_node_id
                        }
                        (Ok(_), Ok(None)) => false,
                        _ => true,
                    }
                }
                None => require_current_node_id,
            },
            "current_node_id",
        ),
        (
            actual.substrate_kind != expected.substrate_kind,
            "substrate_kind",
        ),
        (
            actual.execution_generation != expected.execution_generation,
            "execution_generation",
        ),
        (
            !optional_uuid_strings_equal(
                actual.activation_attempt_id.as_deref(),
                expected.activation_attempt_id.as_deref(),
            ),
            "activation_attempt_id",
        ),
        (
            !pod_uids_equal(actual.pod_uid.as_deref(), expected.pod_uid.as_deref()),
            "pod_uid",
        ),
    ]
    .into_iter()
    .find_map(|(differs, dimension)| differs.then_some(dimension));
    if let Some(dimension) = mismatch {
        return Err(SubstrateError::Unavailable {
            message: format!(
                "sandbox-manager Wake acknowledged a different {dimension} route identity"
            ),
        });
    }
    Ok(())
}

/// Validate authoritative identity evidence when sandbox-manager returns it.
/// Resident Started wakes do not install a new identity, so an older/mid-start
/// manager may omit the evidence without turning guest readiness into an
/// authorization failure. Callers that initialize or rotate an identity pass
/// `required = true` and retain the strict acknowledgement contract.
pub(super) fn validate_wake_route_identity_if_present(
    response: &pplx_space_sbx_mgr_client::proto::WakeResponse,
    expected_sandbox_id: &str,
    expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
    required: bool,
) -> Result<bool, SubstrateError> {
    if response.route_identity.is_none() && !required {
        return Ok(false);
    }
    validate_wake_route_identity_with_node_requirement(
        response,
        expected_sandbox_id,
        expected,
        required,
    )?;
    Ok(true)
}

/// Required evidence and retry budget for one selected execution. Only a
/// healthy, unchanged Started resident may fall back to command readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WakeWaitPolicy {
    RequireIdentity,
    RequireLifecycle,
    ResidentProbe,
}

impl WakeWaitPolicy {
    /// Lifecycle-changing wakes must echo the selected route too: Paused and
    /// restoring paths have no command-readiness probe that could otherwise
    /// prove which runtime accepted the transition. Only an unchanged Started
    /// resident may treat the acknowledgement as optional.
    pub(super) const fn requires_route_identity(self) -> bool {
        !matches!(self, Self::ResidentProbe)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ActivationWakeAction {
    /// The operation already reached its durable target. No lifecycle RPC is
    /// owed. A complete resident route may still be used to fence readiness;
    /// `None` preserves the pre-route legacy binding fallback.
    SkipCompleted {
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    },
    Execute {
        execution_generation: i64,
        route_identity: pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        policy: WakeWaitPolicy,
        /// Only A/B split operations need the prerequisite's durable operation
        /// receipt support. First-route B/B adoption and repair-only probes do
        /// not rely on a distinct receipt identity.
        require_split_receipt_capability: bool,
        publish_legacy_route_after_ack: Option<SandboxActivationAttemptId>,
    },
}

/// Derive lifecycle intent once from the reconciled fence. Callers must not
/// reconstruct `Completed` from nullable IDs: `Joined` and `Completed` both
/// have no locally-owned lease, but only the latter means no lifecycle RPC is
/// owed.
pub(super) fn activation_wake_action(
    execution: &ActiveExecutionStatusRow,
    ownership: ActivationFenceOwnership,
    target: SandboxLifecycle,
) -> Result<ActivationWakeAction, SubstrateError> {
    if ownership == ActivationFenceOwnership::Completed {
        let route_identity =
            if execution.route_activation_id.is_some() && execution.pod_uid.is_some() {
                Some(k8s_execution_route_identity(execution)?)
            } else {
                None
            };
        // A completed Started execution may still receive a bounded no-op
        // repair probe for the pre-deploy poisoned backlog. A completed Paused
        // execution must not redo lifecycle work merely to repair metadata.
        if target == SandboxLifecycle::Started
            && let Some(route_identity) = route_identity
        {
            return Ok(ActivationWakeAction::Execute {
                execution_generation: execution.generation,
                route_identity,
                policy: WakeWaitPolicy::ResidentProbe,
                require_split_receipt_capability: false,
                publish_legacy_route_after_ack: None,
            });
        }
        return Ok(ActivationWakeAction::SkipCompleted { route_identity });
    }

    let route_identity = k8s_execution_route_identity(execution)?;
    let route_activation_id = execution.route_activation_id;
    let operation_activation_id = execution.activation_attempt_id;
    let policy = if route_activation_id.is_none() || route_activation_id == operation_activation_id
    {
        WakeWaitPolicy::RequireIdentity
    } else if target != SandboxLifecycle::Started
        || !execution.lifecycle.trim().eq_ignore_ascii_case("started")
        || execution.state != ActiveExecutionState::Running
    {
        WakeWaitPolicy::RequireLifecycle
    } else {
        WakeWaitPolicy::ResidentProbe
    };
    let require_split_receipt_capability = operation_activation_id.is_some()
        && route_activation_id.is_some()
        && operation_activation_id != route_activation_id;
    let publish_legacy_route_after_ack = route_activation_id
        .is_none()
        .then_some(operation_activation_id)
        .flatten();

    Ok(ActivationWakeAction::Execute {
        execution_generation: execution.generation,
        route_identity,
        policy,
        require_split_receipt_capability,
        publish_legacy_route_after_ack,
    })
}

pub(super) fn k8s_node_internal_ip(node: &Node) -> Option<String> {
    node.status
        .as_ref()
        .and_then(|status| status.addresses.as_ref())
        .and_then(|addresses| {
            addresses
                .iter()
                .find(|address| address.type_ == "InternalIP")
                .map(|address| address.address.clone())
        })
}

/// Collapse multiple `NodeInventoryEntry` candidates that share an `id` down to
/// one, then sort the result.
///
/// The synthesized `id` is keyed on `(k8s node UID, role)`, so during a
/// node-api DaemonSet rollout the cache can hold two Pods on the same Node —
/// e.g. a terminating old Pod alongside a starting new Pod — and the loop
/// above emits two candidates with the same `id`. The UI keys table rows by
/// `id`, so duplicates trigger React duplicate-key warnings and surface
/// inconsistent state. Prefer a Ready entry over a non-Ready one; on a tie,
/// pick the newer `created_at` so a fresh post-rollout Pod wins over a
/// terminating sibling.
pub(super) fn dedupe_inventory_entries(
    candidates: Vec<NodeInventoryEntry>,
) -> Vec<NodeInventoryEntry> {
    let mut by_id: std::collections::HashMap<NodeId, NodeInventoryEntry> =
        std::collections::HashMap::with_capacity(candidates.len());
    for candidate in candidates {
        match by_id.entry(candidate.id) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(candidate);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                let is_ready = |entry: &NodeInventoryEntry| {
                    entry
                        .state
                        .as_deref()
                        .and_then(|state| state.parse::<NodeLifecycleState>().ok())
                        == Some(NodeLifecycleState::Ready)
                };
                let candidate_ready = is_ready(&candidate);
                let existing_ready = is_ready(slot.get());
                let prefer_candidate = match (candidate_ready, existing_ready) {
                    (true, false) => true,
                    (false, true) => false,
                    _ => candidate.created_at > slot.get().created_at,
                };
                if prefer_candidate {
                    slot.insert(candidate);
                }
            }
        }
    }
    let mut entries: Vec<NodeInventoryEntry> = by_id.into_values().collect();
    entries.sort_by(|a, b| {
        a.role
            .cmp(&b.role)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });
    entries
}

pub(super) fn start_wait_poll_interval(started_at: Instant) -> Duration {
    if started_at.elapsed() < START_WAIT_FAST_POLL_WINDOW {
        START_WAIT_FAST_POLL_INTERVAL
    } else {
        START_WAIT_SLOW_POLL_INTERVAL
    }
}

pub(super) fn jittered_command_ready_poll_delay(delay: Duration, jitter_fraction: f64) -> Duration {
    // Subtractive jitter keeps the 10-second cap hard while spreading probes
    // across the preceding 20% of each backoff window.
    delay.mul_f64(1.0 - jitter_fraction.clamp(0.0, COMMAND_READY_POLL_JITTER_FACTOR))
}

/// Preserve low wake latency while the guest normally starts, then shed load
/// when readiness remains stuck and every probe fans out across node services.
pub(super) struct CommandReadyPollBackoff {
    started_at: Instant,
    deadline: Instant,
    next_slow_delay: Duration,
}

impl CommandReadyPollBackoff {
    pub(super) fn new(started_at: Instant, deadline: Instant) -> Self {
        Self {
            started_at,
            deadline,
            next_slow_delay: START_WAIT_SLOW_POLL_INTERVAL,
        }
    }

    /// Every delay is clamped to the readiness deadline so backoff can never
    /// extend the caller's request budget.
    pub(super) fn next_delay(&mut self) -> Duration {
        let delay = if self.started_at.elapsed() < START_WAIT_FAST_POLL_WINDOW {
            START_WAIT_FAST_POLL_INTERVAL
        } else {
            let delay = jittered_command_ready_poll_delay(
                self.next_slow_delay,
                rand::random::<f64>() * COMMAND_READY_POLL_JITTER_FACTOR,
            );
            self.next_slow_delay = self
                .next_slow_delay
                .saturating_mul(2)
                .min(COMMAND_READY_MAX_POLL_INTERVAL);
            delay
        };
        delay.min(self.deadline.saturating_duration_since(Instant::now()))
    }
}

pub(super) fn record_pod_proxy_wait_ready(
    metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
    sandbox_id: SandboxId,
    started_at: Instant,
    connect_started_at: Instant,
) {
    pplx_space_metrics::sandbox::api::api_step_latency(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStep {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "runtime_connect",
            result: "success",
            started_at: connect_started_at.into(),
            phase: None,
            wait_phase: None,
            since: None,
        },
    );
    pplx_space_metrics::sandbox::api::api_step_latency(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStep {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "runtime_reachable",
            result: "success",
            started_at: started_at.into(),
            phase: None,
            wait_phase: None,
            since: None,
        },
    );
}

pub(super) fn record_pod_proxy_wait_timeout(
    metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
    sandbox_id: SandboxId,
    started_at: Instant,
) {
    pplx_space_metrics::sandbox::api::api_step_error(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStepError {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "runtime_reachable",
            reason: "timeout",
            phase: None,
            wait_phase: None,
            grpc_code: None,
        },
    );
    pplx_space_metrics::sandbox::api::api_step_latency(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStep {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "runtime_reachable",
            result: "timeout",
            started_at: started_at.into(),
            phase: None,
            wait_phase: None,
            since: None,
        },
    );
}

pub(super) fn record_pod_ip_ready(
    metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
    sandbox_id: SandboxId,
    started_at: Instant,
    result: &'static str,
) {
    pplx_space_metrics::sandbox::api::api_step_latency(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStep {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "network_address_assigned",
            result,
            started_at: started_at.into(),
            phase: None,
            wait_phase: None,
            since: None,
        },
    );
}

pub(super) fn record_api_wait_ready_latency(
    sandbox_id: SandboxId,
    metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
    wait_phase: &'static str,
    result: &'static str,
    started_at: Instant,
    since: Option<&'static str>,
) {
    pplx_space_metrics::sandbox::api::api_step_latency(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStep {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "wait_ready",
            result,
            started_at: started_at.into(),
            phase: None,
            wait_phase: Some(wait_phase),
            since,
        },
    );
}

pub(super) fn record_api_wait_ready_error(
    sandbox_id: SandboxId,
    metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
    wait_phase: &'static str,
    reason: &'static str,
    grpc_code: &'static str,
) {
    pplx_space_metrics::sandbox::api::api_step_error(
        sandbox_id,
        pplx_space_metrics::sandbox::api::ApiStepError {
            identity: metric_identity,
            backend: "fc",
            template: None,
            step: "wait_ready",
            reason,
            phase: None,
            wait_phase: Some(wait_phase),
            grpc_code: Some(grpc_code),
        },
    );
}

pub(super) fn grpc_code_tag(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

pub(super) fn normalize_pod_phase(phase: Option<String>, fallback: &str) -> String {
    let raw = phase
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| fallback.to_string());
    match raw.as_str() {
        "running" => "started".to_string(),
        "pending" => "creating".to_string(),
        "succeeded" => "stopped".to_string(),
        "failed" => "failed".to_string(),
        "unknown" => "failed".to_string(),
        other => other.to_string(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TerminalPodPhase {
    Succeeded,
    Failed,
}

impl fmt::Display for TerminalPodPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded => formatter.write_str("Succeeded"),
            Self::Failed => formatter.write_str("Failed"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StalePodReason {
    SandboxSuspended,
    SandboxStopped,
    LocalOnlyRestoreNowPortable,
    PodDeleting,
    TerminalPhase(TerminalPodPhase),
}

impl fmt::Display for StalePodReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SandboxSuspended => formatter.write_str("sandbox suspended"),
            Self::SandboxStopped => formatter.write_str("sandbox stopped"),
            Self::LocalOnlyRestoreNowPortable => {
                formatter.write_str("local-only restore now has remote fallback")
            }
            Self::PodDeleting => formatter.write_str("pod deleting"),
            Self::TerminalPhase(phase) => phase.fmt(formatter),
        }
    }
}

pub(super) fn terminal_pod_phase(pod: &SandboxPodView) -> Option<TerminalPodPhase> {
    match pod.phase() {
        Some("Succeeded") => Some(TerminalPodPhase::Succeeded),
        Some("Failed") => Some(TerminalPodPhase::Failed),
        _ => None,
    }
}

/// True if the pod is in a status that callers of `read_sandbox_pod` branch
/// on: it has been assigned an IP, or it has reached a terminal phase, or
/// the scheduler has flagged it unschedulable. Anything else (notably
/// pre-binding `Pending` with no IP) is treated as cache-stale so the
/// reflector watch lag cannot stall callers that are already past the
/// per-pod watch.
pub(super) fn pod_status_is_settled(pod: &SandboxPodView) -> bool {
    assigned_pod_ip(pod).ok().flatten().is_some()
        || terminal_pod_phase(pod).is_some()
        || pod.unschedulable_message().is_some()
}

pub(super) fn execution_pod_create_in_flight(state: ActiveExecutionState) -> bool {
    state == ActiveExecutionState::Creating
}

pub(super) fn execution_pod_lookup_in_flight(state: ActiveExecutionState) -> bool {
    matches!(
        state,
        ActiveExecutionState::Creating | ActiveExecutionState::Recovering
    )
}

pub(super) fn stale_restore_execution_create(
    lifecycle: &str,
    state: ActiveExecutionState,
    create_age_expired: bool,
) -> bool {
    normalize_lifecycle(lifecycle) == "restoring"
        && execution_pod_create_in_flight(state)
        && create_age_expired
}

/// The node-group label of the node a claimed warm pod runs on, if the
/// reflector can say.
///
/// Separate from [`pooled_pod_node_admission`] because it answers a question
/// that has no candidate set behind it: on the `Unprovisioned` arm there is no
/// set to test membership against, and what matters is only whether the node
/// belongs to *any* group.
///
/// Three states, not `Option`. An `Option` here collapses "the node carries no
/// label" with "the node could not be read", and the caller treats the absence
/// as safe to adopt — so a cold or stale node reflector would let a sandbox
/// adopt a warm pod that is in fact running on a labeled dedicated node. That
/// is the same conflation [`PooledPodAdmission`] splits `LabelAbsent` out of,
/// reintroduced one helper over; the enum is what stops it recurring.
pub(super) enum PooledPodNodeGroup {
    /// The node carries a group label.
    Labeled(String),
    /// The node was read and carries no group label.
    Unlabeled,
    /// The node could not be read: unscheduled pod, or absent from the
    /// reflector. Proves nothing either way.
    Unreadable,
}

pub(super) fn pooled_pod_node_group(
    caches: &K8sCaches,
    pod: Option<&SandboxPodView>,
) -> PooledPodNodeGroup {
    let Some(node_name) = pod.and_then(SandboxPodView::node_name) else {
        return PooledPodNodeGroup::Unreadable;
    };
    let Some(node) = caches.node_by_name(node_name) else {
        return PooledPodNodeGroup::Unreadable;
    };
    match node_group_label(&node) {
        Some(group) => PooledPodNodeGroup::Labeled(group.to_owned()),
        None => PooledPodNodeGroup::Unlabeled,
    }
}

/// Verdict on the node a claimed warm pod already runs on.
///
/// Three states, not a bool, because the remedies differ: a slot on capacity
/// this sandbox may not use has to be taken *out* of the pool, while a slot
/// whose node simply cannot be read right now is probably fine and must be
/// handed back intact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PooledPodAdmission {
    /// The node's live label names a group inside the candidate set.
    Admitted,
    /// The node's live label names a group outside the set. Evidence about the
    /// node, so the slot is actionable.
    OutsideSet,
    /// The node carries no group label at all. An `In (…)` term excludes it
    /// just the same, so the slot is still refused — but the *reason* is the
    /// labeler, not the node, and the remedy is therefore not to destroy the
    /// pod. See [`pooled_pod_node_admission`].
    LabelAbsent,
    /// The node's group could not be determined: the pod is unscheduled, the
    /// node is absent from the reflector, or the reflector is still cold.
    Unknown,
}

/// Where the node a claimed warm pod runs on sits relative to `placement`'s
/// candidate set, read from the node's **live** label rather than the mirrored
/// `nodes.node_group_id` column, which lags a reassignment.
///
/// Never answers [`PooledPodAdmission::Admitted`] on incomplete evidence. The
/// asymmetry is deliberate: adopting a pod that turns out to be on another
/// org's dedicated hardware is a capacity-isolation violation, while refusing
/// one costs a cold create.
///
/// The cold-reflector case is worth pricing exactly, because it is narrower
/// than it looks. `kube_runtime::reflector::Store` buffers the initial `LIST`
/// privately and swaps the **complete** buffer into the readable store at
/// `InitDone`, so there is no partially-filled state to read: while the LIST is
/// in flight every lookup misses and yields `Unknown` whatever order this
/// function uses. Readiness is therefore never consulted here at all; what
/// declining to consult it buys is the window *after* that swap and before the
/// readiness task marks `caches.nodes_ready()` — the store is whole, the flag
/// is not yet set, and a readiness-gated version (which this function
/// deliberately is not) would refuse every adoption for the length of it.
///
/// Worth stating because `caches.nodes_ready()` is **not** part of `/readyz` —
/// that probe reports only the *pod* reflector — so a freshly rolled
/// `space-api` replica does take create traffic before its node cache is
/// ready. During the LIST those creates build cold; after the swap they are
/// answered normally.
///
/// Unlike the restore pre-flight, a miss is **not** split by readiness into
/// "gone" and "not yet loaded". The remedy is the same either way — release the
/// slot, build cold — and this enum selects a remedy, so the split would show
/// up only as a metric tag. `pool_claim_admission{verdict:unknown}` is read
/// instead by shape: a burst at process start is the node cache filling, and
/// sustained non-zero at steady state is not.
pub(super) fn pooled_pod_node_admission(
    caches: &K8sCaches,
    placement: &NodeGroupPlacement,
    pod: Option<&SandboxPodView>,
) -> PooledPodAdmission {
    // Defensive: the usability gate makes this unreachable from the only
    // production caller. `spec.nodeName` is written by the binding and kubelet
    // writes `status.phase`/`status.podIP` only after observing it, so
    // `pooled_claim_pod_is_usable`'s `Running` + matching-IP requirement
    // implies a bound node — and both sources here hand over one whole object,
    // so there is no torn read to produce the pair. Kept because it costs
    // nothing and the alternative is an `expect`.
    let Some(node_name) = pod.and_then(SandboxPodView::node_name) else {
        return PooledPodAdmission::Unknown;
    };
    // Readiness is deliberately not consulted at all: a miss is `Unknown`
    // whichever way the store got that way, and the doc comment above explains
    // why splitting it would only produce a metric tag. The restore pre-flight
    // does consult it, for a reason that does not apply here — it has two
    // verdicts to choose between.
    let Some(node) = caches.node_by_name(node_name) else {
        return PooledPodAdmission::Unknown;
    };
    match node_group_label(&node) {
        Some(group) if placement.admits_group(group) => PooledPodAdmission::Admitted,
        // A foreign group is evidence about the node: something reassigned it,
        // and the slot has to leave the pool.
        Some(_) => PooledPodAdmission::OutsideSet,
        // No label at all excludes the node from an `In (…)` term just the
        // same, so the adoption is refused either way — but this is evidence
        // about the *writer*, not the node, and that changes the remedy.
        //
        // An unrelated MNG change can strip an MNG-set `node-restriction.*`
        // label until the labeler repairs it. Treating that as evidence about
        // the slot would invalidate up to `MAX_STALE_POOL_CLAIMS_PER_REQUEST` warm
        // pods per create for the length of the window — draining the pool
        // fleet-wide in response to a label write, and booking it as a group
        // reassignment that never happened.
        //
        // The restore pre-flight already draws this line
        // (`LocalRestorePreflight::LabelAbsent` beside `OutOfSet`); this is the
        // same distinction on the pool path.
        None => PooledPodAdmission::LabelAbsent,
    }
}

pub(super) fn pooled_claim_pod_is_usable(
    claim: &PooledExecutionClaim,
    pod: Option<&SandboxPodView>,
) -> bool {
    let Some(pod) = pod else {
        return false;
    };

    !pod.is_deleting()
        && pod.phase() == Some("Running")
        && claim.pod_uid.is_some()
        && pod.uid() == claim.pod_uid.as_ref().map(|uid| uid.as_str())
        && pod.pod_ip() == Some(claim.pod_ip.as_str())
}

pub(super) fn stale_existing_pod_reason(
    sandbox: &Sandbox,
    pod: &SandboxPodView,
    suspend_restore_storage_uri: Option<&str>,
) -> Option<StalePodReason> {
    // Restore flips the DB lifecycle after the pod is created, so a restore
    // pod can be valid while the sandbox row still reads suspended.
    let pod_is_restore_in_progress = pod.has_annotation(SandboxPodKey::SuspendRestoreStorageUri)
        || pod.has_annotation(SandboxPodKey::SuspendRestoreSourceNode);
    if is_suspended_lifecycle(&sandbox.lifecycle) && !pod_is_restore_in_progress {
        return Some(StalePodReason::SandboxSuspended);
    }
    let is_pending_local_only_restore = suspend_restore_storage_uri.is_some()
        && pod.has_annotation(SandboxPodKey::SuspendRestoreSourceNode)
        && !pod.has_annotation(SandboxPodKey::SuspendRestoreStorageUri)
        && assigned_pod_ip(pod).ok().flatten().is_none();
    if is_pending_local_only_restore {
        return Some(StalePodReason::LocalOnlyRestoreNowPortable);
    }
    if pod.is_deleting() {
        return Some(StalePodReason::PodDeleting);
    }
    terminal_pod_phase(pod).map(StalePodReason::TerminalPhase)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WakeRpcErrorDisposition {
    ReadinessPoll(&'static str),
    Fail,
}

pub(super) fn classify_wake_rpc_error(error: &tonic::Status) -> WakeRpcErrorDisposition {
    if error.code() == Code::FailedPrecondition
        && error
            .message()
            .contains(pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER)
    {
        return WakeRpcErrorDisposition::ReadinessPoll("missing_runtime_context");
    }
    if error.code() == Code::FailedPrecondition
        && (error.message().contains("cannot wake") || error.message().contains("wait for"))
    {
        return WakeRpcErrorDisposition::ReadinessPoll("failed_precondition");
    }
    if error.code() == Code::Unavailable {
        return WakeRpcErrorDisposition::ReadinessPoll("unavailable");
    }
    WakeRpcErrorDisposition::Fail
}

/// Decide whether `delete_execution` should skip best-effort in-VM cleanup
/// (the stop/delete gRPC calls) for a given `resolve_pod_ip` error and proceed
/// straight to deleting the pod object.
///
/// `resolve_pod_ip` surfaces `Unavailable` when the tracked pod has no
/// reachable runtime to talk to — most importantly a `Pending+Unschedulable`
/// pod that never got an IP (e.g. a restore pinned to a now-unsuitable source
/// node via NodeAffinity). The delete path has nothing to say to such a pod,
/// so it swallows the error and falls through to the unconditional pod-delete
/// loop. Every other `resolve_pod_ip` error — DB failure, kube-API query
/// failure, malformed pod IP — is a genuine infra fault that the pod-delete
/// loop would hit too, so it must propagate.
pub(super) fn delete_can_skip_runtime_cleanup(err: &SubstrateError) -> bool {
    matches!(err, SubstrateError::Unavailable { .. })
}

/// Whether an execution teardown destroys node-local sandbox state or
/// preserves it.
///
/// Both modes stop the VM, delete the backing pods, and forget pod tracking.
/// Only `Retire` treats preservation as mandatory by enqueueing a stopped-disk
/// backup before pod tracking is forgotten. Only `Delete` additionally sends
/// the sbx-mgr `Delete` that removes the sandbox's filesystem and
/// local-artifact rows. Failure reconcilers must use `Retire`: a sandbox that
/// reached `failed` mid-suspend may hold its only up-to-date copy on the node,
/// and the node-local rows stay addressable for later salvage/backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExecutionTeardown {
    Delete,
    Retire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SandboxDeleteRpcErrorDisposition {
    ContinueTeardown,
    DeferTeardown,
    RetainedFilesystemOnlyCopy,
}

/// Preserve the pod and execution tracking only when sbx-mgr explicitly says
/// an unpublished suspend owns the sandbox. Other `Aborted` cleanup failures
/// keep the historical best-effort delete behavior.
pub(super) fn classify_sandbox_delete_rpc_error(
    error: &tonic::Status,
) -> SandboxDeleteRpcErrorDisposition {
    if pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(error) {
        SandboxDeleteRpcErrorDisposition::RetainedFilesystemOnlyCopy
    } else if pplx_space_sbx_mgr_client::is_suspend_publication_pending_status(error) {
        SandboxDeleteRpcErrorDisposition::DeferTeardown
    } else {
        SandboxDeleteRpcErrorDisposition::ContinueTeardown
    }
}

impl ExecutionTeardown {
    pub(super) fn sends_sandbox_stop(self) -> bool {
        matches!(self, Self::Retire)
    }

    pub(super) fn sends_sandbox_delete(self) -> bool {
        matches!(self, Self::Delete)
    }

    /// Whether failed preservation work must abort teardown before pod deletion
    /// and pod-tracking forget. Retire must stop the VM and enqueue the
    /// stopped-disk backup before forgetting tracking; once tracking is
    /// inactive, the sandbox drops out of the failed-cleanup retry scan.
    /// sbx-mgr's stop returns success for unknown and not-running sandboxes,
    /// so aborting only fires when a live runtime's stop genuinely failed, the
    /// pod worker could not be reached, or the stopped-disk backup could not be
    /// enqueued.
    pub(super) fn requires_stop_success(self) -> bool {
        matches!(self, Self::Retire)
    }

    /// Whether pod-tracking teardown should preserve `placement_changed_at`
    /// rather than bump it to `NOW()`. Only `Retire` preserves: it exists to
    /// keep a sandbox recoverable, and bumping the clock would strand a
    /// markerless row's ready snapshot behind the legacy freshness gate (see
    /// `store::PlacementClock`). `Delete` bumps — placement genuinely ended.
    pub(super) fn preserves_placement_freshness(self) -> bool {
        matches!(self, Self::Retire)
    }
}

pub(super) fn retire_can_skip_backup_enqueue_error(err: &SubstrateError) -> bool {
    match err {
        // No sbx-mgr sandbox row: there is no node-local source left to
        // salvage, so the backup is moot.
        SubstrateError::NotFound { .. } => true,
        // sbx-mgr's `FailedPrecondition` surfaces as `Conflict` with the
        // message passed through. Two refusals are terminal for retire's
        // fixed current-instance/disk request and can never resolve by
        // retrying: a `failed` local state (nothing moves it to
        // stopped/suspended without external action) and a suspended
        // instance whose current artifact is the full suspend snapshot
        // (its durability is the suspend machinery's job). Aborting on
        // those would wedge the failure reconciler's retry loop forever
        // while the leftover pod keeps feeding kubelet CreatePodSandbox
        // retries; node-local state stays salvageable either way because
        // retire never sends the sbx-mgr delete. Any other `Conflict`
        // (e.g. a still-stopping row that can converge to a backupable
        // state) keeps the execution tracked for retry.
        SubstrateError::Conflict { message } => {
            use pplx_space_common_core::artifact_backup_signals::{
                CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER,
                CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER,
            };
            message.contains(CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER)
                || message.contains(CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER)
        }
        _ => false,
    }
}

/// The scheduler's first placement decision for a freshly-created pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SchedulingOutcome {
    /// Bound to a node (`spec.nodeName` populated at bind, before the CNI
    /// IP). Carries the bound node so a shortlist restore can record whether
    /// the pod actually landed on the source or on a fallback.
    Scheduled { node_name: String },
    /// Marked `PodScheduled=False, reason=Unschedulable`.
    Unschedulable,
}

/// Classify a pod's scheduling state, or `None` if the scheduler has not yet
/// decided. A populated `spec.nodeName` is checked **first** and is
/// authoritative: it is immutable once set, so a pod carrying it is bound no
/// matter what the conditions say — a previously-unschedulable pod that later
/// binds can briefly keep its stale `PodScheduled=False/Unschedulable`
/// condition, and classifying that pod as unschedulable would let the re-drive
/// path delete a pod that is no longer inert. The reverse misread cannot
/// happen: `nodeName` is never set on an unbound pod.
pub(super) fn scheduling_outcome_of(pod: &SandboxPodView) -> Option<SchedulingOutcome> {
    if let Some(node_name) = assigned_pod_node_name(pod) {
        return Some(SchedulingOutcome::Scheduled {
            node_name: node_name.to_string(),
        });
    }
    if pod.unschedulable_message().is_some() {
        return Some(SchedulingOutcome::Unschedulable);
    }
    None
}

/// Why is `CancelGuard` running its cleanup path? Lets `Drop` choose
/// between spawning a real pod-cleanup and deferring to the reconciler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CancelCause {
    /// Caller dropped the future (e.g. HTTP client disconnected and axum
    /// dropped the handler). Pod was created but the request can no longer
    /// reach a normal completion path — must clean up.
    CallerDropped,
    /// Observed node-manager unreachable (`Code::Unavailable` / transport
    /// failure) while creating. Cleanup would just feed the saturated
    /// node-manager — defer to the reconciler GC instead. Cleanup of both
    /// the K8s pod and the `pod_tracking` DB row is the orphaned-sandbox
    /// reconciler's `OrphanKind::Normal` path
    /// (`libs/cluster/controller/server/src/orphaned_sandboxes.rs`), which
    /// fires for `lifecycle='creating'` rows whose pod is missing past
    /// `CREATE_AGE_THRESHOLD_SECS`.
    NodeMgrUnreachable,
}

/// Guard that gives back what a `create_execution` call claimed when the call
/// does not reach a normal completion path — a failure exit, or the future
/// being cancelled (dropped), e.g. when the HTTP client disconnects and axum
/// drops the handler. Its scope follows what the call can prove:
///
/// - claim armed, no pod create sent: no pod can exist, so it releases the
///   claimed execution alone;
/// - pod create in flight: the apiserver may have accepted a pod this call
///   never learns, so it releases nothing and leaves the claim, which still
///   names that pod, to reconciliation;
/// - armed with the created pod: it retires the claim and deletes that pod by
///   UID.
pub(super) struct CancelGuard {
    sandbox_id: SandboxId,
    pod_name: String,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    pod_uid: Option<PodUid>,
    k8s: Option<KubernetesState>,
    store: Option<K8sStore>,
    /// Set once the pod create is sent and cleared once its pod arms the
    /// guard; while set, nothing is released.
    pod_create_in_flight: bool,
    cancel_tracker: Arc<CancelRateTracker>,
    /// The substrate's tracker: graceful shutdown waits for the background
    /// release or cleanup a dropped create leaves running.
    task_tracker: TaskTracker,
    cause: CancelCause,
}

impl CancelGuard {
    pub(super) fn new(
        sandbox_id: SandboxId,
        pod_name: String,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
        cancel_tracker: Arc<CancelRateTracker>,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            sandbox_id,
            pod_name,
            generation,
            activation_attempt_id,
            pod_uid: None,
            k8s: None,
            store: None,
            pod_create_in_flight: false,
            cancel_tracker,
            task_tracker,
            cause: CancelCause::CallerDropped,
        }
    }

    /// Arm the guard for the execution claim alone. Called right after the
    /// claim, so that every exit before the pod exists gives the claim back
    /// without touching Kubernetes.
    pub(super) fn arm_claim(&mut self, store: K8sStore) {
        self.store = Some(store);
    }

    /// Called just before the pod create is sent. From here until its
    /// response arrives, a pod this call cannot see may exist, and only the
    /// claim's execution row still names it — so the guard stops releasing.
    pub(super) fn begin_pod_create(&mut self) {
        self.pod_create_in_flight = true;
    }

    /// Widen the guard to the pod. Called after the pod is created, so that
    /// cancellation from here on also deletes the pod this call created.
    pub(super) fn arm(&mut self, k8s: KubernetesState, store: K8sStore, pod_uid: Option<PodUid>) {
        self.k8s = Some(k8s);
        self.store = Some(store);
        self.pod_uid = pod_uid;
        self.pod_create_in_flight = false;
    }

    /// Give back the claim on a failure exit before the pod create is sent.
    /// Awaited, so the caller's error is returned only after the claim is
    /// released. The caller deletes its fresh sandbox row on that error; if the
    /// delete fails or the request is cancelled, the row survives, and a later
    /// data-plane access, start or wake must find it unclaimed rather than
    /// join an activation that will never produce a pod. The guard keeps
    /// ownership until the release has finished: if this await is cancelled,
    /// its transaction rolls back and `Drop` runs the same fenced release in
    /// the background instead.
    ///
    /// Only valid before `begin_pod_create()` and `arm()`: past them the
    /// early return below drops the guard into the full `Drop` path.
    pub(super) async fn release_claim(mut self) {
        debug_assert!(
            self.k8s.is_none() && !self.pod_create_in_flight,
            "release_claim is only valid before the pod create is sent"
        );
        if self.k8s.is_some() || self.pod_create_in_flight {
            return;
        }
        if let Some(store) = self.store.clone() {
            release_claimed_execution(
                store,
                self.sandbox_id,
                self.generation,
                self.activation_attempt_id,
            )
            .await;
        }
        self.store = None;
    }

    /// Annotate the guard with the cause of the impending drop so `Drop`
    /// can choose between spawning a real cleanup and deferring to the
    /// reconciler. Caller is expected *not* to defuse after this — the
    /// `Drop` impl owns the decision.
    pub(super) fn mark_cause(&mut self, cause: CancelCause) {
        self.cause = cause;
    }

    pub(super) fn defuse(mut self) {
        self.k8s = None;
        self.store = None;
        self.pod_create_in_flight = false;
    }

    #[cfg(test)]
    pub(super) fn is_unarmed(&self) -> bool {
        self.k8s.is_none() && self.store.is_none()
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        let Some(k8s) = self.k8s.take() else {
            let Some(store) = self.store.take() else {
                return;
            };
            if self.pod_create_in_flight {
                // The apiserver may have accepted the pod. Retiring the claim
                // would drop the only record naming it, so leave the claim to
                // the reconcilers that age out a `creating` sandbox.
                tracing::warn!(
                    sandbox_id = %self.sandbox_id,
                    pod_name = %self.pod_name,
                    "create_execution cancelled while its pod create was in flight — leaving \
                     the claimed execution to reconciliation"
                );
                return;
            }
            // No pod create was sent, so there is nothing to delete and
            // nothing that reaches node-mgr: release the claim alone, and leave
            // the cancel-storm signal, which counts pod cleanups, untouched.
            tracing::warn!(
                sandbox_id = %self.sandbox_id,
                pod_name = %self.pod_name,
                "create_execution cancelled before its pod create was sent — releasing the \
                 claimed execution"
            );
            self.task_tracker.spawn(
                release_claimed_execution(
                    store,
                    self.sandbox_id,
                    self.generation,
                    self.activation_attempt_id,
                )
                .instrument(tracing::info_span!(
                    "substrate.k8s.cancel_release_claim",
                    sandbox_id = %self.sandbox_id,
                    pod_name = %self.pod_name,
                )),
            );
            return;
        };
        let store = self.store.take();
        let sandbox_id = self.sandbox_id;
        let pod_name = self.pod_name.clone();
        let generation = self.generation;
        let activation_attempt_id = self.activation_attempt_id;
        let pod_uid = self.pod_uid.take();

        // Always record the cancel — both branches contribute to the
        // backpressure signal that gates new creates upstream.
        self.cancel_tracker.record_cancel();

        if matches!(self.cause, CancelCause::NodeMgrUnreachable) {
            // Node-mgr unreachable is exactly when *not* to spawn cleanup:
            // it would just feed the saturated node-manager via the
            // sbx-mgr cleanup path. Reconciler will GC the pod.
            tracing::warn!(
                sandbox_id = %sandbox_id,
                pod_name = %pod_name,
                "create_execution cancelled (node-mgr unreachable) — deferring pod cleanup to reconciler"
            );
            pplx_space_metrics::sandbox::api::create_cancel_cleanup_deferred();
            return;
        }

        tracing::error!(
            sandbox_id = %sandbox_id,
            pod_name = %pod_name,
            "create_execution cancelled — spawning background pod cleanup"
        );

        let cleanup_span = tracing::info_span!(
            "substrate.k8s.cancel_cleanup",
            sandbox_id = %sandbox_id,
            pod_name = %pod_name,
        );

        self.task_tracker.spawn(
            async move {
                let Some(store) = store else {
                    return;
                };
                let Some(pod_uid) = pod_uid else {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        "cancel cleanup: pod has no UID; leaving it for reconciliation"
                    );
                    return;
                };
                let retired = match store
                    .forget_pod_tracking_if_generation_and_attempt(
                        sandbox_id,
                        generation,
                        activation_attempt_id,
                    )
                    .await
                {
                    Ok(retired) => retired,
                    Err(err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            generation,
                            error = %err,
                            "cancel cleanup: failed to retire pod tracking"
                        );
                        return;
                    }
                };
                if !retired {
                    return;
                }
                match delete_sandbox_pod_if_uid(&k8s, &pod_name, &pod_uid).await {
                    Ok(()) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            "cancel cleanup: sandbox pod delete issued successfully"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            error = %err,
                            "cancel cleanup: failed to delete sandbox pod — pod may be orphaned"
                        );
                    }
                }
            }
            .instrument(cleanup_span),
        );
    }
}

/// Retire the execution a create claimed, fenced on its generation and
/// activation attempt, when the create gives up before sending its pod
/// create. This is the release the pod-create failure arm of
/// `create_execution` runs, from the same database state. Fenced and
/// idempotent: a repeat, or a release racing a newer execution, changes
/// nothing.
async fn release_claimed_execution(
    store: K8sStore,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) {
    if let Err(error) = store
        .forget_pod_tracking_if_generation_and_attempt(
            sandbox_id,
            generation,
            activation_attempt_id,
        )
        .await
    {
        tracing::warn!(
            sandbox_id = %sandbox_id,
            generation,
            error = %error,
            "failed to release the Kubernetes execution claimed by a create that ended before \
             its pod; leaving it to sandbox cleanup"
        );
    }
}

/// Split a requested readiness timeout into the outer backstop budget and
/// the phase-aware inner deadline.
///
/// The inner deadline — which the readiness waits clamp to so their
/// phase-tagged `Timeout` (and their timeout metrics) fire first — keeps the
/// caller's full requested wait whenever it fits inside the channel-safe
/// window (`CREATE_READY_BUDGET − READY_WAIT_PHASE_MARGIN`). The
/// `READY_WAIT_PHASE_MARGIN` is reserved *above* the caller's budget by
/// pushing the outer backstop one margin later, never carved out of the
/// caller's effective wait — a wake with `ready_timeout_secs=30` still gets
/// 30s of readiness waiting. The outer backstop is provably
/// `≤ CREATE_READY_BUDGET`, so the wait always returns a structured response
/// before the node-RPC channel timeout severs the connection.
pub(super) fn ready_wait_bounds(requested: Duration) -> (Duration, Instant) {
    // `READY_WAIT_PHASE_MARGIN < CREATE_READY_BUDGET` is statically
    // asserted next to the constants, so neither bound can underflow or
    // exceed the channel-safe budget.
    SubstrateBudgets::DEFAULT.ready_wait_bounds(requested)
}

/// Time remaining until `phase_deadline`, zero once it has passed. Used to
/// clamp the `ready_timeout` handed to `poll_until_command_ready` when it
/// runs inside a budget-bounded wait.
pub(super) fn remaining_ready_window(phase_deadline: Instant) -> Duration {
    phase_deadline.saturating_duration_since(Instant::now())
}

/// Build the [`SubstrateError::Timeout`] for a command-readiness wait that
/// exhausted its deadline. `phase` attributes the timeout to the step that
/// actually stalled: `PodNeverReady` when no pod IP ever resolved,
/// `ProxyUnreachable` when the sandbox proxy never accepted a TCP
/// connection, `GuestNotReady` when it did but the guest never reported
/// ready. Node-mgr is named only when a readiness probe actually returned
/// gRPC `Unavailable` (`saw_unavailable_probe`) — that suffix is what
/// [`is_nodemgr_unreachable`] keys the brownout classification off.
pub(super) fn command_ready_timeout_error(
    phase: WaitReadyTimeoutPhase,
    saw_unavailable_probe: bool,
) -> SubstrateError {
    let message = if saw_unavailable_probe {
        format!(
            "timed out waiting for sandbox command readiness (phase: {phase}; \
             node-mgr probe returned gRPC Unavailable)"
        )
    } else {
        format!("timed out waiting for sandbox command readiness (phase: {phase})")
    };
    SubstrateError::Timeout {
        message,
        phase: Some(phase),
    }
}

/// Heuristic: does this error look like "node-mgr is unreachable / down"?
/// Used to gate the cascade-amplifier defer-cleanup path. The
/// inner gRPC errors that surface from `wait_for_pod_proxy_reachable` and
/// the pod-proxy worker client get wrapped into `SubstrateError` with the
/// underlying tonic error rendered into `message`, so we string-match on
/// those signatures rather than threading a typed cause through every
/// helper. False negatives here just degrade us to today's behavior
/// (spawn cleanup); false positives leak a pod for the reconciler to GC.
pub(super) fn is_nodemgr_unreachable(err: &SubstrateError) -> bool {
    // The command-ready wait helpers swallow `Code::Unavailable` probe
    // results and exit with `SubstrateError::Timeout` after the deadline.
    // When a probe really returned `Unavailable` the timeout message names
    // node-mgr (see `command_ready_timeout_error`), so we also match
    // Timeout to activate the defer path during the brownout symptoms this
    // code targets. Timeouts whose messages carry only a readiness phase
    // (`pod_never_ready` / `proxy_unreachable` / `guest_not_ready`) do NOT
    // match here — the create path's defer decision treats every `Timeout`
    // as defer-eligible on its own (see `create_execution`'s cleanup arm).
    let message = match err {
        SubstrateError::Unavailable { message } | SubstrateError::Timeout { message, .. } => {
            message
        }
        _ => return false,
    };
    let m = message.to_ascii_lowercase();
    // Specific tonic transport phrases — avoids spurious matches on
    // unrelated words containing the "connect" substring ("reconnect",
    // "interconnect", arbitrary uses of "connection").
    m.contains("unavailable")
        || m.contains("transport")
        || m.contains("connect error")
        || m.contains("connection refused")
}

pub(super) fn assigned_pod_ip(pod: &SandboxPodView) -> Result<Option<PodIp>, SubstrateError> {
    pod.pod_ip().map(PodIp::parse).transpose()
}

pub(super) fn assigned_pod_node_name(pod: &SandboxPodView) -> Option<&str> {
    pod.node_name().filter(|node_name| !node_name.is_empty())
}
