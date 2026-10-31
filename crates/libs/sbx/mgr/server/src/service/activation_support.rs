use super::network_runtime::{network_policy_refresh_error_to_status, refresh_network_policy};
use super::spaced_bridge::cleanup_restored_snapshot_dir_async;
use super::start_support::ActivationCleanupGuard;
use super::*;
use pplx_space_common_core::host::linux::identities::users;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeRole as SpaceNodeRole, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxQuarantineReasonCode, SpaceApiRouteIdentity,
};
use pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER;
use pplx_space_common_svc::types::k8s::space_node_id;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};

const NODE_ID_PATH: &str = "/space/etc/node-id";

#[derive(Clone, Copy, Debug)]
pub(super) enum GuestActivityKind {
    Access,
    Command,
}

#[derive(Clone, Debug)]
pub(super) struct WakeStartConfig {
    pub(super) activation_auto_lifecycle: Option<AutoLifecyclePolicy>,
    pub(super) expected_execution_generation: Option<ExecutionGeneration>,
    pub(super) operation_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(super) vcpu: Option<u32>,
    pub(super) memory_mib: Option<u32>,
    pub(super) network_context: WakeNetworkContext,
    /// Target lifecycle for the wake. Defaults to `SandboxState::Started`
    /// (back-compat for every existing caller). The cluster-controller's
    /// lifecycle-preserving drain evacuation path sets `Paused`: the
    /// restore path skips the post-load resume so the VM stays in the
    /// firecracker-paused state, preserving the user-visible lifecycle.
    /// The same drain sets `Stopped` for a `stopped` resident: the restore
    /// path materializes its disk artifact and stops there, never reaching
    /// Firecracker.
    ///
    /// Only `Started`, `Paused`, or `Stopped` are valid here. Other variants
    /// of `SandboxState` are programmer errors; the proto-to-`SandboxState`
    /// helper [`wake_target_state_from_proto`] enforces this at the wire
    /// boundary. `start_from_checkpoint` refuses `Stopped` and treats any other
    /// unexpected value as `Started`.
    pub(super) target_state: SandboxState,
    pub(super) suspend_restore_storage_uri: Option<String>,
    pub(super) suspend_restore_source_node: Option<String>,
    /// Which artifact `suspend_restore_storage_uri` resolves to, forwarded to
    /// the delegated `Start` so a stopped target can be checked locally.
    pub(super) expected_local_artifact_id: Option<String>,
    /// Route identity exactly as received by Wake, before runtime normalization.
    /// Internal Start must forward this representation so normalization cannot
    /// turn an incomplete producer identity into control-plane fence evidence.
    pub(super) requested_space_api_route_identity: Option<SpaceApiRouteIdentity>,
    pub(super) space_api_route_identity: SpaceApiRouteIdentity,
    pub(super) route_identity_provenance:
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    pub(super) authoritative_route_identity: bool,
    pub(super) provisional_activation_attempt: bool,
    /// The runtime upgrade to wake a stopped or suspended sandbox by, in place
    /// of its start or restore (`WakeRequest.runtime_upgrade`).
    pub(super) runtime_upgrade: Option<super::runtime_upgrade::WakeRuntimeUpgrade>,
}

impl Default for WakeStartConfig {
    fn default() -> Self {
        Self {
            activation_auto_lifecycle: None,
            expected_execution_generation: None,
            operation_activation_attempt_id: None,
            vcpu: None,
            memory_mib: None,
            network_context: WakeNetworkContext::Unspecified,
            target_state: SandboxState::Started,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            expected_local_artifact_id: None,
            requested_space_api_route_identity: None,
            space_api_route_identity: SpaceApiRouteIdentity::default(),
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            authoritative_route_identity: false,
            provisional_activation_attempt: false,
            runtime_upgrade: None,
        }
    }
}

/// Only a complete control-plane Kubernetes route may carry a separate lease.
/// The node dimension is supplied by the runtime, not the incoming request.
pub(super) fn operation_activation_attempt_id_from_wire(
    value: Option<&str>,
    route: &SpaceApiRouteIdentity,
) -> Result<Option<SandboxActivationAttemptId>, Status> {
    let Some(value) = value else { return Ok(None) };
    if route.effective_substrate_kind()
        != Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes)
        || route.route_generation.is_none()
        || route.execution_generation.is_none()
        || route.pod_uid.is_none()
        || route.activation_attempt_id.is_none()
    {
        return Err(Status::invalid_argument(
            "operation_activation_attempt_id requires a Kubernetes route identity",
        ));
    }
    value.parse().map(Some).map_err(|error| {
        Status::invalid_argument(format!("invalid operation_activation_attempt_id: {error}"))
    })
}

pub(super) fn space_api_route_identity_from_create_request(
    req: &CreateRequest,
) -> Result<SpaceApiRouteIdentity, Status> {
    space_api_route_identity_from_wire(req.route_identity.as_ref())
}

pub(super) fn space_api_route_identity_from_start_request(
    req: &StartRequest,
) -> Result<SpaceApiRouteIdentity, Status> {
    space_api_route_identity_from_wire(req.route_identity.as_ref())
}

pub(super) fn space_api_route_identity_from_wake_start_context(
    start: &crate::proto::WakeStartContext,
) -> Result<SpaceApiRouteIdentity, Status> {
    space_api_route_identity_from_wire(start.route_identity.as_ref())
}

pub(super) fn space_api_route_identity_to_wire(
    identity: &SpaceApiRouteIdentity,
) -> Option<crate::proto::SpaceApiRouteIdentity> {
    (!identity.is_absent()).then(|| crate::proto::SpaceApiRouteIdentity {
        sandbox_id: identity.sandbox_id.map(|id| id.to_string()),
        route_generation: identity.route_generation.map(|generation| generation.get()),
        current_node_id: identity.current_node_id.map(|node_id| node_id.to_string()),
        execution_generation: identity
            .execution_generation
            .map(|generation| generation.get()),
        pod_uid: identity.pod_uid.as_ref().map(|pod_uid| pod_uid.to_string()),
        activation_attempt_id: identity.activation_attempt_id.map(|id| id.to_string()),
        substrate_kind: match identity.substrate_kind {
            None => crate::proto::SandboxSubstrateKind::Unspecified as i32,
            Some(pplx_space_common_core::types::NodeManagedBy::Space) => {
                crate::proto::SandboxSubstrateKind::Direct as i32
            }
            Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes) => {
                crate::proto::SandboxSubstrateKind::Kubernetes as i32
            }
        },
    })
}

#[derive(Debug)]
pub(super) struct RuntimeActivationRouteIdentity {
    pub(super) identity: SpaceApiRouteIdentity,
    pub(super) provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    pub(super) open_activation_claim_eligible: bool,
    /// True only when normalization had to invent the attempt. Wake carries
    /// this bit separately from the identity so an already-satisfied path can
    /// reject an unadopted placeholder instead of mistaking it for a real
    /// control-plane attempt.
    pub(super) provisional_activation_attempt: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum IdentityFreeDirectActivationAuthority {
    #[default]
    Denied,
    LocalProducer,
}

impl IdentityFreeDirectActivationAuthority {
    fn allows_mint(self) -> bool {
        self == Self::LocalProducer
    }
}

pub(super) fn space_api_route_identity_for_runtime_activation(
    mut identity: SpaceApiRouteIdentity,
) -> RuntimeActivationRouteIdentity {
    // Classify the wire identity before normalization. The fields synthesized
    // below make a runtime identity publishable, but a locally minted attempt
    // is not evidence of a control-plane-fenced activation.
    let provenance =
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::for_activation_identity(
            &identity,
        );
    let open_activation_claim_eligible = runtime_activation_wire_identity_can_open_claim(&identity);
    if identity.is_absent() {
        return RuntimeActivationRouteIdentity {
            identity,
            provenance,
            open_activation_claim_eligible: false,
            provisional_activation_attempt: false,
        };
    }
    if identity.route_generation.is_none() {
        identity.route_generation = identity
            .execution_generation
            .and_then(|generation| u64::try_from(generation.get()).ok())
            .and_then(|generation| RouteGeneration::new(generation).ok());
    }
    if identity.execution_generation.is_none() {
        identity.execution_generation = identity
            .route_generation
            .and_then(|generation| i64::try_from(generation.get()).ok())
            .and_then(|generation| ExecutionGeneration::new(generation).ok());
    }
    if identity.current_node_id.is_none() {
        identity.current_node_id = local_space_node_id();
    }
    if identity.substrate_kind.is_none() && identity.execution_generation.is_some() {
        identity.substrate_kind = identity
            .effective_substrate_kind()
            .or(Some(pplx_space_common_core::types::NodeManagedBy::Space));
    }
    // A runtime activation must publish a complete route identity, so an
    // activation that arrives without an attempt mints a placeholder. On the
    // direct substrate that placeholder is final — the control plane stores
    // whatever the activation returns as the route receipt. On Kubernetes the
    // control plane is the mint authority and signs route tokens against its
    // own value, so the placeholder only holds until the activation RPC
    // supplies the real one (see `adopt_authoritative_route_identity`).
    let provisional_activation_attempt =
        identity.activation_attempt_id.is_none() && identity.execution_generation.is_some();
    if provisional_activation_attempt {
        identity.activation_attempt_id = Some(SandboxActivationAttemptId::new());
    }
    RuntimeActivationRouteIdentity {
        identity,
        provenance,
        open_activation_claim_eligible,
        provisional_activation_attempt,
    }
}

fn direct_route_identity(
    generation: u64,
    current_node_id: Option<NodeId>,
) -> Result<RuntimeActivationRouteIdentity, Status> {
    let route_generation = RouteGeneration::new(generation)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let execution_generation = i64::try_from(generation)
        .ok()
        .and_then(|value| ExecutionGeneration::new(value).ok())
        .ok_or_else(|| Status::failed_precondition("local Direct generation exhausted"))?;
    let current_node_id = current_node_id
        .or_else(local_space_node_id)
        .ok_or_else(|| {
            Status::failed_precondition(
                "local Direct activation requires SPACE_NODE_ID or a populated /space/etc/node-id",
            )
        })?;
    Ok(RuntimeActivationRouteIdentity {
        identity: SpaceApiRouteIdentity {
            route_generation: Some(route_generation),
            current_node_id: Some(current_node_id),
            substrate_kind: Some(pplx_space_common_core::types::NodeManagedBy::Space),
            execution_generation: Some(execution_generation),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..Default::default()
        },
        provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        open_activation_claim_eligible: false,
        provisional_activation_attempt: false,
    })
}

fn resolved_requested_runtime_activation_identity(
    requested: SpaceApiRouteIdentity,
    identity_free_pool_donor: bool,
    local_node_id: Option<NodeId>,
) -> Option<RuntimeActivationRouteIdentity> {
    if !requested.is_absent() {
        let requested_node_id_was_absent = requested.current_node_id.is_none();
        let mut resolved = space_api_route_identity_for_runtime_activation(requested);
        if requested_node_id_was_absent {
            resolved.identity.current_node_id = local_node_id.or_else(local_space_node_id);
        }
        return Some(resolved);
    }
    identity_free_pool_donor.then_some(RuntimeActivationRouteIdentity {
        identity: requested,
        provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        open_activation_claim_eligible: false,
        provisional_activation_attempt: false,
    })
}

/// Whether the wire identity supplies every control-plane-owned dimension
/// needed to open a Kubernetes activation claim.
///
/// The target node is runtime-owned: sandbox-manager resolves and validates
/// its local node ID after this check. The remaining fields must arrive on the
/// request so normalization cannot promote an incomplete producer identity
/// into a durable continuation claim.
pub(super) fn runtime_activation_wire_identity_can_open_claim(
    identity: &SpaceApiRouteIdentity,
) -> bool {
    identity.route_generation.is_some()
        && identity.execution_generation.is_some()
        && identity.activation_attempt_id.is_some()
        && identity.substrate_kind == Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes)
        && identity.pod_uid.is_some()
}

pub(super) fn fresh_runtime_activation_identity(
    requested: SpaceApiRouteIdentity,
    identity_free_pool_donor: bool,
    local_node_id: Option<NodeId>,
) -> Result<RuntimeActivationRouteIdentity, Status> {
    if let Some(identity) = resolved_requested_runtime_activation_identity(
        requested,
        identity_free_pool_donor,
        local_node_id,
    ) {
        return Ok(identity);
    }
    direct_route_identity(1, local_node_id)
}

/// The open-claim identity a cluster-fenced activation records, or `None`
/// when the activation isn't eligible to hold one.
pub(super) fn open_route_activation_claim(
    space_api_route_identity: &pplx_space_common_core::types::SpaceApiRouteIdentity,
    route_identity_provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    open_activation_claim_eligible: bool,
) -> Option<crate::db::route_activation_claims::OpenRouteActivationClaim> {
    open_activation_claim_eligible
        .then(|| {
            crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
                space_api_route_identity,
                route_identity_provenance,
            )
        })
        .flatten()
}

impl SandboxManagerService {
    /// Refuse to start the runtime of an activation this node already began
    /// for `claim` once the control plane has retired its execution. Kubelet
    /// retries a pod whose first launch failed with the same route identity,
    /// which still matches the claim the failure kept open, so only the
    /// control plane can tell that a newer execution replaced it; started
    /// anyway, the runtime is an orphan.
    /// A first activation isn't asked: its execution was current when the
    /// control plane made its pod.
    ///
    /// A retry of an activation this node rolled back (the row still carries
    /// the rollback receipt for the claim's generation) starts only when the
    /// control plane answers that the execution is current. The receipt is
    /// published with the row, and the control plane retires the execution
    /// when it applies it, so the retry is a legitimate continuation only
    /// while that hasn't happened. Without an answer the node can't tell, and
    /// starting would bring up a runtime for an execution that may already be
    /// retired, in a pod nothing routes to. No answer includes a control plane
    /// that predates the question (during a rollout that upgrades nodes
    /// first), an unreachable or misconfigured one, and one that can't place
    /// the execution on this node; such a retry keeps failing until the
    /// answer changes or a new activation replaces it. The answer is a
    /// point-in-time one: a publication already carrying the receipt can still
    /// retire the execution after it. Whether the backup fence then refuses
    /// the orphan's capture depends on how the control plane retired the
    /// execution.
    ///
    /// The last ask before an activation claims the row runs under the
    /// activation lock, so a rollback cleanup can't write the receipt between
    /// it and the activation; an in-place restore also asks earlier, before
    /// staging, so a retry already refused stages nothing. The last ask holds
    /// the lock for the question, up to its five-second timeout when the
    /// control plane doesn't answer, or until the caller disconnects, which
    /// abandons the question. A cleanup of this sandbox that waits for
    /// the lock waits that long, and one that only tries it gives up; another
    /// activation of it, which waits five seconds for admission, may be
    /// refused as busy.
    pub(in crate::service) async fn refuse_retired_activation_retry(
        &self,
        sandbox_id: &str,
        claim: Option<&crate::db::route_activation_claims::OpenRouteActivationClaim>,
    ) -> Result<(), Status> {
        use pplx_space_node_api_client::proto::ExecutionCurrency;
        let Some(claim) = claim else {
            return Ok(());
        };
        let rolled_back = {
            let db = self.db_conn()?;
            let view = crate::db::read::sandbox_view(&db, sandbox_id);
            let route_claim = view.route_claim();
            let db_error = |error| Status::internal(format!("db error: {error}"));
            if !route_claim.matches(claim).map_err(db_error)? {
                return Ok(());
            }
            route_claim.rolled_back(claim).map_err(db_error)?
        };
        let generation = claim.execution_generation();
        // Boxed: the question's gRPC connect and call make a large future, and
        // inline it would sit in every activation path's future, which an
        // unoptimized build already runs close to a test thread's stack.
        let currency = Box::pin(super::restart_admission::observe_unless_caller_cancelled(
            sandbox_id,
            crate::route_activation_claims::execution_currency_on_cluster(sandbox_id, generation),
        ))
        .await?;
        match currency {
            ExecutionCurrency::Retired => {
                // The gRPC layer logs a `FailedPrecondition` reply at debug, so
                // without this line a pod kubelet keeps retrying leaves no trace
                // in the logs. The pod only stops once something deletes it.
                tracing::warn!(
                    sandbox_id,
                    %generation,
                    "refused a retried activation of a retired execution; its pod must be deleted"
                );
                Err(Status::failed_precondition(format!(
                    "execution generation {generation} of sandbox {sandbox_id} was retired; \
                     its runtime must not start"
                )))
            }
            ExecutionCurrency::Unspecified if rolled_back => {
                tracing::warn!(
                    sandbox_id,
                    %generation,
                    "refused a retried activation this node rolled back, which the control \
                     plane didn't confirm is current"
                );
                Err(Status::failed_precondition(format!(
                    "execution generation {generation} of sandbox {sandbox_id} was rolled back on \
                     this node and the control plane didn't confirm it is current; its runtime \
                     must not start"
                )))
            }
            ExecutionCurrency::Current | ExecutionCurrency::Unspecified => Ok(()),
        }
    }

    pub(super) fn resolve_runtime_activation_identity(
        &self,
        sandbox_id: &str,
        requested: SpaceApiRouteIdentity,
        identity_free_pool_donor: bool,
    ) -> Result<RuntimeActivationRouteIdentity, Status> {
        self.resolve_runtime_activation_identity_with_authority(
            sandbox_id,
            requested,
            identity_free_pool_donor,
            IdentityFreeDirectActivationAuthority::Denied,
        )
    }

    pub(super) fn resolve_runtime_activation_identity_with_authority(
        &self,
        sandbox_id: &str,
        requested: SpaceApiRouteIdentity,
        identity_free_pool_donor: bool,
        identity_free_direct_authority: IdentityFreeDirectActivationAuthority,
    ) -> Result<RuntimeActivationRouteIdentity, Status> {
        validate_route_identity_sandbox(&requested, sandbox_id)?;
        // Older callers omit the optional binding; the resolved RPC target
        // remains authoritative while incarnation dimensions fence admission.
        let requested_can_open_claim = runtime_activation_wire_identity_can_open_claim(&requested);
        if let Some(identity) = resolved_requested_runtime_activation_identity(
            requested,
            identity_free_pool_donor,
            self.local_node_id,
        ) {
            let generation_fence_enabled =
                pplx_space_common_svc::space_api_route::space_api_route_generation_fence_enabled();
            if let Some(requested_node_id) = identity.identity.current_node_id {
                let local_node_id = self.local_node_id.or_else(local_space_node_id).ok_or_else(|| {
                    Status::failed_precondition(
                        "runtime activation identity validation requires SPACE_NODE_ID or a populated /space/etc/node-id",
                    )
                })?;
                if requested_node_id != local_node_id {
                    return Err(Status::failed_precondition(format!(
                        "runtime activation identity targets node {requested_node_id}, but this sandbox-manager is node {local_node_id}",
                    )));
                }
            }
            let provenance = {
                let db = self.db_conn()?;
                match crate::db::read::sandbox_view(&db, sandbox_id).activation_route_provenance() {
                    Ok(provenance) => {
                        if requested_can_open_claim
                            && let Some(claim) = crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
                                &identity.identity,
                                identity.provenance,
                            )
                        {
                            match crate::route_activation_claims::validate_cluster_fenced_activation(
                                &db,
                                sandbox_id,
                                &claim,
                            ) {
                                Ok(_) => return Ok(identity),
                                Err(
                                    crate::route_activation_claims::OpenRouteActivationAdmissionError::StaleOrConflicting {
                                        retained,
                                    },
                                ) => {
                                    if !generation_fence_enabled {
                                        tracing::warn!(
                                            sandbox_id,
                                            retained,
                                            "runtime activation generation fence disabled; accepting stale-or-conflicting cluster-fenced activation"
                                        );
                                        return Ok(identity);
                                    }
                                    return Err(Status::aborted(format!(
                                        "runtime activation identity generation is not newer than retained generation {retained}"
                                    )));
                                }
                                Err(
                                    crate::route_activation_claims::OpenRouteActivationAdmissionError::Database(error),
                                ) => {
                                    return Err(Status::internal(format!("db error: {error}")));
                                }
                            }
                        }
                        Some(provenance)
                    }
                    Err(error) if error.is_no_rows() => None,
                    Err(error) => {
                        return Err(Status::internal(format!("db error: {error}")));
                    }
                }
            };
            if let Some(provenance) = provenance {
                let retained_generation = [
                    provenance
                        .retained_route_generation
                        .map(RouteGeneration::get),
                    provenance
                        .retained_execution_generation
                        .and_then(|value| u64::try_from(value.get()).ok()),
                ]
                .into_iter()
                .flatten()
                .max();
                let requested_generation = [
                    identity.identity.route_generation.map(RouteGeneration::get),
                    identity
                        .identity
                        .execution_generation
                        .and_then(|value| u64::try_from(value.get()).ok()),
                ]
                .into_iter()
                .flatten()
                .min();
                if let (Some(retained), Some(requested)) =
                    (retained_generation, requested_generation)
                    && requested <= retained
                {
                    if !generation_fence_enabled {
                        tracing::warn!(
                            sandbox_id,
                            retained,
                            requested,
                            "runtime activation generation fence disabled; accepting non-monotonic activation identity"
                        );
                    } else {
                        return Err(Status::aborted(format!(
                            "runtime activation identity generation is not newer than retained generation {retained}"
                        )));
                    }
                }
            }
            return Ok(identity);
        }
        let provenance = {
            let db = self.db_conn()?;
            match crate::db::read::sandbox_view(&db, sandbox_id).activation_route_provenance() {
                Ok(provenance) => provenance,
                Err(error) if error.is_no_rows() => {
                    return Err(
                        pplx_space_sbx_mgr_client::authoritative_route_identity_required_status(
                            "identity-free activation has no local route provenance; supply an authoritative route identity",
                        ),
                    );
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
        };
        if !identity_free_direct_authority.allows_mint() {
            return Err(
                pplx_space_sbx_mgr_client::authoritative_route_identity_required_status(
                    "identity-free activation requires an authoritative route identity",
                ),
            );
        }
        let substrate_kind = provenance
            .current_substrate_kind
            .or(provenance.retained_substrate_kind);
        if substrate_kind == Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes) {
            return Err(
                pplx_space_sbx_mgr_client::authoritative_route_identity_required_status(
                    "Kubernetes-managed sandbox activation requires an authoritative route identity",
                ),
            );
        }
        let generation = [
            provenance
                .current_route_generation
                .map(RouteGeneration::get),
            provenance
                .current_execution_generation
                .and_then(|value| u64::try_from(value.get()).ok()),
            provenance
                .retained_route_generation
                .map(RouteGeneration::get),
            provenance
                .retained_execution_generation
                .and_then(|value| u64::try_from(value.get()).ok()),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Status::failed_precondition("local Direct generation exhausted"))?;
        direct_route_identity(generation, self.local_node_id)
    }
}

/// Adopt only the route dimensions minted by the control plane.
///
/// Sandbox, node, substrate, and pod identity describe the local runtime and
/// remain runtime-owned even when the activation request carries copies of
/// them. The request can lag during evacuation, so treating those copies as
/// authoritative would let the runtime publish an identity for another pod.
pub(super) fn merge_authoritative_route_identity(
    current: &SpaceApiRouteIdentity,
    incoming: &SpaceApiRouteIdentity,
) -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        sandbox_id: current.sandbox_id,
        route_generation: incoming.route_generation.or(current.route_generation),
        current_node_id: current.current_node_id,
        substrate_kind: current.substrate_kind,
        execution_generation: incoming
            .execution_generation
            .or(current.execution_generation),
        pod_uid: current.pod_uid.clone(),
        activation_attempt_id: incoming
            .activation_attempt_id
            .or(current.activation_attempt_id),
    }
}

pub(super) fn space_api_route_identity_from_wire(
    identity: Option<&crate::proto::SpaceApiRouteIdentity>,
) -> Result<SpaceApiRouteIdentity, Status> {
    let Some(identity) = identity else {
        return Ok(SpaceApiRouteIdentity::default());
    };
    let route_generation = identity
        .route_generation
        .map(RouteGeneration::new)
        .transpose()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(SpaceApiRouteIdentity {
        sandbox_id: identity
            .sandbox_id
            .as_deref()
            .map(str::parse::<SandboxId>)
            .transpose()
            .map_err(|error| {
                Status::invalid_argument(format!("invalid route identity sandbox_id: {error}"))
            })?,
        route_generation,
        current_node_id: parse_optional_node_id(
            "current_node_id",
            identity.current_node_id.as_deref(),
        )?,
        substrate_kind: match crate::proto::SandboxSubstrateKind::try_from(identity.substrate_kind)
            .map_err(|_| Status::invalid_argument("substrate_kind is invalid"))?
        {
            crate::proto::SandboxSubstrateKind::Unspecified => None,
            crate::proto::SandboxSubstrateKind::Direct => {
                Some(pplx_space_common_core::types::NodeManagedBy::Space)
            }
            crate::proto::SandboxSubstrateKind::Kubernetes => {
                Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes)
            }
        },
        execution_generation: identity
            .execution_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
        pod_uid: parse_optional_pod_uid("pod_uid", identity.pod_uid.as_deref())?,
        activation_attempt_id: identity
            .activation_attempt_id
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|error: uuid::Error| Status::invalid_argument(error.to_string()))?,
    })
}

pub(super) fn validate_route_identity_sandbox(
    identity: &SpaceApiRouteIdentity,
    resolved_sandbox_id: &str,
) -> Result<(), Status> {
    if let Some(bound_id) = identity.sandbox_id {
        let resolved = resolved_sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        if bound_id != resolved {
            return Err(Status::invalid_argument(
                "route identity sandbox_id does not match the resolved sandbox",
            ));
        }
    }
    Ok(())
}

fn parse_optional_node_id(
    name: &'static str,
    value: Option<&str>,
) -> Result<Option<NodeId>, Status> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|error| Status::invalid_argument(format!("{name} is invalid: {error}")))
}

fn parse_optional_pod_uid(
    name: &'static str,
    value: Option<&str>,
) -> Result<Option<PodUid>, Status> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|error| Status::invalid_argument(format!("{name} is invalid: {error}")))
}

/// Where this node's identity is configured: `SPACE_NODE_ID` when the
/// variable is set, else the node-id file that node-api reads or generates.
/// Direct nodes rely on the file; Kubernetes node services set the variable
/// to the kube node name. A set but blank variable names no node: unlike
/// node-api, this does not fall back to the file.
#[derive(Clone, Debug)]
pub(crate) struct NodeIdentitySource {
    env_value: Option<String>,
    node_id_path: std::path::PathBuf,
}

impl NodeIdentitySource {
    pub(crate) fn from_process() -> Self {
        Self {
            env_value: std::env::var("SPACE_NODE_ID").ok(),
            node_id_path: NODE_ID_PATH.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn node_id_file(node_id_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            env_value: None,
            node_id_path: node_id_path.into(),
        }
    }

    /// The configured value as written, trimmed: a node UUID, or a kube node
    /// name that [`local_space_node_id`] maps to one.
    pub(crate) fn configured_node_id(&self) -> Option<String> {
        let value = self
            .env_value
            .clone()
            .or_else(|| std::fs::read_to_string(&self.node_id_path).ok())?;
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    }
}

pub(super) fn local_space_node_id() -> Option<NodeId> {
    let value = NodeIdentitySource::from_process().configured_node_id()?;
    let role = std::env::var("SPACE_NODE_ROLE")
        .ok()
        .and_then(|role| role.parse::<SpaceNodeRole>().ok())
        .filter(|role| matches!(role, SpaceNodeRole::Worker | SpaceNodeRole::Builder))
        .unwrap_or(SpaceNodeRole::Worker);
    space_node_id(&value, role).ok()
}

#[derive(Clone, Debug)]
pub(super) enum WakeNetworkContext {
    // Public/operator wakes and direct-substrate flows keep the legacy
    // sbx-manager-owned namespace behavior. Kubernetes launch paths carry an
    // explicit start_context and therefore use one of the variants below.
    Unspecified,
    RuntimePath(String),
    MissingRuntimePath,
}

impl WakeNetworkContext {
    pub(super) fn for_wake_start_context(start: &crate::proto::WakeStartContext) -> Self {
        // Direct activations use a manager-owned namespace even when the
        // activation carries resources or a restore source.
        let direct_route_identity = start.route_identity.as_ref().is_some_and(|identity| {
            identity.route_generation.is_some()
                && identity.current_node_id.is_some()
                && identity.pod_uid.is_none()
                && matches!(
                    crate::proto::SandboxSubstrateKind::try_from(identity.substrate_kind),
                    Ok(crate::proto::SandboxSubstrateKind::Unspecified)
                        | Ok(crate::proto::SandboxSubstrateKind::Direct)
                )
        });
        // An identity-free activation is a direct one: Kubernetes launches
        // always carry their pod's route identity. It keeps the manager-owned
        // namespace, and route-identity resolution, not a missing namespace,
        // decides whether it may proceed.
        let identity_free = start.route_identity.is_none();
        match start.netns_path.as_deref().filter(|path| !path.is_empty()) {
            Some(path) => Self::RuntimePath(path.to_string()),
            None if direct_route_identity || identity_free => Self::Unspecified,
            None => Self::MissingRuntimePath,
        }
    }

    pub(super) fn netns_path_for_launch(&self) -> Result<Option<String>, Status> {
        match self {
            Self::Unspecified => Ok(None),
            Self::RuntimePath(path) => Ok(Some(path.clone())),
            Self::MissingRuntimePath => {
                Err(Status::failed_precondition(MISSING_RUNTIME_CONTEXT_MARKER))
            }
        }
    }
}

/// Map the wire-level `WakeTargetState` proto enum into the
/// `SandboxState` variant the wake/restore path should converge on.
/// `UNSPECIFIED` and `STARTED` both resolve to `SandboxState::Started`
/// (back-compat with the historical wake-to-started contract); `PAUSED`
/// resolves to `SandboxState::Paused`; `STOPPED` resolves to
/// `SandboxState::Stopped`, the evacuated-stopped disk materialization.
/// Unknown values are rejected before a mutation can run. This is the
/// single boundary where the proto i32 leaves typed code; downstream Rust
/// passes `SandboxState` around without conversions.
pub(super) fn wake_target_state_from_proto(value: i32) -> Result<SandboxState, Status> {
    use pplx_space_sbx_mgr_client::proto::WakeTargetState;
    let target = WakeTargetState::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown target_state value {value}")))?;
    Ok(match target {
        WakeTargetState::Unspecified | WakeTargetState::Started => SandboxState::Started,
        WakeTargetState::Paused => SandboxState::Paused,
        WakeTargetState::Stopped => SandboxState::Stopped,
    })
}

/// Inverse of [`wake_target_state_from_proto`]: encode a `SandboxState`
/// value back to the proto `WakeTargetState` i32 for `StartRequest`
/// re-emission inside sbx-mgr (the wake handler re-issues
/// `start_sandbox` with the same target). Only `Started`, `Paused`, and
/// `Stopped` are meaningful here; any other `SandboxState` is a
/// programmer error and falls through to `STARTED`.
pub(super) fn wake_target_state_to_proto(state: SandboxState) -> i32 {
    use pplx_space_sbx_mgr_client::proto::WakeTargetState;
    match state {
        SandboxState::Paused => WakeTargetState::Paused as i32,
        SandboxState::Stopped => WakeTargetState::Stopped as i32,
        // SandboxState::Started is the canonical "wake to started"; all
        // other variants are unreachable on the wake-target path but
        // resolve to Started defensively.
        _ => WakeTargetState::Started as i32,
    }
}

impl GuestActivityKind {
    pub(super) fn allows_implicit_wake(self, policy: &AutoLifecyclePolicy) -> bool {
        match self {
            Self::Access => policy.wake_on_access(),
            Self::Command => policy.wake_on_access() || policy.wake_on_command(),
        }
    }
}

pub(super) fn grpc_timeout_budget<T>(request: &Request<T>) -> Option<Duration> {
    request
        .metadata()
        .get(GRPC_TIMEOUT_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout)
}

pub(super) fn sandbox_peer_authorizer() -> UdsPeerAuthorizer {
    UdsPeerAuthorizer::new_with_default_local_roles(
        "space-sandbox-manager",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_NODEAPI),
            UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR),
            UdsPeerAccount::with_linux_user(users::SPACE_NODECTL),
            UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
        ],
    )
}

pub(super) fn internal_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, None));
    request
}

pub(super) fn auto_lifecycle_request<T>(message: T) -> Request<T> {
    let mut request = internal_request(message);
    request
        .extensions_mut()
        .insert(pplx_space_events::SandboxEventTrigger::AutoLifecycle);
    request
}

pub(super) fn sandbox_event_trigger<T>(
    request: &Request<T>,
) -> Option<pplx_space_events::SandboxEventTrigger> {
    request
        .extensions()
        .get::<pplx_space_events::SandboxEventTrigger>()
        .copied()
}

pub(super) fn remaining_wake_budget(
    deadline: Option<tokio::time::Instant>,
    state: SandboxState,
) -> Result<Option<Duration>, Status> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return Err(Status::deadline_exceeded(format!(
            "sandbox did not become active in time; current state: {state}"
        )));
    }
    Ok(Some(deadline.duration_since(now)))
}

pub(super) fn wake_ready_timeout_secs(
    deadline: Option<tokio::time::Instant>,
    default_secs: u32,
    state: SandboxState,
) -> Result<u32, Status> {
    let Some(remaining) = remaining_wake_budget(deadline, state)? else {
        return Ok(default_secs);
    };
    let rounded_secs = remaining
        .as_secs()
        .saturating_add(if remaining.subsec_nanos() > 0 { 1 } else { 0 });
    Ok(u32::try_from(rounded_secs).unwrap_or(u32::MAX).max(1))
}

pub(super) fn spaced_host_socket_path(sandbox_id: &str) -> String {
    format!(
        "{RUN_DIR}/{sandbox_id}/{}",
        pplx_space_common_core::host::linux::paths::SPACED_HOST_SOCKET_FILE
    )
}

/// Bound connection setup and all client retries by the caller's readiness
/// budget. Preserve terminal compatibility errors for lifecycle callers.
pub(super) async fn wait_ready_via_spaced_host(
    sandbox_id: &str,
    timeout: Duration,
    reason: &'static str,
) -> Result<pplx_space_spaced_host_client::proto::WaitReadyResponse, Status> {
    use pplx_space_metrics::gateway::readiness::Outcome;

    let mut observation = pplx_space_metrics::sandbox::worker::spaced_guest_readiness_wait(reason);
    let result = tokio::time::timeout(timeout, async {
        let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
            .with_socket_path(spaced_host_socket_path(sandbox_id));
        let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let ready = client.wait_ready(timeout).await?;
        validate_ready_guest_identity(ready.guest_version.as_ref(), || client.get_status()).await?;
        Ok(ready)
    })
    .await
    .unwrap_or_else(|_| {
        Err(Status::deadline_exceeded(
            "guest readiness deadline exceeded",
        ))
    });
    observation.outcome = match &result {
        Ok(_) => Outcome::Ready,
        Err(error) if error.code() == tonic::Code::DeadlineExceeded => Outcome::TimedOut,
        Err(_) => Outcome::Failed,
    };
    result
}

async fn validate_ready_guest_identity<F>(
    guest: Option<&pplx_space_spaced_host_client::proto::VersionInfo>,
    legacy_status: impl FnOnce() -> F,
) -> Result<(), Status>
where
    F: std::future::Future<
            Output = Result<pplx_space_spaced_host_client::proto::GetStatusResponse, Status>,
        >,
{
    // Validate the successful readiness observation. A second, shorter status
    // probe can reject a responsive guest merely because its response is slower.
    // Sidecars can outlive manager upgrades; retain the identity check when an
    // older sidecar omits it from WaitReady.
    let status;
    let guest = match guest {
        Some(guest) => Some(guest),
        None => {
            // Keep the legacy RPC's large future out of the nested activation
            // future. Current sidecars neither construct nor allocate it.
            status = Box::pin(legacy_status()).await?;
            if !status.ready {
                return Err(Status::unavailable(
                    "guest readiness changed while checking compatibility",
                ));
            }
            status.guest_version.as_ref()
        }
    };
    pplx_space_spaced_common::validate_spaced_guest_compatibility(
        guest.map(|guest| guest.version.as_str()),
        guest.map(|guest| guest.compatibility_family.as_str()),
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))
}

pub(super) async fn configure_process_env_via_spaced_host(
    sandbox_id: &str,
    process_env: &BTreeMap<String, String>,
) -> Result<(), Status> {
    if process_env.is_empty() {
        return Ok(());
    }
    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
        .with_socket_path(spaced_host_socket_path(sandbox_id));
    let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    client
        .configure_process_env(process_env.clone().into_iter().collect())
        .await
}

const ACTIVATION_ENV_TIMEOUT: Duration = Duration::from_secs(5);
const ACTIVATION_ENV_ATTEMPTS: usize = 8;

async fn retry_activation_env<F, Fut>(
    sandbox_id: &str,
    phase: &str,
    mut attempt: F,
) -> Result<(), Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), Status>>,
{
    let deadline = tokio::time::Instant::now() + ACTIVATION_ENV_TIMEOUT;
    let mut delay = Duration::from_millis(100);
    for index in 0..ACTIVATION_ENV_ATTEMPTS {
        let result = tokio::time::timeout_at(deadline, attempt())
            .await
            .unwrap_or_else(|_| {
                Err(Status::deadline_exceeded(
                    "activation environment deadline exceeded",
                ))
            });
        pplx_space_metrics::sandbox::worker::activation_env_attempt(
            phase,
            index == 0,
            result.is_ok(),
        );
        match result {
            Err(status)
                if index + 1 < ACTIVATION_ENV_ATTEMPTS
                    && matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Cancelled
                    ) =>
            {
                tokio::time::timeout_at(deadline, tokio::time::sleep(delay))
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded("activation environment deadline exceeded")
                    })?;
                delay = (delay * 2).min(Duration::from_millis(500));
            }
            Ok(()) => {
                if index > 0 {
                    tracing::info!(
                        sandbox_id,
                        phase,
                        attempts = index + 1,
                        "activation environment configured after transient failure"
                    );
                }
                return Ok(());
            }
            result => return result,
        }
    }
    unreachable!("the last attempt always returns")
}

fn activation_env_failure_status(phase: &str, status: Status) -> Status {
    // Supersession and disappearance are lifecycle fences, not failed guest
    // setup. Keep their classification while restoring setup diagnostics for
    // callers and cleanup receipts, which persist only the status message.
    if matches!(status.code(), tonic::Code::Aborted | tonic::Code::NotFound) {
        return status;
    }
    Status::internal(format!(
        "guest activation reached {phase}, but process env injection failed: {status}"
    ))
}

fn activation_env_matches(
    initial: &crate::db::SandboxRow,
    current: &crate::db::SandboxRow,
    process_env: &BTreeMap<String, String>,
) -> bool {
    matches!(
        current.state,
        SandboxState::Starting | SandboxState::Restoring | SandboxState::Resuming
    ) && current.state == initial.state
        && current.node_execution_id == initial.node_execution_id
        && current.space_api_route_identity == initial.space_api_route_identity
        && current.process_env == *process_env
}

pub(super) async fn configure_process_env_for_activation(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    process_env: &BTreeMap<String, String>,
    phase: &'static str,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<(), Status> {
    if process_env.is_empty() {
        return Ok(());
    }
    let read = || {
        let conn = crate::node_state_sync::connection_or_signal_failure(db)
            .map_err(|error| Status::internal(error.to_string()))?;
        let sandbox = crate::db::read::sandbox(&conn, sandbox_id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?
            .ok_or_else(|| Status::not_found("activation sandbox disappeared"))?;
        let episode = crate::db::read::sandbox_view(&conn, sandbox_id)
            .cleanup_episode()
            .strict()
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        // Start and restore pre-arm rollback before guest setup. Its presence
        // is valid until cleanup is requested or another activation owns it.
        // Resident resume has no cleanup episode.
        let owns_cleanup = match (cleanup_id, episode) {
            (Some(expected), Some(episode)) => {
                episode.id == expected && episode.requested_at.is_none()
            }
            (None, None) => sandbox.cleanup_completion.is_none(),
            _ => false,
        };
        if !owns_cleanup {
            return Err(Status::aborted(
                "activation cleanup ownership changed before environment configuration",
            ));
        }
        Ok(sandbox)
    };
    let initial = read()?;
    // Activation owns the wake lock and rejects live metadata replacement while
    // Starting/Restoring/Resuming. Replaying this fixed map is safe even if a
    // guest applied it before losing the reply. Do not generalize this to the
    // ordinary Update RPC, where a newer environment could supersede it.
    let result = retry_activation_env(sandbox_id, phase, || async {
        let current = read()?;
        if !activation_env_matches(&initial, &current, process_env) {
            return Err(Status::aborted(
                "activation changed before environment configuration",
            ));
        }
        // Each attempt establishes a new host connection; the host invalidates
        // failed guest channel generations before returning a transport error.
        configure_process_env_via_spaced_host(sandbox_id, process_env).await
    })
    .await
    .map_err(|status| activation_env_failure_status(phase, status));
    if let Err(error) = &result {
        tracing::warn!(sandbox_id, phase, %error, "failed to configure sandbox process env after guest activation");
    }
    result
}

pub(super) fn wait_ready_guest_uptime_ms(
    response: &pplx_space_spaced_host_client::proto::WaitReadyResponse,
) -> i64 {
    response
        .guest_uptime
        .map(|duration| {
            duration.seconds.saturating_mul(1000) + i64::from(duration.nanos) / 1_000_000
        })
        .unwrap_or_default()
}

pub(super) fn wake_transition_for_state(state: SandboxState) -> Result<WakeTransition, Status> {
    match state {
        SandboxState::Started => Ok(WakeTransition::Noop),
        SandboxState::Paused => Ok(WakeTransition::Resume),
        SandboxState::Suspended => Ok(WakeTransition::Restore),
        SandboxState::Created | SandboxState::Stopped => Ok(WakeTransition::Start),
        SandboxState::Creating
        | SandboxState::Starting
        | SandboxState::Pausing
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Restoring
        | SandboxState::Stopping
        | SandboxState::CleanupRequired => Ok(WakeTransition::Wait),
        SandboxState::Deleting
        | SandboxState::CleanupFailed
        | SandboxState::Quarantined
        | SandboxState::Failed => Err(Status::failed_precondition(format!(
            "sandbox is {state}, cannot wake"
        ))),
    }
}

/// Outcome of the cold-start readiness + network-policy gate.
///
/// The net-gateway is fail-closed: it denies all guest egress until the
/// expected policy is installed and `Ready`. A sandbox must therefore not be
/// marked `Started` (and become reachable) until the guest is ready *and* —
/// when a policy was expected — the policy push has landed. A guest-readiness
/// failure is surfaced ahead of a policy failure because it is the more
/// actionable operator signal.
#[derive(Debug)]
pub(super) enum StartGate {
    Ready(Box<GuestReadyEvidence>),
    GuestNotReady(Status),
    NetworkPolicyFailed(Status),
}

fn activation_completion_lost_status(activation_state: SandboxState) -> Status {
    Status::aborted(format!(
        "sandbox left {activation_state} while activation was completing"
    ))
}

fn activation_failure_status_if_current(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    activation_state: SandboxState,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
    status: Status,
) -> Status {
    let current = db
        .lock()
        .map_err(|error| Status::internal(format!("database mutex poisoned: {error}")))
        .and_then(|db| {
            let row = crate::db::read::sandbox(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?;
            let episode = cleanup_id
                .map(|cleanup_id| {
                    crate::db::read::sandbox_view(&db, sandbox_id)
                        .cleanup_episode()
                        .strict()
                        .map(|episode| {
                            episode.is_some_and(|episode| {
                                episode.id == cleanup_id && episode.requested_at.is_none()
                            })
                        })
                        .map_err(|error| Status::internal(format!("db error: {error}")))
                })
                .transpose()?;
            Ok(row.is_some_and(|row| row.state == activation_state) && episode.unwrap_or(true))
        });
    match current {
        Ok(true) => status,
        Ok(false) => activation_completion_lost_status(activation_state),
        Err(error) => error,
    }
}

pub(super) fn evaluate_start_gate(
    ready: Result<GuestReadyEvidence, Status>,
    network_policy: Option<Result<(), Status>>,
) -> StartGate {
    match ready {
        Err(error) => StartGate::GuestNotReady(error),
        Ok(response) => match network_policy {
            Some(Err(status)) => StartGate::NetworkPolicyFailed(status),
            None | Some(Ok(())) => StartGate::Ready(Box::new(response)),
        },
    }
}

/// Signalled each time `complete_start_after_readiness` commits `Started`, so
/// a caller waiting in `ensure_active_with_start` on a `Starting` row (guest
/// activity or a blocking Wake) proceeds at once instead of at its next
/// re-read of the row. Restore completion, which also finishes a start served
/// from a base snapshot, is not signalled. One signal serves every sandbox on
/// the node: a waiter woken for another sandbox repeats one pass of its wait
/// loop.
pub(super) fn start_succeeded_notify() -> &'static tokio::sync::Notify {
    static NOTIFY: std::sync::OnceLock<tokio::sync::Notify> = std::sync::OnceLock::new();
    NOTIFY.get_or_init(tokio::sync::Notify::new)
}

pub(super) fn notify_start_succeeded() {
    start_succeeded_notify().notify_waiters();
}

/// Waits up to `poll_interval`, or until `notify` fires, while
/// `still_starting` reports the row it reads is `Starting`. The wait is
/// created before that read, so a start that commits after the read still
/// ends it.
pub(super) async fn wait_while_starting(
    notify: &tokio::sync::Notify,
    poll_interval: Duration,
    still_starting: impl FnOnce() -> Result<bool, Status>,
) -> Result<(), Status> {
    let committed = notify.notified();
    if still_starting()? {
        tokio::select! {
            () = tokio::time::sleep(poll_interval) => {}
            () = committed => {}
        }
    }
    Ok(())
}

pub(super) async fn wait_ready_and_mark_started(
    db: crate::db::SharedConnection,
    sandbox_id: &str,
    process_env: &BTreeMap<String, String>,
    ready_timeout_secs: u32,
    phase: &'static str,
    apply_network_policy: bool,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<(), Status> {
    let policy = apply_network_policy.then(|| {
        let db = db.clone();
        let sandbox_id = sandbox_id.to_owned();
        async move {
            refresh_network_policy(&db, &sandbox_id, None, phase)
                .await
                .map_err(network_policy_refresh_error_to_status)
        }
    });
    let image_and_fence = {
        let connection = crate::node_state_sync::connection_or_signal_failure(&db)
            .map_err(|error| Status::internal(error.to_string()))?;
        let row = crate::db::read::sandbox(&connection, sandbox_id)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("activation sandbox disappeared"))?;
        (row.guest_image, row.node_execution_id)
    };
    let guest = async {
        match image_and_fence.0.readiness() {
            pplx_space_common_core::types::guest_image::GuestReadiness::Agent => {
                wait_ready_via_spaced_host(
                    sandbox_id,
                    Duration::from_secs(ready_timeout_secs.into()),
                    phase,
                )
                .await
                .map(GuestReadyEvidence::Agent)
            }
            pplx_space_common_core::types::guest_image::GuestReadiness::TcpService { port } => {
                let execution = image_and_fence.1.ok_or_else(|| {
                    Status::failed_precondition(
                        "guest service readiness requires an execution identity",
                    )
                })?;
                super::guest_image::wait_service(
                    &crate::node_manager_transport::NodeManagerConnector::host(),
                    sandbox_id,
                    execution,
                    port,
                    Duration::from_secs(ready_timeout_secs.into()),
                )
                .await?;
                Ok(GuestReadyEvidence::Service)
            }
        }
    };
    complete_start_after_readiness(
        db,
        sandbox_id,
        process_env,
        ready_timeout_secs,
        phase,
        cleanup_id,
        StartReadiness { guest, policy },
    )
    .await
}

#[derive(Debug)]
pub(super) enum GuestReadyEvidence {
    Agent(pplx_space_spaced_host_client::proto::WaitReadyResponse),
    Service,
}
impl From<pplx_space_spaced_host_client::proto::WaitReadyResponse> for GuestReadyEvidence {
    fn from(value: pplx_space_spaced_host_client::proto::WaitReadyResponse) -> Self {
        Self::Agent(value)
    }
}

struct StartReadiness<G, P> {
    guest: G,
    policy: Option<P>,
}

async fn complete_start_after_readiness<G, P>(
    db: crate::db::SharedConnection,
    sandbox_id: &str,
    process_env: &BTreeMap<String, String>,
    ready_timeout_secs: u32,
    phase: &'static str,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
    readiness: StartReadiness<G, P>,
) -> Result<(), Status>
where
    G: std::future::Future<Output = Result<GuestReadyEvidence, Status>>,
    P: std::future::Future<Output = Result<(), Status>> + Send + 'static,
{
    let t0 = tokio::time::Instant::now();

    // Drive the net-gateway to Ready concurrently with the guest-readiness
    // wait. The net-gateway is fail-closed (it denies egress until it has
    // hydrated a policy), so the sandbox must not be marked Started until the
    // gateway reports Ready; spawning the push lets it overlap the guest boot,
    // which is normally the slower half, so it adds no latency on the common
    // path. We pass no expected ref (`None`): the gate requires only that the
    // gateway is Ready with an enforcing policy, not a specific generation. A
    // SetNetworkPolicy or template change landing mid-start enqueues its own
    // reconcile against the live gateway, so the gate need not — and must not,
    // to avoid a stale-ref mismatch failing the start — assert the latest ref
    // here.
    let policy_task = readiness.policy.map(|policy| {
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(policy.in_current_span()))
    });

    // Bound the start on the guest-readiness deadline. If the guest fails (a
    // fast error or the ready_timeout), return without waiting for the policy
    // push to exhaust its retry schedule, which can outlive ready_timeout_secs.
    let ready = readiness.guest.await;
    // Snapshot the readiness latency before awaiting the policy task: the
    // guest_ready / guest_ready_failed events document readiness latency, which
    // must not absorb network-policy retry time when the push outlasts boot.
    let ready_elapsed = t0.elapsed();
    let policy = match policy_task {
        Some(task) if ready.is_ok() => {
            let mut observation = pplx_space_metrics::gateway::readiness::after_guest_wait();
            let result = task.await.unwrap_or_else(|join_error| {
                Err(Status::internal(format!(
                    "network policy task failed: {join_error}"
                )))
            });
            observation.outcome = if result.is_ok() {
                pplx_space_metrics::gateway::readiness::Outcome::Ready
            } else {
                pplx_space_metrics::gateway::readiness::Outcome::Failed
            };
            Some(result)
        }
        Some(task) => {
            task.abort();
            None
        }
        None => None,
    };

    match evaluate_start_gate(ready, policy) {
        StartGate::Ready(response) => {
            let evidence = *response;
            let agent = match &evidence {
                GuestReadyEvidence::Agent(response) => Some(response),
                GuestReadyEvidence::Service => None,
            };
            let guest_uptime_ms = agent.map(wait_ready_guest_uptime_ms);
            tracing::info!(
                sandbox_id,
                phase,
                ?guest_uptime_ms,
                elapsed_ms = ready_elapsed.as_millis() as u64,
                "guest readiness contract satisfied"
            );
            if let Err(status) = configure_process_env_for_activation(
                &db,
                sandbox_id,
                process_env,
                phase,
                cleanup_id,
            )
            .await
            {
                return Err(activation_failure_status_if_current(
                    &db,
                    sandbox_id,
                    SandboxState::Starting,
                    cleanup_id,
                    status,
                ));
            }
            {
                let db = crate::node_state_sync::connection_or_signal_failure(&db)
                    .map_err(|error| Status::internal(error.to_string()))?;
                let changed = crate::db::mark_start_succeeded(&db, sandbox_id, cleanup_id)
                    .map_err(|e| Status::internal(format!("db error: {e}")))?;
                if !changed {
                    tracing::warn!(
                        sandbox_id,
                        phase,
                        "start readiness succeeded after sandbox left Starting; skipping Started transition"
                    );
                    return Err(activation_completion_lost_status(SandboxState::Starting));
                }
                if let Some(response) = agent
                    && let Ok(Some(sandbox)) = crate::db::read::sandbox(&db, sandbox_id)
                {
                    emit_guest_observed_specs(
                        sandbox_id,
                        &sandbox.template,
                        response.mem_total_bytes,
                        response.vcpu_count,
                    );
                }
            }
            mark_sandbox_dirty(&db, sandbox_id);
            let mut details = SandboxEventDetails::default()
                .operation(phase)
                .elapsed_ms(ready_elapsed.as_millis().to_string());
            if let Some(uptime) = guest_uptime_ms {
                details = details.guest_uptime_ms(uptime.to_string());
            }
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::GuestReady,
                SandboxEventSource::SbxManager,
                details,
            );
            notify_start_succeeded();
            Ok(())
        }
        StartGate::GuestNotReady(error) => {
            tracing::warn!(
                sandbox_id,
                phase,
                timeout_secs = ready_timeout_secs,
                %error,
                "guest readiness contract was not satisfied"
            );
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::GuestReadyFailed,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .operation(phase)
                    .timeout_secs(ready_timeout_secs.to_string())
                    .elapsed_ms(ready_elapsed.as_millis().to_string())
                    .error(error.to_string())
                    .logged_by_caller(),
            );
            let reason = format!(
                "guest did not become ready during {phase} within {ready_timeout_secs}s: {error}"
            );
            Err(activation_failure_status_if_current(
                &db,
                sandbox_id,
                SandboxState::Starting,
                cleanup_id,
                Status::new(error.code(), reason),
            ))
        }
        StartGate::NetworkPolicyFailed(status) => {
            // Guest readiness alone is insufficient: the gateway must confirm
            // network readiness before exposing the sandbox.
            tracing::warn!(
                sandbox_id,
                phase,
                error = %status,
                "network gateway readiness failed during start; failing sandbox closed"
            );
            Err(activation_failure_status_if_current(
                &db,
                sandbox_id,
                SandboxState::Starting,
                cleanup_id,
                status,
            ))
        }
    }
}

fn emit_guest_observed_specs(
    sandbox_id: &str,
    template: &str,
    mem_total_bytes: Option<u64>,
    vcpu_count: Option<u32>,
) {
    match (mem_total_bytes, vcpu_count) {
        (Some(mem_total_bytes), Some(vcpu_count)) => tracing::info!(
            sandbox_id,
            template,
            mem_total_bytes,
            vcpu_count,
            "observed guest sandbox specs"
        ),
        (Some(mem_total_bytes), None) => tracing::info!(
            sandbox_id,
            template,
            mem_total_bytes,
            "observed guest sandbox specs"
        ),
        (None, Some(vcpu_count)) => tracing::info!(
            sandbox_id,
            template,
            vcpu_count,
            "observed guest sandbox specs"
        ),
        (None, None) => return,
    }
    pplx_space_metrics::sandbox::worker::guest_observed_specs(
        template,
        mem_total_bytes,
        vcpu_count,
    );
}

/// Check the restored guest against this host before publishing Started, with a
/// 30s readiness budget followed by the existing 60s network budget. A paused
/// restore cannot answer guest RPCs; its guest check runs on resident resume.
pub(super) async fn complete_restore_and_mark_final(
    db: crate::db::SharedConnection,
    sandbox_id: &str,
    process_env: &BTreeMap<String, String>,
    final_state: SandboxState,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<(), Status> {
    tracing::info!(
        sandbox_id,
        final_state = final_state.as_str(),
        "checking snapshot restore readiness"
    );

    if final_state == SandboxState::Started
        && let Err(status) = wait_ready_via_spaced_host(
            sandbox_id,
            Duration::from_secs(u64::from(DEFAULT_BLOCKING_READY_TIMEOUT_SECS)),
            "restore",
        )
        .await
    {
        return Err(activation_failure_status_if_current(
            &db,
            sandbox_id,
            SandboxState::Restoring,
            cleanup_id,
            status,
        ));
    }

    if let Err(status) =
        configure_process_env_for_activation(&db, sandbox_id, process_env, "restore", cleanup_id)
            .await
    {
        return Err(activation_failure_status_if_current(
            &db,
            sandbox_id,
            SandboxState::Restoring,
            cleanup_id,
            status,
        ));
    }

    // Clock sync is best-effort background work and must not add latency to
    // start/restore. Only kick it off for the Started path: a Paused-final
    // restore means the VM hasn't been resumed yet, so there's nothing to
    // clock-sync until the user wakes it. The restored guest's cached gateway
    // MAC needs nothing here: node-manager's guest L2 ingress policy accepts
    // frames for a stale gateway MAC.
    if final_state == SandboxState::Started {
        spawn_resume_clock_sync(
            sandbox_id.to_string(),
            pplx_space_spaced_host_client::proto::ResumeReason::SnapshotRestore,
            "restore",
        );
    }

    mark_restore_after_network_ready(&db, sandbox_id, final_state, cleanup_id, async {
        refresh_network_policy(&db, sandbox_id, None, "snapshot-restore")
            .await
            .map_err(network_policy_refresh_error_to_status)
    })
    .await
}

async fn mark_restore_after_network_ready(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    final_state: SandboxState,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
    network_ready: impl std::future::Future<Output = Result<(), Status>>,
) -> Result<(), Status> {
    if final_state == SandboxState::Started {
        let assigned = {
            let db = db
                .lock()
                .map_err(|error| Status::internal(format!("database mutex poisoned: {error}")))?;
            crate::db::read::sandbox(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("read restoring sandbox: {error}")))?
                .ok_or_else(|| activation_completion_lost_status(SandboxState::Restoring))?
                .pool_template
                .is_none()
        };
        // Pool members acquire tenant network readiness during adoption.
        // Paused-final restores defer this gate to the eventual resident resume.
        if assigned && let Err(status) = network_ready.await {
            return Err(activation_failure_status_if_current(
                db,
                sandbox_id,
                SandboxState::Restoring,
                cleanup_id,
                status,
            ));
        }
    }

    {
        let db = db
            .lock()
            .map_err(|error| Status::internal(format!("database mutex poisoned: {error}")))?;
        let persisted = crate::db::mark_restore_succeeded(&db, sandbox_id, final_state, cleanup_id)
            .map_err(|error| {
                Status::internal(format!("failed to persist final restore state: {error}"))
            })?;
        if !persisted {
            return Err(Status::aborted(
                "sandbox left restoring while restore activation was completing",
            ));
        }
    }
    mark_sandbox_dirty(db, sandbox_id);
    Ok(())
}

pub(super) async fn notify_resume_clock_sync_best_effort(
    sandbox_id: &str,
    reason: pplx_space_spaced_host_client::proto::ResumeReason,
) -> Result<(), String> {
    match tokio::time::timeout(
        RESUME_CLOCK_SYNC_TIMEOUT,
        notify_resume_via_spaced_host(sandbox_id, reason),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(format!(
            "guest clock sync timed out after {}s",
            RESUME_CLOCK_SYNC_TIMEOUT.as_secs()
        )),
    }
}

pub(super) fn spawn_resume_clock_sync(
    sandbox_id: String,
    reason: pplx_space_spaced_host_client::proto::ResumeReason,
    operation: &'static str,
) {
    // Intentionally outside lifecycle drain: this five-second best-effort
    // notification invalidates host readiness and updates the guest clock,
    // but never writes manager lifecycle persistence. Shutdown may abandon it.
    let span_sandbox_id = sandbox_id.clone();
    tokio::spawn(
        async move {
            match notify_resume_clock_sync_best_effort(&sandbox_id, reason).await {
                Ok(()) => {
                    tracing::debug!(
                        sandbox_id,
                        operation,
                        "guest clock sync notification completed after resume"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        operation,
                        %error,
                        "guest clock sync notification failed after resume"
                    );
                    super::drain::record_detached_failure(&error.to_string());
                }
            }
        }
        .instrument(super::drain::detached_trace_span(
            "ResumeClockSync",
            &span_sandbox_id,
        )),
    );
}

pub(super) struct StartStateTransition {
    pub(super) sandbox_id: String,
    pub(super) process_env: BTreeMap<String, String>,
    pub(super) ready_timeout_secs: u32,
    pub(super) phase: &'static str,
    pub(super) apply_network_policy: bool,
    pub(super) cleanup_id: SandboxCleanupEpisodeId,
}

pub(super) fn spawn_wait_ready_state_transition(
    service: &SandboxManagerService,
    db: crate::db::SharedConnection,
    transition: StartStateTransition,
    cleanup_guard: ActivationCleanupGuard,
) -> Result<(), Status> {
    let StartStateTransition {
        sandbox_id,
        process_env,
        ready_timeout_secs,
        phase,
        apply_network_policy,
        cleanup_id,
    } = transition;
    let span_sandbox_id = sandbox_id.clone();
    spawn_guarded_start_state_transition(service, &span_sandbox_id, cleanup_guard, async move {
        let result = wait_ready_and_mark_started(
            db.clone(),
            &sandbox_id,
            &process_env,
            ready_timeout_secs,
            phase,
            apply_network_policy,
            Some(cleanup_id),
        )
        .await;
        if let Err(error) = &result {
            tracing::warn!(sandbox_id, phase, %error, "start readiness transition failed");
        }
        result
    })
}

fn spawn_guarded_start_state_transition(
    service: &SandboxManagerService,
    sandbox_id: &str,
    mut cleanup_guard: ActivationCleanupGuard,
    transition: impl std::future::Future<Output = Result<(), Status>> + Send + 'static,
) -> Result<(), Status> {
    service.spawn_detached_state_writer("StartFinalizer", sandbox_id, async move {
        match transition.await {
            Ok(()) => cleanup_guard.complete_success(),
            Err(error) => {
                super::drain::record_detached_failure(error.message());
                cleanup_guard.record_failure(&error);
            }
        }
    })?;
    Ok(())
}

pub(super) fn spawn_restore_state_transition(
    service: &SandboxManagerService,
    sandbox_id: String,
    process_env: BTreeMap<String, String>,
    final_state: SandboxState,
    snapshot_dir: Option<String>,
    mut cleanup_guard: ActivationCleanupGuard,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<(), Status> {
    let db = service.db.clone();
    let node_manager = service.node_manager.clone();
    let span_sandbox_id = sandbox_id.clone();
    service.spawn_detached_state_writer("RestoreFinalizer", &span_sandbox_id, async move {
        match complete_restore_and_mark_final(
            db.clone(),
            &sandbox_id,
            &process_env,
            final_state,
            Some(cleanup_id),
        )
        .await
        {
            Ok(()) => {
                if let Some(snapshot_dir) = snapshot_dir {
                    cleanup_restored_snapshot_dir_async(&sandbox_id, &snapshot_dir, &node_manager);
                }
                cleanup_guard.complete_success();
            }
            Err(error) => {
                tracing::warn!(sandbox_id, state = %final_state, %error, "restore final-state transition failed");
                super::drain::record_detached_failure(error.message());
                cleanup_guard.record_failure(&error);
            }
        }
    })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ActivationFailureStage {
    ColdPrepare,
    ColdMetadata,
    ColdEncryptedRootfsPrepare,
    ColdFirecrackerLaunch,
    ColdSidecarLaunch,
    RestorePrepare,
    RestoreMetadata,
    RestoreEncryptedDevicesPrepare,
    RestoreFirecrackerLaunch,
    RestoreNetGatewayLaunch,
    RestoreSnapshotLoad,
    RestoreSpacedHostLaunch,
    RestoreResume,
}

impl ActivationFailureStage {
    pub(super) fn reason_code(self) -> SandboxQuarantineReasonCode {
        match self {
            Self::ColdPrepare => SandboxQuarantineReasonCode::ColdActivationPrepareFailed,
            Self::ColdMetadata => SandboxQuarantineReasonCode::ColdActivationMetadataFailed,
            Self::ColdEncryptedRootfsPrepare => {
                SandboxQuarantineReasonCode::ColdActivationEncryptedRootfsPrepareFailed
            }
            Self::ColdFirecrackerLaunch => {
                SandboxQuarantineReasonCode::ColdActivationFirecrackerLaunchFailed
            }
            Self::ColdSidecarLaunch => {
                SandboxQuarantineReasonCode::ColdActivationSidecarLaunchFailed
            }
            Self::RestorePrepare => SandboxQuarantineReasonCode::RestorePrepareFailed,
            Self::RestoreMetadata => SandboxQuarantineReasonCode::RestoreMetadataFailed,
            Self::RestoreEncryptedDevicesPrepare => {
                SandboxQuarantineReasonCode::RestoreEncryptedDevicesPrepareFailed
            }
            Self::RestoreFirecrackerLaunch => {
                SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed
            }
            Self::RestoreNetGatewayLaunch => {
                SandboxQuarantineReasonCode::RestoreNetGatewayLaunchFailed
            }
            Self::RestoreSnapshotLoad => SandboxQuarantineReasonCode::RestoreSnapshotLoadFailed,
            Self::RestoreSpacedHostLaunch => {
                SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed
            }
            Self::RestoreResume => SandboxQuarantineReasonCode::RestoreResumeFailed,
        }
    }

    /// Whether the error this stage fails with is a status node-manager
    /// produced, so node-manager's own terminal-vs-retryable verdict is
    /// available to preserve.
    ///
    /// The `false` arms build their status locally instead — an execution
    /// metadata parse, a dm-crypt device setup, or a sidecar-launch helper that
    /// already collapsed the node status into `Internal` before returning it.
    /// There is no inbound verdict left to honor at those arms.
    ///
    /// Exhaustive on purpose: a new activation stage has to state which side it
    /// is on before it compiles.
    fn reports_node_manager_status(self) -> bool {
        match self {
            Self::ColdPrepare
            | Self::ColdFirecrackerLaunch
            | Self::RestorePrepare
            | Self::RestoreFirecrackerLaunch
            | Self::RestoreSnapshotLoad
            | Self::RestoreResume => true,
            Self::ColdMetadata
            | Self::ColdEncryptedRootfsPrepare
            | Self::ColdSidecarLaunch
            | Self::RestoreMetadata
            | Self::RestoreEncryptedDevicesPrepare
            | Self::RestoreNetGatewayLaunch
            | Self::RestoreSpacedHostLaunch => false,
        }
    }

    /// The status sbx-mgr answers its own caller with after failing at this
    /// stage, given the `error` that failed it and the human-readable `reason`
    /// already recorded as the quarantine reason.
    ///
    /// Preserving node-manager's code keeps its terminal-vs-retryable verdict
    /// legible instead of discarding it. Precisely what that does and does not
    /// buy today:
    ///
    /// - Callers that can see the gRPC status act on it. The control-plane
    ///   restore path reads `FailedPrecondition` as "try another node" and
    ///   `InvalidArgument` as "this request fails identically everywhere".
    /// - On the Kubernetes path the code does not survive the hop. containerd's
    ///   CRI layer drops it, leaving only the shim's `[space-err=<Code>]`
    ///   message marker in the pod event, and nothing in the control plane
    ///   reads that marker yet. Kubelet keeps retrying `RunPodSandbox` on its
    ///   own backoff whatever is returned here, and a wedged pod is still
    ///   reaped by the age-based stuck-`ContainerCreating` sweep rather than
    ///   failed at the first attempt.
    ///
    /// So this is the classification, not the enforcement. Flattening every
    /// failure to `Internal` would destroy the verdict before any consumer
    /// could be built on it.
    pub(super) fn failure_status(self, error: &Status, reason: String) -> Status {
        if self.reports_node_manager_status() && node_manager_failure_is_terminal(error.code()) {
            Status::new(error.code(), reason)
        } else {
            Status::internal(reason)
        }
    }
}

/// Whether a node-manager lifecycle-RPC failure means retrying the same
/// activation against the same node cannot succeed.
///
/// `FailedPrecondition` is node-manager's verdict that its installed runtime
/// inventory cannot execute this sandbox — a missing guest kernel, an
/// incomplete runtime installation, a snapshot pinning a component digest the
/// node does not have. `InvalidArgument` is a rejected request shape.
/// `PermissionDenied` is a path or peer node-manager refuses on principle — a
/// template resolving outside the managed template root, a rootfs or snapshot
/// artifact that is a symlink, an unauthorized caller. None of the three
/// changes on a retry.
///
/// `NotFound` is deliberately retryable even though node-manager means it
/// terminally. Its producers on these RPCs are a template directory that
/// vanished between template-manager resolution and the call, and a sandbox
/// absent from node-manager's slot pool. The next activation re-resolves the
/// template (re-pulling it when needed) and re-allocates the slot, so retrying
/// is the convergence path rather than a loop. Template resolution reaches the
/// opposite conclusion for *its* `NotFound` — see
/// `start_template_resolution_failed_status` — because a build missing from the
/// registry is not something a node acquires by waiting.
///
/// Exhaustive rather than a `_` arm: a code added upstream must be classified
/// deliberately, not default into either set.
fn node_manager_failure_is_terminal(code: tonic::Code) -> bool {
    use tonic::Code;
    match code {
        Code::FailedPrecondition | Code::InvalidArgument | Code::PermissionDenied => true,
        Code::Ok
        | Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::NotFound
        | Code::AlreadyExists
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::Internal
        | Code::Unavailable
        | Code::DataLoss
        | Code::Unauthenticated => false,
    }
}

pub(super) async fn notify_resume_via_spaced_host(
    sandbox_id: &str,
    reason: pplx_space_spaced_host_client::proto::ResumeReason,
) -> Result<(), String> {
    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
        .with_socket_path(spaced_host_socket_path(sandbox_id));
    let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
        .await
        .map_err(|error| error.to_string())?;
    client
        .notify_resume(reason)
        .await
        .and_then(|response| match response.clock_sync_status() {
            pplx_space_spaced_host_client::proto::ClockSyncStatus::Synced => Ok(()),
            pplx_space_spaced_host_client::proto::ClockSyncStatus::Failed => Err(
                tonic::Status::failed_precondition("guest clock sync failed"),
            ),
            pplx_space_spaced_host_client::proto::ClockSyncStatus::Unspecified => Err(
                tonic::Status::internal("spaced-host returned unspecified clock sync status"),
            ),
        })
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;
