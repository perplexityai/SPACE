use super::activation_support::{
    GuestActivityKind, IdentityFreeDirectActivationAuthority, configure_process_env_for_activation,
    spawn_resume_clock_sync, wait_ready_guest_uptime_ms, wait_ready_via_spaced_host,
};
use super::create_support::*;
use super::network_policy::EffectiveNetworkPolicy;
use super::network_runtime::{network_policy_refresh_error_to_status, refresh_network_policy};
use super::spaced_bridge::StreamPresenceTouch;
use super::spaced_bridge_policy::SpacedBridgePolicy;
use super::tail_support::*;
use super::*;

use pplx_space_common_core::types::{GuestPort, SandboxId, SpaceApiRouteIdentity};
use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, SpaceApiBridgeCallerPolicy, SpaceApiRouteCapabilityPresence,
    SpaceApiRouteIdentityDimension, SpaceApiRouteIdentityMatchError, SpaceApiRoutePurpose,
    match_present_route_identity_dimensions, set_route_identity_rejection_metadata,
    space_api_bridge_caller_policy, space_api_route_capability_presence,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Error, SpaceApiRouteEd25519Verifier,
    space_api_route_ed25519_token_from_metadata,
};
use pplx_space_metrics::sandbox::worker::SpaceApiRouteTokenAuthorizationResult;
use pplx_space_net_common::{EffectiveNetworkPolicyRef, NetworkPolicy};
use tonic::metadata::MetadataMap;

mod resume_begin;
#[cfg(test)]
mod tests;

/// Upper bound on the best-effort artifact-dir permission repair issued before a
/// resume. Generous for a quick fs op, but small enough that a slow or
/// limiter-saturated node-manager can't stall the resume; on timeout the suspend
/// path remains the authoritative repair.
const RESUME_ARTIFACT_DIR_PREPARE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub(super) const SPACE_API_BRIDGE_ROUTE_PURPOSES: &[SpaceApiRoutePurpose] =
    &[SpaceApiRoutePurpose::SpaceApiBridge];
const SERVICE_PORT_INGRESS_ROUTE_PURPOSES: &[SpaceApiRoutePurpose] =
    &[SpaceApiRoutePurpose::ServicePortIngress];

#[cfg(test)]
mod wait_ready_policy_tests;

#[derive(Clone, Debug)]
pub(super) struct SpaceApiBridgeRouteAuthorization {
    operation: &'static str,
    claims: SpaceApiRouteEd25519Claims,
}

impl SpaceApiBridgeRouteAuthorization {
    fn verified(operation: &'static str, claims: SpaceApiRouteEd25519Claims) -> Self {
        Self { operation, claims }
    }

    pub(super) fn authorize_sandbox(&self, sandbox: &crate::db::SandboxRow) -> Result<(), Status> {
        self.authorize_local_identity(&Self::sandbox_identity(sandbox)?, sandbox.state)
    }

    /// A refusal must still reject mismatched surviving identity evidence, but
    /// teardown may have cleared the active route binding. This never grants
    /// runtime access or permission to mutate the sandbox.
    pub(super) fn refuse_sandbox(
        &self,
        sandbox: &crate::db::SandboxRow,
        refusal: Status,
    ) -> Status {
        let identity = match Self::sandbox_identity(sandbox) {
            Ok(identity) => identity,
            Err(error) => return error,
        };
        if let Err(error) = self.check_present_identity(&identity) {
            return error;
        }
        refusal
    }

    fn sandbox_identity(sandbox: &crate::db::SandboxRow) -> Result<SpaceApiRouteIdentity, Status> {
        let sandbox_id = sandbox.id.parse().map_err(|error| {
            Status::internal(format!("persisted sandbox id is invalid: {error}"))
        })?;
        let mut local_identity = sandbox.space_api_route_identity.clone();
        local_identity.sandbox_id = Some(sandbox_id);
        Ok(local_identity)
    }

    pub(super) fn authorize_service_port(
        &self,
        sandbox: &crate::db::SandboxRow,
        port: GuestPort,
    ) -> Result<(), Status> {
        if self.claims.service_port != Some(port) {
            record_space_api_route_token_authorization(
                self.operation,
                SpaceApiRouteTokenAuthorizationResult::ServicePortMismatch,
            );
            return Err(Status::permission_denied(format!(
                "{} SPACE API route token targets a different service port",
                self.operation
            )));
        }
        self.authorize_sandbox(sandbox)
    }

    pub(super) fn authorize_local_identity(
        &self,
        local_identity: &SpaceApiRouteIdentity,
        state: SandboxState,
    ) -> Result<(), Status> {
        self.check_present_identity(local_identity)?;
        local_identity.clone().try_into_complete().map_err(|_| {
            record_space_api_route_token_authorization(
                self.operation,
                SpaceApiRouteTokenAuthorizationResult::LocalIdentityIncomplete,
            );
            // Teardown deliberately retires the live route identity. A valid,
            // matching capability still cannot access an inactive guest, but
            // that is an activation precondition, not invalid credentials.
            // Mismatched dimensions above remain permission failures.
            if matches!(state, SandboxState::Suspended | SandboxState::Stopped) {
                let refusal = Status::failed_precondition(format!(
                    "sandbox is not running ({state}); reconnect before guest access"
                ));
                // Only a start returns a stopped sandbox to running; a
                // suspended one may still be restored through a fresh route.
                return if state == SandboxState::Stopped {
                    pplx_space_common_grpc::mark_sandbox_not_running(refusal, state.as_str())
                } else {
                    refusal
                };
            }
            Status::permission_denied(format!(
                "{} local SPACE API route identity is incomplete",
                self.operation
            ))
        })?;
        record_space_api_route_token_authorization(
            self.operation,
            SpaceApiRouteTokenAuthorizationResult::Ok,
        );
        Ok(())
    }

    fn check_present_identity(&self, local_identity: &SpaceApiRouteIdentity) -> Result<(), Status> {
        match_present_route_identity_dimensions(local_identity, &self.claims.route_identity)
            .map_err(|error| {
                record_space_api_route_token_authorization(
                    self.operation,
                    route_identity_match_metric(error),
                );
                route_identity_match_status(self.operation, error)
            })
    }
}

/// Build the `PermissionDenied` a rejected route identity produces.
///
/// The message is diagnostic. The machine-readable half is the rejection
/// metadata, which tells the caller whether re-resolving the route can turn this
/// rejection into a success; callers must never infer that from the message.
fn route_identity_match_status(
    operation: &'static str,
    error: SpaceApiRouteIdentityMatchError,
) -> Status {
    let mut status = route_identity_match_message(operation, error);
    set_route_identity_rejection_metadata(
        &mut status,
        error.remediation(),
        route_identity_match_dimension(error),
    );
    status
}

fn route_identity_match_dimension(
    error: SpaceApiRouteIdentityMatchError,
) -> Option<SpaceApiRouteIdentityDimension> {
    match error {
        SpaceApiRouteIdentityMatchError::Mismatch(dimension) => Some(dimension),
    }
}

fn route_identity_match_metric(
    error: SpaceApiRouteIdentityMatchError,
) -> SpaceApiRouteTokenAuthorizationResult {
    match error {
        SpaceApiRouteIdentityMatchError::Mismatch(dimension) => match dimension {
            SpaceApiRouteIdentityDimension::SandboxId => {
                SpaceApiRouteTokenAuthorizationResult::SandboxMismatch
            }
            SpaceApiRouteIdentityDimension::RouteGeneration => {
                SpaceApiRouteTokenAuthorizationResult::RouteGenerationMismatch
            }
            SpaceApiRouteIdentityDimension::CurrentNodeId => {
                SpaceApiRouteTokenAuthorizationResult::NodeMismatch
            }
            SpaceApiRouteIdentityDimension::SubstrateKind => {
                SpaceApiRouteTokenAuthorizationResult::SubstrateMismatch
            }
            SpaceApiRouteIdentityDimension::ExecutionGeneration => {
                SpaceApiRouteTokenAuthorizationResult::ExecutionGenerationMismatch
            }
            SpaceApiRouteIdentityDimension::ActivationAttemptId => {
                SpaceApiRouteTokenAuthorizationResult::ActivationAttemptMismatch
            }
            SpaceApiRouteIdentityDimension::PodUid => {
                SpaceApiRouteTokenAuthorizationResult::PodUidMismatch
            }
        },
    }
}

fn route_identity_match_message(
    operation: &'static str,
    error: SpaceApiRouteIdentityMatchError,
) -> Status {
    match error {
        SpaceApiRouteIdentityMatchError::Mismatch(dimension) => {
            let mismatch = match dimension {
                SpaceApiRouteIdentityDimension::SandboxId => "a different sandbox",
                SpaceApiRouteIdentityDimension::RouteGeneration => "a stale route generation",
                SpaceApiRouteIdentityDimension::CurrentNodeId => "a different node",
                SpaceApiRouteIdentityDimension::SubstrateKind => "a different substrate",
                SpaceApiRouteIdentityDimension::ExecutionGeneration => {
                    "a different execution generation"
                }
                SpaceApiRouteIdentityDimension::ActivationAttemptId => {
                    "a different activation attempt"
                }
                SpaceApiRouteIdentityDimension::PodUid => "a different pod",
            };
            Status::permission_denied(format!(
                "{operation} SPACE API route token targets {mismatch}"
            ))
        }
    }
}

fn record_space_api_route_token_authorization(
    operation: &str,
    result: SpaceApiRouteTokenAuthorizationResult,
) {
    pplx_space_metrics::sandbox::worker::space_api_route_token_authorization(operation, result);
}

impl SandboxManagerService {
    pub(super) fn is_node_ctl_peer(peer: &AuthorizedUdsPeer) -> bool {
        peer.principal() == UdsPeerPrincipal::Service(user_names::SPACE_NODECTL)
    }

    pub(super) fn authorize_peer<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
        allowed: &[UdsPeerPrincipal],
    ) -> Result<AuthorizedUdsPeer, Status> {
        self.peer_authorizer
            .authorize_request(request, operation, allowed)
    }

    pub(super) fn prepare_space_api_bridge<T>(
        &self,
        request: &Request<T>,
        sandbox_id: &str,
        rpc: SpacedRpc,
    ) -> Result<SpaceApiBridgeRouteAuthorization, Status> {
        self.prepare_space_api_bridge_metadata(request.metadata(), sandbox_id, rpc)
    }

    pub(super) fn prepare_classified_space_api_bridge<T>(
        &self,
        request: &Request<T>,
        sandbox_id: &str,
        rpc: SpacedRpc,
        peer: &AuthorizedUdsPeer,
    ) -> Result<Option<SpaceApiBridgeRouteAuthorization>, Status> {
        let exempt = match rpc.policy() {
            SpacedBridgePolicy::RouteTokenRequired => false,
            SpacedBridgePolicy::NodeCtlExempt => Self::is_node_ctl_peer(peer),
            SpacedBridgePolicy::TrustedWaitReadyLifecycle
                if peer
                    .principal()
                    .satisfies(UdsPeerPrincipal::Role(LocalRole::Operator)) =>
            {
                true
            }
            SpacedBridgePolicy::TrustedWaitReadyLifecycle => match peer.principal() {
                UdsPeerPrincipal::Root => true,
                UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI) => {
                    space_api_bridge_caller_policy(request.metadata())?
                        == Some(SpaceApiBridgeCallerPolicy::TrustedLifecycle)
                        && space_api_route_capability_presence(request.metadata())
                            == SpaceApiRouteCapabilityPresence::Absent
                }
                _ => false,
            },
        };
        if exempt {
            return Ok(None);
        }
        self.prepare_space_api_bridge_metadata_unchecked(request.metadata(), sandbox_id, rpc)
            .map(Some)
    }

    pub(super) fn prepare_space_api_bridge_metadata(
        &self,
        metadata: &MetadataMap,
        sandbox_id: &str,
        rpc: SpacedRpc,
    ) -> Result<SpaceApiBridgeRouteAuthorization, Status> {
        if rpc.policy() != SpacedBridgePolicy::RouteTokenRequired {
            return Err(Status::internal(format!(
                "{rpc} must use its classified bridge authorization path"
            )));
        }
        self.prepare_space_api_bridge_metadata_unchecked(metadata, sandbox_id, rpc)
    }

    fn prepare_space_api_bridge_metadata_unchecked(
        &self,
        metadata: &MetadataMap,
        sandbox_id: &str,
        rpc: SpacedRpc,
    ) -> Result<SpaceApiBridgeRouteAuthorization, Status> {
        self.prepare_route_token_authorization(
            metadata,
            sandbox_id,
            rpc.as_str(),
            SPACE_API_BRIDGE_ROUTE_PURPOSES,
        )
    }

    pub(super) fn prepare_service_port_ingress(
        &self,
        metadata: &MetadataMap,
        sandbox_id: &str,
        operation: &'static str,
    ) -> Result<SpaceApiBridgeRouteAuthorization, Status> {
        self.prepare_route_token_authorization(
            metadata,
            sandbox_id,
            operation,
            SERVICE_PORT_INGRESS_ROUTE_PURPOSES,
        )
    }

    fn prepare_route_token_authorization(
        &self,
        metadata: &MetadataMap,
        sandbox_id: &str,
        operation: &'static str,
        allowed_purposes: &[SpaceApiRoutePurpose],
    ) -> Result<SpaceApiBridgeRouteAuthorization, Status> {
        let route_ed25519_verification_bundle = self.route_ed25519_verification_bundle.load();
        let claims = verify_route_token_ed25519_from_metadata(
            metadata,
            route_ed25519_verification_bundle
                .as_deref()
                .map(|bundle| bundle.verifier()),
            operation,
        )?
        .ok_or_else(|| {
            record_space_api_route_token_authorization(
                operation,
                SpaceApiRouteTokenAuthorizationResult::MissingToken,
            );
            Status::permission_denied(format!("{operation} requires a SPACE API route token"))
        })?;
        validate_route_token_ed25519_target(&claims, sandbox_id, operation, allowed_purposes)?;
        Ok(SpaceApiBridgeRouteAuthorization::verified(
            operation, claims,
        ))
    }

    pub(super) fn is_template_manager_peer(peer: &AuthorizedUdsPeer) -> bool {
        peer.principal() == UdsPeerPrincipal::Service(SPACE_TMPLMGR_ACCOUNT)
    }

    pub(super) fn sandbox_owned_by_template_manager(sandbox: &crate::db::SandboxRow) -> bool {
        sandbox.user.kind() == SandboxOwnerKind::TemplateManager
    }

    pub(super) fn sandbox_owner_allows_local_route_identity_mint(
        sandbox: &crate::db::SandboxRow,
    ) -> bool {
        matches!(
            sandbox.user.kind(),
            SandboxOwnerKind::TemplateManager
                | SandboxOwnerKind::Builder
                | SandboxOwnerKind::Root
                | SandboxOwnerKind::Deploy
                | SandboxOwnerKind::Dev
        )
    }

    pub(super) fn sandbox_owner_local_route_identity_authority(
        sandbox: &crate::db::SandboxRow,
    ) -> IdentityFreeDirectActivationAuthority {
        if Self::sandbox_owner_allows_local_route_identity_mint(sandbox) {
            IdentityFreeDirectActivationAuthority::LocalProducer
        } else {
            IdentityFreeDirectActivationAuthority::Denied
        }
    }

    pub(super) fn identity_free_start_authority(
        peer: &AuthorizedUdsPeer,
        sandbox: &crate::db::SandboxRow,
    ) -> IdentityFreeDirectActivationAuthority {
        let owner = sandbox.user.kind();
        let allowed = match peer.principal() {
            UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR) => {
                owner == SandboxOwnerKind::TemplateManager
            }
            UdsPeerPrincipal::Root
            | UdsPeerPrincipal::Role(LocalRole::Operator)
            | UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR) => matches!(
                owner,
                SandboxOwnerKind::Builder
                    | SandboxOwnerKind::Root
                    | SandboxOwnerKind::Deploy
                    | SandboxOwnerKind::Dev
            ),
            _ => false,
        };
        if allowed {
            IdentityFreeDirectActivationAuthority::LocalProducer
        } else {
            IdentityFreeDirectActivationAuthority::Denied
        }
    }

    pub(super) fn sandbox_is_pool_member(sandbox: &crate::db::SandboxRow) -> bool {
        sandbox.pool_template.is_some()
    }

    pub(super) fn authorize_template_manager_sandbox(
        &self,
        peer: &AuthorizedUdsPeer,
        sandbox: &crate::db::SandboxRow,
        operation: &'static str,
    ) -> Result<(), Status> {
        if Self::is_template_manager_peer(peer) && !Self::sandbox_owned_by_template_manager(sandbox)
        {
            return Err(Status::permission_denied(format!(
                "space-tmplmgr may only call {operation} for template-owned sandboxes"
            )));
        }
        Ok(())
    }

    fn peer_can_create_builder_sandbox(peer: &AuthorizedUdsPeer) -> bool {
        matches!(
            peer.principal(),
            UdsPeerPrincipal::Root
                | UdsPeerPrincipal::Role(LocalRole::Admin | LocalRole::Operator)
                | UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR)
        )
    }

    fn peer_can_create_reserved_local_sandbox(peer: &AuthorizedUdsPeer) -> bool {
        Self::peer_can_create_builder_sandbox(peer)
            || peer.principal() == UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI)
    }

    fn peer_can_create_platform_sandbox(peer: &AuthorizedUdsPeer) -> bool {
        matches!(
            peer.principal(),
            UdsPeerPrincipal::Root | UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI)
        )
    }

    fn local_operator_owner_help() -> String {
        format_reserved_sandbox_owner_kind_labels(LOCAL_OPERATOR_SANDBOX_OWNER_KINDS)
    }

    pub(super) fn authorize_create_sandbox_owner(
        peer: &AuthorizedUdsPeer,
        req: &CreateRequest,
    ) -> Result<SandboxOwner, Status> {
        Self::authorize_sandbox_owner_label(peer, &req.owner)
    }

    pub(super) fn authorize_sandbox_owner_label(
        peer: &AuthorizedUdsPeer,
        user: &str,
    ) -> Result<SandboxOwner, Status> {
        let sandbox_owner = SandboxOwner::parse(user.to_string())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let kind = sandbox_owner.kind();

        if Self::is_template_manager_peer(peer) {
            if kind != SandboxOwnerKind::TemplateManager {
                return Err(Status::permission_denied(
                    "space-tmplmgr may only create sandboxes for tmplmgr@local",
                ));
            }
            return Ok(sandbox_owner);
        }

        if kind == SandboxOwnerKind::TemplateManager {
            return Err(Status::permission_denied(
                "tmplmgr@local is restricted to space-tmplmgr",
            ));
        }
        if kind == SandboxOwnerKind::Orphaned {
            return Err(Status::permission_denied(
                "orphaned@local is reserved for sandbox-manager recovery",
            ));
        }

        match kind {
            SandboxOwnerKind::Builder if !Self::peer_can_create_builder_sandbox(peer) => {
                return Err(Status::permission_denied(
                    "builder@local is restricted to local build callers",
                ));
            }
            ref local_kind
                if LOCAL_OPERATOR_SANDBOX_OWNER_KINDS.contains(local_kind)
                    && !Self::peer_can_create_reserved_local_sandbox(peer) =>
            {
                return Err(Status::permission_denied(format!(
                    "{} is restricted to local privileged callers",
                    sandbox_owner.as_str()
                )));
            }
            SandboxOwnerKind::PlatformUser(_) if !Self::peer_can_create_platform_sandbox(peer) => {
                return Err(Status::permission_denied(
                    "platform sandbox owners are restricted to space-nodeapi or root callers",
                ));
            }
            SandboxOwnerKind::Local(_) => {
                let accepted = Self::local_operator_owner_help();
                return Err(Status::permission_denied(format!(
                    "unsupported local sandbox owner; use {accepted}"
                )));
            }
            _ => {}
        }
        Ok(sandbox_owner)
    }

    pub(super) fn authorize_netns_start_context<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
        netns_path: &Option<String>,
    ) -> Result<(), Status> {
        if netns_path.is_some() {
            self.authorize_peer(request, operation, ALLOW_SANDBOX_START_CONTEXT)?;
        }
        Ok(())
    }

    /// Look up a sandbox by exact ID, long UUID prefix, or short random label.
    /// Returns error if not found or ambiguous.
    pub(super) fn lookup_sandbox(&self, sandbox_id: &str) -> Result<crate::db::SandboxRow, Status> {
        let db = self.db_conn()?;
        if let Some(sandbox) = crate::db::read::sandbox(&db, sandbox_id)
            .map_err(|e| Status::internal(format!("db error: {e}")))?
        {
            return Ok(sandbox);
        }

        let matches =
            crate::db::scan::resolve_selector(&db, crate::db::scan::RpcSelector, sandbox_id)
                .map_err(|e| Status::internal(format!("db error: {e}")))?;
        match matches.as_slice() {
            [id] => crate::db::read::sandbox(&db, id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox not found: {id}"))),
            [] => Err(Status::not_found(format!(
                "sandbox not found: {sandbox_id}"
            ))),
            _ => Err(Status::invalid_argument(format!(
                "ambiguous sandbox id '{sandbox_id}', matches include: {}",
                matches.join(", ")
            ))),
        }
    }

    pub(super) fn lookup_started_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<crate::db::SandboxRow, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if sandbox.state != SandboxState::Started {
            return Err(Status::failed_precondition(format!(
                "sandbox is {}, not started",
                sandbox.state
            )));
        }
        Ok(sandbox)
    }

    /// Look up a sandbox for in-guest loopback-bridge RPCs. The bridge lives
    /// inside the guest, so the requirement is a state in which the guest VM
    /// may be live and spaced-host reachable
    /// ([`SandboxState::guest_bridge_reachable`]) — not full wake completion.
    /// This lets the exposure reconciler re-arm dynamic-port listeners while
    /// a wake is still converging instead of leaving exposed ports
    /// unreachable until the whole wake sequence finishes.
    pub(super) fn lookup_guest_bridge_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<crate::db::SandboxRow, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if !sandbox.state.guest_bridge_reachable() {
            let status = Status::failed_precondition(format!(
                "sandbox is {}; the guest is not reachable for loopback bridge operations",
                sandbox.state
            ));
            // A pre-run state (`Creating`/`Created`) progresses toward
            // `Started` through its own create/start lifecycle, so it stays
            // retryable — a sandbox created with exposed ports arms the bridge
            // within seconds of `Started` instead of waiting for the periodic
            // backstop. Every other non-reachable state is terminal: the
            // settle-to-frozen transitions (`Suspending`→`Suspended`,
            // `Pausing`→`Paused`, `Stopping`→`Stopped`), the already-settled
            // `Failed`/`Suspended`/`Stopped`/`Paused`, and `Checkpointing`
            // (which resumes to its *origin* — a `Paused` sandbox checkpoints
            // back to `Paused`, still unreachable, and the origin isn't knowable
            // from the row) all become reachable only via a separate wake this
            // ref does not drive. Marking those terminal completes the ref
            // instead of poisoning the queue at the backoff cap; the periodic
            // full reconcile re-verifies once the guest is reachable again.
            // (The transient "guest listening but still initializing" case,
            // `guest_not_ready`, is relayed from the guest without this marker
            // and likewise stays retryable.)
            return Err(if sandbox.state.precedes_first_run() {
                status
            } else {
                pplx_space_common_grpc::mark_desired_state_apply_terminal(status)
            });
        }
        Ok(sandbox)
    }

    pub(super) fn auto_lifecycle_runtime_state(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> AutoLifecycleRuntimeState {
        let mut state = sandbox.auto_lifecycle_state.clone();
        recompute_runtime_state(&mut state, &sandbox.auto_lifecycle);
        state
    }

    pub(super) fn stable_policy_update_state(state: SandboxState) -> bool {
        matches!(
            state,
            SandboxState::Created
                | SandboxState::Started
                | SandboxState::Paused
                | SandboxState::Suspended
                | SandboxState::Stopped
        )
    }

    pub(super) fn resolve_create_auto_lifecycle_policy(
        &self,
        req: &CreateRequest,
        resolved_template: &ResolvedTemplatePaths,
        pool_template: Option<&str>,
    ) -> Result<AutoLifecyclePolicy, Status> {
        let policy = if let Some(policy) = req.auto_lifecycle.as_ref() {
            policy_from_proto(policy)?
        } else {
            resolved_template.default_auto_lifecycle.clone()
        };

        if pool_template.is_some() {
            return Ok(AutoLifecyclePolicy::disabled());
        }

        validate_v1_policy(&policy).map_err(Status::invalid_argument)?;
        Ok(policy.normalized())
    }

    pub(super) fn resolve_create_network_policy_override(
        req: &CreateRequest,
    ) -> Result<Option<NetworkPolicy>, Status> {
        super::network_policy::optional_network_policy_from_proto(req.network_policy.as_ref())
    }

    pub(super) fn touch_runtime_state_for_guest_activity(
        &self,
        sandbox: &crate::db::SandboxRow,
        activity: GuestActivityKind,
    ) -> Result<(), Status> {
        let now = now_secs();
        let mut state = sandbox.auto_lifecycle_state.clone();

        if !sandbox.auto_lifecycle.is_disabled()
            && state.conditions.is_empty()
            && sandbox.state == SandboxState::Started
        {
            let initialized = initial_runtime_state(&sandbox.auto_lifecycle, now);
            state.conditions = initialized.conditions;
            state.next_auto_event_at = initialized.next_auto_event_at;
        }

        match activity {
            GuestActivityKind::Access => touch_access(&mut state, &sandbox.auto_lifecycle, now),
            GuestActivityKind::Command => {
                touch_command_and_access(&mut state, &sandbox.auto_lifecycle, now)
            }
        }

        if sandbox.auto_lifecycle.is_disabled() {
            recompute_runtime_state(&mut state, &sandbox.auto_lifecycle);
        } else {
            disarm_guest_activity_on_external_access(&mut state, &sandbox.auto_lifecycle, now);
            clear_error(&mut state, &sandbox.auto_lifecycle);
        }
        // Activity is recorded at one-second resolution, so back-to-back guest
        // operations usually leave the state as it was; skip the write then.
        if state == sandbox.auto_lifecycle_state {
            return Ok(());
        }
        let db = self.db_conn()?;
        crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &state)
            .map_err(|e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")))
    }

    /// Periodic presence touch from an open foreground-stream forward task.
    ///
    /// `try_lock`, not `lock().await`: the caller awaits this inline in the
    /// stream forward loop, and a touch queued behind a long wake or a
    /// multi-second suspend publication (the scheduler holds the wake lock
    /// for the whole suspend) would stall stdout/stderr forwarding for the
    /// entire stream. Skipping a tick is harmless with the touch interval
    /// well below the idle timeout — and if a wake or suspend for this
    /// sandbox is in flight, the touch is respectively redundant (the wake
    /// touches on completion) or already superseded.
    ///
    /// The `Started` gate matters: `touch_sources` re-arms conditions
    /// regardless of row state, and a background task must never re-arm a
    /// suspending/suspended row.
    pub(super) async fn touch_runtime_state_for_stream_presence(
        &self,
        sandbox_id: &str,
        activity: GuestActivityKind,
    ) -> Result<StreamPresenceTouch, Status> {
        let Ok(_state_writer) = self.state_writer_lease("StreamPresenceTouch") else {
            return Ok(StreamPresenceTouch::SkippedDraining);
        };
        let wake_lock = self.auto_lifecycle.wake_lock(sandbox_id);
        let Ok(_guard) = wake_lock.try_lock() else {
            return Ok(StreamPresenceTouch::SkippedLockContended);
        };
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if sandbox.state != SandboxState::Started {
            return Ok(StreamPresenceTouch::SkippedNotStarted);
        }
        self.touch_runtime_state_for_guest_activity(&sandbox, activity)?;
        Ok(StreamPresenceTouch::Applied)
    }

    /// Resume a paused sandbox while the caller holds this sandbox's wake lock.
    pub(super) async fn resume_after_wake_lock(
        &self,
        sandbox_id: &str,
        activation_policy: Option<(&AutoLifecyclePolicy, Option<ExecutionGeneration>)>,
        blocking: bool,
        ready_timeout_secs: u32,
    ) -> Result<(), Status> {
        let resume_t0 = tokio::time::Instant::now();

        // Validate the sandbox is actually resumable before doing any work, so a
        // Resume on a non-resumable row (e.g. Stopped) still fails fast with
        // `failed_precondition` instead of burning the pre-wake policy-refresh
        // retry budget against a sandbox that has no live net-gateway. The DB
        // transition is deferred until after the gate succeeds.
        let sandbox = {
            let db = self.db_conn()?;
            let sandbox = crate::db::read::sandbox(&db, sandbox_id)
                .map_err(|e| Status::internal(format!("db error: {e}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox not found: {sandbox_id}")))?;
            sandbox
                .state
                .transition(SandboxState::Resuming)
                .map_err(Status::from)?;
            sandbox
        };
        let sandbox_id_typed = sandbox
            .id
            .parse::<SandboxId>()
            .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
        // Pre-wake gate: confirm the network policy is installed in the
        // (already-resident) net-gateway before thawing the VM. The gateway
        // survives pause, so a policy update committed while the sandbox was
        // paused may only be queued for the async reconciler and not yet pushed;
        // thawing first would reopen egress under the stale policy — a window the
        // data-plane retry that covers start/restore can't close, because the
        // guest is already running. Fail closed if the gateway can't confirm: the
        // sandbox stays Paused (no teardown — the VM and gateway are healthy and
        // resident). Runs before the Resuming transition is committed so a
        // failure leaves state untouched. Idempotent and fast (~ms) when the
        // policy is unchanged.
        self.apply_network_policy_before_wake(sandbox_id, "resume")
            .await?;
        self.begin_resident_resume(&sandbox.id, activation_policy)
            .await?;
        self.watchdog.record_lifecycle_edge(&sandbox.id).await;

        let mut node_client = connect_node(&self.node_manager)
            .await
            .map_err(|error| resident_resume_connect_error(&self.db, &sandbox.id, error))?;

        // Ensure the cold-path artifact-dir permissions (`snapshots/` group +
        // setgid) are in place before the VM resumes. A cold boot applies these
        // in node-mgr `PrepareSandbox`; a warm-pool-adopted pod resumes without
        // re-running it, so without this the adopted sandbox's first suspend
        // fails creating its staging subdir as a non-root user. Every resume
        // path funnels through here (the Wake RPC, implicit command/access
        // activation, and the resume RPC), so wait_until_ready=false pool
        // adoptions are covered too. Best-effort and bounded — it reuses the
        // resume node-mgr connection, and the suspend path keeps the
        // authoritative check; failure or delay is logged and skipped rather
        // than blocking the resume.
        match tokio::time::timeout(
            RESUME_ARTIFACT_DIR_PREPARE_TIMEOUT,
            node_client.prepare_sandbox_artifact_dir(PrepareSandboxArtifactDirRequest {
                sandbox_id: sandbox.id.clone(),
            }),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    %error,
                    "resume: ensure artifact dir permissions failed (best-effort); \
                     suspend will re-attempt"
                );
            }
            Err(_elapsed) => {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    timeout_secs = RESUME_ARTIFACT_DIR_PREPARE_TIMEOUT.as_secs(),
                    "resume: ensure artifact dir permissions timed out (best-effort); \
                     suspend will re-attempt"
                );
            }
        }

        if let Err(e) = node_client
            .resume_sandbox(ResumeSandboxRequest {
                execution_fence: sandbox.node_execution_fence(),
                sandbox_id: sandbox.id.clone(),
            })
            .await
        {
            let reason = format!("resume failed: {e}");
            let observation = observe_vm_state(
                &sandbox_id_typed,
                "resident resume response reconciliation",
                &self.node_manager,
            )
            .await;
            let reason = match observation.unobservable_reason() {
                Some(probe_error) => format!("{reason}; {probe_error}"),
                None => reason,
            };
            finish_resident_resume_after_rpc_error(
                &self.db,
                &sandbox.id,
                Status::internal(reason),
                observation.state(),
            )?;
        }

        // Clock sync is required after thawing vCPUs, but it must not block
        // resume readiness or turn a live VM into a failed sandbox.
        spawn_resume_clock_sync(
            sandbox.id.clone(),
            pplx_space_spaced_host_client::proto::ResumeReason::SuspendResume,
            "resume",
        );

        // Even nonblocking resume must check the current host/guest contract
        // before publishing Started. Paused restores defer their check here.
        let setup = async {
            let ready = wait_ready_via_spaced_host(
                &sandbox.id,
                Duration::from_secs(u64::from(ready_timeout_secs)),
                "resume",
            )
            .await?;
            configure_process_env_for_activation(
                &self.db,
                &sandbox.id,
                &sandbox.process_env,
                "resume",
                None,
            )
            .await?;
            Ok::<_, Status>(ready)
        }
        .await;
        let ready = match setup {
            Ok(ready) => ready,
            Err(status) => {
                let (rollback_vm_state, status) = match node_client
                    .pause_sandbox(PauseSandboxRequest {
                        execution_fence: sandbox.node_execution_fence(),
                        sandbox_id: sandbox.id.clone(),
                        balloon_hint_timeout_ms: None,
                    })
                    .await
                {
                    Ok(_) => (Some(VmState::Paused), status),
                    Err(pause_error) => {
                        let observation = observe_vm_state(
                            &sandbox_id_typed,
                            "resident resume guest setup rollback",
                            &self.node_manager,
                        )
                        .await;
                        let probe_detail = observation
                            .unobservable_reason()
                            .map(|reason| format!("; {reason}"))
                            .unwrap_or_default();
                        (
                            observation.state(),
                            Status::internal(format!(
                                "{}; rollback pause failed: {pause_error}{probe_detail}",
                                status.message()
                            )),
                        )
                    }
                };
                return finish_resident_resume_after_guest_setup(
                    &self.db,
                    &sandbox.id,
                    Err(status),
                    rollback_vm_state,
                );
            }
        };
        finish_resident_resume_after_guest_setup(&self.db, &sandbox.id, Ok(()), None)?;

        if sandbox.snapshots_enabled && sandbox.pool_template.is_none() {
            self.snapshot_scheduler
                .add_sandbox(sandbox.id.clone(), sandbox.template.clone())
                .await;
        }

        if blocking {
            let guest_uptime_ms = wait_ready_guest_uptime_ms(&ready);
            tracing::info!(
                sandbox_id = %sandbox.id,
                guest_uptime_ms,
                total_elapsed_ms = resume_t0.elapsed().as_millis() as u64,
                "guest ready during blocking resume via spaced-host"
            );
        }

        Ok(())
    }

    pub(super) async fn set_auto_lifecycle_policy_exact(
        &self,
        sandbox_id: &str,
        mut policy: AutoLifecyclePolicy,
        inherit_template_default: bool,
        authority: crate::db::PolicyAuthority,
    ) -> Result<AutoLifecyclePolicy, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        super::guest_image::validate_policy(sandbox.guest_image, &policy)?;
        if !Self::stable_policy_update_state(sandbox.state) {
            let status = Status::failed_precondition(format!(
                "cannot update auto lifecycle while sandbox is {}",
                sandbox.state
            ));
            // A transient state (`Starting`/`Resuming`/`Suspending`/… — the
            // `is_transient` set) settles into a stable state where the same
            // update succeeds, so the node-ctl reconcile ref must keep
            // retrying. A stuck state (`Failed`/`CleanupRequired`/
            // `CleanupFailed`/`Quarantined`)
            // never settles on its own: mark it terminal so the ref completes
            // until a lifecycle/cluster repair changes the state, instead of
            // poisoning the queue at the backoff cap.
            return Err(if sandbox.state.is_transient() {
                status
            } else {
                pplx_space_common_grpc::mark_desired_state_apply_terminal(status)
            });
        }

        if inherit_template_default {
            let resolved_template = resolve_template_paths(
                self.template_resolver.as_ref(),
                pplx_space_template_mgr_client::proto::TemplateRef {
                    name: sandbox.template.clone(),
                    content_hash: sandbox.template_hash.clone(),
                    tag: None,
                },
                template_lookup_scope(&sandbox.user, Some(&sandbox.id))?,
            )
            .await?;
            policy = resolved_template.default_auto_lifecycle;
        }

        super::guest_image::validate_policy(sandbox.guest_image, &policy)?;
        validate_v1_policy(&policy).map_err(Status::invalid_argument)?;
        let runtime = sandbox
            .runtime
            .parse()
            .map_err(|e| Status::failed_precondition(format!("runtime: {e}")))?;
        super::validation::validate_runtime_lifecycle_policy(runtime, &policy)
            .map_err(pplx_space_common_grpc::mark_desired_state_apply_terminal)?;
        super::validation::validate_execution_lifecycle_policy(
            sandbox.execution_metadata.as_ref(),
            &policy,
        )
        .map_err(pplx_space_common_grpc::mark_desired_state_apply_terminal)?;
        policy.normalize_in_place();

        let mut runtime_state = if sandbox.auto_lifecycle == policy {
            sandbox.auto_lifecycle_state.clone()
        } else if sandbox.state == SandboxState::Started {
            initial_runtime_state(&policy, now_secs())
        } else {
            AutoLifecycleRuntimeState::default()
        };
        recompute_runtime_state(&mut runtime_state, &policy);

        let db = self.db_conn()?;
        let transaction = crate::db::Transaction::deferred(&db)
            .map_err(|e| Status::internal(format!("db transaction failed: {e}")))?;
        crate::db::update_auto_lifecycle_with_state(
            &transaction,
            &sandbox.id,
            &policy,
            &runtime_state,
            authority,
        )
        .map_err(|e| Status::internal(format!("db auto_lifecycle update failed: {e}")))?;
        transaction
            .commit()
            .map_err(|e| Status::internal(format!("db transaction commit failed: {e}")))?;
        Ok(policy)
    }

    pub(super) fn lookup_sandbox_by_spaced_host_process(
        &self,
        pid: u32,
        uid: u32,
    ) -> Result<crate::db::SandboxRow, Status> {
        let db = self.db_conn()?;
        let mut matches = crate::db::scan::by_peer(
            &db,
            crate::db::scan::Peer::SpacedHost {
                spaced_host_pid: pid,
                uid,
            },
        )
        .map_err(|e| Status::internal(format!("db error: {e}")))?
        .into_iter();

        match (matches.next(), matches.next()) {
            (Some(sandbox), None) => Ok(sandbox),
            (None, _) => Err(Status::permission_denied(
                "host control caller is not the active spaced-host for any sandbox",
            )),
            (Some(_), Some(_)) => Err(Status::internal(
                "multiple sandboxes matched one host control caller",
            )),
        }
    }

    pub(super) fn lookup_sandbox_by_net_gateway_process(
        &self,
        pid: u32,
        uid: u32,
    ) -> Result<crate::db::SandboxRow, Status> {
        let uid = pplx_space_common_core::host::linux::gateway::sandbox_uid(
            pplx_space_common_core::types::LinuxUid::new(uid),
        )
        .map(|owner| owner.get())
        // Local runtimes and legacy gateways retain their registered identity.
        // The exact PID/owner lookup below remains mandatory in either case.
        .unwrap_or(uid);
        let db = self.db_conn()?;
        let mut matches = crate::db::scan::by_peer(
            &db,
            crate::db::scan::Peer::NetGateway {
                netgtw_pid: pid,
                uid,
            },
        )
        .map_err(|e| Status::internal(format!("db error: {e}")))?
        .into_iter();

        match (matches.next(), matches.next()) {
            (Some(sandbox), None) => Ok(sandbox),
            (None, _) => Err(Status::permission_denied(
                "host control caller is not the active net-gateway for any sandbox",
            )),
            (Some(_), Some(_)) => Err(Status::internal(
                "multiple sandboxes matched one net-gateway caller",
            )),
        }
    }

    pub(super) async fn effective_network_policy(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<EffectiveNetworkPolicy, Status> {
        if let Some(policy) = sandbox.network_policy_override.as_ref() {
            return Ok(EffectiveNetworkPolicy::new(
                policy.clone(),
                sandbox.network_policy_generation,
                sandbox.network_policy_authority,
                false,
            ));
        }

        if sandbox.template == ORPHANED_SANDBOX_TEMPLATE
            || sandbox.user.kind() == SandboxOwnerKind::Orphaned
        {
            return Ok(EffectiveNetworkPolicy::new(
                NetworkPolicyV1::deny_all(),
                0,
                crate::db::PolicyAuthority::Local,
                false,
            ));
        }

        let policy = self
            .template_network_policies
            .get_or_fetch(&sandbox.template)
            .await?;
        Ok(policy)
    }

    pub(super) fn network_policy_applied_source(
        inherited: bool,
    ) -> crate::db::NetworkPolicyAppliedSource {
        if inherited {
            crate::db::NetworkPolicyAppliedSource::Template
        } else {
            crate::db::NetworkPolicyAppliedSource::Override
        }
    }

    /// Resolve the effective network policy (and its ref) to confirm in the
    /// net-gateway before a wake/restore reports ready. Pool members run with no
    /// network identity until adoption, so they resolve to `None` and skip the
    /// pre-wake apply entirely. The ref is carried alongside the policy because
    /// the apply needs both: the ref to push/confirm, and the policy's
    /// `inherited`/`generation` to record the applied marker on success.
    pub(super) async fn effective_network_policy_for_wake(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<Option<(EffectiveNetworkPolicy, EffectiveNetworkPolicyRef)>, Status> {
        if Self::sandbox_is_pool_member(sandbox) {
            return Ok(None);
        }
        let effective_policy = self.effective_network_policy(sandbox).await?;
        let effective_policy_ref = effective_network_policy_ref(sandbox, &effective_policy)?;
        Ok(Some((effective_policy, effective_policy_ref)))
    }

    /// Push the sandbox's effective network policy to its net-gateway and block
    /// until the gateway confirms the policy is installed, then record the applied
    /// generation/source (mirroring the periodic reconciler).
    ///
    /// This is the pre-resume gate: resume runs it before thawing a paused VM, so
    /// the guest can't run against a stale gateway policy once vCPUs are live.
    /// Cold start and snapshot restore instead confirm gateway network readiness
    /// before publishing Started; they do not use this pre-thaw policy fence. Any
    /// failure — a refused push, a gateway that never reports the expected ref, or
    /// a confirmation timeout — is returned so resume can fail closed (leaving the
    /// sandbox Paused) rather than thawing under the wrong policy. The applied state
    /// is recorded only once the gateway has confirmed.
    ///
    /// The effective policy is resolved from a *fresh* sandbox read here, not
    /// captured early by the caller: the gateway pulls the current desired policy
    /// from host-control when it refreshes, so the expected ref must reflect the
    /// latest committed state. A policy update landing during the (potentially
    /// multi-second) allocate/prepare/launch awaits would otherwise make the
    /// gateway install a newer policy than an early-captured ref expects, and the
    /// confirm would spuriously fail the start. Pool members run with no network
    /// identity until adoption, so this is a no-op for them.
    ///
    /// The fresh read is only sufficient because `SetNetworkPolicy` takes the
    /// same per-sandbox wake lock the resume path already holds. That mutual
    /// exclusion guarantees the desired policy cannot change between this
    /// confirm and the VM thaw, so the gateway-confirmed policy is exactly the
    /// one the guest runs under once vCPUs are live. Without that serialization
    /// a concurrent update could commit in the confirm->thaw window and leave
    /// the live guest on a stale gateway policy until the reconciler drained.
    pub(super) async fn apply_network_policy_before_wake(
        &self,
        sandbox_id: &str,
        reason: &'static str,
    ) -> Result<(), Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let Some((effective_policy, effective_policy_ref)) =
            self.effective_network_policy_for_wake(&sandbox).await?
        else {
            return Ok(());
        };
        refresh_network_policy(&self.db, sandbox_id, Some(&effective_policy_ref), reason)
            .await
            .map_err(network_policy_refresh_error_to_status)?;
        self.mark_network_policy_applied(
            sandbox_id,
            effective_policy.inherited,
            effective_policy.generation,
        )
    }

    pub(super) fn mark_network_policy_applied(
        &self,
        sandbox_id: &str,
        inherited: bool,
        generation: u64,
    ) -> Result<(), Status> {
        let db = self.db_conn()?;
        crate::db::mark_network_policy_applied(
            &db,
            sandbox_id,
            Self::network_policy_applied_source(inherited),
            generation,
        )
        .map_err(|e| Status::internal(format!("db network_policy applied failed: {e}")))?;
        Ok(())
    }

    pub(super) fn sandbox_has_live_net_gateway(sandbox: &crate::db::SandboxRow) -> bool {
        sandbox.netgtw_pid.is_some() && sidecar_alive(&sandbox.id, NET_GATEWAY_SIDECAR_PID_NAME)
    }

    pub(super) async fn reject_autostart_create_without_ready_slot(
        &self,
        node_client: &mut NodeManagerGrpcClient,
    ) -> Result<(), Status> {
        let status = node_client
            .get_network_pool_status(NetworkPoolStatusRequest {})
            .await
            .map_err(|error| {
                Status::new(
                    error.code(),
                    format!("network pool status failed: {}", error.message()),
                )
            })?
            .into_inner();

        if status.network_slots_ready == 0 {
            return Err(Status::resource_exhausted("no ready network slots"));
        }

        Ok(())
    }
}

fn resident_resume_connect_error(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    error: super::tail_support::NodeManagerConnectError,
) -> Status {
    // No resume RPC has been dispatched, so the resident VM is still paused.
    // CAS rollback preserves any newer Stop/Delete intent and leaves a failed
    // connection retryable instead of stranding the sandbox in Resuming.
    match restore_resident_resume_prior_state(db, sandbox_id) {
        Ok(()) => Status::unavailable(format!("cannot connect to node-manager: {error}")),
        Err(status) => status,
    }
}

fn restore_resident_resume_prior_state(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
) -> Result<(), Status> {
    let restored = {
        let db = db
            .lock()
            .map_err(|_| Status::internal("database mutex poisoned"))?;
        crate::db::restore_resident_resume_state(&db, sandbox_id, SandboxState::Paused)
            .map_err(|error| Status::internal(error.to_string()))?
    };
    if !restored {
        return Err(Status::aborted(
            "resident resume reconciliation was superseded by a newer lifecycle state",
        ));
    }
    mark_sandbox_dirty(db, sandbox_id);
    Ok(())
}

pub(super) fn finish_resident_resume_after_guest_setup(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    setup_result: Result<(), Status>,
    rollback_vm_state: Option<VmState>,
) -> Result<(), Status> {
    if let Err(status) = setup_result {
        let failure_status = if status.code() == tonic::Code::FailedPrecondition {
            status
        } else {
            Status::unavailable(status.message().to_string())
        };
        if rollback_vm_state == Some(VmState::Paused) {
            let restored = match db.lock() {
                Ok(db) => {
                    crate::db::restore_resident_resume_state(&db, sandbox_id, SandboxState::Paused)
                        .map_err(|error| Status::internal(error.to_string()))
                }
                Err(_) => Err(Status::internal("database mutex poisoned")),
            };
            match restored {
                Ok(true) => mark_sandbox_dirty(db, sandbox_id),
                Ok(false) => tracing::warn!(
                    sandbox_id,
                    "resident resume guest setup rollback was superseded by a newer lifecycle state"
                ),
                Err(error) => tracing::warn!(
                    sandbox_id,
                    %error,
                    "failed to restore Paused after resident resume guest setup rollback"
                ),
            }
        } else {
            mark_quarantined_best_effort(
                db,
                sandbox_id,
                SandboxState::Resuming,
                SandboxQuarantineReasonCode::ResidentResumeFailed,
                failure_status.message(),
            );
        }
        return Err(failure_status);
    }
    let restored = {
        let db = db
            .lock()
            .map_err(|_| Status::internal("database mutex poisoned"))?;
        crate::db::restore_resident_resume_state(&db, sandbox_id, SandboxState::Started)
            .map_err(|error| Status::internal(format!("complete resident resume: {error}")))?
    };
    if !restored {
        return Err(Status::aborted(
            "resident resume completion was superseded by a newer lifecycle state",
        ));
    }
    Ok(())
}

pub(super) fn finish_resident_resume_after_rpc_error(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    status: Status,
    vm_state: Option<VmState>,
) -> Result<(), Status> {
    match vm_state {
        Some(VmState::Running) => {
            tracing::warn!(
                sandbox_id,
                error = %status,
                "resident resume response was lost but Firecracker is running"
            );
            Ok(())
        }
        Some(VmState::Paused) => {
            let restored = match db.lock() {
                Ok(db) => {
                    crate::db::restore_resident_resume_state(&db, sandbox_id, SandboxState::Paused)
                        .map_err(|error| Status::internal(error.to_string()))
                }
                Err(_) => Err(Status::internal("database mutex poisoned")),
            };
            match restored {
                Ok(true) => mark_sandbox_dirty(db, sandbox_id),
                Ok(false) => tracing::warn!(
                    sandbox_id,
                    "resident resume pause reconciliation was superseded"
                ),
                Err(error) => tracing::warn!(
                    sandbox_id,
                    %error,
                    "failed to restore Paused after unconfirmed resident resume"
                ),
            }
            Err(status)
        }
        _ => {
            mark_quarantined_best_effort(
                db,
                sandbox_id,
                SandboxState::Resuming,
                SandboxQuarantineReasonCode::ResidentResumeFailed,
                status.message(),
            );
            Err(status)
        }
    }
}

fn validate_route_token_ed25519_target(
    claims: &SpaceApiRouteEd25519Claims,
    sandbox_id: &str,
    operation: &'static str,
    allowed_purposes: &[SpaceApiRoutePurpose],
) -> Result<(), Status> {
    let sandbox_id = sandbox_id.parse().map_err(|_| {
        record_space_api_route_token_authorization(
            operation,
            SpaceApiRouteTokenAuthorizationResult::InvalidSandboxId,
        );
        Status::invalid_argument(format!("{operation} request has invalid sandbox id"))
    })?;
    if claims.route_identity.sandbox_id() != sandbox_id {
        record_space_api_route_token_authorization(
            operation,
            SpaceApiRouteTokenAuthorizationResult::SandboxMismatch,
        );
        return Err(Status::permission_denied(format!(
            "{operation} SPACE API Ed25519 route token targets a different sandbox"
        )));
    }
    if !allowed_purposes.contains(&claims.purpose) {
        record_space_api_route_token_authorization(
            operation,
            SpaceApiRouteTokenAuthorizationResult::PurposeMismatch,
        );
        return Err(Status::permission_denied(format!(
            "{operation} SPACE API Ed25519 route token purpose is not allowed"
        )));
    }
    Ok(())
}

fn verify_route_token_ed25519_from_metadata(
    metadata: &MetadataMap,
    verifier: Option<&SpaceApiRouteEd25519Verifier>,
    operation: &'static str,
) -> Result<Option<SpaceApiRouteEd25519Claims>, Status> {
    let Some(token) = space_api_route_ed25519_token_from_metadata(metadata).map_err(|error| {
        record_space_api_route_token_authorization(
            operation,
            SpaceApiRouteTokenAuthorizationResult::InvalidToken,
        );
        Status::permission_denied(format!("invalid SPACE API route token: {error}"))
    })?
    else {
        return Ok(None);
    };
    let Some(verifier) = verifier else {
        record_space_api_route_token_authorization(
            operation,
            SpaceApiRouteTokenAuthorizationResult::VerifierUnavailable,
        );
        return Err(Status::permission_denied(
            "SPACE API route token verifier is not configured",
        ));
    };
    match verifier.verify(&token) {
        Ok(claims) => Ok(Some(claims)),
        Err(error) => {
            let result = match &error {
                SpaceApiRouteEd25519Error::Expired => {
                    SpaceApiRouteTokenAuthorizationResult::Expired
                }
                _ => SpaceApiRouteTokenAuthorizationResult::InvalidToken,
            };
            record_space_api_route_token_authorization(operation, result);
            let mut status =
                Status::permission_denied(format!("invalid SPACE API route token: {error}"));
            // Expiry still denies this call. A fresh authoritative lookup must
            // recheck access and mint a current capability before it is retried.
            if matches!(error, SpaceApiRouteEd25519Error::Expired) {
                set_route_identity_rejection_metadata(
                    &mut status,
                    RouteIdentityRemediation::RefreshRoute,
                    None,
                );
            }
            Err(status)
        }
    }
}
