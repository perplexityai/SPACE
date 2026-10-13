//! NetGatewayControl gRPC service implementation.

use std::sync::Arc;

use pplx_space_common_svc::core::host::linux::identities::{user_names, users};
use pplx_space_common_svc::{
    AuthorizedUdsPeer, LocalRole, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal,
};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefParseError, TlsInterception,
    proto::NetworkPolicyV1 as ProtoNetworkPolicyV1,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, metadata::MetadataValue};
use tracing::info;

use crate::debug::GatewayDebugState;
use crate::ingress::{
    ServicePortIngressState, ServicePortIngressStream, open_service_port_ingress,
    route_identity_from_proto, status_for_route_identity_parse, status_for_route_identity_update,
};
use crate::proto::net_gateway_control_server::NetGatewayControl;
use crate::proto::{
    EnableDebugRequest, EnableDebugResponse, GatewayPingRequest, GatewayPingResponse,
    GetNetworkPolicyRequest, GetNetworkPolicyResponse, GetNetworkStateRequest,
    GetNetworkStateResponse, GetStatsRequest, GetStatsResponse,
    InterceptionReadiness as ProtoInterceptionReadiness, RefreshNetworkPolicyRequest,
    RefreshNetworkPolicyResponse, ServicePortIngressFrame, SetNoIdentityRequest,
    SetNoIdentityResponse, SetRouteIdentityRequest, SetRouteIdentityResponse,
    SetRouteVerificationKeysRequest, SetRouteVerificationKeysResponse,
};
use crate::route_verification::{RouteVerificationUpdateError, verification_key_from_wire};
use crate::state::{GatewayNetworkState, InterceptionReadiness, NetworkStateReason, PolicyLoader};
use crate::stats::ProxyStats;
use pplx_space_net_common::proto::TlsInterception as ProtoTlsInterception;
use pplx_space_net_gateway_client::IDENTITY_PROFILE_VERSION;

const SPACE_SBXMGR_ACCOUNT: &str = user_names::SPACE_SBXMGR;
const ALLOW_NET_OBSERVER: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
];
const ALLOW_NET_SERVICE: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
];

pub struct NetGatewayControlService {
    stats: Arc<ProxyStats>,
    network_state: GatewayNetworkState,
    policy_loader: PolicyLoader,
    /// Verbose debug-logging toggle, shared with the proxy hot paths.
    /// `EnableDebug` flips this on for a sandbox-scoped, self-expiring window.
    debug_state: GatewayDebugState,
    service_port_ingress: ServicePortIngressState,
    shutdown_token: CancellationToken,
    peer_authorizer: UdsPeerAuthorizer,
}

impl NetGatewayControlService {
    pub fn new(
        stats: Arc<ProxyStats>,
        network_state: GatewayNetworkState,
        policy_loader: PolicyLoader,
        debug_state: GatewayDebugState,
        service_port_ingress: ServicePortIngressState,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            stats,
            network_state,
            policy_loader,
            debug_state,
            service_port_ingress,
            shutdown_token,
            peer_authorizer: UdsPeerAuthorizer::new_with_default_local_roles(
                "space-net-gateway",
                [UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR)],
            ),
        }
    }

    fn authorize_peer<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
    ) -> Result<AuthorizedUdsPeer, GatewayControlError> {
        self.peer_authorizer
            .authorize_request(request, operation, ALLOW_NET_SERVICE)
            .map_err(GatewayControlError::Status)
    }
}

#[derive(Debug, thiserror::Error)]
enum GatewayControlError {
    #[error("{0}")]
    Status(#[source] Status),
    #[error("expected_policy_ref: {0}")]
    InvalidPolicyRef(#[source] EffectiveNetworkPolicyRefParseError),
    #[error("network policy is not ready: state={state} error_kind={error_kind} error={error}")]
    NetworkPolicyNotReady {
        state: &'static str,
        error_kind: &'static str,
        error: String,
    },
}

impl From<GatewayControlError> for Status {
    fn from(error: GatewayControlError) -> Self {
        match error {
            GatewayControlError::Status(status) => status,
            GatewayControlError::InvalidPolicyRef(error) => {
                Status::invalid_argument(error.to_string())
            }
            GatewayControlError::NetworkPolicyNotReady {
                state,
                error_kind,
                error: _,
            } => {
                let mut status = Status::failed_precondition("network policy is not ready");
                status
                    .metadata_mut()
                    .insert("space-network-state", MetadataValue::from_static(state));
                status.metadata_mut().insert(
                    "space-network-error-kind",
                    MetadataValue::from_static(error_kind),
                );
                status
            }
        }
    }
}

fn status_for_route_verification_update(error: RouteVerificationUpdateError) -> Status {
    match error {
        RouteVerificationUpdateError::StatePoisoned => Status::internal(error.to_string()),
        RouteVerificationUpdateError::InvalidVerifier(_) => {
            Status::invalid_argument(error.to_string())
        }
        RouteVerificationUpdateError::StaleGeneration { .. }
        | RouteVerificationUpdateError::GenerationConflict { .. } => {
            Status::failed_precondition(error.to_string())
        }
    }
}

#[tonic::async_trait]
impl NetGatewayControl for NetGatewayControlService {
    type ServicePortIngressStream = ServicePortIngressStream;

    async fn ping(
        &self,
        request: Request<GatewayPingRequest>,
    ) -> Result<Response<GatewayPingResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "Ping", ALLOW_NET_OBSERVER)?;
        Ok(Response::new(GatewayPingResponse {
            message: "pong".to_string(),
        }))
    }

    async fn get_stats(
        &self,
        request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "GetStats", ALLOW_NET_OBSERVER)?;
        let snapshot = self.network_state.snapshot().await;
        let tls_interception_mode =
            snapshot
                .policy
                .as_ref()
                .map_or(ProtoTlsInterception::Off, |policy| {
                    match policy.tls_interception {
                        TlsInterception::Off => ProtoTlsInterception::Off,
                        TlsInterception::On => ProtoTlsInterception::On,
                    }
                });
        Ok(Response::new(GetStatsResponse {
            tcp_active_connections: self.stats.tcp_active(),
            tcp_total_connections: self.stats.tcp_total(),
            udp_active_mappings: self.stats.udp_active(),
            bytes_forwarded: self.stats.bytes_forwarded(),
            network_tx_bytes: self.stats.network_tx_bytes(),
            network_rx_bytes: self.stats.network_rx_bytes(),
            tcp_denied_connections: self.stats.tcp_denied(),
            network_state: snapshot.mode as i32,
            installed_policy_ref: snapshot.installed_policy_ref,
            last_network_error: snapshot.last_error,
            udp_pending_datagrams: self.stats.udp_pending_datagrams(),
            udp_pending_bytes: self.stats.udp_pending_bytes(),
            tcp_pending_connections: snapshot.pending_tcp_connections,
            last_transition_unix_ms: snapshot.last_transition_unix_ms,
            current_retry_backoff_ms: snapshot.current_retry_backoff_ms,
            last_hydration_latency_ms: snapshot.last_hydration_latency_ms,
            last_network_error_kind: snapshot.last_error_kind as i32,
            consecutive_hydration_failures: snapshot.consecutive_hydration_failures,
            debug_enabled: self.debug_state.is_enabled(),
            build_git_sha: pplx_space_observability::git_sha().to_string(),
            identity_profile_version: IDENTITY_PROFILE_VERSION,
            route_verification_key_generation: self
                .service_port_ingress
                .route_verification()
                .generation(),
            tls_interception_mode: tls_interception_mode as i32,
            interception_readiness: proto_interception_readiness(snapshot.interception_readiness)
                as i32,
        }))
    }

    async fn service_port_ingress(
        &self,
        request: Request<tonic::Streaming<ServicePortIngressFrame>>,
    ) -> Result<Response<Self::ServicePortIngressStream>, Status> {
        self.authorize_peer(&request, "ServicePortIngress")?;
        let metadata = request.metadata().clone();
        let inbound = request.into_inner();
        open_service_port_ingress(
            self.service_port_ingress.clone(),
            self.network_state.clone(),
            metadata,
            inbound,
            self.shutdown_token.child_token(),
        )
        .await
    }

    async fn set_route_identity(
        &self,
        request: Request<SetRouteIdentityRequest>,
    ) -> Result<Response<SetRouteIdentityResponse>, Status> {
        self.authorize_peer(&request, "SetRouteIdentity")?;
        let identity = route_identity_from_proto(request.into_inner().identity)
            .map_err(status_for_route_identity_parse)?;
        let Some(identity) = identity else {
            return Err(Status::invalid_argument(
                "SetRouteIdentity requires route identity",
            ));
        };
        self.service_port_ingress
            .update_identity(identity)
            .map_err(status_for_route_identity_update)?;
        Ok(Response::new(SetRouteIdentityResponse {
            identity_profile_version: IDENTITY_PROFILE_VERSION,
        }))
    }

    async fn set_route_verification_keys(
        &self,
        request: Request<SetRouteVerificationKeysRequest>,
    ) -> Result<Response<SetRouteVerificationKeysResponse>, Status> {
        self.authorize_peer(&request, "SetRouteVerificationKeys")?;
        let request = request.into_inner();
        let cluster_id = request
            .cluster_id
            .parse()
            .map_err(|_| Status::invalid_argument("cluster_id is invalid"))?;
        let keys = request
            .keys
            .into_iter()
            .map(|key| verification_key_from_wire(key.key_id, key.ed25519_public_key))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let update = self
            .service_port_ingress
            .route_verification()
            .update(
                request
                    .generation
                    .try_into()
                    .map_err(|error: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error| {
                        Status::invalid_argument(error.to_string())
                    })?,
                request
                    .issuer
                    .parse()
                    .map_err(|error: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error| {
                        Status::invalid_argument(error.to_string())
                    })?,
                cluster_id,
                keys,
            )
            .map_err(status_for_route_verification_update)?;
        Ok(Response::new(SetRouteVerificationKeysResponse {
            generation: update.generation().get(),
            identity_profile_version: IDENTITY_PROFILE_VERSION,
        }))
    }

    async fn get_network_state(
        &self,
        request: Request<GetNetworkStateRequest>,
    ) -> Result<Response<GetNetworkStateResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "GetNetworkState", ALLOW_NET_OBSERVER)?;
        let snapshot = self.network_state.snapshot().await;
        Ok(Response::new(GetNetworkStateResponse {
            state: snapshot.mode as i32,
            installed_policy_ref: snapshot.installed_policy_ref,
            last_error: snapshot.last_error,
            last_hydration_latency_ms: snapshot.last_hydration_latency_ms,
            last_transition_unix_ms: snapshot.last_transition_unix_ms,
            pending_tcp_connections: snapshot.pending_tcp_connections,
            pending_udp_datagrams: self.stats.udp_pending_datagrams(),
            pending_udp_bytes: self.stats.udp_pending_bytes(),
            current_retry_backoff_ms: snapshot.current_retry_backoff_ms,
            last_error_kind: snapshot.last_error_kind as i32,
            consecutive_hydration_failures: snapshot.consecutive_hydration_failures,
            debug_enabled: self.debug_state.is_enabled(),
            build_git_sha: pplx_space_observability::git_sha().to_string(),
            interception_readiness: proto_interception_readiness(snapshot.interception_readiness)
                as i32,
        }))
    }

    async fn refresh_network_policy(
        &self,
        request: Request<RefreshNetworkPolicyRequest>,
    ) -> Result<Response<RefreshNetworkPolicyResponse>, Status> {
        self.authorize_peer(&request, "RefreshNetworkPolicy")?;
        self.network_state.record_owner_contact();
        let req = request.into_inner();
        let expected_policy_ref = parse_expected_policy_ref(&req.expected_policy_ref)?;
        let reason = NetworkStateReason::from_external(&req.reason);
        // Cache pruning + TCP resweep run inside the state's post-install hook
        // (wired at construction), so every install path reconciles — no explicit
        // call needed here.
        let snapshot = self
            .network_state
            .refresh(self.policy_loader.clone(), expected_policy_ref, reason)
            .await;
        Ok(Response::new(RefreshNetworkPolicyResponse {
            state: snapshot.mode as i32,
            installed_policy_ref: snapshot.installed_policy_ref,
            last_error: snapshot.last_error,
            last_error_kind: snapshot.last_error_kind as i32,
            consecutive_hydration_failures: snapshot.consecutive_hydration_failures,
            interception_readiness: proto_interception_readiness(snapshot.interception_readiness)
                as i32,
        }))
    }

    async fn set_no_identity(
        &self,
        request: Request<SetNoIdentityRequest>,
    ) -> Result<Response<SetNoIdentityResponse>, Status> {
        self.authorize_peer(&request, "SetNoIdentity")?;
        self.network_state.record_owner_contact();
        let req = request.into_inner();
        let snapshot = self
            .network_state
            .set_no_identity(NetworkStateReason::from_external(&req.reason))
            .await;
        Ok(Response::new(SetNoIdentityResponse {
            state: snapshot.mode as i32,
            installed_policy_ref: snapshot.installed_policy_ref,
            last_error: snapshot.last_error,
            last_error_kind: snapshot.last_error_kind as i32,
            consecutive_hydration_failures: snapshot.consecutive_hydration_failures,
        }))
    }

    async fn get_network_policy(
        &self,
        request: Request<GetNetworkPolicyRequest>,
    ) -> Result<Response<GetNetworkPolicyResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "GetNetworkPolicy", ALLOW_NET_OBSERVER)?;
        let snapshot = self.network_state.snapshot().await;
        let Some(policy) = snapshot.policy else {
            return Err(GatewayControlError::NetworkPolicyNotReady {
                state: snapshot.mode.as_str(),
                error_kind: snapshot.last_error_kind.as_str(),
                error: snapshot.last_error,
            }
            .into());
        };
        Ok(Response::new(GetNetworkPolicyResponse {
            policy: Some(ProtoNetworkPolicyV1::from(policy.as_ref())),
            installed_policy_ref: snapshot.installed_policy_ref,
        }))
    }

    async fn enable_debug(
        &self,
        request: Request<EnableDebugRequest>,
    ) -> Result<Response<EnableDebugResponse>, Status> {
        let peer = self.authorize_peer(&request, "EnableDebug")?;
        let requested_until_unix_ms = request.into_inner().debug_until_unix_ms;
        let effective_until_unix_ms = self.debug_state.set_deadline(requested_until_unix_ms);
        let enabled = self.debug_state.is_enabled();
        info!(
            enabled,
            requested_until_unix_ms,
            effective_until_unix_ms,
            peer_principal = peer.principal().as_str(),
            peer_uid = peer.identity().uid(),
            peer_gid = peer.identity().gid(),
            peer_pid = ?peer.identity().pid(),
            "net-gateway debug logging toggled"
        );
        Ok(Response::new(EnableDebugResponse {
            enabled,
            debug_until_unix_ms: effective_until_unix_ms,
        }))
    }
}

fn proto_interception_readiness(readiness: InterceptionReadiness) -> ProtoInterceptionReadiness {
    match readiness {
        InterceptionReadiness::NotRequired => ProtoInterceptionReadiness::NotRequired,
        InterceptionReadiness::Ready => ProtoInterceptionReadiness::Ready,
        InterceptionReadiness::DependenciesNotReady => {
            ProtoInterceptionReadiness::DependenciesNotReady
        }
    }
}

fn parse_expected_policy_ref(
    value: &str,
) -> Result<Option<EffectiveNetworkPolicyRef>, GatewayControlError> {
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse()
        .map(Some)
        .map_err(GatewayControlError::InvalidPolicyRef)
}

#[cfg(test)]
mod tests;
