use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use pplx_space_common_grpc::SandboxTraceContext;
use pplx_space_common_svc::core::guest::linux::network::IPV4_ADDR as GUEST_IPV4_ADDR;
use pplx_space_common_svc::core::types::{
    ExecutionGeneration, GuestPort, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SpaceApiRouteIdentity,
};
use pplx_space_common_svc::space_api_route::{
    SpaceApiRouteIdentityDimension, SpaceApiRouteIdentityMatchError, SpaceApiRouteProtocol,
    SpaceApiRoutePurpose, match_present_route_identity_dimensions,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Error,
    space_api_route_ed25519_token_from_metadata,
};
use pplx_space_metrics::gateway::{ServicePortIngressCloseReason, ServicePortIngressOpenResult};
use pplx_space_net_gateway_client::SERVICE_PORT_INGRESS_START_TIMEOUT;
use pplx_space_net_gateway_client::proto::{
    NetworkState, SandboxSubstrateKind, ServicePortIngressFrame, ServicePortIngressStart,
    SpaceApiRouteIdentity as ProtoSpaceApiRouteIdentity, service_port_ingress_frame,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{Status, metadata::MetadataMap};
use tracing::Instrument;

use crate::resource_budget::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS;
use crate::route_verification::RouteVerificationState;
use crate::state::GatewayNetworkState;

const INGRESS_BUFFER_BYTES: usize = 16 * 1024;
const INGRESS_CHANNEL_CAPACITY: usize = 32;
const SERVICE_PORT_INGRESS_GUEST_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub use pplx_space_net_gateway_client::{
    DEFAULT_SERVICE_PORT_INGRESS_IDLE_TIMEOUT_SECS, service_port_ingress_idle_timeout_from_env,
};
pub struct ServicePortIngressStream {
    inner: ReceiverStream<Result<ServicePortIngressFrame, Status>>,
    pump: JoinHandle<()>,
    _admission: OwnedSemaphorePermit,
}

impl ServicePortIngressStream {
    fn new(
        inner: ReceiverStream<Result<ServicePortIngressFrame, Status>>,
        pump: JoinHandle<()>,
        admission: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            inner,
            pump,
            _admission: admission,
        }
    }
}

impl Stream for ServicePortIngressStream {
    type Item = Result<ServicePortIngressFrame, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Drop for ServicePortIngressStream {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

#[derive(Clone, Debug)]
pub struct ServicePortIngressState {
    trusted_sandbox_id: Option<SandboxId>,
    identity_epoch: Arc<Mutex<(Option<SpaceApiRouteIdentity>, CancellationToken)>>,
    route_verification: RouteVerificationState,
    stream_slots: Arc<Semaphore>,
}

impl ServicePortIngressState {
    pub fn new(identity: Option<SpaceApiRouteIdentity>) -> Self {
        Self::new_inner(None, identity)
    }

    pub fn new_for_sandbox(
        trusted_sandbox_id: SandboxId,
        mut identity: Option<SpaceApiRouteIdentity>,
    ) -> Self {
        if let Some(identity) = identity.as_mut() {
            identity.sandbox_id = Some(trusted_sandbox_id);
        }
        Self::new_inner(Some(trusted_sandbox_id), identity)
    }

    fn new_inner(
        trusted_sandbox_id: Option<SandboxId>,
        identity: Option<SpaceApiRouteIdentity>,
    ) -> Self {
        Self {
            trusted_sandbox_id,
            identity_epoch: Arc::new(Mutex::new((identity, CancellationToken::new()))),
            route_verification: RouteVerificationState::default(),
            stream_slots: Arc::new(Semaphore::new(MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS)),
        }
    }

    fn try_acquire_stream_slot(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.stream_slots.clone().try_acquire_owned().map_err(|_| {
            pplx_space_metrics::gateway::service_port_ingress_open(
                ServicePortIngressOpenResult::CapacityExceeded,
            );
            Status::resource_exhausted("too many active service port ingress streams")
        })
    }

    pub fn update_identity(
        &self,
        mut identity: SpaceApiRouteIdentity,
    ) -> Result<RouteIdentityUpdate, RouteIdentityUpdateError> {
        if let Some(trusted_sandbox_id) = self.trusted_sandbox_id {
            if identity
                .sandbox_id
                .is_some_and(|sandbox_id| sandbox_id != trusted_sandbox_id)
            {
                return Err(RouteIdentityUpdateError::SandboxMismatch);
            }
            identity.sandbox_id = Some(trusted_sandbox_id);
        }
        let mut current = self
            .identity_epoch
            .lock()
            .map_err(|_| RouteIdentityUpdateError::StatePoisoned)?;
        let Some(incoming_generation) = identity.route_generation else {
            return Err(RouteIdentityUpdateError::MissingRouteGeneration);
        };

        let Some(existing) = current.0.as_ref() else {
            current.1.cancel();
            *current = (Some(identity), CancellationToken::new());
            return Ok(RouteIdentityUpdate::Updated);
        };
        let Some(existing_generation) = existing.route_generation else {
            current.1.cancel();
            *current = (Some(identity), CancellationToken::new());
            return Ok(RouteIdentityUpdate::Updated);
        };

        match incoming_generation.get().cmp(&existing_generation.get()) {
            std::cmp::Ordering::Less => {
                return Err(RouteIdentityUpdateError::StaleRouteGeneration {
                    current: existing_generation,
                    incoming: incoming_generation,
                });
            }
            std::cmp::Ordering::Greater => {
                current.1.cancel();
                *current = (Some(identity), CancellationToken::new());
                return Ok(RouteIdentityUpdate::Updated);
            }
            std::cmp::Ordering::Equal => {}
        }

        match merge_same_generation_identity(existing, &identity)? {
            SameGenerationMerge::Enriched => {
                current.1.cancel();
                *current = (Some(identity), CancellationToken::new());
                Ok(RouteIdentityUpdate::Updated)
            }
            SameGenerationMerge::AttemptRotated => {
                // The execution did not move — same generation, node,
                // substrate, and pod — so streams admitted under the previous
                // attempt stay valid; only new opens verify against the
                // rotated attempt.
                current.0 = Some(identity);
                Ok(RouteIdentityUpdate::Updated)
            }
            SameGenerationMerge::Unchanged => Ok(RouteIdentityUpdate::Unchanged),
        }
    }

    /// Snapshot the installed identity and an epoch token cancelled on replacement.
    pub fn identity_with_epoch(
        &self,
    ) -> Result<(Option<SpaceApiRouteIdentity>, CancellationToken), Status> {
        self.identity_epoch
            .lock()
            .map(|epoch| (epoch.0.clone(), epoch.1.child_token()))
            .map_err(|_| {
                Status::internal("ServicePortIngress route identity epoch mutex is poisoned")
            })
    }

    #[cfg(test)]
    fn identity(&self) -> Option<SpaceApiRouteIdentity> {
        self.identity_with_epoch()
            .ok()
            .and_then(|(identity, _)| identity)
    }

    pub fn route_verification(&self) -> &RouteVerificationState {
        &self.route_verification
    }
}

impl Default for ServicePortIngressState {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityUpdate {
    Updated,
    Unchanged,
}

#[derive(Debug, thiserror::Error)]
pub enum RouteIdentityUpdateError {
    #[error("route identity state mutex poisoned")]
    StatePoisoned,
    #[error("route identity update requires route_generation")]
    MissingRouteGeneration,
    #[error("route identity targets a different sandbox")]
    SandboxMismatch,
    #[error(
        "stale route identity update: current route_generation={current}, incoming route_generation={incoming}"
    )]
    StaleRouteGeneration {
        current: RouteGeneration,
        incoming: RouteGeneration,
    },
    #[error("conflicting route identity update for route_generation={route_generation}: {field}")]
    Conflict {
        route_generation: RouteGeneration,
        field: &'static str,
    },
}

pub fn status_for_route_identity_update(error: RouteIdentityUpdateError) -> Status {
    match error {
        RouteIdentityUpdateError::StatePoisoned => Status::internal(error.to_string()),
        RouteIdentityUpdateError::MissingRouteGeneration => {
            Status::invalid_argument(error.to_string())
        }
        RouteIdentityUpdateError::SandboxMismatch
        | RouteIdentityUpdateError::StaleRouteGeneration { .. }
        | RouteIdentityUpdateError::Conflict { .. } => {
            Status::failed_precondition(error.to_string())
        }
    }
}

/// How an accepted same-generation identity update differs from the resident
/// identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SameGenerationMerge {
    Unchanged,
    /// A previously absent dimension was filled in. The identity became
    /// strictly more specific, so streams admitted against the weaker
    /// identity must not survive it.
    Enriched,
    /// Only the activation attempt was replaced. The control plane mints the
    /// activation attempt and rotates it on a wake whose lifecycle target is
    /// already satisfied; the runtime adopts the rotation and republishes it
    /// over this peer-authorized control socket, so the gateway has no basis
    /// to refuse it. Every dimension that describes where the execution
    /// lives stays immutable within a generation.
    AttemptRotated,
}

fn merge_same_generation_identity(
    current: &SpaceApiRouteIdentity,
    incoming: &SpaceApiRouteIdentity,
) -> Result<SameGenerationMerge, RouteIdentityUpdateError> {
    let route_generation = incoming
        .route_generation
        .expect("same-generation merge only runs after route_generation validation");
    let mut filled = false;
    filled |= merge_identity_field(
        route_generation,
        "sandbox_id",
        current.sandbox_id.as_ref(),
        incoming.sandbox_id.as_ref(),
    )?;
    filled |= merge_identity_field(
        route_generation,
        "current_node_id",
        current.current_node_id.as_ref(),
        incoming.current_node_id.as_ref(),
    )?;
    filled |= merge_identity_field(
        route_generation,
        "substrate_kind",
        current.substrate_kind.as_ref(),
        incoming.substrate_kind.as_ref(),
    )?;
    filled |= merge_identity_field(
        route_generation,
        "execution_generation",
        current.execution_generation.as_ref(),
        incoming.execution_generation.as_ref(),
    )?;
    filled |= merge_identity_field(
        route_generation,
        "pod_uid",
        current.pod_uid.as_ref(),
        incoming.pod_uid.as_ref(),
    )?;
    let attempt_rotated = match (
        current.activation_attempt_id.as_ref(),
        incoming.activation_attempt_id.as_ref(),
    ) {
        (Some(current), Some(incoming)) => current != incoming,
        (Some(_), None) => {
            return Err(RouteIdentityUpdateError::Conflict {
                route_generation,
                field: "activation_attempt_id",
            });
        }
        (None, Some(_)) => {
            filled = true;
            false
        }
        (None, None) => false,
    };
    Ok(if filled {
        SameGenerationMerge::Enriched
    } else if attempt_rotated {
        SameGenerationMerge::AttemptRotated
    } else {
        SameGenerationMerge::Unchanged
    })
}

fn merge_identity_field<T: Eq>(
    route_generation: RouteGeneration,
    field: &'static str,
    current: Option<&T>,
    incoming: Option<&T>,
) -> Result<bool, RouteIdentityUpdateError> {
    match (current, incoming) {
        (Some(current), Some(incoming)) if current == incoming => Ok(false),
        (Some(_), Some(_)) | (Some(_), None) => Err(RouteIdentityUpdateError::Conflict {
            route_generation,
            field,
        }),
        (None, Some(_)) => Ok(true),
        (None, None) => Ok(false),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouteIdentityParseError {
    #[error("sandbox_id is invalid")]
    SandboxId,
    #[error("route_generation is invalid")]
    RouteGeneration,
    #[error("current_node_id is invalid")]
    CurrentNodeId,
    #[error("execution_generation is invalid")]
    ExecutionGeneration,
    #[error("pod_uid is invalid")]
    PodUid,
    #[error("activation_attempt_id is invalid")]
    ActivationAttemptId,
    #[error("substrate_kind is invalid")]
    SubstrateKind,
}

pub fn route_identity_from_proto(
    identity: Option<ProtoSpaceApiRouteIdentity>,
) -> Result<Option<SpaceApiRouteIdentity>, RouteIdentityParseError> {
    let Some(identity) = identity else {
        return Ok(None);
    };
    Ok(Some(SpaceApiRouteIdentity {
        sandbox_id: identity
            .sandbox_id
            .as_deref()
            .map(str::parse::<SandboxId>)
            .transpose()
            .map_err(|_| RouteIdentityParseError::SandboxId)?,
        route_generation: identity
            .route_generation
            .map(RouteGeneration::try_from)
            .transpose()
            .map_err(|_| RouteIdentityParseError::RouteGeneration)?,
        current_node_id: identity
            .current_node_id
            .as_deref()
            .map(str::parse::<NodeId>)
            .transpose()
            .map_err(|_| RouteIdentityParseError::CurrentNodeId)?,
        substrate_kind: match SandboxSubstrateKind::try_from(identity.substrate_kind)
            .map_err(|_| RouteIdentityParseError::SubstrateKind)?
        {
            SandboxSubstrateKind::Unspecified => None,
            SandboxSubstrateKind::Direct => Some(NodeManagedBy::Space),
            SandboxSubstrateKind::Kubernetes => Some(NodeManagedBy::Kubernetes),
        },
        execution_generation: identity
            .execution_generation
            .map(|value| {
                ExecutionGeneration::new(value)
                    .map_err(|_| RouteIdentityParseError::ExecutionGeneration)
            })
            .transpose()?,
        pod_uid: identity
            .pod_uid
            .as_deref()
            .map(PodUid::parse)
            .transpose()
            .map_err(|_| RouteIdentityParseError::PodUid)?,
        activation_attempt_id: identity
            .activation_attempt_id
            .as_deref()
            .map(str::parse::<SandboxActivationAttemptId>)
            .transpose()
            .map_err(|_| RouteIdentityParseError::ActivationAttemptId)?,
    }))
}

pub fn route_identity_to_proto(
    identity: &SpaceApiRouteIdentity,
) -> Option<ProtoSpaceApiRouteIdentity> {
    (!identity.is_absent()).then(|| ProtoSpaceApiRouteIdentity {
        sandbox_id: identity.sandbox_id.map(|value| value.to_string()),
        route_generation: identity.route_generation.map(RouteGeneration::get),
        current_node_id: identity.current_node_id.map(|value| value.to_string()),
        substrate_kind: match identity.substrate_kind {
            None => SandboxSubstrateKind::Unspecified as i32,
            Some(NodeManagedBy::Space) => SandboxSubstrateKind::Direct as i32,
            Some(NodeManagedBy::Kubernetes) => SandboxSubstrateKind::Kubernetes as i32,
        },
        execution_generation: identity.execution_generation.map(ExecutionGeneration::get),
        pod_uid: identity.pod_uid.as_ref().map(ToString::to_string),
        activation_attempt_id: identity
            .activation_attempt_id
            .map(|value| value.to_string()),
    })
}

pub async fn open_service_port_ingress(
    state: ServicePortIngressState,
    network_state: GatewayNetworkState,
    metadata: MetadataMap,
    mut inbound: tonic::Streaming<ServicePortIngressFrame>,
    shutdown_token: CancellationToken,
) -> Result<tonic::Response<ServicePortIngressStream>, Status> {
    let admission = state.try_acquire_stream_slot()?;
    let first = tokio::select! {
        _ = shutdown_token.cancelled() => return Err(Status::cancelled(
            "service port ingress shut down before its start frame",
        )),
        result = tokio::time::timeout(SERVICE_PORT_INGRESS_START_TIMEOUT, inbound.message()) => {
            match result {
                Ok(result) => result?,
                Err(_) => {
                    pplx_space_metrics::gateway::service_port_ingress_open(
                        ServicePortIngressOpenResult::StartTimeout,
                    );
                    return Err(Status::deadline_exceeded(
                        "service port ingress start frame timed out",
                    ));
                }
            }
        }
    }
    .ok_or_else(|| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::InvalidStart,
        );
        Status::invalid_argument("service port ingress start frame is required")
    })?;
    let start = match first.message {
        Some(service_port_ingress_frame::Message::Start(start)) => start,
        _ => {
            pplx_space_metrics::gateway::service_port_ingress_open(
                ServicePortIngressOpenResult::InvalidStart,
            );
            return Err(Status::invalid_argument(
                "service port ingress first frame must be start",
            ));
        }
    };
    // Missing opt-in uses ordinary stream termination for older callers.
    let downstream_completion = metadata
        .get(pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA)
        .is_some_and(|value| value == "1");
    let (port, identity_token, trace_context) =
        authorize_service_port_ingress(&state, &network_state, &metadata, &start).await?;
    trace_context.record(&tracing::Span::current());
    let upstream = connect_guest_endpoint(port, &trace_context).await?;
    pplx_space_metrics::gateway::service_port_ingress_open(ServicePortIngressOpenResult::Ok);

    let (tx, rx) = tokio::sync::mpsc::channel(INGRESS_CHANNEL_CAPACITY);
    let idle_timeout = service_port_ingress_idle_timeout_from_env();
    let pump = tokio::spawn(async move {
        let result = pump_service_port_ingress(
            inbound,
            upstream,
            tx.clone(),
            shutdown_token,
            identity_token,
            idle_timeout,
            downstream_completion,
        )
        .await;
        pplx_space_metrics::gateway::service_port_ingress_closed(
            service_port_ingress_close_reason(&result),
        );
        if let Err(error) = result {
            let _ = tx.send(Err(error)).await;
        }
    });
    let mut response = tonic::Response::new(ServicePortIngressStream::new(
        ReceiverStream::new(rx),
        pump,
        admission,
    ));
    if downstream_completion {
        response.metadata_mut().insert(
            pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA,
            tonic::metadata::MetadataValue::from_static("1"),
        );
    }
    Ok(response)
}

async fn authorize_service_port_ingress(
    state: &ServicePortIngressState,
    network_state: &GatewayNetworkState,
    metadata: &MetadataMap,
    start: &ServicePortIngressStart,
) -> Result<(GuestPort, CancellationToken, SandboxTraceContext), Status> {
    let network_snapshot = network_state.snapshot().await;
    // Policy installation only. Interception readiness is deliberately not a
    // condition here: the CA exists to inspect guest egress, and this path
    // relays inbound connections to a guest port without consulting it. An
    // earlier gateway refused ingress whenever the CA was missing, but only
    // because a closed traffic gate demoted this mode to PendingPolicy.
    if network_snapshot.mode != NetworkState::Ready {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::PolicyNotReady,
        );
        return Err(Status::failed_precondition("network policy is not ready"));
    }
    let sandbox_id = start.sandbox_id.parse::<SandboxId>().map_err(|_| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::InvalidStart,
        );
        Status::invalid_argument("sandbox_id must be a UUID")
    })?;
    let port = GuestPort::try_from_u16(u16::try_from(start.port).map_err(|_| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::InvalidStart,
        );
        Status::invalid_argument("service port out of range")
    })?)
    .map_err(|error| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::InvalidStart,
        );
        Status::invalid_argument(format!("service port {error}"))
    })?;
    let (local_identity, identity_token) = state.identity_with_epoch()?;

    let token = space_api_route_ed25519_token_from_metadata(metadata).map_err(|error| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::InvalidToken,
        );
        Status::permission_denied(format!("invalid SPACE API route token: {error}"))
    })?;
    let Some(token) = token else {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::MissingToken,
        );
        return Err(Status::permission_denied(
            "ServicePortIngress requires a SPACE API route token",
        ));
    };
    let claims = state.route_verification().verify(&token).map_err(|error| {
        pplx_space_metrics::gateway::service_port_ingress_open(route_token_ed25519_error_label(
            &error,
        ));
        Status::permission_denied(format!("invalid SPACE API route token: {error}"))
    })?;
    authorize_service_port_ingress_claims(local_identity.as_ref(), sandbox_id, port, &claims)?;
    let mut trace_context = SandboxTraceContext::new(sandbox_id, port);
    if let Some(identity) = local_identity.as_ref() {
        trace_context.route_generation = identity.route_generation;
        trace_context.execution_generation = identity.execution_generation;
        trace_context.current_node_id = identity.current_node_id;
    }
    Ok((port, identity_token, trace_context))
}

fn authorize_service_port_ingress_claims(
    local_identity: Option<&SpaceApiRouteIdentity>,
    sandbox_id: SandboxId,
    port: GuestPort,
    claims: &SpaceApiRouteEd25519Claims,
) -> Result<(), Status> {
    let rejected = |result, message: &str| {
        pplx_space_metrics::gateway::service_port_ingress_open(result);
        Err(Status::permission_denied(message))
    };
    if claims.route_identity.sandbox_id() != sandbox_id {
        return rejected(
            ServicePortIngressOpenResult::SandboxMismatch,
            "ServicePortIngress token targets a different sandbox",
        );
    }
    if claims.purpose != SpaceApiRoutePurpose::ServicePortIngress {
        return rejected(
            ServicePortIngressOpenResult::PurposeMismatch,
            "ServicePortIngress token purpose is not allowed",
        );
    }
    if claims.service_port != Some(port) {
        return rejected(
            ServicePortIngressOpenResult::ServicePortMismatch,
            "ServicePortIngress token targets a different service port",
        );
    }
    if claims.protocol != Some(SpaceApiRouteProtocol::Tcp) {
        return rejected(
            ServicePortIngressOpenResult::InvalidToken,
            "ServicePortIngress token protocol is not allowed",
        );
    }
    let local_identity = local_identity.ok_or_else(|| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::IdentityMissing,
        );
        Status::permission_denied("ServicePortIngress has no local route identity")
    })?;
    match_present_route_identity_dimensions(local_identity, &claims.route_identity).map_err(
        |error| {
            pplx_space_metrics::gateway::service_port_ingress_open(
                service_port_identity_mismatch_result(error),
            );
            Status::permission_denied(format!(
                "ServicePortIngress route identity is not authorized: {error}"
            ))
        },
    )?;
    local_identity.clone().try_into_complete().map_err(|_| {
        pplx_space_metrics::gateway::service_port_ingress_open(
            ServicePortIngressOpenResult::IdentityMissing,
        );
        Status::permission_denied("ServicePortIngress local route identity is incomplete")
    })?;
    Ok(())
}

fn service_port_identity_mismatch_result(
    error: SpaceApiRouteIdentityMatchError,
) -> ServicePortIngressOpenResult {
    match error {
        SpaceApiRouteIdentityMatchError::Mismatch(dimension) => match dimension {
            SpaceApiRouteIdentityDimension::SandboxId => {
                ServicePortIngressOpenResult::SandboxMismatch
            }
            SpaceApiRouteIdentityDimension::RouteGeneration => {
                ServicePortIngressOpenResult::RouteGenerationMismatch
            }
            SpaceApiRouteIdentityDimension::CurrentNodeId => {
                ServicePortIngressOpenResult::NodeMismatch
            }
            SpaceApiRouteIdentityDimension::SubstrateKind => {
                ServicePortIngressOpenResult::SubstrateMismatch
            }
            SpaceApiRouteIdentityDimension::ExecutionGeneration => {
                ServicePortIngressOpenResult::ExecutionGenerationMismatch
            }
            SpaceApiRouteIdentityDimension::ActivationAttemptId => {
                ServicePortIngressOpenResult::ActivationAttemptMismatch
            }
            SpaceApiRouteIdentityDimension::PodUid => ServicePortIngressOpenResult::PodUidMismatch,
        },
    }
}

fn route_token_ed25519_error_label(
    error: &SpaceApiRouteEd25519Error,
) -> ServicePortIngressOpenResult {
    match error {
        SpaceApiRouteEd25519Error::Expired => ServicePortIngressOpenResult::Expired,
        SpaceApiRouteEd25519Error::MissingVerificationKeys => {
            ServicePortIngressOpenResult::VerifierSecretUnset
        }
        _ => ServicePortIngressOpenResult::InvalidToken,
    }
}

async fn connect_guest_endpoint(
    port: GuestPort,
    trace_context: &SandboxTraceContext,
) -> Result<TcpStream, Status> {
    let guest_ip = GUEST_IPV4_ADDR.parse::<IpAddr>().map_err(|error| {
        Status::internal(format!(
            "guest ServicePort ingress address is invalid: {error}"
        ))
    })?;
    let endpoint = SocketAddr::new(guest_ip, port.get().get());
    let span = tracing::info_span!(
        "service_port_ingress.connect_guest",
        sandbox_id = tracing::field::Empty,
        sandbox_port = tracing::field::Empty,
        route_generation = tracing::field::Empty,
        execution_generation = tracing::field::Empty,
        current_node_id = tracing::field::Empty,
            traceparent = tracing::field::Empty,
        failure_origin = tracing::field::Empty,
        failure_reason = tracing::field::Empty,
        otel.kind = "client",
        "operation.name" = "tcp.client.connect",
        network.peer.address = %endpoint.ip(),
        network.peer.port = endpoint.port(),
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    );
    trace_context.record(&span);
    let result = tokio::time::timeout(
        SERVICE_PORT_INGRESS_GUEST_CONNECT_TIMEOUT,
        TcpStream::connect(endpoint).instrument(span.clone()),
    )
    .await;
    match result {
        Ok(Ok(stream)) => {
            span.record("otel.status_code", "OK");
            Ok(stream)
        }
        Ok(Err(error)) => {
            let status = pplx_space_common_grpc::guest_service_connect_status(&error);
            let refused = pplx_space_common_grpc::record_guest_service_status(&status, &span);
            pplx_space_metrics::gateway::service_port_ingress_open(if refused {
                ServicePortIngressOpenResult::GuestConnectionRefused
            } else {
                ServicePortIngressOpenResult::UpstreamConnectError
            });
            if !refused {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_description", error.to_string().as_str());
            }
            Err(status)
        }
        Err(_) => {
            pplx_space_metrics::gateway::service_port_ingress_open(
                ServicePortIngressOpenResult::UpstreamConnectError,
            );
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", "connect timeout");
            Err(Status::deadline_exceeded(
                "service port ingress connect timed out",
            ))
        }
    }
}

async fn pump_service_port_ingress<I, U>(
    mut inbound: I,
    upstream: U,
    tx: tokio::sync::mpsc::Sender<Result<ServicePortIngressFrame, Status>>,
    shutdown_token: CancellationToken,
    identity_token: CancellationToken,
    idle_timeout: Duration,
    downstream_completion: bool,
) -> Result<ServicePortIngressCloseReason, Status>
where
    I: Stream<Item = Result<ServicePortIngressFrame, Status>> + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream);
    let mut buffer = vec![0_u8; INGRESS_BUFFER_BYTES];
    let mut idle_deadline = Instant::now() + idle_timeout;
    let mut read_closed = false;
    let mut write_closed = false;

    loop {
        tokio::select! {
            frame = inbound.next() => {
                let Some(frame) = frame.transpose()? else {
                    let shutdown_result = await_ingress_activity(
                        upstream_write.shutdown(),
                        idle_deadline,
                        &shutdown_token,
                        &identity_token,
                        "service port ingress shutdown timed out",
                    ).await?;
                    let Some(result) = shutdown_result else {
                        return Ok(ServicePortIngressCloseReason::ServiceShutdown);
                    };
                    result.map_err(|error| {
                        Status::unavailable(format!("service port ingress shutdown failed: {error}"))
                    })?;
                    return Ok(ServicePortIngressCloseReason::ClientClosed);
                };
                match frame.message {
                Some(service_port_ingress_frame::Message::Complete(_)) => {
                    return Ok(ServicePortIngressCloseReason::ClientClosed);
                }
                Some(service_port_ingress_frame::Message::Eof(_)) => {
                    if write_closed {
                        return Err(Status::invalid_argument("duplicate ingress EOF"));
                    }
                    let result = await_ingress_activity(upstream_write.shutdown(), idle_deadline,
                        &shutdown_token, &identity_token, "service port ingress half-close timed out").await?;
                    let Some(result) = result else { return Ok(ServicePortIngressCloseReason::ServiceShutdown); };
                    result.map_err(|error| Status::unavailable(error.to_string()))?;
                    write_closed = true;
                }
                Some(service_port_ingress_frame::Message::Data(data)) => {
                    if write_closed {
                        return Err(Status::invalid_argument("ingress data after EOF"));
                    }
                    if data.is_empty() {
                        continue;
                    }
                    let write_result = await_ingress_activity(
                        upstream_write.write_all(&data),
                        idle_deadline,
                        &shutdown_token,
                        &identity_token,
                        "service port ingress write timed out",
                    ).await?;
                    let Some(result) = write_result else {
                        return Ok(ServicePortIngressCloseReason::ServiceShutdown);
                    };
                    result.map_err(|error| {
                        Status::unavailable(format!("service port ingress write failed: {error}"))
                    })?;
                    idle_deadline = Instant::now() + idle_timeout;
                }
                Some(service_port_ingress_frame::Message::Start(_)) => {
                    return Err(Status::invalid_argument(
                        "service port ingress start frame may only appear first",
                    ));
                }
                None => {}
                }
            }
            read = upstream_read.read(&mut buffer), if !read_closed => {
                let read = read.map_err(|error| {
                    Status::unavailable(format!("service port ingress read failed: {error}"))
                })?;
                if read == 0 {
                    if !downstream_completion {
                        return Ok(ServicePortIngressCloseReason::TransportLoss);
                    }
                    read_closed = true;
                    let sent = await_ingress_activity(
                        tx.send(Ok(ServicePortIngressFrame {
                            message: Some(service_port_ingress_frame::Message::Eof(())),
                        })), idle_deadline, &shutdown_token, &identity_token,
                        "service port ingress EOF send timed out",
                    ).await?;
                    if sent.is_none() {
                        return Ok(ServicePortIngressCloseReason::ServiceShutdown);
                    }
                    if sent.is_some_and(|result| result.is_err()) {
                        return Err(Status::cancelled("service port ingress client disconnected"));
                    }
                    continue;
                }
                let Some(data) = buffer.get(..read) else {
                    return Err(Status::internal("invalid service port ingress read"));
                };
                let send_result = await_ingress_activity(
                    tx.send(Ok(ServicePortIngressFrame {
                    message: Some(service_port_ingress_frame::Message::Data(data.to_vec())),
                    })),
                    idle_deadline,
                    &shutdown_token,
                    &identity_token,
                    "service port ingress response send timed out",
                ).await?;
                let Some(result) = send_result else {
                    return Ok(ServicePortIngressCloseReason::ServiceShutdown);
                };
                result.map_err(|_| Status::cancelled("service port ingress client disconnected"))?;
                idle_deadline = Instant::now() + idle_timeout;
            }
            _ = tokio::time::sleep_until(idle_deadline), if !read_closed => {
                return Err(Status::deadline_exceeded("service port ingress stream idle timed out"));
            }
            _ = shutdown_token.cancelled() => {
                return Ok(ServicePortIngressCloseReason::ServiceShutdown);
            }
            _ = identity_token.cancelled() => {
                return Err(Status::aborted(
                    "service port ingress execution identity changed",
                ));
            }
        }
    }
}

fn service_port_ingress_close_reason(
    result: &Result<ServicePortIngressCloseReason, Status>,
) -> ServicePortIngressCloseReason {
    match result {
        Ok(reason) => *reason,
        Err(status) => match status.code() {
            tonic::Code::Aborted => ServicePortIngressCloseReason::IdentityChanged,
            tonic::Code::DeadlineExceeded => ServicePortIngressCloseReason::IdleTimeout,
            tonic::Code::Cancelled => ServicePortIngressCloseReason::ClientClosed,
            tonic::Code::InvalidArgument => ServicePortIngressCloseReason::ProtocolError,
            _ => ServicePortIngressCloseReason::TransportLoss,
        },
    }
}

async fn await_ingress_activity<F, T>(
    operation: F,
    idle_deadline: Instant,
    shutdown_token: &CancellationToken,
    identity_token: &CancellationToken,
    timeout_message: &'static str,
) -> Result<Option<T>, Status>
where
    F: Future<Output = T>,
{
    tokio::select! {
        result = operation => Ok(Some(result)),
        _ = tokio::time::sleep_until(idle_deadline) => {
            Err(Status::deadline_exceeded(timeout_message))
        }
        _ = shutdown_token.cancelled() => Ok(None),
        _ = identity_token.cancelled() => {
            Err(Status::aborted("service port ingress execution identity changed"))
        }
    }
}

pub fn status_for_route_identity_parse(error: RouteIdentityParseError) -> Status {
    Status::invalid_argument(error.to_string())
}

#[cfg(test)]
mod tests;
