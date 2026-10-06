//! Transport-aware caller resolution and per-RPC authorization.
//!
//! credmgr serves two listener shapes. Each first resolves to a single
//! [`ResolvedCaller`] ([`CallerResolver::resolve`]); the per-RPC policy check
//! ([`CallerResolver::authorize_caller`]) then runs uniformly over that
//! resolved identity. Both live on [`CallerResolver`] because it holds the one
//! piece of configured state the UDS path needs: the [`UdsPeerAuthorizer`],
//! which owns both the uid→principal table and the per-account `/proc`
//! peer-process checks applied after the allow-list. A UDS caller resolves to
//! just its kernel-attested `SO_PEERCRED` credentials; classification to a
//! [`UdsPeerPrincipal`] happens inside the policy check
//! (`UdsPeerAuthorizer::authorize_identity`), so the allow-list decision and
//! the process checks always share one classification of the same identity.
//! The TCP arms of the policy decision stay pure functions of the resolved
//! caller and the per-operation allow-lists.
//!
//! - **Unix socket** (per-node deployments): Linux peer credentials,
//!   classified to a [`UdsPeerPrincipal`] at decision time via the
//!   uid→principal table held by [`CallerResolver`].
//! - **TCP** (the control-plane deployment on EKS): identity is established by
//!   [`crate::tcp_auth`] before the handler runs and injected into the request
//!   extensions as a [`TcpCallerIdentity`]:
//!   - a projected ServiceAccount JWT validated via apiserver `TokenReview`
//!     resolves to a [`K8sIdentity`] containing the caller service plus its
//!     Pod and node identities;
//!   - on non-Kubernetes plaintext profiles only, the shared static internal
//!     token resolves to the coarse internal principal.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use pplx_space_common_svc::UdsPeerAccount;
use pplx_space_common_svc::core::host::linux::identities::{user_names, users};
use tonic::{Request, Status};
use tower::{Layer, Service};

use pplx_space_common_svc::core::types::{K8sNodeName, K8sNodeUid, PodName, PodUid};
use pplx_space_common_svc::{
    PeerIdentity, UdsPeerAuthorizer, UdsPeerPrincipal, peer_identity_from_tonic_request,
};

use crate::tcp_auth::TcpCallerIdentity;

/// The services allowed to authenticate to credmgr over TCP. The wire form is
/// the Kubernetes ServiceAccount name each client runs as (which `TokenReview`
/// returns as `system:serviceaccount:<ns>:<name>`); it is also the DNS name
/// historically used on the now-removed client certificate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClientService {
    NodeManager,
    SandboxManager,
    VolumeManager,
    TemplateManager,
    BuildManager,
    ApiSvc,
    /// The node-local credmgr agent (`--role agent`, see [`crate::proxy`]),
    /// which forwards node-local callers' RPCs to this authority under its own
    /// credential. Its wire form is the credmgr DaemonSet's own ServiceAccount
    /// name, which is what `TokenReview` reports for the agent's token.
    NodeCredentialAgent,
}

/// Kubernetes caller identity established by TokenReview.
///
/// Every accepted projected Pod token carries all three identity dimensions.
/// Holding them together keeps an incomplete Kubernetes caller
/// unrepresentable. The node identity does not itself grant node-scoped
/// authority: consumers must separately require the exact node-scoped service
/// before using it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct K8sIdentity {
    service: ClientService,
    pod: K8sPodIdentity,
    node: K8sNodeIdentity,
}

/// Kubernetes Pod incarnation authenticated by a projected Pod token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct K8sPodIdentity {
    pod_name: PodName,
    pod_uid: PodUid,
}

impl K8sPodIdentity {
    pub fn pod_name(&self) -> &PodName {
        &self.pod_name
    }

    pub fn pod_uid(&self) -> &PodUid {
        &self.pod_uid
    }
}

/// Kubernetes node incarnation carried by a TokenReview-authenticated
/// projected Pod token.
///
/// The name identifies the SPACE node record; the UID fences a same-name
/// Kubernetes Node replacement. Consumers must compare both against current
/// authoritative state before granting node-scoped access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct K8sNodeIdentity {
    node_name: K8sNodeName,
    node_uid: K8sNodeUid,
}

impl K8sNodeIdentity {
    pub fn node_name(&self) -> &K8sNodeName {
        &self.node_name
    }

    pub fn node_uid(&self) -> &K8sNodeUid {
        &self.node_uid
    }
}

impl K8sIdentity {
    /// A synthetic projected-token identity for cross-service authorization tests.
    #[cfg(feature = "test-hooks")]
    pub fn for_test(service: ClientService) -> Self {
        Self::from_token_review(
            service,
            "test-pod",
            "test-pod-uid",
            "test-node",
            "test-node-uid",
        )
        .expect("valid synthetic Kubernetes identity")
    }

    pub(crate) fn from_token_review(
        service: ClientService,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
        node_uid: &str,
    ) -> Result<Self, InvalidK8sIdentity> {
        let pod_name = pod_name
            .parse::<PodName>()
            .map_err(|_| InvalidK8sIdentity)?;
        let pod_uid = pod_uid.parse::<PodUid>().map_err(|_| InvalidK8sIdentity)?;
        let node_name = node_name
            .parse::<K8sNodeName>()
            .map_err(|_| InvalidK8sIdentity)?;
        let node_uid = node_uid
            .parse::<K8sNodeUid>()
            .map_err(|_| InvalidK8sIdentity)?;
        Ok(Self {
            service,
            pod: K8sPodIdentity { pod_name, pod_uid },
            node: K8sNodeIdentity {
                node_name,
                node_uid,
            },
        })
    }

    pub fn service(&self) -> ClientService {
        self.service
    }

    pub fn pod(&self) -> &K8sPodIdentity {
        &self.pod
    }

    /// The authenticated node metadata. A node-scoped operation must require
    /// [`Self::service`] to be the expected node-scoped service before
    /// consuming this identity.
    pub fn node(&self) -> &K8sNodeIdentity {
        &self.node
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("projected Pod token lacks valid Pod or node identity metadata")]
pub(crate) struct InvalidK8sIdentity;

impl ClientService {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeManager => "space-node-manager",
            Self::SandboxManager => "space-sandbox-manager",
            Self::VolumeManager => "space-volume-manager",
            Self::TemplateManager => "space-template-manager",
            Self::BuildManager => "space-build-manager",
            Self::ApiSvc => "space-api",
            Self::NodeCredentialAgent => "space-credential-manager",
        }
    }

    pub(crate) const ALL: [Self; 7] = [
        Self::NodeManager,
        Self::SandboxManager,
        Self::VolumeManager,
        Self::TemplateManager,
        Self::BuildManager,
        Self::ApiSvc,
        Self::NodeCredentialAgent,
    ];
}

impl fmt::Display for ClientService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unrecognized credmgr client service {0:?}")]
pub struct ParseClientServiceError(String);

impl FromStr for ClientService {
    type Err = ParseClientServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|service| service.as_str() == s)
            .ok_or_else(|| ParseClientServiceError(s.to_string()))
    }
}

/// Who is on the other end of this request, fully resolved by the transport
/// layer. One variant per listener shape credmgr serves.
#[derive(Debug, Clone)]
pub enum ResolvedCaller {
    /// Unix socket peer (node-local managers), carrying only its
    /// kernel-attested `SO_PEERCRED` credentials. Deliberately no principal
    /// slot: the principal is derived from these credentials at decision time
    /// in [`CallerResolver::authorize_caller`], so a caller whose label
    /// disagrees with its identity is unrepresentable.
    UdsPeer(PeerIdentity),
    /// Cloud TCP caller that presented a projected Kubernetes ServiceAccount
    /// token, validated via TokenReview and mapped to a recognized credmgr
    /// client service.
    K8sJwt(K8sIdentity),
    /// Direct-node plaintext TCP caller (non-cloud `--allow-plaintext`
    /// profiles): authenticated by the shared internal token, with no
    /// per-service identity.
    Plaintext,
}

/// Resolves a UDS peer to a [`ResolvedCaller`]. Also holds the
/// [`UdsPeerAuthorizer`] the policy check needs: classification to a
/// principal is deferred to [`CallerResolver::authorize_caller`] so it
/// cannot diverge from the authorization decision.
#[derive(Clone)]
struct UdsPeerResolver {
    authorizer: UdsPeerAuthorizer,
}

impl UdsPeerResolver {
    /// `Some` when the request arrived over a Unix socket carrying peer
    /// credentials.
    fn resolve<T>(&self, request: &Request<T>) -> Option<ResolvedCaller> {
        Some(ResolvedCaller::UdsPeer(peer_identity_from_tonic_request(
            request,
        )?))
    }
}

/// Resolves a cloud TCP caller's Kubernetes ServiceAccount identity. Stateless:
/// [`crate::tcp_auth`] already validated the projected token via TokenReview
/// and injected the resolved [`K8sIdentity`] as a [`TcpCallerIdentity`].
#[derive(Clone, Copy)]
struct K8sJwtResolver;

impl K8sJwtResolver {
    /// `Some` when the auth layer injected a projected-token service identity.
    fn resolve<T>(&self, request: &Request<T>) -> Option<ResolvedCaller> {
        match request.extensions().get::<TcpCallerIdentity>()? {
            TcpCallerIdentity::K8sJwt(identity) => Some(ResolvedCaller::K8sJwt(identity.clone())),
            TcpCallerIdentity::Plaintext => None,
        }
    }
}

/// Resolves a direct-node plaintext TCP caller. Stateless and identity-free:
/// the auth layer authenticated it by the shared internal token (the only
/// credential on non-Kubernetes `--allow-plaintext` listeners).
#[derive(Clone, Copy)]
struct PlaintextResolver;

impl PlaintextResolver {
    /// `Some(Plaintext)` when the auth layer injected the internal-token
    /// principal (no per-service identity).
    fn resolve<T>(&self, request: &Request<T>) -> Option<ResolvedCaller> {
        match request.extensions().get::<TcpCallerIdentity>()? {
            TcpCallerIdentity::Plaintext => Some(ResolvedCaller::Plaintext),
            TcpCallerIdentity::K8sJwt(_) => None,
        }
    }
}

/// Resolves a request to a [`ResolvedCaller`] by trying each transport's
/// resolver in turn. One member per listener shape credmgr serves: UDS peer
/// credentials are read here directly; the TCP identity (`K8sJwt` or
/// `Plaintext`) was established and injected as a [`TcpCallerIdentity`] by
/// [`crate::tcp_auth`] before the handler ran.
#[derive(Clone)]
pub struct CallerResolver {
    uds: UdsPeerResolver,
    k8s_jwt: K8sJwtResolver,
    plaintext: PlaintextResolver,
}

impl CallerResolver {
    pub fn new(uds: UdsPeerAuthorizer) -> Self {
        Self {
            uds: UdsPeerResolver { authorizer: uds },
            k8s_jwt: K8sJwtResolver,
            plaintext: PlaintextResolver,
        }
    }

    /// Classify the request's transport into a fully-resolved caller. A
    /// request with neither UDS peer credentials nor an injected TCP identity
    /// is `Unauthenticated` — the auth layer must run before the handler on
    /// every TCP listener.
    pub fn resolve<T>(&self, request: &Request<T>) -> Result<ResolvedCaller, Status> {
        if let Some(caller) = self.uds.resolve(request) {
            return Ok(caller);
        }
        if let Some(caller) = self.k8s_jwt.resolve(request) {
            return Ok(caller);
        }
        if let Some(caller) = self.plaintext.resolve(request) {
            return Ok(caller);
        }
        Err(Status::unauthenticated(
            "request transport carries no peer identity",
        ))
    }

    /// Per-RPC authorization decision over an already-resolved caller. Records
    /// the peer principal on the current gRPC server span once identity is
    /// accepted.
    ///
    /// A method rather than a free function because the UDS arm's whole
    /// decision — uid→principal classification, the allow-list, and the
    /// per-account `/proc` peer-process checks — is configured on
    /// [`UdsPeerAuthorizer`], which only the resolver holds. The TCP arms
    /// remain pure functions of the caller and the allow-lists.
    pub fn authorize_caller(
        &self,
        caller: &ResolvedCaller,
        operation: &'static str,
        allow_uds: &[UdsPeerPrincipal],
        allow_services: &[ClientService],
    ) -> Result<(), Status> {
        match caller {
            // One classification for the whole decision:
            // `authorize_identity` derives the principal from the
            // kernel-attested identity, applies the allow-list first (an
            // unauthorized principal is denied on identity, not on a `/proc`
            // read), records `rpc.client.peer_principal` on the span, then
            // runs the matched account's process checks.
            ResolvedCaller::UdsPeer(identity) => self
                .uds
                .authorizer
                .authorize_identity(identity.clone(), operation, allow_uds)
                .map(|_| ()),
            ResolvedCaller::K8sJwt(identity) => {
                authorize_tcp_caller_service(identity.service(), operation, allow_services)
            }
            // The shared internal token authenticated the caller as the
            // cluster-internal principal; there is no finer-grained identity
            // to check against, so it is authorized for any operation it can
            // reach. Reachable only on direct-node plaintext listeners.
            ResolvedCaller::Plaintext => {
                tracing::Span::current().record("rpc.client.peer_principal", "internal-token");
                Ok(())
            }
        }
    }
}

/// Allow-list check for a TCP caller whose Kubernetes ServiceAccount identity
/// [`crate::tcp_auth`] already validated. Pure function of the service and the
/// per-operation allow-list.
fn authorize_tcp_caller_service(
    service: ClientService,
    operation: &'static str,
    allow_services: &[ClientService],
) -> Result<(), Status> {
    if allow_services.contains(&service) {
        tracing::Span::current().record("rpc.client.peer_principal", service.as_str());
        return Ok(());
    }
    tracing::warn!(
        operation,
        peer_service = service.as_str(),
        allowed_services = ?allow_services
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>(),
        "rejecting unauthorized TCP RPC"
    );
    Err(Status::permission_denied(format!(
        "service {service} is not authorized for {operation}"
    )))
}

/// Listener-local authorization runs before either the authority or agent can
/// handle a request. The upstream authority cannot recover the original UID
/// after the agent has re-originated a call with its own credential.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CredentialUdsAccess {
    Standard,
    Volume,
}

impl CredentialUdsAccess {
    fn authorize<T>(
        &self,
        path: &str,
        request: &Request<T>,
        authorizer: &UdsPeerAuthorizer,
    ) -> Result<(), Status> {
        if matches!(
            path,
            "/grpc.health.v1.Health/Check" | "/grpc.health.v1.Health/Watch"
        ) {
            return Ok(());
        }
        match self {
            Self::Standard => {
                // Preserve existing policies for other peers, while requiring
                // volume-manager to use its restricted listener even if it can
                // traverse the standard socket's shared client group.
                authorizer.authorize_request(
                    request,
                    "standard listener",
                    &[UdsPeerPrincipal::Root, UdsPeerPrincipal::Other],
                )?;
            }
            Self::Volume => {
                let operation = match path {
                    "/space.credential.manager.v1.CredentialManager/ExchangeToken" => {
                        "ExchangeToken"
                    }
                    "/space.credential.manager.v1.CredentialManager/IssueDek" => "IssueDek",
                    "/space.credential.manager.v1.CredentialManager/UnwrapDek" => "UnwrapDek",
                    _ => {
                        let peer = peer_identity_from_tonic_request(request);
                        tracing::warn!(
                            peer_uid = ?peer.as_ref().map(|identity| identity.uid()),
                            peer_pid = ?peer.as_ref().and_then(|identity| identity.pid()),
                            "rejecting operation outside the volume credential socket allowlist"
                        );
                        return Err(Status::permission_denied(
                            "operation is not available on the volume credential socket",
                        ));
                    }
                };
                authorizer.authorize_request(
                    request,
                    operation,
                    &[UdsPeerPrincipal::Service(user_names::SPACE_VOLMGR)],
                )?;
            }
        }
        Ok(())
    }
}

impl<S> Layer<S> for CredentialUdsAccess {
    type Service = CredentialUdsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CredentialUdsService {
            inner,
            access: *self,
            authorizer: volume_peer_authorizer(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CredentialUdsService<S> {
    inner: S,
    access: CredentialUdsAccess,
    authorizer: UdsPeerAuthorizer,
}

fn volume_peer_authorizer() -> UdsPeerAuthorizer {
    UdsPeerAuthorizer::new(
        "space-credential-manager",
        [UdsPeerAccount::with_linux_user(users::SPACE_VOLMGR)],
    )
}

type UdsBoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for CredentialUdsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<UdsBoxError> + Send,
    ReqBody: Send + 'static,
    ResBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    ResBody::Error: Into<UdsBoxError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = UdsBoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let mut peer = Request::new(());
        *peer.extensions_mut() = request.extensions().clone();
        let authorized = self
            .access
            .authorize(request.uri().path(), &peer, &self.authorizer);
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            // Outer tracing/rate-limit layers must see a gRPC response, not a
            // transport error that tonic would recover only after those layers.
            if let Err(status) = authorized {
                return Ok(status.into_http());
            }
            let response = inner.call(request).await.map_err(Into::into)?;
            Ok(response.map(tonic::body::Body::new))
        })
    }
}

#[cfg(test)]
mod tests;
