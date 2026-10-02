//! The `CredentialManager` service a node serves.
//!
//! Most methods are forwarded to the control-plane authority, which owns the
//! KEKs and the Postgres state. Certificate issuance is local, authorized to
//! Sandbox Manager. `IssueIntermediateCertificate` signs gateway CAs under this
//! node's CA; only the node's own CSR is sent upstream to obtain that CA.
//!
//! It holds no KEKs and no database. Node-local callers (sandbox-manager,
//! net-gateway) keep talking to the same Unix socket they always have, while the
//! authority that owns external key access and the Postgres state runs as a control-plane
//! Deployment. Each forwarded request is re-originated over the configured
//! upstream endpoint — server-TLS TCP in cloud, UDS or plaintext TCP in
//! local-dev — using the credmgr client, which attaches this agent's projected
//! ServiceAccount token (or the static-secret fallback).
//! Credential-injection resolution additionally authenticates Sandbox Manager
//! on the node socket; the authority checks this node's identity and the exact
//! sandbox execution. Certificate handling belongs to a separate caller.
//!
//! Response caching and injection-rule leases that survive a control-plane blip
//! are deliberately not implemented yet; every forwarded call is a straight pass
//! through.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use pplx_space_common_svc::core::host::linux::identities::{user_names, users};
use pplx_space_common_svc::{
    PeerProcessCheckMode, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal,
};
use pplx_space_credmgr_client::proto::credential_manager_server::CredentialManager;
use pplx_space_credmgr_client::proto::*;
use pplx_space_credmgr_client::{
    CredentialManagerGrpcClient, CredmgrEndpoint, SERVICE_ACTOR_TOKEN_TYPE,
};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status};

use crate::auth::{CallerResolver, ClientService, ResolvedCaller};
use crate::node_ca::NodeCa;

/// Only Sandbox Manager may assert a sandbox execution when requesting
/// secret-bearing network credentials from this node.
const ALLOW_RESOLVE_NETWORK_CREDENTIALS: &[UdsPeerPrincipal] =
    &[UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR)];

/// Storage exchange is available only to the two local data-plane consumers.
/// The old shared UID remains a Volume Manager actor during staged upgrades;
/// remove that compatibility mapping only after its rollback path is retired.
const ALLOW_EXCHANGE_TOKEN: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Service(user_names::SPACE_VOLMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];

/// Who may ask this node to sign a gateway CA: sandbox-manager, the only
/// principal allowed to request gateway signing on this socket. A gateway cannot
/// be named here — each runs under its own sandbox's uid, so peer credentials classify it as `Other`. Root is absent
/// deliberately: naming the one service that mints is tighter than granting it to
/// whatever else runs privileged on the node.
const ALLOW_GATEWAY_CA_ISSUANCE: &[UdsPeerPrincipal] =
    &[UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR)];

/// Stable, low-cardinality tag string for a gRPC status code.
pub(crate) fn grpc_code_name(code: Code) -> &'static str {
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

/// How a forwarded RPC to the upstream authority resolved.
///
/// The agent must not conflate two very different outcomes. A *reachability*
/// failure means the request never got an answer — the TCP connect was
/// refused, the channel dropped, or the TLS handshake failed — and surfaces
/// from the tonic client as [`Code::Unavailable`] (the same signal the
/// substrate layer keys "node-mgr unreachable" off). An *upstream error
/// response* means the authority was reached and answered with a non-OK
/// status (`FailedPrecondition`, `NotFound`, `Internal`, …); the agent
/// faithfully relayed that verdict. Reporting a relayed `FailedPrecondition`
/// as "failed to reach" points on-call at the network when the link is fine,
/// so the two get distinct log wording and distinct metric `result` tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardOutcome {
    /// The upstream answered OK.
    Ok,
    /// The agent could not reach the authority (transport: connect refused,
    /// channel dropped, TLS). Surfaces as [`Code::Unavailable`].
    Unreachable,
    /// The authority was reached and answered with a non-OK status.
    UpstreamError,
}

impl ForwardOutcome {
    /// Classify by the forwarded call's gRPC code. `None` is the OK case.
    /// Only [`Code::Unavailable`] is a reachability failure; every other
    /// non-OK code is a status the authority produced, i.e. it was reached.
    fn from_code(code: Option<Code>) -> Self {
        match code {
            None => Self::Ok,
            Some(Code::Unavailable) => Self::Unreachable,
            Some(_) => Self::UpstreamError,
        }
    }

    /// Low-cardinality metric `result` tag for this outcome.
    fn result_tag(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Unreachable => "unreachable",
            Self::UpstreamError => "upstream_error",
        }
    }
}

/// Why an agent refused its configured upstream endpoint.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AgentUpstreamError {
    #[error(
        "the node credential agent upstream resolves to a local Unix socket, which would \
         loop back to the agent's own socket; set SPACE_CREDMGR_ENDPOINT to an http:// or \
         https:// authority"
    )]
    Loopback,
}

/// Validate that the agent's upstream points at a remote authority and not
/// back at the agent's own node-local Unix socket. A `unix://` (or unset,
/// which defaults to UDS) endpoint would make the agent forward RPCs to
/// itself, so it is rejected up front at startup.
pub fn validate_upstream(endpoint: &CredmgrEndpoint) -> Result<(), AgentUpstreamError> {
    match endpoint {
        CredmgrEndpoint::Uds(_) => Err(AgentUpstreamError::Loopback),
        CredmgrEndpoint::Tcp(_) | CredmgrEndpoint::Tls { .. } => Ok(()),
    }
}

/// The node's `CredentialManager`: forwards what the authority owns, serves
/// what only this node can.
///
/// Cloning is cheap: the inner client is a `tonic` channel handle
/// (`Arc`-backed), so each RPC clones it to get the `&mut` receiver the
/// generated client methods need without serializing calls behind a lock.
#[derive(Clone)]
pub struct NodeCredentialManager {
    upstream: CredentialManagerGrpcClient,
    /// This node's intermediate CA. Its private key is generated in this process
    /// and never leaves it; the certificate over it comes from `upstream`.
    node_ca: Arc<NodeCa>,
    /// Local authorization for certificate signing and credential-injection resolution.
    /// The authority sees this agent's credential rather than the local caller's.
    caller_resolver: CallerResolver,
    shutdown_token: CancellationToken,
}

impl NodeCredentialManager {
    /// `peer_process_check_mode` is threaded from startup so an armed switch is
    /// consistently live on this resolver rather than silently dropped. It bears
    /// on the configured accounts. Sandbox-manager remains the only principal
    /// allowed to resolve injection rules or request certificate signing.
    pub fn new(
        upstream: CredentialManagerGrpcClient,
        node_ca: Arc<NodeCa>,
        peer_process_check_mode: PeerProcessCheckMode,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            upstream,
            node_ca,
            shutdown_token,
            caller_resolver: CallerResolver::new(
                UdsPeerAuthorizer::new(
                    "space-credential-manager",
                    [
                        UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
                        UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR),
                        UdsPeerAccount::with_linux_user(users::SPACE_VOLMGR),
                    ],
                )
                .with_process_check_mode(peer_process_check_mode),
            ),
        }
    }

    /// Forward an authorized network-credential read. Shutdown cancels pending
    /// reads with a retryable status so they cannot hold the service lock
    /// through drain. Normal-operation request deadlines remain unchanged.
    ///
    /// Shutdown cancellations are logged here but do not emit the upstream-call
    /// metric: cancellation drops the instrumented forwarding future.
    async fn forward_network_credentials(
        &self,
        request: ResolveNetworkCredentialsRequest,
    ) -> Result<Response<ResolveNetworkCredentialsResponse>, Status> {
        let mut up = self.upstream.clone();
        tokio::select! {
            biased;
            () = self.shutdown_token.cancelled() => {
                tracing::info!(
                    "node credential agent cancelling network-credential forwarding for shutdown"
                );
                Err(Status::unavailable("credential agent is draining; retry through successor"))
            }
            result = self.forward("ResolveNetworkCredentials", async move {
                up.resolve_network_credentials(request).await
            }) => result,
        }
    }

    /// Forward one RPC to the upstream authority, recording its outcome +
    /// latency and a structured log if it completes. Completed forwarded calls
    /// get uniform visibility into the agent->authority link —
    /// successes, reachability failures, and upstream error responses —
    /// rather than relying on each generated method.
    ///
    /// The outcome is classified ([`ForwardOutcome`]) so a transport-level
    /// reachability failure (`Code::Unavailable`: connect refused, channel
    /// dropped, TLS) is logged and metered distinctly from a non-OK response
    /// the authority produced and the agent relayed. A reachability failure is
    /// WARN (the link is down — on-call investigates the network/authority); a
    /// relayed upstream error is also WARN but worded as a response, never as a
    /// "failed to reach", because the link is healthy.
    async fn forward<T, F>(&self, method: &'static str, call: F) -> Result<Response<T>, Status>
    where
        F: Future<Output = Result<Response<T>, Status>>,
    {
        let started = Instant::now();
        let result = call.await;
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let outcome = ForwardOutcome::from_code(result.as_ref().err().map(Status::code));
        match &result {
            Ok(_) => {
                tracing::debug!(
                    method,
                    latency_ms,
                    "credmgr agent forwarded RPC to authority"
                );
            }
            // Transport-level: the request never reached the authority.
            Err(status) if outcome == ForwardOutcome::Unreachable => {
                tracing::warn!(
                    method,
                    grpc_code = grpc_code_name(status.code()),
                    error = status.message(),
                    latency_ms,
                    "credmgr agent could not reach the upstream authority"
                );
            }
            // The authority answered with a non-OK status; the agent relays it.
            // Worded as a response, not a "failed to reach", since the link is up.
            Err(status) => {
                tracing::warn!(
                    method,
                    grpc_code = grpc_code_name(status.code()),
                    error = status.message(),
                    latency_ms,
                    "credmgr agent relayed a non-OK response from the upstream authority"
                );
            }
        }
        pplx_space_metrics::credmgr::agent_upstream_call(
            method,
            outcome.result_tag(),
            result
                .as_ref()
                .err()
                .map_or("ok", |status| grpc_code_name(status.code())),
            latency_ms,
        );
        result
    }
}

/// Replace both actor fields after local authorization. Treating caller input
/// as an actor claim would let a local service turn the agent into a confused
/// deputy; the authority accepts this claim only from the authenticated agent.
fn forwarded_exchange_request(
    caller: &ResolvedCaller,
    mut request: ExchangeTokenRequest,
) -> Result<ExchangeTokenRequest, Status> {
    let ResolvedCaller::UdsPeer(identity) = caller else {
        return Err(Status::permission_denied(
            "storage credential actor requires a UDS peer identity",
        ));
    };
    let actor =
        if identity.uid() == users::SPACE_VOLMGR.uid || identity.uid() == users::SPACE_SBXMGR.uid {
            ClientService::VolumeManager
        } else if identity.uid() == users::SPACE_TMPLMGR.uid {
            ClientService::TemplateManager
        } else {
            return Err(Status::permission_denied(
                "local service is not authorized for storage credential exchange",
            ));
        };
    request.actor_token = Some(actor.as_str().to_string());
    request.actor_token_type = Some(SERVICE_ACTOR_TOKEN_TYPE.to_string());
    Ok(request)
}

#[tonic::async_trait]
impl CredentialManager for NodeCredentialManager {
    async fn resolve_network_credentials(
        &self,
        request: Request<ResolveNetworkCredentialsRequest>,
    ) -> Result<Response<ResolveNetworkCredentialsResponse>, Status> {
        let caller = self.caller_resolver.resolve(&request)?;
        if !matches!(caller, ResolvedCaller::UdsPeer(_)) {
            return Err(Status::permission_denied(
                "network credential resolution requires the node-local sandbox manager",
            ));
        }
        self.caller_resolver.authorize_caller(
            &caller,
            "ResolveNetworkCredentials",
            ALLOW_RESOLVE_NETWORK_CREDENTIALS,
            &[],
        )?;

        self.forward_network_credentials(request.into_inner()).await
    }

    async fn exchange_token(
        &self,
        request: Request<ExchangeTokenRequest>,
    ) -> Result<Response<ExchangeTokenResponse>, Status> {
        let caller = self.caller_resolver.resolve(&request)?;
        if !matches!(caller, ResolvedCaller::UdsPeer(_)) {
            return Err(Status::permission_denied(
                "storage credential exchange requires an authorized node-local service",
            ));
        }
        self.caller_resolver.authorize_caller(
            &caller,
            "ExchangeToken",
            ALLOW_EXCHANGE_TOKEN,
            &[],
        )?;
        let forwarded = forwarded_exchange_request(&caller, request.into_inner())?;
        let mut up = self.upstream.clone();
        self.forward("ExchangeToken", async move {
            up.exchange_token(forwarded).await
        })
        .await
    }

    async fn issue_dek(
        &self,
        request: Request<IssueDekRequest>,
    ) -> Result<Response<IssueDekResponse>, Status> {
        // Reject malformed scope locally before an upstream call; validation
        // must remain available when the authority is unreachable.
        crate::service::parse_organization_id(&request.get_ref().organization_id)?;
        let mut up = self.upstream.clone();
        self.forward("IssueDek", async move {
            up.issue_dek(request.into_inner()).await
        })
        .await
    }

    async fn unwrap_dek(
        &self,
        request: Request<UnwrapDekRequest>,
    ) -> Result<Response<UnwrapDekResponse>, Status> {
        // Reject malformed scope locally before an upstream call; validation
        // must remain available when the authority is unreachable.
        crate::service::parse_organization_id(&request.get_ref().organization_id)?;
        let mut up = self.upstream.clone();
        self.forward("UnwrapDek", async move {
            up.unwrap_dek(request.into_inner()).await
        })
        .await
    }

    async fn encrypt(
        &self,
        request: Request<EncryptRequest>,
    ) -> Result<Response<EncryptResponse>, Status> {
        let mut up = self.upstream.clone();
        self.forward(
            "Encrypt",
            async move { up.encrypt(request.into_inner()).await },
        )
        .await
    }

    async fn decrypt(
        &self,
        request: Request<DecryptRequest>,
    ) -> Result<Response<DecryptResponse>, Status> {
        let mut up = self.upstream.clone();
        self.forward(
            "Decrypt",
            async move { up.decrypt(request.into_inner()).await },
        )
        .await
    }

    async fn get_or_issue_sandbox_volume_dek(
        &self,
        request: Request<GetOrIssueSandboxVolumeDekRequest>,
    ) -> Result<Response<GetOrIssueSandboxVolumeDekResponse>, Status> {
        let mut up = self.upstream.clone();
        self.forward("GetOrIssueSandboxVolumeDek", async move {
            up.get_or_issue_sandbox_volume_dek(request.into_inner())
                .await
        })
        .await
    }

    async fn issue_intermediate_certificate(
        &self,
        request: Request<IssueIntermediateCertificateRequest>,
    ) -> Result<Response<IssueIntermediateCertificateResponse>, Status> {
        let caller = self.caller_resolver.resolve(&request)?;
        if !matches!(caller, ResolvedCaller::UdsPeer(_)) {
            return Err(Status::permission_denied(
                "gateway CA issuance requires the node-local sandbox manager",
            ));
        }
        self.caller_resolver.authorize_caller(
            &caller,
            "IssueIntermediateCertificate",
            ALLOW_GATEWAY_CA_ISSUANCE,
            &[],
        )?;
        self.node_ca
            .issue_intermediate(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.to_status())
    }
}

#[cfg(test)]
mod tests;
