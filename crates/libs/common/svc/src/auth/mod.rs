use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request as HttpRequest, Response};
use http_body::Body as HttpBody;
use subtle::ConstantTimeEq;
use tonic::{Request, Status};
use tower::{Layer, Service};

use crate::space_api_route::SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY;

pub mod build;
mod node_identity;
pub mod template;
pub mod template_transfer;
pub use node_identity::{AuthenticatedKubernetesNode, AuthenticatedNodeIdentity};

#[cfg(all(feature = "k8s", any(test, feature = "test-support")))]
pub mod test_support;

#[cfg(feature = "k8s")]
mod k8s;
#[cfg(feature = "k8s")]
use k8s::{K8sTokenAuthConfig, K8sTokenReviewer};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub use crate::bearer_token::SPACE_INTERNAL_TOKEN_ENV;
pub use crate::bearer_token::{SpaceBearerInterceptor, required_space_bearer_token};

const SPACE_K8S_TOKEN_AUTH_ENABLED_ENV: &str = "SPACE_K8S_TOKEN_AUTH_ENABLED";
const SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED_ENV: &str =
    "SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED";

/// Node role for an auth surface. Reuses the shared registry vocabulary from
/// `pplx-space-common-core` (with its `FromStr`/`Display`) rather than forking a
/// parallel enum; parse inbound role strings at the boundary via `FromStr`.
pub use crate::core::types::NodeRole;

#[derive(Clone)]
pub struct NodeApiAuthLayer {
    authenticator: Arc<NodeApiAuthenticator>,
}

impl NodeApiAuthLayer {
    /// Use a supplied TokenReview transport while exercising real auth and RPC wiring.
    #[cfg(all(feature = "test-support", feature = "k8s"))]
    pub fn with_k8s_client_for_test(config: NodeApiAuthConfig, client: kube::Client) -> Self {
        Self {
            authenticator: Arc::new(NodeApiAuthenticator {
                role: config.role,
                static_token: config.static_token,
                static_control_token: config.static_control_token,
                static_fallback_enabled: config.static_fallback_enabled,
                k8s: config
                    .k8s
                    .map(|config| K8sTokenReviewer::with_client(config, client)),
            }),
        }
    }

    pub async fn from_config(config: NodeApiAuthConfig) -> Result<Self, AuthConfigError> {
        #[cfg(feature = "k8s")]
        let k8s = if let Some(k8s_config) = config.k8s {
            // Boot-time record of the effective auth mode so an operator can
            // confirm per-pod which mode it came up in during the migration.
            tracing::info!(
                role = ?config.role,
                mode = "k8s_token_review",
                static_fallback_enabled = config.static_fallback_enabled,
                namespace = %k8s_config.namespace,
                node_api_audience = %k8s_config.node_api_audience,
                node_api_internal_audience = %k8s_config.node_api_internal_audience,
                cluster_api_audience = %k8s_config.cluster_api_audience,
                max_inflight_reviews = k8s_config.max_inflight_reviews,
                review_timeout_secs = k8s_config.review_timeout.as_secs(),
                "node-api auth initialized"
            );
            Some(K8sTokenReviewer::new(k8s_config).await?)
        } else {
            tracing::info!(
                role = ?config.role,
                mode = "static_only",
                static_fallback_enabled = config.static_fallback_enabled,
                "node-api auth initialized"
            );
            None
        };
        #[cfg(not(feature = "k8s"))]
        tracing::info!(
            mode = "static_only",
            static_fallback_enabled = config.static_fallback_enabled,
            "node-api auth initialized (built without k8s feature)"
        );
        Ok(Self {
            authenticator: Arc::new(NodeApiAuthenticator {
                static_token: config.static_token,
                static_control_token: config.static_control_token,
                #[cfg(feature = "k8s")]
                role: config.role,
                static_fallback_enabled: config.static_fallback_enabled,
                #[cfg(feature = "k8s")]
                k8s,
            }),
        })
    }
}

impl<S> Layer<S> for NodeApiAuthLayer {
    type Service = NodeApiAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NodeApiAuthService {
            inner,
            authenticator: Arc::clone(&self.authenticator),
        }
    }
}

#[derive(Clone)]
pub struct NodeApiAuthService<S> {
    inner: S,
    authenticator: Arc<NodeApiAuthenticator>,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for NodeApiAuthService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
    ReqBody: Send + 'static,
    ResBody: HttpBody<Data = bytes::Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: HttpRequest<ReqBody>) -> Self::Future {
        strip_untrusted_bridge_policy(&mut req);
        let path = req.uri().path().to_owned();
        let auth_header = req
            .headers()
            .get("authorization")
            .map(|value| value.to_str().map(str::to_owned));
        let authenticator = Arc::clone(&self.authenticator);
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let caller = match auth_header {
                Some(Err(_)) => authenticator.reject_invalid_auth_header(&path)?,
                Some(Ok(auth_header)) => {
                    authenticator
                        .authenticate(&path, Some(&auth_header))
                        .await?
                }
                None => authenticator.authenticate(&path, None).await?,
            };
            if let Some(caller) = caller {
                req.extensions_mut().insert(caller);
            }
            let response = inner.call(req).await.map_err(Into::into)?;
            Ok(response.map(tonic::body::Body::new))
        })
    }
}

fn strip_untrusted_bridge_policy<B>(request: &mut HttpRequest<B>) {
    request
        .headers_mut()
        .remove(SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY);
}

pub struct NodeApiAuthConfig {
    #[cfg(feature = "k8s")]
    role: NodeRole,
    static_token: String,
    static_control_token: Option<String>,
    static_fallback_enabled: bool,
    #[cfg(feature = "k8s")]
    k8s: Option<K8sTokenAuthConfig>,
}

impl NodeApiAuthConfig {
    pub fn from_env(
        role: NodeRole,
        static_token: String,
        static_control_token: Option<String>,
    ) -> Result<Self, AuthConfigError> {
        Self::from_environment_impl(
            &pplx_space_common_core::env::ProcessEnvironment,
            role,
            static_token,
            static_control_token,
        )
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
        role: NodeRole,
        static_token: String,
        static_control_token: Option<String>,
    ) -> Result<Self, AuthConfigError> {
        Self::from_environment_impl(env, role, static_token, static_control_token)
    }

    fn from_environment_impl(
        env: &dyn pplx_space_common_core::env::Environment,
        role: NodeRole,
        static_token: String,
        static_control_token: Option<String>,
    ) -> Result<Self, AuthConfigError> {
        #[cfg(not(feature = "k8s"))]
        let _ = role;

        #[cfg(feature = "k8s")]
        let k8s = K8sTokenAuthConfig::from_environment(env)?;
        #[cfg(feature = "k8s")]
        if let Some(k8s) = &k8s {
            k8s.validate_for_role(role)?;
        }

        #[cfg(not(feature = "k8s"))]
        if env_bool_from_environment(env, SPACE_K8S_TOKEN_AUTH_ENABLED_ENV) {
            return Err(AuthConfigError::KubernetesAuthFeatureDisabled);
        }

        Ok(Self {
            #[cfg(feature = "k8s")]
            role,
            static_token,
            static_control_token,
            static_fallback_enabled: env_bool_default_from_environment(
                env,
                SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED_ENV,
                true,
            ),
            #[cfg(feature = "k8s")]
            k8s,
        })
    }
}

#[cfg(feature = "k8s")]
#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("Kubernetes token auth is enabled but KUBERNETES_SERVICE_HOST is not set")]
    KubernetesServiceHostMissing,
    #[error("Kubernetes token auth is enabled but the pod namespace is not configured")]
    KubernetesNamespaceMissing,
    #[error(
        "Kubernetes token auth is enabled for a worker but no trusted WaitReady service accounts are configured"
    )]
    TrustedWaitReadyServiceAccountsMissing,
    #[error(
        "Kubernetes token auth is enabled for a worker or builder node-api but no trusted policy service accounts are configured"
    )]
    TrustedPolicyServiceAccountsMissing,
    #[error("failed to initialize Kubernetes TokenReview client: {0}")]
    KubeClient(#[from] kube::Error),
}

#[cfg(not(feature = "k8s"))]
#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("Kubernetes token auth is enabled but this binary was built without the k8s feature")]
    KubernetesAuthFeatureDisabled,
}

struct NodeApiAuthenticator {
    #[cfg(feature = "k8s")]
    role: NodeRole,
    static_token: String,
    static_control_token: Option<String>,
    static_fallback_enabled: bool,
    #[cfg(feature = "k8s")]
    k8s: Option<K8sTokenReviewer>,
}

/// Lifecycle and policy permissions established by node-api authentication.
///
/// RPC handlers may use this extension to make narrow authorization decisions
/// without re-parsing or trusting caller-controlled metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeApiAuthenticatedCaller {
    node_identity: AuthenticatedNodeIdentity,
    wait_ready_lifecycle_authority: WaitReadyLifecycleAuthority,
    network_policy_authority: NetworkPolicyMutationAuthority,
    data_plane_session_refresh_authority: DataPlaneSessionRefreshAuthority,
    template_release_read_authority: TemplateReleaseReadAuthority,
    template_transfer_authority: template_transfer::TemplateTransferAuthority,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitReadyLifecycleAuthority {
    Denied,
    Granted,
}

/// Whether the caller may drive the sandbox policy-mutation RPCs
/// (`SetSandboxNetworkPolicy`, `SetSandboxAutoLifecyclePolicy`).
///
/// Those requests carry an attacker-controllable `authority` and `generation`;
/// a non-control-plane caller that reaches them can pin a node-local or
/// stale-generation policy that cluster reconciliation can never override (the
/// sbx-mgr `authority=local` gate blocks the Local variant, but a forwarded
/// `authority=cluster` write with an inflated generation still wins). Only the
/// cluster control plane forwards these RPCs, so node-api restricts them to the
/// configured trusted service accounts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkPolicyMutationAuthority {
    Denied,
    Granted,
}

/// Whether the caller may resolve a data-plane session route without the
/// session's attach token.
///
/// `ResolveDataPlaneSessionRoute` accepts a tokenless request so the
/// data-plane edge can refresh the route of a session it already attached
/// without holding the attach secret. That refresh must stay confined to the
/// edge identity: any other admitted workload that learns a live session ID
/// would otherwise mint a route token for another tenant's session. Only the
/// guest-bridge service accounts receive it; in a static-only deployment the
/// shared token is the trust boundary and keeps it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataPlaneSessionRefreshAuthority {
    Denied,
    Granted,
}

/// How far a caller may see into the template-release catalog
/// (`FindTemplateRelease`, `FindTemplateReleaseByHash`,
/// `ListLatestTemplateReleases`).
///
/// Those requests carry a caller-supplied `caller_org_id` that the server
/// cannot verify, and the node pull path sends none at all. A node-scoped
/// caller therefore resolves only generic templates plus the org-specific
/// templates the control plane has placed on the caller's own nodes; it
/// cannot discover another tenant's private release by naming it. Only a
/// static-only deployment's shared token, which is the trust boundary there,
/// reads the catalog unscoped. Every TokenReview identity is node-scoped: no
/// control-plane consumer of these reads exists today, and a future one
/// needs an explicit grant rather than an implicit one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemplateReleaseReadAuthority {
    NodeScoped,
    Unscoped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KubernetesAuthenticationMode {
    Disabled,
    #[cfg(feature = "k8s")]
    Configured,
}

impl NodeApiAuthenticatedCaller {
    const fn static_token(mode: KubernetesAuthenticationMode) -> Self {
        // In a static-only deployment (no Kubernetes auth) the shared internal
        // token is the trust boundary, so it retains full authority. Once
        // service-account auth is configured, the static token is only a
        // migration fallback that a lower-trust service may also hold, so it is
        // not treated as proof of control-plane identity: it may not mutate
        // policy, drive the tokenless WaitReady lifecycle path, or refresh a
        // data-plane session route without its attach token.
        let (
            wait_ready_lifecycle_authority,
            network_policy_authority,
            data_plane_session_refresh_authority,
            template_release_read_authority,
        ) = match mode {
            KubernetesAuthenticationMode::Disabled => (
                WaitReadyLifecycleAuthority::Granted,
                NetworkPolicyMutationAuthority::Granted,
                DataPlaneSessionRefreshAuthority::Granted,
                TemplateReleaseReadAuthority::Unscoped,
            ),
            #[cfg(feature = "k8s")]
            KubernetesAuthenticationMode::Configured => (
                WaitReadyLifecycleAuthority::Denied,
                NetworkPolicyMutationAuthority::Denied,
                DataPlaneSessionRefreshAuthority::Denied,
                TemplateReleaseReadAuthority::NodeScoped,
            ),
        };
        Self {
            node_identity: AuthenticatedNodeIdentity::RemoteAddress,
            wait_ready_lifecycle_authority,
            network_policy_authority,
            data_plane_session_refresh_authority,
            template_release_read_authority,
            template_transfer_authority: match mode {
                KubernetesAuthenticationMode::Disabled => {
                    template_transfer::TemplateTransferAuthority::Granted
                }
                #[cfg(feature = "k8s")]
                KubernetesAuthenticationMode::Configured => {
                    template_transfer::TemplateTransferAuthority::Denied
                }
            },
        }
    }

    #[cfg(feature = "k8s")]
    const fn kubernetes_service_account(
        wait_ready_lifecycle_authority: WaitReadyLifecycleAuthority,
        network_policy_authority: NetworkPolicyMutationAuthority,
        data_plane_session_refresh_authority: DataPlaneSessionRefreshAuthority,
    ) -> Self {
        Self {
            node_identity: AuthenticatedNodeIdentity::MissingKubernetesClaims,
            wait_ready_lifecycle_authority,
            network_policy_authority,
            data_plane_session_refresh_authority,
            template_release_read_authority: TemplateReleaseReadAuthority::NodeScoped,
            template_transfer_authority: template_transfer::TemplateTransferAuthority::Denied,
        }
    }

    /// The data-plane edge's authorities, for tests of the tokenless
    /// session-route refresh.
    #[cfg(any(test, feature = "test-support"))]
    pub const fn data_plane_edge_for_test() -> Self {
        Self {
            node_identity: AuthenticatedNodeIdentity::RemoteAddress,
            wait_ready_lifecycle_authority: WaitReadyLifecycleAuthority::Denied,
            network_policy_authority: NetworkPolicyMutationAuthority::Denied,
            data_plane_session_refresh_authority: DataPlaneSessionRefreshAuthority::Granted,
            template_release_read_authority: TemplateReleaseReadAuthority::NodeScoped,
            template_transfer_authority: template_transfer::TemplateTransferAuthority::Denied,
        }
    }

    /// An admitted workload holding none of the narrow authorities, for tests.
    #[cfg(any(test, feature = "test-support"))]
    pub const fn unprivileged_workload_for_test() -> Self {
        Self {
            node_identity: AuthenticatedNodeIdentity::RemoteAddress,
            wait_ready_lifecycle_authority: WaitReadyLifecycleAuthority::Denied,
            network_policy_authority: NetworkPolicyMutationAuthority::Denied,
            data_plane_session_refresh_authority: DataPlaneSessionRefreshAuthority::Denied,
            template_release_read_authority: TemplateReleaseReadAuthority::NodeScoped,
            template_transfer_authority: template_transfer::TemplateTransferAuthority::Denied,
        }
    }

    pub const fn node_identity(&self) -> &AuthenticatedNodeIdentity {
        &self.node_identity
    }

    pub const fn wait_ready_lifecycle_authority(&self) -> WaitReadyLifecycleAuthority {
        self.wait_ready_lifecycle_authority
    }

    pub const fn network_policy_authority(&self) -> NetworkPolicyMutationAuthority {
        self.network_policy_authority
    }

    pub const fn data_plane_session_refresh_authority(&self) -> DataPlaneSessionRefreshAuthority {
        self.data_plane_session_refresh_authority
    }

    pub const fn template_transfer_authority(
        &self,
    ) -> template_transfer::TemplateTransferAuthority {
        self.template_transfer_authority
    }

    pub const fn template_release_read_authority(&self) -> TemplateReleaseReadAuthority {
        self.template_release_read_authority
    }
}

impl NodeApiAuthenticator {
    async fn authenticate(
        &self,
        path: &str,
        auth_header: Option<&str>,
    ) -> Result<Option<NodeApiAuthenticatedCaller>, Status> {
        let Some(surface) = auth_surface(path) else {
            return Ok(None);
        };

        let token = match bearer_token(auth_header) {
            Ok(token) => token,
            Err(status) => {
                // Emit a deny decision for missing/malformed bearers too, so the
                // rollout dashboards don't undercount denials. Derive the reason
                // from the header shape, not the Status message.
                let reason = if auth_header.is_none() {
                    "missing_bearer"
                } else {
                    "invalid_bearer_scheme"
                };
                pplx_space_metrics::auth::node_api_decision(
                    "static",
                    surface.as_str(),
                    "deny",
                    reason,
                );
                return Err(status);
            }
        };
        if self.static_token_matches(surface, token) {
            if self.static_fallback_enabled {
                pplx_space_metrics::auth::node_api_decision(
                    "static",
                    surface.as_str(),
                    "allow",
                    "ok",
                );
                #[cfg(feature = "k8s")]
                let mode = if self.k8s.is_some() {
                    KubernetesAuthenticationMode::Configured
                } else {
                    KubernetesAuthenticationMode::Disabled
                };
                #[cfg(not(feature = "k8s"))]
                let mode = KubernetesAuthenticationMode::Disabled;
                return Ok(Some(NodeApiAuthenticatedCaller::static_token(mode)));
            }
            pplx_space_metrics::auth::node_api_decision(
                "static",
                surface.as_str(),
                "deny",
                "static_fallback_disabled",
            );
            #[cfg(feature = "k8s")]
            if !is_jwt_shaped(token) {
                return Err(Status::unauthenticated("invalid token"));
            }
            #[cfg(not(feature = "k8s"))]
            return Err(Status::unauthenticated("invalid token"));
        }

        #[cfg(feature = "k8s")]
        if let Some(k8s) = &self.k8s {
            if !is_jwt_shaped(token) {
                pplx_space_metrics::auth::node_api_decision(
                    "k8s",
                    surface.as_str(),
                    "deny",
                    "non_jwt_bearer",
                );
                return Err(Status::unauthenticated("invalid token"));
            }
            let caller = k8s.authenticate(self.role, surface, path, token).await?;
            return Ok(Some(caller));
        }

        pplx_space_metrics::auth::node_api_decision(
            "static",
            surface.as_str(),
            "deny",
            "invalid_static_token",
        );
        Err(Status::unauthenticated("invalid token"))
    }

    fn static_token_matches(&self, surface: AuthSurface, token: &str) -> bool {
        match surface {
            AuthSurface::Normal => constant_time_eq(token, &self.static_token),
            AuthSurface::WorkerInternal => self
                .static_control_token
                .as_deref()
                .is_some_and(|expected| constant_time_eq(token, expected)),
        }
    }

    fn reject_invalid_auth_header(
        &self,
        path: &str,
    ) -> Result<Option<NodeApiAuthenticatedCaller>, Status> {
        if let Some(surface) = auth_surface(path) {
            pplx_space_metrics::auth::node_api_decision(
                "static",
                surface.as_str(),
                "deny",
                "invalid_auth_header",
            );
            return Err(Status::unauthenticated("invalid authorization header"));
        }
        Ok(None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuthSurface {
    Normal,
    WorkerInternal,
}

impl AuthSurface {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::WorkerInternal => "worker_internal",
        }
    }
}

fn auth_surface(path: &str) -> Option<AuthSurface> {
    if matches!(
        path,
        "/" | "/grpc.health.v1.Health/Check"
            | "/grpc.health.v1.Health/Watch"
            | "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
            | "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
    ) {
        return None;
    }
    if path.starts_with("/space.node.api.v1.WorkerApiInternal/") {
        Some(AuthSurface::WorkerInternal)
    } else {
        Some(AuthSurface::Normal)
    }
}

fn bearer_token(auth_header: Option<&str>) -> Result<&str, Status> {
    match auth_header {
        Some(value) => {
            let Some((scheme, token)) = value.split_once(' ') else {
                return Err(Status::unauthenticated("expected Bearer scheme"));
            };
            let token = token.trim();
            if scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() {
                Ok(token)
            } else {
                Err(Status::unauthenticated("expected Bearer scheme"))
            }
        }
        None => Err(Status::unauthenticated("missing authorization header")),
    }
}

/// Create a synchronous tonic interceptor for tests and local simulators.
///
/// Production node-api uses `NodeApiAuthLayer` so it can validate Kubernetes
/// projected tokens through TokenReview.
pub fn bearer_auth_interceptor(
    expected_token: String,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        let token = bearer_token(
            req.metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
        )?;
        if constant_time_eq(token, &expected_token) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid token"))
        }
    }
}

pub fn bearer_client_interceptor(
    token: String,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut req: Request<()>| {
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| Status::internal("invalid token format"))?,
        );
        Ok(req)
    }
}

pub fn required_bearer_token(env_var: &str) -> Result<String, Status> {
    std::env::var(env_var)
        .map_err(|_| Status::internal(format!("{env_var} not set")))
        .and_then(|token| validate_required_bearer_token(env_var, token))
}

fn validate_required_bearer_token(env_var: &str, token: String) -> Result<String, Status> {
    let token = token.trim().to_owned();
    if token.is_empty() {
        Err(Status::internal(format!("{env_var} is empty")))
    } else {
        Ok(token)
    }
}

fn constant_time_eq(candidate: &str, expected: &str) -> bool {
    bool::from(candidate.as_bytes().ct_eq(expected.as_bytes()))
}

#[cfg(feature = "k8s")]
fn is_jwt_shaped(token: &str) -> bool {
    let mut parts = token.split('.');
    let Some(header) = parts.next() else {
        return false;
    };
    let Some(payload) = parts.next() else {
        return false;
    };
    let Some(signature) = parts.next() else {
        return false;
    };
    parts.next().is_none() && !header.is_empty() && !payload.is_empty() && !signature.is_empty()
}

#[cfg(not(feature = "k8s"))]
fn env_bool(name: &str) -> bool {
    env_bool_default(name, false)
}

fn env_bool_default(name: &str, default: bool) -> bool {
    env_bool_default_from_environment(
        &pplx_space_common_core::env::ProcessEnvironment,
        name,
        default,
    )
}
fn env_bool_from_environment(
    env: &dyn pplx_space_common_core::env::Environment,
    name: &str,
) -> bool {
    env_bool_default_from_environment(env, name, false)
}
fn env_bool_default_from_environment(
    env: &dyn pplx_space_common_core::env::Environment,
    name: &str,
    default: bool,
) -> bool {
    env.var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

#[cfg(test)]
mod tests;
