//! Authentication for build and publication RPCs, independent of node-api surface access.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request as HttpRequest, Response};
use http_body::Body as HttpBody;
use tonic::{Request, Status};
use tower::{Layer, Service};

use super::{
    AuthConfigError, BoxError, SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED_ENV, bearer_token,
    constant_time_eq, env_bool_default,
};

mod direct;
pub use direct::{DirectPublicationError, publication_request};

#[cfg(feature = "k8s")]
mod k8s;

#[derive(Clone, Copy)]
enum BuildSurface {
    Manager,
    NodeApi,
    Publication,
}

impl BuildSurface {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Manager => "build-manager",
            Self::NodeApi => "node-api-build",
            Self::Publication => "template-publication",
        }
    }

    fn handles(self, path: &str) -> bool {
        match self {
            Self::Manager => path.starts_with("/build.manager.v1.BuildManager/"),
            Self::NodeApi => path == "/space.node.api.v1.BuilderApi/BuildTemplate",
            Self::Publication => matches!(
                path,
                "/space.node.api.v1.ClusterApi/RegisterTemplate"
                    | "/space.node.api.v1.ClusterApi/RegisterTemplateRelease"
                    | "/space.node.api.v1.ClusterApi/CreateSystemTemplatePublicationGrant"
            ),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildAuthError {
    #[error(transparent)]
    Configuration(#[from] AuthConfigError),
    #[error(transparent)]
    DirectPublication(#[from] DirectPublicationError),
    #[cfg(feature = "k8s")]
    #[error("Kubernetes build auth requires allowed service accounts")]
    AllowedServiceAccountsMissing,
    #[cfg(feature = "k8s")]
    #[error("failed to initialize build TokenReview client: {0}")]
    KubeClient(#[from] kube::Error),
}

#[derive(Clone)]
pub struct BuildAuthLayer {
    authenticator: Arc<BuildAuthenticator>,
}

impl BuildAuthLayer {
    pub async fn for_build_manager(static_token: String) -> Result<Self, BuildAuthError> {
        Self::from_env(BuildSurface::Manager, static_token).await
    }

    pub async fn for_node_api(static_token: String) -> Result<Self, BuildAuthError> {
        Self::from_env(BuildSurface::NodeApi, static_token).await
    }

    pub async fn for_template_publication(static_token: String) -> Result<Self, BuildAuthError> {
        Self::from_env(BuildSurface::Publication, static_token).await
    }

    async fn from_env(surface: BuildSurface, static_token: String) -> Result<Self, BuildAuthError> {
        let direct_publication_token = if matches!(surface, BuildSurface::Publication) {
            if std::env::var_os(direct::TOKEN_PATH_ENV).is_some()
                && env_bool_default(super::SPACE_K8S_TOKEN_AUTH_ENABLED_ENV, false)
            {
                return Err(DirectPublicationError::KubernetesAuthentication.into());
            }
            let token = direct::token_from_env()?;
            if token
                .as_ref()
                .is_some_and(|token| constant_time_eq(token, &static_token))
            {
                return Err(DirectPublicationError::SharedCredential.into());
            }
            token
        } else {
            None
        };
        #[cfg(feature = "k8s")]
        let reviewer = match super::k8s::K8sTokenAuthConfig::from_env()? {
            Some(config) => Some(k8s::BuildTokenReviewer::from_env(&config, surface).await?),
            None => None,
        };
        #[cfg(not(feature = "k8s"))]
        if super::env_bool(super::SPACE_K8S_TOKEN_AUTH_ENABLED_ENV) {
            return Err(AuthConfigError::KubernetesAuthFeatureDisabled.into());
        }
        let static_fallback_enabled =
            env_bool_default(SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED_ENV, true)
                && !matches!(surface, BuildSurface::Publication);
        #[cfg(feature = "k8s")]
        let static_fallback_enabled = static_fallback_enabled && reviewer.is_none();
        let authenticator = BuildAuthenticator {
            surface,
            static_token,
            static_fallback_enabled,
            direct_publication_token,
            #[cfg(feature = "k8s")]
            reviewer,
        };
        tracing::info!(
            surface = surface.as_str(),
            mode = authenticator.mode(),
            static_fallback_enabled = authenticator.static_fallback_enabled,
            direct_publication_enabled = authenticator.direct_publication_token.is_some(),
            "build auth initialized"
        );
        Ok(Self {
            authenticator: Arc::new(authenticator),
        })
    }
}

struct BuildAuthenticator {
    surface: BuildSurface,
    static_token: String,
    static_fallback_enabled: bool,
    direct_publication_token: Option<String>,
    #[cfg(feature = "k8s")]
    reviewer: Option<k8s::BuildTokenReviewer>,
}

impl BuildAuthenticator {
    fn authenticate_direct_publication(
        &self,
        path: &str,
        bearer: Option<&str>,
        credential: Option<&str>,
    ) -> Result<Option<BuildAuthenticatedCaller>, Status> {
        let Some(expected) = self.direct_publication_token.as_deref() else {
            return Ok(None);
        };
        if !matches!(self.surface, BuildSurface::Publication) || !self.surface.handles(path) {
            return Ok(None);
        }
        let Some(credential) = credential else {
            return Ok(None);
        };
        let bearer = bearer_token(bearer)?;
        if !constant_time_eq(bearer, &self.static_token) || !constant_time_eq(credential, expected)
        {
            return Err(Status::unauthenticated(
                "invalid direct publication credentials",
            ));
        }
        Ok(Some(BuildAuthenticatedCaller {
            node: None,
            tenant: TenantTemplateAuthority::Granted,
            system: SystemTemplateAuthority::Granted,
        }))
    }

    async fn authenticate(
        &self,
        path: &str,
        auth_header: Option<&str>,
    ) -> Result<Option<BuildAuthenticatedCaller>, Status> {
        if !self.surface.handles(path) {
            return Ok(None);
        }
        let token = match bearer_token(auth_header) {
            Ok(token) => token,
            Err(status) => {
                let reason = if auth_header.is_none() {
                    "missing_bearer"
                } else {
                    "invalid_bearer_scheme"
                };
                pplx_space_metrics::auth::decision(
                    self.mode(),
                    self.surface.as_str(),
                    "deny",
                    reason,
                );
                return Err(status);
            }
        };
        let is_static = constant_time_eq(token, &self.static_token);
        let mode = if is_static { "static" } else { self.mode() };
        let result = self.authenticate_token(token, is_static).await;
        let (outcome, reason) = match &result {
            Ok(_) => ("allow", "ok"),
            Err(status) => (
                "deny",
                match status.code() {
                    tonic::Code::Unavailable => "token_review_unavailable",
                    tonic::Code::PermissionDenied => "service_account_not_allowed",
                    _ if is_static => "static_fallback_disabled",
                    _ if mode == "static" => "invalid_static_token",
                    _ => "invalid_build_token",
                },
            ),
        };
        pplx_space_metrics::auth::decision(mode, self.surface.as_str(), outcome, reason);
        result
    }

    fn mode(&self) -> &'static str {
        #[cfg(feature = "k8s")]
        if self.reviewer.is_some() {
            return "k8s";
        }
        "static"
    }

    async fn authenticate_token(
        &self,
        _token: &str,
        is_static: bool,
    ) -> Result<Option<BuildAuthenticatedCaller>, Status> {
        if is_static {
            #[cfg(feature = "k8s")]
            if self.reviewer.is_some() {
                return Err(Status::unauthenticated(
                    "build RPC requires a projected ServiceAccount token",
                ));
            }
            if self.static_fallback_enabled {
                return Ok(Some(BuildAuthenticatedCaller::trusted_deployment()));
            }
            return Err(Status::unauthenticated(
                "static build credentials are disabled",
            ));
        }
        #[cfg(feature = "k8s")]
        if let Some(reviewer) = &self.reviewer {
            return reviewer.authenticate(_token).await.map(Some);
        }
        Err(Status::unauthenticated("invalid build token"))
    }
}

/// What a caller may do with system (ownerless) templates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SystemTemplateAuthority {
    Denied,
    /// System publication only for an independently authorized build: ClusterApi
    /// requires that build's `template_system_publication_grants` row.
    Granted,
    Unconditional,
}

/// What a caller may do with organization-owned templates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TenantTemplateAuthority {
    Denied,
    /// Tenant publication only for a build the control plane dispatched:
    /// ClusterApi requires that build's `template_tenant_publication_grants`
    /// row, bound to its target, owner, and builder.
    Granted,
    Unconditional,
}

#[derive(Clone, Debug)]
pub struct BuildAuthenticatedCaller {
    node: Option<AuthenticatedBuildNode>,
    tenant: TenantTemplateAuthority,
    system: SystemTemplateAuthority,
}

/// Node identity from a freshly reviewed pod-bound Kubernetes token.
#[derive(Clone, Debug)]
pub struct AuthenticatedBuildNode {
    pub name: pplx_space_common_core::types::K8sNodeName,
    pub uid: pplx_space_common_core::types::K8sNodeUid,
}

pub fn publication_node<T>(request: &Request<T>) -> Option<&AuthenticatedBuildNode> {
    request
        .extensions()
        .get::<BuildAuthenticatedCaller>()?
        .node
        .as_ref()
}

impl BuildAuthenticatedCaller {
    const fn trusted_deployment() -> Self {
        Self {
            node: None,
            tenant: TenantTemplateAuthority::Unconditional,
            system: SystemTemplateAuthority::Unconditional,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub const fn trusted_by_configuration() -> Self {
        Self::trusted_deployment()
    }
}

/// The gateway resolves tenant identity; omitting the owner requests a system build.
pub fn authorize_template_build<T>(
    request: &Request<T>,
    org_id: Option<&str>,
) -> Result<(), Status> {
    let caller = request
        .extensions()
        .get::<BuildAuthenticatedCaller>()
        .ok_or_else(|| {
            Status::permission_denied("template build requires authenticated authority")
        })?;
    match org_id {
        Some(_) if caller.tenant == TenantTemplateAuthority::Denied => Err(
            Status::permission_denied("tenant build requires organization assertion authority"),
        ),
        None if caller.system == SystemTemplateAuthority::Denied => Err(Status::permission_denied(
            "system build requires system publisher authority",
        )),
        _ => Ok(()),
    }
}

/// Only an independently authorized system publisher may delegate publication
/// to a build. A dual-authority publisher cannot mint its own authorization.
pub fn authorize_system_publication_grant<T>(request: &Request<T>) -> Result<(), Status> {
    if request
        .extensions()
        .get::<BuildAuthenticatedCaller>()
        .is_some_and(|caller| caller.system == SystemTemplateAuthority::Unconditional)
    {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "issuing a system publication grant requires system-only publisher authority",
        ))
    }
}

/// Whether a system publication by the caller needs an independently issued
/// build grant. An unauthenticated caller is rejected by [`authorize_template_build`].
pub fn system_publication_requires_grant<T>(request: &Request<T>) -> bool {
    request
        .extensions()
        .get::<BuildAuthenticatedCaller>()
        .is_some_and(|caller| caller.system == SystemTemplateAuthority::Granted)
}

/// Whether a tenant publication by the caller needs the dispatched build's
/// grant. An unauthenticated caller is rejected by [`authorize_template_build`].
pub fn tenant_publication_requires_grant<T>(request: &Request<T>) -> bool {
    request
        .extensions()
        .get::<BuildAuthenticatedCaller>()
        .is_some_and(|caller| caller.tenant == TenantTemplateAuthority::Granted)
}

impl<S> Layer<S> for BuildAuthLayer {
    type Service = BuildAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BuildAuthService {
            inner,
            authenticator: Arc::clone(&self.authenticator),
        }
    }
}

#[derive(Clone)]
pub struct BuildAuthService<S> {
    inner: S,
    authenticator: Arc<BuildAuthenticator>,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for BuildAuthService<S>
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

    fn call(&mut self, mut request: HttpRequest<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let authenticator = Arc::clone(&self.authenticator);
        Box::pin(async move {
            let header = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok());
            let credential = request
                .headers()
                .get(direct::HEADER)
                .and_then(|value| value.to_str().ok());
            let caller = match authenticator.authenticate_direct_publication(
                request.uri().path(),
                header,
                credential,
            )? {
                Some(caller) => Some(caller),
                None => {
                    authenticator
                        .authenticate(request.uri().path(), header)
                        .await?
                }
            };
            if let Some(caller) = caller {
                request.extensions_mut().insert(caller);
            }
            let response = inner.call(request).await.map_err(Into::into)?;
            Ok(response.map(tonic::body::Body::new))
        })
    }
}

#[cfg(test)]
mod tests;
