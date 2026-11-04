//! Authentication tower layer for the credmgr TCP listener.
//!
//! Establishes *who* a TCP caller is before any handler runs, and injects the
//! resolved identity ([`TcpCallerIdentity`]) into the request extensions where
//! the per-RPC authorization in [`crate::auth::authorize_caller`] reads it.
//!
//! Exactly one credential is accepted, selected by deployment shape:
//!
//! - Kubernetes listeners accept a **projected ServiceAccount JWT**, validated
//!   via the apiserver `TokenReview`
//!   ([`crate::k8s_identity::CredmgrTokenReviewer`]) and mapped to a
//!   [`crate::auth::K8sIdentity`]. They never load or accept the shared static
//!   internal token.
//! - Direct-node and local-development listeners with no apiserver accept the
//!   **shared static internal token** as their only credential. It carries no
//!   per-service identity, so it resolves to the coarse
//!   [`TcpCallerIdentity::Plaintext`] principal.
//!
//! Server-side TLS still terminates and encrypts the connection; this layer
//! does not look at client certificates.
//!
//! Health and reflection paths bypass authentication so Kubernetes gRPC probes
//! work without a credential.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use subtle::ConstantTimeEq;
use tonic::Status;
use tower::{Layer, Service};

use crate::auth::K8sIdentity;
use crate::k8s_identity::{CredmgrTokenReviewer, K8sAuthConfig, K8sAuthConfigError};
use tokio_util::sync::CancellationToken;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The identity a TCP request authenticated as, injected into request
/// extensions by [`CredmgrTcpAuthService`] for the per-RPC authorizer to read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpCallerIdentity {
    /// A projected Kubernetes ServiceAccount token (validated via TokenReview)
    /// resolved to this credmgr client service.
    K8sJwt(K8sIdentity),
    /// The shared static internal token on a direct-node `--allow-plaintext`
    /// listener with no TokenReview. No per-service identity.
    Plaintext,
}

/// Authenticates TCP callers and resolves their identity. Cloned per request.
enum Authenticator {
    ProjectedToken(Box<CredmgrTokenReviewer>),
    StaticToken(String),
}

impl Authenticator {
    async fn authenticate(
        &self,
        auth_header: Option<&str>,
        extensions: &http::Extensions,
    ) -> Result<TcpCallerIdentity, Status> {
        let token = bearer_token(auth_header)?;

        match self {
            Self::ProjectedToken(reviewer) => {
                // A TokenReview-configured listener has no static credential to
                // compare against. Non-JWT bearers fail before reaching the
                // apiserver; JWT-shaped values are reviewed normally.
                if !is_jwt_shaped(token) {
                    pplx_space_metrics::auth::decision(
                        "k8s",
                        "credmgr-tcp",
                        "deny",
                        "non_jwt_bearer",
                    );
                    return Err(Status::unauthenticated("invalid token"));
                }
                let caller = reviewer
                    .resolve_token_identity(token, peer_ip(extensions))
                    .await?;
                Ok(TcpCallerIdentity::K8sJwt(caller))
            }
            Self::StaticToken(expected) => {
                if constant_time_eq(token, expected) {
                    pplx_space_metrics::auth::decision("static", "credmgr-tcp", "allow", "ok");
                    Ok(TcpCallerIdentity::Plaintext)
                } else {
                    pplx_space_metrics::auth::decision(
                        "static",
                        "credmgr-tcp",
                        "deny",
                        "invalid_static_token",
                    );
                    Err(Status::unauthenticated("invalid token"))
                }
            }
        }
    }
}

/// Bearer-token-or-projected-JWT auth layer for the credmgr TCP listener.
#[derive(Clone)]
pub struct CredmgrTcpAuthLayer {
    authenticator: Arc<Authenticator>,
}

impl CredmgrTcpAuthLayer {
    /// Build the projected-token authentication path, sharing one Kubernetes
    /// client between TokenReview and the caller pod reflector.
    pub async fn projected_token(
        config: K8sAuthConfig,
        database_pool: sqlx::PgPool,
        shutdown: CancellationToken,
    ) -> Result<Self, K8sAuthConfigError> {
        let client = pplx_space_common_svc::kube_client::traced_kube_client_from_env().await?;
        let reviewer = CredmgrTokenReviewer::new(config, client, database_pool, shutdown);
        tracing::info!(
            mode = "k8s_token_review",
            "credmgr TCP auth initialized; static token is not accepted"
        );
        Ok(Self {
            authenticator: Arc::new(Authenticator::ProjectedToken(Box::new(reviewer))),
        })
    }

    /// Build a non-Kubernetes listener whose sole credential is the static
    /// internal token. Used by direct-node and local-development profiles.
    pub fn static_token(token: String) -> Self {
        tracing::info!(mode = "static_only", "credmgr TCP auth initialized");
        Self {
            authenticator: Arc::new(Authenticator::StaticToken(token)),
        }
    }
}

impl<S> Layer<S> for CredmgrTcpAuthLayer {
    type Service = CredmgrTcpAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CredmgrTcpAuthService {
            inner,
            authenticator: Arc::clone(&self.authenticator),
        }
    }
}

#[derive(Clone)]
pub struct CredmgrTcpAuthService<S> {
    inner: S,
    authenticator: Arc<Authenticator>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for CredmgrTcpAuthService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
    ReqBody: Send + 'static,
    ResBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path().to_owned();
        let auth_header = req
            .headers()
            .get(http::header::AUTHORIZATION)
            .map(|value| value.to_str().map(str::to_owned));
        let authenticator = Arc::clone(&self.authenticator);
        // Clone-and-swap so the captured `inner` is the ready service (tower
        // contract: the cloned copy may not be ready yet).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            if !is_auth_exempt(&path) {
                let identity = match auth_header {
                    Some(Ok(header)) => {
                        authenticator
                            .authenticate(Some(&header), req.extensions())
                            .await?
                    }
                    None => authenticator.authenticate(None, req.extensions()).await?,
                    Some(Err(_)) => {
                        pplx_space_metrics::auth::decision(
                            "static",
                            "credmgr-tcp",
                            "deny",
                            "invalid_auth_header",
                        );
                        return Err(Status::unauthenticated("invalid authorization header").into());
                    }
                };
                req.extensions_mut().insert(identity);
            }
            // A `Status` error is recovered by tonic's server into a clean
            // gRPC trailers response (via `Status::from_error`), so an auth
            // rejection reaches the client as UNAUTHENTICATED/PERMISSION_DENIED
            // rather than a dropped connection.
            let response = inner.call(req).await.map_err(Into::into)?;
            Ok(response.map(tonic::body::Body::new))
        })
    }
}

fn peer_ip(extensions: &http::Extensions) -> Option<std::net::IpAddr> {
    use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
    extensions
        .get::<TcpConnectInfo>()
        .and_then(|info| info.remote_addr)
        .or_else(|| {
            extensions
                .get::<TlsConnectInfo<TcpConnectInfo>>()
                .and_then(|info| info.get_ref().remote_addr)
        })
        .map(|addr| addr.ip())
}

/// Health and reflection bypass auth so Kubernetes gRPC probes work without a
/// credential.
fn is_auth_exempt(path: &str) -> bool {
    matches!(
        path,
        "/" | "/grpc.health.v1.Health/Check"
            | "/grpc.health.v1.Health/Watch"
            | "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
            | "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
    )
}

fn bearer_token(auth_header: Option<&str>) -> Result<&str, Status> {
    let header = auth_header.ok_or_else(|| {
        pplx_space_metrics::auth::decision("static", "credmgr-tcp", "deny", "missing_bearer");
        Status::unauthenticated("missing authorization header")
    })?;
    header.strip_prefix("Bearer ").ok_or_else(|| {
        pplx_space_metrics::auth::decision(
            "static",
            "credmgr-tcp",
            "deny",
            "invalid_bearer_scheme",
        );
        Status::unauthenticated("expected Bearer scheme")
    })
}

fn is_jwt_shaped(token: &str) -> bool {
    let mut parts = token.split('.');
    let (Some(a), Some(b), Some(c), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    !a.is_empty() && !b.is_empty() && !c.is_empty()
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests;
