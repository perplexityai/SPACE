//! Authenticated TCP transport for internal RPC services. Unix sockets use
//! peer credentials and deliberately do not pass through this policy.

mod incoming;
pub use incoming::Incoming;

use http::Uri;
use pplx_space_common_core::env::{Environment, ProcessEnvironment};
use std::path::PathBuf;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

pub const INSECURE_DEV_ENV: &str = "SPACE_INTERNAL_RPC_INSECURE_DEV";
pub const CA_ENV: &str = "SPACE_INTERNAL_RPC_TLS_CA";
pub const CERT_ENV: &str = "SPACE_INTERNAL_RPC_TLS_CERT";
pub const KEY_ENV: &str = "SPACE_INTERNAL_RPC_TLS_KEY";

#[derive(Clone, Copy, Debug)]
pub enum Service {
    ClusterApi,
    NodeApi,
    BuildManager,
}
impl Service {
    fn name_env(self) -> &'static str {
        match self {
            Self::ClusterApi => "SPACE_CLUSTER_API_TLS_SERVER_NAME",
            Self::NodeApi => "SPACE_NODE_API_TLS_SERVER_NAME",
            Self::BuildManager => "SPACE_BUILD_MANAGER_TLS_SERVER_NAME",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(
        "plaintext internal RPC is disabled; configure HTTPS or explicitly opt into {INSECURE_DEV_ENV} for development"
    )]
    Plaintext,
    #[error("internal RPC requires an http or https TCP endpoint")]
    Scheme,
    #[error("internal RPC TLS requires {0}")]
    Missing(&'static str),
    #[error("cannot read internal RPC TLS file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("internal RPC transport: {0}")]
    Tonic(#[from] tonic::transport::Error),
}

fn insecure(env: &dyn Environment) -> bool {
    pplx_space_common_core::env::env_flag(env.var(INSECURE_DEV_ENV).ok().as_deref(), false)
}

pub fn default_scheme() -> &'static str {
    if insecure(&ProcessEnvironment) {
        "http"
    } else {
        "https"
    }
}

/// Advertise HTTPS when the listener has TLS identity, or HTTP for explicit
/// development listeners without identity.
pub fn advertised_endpoint_prefix() -> &'static str {
    advertised_endpoint_prefix_from_environment(&ProcessEnvironment)
}

fn advertised_endpoint_prefix_from_environment(env: &dyn Environment) -> &'static str {
    if insecure(env) && env.var_os(CERT_ENV).is_none() && env.var_os(KEY_ENV).is_none() {
        "http://"
    } else {
        "https://"
    }
}

/// Add the configured TCP scheme only to bare addresses. Explicit schemes,
/// including unsupported ones, remain intact for transport validation.
pub fn qualify_endpoint(address: &str) -> String {
    if address.contains("://") {
        address.to_owned()
    } else {
        format!("{}://{address}", default_scheme())
    }
}

fn file_path(env: &dyn Environment, name: &'static str) -> Result<PathBuf, TransportError> {
    env.var_os(name)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .ok_or(TransportError::Missing(name))
}

fn file(env: &dyn Environment, name: &'static str) -> Result<Vec<u8>, TransportError> {
    let path = file_path(env, name)?;
    std::fs::read(&path).map_err(|source| TransportError::Read { path, source })
}

#[derive(Clone, Debug)]
pub struct ClientTransport {
    allow_plaintext: bool,
    tls: Option<ClientTlsConfig>,
}
impl ClientTransport {
    pub async fn from_env(service: Service, uri: &Uri) -> Result<Self, TransportError> {
        Self::from_environment(service, uri, &ProcessEnvironment).await
    }
    pub async fn from_environment(
        service: Service,
        uri: &Uri,
        env: &(dyn Environment + Sync),
    ) -> Result<Self, TransportError> {
        let ca = if uri.scheme_str() == Some("https") {
            let path = file_path(env, CA_ENV)?;
            Some(
                tokio::fs::read(&path)
                    .await
                    .map_err(|source| TransportError::Read { path, source })?,
            )
        } else {
            None
        };
        Self::with_ca(service, uri, env, ca)
    }

    /// Load trust during synchronous client initialization, without opening a connection.
    /// Async callers should use `from_env` to avoid blocking on certificate file I/O.
    pub fn from_env_sync(service: Service, uri: &Uri) -> Result<Self, TransportError> {
        let env = &ProcessEnvironment;
        let ca = if uri.scheme_str() == Some("https") {
            Some(file(env, CA_ENV)?)
        } else {
            None
        };
        Self::with_ca(service, uri, env, ca)
    }

    fn with_ca(
        service: Service,
        uri: &Uri,
        env: &dyn Environment,
        ca: Option<Vec<u8>>,
    ) -> Result<Self, TransportError> {
        let allow_plaintext = insecure(env);
        match uri.scheme_str() {
            Some("http") if allow_plaintext => Ok(Self {
                allow_plaintext,
                tls: None,
            }),
            Some("http") => Err(TransportError::Plaintext),
            Some("https") => {
                let name = env
                    .var(service.name_env())
                    .ok()
                    .filter(|v| !v.is_empty())
                    .or_else(|| {
                        uri.host().map(|host| {
                            // URI authorities bracket IPv6 literals; rustls expects
                            // the IP address itself when checking an IP SAN.
                            host.strip_prefix('[')
                                .and_then(|host| host.strip_suffix(']'))
                                .unwrap_or(host)
                                .to_owned()
                        })
                    })
                    .ok_or(TransportError::Missing(service.name_env()))?;
                let ca = ca.ok_or(TransportError::Missing(CA_ENV))?;
                let tls = ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca))
                    .domain_name(name);
                // Validate the fixed trust/name once before an endpoint discovery
                // closure uses it for individual pod addresses.
                Endpoint::from(uri.clone()).tls_config(tls.clone())?;
                Ok(Self {
                    allow_plaintext,
                    tls: Some(tls),
                })
            }
            _ => Err(TransportError::Scheme),
        }
    }
    pub fn endpoint(&self, endpoint: Endpoint) -> Result<Endpoint, TransportError> {
        match endpoint.uri().scheme_str() {
            Some("https") => Ok(
                endpoint.tls_config(self.tls.clone().ok_or(TransportError::Missing(CA_ENV))?)?
            ),
            Some("http") if self.allow_plaintext => Ok(endpoint),
            Some("http") => Err(TransportError::Plaintext),
            _ => Err(TransportError::Scheme),
        }
    }
}

pub async fn client_endpoint(
    service: Service,
    endpoint: Endpoint,
) -> Result<Endpoint, TransportError> {
    ClientTransport::from_env(service, endpoint.uri())
        .await?
        .endpoint(endpoint)
}

pub fn server_builder() -> Result<crate::GrpcServer, TransportError> {
    server_builder_from_environment(&ProcessEnvironment)
}

pub fn server_builder_from_environment(
    env: &dyn Environment,
) -> Result<crate::GrpcServer, TransportError> {
    let builder = crate::grpc_server_builder();
    // Even the development escape hatch must not hide a partially configured
    // certificate: missing key/cert pairs fail instead of downgrading silently.
    if insecure(env) && env.var_os(CERT_ENV).is_none() && env.var_os(KEY_ENV).is_none() {
        return Ok(builder);
    }
    let identity = Identity::from_pem(file(env, CERT_ENV)?, file(env, KEY_ENV)?);
    // TLS negotiation happens before bearer admission, so an idle peer must
    // not retain a pre-authentication handshake forever.
    Ok(builder.tls_config(
        ServerTlsConfig::new()
            .identity(identity)
            .timeout(std::time::Duration::from_secs(10)),
    )?)
}

#[cfg(test)]
mod tests;
