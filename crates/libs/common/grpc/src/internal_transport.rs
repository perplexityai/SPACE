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

/// How long a listener gives an accepted connection to finish its TLS
/// handshake. TLS negotiation happens before bearer admission, so an idle peer
/// must not retain a pre-authentication handshake forever.
pub const SERVER_TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Bound for an internal-RPC client's TLS handshake, every handshake of a
/// channel included. It must exceed [`SERVER_TLS_HANDSHAKE_TIMEOUT`]: a
/// listener takes one of its handshake slots before it accepts, so while every
/// slot is busy a connected socket waits in the kernel accept queue, and that
/// wait counts against the client's timer, which starts once TCP connects,
/// but not against the server's, which starts at accept. The extra five
/// seconds absorb a short queue; no client bound can guarantee a handshake
/// while a listener stays saturated.
pub const CLIENT_TLS_HANDSHAKE_TIMEOUT: std::time::Duration =
    SERVER_TLS_HANDSHAKE_TIMEOUT.saturating_add(std::time::Duration::from_secs(5));

#[derive(Clone, Copy, Debug)]
pub enum Service {
    ClusterApi,
    NodeApi,
    BuildManager,
    IngressActivation,
}
impl Service {
    fn name_env(self) -> &'static str {
        match self {
            Self::ClusterApi => "SPACE_CLUSTER_API_TLS_SERVER_NAME",
            Self::NodeApi => "SPACE_NODE_API_TLS_SERVER_NAME",
            Self::IngressActivation => "SPACE_INGRESS_ACTIVATION_TLS_SERVER_NAME",
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
    tls: Option<ClientTls>,
}

/// A client TLS config together with the handshake bound it sets, so the
/// bound the connect helpers size their attempt caps from is the one applied.
#[derive(Clone, Debug)]
struct ClientTls {
    config: ClientTlsConfig,
    handshake_timeout: std::time::Duration,
}

impl ClientTls {
    fn new(ca: Vec<u8>, name: String, handshake_timeout: std::time::Duration) -> Self {
        Self {
            config: ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name(name)
                .timeout(handshake_timeout),
            handshake_timeout,
        }
    }
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
        Self::from_environment_with_tls_handshake_timeout(
            service,
            uri,
            env,
            CLIENT_TLS_HANDSHAKE_TIMEOUT,
        )
        .await
    }

    /// [`Self::from_environment`] with a TLS handshake bound other than
    /// [`CLIENT_TLS_HANDSHAKE_TIMEOUT`].
    pub(crate) async fn from_environment_with_tls_handshake_timeout(
        service: Service,
        uri: &Uri,
        env: &(dyn Environment + Sync),
        tls_handshake_timeout: std::time::Duration,
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
        Self::with_ca(service, uri, env, ca, tls_handshake_timeout)
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
        Self::with_ca(service, uri, env, ca, CLIENT_TLS_HANDSHAKE_TIMEOUT)
    }

    fn with_ca(
        service: Service,
        uri: &Uri,
        env: &dyn Environment,
        ca: Option<Vec<u8>>,
        tls_handshake_timeout: std::time::Duration,
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
                let tls = ClientTls::new(ca, name, tls_handshake_timeout);
                // Validate the fixed trust/name once before an endpoint discovery
                // closure uses it for individual pod addresses.
                Endpoint::from(uri.clone()).tls_config(tls.config.clone())?;
                Ok(Self {
                    allow_plaintext,
                    tls: Some(tls),
                })
            }
            _ => Err(TransportError::Scheme),
        }
    }
    /// Apply this transport to `endpoint` for a channel tonic dials itself,
    /// such as a balanced channel's. Its connect-phase failures carry no
    /// [`crate::ConnectPhaseError`]; connect through
    /// [`Self::connect_endpoint`] wherever SPACE's TCP connector can dial.
    pub fn endpoint(&self, endpoint: Endpoint) -> Result<Endpoint, TransportError> {
        Ok(self.connect_endpoint(endpoint)?.endpoint)
    }

    /// Apply this transport to `endpoint` for the TCP connect helpers: the
    /// result carries the handshake bound of the TLS config it applies.
    pub fn connect_endpoint(&self, endpoint: Endpoint) -> Result<ConnectEndpoint, TransportError> {
        // A Unix-socket endpoint reports an http fallback URI; it is not TCP.
        if crate::connect::is_unix_socket_endpoint(&endpoint) {
            return Err(TransportError::Scheme);
        }
        match endpoint.uri().scheme_str() {
            Some("https") => {
                let tls = self.tls.as_ref().ok_or(TransportError::Missing(CA_ENV))?;
                Ok(ConnectEndpoint {
                    endpoint: endpoint.tls_config(tls.config.clone())?,
                    tls_handshake_timeout: Some(tls.handshake_timeout),
                })
            }
            Some("http") if self.allow_plaintext => Ok(ConnectEndpoint {
                endpoint,
                tls_handshake_timeout: None,
            }),
            Some("http") => Err(TransportError::Plaintext),
            _ => Err(TransportError::Scheme),
        }
    }
}

/// An endpoint with a [`ClientTransport`] applied, and the handshake bound of
/// the TLS config it set (none for plaintext). Internal-RPC callers connect
/// through it so the TCP connect helpers size their attempt caps from the
/// bound actually applied. Only [`ClientTransport::connect_endpoint`] builds
/// one, which guarantees a TCP endpoint whose `https` scheme comes with its
/// handshake bound, so its connects cannot fail the helpers' endpoint checks.
#[derive(Clone, Debug)]
pub struct ConnectEndpoint {
    endpoint: Endpoint,
    tls_handshake_timeout: Option<std::time::Duration>,
}

impl ConnectEndpoint {
    /// [`crate::connect_tcp_channel`] with this endpoint's handshake bound.
    pub async fn connect(self) -> Result<tonic::transport::Channel, tonic::transport::Error> {
        crate::connect::dial_tcp_channel(self.endpoint, self.tls_handshake_timeout).await
    }

    /// [`crate::connect_tcp_channel_lazy`] with this endpoint's handshake
    /// bound.
    pub fn connect_lazy(self) -> tonic::transport::Channel {
        crate::connect::lazy_tcp_channel(self.endpoint, self.tls_handshake_timeout)
    }

    /// [`crate::RotatingChannel::connect`] with this endpoint's handshake
    /// bound.
    pub async fn connect_rotating(
        self,
        service_name: &'static str,
        max_age: Option<std::time::Duration>,
    ) -> Result<crate::RotatingChannel, tonic::transport::Error> {
        crate::RotatingChannel::connect_tcp(
            service_name,
            self.endpoint,
            self.tls_handshake_timeout,
            max_age,
        )
        .await
    }

    /// [`crate::connect_tcp_channel_with_retry`] with this endpoint's
    /// handshake bound.
    pub async fn connect_with_retry(
        self,
        service_name: &'static str,
        target: impl AsRef<str>,
        retry_policy: crate::GrpcRetryPolicy,
    ) -> Result<tonic::transport::Channel, crate::TcpConnectError> {
        crate::connect_tcp_channel_with_retry(
            service_name,
            target,
            self.endpoint,
            self.tls_handshake_timeout,
            retry_policy,
        )
        .await
    }

    /// [`crate::connect_tcp_channel_with_retry_watched`] with this endpoint's
    /// handshake bound.
    pub async fn connect_with_retry_watched(
        self,
        service_name: &'static str,
        target: impl AsRef<str>,
        retry_policy: crate::GrpcRetryPolicy,
    ) -> Result<(tonic::transport::Channel, crate::ConnectionWatch), crate::TcpConnectError> {
        crate::connect_tcp_channel_with_retry_watched(
            service_name,
            target,
            self.endpoint,
            self.tls_handshake_timeout,
            retry_policy,
        )
        .await
    }

    /// [`crate::connect_tcp_channel_with_retrying_redials`] with this
    /// endpoint's handshake bound.
    pub async fn connect_with_retrying_redials(
        self,
        service_name: &'static str,
        attempt_timeout: std::time::Duration,
        retry_policy: crate::GrpcRetryPolicy,
    ) -> Result<tonic::transport::Channel, tonic::transport::Error> {
        crate::connect::connect_retrying_redials(
            service_name,
            self.endpoint,
            attempt_timeout,
            self.tls_handshake_timeout,
            retry_policy,
        )
        .await
    }

    /// The longest a [`Self::connect_with_retrying_redials`] dial or redial
    /// with these arguments may take: [`crate::retrying_redial_bound`] with
    /// this endpoint's handshake bound.
    pub fn retrying_redial_bound(
        &self,
        retry_policy: crate::GrpcRetryPolicy,
        attempt_timeout: std::time::Duration,
    ) -> std::time::Duration {
        crate::retrying_redial_bound(retry_policy, attempt_timeout, self.tls_handshake_timeout)
    }
}

/// Apply the internal transport for `service`, read from the environment, to
/// `endpoint` for the TCP connect helpers: see
/// [`ClientTransport::connect_endpoint`].
pub async fn client_connect_endpoint(
    service: Service,
    endpoint: Endpoint,
) -> Result<ConnectEndpoint, TransportError> {
    ClientTransport::from_env(service, endpoint.uri())
        .await?
        .connect_endpoint(endpoint)
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
    Ok(builder.tls_config(
        ServerTlsConfig::new()
            .identity(identity)
            .timeout(SERVER_TLS_HANDSHAKE_TIMEOUT),
    )?)
}

#[cfg(test)]
mod tests;
