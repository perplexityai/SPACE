//! Where to reach the credential manager: the node-local Unix socket
//! (per-node DaemonSet deployments), plaintext TCP (non-cloud profiles such
//! as the local-dev compose stack), or server-TLS TCP (the control-plane
//! Deployment on EKS).
//!
//! The endpoint is configured through environment variables, parsed once per
//! process:
//!
//! - `SPACE_CREDMGR_ENDPOINT` — `unix://<socket-path>`, `http://host:port`,
//!   or `https://host:port`. Unset or empty means the default Unix socket.
//! - `SPACE_CREDMGR_TLS_CA` — PEM path of the CA that signed the credmgr
//!   server certificate, required for `https://` so the client can verify the
//!   server. Re-read on every connect so renewals are picked up without a
//!   process restart. The client presents *no* certificate of its own;
//!   identity is the projected ServiceAccount token (see [`crate::token`]).

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use tonic::codegen::http::Uri;
use tonic::transport::{Certificate, ClientTlsConfig};

use crate::{ConnectError, CredentialManagerGrpcClient, CredmgrBearerInterceptor};

const ENDPOINT_VAR: &str = "SPACE_CREDMGR_ENDPOINT";
const TLS_CA_VAR: &str = "SPACE_CREDMGR_TLS_CA";

const SERVICE_NAME: &str = "credential-manager";

/// Bounds each TCP dial, so a SYN pinned to a stale conntrack entry (one sent
/// to a ClusterIP before kube-proxy programmed it on the node) is redialed
/// after this bound rather than at the connect helper's longer attempt cap.
/// The TLS handshake has its own bound, [`TLS_HANDSHAKE_TIMEOUT`].
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Bounds every TLS handshake of a credmgr channel, reconnects included, so a
/// peer that accepts TCP but never answers the ClientHello fails the dial.
/// The credential-manager listener hands each accepted socket straight to its
/// handshake, without the internal-transport listeners' admission slots, so
/// the client does not first wait in an accept queue behind other handshakes
/// and needs no more than a normal handshake's ample margin.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);

/// PEM file location for the server-TLS client side: the CA bundle that
/// signed the credmgr server certificate. With identity moved to the
/// projected ServiceAccount token, the client no longer holds a certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    ca: PathBuf,
}

impl TlsFiles {
    pub fn new(ca: PathBuf) -> Self {
        Self { ca }
    }

    pub fn ca(&self) -> &std::path::Path {
        &self.ca
    }
}

/// A parsed credmgr endpoint. See the module docs for the wire shapes each
/// variant corresponds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredmgrEndpoint {
    /// Node-local Unix domain socket (peer-credential auth).
    Uds(PathBuf),
    /// Plaintext TCP. Non-cloud profiles only; the server refuses this
    /// listener unless explicitly allowed.
    Tcp(Uri),
    /// TCP with mutual TLS.
    Tls { uri: Uri, files: TlsFiles },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EndpointConfigError {
    #[error("{ENDPOINT_VAR} is not a valid URI ({value:?}): {message}")]
    InvalidUri { value: String, message: String },
    #[error(
        "{ENDPOINT_VAR} has unsupported scheme {scheme:?} \
         (expected unix://, http://, or https://)"
    )]
    UnsupportedScheme { scheme: String },
    #[error("{ENDPOINT_VAR} has no host: {value:?}")]
    MissingHost { value: String },
    #[error("{ENDPOINT_VAR} is a unix:// URI with an empty socket path")]
    EmptyUdsPath,
    #[error("{ENDPOINT_VAR} is https but {var} is unset")]
    MissingTlsVar { var: &'static str },
}

impl Default for CredmgrEndpoint {
    fn default() -> Self {
        Self::Uds(PathBuf::from(crate::CREDMGR_SOCKET))
    }
}

impl CredmgrEndpoint {
    /// Parse the endpoint from the process environment. Unset or empty
    /// `SPACE_CREDMGR_ENDPOINT` selects the default Unix socket.
    pub fn from_env() -> Result<Self, EndpointConfigError> {
        Self::from_env_with(|var| std::env::var(var).ok())
    }

    fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Result<Self, EndpointConfigError> {
        let raw = match get(ENDPOINT_VAR).filter(|v| !v.is_empty()) {
            Some(raw) => raw,
            None => return Ok(Self::default()),
        };

        if let Some(path) = raw.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(EndpointConfigError::EmptyUdsPath);
            }
            return Ok(Self::Uds(PathBuf::from(path)));
        }

        let uri: Uri = raw
            .parse()
            .map_err(|e: tonic::codegen::http::uri::InvalidUri| {
                EndpointConfigError::InvalidUri {
                    value: raw.clone(),
                    message: e.to_string(),
                }
            })?;
        if uri.host().is_none() {
            return Err(EndpointConfigError::MissingHost { value: raw });
        }

        match uri.scheme_str() {
            Some("http") => Ok(Self::Tcp(uri)),
            Some("https") => {
                let ca = get(TLS_CA_VAR)
                    .filter(|v| !v.is_empty())
                    .map(PathBuf::from)
                    .ok_or(EndpointConfigError::MissingTlsVar { var: TLS_CA_VAR })?;
                Ok(Self::Tls {
                    uri,
                    files: TlsFiles::new(ca),
                })
            }
            other => Err(EndpointConfigError::UnsupportedScheme {
                scheme: other.unwrap_or("").to_string(),
            }),
        }
    }

    /// Connect to credmgr at this endpoint, with the standard connect retry
    /// policy and client trace layer. TCP/TLS endpoints attach the projected
    /// ServiceAccount token (or static-secret fallback) per request; the UDS
    /// endpoint authorizes by peer credentials and sends no token.
    pub async fn connect(&self) -> Result<CredentialManagerGrpcClient, ConnectError> {
        let (channel, interceptor) = match self {
            Self::Uds(path) => {
                let channel = pplx_space_common_grpc::connect_uds_channel_with_retry(
                    SERVICE_NAME,
                    path,
                    pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
                )
                .await?;
                (channel, CredmgrBearerInterceptor::None)
            }
            Self::Tcp(uri) => {
                let endpoint =
                    tonic::transport::Endpoint::from(uri.clone()).connect_timeout(CONNECT_TIMEOUT);
                let channel = pplx_space_common_grpc::connect_tcp_channel_with_retry(
                    SERVICE_NAME,
                    uri.to_string(),
                    endpoint,
                    None,
                    pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
                )
                .await?;
                (channel, CredmgrBearerInterceptor::TokenSource)
            }
            Self::Tls { uri, files } => {
                let (tls, tls_handshake_timeout) = client_tls_config(files).await?;
                let endpoint = tonic::transport::Endpoint::from(uri.clone())
                    .connect_timeout(CONNECT_TIMEOUT)
                    .tls_config(tls)?;
                let channel = pplx_space_common_grpc::connect_tcp_channel_with_retry(
                    SERVICE_NAME,
                    uri.to_string(),
                    endpoint,
                    Some(tls_handshake_timeout),
                    pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
                )
                .await?;
                (channel, CredmgrBearerInterceptor::TokenSource)
            }
        };
        Ok(crate::build_client(channel, interceptor))
    }
}

/// Ensure the repository-standard AWS-LC `CryptoProvider` is installed before
/// tonic materializes a rustls `ClientConfig`.
fn ensure_crypto_provider() {
    pplx_space_common_svc::install_default_rustls_crypto_provider();
}

/// Build the rustls client config, re-reading the CA PEM so renewed server
/// certificates are picked up at the next connect. Server-auth only: the
/// client verifies the credmgr server cert and presents none of its own.
/// Also returns the handshake bound the config sets, for the connect helper's
/// attempt cap.
async fn client_tls_config(files: &TlsFiles) -> Result<(ClientTlsConfig, Duration), ConnectError> {
    ensure_crypto_provider();
    let ca = {
        let path = files.ca().to_path_buf();
        tokio::fs::read(&path)
            .await
            .map_err(|source| ConnectError::TlsRead { path, source })?
    };
    Ok((
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca))
            .timeout(TLS_HANDSHAKE_TIMEOUT),
        TLS_HANDSHAKE_TIMEOUT,
    ))
}

static CACHED_ENDPOINT: OnceLock<Result<CredmgrEndpoint, EndpointConfigError>> = OnceLock::new();

/// The process-wide endpoint, parsed from the environment on first use.
pub(crate) fn cached_from_env() -> Result<&'static CredmgrEndpoint, EndpointConfigError> {
    CACHED_ENDPOINT
        .get_or_init(CredmgrEndpoint::from_env)
        .as_ref()
        .map_err(Clone::clone)
}

#[cfg(test)]
mod tests;
