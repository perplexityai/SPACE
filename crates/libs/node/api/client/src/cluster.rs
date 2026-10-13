use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use pplx_space_common_grpc::RotatingChannel;
use pplx_space_common_svc::bearer_token::BearerTokenError;
use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Endpoint, Uri};

use crate::proto::cluster_api_client::ClusterApiClient;

const CLUSTER_API_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CLUSTER_API_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_API_ENDPOINT_DISCOVERY_ENV: &str = "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY";
/// Identifies this client's connection rotations in logs and in
/// `grpc.client.connection_rotation`.
const CLUSTER_API_SERVICE_NAME: &str = "cluster-api";

pub type AuthenticatedClusterApiClient =
    ClusterApiClient<GrpcClientTrace<InterceptedService<RotatingChannel, BearerAuthInterceptor>>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EndpointDiscovery {
    #[default]
    Off,
    Kubernetes,
}

impl EndpointDiscovery {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Kubernetes => "kubernetes",
        }
    }

    #[must_use]
    pub fn from_env() -> Self {
        let Some(value) = std::env::var(CLUSTER_API_ENDPOINT_DISCOVERY_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Self::Off;
        };
        match value.parse() {
            Ok(discovery) => discovery,
            Err(error) => {
                tracing::error!(
                    env = CLUSTER_API_ENDPOINT_DISCOVERY_ENV,
                    value,
                    %error,
                    "unknown endpoint discovery mode; dialing the service address"
                );
                Self::Off
            }
        }
    }
}

impl fmt::Display for EndpointDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown endpoint discovery mode '{0}'")]
pub struct ParseEndpointDiscoveryError(String);

impl FromStr for EndpointDiscovery {
    type Err = ParseEndpointDiscoveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("off") {
            Ok(Self::Off)
        } else if value.eq_ignore_ascii_case("kubernetes") {
            Ok(Self::Kubernetes)
        } else {
            Err(ParseEndpointDiscoveryError(value.to_owned()))
        }
    }
}

#[derive(Debug, Clone)]
pub struct BearerAuthInterceptor {
    source: BearerAuthSource,
}

#[derive(Debug, Clone)]
enum BearerAuthSource {
    Fixed(MetadataValue<Ascii>),
    ClusterApiTokenFile,
}

impl BearerAuthInterceptor {
    pub const fn new(bearer: MetadataValue<Ascii>) -> Self {
        Self {
            source: BearerAuthSource::Fixed(bearer),
        }
    }

    pub const fn cluster_api_token_file() -> Self {
        Self {
            source: BearerAuthSource::ClusterApiTokenFile,
        }
    }
}

impl tonic::service::Interceptor for BearerAuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let bearer = match &self.source {
            BearerAuthSource::Fixed(bearer) => bearer.clone(),
            BearerAuthSource::ClusterApiTokenFile => {
                // A local token-source failure (unreadable/mid-rotation projected
                // token) is transient and retryable: surface UNAVAILABLE, not
                // INTERNAL, so a credential blip on this side is never misread as
                // remote wedge evidence. Mirrors `SpaceBearerInterceptor`.
                pplx_space_common_svc::bearer_token::space_bearer_metadata(
                    pplx_space_common_svc::bearer_token::SpaceBearerToken::ClusterApi,
                )
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?
            }
        };
        request.metadata_mut().insert("authorization", bearer);
        Ok(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectClusterApiError {
    #[error(transparent)]
    InternalTransport(pplx_space_common_grpc::internal_transport::TransportError),
    #[error("SPACE_INTERNAL_API_ENDPOINT (or legacy SPACE_CLUSTER_NODE_API_ENDPOINT) is not set")]
    MissingEndpoint,
    /// No cluster-api credential is configured on this node. Callers treat
    /// this (with `MissingEndpoint`) as "not wired to a cluster API" and take
    /// their local/no-op leg, so it must only cover genuinely-unconfigured
    /// states — see [`classify_token_error`].
    #[error("cluster-api bearer token is not configured: {0}")]
    MissingToken(#[source] BearerTokenError),
    /// A cluster-api credential is configured but unusable (unreadable or
    /// empty token file, malformed token value). Deliberately distinct from
    /// `MissingToken` so a broken deployment surfaces on callers' loud
    /// retriable/error legs instead of silently degrading to local-only paths.
    #[error("cluster-api bearer token is configured but unusable: {0}")]
    TokenSource(#[source] BearerTokenError),
    #[error("invalid SPACE_INTERNAL_API_ENDPOINT '{endpoint}': {source}")]
    InvalidEndpoint {
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("cluster api connect failed: {0}")]
    Transport(#[source] tonic::transport::Error),
}

fn classify_token_error(error: BearerTokenError) -> ConnectClusterApiError {
    match error {
        // Parity with the env-secret world: no token file and an unset (or
        // set-but-empty) fallback env var means this node is simply not wired
        // to a cluster API.
        BearerTokenError::Missing { .. } | BearerTokenError::EmptyEnv { .. } => {
            ConnectClusterApiError::MissingToken(error)
        }
        // An explicitly configured token path that is missing, empty, or
        // unreadable (e.g. a projected volume the process cannot read), or a
        // token that is not valid metadata, is a broken deployment.
        BearerTokenError::MissingFile { .. }
        | BearerTokenError::EmptyFile { .. }
        | BearerTokenError::ReadFile { .. }
        | BearerTokenError::InvalidMetadata(_) => ConnectClusterApiError::TokenSource(error),
    }
}

pub const CLUSTER_API_ENDPOINT_ENV: &str = "SPACE_INTERNAL_API_ENDPOINT";
/// Deprecated name, still read for one release so directly provisioned hosts
/// keep their cluster-api wiring across the upgrade.
pub const CLUSTER_API_ENDPOINT_ENV_LEGACY: &str = "SPACE_CLUSTER_NODE_API_ENDPOINT";

pub async fn connect_from_env() -> Result<AuthenticatedClusterApiClient, ConnectClusterApiError> {
    connect_from_env_with_discovery(EndpointDiscovery::from_env()).await
}

pub async fn connect_from_env_with_discovery(
    discovery: EndpointDiscovery,
) -> Result<AuthenticatedClusterApiClient, ConnectClusterApiError> {
    let endpoint = required_env(CLUSTER_API_ENDPOINT_ENV)
        .or_else(|| required_env(CLUSTER_API_ENDPOINT_ENV_LEGACY))
        .ok_or(ConnectClusterApiError::MissingEndpoint)?;
    // Probe the cluster-api credential once at connect time so callers can
    // classify the failure: `MissingToken` (genuinely unconfigured → local
    // fallback leg) vs `TokenSource` (configured but broken → retriable/error
    // leg). Without this probe an endpoint-set-but-token-missing node would
    // connect fine and only fail on the first RPC as an opaque `internal`
    // Status. The returned client still uses the token-file interceptor, which
    // re-reads on every RPC so projected-token rotation is picked up.
    if let Err(error) = pplx_space_common_svc::bearer_token::space_bearer_metadata(
        pplx_space_common_svc::bearer_token::SpaceBearerToken::ClusterApi,
    ) {
        return Err(classify_token_error(error));
    }
    connect_with_interceptor(
        endpoint,
        BearerAuthInterceptor::cluster_api_token_file(),
        discovery,
    )
    .await
}

pub async fn connect(
    endpoint: impl Into<String>,
    token: impl AsRef<str>,
) -> Result<AuthenticatedClusterApiClient, ConnectClusterApiError> {
    let bearer = format!("Bearer {}", token.as_ref())
        .parse()
        .map_err(|error| classify_token_error(BearerTokenError::from(error)))?;
    connect_with_bearer(endpoint, bearer).await
}

pub async fn connect_with_bearer(
    endpoint: impl Into<String>,
    bearer: MetadataValue<Ascii>,
) -> Result<AuthenticatedClusterApiClient, ConnectClusterApiError> {
    connect_with_interceptor(
        endpoint,
        BearerAuthInterceptor::new(bearer),
        EndpointDiscovery::Off,
    )
    .await
}

async fn connect_with_interceptor(
    endpoint: impl Into<String>,
    interceptor: BearerAuthInterceptor,
    discovery: EndpointDiscovery,
) -> Result<AuthenticatedClusterApiClient, ConnectClusterApiError> {
    let uri = endpoint.into();
    let uri = Endpoint::from_shared(uri.clone())
        .map_err(|source| ConnectClusterApiError::InvalidEndpoint {
            endpoint: uri,
            source,
        })?
        .uri()
        .clone();
    let transport = pplx_space_common_grpc::internal_transport::ClientTransport::from_env(
        pplx_space_common_grpc::internal_transport::Service::ClusterApi,
        &uri,
    )
    .await
    .map_err(ConnectClusterApiError::InternalTransport)?;
    let channel = cluster_api_channel(uri, discovery, transport).await?;
    Ok(client_with_interceptor(channel, interceptor))
}

fn cluster_api_endpoint(uri: Uri) -> Endpoint {
    Endpoint::from(uri)
        .tcp_nodelay(true)
        .connect_timeout(CLUSTER_API_CONNECT_TIMEOUT)
        .timeout(CLUSTER_API_RPC_TIMEOUT)
}

#[cfg(feature = "kubernetes")]
async fn cluster_api_channel(
    uri: Uri,
    discovery: EndpointDiscovery,
    transport: pplx_space_common_grpc::internal_transport::ClientTransport,
) -> Result<RotatingChannel, ConnectClusterApiError> {
    let Some(kubernetes_service) = configured_discovery_service(&uri, discovery) else {
        return rotating_channel(uri, transport).await;
    };
    match pplx_space_common_grpc_lb::KubernetesEndpoints::connect(
        CLUSTER_API_SERVICE_NAME,
        kubernetes_service,
        pplx_space_common_grpc_lb::PortSelector::Sole,
    )
    .await
    {
        Ok(endpoints) => {
            // A balanced channel dials lazily. Preserve the old startup
            // contract by proving the configured Service address is reachable
            // before reporting this client as connected.
            transport
                .connect_endpoint(cluster_api_endpoint(uri.clone()))
                .map_err(ConnectClusterApiError::InternalTransport)?
                .connect()
                .await
                .map_err(ConnectClusterApiError::Transport)?;
            let balanced = pplx_space_common_grpc_lb::balanced_channel(
                CLUSTER_API_SERVICE_NAME,
                uri,
                move |uri| {
                    transport.endpoint(cluster_api_endpoint(uri)).expect(
                        "fixed TLS trust and service identity were validated before discovery",
                    )
                },
                endpoints.watch(),
                pplx_space_common_grpc::max_connection_age_from_env(),
            );
            Ok(RotatingChannel::from_channel(
                CLUSTER_API_SERVICE_NAME,
                balanced,
            ))
        }
        // Discovery is an optimization over the Service address. Missing
        // RBAC or an unavailable Kubernetes API must preserve the old path.
        Err(error) => {
            pplx_space_common_svc::observability::with_peer_service!(tracing::error!(
                peer.service = CLUSTER_API_SERVICE_NAME,
                error = %error,
                "endpoint discovery unavailable; dialing the service address"
            ));
            rotating_channel(uri, transport).await
        }
    }
}

#[cfg(feature = "kubernetes")]
fn configured_discovery_service(uri: &Uri, discovery: EndpointDiscovery) -> Option<String> {
    if discovery == EndpointDiscovery::Off {
        return None;
    }
    let Some(service) = kubernetes_service_name(uri) else {
        tracing::error!(
            endpoint = %uri,
            "cluster-api endpoint has no service name; dialing the configured address"
        );
        return None;
    };
    Some(service.to_owned())
}

#[cfg(feature = "kubernetes")]
fn kubernetes_service_name(uri: &Uri) -> Option<&str> {
    uri.host().and_then(|host| host.split('.').next())
}

#[cfg(not(feature = "kubernetes"))]
async fn cluster_api_channel(
    uri: Uri,
    discovery: EndpointDiscovery,
    transport: pplx_space_common_grpc::internal_transport::ClientTransport,
) -> Result<RotatingChannel, ConnectClusterApiError> {
    if discovery == EndpointDiscovery::Kubernetes {
        tracing::warn!(
            env = CLUSTER_API_ENDPOINT_DISCOVERY_ENV,
            "endpoint discovery is configured but this binary was built without it"
        );
    }
    rotating_channel(uri, transport).await
}

async fn rotating_channel(
    uri: Uri,
    transport: pplx_space_common_grpc::internal_transport::ClientTransport,
) -> Result<RotatingChannel, ConnectClusterApiError> {
    transport
        .connect_endpoint(cluster_api_endpoint(uri))
        .map_err(ConnectClusterApiError::InternalTransport)?
        .connect_rotating(
            CLUSTER_API_SERVICE_NAME,
            pplx_space_common_grpc::max_connection_age_from_env(),
        )
        .await
        .map_err(ConnectClusterApiError::Transport)
}

fn client_with_interceptor(
    channel: RotatingChannel,
    interceptor: BearerAuthInterceptor,
) -> AuthenticatedClusterApiClient {
    let intercepted = InterceptedService::new(channel, interceptor);
    ClusterApiClient::new(wrap_grpc_client_channel(intercepted))
}

fn required_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
