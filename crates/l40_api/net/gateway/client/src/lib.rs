pub use pplx_space_net_common::{orig_dst, proxy::*};

pub use pplx_space_net_gateway_protocol::proto;

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};

pub type NetGatewayControlGrpcClient = proto::net_gateway_control_client::NetGatewayControlClient<
    GrpcClientTrace<tonic::transport::Channel>,
>;

/// Route-identity profile this build of net-gateway advertises.
pub const IDENTITY_PROFILE_VERSION: u32 = 1;

/// Oldest route-identity profile that supports verification-key updates. Kept
/// separate from the advertised version so bumping the profile does not
/// retroactively declare every older gateway unable to take keys.
pub const MIN_IDENTITY_PROFILE_VERSION_FOR_VERIFICATION_KEYS: u32 = 1;

/// TCP domain-authorization behavior selected once when net-gateway starts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TcpDomainAuthorizationMode {
    #[default]
    Off,
    Observe,
    Enforce,
}

impl TcpDomainAuthorizationMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

impl std::fmt::Display for TcpDomainAuthorizationMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown TCP domain-authorization mode '{value}'")]
pub struct TcpDomainAuthorizationModeParseError {
    value: String,
}

impl std::str::FromStr for TcpDomainAuthorizationMode {
    type Err = TcpDomainAuthorizationModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "observe" => Ok(Self::Observe),
            "enforce" => Ok(Self::Enforce),
            value => Err(TcpDomainAuthorizationModeParseError {
                value: value.to_string(),
            }),
        }
    }
}

/// Timeout for idle UDP mapping entries.
pub const UDP_MAPPING_TIMEOUT_SECS: u64 = 30;

/// Maximum time a ServicePortIngress caller may hold a request open without
/// sending its required start frame. Both sandbox-manager and net-gateway
/// enforce this at their respective streaming boundaries.
pub const SERVICE_PORT_INGRESS_START_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(3);

/// Per-sandbox ServicePortIngress concurrency contract. Sandbox-manager
/// enforces this before opening a control connection so old gateways remain
/// protected during rollout; current gateways enforce it again locally. Each
/// layer reserves at the earliest point where it knows its own scope: the
/// gateway reserves before the untrusted start frame, while sandbox-manager
/// reserves after parsing and authorizing the sandbox identity.
pub const MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS: usize = 64;

/// Protected listener path for the net-gateway control service.
///
/// The compatibility alias lives in the sandbox UID's writable directory and
/// cannot authenticate a gateway. Clients must never fall back to that alias.
pub fn control_socket_path(sandbox_id: &str) -> String {
    pplx_space_common_core::host::linux::paths::isolated_net_gateway_control_socket(sandbox_id)
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("net-gateway transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("net-gateway connect failed: {0}")]
    Connect(#[from] pplx_space_common_grpc::UdsConnectError),
    #[error(
        "protected net-gateway control endpoint unavailable for sandbox {sandbox_id}: {source}; if the gateway predates isolation, cycle it with space-sbxmgr cycle-net-gateway {sandbox_id} --execute"
    )]
    ProtectedEndpoint {
        sandbox_id: String,
        #[source]
        source: Box<ConnectError>,
    },
}

/// Connect to a net-gateway control service at an arbitrary Unix socket path.
pub async fn connect_path(socket_path: &str) -> Result<NetGatewayControlGrpcClient, ConnectError> {
    connect_path_with_retry_policy(
        socket_path,
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
}

/// Connect to a net-gateway control service with an operation-specific retry budget.
pub async fn connect_path_with_retry_policy(
    socket_path: &str,
    retry_policy: pplx_space_common_grpc::GrpcRetryPolicy,
) -> Result<NetGatewayControlGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_retry(
        "net-gateway",
        std::path::Path::new(socket_path),
        retry_policy,
    )
    .await?;
    Ok(
        proto::net_gateway_control_client::NetGatewayControlClient::new(wrap_grpc_client_channel(
            channel,
        )),
    )
}

/// Connect to a sandbox's net-gateway control service over Unix socket.
pub async fn connect(sandbox_id: &str) -> Result<NetGatewayControlGrpcClient, ConnectError> {
    connect_with_retry_policy(
        sandbox_id,
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
}

/// Connect to a sandbox's net-gateway with an operation-specific retry budget.
pub async fn connect_with_retry_policy(
    sandbox_id: &str,
    retry_policy: pplx_space_common_grpc::GrpcRetryPolicy,
) -> Result<NetGatewayControlGrpcClient, ConnectError> {
    connect_path_with_retry_policy(&control_socket_path(sandbox_id), retry_policy)
        .await
        .map_err(|source| ConnectError::ProtectedEndpoint {
            sandbox_id: sandbox_id.to_owned(),
            source: Box::new(source),
        })
}

#[cfg(test)]
mod tests;
pub const DEFAULT_SERVICE_PORT_INGRESS_IDLE_TIMEOUT_SECS: f64 = 1800.0;
pub fn service_port_ingress_idle_timeout_from_env() -> std::time::Duration {
    std::time::Duration::from_secs_f64(DEFAULT_SERVICE_PORT_INGRESS_IDLE_TIMEOUT_SECS)
}
/// Temporary rollout opt-in/acknowledgement for completion framing.
/// Remove with the fallback after deployment; older forwarding hops filter it out.
pub const INGRESS_COMPLETION_METADATA: &str = "x-space-ingress-completion";
