pub mod runtime;

pub mod proto {
    tonic::include_proto!("build.manager.v1");
}

use std::path::Path;

use pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY;
use pplx_space_common_grpc::internal_transport::{self, ClientTransport, Service};
use pplx_space_common_svc::auth::{SPACE_INTERNAL_TOKEN_ENV, SpaceBearerInterceptor};
use pplx_space_common_svc::bearer_token::SpaceBearerToken;
use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use tokio::sync::mpsc;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

const DEFAULT_ENDPOINT: &str = "127.0.0.1:9900";
const SPACE_BUILD_MANAGER_ENDPOINT_ENV: &str = "SPACE_BUILD_MANAGER_ENDPOINT";
const DEFAULT_SPACE_ENV_FILE: &str = "/space/config/space.env";
pub const BLOB_UPLOAD_CHUNK_BYTES: usize = 1024 * 1024;
pub const BLOB_UPLOAD_LIMIT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const GRPC_MESSAGE_SIZE_LIMIT_BYTES: usize = BLOB_UPLOAD_CHUNK_BYTES + 16 * 1024;
pub const BUILD_MANAGER_DRAINING_MESSAGE: &str =
    "build-manager is draining and not accepting new builds";

/// Whether an RPC was rejected before submission because build-manager is draining.
///
/// Node API preserves the gRPC code but prefixes the daemon's message while
/// forwarding `StartBuild`, so callers match the stable message suffix.
pub fn is_build_manager_draining_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && status.message().ends_with(BUILD_MANAGER_DRAINING_MESSAGE)
}

#[derive(Debug, thiserror::Error)]
#[error("blob upload receiver closed")]
pub struct BlobUploadSendError;

pub struct BlobUploadSender {
    sender: mpsc::Sender<proto::UploadBlobChunk>,
}

impl BlobUploadSender {
    pub async fn begin(
        sender: mpsc::Sender<proto::UploadBlobChunk>,
    ) -> Result<Self, BlobUploadSendError> {
        let upload = Self { sender };
        upload
            .send(Vec::new(), proto::UploadBlobPhase::Data)
            .await?;
        Ok(upload)
    }

    pub async fn send_data(&self, data: Vec<u8>) -> Result<(), BlobUploadSendError> {
        self.send(data, proto::UploadBlobPhase::Data).await
    }

    pub async fn commit(self) -> Result<(), BlobUploadSendError> {
        self.send(Vec::new(), proto::UploadBlobPhase::Commit).await
    }

    async fn send(
        &self,
        data: Vec<u8>,
        phase: proto::UploadBlobPhase,
    ) -> Result<(), BlobUploadSendError> {
        self.sender
            .send(proto::UploadBlobChunk {
                data,
                phase: phase as i32,
            })
            .await
            .map_err(|_| BlobUploadSendError)
    }
}

pub type BuildManagerGrpcClient = proto::build_manager_client::BuildManagerClient<
    GrpcClientTrace<InterceptedService<Channel, BearerAuthInterceptor>>,
>;

#[derive(Debug, Clone)]
pub enum BearerAuthInterceptor {
    Fixed(MetadataValue<Ascii>),
    Projected(SpaceBearerToken),
}

impl BearerAuthInterceptor {
    pub const fn new(bearer: MetadataValue<Ascii>) -> Self {
        Self::Fixed(bearer)
    }

    fn from_env() -> Result<Self, ConnectError> {
        let kind = SpaceBearerToken::BuildManager;
        if std::env::var_os(kind.path_env()).is_some() || kind.default_path().exists() {
            Ok(Self::Projected(kind))
        } else {
            let token = required_internal_token().ok_or(ConnectError::MissingToken)?;
            let bearer = format!("Bearer {token}")
                .parse()
                .map_err(ConnectError::InvalidBearer)?;
            Ok(Self::Fixed(bearer))
        }
    }
}

impl tonic::service::Interceptor for BearerAuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        match self {
            Self::Fixed(bearer) => {
                request
                    .metadata_mut()
                    .insert("authorization", bearer.clone());
                Ok(request)
            }
            Self::Projected(kind) => SpaceBearerInterceptor::new(*kind).call(request),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    InternalTransport(#[from] internal_transport::TransportError),
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("SPACE_INTERNAL_TOKEN is not set and /space/config/space.env does not contain it")]
    MissingToken,
    #[error("SPACE_INTERNAL_TOKEN is not valid as a bearer token: {0}")]
    InvalidBearer(#[source] tonic::metadata::errors::InvalidMetadataValue),
    #[error("build manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("build manager connect failed: {0}")]
    TcpConnect(#[source] pplx_space_common_grpc::TcpConnectError),
}

impl From<pplx_space_common_grpc::TcpConnectError> for ConnectError {
    /// A connect's transport failure stays [`ConnectError::Transport`]; only an
    /// abandoned attempt is [`ConnectError::TcpConnect`].
    fn from(error: pplx_space_common_grpc::TcpConnectError) -> Self {
        match error {
            pplx_space_common_grpc::TcpConnectError::Transport(error) => Self::Transport(error),
            error => Self::TcpConnect(error),
        }
    }
}

pub async fn connect() -> Result<BuildManagerGrpcClient, ConnectError> {
    let channel = connect_channel(build_manager_endpoint()).await?;
    client_from_env(channel)
}

pub async fn connect_with_token(
    endpoint: impl Into<String>,
    token: impl AsRef<str>,
) -> Result<BuildManagerGrpcClient, ConnectError> {
    let channel = connect_channel(endpoint.into()).await?;
    client_with_token(channel, token)
}

async fn connect_channel(endpoint: String) -> Result<Channel, ConnectError> {
    let endpoint = internal_transport::qualify_endpoint(&endpoint);
    let transport = internal_transport::client_connect_endpoint(
        Service::BuildManager,
        Endpoint::from_shared(endpoint.clone())?.tcp_nodelay(true),
    )
    .await?;
    Ok(transport
        .connect_with_retry(
            "build-manager",
            &endpoint,
            DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await?)
}

pub fn connect_lazy() -> Result<BuildManagerGrpcClient, ConnectError> {
    client_from_env(lazy_channel(build_manager_endpoint())?)
}

fn lazy_channel(endpoint: String) -> Result<Channel, ConnectError> {
    let endpoint =
        Endpoint::from_shared(internal_transport::qualify_endpoint(&endpoint))?.tcp_nodelay(true);
    Ok(
        ClientTransport::from_env_sync(Service::BuildManager, endpoint.uri())?
            .endpoint(endpoint)?
            .connect_lazy(),
    )
}

/// Build an environment-authenticated client on a fixture's transport.
#[cfg(feature = "test-support")]
pub fn test_client_from_env(channel: Channel) -> Result<BuildManagerGrpcClient, ConnectError> {
    client_from_env(channel)
}

fn client_from_env(channel: Channel) -> Result<BuildManagerGrpcClient, ConnectError> {
    Ok(client_with_interceptor(
        channel,
        BearerAuthInterceptor::from_env()?,
    ))
}

pub fn client_with_token(
    channel: Channel,
    token: impl AsRef<str>,
) -> Result<BuildManagerGrpcClient, ConnectError> {
    let bearer = format!("Bearer {}", token.as_ref())
        .parse()
        .map_err(ConnectError::InvalidBearer)?;
    Ok(client_with_bearer(channel, bearer))
}

pub fn client_with_bearer(
    channel: Channel,
    bearer: MetadataValue<Ascii>,
) -> BuildManagerGrpcClient {
    client_with_interceptor(channel, BearerAuthInterceptor::new(bearer))
}

fn client_with_interceptor(
    channel: Channel,
    interceptor: BearerAuthInterceptor,
) -> BuildManagerGrpcClient {
    let intercepted = InterceptedService::new(channel, interceptor);
    proto::build_manager_client::BuildManagerClient::new(wrap_grpc_client_channel(intercepted))
        .max_encoding_message_size(GRPC_MESSAGE_SIZE_LIMIT_BYTES)
        .max_decoding_message_size(GRPC_MESSAGE_SIZE_LIMIT_BYTES)
}

pub fn build_manager_endpoint() -> String {
    std::env::var(SPACE_BUILD_MANAGER_ENDPOINT_ENV)
        .ok()
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .map(|endpoint| internal_transport::qualify_endpoint(&endpoint))
        .unwrap_or_else(|| internal_transport::qualify_endpoint(DEFAULT_ENDPOINT))
}

fn required_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        _ => None,
    }
}

fn required_internal_token() -> Option<String> {
    required_env(SPACE_INTERNAL_TOKEN_ENV)
        .or_else(|| read_env_file_value(DEFAULT_SPACE_ENV_FILE, SPACE_INTERNAL_TOKEN_ENV))
}

fn read_env_file_value(path: impl AsRef<Path>, name: &str) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    parse_env_file_value(&content, name)
}

fn parse_env_file_value(content: &str, name: &str) -> Option<String> {
    content.lines().find_map(|raw| {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        (key.trim() == name)
            .then(|| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests;
