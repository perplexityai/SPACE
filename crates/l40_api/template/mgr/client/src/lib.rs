pub mod proto {
    tonic::include_proto!("space.template.manager.v1");
}

use pplx_space_common_core::types::{TemplateReference, TemplateSelector};
use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::transport::{Channel, Endpoint};

/// Unix socket path for the template-manager gRPC service.
pub const SOCKET_PATH: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_MANAGER_SOCKET;
/// Server request budget, including cold-cache artifact materialization in GetTemplate.
pub const GRPC_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(10);
const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

pub type TemplateManagerGrpcClient =
    proto::template_manager_client::TemplateManagerClient<GrpcClientTrace<Channel>>;

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("template-manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("template-manager connect failed: {0}")]
    Connect(#[from] pplx_space_common_grpc::UdsConnectError),
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateRefFromReferenceError {
    #[error(
        "template reference '{reference}' must use a tag or full artifact digest for template-manager requests"
    )]
    ArtifactDigestPrefix { reference: String },
}

impl TryFrom<&TemplateReference> for proto::TemplateRef {
    type Error = TemplateRefFromReferenceError;

    fn try_from(reference: &TemplateReference) -> Result<Self, Self::Error> {
        let mut template = Self {
            name: reference.name().as_str().to_string(),
            content_hash: None,
            tag: None,
            environment: None,
        };
        match reference.selector() {
            TemplateSelector::Tag(tag) => {
                template.tag = Some(tag.as_str().to_string());
            }
            TemplateSelector::ArtifactDigest(digest) => {
                template.content_hash = Some(digest.content_hash().to_string());
            }
            TemplateSelector::ArtifactDigestPrefix(_) => {
                return Err(TemplateRefFromReferenceError::ArtifactDigestPrefix {
                    reference: reference.to_string(),
                });
            }
        }
        Ok(template)
    }
}

impl proto::TemplateFullInfo {
    /// Environment the listed build is bound to; `None` for unbound builds.
    /// Template-manager caches a rootfs once per environment it's bound to,
    /// so one digest can be listed more than once.
    pub fn environment(
        &self,
    ) -> Result<
        Option<pplx_space_common_core::types::EnvironmentReference>,
        pplx_space_common_core::types::DigestParseError,
    > {
        self.execution
            .as_ref()
            .and_then(|execution| execution.environment.as_deref())
            .map(str::parse)
            .transpose()
    }
}

fn endpoint() -> Result<Endpoint, tonic::transport::Error> {
    Endpoint::try_from(LOCAL_ENDPOINT_URI)
}

/// Connect to the template-manager gRPC service over Unix socket.
pub async fn connect() -> Result<TemplateManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint_with_retry(
        "template-manager",
        std::path::Path::new(SOCKET_PATH),
        endpoint()?,
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await?;
    Ok(proto::template_manager_client::TemplateManagerClient::new(
        wrap_grpc_client_channel(channel),
    ))
}

/// Build a template-manager client without opening the Unix socket immediately.
pub fn lazy() -> TemplateManagerGrpcClient {
    static CLIENT: std::sync::OnceLock<TemplateManagerGrpcClient> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let channel = pplx_space_common_grpc::connect_uds_channel_lazy(
                std::path::Path::new(SOCKET_PATH),
                endpoint().expect("static endpoint URI is valid"),
            );
            proto::template_manager_client::TemplateManagerClient::new(wrap_grpc_client_channel(
                channel,
            ))
        })
        .clone()
}

#[cfg(test)]
mod tests;
