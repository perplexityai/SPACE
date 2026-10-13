pub mod proto {
    tonic::include_proto!("space.credential.manager.v1");

    macro_rules! redacted_debug {
        ($($ty:ty => $name:literal),+ $(,)?) => {
            $(
                impl std::fmt::Debug for $ty {
                    fn fmt(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str(concat!($name, "(<redacted>)"))
                    }
                }
            )+
        };
    }

    // Protobuf messages derive `Debug` by default. Treat each transport type
    // that carries a raw credential or plaintext secret as one opaque value so
    // adding a new field cannot silently make it loggable.
    redacted_debug! {
        ExchangeTokenRequest => "ExchangeTokenRequest",
        ExchangeTokenResponse => "ExchangeTokenResponse",
        exchange_token_response::Payload => "Payload",
        AwsCredentials => "AwsCredentials",
        IssueDekResponse => "IssueDekResponse",
        UnwrapDekResponse => "UnwrapDekResponse",
        EncryptRequest => "EncryptRequest",
        DecryptResponse => "DecryptResponse",
        GetOrIssueSandboxVolumeDekResponse => "GetOrIssueSandboxVolumeDekResponse",
        ResolveNetworkCredentialsResponse => "ResolveNetworkCredentialsResponse",
    }
}

mod endpoint;
mod token;

/// Fixed subject and size limit for both hops of intermediate CA issuance.
pub const INTERMEDIATE_CSR_COMMON_NAME: &str = "space-node-intermediate-ca";
pub const MAX_INTERMEDIATE_CSR_BYTES: usize = 4096;

use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

pub use endpoint::{CredmgrEndpoint, EndpointConfigError, TlsFiles};
pub use proto::credential_manager_client::CredentialManagerClient;
pub use token::{BearerTokenError, SPACE_CREDMGR_TOKEN_PATH_ENV, bearer_metadata, bearer_token};

pub type CredentialManagerGrpcClient =
    CredentialManagerClient<GrpcClientTrace<InterceptedService<Channel, CredmgrBearerInterceptor>>>;

/// Attaches the credmgr bearer credential to TCP requests. The token (a
/// projected ServiceAccount JWT, or the static-secret fallback) is read fresh
/// per request from [`token::bearer_metadata`] so kubelet rotation is picked
/// up on the long-lived channel without reconnecting. The UDS transport
/// authorizes by Linux peer credentials and sends no token.
#[derive(Debug, Clone, Copy)]
pub enum CredmgrBearerInterceptor {
    /// Unix socket: peer-credential auth, no bearer.
    None,
    /// TCP/TLS: resolve and attach the bearer token on every request.
    TokenSource,
}

impl tonic::service::Interceptor for CredmgrBearerInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if matches!(self, Self::TokenSource) {
            // A token-source read failure is local, transient infrastructure
            // (an unreadable projected-token mount), so surface UNAVAILABLE
            // (retryable) rather than a permanent error.
            let value = token::bearer_metadata().map_err(|error| {
                tonic::Status::unavailable(format!("credmgr bearer token unavailable: {error}"))
            })?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

fn build_client(
    channel: Channel,
    interceptor: CredmgrBearerInterceptor,
) -> CredentialManagerGrpcClient {
    let intercepted = InterceptedService::new(channel, interceptor);
    CredentialManagerClient::new(wrap_grpc_client_channel(intercepted))
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("credential-manager endpoint configuration: {0}")]
    Config(#[from] EndpointConfigError),
    #[error("failed to read TLS material from {path}: {source}")]
    TlsRead {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("credential-manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("credential-manager connect failed: {0}")]
    Connect(#[from] pplx_space_common_grpc::UdsConnectError),
    #[error("credential-manager connect failed: {0}")]
    TcpConnect(#[source] pplx_space_common_grpc::TcpConnectError),
}

impl From<pplx_space_common_grpc::TcpConnectError> for ConnectError {
    /// A TCP connect's transport failure stays [`ConnectError::Transport`]; only an
    /// abandoned attempt is [`ConnectError::TcpConnect`].
    fn from(error: pplx_space_common_grpc::TcpConnectError) -> Self {
        match error {
            pplx_space_common_grpc::TcpConnectError::Transport(error) => Self::Transport(error),
            error => Self::TcpConnect(error),
        }
    }
}

/// Connect to credmgr at the endpoint configured by the
/// `SPACE_CREDMGR_ENDPOINT` family of environment variables, defaulting to
/// the node-local Unix socket. The environment is parsed once per process;
/// TLS material is re-read from disk on every connect so renewed
/// certificates are picked up without a restart.
pub async fn connect_from_env() -> Result<CredentialManagerGrpcClient, ConnectError> {
    endpoint::cached_from_env()?.connect().await
}

/// Connect to the credmgr gRPC service over Unix socket. No bearer token:
/// the UDS listener authorizes by Linux peer credentials.
pub async fn connect(socket_path: &str) -> Result<CredentialManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_retry(
        "credential-manager",
        std::path::Path::new(socket_path),
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await?;
    Ok(build_client(channel, CredmgrBearerInterceptor::None))
}

/// Default socket path for credmgr.
pub const CREDMGR_SOCKET: &str =
    pplx_space_common_core::host::linux::paths::CREDENTIAL_MANAGER_SOCKET;

/// Authority budget for network-credential resolution, including both state
/// snapshots and provider work. Callers also need time for connection, local
/// authorization, and response delivery.
pub const NETWORK_CREDENTIAL_RESOLUTION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(15);

/// Metadata key on credmgr error `Status`es whose cause callers must react
/// to programmatically. Values are the `CREDMGR_ERROR_REASON_*` constants;
/// absence means the status carries no machine-readable reason.
pub const CREDMGR_ERROR_REASON_METADATA_KEY: &str = "x-space-credmgr-error-reason";

/// Reason value: the org's KEK access was revoked at its external provider.
/// Terminal for the org until re-enrollment — consumers trigger org cleanup
/// and sandbox shutdown on this, never retry.
pub const CREDMGR_ERROR_REASON_ORG_KEK_ACCESS_REVOKED: &str = "org-kek-access-revoked";

/// Reason value: the external provider rejected the persisted KEK reference.
/// Permanent for that key reference; callers must not retry the same request.
pub const CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE: &str = "invalid-key-reference";

/// Reason prefix: the node credential agent holds no intermediate CA it can
/// sign gateway CAs under, so `IssueIntermediateCertificate` answered
/// `UNAVAILABLE`. The suffix names why — `authority_unavailable`,
/// `authority_not_configured`, `authority_refused`, or
/// `unusable_certificate` — for dashboards and logs. Whatever the suffix, the
/// node retries the authority itself on a bounded schedule, so callers keep
/// retrying with the same key and never treat this as terminal. The same
/// `<prefix><suffix>` token also opens the status message.
pub const CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX: &str = "node_ca_unavailable:";

/// True when a credmgr error `Status` states that the node credential agent
/// currently holds no usable intermediate CA, for any reason. Checks the
/// machine-readable reason metadata, never the human-readable message.
pub fn status_is_node_ca_unavailable(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && status
            .metadata()
            .get(CREDMGR_ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with(CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX))
}

/// True when a credmgr error `Status` states that the org's KEK access was
/// revoked at its external provider. Checks the machine-readable reason
/// metadata, never the human-readable message.
pub fn status_is_org_kek_access_revoked(status: &tonic::Status) -> bool {
    status
        .metadata()
        .get(CREDMGR_ERROR_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        == Some(CREDMGR_ERROR_REASON_ORG_KEK_ACCESS_REVOKED)
}

/// True when a credmgr error `Status` states that the external provider
/// rejected the persisted KEK reference.
pub fn status_is_invalid_key_reference(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status
            .metadata()
            .get(CREDMGR_ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE)
}

/// Reason value: a storage write vend named a template release that already
/// has a registered storage location. Releases are immutable, so the artifacts
/// are published and only tags remain to register; a builder that meets this
/// after its own publication check converges on tags-only registration instead
/// of failing the build.
pub const CREDMGR_ERROR_REASON_RELEASE_ALREADY_PUBLISHED: &str = "release-already-published";

/// True when a credmgr error `Status` refuses a storage write because the
/// template release is already published. Checks the machine-readable reason
/// metadata, never the human-readable message, and only for
/// `PERMISSION_DENIED`, so unrelated denials keep failing the caller.
pub fn status_is_release_already_published(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::PermissionDenied
        && status
            .metadata()
            .get(CREDMGR_ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(CREDMGR_ERROR_REASON_RELEASE_ALREADY_PUBLISHED)
}

/// RFC 8693 `grant_type` URI for the token-exchange flow.
pub const TOKEN_EXCHANGE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";

/// Token-type URI advertised on the issued AWS-credentials variant.
/// Mirrors the convention used in RFC 8693 implementations that vend
/// AWS sigv4 credentials.
pub const AWS_CREDENTIALS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:aws-credentials";

/// Audience required for storage credential exchange.
pub const STORAGE_TOKEN_AUDIENCE: &str = "s3.amazonaws.com";

/// Token type for a canonical `SubjectId` (`sandbox:<uuid>` or
/// `template:<name>`).
pub const SUBJECT_ID_TOKEN_TYPE: &str = "urn:space:token-type:subject-id";

/// Token type for a lowercase hexadecimal SHA-256 build-input digest.
pub const BUILD_INPUT_SHA256_TOKEN_TYPE: &str = "urn:space:token-type:build-input-sha256";

/// Token type used by the authenticated node credential agent to carry the
/// original, locally authorized SPACE service identity.
pub const SERVICE_ACTOR_TOKEN_TYPE: &str = "urn:space:token-type:service";

/// Canonical storage scopes understood by Credential Manager.
pub const STORAGE_READ_SCOPE: &str = "space.storage.read";
pub const STORAGE_WRITE_SCOPE: &str = "space.storage.write";

/// Storage credentials last one hour so multi-request transfers can finish
/// without in-process refresh support.
pub const STORAGE_SESSION_TTL_SECONDS: u32 = 60 * 60;

#[cfg(test)]
mod debug_redaction_tests;
#[cfg(test)]
mod error_reason_tests;
#[cfg(test)]
mod zeroize_tests;
