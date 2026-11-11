//! Authenticated gRPC client for the provider-neutral intermediate-key broker.
//! The credmgr authority uses it through the `intermediate_key_broker` backend;
//! broker implementations use the dependency-light generated proto crate.

pub use pplx_space_credential_provider_proto::v2;

use std::path::PathBuf;

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

pub use v2::intermediate_key_broker_client::IntermediateKeyBrokerClient;

/// Traced, authenticated v2 client for the provider-neutral broker contract.
pub type IntermediateKeyBrokerGrpcClient = IntermediateKeyBrokerClient<
    GrpcClientTrace<InterceptedService<Channel, BearerTokenFileInterceptor>>,
>;

/// Attaches `authorization: Bearer <token>` to every request, reading the
/// token file fresh per call so rotated credentials — including a projected
/// ServiceAccount token, the intended production shape — are picked up on
/// the long-lived channel without reconnecting. Read or format failures
/// surface as UNAVAILABLE (retriable local infrastructure, not an outcome).
#[derive(Debug, Clone)]
pub struct BearerTokenFileInterceptor {
    path: PathBuf,
}

impl BearerTokenFileInterceptor {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl tonic::service::Interceptor for BearerTokenFileInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let token = std::fs::read_to_string(&self.path).map_err(|error| {
            tonic::Status::unavailable(format!(
                "intermediate-key broker bearer token unreadable at {}: {error}",
                self.path.display()
            ))
        })?;
        let token = token.trim();
        if token.is_empty() {
            return Err(tonic::Status::unavailable(format!(
                "intermediate-key broker bearer token at {} is empty",
                self.path.display()
            )));
        }
        let value = format!("Bearer {token}").parse().map_err(|_| {
            tonic::Status::unavailable(
                "intermediate-key broker bearer token is not valid header ASCII",
            )
        })?;
        request.metadata_mut().insert("authorization", value);
        Ok(request)
    }
}

/// Wrap a connected channel for the provider-neutral v2 broker contract.
pub fn traced_intermediate_key_broker_client(
    channel: Channel,
    interceptor: BearerTokenFileInterceptor,
) -> IntermediateKeyBrokerGrpcClient {
    let intercepted = InterceptedService::new(channel, interceptor);
    IntermediateKeyBrokerClient::new(wrap_grpc_client_channel(intercepted))
}

/// Endpoint env var the credmgr authority reads to find the provider.
/// Unset => the intermediate-key-broker backend is not installed and orgs on
/// that provider fail closed with `UnsupportedProvider`.
pub const INTERMEDIATE_KEY_BROKER_URL_ENV: &str = "SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_URL";

/// Path env var for the client bearer credential file. Required whenever the
/// backend is enabled: the provider vends plaintext key material, so the
/// caller always authenticates itself beyond network reachability.
pub const INTERMEDIATE_KEY_BROKER_TOKEN_FILE_ENV: &str =
    "SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TOKEN_FILE";
