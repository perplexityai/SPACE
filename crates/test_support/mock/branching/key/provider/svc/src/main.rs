//! Local-only implementation of the public intermediate-key broker contract.
//!
//! This is an integration fixture, not a cryptographic key authority. It
//! derives deterministic material so the compose harness can exercise the
//! full external-provider path without cloud credentials or services.

use std::net::SocketAddr;

use clap::Parser;
use pplx_space_credential_provider_client::v2::intermediate_key_broker_server::{
    IntermediateKeyBroker, IntermediateKeyBrokerServer,
};
use pplx_space_credential_provider_client::v2::issue_intermediate_key_response::Outcome as IssueOutcome;
use pplx_space_credential_provider_client::v2::unwrap_intermediate_key_response::Outcome as UnwrapOutcome;
use pplx_space_credential_provider_client::v2::{
    IntermediateKeyMaterial, IssueFailure, IssueFailureReason, IssueIntermediateKeyRequest,
    IssueIntermediateKeyResponse, TenantRef, UnwrapFailure, UnwrapFailureReason,
    UnwrapIntermediateKeyRequest, UnwrapIntermediateKeyResponse, WrappedIntermediateKey,
};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

#[derive(Parser)]
#[command(
    name = "space-mock-branching-key-provider-svc",
    about = "Local integration fixture for SPACE's intermediate-key broker contract",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[arg(long, default_value = "0.0.0.0:50051")]
    listen: SocketAddr,

    #[arg(long, default_value = "local-branching-provider-token")]
    bearer_token: String,
}

struct MockBroker {
    authorization: String,
}

impl MockBroker {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        if presented == Some(self.authorization.as_str()) {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid bearer credential"))
        }
    }
}

fn tenant_scope(tenant: &TenantRef) -> String {
    format!("{}:{}", tenant.namespace, tenant.id)
}

fn key_id(tenant: &TenantRef) -> String {
    format!("local-integration-{}", tenant_scope(tenant))
}

fn derived_bytes(domain: &str, tenant: &TenantRef) -> Vec<u8> {
    Sha256::digest(format!("{domain}:{}", tenant_scope(tenant)).as_bytes()).to_vec()
}

fn valid_tenant(tenant: &TenantRef) -> bool {
    !tenant.namespace.is_empty() && !tenant.id.is_empty()
}

#[tonic::async_trait]
impl IntermediateKeyBroker for MockBroker {
    async fn issue_intermediate_key(
        &self,
        request: Request<IssueIntermediateKeyRequest>,
    ) -> Result<Response<IssueIntermediateKeyResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let outcome = match request.tenant {
            Some(tenant) if valid_tenant(&tenant) => IssueOutcome::Key(WrappedIntermediateKey {
                key_id: key_id(&tenant),
                wrapped_key: derived_bytes("wrapped", &tenant),
            }),
            _ => IssueOutcome::Failure(IssueFailure {
                reason: IssueFailureReason::InvalidTenant as i32,
                detail: "tenant namespace and id are required".to_owned(),
            }),
        };
        Ok(Response::new(IssueIntermediateKeyResponse {
            outcome: Some(outcome),
        }))
    }

    async fn unwrap_intermediate_key(
        &self,
        request: Request<UnwrapIntermediateKeyRequest>,
    ) -> Result<Response<UnwrapIntermediateKeyResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let tenant = request.tenant.as_ref();
        let valid = tenant.is_some_and(|tenant| {
            valid_tenant(tenant)
                && request.key_id == key_id(tenant)
                && request.wrapped_key == derived_bytes("wrapped", tenant)
        });
        let outcome = if let Some(tenant) = tenant.filter(|_| valid) {
            UnwrapOutcome::Key(IntermediateKeyMaterial {
                plaintext_key: derived_bytes("plaintext", tenant),
                max_cache_ttl: Some(prost_types::Duration {
                    seconds: 120,
                    nanos: 0,
                }),
            })
        } else {
            UnwrapOutcome::Failure(UnwrapFailure {
                reason: UnwrapFailureReason::InvalidKey as i32,
                detail: "unknown local integration key".to_owned(),
            })
        };
        Ok(Response::new(UnwrapIntermediateKeyResponse {
            outcome: Some(outcome),
        }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = pplx_space_common_svc::init_service("space-mock-branching-key-provider")?;
    let cli = Cli::parse();
    let broker = MockBroker {
        authorization: format!("Bearer {}", cli.bearer_token),
    };
    tracing::info!(listen = %cli.listen, "mock intermediate-key broker listening");
    tonic::transport::Server::builder()
        .add_service(IntermediateKeyBrokerServer::new(broker))
        .serve(cli.listen)
        .await?;
    Ok(())
}
