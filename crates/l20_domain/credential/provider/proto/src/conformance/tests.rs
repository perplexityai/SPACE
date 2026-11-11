use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use super::*;
use crate::v2::intermediate_key_broker_server::{
    IntermediateKeyBroker, IntermediateKeyBrokerServer,
};
use crate::v2::{IssueFailure, UnwrapFailure};

const SUPPORTED_SCHEME: &str = "urn:example:broker-grant:v1";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct KeyTuple {
    namespace: String,
    tenant_id: String,
    key_id: String,
    wrapped_key: Vec<u8>,
}

#[derive(Debug)]
struct FixtureState {
    require_evidence: bool,
    next_key: u8,
    backend_generation: u8,
    issue_failure: Option<IssueFailureReason>,
    unwrap_failure: Option<UnwrapFailureReason>,
    keys: HashMap<KeyTuple, Vec<u8>>,
}

#[derive(Clone)]
struct FixtureBroker {
    state: Arc<Mutex<FixtureState>>,
}

impl FixtureBroker {
    fn authorize(
        state: &FixtureState,
        tenant: &TenantRef,
        authorization: Option<&AuthorizationEvidence>,
    ) -> bool {
        match authorization {
            None => !state.require_evidence,
            Some(authorization) => {
                authorization.scheme == SUPPORTED_SCHEME
                    && authorization.evidence == evidence_for(tenant).evidence
            }
        }
    }
}

#[tonic::async_trait]
impl IntermediateKeyBroker for FixtureBroker {
    async fn issue_intermediate_key(
        &self,
        request: Request<IssueIntermediateKeyRequest>,
    ) -> Result<Response<IssueIntermediateKeyResponse>, Status> {
        let request = request.into_inner();
        let Some(tenant) = request.tenant else {
            return Ok(Response::new(issue_failure(
                IssueFailureReason::InvalidTenant,
            )));
        };
        let mut state = self
            .state
            .lock()
            .expect("fixture state must not be poisoned");
        if !Self::authorize(&state, &tenant, request.authorization.as_ref()) {
            return Ok(Response::new(issue_failure(
                IssueFailureReason::AuthorizationRejected,
            )));
        }
        if let Some(reason) = state.issue_failure {
            return Ok(Response::new(issue_failure(reason)));
        }

        state.next_key = state
            .next_key
            .checked_add(1)
            .expect("fixture key space exhausted");
        let generation = state.next_key;
        let key_id = format!("fixture-key-{generation}");
        let wrapped_key =
            format!("backend-{}-key-{generation}", state.backend_generation).into_bytes();
        let plaintext_key = vec![generation; 32];
        state.keys.insert(
            KeyTuple {
                namespace: tenant.namespace,
                tenant_id: tenant.id,
                key_id: key_id.clone(),
                wrapped_key: wrapped_key.clone(),
            },
            plaintext_key,
        );
        Ok(Response::new(IssueIntermediateKeyResponse {
            outcome: Some(IssueOutcome::Key(WrappedIntermediateKey {
                key_id,
                wrapped_key,
            })),
        }))
    }

    async fn unwrap_intermediate_key(
        &self,
        request: Request<UnwrapIntermediateKeyRequest>,
    ) -> Result<Response<UnwrapIntermediateKeyResponse>, Status> {
        let request = request.into_inner();
        let Some(tenant) = request.tenant else {
            return Ok(Response::new(unwrap_failure(
                UnwrapFailureReason::InvalidKey,
            )));
        };
        let state = self
            .state
            .lock()
            .expect("fixture state must not be poisoned");
        if !Self::authorize(&state, &tenant, request.authorization.as_ref()) {
            return Ok(Response::new(unwrap_failure(
                UnwrapFailureReason::AuthorizationRejected,
            )));
        }
        if let Some(reason) = state.unwrap_failure {
            return Ok(Response::new(unwrap_failure(reason)));
        }
        let Some(plaintext_key) = state.keys.get(&KeyTuple {
            namespace: tenant.namespace,
            tenant_id: tenant.id,
            key_id: request.key_id,
            wrapped_key: request.wrapped_key,
        }) else {
            return Ok(Response::new(unwrap_failure(
                UnwrapFailureReason::InvalidKey,
            )));
        };
        let ttl_seconds = if request.authorization.is_some() {
            30
        } else {
            120
        };
        Ok(Response::new(UnwrapIntermediateKeyResponse {
            outcome: Some(UnwrapOutcome::Key(IntermediateKeyMaterial {
                plaintext_key: plaintext_key.clone(),
                max_cache_ttl: Some(prost_types::Duration {
                    seconds: ttl_seconds,
                    nanos: 0,
                }),
            })),
        }))
    }
}

fn issue_failure(reason: IssueFailureReason) -> IssueIntermediateKeyResponse {
    IssueIntermediateKeyResponse {
        outcome: Some(IssueOutcome::Failure(IssueFailure {
            reason: reason as i32,
            detail: "fixture-selected outcome".to_owned(),
        })),
    }
}

fn unwrap_failure(reason: UnwrapFailureReason) -> UnwrapIntermediateKeyResponse {
    UnwrapIntermediateKeyResponse {
        outcome: Some(UnwrapOutcome::Failure(UnwrapFailure {
            reason: reason as i32,
            detail: "fixture-selected outcome".to_owned(),
        })),
    }
}

#[tonic::async_trait]
impl ConformanceControl for FixtureBroker {
    async fn set_issue_failure(
        &self,
        reason: Option<IssueFailureReason>,
    ) -> Result<(), ConformanceControlError> {
        self.state
            .lock()
            .expect("fixture state must not be poisoned")
            .issue_failure = reason;
        Ok(())
    }

    async fn set_unwrap_failure(
        &self,
        reason: Option<UnwrapFailureReason>,
    ) -> Result<(), ConformanceControlError> {
        self.state
            .lock()
            .expect("fixture state must not be poisoned")
            .unwrap_failure = reason;
        Ok(())
    }

    async fn migrate_backing_provider(&self) -> Result<(), ConformanceControlError> {
        let mut state = self
            .state
            .lock()
            .expect("fixture state must not be poisoned");
        state.backend_generation = state
            .backend_generation
            .checked_add(1)
            .expect("fixture backend generation exhausted");
        Ok(())
    }
}

struct Fixture {
    client: IntermediateKeyBrokerClient<Channel>,
    control: FixtureBroker,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_fixture(require_evidence: bool) -> Fixture {
    let control = FixtureBroker {
        state: Arc::new(Mutex::new(FixtureState {
            require_evidence,
            next_key: 0,
            backend_generation: 1,
            issue_failure: None,
            unwrap_failure: None,
            keys: HashMap::new(),
        })),
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener must bind");
    let address = listener
        .local_addr()
        .expect("listener must have an address");
    let broker = control.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(IntermediateKeyBrokerServer::new(broker))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let client = IntermediateKeyBrokerClient::connect(format!("http://{address}"))
        .await
        .expect("fixture client must connect");
    Fixture {
        client,
        control,
        server,
    }
}

fn tenant(id: &str) -> TenantRef {
    TenantRef {
        namespace: "urn:example:tenant".to_owned(),
        id: id.to_owned(),
    }
}

fn evidence_for(tenant: &TenantRef) -> AuthorizationEvidence {
    AuthorizationEvidence {
        scheme: SUPPORTED_SCHEME.to_owned(),
        subject: Some(format!("subject-{}", tenant.id)),
        evidence: format!("grant:{}:{}", tenant.namespace, tenant.id).into_bytes(),
    }
}

fn unknown_evidence() -> AuthorizationEvidence {
    AuthorizationEvidence {
        scheme: "urn:example:unknown:v1".to_owned(),
        subject: None,
        evidence: b"unsupported".to_vec(),
    }
}

#[tokio::test]
async fn transport_identity_only_profile_conforms() {
    let fixture = start_fixture(false).await;
    let config = ConformanceConfig::new(
        tenant("primary"),
        tenant("other"),
        AuthorizationProfile::TransportIdentityOnly,
        unknown_evidence(),
        Duration::from_secs(120),
    )
    .expect("fixture config must be valid");
    IntermediateKeyBrokerConformance::new(fixture.client.clone(), config)
        .run(&fixture.control)
        .await
        .expect("transport-identity broker must conform");
}

#[tokio::test]
async fn required_authorization_evidence_profile_conforms() {
    let fixture = start_fixture(true).await;
    let primary = tenant("primary");
    let other = tenant("other");
    let config = ConformanceConfig::new(
        primary.clone(),
        other.clone(),
        AuthorizationProfile::EvidenceRequired {
            primary: evidence_for(&primary),
            other_tenant: evidence_for(&other),
            max_cache_ttl: Duration::from_secs(30),
        },
        unknown_evidence(),
        Duration::from_secs(120),
    )
    .expect("fixture config must be valid");
    IntermediateKeyBrokerConformance::new(fixture.client.clone(), config)
        .run(&fixture.control)
        .await
        .expect("evidence-requiring broker must conform");
}
