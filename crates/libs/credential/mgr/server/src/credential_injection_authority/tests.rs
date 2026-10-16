use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pplx_space_common_svc::core::types::{K8sNodeUid, NodeId};
use pplx_space_credential_injection_core::secret_arn::SecretArn;
use pplx_space_credential_injection_store::references::{Config, ReferenceState};
use pplx_space_net_common::{
    CredentialHeaderBindingV1, EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope,
    NetworkCredentialDestinationV1, NetworkCredentialHttpMethodV1, NetworkCredentialRuleV1,
    NetworkPolicyV1, TlsInterception, network_policy_hash,
};
use uuid::Uuid;
use zeroize::Zeroizing;

use super::state::{NetworkPolicyResolutionCandidate, ResolutionStateError, ResolutionStateReader};
use super::*;
use crate::aws_secrets_manager::ResolvedAwsSecret;
use crate::credential_provider::{
    CredentialFetch, CredentialProviderResolver, PreparedCredential, ResolutionContext,
    ResolutionError,
};
use crate::network_credential_materializer::{
    CredentialProviderDispatcher, NetworkCredentialMaterializer,
};

const GENERATION: i64 = 3;
const NODE_UID: &str = "worker-node-uid";
const DESTINATION: &str = "api.openai.com";

fn caller(node_id: NodeId) -> ResolutionCaller {
    ResolutionCaller::new(node_id, K8sNodeUid::parse(NODE_UID).unwrap())
}

fn request() -> ExecutionResolutionRequest {
    ExecutionResolutionRequest::new(
        SandboxId::new(),
        ExecutionGeneration::new(GENERATION).unwrap(),
    )
}

struct ScriptedNetworkPolicyStateReader {
    candidates: Mutex<VecDeque<Option<NetworkPolicyResolutionCandidate>>>,
}

#[async_trait]
impl ResolutionStateReader for ScriptedNetworkPolicyStateReader {
    async fn load_network_policy_candidate(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<NetworkPolicyResolutionCandidate>, ResolutionStateError> {
        Ok(self
            .candidates
            .lock()
            .expect("scripted state reader lock poisoned")
            .pop_front()
            .expect("unexpected state read"))
    }
}

fn network_policy_candidate(
    organization_id: Uuid,
    node_id: NodeId,
    reference_id: CredentialReferenceId,
) -> NetworkPolicyResolutionCandidate {
    let policy = network_policy(reference_id);
    let effective_network_policy_ref = EffectiveNetworkPolicyRef::cluster(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        11,
        network_policy_hash(&policy),
    )
    .unwrap()
    .to_string();
    NetworkPolicyResolutionCandidate {
        state: ResolutionState {
            organization_id,
            node_id,
            k8s_node_uid: K8sNodeUid::parse(NODE_UID).unwrap(),
            execution_generation: ExecutionGeneration::new(GENERATION).unwrap(),
        },
        effective_network_policy_ref,
        policy,
        references: vec![
            pplx_space_credential_injection_store::resolution::NetworkPolicyReference {
                id: reference_id,
                config: Config::AwsSecretsManager {
                    secret_arn:
                        "arn:aws:secretsmanager:us-west-2:123456789012:secret:openai-AbCdEf".into(),
                },
                state: ReferenceState::Active,
            },
        ],
    }
}

fn network_policy(reference_id: CredentialReferenceId) -> NetworkPolicyV1 {
    let mut policy = NetworkPolicyV1::allow_all();
    policy.tls_interception = TlsInterception::On;
    policy.network_credential_rules = vec![NetworkCredentialRuleV1 {
        rule_id: "openai".into(),
        destination: NetworkCredentialDestinationV1::new(
            DESTINATION.parse().unwrap(),
            Some(443),
            Some("/v1".into()),
        )
        .unwrap(),
        credential_header_bindings: vec![CredentialHeaderBindingV1 {
            header_name: "authorization".into(),
            credential_reference_id: reference_id.as_uuid(),
        }],
        supported_http_methods: vec![NetworkCredentialHttpMethodV1::Get],
    }];
    policy
}

fn network_request(
    candidate: &NetworkPolicyResolutionCandidate,
) -> NetworkCredentialResolutionRequest {
    NetworkCredentialResolutionRequest::new(
        SandboxId::new(),
        ExecutionGeneration::new(GENERATION).unwrap(),
        candidate.effective_network_policy_ref.clone(),
    )
}

#[test]
fn network_resolution_request_requires_canonical_policy_identity() {
    let sandbox_id = SandboxId::new();
    let expected_generation = ExecutionGeneration::new(GENERATION).unwrap();
    let canonical =
        network_policy_candidate(Uuid::now_v7(), NodeId::new(), CredentialReferenceId::new())
            .effective_network_policy_ref;

    let parsed = NetworkCredentialResolutionRequest::parse(
        &sandbox_id.to_string(),
        expected_generation.get(),
        &canonical,
    )
    .expect("canonical committed-policy identity");
    assert_eq!(parsed.sandbox_id, sandbox_id);
    assert_eq!(parsed.execution_generation, expected_generation);
    assert_eq!(parsed.effective_network_policy_ref, canonical);

    assert!(matches!(
        NetworkCredentialResolutionRequest::parse(
            &sandbox_id.to_string(),
            expected_generation.get(),
            "none"
        ),
        Err(InvalidResolutionRequest::EffectiveNetworkPolicyRef)
    ));
    assert!(matches!(
        NetworkCredentialResolutionRequest::parse(
            &sandbox_id.to_string(),
            expected_generation.get(),
            "cluster:sandbox:not-canonical"
        ),
        Err(InvalidResolutionRequest::EffectiveNetworkPolicyRef)
    ));
}

struct NetworkPolicyTestProvider;

#[async_trait]
impl CredentialProviderResolver for NetworkPolicyTestProvider {
    type Reference = SecretArn;
    type Material = ResolvedAwsSecret;

    async fn prepare(
        &self,
        _context: ResolutionContext,
        _reference: &SecretArn,
    ) -> Result<PreparedCredential<Self::Material>, ResolutionError> {
        Ok(PreparedCredential::new(NetworkPolicyTestFetch))
    }
}

struct FailingNetworkPolicyTestProvider(ResolutionError);

#[async_trait]
impl CredentialProviderResolver for FailingNetworkPolicyTestProvider {
    type Reference = SecretArn;
    type Material = ResolvedAwsSecret;

    async fn prepare(
        &self,
        _context: ResolutionContext,
        _reference: &SecretArn,
    ) -> Result<PreparedCredential<Self::Material>, ResolutionError> {
        Err(self.0)
    }
}

struct NetworkPolicyTestFetch;

#[async_trait]
impl CredentialFetch<ResolvedAwsSecret> for NetworkPolicyTestFetch {
    async fn fetch(self: Box<Self>) -> Result<ResolvedAwsSecret, ResolutionError> {
        Ok(ResolvedAwsSecret::new(Zeroizing::new(
            "Bearer authority-canary".into(),
        )))
    }
}

fn network_policy_authority(
    candidates: impl IntoIterator<Item = Option<NetworkPolicyResolutionCandidate>>,
) -> CredentialInjectionAuthority {
    let providers = CredentialProviderDispatcher::default()
        .with_aws_secrets_manager(Arc::new(NetworkPolicyTestProvider));
    CredentialInjectionAuthority::from_state_reader(
        Arc::new(ScriptedNetworkPolicyStateReader {
            candidates: Mutex::new(candidates.into_iter().collect()),
        }),
        NetworkCredentialMaterializer::new(providers),
    )
}

#[test]
fn candidate_authorization_fences_node_and_execution() {
    let organization_id = Uuid::now_v7();
    let node_id = NodeId::new();
    let candidate =
        network_policy_candidate(organization_id, node_id, CredentialReferenceId::new());
    let current_request = request();

    authorize_candidate(&candidate.state, &caller(node_id), current_request).unwrap();
    assert!(matches!(
        authorize_candidate(&candidate.state, &caller(NodeId::new()), current_request),
        Err(NetworkCredentialResolutionError::CandidateUnavailableOrUnauthorized)
    ));

    let replaced_node =
        ResolutionCaller::new(node_id, K8sNodeUid::parse("replacement-node-uid").unwrap());
    assert!(matches!(
        authorize_candidate(&candidate.state, &replaced_node, current_request),
        Err(NetworkCredentialResolutionError::CandidateUnavailableOrUnauthorized)
    ));

    let stale_generation = ExecutionResolutionRequest::new(
        current_request.sandbox_id,
        ExecutionGeneration::new(GENERATION + 1).unwrap(),
    );
    assert!(matches!(
        authorize_candidate(&candidate.state, &caller(node_id), stale_generation),
        Err(NetworkCredentialResolutionError::StaleCandidate)
    ));
}

#[tokio::test]
async fn network_policy_resolution_rejects_execution_changed_between_reads() {
    let organization_id = Uuid::now_v7();
    let node_id = NodeId::new();
    let first = network_policy_candidate(organization_id, node_id, CredentialReferenceId::new());
    let replacements = [
        None,
        Some(NetworkPolicyResolutionCandidate {
            state: ResolutionState {
                k8s_node_uid: K8sNodeUid::parse("replacement-node-uid").unwrap(),
                ..first.state.clone()
            },
            ..first.clone()
        }),
        Some(NetworkPolicyResolutionCandidate {
            state: ResolutionState {
                execution_generation: ExecutionGeneration::new(GENERATION + 1).unwrap(),
                ..first.state.clone()
            },
            ..first.clone()
        }),
    ];
    for replacement in replacements {
        let authority = network_policy_authority([Some(first.clone()), replacement]);
        assert!(matches!(
            authority
                .resolve_network_credentials(&caller(node_id), network_request(&first))
                .await,
            Err(NetworkCredentialResolutionError::StaleCandidate)
        ));
    }
}

#[tokio::test]
async fn network_policy_resolution_returns_only_uuid_keyed_material_and_rechecks_state() {
    let organization_id = Uuid::now_v7();
    let node_id = NodeId::new();
    let reference_id = CredentialReferenceId::new();
    let candidate = network_policy_candidate(organization_id, node_id, reference_id);
    let request = network_request(&candidate);
    let expected_policy_ref = candidate.effective_network_policy_ref.clone();
    let authority = network_policy_authority([Some(candidate.clone()), Some(candidate)]);

    let resolved = authority
        .resolve_network_credentials(&caller(node_id), request)
        .await
        .unwrap();

    assert_eq!(resolved.effective_network_policy_ref, expected_policy_ref);
    assert_eq!(resolved.credentials.len(), 1);
    let credential = &resolved.credentials[0];
    assert_eq!(credential.credential_reference_id, reference_id.to_string());
    assert_eq!(credential.value, b"Bearer authority-canary");
    let debug = format!("{credential:?}");
    assert!(!debug.contains("authority-canary"));
}

#[tokio::test]
async fn network_policy_resolution_omits_retired_and_rejects_changed_authority_state() {
    let organization_id = Uuid::now_v7();
    let node_id = NodeId::new();
    let reference_id = CredentialReferenceId::new();
    let first = network_policy_candidate(organization_id, node_id, reference_id);
    let request = network_request(&first);

    let missing = network_policy_authority([None]);
    assert!(matches!(
        missing
            .resolve_network_credentials(&caller(node_id), request)
            .await,
        Err(NetworkCredentialResolutionError::CandidateUnavailableOrUnauthorized)
    ));

    let mut changed = first.clone();
    changed.references[0].state = ReferenceState::Retired;
    let request = network_request(&first);
    let stale = network_policy_authority([Some(first), Some(changed)]);
    assert!(matches!(
        stale
            .resolve_network_credentials(&caller(node_id), request)
            .await,
        Err(NetworkCredentialResolutionError::StaleCandidate)
    ));

    let mut retired = network_policy_candidate(organization_id, node_id, reference_id);
    retired.references[0].state = ReferenceState::Retired;
    let request = network_request(&retired);
    let authority = network_policy_authority([Some(retired.clone()), Some(retired)]);
    let resolved = authority
        .resolve_network_credentials(&caller(node_id), request)
        .await
        .unwrap();
    assert!(resolved.credentials.is_empty());

    let mut missing = network_policy_candidate(organization_id, node_id, reference_id);
    missing.references.clear();
    let request = network_request(&missing);
    let authority = network_policy_authority([Some(missing.clone()), Some(missing)]);
    let resolved = authority
        .resolve_network_credentials(&caller(node_id), request)
        .await
        .unwrap();
    assert!(resolved.credentials.is_empty());
}

#[tokio::test]
async fn network_policy_resolution_omits_permanent_provider_denials_but_aborts_transient_failure() {
    let organization_id = Uuid::now_v7();
    let node_id = NodeId::new();
    let reference_id = CredentialReferenceId::new();
    let candidate = network_policy_candidate(organization_id, node_id, reference_id);

    let permanent_providers =
        CredentialProviderDispatcher::default().with_aws_secrets_manager(Arc::new(
            FailingNetworkPolicyTestProvider(ResolutionError::CredentialUnavailable),
        ));
    let permanent = CredentialInjectionAuthority::from_state_reader(
        Arc::new(ScriptedNetworkPolicyStateReader {
            candidates: Mutex::new(VecDeque::from([
                Some(candidate.clone()),
                Some(candidate.clone()),
            ])),
        }),
        NetworkCredentialMaterializer::new(permanent_providers),
    );
    let resolved = permanent
        .resolve_network_credentials(&caller(node_id), network_request(&candidate))
        .await
        .unwrap();
    assert!(resolved.credentials.is_empty());

    let transient_providers =
        CredentialProviderDispatcher::default().with_aws_secrets_manager(Arc::new(
            FailingNetworkPolicyTestProvider(ResolutionError::ProviderUnavailable),
        ));
    let transient = CredentialInjectionAuthority::from_state_reader(
        Arc::new(ScriptedNetworkPolicyStateReader {
            candidates: Mutex::new(VecDeque::from([Some(candidate.clone())])),
        }),
        NetworkCredentialMaterializer::new(transient_providers),
    );
    assert!(matches!(
        transient
            .resolve_network_credentials(&caller(node_id), network_request(&candidate))
            .await,
        Err(NetworkCredentialResolutionError::Materialize(
            NetworkCredentialMaterializationError::Provider(ResolutionError::ProviderUnavailable)
        ))
    ));
}

#[tokio::test]
async fn network_policy_resolution_rejects_request_policy_mismatch() {
    let organization_id = Uuid::now_v7();
    let node_id = NodeId::new();
    let reference_id = CredentialReferenceId::new();
    let candidate = network_policy_candidate(organization_id, node_id, reference_id);
    let authority = network_policy_authority([Some(candidate.clone())]);
    let wrong_ref = EffectiveNetworkPolicyRef::cluster(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        12,
        network_policy_hash(&candidate.policy),
    )
    .unwrap()
    .to_string();

    assert!(matches!(
        authority
            .resolve_network_credentials(
                &caller(node_id),
                NetworkCredentialResolutionRequest::new(
                    SandboxId::new(),
                    ExecutionGeneration::new(GENERATION).unwrap(),
                    wrong_ref,
                ),
            )
            .await,
        Err(NetworkCredentialResolutionError::StaleCandidate)
    ));
}
