//! Authorizes and resolves reference-based credentials.
//!
//! Network-credential resolution follows this authority boundary:
//!
//! 1. Read the sandbox's current node, Kubernetes execution, and committed
//!    control-plane intent in one Postgres statement.
//! 2. Fence that snapshot to the authenticated node incarnation and requested
//!    execution generation.
//! 3. Resolve only references derived from that intent through their provider.
//! 4. Read the complete snapshot again and discard material if any authorizing
//!    state changed during provider access.
//!
//! This module does not cache or persist resolved values, mint node-local
//! certificates, install material into netgtw, or authenticate transports.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pplx_space_common_svc::core::types::{
    CredentialReferenceId, ExecutionGeneration, K8sNodeUid, NodeId, SandboxId,
};
use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
use pplx_space_credential_injection_store::references::ReferenceState;
use pplx_space_net_common::EffectiveNetworkPolicyRef;

use crate::credential_provider::DestinationMatcher;
use crate::network_credential_materializer::{
    NetworkCredentialMaterialization, NetworkCredentialMaterializationError,
    NetworkCredentialMaterializer,
};

use self::state::{
    PgResolutionStateReader, ResolutionState, ResolutionStateError, ResolutionStateReader,
};

mod state;

/// The sandbox execution whose credentials are requested by a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExecutionResolutionRequest {
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
}

impl ExecutionResolutionRequest {
    pub(crate) fn parse(
        sandbox_id: &str,
        execution_generation: i64,
    ) -> Result<Self, InvalidResolutionRequest> {
        let sandbox_id = sandbox_id
            .parse::<SandboxId>()
            .map_err(|_| InvalidResolutionRequest::SandboxId)?;
        let execution_generation = ExecutionGeneration::new(execution_generation)
            .map_err(|_| InvalidResolutionRequest::ExecutionGeneration)?;
        Ok(Self {
            sandbox_id,
            execution_generation,
        })
    }

    #[cfg(test)]
    fn new(sandbox_id: SandboxId, execution_generation: ExecutionGeneration) -> Self {
        Self {
            sandbox_id,
            execution_generation,
        }
    }
}

pub(crate) struct NetworkCredentialResolutionRequest {
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
    effective_network_policy_ref: String,
}

impl NetworkCredentialResolutionRequest {
    pub(crate) fn parse(
        sandbox_id: &str,
        execution_generation: i64,
        effective_network_policy_ref: &str,
    ) -> Result<Self, InvalidResolutionRequest> {
        let base = ExecutionResolutionRequest::parse(sandbox_id, execution_generation)?;
        let parsed = effective_network_policy_ref
            .parse::<EffectiveNetworkPolicyRef>()
            .map_err(|_| InvalidResolutionRequest::EffectiveNetworkPolicyRef)?;
        if parsed.is_no_identity() || parsed.to_string() != effective_network_policy_ref {
            return Err(InvalidResolutionRequest::EffectiveNetworkPolicyRef);
        }
        Ok(Self {
            sandbox_id: base.sandbox_id,
            execution_generation: base.execution_generation,
            effective_network_policy_ref: effective_network_policy_ref.to_owned(),
        })
    }

    #[cfg(test)]
    fn new(
        sandbox_id: SandboxId,
        execution_generation: ExecutionGeneration,
        effective_network_policy_ref: String,
    ) -> Self {
        Self {
            sandbox_id,
            execution_generation,
            effective_network_policy_ref,
        }
    }

    fn execution(&self) -> ExecutionResolutionRequest {
        ExecutionResolutionRequest {
            sandbox_id: self.sandbox_id,
            execution_generation: self.execution_generation,
        }
    }
}

pub(crate) struct ResolvedNetworkCredentials {
    pub(crate) effective_network_policy_ref: String,
    pub(crate) credentials: Vec<ResolvedNetworkCredential>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InvalidResolutionRequest {
    #[error("sandbox_id is malformed")]
    SandboxId,
    #[error("execution_generation is invalid")]
    ExecutionGeneration,
    #[error("effective_network_policy_ref is invalid")]
    EffectiveNetworkPolicyRef,
}

/// Authenticated Kubernetes node incarnation making a resolution request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolutionCaller {
    node_id: NodeId,
    k8s_node_uid: K8sNodeUid,
}

impl ResolutionCaller {
    pub(crate) fn new(node_id: NodeId, k8s_node_uid: K8sNodeUid) -> Self {
        Self {
            node_id,
            k8s_node_uid,
        }
    }
}

/// Resolves only state belonging to the authenticated current node and exact
/// sandbox execution.
#[derive(Clone)]
pub struct CredentialInjectionAuthority {
    state_reader: Arc<dyn ResolutionStateReader>,
    materializer: NetworkCredentialMaterializer,
}

impl CredentialInjectionAuthority {
    /// Construct the production Postgres-backed authority.
    pub fn new(pool: sqlx::PgPool, materializer: NetworkCredentialMaterializer) -> Self {
        Self::from_state_reader(Arc::new(PgResolutionStateReader::new(pool)), materializer)
    }

    fn from_state_reader(
        state_reader: Arc<dyn ResolutionStateReader>,
        materializer: NetworkCredentialMaterializer,
    ) -> Self {
        Self {
            state_reader,
            materializer,
        }
    }

    /// Resolve the references bound by the exact committed network policy and
    /// reject the result if its authorization snapshot changed during
    /// provider access.
    pub(crate) async fn resolve_network_credentials(
        &self,
        caller: &ResolutionCaller,
        request: NetworkCredentialResolutionRequest,
    ) -> Result<ResolvedNetworkCredentials, NetworkCredentialResolutionError> {
        let candidate = self
            .state_reader
            .load_network_policy_candidate(request.sandbox_id)
            .await?
            .ok_or(NetworkCredentialResolutionError::CandidateUnavailableOrUnauthorized)?;
        authorize_candidate(&candidate.state, caller, request.execution())?;
        if candidate.effective_network_policy_ref != request.effective_network_policy_ref {
            return Err(NetworkCredentialResolutionError::StaleCandidate);
        }

        let mut destinations = BTreeMap::<CredentialReferenceId, BTreeSet<(String, u16)>>::new();
        for rule in &candidate.policy.network_credential_rules {
            for binding in &rule.credential_header_bindings {
                destinations
                    .entry(CredentialReferenceId::from_uuid(
                        binding.credential_reference_id,
                    ))
                    .or_default()
                    .insert((
                        rule.destination.exact_host.to_string(),
                        rule.destination.port,
                    ));
            }
        }
        let materialization = candidate
            .references
            .iter()
            .filter(|reference| reference.state == ReferenceState::Active)
            .map(|reference| {
                let destinations = destinations.get(&reference.id).ok_or(
                    NetworkCredentialResolutionError::InvalidState(
                        "network policy reference snapshot is inconsistent",
                    ),
                )?;
                let reference_config =
                    reference.config.to_validated_reference().map_err(|error| {
                        NetworkCredentialMaterializationError::InvalidRules(error.0)
                    })?;
                Ok(NetworkCredentialMaterialization {
                    reference_id: reference.id,
                    reference: reference_config,
                    destinations: destinations
                        .iter()
                        .map(|(exact_host, port)| DestinationMatcher {
                            exact_host: exact_host.clone(),
                            port: *port,
                        })
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, NetworkCredentialResolutionError>>()?;
        let credentials = self
            .materializer
            .materialize_network_credentials(candidate.state.organization_id, materialization)
            .await?;

        let current = self
            .state_reader
            .load_network_policy_candidate(request.sandbox_id)
            .await?
            .ok_or(NetworkCredentialResolutionError::StaleCandidate)?;
        if current != candidate {
            return Err(NetworkCredentialResolutionError::StaleCandidate);
        }
        Ok(ResolvedNetworkCredentials {
            effective_network_policy_ref: candidate.effective_network_policy_ref,
            credentials,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NetworkCredentialResolutionError {
    /// Missing and foreign candidates intentionally share one error so a
    /// compromised node agent cannot probe sandbox or policy existence.
    #[error("credential-injection resolution is unavailable or unauthorized")]
    CandidateUnavailableOrUnauthorized,
    #[error("sandbox credential-resolution state is no longer current")]
    StaleCandidate,
    #[error("credential-injection resolution state is unavailable")]
    StateUnavailable(#[source] sqlx::Error),
    #[error("credential-injection resolution state is invalid: {0}")]
    InvalidState(&'static str),
    #[error(transparent)]
    Materialize(#[from] NetworkCredentialMaterializationError),
}

impl From<ResolutionStateError> for NetworkCredentialResolutionError {
    fn from(error: ResolutionStateError) -> Self {
        match error {
            ResolutionStateError::Query(error) => Self::StateUnavailable(error),
            ResolutionStateError::InvalidData(message) => Self::InvalidState(message),
        }
    }
}

fn authorize_candidate(
    candidate: &ResolutionState,
    caller: &ResolutionCaller,
    request: ExecutionResolutionRequest,
) -> Result<(), NetworkCredentialResolutionError> {
    if candidate.node_id != caller.node_id || candidate.k8s_node_uid != caller.k8s_node_uid {
        return Err(NetworkCredentialResolutionError::CandidateUnavailableOrUnauthorized);
    }
    if candidate.execution_generation != request.execution_generation {
        return Err(NetworkCredentialResolutionError::StaleCandidate);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
