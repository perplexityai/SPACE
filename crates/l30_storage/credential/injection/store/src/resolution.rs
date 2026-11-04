//! Postgres snapshots used to authorize network-credential resolution.

use pplx_space_common_core::{
    sql,
    types::{CredentialReferenceId, ExecutionGeneration, K8sNodeUid, NodeId, SandboxId},
};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, NetworkPolicyV1, network_policy_hash,
};
use sqlx::types::Json;
use uuid::Uuid;

use crate::references::{Config, ReferenceState};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolutionState {
    pub organization_id: Uuid,
    pub node_id: NodeId,
    pub k8s_node_uid: K8sNodeUid,
    pub execution_generation: ExecutionGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkPolicyReference {
    pub id: CredentialReferenceId,
    pub config: Config,
    pub state: ReferenceState,
}

#[derive(Clone, PartialEq, Eq)]
pub struct NetworkPolicyResolutionCandidate {
    pub state: ResolutionState,
    pub effective_network_policy_ref: String,
    pub policy: NetworkPolicyV1,
    pub references: Vec<NetworkPolicyReference>,
}

impl std::fmt::Debug for NetworkPolicyResolutionCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkPolicyResolutionCandidate")
            .field("state", &self.state)
            .field(
                "effective_network_policy_ref",
                &self.effective_network_policy_ref,
            )
            .field(
                "network_credential_rule_count",
                &self.policy.network_credential_rules.len(),
            )
            .field("reference_count", &self.references.len())
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResolutionStateError {
    #[error("credential-injection state query failed: {0}")]
    Query(#[source] sqlx::Error),
    #[error("credential-injection state contains invalid data: {0}")]
    InvalidData(&'static str),
}

impl From<sqlx::Error> for ResolutionStateError {
    fn from(error: sqlx::Error) -> Self {
        match error {
            sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => {
                Self::InvalidData("stored credential intent is malformed")
            }
            error => Self::Query(error),
        }
    }
}

#[derive(Clone, sqlx::FromRow)]
struct ResolutionStateRow {
    organization_id: Uuid,
    node_id: NodeId,
    k8s_node_uid: String,
    execution_generation: i64,
}

impl TryFrom<ResolutionStateRow> for ResolutionState {
    type Error = ResolutionStateError;

    fn try_from(row: ResolutionStateRow) -> Result<Self, Self::Error> {
        Ok(Self {
            organization_id: row.organization_id,
            node_id: row.node_id,
            k8s_node_uid: K8sNodeUid::parse(row.k8s_node_uid).map_err(|_| {
                ResolutionStateError::InvalidData("stored Kubernetes node UID is malformed")
            })?,
            execution_generation: ExecutionGeneration::new(row.execution_generation).map_err(
                |_| {
                    ResolutionStateError::InvalidData(
                        "stored sandbox execution generation is invalid",
                    )
                },
            )?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct NetworkPolicyCandidateRow {
    #[sqlx(flatten)]
    state: ResolutionStateRow,
    policy_json: Json<NetworkPolicyV1>,
    policy_scope: String,
    policy_owner: String,
    policy_generation: i64,
    requested_reference_id: Option<String>,
    reference_id: Option<CredentialReferenceId>,
    reference_organization_id: Option<Uuid>,
    reference_state: Option<ReferenceState>,
    reference_config: Option<Json<Config>>,
}

pub async fn load_network_policy_candidate(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<NetworkPolicyResolutionCandidate>, ResolutionStateError> {
    // Policy and referenced identities are one SQL statement so the candidate
    // is a coherent authorization snapshot. The second identical read after
    // provider access is the TOCTOU fence.
    let rows = sqlx::query_as::<_, NetworkPolicyCandidateRow>(sql!(
        "SELECT s.org_id AS organization_id, s.active_node_id AS node_id,
                k.k8s_node_uid, e.generation AS execution_generation,
                p.policy_json, p.policy_scope, p.policy_owner, p.policy_generation,
                requested.credential_reference_id AS requested_reference_id,
                c.id AS reference_id, c.org_id AS reference_organization_id,
                c.state AS reference_state,
                CASE WHEN c.id IS NOT NULL THEN jsonb_build_object(
                    'upstream_provider', c.upstream_provider,
                    'upstream_provider_config', c.upstream_provider_config)
                END AS reference_config
         FROM sandboxes s
         JOIN node_k8s k ON k.node_id = s.active_node_id AND k.k8s_node_uid IS NOT NULL
         JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
             AND e.k8s_node_name = k.k8s_node_name
         LEFT JOIN templates t ON t.name = s.template_id
         CROSS JOIN LATERAL (
             SELECT CASE WHEN s.network_policy_override_json IS NOT NULL
                         THEN s.network_policy_override_json
                         ELSE t.network_policy_json
                    END AS policy_json,
                    CASE WHEN s.network_policy_override_json IS NOT NULL
                         THEN 'sandbox' ELSE 'template'
                    END AS policy_scope,
                    CASE WHEN s.network_policy_override_json IS NOT NULL
                         THEN s.id::TEXT ELSE t.name
                    END AS policy_owner,
                    CASE WHEN s.network_policy_override_json IS NOT NULL
                         THEN s.network_policy_generation
                         ELSE t.network_policy_generation
                    END AS policy_generation
         ) p
         LEFT JOIN LATERAL (
             SELECT DISTINCT binding.value ->> 'credential_reference_id'
                    AS credential_reference_id
             FROM jsonb_array_elements(
                 COALESCE(p.policy_json -> 'network_credential_rules', '[]'::JSONB)
             ) rule
             CROSS JOIN LATERAL jsonb_array_elements(
                 COALESCE(
                     rule.value -> 'credential_header_bindings',
                     '[]'::JSONB
                 )
             ) binding
         ) requested ON TRUE
         LEFT JOIN credential_references c
             ON c.id::TEXT = requested.credential_reference_id
         WHERE s.id = $1 AND p.policy_json IS NOT NULL
         ORDER BY requested.credential_reference_id"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_all(executor)
    .await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let state = ResolutionState::try_from(first.state.clone())?;
    let policy = first.policy_json.0.clone();
    let policy_scope_name = first.policy_scope.clone();
    let policy_owner = first.policy_owner.clone();
    let raw_policy_generation = first.policy_generation;
    let policy_scope = match policy_scope_name.as_str() {
        "sandbox" => EffectiveNetworkPolicyRefScope::Sandbox,
        "template" => EffectiveNetworkPolicyRefScope::Template,
        _ => {
            return Err(ResolutionStateError::InvalidData(
                "network policy scope is invalid",
            ));
        }
    };
    let policy_generation = u64::try_from(raw_policy_generation)
        .map_err(|_| ResolutionStateError::InvalidData("network policy generation is invalid"))?;
    let effective_network_policy_ref = EffectiveNetworkPolicyRef::cluster(
        policy_scope,
        policy_owner.clone(),
        policy_generation,
        network_policy_hash(&policy),
    )
    .map_err(|_| ResolutionStateError::InvalidData("network policy identity is invalid"))?
    .to_string();

    let mut references = Vec::with_capacity(rows.len());
    for row in rows {
        if row.policy_json.0 != policy
            || row.policy_scope != policy_scope_name
            || row.policy_owner != policy_owner
            || row.policy_generation != raw_policy_generation
        {
            return Err(ResolutionStateError::InvalidData(
                "network policy snapshot is inconsistent",
            ));
        }
        let Some(raw_id) = row.requested_reference_id else {
            continue;
        };
        let id = raw_id.parse::<CredentialReferenceId>().map_err(|_| {
            ResolutionStateError::InvalidData("network policy credential reference is malformed")
        })?;
        if row.reference_id != Some(id)
            || row.reference_organization_id != Some(state.organization_id)
        {
            // Missing and foreign references are deliberately indistinguishable
            // and absent from the authorized material set. Keeping the policy
            // snapshot valid lets the gateway fail the named marker with 403
            // instead of turning a permanent denial into transient 503.
            continue;
        }
        references.push(NetworkPolicyReference {
            id,
            config: row
                .reference_config
                .ok_or(ResolutionStateError::InvalidData(
                    "network-policy credential reference configuration is missing",
                ))?
                .0,
            state: row
                .reference_state
                .ok_or(ResolutionStateError::InvalidData(
                    "network-policy credential reference state is missing",
                ))?,
        });
    }
    Ok(Some(NetworkPolicyResolutionCandidate {
        state,
        effective_network_policy_ref,
        policy,
        references,
    }))
}

#[cfg(test)]
mod tests;
