//! The api's observed-state reads: what nodes last published about a
//! sandbox, with the node, template and network-policy facts its responses
//! show beside it. They read `node_direct` for a Direct node's name and
//! endpoint.

use pplx_space_accounts::NodeId;
use pplx_space_net_common::NetworkPolicy;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::types::Json;

use crate::observed_states::ObservedSpacedVersion;

#[cfg(test)]
mod tests;

/// One `sandbox_observed_states` row as the api's responses read it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ObservedStateRow {
    pub sandbox_id: SandboxId,
    pub node_id: Option<NodeId>,
    pub node_name: Option<String>,
    pub node_role: Option<String>,
    pub node_managed_by: Option<String>,
    pub node_api_endpoint: Option<String>,
    pub substrate_node_id: Option<String>,
    pub template: Option<String>,
    pub status: Option<String>,
    #[sqlx(flatten)]
    pub version: ObservedSpacedVersion,
    pub pid: Option<i32>,
    pub namespace: Option<String>,
    pub uptime: Option<String>,
    pub sandbox_owner: Option<String>,
    pub image_hash: Option<String>,
    pub snapshots_enabled: bool,
    pub disk_usage_bytes: Option<i64>,
    pub pool_template: Option<String>,
    pub failure_reason: Option<String>,
    pub observed_created_at: Option<String>,
    pub observed_at: time::OffsetDateTime,
    pub release_tags: Vec<String>,
    pub observed_network_policy: Option<Json<NetworkPolicy>>,
    pub observed_network_policy_generation: Option<i64>,
    pub observed_network_policy_source: Option<String>,
    pub observed_network_policy_authority: Option<String>,
    pub observed_network_policy_at: Option<time::OffsetDateTime>,
    pub network_policy_conditions: Json<Vec<ObservedNetworkPolicyCondition>>,
}

/// One element of [`ObservedStateRow::network_policy_conditions`], as the
/// statement builds it from `node_facet_conditions`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct ObservedNetworkPolicyCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_at: Option<String>,
}

/// The observed states of `ids`, in no particular order. The
/// network-policy columns come from the node facet named
/// `network_policy_facet`.
pub async fn observed_states<'e>(
    executor: impl PgExecutor<'e>,
    ids: &[SandboxId],
    network_policy_facet: &str,
) -> Result<Vec<ObservedStateRow>, sqlx::Error> {
    sqlx::query_as::<_, ObservedStateRow>(
        sql!("SELECT sos.sandbox_id,
                sos.node_id,
                COALESCE(observed_direct.name, observed_k8s.k8s_node_name) AS node_name,
                observed_node.role AS node_role,
                observed_node.managed_by AS node_managed_by,
                observed_direct.node_api_endpoint AS node_api_endpoint,
                observed_k8s.k8s_node_name AS substrate_node_id,
                COALESCE(sos.template, s.template_id, observed_template.template_name) AS template,
                sos.status,
                sos.spaced_version,
                sos.pid,
                sos.namespace,
                sos.uptime,
                sos.sandbox_owner,
                sos.image_hash,
                sos.snapshots_enabled,
                sos.disk_usage_bytes,
                sos.pool_template,
                sos.failure_reason,
                sos.observed_created_at,
                sos.observed_at,
                COALESCE(observed_release_tags.tags, ARRAY[]::TEXT[]) AS release_tags,
                nfos.observed_payload AS observed_network_policy,
                nfos.observed_generation AS observed_network_policy_generation,
                nfos.applied_source AS observed_network_policy_source,
                nfos.applied_authority AS observed_network_policy_authority,
                nfos.observed_at AS observed_network_policy_at,
                nfc.network_policy_conditions
         FROM sandbox_observed_states sos
         LEFT JOIN sandboxes s ON s.id = sos.sandbox_id
         LEFT JOIN nodes observed_node ON observed_node.id = sos.node_id
         LEFT JOIN node_direct observed_direct ON observed_direct.node_id = observed_node.id
         LEFT JOIN node_k8s observed_k8s ON observed_k8s.node_id = observed_node.id
         LEFT JOIN LATERAL (
             SELECT nti.template_name
             FROM node_template_inventory nti
             WHERE nti.node_id = sos.node_id
               AND sos.image_hash IS NOT NULL
               AND nti.content_hash = sos.image_hash
             ORDER BY nti.observed_at DESC, nti.template_name ASC
             LIMIT 1
         ) observed_template ON TRUE
         LEFT JOIN LATERAL (
             SELECT ARRAY_AGG(trt.tag ORDER BY trt.tag) AS tags
             FROM template_releases tr
             JOIN template_release_tags trt ON trt.template_release_id = tr.id
             WHERE tr.template_name = COALESCE(sos.template, s.template_id, observed_template.template_name)
               AND sos.image_hash IS NOT NULL
               AND tr.hash = sos.image_hash
         ) observed_release_tags ON TRUE
         LEFT JOIN node_facet_observed_status nfos
           ON nfos.node_id = sos.node_id
          AND nfos.facet = $2
          AND nfos.element = 'sandbox:' || sos.sandbox_id::text
         LEFT JOIN LATERAL (
             SELECT COALESCE(
                 jsonb_agg(
                     jsonb_build_object(
                         'type', condition_type,
                         'status', condition_status,
                         'reason', reason,
                         'message', message,
                         'last_transition_at', last_transition_at
                     )
                     ORDER BY condition_type ASC
                 ),
                 '[]'::jsonb
             ) AS network_policy_conditions
             FROM node_facet_conditions
             WHERE node_id = sos.node_id
               AND facet = $2
               AND element = 'sandbox:' || sos.sandbox_id::text
         ) nfc ON TRUE
         WHERE sos.sandbox_id = ANY($1)"),
    )
    .bind(ids)
    .bind(network_policy_facet)
    .fetch_all(executor)
    .await
}

/// The observed states whose sandbox has no `sandboxes` row and is not
/// among `known_ids`, newest observation first. The network-policy columns
/// come from the node facet named `network_policy_facet`.
pub async fn observed_only<'e>(
    executor: impl PgExecutor<'e>,
    known_ids: &[SandboxId],
    network_policy_facet: &str,
) -> Result<Vec<ObservedStateRow>, sqlx::Error> {
    sqlx::query_as::<_, ObservedStateRow>(
        sql!("SELECT sos.sandbox_id,
                sos.node_id,
                COALESCE(observed_direct.name, observed_k8s.k8s_node_name) AS node_name,
                observed_node.role AS node_role,
                observed_node.managed_by AS node_managed_by,
                observed_direct.node_api_endpoint AS node_api_endpoint,
                observed_k8s.k8s_node_name AS substrate_node_id,
                COALESCE(sos.template, s.template_id, observed_template.template_name) AS template,
                sos.status,
                sos.spaced_version,
                sos.pid,
                sos.namespace,
                sos.uptime,
                sos.sandbox_owner,
                sos.image_hash,
                sos.snapshots_enabled,
                sos.disk_usage_bytes,
                sos.pool_template,
                sos.failure_reason,
                sos.observed_created_at,
                sos.observed_at,
                COALESCE(observed_release_tags.tags, ARRAY[]::TEXT[]) AS release_tags,
                nfos.observed_payload AS observed_network_policy,
                nfos.observed_generation AS observed_network_policy_generation,
                nfos.applied_source AS observed_network_policy_source,
                nfos.applied_authority AS observed_network_policy_authority,
                nfos.observed_at AS observed_network_policy_at,
                nfc.network_policy_conditions
         FROM sandbox_observed_states sos
         LEFT JOIN sandboxes s ON s.id = sos.sandbox_id
         LEFT JOIN nodes observed_node ON observed_node.id = sos.node_id
         LEFT JOIN node_direct observed_direct ON observed_direct.node_id = observed_node.id
         LEFT JOIN node_k8s observed_k8s ON observed_k8s.node_id = observed_node.id
         LEFT JOIN LATERAL (
             SELECT nti.template_name
             FROM node_template_inventory nti
             WHERE nti.node_id = sos.node_id
               AND sos.image_hash IS NOT NULL
               AND nti.content_hash = sos.image_hash
             ORDER BY nti.observed_at DESC, nti.template_name ASC
             LIMIT 1
         ) observed_template ON TRUE
         LEFT JOIN LATERAL (
             SELECT ARRAY_AGG(trt.tag ORDER BY trt.tag) AS tags
             FROM template_releases tr
             JOIN template_release_tags trt ON trt.template_release_id = tr.id
             WHERE tr.template_name = COALESCE(sos.template, s.template_id, observed_template.template_name)
               AND sos.image_hash IS NOT NULL
               AND tr.hash = sos.image_hash
         ) observed_release_tags ON TRUE
         LEFT JOIN node_facet_observed_status nfos
           ON nfos.node_id = sos.node_id
          AND nfos.facet = $2
          AND nfos.element = 'sandbox:' || sos.sandbox_id::text
         LEFT JOIN LATERAL (
             SELECT COALESCE(
                 jsonb_agg(
                     jsonb_build_object(
                         'type', condition_type,
                         'status', condition_status,
                         'reason', reason,
                         'message', message,
                         'last_transition_at', last_transition_at
                     )
                     ORDER BY condition_type ASC
                 ),
                 '[]'::jsonb
             ) AS network_policy_conditions
             FROM node_facet_conditions
             WHERE node_id = sos.node_id
               AND facet = $2
               AND element = 'sandbox:' || sos.sandbox_id::text
         ) nfc ON TRUE
         WHERE s.id IS NULL
           AND NOT (sos.sandbox_id = ANY($1))
         ORDER BY sos.observed_at DESC"),
    )
    .bind(known_ids)
    .bind(network_policy_facet)
    .fetch_all(executor)
    .await
}
