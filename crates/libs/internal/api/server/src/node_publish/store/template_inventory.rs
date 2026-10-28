use pplx_space_common_core::{sql, types::NodeId};
use sqlx::{PgConnection, types::Json};

pub(crate) type AcceptedTemplateInventoryRow = (String, String, bool, bool, String);

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertTemplateInventoryError {
    #[error("upsert published node template inventory: {0}")]
    Upsert(#[source] sqlx::Error),
}

/// Accept only inventory for registered templates, writing changed observations.
/// Conflict handling locks node-inventory rows even for no-op updates, without
/// explicit template-row locks. Empty means no input named a known template;
/// accepted unchanged rows remain in the result with their change flag false.
pub(crate) async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<AcceptedTemplateInventoryRow>, UpsertTemplateInventoryError> {
    sqlx::query_as::<_, AcceptedTemplateInventoryRow>(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 runtime TEXT,
                 arch TEXT,
                 runtime_version TEXT,
                 guest_os TEXT,
                 state_format_name TEXT,
                 state_format_version TEXT,
                 execution_metadata JSONB,
                 execution_components JSONB,
                 built_at TEXT,
                 artifact_size_bytes BIGINT,
                 cache_size_bytes BIGINT,
                 has_base_snapshot BOOLEAN,
                 base_snapshot_size_bytes BIGINT,
                 pool_target INTEGER,
                 pool_current INTEGER,
                 pool_pending INTEGER,
                 pool_sandbox_ids JSONB,
                 snapshots JSONB,
                 default_auto_lifecycle_json JSONB,
                 default_auto_lifecycle_authority TEXT,
                 network_policy_authority TEXT,
                 ensure_base_snapshot BOOLEAN,
                 snapshot_vcpu INTEGER,
                 snapshot_memory_mib INTEGER,
                 vcpu INTEGER,
                 memory_mib INTEGER,
                 disk_size_mb INTEGER
             )
         ),
         known_templates AS (
             SELECT name, desired_state
             FROM templates
             WHERE name IN (SELECT template_name FROM input)
         ),
         existing AS (
             SELECT DISTINCT nti.template_name, nti.content_hash
             FROM node_template_inventory nti
             JOIN input
               ON input.template_name = nti.template_name
              AND COALESCE(input.content_hash, '') = nti.content_hash
             WHERE nti.node_id = $1
         ),
         accepted AS (
             SELECT DISTINCT input.template_name,
                             COALESCE(input.content_hash, '') AS content_hash,
                             kt.desired_state
             FROM input
             JOIN known_templates kt ON kt.name = input.template_name
         ),
         upserted AS (
             INSERT INTO node_template_inventory (
                 node_id, template_name, content_hash, runtime, arch, runtime_version, guest_os,
                 state_format_name, state_format_version, execution_metadata, built_at,
                 artifact_size_bytes, cache_size_bytes, has_base_snapshot, base_snapshot_size_bytes,
                 pool_target, pool_current, pool_pending, pool_sandbox_ids, snapshots,
                 default_auto_lifecycle_json, default_auto_lifecycle_authority,
                 network_policy_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
                 vcpu, memory_mib, disk_size_mb,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT
                 $1,
                 template_name,
                 COALESCE(content_hash, ''),
                 runtime,
                 arch,
                 runtime_version,
                 guest_os,
                 state_format_name,
                 state_format_version,
                 execution_metadata,
                 built_at,
                 artifact_size_bytes,
                 cache_size_bytes,
                 COALESCE(has_base_snapshot, FALSE),
                 base_snapshot_size_bytes,
                 pool_target,
                 COALESCE(pool_current, 0),
                 COALESCE(pool_pending, 0),
                 COALESCE(pool_sandbox_ids, '[]'::jsonb),
                 COALESCE(snapshots, '[]'::jsonb),
                 default_auto_lifecycle_json,
                 default_auto_lifecycle_authority,
                 network_policy_authority,
                 ensure_base_snapshot,
                 snapshot_vcpu,
                 snapshot_memory_mib,
                 vcpu,
                 memory_mib,
                 disk_size_mb,
                 0,
                 NOW(),
                 NOW()
             FROM input
             JOIN known_templates kt ON kt.name = input.template_name
             ON CONFLICT (node_id, template_name, content_hash) DO UPDATE
             SET runtime = EXCLUDED.runtime,
                 arch = EXCLUDED.arch,
                 runtime_version = EXCLUDED.runtime_version,
                 guest_os = EXCLUDED.guest_os,
                 state_format_name = EXCLUDED.state_format_name,
                 state_format_version = EXCLUDED.state_format_version,
                 execution_metadata = EXCLUDED.execution_metadata,
                 built_at = EXCLUDED.built_at,
                 artifact_size_bytes = EXCLUDED.artifact_size_bytes,
                 cache_size_bytes = EXCLUDED.cache_size_bytes,
                 has_base_snapshot = EXCLUDED.has_base_snapshot,
                 base_snapshot_size_bytes = EXCLUDED.base_snapshot_size_bytes,
                 pool_target = EXCLUDED.pool_target,
                 pool_current = EXCLUDED.pool_current,
                 pool_pending = EXCLUDED.pool_pending,
                 pool_sandbox_ids = EXCLUDED.pool_sandbox_ids,
                 snapshots = EXCLUDED.snapshots,
                 default_auto_lifecycle_json = EXCLUDED.default_auto_lifecycle_json,
                 default_auto_lifecycle_authority = EXCLUDED.default_auto_lifecycle_authority,
                 network_policy_authority = EXCLUDED.network_policy_authority,
                 ensure_base_snapshot = EXCLUDED.ensure_base_snapshot,
                 snapshot_vcpu = EXCLUDED.snapshot_vcpu,
                 snapshot_memory_mib = EXCLUDED.snapshot_memory_mib,
                 vcpu = EXCLUDED.vcpu,
                 memory_mib = EXCLUDED.memory_mib,
                 disk_size_mb = EXCLUDED.disk_size_mb,
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             WHERE (
                 node_template_inventory.runtime,
                 node_template_inventory.arch,
                 node_template_inventory.runtime_version,
                 node_template_inventory.guest_os,
                 node_template_inventory.state_format_name,
                 node_template_inventory.state_format_version,
                 node_template_inventory.execution_metadata,
                 node_template_inventory.built_at,
                 node_template_inventory.artifact_size_bytes,
                 node_template_inventory.cache_size_bytes,
                 node_template_inventory.has_base_snapshot,
                 node_template_inventory.base_snapshot_size_bytes,
                 node_template_inventory.pool_target,
                 node_template_inventory.pool_current,
                 node_template_inventory.pool_pending,
                 node_template_inventory.pool_sandbox_ids,
                 node_template_inventory.snapshots,
                 node_template_inventory.default_auto_lifecycle_json,
                 node_template_inventory.default_auto_lifecycle_authority,
                 node_template_inventory.network_policy_authority,
                 node_template_inventory.ensure_base_snapshot,
                 node_template_inventory.snapshot_vcpu,
                 node_template_inventory.snapshot_memory_mib,
                 node_template_inventory.vcpu,
                 node_template_inventory.memory_mib,
                 node_template_inventory.disk_size_mb,
                 node_template_inventory.missing_publish_count
             ) IS DISTINCT FROM (
                 EXCLUDED.runtime,
                 EXCLUDED.arch,
                 EXCLUDED.runtime_version,
                 EXCLUDED.guest_os,
                 EXCLUDED.state_format_name,
                 EXCLUDED.state_format_version,
                 EXCLUDED.execution_metadata,
                 EXCLUDED.built_at,
                 EXCLUDED.artifact_size_bytes,
                 EXCLUDED.cache_size_bytes,
                 EXCLUDED.has_base_snapshot,
                 EXCLUDED.base_snapshot_size_bytes,
                 EXCLUDED.pool_target,
                 EXCLUDED.pool_current,
                 EXCLUDED.pool_pending,
                 EXCLUDED.pool_sandbox_ids,
                 EXCLUDED.snapshots,
                 EXCLUDED.default_auto_lifecycle_json,
                 EXCLUDED.default_auto_lifecycle_authority,
                 EXCLUDED.network_policy_authority,
                 EXCLUDED.ensure_base_snapshot,
                 EXCLUDED.snapshot_vcpu,
                 EXCLUDED.snapshot_memory_mib,
                 EXCLUDED.vcpu,
                 EXCLUDED.memory_mib,
                 EXCLUDED.disk_size_mb,
                 0
             )
             RETURNING template_name, content_hash
         )
         SELECT accepted.template_name,
                accepted.content_hash,
                EXISTS (
                    SELECT 1
                    FROM upserted
                    WHERE upserted.template_name = accepted.template_name
                      AND upserted.content_hash = accepted.content_hash
                ) AS changed,
                NOT EXISTS (
                    SELECT 1
                    FROM existing
                    WHERE existing.template_name = accepted.template_name
                      AND existing.content_hash = accepted.content_hash
                ) AS first_seen,
                accepted.desired_state
         FROM accepted"
    ))
    .bind(node_id)
    .bind(rows_json)
    .fetch_all(&mut *conn)
    .await
    .map_err(UpsertTemplateInventoryError::Upsert)
}
