//! What a node publishes about the templates it caches: the inventory rows
//! and their execution components (`node_template_components`, keyed to an
//! inventory row and deleted with it), in the publishing transaction.
//!
//! Each verb but [`prune_absent_templates`] takes the publication as one
//! JSON array of objects, the recordset shape its statement reads with
//! `jsonb_to_recordset`; the publisher builds and validates the objects.
//! [`prune_absent_templates`] takes the builds a complete publication lists
//! and deletes the rest.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::FACET_NETWORK_POLICY;
use sqlx::FromRow;
use sqlx::types::Json;

/// One published template build the inventory accepted: its template has a
/// `templates` row.
#[derive(Debug, Clone, FromRow)]
pub struct AcceptedTemplateBuild {
    pub template_name: String,
    pub content_hash: String,
    /// The upsert wrote the row: it is new or a column changed.
    pub changed: bool,
    /// The node had no row for this build before.
    pub first_seen: bool,
    /// The template's `desired_state`.
    pub desired_state: String,
}

/// Upsert `node_id`'s published template builds, skipping builds of unknown
/// templates and leaving unchanged rows unwritten, and report each accepted
/// build.
pub async fn upsert_published_templates(
    executor: impl Executor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
) -> Result<Vec<AcceptedTemplateBuild>, DbError> {
    Ok(sqlx::query_as::<_, AcceptedTemplateBuild>(sql!(
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
    .bind(Json(rows))
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Delete the components of the published builds that the publication no
/// longer lists for them.
pub async fn delete_unpublished_template_components(
    executor: impl Executor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "WITH scope AS (
             SELECT DISTINCT input.template_name, COALESCE(input.content_hash, '') AS content_hash
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT
             )
         ),
         input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 execution_components JSONB
             )
         ),
         incoming AS (
             SELECT input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    component.kind, component.role_key, component.version,
                    component.digest, component.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
             CROSS JOIN LATERAL jsonb_to_recordset(
                 COALESCE(input.execution_components, '[]'::jsonb)
             ) AS component(
                 kind TEXT, role_key TEXT, version TEXT, digest TEXT, format TEXT
             )
         )
         DELETE FROM node_template_components component
         USING scope
         WHERE component.node_id = $1
           AND component.template_name = scope.template_name
           AND component.content_hash = scope.content_hash
           AND NOT EXISTS (
               SELECT 1
               FROM incoming
               WHERE incoming.template_name = component.template_name
                 AND incoming.content_hash = component.content_hash
                 AND incoming.kind = component.kind
                 AND incoming.role_key = component.role_key
           )"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Upsert the components the publication lists for builds the node has an
/// inventory row for, leaving unchanged rows unwritten.
pub async fn upsert_published_template_components(
    executor: impl Executor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
) -> Result<(), DbError> {
    sqlx::query(sql!("WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 execution_components JSONB
             )
         ),
         incoming AS (
             SELECT $1 AS node_id, input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    component.kind, component.role_key, component.version,
                    component.digest, component.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
             CROSS JOIN LATERAL jsonb_to_recordset(
                 COALESCE(input.execution_components, '[]'::jsonb)
             ) AS component(
                 kind TEXT, role_key TEXT, version TEXT, digest TEXT, format TEXT
             )
         )
         INSERT INTO node_template_components (
             node_id, template_name, content_hash, kind, role_key, version, digest, format
         )
         SELECT node_id, template_name, content_hash, kind, role_key, version, digest, format
         FROM incoming
         ON CONFLICT (node_id, template_name, content_hash, kind, role_key) DO UPDATE
         SET version = EXCLUDED.version,
             digest = EXCLUDED.digest,
             format = EXCLUDED.format
         WHERE (
             node_template_components.version,
             node_template_components.digest,
             node_template_components.format
         ) IS DISTINCT FROM (
             EXCLUDED.version,
             EXCLUDED.digest,
             EXCLUDED.format
         )"))
    .bind(node_id)
    .bind(Json(rows))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete `node_id`'s inventory rows whose `(template_name, content_hash)`
/// is not among `present_keys`, the builds a complete publication lists
/// (every row of the node when it is empty), and in the same statement the
/// node's `network-policy` rows in `node_facet_observed_status` whose
/// element is `template:<name>` for a template that lost a row. Their
/// `node_template_components` and `node_facet_conditions` rows go with
/// them (`ON DELETE CASCADE`).
///
/// `node_facet_observed_status` is sandbox-teardown's table, which this
/// statement writes under a temporary co-writer grant: the two deletes stay
/// one statement, reading one snapshot.
pub async fn prune_absent_templates(
    executor: impl Executor<'_>,
    node_id: NodeId,
    present_keys: &[(String, String)],
) -> Result<(), DbError> {
    let templates = present_keys
        .iter()
        .map(|(template, _)| template.clone())
        .collect::<Vec<_>>();
    let content_hashes = present_keys
        .iter()
        .map(|(_, content_hash)| content_hash.clone())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "WITH present AS (
             SELECT *
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS present(template_name, content_hash)
         ),
         removed AS (
             DELETE FROM node_template_inventory nti
             WHERE nti.node_id = $1
               AND NOT EXISTS (
                   SELECT 1
                   FROM present
                   WHERE present.template_name = nti.template_name
                     AND present.content_hash = nti.content_hash
               )
             RETURNING nti.template_name
         ),
         removed_elements AS (
             SELECT DISTINCT 'template:' || template_name AS element
             FROM removed
         )
         DELETE FROM node_facet_observed_status nfos
         USING removed_elements
         WHERE nfos.node_id = $1
           AND nfos.facet = $4
           AND nfos.element = removed_elements.element"
    ))
    .bind(node_id)
    .bind(templates)
    .bind(content_hashes)
    .bind(FACET_NETWORK_POLICY)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
