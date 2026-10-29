use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId},
};
use sqlx::{PgConnection, types::Json};

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertObservedStateError {
    #[error("write published sandbox observed state: {0}")]
    Upsert(#[source] sqlx::Error),
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct AcceptedSandboxPublishRow {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) assigned_to_node: bool,
    pub(crate) inserted: bool,
    pub(crate) updated: bool,
    pub(crate) first_seen: bool,
    pub(crate) published_status: Option<String>,
    pub(crate) state_changed_at: Option<time::OffsetDateTime>,
    pub(crate) status_changed: bool,
}

const UPSERT_OBSERVED_STATE_SQL: &str = sql!(
        "WITH input AS (
             SELECT sandbox_id, status, state_changed_at, template, runtime,
                    spaced_version, runtime_version, guest_os, guest_arch,
                    state_format_name, state_format_version, execution_metadata,
                    execution_components, pid, namespace, uptime, sandbox_owner,
                    sandbox_user, image_hash, snapshots_enabled, pool_template,
                    failure_reason, observed_created_at, network_tx_bytes,
                    network_rx_bytes, disk_usage_bytes, observed_network_policy_json,
                    observed_network_policy_generation, observed_network_policy_source,
                    observed_network_policy_authority, auto_lifecycle_authority
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT,
                 state_changed_at TIMESTAMPTZ,
                 template TEXT,
                 runtime TEXT,
                 spaced_version TEXT,
                 runtime_version TEXT,
                 guest_os TEXT,
                 guest_arch TEXT,
                 state_format_name TEXT,
                 state_format_version TEXT,
                 execution_metadata JSONB,
                 execution_components JSONB,
                 pid INTEGER,
                 namespace TEXT,
                 uptime TEXT,
                 sandbox_owner TEXT,
                 sandbox_user TEXT,
                 image_hash TEXT,
                 snapshots_enabled BOOLEAN,
                 pool_template TEXT,
                 failure_reason TEXT,
                 observed_created_at TEXT,
                 network_tx_bytes BIGINT,
                 network_rx_bytes BIGINT,
                 disk_usage_bytes BIGINT,
                 observed_network_policy_json JSONB,
                 observed_network_policy_generation BIGINT,
                 observed_network_policy_source TEXT,
                 observed_network_policy_authority TEXT,
                 auto_lifecycle_authority TEXT
             )
         ),
         publishable AS (
             SELECT input.sandbox_id, input.status, input.state_changed_at,
                    input.template, input.runtime, input.spaced_version,
                    input.runtime_version, input.guest_os, input.guest_arch,
                    input.state_format_name, input.state_format_version,
                    input.execution_metadata, input.execution_components,
                    input.pid, input.namespace, input.uptime, input.sandbox_owner,
                    input.sandbox_user, input.image_hash, input.snapshots_enabled,
                    input.pool_template, input.failure_reason, input.observed_created_at,
                    input.network_tx_bytes, input.network_rx_bytes, input.disk_usage_bytes,
                    input.observed_network_policy_json, input.observed_network_policy_generation,
                    input.observed_network_policy_source, input.observed_network_policy_authority,
                    input.auto_lifecycle_authority,
                    s.id IS NOT NULL AS assigned_to_node
             FROM input
             LEFT JOIN sandboxes s ON s.id = input.sandbox_id
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             WHERE s.id IS NULL
                OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
         ),
         observed_current AS MATERIALIZED (
             SELECT sos.sandbox_id, sos.node_id, sos.status, sos.runtime,
                    sos.pid, sos.namespace, sos.uptime, sos.sandbox_user,
                    sos.sandbox_owner, sos.image_hash, sos.snapshots_enabled,
                    sos.disk_usage_bytes, sos.pool_template, sos.failure_reason,
                    sos.observed_created_at, sos.network_tx_bytes, sos.network_rx_bytes,
                    sos.observed_at, sos.updated_at, sos.missing_publish_count,
                    sos.template, sos.network_policy_authority, sos.auto_lifecycle_authority,
                    sos.runtime_version, sos.guest_os, sos.guest_arch,
                    sos.state_format_name, sos.state_format_version,
                    sos.execution_metadata, sos.spaced_version
             FROM sandbox_observed_states sos
             JOIN publishable ON publishable.sandbox_id = sos.sandbox_id
         ),
         existing AS (
             SELECT DISTINCT sandbox_id
             FROM observed_current
             WHERE node_id = $1
         ),
         changed_or_missing AS MATERIALIZED (
             SELECT publishable.sandbox_id, publishable.status, publishable.state_changed_at,
                    publishable.template, publishable.runtime, publishable.spaced_version,
                    publishable.runtime_version, publishable.guest_os, publishable.guest_arch,
                    publishable.state_format_name, publishable.state_format_version,
                    publishable.execution_metadata, publishable.execution_components,
                    publishable.pid, publishable.namespace, publishable.uptime,
                    publishable.sandbox_owner, publishable.sandbox_user, publishable.image_hash,
                    publishable.snapshots_enabled, publishable.pool_template,
                    publishable.failure_reason, publishable.observed_created_at,
                    publishable.network_tx_bytes, publishable.network_rx_bytes,
                    publishable.disk_usage_bytes, publishable.observed_network_policy_json,
                    publishable.observed_network_policy_generation,
                    publishable.observed_network_policy_source,
                    publishable.observed_network_policy_authority,
                    publishable.auto_lifecycle_authority, publishable.assigned_to_node
             FROM publishable
             LEFT JOIN observed_current ON observed_current.sandbox_id = publishable.sandbox_id
             WHERE observed_current.sandbox_id IS NULL
                OR (observed_current.node_id,
                    observed_current.status,
                    observed_current.runtime,
                    observed_current.spaced_version,
                    observed_current.runtime_version,
                    observed_current.guest_os,
                    observed_current.guest_arch,
                    observed_current.state_format_name,
                    observed_current.state_format_version,
                    observed_current.execution_metadata,
                    observed_current.pid,
                    observed_current.namespace,
                    observed_current.uptime,
                    observed_current.sandbox_owner,
                    observed_current.sandbox_user,
                    observed_current.image_hash,
                    observed_current.snapshots_enabled,
                    observed_current.template,
                    observed_current.pool_template,
                    observed_current.failure_reason,
                    observed_current.observed_created_at,
                    observed_current.network_tx_bytes,
                    observed_current.network_rx_bytes,
                    observed_current.disk_usage_bytes,
                    observed_current.auto_lifecycle_authority,
                    observed_current.missing_publish_count)
                   IS DISTINCT FROM
                   ($1,
                    publishable.status,
                    publishable.runtime,
                    publishable.spaced_version,
                    publishable.runtime_version,
                    publishable.guest_os,
                    publishable.guest_arch,
                    publishable.state_format_name,
                    publishable.state_format_version,
                    publishable.execution_metadata,
                    publishable.pid,
                    publishable.namespace,
                    publishable.uptime,
                    publishable.sandbox_owner,
                    publishable.sandbox_user,
                    publishable.image_hash,
                    COALESCE(publishable.snapshots_enabled, FALSE),
                    publishable.template,
                    publishable.pool_template,
                    publishable.failure_reason,
                    publishable.observed_created_at,
                    publishable.network_tx_bytes,
                    publishable.network_rx_bytes,
                    COALESCE(publishable.disk_usage_bytes, observed_current.disk_usage_bytes),
                    publishable.auto_lifecycle_authority,
                    0)
         ),
         upserted AS (
             INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, status, runtime, spaced_version, runtime_version, guest_os, guest_arch,
                 state_format_name, state_format_version, execution_metadata, pid, namespace, uptime,
                 sandbox_owner, sandbox_user, image_hash,
                 snapshots_enabled, template, pool_template, failure_reason,
                 observed_created_at, network_tx_bytes, network_rx_bytes, disk_usage_bytes,
                 auto_lifecycle_authority,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT
                 changed_or_missing.sandbox_id,
                 $1,
                status,
                runtime,
                spaced_version,
                runtime_version,
                 guest_os,
                 guest_arch,
                 state_format_name,
                 state_format_version,
                 execution_metadata,
                 pid,
                 namespace,
                 uptime,
                 sandbox_owner,
                 sandbox_user,
                 image_hash,
                 COALESCE(snapshots_enabled, FALSE),
                 template,
                 pool_template,
                 failure_reason,
                 observed_created_at,
                network_tx_bytes,
                network_rx_bytes,
                disk_usage_bytes,
                auto_lifecycle_authority,
                0,
                NOW(),
                NOW()
             FROM changed_or_missing
             ON CONFLICT (sandbox_id) DO UPDATE
             SET node_id = EXCLUDED.node_id,
                 status = EXCLUDED.status,
                 runtime = EXCLUDED.runtime,
                 spaced_version = EXCLUDED.spaced_version,
                 runtime_version = EXCLUDED.runtime_version,
                 guest_os = EXCLUDED.guest_os,
                 guest_arch = EXCLUDED.guest_arch,
                 state_format_name = EXCLUDED.state_format_name,
                 state_format_version = EXCLUDED.state_format_version,
                 execution_metadata = EXCLUDED.execution_metadata,
                 pid = EXCLUDED.pid,
                 namespace = EXCLUDED.namespace,
                 uptime = EXCLUDED.uptime,
                 sandbox_owner = EXCLUDED.sandbox_owner,
                 sandbox_user = EXCLUDED.sandbox_user,
                 image_hash = EXCLUDED.image_hash,
                 snapshots_enabled = EXCLUDED.snapshots_enabled,
                 template = EXCLUDED.template,
                 pool_template = EXCLUDED.pool_template,
                 failure_reason = EXCLUDED.failure_reason,
                 observed_created_at = EXCLUDED.observed_created_at,
                 network_tx_bytes = EXCLUDED.network_tx_bytes,
                 network_rx_bytes = EXCLUDED.network_rx_bytes,
                 disk_usage_bytes = COALESCE(
                     EXCLUDED.disk_usage_bytes,
                     sandbox_observed_states.disk_usage_bytes
                 ),
                 auto_lifecycle_authority = EXCLUDED.auto_lifecycle_authority,
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
            WHERE (sandbox_observed_states.node_id,
                   sandbox_observed_states.status,
                   sandbox_observed_states.runtime,
                   sandbox_observed_states.spaced_version,
                   sandbox_observed_states.runtime_version,
                   sandbox_observed_states.guest_os,
                   sandbox_observed_states.guest_arch,
                   sandbox_observed_states.state_format_name,
                   sandbox_observed_states.state_format_version,
                   sandbox_observed_states.execution_metadata,
                   sandbox_observed_states.pid,
                   sandbox_observed_states.namespace,
                   sandbox_observed_states.uptime,
                   sandbox_observed_states.sandbox_owner,
                   sandbox_observed_states.sandbox_user,
                   sandbox_observed_states.image_hash,
                   sandbox_observed_states.snapshots_enabled,
                   sandbox_observed_states.template,
                   sandbox_observed_states.pool_template,
                   sandbox_observed_states.failure_reason,
                   sandbox_observed_states.observed_created_at,
                   sandbox_observed_states.network_tx_bytes,
                   sandbox_observed_states.network_rx_bytes,
                   sandbox_observed_states.disk_usage_bytes,
                   sandbox_observed_states.auto_lifecycle_authority,
                   sandbox_observed_states.missing_publish_count)
                  IS DISTINCT FROM
                  (EXCLUDED.node_id,
                   EXCLUDED.status,
                   EXCLUDED.runtime,
                   EXCLUDED.spaced_version,
                   EXCLUDED.runtime_version,
                   EXCLUDED.guest_os,
                   EXCLUDED.guest_arch,
                   EXCLUDED.state_format_name,
                   EXCLUDED.state_format_version,
                   EXCLUDED.execution_metadata,
                   EXCLUDED.pid,
                   EXCLUDED.namespace,
                   EXCLUDED.uptime,
                   EXCLUDED.sandbox_owner,
                   EXCLUDED.sandbox_user,
                   EXCLUDED.image_hash,
                   EXCLUDED.snapshots_enabled,
                   EXCLUDED.template,
                   EXCLUDED.pool_template,
                   EXCLUDED.failure_reason,
                   EXCLUDED.observed_created_at,
                   EXCLUDED.network_tx_bytes,
                   EXCLUDED.network_rx_bytes,
                   COALESCE(EXCLUDED.disk_usage_bytes, sandbox_observed_states.disk_usage_bytes),
                   EXCLUDED.auto_lifecycle_authority,
                   0)
            RETURNING sandbox_id, (xmax = 0) AS inserted
        )
         SELECT publishable.sandbox_id,
                publishable.assigned_to_node,
                COALESCE(upserted.inserted, FALSE) AS inserted,
                COALESCE(NOT upserted.inserted, FALSE) AS updated,
                NOT EXISTS (
                    SELECT 1
                    FROM existing
                    WHERE existing.sandbox_id = publishable.sandbox_id
                ) AS first_seen,
                publishable.status AS published_status,
                publishable.state_changed_at,
                observed_current.sandbox_id IS NULL
                    OR observed_current.status IS DISTINCT FROM publishable.status
                    AS status_changed
         FROM publishable
         LEFT JOIN upserted ON upserted.sandbox_id = publishable.sandbox_id
         LEFT JOIN observed_current ON observed_current.sandbox_id = publishable.sandbox_id"
);

/// Accept observations for unknown sandboxes or the current publisher, using
/// Direct route ownership when present. Conflict handling locks observation
/// rows even for no-op updates, without explicit sandbox locks. Unchanged observations
/// remain accepted, and an empty result means no input passed the owner guard.
pub(crate) async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    rows_json: &Json<serde_json::Value>,
) -> Result<Vec<AcceptedSandboxPublishRow>, UpsertObservedStateError> {
    sqlx::query_as::<_, AcceptedSandboxPublishRow>(UPSERT_OBSERVED_STATE_SQL)
        .bind(node_id)
        .bind(rows_json)
        .fetch_all(&mut *conn)
        .await
        .map_err(UpsertObservedStateError::Upsert)
}
