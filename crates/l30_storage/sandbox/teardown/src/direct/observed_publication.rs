//! A node's sandbox state publication, written to `sandbox_observed_states`.
//! Each write is fenced on placement: it accepts a sandbox only when the row
//! is gone or places the sandbox on the publishing node, through the Direct
//! route identity's node when there is one, else the row's active node.
//!
//! These are observed projections, not transitions. Each verb runs on the
//! executor its caller passes; internal-api-server runs them inside its
//! publication transaction and keeps everything around them.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use serde::Serialize;
use sqlx::PgExecutor;
use sqlx::types::{Json, JsonValue};

#[cfg(test)]
mod tests;

/// One sandbox whose publication was degraded to a status alone, as
/// [`write_degraded_observed_states`] writes it. It is bound as one element
/// of a JSON array, serialized `{"sandbox_id", "status"}` in field order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DegradedObservedState {
    pub sandbox_id: SandboxId,
    /// Serialized as JSON `null` when absent, which keeps a stored status.
    pub status: Option<String>,
}

/// Observed projection: records the status of sandboxes whose publication
/// was degraded to a status alone, and returns the ids it wrote.
///
/// A sandbox not placed on `node_id` is skipped. A new row has snapshots
/// off; an existing one keeps its other columns, and keeps its status when
/// the published one is NULL.
pub async fn write_degraded_observed_states<'e>(
    executor: impl PgExecutor<'e>,
    node_id: NodeId,
    states: &[DegradedObservedState],
) -> Result<Vec<SandboxId>, sqlx::Error> {
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     status TEXT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 LEFT JOIN sandbox_direct_route_identities direct_route
                   ON direct_route.sandbox_id = s.id
                 WHERE s.id IS NULL
                    OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
             )
             INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, status, snapshots_enabled,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT sandbox_id, $1, status, FALSE, 0, NOW(), NOW()
             FROM publishable
             ON CONFLICT (sandbox_id) DO UPDATE
             SET node_id = EXCLUDED.node_id,
                 status = COALESCE(EXCLUDED.status, sandbox_observed_states.status),
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             RETURNING sandbox_id"
    ))
    .bind(node_id)
    .bind(Json(states))
    .fetch_all(executor)
    .await
}

/// One sandbox's published observation, as [`write_published_observed_states`]
/// writes it.
#[derive(Debug, Clone, Copy)]
pub struct PublishedObservedState<'a> {
    pub sandbox_id: SandboxId,
    pub status: Option<&'a str>,
    /// An RFC 3339 timestamp, cast by the statement.
    pub state_changed_at: Option<&'a str>,
    pub template: Option<&'a str>,
    pub runtime: Option<&'a str>,
    pub spaced_version: Option<&'a str>,
    pub runtime_version: Option<&'a str>,
    pub guest_os: Option<&'a str>,
    pub guest_arch: Option<&'a str>,
    pub state_format_name: Option<&'a str>,
    pub state_format_version: Option<&'a str>,
    /// Written as SQL NULL when absent or JSON `null`.
    pub execution_metadata: Option<&'a JsonValue>,
    pub pid: Option<i32>,
    pub namespace: Option<&'a str>,
    pub uptime: Option<&'a str>,
    pub sandbox_owner: Option<&'a str>,
    pub sandbox_user: Option<&'a str>,
    pub image_hash: Option<&'a str>,
    pub snapshots_enabled: bool,
    pub pool_template: Option<&'a str>,
    pub failure_reason: Option<&'a str>,
    pub observed_created_at: Option<&'a str>,
    pub network_tx_bytes: Option<i64>,
    pub network_rx_bytes: Option<i64>,
    pub disk_usage_bytes: Option<i64>,
    pub auto_lifecycle_authority: Option<&'a str>,
}

/// What [`write_published_observed_states`] did with one accepted sandbox.
#[derive(Debug, sqlx::FromRow)]
pub struct AcceptedSandboxPublishRow {
    pub sandbox_id: SandboxId,
    pub assigned_to_node: bool,
    pub inserted: bool,
    pub updated: bool,
    pub first_seen: bool,
    pub published_status: Option<String>,
    pub state_changed_at: Option<time::OffsetDateTime>,
    pub status_changed: bool,
}

/// Observed projection: applies `node_id`'s published observations, writing
/// only the rows that differ from what is stored, and returns one row per
/// accepted sandbox (one placed on `node_id`, or with no `sandboxes` row).
///
/// A published NULL disk usage keeps the stored value, and a write resets
/// the missing-publish count.
pub async fn write_published_observed_states<'e>(
    executor: impl PgExecutor<'e>,
    node_id: NodeId,
    rows: &[PublishedObservedState<'_>],
) -> Result<Vec<AcceptedSandboxPublishRow>, sqlx::Error> {
    // Typed arrays avoid decoding the entire publication JSON for this hot write.
    sqlx::query_as::<_, AcceptedSandboxPublishRow>(sql!(
        "WITH input AS (
             SELECT input.sandbox_id,
                    input.status,
                    input.state_changed_at::TIMESTAMPTZ AS state_changed_at,
                    input.template,
                    input.runtime,
                    input.spaced_version,
                    input.runtime_version,
                    input.guest_os,
                    input.guest_arch,
                    input.state_format_name,
                    input.state_format_version,
                    input.execution_metadata,
                    input.pid,
                    input.namespace,
                    input.uptime,
                    input.sandbox_owner,
                    input.sandbox_user,
                    input.image_hash,
                    input.snapshots_enabled,
                    input.pool_template,
                    input.failure_reason,
                    input.observed_created_at,
                    input.network_tx_bytes,
                    input.network_rx_bytes,
                    input.disk_usage_bytes,
                    input.auto_lifecycle_authority
             FROM UNNEST(
                 $2::UUID[],
                 $3::TEXT[],
                 $4::TEXT[],
                 $5::TEXT[],
                 $6::TEXT[],
                 $7::TEXT[],
                 $8::TEXT[],
                 $9::TEXT[],
                 $10::TEXT[],
                 $11::TEXT[],
                 $12::TEXT[],
                 $13::JSONB[],
                 $14::INTEGER[],
                 $15::TEXT[],
                 $16::TEXT[],
                 $17::TEXT[],
                 $18::TEXT[],
                 $19::TEXT[],
                 $20::BOOLEAN[],
                 $21::TEXT[],
                 $22::TEXT[],
                 $23::TEXT[],
                 $24::BIGINT[],
                 $25::BIGINT[],
                 $26::BIGINT[],
                 $27::TEXT[]
             ) AS input(
                 sandbox_id,
                 status,
                 state_changed_at,
                 template,
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
                 snapshots_enabled,
                 pool_template,
                 failure_reason,
                 observed_created_at,
                 network_tx_bytes,
                 network_rx_bytes,
                 disk_usage_bytes,
                 auto_lifecycle_authority
             )
         ),
         publishable AS (
             SELECT input.*,
                    s.id IS NOT NULL AS assigned_to_node
             FROM input
             LEFT JOIN sandboxes s ON s.id = input.sandbox_id
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             WHERE s.id IS NULL
                OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
         ),
         observed_current AS MATERIALIZED (
             SELECT sos.*
             FROM sandbox_observed_states sos
             JOIN publishable ON publishable.sandbox_id = sos.sandbox_id
         ),
         existing AS (
             SELECT DISTINCT sandbox_id
             FROM observed_current
             WHERE node_id = $1
         ),
         changed_or_missing AS MATERIALIZED (
             SELECT publishable.*
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
                 observed_current.missing_publish_count) IS DISTINCT FROM ($1,
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
                 sandbox_observed_states.missing_publish_count) IS DISTINCT FROM (EXCLUDED.node_id,
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
    ))
    .bind(node_id)
    .bind(rows.iter().map(|row| row.sandbox_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.status).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.state_changed_at).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.template).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.runtime).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.spaced_version).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.runtime_version).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.guest_os).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.guest_arch).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.state_format_name).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.state_format_version).collect::<Vec<_>>())
    // jsonb_to_recordset treated both an absent value and JSON null as SQL NULL.
    .bind(
        rows.iter()
            .map(|row| row.execution_metadata.filter(|value| !value.is_null()).map(Json))
            .collect::<Vec<_>>(),
    )
    .bind(rows.iter().map(|row| row.pid).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.namespace).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.uptime).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.sandbox_owner).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.sandbox_user).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.image_hash).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.snapshots_enabled).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.pool_template).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.failure_reason).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.observed_created_at).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.network_tx_bytes).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.network_rx_bytes).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.disk_usage_bytes).collect::<Vec<_>>())
    .bind(rows.iter().map(|row| row.auto_lifecycle_authority).collect::<Vec<_>>())
    .fetch_all(executor)
    .await
}
