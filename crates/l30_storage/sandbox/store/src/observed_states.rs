//! `sandbox_observed_states`: the last state a node published for each
//! sandbox it reports, overlaid on the row's lifecycle by status reads.
//!
//! Node publications write these rows ([`crate::direct::observed_publication`]
//! has the publication's inserts), and not every writer is in this crate yet.
//! [`crate::delete::delete_with_dependents`] removes them with the row.
//! [`delete`] is the unguarded delete, run on the executor its caller passes.
//! [`clear_failed_observation`] is the narrower step the failed-row repair
//! and the pod-less drain give-up run after their own guarded transition.
//! [`record_network_policy_authority`] copies a publishing node's applied
//! network-policy authority onto the rows it reports.
//! [`newest`] and [`window`] are unlocked reads of the rows' ids, as is
//! [`rowless_sandbox_ids`]; [`last_published_execution`] is an unlocked read
//! of one row's published execution.
//! [`delete_absent_on_node`] is the publish path's unguarded delete of the
//! rows a node no longer lists, and [`get_with_node`] an unlocked read of one
//! sandbox's row with its publishing node's registration.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeAddress, NodeManagedBy, NodeRole};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::types::{Json, JsonValue};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::SandboxStoreError;

/// One sandbox's observed state with the registration of the node that
/// published it, as [`get_with_node`] reads it. `node_id` is the
/// publishing node, and the row is deleted with that node, so `node_role`
/// and `node_managed_by` are always set. `node_address` is `None` when the
/// node has no address, and `k8s_node_name` when it has no `node_k8s` row.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct ObservedStateWithNode {
    pub sandbox_id: SandboxId,
    pub node_id: Option<NodeId>,
    pub node_address: Option<NodeAddress>,
    pub node_role: Option<NodeRole>,
    pub node_managed_by: Option<NodeManagedBy>,
    pub k8s_node_name: Option<String>,
    pub status: Option<String>,
    pub pid: Option<i32>,
    pub namespace: Option<String>,
    pub uptime: Option<String>,
    /// `sandbox_observed_states.sandbox_owner`.
    pub sandbox_user: Option<String>,
    pub image_hash: Option<String>,
    pub snapshots_enabled: bool,
    pub disk_usage_bytes: Option<i64>,
    pub pool_template: Option<String>,
    pub failure_reason: Option<String>,
    pub observed_created_at: Option<String>,
    pub network_tx_bytes: Option<i64>,
    pub network_rx_bytes: Option<i64>,
    pub missing_publish_count: i32,
    pub template: Option<String>,
    pub network_policy_authority: Option<String>,
    pub auto_lifecycle_authority: Option<String>,
    pub observed_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// The sandbox's observed state with its publishing node's registration.
/// `None` when no node has published one.
pub async fn get_with_node(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<ObservedStateWithNode>, sqlx::Error> {
    sqlx::query_as::<_, ObservedStateWithNode>(sql!(
        "SELECT sos.sandbox_id,
                sos.node_id,
                host(n.address) AS node_address,
                n.role AS node_role,
                n.managed_by AS node_managed_by,
                nk.k8s_node_name,
                sos.status,
                sos.pid,
                sos.namespace,
                sos.uptime,
                sos.sandbox_owner AS sandbox_user,
                sos.image_hash,
                sos.snapshots_enabled,
                sos.disk_usage_bytes,
                sos.pool_template,
                sos.failure_reason,
                sos.observed_created_at,
                sos.network_tx_bytes,
                sos.network_rx_bytes,
                sos.missing_publish_count,
                sos.template,
                sos.network_policy_authority,
                sos.auto_lifecycle_authority,
                sos.observed_at,
                sos.updated_at
           FROM sandbox_observed_states sos
      LEFT JOIN nodes n ON n.id = sos.node_id
      LEFT JOIN node_k8s nk ON nk.node_id = n.id
          WHERE sos.sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}

/// Drop the observed states `node_id` published for those of `sandbox_ids`
/// (an observed projection): the sandboxes a node's publication lists as no
/// longer present. A row another node published is kept. Returns the rows
/// deleted.
pub async fn delete_absent_on_node(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    sandbox_ids: &[SandboxId],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states sos
         WHERE sos.node_id = $1
           AND sos.sandbox_id = ANY($2)"
    ))
    .bind(node_id)
    .bind(sandbox_ids)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Drop the sandbox's observed state, whatever it says (an operation step):
/// for a path that has just settled the row's lifecycle itself and must not
/// leave a node's stale report overlaying it.
/// The sandbox's `sandbox_snapshot_inventory` rows go with it, through that
/// table's foreign-key cascade.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Drop the sandbox's observed state only while it is a `failed`, pid-less
/// record (an operation step), so a record that turned live survives. Run
/// in the transaction whose own guarded statement already moved the row
/// out of the lifecycle that record describes.
/// When the record is purged, the sandbox's `sandbox_snapshot_inventory`
/// rows go with it, through that table's foreign-key cascade.
pub async fn clear_failed_observation(
    conn: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states
          WHERE sandbox_id = $1 AND status = 'failed' AND pid IS NULL"
    ))
    .bind(sandbox_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Record the network-policy authority `node_id` published (an observed
/// projection) on its observed row, for each record of `rows` (a JSON array
/// of objects with `sandbox_id`, `observed_network_policy_generation` and
/// `observed_network_policy_authority`) that carries a generation and names
/// a sandbox with no `sandboxes` row or one active on `node_id`. A row
/// already holding that authority is left as it is.
pub async fn record_network_policy_authority(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    rows: &JsonValue,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        sql!("WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     observed_network_policy_generation BIGINT,
                     observed_network_policy_authority TEXT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE (s.id IS NULL OR s.active_node_id = $1)
                   AND input.observed_network_policy_generation IS NOT NULL
             )
             UPDATE sandbox_observed_states sos
             SET network_policy_authority = publishable.observed_network_policy_authority
             FROM publishable
             WHERE sos.sandbox_id = publishable.sandbox_id
               AND sos.node_id = $1
               AND sos.network_policy_authority IS DISTINCT FROM publishable.observed_network_policy_authority"),
    )
    .bind(node_id)
    .bind(Json(rows))
    .execute(executor)
    .await?;
    Ok(())
}

/// The newest observed-row id; `None` when the table is empty.
pub async fn newest(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<Uuid>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id
             FROM sandbox_observed_states
             ORDER BY sandbox_id DESC
             LIMIT 1"
    ))
    .fetch_optional(executor)
    .await?)
}

/// Up to `limit` observed-row ids in `(after, through]`, ascending, from a
/// primary-key range scan. The ordering is a resume position, not a fairness
/// policy.
pub async fn window(
    executor: impl sqlx::PgExecutor<'_>,
    after: Uuid,
    through: Uuid,
    limit: i64,
) -> Result<Vec<Uuid>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id
             FROM sandbox_observed_states
             WHERE sandbox_id > $1
               AND sandbox_id <= $2
             ORDER BY sandbox_id
             LIMIT $3"
    ))
    .bind(after)
    .bind(through)
    .bind(limit)
    .fetch_all(executor)
    .await?)
}

/// The sandboxes nodes report that have no `sandboxes` row.
pub async fn rowless_sandbox_ids(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "SELECT sos.sandbox_id
                 FROM sandbox_observed_states sos
                 WHERE NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = sos.sandbox_id)"
    ))
    .fetch_all(executor)
    .await
}

/// The execution a sandbox's node last published, as evidence of what it
/// runs. `subject` digests the record and the sandbox's last lifecycle
/// change, so one stopped period names one subject; `local_artifact_id` is
/// always `None` here.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct PublishedExecution {
    pub subject: String,
    pub local_artifact_id: Option<LocalArtifactId>,
    pub execution_metadata: Option<Json<JsonValue>>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

/// The execution `sandbox_id`'s node last published; `None` when nothing is
/// published or the publish carried no execution.
pub async fn last_published_execution(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<PublishedExecution>, sqlx::Error> {
    sqlx::query_as::<_, PublishedExecution>(sql!(
        "SELECT md5(concat_ws('/', s.lifecycle_changed_at::text, o.execution_metadata::text,
                                  o.runtime, o.runtime_version, o.state_format_name,
                                  o.state_format_version)) AS subject,
                    NULL::uuid AS local_artifact_id, o.execution_metadata, o.runtime,
                    o.runtime_version, o.state_format_name, o.state_format_version
               FROM sandbox_observed_states o
               JOIN sandboxes s ON s.id = o.sandbox_id
              WHERE o.sandbox_id = $1
                -- A publish that carried no execution is no evidence: it
                -- isn't a record from before execution metadata existed.
                AND (o.execution_metadata IS NOT NULL
                     OR o.runtime_version IS NOT NULL
                     OR o.state_format_version IS NOT NULL)"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}
