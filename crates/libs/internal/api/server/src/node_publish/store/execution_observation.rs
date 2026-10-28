//! Decode publication evidence without treating omission or collection failure
//! as an empty inventory. Existing placement projections remain unchanged.

use pplx_space_common_core::{sql, types::NodeId};
use sqlx::types::Json;

use crate::node_publish::validate::execution_inventory::ValidatedExecutionInventory;
use crate::node_publish::validate::execution_observation::{
    ExecutionInventoryEvidence, ValidatedReport,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum PrepareExecutionObservationError {
    #[error("lock node execution publication: {0}")]
    Lock(#[source] sqlx::Error),
    #[error("publishing node no longer exists")]
    MissingNode,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PublishExecutionInventoryError {
    #[error("replace node runtime inventory: {0}")]
    ReplaceRuntimes(#[source] sqlx::Error),
    #[error("replace node component inventory: {0}")]
    ReplaceComponents(#[source] sqlx::Error),
    #[error("insert node runtime inventory: {0}")]
    InsertRuntimes(#[source] sqlx::Error),
    #[error("prune node runtime guests: {0}")]
    PruneGuests(#[source] sqlx::Error),
    #[error("insert node runtime guests: {0}")]
    InsertGuests(#[source] sqlx::Error),
    #[error("prune node state formats: {0}")]
    PruneFormats(#[source] sqlx::Error),
    #[error("insert node state formats: {0}")]
    InsertFormats(#[source] sqlx::Error),
    #[error("insert node component inventory: {0}")]
    InsertComponents(#[source] sqlx::Error),
}

/// The outcome recorded only when the surrounding publication commits.
#[derive(Clone, Copy, Debug)]
pub(super) enum InventoryStatus {
    Complete,
    Unavailable,
    Legacy,
}

impl InventoryStatus {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Unavailable => "unavailable",
            Self::Legacy => "legacy",
        }
    }
}

/// Holds decoded evidence after acquiring this publication's node lock.
/// Construct before census writes; consume before the status upsert.
#[derive(Debug)]
pub(super) struct PreparedObservation {
    node_id: NodeId,
    report: ValidatedReport,
}

/// Lock the registered node before inventory writes, including empty censuses.
/// Omitted evidence acquires no lock; a missing registration is distinct from
/// an unavailable inventory and must not create a new node.
pub(super) async fn prepare(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    report: Option<ValidatedReport>,
) -> Result<Option<PreparedObservation>, PrepareExecutionObservationError> {
    let Some(report) = report else {
        return Ok(None);
    };
    // Acquire before census writes to preserve node-before-sandbox lock order.
    // Serialize even empty replacements, which have no artifact row to lock.
    // Keep artifact writes before the status upsert, as legacy readers do.
    // Locking the status row first would invert that order during rollout.
    // NO KEY UPDATE permits the foreign-key key-share locks of node reports.
    sqlx::query_scalar::<_, NodeId>(sql!("SELECT id FROM nodes WHERE id = $1 FOR NO KEY UPDATE"))
        .bind(node_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(PrepareExecutionObservationError::Lock)?
        .ok_or(PrepareExecutionObservationError::MissingNode)?;
    Ok(Some(PreparedObservation { node_id, report }))
}

pub(super) struct PublishedExecutionObservation {
    pub status: InventoryStatus,
    pub policy: Option<pplx_space_common_core::types::ExecutionPolicyObservation>,
}

/// Apply inventory evidence under the node lock acquired by `prepare`.
/// Omitted or unavailable evidence preserves inventory rows; a complete empty
/// census replaces the inventory. Unchanged rows are a successful outcome.
pub(super) async fn publish(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    prepared: Option<PreparedObservation>,
) -> Result<Option<PublishedExecutionObservation>, PublishExecutionInventoryError> {
    let Some(PreparedObservation { node_id, report }) = prepared else {
        return Ok(None);
    };
    let (status, evidence) = match report {
        ValidatedReport::Complete(evidence) => (InventoryStatus::Complete, Some(evidence)),
        ValidatedReport::Legacy(evidence) => (InventoryStatus::Legacy, Some(evidence)),
        ValidatedReport::Unavailable => (InventoryStatus::Unavailable, None),
    };
    let policy = if let Some(ExecutionInventoryEvidence { inventory, policy }) = evidence {
        publish_inventory(tx, node_id, inventory).await?;
        match policy {
            Ok(policy) => policy,
            Err(error) => {
                tracing::warn!(%error, %node_id, "invalid execution policy observation; withholding policy evidence");
                None
            }
        }
    } else {
        None
    };
    Ok(Some(PublishedExecutionObservation { status, policy }))
}

/// Reconcile a complete inventory without rewriting unchanged artifact rows.
/// The publication holds the registered-node lock and commits this projection
/// with its collection receipt. Row timestamps advance on changes; collection
/// freshness is carried by the independent receipt, even for identical samples.
async fn publish_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: ValidatedExecutionInventory,
) -> Result<(), PublishExecutionInventoryError> {
    let (runtime_rows, component_rows) = inventory.into_legacy_rows();
    let runtime_rows = Json(serde_json::Value::Array(runtime_rows));
    let component_rows = Json(serde_json::Value::Array(component_rows));
    sqlx::query(sql!(
        "DELETE FROM node_runtime_installations installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb)
                 AS input(runtime TEXT, runtime_version TEXT)
             WHERE (input.runtime, input.runtime_version) =
                   (installed.runtime, installed.runtime_version)
         )"
    ))
    .bind(node_id)
    .bind(&runtime_rows)
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::ReplaceRuntimes)?;
    sqlx::query(sql!(
        "DELETE FROM node_artifact_components installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb) AS input(
                 kind TEXT, role_key TEXT, version_key TEXT, digest_key TEXT, format_key TEXT
             )
             WHERE (input.kind, input.role_key, input.version_key, input.digest_key, input.format_key) =
                   (installed.kind, installed.role_key, installed.version_key,
                    installed.digest_key, installed.format_key)
         )"
    ))
    .bind(node_id)
    .bind(&component_rows)
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::ReplaceComponents)?;

    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 runtime TEXT, runtime_version TEXT, guests JSONB, state_formats JSONB
             )
         )
         INSERT INTO node_runtime_installations (node_id, runtime, runtime_version, observed_at)
         SELECT DISTINCT $1, runtime, runtime_version, NOW() FROM input WHERE TRUE
         ON CONFLICT (node_id, runtime, runtime_version) DO NOTHING"
    ))
    .bind(node_id)
    .bind(runtime_rows.clone())
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertRuntimes)?;
    // A retained runtime can lose a guest or format; pruning only removed
    // runtime parents would leave those capabilities advertised indefinitely.
    sqlx::query(sql!(
        "DELETE FROM node_runtime_guests installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb)
                 AS input(runtime TEXT, runtime_version TEXT, guests JSONB)
             CROSS JOIN LATERAL jsonb_to_recordset(input.guests)
                 AS guest(guest_os TEXT, guest_arch TEXT)
             WHERE (input.runtime, input.runtime_version, guest.guest_os, guest.guest_arch) =
                   (installed.runtime, installed.runtime_version,
                    installed.guest_os, installed.guest_arch)
         )"
    ))
    .bind(node_id)
    .bind(&runtime_rows)
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::PruneGuests)?;
    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 runtime TEXT, runtime_version TEXT, guests JSONB
             )
         )
         INSERT INTO node_runtime_guests (
             node_id, runtime, runtime_version, guest_os, guest_arch
         )
         SELECT DISTINCT $1, input.runtime, input.runtime_version,
                guest.guest_os, guest.guest_arch
         FROM input
         CROSS JOIN LATERAL jsonb_to_recordset(COALESCE(input.guests, '[]'::jsonb))
             AS guest(guest_os TEXT, guest_arch TEXT)
         WHERE TRUE
         ON CONFLICT (node_id, runtime, runtime_version, guest_os, guest_arch) DO NOTHING"
    ))
    .bind(node_id)
    .bind(runtime_rows.clone())
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertGuests)?;
    sqlx::query(sql!(
        "DELETE FROM node_runtime_state_formats installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb)
                 AS input(runtime TEXT, runtime_version TEXT, state_formats JSONB)
             CROSS JOIN LATERAL jsonb_to_recordset(input.state_formats)
                 AS state_format(state_format_name TEXT, state_format_version TEXT)
             WHERE (input.runtime, input.runtime_version,
                    state_format.state_format_name, state_format.state_format_version) =
                   (installed.runtime, installed.runtime_version,
                    installed.state_format_name, installed.state_format_version)
         )"
    ))
    .bind(node_id)
    .bind(&runtime_rows)
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::PruneFormats)?;
    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 runtime TEXT, runtime_version TEXT, state_formats JSONB
             )
         )
         INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name,
             state_format_version, can_create, can_load
         )
         SELECT $1, input.runtime, input.runtime_version,
                state_format.state_format_name, state_format.state_format_version,
                bool_or(state_format.can_create), bool_or(state_format.can_load)
         FROM input
         CROSS JOIN LATERAL jsonb_to_recordset(
             COALESCE(input.state_formats, '[]'::jsonb)
         ) AS state_format(
             state_format_name TEXT, state_format_version TEXT,
             can_create BOOLEAN, can_load BOOLEAN
         )
         GROUP BY input.runtime, input.runtime_version,
                  state_format.state_format_name, state_format.state_format_version
         ON CONFLICT (
             node_id, runtime, runtime_version,
             state_format_name, state_format_version
         ) DO UPDATE SET
             can_create = EXCLUDED.can_create,
             can_load = EXCLUDED.can_load
         WHERE (node_runtime_state_formats.can_create, node_runtime_state_formats.can_load)
               IS DISTINCT FROM (EXCLUDED.can_create, EXCLUDED.can_load)"
    ))
    .bind(node_id)
    .bind(runtime_rows)
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertFormats)?;

    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 kind TEXT, role_key TEXT, version_key TEXT, digest_key TEXT,
                 format_key TEXT, version TEXT, digest TEXT, format TEXT,
                 default_for_new BOOLEAN, legacy_default BOOLEAN
             )
         )
         INSERT INTO node_artifact_components (
             node_id, kind, role_key, version_key, digest_key, format_key,
             version, digest, format, default_for_new, legacy_default, observed_at
         )
         SELECT $1, kind, role_key, version_key, digest_key, format_key,
                version, digest, format, bool_or(default_for_new), bool_or(legacy_default), NOW()
         FROM input
         GROUP BY kind, role_key, version_key, digest_key, format_key, version, digest, format
         ON CONFLICT (node_id, kind, role_key, version_key, digest_key, format_key)
         DO UPDATE SET
             default_for_new = EXCLUDED.default_for_new,
             legacy_default = EXCLUDED.legacy_default,
             version = EXCLUDED.version,
             digest = EXCLUDED.digest,
             format = EXCLUDED.format,
             observed_at = EXCLUDED.observed_at
         WHERE (node_artifact_components.default_for_new, node_artifact_components.legacy_default,
                node_artifact_components.version, node_artifact_components.digest,
                node_artifact_components.format)
               IS DISTINCT FROM (EXCLUDED.default_for_new, EXCLUDED.legacy_default,
                                 EXCLUDED.version, EXCLUDED.digest, EXCLUDED.format)"
    ))
    .bind(node_id)
    .bind(component_rows)
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertComponents)?;
    Ok(())
}

#[cfg(test)]
#[path = "../../execution_observation/tests.rs"]
mod tests;
