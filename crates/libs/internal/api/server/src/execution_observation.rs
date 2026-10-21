//! Decode publication evidence without treating omission or collection failure
//! as an empty inventory. Existing placement projections remain unchanged.

use pplx_space_common_core::{
    sql,
    types::{
        ArtifactComponentMetadata, GuestPlatform, NodeId, RuntimeIdentity, StateFormatIdentity,
    },
};
use pplx_space_execution_common::proto::NodeExecutionInventory;
use sqlx::types::Json;
use tonic::Status;

use crate::proto::{ExecutionInventoryObservation, execution_inventory_observation::Outcome};

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

#[derive(Debug)]
enum Report<'a> {
    Complete(&'a NodeExecutionInventory),
    Legacy(&'a NodeExecutionInventory),
    Unavailable,
}

#[derive(Debug, thiserror::Error)]
enum ObservationError {
    #[error("execution observation is missing a supported outcome")]
    MissingOutcome,
    #[error("complete execution observation disagrees with legacy inventory")]
    ConflictingInventory,
    #[error("unavailable execution observation cannot include legacy inventory")]
    UnavailableWithInventory,
}

fn decode<'a>(
    observation: Option<&'a ExecutionInventoryObservation>,
    legacy: Option<&'a NodeExecutionInventory>,
) -> Result<Option<Report<'a>>, ObservationError> {
    let Some(observation) = observation else {
        return Ok(legacy.map(Report::Legacy));
    };
    match observation.outcome.as_ref() {
        Some(Outcome::Complete(inventory)) => {
            if legacy.is_some_and(|legacy| legacy != inventory) {
                return Err(ObservationError::ConflictingInventory);
            }
            Ok(Some(Report::Complete(inventory)))
        }
        Some(Outcome::Unavailable(_)) => {
            if legacy.is_some() {
                return Err(ObservationError::UnavailableWithInventory);
            }
            Ok(Some(Report::Unavailable))
        }
        None => Err(ObservationError::MissingOutcome),
    }
}

/// Holds decoded evidence after acquiring this publication's node lock.
/// Construct before census writes; consume before the status upsert.
pub(super) struct PreparedObservation<'a> {
    node_id: NodeId,
    report: Report<'a>,
}

pub(super) async fn prepare<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    observation: Option<&'a ExecutionInventoryObservation>,
    legacy: Option<&'a NodeExecutionInventory>,
) -> Result<Option<PreparedObservation<'a>>, Status> {
    let Some(report) =
        decode(observation, legacy).map_err(|error| Status::invalid_argument(error.to_string()))?
    else {
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
        .map_err(|error| Status::internal(format!("lock node execution publication: {error}")))?
        .ok_or_else(|| Status::not_found("publishing node no longer exists"))?;
    Ok(Some(PreparedObservation { node_id, report }))
}

pub(super) async fn publish(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    prepared: Option<PreparedObservation<'_>>,
) -> Result<Option<InventoryStatus>, Status> {
    let Some(PreparedObservation { node_id, report }) = prepared else {
        return Ok(None);
    };
    let (status, inventory) = match report {
        Report::Complete(inventory) => (InventoryStatus::Complete, Some(inventory)),
        Report::Legacy(inventory) => (InventoryStatus::Legacy, Some(inventory)),
        Report::Unavailable => (InventoryStatus::Unavailable, None),
    };
    if let Some(inventory) = inventory {
        publish_inventory(tx, node_id, inventory).await?;
    }
    Ok(Some(status))
}

/// Reconcile a complete inventory without rewriting unchanged artifact rows.
/// The publication holds the registered-node lock and commits this projection
/// with its collection receipt. Row timestamps advance on changes; collection
/// freshness is carried by the independent receipt, even for identical samples.
async fn publish_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: &pplx_space_execution_common::proto::NodeExecutionInventory,
) -> Result<(), Status> {
    let mut runtime_rows = Vec::with_capacity(inventory.runtimes.len());
    for installation in &inventory.runtimes {
        let runtime = installation
            .runtime
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("runtime installation is missing identity"))
            .and_then(|runtime| {
                RuntimeIdentity::try_from(runtime)
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })?;
        let guests = installation
            .supported_guests
            .iter()
            .map(|guest| {
                GuestPlatform::try_from(guest)
                    .map(|guest| {
                        serde_json::json!({
                            "guest_os": guest.os.as_str(),
                            "guest_arch": guest.architecture.as_str(),
                        })
                    })
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let state_formats = installation
            .state_formats
            .iter()
            .map(|capability| {
                let identity = capability.identity.as_ref().ok_or_else(|| {
                    Status::invalid_argument("state format capability is missing identity")
                })?;
                let identity = StateFormatIdentity::try_from(identity)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                Ok(serde_json::json!({
                    "state_format_name": identity.format.as_str(),
                    "state_format_version": identity.version.as_str(),
                    "can_create": capability.can_create,
                    "can_load": capability.can_load,
                }))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        runtime_rows.push(serde_json::json!({
            "runtime": runtime.kind.alias(),
            "runtime_version": runtime.version.as_str(),
            "guests": guests,
            "state_formats": state_formats,
        }));
    }

    let component_rows = inventory
        .components
        .iter()
        .map(|installation| {
            let component = installation.component.as_ref().ok_or_else(|| {
                Status::invalid_argument("artifact component installation is missing component")
            })?;
            let component = ArtifactComponentMetadata::try_from(component)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(serde_json::json!({
                "kind": component.kind.as_str(),
                "role_key": component.role.as_ref().map_or("", |role| role.as_str()),
                "version_key": component.version.as_ref().map(ToString::to_string).unwrap_or_default(),
                "digest_key": component.digest.as_ref().map(ToString::to_string).unwrap_or_default(),
                "format_key": component.format.as_ref().map(ToString::to_string).unwrap_or_default(),
                "version": component.version.as_ref().map(ToString::to_string),
                "digest": component.digest.as_ref().map(ToString::to_string),
                "format": component.format.as_ref().map(ToString::to_string),
                "default_for_new": installation.default_for_new,
                "legacy_default": installation.legacy_default,
            }))
        })
        .collect::<Result<Vec<_>, Status>>()?;

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
    .map_err(|error| Status::internal(format!("replace node runtime inventory: {error}")))?;
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
    .map_err(|error| Status::internal(format!("replace node component inventory: {error}")))?;

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
    .map_err(|error| Status::internal(format!("insert node runtime inventory: {error}")))?;
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
    .map_err(|error| Status::internal(format!("prune node runtime guests: {error}")))?;
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
    .map_err(|error| Status::internal(format!("insert node runtime guests: {error}")))?;
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
    .map_err(|error| Status::internal(format!("prune node state formats: {error}")))?;
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
    .map_err(|error| Status::internal(format!("insert node state formats: {error}")))?;

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
    .map_err(|error| Status::internal(format!("insert node component inventory: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests;
