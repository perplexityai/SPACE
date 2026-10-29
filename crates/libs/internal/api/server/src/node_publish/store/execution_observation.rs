//! Decode publication evidence without treating omission or collection failure
//! as an empty inventory. Existing placement projections remain unchanged.

use pplx_space_common_core::{
    sql,
    types::{ArtifactComponentMetadata, NodeId, RuntimeIdentity},
};

use crate::node_publish::validate::execution_inventory::{
    ComponentInstallation, RuntimeFormatClaim, RuntimeFormatKey, RuntimeGuestClaim,
    ValidatedExecutionInventory,
};
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
pub(super) struct PreparedObservation<'a> {
    node_id: NodeId,
    report: &'a ValidatedReport,
}

/// Lock the registered node before inventory writes, including empty censuses.
/// Omitted evidence acquires no lock; a missing registration is distinct from
/// an unavailable inventory and must not create a new node.
pub(super) async fn prepare<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    report: Option<&'a ValidatedReport>,
) -> Result<Option<PreparedObservation<'a>>, PrepareExecutionObservationError> {
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

pub(super) struct PublishedExecutionObservation<'a> {
    pub status: InventoryStatus,
    pub policy: Option<&'a pplx_space_common_core::types::ExecutionPolicyObservation>,
}

/// Apply inventory evidence under the node lock acquired by `prepare`.
/// Omitted or unavailable evidence preserves inventory rows; a complete empty
/// census replaces the inventory. Unchanged rows are a successful outcome.
pub(super) async fn publish<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    prepared: Option<PreparedObservation<'a>>,
) -> Result<Option<PublishedExecutionObservation<'a>>, PublishExecutionInventoryError> {
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
            Ok(policy) => policy.as_ref(),
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
    inventory: &ValidatedExecutionInventory,
) -> Result<(), PublishExecutionInventoryError> {
    let component_keys = inventory
        .components
        .iter()
        .map(|claim| &claim.component)
        .collect::<Vec<_>>();
    let runtimes = &inventory.runtimes;
    prune_runtime_installations(conn, node_id, &runtimes.installations).await?;
    prune_component_installations(conn, node_id, &component_keys).await?;
    upsert_runtime_installations(conn, node_id, &runtimes.installations).await?;
    prune_runtime_guests(conn, node_id, &runtimes.guests).await?;
    upsert_runtime_guests(conn, node_id, &runtimes.guests).await?;
    let format_keys = runtimes
        .formats
        .iter()
        .map(|claim| &claim.key)
        .collect::<Vec<_>>();
    prune_runtime_formats(conn, node_id, &format_keys).await?;
    upsert_runtime_formats(conn, node_id, &runtimes.formats).await?;
    upsert_component_inventory(conn, node_id, &inventory.components).await
}

/// Prune omitted installations under the caller's node lock.
/// Empty censuses still delete previous rows; zero deletions is normal.
async fn prune_runtime_installations(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    claims: &[RuntimeIdentity],
) -> Result<(), PublishExecutionInventoryError> {
    sqlx::query(sql!(
        "DELETE FROM node_runtime_installations installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM UNNEST($2::TEXT[], $3::TEXT[]) AS input(runtime, runtime_version)
             WHERE (input.runtime, input.runtime_version) =
                   (installed.runtime, installed.runtime_version)
         )"
    ))
    .bind(node_id)
    .bind(column_values(claims, |claim| claim.kind.alias()))
    .bind(column_values(claims, |claim| claim.version.as_str()))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::ReplaceRuntimes)?;
    Ok(())
}

/// Prune omitted components under the caller's node lock.
/// Complete empty censuses still delete previous rows; zero deletions is normal.
async fn prune_component_installations(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    keys: &[&ArtifactComponentMetadata],
) -> Result<(), PublishExecutionInventoryError> {
    sqlx::query(sql!(
        "DELETE FROM node_artifact_components installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[]) AS input(
                 kind, role_key, version_key, digest_key, format_key
             )
             WHERE (input.kind, input.role_key, input.version_key, input.digest_key, input.format_key) =
                   (installed.kind, installed.role_key, installed.version_key,
                    installed.digest_key, installed.format_key)
         )"
    ))
    .bind(node_id)
    .bind(column_values(keys, |key| key.kind.as_str()))
    .bind(column_values(keys, |key| key.role.as_ref().map_or("", |role| role.as_str())))
    // Empty strings are the existing non-null key representation of omitted metadata.
    .bind(column_values(keys, |key| key.version.as_ref().map_or("", |version| version.as_str())))
    .bind(column_values(keys, |key| key.digest.as_ref().map(ToString::to_string).unwrap_or_default()))
    .bind(column_values(keys, |key| key.format.as_ref().map_or("", |format| format.as_str())))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::ReplaceComponents)?;
    Ok(())
}

/// Add reported runtime keys under the caller's node lock.
/// Existing keys and empty claims are successful no-ops.
async fn upsert_runtime_installations(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    claims: &[RuntimeIdentity],
) -> Result<(), PublishExecutionInventoryError> {
    if claims.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "INSERT INTO node_runtime_installations (node_id, runtime, runtime_version, observed_at)
         SELECT DISTINCT $1, runtime, runtime_version, NOW()
         FROM UNNEST($2::TEXT[], $3::TEXT[]) AS input(runtime, runtime_version) WHERE TRUE
         ON CONFLICT (node_id, runtime, runtime_version) DO NOTHING"
    ))
    .bind(node_id)
    .bind(column_values(claims, |claim| claim.kind.alias()))
    .bind(column_values(claims, |claim| claim.version.as_str()))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertRuntimes)?;
    Ok(())
}

/// Delete guests omitted from the complete census under the caller's node lock.
/// The empty census must revoke retained capabilities too; zero deletions is normal.
async fn prune_runtime_guests(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    claims: &[RuntimeGuestClaim],
) -> Result<(), PublishExecutionInventoryError> {
    // A retained runtime can lose a guest or format; pruning only removed
    // runtime parents would leave those capabilities advertised indefinitely.
    sqlx::query(sql!(
        "DELETE FROM node_runtime_guests installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[])
                 AS input(runtime, runtime_version, guest_os, guest_arch)
             WHERE (input.runtime, input.runtime_version, input.guest_os, input.guest_arch) =
                   (installed.runtime, installed.runtime_version,
                    installed.guest_os, installed.guest_arch)
         )"
    ))
    .bind(node_id)
    .bind(column_values(claims, |claim| claim.runtime.kind.alias()))
    .bind(column_values(claims, |claim| {
        claim.runtime.version.as_str()
    }))
    .bind(column_values(claims, |claim| claim.guest.os.as_str()))
    .bind(column_values(claims, |claim| {
        claim.guest.architecture.as_str()
    }))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::PruneGuests)?;
    Ok(())
}

/// Add reported guest keys under the caller's node lock.
/// Existing keys and empty claims are successful no-ops.
async fn upsert_runtime_guests(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    claims: &[RuntimeGuestClaim],
) -> Result<(), PublishExecutionInventoryError> {
    if claims.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "INSERT INTO node_runtime_guests (
             node_id, runtime, runtime_version, guest_os, guest_arch
         )
         SELECT DISTINCT $1, input.runtime, input.runtime_version,
                input.guest_os, input.guest_arch
         FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[])
             AS input(runtime, runtime_version, guest_os, guest_arch)
         WHERE TRUE
         ON CONFLICT (node_id, runtime, runtime_version, guest_os, guest_arch) DO NOTHING"
    ))
    .bind(node_id)
    .bind(column_values(claims, |claim| claim.runtime.kind.alias()))
    .bind(column_values(claims, |claim| {
        claim.runtime.version.as_str()
    }))
    .bind(column_values(claims, |claim| claim.guest.os.as_str()))
    .bind(column_values(claims, |claim| {
        claim.guest.architecture.as_str()
    }))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertGuests)?;
    Ok(())
}

/// Revoke formats omitted from the complete census under the caller's node lock.
/// Empty censuses still delete previous rows; zero deletions is normal.
async fn prune_runtime_formats(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    keys: &[&RuntimeFormatKey],
) -> Result<(), PublishExecutionInventoryError> {
    sqlx::query(sql!(
        "DELETE FROM node_runtime_state_formats installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[])
                 AS input(runtime, runtime_version, state_format_name, state_format_version)
             WHERE (input.runtime, input.runtime_version,
                    input.state_format_name, input.state_format_version) =
                   (installed.runtime, installed.runtime_version,
                    installed.state_format_name, installed.state_format_version)
         )"
    ))
    .bind(node_id)
    .bind(column_values(keys, |key| key.runtime.kind.alias()))
    .bind(column_values(keys, |key| key.runtime.version.as_str()))
    .bind(column_values(keys, |key| key.format.format.as_str()))
    .bind(column_values(keys, |key| key.format.version.as_str()))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::PruneFormats)?;
    Ok(())
}

/// Merge reported format capabilities under the caller's node lock.
/// Repeated keys retain bool-or semantics; unchanged and empty claims are no-ops.
async fn upsert_runtime_formats(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    claims: &[RuntimeFormatClaim],
) -> Result<(), PublishExecutionInventoryError> {
    if claims.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name,
             state_format_version, can_create, can_load
         )
         SELECT $1, input.runtime, input.runtime_version,
                input.state_format_name, input.state_format_version,
                bool_or(input.can_create), bool_or(input.can_load)
         FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::BOOLEAN[], $7::BOOLEAN[])
             AS input(runtime, runtime_version, state_format_name, state_format_version,
                      can_create, can_load)
         GROUP BY input.runtime, input.runtime_version,
                  input.state_format_name, input.state_format_version
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
    .bind(column_values(claims, |claim| {
        claim.key.runtime.kind.alias()
    }))
    .bind(column_values(claims, |claim| {
        claim.key.runtime.version.as_str()
    }))
    .bind(column_values(claims, |claim| {
        claim.key.format.format.as_str()
    }))
    .bind(column_values(claims, |claim| {
        claim.key.format.version.as_str()
    }))
    .bind(column_values(claims, |claim| claim.can_create))
    .bind(column_values(claims, |claim| claim.can_load))
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertFormats)?;
    Ok(())
}

/// Write component descriptions after runtime replacement on the same connection.
/// Conflicts lock component rows; unchanged descriptions remain successful.
async fn upsert_component_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    claims: &[ComponentInstallation],
) -> Result<(), PublishExecutionInventoryError> {
    if claims.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "WITH input AS (
             SELECT kind, role_key,
                    COALESCE(version, $9::TEXT) AS version_key,
                    COALESCE(digest, $9::TEXT) AS digest_key,
                    COALESCE(format, $9::TEXT) AS format_key,
                    version, digest, format, default_for_new, legacy_default
             FROM UNNEST(
                 $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7::BOOLEAN[], $8::BOOLEAN[]
             ) AS input(
                 kind, role_key, version, digest, format, default_for_new, legacy_default
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
    .bind(column_values(claims, |claim| claim.component.kind.as_str()))
    .bind(column_values(claims, |claim| claim.component.role.as_ref().map_or("", |role| role.as_str())))
    .bind(column_values(claims, |claim| claim.component.version.as_ref().map(|version| version.as_str())))
    .bind(column_values(claims, |claim| claim.component.digest.as_ref().map(ToString::to_string)))
    .bind(column_values(claims, |claim| claim.component.format.as_ref().map(|format| format.as_str())))
    .bind(column_values(claims, |claim| claim.default_for_new))
    .bind(column_values(claims, |claim| claim.legacy_default))
    .bind("")
    .execute(&mut *conn)
    .await
    .map_err(PublishExecutionInventoryError::InsertComponents)?;
    Ok(())
}

fn column_values<'a, T, C>(claims: &'a [C], value: impl Fn(&'a C) -> T) -> Vec<T> {
    claims.iter().map(value).collect()
}

#[cfg(test)]
#[path = "../../execution_observation/tests.rs"]
mod tests;
