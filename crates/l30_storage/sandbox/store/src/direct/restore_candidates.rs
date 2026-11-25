//! The Direct restore's candidate workers: the source node still holding a
//! suspended sandbox's local artifact, and the other workers that can
//! restore one of its full snapshots from storage. Each read has two whole
//! statements, one for a fresh restore and one for a retry of a restore
//! already claimed, which use different source snapshots. A fresh remote
//! restore uses only the newest full snapshot, and only when it is `ready`
//! with a storage location: `no_newer_full_snapshot` counts a newer full
//! snapshot whatever its upload state, so a later failed snapshot blocks
//! falling back to an older ready one.
//!
//! A candidate must be a registered, reachable, schedulable Direct worker
//! (`node_direct`) whose artifact components can load the snapshot, and
//! whose runtime, guest and state format can load it too, a check each
//! statement skips when the caller says the environment gate refuses and
//! the snapshot names an environment. Each statement also reports what the
//! environment gate would refuse a candidate for, from
//! `pplx_space_node_status_store::environment_refusal`, and refuses it only
//! when the caller says the gate refuses.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_node_status_store::environment_refusal;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    no_newer_full_snapshot, restore_retry_source_snapshot_is_current,
};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

#[cfg(test)]
mod tests;

/// A restore candidate worker, with what the environment gate would refuse
/// it for (an `EnvironmentRefusal` wire value); NULL when there's nothing to
/// refuse.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RestoreCandidateRow {
    pub id: NodeId,
    pub node_api_endpoint: String,
    /// The snapshot's storage URI; NULL for the source node, which restores
    /// from its local artifact.
    pub storage_uri: Option<String>,
    pub source_node_id: Option<NodeId>,
    pub environment_refusal: Option<String>,
}

// A fresh restore uses the newest full row regardless of upload state:
// those execution columns describe the artifact held by the source
// node even when its upload failed or its durable copy was claimed.
// A retry instead applies the same pre-claim freshness predicate as the
// remote leg, but the legs can pick different rows: the source leg takes the
// newest full row in that window whatever its upload state, while the remote
// leg also needs a `ready` row with a storage location. Newer post-claim
// attempts are not authoritative for compatibility.
// The observed-state join supplies only existence and placement.

/// The source-node statement of a fresh restore: the newest full snapshot.
static SOURCE_FRESH: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH latest_snapshot AS (
                 SELECT sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND TRUE
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    NULL::TEXT AS storage_uri,
                    COALESCE(s.target_node_id, s.active_node_id) AS source_node_id,
                    environment_gate.refusal AS environment_refusal
             FROM sandboxes s
             CROSS JOIN latest_snapshot
             JOIN sandbox_observed_states sos
               ON sos.sandbox_id = s.id
              AND sos.node_id = COALESCE(s.target_node_id, s.active_node_id)
             JOIN nodes n ON n.id = sos.node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND (($3::BOOLEAN IS TRUE AND latest_snapshot.execution_metadata->>'environment' IS NOT NULL)
                    OR EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND EXISTS (
                              SELECT 1
                              FROM node_runtime_state_formats state_format
                              WHERE state_format.node_id = runtime.node_id
                                AND state_format.runtime = runtime.runtime
                                AND state_format.runtime_version = runtime.runtime_version
                                AND state_format.state_format_name = latest_snapshot.state_format_name
                                AND split_part(state_format.state_format_version, '.', 1)
                                    = split_part(latest_snapshot.state_format_version, '.', 1)
                                AND state_format.can_load IS TRUE
                          )
                   ))
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND (
                    $3::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             LIMIT 1",
        refusal = environment_refusal("nss", "latest_snapshot.execution_metadata->>'environment'"),
    ))
});

/// The source-node statement of a restore retry: the pre-claim source
/// snapshot.
static SOURCE_RETRY: LazyLock<String> = LazyLock::new(|| {
    let source_snapshot = restore_retry_source_snapshot_is_current("s", "sn");
    sql!(format!(
        "WITH latest_snapshot AS (
                 SELECT sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND {source_snapshot}
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    NULL::TEXT AS storage_uri,
                    COALESCE(s.target_node_id, s.active_node_id) AS source_node_id,
                    environment_gate.refusal AS environment_refusal
             FROM sandboxes s
             CROSS JOIN latest_snapshot
             JOIN sandbox_observed_states sos
               ON sos.sandbox_id = s.id
              AND sos.node_id = COALESCE(s.target_node_id, s.active_node_id)
             JOIN nodes n ON n.id = sos.node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND (($3::BOOLEAN IS TRUE AND latest_snapshot.execution_metadata->>'environment' IS NOT NULL)
                    OR EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND EXISTS (
                              SELECT 1
                              FROM node_runtime_state_formats state_format
                              WHERE state_format.node_id = runtime.node_id
                                AND state_format.runtime = runtime.runtime
                                AND state_format.runtime_version = runtime.runtime_version
                                AND state_format.state_format_name = latest_snapshot.state_format_name
                                AND split_part(state_format.state_format_version, '.', 1)
                                    = split_part(latest_snapshot.state_format_version, '.', 1)
                                AND state_format.can_load IS TRUE
                          )
                   ))
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND (
                    $3::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             LIMIT 1",
        refusal = environment_refusal("nss", "latest_snapshot.execution_metadata->>'environment'"),
    ))
});

/// The remote statement of a fresh restore: the newest full snapshot, when
/// it is ready.
static REMOTE_FRESH: LazyLock<String> = LazyLock::new(|| {
    let remote_source = no_newer_full_snapshot("s", "sn");
    sql!(format!(
        "WITH latest_snapshot AS (
                 SELECT sn.id AS snapshot_id,
                        storage.storage_connection_string AS storage_uri,
                        COALESCE(sn.source_node_id, s.target_node_id, s.active_node_id) AS source_node_id,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 JOIN LATERAL (
                     SELECT ssl.storage_connection_string
                     FROM snapshot_storage_locations ssl
                     WHERE ssl.snapshot_id = sn.id
                     ORDER BY ssl.created_at DESC, ssl.id DESC
                     LIMIT 1
                 ) storage ON TRUE
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
                   AND {remote_source}
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    latest_snapshot.storage_uri,
                    latest_snapshot.source_node_id,
                    environment_gate.refusal AS environment_refusal
             FROM latest_snapshot
             JOIN nodes n ON n.id IS DISTINCT FROM latest_snapshot.source_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE n.managed_by = $3
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND (($4::BOOLEAN IS TRUE AND latest_snapshot.execution_metadata->>'environment' IS NOT NULL)
                    OR EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND EXISTS (
                              SELECT 1
                              FROM node_runtime_state_formats state_format
                              WHERE state_format.node_id = runtime.node_id
                                AND state_format.runtime = runtime.runtime
                                AND state_format.runtime_version = runtime.runtime_version
                                AND state_format.state_format_name = latest_snapshot.state_format_name
                                AND split_part(state_format.state_format_version, '.', 1)
                                    = split_part(latest_snapshot.state_format_version, '.', 1)
                                AND state_format.can_load IS TRUE
                          )
                   ))
                   -- Component compatibility reads the components array of the
                   -- execution_metadata document written by BeginArtifactBackup.
                   -- Omitted optional keys surface as NULL
                   -- columns: NULL digest/version/format arms are unconstrained,
                   -- and an absent role maps to node_artifact_components'
                   -- role_key ''. A NULL document (legacy rows) means no
                   -- compatibility requirements. Malformed documents fail
                   -- closed: a non-array components value errors the whole
                   -- candidate query rather than silently passing, and a
                   -- component with no kind can never match an installed
                   -- component, so it excludes the node.
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             ORDER BY random()
             LIMIT $2",
        refusal = environment_refusal("nss", "latest_snapshot.execution_metadata->>'environment'"),
    ))
});

/// The remote statement of a restore retry: the pre-claim source snapshot,
/// when it is ready.
static REMOTE_RETRY: LazyLock<String> = LazyLock::new(|| {
    let remote_source = restore_retry_source_snapshot_is_current("s", "sn");
    sql!(format!(
        "WITH latest_snapshot AS (
                 SELECT sn.id AS snapshot_id,
                        storage.storage_connection_string AS storage_uri,
                        COALESCE(sn.source_node_id, s.target_node_id, s.active_node_id) AS source_node_id,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 JOIN LATERAL (
                     SELECT ssl.storage_connection_string
                     FROM snapshot_storage_locations ssl
                     WHERE ssl.snapshot_id = sn.id
                     ORDER BY ssl.created_at DESC, ssl.id DESC
                     LIMIT 1
                 ) storage ON TRUE
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
                   AND {remote_source}
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    latest_snapshot.storage_uri,
                    latest_snapshot.source_node_id,
                    environment_gate.refusal AS environment_refusal
             FROM latest_snapshot
             JOIN nodes n ON n.id IS DISTINCT FROM latest_snapshot.source_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE n.managed_by = $3
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND (($4::BOOLEAN IS TRUE AND latest_snapshot.execution_metadata->>'environment' IS NOT NULL)
                    OR EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND EXISTS (
                              SELECT 1
                              FROM node_runtime_state_formats state_format
                              WHERE state_format.node_id = runtime.node_id
                                AND state_format.runtime = runtime.runtime
                                AND state_format.runtime_version = runtime.runtime_version
                                AND state_format.state_format_name = latest_snapshot.state_format_name
                                AND split_part(state_format.state_format_version, '.', 1)
                                    = split_part(latest_snapshot.state_format_version, '.', 1)
                                AND state_format.can_load IS TRUE
                          )
                   ))
                   -- Component compatibility reads the components array of the
                   -- execution_metadata document written by BeginArtifactBackup.
                   -- Omitted optional keys surface as NULL
                   -- columns: NULL digest/version/format arms are unconstrained,
                   -- and an absent role maps to node_artifact_components'
                   -- role_key ''. A NULL document (legacy rows) means no
                   -- compatibility requirements. Malformed documents fail
                   -- closed: a non-array components value errors the whole
                   -- candidate query rather than silently passing, and a
                   -- component with no kind can never match an installed
                   -- component, so it excludes the node.
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             ORDER BY random()
             LIMIT $2",
        refusal = environment_refusal("nss", "latest_snapshot.execution_metadata->>'environment'"),
    ))
});

/// The source node that can restore the sandbox from its local artifact,
/// if any: the row's `target_node_id`, else its `active_node_id`, with an
/// observed state there. `restore_retry` picks the retry's source snapshot;
/// `environment_refuses` drops a candidate the environment gate refuses, and,
/// for a snapshot that names an environment, replaces the runtime check.
pub async fn source_candidates<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    restore_retry: bool,
    environment_refuses: bool,
) -> Result<Vec<RestoreCandidateRow>, sqlx::Error> {
    let statement = if restore_retry {
        &SOURCE_RETRY
    } else {
        &SOURCE_FRESH
    };
    sqlx::query_as::<_, RestoreCandidateRow>(sqlx::AssertSqlSafe(statement.as_str()))
        .bind(sandbox_id.as_uuid())
        .bind(SubstrateKind::Space)
        .bind(environment_refuses)
        .fetch_all(executor)
        .await
}

/// Up to `limit` workers other than the snapshot's source node that can
/// restore the sandbox's ready full snapshot from storage, in random order.
/// `restore_retry` picks the retry's source snapshot; `environment_refuses`
/// drops a candidate the environment gate refuses, and, for a snapshot that
/// names an environment, replaces the runtime check.
pub async fn remote_candidates<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    restore_retry: bool,
    limit: i64,
    environment_refuses: bool,
) -> Result<Vec<RestoreCandidateRow>, sqlx::Error> {
    let statement = if restore_retry {
        &REMOTE_RETRY
    } else {
        &REMOTE_FRESH
    };
    sqlx::query_as::<_, RestoreCandidateRow>(sqlx::AssertSqlSafe(statement.as_str()))
        .bind(sandbox_id.as_uuid())
        .bind(limit)
        .bind(SubstrateKind::Space)
        .bind(environment_refuses)
        .fetch_all(executor)
        .await
}
