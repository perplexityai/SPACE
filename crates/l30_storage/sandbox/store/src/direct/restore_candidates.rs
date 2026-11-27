//! The Direct restore's candidate workers: the source node still holding a
//! suspended sandbox's local artifact, and the other workers that can
//! restore, from storage, the capture the shared rule selected
//! ([`crate::restore_source::select_restore_source`]). The source statement
//! checks the node against the local copy it will load, whatever that copy's
//! upload state: the pinned artifact's backup, else what the node published
//! for its suspended record, else its newest suspend capture, a retry reading
//! its pre-claim window.
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
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    capture_registered_at, registered_before_restore_claim, restore_pin_predates_claim,
    restore_retry_source_snapshot_window, snapshot_is_current_local_artifact,
};
use pplx_space_snapshot_model::ArtifactOrigin;
use pplx_space_sql_locator::sql;

use crate::restore_source::SelectedRestoreSource;

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

// Resolve compatibility metadata for the local copy `sos.node_id` will
// load:
// 0. When pinned (`current_local_artifact_id`, gated to pre-claim on
//    retry), use that exact artifact's `snapshots` row.
// 1. Otherwise, when `sandbox_observed_states` carries the suspended
//    local record's execution columns from `PublishSandboxStates`, use
//    that node's own published execution metadata directly.
// 2. When the node observation has no execution columns (legacy or
//    state-only degraded publish), fall back to full suspend captures
//    from that source node, ordering generated captures ahead of
//    ungenerated ones so a later ungenerated capture in an unordered
//    history never masks the generated local capture.
/// The source-node statement. `$1` = sandbox id, `$2` = the Direct substrate
/// kind, `$3` = whether the environment gate refuses, `$4` = whether the
/// restore is a retry, `$5` = the suspend artifact origin.
static SOURCE: LazyLock<String> = LazyLock::new(|| {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let pin_pre_claim = restore_pin_predates_claim("s");
    let window = registered_before_restore_claim("s", "sn");
    let retry_window = restore_retry_source_snapshot_window("s", "sn");
    let captured = capture_registered_at("sn");
    let refusal = environment_refusal("nss", "source_execution.execution_metadata->>'environment'");
    sql!(format!(
        "SELECT n.id,
                nd.node_api_endpoint,
                NULL::TEXT AS storage_uri,
                COALESCE(s.target_node_id, s.active_node_id) AS source_node_id,
                environment_gate.refusal AS environment_refusal
         FROM sandboxes s
         JOIN sandbox_observed_states sos
           ON sos.sandbox_id = s.id
          AND sos.node_id = COALESCE(s.target_node_id, s.active_node_id)
         CROSS JOIN LATERAL (
             SELECT candidate.runtime,
                    candidate.guest_os,
                    candidate.guest_arch,
                    candidate.state_format_name,
                    candidate.state_format_version,
                    candidate.execution_metadata
             FROM (
                 SELECT 0 AS priority,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata,
                        TRUE AS has_generation,
                        sn.generation,
                        {captured} AS captured_at,
                        sn.local_artifact_id,
                        sn.timestamp,
                        sn.id
                   FROM snapshots sn
                  WHERE sn.sandbox_id = s.id
                    AND sn.type = 'full'
                    AND {pinned}
                    AND (NOT $4::BOOLEAN OR {window})
                 UNION ALL
                 SELECT 1 AS priority,
                        sos.runtime,
                        sos.guest_os,
                        sos.guest_arch,
                        sos.state_format_name,
                        sos.state_format_version,
                        sos.execution_metadata,
                        TRUE AS has_generation,
                        NULL::BIGINT AS generation,
                        NULL::TIMESTAMPTZ AS captured_at,
                        NULL::UUID AS local_artifact_id,
                        NULL::TIMESTAMPTZ AS timestamp,
                        NULL::UUID AS id
                  WHERE (NOT $4::BOOLEAN OR NOT {pin_pre_claim})
                    AND sos.status = 'suspended'
                    AND sos.runtime IS NOT NULL
                    AND sos.guest_os IS NOT NULL
                    AND sos.guest_arch IS NOT NULL
                    AND sos.state_format_name IS NOT NULL
                    AND sos.state_format_version IS NOT NULL
                 UNION ALL
                 SELECT 2 AS priority,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata,
                        (sn.generation IS NOT NULL) AS has_generation,
                        sn.generation,
                        {captured} AS captured_at,
                        sn.local_artifact_id,
                        sn.timestamp,
                        sn.id
                   FROM snapshots sn
                  WHERE sn.sandbox_id = s.id
                    AND sn.type = 'full'
                    AND (sn.source_node_id IS NULL
                         OR sn.source_node_id = COALESCE(s.target_node_id, s.active_node_id))
                    AND CASE
                            WHEN $4::BOOLEAN THEN NOT {pin_pre_claim} AND {retry_window}
                            ELSE s.current_local_artifact_id IS NULL
                                 AND (sn.artifact_origin IS NULL OR sn.artifact_origin = $5)
                        END
             ) candidate
             ORDER BY candidate.priority ASC,
                      candidate.has_generation DESC,
                      candidate.generation DESC NULLS LAST,
                      candidate.captured_at DESC NULLS LAST,
                      candidate.local_artifact_id DESC NULLS LAST,
                      candidate.timestamp DESC NULLS LAST,
                      candidate.id DESC NULLS LAST
             LIMIT 1
         ) source_execution
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
           AND (source_execution.runtime = 'fc' OR (
                nss.execution_inventory_status = 'complete'
                AND nss.execution_inventory_received_at
                    + (nss.stale_after - nss.observed_at) > NOW()
                AND source_execution.execution_metadata IS NOT NULL
           ))
           AND source_execution.runtime IS NOT NULL
           AND source_execution.guest_os IS NOT NULL
           AND source_execution.guest_arch IS NOT NULL
           AND source_execution.state_format_name IS NOT NULL
           AND source_execution.state_format_version IS NOT NULL
           AND (($3::BOOLEAN IS TRUE AND source_execution.execution_metadata->>'environment' IS NOT NULL)
                OR EXISTS (
                SELECT 1
                FROM node_runtime_installations runtime
                JOIN node_runtime_guests guest
                  ON guest.node_id = runtime.node_id
                 AND guest.runtime = runtime.runtime
                 AND guest.runtime_version = runtime.runtime_version
                WHERE runtime.node_id = n.id
                  AND runtime.runtime = source_execution.runtime
                  AND guest.guest_os = source_execution.guest_os
                  AND guest.guest_arch = source_execution.guest_arch
                  AND EXISTS (
                          SELECT 1
                          FROM node_runtime_state_formats state_format
                          WHERE state_format.node_id = runtime.node_id
                            AND state_format.runtime = runtime.runtime
                            AND state_format.runtime_version = runtime.runtime_version
                            AND state_format.state_format_name = source_execution.state_format_name
                            AND split_part(state_format.state_format_version, '.', 1)
                                = split_part(source_execution.state_format_version, '.', 1)
                            AND state_format.can_load IS TRUE
                      )
               ))
               AND NOT EXISTS (
                SELECT 1
                FROM jsonb_to_recordset(
                         COALESCE(source_execution.execution_metadata->'components',
                                  '[]'::jsonb)
                     ) AS requested(kind text, role text, digest text,
                                    version text, format text)
                WHERE (source_execution.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
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
         LIMIT 1"
    ))
});

/// The remote statement. `$1` = sandbox id, `$2` = the candidate limit, `$3` =
/// the Direct substrate kind, `$4` = whether the environment gate refuses,
/// `$5` = the selected capture's storage location, `$6` = its snapshot id.
static REMOTE: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH selected_snapshot AS (
             SELECT $5::TEXT AS storage_uri,
                    COALESCE(sn.source_node_id, s.target_node_id, s.active_node_id) AS source_node_id,
                    sn.runtime,
                    sn.guest_os,
                    sn.guest_arch,
                    sn.state_format_name,
                    sn.state_format_version,
                    sn.execution_metadata
             FROM sandboxes s
             JOIN snapshots sn ON sn.sandbox_id = s.id
             WHERE s.id = $1
               AND sn.id = $6
         )
         SELECT n.id,
                nd.node_api_endpoint,
                selected_snapshot.storage_uri,
                selected_snapshot.source_node_id,
                environment_gate.refusal AS environment_refusal
         FROM selected_snapshot
         JOIN nodes n ON n.id IS DISTINCT FROM selected_snapshot.source_node_id
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
           AND (selected_snapshot.runtime = 'fc' OR (
                nss.execution_inventory_status = 'complete'
                AND nss.execution_inventory_received_at
                    + (nss.stale_after - nss.observed_at) > NOW()
                AND selected_snapshot.execution_metadata IS NOT NULL
           ))
           AND selected_snapshot.runtime IS NOT NULL
           AND selected_snapshot.guest_os IS NOT NULL
           AND selected_snapshot.guest_arch IS NOT NULL
           AND selected_snapshot.state_format_name IS NOT NULL
           AND selected_snapshot.state_format_version IS NOT NULL
           AND (($4::BOOLEAN IS TRUE AND selected_snapshot.execution_metadata->>'environment' IS NOT NULL)
                OR EXISTS (
                SELECT 1
                FROM node_runtime_installations runtime
                JOIN node_runtime_guests guest
                  ON guest.node_id = runtime.node_id
                 AND guest.runtime = runtime.runtime
                 AND guest.runtime_version = runtime.runtime_version
                WHERE runtime.node_id = n.id
                  AND runtime.runtime = selected_snapshot.runtime
                  AND guest.guest_os = selected_snapshot.guest_os
                  AND guest.guest_arch = selected_snapshot.guest_arch
                  AND EXISTS (
                          SELECT 1
                          FROM node_runtime_state_formats state_format
                          WHERE state_format.node_id = runtime.node_id
                            AND state_format.runtime = runtime.runtime
                            AND state_format.runtime_version = runtime.runtime_version
                            AND state_format.state_format_name = selected_snapshot.state_format_name
                            AND split_part(state_format.state_format_version, '.', 1)
                                = split_part(selected_snapshot.state_format_version, '.', 1)
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
                         COALESCE(selected_snapshot.execution_metadata->'components',
                                  '[]'::jsonb)
                     ) AS requested(kind text, role text, digest text,
                                    version text, format text)
                WHERE (selected_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
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
        refusal = environment_refusal("nss", "selected_snapshot.execution_metadata->>'environment'"),
    ))
});

/// The source node that can restore the sandbox from its local artifact,
/// if any: the row's `target_node_id`, else its `active_node_id`, with an
/// observed state there. `restore_retry` reads the retry's pre-claim window;
/// `environment_refuses` drops a candidate the environment gate refuses, and,
/// for a copy that names an environment, replaces the runtime check.
pub async fn source_candidates(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    restore_retry: bool,
    environment_refuses: bool,
) -> Result<Vec<RestoreCandidateRow>, DbError> {
    Ok(
        sqlx::query_as::<_, RestoreCandidateRow>(sqlx::AssertSqlSafe(SOURCE.as_str()))
            .bind(sandbox_id.as_uuid())
            .bind(SubstrateKind::Space)
            .bind(environment_refuses)
            .bind(restore_retry)
            .bind(ArtifactOrigin::Suspend.as_str())
            .fetch_all(executor.into_sqlx())
            .await?,
    )
}

/// Up to `limit` workers other than the capture's source node that can
/// restore `selected`, the capture the shared rule selected for the sandbox,
/// from storage, in random order. `environment_refuses` drops a candidate the
/// environment gate refuses, and, for a capture that names an environment,
/// replaces the runtime check.
pub async fn remote_candidates(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    selected: &SelectedRestoreSource,
    limit: i64,
    environment_refuses: bool,
) -> Result<Vec<RestoreCandidateRow>, DbError> {
    Ok(
        sqlx::query_as::<_, RestoreCandidateRow>(sqlx::AssertSqlSafe(REMOTE.as_str()))
            .bind(sandbox_id.as_uuid())
            .bind(limit)
            .bind(SubstrateKind::Space)
            .bind(environment_refuses)
            .bind(&selected.storage_connection_string)
            .bind(selected.snapshot_id)
            .fetch_all(executor.into_sqlx())
            .await?,
    )
}

/// Whether the worker holding the sandbox's local copy, its
/// `target_node_id`, else its `active_node_id`, is still registered: its
/// `node_direct` row isn't deleted and its `nodes` row isn't a `deleted`
/// tombstone. A registered worker may take the restore once it's eligible
/// again.
pub async fn source_node_registered(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes s
               JOIN nodes n ON n.id = COALESCE(s.target_node_id, s.active_node_id)
               JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
              WHERE s.id = $1
                AND n.managed_by = $2
                AND n.role = 'worker'
                AND COALESCE(n.state, 'registered') != 'deleted'
         )"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SubstrateKind::Space)
    .fetch_one(executor.into_sqlx())
    .await?)
}
