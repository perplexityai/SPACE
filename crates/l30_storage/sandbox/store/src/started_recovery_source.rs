//! The restore source a Kubernetes started recovery reads for a `started`
//! sandbox whose active execution is `recovering`: the snapshot of the row's
//! current suspend-origin artifact, its source node when that node is a ready,
//! undrained worker, and its newest live storage location.
//!
//! A plain read: it takes no lock and runs on the executor its caller passes,
//! which for the recovery handoff is the transaction that has already locked
//! the execution row. The caller decides what the snapshot state means for the
//! recovery.

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId};
use pplx_space_sandbox_portability::no_newer_full_snapshot;
use pplx_space_sql_locator::sql;

/// What [`started_recovery_restore_candidate`] selects, undecoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedRecoveryRestoreCandidate {
    /// Raw `snapshots.state`; the caller parses it.
    pub state: String,
    /// The snapshot's source node's Kubernetes name, when that node is a
    /// `ready` worker with no drain phase and a `node_k8s` row.
    pub source_node_name: Option<String>,
    /// The newest non-orphaned storage location's connection string,
    /// unvalidated.
    pub storage_uri: Option<String>,
}

/// The newest full suspend-origin snapshot of `sandbox_id`'s current local
/// artifact, for a `started` row whose active execution at `generation` is
/// `recovering`. The snapshot must be no older than the row's last command and
/// last access, and have no newer full snapshot ([`no_newer_full_snapshot`]).
/// `None` when nothing matches.
pub async fn started_recovery_restore_candidate(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<Option<StartedRecoveryRestoreCandidate>, DbError> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let no_newer = no_newer_full_snapshot("s", "sn");
        sql!(format!(
            "SELECT sn.state,
                    nk.k8s_node_name,
                    (
                        SELECT ssl.storage_connection_string
                          FROM snapshot_storage_locations ssl
                         WHERE ssl.snapshot_id = sn.id
                           AND ssl.orphaned_at IS NULL
                         ORDER BY ssl.created_at DESC
                         LIMIT 1
                    ) AS storage_uri
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.active
                AND e.state = 'recovering'
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
                AND sn.local_artifact_id = s.current_local_artifact_id
                AND sn.artifact_kind = s.current_local_artifact_kind
                AND sn.type = 'full'
                AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
               LEFT JOIN nodes n
                 ON n.id = sn.source_node_id
                AND n.role = 'worker'
                AND n.state = 'ready'
                AND n.drain_phase IS NULL
               LEFT JOIN node_k8s nk ON nk.node_id = n.id
              WHERE s.id = $1
                AND s.lifecycle = 'started'
                AND s.current_local_artifact_origin = 'suspend'
                AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
                AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
                AND {no_newer}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
        ))
    });
    let row: Option<(String, Option<String>, Option<String>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(QUERY.as_str()))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .fetch_optional(executor.into_sqlx())
            .await?;
    Ok(row.map(
        |(state, source_node_name, storage_uri)| StartedRecoveryRestoreCandidate {
            state,
            source_node_name,
            storage_uri,
        },
    ))
}
