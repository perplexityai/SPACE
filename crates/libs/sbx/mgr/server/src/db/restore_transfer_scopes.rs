//! `sandbox_restore_transfer_scopes` rows.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreTransferScopeRow {
    pub transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    pub execution_generation: Option<ExecutionGeneration>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub staging_dir: String,
}

pub(super) fn restore_transfer_scope_from_row(
    row: &rusqlite::Row<'_>,
    first_column: usize,
) -> rusqlite::Result<RestoreTransferScopeRow> {
    let scope: String = row.get(first_column)?;
    let generation: Option<i64> = row.get(first_column + 1)?;
    let attempt: Option<String> = row.get(first_column + 2)?;
    Ok(RestoreTransferScopeRow {
        transfer_scope_id: scope.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                first_column,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        execution_generation: generation
            .map(ExecutionGeneration::try_from)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    first_column + 1,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?,
        activation_attempt_id: attempt
            .map(|value| value.parse())
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    first_column + 2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        staging_dir: row.get(first_column + 3)?,
    })
}

pub fn restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<RestoreTransferScopeRow>> {
    conn.query_row(
        "SELECT transfer_scope_id, execution_generation, activation_attempt_id, staging_dir
           FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1",
        params![sandbox_id],
        |row| restore_transfer_scope_from_row(row, 0),
    )
    .optional()
}

pub fn rowless_restore_transfer_scopes(
    conn: &Connection,
) -> Result<Vec<(String, RestoreTransferScopeRow)>> {
    let mut statement = conn.prepare(
        "SELECT scopes.sandbox_id, scopes.transfer_scope_id,
                scopes.execution_generation, scopes.activation_attempt_id,
                scopes.staging_dir
           FROM sandbox_restore_transfer_scopes AS scopes
      LEFT JOIN sandboxes ON sandboxes.id = scopes.sandbox_id
          WHERE sandboxes.id IS NULL",
    )?;
    let mut rows = statement.query([])?;
    let mut scopes = Vec::new();
    while let Some(row) = rows.next()? {
        let decoded = (|| Ok((row.get(0)?, restore_transfer_scope_from_row(row, 1)?)))();
        match decoded {
            Ok(scope) => scopes.push(scope),
            Err(error) if is_row_decode_error(&error) => tracing::warn!(
                sandbox_id = ?row.get_ref(0)?,
                error = %describe_error(&error),
                "retaining malformed rowless restore scope; operator repair required"
            ),
            Err(error) => return Err(error),
        }
    }
    Ok(scopes)
}

pub fn retire_rowless_restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<bool> {
    let changed = conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1
            AND transfer_scope_id = ?2
            AND NOT EXISTS (
                SELECT 1 FROM sandboxes WHERE id = ?1
            )",
        params![sandbox_id, transfer_scope_id.to_string()],
    )?;
    Ok(changed == 1)
}

pub fn retire_orphan_quarantine_restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<bool> {
    let changed = conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1
            AND transfer_scope_id = ?2
            AND EXISTS (
                SELECT 1
                  FROM sandboxes
                  JOIN sandbox_quarantine_state qs ON qs.sandbox_id = sandboxes.id
                 WHERE sandboxes.id = ?1
                   AND sandboxes.state IN ('quarantined', 'restoring')
                   AND qs.source = ?3
                   AND qs.reason_code = ?4
            )",
        params![
            sandbox_id,
            transfer_scope_id.to_string(),
            SandboxQuarantineSource::SbxManager.as_str(),
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str(),
        ],
    )?;
    Ok(changed == 1)
}

/// Claim cleanup of a retryable interrupted-restore scope with the current or
/// a strictly newer control-plane activation attempt.
///
/// This deliberately retains the physical scope and staging path. Cancellation
/// does not permanently close a volume-manager scope, so the new owner can
/// reuse it after cleanup while a crash leaves a durable retry reference.
pub fn claim_retryable_restore_transfer_scope_if_current_or_newer(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandbox_restore_transfer_scopes
            SET execution_generation = ?1,
                activation_attempt_id = ?2,
                created_at = ?5
          WHERE sandbox_id = ?3
            AND transfer_scope_id = ?4
            AND (
                (
                    execution_generation IS NULL
                    AND activation_attempt_id IS NULL
                )
                OR (
                    execution_generation IS NOT NULL
                    AND activation_attempt_id IS NOT NULL
                    AND (
                        execution_generation < ?1
                        OR (
                            execution_generation = ?1
                            AND activation_attempt_id <= ?2
                        )
                    )
                )
            )
            AND EXISTS (
                SELECT 1
                 FROM sandboxes
                 WHERE sandboxes.id = ?3
                   AND sandboxes.state = 'restoring'
                   AND sandboxes.restore_origin = 'remote'
                   AND sandboxes.uid IS NULL
                   AND sandboxes.slot IS NULL
                   AND sandboxes.guest_cid IS NULL
                   AND sandboxes.pid IS NULL
                   AND sandboxes.spaced_host_pid IS NULL
                   AND sandboxes.netgtw_pid IS NULL
                   AND sandboxes.vsock_uds_path IS NULL
                   AND sandboxes.api_socket_path IS NULL
                   AND sandboxes.current_local_artifact_id IS NULL
                   AND sandboxes.cleanup_completion IS NULL
                   AND sandboxes.cleanup_keep_filesystem IS NULL
            )",
        params![
            execution_generation.get(),
            activation_attempt_id.to_string(),
            sandbox_id,
            transfer_scope_id.to_string(),
            now()?,
        ],
    )?;
    Ok(changed == 1)
}

/// Transfer cleanup for an already-exchanged canonical root to a strictly
/// newer control-plane activation attempt without changing the scope that
/// owns the old root on disk.
///
/// The exchange receipt remains immutable evidence of the attempt that moved
/// the old root. This CAS only advances cleanup ownership, so a successor can
/// retry fallible removal while delayed predecessors remain fenced out.
pub fn advance_exchanged_restore_cleanup_owner_if_newer(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandbox_restore_transfer_scopes
            SET execution_generation = ?1,
                activation_attempt_id = ?2,
                created_at = ?5
          WHERE sandbox_id = ?3
            AND transfer_scope_id = ?4
            AND execution_generation IS NOT NULL
            AND activation_attempt_id IS NOT NULL
            AND (
                execution_generation < ?1
                OR (
                    execution_generation = ?1
                    AND activation_attempt_id < ?2
                )
            )
            AND EXISTS (
                SELECT 1
                  FROM sandboxes
                 WHERE sandboxes.id = ?3
                   AND sandboxes.state = 'restoring'
                   AND sandboxes.restore_origin = 'remote'
                   AND sandboxes.uid IS NULL
                   AND sandboxes.slot IS NULL
                   AND sandboxes.guest_cid IS NULL
                   AND sandboxes.pid IS NULL
                   AND sandboxes.spaced_host_pid IS NULL
                   AND sandboxes.netgtw_pid IS NULL
                   AND sandboxes.vsock_uds_path IS NULL
                   AND sandboxes.api_socket_path IS NULL
                   AND sandboxes.current_local_artifact_id IS NULL
                   AND sandboxes.cleanup_completion IS NULL
                   AND sandboxes.cleanup_keep_filesystem IS NULL
            )",
        params![
            execution_generation.get(),
            activation_attempt_id.to_string(),
            sandbox_id,
            transfer_scope_id.to_string(),
            now()?,
        ],
    )?;
    Ok(changed == 1)
}

pub fn insert_restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    staging_dir: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "INSERT INTO sandbox_restore_transfer_scopes (
             sandbox_id, transfer_scope_id, execution_generation,
             activation_attempt_id, staging_dir, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(sandbox_id) DO NOTHING",
        params![
            sandbox_id,
            transfer_scope_id.to_string(),
            execution_generation.map(ExecutionGeneration::get),
            activation_attempt_id.map(|value| value.to_string()),
            staging_dir,
            now()?,
        ],
    )?;
    Ok(changed == 1)
}

pub(super) fn retire_restore_transfer_scope(conn: &Connection, sandbox_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1",
        params![sandbox_id],
    )?;
    Ok(())
}

pub fn retire_restore_transfer_scope_if_matches(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<bool> {
    let changed = conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1 AND transfer_scope_id = ?2",
        params![sandbox_id, transfer_scope_id.to_string()],
    )?;
    Ok(changed == 1)
}

pub fn canonical_restore_reclaim_attempt_is_current(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool> {
    let Some(row) = get_sandbox(conn, sandbox_id)? else {
        return Ok(false);
    };
    if !sandbox_is_retryable_interrupted_restore(&row) {
        return Ok(false);
    }
    Ok(
        restore_transfer_scope(conn, sandbox_id)?.is_some_and(|scope| {
            scope.transfer_scope_id == transfer_scope_id
                && scope.execution_generation == Some(execution_generation)
                && scope.activation_attempt_id == Some(activation_attempt_id)
        }),
    )
}
