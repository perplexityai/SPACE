use pplx_space_common_core::types::{
    SandboxActivationAttemptId, SandboxQuarantineNextAction, SandboxQuarantineReasonCode,
    SandboxQuarantineSource,
};
use rusqlite::{Connection, OptionalExtension, params, types::Type};

use crate::state::SandboxState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GuestProbeAuthority {
    pub(crate) state: SandboxState,
    pub(crate) node_execution_id: Option<SandboxActivationAttemptId>,
    pub(crate) runtime_pid: Option<u32>,
    pub(crate) vsock_uds_path: Option<String>,
    execution_retired: bool,
    state_changed_at: String,
}

pub(crate) fn load_probe_authority(
    conn: &Connection,
    id: &str,
) -> rusqlite::Result<Option<GuestProbeAuthority>> {
    conn.query_row(
        "SELECT state, node_execution_id, pid, vsock_uds_path, CAST(state_changed_at AS TEXT), node_execution_retired
         FROM sandboxes WHERE id = ?1",
        [id],
        |row| {
            Ok(GuestProbeAuthority {
                state: row.get::<_, String>(0)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
                })?,
                node_execution_id: row
                    .get::<_, Option<String>>(1)?
                    .map(|raw| {
                        raw.parse().map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?,
                runtime_pid: row.get(2)?,
                vsock_uds_path: row.get(3)?,
                state_changed_at: row.get(4)?,
                execution_retired: row.get(5)?,
            })
        },
    )
    .optional()
}

pub(crate) fn record_guest_probe(
    conn: &Connection,
    id: &str,
    authority: &GuestProbeAuthority,
    guest_failed: bool,
    observed_at: i64,
) -> rusqlite::Result<Option<i64>> {
    let tx = conn.unchecked_transaction()?;
    if load_probe_authority(&tx, id)?.as_ref() != Some(authority) {
        return Ok(None);
    }
    if authority.state != SandboxState::Started || authority.execution_retired || !guest_failed {
        tx.execute(
            "DELETE FROM sandbox_guest_probe_failures WHERE sandbox_id = ?1",
            [id],
        )?;
        tx.commit()?;
        return Ok(None);
    }
    tx.execute(
        "INSERT INTO sandbox_guest_probe_failures
         (sandbox_id, node_execution_id, runtime_pid, vsock_uds_path, first_failed_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(sandbox_id) DO UPDATE SET
             node_execution_id = excluded.node_execution_id,
             runtime_pid = excluded.runtime_pid,
             vsock_uds_path = excluded.vsock_uds_path,
             first_failed_at = CASE
                 WHEN node_execution_id IS excluded.node_execution_id
                  AND runtime_pid IS excluded.runtime_pid
                  AND vsock_uds_path IS excluded.vsock_uds_path
                 THEN first_failed_at ELSE excluded.first_failed_at END",
        params![
            id,
            authority.node_execution_id.map(|id| id.to_string()),
            authority.runtime_pid,
            authority.vsock_uds_path,
            observed_at,
        ],
    )?;
    let first_failed_at = tx.query_row(
        "SELECT first_failed_at FROM sandbox_guest_probe_failures WHERE sandbox_id = ?1",
        [id],
        |row| row.get(0),
    )?;
    tx.commit()?;
    Ok(Some(first_failed_at))
}

pub(crate) fn quarantine_unresponsive_guest(
    conn: &Connection,
    id: &str,
    authority: &GuestProbeAuthority,
) -> Result<bool, super::MarkQuarantinedError> {
    super::mark_quarantined_inner(
        conn,
        id,
        Some(SandboxState::Started),
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "guest agent unresponsive for at least 300 seconds while Firecracker is alive; sandbox quarantined, operator attention required",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
        Some(authority),
    )
}
