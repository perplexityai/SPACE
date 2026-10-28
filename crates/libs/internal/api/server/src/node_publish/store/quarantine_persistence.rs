use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxQuarantineHistoryAction, SandboxQuarantineNextAction},
};
use pplx_space_common_svc::sql::sandbox_quarantine::{
    quarantine_history_insert_columns, quarantine_state_conflict_assignments,
    quarantine_state_insert_columns,
};
use sqlx::{PgConnection, types::Json};

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordStateError {
    #[error("record quarantine state: {0}")]
    Upsert(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordHistoryError {
    #[error("record quarantine history: {0}")]
    Insert(#[source] sqlx::Error),
}

/// Persist quarantine state only for rows returned by the caller's lifecycle
/// CAS. Conflict handling locks quarantine rows while the sandbox locks remain
/// held. False means the supplied input produced no state rows.
pub(crate) async fn record_state(
    conn: &mut PgConnection,
    node_id: NodeId,
    updated_json: Json<serde_json::Value>,
) -> Result<bool, RecordStateError> {
    let state_columns = quarantine_state_insert_columns();
    let state_conflict_assignments = quarantine_state_conflict_assignments();
    let query = format!(
        "INSERT INTO sandbox_quarantine_state ({state_columns})
                     SELECT
                         input.sandbox_id,
                         input.quarantine_id,
                         NOW(),
                         NOW(),
                         input.from_lifecycle,
                         input.source,
                         input.reason_code,
                         NULLIF(BTRIM(input.failure_reason), ''),
                         jsonb_build_object('node_id', $3::TEXT),
                         $2,
                         $1
                     FROM jsonb_to_recordset($4::jsonb) AS input(
                         sandbox_id UUID,
                         from_lifecycle TEXT,
                         failure_reason TEXT,
                         source TEXT,
                         reason_code TEXT,
                         quarantine_id UUID
                     )
                     ON CONFLICT (sandbox_id) DO UPDATE SET {state_conflict_assignments}",
    );
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
        .bind(node_id.to_string())
        .bind(updated_json)
        .execute(&mut *conn)
        .await
        .map_err(RecordStateError::Upsert)?;
    Ok(result.rows_affected() > 0)
}

/// Append history for the same successful lifecycle CAS after state persistence.
/// Inserts take normal row/index locks and retain the caller's sandbox and
/// quarantine locks. False means the input produced no history rows.
pub(crate) async fn record_history(
    conn: &mut PgConnection,
    node_id: NodeId,
    updated_json: Json<serde_json::Value>,
) -> Result<bool, RecordHistoryError> {
    let history_columns = quarantine_history_insert_columns();
    let query = format!(
        "INSERT INTO sandbox_quarantine_history ({history_columns})
                     SELECT
                         input.history_id,
                         input.quarantine_id,
                         input.sandbox_id,
                         NOW(),
                         input.source,
                         $2,
                         input.reason_code,
                         NULLIF(BTRIM(input.failure_reason), ''),
                         $3,
                         jsonb_build_object('node_id', $1::TEXT)
                     FROM jsonb_to_recordset($4::jsonb) AS input(
                         sandbox_id UUID,
                         failure_reason TEXT,
                         source TEXT,
                         reason_code TEXT,
                         quarantine_id UUID,
                         history_id UUID
                     )",
    );
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id.to_string())
        .bind(SandboxQuarantineHistoryAction::EnteredQuarantine.as_str())
        .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
        .bind(updated_json)
        .execute(&mut *conn)
        .await
        .map_err(RecordHistoryError::Insert)?;
    Ok(result.rows_affected() > 0)
}
