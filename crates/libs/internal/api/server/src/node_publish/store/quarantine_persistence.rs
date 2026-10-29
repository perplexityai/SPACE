use pplx_space_common_core::{
    sql,
    types::{
        NodeId, SandboxId, SandboxQuarantineHistoryAction, SandboxQuarantineNextAction,
        SandboxQuarantineReasonCode, SandboxQuarantineSource,
    },
};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct QuarantineStateRecord<'a> {
    pub(super) sandbox_id: SandboxId,
    pub(super) from_lifecycle: &'a str,
    pub(super) failure_reason: Option<&'a str>,
    pub(super) source: SandboxQuarantineSource,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) quarantine_id: Uuid,
}

#[derive(Debug)]
pub(crate) struct QuarantineHistoryRecord<'a> {
    pub(super) sandbox_id: SandboxId,
    pub(super) failure_reason: Option<&'a str>,
    pub(super) source: SandboxQuarantineSource,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) quarantine_id: Uuid,
    pub(super) history_id: Uuid,
}

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
    records: &[QuarantineStateRecord<'_>],
) -> Result<bool, RecordStateError> {
    if records.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (
                         sandbox_id, quarantine_id, entered_at, updated_at,
                         entered_from_lifecycle, source, reason_code, reason_message,
                         evidence, next_action, last_observed_node_id
                     )
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
                     FROM UNNEST(
                         $4::UUID[], $5::TEXT[], $6::TEXT[], $7::TEXT[], $8::TEXT[], $9::UUID[]
                     ) AS input(
                         sandbox_id, from_lifecycle, failure_reason, source, reason_code,
                         quarantine_id
                     )
                     ON CONFLICT (sandbox_id) DO UPDATE SET
                         quarantine_id = EXCLUDED.quarantine_id,
                         entered_at = EXCLUDED.entered_at,
                         updated_at = EXCLUDED.updated_at,
                         entered_from_lifecycle = EXCLUDED.entered_from_lifecycle,
                         source = EXCLUDED.source,
                         reason_code = EXCLUDED.reason_code,
                         reason_message = EXCLUDED.reason_message,
                         evidence = EXCLUDED.evidence,
                         next_action = EXCLUDED.next_action,
                         next_action_after = NULL,
                         next_action_owner = NULL,
                         attempt_count = 0,
                         claim_token = NULL,
                         claim_expires_at = NULL,
                         last_error = NULL,
                         last_observed_node_id = EXCLUDED.last_observed_node_id",
    ))
    .bind(node_id)
    .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
    .bind(node_id.to_string())
    .bind(
        records
            .iter()
            .map(|record| record.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.from_lifecycle)
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.failure_reason)
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.source.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.reason_code.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.quarantine_id)
            .collect::<Vec<_>>(),
    )
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
    records: &[QuarantineHistoryRecord<'_>],
) -> Result<bool, RecordHistoryError> {
    if records.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history (
                         id, quarantine_id, sandbox_id, occurred_at, source,
                         action, reason_code, message, next_action, evidence
                     )
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
                     FROM UNNEST(
                         $4::UUID[], $5::TEXT[], $6::TEXT[], $7::TEXT[], $8::UUID[], $9::UUID[]
                     ) AS input(
                         sandbox_id, failure_reason, source, reason_code, quarantine_id, history_id
                     )",
    ))
    .bind(node_id.to_string())
    .bind(SandboxQuarantineHistoryAction::EnteredQuarantine.as_str())
    .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
    .bind(
        records
            .iter()
            .map(|record| record.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.failure_reason)
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.source.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.reason_code.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.quarantine_id)
            .collect::<Vec<_>>(),
    )
    .bind(
        records
            .iter()
            .map(|record| record.history_id)
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(RecordHistoryError::Insert)?;
    Ok(result.rows_affected() > 0)
}
