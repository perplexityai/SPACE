//! The internal API's consumption of a node's activation-rollback receipts
//! during a node-state publication: the lock of the rows each receipt still
//! fences, and the retire of the execution whose activation the node rolled
//! back.
//!
//! The caller keeps the publication's transaction, the parse of each
//! candidate's stored text, the restore-retirement decision, and the
//! lifecycle convergence that follows each retire
//! ([`crate::quarantine::converge_activation_rollback`]).

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{
    SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId, SandboxTransitionReceiptKind,
};
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;
use sqlx::types::{Json, JsonValue};

/// One receipt [`lock_activation_rollback_candidates`] selected, with the
/// stored text the caller parses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationRollbackCandidateRow {
    pub sandbox_id: SandboxId,
    pub receipt_id: SandboxCleanupEpisodeId,
    pub execution_generation: i64,
    pub activation_attempt_id: SandboxActivationAttemptId,
    /// The receipt's raw target state.
    pub target_state: String,
    /// The receipt's raw reason code.
    pub reason_code: String,
    /// The stored `sandboxes.lifecycle`, trimmed and lower-cased.
    pub prior_lifecycle: String,
    pub failure_message: Option<String>,
}

/// [`lock_activation_rollback_candidates`]'s statement.
const LOCK_ACTIVATION_ROLLBACK_CANDIDATES: &str = sql!(
    "SELECT s.id,
            input.transition_receipt_id,
            input.transition_receipt_execution_generation,
            input.transition_receipt_activation_attempt_id,
            input.transition_receipt_target_state,
            input.transition_receipt_reason_code,
            LOWER(TRIM(s.lifecycle)),
            input.failure_reason
       FROM sandboxes s
       JOIN jsonb_to_recordset($2::jsonb) AS input(
            sandbox_id UUID,
            published_lifecycle TEXT,
            transition_receipt_id UUID,
            transition_receipt_kind TEXT,
            transition_receipt_target_state TEXT,
            transition_receipt_execution_generation BIGINT,
            transition_receipt_reason_code TEXT,
            transition_receipt_recovery_policy TEXT,
            transition_receipt_activation_attempt_id UUID,
            failure_reason TEXT,
            current_local_artifact_id UUID,
            current_local_artifact_kind TEXT,
            current_local_artifact_origin TEXT,
            execution_generation BIGINT,
            pod_uid TEXT
       )
         ON input.sandbox_id = s.id
       JOIN sandbox_k8s_executions execution
         ON execution.sandbox_id = s.id
        AND execution.active
        AND execution.generation = input.transition_receipt_execution_generation
        AND execution.activation_attempt_id = input.transition_receipt_activation_attempt_id
      WHERE s.active_node_id = $1
        AND input.transition_receipt_kind = $3
        AND input.published_lifecycle = input.transition_receipt_target_state
        AND input.transition_receipt_target_state IN ('created', 'stopped', 'suspended')
        AND LOWER(TRIM(s.lifecycle)) NOT IN (
            'quarantined', 'deleted', 'deleting'
        )
      FOR UPDATE OF s, execution"
);

/// Lock, `FOR UPDATE OF s, execution`, each sandbox active on `node_id` and
/// its active execution, for the receipts in `rows` of `receipt_kind` whose
/// published lifecycle is their target (`created`, `stopped` or
/// `suspended`), whose execution generation and activation attempt are both
/// the active execution's, and whose sandbox's trimmed, lower-cased
/// lifecycle is not `quarantined`, `deleted` or `deleting`. Generation alone
/// is not a freshness fence: retries reuse it.
///
/// `rows` is a JSON array of transition-receipt records, in the shape of
/// [`crate::ingest::TRANSITION_RECEIPT_RECORD_COLUMNS`], which this
/// statement spells out: change them together. The lock lasts until the
/// caller's transaction ends.
pub async fn lock_activation_rollback_candidates(
    conn: &mut PgConnection,
    node_id: NodeId,
    rows: Json<JsonValue>,
    receipt_kind: SandboxTransitionReceiptKind,
) -> Result<Vec<ActivationRollbackCandidateRow>, sqlx::Error> {
    type CandidateRow = (
        SandboxId,
        SandboxCleanupEpisodeId,
        i64,
        SandboxActivationAttemptId,
        String,
        String,
        String,
        Option<String>,
    );
    let rows: Vec<CandidateRow> = sqlx::query_as(LOCK_ACTIVATION_ROLLBACK_CANDIDATES)
        .bind(node_id)
        .bind(rows)
        .bind(receipt_kind.as_str())
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                sandbox_id,
                receipt_id,
                execution_generation,
                activation_attempt_id,
                target_state,
                reason_code,
                prior_lifecycle,
                failure_message,
            )| ActivationRollbackCandidateRow {
                sandbox_id,
                receipt_id,
                execution_generation,
                activation_attempt_id,
                target_state,
                reason_code,
                prior_lifecycle,
                failure_message,
            },
        )
        .collect())
}

/// Retire the active execution at `generation` holding
/// `activation_attempt_id` as `execution_state` (the caller's choice between
/// `replaced` and `suspended`). **Operation step**: the receipt's fence.
/// `false` when the active execution is no longer that generation and
/// attempt, and nothing changes.
pub async fn retire_rolled_back_execution(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    execution_state: &str,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET active = FALSE,
                    state = $3,
                    pod_ip = NULL,
                    terminated_at = COALESCE(terminated_at, NOW()),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND activation_attempt_id = $4
                AND active"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(execution_state)
    .bind(activation_attempt_id)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

#[cfg(test)]
mod tests;
