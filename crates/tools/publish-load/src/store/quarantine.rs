use anyhow::{Context, Result, ensure};
use pplx_space_common_core::types::{
    NodeId, SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction,
    SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

/// Check a node-authored hold and its single history entry without row locks.
/// Missing state or inconsistent lifecycle/history fails the synthetic run;
/// the returned episode identity lets the caller check repeat publication.
pub async fn verify_quarantine(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> Result<Uuid> {
    let (quarantine_id, matches) = sqlx::query_as::<_, (Uuid, bool)>(sql!(
        "SELECT qs.quarantine_id,
                s.lifecycle = $3 AND s.active_node_id = $2
                AND qs.source = $4 AND qs.reason_code = $5
                AND qs.reason_message = $6 AND qs.next_action = $7
                AND qs.last_observed_node_id = $2
                AND (SELECT COUNT(*) FROM sandbox_quarantine_history qh
                     WHERE qh.sandbox_id = s.id) = $8
                AND EXISTS (
                    SELECT 1 FROM sandbox_quarantine_history qh
                    WHERE qh.sandbox_id = s.id AND qh.quarantine_id = qs.quarantine_id
                      AND qh.action = $9 AND qh.source = qs.source
                      AND qh.reason_code = qs.reason_code AND qh.message = qs.reason_message
                )
         FROM sandboxes s JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(SandboxLifecycle::Quarantined.as_str())
    .bind(SandboxQuarantineSource::NodeStatePublish.as_str())
    .bind(SandboxQuarantineReasonCode::NodePublishedQuarantined.as_str())
    .bind("synthetic quarantine evidence")
    .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
    .bind(1_i64)
    .bind(SandboxQuarantineHistoryAction::EnteredQuarantine.as_str())
    .fetch_optional(pool)
    .await?
    .context("node-authored quarantine missing")?;
    ensure!(matches, "quarantine lifecycle, state, or history differed");
    Ok(quarantine_id)
}
