use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::sql::sandbox_quarantine;
use sqlx::PgPool;

use crate::routes::debug::{
    response::{
        ActiveQuarantine, QUARANTINE_HISTORY_LIMIT, QuarantineHistoryEntry, QuarantineSection,
    },
    sources::begin_read_only,
};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<QuarantineSection, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let active = sandbox_quarantine::active_quarantine(&mut *tx, sandbox_id).await?;
    let history =
        sandbox_quarantine::quarantine_history(&mut *tx, sandbox_id, QUARANTINE_HISTORY_LIMIT)
            .await?;
    tx.commit().await?;
    Ok(QuarantineSection {
        quarantined: active.is_some(),
        active: active.map(|active| ActiveQuarantine {
            quarantine_id: active.quarantine_id,
            entered_at: active.entered_at,
            updated_at: active.updated_at,
            entered_from_lifecycle: active.entered_from_lifecycle,
            source: active.source,
            reason_code: active.reason_code,
            reason_message: active.reason_message,
            next_action: active.next_action,
            next_action_after: active.next_action_after,
            attempt_count: active.attempt_count,
            last_error: active.last_error,
            last_observed_node_id: active.last_observed_node_id,
        }),
        history: history
            .into_iter()
            .map(|entry| QuarantineHistoryEntry {
                id: entry.id,
                quarantine_id: entry.quarantine_id,
                occurred_at: entry.occurred_at,
                source: entry.source,
                action: entry.action,
                reason_code: entry.reason_code,
                message: entry.message,
                next_action: entry.next_action,
                error: entry.error,
            })
            .collect(),
    })
}
