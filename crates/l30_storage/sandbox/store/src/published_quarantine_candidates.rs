//! Which of a node's published quarantine reports name a sandbox that the
//! quarantine projection may still move.

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};
use uuid::Uuid;

use crate::SandboxStoreError;

/// One published quarantine report joined to the row it names.
#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct PublishedQuarantineCandidate {
    /// `sandboxes.id`.
    pub id: Uuid,
    /// The row's stored lifecycle, the one the projection would leave.
    pub lifecycle: String,
    pub failure_reason: Option<String>,
    pub source: String,
    pub reason_code: String,
    pub quarantine_id: Uuid,
    pub history_id: Uuid,
}

/// The reports in `inputs` (a JSON array of `{sandbox_id, failure_reason,
/// source, reason_code, quarantine_id, history_id}`) whose sandbox is active
/// on `node_id` and neither quarantined nor deleted.
///
/// A plain read that takes no lock: the projection's own write,
/// [`crate::ingest::mark_published_quarantined`], re-checks the row, and the
/// caller keeps only the ids it moved.
pub async fn list(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    inputs: Json<JsonValue>,
) -> Result<Vec<PublishedQuarantineCandidate>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, PublishedQuarantineCandidate>(sql!(
        "SELECT
                     s.id,
                     s.lifecycle,
                     input.failure_reason,
                     input.source,
                     input.reason_code,
                     input.quarantine_id,
                     input.history_id
                 FROM sandboxes s
                 JOIN jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     failure_reason TEXT,
                     source TEXT,
                     reason_code TEXT,
                     quarantine_id UUID,
                     history_id UUID
                 ) ON input.sandbox_id = s.id
                 WHERE s.active_node_id = $1
                   AND LOWER(TRIM(s.lifecycle)) NOT IN ('quarantined', 'deleted')"
    ))
    .bind(node_id)
    .bind(inputs)
    .fetch_all(executor)
    .await?)
}
