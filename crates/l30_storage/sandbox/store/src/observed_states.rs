//! `sandbox_observed_states`: the last state a node published for each
//! sandbox it reports, overlaid on the row's lifecycle by status reads.
//!
//! Node publications write these rows, and not every writer is in this crate
//! yet. [`crate::delete::delete_with_dependents`] removes them with the row.
//! [`delete`] is the unguarded delete, run on the executor its caller passes.
//! [`newest`] and [`window`] are unlocked reads of the rows' ids, as is
//! [`rowless_sandbox_ids`]; [`last_published_execution`] is an unlocked read
//! of one row's published execution.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::types::{Json, JsonValue};
use uuid::Uuid;

use crate::SandboxStoreError;

/// Drop the sandbox's observed state, whatever it says (an operation step):
/// for a path that has just settled the row's lifecycle itself and must not
/// leave a node's stale report overlaying it.
/// The sandbox's `sandbox_snapshot_inventory` rows go with it, through that
/// table's foreign-key cascade.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// The newest observed-row id; `None` when the table is empty.
pub async fn newest(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<Uuid>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id
             FROM sandbox_observed_states
             ORDER BY sandbox_id DESC
             LIMIT 1"
    ))
    .fetch_optional(executor)
    .await?)
}

/// Up to `limit` observed-row ids in `(after, through]`, ascending, from a
/// primary-key range scan. The ordering is a resume position, not a fairness
/// policy.
pub async fn window(
    executor: impl sqlx::PgExecutor<'_>,
    after: Uuid,
    through: Uuid,
    limit: i64,
) -> Result<Vec<Uuid>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id
             FROM sandbox_observed_states
             WHERE sandbox_id > $1
               AND sandbox_id <= $2
             ORDER BY sandbox_id
             LIMIT $3"
    ))
    .bind(after)
    .bind(through)
    .bind(limit)
    .fetch_all(executor)
    .await?)
}

/// The sandboxes nodes report that have no `sandboxes` row.
pub async fn rowless_sandbox_ids(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "SELECT sos.sandbox_id
                 FROM sandbox_observed_states sos
                 WHERE NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = sos.sandbox_id)"
    ))
    .fetch_all(executor)
    .await
}

/// The execution a sandbox's node last published, as evidence of what it
/// runs. `subject` digests the record and the sandbox's last lifecycle
/// change, so one stopped period names one subject; `local_artifact_id` is
/// always `None` here.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct PublishedExecution {
    pub subject: String,
    pub local_artifact_id: Option<LocalArtifactId>,
    pub execution_metadata: Option<Json<JsonValue>>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

/// The execution `sandbox_id`'s node last published; `None` when nothing is
/// published or the publish carried no execution.
pub async fn last_published_execution(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<PublishedExecution>, sqlx::Error> {
    sqlx::query_as::<_, PublishedExecution>(sql!(
        "SELECT md5(concat_ws('/', s.lifecycle_changed_at::text, o.execution_metadata::text,
                                  o.runtime, o.runtime_version, o.state_format_name,
                                  o.state_format_version)) AS subject,
                    NULL::uuid AS local_artifact_id, o.execution_metadata, o.runtime,
                    o.runtime_version, o.state_format_name, o.state_format_version
               FROM sandbox_observed_states o
               JOIN sandboxes s ON s.id = o.sandbox_id
              WHERE o.sandbox_id = $1
                -- A publish that carried no execution is no evidence: it
                -- isn't a record from before execution metadata existed.
                AND (o.execution_metadata IS NOT NULL
                     OR o.runtime_version IS NOT NULL
                     OR o.state_format_version IS NOT NULL)"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}
