//! `public_operation_events`: each operation's numbered event log.

use pplx_space_accounts::OrgId;
use pplx_space_public_api::{OperationId, PublicOperationEventType};
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, PgExecutor, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

/// One event of an operation.
#[derive(Debug, FromRow)]
pub struct PublicOperationEventRow {
    pub operation_id: Uuid,
    pub sequence: i64,
    pub event_type: String,
    pub event_json: Json<serde_json::Value>,
    pub created_at: OffsetDateTime,
}

/// Where a page of [`list_for_targets_after`] resumes: the last event the
/// previous page returned.
#[derive(Clone, Copy, Debug)]
pub struct EventCursor {
    pub created_at: OffsetDateTime,
    pub operation_id: Uuid,
    pub sequence: i64,
}

/// Up to `limit` events of `operation_id` after `after_sequence`, in order.
pub async fn list_after_sequence(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
    after_sequence: Option<i64>,
    limit: i64,
) -> Result<Vec<PublicOperationEventRow>, sqlx::Error> {
    sqlx::query_as::<_, PublicOperationEventRow>(sql!(
        "SELECT e.operation_id, e.sequence, e.event_type, e.event_json, e.created_at
         FROM public_operation_events e
         WHERE e.operation_id = $1
           AND e.sequence > COALESCE($2, 0)
         ORDER BY e.sequence ASC
         LIMIT $3"
    ))
    .bind(operation_id.as_uuid())
    .bind(after_sequence)
    .bind(limit)
    .fetch_all(executor)
    .await
}

/// Up to `limit` events after `after` of the operations of `org_id` that
/// target a sandbox named by any of `target_ids`, oldest first.
pub async fn list_for_targets_after(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    target_ids: &[String],
    after: EventCursor,
    limit: i64,
) -> Result<Vec<PublicOperationEventRow>, sqlx::Error> {
    sqlx::query_as::<_, PublicOperationEventRow>(sql!(
        "SELECT e.operation_id, e.sequence, e.event_type, e.event_json, e.created_at
         FROM public_operation_events e
         JOIN public_operations o ON o.operation_id = e.operation_id
         WHERE o.org_id = $1
           AND o.target_type = 'sandbox'
           AND o.target_id = ANY($2)
           AND (e.created_at, e.operation_id, e.sequence) > ($3, $4, $5)
         ORDER BY e.created_at ASC, e.operation_id ASC, e.sequence ASC
         LIMIT $6"
    ))
    .bind(org_id)
    .bind(target_ids)
    .bind(after.created_at)
    .bind(after.operation_id)
    .bind(after.sequence)
    .bind(limit)
    .fetch_all(executor)
    .await
}

/// The `limit` most recent events of the operations of `org_id` that target
/// a sandbox named by any of `target_ids`, oldest first.
pub async fn list_recent_for_targets(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    target_ids: &[String],
    limit: i64,
) -> Result<Vec<PublicOperationEventRow>, sqlx::Error> {
    sqlx::query_as::<_, PublicOperationEventRow>(sql!(
        "SELECT operation_id, sequence, event_type, event_json, created_at
         FROM (
             SELECT e.operation_id, e.sequence, e.event_type, e.event_json, e.created_at
             FROM public_operation_events e
             JOIN public_operations o ON o.operation_id = e.operation_id
             WHERE o.org_id = $1
               AND o.target_type = 'sandbox'
               AND o.target_id = ANY($2)
             ORDER BY e.created_at DESC, e.operation_id DESC, e.sequence DESC
             LIMIT $3
         ) recent
         ORDER BY created_at ASC, operation_id ASC, sequence ASC"
    ))
    .bind(org_id)
    .bind(target_ids)
    .bind(limit)
    .fetch_all(executor)
    .await
}

/// Append an event to `operation_id`'s log, returning its sequence number.
pub async fn append(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
    event_type: PublicOperationEventType,
    event_json: serde_json::Value,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "WITH next_sequence AS (
             SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
             FROM public_operation_events
             WHERE operation_id = $1
         )
         INSERT INTO public_operation_events (
             operation_id,
             sequence,
             event_type,
             event_json
         )
         SELECT $1, sequence, $2, $3
         FROM next_sequence
         RETURNING sequence"
    ))
    .bind(operation_id.as_uuid())
    .bind(event_type.as_str())
    .bind(Json(event_json))
    .fetch_one(executor)
    .await
}

/// Append one `event_type` event to each operation of `operation_ids`, with
/// the event of the same index in `event_jsons`.
pub async fn append_to_each(
    executor: impl PgExecutor<'_>,
    operation_ids: &[Uuid],
    event_jsons: &[Json<serde_json::Value>],
    event_type: PublicOperationEventType,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO public_operation_events (operation_id, sequence, event_type, event_json)
         SELECT expired.operation_id,
                COALESCE(
                    (SELECT MAX(event.sequence)
                     FROM public_operation_events event
                     WHERE event.operation_id = expired.operation_id),
                    0
                ) + 1,
                $3,
                expired.event_json
         FROM UNNEST($1::UUID[], $2::JSONB[]) AS expired(operation_id, event_json)"
    ))
    .bind(operation_ids)
    .bind(event_jsons)
    .bind(event_type.as_str())
    .execute(executor)
    .await?;
    Ok(())
}
