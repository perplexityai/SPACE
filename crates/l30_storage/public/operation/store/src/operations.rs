//! `public_operations` rows: reads, inserts (one of them with its first
//! event), deletes, and the claim, cancel and expiry writes whose rows the
//! caller's transaction holds.

use std::sync::LazyLock;

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicOperationEventType,
    PublicSandboxId,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, PgExecutor, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

/// The columns every whole-operation read selects, in [`PublicOperationRow`]'s
/// shape. Named rather than `*`: adding or dropping a `public_operations`
/// column changes what `SELECT *` returns, and every pooled connection that
/// already ran one then fails it with `0A000` ("cached plan must not change
/// result type") until the pool recycles that connection. A new column a read
/// needs is selected here and decoded in [`PublicOperationRow`]; a record
/// that exposes it also needs the field and its line in that record's
/// conversion. The two admin reads ([`fetch_admin`], [`list_admin`]) also
/// select the payload's `requested_by_user_id` and the joined actor's email,
/// with every column qualified by its table, so they spell this list out and
/// need the new column added there too.
const PUBLIC_OPERATION_COLUMNS: &str = "operation_id,
       org_id,
       actor_user_id,
       api_product,
       kind,
       target_type,
       target_id,
       state,
       progress_message,
       terminal_error_code,
       terminal_error_json,
       result_json,
       attempts,
       requested_at,
       next_attempt_at,
       created_at,
       started_at,
       completed_at,
       updated_at";

/// `sql` with `{public_operation_columns}` replaced by
/// [`PUBLIC_OPERATION_COLUMNS`], the one fragment these statements take (the
/// data-store skill's exception for one row type's column list). Each
/// statement is otherwise complete, built once at its query site, and binds
/// every value.
fn public_operation_query(sql: &str) -> String {
    sql.replace("{public_operation_columns}", PUBLIC_OPERATION_COLUMNS)
}

/// One whole operation, as [`PUBLIC_OPERATION_COLUMNS`] selects it. Both the
/// public and the admin record are built from it.
#[derive(Debug, FromRow)]
pub struct PublicOperationRow {
    pub operation_id: Uuid,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: String,
    pub kind: String,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub state: String,
    pub progress_message: String,
    pub terminal_error_code: Option<String>,
    pub terminal_error_json: Option<Json<serde_json::Value>>,
    pub result_json: Option<Json<serde_json::Value>>,
    pub attempts: i32,
    pub requested_at: OffsetDateTime,
    pub next_attempt_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub completed_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

/// [`PublicOperationRow`] plus the columns only the admin record has: the
/// payload's `requested_by_user_id` and the joined actor's email.
#[derive(Debug, FromRow)]
pub struct AdminPublicOperationRow {
    #[sqlx(flatten)]
    pub operation: PublicOperationRow,
    pub requested_by_user_id: Option<String>,
    pub actor_email: Option<String>,
}

/// Whole seconds left before `hard_deadline_secs` after `operation_id` was
/// created, by the database clock, and never below zero.
pub async fn remaining_deadline_secs(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
    hard_deadline_secs: i64,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT GREATEST(
             0,
             FLOOR(EXTRACT(EPOCH FROM (
                 created_at + ($2::BIGINT * INTERVAL '1 second') - NOW()
             )))::BIGINT
         )
         FROM public_operations
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(hard_deadline_secs)
    .fetch_optional(executor)
    .await
}

/// Seconds since `operation_id` was first claimed, by the database clock.
/// `None` when it does not exist or has never been claimed.
pub async fn secs_since_first_claim(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
) -> Result<Option<f64>, sqlx::Error> {
    let seconds = sqlx::query_scalar::<_, Option<f64>>(sql!(
        "SELECT EXTRACT(EPOCH FROM NOW() - started_at)::DOUBLE PRECISION
         FROM public_operations
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_optional(executor)
    .await?;
    Ok(seconds.flatten())
}

/// The operation `operation_id` of `org_id`.
pub async fn fetch(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<PublicOperationRow>, sqlx::Error> {
    static FETCH_OPERATION_SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(public_operation_query(
            "SELECT {public_operation_columns}
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2"
        ))
    });
    sqlx::query_as::<_, PublicOperationRow>(sqlx::AssertSqlSafe(FETCH_OPERATION_SQL.as_str()))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .fetch_optional(executor)
        .await
}

/// Whether `operation_id` of `org_id` is in the owner scope of `subject_id`:
/// a native operation that subject requested, or, with no subject, one with
/// no actor or a local actor.
pub async fn in_owner_scope(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    subject_id: Option<UserId>,
    operation_id: OperationId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(
             SELECT 1 FROM public_operations op
             LEFT JOIN users actor ON actor.id = op.actor_user_id
             WHERE op.org_id = $1 AND op.operation_id = $2
               AND ($3::UUID IS NULL OR op.api_product = 'native')
               AND (($3::UUID IS NOT NULL AND op.actor_user_id = $3)
                 OR ($3::UUID IS NULL AND
                     (op.actor_user_id IS NULL OR actor.owner_source = 'local')))
         )"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .bind(subject_id)
    .fetch_one(executor)
    .await
}

/// What a fork operation's outcome is read from.
#[derive(Debug, FromRow)]
pub struct ForkOutcomeRow {
    pub state: String,
    pub result_json: Option<Json<serde_json::Value>>,
    pub terminal_error_code: Option<String>,
    pub terminal_error_json: Option<Json<serde_json::Value>>,
}

/// The state, result and terminal error of the fork operation `operation_id`.
pub async fn fork_outcome(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<ForkOutcomeRow>, sqlx::Error> {
    sqlx::query_as::<_, ForkOutcomeRow>(sql!(
        "SELECT state, result_json, terminal_error_code, terminal_error_json
         FROM public_operations
         WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_optional(executor)
    .await
}

/// The payload of the fork operation `operation_id`.
pub async fn fork_payload(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<Json<serde_json::Value>>, sqlx::Error> {
    sqlx::query_scalar::<_, Json<serde_json::Value>>(sql!(
        "SELECT payload_json
         FROM public_operations
         WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_optional(executor)
    .await
}

/// The state and payload of a fork operation.
#[derive(Debug, FromRow)]
pub struct ForkStateRow {
    pub state: String,
    pub payload_json: Json<serde_json::Value>,
}

/// The state and payload of the fork operation `operation_id`.
pub async fn fork_state(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<ForkStateRow>, sqlx::Error> {
    sqlx::query_as::<_, ForkStateRow>(sql!(
        "SELECT state, payload_json
         FROM public_operations
         WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_optional(executor)
    .await
}

/// A queued or running fork of a source sandbox.
#[derive(Debug, FromRow)]
pub struct ActiveForkRow {
    pub operation_id: Uuid,
    pub payload_json: Json<serde_json::Value>,
}

/// The queued or running forks of `source_sandbox_id` in `org_id`, oldest
/// first. Forks of one source name it by either id form.
pub async fn active_forks_of_source(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    source_sandbox_id: SandboxId,
) -> Result<Vec<ActiveForkRow>, sqlx::Error> {
    sqlx::query_as::<_, ActiveForkRow>(sql!(
        "SELECT operation_id, payload_json
         FROM public_operations
         WHERE org_id = $1
           AND kind = 'fork_sandbox'
           AND target_id IN ($2, $5)
           AND state IN ($3, $4)
         ORDER BY created_at, operation_id"
    ))
    .bind(org_id)
    .bind(source_sandbox_id.to_string())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .bind(PublicSandboxId::from(source_sandbox_id).to_string())
    .fetch_all(executor)
    .await
}

/// [`fetch`] for an admin, in any org, as an [`AdminPublicOperationRow`].
///
/// The admin reads spell the column list out rather than take
/// [`PUBLIC_OPERATION_COLUMNS`], whose fragment may only be a statement's
/// whole `SELECT` list.
pub async fn fetch_admin(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
) -> Result<Option<AdminPublicOperationRow>, sqlx::Error> {
    sqlx::query_as::<_, AdminPublicOperationRow>(sql!(
        "SELECT op.operation_id,
                    op.org_id,
                    op.actor_user_id,
                    op.api_product,
                    op.kind,
                    op.target_type,
                    op.target_id,
                    op.state,
                    op.progress_message,
                    op.terminal_error_code,
                    op.terminal_error_json,
                    op.result_json,
                    op.attempts,
                    op.requested_at,
                    op.next_attempt_at,
                    op.created_at,
                    op.started_at,
                    op.completed_at,
                    op.updated_at,
                    CASE WHEN op.kind = $2
                         THEN op.payload_json ->> 'requested_by_user_id'
                    END AS requested_by_user_id,
                    actor.email AS actor_email
           FROM public_operations op
           LEFT JOIN users actor ON actor.id = op.actor_user_id
          WHERE op.operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(OperationKind::CreateSandbox.as_str())
    .fetch_optional(executor)
    .await
}

/// The filters of [`list_admin`]. `None` matches every value.
#[derive(Debug)]
pub struct AdminOperationFilter<'a> {
    pub org_id: Option<OrgId>,
    pub state: Option<&'a OperationState>,
    pub kind: Option<OperationKind>,
    pub target_type: Option<&'a OperationTargetType>,
    /// Matches an operation whose `target_id` is any of these.
    pub target_ids: Option<Vec<String>>,
    pub limit: i64,
}

/// The newest operations matching `filter`, in any org, as
/// [`AdminPublicOperationRow`]s.
pub async fn list_admin(
    executor: impl PgExecutor<'_>,
    filter: AdminOperationFilter<'_>,
) -> Result<Vec<AdminPublicOperationRow>, sqlx::Error> {
    sqlx::query_as::<_, AdminPublicOperationRow>(sql!(
        "SELECT op.operation_id,
                    op.org_id,
                    op.actor_user_id,
                    op.api_product,
                    op.kind,
                    op.target_type,
                    op.target_id,
                    op.state,
                    op.progress_message,
                    op.terminal_error_code,
                    op.terminal_error_json,
                    op.result_json,
                    op.attempts,
                    op.requested_at,
                    op.next_attempt_at,
                    op.created_at,
                    op.started_at,
                    op.completed_at,
                    op.updated_at,
                    CASE WHEN op.kind = $7
                         THEN op.payload_json ->> 'requested_by_user_id'
                    END AS requested_by_user_id,
                    actor.email AS actor_email
           FROM public_operations op
           LEFT JOIN users actor ON actor.id = op.actor_user_id
          WHERE ($1::UUID IS NULL OR op.org_id = $1)
            AND ($2::TEXT IS NULL OR op.state = $2)
            AND ($3::TEXT IS NULL OR op.kind = $3)
            AND ($4::TEXT IS NULL OR op.target_type = $4)
            AND ($5::TEXT[] IS NULL OR op.target_id = ANY($5))
          ORDER BY op.created_at DESC, op.operation_id DESC
          LIMIT $6"
    ))
    .bind(filter.org_id)
    .bind(filter.state.map(OperationState::as_str))
    .bind(filter.kind.map(OperationKind::as_str))
    .bind(filter.target_type.map(OperationTargetType::as_str))
    .bind(filter.target_ids)
    .bind(filter.limit)
    .bind(OperationKind::CreateSandbox.as_str())
    .fetch_all(executor)
    .await
}

/// Whether `operation_id` exists in `org_id`.
pub async fn exists_in_org(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(
             SELECT 1
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2
         )"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_one(executor)
    .await
}

/// Whether `operation_id` can still run: it exists and is queued or running.
pub async fn is_active(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM public_operations
             WHERE operation_id = $1
               AND state IN ('queued', 'running')
         )"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(executor)
    .await
}

/// The target of `operation_id` when it is a `kind` operation on a
/// `target_type` target.
pub async fn target_id(
    executor: impl PgExecutor<'_>,
    operation_id: Uuid,
    kind: OperationKind,
    target_type: OperationTargetType,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sql!(
        "SELECT target_id
         FROM public_operations
         WHERE operation_id = $1
           AND kind = $2
           AND target_type = $3"
    ))
    .bind(operation_id)
    .bind(kind.as_str())
    .bind(target_type.as_str())
    .fetch_optional(executor)
    .await
}

/// The earliest queued or running `kind` operation on the sandbox
/// `target_id` of `org_id`.
pub async fn earliest_active_on_sandbox(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id
           FROM public_operations
          WHERE org_id = $1
            AND kind = $3
            AND target_type = 'sandbox'
            AND target_id = $2
            AND state IN ('queued', 'running')
          ORDER BY requested_at, operation_id
          LIMIT 1"
    ))
    .bind(org_id)
    .bind(target_id)
    .bind(kind.as_str())
    .fetch_optional(executor)
    .await
}

/// `kinds` as the labels a statement binds.
fn kind_labels(kinds: &[OperationKind]) -> Vec<&'static str> {
    kinds.iter().map(|kind| kind.as_str()).collect()
}

/// Whether an operation on the sandbox `target_id` of `org_id` other than
/// `operation_id`, and of no kind in `outside_queue_kinds`, is queued or
/// running.
pub async fn other_active_on_sandbox(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    target_id: &str,
    outside_queue_kinds: &[OperationKind],
    operation_id: OperationId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM public_operations
              WHERE org_id = $1
                AND target_type = $2
                AND target_id = $3
                AND kind <> ALL($4)
                AND state IN ($5, $6)
                AND operation_id <> $7
         )"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(kind_labels(outside_queue_kinds))
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .bind(operation_id.as_uuid())
    .fetch_one(executor)
    .await
}

/// The creates of `target`, a sandbox of `org_id`, that are still queued or
/// running.
pub async fn sandbox_creates_in_flight(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    target: PublicSandboxId,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id FROM public_operations
         WHERE org_id = $1
           AND target_type = $2
           AND target_id = $3
           AND kind = $4
           AND state IN ($5, $6)"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target.to_string())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_all(executor)
    .await
}

/// The earliest delete of `create_id`'s sandbox requested after it that is
/// still queued or running, if `create_id` is a sandbox create.
pub async fn superseding_delete(
    executor: impl PgExecutor<'_>,
    create_id: OperationId,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT delete_op.operation_id
         FROM public_operations create_op
         JOIN public_operations delete_op
           ON delete_op.org_id = create_op.org_id
          AND delete_op.target_type = create_op.target_type
          AND delete_op.target_id = create_op.target_id
         WHERE create_op.operation_id = $1
           AND create_op.kind = $2
           AND create_op.target_type = $3
           AND delete_op.kind = $4
           AND delete_op.state IN ($5, $6)
           AND (delete_op.requested_at, delete_op.operation_id)
               > (create_op.requested_at, create_op.operation_id)
         ORDER BY delete_op.requested_at, delete_op.operation_id
         LIMIT 1"
    ))
    .bind(create_id.as_uuid())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(OperationKind::DeleteSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_optional(executor)
    .await
}

/// An active operation [`joinable_on_sandbox`] found.
#[derive(Debug)]
pub struct JoinableOperation {
    pub operation_id: Uuid,
    /// No actor: the system's, as a command or process session queues it.
    pub system: bool,
}

/// The earliest queued or running `kind` operation on the sandbox
/// `target_id` of `org_id` that no later operation in the sandbox's queue
/// (of another kind, outside `outside_queue_kinds`) is queued or running
/// behind. With `joiner`, only a native one that `joiner` requested, or the
/// system's while it is still queued.
pub async fn joinable_on_sandbox(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
    joiner: Option<UserId>,
    outside_queue_kinds: &[OperationKind],
) -> Result<Option<JoinableOperation>, sqlx::Error> {
    let joinable = sqlx::query_as::<_, (Uuid, bool)>(sql!(
        "SELECT active.operation_id, active.actor_user_id IS NULL
           FROM public_operations active
          WHERE active.org_id = $1
            AND active.kind = $3
            AND active.target_type = 'sandbox'
            AND active.target_id = $2
            AND active.state IN ('queued', 'running')
            AND ($4::UUID IS NULL
                 OR (active.api_product = 'native'
                     AND (active.actor_user_id = $4
                          OR (active.actor_user_id IS NULL AND active.state = 'queued'))))
            AND NOT EXISTS (
                SELECT 1
                  FROM public_operations later
                 WHERE later.org_id = active.org_id
                   AND later.target_type = 'sandbox'
                   AND later.target_id = active.target_id
                   AND later.kind <> active.kind
                   AND later.kind <> ALL($5)
                   AND later.state IN ('queued', 'running')
                   AND (later.requested_at, later.operation_id)
                       > (active.requested_at, active.operation_id)
            )
          ORDER BY active.requested_at, active.operation_id
          LIMIT 1"
    ))
    .bind(org_id)
    .bind(target_id)
    .bind(kind.as_str())
    .bind(joiner)
    .bind(kind_labels(outside_queue_kinds))
    .fetch_optional(executor)
    .await?;
    Ok(joinable.map(|(operation_id, system)| JoinableOperation {
        operation_id,
        system,
    }))
}

/// Delete the operations `operation_ids`, returning the ids deleted.
pub async fn delete_by_ids(
    executor: impl PgExecutor<'_>,
    operation_ids: &[Uuid],
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_operations
         WHERE operation_id = ANY($1::UUID[])
         RETURNING operation_id"
    ))
    .bind(operation_ids)
    .fetch_all(executor)
    .await
}

/// A new queued operation, as [`insert`] records it.
#[derive(Debug)]
pub struct NewOperation<'a> {
    pub operation_id: OperationId,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: &'a str,
    pub kind: OperationKind,
    pub target_type: Option<&'a OperationTargetType>,
    pub target_id: Option<&'a str>,
    pub payload_json: serde_json::Value,
    /// Days after now, by the database clock, before the operation may be
    /// garbage-collected once finished.
    pub gc_after_days: i64,
    pub traceparent: Option<&'a str>,
}

/// Record `operation`, queued, requested at the statement's clock time
/// rather than the transaction's start.
pub async fn insert(
    executor: impl PgExecutor<'_>,
    operation: NewOperation<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO public_operations (
             operation_id,
             org_id,
             actor_user_id,
             api_product,
             kind,
             target_type,
             target_id,
             payload_json,
             gc_after,
             traceparent,
             requested_at
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8,
             NOW() + ($9::TEXT || ' days')::INTERVAL,
             $10,
             clock_timestamp()
         )"
    ))
    .bind(operation.operation_id.as_uuid())
    .bind(operation.org_id)
    .bind(operation.actor_user_id)
    .bind(operation.api_product)
    .bind(operation.kind.as_str())
    .bind(operation.target_type.map(OperationTargetType::as_str))
    .bind(operation.target_id)
    .bind(Json(operation.payload_json))
    .bind(operation.gc_after_days.to_string())
    .bind(operation.traceparent)
    .execute(executor)
    .await?;
    Ok(())
}

/// A sandbox-session create that finished as it was requested:
/// `operation_id`, succeeded with `result_json`, and its only event, its
/// completion with `event_json`, as sequence 1.
#[derive(Debug)]
pub struct CompletedSessionCreate<'a> {
    pub operation_id: Uuid,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: &'a str,
    pub session_id: String,
    pub progress_message: &'a str,
    pub result_json: serde_json::Value,
    pub event_json: serde_json::Value,
}

/// Record `create`; see [`CompletedSessionCreate`]. The event's foreign key
/// is checked at the end of the statement, after the operation row.
pub async fn insert_completed_session_create(
    executor: impl PgExecutor<'_>,
    create: CompletedSessionCreate<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH operation AS (
         INSERT INTO public_operations (
             operation_id,
             org_id,
             actor_user_id,
             api_product,
             kind,
             target_type,
             target_id,
             state,
             progress_message,
             result_json,
             started_at,
             completed_at,
             gc_after
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8, $9, $10, NOW(), NOW(),
             NOW() + ('7 days')::INTERVAL
         )
         RETURNING operation_id
         )
         INSERT INTO public_operation_events (
             operation_id,
             sequence,
             event_type,
             event_json
         )
         SELECT operation_id, 1, $11, $12
         FROM operation"
    ))
    .bind(create.operation_id)
    .bind(create.org_id)
    .bind(create.actor_user_id)
    .bind(create.api_product)
    .bind(OperationKind::CreateSession.as_str())
    .bind(OperationTargetType::Session.as_str())
    .bind(create.session_id)
    .bind(OperationState::Succeeded.as_str())
    .bind(create.progress_message)
    .bind(Json(create.result_json))
    .bind(PublicOperationEventType::Completed.as_str())
    .bind(Json(create.event_json))
    .execute(executor)
    .await?;
    Ok(())
}

/// An operation as a claim returns it. `pplx-space-api`'s dispatcher claim
/// also decodes this row, flattened into its own row type, so its
/// `RETURNING` list must select every field added here.
#[derive(Debug, FromRow)]
pub struct ClaimedPublicOperationRow {
    pub operation_id: Uuid,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: String,
    pub kind: String,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub payload_json: Json<serde_json::Value>,
    pub attempts: i32,
    pub claim_generation: i64,
    pub traceparent: Option<String>,
    pub age_at_first_claim_secs: Option<i64>,
}

/// Claim `operation_id`, which the caller's transaction just created and
/// holds: `running` under `claim_token`, one more attempt and generation,
/// stamped at the statement's clock time.
pub async fn claim_created(
    executor: impl PgExecutor<'_>,
    operation_id: OperationId,
    claim_token: &str,
) -> Result<ClaimedPublicOperationRow, sqlx::Error> {
    sqlx::query_as::<_, ClaimedPublicOperationRow>(sql!(
        "UPDATE public_operations
         SET state = $3,
             attempts = attempts + 1,
             claim_token = $2,
             claim_generation = claim_generation + 1,
             claimed_at = clock_timestamp(),
             started_at = COALESCE(started_at, clock_timestamp()),
             progress_message = 'operation claimed',
             updated_at = NOW()
         WHERE operation_id = $1
         RETURNING operation_id,
                   org_id,
                   actor_user_id,
                   api_product,
                   kind,
                   target_type,
                   target_id,
                   payload_json,
                   attempts,
                   claim_generation,
                   traceparent,
                   FLOOR(EXTRACT(EPOCH FROM NOW() - created_at))::BIGINT
                       AS age_at_first_claim_secs"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(OperationState::Running.as_str())
    .fetch_one(executor)
    .await
}

/// Record `create_id`, whose row the caller's transaction holds, canceled
/// with `progress_message` and the terminal error `code` and `error`,
/// releasing any claim and dropping a federated create's service grant.
pub async fn cancel_superseded_create(
    executor: impl PgExecutor<'_>,
    create_id: OperationId,
    progress_message: &str,
    code: &str,
    error: serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = $2,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $3,
             terminal_error_code = $4,
             terminal_error_json = $5,
             payload_json = payload_json #- '{federated_create,service_grant_base64}'::text[],
             updated_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(create_id.as_uuid())
    .bind(OperationState::Canceled.as_str())
    .bind(progress_message)
    .bind(code)
    .bind(Json(error))
    .execute(executor)
    .await?;
    Ok(())
}

/// One expiry for [`expire_each`]: the operations, and for each, at the
/// same index, its progress message and terminal error.
#[derive(Debug)]
pub struct Expiries<'a> {
    pub operation_ids: &'a [Uuid],
    pub progress_messages: &'a [String],
    pub terminal_error_codes: &'a [String],
    pub terminal_error_jsons: &'a [Json<serde_json::Value>],
}

/// Record each operation of `expiries`, whose rows the caller's transaction
/// holds, expired with its message and terminal error, dropping a sandbox
/// create's federated service grant.
pub async fn expire_each(
    executor: impl PgExecutor<'_>,
    expiries: Expiries<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE public_operations o
         SET state = $1,
             completed_at = NOW(),
             progress_message = expiry.progress_message,
             terminal_error_code = expiry.terminal_error_code,
             terminal_error_json = expiry.terminal_error_json,
             payload_json = CASE WHEN o.kind = 'create_sandbox'
                 THEN o.payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE o.payload_json END,
             updated_at = NOW()
         FROM UNNEST($2::UUID[], $3::TEXT[], $4::TEXT[], $5::JSONB[])
              AS expiry(operation_id, progress_message, terminal_error_code, terminal_error_json)
         WHERE o.operation_id = expiry.operation_id"
    ))
    .bind(OperationState::Expired.as_str())
    .bind(expiries.operation_ids)
    .bind(expiries.progress_messages)
    .bind(expiries.terminal_error_codes)
    .bind(expiries.terminal_error_jsons)
    .execute(executor)
    .await?;
    Ok(())
}
