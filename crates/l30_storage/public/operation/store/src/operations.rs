//! `public_operations` rows: reads, inserts (one of them with its first
//! event), deletes, the claim, cancel and expiry writes whose rows the
//! caller's transaction holds, and the transitions guarded on a claim token
//! (and, for payload writes and claim renewals, its generation), a state the
//! row must still be in, a row with no actor, or a federated grant's newness.
//! A guarded transition returns whether it matched, as another writer
//! winning is a normal outcome; the caller decides what follows. The reads
//! that lock rows until the caller's transaction ends, and the advisory
//! locks on a sandbox's queue, on one kind's active operation on a sandbox,
//! and on a fork source's capture leader, take its connection.
//! The dispatcher's claim of due operations is [`crate::claim_queue`]'s.

use std::sync::LazyLock;

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicOperationEventType,
    PublicSandboxId,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

/// The columns every whole-operation read selects, in [`PublicOperationRow`]'s
/// shape. Named rather than `*`: adding or dropping a `public_operations`
/// column changes what `SELECT *` returns, and every pooled connection that
/// already ran one then fails it with `0A000` ("cached plan must not change
/// result type") until the pool recycles that connection. A new column a read
/// needs is selected here, decoded in [`PublicOperationDecodeRow`], and copied
/// into a field of [`PublicOperationRow`] by a line in their `From`
/// conversion; an API record that exposes it also needs the field and its
/// line in that record's conversion. The two admin reads ([`fetch_admin`],
/// [`list_admin`]) also select the payload's `requested_by_user_id` and the
/// joined actor's email, with every column qualified by its table, so they
/// spell this list out and need the new column added there too.
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
#[derive(Debug)]
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
    pub terminal_error_json: Option<serde_json::Value>,
    pub result_json: Option<serde_json::Value>,
    pub attempts: i32,
    pub requested_at: OffsetDateTime,
    pub next_attempt_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub completed_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

/// A [`PublicOperationRow`] as read.
#[derive(FromRow)]
struct PublicOperationDecodeRow {
    operation_id: Uuid,
    org_id: OrgId,
    actor_user_id: Option<UserId>,
    api_product: String,
    kind: String,
    target_type: Option<String>,
    target_id: Option<String>,
    state: String,
    progress_message: String,
    terminal_error_code: Option<String>,
    terminal_error_json: Option<Json<serde_json::Value>>,
    result_json: Option<Json<serde_json::Value>>,
    attempts: i32,
    requested_at: OffsetDateTime,
    next_attempt_at: OffsetDateTime,
    created_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

impl From<PublicOperationDecodeRow> for PublicOperationRow {
    fn from(record: PublicOperationDecodeRow) -> Self {
        Self {
            operation_id: record.operation_id,
            org_id: record.org_id,
            actor_user_id: record.actor_user_id,
            api_product: record.api_product,
            kind: record.kind,
            target_type: record.target_type,
            target_id: record.target_id,
            state: record.state,
            progress_message: record.progress_message,
            terminal_error_code: record.terminal_error_code,
            terminal_error_json: record.terminal_error_json.map(|json| json.0),
            result_json: record.result_json.map(|json| json.0),
            attempts: record.attempts,
            requested_at: record.requested_at,
            next_attempt_at: record.next_attempt_at,
            created_at: record.created_at,
            started_at: record.started_at,
            completed_at: record.completed_at,
            updated_at: record.updated_at,
        }
    }
}

/// [`PublicOperationRow`] plus the columns only the admin record has: the
/// payload's `requested_by_user_id` and the joined actor's email.
#[derive(Debug)]
pub struct AdminPublicOperationRow {
    pub operation: PublicOperationRow,
    pub requested_by_user_id: Option<String>,
    pub actor_email: Option<String>,
}

/// An [`AdminPublicOperationRow`] as read.
#[derive(FromRow)]
struct AdminPublicOperationDecodeRow {
    #[sqlx(flatten)]
    operation: PublicOperationDecodeRow,
    requested_by_user_id: Option<String>,
    actor_email: Option<String>,
}

impl From<AdminPublicOperationDecodeRow> for AdminPublicOperationRow {
    fn from(record: AdminPublicOperationDecodeRow) -> Self {
        Self {
            operation: record.operation.into(),
            requested_by_user_id: record.requested_by_user_id,
            actor_email: record.actor_email,
        }
    }
}

/// Whole seconds left before `hard_deadline_secs` after `operation_id` was
/// created, by the database clock, and never below zero.
pub async fn remaining_deadline_secs(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    hard_deadline_secs: i64,
) -> Result<Option<i64>, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Seconds since `operation_id` was first claimed, by the database clock.
/// `None` when it does not exist or has never been claimed.
pub async fn secs_since_first_claim(
    executor: impl Executor<'_>,
    operation_id: OperationId,
) -> Result<Option<f64>, DbError> {
    let seconds = sqlx::query_scalar::<_, Option<f64>>(sql!(
        "SELECT EXTRACT(EPOCH FROM NOW() - started_at)::DOUBLE PRECISION
         FROM public_operations
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(seconds.flatten())
}

/// The operation `operation_id` of `org_id`.
pub async fn fetch(
    executor: impl Executor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<PublicOperationRow>, DbError> {
    static FETCH_OPERATION_SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(public_operation_query(
            "SELECT {public_operation_columns}
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2"
        ))
    });
    Ok(
        sqlx::query_as::<_, PublicOperationDecodeRow>(sqlx::AssertSqlSafe(
            FETCH_OPERATION_SQL.as_str(),
        ))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .fetch_optional(executor.into_sqlx())
        .await?
        .map(Into::into),
    )
}

/// Whether `operation_id` of `org_id` is in the owner scope of `subject_id`:
/// a native operation that subject requested, or, with no subject, one with
/// no actor or a local actor.
pub async fn in_owner_scope(
    executor: impl Executor<'_>,
    org_id: OrgId,
    subject_id: Option<UserId>,
    operation_id: OperationId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
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
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// What a fork operation's outcome is read from.
#[derive(Debug)]
pub struct ForkOutcomeRow {
    pub state: String,
    pub result_json: Option<serde_json::Value>,
    pub terminal_error_code: Option<String>,
    pub terminal_error_json: Option<serde_json::Value>,
}

/// A [`ForkOutcomeRow`] as read.
#[derive(FromRow)]
struct ForkOutcomeDecodeRow {
    state: String,
    result_json: Option<Json<serde_json::Value>>,
    terminal_error_code: Option<String>,
    terminal_error_json: Option<Json<serde_json::Value>>,
}

impl From<ForkOutcomeDecodeRow> for ForkOutcomeRow {
    fn from(record: ForkOutcomeDecodeRow) -> Self {
        Self {
            state: record.state,
            result_json: record.result_json.map(|json| json.0),
            terminal_error_code: record.terminal_error_code,
            terminal_error_json: record.terminal_error_json.map(|json| json.0),
        }
    }
}

/// The state, result and terminal error of the fork operation `operation_id`.
pub async fn fork_outcome(
    executor: impl Executor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<ForkOutcomeRow>, DbError> {
    Ok(sqlx::query_as::<_, ForkOutcomeDecodeRow>(sql!(
        "SELECT state, result_json, terminal_error_code, terminal_error_json
         FROM public_operations
         WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?
    .map(Into::into))
}

/// The payload of the fork operation `operation_id`.
pub async fn fork_payload(
    executor: impl Executor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<serde_json::Value>, DbError> {
    Ok(sqlx::query_scalar::<_, Json<serde_json::Value>>(sql!(
        "SELECT payload_json
         FROM public_operations
         WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?
    .map(|json| json.0))
}

/// The state and payload of a fork operation.
#[derive(Debug, FromRow)]
pub struct ForkStateRow {
    pub state: String,
    #[sqlx(json)]
    pub payload_json: serde_json::Value,
}

/// The state and payload of the fork operation `operation_id`.
pub async fn fork_state(
    executor: impl Executor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<ForkStateRow>, DbError> {
    Ok(sqlx::query_as::<_, ForkStateRow>(sql!(
        "SELECT state, payload_json
         FROM public_operations
         WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// A queued or running fork of a source sandbox.
#[derive(Debug, FromRow)]
pub struct ActiveForkRow {
    pub operation_id: Uuid,
    #[sqlx(json)]
    pub payload_json: serde_json::Value,
}

/// The queued or running forks of `source_sandbox_id` in `org_id`, oldest
/// first. Forks of one source name it by either id form.
pub async fn active_forks_of_source(
    executor: impl Executor<'_>,
    org_id: OrgId,
    source_sandbox_id: SandboxId,
) -> Result<Vec<ActiveForkRow>, DbError> {
    Ok(sqlx::query_as::<_, ActiveForkRow>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// [`fetch`] for an admin, in any org, as an [`AdminPublicOperationRow`].
///
/// The admin reads spell the column list out rather than take
/// [`PUBLIC_OPERATION_COLUMNS`], whose fragment may only be a statement's
/// whole `SELECT` list.
pub async fn fetch_admin(
    executor: impl Executor<'_>,
    operation_id: OperationId,
) -> Result<Option<AdminPublicOperationRow>, DbError> {
    Ok(sqlx::query_as::<_, AdminPublicOperationDecodeRow>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?
    .map(Into::into))
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
    executor: impl Executor<'_>,
    filter: AdminOperationFilter<'_>,
) -> Result<Vec<AdminPublicOperationRow>, DbError> {
    Ok(sqlx::query_as::<_, AdminPublicOperationDecodeRow>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?
    .into_iter()
    .map(Into::into)
    .collect())
}

/// Whether `operation_id` exists in `org_id`.
pub async fn exists_in_org(
    executor: impl Executor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(
             SELECT 1
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2
         )"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether `operation_id` can still run: it exists and is queued or running.
pub async fn is_active(
    executor: impl Executor<'_>,
    operation_id: OperationId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM public_operations
             WHERE operation_id = $1
               AND state IN ('queued', 'running')
         )"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The target of `operation_id` when it is a `kind` operation on a
/// `target_type` target.
pub async fn target_id(
    executor: impl Executor<'_>,
    operation_id: Uuid,
    kind: OperationKind,
    target_type: OperationTargetType,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT target_id
         FROM public_operations
         WHERE operation_id = $1
           AND kind = $2
           AND target_type = $3"
    ))
    .bind(operation_id)
    .bind(kind.as_str())
    .bind(target_type.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The earliest queued or running `kind` operation on the sandbox
/// `target_id` of `org_id`.
pub async fn earliest_active_on_sandbox(
    executor: impl Executor<'_>,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
) -> Result<Option<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// `kinds` as the labels a statement binds.
pub(crate) fn kind_labels(kinds: &[OperationKind]) -> Vec<&'static str> {
    kinds.iter().map(|kind| kind.as_str()).collect()
}

/// Whether an operation on the sandbox `target_id` of `org_id` other than
/// `operation_id`, and of no kind in `outside_queue_kinds`, is queued or
/// running.
pub async fn other_active_on_sandbox(
    executor: impl Executor<'_>,
    org_id: OrgId,
    target_id: &str,
    outside_queue_kinds: &[OperationKind],
    operation_id: OperationId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
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
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The creates of `target`, a sandbox of `org_id`, that are still queued or
/// running.
pub async fn sandbox_creates_in_flight(
    executor: impl Executor<'_>,
    org_id: OrgId,
    target: PublicSandboxId,
) -> Result<Vec<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The earliest delete of `create_id`'s sandbox requested after it that is
/// still queued or running, if `create_id` is a sandbox create.
pub async fn superseding_delete(
    executor: impl Executor<'_>,
    create_id: OperationId,
) -> Result<Option<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?)
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
    executor: impl Executor<'_>,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
    joiner: Option<UserId>,
    outside_queue_kinds: &[OperationKind],
) -> Result<Option<JoinableOperation>, DbError> {
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
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(joinable.map(|(operation_id, system)| JoinableOperation {
        operation_id,
        system,
    }))
}

/// Delete the operations `operation_ids`, returning the ids deleted.
pub async fn delete_by_ids(
    executor: impl Executor<'_>,
    operation_ids: &[Uuid],
) -> Result<Vec<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_operations
         WHERE operation_id = ANY($1::UUID[])
         RETURNING operation_id"
    ))
    .bind(operation_ids)
    .fetch_all(executor.into_sqlx())
    .await?)
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
    executor: impl Executor<'_>,
    operation: NewOperation<'_>,
) -> Result<(), DbError> {
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
    .execute(executor.into_sqlx())
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
    executor: impl Executor<'_>,
    create: CompletedSessionCreate<'_>,
) -> Result<(), DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// An operation as a claim returns it. The dispatcher's claim
/// ([`crate::claim_queue::claim_due`]) also decodes this row, flattened into
/// its own row type, so its `RETURNING` list must select every field added
/// here.
#[derive(Debug, FromRow)]
pub struct ClaimedPublicOperationRow {
    pub operation_id: Uuid,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: String,
    pub kind: String,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    #[sqlx(json)]
    pub payload_json: serde_json::Value,
    pub attempts: i32,
    pub claim_generation: i64,
    pub traceparent: Option<String>,
    pub age_at_first_claim_secs: Option<i64>,
}

/// Claim `operation_id`, which the caller's transaction just created and
/// holds: `running` under `claim_token`, one more attempt and generation,
/// stamped at the statement's clock time. The caller must pass its open
/// transaction.
pub async fn claim_created<C>(
    tx: &mut C,
    operation_id: OperationId,
    claim_token: &str,
) -> Result<ClaimedPublicOperationRow, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, ClaimedPublicOperationRow>(sql!(
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
    .fetch_one(tx.into_sqlx())
    .await?)
}

/// Record `create_id`, whose row the caller's transaction holds, canceled
/// with `progress_message` and the terminal error `code` and `error`,
/// releasing any claim and dropping a federated create's service grant. The
/// caller must pass its open transaction.
pub async fn cancel_superseded_create<C>(
    tx: &mut C,
    create_id: OperationId,
    progress_message: &str,
    code: &str,
    error: serde_json::Value,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
    .execute(tx.into_sqlx())
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
    pub terminal_error_jsons: &'a [serde_json::Value],
}

/// Record each operation of `expiries`, whose rows the caller's transaction
/// holds, expired with its message and terminal error, dropping a sandbox
/// create's federated service grant. The caller must pass its open
/// transaction.
pub async fn expire_each<C>(tx: &mut C, expiries: Expiries<'_>) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
    .bind(
        expiries
            .terminal_error_jsons
            .iter()
            .map(Json)
            .collect::<Vec<_>>(),
    )
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Replace the payload of `operation_id` with `payload_json` while it is
/// running under `claim_token` at `claim_generation`. `false` means the
/// claim was lost, and nothing changed.
pub async fn set_claimed_payload(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    claim_generation: i64,
    payload_json: serde_json::Value,
) -> Result<bool, DbError> {
    let changed = sqlx::query(sql!(
        "UPDATE public_operations
         SET payload_json = $4, updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND claim_generation = $3
           AND state = 'running'"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(claim_generation)
    .bind(Json(payload_json))
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(changed == 1)
}

/// Remove the payload's `json_path` from `operation_id` while it is running
/// under `claim_token` at `claim_generation`. `false` means the claim was
/// lost, and nothing changed.
pub async fn scrub_claimed_payload_path(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    claim_generation: i64,
    json_path: &[&str],
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE public_operations
             SET payload_json = payload_json #- $4::text[],
                 updated_at = NOW()
             WHERE operation_id = $1
               AND claim_token = $2
               AND claim_generation = $3
               AND state = 'running'"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(claim_generation)
    .bind(json_path)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Renew the claim on `operation_id` held under `claim_token` at
/// `claim_generation`, stamping it at the database clock. `false` means the
/// claim was lost, and nothing changed.
pub async fn renew_claim(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    claim_generation: i64,
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE public_operations
             SET claimed_at = NOW(), updated_at = NOW()
             WHERE operation_id = $1
               AND claim_token = $2
               AND claim_generation = $3
               AND state = $4"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(claim_generation)
    .bind(OperationState::Running.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Give the queued native sandbox create `operation_id` of `org_id`, which
/// `actor_user_id` requested, the federated create `context`, a JSON object,
/// when it names the same issuer, org and subject, its grant has not
/// expired by the database clock, and it is newer than the one recorded.
/// `false` means one of these did not hold, or no queued native create
/// `operation_id` of `org_id` by `actor_user_id` exists, and nothing
/// changed.
pub async fn renew_queued_native_create_grant(
    executor: impl Executor<'_>,
    org_id: OrgId,
    actor_user_id: UserId,
    operation_id: OperationId,
    context: serde_json::Value,
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE public_operations
             SET payload_json = jsonb_set(payload_json, '{federated_create}', $4),
                 updated_at = NOW()
             WHERE org_id = $1 AND actor_user_id = $2 AND operation_id = $3
               AND api_product = 'native' AND kind = 'create_sandbox' AND state = 'queued'
               AND payload_json #>> '{federated_create,issuer}' = $4->>'issuer'
               AND payload_json #>> '{federated_create,org_id}' = $4->>'org_id'
               AND payload_json #>> '{federated_create,subject_id}' = $4->>'subject_id'
               AND ($4->>'service_grant_expires_at_unix')::BIGINT > EXTRACT(EPOCH FROM NOW())
               AND CASE
                   WHEN $4->>'service_grant_issued_at_unix' IS NOT NULL
                    AND payload_json #>> '{federated_create,service_grant_issued_at_unix}' IS NOT NULL
                   THEN ($4->>'service_grant_issued_at_unix')::BIGINT >
                     (payload_json #>> '{federated_create,service_grant_issued_at_unix}')::BIGINT
                   WHEN $4->>'service_grant_issued_at_unix' IS NOT NULL THEN TRUE
                   WHEN payload_json #>> '{federated_create,service_grant_issued_at_unix}' IS NOT NULL THEN FALSE
                   ELSE ($4->>'service_grant_expires_at_unix')::BIGINT >
                     (payload_json #>> '{federated_create,service_grant_expires_at_unix}')::BIGINT
               END"
    ))
    .bind(org_id)
    .bind(actor_user_id)
    .bind(operation_id.as_uuid())
    .bind(Json(context))
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Replace the payload of the fork operation `operation_id` of `org_id` with
/// `payload_json` while it is queued or running. `false` means no queued or
/// running fork `operation_id` of `org_id` exists, and nothing changed.
pub async fn set_active_fork_payload(
    executor: impl Executor<'_>,
    org_id: OrgId,
    operation_id: OperationId,
    payload_json: serde_json::Value,
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE public_operations
                 SET payload_json = $3, updated_at = NOW()
                 WHERE org_id = $1
                   AND operation_id = $2
                   AND kind = 'fork_sandbox'
                   AND state IN ('queued', 'running')"
    ))
    .bind(org_id)
    .bind(operation_id.as_uuid())
    .bind(Json(payload_json))
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Make `actor_user_id` the actor of the system's operation `operation_id`
/// while it is still queued with no actor. `false` means it had an actor or
/// had left the queue, and nothing changed.
pub async fn hand_over_queued_system_operation(
    executor: impl Executor<'_>,
    operation_id: Uuid,
    actor_user_id: UserId,
) -> Result<bool, DbError> {
    let handed_over = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
                    SET actor_user_id = $2, updated_at = NOW()
                  WHERE operation_id = $1
                    AND actor_user_id IS NULL
                    AND state = 'queued'
                  RETURNING operation_id"
    ))
    .bind(operation_id)
    .bind(actor_user_id)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(handed_over.is_some())
}

/// The count [`inline_org_at_limit`] runs.
///
/// Cost: the organization's running operations, however claimed, through
/// `idx_public_operations_org_state`, which has none of the other columns, so
/// the claim-token, claim-time and kind filters apply after the read. Without
/// that index this read every running row in the fleet through
/// `idx_public_operations_state_next_attempt`, or, under a generic plan,
/// intersected that with every operation the organization retains through
/// `idx_public_operations_tenant_created`. A constant rather than a literal at
/// its query site so the API's plan-cost test can `EXPLAIN` the text this
/// runs.
pub(crate) const INLINE_ORG_AT_LIMIT: &str = sql!(
    "SELECT COUNT(*) >= $2
       FROM public_operations
      WHERE state = $3
        AND org_id = $1
        AND starts_with(claim_token, $4)
        AND claimed_at > NOW() - make_interval(secs => $5::BIGINT)
        AND kind <> ALL($6)"
);

/// Whether `org_id` has at least `limit` running operations, of no kind in
/// `excluded_kinds`, whose claim token starts with `claim_token_prefix` and
/// was renewed within `claim_stale_after_secs` of now by the database clock.
/// See `INLINE_ORG_AT_LIMIT` for what it reads.
pub async fn inline_org_at_limit(
    executor: impl Executor<'_>,
    org_id: OrgId,
    limit: i64,
    claim_token_prefix: &str,
    claim_stale_after_secs: i64,
    excluded_kinds: &[OperationKind],
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(INLINE_ORG_AT_LIMIT)
        .bind(org_id)
        .bind(limit)
        .bind(OperationState::Running.as_str())
        .bind(claim_token_prefix)
        .bind(claim_stale_after_secs)
        .bind(kind_labels(excluded_kinds))
        .fetch_one(executor.into_sqlx())
        .await?)
}

/// A queued or running sandbox create [`lock_active_creates_on_sandbox`]
/// found.
#[derive(Debug)]
pub struct ActiveCreate {
    pub operation_id: Uuid,
    pub api_product: String,
    /// Queued with no claim, live or stale.
    pub unclaimed: bool,
}

/// The queued or running creates of the sandbox `target_id` of `org_id`
/// requested no later than now by the database clock, in request order,
/// each locked until the caller's transaction ends. Waits for a claim or a
/// release that holds one.
/// The caller must pass its open transaction.
pub async fn lock_active_creates_on_sandbox<C>(
    conn: &mut C,
    org_id: OrgId,
    target_id: &str,
) -> Result<Vec<ActiveCreate>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let active = sqlx::query_as::<_, (Uuid, String, bool)>(sql!(
        "SELECT operation_id, api_product, state = $5 AND claim_token = ''
         FROM public_operations
         WHERE org_id = $1
           AND target_type = $2
           AND target_id = $3
           AND kind = $4
           AND state IN ($5, $6)
           AND requested_at <= NOW()
         ORDER BY requested_at, operation_id
         FOR UPDATE"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_all(conn.into_sqlx())
    .await?;
    Ok(active
        .into_iter()
        .map(|(operation_id, api_product, unclaimed)| ActiveCreate {
            operation_id,
            api_product,
            unclaimed,
        })
        .collect())
}

/// Record `operation_id`, running under `claim_token`, succeeded with
/// `progress_message` and `result_json`, releasing the claim and dropping a
/// sandbox create's federated service grant. `false` means the claim was
/// lost, and nothing changed.
pub async fn complete_claimed(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    progress_message: &str,
    result_json: serde_json::Value,
) -> Result<bool, DbError> {
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $4,
             result_json = $5,
             payload_json = CASE WHEN kind = 'create_sandbox'
                 THEN payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE payload_json END,
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $6
         RETURNING operation_id"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(OperationState::Succeeded.as_str())
    .bind(progress_message)
    .bind(Json(result_json))
    .bind(OperationState::Running.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(updated.is_some())
}

/// A terminal error, as [`fail_claimed`] and [`expire_claimed`] record it.
#[derive(Debug)]
pub struct TerminalError<'a> {
    pub progress_message: &'a str,
    pub code: &'a str,
    pub error: serde_json::Value,
}

/// Record `operation_id`, running under `claim_token`, failed with
/// `terminal`, releasing the claim and dropping a sandbox create's federated
/// service grant. `false` means the claim was lost, and nothing changed.
pub async fn fail_claimed(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    terminal: TerminalError<'_>,
) -> Result<bool, DbError> {
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $4,
             terminal_error_code = $5,
             terminal_error_json = $6,
             payload_json = CASE WHEN kind = 'create_sandbox'
                 THEN payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE payload_json END,
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $7
         RETURNING operation_id"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(OperationState::Failed.as_str())
    .bind(terminal.progress_message)
    .bind(terminal.code)
    .bind(Json(terminal.error))
    .bind(OperationState::Running.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(updated.is_some())
}

/// Record `operation_id`, running under `claim_token`, expired with
/// `terminal` rather than run, undoing the claim's attempt and start,
/// releasing the claim and dropping a sandbox create's federated service
/// grant. `false` means the claim was lost, and nothing changed.
pub async fn expire_claimed(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    terminal: TerminalError<'_>,
) -> Result<bool, DbError> {
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             attempts = GREATEST(attempts - 1, 0),
             started_at = NULL,
             completed_at = NOW(),
             progress_message = $4,
             terminal_error_code = $5,
             terminal_error_json = $6,
             payload_json = CASE WHEN kind = 'create_sandbox'
                 THEN payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE payload_json END,
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $7
         RETURNING operation_id"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(OperationState::Expired.as_str())
    .bind(terminal.progress_message)
    .bind(terminal.code)
    .bind(Json(terminal.error))
    .bind(OperationState::Running.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(updated.is_some())
}

/// A queued operation [`lock_overdue_queued`] found.
#[derive(Debug, FromRow)]
pub struct OverdueQueuedRow {
    pub operation_id: Uuid,
    pub kind: String,
    pub attempts: i32,
    pub progress_message: String,
}

/// Up to `limit` of the oldest queued, unclaimed, never-started
/// `api_product` operations of `kinds` created more than `max_age_secs`
/// before now by the database clock, each locked until the caller's
/// transaction ends, skipping any another transaction holds.
/// The caller must pass its open transaction.
pub async fn lock_overdue_queued<C>(
    conn: &mut C,
    api_product: &str,
    kinds: &[OperationKind],
    max_age_secs: i64,
    limit: i64,
) -> Result<Vec<OverdueQueuedRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, OverdueQueuedRow>(sql!(
        "SELECT operation_id, kind, attempts, progress_message
         FROM public_operations
         WHERE state = $1
           AND claim_token = ''
           AND started_at IS NULL
           AND api_product = $2
           AND kind = ANY($3)
           AND created_at <= NOW() - make_interval(secs => $4::BIGINT)
         ORDER BY created_at, operation_id
         LIMIT $5
         FOR UPDATE SKIP LOCKED"
    ))
    .bind(OperationState::Queued.as_str())
    .bind(api_product)
    .bind(kind_labels(kinds))
    .bind(max_age_secs)
    .bind(limit)
    .fetch_all(conn.into_sqlx())
    .await?)
}

/// The kind of `operation_id` while it is running under `claim_token`,
/// locking its row until the caller's transaction ends. `None` means the
/// claim was lost.
/// The caller must pass its open transaction.
pub async fn claimed_kind_locked<C>(
    conn: &mut C,
    operation_id: OperationId,
    claim_token: &str,
) -> Result<Option<String>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT kind
         FROM public_operations
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $3
         FOR UPDATE"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(OperationState::Running.as_str())
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// A requeue for [`requeue_claimed`].
#[derive(Debug)]
pub struct Requeue<'a> {
    pub progress_message: &'a str,
    /// Seconds after now, by the database clock, before it is due again.
    pub retry_after_secs: f64,
    /// Whether the claim's attempt stays counted.
    pub count_attempt: bool,
    /// Whether the claim's start is undone.
    pub undo_first_start: bool,
}

/// Return `operation_id`, running under `claim_token`, to the queue as
/// `requeue` says, releasing the claim. `false` means the claim was lost,
/// and nothing changed.
pub async fn requeue_claimed(
    executor: impl Executor<'_>,
    operation_id: OperationId,
    claim_token: &str,
    requeue: Requeue<'_>,
) -> Result<bool, DbError> {
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             progress_message = $4,
             next_attempt_at = NOW() + make_interval(secs => $5::DOUBLE PRECISION),
             attempts = GREATEST(attempts - CASE WHEN $7 THEN 0 ELSE 1 END, 0),
             started_at = CASE WHEN $8 THEN NULL ELSE started_at END,
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $6
         RETURNING operation_id"
    ))
    .bind(operation_id.as_uuid())
    .bind(claim_token)
    .bind(OperationState::Queued.as_str())
    .bind(requeue.progress_message)
    .bind(requeue.retry_after_secs)
    .bind(OperationState::Running.as_str())
    .bind(requeue.count_attempt)
    .bind(requeue.undo_first_start)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(updated.is_some())
}

/// Remove the payload's `json_path` from up to `limit` finished `kind`
/// operations that still carry it, least recently updated first, skipping
/// any another transaction holds. Returns how many were scrubbed.
pub async fn scrub_finished_payload_path(
    executor: impl Executor<'_>,
    kind: OperationKind,
    json_path: &[&str],
    limit: i64,
) -> Result<u64, DbError> {
    let scrubbed = sqlx::query(sql!(
        "WITH terminal_operations AS (
             SELECT operation_id
             FROM public_operations
             WHERE kind = $2
               AND state IN ($3, $4, $5, $6)
               AND payload_json #> $7::text[] IS NOT NULL
             ORDER BY updated_at ASC, operation_id ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE public_operations AS operation
         SET payload_json = operation.payload_json #- $7::text[],
             updated_at = NOW()
         FROM terminal_operations
         WHERE operation.operation_id = terminal_operations.operation_id"
    ))
    .bind(limit)
    .bind(kind.as_str())
    .bind(OperationState::Succeeded.as_str())
    .bind(OperationState::Failed.as_str())
    .bind(OperationState::Canceled.as_str())
    .bind(OperationState::Expired.as_str())
    .bind(json_path)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(scrubbed)
}

/// Up to `limit` finished operations past their `gc_after` by the database
/// clock, earliest first.
pub async fn due_for_gc(executor: impl Executor<'_>, limit: i64) -> Result<Vec<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id
         FROM public_operations
         WHERE state IN ($2, $3, $4, $5)
           AND gc_after <= NOW()
         ORDER BY gc_after ASC, operation_id ASC
         LIMIT $1"
    ))
    .bind(limit)
    .bind(OperationState::Succeeded.as_str())
    .bind(OperationState::Failed.as_str())
    .bind(OperationState::Canceled.as_str())
    .bind(OperationState::Expired.as_str())
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Take the transaction-level advisory lock on `key`, hashed, waiting for
/// it. Callers key it per sandbox: one key for inserts into its queue, and
/// one for creating or joining each kind's active operation on it. The
/// lock lasts until the caller's transaction ends.
/// The caller must pass its open transaction.
pub async fn lock_sandbox_key<C>(conn: &mut C, key: &str) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(key)
    .execute(conn.into_sqlx())
    .await?;
    Ok(())
}

/// Take the transaction-level advisory lock that serializes choosing the
/// capture leader among the forks of one source at one execution
/// `generation`, keyed by `source_key` hashed with `generation` as the seed,
/// waiting for it. The lock lasts until the caller's transaction ends.
/// The caller must pass its open transaction.
pub async fn lock_fork_capture_leader<C>(
    conn: &mut C,
    source_key: &str,
    generation: i64,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))"
    ))
    .bind(source_key)
    .bind(generation)
    .execute(conn.into_sqlx())
    .await?;
    Ok(())
}
