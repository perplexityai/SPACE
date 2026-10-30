//! Helpers shared by the `/admin/v1` handlers: the JSON error shape, the
//! operator-authorization gate, and timestamp formatting. Route modules stay
//! focused on their own HTTP surface and state machines.

use axum::{
    Json,
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use pplx_space_common_svc::sql::contention;
use time::OffsetDateTime;

use crate::models::User;
use crate::routes::authz::is_managing_org_admin;

/// The header that makes a replay of an admin mutation adopt the existing
/// operation instead of opening a second one.
///
/// Read by the two `/admin` modules whose writes honour it — `sandboxes`'
/// `create_sandbox`, `dispatch_sandbox_action` and `update_sandbox`, and
/// `templates::delete_template` — so those two share one definition rather
/// than each carrying a copy. (The `public` and `e2b` surfaces keep their own;
/// this is not a crate-wide constant.)
///
/// Deliberately *not* read by `require_admin_auth`: see
/// `audit_failure_response` for why the header's presence on a request is not
/// evidence that a replay is safe, and why deciding from it was reverted.
pub(super) const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

pub(super) fn api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": message.into()})))
}

/// An admin handler's response when a `SandboxController` call it delegates to
/// failed.
///
/// The controller's read and projection paths classify contention, so a lost
/// lock race arrives here already marked. Matching on the status alone would
/// not do: these arms answered a hardcoded 500, which discards the
/// classification one layer above the statement that produced it — the same
/// way a translator that ignores the marker re-labels a lock race as a fleet
/// condition.
///
/// The SQLSTATE is gone by this point (`ControllerError::lock_contention`
/// collapses all three codes), so this cannot make [`db_error`]'s 503/409
/// split and answers the retryable 503 that split gives `55P03`. `log` runs
/// only on the non-contention path, so each call site keeps its own message
/// and fields while a bounded lock wait lands at `warn` inside
/// [`ControllerError::lock_contention`]'s producer instead of `warn`-ing twice.
pub(super) fn controller_read_error(
    error: &crate::controllers::ControllerError,
    log: impl FnOnce(),
) -> axum::response::Response {
    if error.is_lock_contention() {
        return crate::controllers::ControllerError::lock_contention().into_response();
    }
    log();
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

/// Whether the statement that raised a contention SQLSTATE could have been
/// racing writes this request had already made.
///
/// The two contention classes do not describe the same event, so the answer
/// depends on where on the request the statement sat, not on which handler it
/// belongs to. A `GET`'s read, an authz preflight, and any statement issued
/// before a write handler's first write are [`Self::ReadOnly`] — with one
/// exception, a read that takes row locks for a write about to follow, which
/// the [`Self::Writing`] variant explains. See [`contention_disposition`] for
/// what each scope withholds and why.
///
/// This describes what each *caller* should pass, not a property the code
/// enforces: [`db_error`] hardcodes [`Self::Writing`] for all of its callers,
/// several of which are reads. Its own doc says so.
#[derive(Clone, Copy, Debug)]
pub(in crate::routes) enum ContentionScope {
    /// This request had neither written nor locked anything to write when the
    /// statement failed — an authz preflight, a `GET` handler's read, a
    /// statement a write handler issues before its first write, or a read on a
    /// branch where that write did not happen. A plain read before the first
    /// write belongs here; one that takes row locks does not, per
    /// [`Self::Writing`].
    ReadOnly,
    /// The failing statement was a write, took row locks for one, or shared a
    /// transaction with a write this request had already issued.
    ///
    /// "Write" here means a write was *issued*, not that a row changed: a
    /// guarded `UPDATE` or `DELETE` that matched nothing still took its locks
    /// and still raced whoever it lost to.
    ///
    /// A lock-taking read belongs here despite having modified nothing —
    /// `tag_template_release` opens with a `SELECT ... FOR UPDATE` before its
    /// `INSERT` — because a deadlock on those row locks is an ordering
    /// interleave between writers, which is exactly what the 409 names. The
    /// distinction that matters is whether the request had declared write
    /// intent, not whether a row had changed yet.
    Writing,
}

impl ContentionScope {
    /// Whether a serialization failure or deadlock on this statement can be
    /// reported as the caller's write losing a race — i.e. whether it earns the
    /// 409 rather than the generic retryable 503. Both are retryable; this is
    /// only about which one describes what happened.
    ///
    /// A `match` rather than `self == Writing` so a third scope is a compile
    /// error at this one place instead of silently joining `ReadOnly`.
    const fn races_a_caller_write(self) -> bool {
        match self {
            Self::ReadOnly => false,
            Self::Writing => true,
        }
    }
}

/// The `(status, message)` this surface answers a contention abort with, or
/// `None` when `err` is a real failure the caller should log and answer 500.
///
/// Split out from [`db_error`] so an admin handler carrying its own error
/// envelope reaches the same disposition instead of restating it: the whole
/// `/admin` surface has to agree on which code a lost race gets, or one
/// handler's 409 and another's 500 describe the same `40P01`. The `warn` is
/// here so it fires once per classification wherever the caller took the
/// answer.
///
/// Every SQLSTATE in the contention set is retryable — that is what
/// `sql::contention` means by the word — so all three get a retryable status
/// here regardless of scope. `scope` chooses only *which* retryable status.
///
/// `40001`/`40P01` on a [`ContentionScope::Writing`] statement is the AB-BA
/// case: the guarded admin writes take their row locks in different orders
/// across handlers (a compat edit locks the group then the templates, while a
/// visibility flip locks the template then, via the compat cascade, the group),
/// so Postgres aborts one arm of an interleave. "Conflicted with a concurrent
/// admin write" describes that, and 409 is the code for it.
///
/// On a read that wrote nothing, that wording would blame the caller for a
/// write it never made — so the 409 is withheld and the abort takes the generic
/// retryable 503 instead. Withholding the 409 is not a reason to answer 500:
/// the statement can still succeed on retry, and reporting a bounded, retryable
/// race as an internal failure is the defect this whole disposition exists to
/// remove.
///
/// `55P03` never gets the 409 in either scope. Any statement can raise it,
/// including a plain read on a `GET` handler that blocked behind DDL or behind
/// the drained-node terminator's cascade, so it never carries the 409's
/// meaning.
pub(in crate::routes) fn contention_disposition(
    scope: ContentionScope,
    err: &sqlx::Error,
) -> Option<(StatusCode, &'static str)> {
    if !contention::is_lock_contention(err) {
        return None;
    }
    if scope.races_a_caller_write() && !contention::is_lock_not_available(err) {
        tracing::warn!(error = %err, "admin query aborted by serialization/deadlock; retryable");
        return Some((
            StatusCode::CONFLICT,
            "operation conflicted with a concurrent admin write; retry the request",
        ));
    }
    tracing::warn!(error = %err, "admin query lost a lock race; retryable");
    Some((
        StatusCode::SERVICE_UNAVAILABLE,
        "operation conflicted with a concurrent change; retry the request",
    ))
}

/// [`contention_disposition`] for a [`PublicOperationStore`] failure.
///
/// Every admin surface that opens or replays a public operation reaches its
/// database error through this enum, so the "which variants can carry a
/// SQLSTATE" judgment lives here rather than being re-made in each module that
/// touches the store. The non-`Sql` variants are decode failures on rows this
/// service wrote; no retry re-reads them into something valid.
///
/// [`PublicOperationStore`]: crate::public_operations::PublicOperationStore
pub(in crate::routes) fn store_contention_disposition(
    scope: ContentionScope,
    error: &crate::public_operations::PublicOperationStoreError,
) -> Option<(StatusCode, &'static str)> {
    use crate::public_operations::PublicOperationStoreError as StoreError;

    // Exhaustive for the same reason `races_a_caller_write` is: a future
    // variant that wraps a `sqlx::Error` must be a compile error here rather
    // than joining the unclassified set silently.
    match error {
        StoreError::Sql(error) => contention_disposition(scope, error),
        StoreError::InvalidStoredEnum { .. }
        | StoreError::InvalidStoredResult(_)
        | StoreError::InvalidStoredForkResult
        | StoreError::InvalidStoredForkPayload(_)
        | StoreError::InvalidStoredFingerprint(_) => None,
    }
}

/// The response for a database failure on an admin handler, with a lost lock
/// race classified by [`contention_disposition`].
///
/// **Hardcodes [`ContentionScope::Writing`]**, which is right for the guarded
/// writes that are most of its callers across `node_groups`, `nodes`,
/// `node_maintenance`, `template_visibility` and `warm_pools` — and wrong for
/// the reads among them. Four found so far, and the list is not claimed to be
/// complete: `list_node_groups` (`node_groups.rs`), the admin node listing
/// (`nodes.rs`), and `node_health` and `list_node_templates`
/// (`node_maintenance.rs`, reaching `fetch_nodes` through
/// `fetch_inventory_node` and `fetch_inventory_nodes` respectively) all answer
/// a `40P01` with "conflicted with a concurrent admin write" having issued
/// only `SELECT`s.
///
/// That predates this disposition and is not changed here: giving `db_error` a
/// scope parameter forces a read-or-write judgment at every one of those
/// callers, which is its own audit. Until then, a claim that the `/admin`
/// surface never blames a read for a write holds for the handlers that call
/// [`contention_disposition`] directly, not for `db_error`'s.
pub(super) fn db_error(err: sqlx::Error) -> (StatusCode, Json<serde_json::Value>) {
    if let Some((status, message)) = contention_disposition(ContentionScope::Writing, &err) {
        return api_error(status, message);
    }
    tracing::warn!(error = %err, "admin query failed");
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

/// The response for a database failure on a statement that wrote nothing,
/// with a lost lock race classified as [`ContentionScope::ReadOnly`]: unlike
/// [`db_error`], it never blames a read for a write the caller didn't make.
pub(super) fn read_db_error(err: sqlx::Error) -> axum::response::Response {
    if let Some((status, message)) = contention_disposition(ContentionScope::ReadOnly, &err) {
        return api_error(status, message).into_response();
    }
    tracing::warn!(error = %err, "admin read failed");
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

/// Required-JSON-body parser for admin writes. Bodies are never optional
/// here — an accidental empty POST must not be interpreted as (for example)
/// an org release — and every request type uses `deny_unknown_fields`, so a
/// typoed key surfaces as a 400 instead of silently changing meaning.
pub(super) fn parse_json_body<T: serde::de::DeserializeOwned>(
    body: &[u8],
    what: &str,
) -> Result<T, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("missing JSON body for {what}"),
        ));
    }
    serde_json::from_slice::<T>(body).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid {what} body: {err}"),
        )
    })
}

/// The generation an `If-Match` header asks a write to apply at: `None` when
/// the header is absent, which makes the write unconditional. The error is a
/// 400 message, so each caller answers it in its own error shape.
pub(super) fn parse_if_match_generation(headers: &HeaderMap) -> Result<Option<u64>, &'static str> {
    let Some(value) = headers.get(header::IF_MATCH) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| "Invalid If-Match header")?
        .trim()
        .trim_matches('"');
    raw.parse::<u64>()
        .map(Some)
        .map_err(|_| "If-Match must be a generation number")
}

pub(super) fn format_time(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

pub(super) fn format_optional_time(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(format_time)
}

/// Operator gate for every `/admin/v1` write and read: the actor must be an
/// `org_admin` of the cluster's managing org. 403 otherwise.
///
/// The four private copies this replaced each logged a surface-specific
/// message ("failed to authorize admin warm-pool action", and so on) that this
/// one does not. That attribution is not lost: both routers that mount this
/// tree — `routes::shared_host_router` and `routes::dedicated_admin_router` —
/// wrap it in `http_trace_layer`, whose `http.server` span carries `url.path`
/// and `http.route`, naming the surface more precisely than a hand-written
/// label could. Threading a label through this gate's 36 call sites would
/// duplicate it.
///
/// `actor_id` is on the real-failure arm only. A contention warn comes from
/// `contention_disposition`, which is shared and takes no actor. The two arms
/// also differ in what the error itself yields: this one formats `Debug`,
/// which renders `PgDatabaseError`'s `table`, `constraint` and `detail`, while
/// the shared warn formats `Display`, which is the primary message alone — so
/// a contention warn names no table. The route is on the span either way.
pub(super) async fn require_managing_org_admin(
    pool: &sqlx::PgPool,
    actor: &User,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    match is_managing_org_admin(pool, actor).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(api_error(StatusCode::FORBIDDEN, "Insufficient permissions")),
        // The rationale for this arm. Two of the three remaining private
        // copies of this gate point back to it —
        // `templates::require_template_admin` and
        // `debug::sandbox::require_managing_org_admin`, each kept because its
        // module returns a different error type. The third,
        // `identity::require_managing_org_admin`, has no contention arm at all
        // and still answers 500 for every SQLSTATE; classifying it belongs
        // with the rest of that module's typed error enum.
        //
        // A database error here is the authz preflight's, not the handler's.
        // Contention has to be classified at this gate too, or the handler's
        // 503 is unreachable: the preflight `SELECT` on `clusters` blocks
        // behind DDL exactly like any other read, 500s, and the request never
        // gets that far. The same argument one layer up is why
        // `require_admin_auth` classifies as well — it runs this identical
        // query before any handler is reached.
        //
        // `ReadOnly` because the preflight wrote nothing: every contention
        // SQLSTATE still answers a retryable 503, but none of them takes the
        // 409, which would report a read as the caller's write losing a race.
        Err(err) => match contention_disposition(ContentionScope::ReadOnly, &err) {
            Some((status, message)) => Err(api_error(status, message)),
            None => {
                tracing::error!(
                    error = ?err,
                    actor_id = %actor.id,
                    "failed to authorize admin action"
                );
                Err(api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                ))
            }
        },
    }
}
