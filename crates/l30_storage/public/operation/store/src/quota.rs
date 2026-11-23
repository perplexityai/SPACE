//! The live-sandbox quota count, which reads `sandboxes` beside this store's
//! `public_operations` in one statement.

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_public_api::{OperationKind, PUBLIC_SANDBOX_ID_PREFIX};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// What [`sandboxes_held`] counts: the org's, and the subject's share.
#[derive(Clone, Copy, Debug)]
pub struct SandboxesHeld {
    pub org: i64,
    pub subject: i64,
}

/// The org's live sandboxes plus its in-flight creates of the product bound
/// as `$3` (the API binds native) without a live row, and the share of those
/// `subject_id` owns or requested (zero without one). An in-flight create
/// whose row went terminal (a drain sweep failed it while the create ran)
/// still counts: it can retry and succeed.
///
/// One statement on purpose: split into a row count and an operation count,
/// two READ COMMITTED snapshots could miss a create whose row lands between
/// them, counted by neither half. It lives here, beside the
/// `public_operations` half this store owns, and runs inside the caller's
/// admission transaction, rather than as a composed verb of
/// `pplx-space-sandbox-store`, which owns `sandboxes`; moving it there has to
/// keep it one statement. The
/// terminal set repeats the lifecycle CAS guards' list.
///
/// A create's row is the one its target names, so the target is parsed back
/// to a sandbox id and each create is joined to its row by id. Comparing the
/// target with `$5 || s.id::text` instead can use no index, and a generic
/// plan then rescans the org's rows once per create. Only the text
/// `s.id::text` produces is parsed: the `LIKE` fixes its length and its
/// hyphens' places, and the expression allows exactly four hyphens with
/// lowercase hex between them. Any other target names no row, as the text
/// comparison has it, and never reaches the cast, which would fail the
/// statement. One expression with counted repetitions would say the same,
/// but Postgres evaluates it many times slower than this pair.
///
/// Cost: the org's rows through `idx_sandboxes_org_id`, `deleted`
/// tombstones included; the org's queued or running operations, of any
/// kind or product, through `idx_public_operations_org_state`, which has
/// neither of those columns, so the native-create filter applies after the
/// read; and each of the org's in-flight creates joined to its row by id.
/// A constant rather than a literal at its query site so the API's plan-cost
/// test can `EXPLAIN` the text this runs.
pub const SANDBOXES_HELD: &str = sql!(
    "SELECT COUNT(*), COUNT(*) FILTER (WHERE held.user_id = $2)
       FROM (
            SELECT s.user_id
              FROM sandboxes s
             WHERE s.org_id = $1
               AND s.lifecycle NOT IN ('failed', 'quarantined', 'deleted')
            UNION ALL
            SELECT op.actor_user_id
              FROM public_operations op
             WHERE op.org_id = $1
               AND op.state IN ('queued', 'running')
               AND op.api_product = $3
               AND op.kind = $4
               AND NOT EXISTS (
                   SELECT 1
                     FROM sandboxes s
                    WHERE s.id = CASE
                              WHEN starts_with(op.target_id, $5)
                               AND substr(op.target_id, length($5) + 1)
                                   LIKE '________-____-____-____-____________'
                               AND substr(op.target_id, length($5) + 1)
                                   ~ '^[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+$'
                              THEN substr(op.target_id, length($5) + 1)::uuid
                          END
                      AND s.org_id = $1
                      AND s.lifecycle NOT IN ('failed', 'quarantined', 'deleted')
               )
       ) held"
);

/// Count with [`SANDBOXES_HELD`], for in-flight creates of `api_product`;
/// see there for what is counted.
pub async fn sandboxes_held(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    subject_id: Option<UserId>,
    api_product: &str,
) -> Result<SandboxesHeld, sqlx::Error> {
    let (org, subject) = sqlx::query_as::<_, (i64, i64)>(SANDBOXES_HELD)
        .bind(org_id)
        .bind(subject_id)
        .bind(api_product)
        .bind(OperationKind::CreateSandbox.as_str())
        .bind(PUBLIC_SANDBOX_ID_PREFIX)
        .fetch_one(executor)
        .await?;
    Ok(SandboxesHeld { org, subject })
}
