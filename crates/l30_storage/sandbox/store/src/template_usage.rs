//! How many sandboxes still reference a template, for the admin visibility
//! flip's narrowing guard.
//!
//! A plain read: it takes no lock and runs on the executor its caller passes.
//! The admin caller runs it in the transaction that holds its `templates` row
//! lock.

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::failed_repair_revert_eligibility;
use pplx_space_sql_locator::sql;

/// How many sandboxes whose `template_id` is `template_name` are not gone:
/// any lifecycle but `failed` and `deleted`, plus the `failed` rows the
/// failed-restorable repair loop could still revert to `suspended`
/// ([`failed_repair_revert_eligibility`]'s `eligible`).
pub async fn template_sandboxes_in_use(
    executor: impl Executor<'_>,
    template_name: &str,
) -> Result<i64, DbError> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let eligible = failed_repair_revert_eligibility("s").eligible;
        sql!(format!(
            "SELECT COUNT(*)::BIGINT
           FROM sandboxes s
          WHERE s.template_id = $1
            AND (
                 s.lifecycle NOT IN ('failed', 'deleted')
                 OR (s.lifecycle = 'failed' AND {eligible})
            )"
        ))
    });
    Ok(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(QUERY.as_str()))
            .bind(template_name)
            .fetch_one(executor.into_sqlx())
            .await?,
    )
}

#[cfg(test)]
mod in_use_tests;
