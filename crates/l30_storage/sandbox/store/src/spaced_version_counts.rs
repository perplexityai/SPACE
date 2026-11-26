//! How many sandboxes run each `spaced` version, for the admin list's count:
//! the `sandboxes` rows and the observed states nodes publish, joined the way
//! the list projects them.

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

use pplx_space_sandbox_teardown::observed_states::ObservedSpacedVersion;

const DELETED_LIFECYCLE: &str = "deleted";

/// One group of [`count_by_spaced_version`].
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SpacedVersionCountRecord {
    /// The row's template, else the node's, else the template the node's
    /// inventory holds for the image it runs, else the pool's.
    pub template: Option<String>,
    #[sqlx(flatten)]
    pub version: ObservedSpacedVersion,
    /// The node's observed status, else the stored lifecycle.
    pub status: Option<String>,
    /// The group's sandboxes have a `sandboxes` row.
    pub has_row: bool,
    /// The group's rowless sandboxes are pool slots: their node reports a
    /// pool template, or their ID is in the caller's `pool_reserved`.
    pub pool_slot: bool,
    pub count: i64,
}

/// Every sandbox whose status is not `deleted`, counted by template,
/// `spaced` version, status, whether it has a row and whether it is a pool
/// slot. With `org_id`, only that organization's rows; without it, the
/// observed states with no sandbox row too. `pool_reserved` lists the
/// rowless sandboxes the caller found a warm-pool slot reserving.
pub async fn count_by_spaced_version(
    executor: impl Executor<'_>,
    org_id: Option<OrgId>,
    pool_reserved: &[SandboxId],
) -> Result<Vec<SpacedVersionCountRecord>, DbError> {
    Ok(sqlx::query_as::<_, SpacedVersionCountRecord>(sql!(
        "SELECT counted.template,
                counted.spaced_version,
                counted.status,
                counted.has_row,
                counted.pool_slot,
                COUNT(*)::BIGINT AS count
         FROM (
             SELECT COALESCE(s.template_id, sos.template, observed_template.template_name,
                             sos.pool_template) AS template,
                    sos.spaced_version,
                    COALESCE(sos.status, s.lifecycle) AS status,
                    s.id IS NOT NULL AS has_row,
                    s.id IS NULL
                        AND (sos.pool_template IS NOT NULL
                             OR sos.sandbox_id IN (SELECT unnest($3::UUID[]))) AS pool_slot
             FROM sandboxes s
             FULL JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
             LEFT JOIN LATERAL (
                 SELECT nti.template_name
                 FROM node_template_inventory nti
                 WHERE nti.node_id = sos.node_id
                   AND sos.image_hash IS NOT NULL
                   AND nti.content_hash = sos.image_hash
                 ORDER BY nti.observed_at DESC, nti.template_name ASC
                 LIMIT 1
             ) observed_template ON TRUE
             WHERE ($1::UUID IS NULL OR s.org_id = $1)
         ) counted
         WHERE counted.status IS DISTINCT FROM $2
         GROUP BY 1, 2, 3, 4, 5
         ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 4 DESC, 5"
    ))
    .bind(org_id)
    .bind(DELETED_LIFECYCLE)
    .bind(pool_reserved)
    .fetch_all(executor.into_sqlx())
    .await?)
}
