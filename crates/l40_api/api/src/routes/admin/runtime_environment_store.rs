//! Observed-state reads behind the runtime environment admin routes: which
//! environment each sandbox and template release is bound to. What each node
//! reports enforcing comes from the admin node listing in `node_store`. The
//! policy itself is read and written only through
//! `pplx_space_runtime_environment_policy_store`.
//!
//! Every read here is observational. Nothing takes a lock, and a count can be
//! stale by the time the caller acts on it.

use pplx_space_sql_locator::sql;
use pplx_space_template_release_original::digest_original;
use sqlx::FromRow;
use std::sync::LazyLock;

/// Sandboxes grouped by their environment binding and lifecycle.
#[derive(Debug, FromRow)]
pub(super) struct SandboxPopulationRow {
    /// The `environment` field of the binding record, unparsed. `None` when
    /// the record carries no binding, or when there's no record.
    pub(super) environment_ref: Option<String>,
    /// Whether a binding record exists: the node's observation for a sandbox
    /// that isn't suspended or restoring, the snapshot a restore would pick for
    /// one that is.
    pub(super) recorded: bool,
    /// Raw `sandboxes.lifecycle`, so a value this build doesn't know is still
    /// counted rather than failing the read.
    pub(super) lifecycle: String,
    /// Whether these sandboxes can run again, by the repo's live-work rule:
    /// not `deleting`, and not `failed` unless it looks live (an
    /// observation or Kubernetes execution the repair loop reads as live) or
    /// the repair loop can revert it to `suspended`
    /// (`failed_repair_revert_eligibility`).
    pub(super) runnable: bool,
    /// The binding record exists but isn't execution metadata this build can
    /// read: not an object, or an `environment` that's neither a string nor
    /// null (which, like an absent one, means unbound).
    pub(super) malformed: bool,
    pub(super) sandboxes: i64,
}

/// Count every sandbox that isn't a deletion tombstone by environment binding
/// and lifecycle, as [`pplx_space_sandbox_store::runtime_population::sandbox_population`]
/// reads them.
pub(super) async fn sandbox_population(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SandboxPopulationRow>, sqlx::Error> {
    Ok(
        pplx_space_sandbox_store::runtime_population::sandbox_population(executor)
            .await?
            .into_iter()
            .map(SandboxPopulationRow::from)
            .collect(),
    )
}

impl From<pplx_space_sandbox_store::runtime_population::SandboxPopulationRow>
    for SandboxPopulationRow
{
    fn from(row: pplx_space_sandbox_store::runtime_population::SandboxPopulationRow) -> Self {
        let pplx_space_sandbox_store::runtime_population::SandboxPopulationRow {
            environment_ref,
            recorded,
            lifecycle,
            runnable,
            malformed,
            sandboxes,
        } = row;
        Self {
            environment_ref,
            recorded,
            lifecycle,
            runnable,
            malformed,
            sandboxes,
        }
    }
}

/// Template releases grouped by environment binding and whether anything
/// still creates sandboxes from them.
#[derive(Debug, FromRow)]
pub(super) struct TemplateReleasePopulationRow {
    /// Unparsed, like [`SandboxPopulationRow::environment_ref`]. `None` for
    /// a release built before environments were recorded.
    pub(super) environment_ref: Option<String>,
    /// Whether creates by tag run these releases, or an enabled pool config
    /// pins them, which keeps creating warm slots. Any other release is
    /// reachable only by pinning its exact hash. A create by tag runs the
    /// tagged digest's original
    /// (`pplx_space_template_release_original::digest_original`), whichever
    /// spelling the tag sits on, or, where creates boot the twin they resolve
    /// to, the build's resolved release
    /// (`pplx_space_template_release_store::resolved_release_sql`), which may
    /// be a twin; the original when no release of the build qualifies.
    /// Where that falls back to the original, the tagged release itself
    /// counts too, because an API image that predates the shared
    /// original-release rule falls back to it; drop that once no such image
    /// can serve or be rolled back to. Such an image's own resolved twin
    /// isn't counted: its resolver can break a spelling tie the other way,
    /// but both exclude the same blocked environments, so a block moves its
    /// creates rather than refusing them.
    pub(super) in_use: bool,
    /// As [`SandboxPopulationRow::malformed`].
    pub(super) malformed: bool,
    pub(super) template_releases: i64,
}

/// Count every template release by the environment it was built for and
/// whether it's in use. Resolution counts an unbound release as the initial
/// environment's; the rows still report it as unbound.
pub(super) async fn template_release_population(
    executor: impl sqlx::PgExecutor<'_>,
    boots_template_twins: bool,
) -> Result<Vec<TemplateReleasePopulationRow>, sqlx::Error> {
    static STATEMENT: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT tr.execution_metadata ->> 'environment' AS environment_ref,
                    (EXISTS (
                         SELECT 1
                         FROM template_release_tags tag
                         JOIN template_releases tagged ON tagged.id = tag.template_release_id
                         WHERE tagged.template_name = tr.template_name
                           -- Either spelling of the build's hash, as the
                           -- resolver and the digest original match it, in a
                           -- form the (template_name, hash, ...) index serves.
                           AND tagged.hash IN (
                               tr.hash,
                               replace(tr.hash, 'sha256:', ''),
                               'sha256:' || replace(tr.hash, 'sha256:', ''))
                           -- Creates booting twins boot the resolved twin;
                           -- otherwise, or with none, the digest's original,
                           -- or, on an API image that predates the shared
                           -- original-release rule, the tagged release
                           -- itself. Both fallbacks count while such an
                           -- image may serve or be rolled back to.
                           AND COALESCE(
                                   CASE WHEN $1 THEN ({resolved}) = tr.id END,
                                   original.is_digest_original OR tagged.id = tr.id
                               )
                     )
                     OR EXISTS (
                         SELECT 1 FROM kubernetes_pool_configs pool
                         WHERE pool.template_release_id = tr.id AND pool.enabled
                     )) AS in_use,
                    (tr.execution_metadata IS NOT NULL
                     AND (jsonb_typeof(tr.execution_metadata) <> 'object'
                          OR (tr.execution_metadata ? 'environment'
                              AND jsonb_typeof(tr.execution_metadata -> 'environment') NOT IN ('string', 'null'))))
                        AS malformed,
                    COUNT(*) AS template_releases
             FROM template_releases tr
             -- Once per release: whether it's its digest's original, the
             -- release a tag on either spelling resolves to.
             CROSS JOIN LATERAL (
                 SELECT {digest_original} AS is_digest_original
             ) original
             GROUP BY 1, 2, 3",
            digest_original = digest_original("tr"),
            resolved = pplx_space_template_release_store::resolved_release_sql(
                "tagged.template_name",
                "tagged.hash",
                "$2",
            ),
        ))
    });
    sqlx::query_as(sqlx::AssertSqlSafe(STATEMENT.as_str()))
        .bind(boots_template_twins)
        .bind(pplx_space_execution_catalog::catalog::initial_environment_reference().to_string())
        .fetch_all(executor)
        .await
}

/// Every environment a template release is bound to, an unbound release
/// counting as the initial environment's, as resolution and the campaign
/// driver count it. A binding that isn't a string comes back as its JSON
/// text, which no environment reference parses as. Takes no lock.
pub(super) async fn template_release_environments(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT DISTINCT COALESCE(execution_metadata ->> 'environment', $1)
         FROM template_releases"
    ))
    .bind(pplx_space_execution_catalog::catalog::initial_environment_reference().to_string())
    .fetch_all(executor)
    .await
}
