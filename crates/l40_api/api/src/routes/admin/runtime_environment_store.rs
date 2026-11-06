//! Observed-state reads behind the runtime environment admin routes: which
//! environment each sandbox and template release is bound to. What each node
//! reports enforcing comes from the admin node listing in `node_store`. The
//! policy itself is read and written only through
//! `pplx_space_runtime_environment_policy_store`.
//!
//! Every read here is observational. Nothing takes a lock, and a count can be
//! stale by the time the caller acts on it.

use pplx_space_common_core::sql;
use sqlx::FromRow;

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
    /// Whether a tag points to these releases, which creates by tag resolve
    /// to, or an enabled pool config pins them, which keeps creating warm
    /// slots. Any other release is reachable only by pinning its exact hash.
    pub(super) in_use: bool,
    /// As [`SandboxPopulationRow::malformed`].
    pub(super) malformed: bool,
    pub(super) template_releases: i64,
}

/// Count every template release by the environment it was built for and
/// whether it's in use.
pub(super) async fn template_release_population(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<TemplateReleasePopulationRow>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT tr.execution_metadata ->> 'environment' AS environment_ref,
                (EXISTS (
                     SELECT 1 FROM template_release_tags tag
                     WHERE tag.template_release_id = tr.id
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
         GROUP BY 1, 2, 3"
    ))
    .fetch_all(executor)
    .await
}
