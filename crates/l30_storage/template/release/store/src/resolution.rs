//! Which release of a template build a create runs: the one rule every
//! caller applies, as one SQL fragment.
//!
//! A build (`template_name`, `hash`) has its original release and, once a
//! runtime upgrade campaign has moved it, twins bound to other execution
//! environments. A create resolves, in order:
//!
//! 1. the release bound to the cluster default
//!    (`runtime_environment_policy.default_environment_ref`);
//! 2. else a twin in an `allowed` environment, the one whose catalog entry
//!    (`runtime_environments.version_sort_key`) sorts highest;
//! 3. else a twin in a `deprecated` environment, the same way;
//! 4. else the original.
//!
//! Never a release whose environment is `blocked`, and never a twin whose
//! environment has no state at all. A release recorded without an
//! environment counts as the initial environment's, whose legacy partition
//! nodes cache it in, for both its state and the default match. When nothing
//! qualifies there is no resolved release; callers keep the release their tag
//! or digest named, as before twins. That release can itself be in a
//! `blocked` environment (no resolved release is also what a build whose
//! every release is blocked gets), so a caller that keeps it still applies
//! its own block check.
//!
//! After an E0 to E1 campaign, moving the default to E2 keeps creates on the
//! E1 twins until each build's E2 twin exists, rather than sending them back
//! to the E0 originals.

use std::sync::LazyLock;

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// A scalar subquery evaluating to the id of the release a create of
/// `template_name` at `hash` resolves to, or `NULL` when none qualifies.
///
/// Each argument is a SQL expression the caller's statement binds or names:
/// `template_name` and `hash` text (either spelling, bare or `sha256:<hex>`,
/// for SHA-256 digests; releases under both spellings of the build are
/// candidates, excluding releases without storage whenever any release of the
/// build owns a storage location; those in the environment of the build's
/// preferred original — its earliest release, preferring one that owns a
/// storage location — are originals; and between candidates of the same
/// rank and catalog version the spelling of the build's digest original
/// (`pplx_space_template_release_original::digest_original`, which tags and
/// the warm pool resolve the digest to) wins, whichever spelling `hash` is), and
/// `initial_environment` the initial environment's reference as text, which
/// unbound releases count as.
/// The fragment's own aliases start with `resolve_`, and its tables have
/// columns such as `template_name`, `hash`, `id` and `state`, so an
/// unqualified column name as an argument binds to the fragment's table, not
/// the caller's: pass placeholders or columns qualified by the caller's alias.
///
/// Interpolate the returned text whole, once per statement, into a statement
/// built once (a `LazyLock<String>`), with every value bound: it's the single
/// definition of the rule, so the create path, pod placement, pool claims,
/// the warm-pool autoscaler and the environment block guard agree.
pub fn resolved_release_sql(template_name: &str, hash: &str, initial_environment: &str) -> String {
    let candidate_spellings = sha256_spellings("resolve_r.hash");
    let query_spellings = sha256_spellings(hash);
    let digest_original = pplx_space_template_release_original::digest_original("resolve_d_r");
    format!(
        "(SELECT resolve_r.id
          FROM template_releases resolve_r
          JOIN runtime_environment_policy resolve_p ON resolve_p.singleton
          LEFT JOIN runtime_environment_states resolve_s
            ON resolve_s.environment_ref =
               COALESCE(resolve_r.execution_metadata->>'environment', ({initial_environment}))
          LEFT JOIN runtime_environments resolve_c
            ON resolve_c.environment_ref =
               COALESCE(resolve_r.execution_metadata->>'environment', ({initial_environment}))
          -- The spelling of the build's digest original
          -- (`pplx_space_template_release_original::digest_original`), the
          -- release a lookup without an environment, a tag and the warm pool
          -- resolve the digest to: it breaks ties between spellings.
          LEFT JOIN LATERAL (
              SELECT resolve_d_r.hash
              FROM template_releases resolve_d_r
              WHERE resolve_d_r.template_name = resolve_r.template_name
                AND resolve_d_r.hash IN {candidate_spellings}
                AND {digest_original}
              LIMIT 1
          ) resolve_d ON TRUE
          CROSS JOIN LATERAL (
              SELECT
                  COALESCE(resolve_r.execution_metadata->>'environment', ({initial_environment}))
                      = resolve_p.default_environment_ref AS is_default,
                  COALESCE(resolve_r.execution_metadata->>'environment', ({initial_environment}))
                      IS NOT DISTINCT FROM (
                          SELECT COALESCE(
                              resolve_o.execution_metadata->>'environment',
                              ({initial_environment}))
                          FROM template_releases resolve_o
                          WHERE resolve_o.template_name = resolve_r.template_name
                            AND resolve_o.hash IN {candidate_spellings}
                          ORDER BY
                              EXISTS (
                                  SELECT 1
                                  FROM template_release_storage_locations resolve_ol
                                  WHERE resolve_ol.template_release_id = resolve_o.id) DESC,
                              resolve_o.created_at,
                              resolve_o.id
                          LIMIT 1) AS is_original
          ) resolve_k
          WHERE resolve_r.template_name = ({template_name})
            AND resolve_r.hash IN {query_spellings}
            AND (EXISTS (
                     SELECT 1
                     FROM template_release_storage_locations resolve_l
                     WHERE resolve_l.template_release_id = resolve_r.id)
                 OR NOT EXISTS (
                     SELECT 1
                     FROM template_releases resolve_any
                     JOIN template_release_storage_locations resolve_al
                       ON resolve_al.template_release_id = resolve_any.id
                     WHERE resolve_any.template_name = resolve_r.template_name
                       AND resolve_any.hash IN {candidate_spellings}))
            AND resolve_s.state IS DISTINCT FROM 'blocked'
            AND (resolve_k.is_default
                 OR resolve_k.is_original
                 OR resolve_s.state IN ('allowed', 'deprecated'))
          ORDER BY
              CASE
                  WHEN resolve_k.is_default THEN 0
                  WHEN resolve_k.is_original THEN 3
                  WHEN resolve_s.state = 'allowed' THEN 1
                  ELSE 2
              END,
              resolve_c.version_sort_key DESC NULLS LAST,
              (resolve_r.hash = resolve_d.hash) DESC,
              resolve_r.created_at,
              resolve_r.id
          LIMIT 1)"
    )
}

/// A SQL `IN` list matching `expr` and, when `expr` is a lowercase SHA-256
/// digest in bare or `sha256:` form, its other spelling.
fn sha256_spellings(expr: &str) -> String {
    format!(
        "(({expr}),
          CASE
              WHEN ({expr}) ~ '^sha256:[0-9a-f]{{64}}$' THEN substr(({expr}), 8)
              ELSE ({expr})
          END,
          CASE
              WHEN ({expr}) ~ '^[0-9a-f]{{64}}$' THEN 'sha256:' || ({expr})
              ELSE ({expr})
          END)"
    )
}

/// The statement [`resolved_release`] runs, built once; the plan tests
/// explain it.
pub(crate) static RESOLVED_RELEASE: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "SELECT r.id, r.execution_metadata->>'environment'
         FROM template_releases r
         WHERE r.id = {resolved}",
        resolved = resolved_release_sql("$1", "$2", "$3"),
    ))
});

/// The release a create of `template_name` at `hash` resolves to
/// ([`resolved_release_sql`]), with the environment it records, unparsed;
/// `None` when no release qualifies. `initial_environment` is the initial
/// environment, which unbound releases count as. Takes no lock.
pub async fn resolved_release(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    hash: &str,
    initial_environment: EnvironmentReference,
) -> Result<Option<(Uuid, Option<String>)>, sqlx::Error> {
    sqlx::query_as::<_, (Uuid, Option<String>)>(sqlx::AssertSqlSafe(RESOLVED_RELEASE.as_str()))
        .bind(template_name)
        .bind(hash)
        .bind(initial_environment.to_string())
        .fetch_optional(executor)
        .await
}
