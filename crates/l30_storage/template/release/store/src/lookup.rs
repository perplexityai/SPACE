//! Reads of releases for the api's creates, placement, pool claims and
//! configs, and admin routes. None takes a lock but
//! [`lock_release_of_present_template`], the admin tag move's, whose lock
//! lasts until the caller's transaction ends.

use pplx_space_accounts::OrgId;
use pplx_space_content_digest::HashAlg;
use std::sync::LazyLock;

use pplx_space_execution_spec::{EnvironmentReference, RuntimeKind};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{
    TemplateArtifactDigest, TemplateArtifactDigestPrefix, TemplateDesiredState, TemplateName,
    TemplateTag, TemplateVisibility,
};
use pplx_space_template_release_original::{digest_original, spelling_original};
use sqlx::PgExecutor;
use time::OffsetDateTime;
use uuid::Uuid;

/// A release of a template the caller may see, as the admin release list
/// reads it.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct VisibleRelease {
    pub id: Uuid,
    pub template_name: String,
    /// The release's `type`.
    pub runtime: String,
    /// The stored spelling of the hash, bare or `sha256:`.
    pub content_hash: String,
    pub created_at: OffsetDateTime,
}

/// A release's id and stored hash, as [`named_release`] and
/// [`pool_target_release`] read it.
#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct NamedRelease {
    pub id: Uuid,
    /// The stored spelling of the hash, bare or `sha256:`.
    pub content_hash: String,
}

/// The execution environment the release `release_id` is bound to, unparsed;
/// `None` for an unbound release or no release. This reads that release's
/// own row, which may be a twin, not the digest original's.
pub async fn release_environment_by_id(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT execution_metadata->>'environment'
         FROM template_releases
         WHERE id = $1"
    ))
    .bind(release_id)
    .fetch_optional(executor)
    .await?
    .flatten())
}

/// The environments outside the legacy cache partition that the releases of
/// `template_name` at `content_hash` are bound to, unparsed, under both
/// spellings of the digest, whichever one `content_hash` is. Placement reads
/// them only for a release that records no binding, so each is a twin's or,
/// where the digest is stored under both spellings, the other spelling's
/// original's: either way another release's build, which a holder of it
/// doesn't hold for this release. `initial_environment`, the initial
/// environment, is left out: an unbound release and one bound to it share the
/// legacy partition, so a holder bound to it holds the unbound release's
/// build. Both can exist only under different spellings of the digest.
pub async fn bound_release_environments(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    content_hash: &str,
    initial_environment: EnvironmentReference,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sql!(
        "SELECT DISTINCT r.execution_metadata->>'environment'
         FROM template_releases r
         WHERE r.template_name = $1
           AND r.hash IN ('sha256:' || replace($2, 'sha256:', ''), replace($2, 'sha256:', ''))
           AND r.execution_metadata->>'environment' IS NOT NULL
           AND r.execution_metadata->>'environment' <> $3"
    ))
    .bind(template_name)
    .bind(content_hash)
    .bind(initial_environment.to_string())
    .fetch_all(executor)
    .await
}

/// The stored hashes of `template_name`'s `runtime` releases that start with
/// `prefix`, each once, of a `present` template that is generic or owned by
/// `caller_org`. A hash stored as bare hex is a SHA-256 digest, so it matches
/// a SHA-256 prefix's hex. The hashes come back as stored; the caller parses
/// them.
pub async fn visible_release_hashes_with_prefix(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    prefix: &TemplateArtifactDigestPrefix,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Vec<String>, sqlx::Error> {
    let prefix = prefix.digest_prefix();
    let bare_sha256_prefix =
        (prefix.algorithm() == HashAlg::Sha256).then(|| prefix.hex_prefix().to_owned());
    sqlx::query_scalar::<_, String>(sql!(
        "SELECT DISTINCT r.hash
         FROM template_releases r
         JOIN templates tpl ON tpl.name = r.template_name
         WHERE r.template_name = $1
           AND (
               starts_with(r.hash, $2)
               OR (strpos(r.hash, ':') = 0 AND starts_with(r.hash, $7))
           )
           AND r.type = $3
           AND (tpl.visibility = $4 OR tpl.org_id = $5)
           AND tpl.desired_state = $6"
    ))
    .bind(template_name.as_str())
    .bind(prefix.to_string())
    .bind(runtime.alias())
    .bind(TemplateVisibility::Generic.as_str())
    .bind(caller_org.as_uuid())
    .bind(TemplateDesiredState::Present.as_str())
    .bind(bare_sha256_prefix)
    .fetch_all(executor)
    .await
}

/// Whether `template_name` has any release, whatever its runtime and tags, of
/// a `present` template that is generic or owned by `caller_org`.
pub async fn template_has_visible_release(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    caller_org: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM template_releases r
             JOIN templates tpl ON tpl.name = r.template_name
             WHERE r.template_name = $1
               AND (tpl.visibility = $2 OR tpl.org_id = $3)
               AND tpl.desired_state = $4
         )"
    ))
    .bind(template_name.as_str())
    .bind(TemplateVisibility::Generic.as_str())
    .bind(caller_org.as_uuid())
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_one(executor)
    .await
}

/// Every release of a `present` template that has no owning organization or
/// is owned by one of `org_ids`, by template name, then newest first. This
/// filters by owner, not by visibility, unlike
/// [`template_has_visible_release`]: a `generic` template owned by an
/// organization outside `org_ids` is left out.
pub async fn releases_unowned_or_owned_by(
    executor: impl PgExecutor<'_>,
    org_ids: &[OrgId],
) -> Result<Vec<VisibleRelease>, sqlx::Error> {
    sqlx::query_as::<_, VisibleRelease>(sql!(
        "SELECT r.id, r.template_name, r.type AS runtime, r.hash AS content_hash, r.created_at
         FROM template_releases r
         JOIN templates t ON t.name = r.template_name
         WHERE t.desired_state = $2
           AND (t.org_id IS NULL OR t.org_id = ANY($1))
         ORDER BY r.template_name ASC, r.created_at DESC"
    ))
    .bind(org_ids)
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_all(executor)
    .await
}

/// The release `release_id` names, when it's a release of `template_name`
/// (and stored under exactly `content_hash`, when given).
pub async fn named_release(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    template_name: &str,
    content_hash: Option<&str>,
) -> Result<Option<NamedRelease>, sqlx::Error> {
    sqlx::query_as::<_, NamedRelease>(sql!(
        "SELECT r.id, r.hash AS content_hash
         FROM template_releases r
         WHERE r.id = $1
           AND r.template_name = $2
           AND ($3::TEXT IS NULL OR r.hash = $3)"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(content_hash)
    .fetch_optional(executor)
    .await
}

/// The release a Kubernetes pool claim or config upsert targets: the
/// release matching `content_hash`, under either spelling, when one is
/// pinned, otherwise the release of the digest tagged `latest` (the tag may
/// sit on the other spelling's original when it predates the shared rule),
/// otherwise the newest release of the template. Only digest originals are
/// candidates (`pplx_space_template_release_original::digest_original`, the
/// release the create path and the warm-pool store resolve the digest to): a
/// twin bound to another environment is registered after its original and
/// would otherwise be the newest, and the other spelling's original of a
/// digest would hold slots no create claims. The hash comes back as stored.
pub async fn pool_target_release(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    content_hash: Option<&str>,
) -> Result<Option<NamedRelease>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.id, r.hash AS content_hash
             FROM template_releases r
             WHERE r.template_name = $1
               AND ($2::TEXT IS NULL
                    OR r.hash IN (replace($2, 'sha256:', ''),
                                  'sha256:' || replace($2, 'sha256:', '')))
               AND {digest_original}
             ORDER BY EXISTS (
                          SELECT 1
                          FROM template_release_tags t
                          JOIN template_releases tagged
                            ON tagged.id = t.template_release_id
                           AND tagged.template_name = t.template_name
                          WHERE t.template_name = r.template_name
                            AND t.tag = 'latest'
                            AND tagged.hash IN (replace(r.hash, 'sha256:', ''),
                                                'sha256:' || replace(r.hash, 'sha256:', ''))) DESC,
                      r.created_at DESC
             LIMIT 1",
            digest_original = digest_original("r"),
        ))
    });
    sqlx::query_as::<_, NamedRelease>(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(template_name)
        .bind(content_hash)
        .fetch_optional(executor)
        .await
}

/// Every environment a release is bound to, an unbound release counting as
/// `initial_environment`'s, as resolution and the campaign driver count it.
/// A binding that isn't a string comes back as its JSON text, which no
/// environment reference parses as.
pub async fn release_environments(
    executor: impl PgExecutor<'_>,
    initial_environment: EnvironmentReference,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT DISTINCT COALESCE(execution_metadata ->> 'environment', $1)
         FROM template_releases"
    ))
    .bind(initial_environment.to_string())
    .fetch_all(executor)
    .await
}

/// The release a create names by tag: the digest original
/// (`pplx_space_template_release_original::digest_original`) of the tagged
/// release's digest. Tag writers attach tags to the digest original, but a tag
/// written before the shared rule can sit on the other spelling's original,
/// and the warm pool only warms the digest original. The hash comes back as
/// the tagged release stores it, as before the shared rule: the create's
/// node inventory and lifecycle-default reads match it exactly, and a pool
/// claim matches either spelling.
pub async fn resolve_by_tag(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    tag: &TemplateTag,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.id, tagged.hash
             FROM template_release_tags t
             JOIN template_releases tagged
               ON tagged.id = t.template_release_id
              AND tagged.template_name = t.template_name
             JOIN template_releases r
               ON r.template_name = tagged.template_name
              AND r.hash IN (replace(tagged.hash, 'sha256:', ''),
                             'sha256:' || replace(tagged.hash, 'sha256:', ''))
             JOIN templates tpl
               ON tpl.name = t.template_name
             WHERE t.template_name = $1
               AND t.tag = $2
               AND r.type = $3
               AND tpl.desired_state = $6
               AND (tpl.visibility = $4 OR tpl.org_id = $5)
               AND {digest_original}",
            digest_original = digest_original("r"),
        ))
    });
    sqlx::query_as::<_, (Uuid, String)>(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(template_name.as_str())
        .bind(tag.as_str())
        .bind(runtime.alias())
        .bind(TemplateVisibility::Generic.as_str())
        .bind(caller_org.as_uuid())
        .bind(TemplateDesiredState::Present.as_str())
        .fetch_optional(executor)
        .await
}

/// The release a create or base resolution names by digest: the digest's
/// original release under either spelling
/// (`pplx_space_template_release_original::digest_original`, the rule the
/// internal API and the warm pool resolve it with), never a twin bound to
/// another environment.
pub async fn resolve_by_digest(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    digest: TemplateArtifactDigest,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.id
             FROM template_releases r
             JOIN templates tpl ON tpl.name = r.template_name
             WHERE r.template_name = $1
               AND r.hash IN ($2, replace($2, 'sha256:', ''))
               AND r.type = $3
               AND (tpl.visibility = $4 OR tpl.org_id = $5)
               AND tpl.desired_state = $6
               AND {digest_original}",
            digest_original = digest_original("r"),
        ))
    });
    let hash = digest.to_string();
    let release_id = sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(template_name.as_str())
        .bind(&hash)
        .bind(runtime.alias())
        .bind(TemplateVisibility::Generic.as_str())
        .bind(caller_org.as_uuid())
        .bind(TemplateDesiredState::Present.as_str())
        .fetch_optional(executor)
        .await?;
    // Legacy unprefixed rows matched the same digest; return its canonical form.
    Ok(release_id.map(|id| (id, hash)))
}

/// The execution environment the original release of `template_name` at
/// `content_hash` is bound to (its `execution_metadata.environment`), unparsed;
/// `None` for an unbound release or no release. `content_hash` may be either
/// spelling, and the release is the digest's original under either one, the
/// one [`resolve_by_digest`] resolves: its twins, bound to
/// other environments, are registered after it, and a create that names no
/// environment resolves the original.
///
/// Placement reads it as the binding of Direct's create, pool and pull
/// candidates whose own inventory row names none, of pull candidates that
/// hold nothing, and of a Kubernetes pod's template work, through
/// `environment_placement::work_environment`.
pub async fn original_release_environment(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    content_hash: &str,
) -> Result<Option<String>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.execution_metadata->>'environment'
             FROM template_releases r
             WHERE r.template_name = $1
               AND r.hash IN (replace($2, 'sha256:', ''),
                              'sha256:' || replace($2, 'sha256:', ''))
               AND {digest_original}",
            digest_original = digest_original("r"),
        ))
    });
    Ok(
        sqlx::query_scalar::<_, Option<String>>(sqlx::AssertSqlSafe(QUERY.as_str()))
            .bind(template_name)
            .bind(content_hash)
            .fetch_optional(executor)
            .await?
            .flatten(),
    )
}

/// Release counts by the environment the releases were built for and
/// whether creates or pools still reach them.
#[derive(Debug, sqlx::FromRow)]
pub struct ReleasePopulationRow {
    /// `execution_metadata ->> 'environment'`, unparsed; `None` for a
    /// release built before environments were recorded.
    pub environment_ref: Option<String>,
    /// Whether creates by tag run these releases, or an enabled pool config
    /// pins them. A create by tag runs the tagged digest's original
    /// (`pplx_space_template_release_original::digest_original`), whichever
    /// spelling the tag sits on, or, when `boots_template_twins`, the build's
    /// resolved release ([`crate::resolved_release_sql`]); the original when
    /// no release of the build qualifies. Where that falls back to the
    /// original, the tagged release itself counts too.
    pub in_use: bool,
    /// The release's execution metadata isn't an object, or binds an
    /// `environment` that's neither a string nor null.
    pub malformed: bool,
    pub template_releases: i64,
}

/// Count every release by the environment it was built for and whether it's
/// in use; see [`ReleasePopulationRow`]. Resolution counts an unbound release
/// as `initial_environment`'s; the rows still report it as unbound.
pub async fn release_population(
    executor: impl PgExecutor<'_>,
    boots_template_twins: bool,
    initial_environment: EnvironmentReference,
) -> Result<Vec<ReleasePopulationRow>, sqlx::Error> {
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
            resolved = crate::resolved_release_sql(
                "tagged.template_name",
                "tagged.hash",
                "$2",
            ),
        ))
    });
    sqlx::query_as(sqlx::AssertSqlSafe(STATEMENT.as_str()))
        .bind(boots_template_twins)
        .bind(initial_environment.to_string())
        .fetch_all(executor)
        .await
}

#[cfg(test)]
mod tests;

/// The release `release_id` when its template is present, or `None`,
/// locking the release's row `FOR UPDATE` until the caller's transaction
/// ends. The admin tag move locks the template's row first
/// (`pplx_space_template_store::templates::lock_no_key_update_of_release`).
pub async fn lock_release_of_present_template(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
) -> Result<Option<VisibleRelease>, sqlx::Error> {
    sqlx::query_as::<_, VisibleRelease>(sql!(
        "SELECT r.id, r.template_name, r.type AS runtime, r.hash AS content_hash, r.created_at
             FROM template_releases r
             JOIN templates t ON t.name = r.template_name
             WHERE r.id = $1
               AND t.desired_state = $2
             FOR UPDATE OF r"
    ))
    .bind(release_id)
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(&mut **tx)
    .await
}

/// The id of the original release of `template_name` at `content_hash`,
/// either spelling, under either spelling of the digest
/// (`pplx_space_template_release_original::digest_original`): the release
/// tags attach to, which [`original_release_environment`] reads the binding
/// of. `None` when no release has the digest.
pub async fn original_release_id(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    content_hash: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.id FROM template_releases r
             WHERE r.template_name = $1
               AND r.hash IN (replace($2, 'sha256:', ''),
                              'sha256:' || replace($2, 'sha256:', ''))
               AND {digest_original}",
            digest_original = digest_original("r"),
        ))
    });
    sqlx::query_scalar(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(template_name)
        .bind(content_hash)
        .fetch_optional(executor)
        .await
}

/// Whether release `release_id` is its spelling's original
/// (`pplx_space_template_release_original::spelling_original`) and has
/// twins: releases of the same template and exact hash registered after it.
/// The admin release delete refuses to delete such an original.
pub async fn release_has_twins(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static HAS_TWINS: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT EXISTS (
                     SELECT 1
                     FROM template_releases r
                     JOIN template_releases twin
                       ON twin.template_name = r.template_name
                      AND twin.hash = r.hash
                      AND (twin.created_at, twin.id) > (r.created_at, r.id)
                     WHERE r.id = $1
                       AND {spelling_original})",
            spelling_original = spelling_original("r"),
        ))
    });
    sqlx::query_scalar(sqlx::AssertSqlSafe(HAS_TWINS.as_str()))
        .bind(release_id)
        .fetch_one(executor)
        .await
}

/// Whether release `release_id` is tagged `latest`, or is its digest's
/// original (`pplx_space_template_release_original::digest_original`) while a
/// tag names another release of the same digest, under either spelling. The
/// admin release delete refuses to delete such a release.
pub async fn release_serves_a_tag(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static SERVES_A_TAG: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT EXISTS (
                     SELECT 1
                     FROM template_release_tags t
                     WHERE t.template_release_id = $1
                       AND t.tag = 'latest')
                 OR EXISTS (
                     SELECT 1
                     FROM template_releases r
                     JOIN template_releases tagged
                       ON tagged.template_name = r.template_name
                      AND tagged.hash IN (replace(r.hash, 'sha256:', ''),
                                          'sha256:' || replace(r.hash, 'sha256:', ''))
                     JOIN template_release_tags t
                       ON t.template_release_id = tagged.id
                     WHERE r.id = $1
                       AND tagged.id <> r.id
                       AND {digest_original})",
            digest_original = digest_original("r"),
        ))
    });
    sqlx::query_scalar(sqlx::AssertSqlSafe(SERVES_A_TAG.as_str()))
        .bind(release_id)
        .fetch_one(executor)
        .await
}
