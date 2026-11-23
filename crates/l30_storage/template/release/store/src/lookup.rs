//! Reads of releases for the api's creates, placement, pool configs and admin
//! routes. None takes a lock.

use pplx_space_accounts::OrgId;
use pplx_space_content_digest::HashAlg;
use pplx_space_execution_spec::{EnvironmentReference, RuntimeKind};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{
    TemplateArtifactDigestPrefix, TemplateDesiredState, TemplateName, TemplateVisibility,
};
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

/// A release a caller named by id, with its stored hash.
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
