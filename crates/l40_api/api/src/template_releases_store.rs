use pplx_space_accounts::OrgId;
use pplx_space_content_digest::{ContentHash, HashAlg};
use pplx_space_execution_spec::RuntimeKind;
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{
    TemplateArtifactDigest, TemplateArtifactDigestPrefix, TemplateDesiredState, TemplateName,
    TemplateReference, TemplateSelector, TemplateTag, TemplateVisibility,
};
use sqlx::PgExecutor;
use uuid::Uuid;

pub(crate) async fn resolve_template_release_by_tag(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    tag: &TemplateTag,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    sqlx::query_as::<_, (Uuid, String)>(sql!(
        "SELECT r.id, r.hash
         FROM template_release_tags t
         JOIN template_releases r
           ON r.id = t.template_release_id
          AND r.template_name = t.template_name
         JOIN templates tpl
           ON tpl.name = t.template_name
         WHERE t.template_name = $1
           AND t.tag = $2
           AND r.type = $3
           AND tpl.desired_state = $6
           AND (tpl.visibility = $4 OR tpl.org_id = $5)"
    ))
    .bind(template_name.as_str())
    .bind(tag.as_str())
    .bind(runtime.alias())
    .bind(TemplateVisibility::Generic.as_str())
    .bind(caller_org.as_uuid())
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(executor)
    .await
}

/// The release a create or base resolution names by digest: the original
/// release of the digest (the first registered), never a twin bound to
/// another environment, and the canonical spelling over a legacy bare one.
pub(crate) async fn resolve_template_release_by_digest(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    digest: TemplateArtifactDigest,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    let hash = digest.to_string();
    let release_id = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT r.id
         FROM template_releases r
         JOIN templates tpl ON tpl.name = r.template_name
         WHERE r.template_name = $1
           AND r.hash IN ($2, replace($2, 'sha256:', ''))
           AND r.type = $3
           AND (tpl.visibility = $4 OR tpl.org_id = $5)
           AND tpl.desired_state = $6
         ORDER BY (r.hash = $2) DESC, r.created_at ASC, r.id ASC
         LIMIT 1"
    ))
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
/// `None` for an unbound release or no release. A legacy unprefixed hash
/// matches the way [`resolve_template_release_by_digest`] matches it. The
/// original release is the first registered for the hash: its twins, bound to
/// other environments, are registered after it, and a create that names no
/// environment resolves the original.
///
/// Placement reads it as the binding of Direct's create, pool and pull
/// candidates whose own inventory row names none, of pull candidates that
/// hold nothing, and of a Kubernetes pod's template work, through
/// `environment_placement::work_environment`.
pub(crate) async fn release_environment(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    content_hash: &str,
) -> Result<Option<String>, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT r.execution_metadata->>'environment'
         FROM template_releases r
         WHERE r.template_name = $1
           AND r.hash IN ($2, replace($2, 'sha256:', ''))
         ORDER BY (r.hash = $2) DESC, r.created_at ASC, r.id ASC
         LIMIT 1"
    ))
    .bind(template_name)
    .bind(content_hash)
    .fetch_optional(executor)
    .await?
    .flatten())
}

/// The execution environment the release `release_id` is bound to, unparsed;
/// `None` for an unbound release or no release. Unlike
/// [`release_environment`], this reads the chosen release's own row, which
/// may be a twin.
#[cfg(feature = "kubernetes")]
pub(crate) async fn release_environment_by_id(
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
/// spellings of the digest, whichever one `content_hash` is. Placement reads them only for a release that
/// records no binding, so each is a twin's or, where the digest is stored
/// under both spellings, the other spelling's original's: either way another
/// release's build, which a holder of it doesn't hold for this release. The
/// initial environment is left
/// out: an unbound release and one bound to the initial environment share the
/// legacy partition, so a holder bound to it holds the unbound release's
/// build. Both can exist only under different spellings of the digest.
pub(crate) async fn bound_release_environments(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    content_hash: &str,
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
    .bind(pplx_space_execution_catalog::catalog::initial_environment_reference().to_string())
    .fetch_all(executor)
    .await
}

/// The digests of the template's `runtime` releases the caller may resolve
/// that start with `prefix`, each once in canonical `<algorithm>:<hex>` form. Same
/// visibility predicate as the lookups above. A legacy row stored as bare
/// hex is a SHA-256 digest, as in [`resolve_template_release_by_digest`].
pub(crate) async fn template_release_digests_with_prefix(
    executor: impl PgExecutor<'_>,
    template_name: &TemplateName,
    prefix: &TemplateArtifactDigestPrefix,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Vec<TemplateArtifactDigest>, sqlx::Error> {
    let prefix = prefix.digest_prefix();
    let bare_sha256_prefix =
        (prefix.algorithm() == HashAlg::Sha256).then(|| prefix.hex_prefix().to_owned());
    let hashes = sqlx::query_scalar::<_, String>(sql!(
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
    .await?;
    // A stored hash that does not parse cannot be what the caller named. A
    // digest stored in both forms is one candidate, not an ambiguity.
    let digests: std::collections::BTreeSet<TemplateArtifactDigest> = hashes
        .iter()
        .filter_map(|hash| {
            if hash.contains(':') {
                hash.parse().ok()
            } else {
                format!("{}:{hash}", HashAlg::Sha256).parse().ok()
            }
        })
        .collect();
    Ok(digests.into_iter().collect())
}

/// Whether the template has any release the caller may resolve, whatever its
/// runtime and tags. Same visibility predicate as the lookups above.
pub(crate) async fn template_has_visible_release(
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveBaseError {
    #[error("base template not found")]
    NotFound,
    #[error("base template requires a tag or full digest")]
    DigestPrefix,
    #[error("resolve base template: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid stored base digest: {0}")]
    Digest(#[from] pplx_space_content_digest::DigestParseError),
}

pub(crate) async fn resolve_base(
    executor: impl PgExecutor<'_>,
    reference: &TemplateReference,
    caller_org: OrgId,
) -> Result<ContentHash, ResolveBaseError> {
    let hash = match reference.selector() {
        TemplateSelector::Tag(tag) => resolve_template_release_by_tag(
            executor,
            reference.name(),
            tag,
            RuntimeKind::Firecracker,
            caller_org,
        )
        .await?
        .map(|(_, hash)| hash),
        TemplateSelector::ArtifactDigest(digest) => resolve_template_release_by_digest(
            executor,
            reference.name(),
            *digest,
            RuntimeKind::Firecracker,
            caller_org,
        )
        .await?
        .map(|(_, hash)| hash),
        TemplateSelector::ArtifactDigestPrefix(_) => return Err(ResolveBaseError::DigestPrefix),
    };
    let hash = hash.ok_or(ResolveBaseError::NotFound)?;
    Ok(hash.parse()?)
}
