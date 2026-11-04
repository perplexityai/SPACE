use pplx_space_common_core::{
    sql,
    types::{
        ContentHash, OrgId, RuntimeKind, TemplateArtifactDigest, TemplateDesiredState,
        TemplateName, TemplateReference, TemplateSelector, TemplateTag, TemplateVisibility,
    },
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
         ORDER BY (r.hash = $2) DESC
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
    Digest(#[from] pplx_space_common_core::types::DigestParseError),
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
