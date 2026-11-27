use pplx_space_accounts::OrgId;
use pplx_space_content_digest::{ContentHash, HashAlg};
use pplx_space_execution_spec::RuntimeKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_template_model::{
    TemplateArtifactDigest, TemplateArtifactDigestPrefix, TemplateName, TemplateReference,
    TemplateSelector,
};

/// The digests of the template's `runtime` releases the caller may resolve
/// that start with `prefix`, each once in canonical `<algorithm>:<hex>` form. Same
/// visibility predicate as the tag and digest lookups
/// (`pplx_space_template_release_store::lookup`). A legacy row stored as bare
/// hex is a SHA-256 digest, as in
/// [`pplx_space_template_release_store::lookup::resolve_by_digest`].
pub(crate) async fn template_release_digests_with_prefix(
    executor: impl Executor<'_>,
    template_name: &TemplateName,
    prefix: &TemplateArtifactDigestPrefix,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Vec<TemplateArtifactDigest>, sqlx::Error> {
    let hashes = pplx_space_template_release_store::lookup::visible_release_hashes_with_prefix(
        executor,
        template_name,
        prefix,
        runtime,
        caller_org,
    )
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

impl From<DbError> for ResolveBaseError {
    fn from(error: DbError) -> Self {
        Self::from(sqlx::Error::from(error))
    }
}

pub(crate) async fn resolve_base(
    executor: impl Executor<'_>,
    reference: &TemplateReference,
    caller_org: OrgId,
) -> Result<ContentHash, ResolveBaseError> {
    let hash = match reference.selector() {
        TemplateSelector::Tag(tag) => pplx_space_template_release_store::lookup::resolve_by_tag(
            executor,
            reference.name(),
            tag,
            RuntimeKind::Firecracker,
            caller_org,
        )
        .await?
        .map(|(_, hash)| hash),
        TemplateSelector::ArtifactDigest(digest) => {
            pplx_space_template_release_store::lookup::resolve_by_digest(
                executor,
                reference.name(),
                *digest,
                RuntimeKind::Firecracker,
                caller_org,
            )
            .await?
            .map(|(_, hash)| hash)
        }
        TemplateSelector::ArtifactDigestPrefix(_) => return Err(ResolveBaseError::DigestPrefix),
    };
    let hash = hash.ok_or(ResolveBaseError::NotFound)?;
    Ok(hash.parse()?)
}
