//! The template read the organization-delete route needs: whether the
//! organization still owns template rows, and how long tombstone retention
//! keeps the deleted ones.

use pplx_space_accounts::OrgId;
use time::OffsetDateTime;

/// What an organization's `templates` rows are, as far as deleting the
/// organization goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OrgTemplateRows {
    /// The organization owns no template rows.
    None,
    /// At least one of them is not deleted.
    Live,
    /// Every one is deleted, and tombstone retention keeps the rows. It
    /// removes a deleted template a retention period after its delete was
    /// requested, once no template release and no node's inventory names it.
    /// `retained_until` is about when the last goes: `None` while a release
    /// or an inventory row still holds one, which no fixed time bounds.
    DeletedOnly {
        retained_until: Option<OffsetDateTime>,
    },
}

/// Classify `org_id`'s template rows; see [`OrgTemplateRows`]. The
/// conditions are the cluster-controller's tombstone retention's, which uses
/// [`pplx_space_sandbox_store::DELETED_SANDBOX_RETENTION`] for every tombstone
/// kind.
pub(super) async fn org_template_rows(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
) -> Result<OrgTemplateRows, sqlx::Error> {
    let pplx_space_template_store::templates::OrgTemplates {
        any_live: live,
        any_held: held,
        retained_until,
    } = pplx_space_template_store::templates::org_templates(
        executor,
        org_id,
        pplx_space_sandbox_store::DELETED_SANDBOX_RETENTION,
    )
    .await?;
    Ok(match (live, held, retained_until) {
        (Some(true), _, _) => OrgTemplateRows::Live,
        (Some(false), Some(true), _) => OrgTemplateRows::DeletedOnly {
            retained_until: None,
        },
        (Some(false), _, retained_until) => OrgTemplateRows::DeletedOnly { retained_until },
        (None, _, _) => OrgTemplateRows::None,
    })
}

/// Postgres-backed regression tests for the visibility-flip guards and the
/// narrowing-flip compat prune, through the store verbs the handler calls.
#[cfg(test)]
mod tests;
