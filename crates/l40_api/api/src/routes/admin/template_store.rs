//! Postgres-side queries that back the `/admin/v1/templates/{name}:visibility`
//! admin endpoint, and the template reads other admin routes need.
//!
//! The handler in [`super::templates`] stays focused on HTTP, auth, the flip
//! guards, and audit logging; the SQL strings and transaction-bound helpers
//! live here. The flip runs entirely under a `FOR UPDATE` lock on the
//! template row so two operators cannot race the in-use guards.

use pplx_space_accounts::OrgId;
use pplx_space_template_model::{TemplateName, TemplateVisibility};
use sqlx::{Postgres, Transaction};
use time::OffsetDateTime;

/// Whether a template row named `template_name` exists and is not deleted:
/// the rows the template read routes list.
pub(super) async fn template_exists(
    executor: impl sqlx::PgExecutor<'_>,
    template_name: &TemplateName,
) -> Result<bool, sqlx::Error> {
    pplx_space_template_store::templates::exists_not_deleted(executor, template_name).await
}

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

pub(super) type TemplateVisibilityRow = pplx_space_template_store::templates::VisibilityOwner;

/// `SELECT ... FOR UPDATE` — the visibility write and its guards share one
/// lock window on the template row.
pub(super) async fn lock_template_for_update(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
) -> Result<Option<TemplateVisibilityRow>, sqlx::Error> {
    pplx_space_template_store::templates::lock_visibility(tx, template_name).await
}

/// Sandboxes still referencing the template: anything not terminally gone
/// (`failed` / `deleted`). Suspended sandboxes count —
/// they can be restored later, and a restore after a narrowing flip would
/// cross the isolation boundary the flip is trying to establish.
///
/// Scope of the guard: the template row lock serializes this count against
/// the *admin* surface (compat edits take the matching shared lock), but the
/// sandbox-create path does not yet read the template row under a lock, so a
/// create committing between this count and the flip's commit can slip past
/// it. Closing that window is the create-side half of the RFC's
/// create-vs-flip lock (`FOR SHARE` across the placement decision), which
/// lands with the create-time authorization gate; until then this guard is
/// best-effort against concurrent creates.
pub(super) async fn template_sandboxes_in_use_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
) -> Result<i64, sqlx::Error> {
    // `failed` is not unconditionally terminal: the failed-restorable repair
    // loop reverts a `failed` row that still has a provably-safe durable
    // suspend snapshot back to `suspended`, from where a user restore would
    // cross the isolation boundary a narrowing flip establishes. Count such
    // recoverable-failed rows as in-use, reusing the same snapshot-eligibility
    // predicate the repair loop selects on — it is clock-safe for the poisoned
    // lifecycle/placement stamps a failed transition leaves behind, which the
    // placement-freshness snapshot predicates are not. A `failed` row with no
    // restorable snapshot stays excluded: it is genuinely gone.
    pplx_space_sandbox_store::template_usage::template_sandboxes_in_use(&mut **tx, template_name)
        .await
}

/// Nodes still holding the template's rootfs in their local cache
/// (`node_template_inventory`). Consulted on a narrowing flip: cached rootfs
/// on shared (or another org's) nodes is exactly the residue the
/// data-isolation guarantee forbids. v1 is conservative — any cached copy
/// blocks the narrow, including copies already on the owner's dedicated
/// nodes.
pub(super) async fn template_cached_rootfs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
) -> Result<i64, sqlx::Error> {
    pplx_space_node_template_inventory_store::template_inventory_rows(&mut **tx, template_name)
        .await
}

/// Write the new visibility, assigning an owner in the same statement when
/// `new_org_id` is supplied (the `org_specific_requires_owner`
/// CHECK demands owner and visibility move together for previously
/// ownerless templates).
pub(super) async fn update_template_visibility_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
    visibility: TemplateVisibility,
    new_org_id: Option<OrgId>,
) -> Result<(), sqlx::Error> {
    pplx_space_template_store::templates::update_visibility(
        &mut **tx,
        template_name,
        visibility,
        new_org_id,
    )
    .await
}

/// After a narrowing flip to owner `owner_org_id`, allowlist rows naming
/// this template in any *other* org's group become illegal under the hard
/// cross-org rule. Prune them in the same transaction and report how many
/// were removed. (Generic groups cannot have compat rows at all — the
/// `restricted` policy is org-only — so every pruned row belonged to another
/// org's group.)
pub(super) async fn prune_cross_org_compat_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
    owner_org_id: OrgId,
) -> Result<u64, sqlx::Error> {
    pplx_space_node_group_store::prune_cross_org_template_compat(
        &mut **tx,
        template_name,
        owner_org_id,
    )
    .await
}

/// Postgres-backed regression tests for the visibility-flip guards and the
/// narrowing-flip compat prune.
#[cfg(test)]
mod tests;
