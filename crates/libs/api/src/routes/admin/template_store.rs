//! Postgres-side queries that back the `/admin/v1/templates/{name}:visibility`
//! admin endpoint, and the template reads other admin routes need.
//!
//! The handler in [`super::templates`] stays focused on HTTP, auth, the flip
//! guards, and audit logging; the SQL strings and transaction-bound helpers
//! live here. The flip runs entirely under a `FOR UPDATE` lock on the
//! template row so two operators cannot race the in-use guards.

use pplx_space_common_core::types::OrgId;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{TemplateDesiredState, TemplateName, TemplateVisibility};
use pplx_space_sandbox_portability::failed_repair_revert_eligibility;
use sqlx::{FromRow, Postgres, Transaction};
use time::OffsetDateTime;

/// Whether a template row named `template_name` exists and is not deleted:
/// the rows the template read routes list.
pub(super) async fn template_exists(
    executor: impl sqlx::PgExecutor<'_>,
    template_name: &TemplateName,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM templates WHERE name = $1 AND desired_state <> $2)"
    ))
    .bind(template_name.as_str())
    .bind(TemplateDesiredState::Deleted.as_str())
    .fetch_one(executor)
    .await
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
    let (live, held, retained_until) =
        sqlx::query_as::<_, (Option<bool>, Option<bool>, Option<OffsetDateTime>)>(sql!(
            "SELECT bool_or(t.desired_state <> $2),
                    bool_or(
                        EXISTS (
                            SELECT 1 FROM template_releases r WHERE r.template_name = t.name
                        )
                        OR EXISTS (
                            SELECT 1
                              FROM node_template_inventory i
                             WHERE i.template_name = t.name
                        )
                    ),
                    MAX(COALESCE(t.delete_requested_at, t.updated_at, t.created_at))
                        + make_interval(secs => $3)
               FROM templates t
              WHERE t.org_id = $1"
        ))
        .bind(org_id)
        .bind(TemplateDesiredState::Deleted.as_str())
        .bind(pplx_space_sandbox_store::DELETED_SANDBOX_RETENTION.as_secs_f64())
        .fetch_one(executor)
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

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplateVisibilityRow {
    pub(super) name: String,
    pub(super) org_id: Option<OrgId>,
    pub(super) visibility: TemplateVisibility,
}

/// `SELECT ... FOR UPDATE` — the visibility write and its guards share one
/// lock window on the template row.
pub(super) async fn lock_template_for_update(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
) -> Result<Option<TemplateVisibilityRow>, sqlx::Error> {
    sqlx::query_as::<_, TemplateVisibilityRow>(sql!(
        "SELECT name, org_id, visibility FROM templates WHERE name = $1 FOR UPDATE"
    ))
    .bind(template_name)
    .fetch_optional(&mut **tx)
    .await
}

/// Sandboxes still referencing the template: anything not terminally gone
/// (`failed` / `cleanup_required` / `deleted`). Suspended sandboxes count —
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
    let eligible = failed_repair_revert_eligibility("s").eligible;
    let query = format!(
        "SELECT COUNT(*)::BIGINT
           FROM sandboxes s
          WHERE s.template_id = $1
            AND (
                 s.lifecycle NOT IN ('failed', 'cleanup_required', 'deleted')
                 OR (s.lifecycle = 'failed' AND {eligible})
            )"
    );
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(template_name)
        .fetch_one(&mut **tx)
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
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)::BIGINT FROM node_template_inventory WHERE template_name = $1"
    ))
    .bind(template_name)
    .fetch_one(&mut **tx)
    .await
}

/// `true` when the template carries an owner-scoped encryption sidecar —
/// either the per-template DEK (`template_encrypted_fields`) or a per-release
/// storage-location DEK (`template_release_storage_location_encrypted_fields`,
/// reached through the release its location belongs to). Widening such a
/// template to `generic` would publish content wrapped under one org's key to
/// every org — the "launchable-but-undecryptable" state the org-scoped
/// templates RFC forbids. The re-key-on-widen transition is deferred to the
/// encryption follow-up, so v1 refuses the widen rather than performing it.
///
/// Both sidecar tables are credential-fenced: `space_user` (the general
/// application pool the visibility transaction runs on) has no grant on them,
/// so this existence check runs on the credential pool as its own read rather
/// than inside the flip transaction. The sidecars are written at
/// template-encrypt time and do not change during a visibility flip, so an
/// unlocked read here is consistent with the flip's other best-effort
/// quiescence guards.
pub(super) async fn template_has_owner_encryption(
    credentials_pool: &sqlx::PgPool,
    template_name: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                 SELECT 1 FROM template_encrypted_fields WHERE template_name = $1
             )
             OR EXISTS (
                 SELECT 1
                   FROM template_release_storage_location_encrypted_fields e
                   JOIN template_release_storage_locations l
                     ON l.id = e.template_release_storage_location_id
                   JOIN template_releases r
                     ON r.id = l.template_release_id
                  WHERE r.template_name = $1
             )"
    ))
    .bind(template_name)
    .fetch_one(credentials_pool)
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
    sqlx::query(sql!(
        "UPDATE templates
            SET visibility = $2,
                org_id = COALESCE($3, org_id)
          WHERE name = $1"
    ))
    .bind(template_name)
    .bind(visibility)
    .bind(new_org_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
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
    let result = sqlx::query(sql!(
        "DELETE FROM node_group_template_compat c
          USING node_groups g
          WHERE c.node_group_id = g.id
            AND c.template_name = $1
            AND g.org_id IS DISTINCT FROM $2"
    ))
    .bind(template_name)
    .bind(owner_org_id)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

/// Postgres-backed regression tests for the visibility-flip guards and the
/// narrowing-flip compat prune.
#[cfg(test)]
mod tests;
