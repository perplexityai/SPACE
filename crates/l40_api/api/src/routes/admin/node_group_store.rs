//! Postgres-side queries that back the `/admin/v1/node-groups` admin
//! endpoints.
//!
//! Handlers in [`super::node_groups`] stay focused on HTTP, auth, invariant
//! orchestration, and audit logging; the SQL strings, row structs, and
//! transaction-bound helpers live here. Multi-step guarded writes (assign-org,
//! set-default, delete) lock the group row `FOR UPDATE` and run their residue
//! checks inside the same transaction so two operators cannot race a guard.

use pplx_space_common_core::types::OrgId;

use std::collections::HashMap;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeGroupId;
use pplx_space_common_core::types::{NodeGroupName, NodeGroupTemplatePolicy, TemplateVisibility};
use pplx_space_common_svc::sql::sandbox_residency::non_resident_lifecycle_exclusion;
use pplx_space_sandbox_portability::{
    inflight_suspend_upload_from_node_exists, snapshot_durable_for_source_release,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use time::OffsetDateTime;

use super::node_store::SandboxOccupancy;

#[derive(Debug, Clone, FromRow)]
pub(super) struct NodeGroupRow {
    pub(super) id: NodeGroupId,
    pub(super) name: NodeGroupName,
    pub(super) org_id: Option<OrgId>,
    pub(super) template_policy: NodeGroupTemplatePolicy,
    pub(super) is_default: bool,
    pub(super) created_at: OffsetDateTime,
    pub(super) nodes_count: i64,
}

/// Template fields consulted when validating a `restricted` allowlist entry.
#[derive(Debug, Clone, FromRow)]
pub(super) struct CompatTemplateRow {
    pub(super) name: String,
    pub(super) visibility: TemplateVisibility,
    pub(super) org_id: Option<OrgId>,
}

// `nodes_count` counts worker rows only — node groups are worker pools, and
// builder/cluster rows always sit at `node_group_id` NULL because the mirror
// stamps workers only. For the singleton default group the count also
// includes NULL-membership workers: an unlabeled (or unknown-label) worker
// mirrors as NULL, which the data model resolves to the default generic
// group — without the `is_default` arm the shared pool would report zero
// nodes until every worker is explicitly labeled. (A correlated subquery
// rather than a GROUP BY join because this SELECT also backs
// `FOR UPDATE OF g`, which Postgres forbids alongside GROUP BY.)
const NODE_GROUP_SELECT: &str = "SELECT g.id,
       g.name,
       g.org_id,
       g.template_policy,
       g.is_default,
       g.created_at,
       (SELECT COUNT(*) FROM nodes n
         WHERE n.role = 'worker'
           AND (n.node_group_id = g.id
                OR (g.is_default AND n.node_group_id IS NULL)))::BIGINT AS nodes_count
FROM node_groups g";

/// Pool-bound repository for node-group reads. Guarded writes that need a
/// `FOR UPDATE` lock live as free functions below, since a `Transaction`
/// cannot be held inside `&self`.
#[derive(Clone)]
pub(super) struct AdminNodeGroupStore {
    pool: PgPool,
}

impl AdminNodeGroupStore {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn fetch(
        &self,
        node_group_id: NodeGroupId,
    ) -> Result<Option<NodeGroupRow>, sqlx::Error> {
        sqlx::query_as::<_, NodeGroupRow>(sqlx::AssertSqlSafe(sql!(format!(
            "{NODE_GROUP_SELECT}\nWHERE g.id = $1"
        ))))
        .bind(node_group_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub(super) async fn list(&self) -> Result<Vec<NodeGroupRow>, sqlx::Error> {
        sqlx::query_as::<_, NodeGroupRow>(sqlx::AssertSqlSafe(sql!(format!(
            "{NODE_GROUP_SELECT}\nORDER BY g.name ASC"
        ))))
        .fetch_all(&self.pool)
        .await
    }

    /// Allowlist template names per group in one round trip. Groups with no
    /// compat rows do not appear in the returned map.
    pub(super) async fn compat_templates_for(
        &self,
        node_group_ids: &[NodeGroupId],
    ) -> Result<HashMap<NodeGroupId, Vec<String>>, sqlx::Error> {
        if node_group_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<(NodeGroupId, String)> = sqlx::query_as(sql!(
            "SELECT node_group_id, template_name
               FROM node_group_template_compat
              WHERE node_group_id = ANY($1::UUID[])
              ORDER BY template_name ASC"
        ))
        .bind(node_group_ids)
        .fetch_all(&self.pool)
        .await?;
        let mut map: HashMap<NodeGroupId, Vec<String>> = HashMap::new();
        for (group_id, template_name) in rows {
            map.entry(group_id).or_default().push(template_name);
        }
        Ok(map)
    }
}

/// Returns the violated constraint name for a database error, so handlers
/// can map known unique/check violations onto friendly conflict responses
/// instead of a blanket 500.
pub(super) fn violated_constraint(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db) => db.constraint().map(str::to_string),
        _ => None,
    }
}

/// Whether any generic (shared) node group exists.
///
/// Asked before creating an org-owned group, because a dedicated group created
/// on an empty table latches the cluster into a state nothing recovers from:
/// the labeler's `ensure_default_group` seeds the default generic group only
/// while no non-default group exists, so the dedicated row it finds afterwards
/// blocks the seeding permanently, and no admin route can clear it (the default
/// cannot be deleted or un-defaulted). See the caller.
pub(super) async fn generic_node_group_exists(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_groups WHERE org_id IS NULL"
    ))
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

/// Insert a new group. The id is an application-generated UUIDv7; all
/// cross-field invariants (policy requires org, default must be generic) are
/// pre-validated by the handler, so a CHECK violation here is a bug — only
/// the unique violations (name, singleton default) are expected runtime
/// conflicts.
pub(super) async fn insert_node_group(
    pool: &PgPool,
    name: &NodeGroupName,
    org_id: Option<OrgId>,
    template_policy: NodeGroupTemplatePolicy,
    is_default: bool,
) -> Result<NodeGroupId, sqlx::Error> {
    let id = NodeGroupId::new();
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(id)
    .bind(name)
    .bind(org_id)
    .bind(template_policy)
    .bind(is_default)
    .execute(pool)
    .await?;
    Ok(id)
}

/// `SELECT ... FOR UPDATE OF g` — guarded writes lock the group row for the
/// duration of the validate + write window so two operators cannot race
/// residue guards or the singleton-default move.
pub(super) async fn lock_group_for_update(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
) -> Result<Option<NodeGroupRow>, sqlx::Error> {
    sqlx::query_as::<_, NodeGroupRow>(sqlx::AssertSqlSafe(sql!(format!(
        "{NODE_GROUP_SELECT}\nWHERE g.id = $1\nFOR UPDATE OF g"
    ))))
    .bind(node_group_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Lock the current default group's row (if any) and return its id.
///
/// `:set-default` locks the incumbent **first**, before the target, so
/// concurrent moves serialize on one row in a consistent order instead of
/// racing the single-default unique index.
pub(super) async fn lock_current_default_for_update(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<NodeGroupId>, sqlx::Error> {
    sqlx::query_scalar::<_, NodeGroupId>(sql!(
        "SELECT id FROM node_groups WHERE is_default FOR UPDATE"
    ))
    .fetch_optional(&mut **tx)
    .await
}

/// In-transaction sandbox occupancy across every node currently mirrored
/// into this group: live placements plus suspended sandboxes whose only
/// snapshot is still pinned to a group node's local disk (same shared
/// durability predicate as the node-drain guards). Used as the residue guard
/// for `:assign-org` — reassigning a group out from under live or pinned
/// sandboxes would violate the isolation guarantees the group encodes.
///
/// The `active` bucket also counts in-flight `suspending` / non-evacuation
/// `restoring` sandboxes pinned to a group node via `target_node_id` (their
/// `active_node_id` is cleared while the restore/suspend runs), mirroring the
/// node-drain blocker so a restore in progress on a group node still blocks
/// reassignment. The `OR {inflight_from_group}` arm mirrors the per-node
/// `:terminate` guard: a suspend-snapshot upload still in flight *from* a
/// group node counts as group residue even when the sandbox's
/// `target_node_id` has drifted off-group — that node still holds the only
/// copy.
pub(super) async fn group_occupancy_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
) -> Result<SandboxOccupancy, sqlx::Error> {
    let snapshot_exists = snapshot_durable_for_source_release("sandboxes");
    let inflight_from_group = inflight_suspend_upload_from_node_exists(
        "sandboxes",
        "ANY(SELECT id FROM nodes WHERE node_group_id = $1)",
    );
    // Shares the lifecycle scope but deliberately *not* the census discount its
    // node-level sibling now applies: nothing drives group teardown
    // automatically the way `space-rollout-mng.py` drives `:terminate`, so the
    // phantom-refusal wedge does not arise here, and counting every row stays
    // the conservative reading for an operator-initiated teardown.
    let non_resident = non_resident_lifecycle_exclusion("sandboxes");
    let query = format!(
        "SELECT
             COUNT(*) FILTER (
                 WHERE (
                         active_node_id IN (SELECT id FROM nodes WHERE node_group_id = $1)
                         AND {non_resident}
                       )
                    -- An in-flight restore/suspend pins a sandbox to a group
                    -- node via target_node_id while active_node_id is still
                    -- NULL (begin_restore_lifecycle clears active_node_id but
                    -- leaves target_node_id on the source). Mirror the
                    -- node-drain blocker (drain_blocking_sandboxes_query):
                    -- `suspending` always occupies; `restoring` occupies for
                    -- non-evacuation restores.
                    OR (
                         target_node_id IN (SELECT id FROM nodes WHERE node_group_id = $1)
                         AND (
                             lifecycle = 'suspending'
                             OR (lifecycle = 'restoring'
                                 AND evacuation_restore_target IS NULL)
                         )
                       )
             )::BIGINT AS active,
             COUNT(*) FILTER (
                 WHERE (
                         lifecycle = 'suspended'
                         AND target_node_id IN (SELECT id FROM nodes WHERE node_group_id = $1)
                         AND NOT {snapshot_exists}
                       )
                    OR ({inflight_from_group} AND NOT {snapshot_exists})
             )::BIGINT AS local_snapshots
         FROM sandboxes"
    );
    let row: (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_group_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(SandboxOccupancy {
        active: row.0,
        local_snapshots: row.1,
    })
}

/// In-transaction count of org-specific template rootfs still cached on the
/// group's nodes that the proposed org assignment would strand: every cached
/// `org_specific` template whose owner differs from the new org (for a
/// release to generic, any cached `org_specific` template). Cached *generic*
/// rootfs never blocks — generic templates are placeable on any group.
pub(super) async fn stranded_cached_templates_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
    new_org_id: Option<OrgId>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)::BIGINT
           FROM node_template_inventory nti
           JOIN nodes n ON n.id = nti.node_id
           JOIN templates t ON t.name = nti.template_name
          WHERE n.node_group_id = $1
            AND t.visibility = 'org_specific'
            AND ($2::UUID IS NULL OR t.org_id IS DISTINCT FROM $2)"
    ))
    .bind(node_group_id)
    .bind(new_org_id)
    .fetch_one(&mut **tx)
    .await
}

pub(super) async fn org_exists_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1)"
    ))
    .bind(org_id)
    .fetch_one(&mut **tx)
    .await
}

/// Apply an org assignment/release. When `reset_policy` is set (any org
/// change on a `restricted` group), the policy snaps back to `default` and
/// the allowlist is cleared in the same transaction — a restricted allowlist
/// curated for one org is meaningless (and partly illegal) under another org
/// or under generic, and leaving it behind would silently resurrect if the
/// group were later restricted again.
pub(super) async fn update_group_org_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
    new_org_id: Option<OrgId>,
    reset_policy: bool,
) -> Result<(), sqlx::Error> {
    if reset_policy {
        sqlx::query(sql!(
            "DELETE FROM node_group_template_compat WHERE node_group_id = $1"
        ))
        .bind(node_group_id)
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query(sql!(
        "UPDATE node_groups
            SET org_id = $2,
                template_policy = CASE WHEN $3 THEN 'default' ELSE template_policy END
          WHERE id = $1"
    ))
    .bind(node_group_id)
    .bind(new_org_id)
    .bind(reset_policy)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Move the singleton default flag onto `node_group_id`. The caller has
/// already locked the incumbent (via [`lock_current_default_for_update`])
/// and the target, and validated the target is generic.
pub(super) async fn move_default_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE node_groups SET is_default = FALSE WHERE is_default"
    ))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE node_groups SET is_default = TRUE WHERE id = $1"
    ))
    .bind(node_group_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Delete the group row. Compat rows cascade; the `nodes.node_group_id` FK
/// (NO ACTION) backstops the handler's empty-group guard.
pub(super) async fn delete_group_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!("DELETE FROM node_groups WHERE id = $1"))
        .bind(node_group_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Set the template policy. Moving to `default` clears the allowlist in the
/// same transaction: the rows would be dead configuration that silently
/// resurrects on a later flip back to `restricted`.
pub(super) async fn set_template_policy_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
    policy: NodeGroupTemplatePolicy,
) -> Result<(), sqlx::Error> {
    if matches!(policy, NodeGroupTemplatePolicy::Default) {
        sqlx::query(sql!(
            "DELETE FROM node_group_template_compat WHERE node_group_id = $1"
        ))
        .bind(node_group_id)
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query(sql!(
        "UPDATE node_groups SET template_policy = $2 WHERE id = $1"
    ))
    .bind(node_group_id)
    .bind(policy)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Fetch the template rows named by a proposed allowlist so the handler can
/// reject unknown names and cross-org org-specific entries before writing.
///
/// `FOR SHARE` so the validation serializes against a concurrent visibility
/// flip: the flip path takes `FOR UPDATE` on the template row, so without a
/// shared lock here a narrowing flip could commit between this read and the
/// allowlist insert — leaving another org's org-specific template in the
/// allowlist with no database constraint to catch it. With the shared lock,
/// either this transaction observes the post-flip row and rejects, or the
/// flip waits and its cross-org prune removes the freshly-inserted row.
pub(super) async fn templates_by_names_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    names: &[String],
) -> Result<Vec<CompatTemplateRow>, sqlx::Error> {
    sqlx::query_as::<_, CompatTemplateRow>(sql!(
        "SELECT name, visibility, org_id FROM templates WHERE name = ANY($1::TEXT[]) FOR SHARE"
    ))
    .bind(names)
    .fetch_all(&mut **tx)
    .await
}

/// Replace the group's allowlist with `names` (full-set PUT semantics).
pub(super) async fn replace_compat_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
    names: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_group_template_compat WHERE node_group_id = $1"
    ))
    .bind(node_group_id)
    .execute(&mut **tx)
    .await?;
    if names.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "INSERT INTO node_group_template_compat (node_group_id, template_name)
         SELECT $1, name FROM UNNEST($2::TEXT[]) AS t(name)"
    ))
    .bind(node_group_id)
    .bind(names)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Postgres-backed regression tests for the node-group store: the schema
/// invariants the handlers map onto friendly conflicts (unique name,
/// singleton default, the generic/restricted CHECKs) and the guarded-write
/// helpers (residue counts, restricted-release reset, default move).
#[cfg(test)]
mod tests;
