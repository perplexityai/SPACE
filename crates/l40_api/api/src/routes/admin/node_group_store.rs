//! Postgres-side helpers that back the `/admin/v1/node-groups` admin
//! endpoints.
//!
//! Handlers in [`super::node_groups`] stay focused on HTTP, auth, invariant
//! orchestration, and audit logging. Multi-step guarded writes (assign-org,
//! set-default, template-policy, compat, delete) lock the group row
//! `FOR UPDATE` (`pplx-space-node-group-store`'s `groups::lock_node_group`)
//! for the validate + write window and run their residue checks inside the
//! same transaction, so two operators cannot race a guard or the
//! singleton-default move.
//!
//! This module keeps the pool-bound group reads ([`AdminNodeGroupStore`]),
//! the constraint-name mapping, and the helpers that do more than forward to
//! one store verb: id generation on insert, the occupancy count's mapping,
//! and the writes that clear the allowlist or the incumbent default in the
//! same transaction. Handlers call the single-statement verbs directly: the
//! default-row and group-row locks, the generic-group check and the delete
//! (`pplx-space-node-group-store`), the template `FOR SHARE` read
//! (`pplx-space-template-store`'s `templates::share_lock_visibilities`), the
//! stranded cached-template count
//! (`pplx-space-node-template-inventory-store`) and the organization check
//! (`pplx-space-accounts-store`). This module keeps no statements of its own;
//! each helper runs on the pool or transaction it is given.

use pplx_space_accounts::OrgId;

use std::collections::HashMap;

use pplx_space_node_model::NodeGroupId;
use pplx_space_node_model::{NodeGroupName, NodeGroupTemplatePolicy};
use sqlx::{PgPool, Postgres, Transaction};

use super::node_store::SandboxOccupancy;

pub(super) use pplx_space_node_group_store::groups::NodeGroupRow;

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
        pplx_space_node_group_store::groups::node_group(&self.pool, node_group_id)
            .await
            .map_err(sqlx::Error::from)
    }

    pub(super) async fn list(&self) -> Result<Vec<NodeGroupRow>, sqlx::Error> {
        pplx_space_node_group_store::groups::node_groups(&self.pool)
            .await
            .map_err(sqlx::Error::from)
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
        let rows =
            pplx_space_node_group_store::compat_templates(&self.pool, node_group_ids).await?;
        let mut map: HashMap<NodeGroupId, Vec<String>> = HashMap::new();
        for row in rows {
            map.entry(row.node_group_id)
                .or_default()
                .push(row.template_name);
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
    pplx_space_node_group_store::insert_node_group(
        pool,
        pplx_space_node_group_store::NewNodeGroup {
            id,
            name,
            org_id,
            template_policy,
            is_default,
        },
    )
    .await?;
    Ok(id)
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
    // Shares the lifecycle scope but deliberately *not* the census discount its
    // node-level sibling now applies: nothing drives group teardown
    // automatically the way `space-rollout-mng.py` drives `:terminate`, so the
    // phantom-refusal wedge does not arise here, and counting every row stays
    // the conservative reading for an operator-initiated teardown.
    let counts =
        pplx_space_sandbox_store::admin_occupancy::group_occupancy(&mut **tx, node_group_id)
            .await?;
    Ok(SandboxOccupancy {
        active: counts.active,
        local_snapshots: counts.local_snapshots,
    })
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
        pplx_space_node_group_store::clear_template_compat(&mut **tx, node_group_id).await?;
    }
    pplx_space_node_group_store::update_node_group_org(
        &mut **tx,
        node_group_id,
        new_org_id,
        reset_policy,
    )
    .await
    .map_err(sqlx::Error::from)
}

/// Move the singleton default flag onto `node_group_id`. The caller has
/// already locked the incumbent (via
/// [`pplx_space_node_group_store::locks::lock_default_node_group`])
/// and the target, and validated the target is generic.
pub(super) async fn move_default_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
) -> Result<(), sqlx::Error> {
    pplx_space_node_group_store::clear_default_node_group(&mut **tx).await?;
    pplx_space_node_group_store::set_default_node_group(&mut **tx, node_group_id)
        .await
        .map_err(sqlx::Error::from)
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
        pplx_space_node_group_store::clear_template_compat(&mut **tx, node_group_id).await?;
    }
    pplx_space_node_group_store::set_node_group_template_policy(&mut **tx, node_group_id, policy)
        .await
        .map_err(sqlx::Error::from)
}

/// Replace the group's allowlist with `names` (full-set PUT semantics).
pub(super) async fn replace_compat_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_group_id: NodeGroupId,
    names: &[String],
) -> Result<(), sqlx::Error> {
    pplx_space_node_group_store::clear_template_compat(&mut **tx, node_group_id).await?;
    if names.is_empty() {
        return Ok(());
    }
    pplx_space_node_group_store::insert_template_compat(&mut **tx, node_group_id, names)
        .await
        .map_err(sqlx::Error::from)
}

/// Postgres-backed regression tests for the node-group store: the schema
/// invariants the handlers map onto friendly conflicts (unique name,
/// singleton default, the generic/restricted CHECKs) and the guarded-write
/// helpers and store verbs the handlers call (residue counts,
/// restricted-release reset, default move).
#[cfg(test)]
mod tests;
