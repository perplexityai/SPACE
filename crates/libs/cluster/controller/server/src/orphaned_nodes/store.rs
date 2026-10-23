//! Postgres-backed implementation of [`super::OrphanedNodesSink`].
//!
//! The candidate scan and the conditional DELETE share a vocabulary:
//!
//! - `managed_by = 'k8s'` — leaves direct-managed SPACE nodes alone. Kubernetes
//!   nodes whose instances disappear outside the drain path can leave orphan
//!   rows; direct-managed rows are owned by a different lifecycle.
//! - `drain_phase IS NULL` — rows in any drain phase belong to `node_drain`
//!   and `drained_node_terminator`. Touching them out from under those
//!   reconcilers would strand the drain pipeline.
//! - `NOT EXISTS (sandboxes pinned to this node, non-terminal lifecycle)` —
//!   even after the EC2 instance is gone, a row may still have suspended
//!   sandboxes pinned to it via `target_node_id`. Those snapshots live on
//!   the (now-gone) local disk; the row must not be deleted until the
//!   sandbox is restored or marked terminal elsewhere. Same shape as the
//!   guard the admin `:terminate` endpoint already enforces.
//!
//! The DELETE additionally checks `node_k8s.k8s_node_uid` against the value
//! we observed during the scan. This catches the same-private-DNS ASG
//! replacement race: `nodes.id` is `Uuid::new_v5(k8s_node_name, role)`, so a
//! replacement instance lands on the same row id. Between candidate-scan
//! and DELETE, kube node-sync upserts the row and bumps the UID. The DELETE
//! refuses to act if the UID has moved.

use pplx_space_common_core::types::{NodeId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_common_svc::sql::node_placement_fence::lock_node_for_phase_write;
use sqlx::PgPool;
use std::time::Duration;

use super::{OrphanCandidate, OrphanedNodesSink};

/// Sandbox lifecycles considered *terminal* for placement: a sandbox in
/// one of these states no longer blocks deleting the node row it pinned.
/// The SQL uses `lifecycle NOT IN {...}` to gate orphan eligibility on
/// *non-terminal* placements. `quarantined` deliberately remains
/// non-terminal here: it preserves the node association until an operator or
/// healer can inspect and release the sandbox.
const TERMINAL_PLACEMENT_LIFECYCLES: &str = "('failed', 'cleanup_required', 'deleted')";

pub struct NodeDbStore {
    pool: PgPool,
}

impl NodeDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// How long the orphan DELETE waits for the `nodes` row lock before giving up.
///
/// Bounded for the same reason the drain reconcilers bound theirs: the scan
/// iterates candidates sequentially, so an unbounded wait on one contended row
/// stalls every node behind it. A timeout costs this node one tick, and the next
/// scan re-lists it.
const ORPHAN_DELETE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

impl OrphanedNodesSink for NodeDbStore {
    async fn list_candidates(
        &self,
        limit: i64,
        after: Option<NodeId>,
    ) -> Result<Vec<OrphanCandidate>, sqlx::Error> {
        // The `NOT EXISTS` subquery is the live-placement gate: even with
        // EC2 gone, a row that still has a non-terminal sandbox pointing
        // at it isn't safe to delete (that sandbox's only on-disk state
        // is gone too — that's a separate ghost-row remediation, not an
        // orphan-node cleanup).
        //
        // The gate is two NOT EXISTS probes rather than one with
        // `(active_node_id = n.id OR target_node_id = n.id)`: the OR form
        // makes the planner BitmapOr both partial indexes, materializing
        // every sandbox row pinned to the node before EXISTS can judge —
        // O(occupancy) per node even when the first resident disproves absence.
        // Split, each probe walks its own partial index and stops at the first
        // non-terminal row. `NOT EXISTS (A OR B)` and
        // `NOT EXISTS (A) AND NOT EXISTS (B)` are exactly equivalent.
        //
        // The `n.id > $1` filter is the per-sweep cursor: the reconciler
        // passes the last-scanned id from the prior tick so a fixed
        // `max_per_tick` cap can't permanently starve later candidates
        // when the front of the ascending-`id` order is dominated by live
        // K8s nodes. `IS NULL` lets the first tick of a sweep start from
        // the beginning.
        let rows: Vec<(NodeId, String, Option<String>)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(format!(
                "SELECT n.id, k.k8s_node_name, k.k8s_node_uid
             FROM nodes n
             JOIN node_k8s k ON k.node_id = n.id
             WHERE n.managed_by = $3
               AND n.drain_phase IS NULL
               AND k.k8s_node_name IS NOT NULL
               AND ($1::UUID IS NULL OR n.id > $1)
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE active_node_id = n.id
                      AND lifecycle NOT IN {TERMINAL_PLACEMENT_LIFECYCLES}
               )
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE target_node_id = n.id
                      AND lifecycle NOT IN {TERMINAL_PLACEMENT_LIFECYCLES}
               )
             ORDER BY n.id ASC
             LIMIT $2"
            ))))
            .bind(after)
            .bind(limit)
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|(node_id, k8s_node_name, k8s_node_uid)| OrphanCandidate {
                node_id,
                k8s_node_name,
                k8s_node_uid,
            })
            .collect())
    }

    async fn delete_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<bool, sqlx::Error> {
        // **The `FOR UPDATE` preamble is required by the placement fence, not by
        // this reconciler's own logic**, and it was added when that fence landed.
        // This is a hard `DELETE FROM nodes`, which takes `FOR UPDATE` on the row
        // — a mode the placement writers' `FOR KEY SHARE` conflicts with. Before
        // the fence those writers took no lock on `nodes`, so this statement could
        // never block on one; now it can, and a blocked statement's `NOT EXISTS`
        // over *`sandboxes`* is evaluated on the pre-wait snapshot (the READ
        // COMMITTED rule `node_placement_fence::lock_node_for_phase_write`
        // documents). Taking the lock in a prior statement is what gives the
        // guarded DELETE a snapshot that includes whatever committed while it
        // waited.
        //
        // This one is worse than the same shape in `drained_node_terminator`, and
        // that is why it is fixed rather than merely noted: the candidate here has
        // `drain_phase IS NULL`, so the fence returns `Open` and the placement
        // *commits*. The guest is live. Deleting the `nodes` row underneath it
        // leaves a running sandbox pointing at a row that no longer exists —
        // exactly what the `NOT EXISTS` guard was written to refuse.
        let mut tx = self.pool.begin().await?;
        lock_node_for_phase_write(&mut tx, node_id.as_uuid(), ORPHAN_DELETE_LOCK_TIMEOUT).await?;
        // The COALESCE-to-'' shape lets a NULL stored UID compare equal to
        // a NULL expected UID without `IS NOT DISTINCT FROM` (which would
        // require a different bind-parameter shape under sqlx). A NULL on
        // either side that "becomes" non-NULL between scan and DELETE is
        // exactly the replacement case we want to refuse.
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
            "DELETE FROM nodes n
             WHERE n.id = $1
               AND n.managed_by = $3
               AND n.drain_phase IS NULL
               AND COALESCE(
                   (SELECT k8s_node_uid FROM node_k8s WHERE node_id = $1),
                   ''
               ) = COALESCE($2, '')
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE active_node_id = $1
                      AND lifecycle NOT IN {TERMINAL_PLACEMENT_LIFECYCLES}
               )
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE target_node_id = $1
                      AND lifecycle NOT IN {TERMINAL_PLACEMENT_LIFECYCLES}
               )"
        ))))
        .bind(node_id)
        .bind(expected_uid)
        .bind(SubstrateKind::Kubernetes)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }
}
