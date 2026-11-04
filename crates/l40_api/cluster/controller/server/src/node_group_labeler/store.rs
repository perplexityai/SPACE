//! Postgres-backed implementation of [`super::NodeGroupSink`].
//!
//! - **Seed**: a lock-free `SELECT ... WHERE is_default` fast path, and — only
//!   when no default exists — a short transaction taking
//!   `LOCK TABLE node_groups IN SHARE ROW EXCLUSIVE MODE`, re-reading both "is
//!   there a default" and "are there other groups" under it, then inserting.
//!
//!   The lock is what makes the precondition real: seeding a default changes
//!   what every `NULL` `node_group_id` resolves to cluster-wide, so deciding
//!   "the table is empty" and acting on it must be one step. `SHARE ROW
//!   EXCLUSIVE` conflicts with the `ROW EXCLUSIVE` every writer takes, so it
//!   serializes against writers that know nothing about us — which the future
//!   admin create surface will be — and subsumes the double-seed race that
//!   `idx_node_groups_single_default` would otherwise arbitrate.
//!
//!   Only the INSERT reports [`super::EnsuredDefaultGroup::Seeded`]. Both
//!   reads — the fast path and the re-read under the lock, where another
//!   replica won the race and owes the audit entry — report `Existing`.
//!
//! - **Group census**: a `COUNT(*)` of non-default groups, gating whether
//!   labeling may run at all.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeGroupId;
use pplx_space_common_core::types::{NodeGroupName, NodeGroupTemplatePolicy};
use sqlx::PgPool;

use super::{DefaultNodeGroup, EnsureDefaultGroupError, EnsuredDefaultGroup, NodeGroupSink};

/// Bound on the seed's wait for the `node_groups` table lock. Same duration
/// as the table's migration.
const SEED_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

pub struct NodeGroupDbStore {
    pool: PgPool,
    /// The seed transaction's `lock_timeout`. A field rather than a literal
    /// so a test can bound its own wait; every production store takes
    /// [`SEED_LOCK_TIMEOUT`].
    seed_lock_timeout: Duration,
}

impl NodeGroupDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            seed_lock_timeout: SEED_LOCK_TIMEOUT,
        }
    }

    #[cfg(test)]
    pub(super) fn with_seed_lock_timeout(pool: PgPool, seed_lock_timeout: Duration) -> Self {
        Self {
            pool,
            seed_lock_timeout,
        }
    }
}

/// Parse a stored `(id, name)` row into a [`DefaultNodeGroup`].
///
/// The SQL boundary is where the name becomes a domain value: the table has
/// its own `name` CHECK, but an out-of-band write (or a future migration that
/// relaxes it) must not be able to hand a bad slug on to a Node label.
///
/// For **reads** only — a row this process did not write, where parsing is the
/// whole point. Do not reuse it on the seed path to re-parse the name the
/// INSERT just echoed back: that value came from an already-validated
/// [`NodeGroupName`] the caller holds, so parsing it again buys nothing and
/// makes the post-commit step fallible. A failure there would leave the row
/// committed and the tick returning `Err` before it emits the seed's
/// `space.admin.audit` entry — the one `node_groups` write that owes its own —
/// and every later tick takes the lock-free fast path and reports `Existing`,
/// which emits nothing. So the entry would be lost permanently.
///
/// To be clear about what that reasoning protects and what it doesn't: the seed's
/// audit entry is **at-most-once by construction**, and no code here makes it
/// exactly-once. The commit lands in this impl and the entry is emitted by the
/// caller afterwards, so a crash, a lost lease or a SIGKILL in between leaves the
/// `node_groups` row with no audit record — permanently, for the same reason as
/// above: `Seeded` is reported only by the call that performed the INSERT, and
/// every later tick reads the row on the fast path and reports `Existing`. The
/// window is microseconds wide and the entry is a log line rather than a durable
/// row, so closing it would mean writing the audit inside this transaction and
/// giving the trail a `node_groups`-side outbox — out of proportion to a
/// one-per-cluster-lifetime event. What the paragraph above buys is not closing
/// the window but refusing to *widen* it: nothing fallible may be added between
/// the commit and the caller's emit.
fn to_default_group(
    id: NodeGroupId,
    name: String,
) -> Result<DefaultNodeGroup, EnsureDefaultGroupError> {
    match NodeGroupName::parse(name.clone()) {
        Ok(name) => Ok(DefaultNodeGroup { id, name }),
        Err(source) => Err(EnsureDefaultGroupError::InvalidName { name, source }),
    }
}

async fn select_default_group<'e, E>(
    executor: E,
) -> Result<Option<DefaultNodeGroup>, EnsureDefaultGroupError>
where
    E: sqlx::PgExecutor<'e>,
{
    let row: Option<(NodeGroupId, String)> = sqlx::query_as(sql!(
        "SELECT id, name FROM node_groups WHERE is_default LIMIT 1"
    ))
    .fetch_optional(executor)
    .await?;
    row.map(|(id, name)| to_default_group(id, name)).transpose()
}

impl NodeGroupSink for NodeGroupDbStore {
    async fn ensure_default_group(
        &self,
        seed_name: &NodeGroupName,
    ) -> Result<EnsuredDefaultGroup, EnsureDefaultGroupError> {
        // Fast path, taken on every tick after the first: a default already
        // exists, so no transaction and no lock. A read, so `Existing` — the
        // audit entry belongs to the INSERT below and nothing else.
        if let Some(existing) = select_default_group(&self.pool).await? {
            return Ok(EnsuredDefaultGroup::Existing(existing));
        }

        // Seeding path. "No default exists, and no other group exists either"
        // has to be decided and acted on atomically: seeding a default changes
        // what every `NULL` `node_group_id` resolves to, cluster-wide, so a
        // group committing between the check and the INSERT would leave the
        // invariant violated and the row already written — and no later census
        // can undo it.
        //
        // `SHARE ROW EXCLUSIVE` conflicts with the `ROW EXCLUSIVE` that any
        // INSERT/UPDATE/DELETE takes, so it serializes against *every* writer
        // rather than only those that agreed to share an advisory lock. That
        // matters because the writer this needs to exclude already exists and
        // does not cooperate: the admin node-group create surface
        // (`routes::admin::node_groups::create_node_group`) issues a plain
        // `INSERT INTO node_groups` and takes no advisory lock, and nothing
        // here can make it take one. The repo's usual idiom is
        // `pg_advisory_xact_lock`, which only excludes writers that opted into
        // the same convention; this is the deliberate exception, and it is
        // affordable because `node_groups` is tiny, rarely written, and this
        // path only runs while no default exists.
        // Lock-free pre-check. `SeedBlockedByExistingGroups` clears only by
        // operator action, so it can hold indefinitely — and re-taking a table
        // lock that conflicts with ROW EXCLUSIVE every tick would stall the very
        // admin create surface this lock exists to exclude, forever. Racy on
        // purpose: the under-lock check below stays authoritative, and losing
        // this race only costs the lock we would have taken anyway.
        let (blocking_groups,): (i64,) = sqlx::query_as(sql!(
            "SELECT COUNT(*) FROM node_groups WHERE NOT is_default"
        ))
        .fetch_one(&self.pool)
        .await?;
        if blocking_groups != 0 {
            return Err(EnsureDefaultGroupError::SeedBlockedByExistingGroups {
                other_groups: blocking_groups,
            });
        }

        let mut tx = self.pool.begin().await?;
        // Bound the wait, not just the hold: the tick has no Postgres timeout of
        // its own, so without this a conflicting writer holding `node_groups`
        // parks the seed for as long as it likes. Failing fast books a
        // `tick_error{kind:seed}` and retries next tick. Same idiom and duration
        // as the table's migration.
        //
        // It does NOT protect other readers. A queued SHARE ROW EXCLUSIVE is
        // invisible to a plain SELECT, because ACCESS SHARE conflicts only with
        // ACCESS EXCLUSIVE — Postgres admits a lock request that doesn't
        // conflict with the wait queue's mask. The bound is for this tick alone.
        //
        // `set_config(..., is_local => true)` rather than `SET LOCAL` because
        // `SET` takes no bind parameter. It is the same statement in function
        // form: transaction-local scope, the same value, and a wait past it
        // still fails with `55P03`.
        sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
            .bind(format!("{}ms", self.seed_lock_timeout.as_millis()))
            .execute(&mut *tx)
            .await?;
        sqlx::query(sql!("LOCK TABLE node_groups IN SHARE ROW EXCLUSIVE MODE"))
            .execute(&mut *tx)
            .await?;

        // Both facts are re-read under the lock: another replica may have
        // seeded a default since the fast path (making us a reader), or a
        // group may have appeared (blocking the seed).
        if let Some(existing) = select_default_group(&mut *tx).await? {
            // `Existing`, not `Seeded`: another replica performed the INSERT
            // and owes the audit entry for it. This call only read the row.
            //
            // Intentional rollback-on-drop: nothing has been written, and
            // dropping `tx` releases the table lock. Anything added above this
            // return would need moving below it or committing here.
            return Ok(EnsuredDefaultGroup::Existing(existing));
        }
        let (other_groups,): (i64,) = sqlx::query_as(sql!(
            "SELECT COUNT(*) FROM node_groups WHERE NOT is_default"
        ))
        .fetch_one(&mut *tx)
        .await?;
        if other_groups != 0 {
            // Same: rollback-on-drop releases the table lock, and no write has
            // happened. Returning an error while holding a table-level lock is
            // safe only because of that.
            return Err(EnsureDefaultGroupError::SeedBlockedByExistingGroups { other_groups });
        }

        let id = NodeGroupId::new();
        // `RETURNING id` for the row's *presence*, not its value: `ON CONFLICT
        // DO NOTHING` means the only id this can return is the one bound above.
        // The name is deliberately not read back — see below.
        let inserted: Option<(NodeGroupId,)> = sqlx::query_as(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, $2, NULL, $3, TRUE)
             ON CONFLICT DO NOTHING
             RETURNING id"
        ))
        .bind(id)
        .bind(seed_name.as_str())
        // Bound from the enum rather than written as a `'default'` literal, for
        // the same reason the audit summary renders through the admin surface's
        // formatter: the enum owns the token, and the admin create surface
        // already binds this column the same way.
        .bind(NodeGroupTemplatePolicy::Default)
        .fetch_optional(&mut *tx)
        .await?;
        if inserted.is_none() {
            // Unreachable as reasoned above: under the lock we have just
            // established the table holds no default and no other group, so
            // it is empty and nothing can conflict. Kept rather than
            // `fetch_one` so that if that reasoning is ever wrong, the
            // operator sees "the name is taken" instead of a generic DB error
            // reported as "the migration probably isn't applied".
            return Err(EnsureDefaultGroupError::NameConflict {
                name: seed_name.to_string(),
            });
        }
        tx.commit().await?;

        tracing::info!(
            group_id = %id,
            name = %seed_name,
            "node_group_labeler: seeded default generic node group"
        );
        // Built from the values this call supplied, so nothing here can fail
        // after the commit. Re-parsing the name the `RETURNING` echoed back
        // would make this fallible, and an `Err` past the commit loses the
        // seed's audit entry for good — see `to_default_group`'s doc.
        //
        // The only `Seeded` in this impl: this call committed the INSERT, so
        // this call owes the `space.admin.audit` entry the tick emits.
        Ok(EnsuredDefaultGroup::Seeded(DefaultNodeGroup {
            id,
            name: seed_name.clone(),
        }))
    }

    async fn non_default_group_count(&self) -> Result<i64, sqlx::Error> {
        // `is_default` is NOT NULL, so `NOT is_default` has no third case.
        let (count,): (i64,) = sqlx::query_as(sql!(
            "SELECT COUNT(*) FROM node_groups WHERE NOT is_default"
        ))
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests;
