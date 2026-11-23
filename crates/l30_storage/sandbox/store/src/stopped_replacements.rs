//! How many evacuated `stopped` sandboxes can't serve a `start` yet.

use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// Count every evacuated `stopped` sandbox that can't serve a `start` yet:
/// released and on no node, or claimed for re-placement and not yet settled.
/// Cluster-wide, and **deliberately not** the retry-eligible subset that
/// node drain's restore-pending work list returns.
///
/// This is the population an on-call alert reads, so it has to answer "how many
/// sandboxes are currently unstartable", not "how many will the next sweep try".
/// Those differ by exactly the rows that matter most: a re-placement that keeps
/// failing carries a `next_restore_attempt_at` up to the 30-minute backoff cap,
/// which would drop it out of the work list — and therefore out of the gauge —
/// for the whole window, precisely while it is most stuck. The alert would go
/// quiet as the incident got worse.
///
/// It also omits the durability term the work list applies. A torn-down row
/// whose backup is *not* resolvable (its only storage location orphaned, say)
/// is unplaceable rather than un-ready, and dropping it from the count would
/// silence the one case with no automatic recovery at all.
///
/// The two lifecycles are read differently on purpose:
///
/// * a `stopped` row counts only with `active_node_id IS NULL`, which is what
///   makes it "released" rather than "mid evacuation": a claimed row still on
///   its node is held by the drain, visible as a drain blocker, and startable
///   again the moment the marker clears;
/// * a `restoring` row counts whatever its `active_node_id`. The re-placement
///   pod's placement sets the column as soon as the pod binds, while the row
///   stays unstartable until the drain settles it, which is exactly where a
///   re-placement that never converges (a destination whose shim predates
///   stopped materialization, say) sits indefinitely.
pub async fn pending(executor: impl sqlx::PgExecutor<'_>) -> Result<i64, SandboxStoreError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)::BIGINT
           FROM sandboxes s
          WHERE s.evacuation_restore_target = 'stopped'
            AND (
                (s.lifecycle = 'stopped' AND s.active_node_id IS NULL)
                OR s.lifecycle = 'restoring'
            )"
    ))
    .fetch_one(executor)
    .await?)
}
