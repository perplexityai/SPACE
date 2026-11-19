//! Leasing a rollout's saved targets: permission for one controller to work
//! on one node for a while. A lease is not an assignment. Nothing here writes
//! an assignment, an issuance stamp, `next_check_at`, an observation or a
//! wakeup; the later assignment transaction does, under the lease.
//!
//! Claims and renewals run [`pplx_space_rollout_queue::claim`] and
//! [`pplx_space_rollout_queue::renew`] on these tables, in their lock order:
//! the rollout row, then the lease handoff lock, then item rows. Requeues and
//! redrives are single fenced updates under the shared outcome contract
//! ([`pplx_space_rollout_queue::LEASE_EXPIRED`]).
//!
//! A lease isn't permission to assign: the assignment step must still recheck
//! the live token and the node's eligibility before it issues, stamping in the
//! same `UPDATE` that checks `phase = 'in_flight' AND lease_token = $t`; the
//! claim's canary pin relies on that.
//!
//! In rolling, only leases count against `active_limit`: issued targets
//! awaiting their check free their slot. Nothing here caps them; the
//! assignment step or the rollout controller must.
//!
//! A check lease spends an attempt, so repeated lapses can redrive an issued
//! canary to failed. The pin then holds the rollout in canary, and with no
//! failure budget it never pauses on its own.

use std::collections::HashSet;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_rollout_queue::{
    Candidates, ClaimStore, FailedItems, HeaderPhase, LEASE_EXPIRED, Pacing, RedriveReport,
    RenewStore,
};
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

use crate::{ItemPhase, RolloutId, end_transaction, rollout_phase};

/// Firewall rollouts don't pause on failures, and the shared claim refuses
/// once `failed >= failure_budget`, so the budget is one no count reaches.
const NO_FAILURE_BUDGET: u64 = u64::MAX;

/// Key of the advisory lock that hands a node between rollouts. Frozen:
/// mixed versions must contend on the same key.
const HANDOFF_LOCK: &str = "firewall_rules_rollout_items/lease";

/// One saved target leased to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeasedTarget {
    pub rollout_id: RolloutId,
    pub node_id: NodeId,
    /// The token every later call on this lease is fenced by.
    pub lease_token: uuid::Uuid,
    /// Leases taken on this item so far, this one included.
    pub attempts: u64,
    pub work: Work,
}

/// What the lease is for, read from the item's issuance stamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Work {
    /// Nothing issued yet: the caller may issue the first assignment.
    FirstAssignment,
    /// An assignment was issued; check it, don't issue another. The stamp
    /// says what was issued, not that the node received or enforces it.
    Check {
        assignment_generation: u64,
        issued_at: OffsetDateTime,
    },
}

/// Lease up to `active_limit` of `rollout_id`'s saved targets for `lease`
/// (clamped by [`clamp_lease`](crate::clamp_lease)), counting those already
/// leased, from the first `scan_limit` eligible ones in node order. Commits
/// before it returns.
///
/// A target is eligible while its node is online (reachable, with a status
/// snapshot that isn't stale) and, once issued, while its check is due. In
/// canary, once any target is issued, only issued targets are leased: the
/// first committed assignment chose the canary.
pub async fn lease_targets(
    conn: &mut PgConnection,
    rollout_id: RolloutId,
    active_limit: u64,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<LeasedTarget>, sqlx::Error> {
    // Its own transaction, never a savepoint: a caller's snapshot or locks
    // would break the shared claim's lock order.
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    let mut claiming = Claiming {
        tx: &mut tx,
        rollout_id,
        active_limit,
        held: Vec::new(),
    };
    let leased = pplx_space_rollout_queue::claim(&mut claiming, lease, scan_limit).await;
    end_transaction(tx, leased).await
}

struct Claiming<'c> {
    tx: &'c mut PgConnection,
    rollout_id: RolloutId,
    active_limit: u64,
    /// What `held_elsewhere` read, for `candidates` to skip.
    held: Vec<uuid::Uuid>,
}

impl FailedItems for Claiming<'_> {
    type Error = sqlx::Error;

    /// Never compared: there's no failure budget ([`NO_FAILURE_BUDGET`]).
    async fn failed(&mut self) -> Result<u64, sqlx::Error> {
        Ok(0)
    }
}

impl ClaimStore for Claiming<'_> {
    /// Whether the rollout is in canary.
    type Header = bool;
    type Subject = NodeId;
    type Placement = ();
    type Claimed = LeasedTarget;

    async fn lock_leasable(&mut self) -> Result<Option<(bool, Pacing)>, sqlx::Error> {
        let canary = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT phase = $2
             FROM firewall_rules_rollouts
             WHERE id = $1
               AND phase IN ($2, $3)
             FOR UPDATE"
        ))
        .bind(self.rollout_id)
        .bind(rollout_phase(HeaderPhase::Canary))
        .bind(rollout_phase(HeaderPhase::Rolling))
        .fetch_optional(&mut *self.tx)
        .await?;
        Ok(canary.map(|canary| {
            (
                canary,
                Pacing {
                    canary,
                    per_cluster: self.active_limit,
                    failure_budget: NO_FAILURE_BUDGET,
                },
            )
        }))
    }

    async fn canary_progress(&mut self) -> Result<(bool, u64), sqlx::Error> {
        let (done, in_flight) = sqlx::query_as::<_, (bool, i64)>(sql!(
            "SELECT coalesce(bool_or(phase = $2), false), count(*) FILTER (WHERE phase = $3)
             FROM firewall_rules_rollout_items
             WHERE rollout_id = $1
               AND phase IN ($2, $3)"
        ))
        .bind(self.rollout_id)
        .bind(ItemPhase::Done.as_str())
        .bind(ItemPhase::InFlight.as_str())
        .fetch_one(&mut *self.tx)
        .await?;
        Ok((done, u64::try_from(in_flight).unwrap_or(0)))
    }

    async fn lock_handoff(&mut self, _canary: &bool) -> Result<(), sqlx::Error> {
        sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
        ))
        .bind(HANDOFF_LOCK)
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }

    async fn held_elsewhere(&mut self, _canary: &bool) -> Result<HashSet<NodeId>, sqlx::Error> {
        // Any phase: a finished rollout's lease holds its node until it ends,
        // since its controller may still be on it. Rollouts first, so each
        // one's in-flight items come from the item index: one probe per
        // rollout, growing with history, about 0.1 ms at 21 rollouts.
        self.held = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
            "SELECT i.node_id
             FROM firewall_rules_rollouts r
             JOIN firewall_rules_rollout_items i ON i.rollout_id = r.id AND i.phase = $2
             WHERE r.id <> $1
               AND i.lease_expires_at > clock_timestamp()"
        ))
        .bind(self.rollout_id)
        .bind(ItemPhase::InFlight.as_str())
        .fetch_all(&mut *self.tx)
        .await?;
        Ok(self.held.iter().copied().map(NodeId::from_uuid).collect())
    }

    /// Where eligibility is decided; `claim` mirrors the online and due
    /// filters. Membership is the saved items; the node inventory only says
    /// whether a saved node may be leased now.
    ///
    /// Online is the node's status snapshot: reachable, and not past the
    /// `stale_after` its writer derives from node-ctl's poll interval. A
    /// deregistered node has no snapshot (it cascades with the `nodes` row).
    /// Provisional: no firewall online signal is agreed yet, so this is the
    /// heartbeat rule other node readers use, on the lease's clock.
    async fn candidates(
        &mut self,
        &canary: &bool,
        scan_limit: u32,
    ) -> Result<Candidates<NodeId, ()>, sqlx::Error> {
        // One statement, so the in-flight count and the pending items come
        // from one snapshot. The canary pin looks at every item before any
        // filter: an issued canary that's offline or not due still holds it.
        // It counts: as an EXISTS, the planner scans every rollout's items.
        // `held` is excluded before the limit, so it can't fill the scan;
        // under the exclusive handoff lock it can only shrink.
        let (in_flight, pending) = sqlx::query_as::<_, (i64, Vec<uuid::Uuid>)>(sql!(
            "SELECT
                 (SELECT count(*)
                  FROM firewall_rules_rollout_items
                  WHERE rollout_id = $1
                    AND phase = $2),
                 ARRAY(SELECT i.node_id
                       FROM firewall_rules_rollout_items i
                       JOIN node_status_snapshots nss ON nss.node_id = i.node_id
                       WHERE i.rollout_id = $1
                         AND i.phase = $3
                         AND (NOT $4
                              OR i.issued_at IS NOT NULL
                              OR (SELECT count(*)
                                  FROM firewall_rules_rollout_items issued
                                  WHERE issued.rollout_id = $1
                                    AND issued.issued_at IS NOT NULL) = 0)
                         AND (i.issued_at IS NULL
                              OR (i.next_check_at IS NOT NULL
                                  AND i.next_check_at <= clock_timestamp()))
                         AND nss.reachable
                         AND nss.stale_after > clock_timestamp()
                         AND i.node_id <> ALL ($6)
                       ORDER BY i.node_id
                       LIMIT $5)"
        ))
        .bind(self.rollout_id)
        .bind(ItemPhase::InFlight.as_str())
        .bind(ItemPhase::Pending.as_str())
        .bind(canary)
        .bind(i64::from(scan_limit))
        .bind(&self.held)
        .fetch_one(&mut *self.tx)
        .await?;
        Ok(Candidates {
            in_flight: u64::try_from(in_flight).unwrap_or(0),
            pending: pending
                .into_iter()
                .map(|node| (NodeId::from_uuid(node), ()))
                .collect(),
        })
    }

    async fn claim(
        &mut self,
        _canary: &bool,
        node_id: NodeId,
        _placement: &(),
        lease_token: uuid::Uuid,
        lease: Duration,
    ) -> Result<Option<LeasedTarget>, sqlx::Error> {
        // The online and due filters of `candidates` again, since either can
        // change after the scan; a miss spends no attempt. The canary pin
        // needs no recheck: under the header lock nothing of this rollout is
        // in flight to issue. The item is locked first, as renewal does: an
        // update that waited on its row would recheck on its old snapshot.
        sqlx::query(sql!(
            "SELECT 1
             FROM firewall_rules_rollout_items
             WHERE rollout_id = $1
               AND node_id = $2
               AND phase = $3
             FOR UPDATE"
        ))
        .bind(self.rollout_id)
        .bind(node_id)
        .bind(ItemPhase::Pending.as_str())
        .fetch_optional(&mut *self.tx)
        .await?;
        let claimed = sqlx::query_as::<_, (i64, Option<i64>, Option<OffsetDateTime>)>(sql!(
            "UPDATE firewall_rules_rollout_items
             SET phase = $3,
                 lease_token = $4,
                 lease_expires_at = clock_timestamp() + make_interval(secs => $5),
                 attempts = attempts + 1
             WHERE rollout_id = $1
               AND node_id = $2
               AND phase = $6
               AND (issued_at IS NULL
                    OR (next_check_at IS NOT NULL
                        AND next_check_at <= clock_timestamp()))
               AND EXISTS (
                   SELECT 1
                   FROM node_status_snapshots nss
                   WHERE nss.node_id = $2
                     AND nss.reachable
                     AND nss.stale_after > clock_timestamp())
             RETURNING attempts, issued_assignment_generation, issued_at"
        ))
        .bind(self.rollout_id)
        .bind(node_id)
        .bind(ItemPhase::InFlight.as_str())
        .bind(lease_token)
        .bind(lease.as_secs_f64())
        .bind(ItemPhase::Pending.as_str())
        .fetch_optional(&mut *self.tx)
        .await?;
        let Some((attempts, generation, issued_at)) = claimed else {
            return Ok(None);
        };
        // The schema keeps these non-negative and the stamp's pair together;
        // report a violation as a decode error rather than guess.
        let rollout_id = self.rollout_id;
        let invalid = |what: &str| {
            sqlx::Error::Decode(
                format!("firewall rules rollout item {rollout_id}/{node_id} has {what}").into(),
            )
        };
        let work = match (generation, issued_at) {
            (None, None) => Work::FirstAssignment,
            (Some(generation), Some(issued_at)) => Work::Check {
                assignment_generation: u64::try_from(generation)
                    .map_err(|_| invalid("a negative generation"))?,
                issued_at,
            },
            _ => return Err(invalid("half an issuance stamp")),
        };
        Ok(Some(LeasedTarget {
            rollout_id,
            node_id,
            lease_token,
            attempts: u64::try_from(attempts).map_err(|_| invalid("negative attempts"))?,
            work,
        }))
    }
}

/// Extend `target`'s lease to `lease` from now (clamped as for
/// [`lease_targets`]). `false` when the lease is no longer the caller's: it
/// lapsed on the database clock, even while this call waited for a lock; it
/// was requeued or redriven; or its rollout finished.
pub async fn renew_lease(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    lease: Duration,
) -> Result<bool, sqlx::Error> {
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    let mut renewing = Renewing {
        tx: &mut tx,
        target,
    };
    let renewed = pplx_space_rollout_queue::renew(&mut renewing, lease).await;
    end_transaction(tx, renewed).await
}

struct Renewing<'c, 't> {
    tx: &'c mut PgConnection,
    target: &'t LeasedTarget,
}

impl RenewStore for Renewing<'_, '_> {
    type Error = sqlx::Error;

    async fn lock_unfinished(&mut self) -> Result<bool, sqlx::Error> {
        let unfinished = sqlx::query_scalar::<_, i32>(sql!(
            "SELECT 1
             FROM firewall_rules_rollouts
             WHERE id = $1
               AND finished_at IS NULL
             FOR SHARE"
        ))
        .bind(self.target.rollout_id)
        .fetch_optional(&mut *self.tx)
        .await?;
        Ok(unfinished.is_some())
    }

    async fn lock_handoff(&mut self) -> Result<(), sqlx::Error> {
        sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))"
        ))
        .bind(HANDOFF_LOCK)
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }

    async fn lock_item(&mut self) -> Result<(), sqlx::Error> {
        sqlx::query(sql!(
            "SELECT 1
             FROM firewall_rules_rollout_items
             WHERE rollout_id = $1
               AND node_id = $2
             FOR UPDATE"
        ))
        .bind(self.target.rollout_id)
        .bind(self.target.node_id)
        .fetch_optional(&mut *self.tx)
        .await?;
        Ok(())
    }

    async fn extend(&mut self, lease: Duration) -> Result<bool, sqlx::Error> {
        let renewed = sqlx::query(sql!(
            "UPDATE firewall_rules_rollout_items
             SET lease_expires_at = clock_timestamp() + make_interval(secs => $4)
             WHERE rollout_id = $1
               AND node_id = $2
               AND phase = $5
               AND lease_token = $3
               AND lease_expires_at > clock_timestamp()"
        ))
        .bind(self.target.rollout_id)
        .bind(self.target.node_id)
        .bind(self.target.lease_token)
        .bind(lease.as_secs_f64())
        .bind(ItemPhase::InFlight.as_str())
        .execute(&mut *self.tx)
        .await?
        .rows_affected();
        Ok(renewed == 1)
    }
}

/// How a lease ends without finishing its target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Requeue {
    /// Not attempted: the lease's attempt is given back.
    Release,
    /// Attempted and to be tried again: the attempt counts.
    Retry,
}

/// End `target`'s lease and return the item to pending. `false` when the
/// token no longer holds the item, in which case nothing changes. A lapsed
/// lease that hasn't been redriven still requeues: nobody else holds the
/// item. Bookkeeping only; it permits no assignment. Issuance and scheduling
/// are left as they are.
pub async fn requeue(
    executor: impl sqlx::PgExecutor<'_>,
    target: &LeasedTarget,
    requeue: Requeue,
) -> Result<bool, sqlx::Error> {
    let refund: i64 = match requeue {
        Requeue::Release => 1,
        Requeue::Retry => 0,
    };
    let requeued = sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET phase = $4,
             attempts = GREATEST(attempts - $6, 0),
             lease_token = NULL,
             lease_expires_at = NULL
         WHERE rollout_id = $1
           AND node_id = $2
           AND phase = $5
           AND lease_token = $3"
    ))
    .bind(target.rollout_id)
    .bind(target.node_id)
    .bind(target.lease_token)
    .bind(ItemPhase::Pending.as_str())
    .bind(ItemPhase::InFlight.as_str())
    .bind(refund)
    .execute(executor)
    .await?
    .rows_affected();
    Ok(requeued == 1)
}

/// Return `rollout_id`'s items whose leases lapsed on the database clock to
/// pending, or fail those leased `max_attempts` times. Their old tokens no
/// longer hold them. Issuance and scheduling are left as they are.
pub async fn redrive_expired(
    executor: impl sqlx::PgExecutor<'_>,
    rollout_id: RolloutId,
    max_attempts: u64,
) -> Result<RedriveReport, sqlx::Error> {
    let phases = sqlx::query_scalar::<_, String>(sql!(
        "UPDATE firewall_rules_rollout_items
         SET phase = CASE WHEN attempts >= $2 THEN $3 ELSE $4 END,
             last_error = $5,
             lease_token = NULL,
             lease_expires_at = NULL
         WHERE rollout_id = $1
           AND phase = $6
           AND lease_expires_at <= clock_timestamp()
         RETURNING phase"
    ))
    .bind(rollout_id)
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .bind(ItemPhase::Failed.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(LEASE_EXPIRED)
    .bind(ItemPhase::InFlight.as_str())
    .fetch_all(executor)
    .await?;
    let failed = phases
        .iter()
        .filter(|phase| phase.as_str() == ItemPhase::Failed.as_str())
        .count() as u64;
    Ok(RedriveReport {
        requeued: phases.len() as u64 - failed,
        failed,
    })
}
