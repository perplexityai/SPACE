//! Leasing a rollout's saved targets: permission for one controller to work
//! on one node for a while. A lease is not an assignment. Nothing here writes
//! an assignment, an issuance stamp, an observation or a wakeup;
//! [`issue_first_assignment`](crate::issue_first_assignment) writes the
//! assignment, issuance stamp and wakeup under the lease, and no observation.
//! Only a retried check reschedules `next_check_at` ([`Requeue::Retry`]).
//!
//! Claims and renewals run [`pplx_space_rollout_queue::claim`] and
//! [`pplx_space_rollout_queue::renew`] on these tables, in their lock order:
//! the rollout row, then the lease handoff lock, then item rows. A canary's
//! first claim takes back its lapsed unissued claims in the same transaction,
//! right after the rollout row. Requeues and redrives are single fenced
//! updates under the shared outcome contract
//! ([`pplx_space_rollout_queue::LEASE_EXPIRED`]).
//!
//! A lease isn't permission to assign: the assignment step must still recheck
//! the live token and the node's eligibility before it issues, holding the
//! item's lock from that check through its stamp; the claim's canary pin
//! relies on that.
//!
//! In rolling, `active_limit` bounds the nodes leased or holding an
//! unverified assignment: an issued target keeps its slot until it's done
//! or failed, even if it never proves, and failing one that can't be reached
//! is the controller's job; one whose node deregistered (no status snapshot)
//! frees its slot. A due check adds no unverified node, so only the leases in
//! flight bound it.
//!
//! A check lease spends an attempt, so repeated lapses can redrive an issued
//! canary to failed. The pin then holds the rollout in canary, and with no
//! failure budget it never pauses on its own.

use std::collections::HashSet;
use std::time::Duration;

use jiff::Timestamp;
use pplx_space_accounts::NodeId;
use pplx_space_rollout_queue::{
    Candidates, ClaimStore, FailedItems, HeaderPhase, LEASE_EXPIRED, Pacing, RedriveReport,
    RenewStore,
};
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

use crate::{ItemPhase, RolloutId, Scope, end_transaction, rollout_phase};

/// Firewall rollouts don't pause on failures, and the shared claim refuses
/// once `failed >= failure_budget`, so the budget is one no count reaches.
const NO_FAILURE_BUDGET: u64 = u64::MAX;

/// Key of the advisory lock that hands a node between rollouts. Frozen:
/// mixed versions must contend on the same key.
pub(crate) const HANDOFF_LOCK: &str = "firewall_rules_rollout_items/lease";

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
        issued_at: Timestamp,
    },
}

/// Lease up to `active_limit` of `rollout_id`'s saved targets for `lease`
/// (clamped by [`clamp_lease`](crate::clamp_lease)), counting those leased
/// or holding an unverified assignment, from the first `scan_limit` eligible
/// ones, due checks first, each group in node order. Commits before it
/// returns.
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
        unverified: 0,
        scope: Scope::Any,
    };
    let leased = pplx_space_rollout_queue::claim(&mut claiming, lease, scan_limit).await;
    end_transaction(tx, leased).await
}

/// Lease one first assignment of the database's unfinished rollout for
/// `lease` (clamped as for [`lease_targets`]), from the first `scan_limit`
/// eligible targets, while it's in canary and none of its saved targets is
/// issued. No check is leased. Commits before it returns.
///
/// First, in the same transaction once the header lock finds it in canary,
/// its unissued targets whose leases lapsed go back to pending, keeping their
/// attempts: their claimants died before issuing. An issued one may be a
/// check's, so it's left alone.
pub async fn lease_first_canary(
    conn: &mut PgConnection,
    lease: Duration,
    scan_limit: u32,
) -> Result<Option<LeasedTarget>, sqlx::Error> {
    let Some(rollout_id) = sqlx::query_scalar::<_, RolloutId>(sql!(
        "SELECT id
         FROM firewall_rules_rollouts
         WHERE unfinished_slot IS NOT NULL
           AND phase = $1"
    ))
    .bind(rollout_phase(HeaderPhase::Canary))
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    let mut claiming = Claiming {
        tx: &mut tx,
        rollout_id,
        active_limit: 1,
        held: Vec::new(),
        unverified: 0,
        scope: Scope::FirstCanary,
    };
    let leased = pplx_space_rollout_queue::claim(&mut claiming, lease, scan_limit).await;
    // A canary leases one target at a time.
    Ok(end_transaction(tx, leased).await?.pop())
}

struct Claiming<'c> {
    tx: &'c mut PgConnection,
    rollout_id: RolloutId,
    active_limit: u64,
    /// What `held_elsewhere` read, for `candidates` to skip.
    held: Vec<uuid::Uuid>,
    /// Nodes leased or holding an unverified assignment, as `candidates`
    /// counted them, plus the first assignments `claim` has leased since.
    unverified: u64,
    /// [`Scope::FirstCanary`] leases only a canary's first assignments.
    scope: Scope,
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
    /// Whether the target is a first assignment, which `claim` caps.
    type Placement = bool;
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
        // A canary's first claim found its rollout without a lock: this is
        // its only canary check.
        let canary = canary.filter(|&canary| canary || self.scope == Scope::Any);
        if canary == Some(true) && self.scope == Scope::FirstCanary {
            // Never failed, which is the later driver's call; the attempts stay.
            redrive(&mut *self.tx, self.rollout_id, u64::MAX, Scope::FirstCanary).await?;
        }
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
    /// Provisional: no firewall online signal is agreed yet, so this mirrors
    /// sandbox-residency's `node_heartbeat_fresh`, on the lease's clock.
    async fn candidates(
        &mut self,
        &canary: &bool,
        scan_limit: u32,
    ) -> Result<Candidates<NodeId, bool>, sqlx::Error> {
        // One statement, so the counts and the pending items come from one
        // snapshot: the leases in flight, and the nodes leased or holding an
        // unverified assignment. Due checks are scanned first; `OFFSET 0`
        // keeps the planner from walking every snapshot in node order for
        // them. The canary pin looks at every item before any filter: an
        // issued canary that's offline or not due still holds it. It counts:
        // as an EXISTS, the planner scans every rollout's items. `held` is
        // excluded before the limits, so it can't fill the scan; under the
        // exclusive handoff lock it can only shrink. A deregistered node's
        // issued item isn't counted; the scalar subquery probes the snapshot
        // index per issued item, where an EXISTS is hashed over every node.
        let (in_flight, unverified, due, first) =
            sqlx::query_as::<_, (i64, i64, Vec<uuid::Uuid>, Vec<uuid::Uuid>)>(sql!(
                "SELECT
                     counted.in_flight,
                     counted.unverified,
                     ARRAY(SELECT node_id
                           FROM (SELECT i.node_id
                                 FROM firewall_rules_rollout_items i
                                 JOIN node_status_snapshots nss ON nss.node_id = i.node_id
                                 WHERE i.rollout_id = $1
                                   AND i.phase = $3
                                   AND i.issued_at IS NOT NULL
                                   AND i.next_check_at <= clock_timestamp()
                                   AND nss.reachable
                                   AND nss.stale_after > clock_timestamp()
                                   AND i.node_id <> ALL ($6)
                                 OFFSET 0) due
                           ORDER BY node_id
                           LIMIT $5),
                     ARRAY(SELECT i.node_id
                           FROM firewall_rules_rollout_items i
                           JOIN node_status_snapshots nss ON nss.node_id = i.node_id
                           WHERE i.rollout_id = $1
                             AND i.phase = $3
                             AND i.issued_at IS NULL
                             AND (NOT $4
                                  OR (SELECT count(*)
                                      FROM firewall_rules_rollout_items issued
                                      WHERE issued.rollout_id = $1
                                        AND issued.issued_at IS NOT NULL) = 0)
                             AND nss.reachable
                             AND nss.stale_after > clock_timestamp()
                             AND i.node_id <> ALL ($6)
                           ORDER BY i.node_id
                           LIMIT $5)
                 FROM (SELECT count(*) FILTER (WHERE i.phase = $2) AS in_flight,
                              count(*) AS unverified
                       FROM firewall_rules_rollout_items i
                       WHERE i.rollout_id = $1
                         AND (i.phase = $2
                              OR (i.phase = $3
                                  AND i.issued_at IS NOT NULL
                                  AND (SELECT true
                                       FROM node_status_snapshots nss
                                       WHERE nss.node_id = i.node_id)))) counted"
            ))
            .bind(self.rollout_id)
            .bind(ItemPhase::InFlight.as_str())
            .bind(ItemPhase::Pending.as_str())
            .bind(canary)
            .bind(i64::from(scan_limit))
            .bind(&self.held)
            .fetch_one(&mut *self.tx)
            .await?;
        self.unverified = u64::try_from(unverified).unwrap_or(0);
        Ok(Candidates {
            in_flight: u64::try_from(in_flight).unwrap_or(0),
            pending: due
                .into_iter()
                .map(|node| (node, false))
                .chain(first.into_iter().map(|node| (node, true)))
                // In canary a due check means one is issued, so there are no
                // first assignments for it to crowd out.
                .filter(|&(_, first)| first || self.scope == Scope::Any)
                .take(scan_limit as usize)
                .map(|(node, first)| (NodeId::from_uuid(node), first))
                .collect(),
        })
    }

    async fn claim(
        &mut self,
        _canary: &bool,
        node_id: NodeId,
        &first_assignment: &bool,
        lease_token: uuid::Uuid,
        lease: Duration,
    ) -> Result<Option<LeasedTarget>, sqlx::Error> {
        // Only a first assignment adds an unverified node. Counted here, so a
        // check that misses below can't hand its slot to one.
        if first_assignment && self.unverified >= self.active_limit {
            return Ok(None);
        }
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
        if first_assignment {
            self.unverified += 1;
        }
        let rollout_id = self.rollout_id;
        Ok(Some(LeasedTarget {
            rollout_id,
            node_id,
            lease_token,
            attempts: u64::try_from(attempts)
                .map_err(|_| invalid_item(rollout_id, node_id, "negative attempts"))?,
            work: stamped_work(rollout_id, node_id, generation, issued_at)?,
        }))
    }
}

/// The schema keeps an item's counts non-negative and its stamp's pair
/// together; a violation, or a time jiff can't hold, is a decode error rather
/// than a guess.
fn invalid_item(rollout_id: RolloutId, node_id: NodeId, what: &str) -> sqlx::Error {
    sqlx::Error::Decode(
        format!("firewall rules rollout item {rollout_id}/{node_id} has {what}").into(),
    )
}

/// An item's time, from SQLx's `time` into jiff; `what` describes it in the error.
pub(crate) fn item_timestamp(
    rollout_id: RolloutId,
    node_id: NodeId,
    what: &str,
    value: OffsetDateTime,
) -> Result<Timestamp, sqlx::Error> {
    Timestamp::from_nanosecond(value.unix_timestamp_nanos())
        .map_err(|_| invalid_item(rollout_id, node_id, what))
}

/// The work an item's issuance stamp leaves.
pub(crate) fn stamped_work(
    rollout_id: RolloutId,
    node_id: NodeId,
    generation: Option<i64>,
    issued_at: Option<OffsetDateTime>,
) -> Result<Work, sqlx::Error> {
    match (generation, issued_at) {
        (None, None) => Ok(Work::FirstAssignment),
        (Some(generation), Some(issued_at)) => Ok(Work::Check {
            assignment_generation: u64::try_from(generation)
                .map_err(|_| invalid_item(rollout_id, node_id, "a negative generation"))?,
            issued_at: item_timestamp(
                rollout_id,
                node_id,
                "an out-of-range issuance time",
                issued_at,
            )?,
        }),
        _ => Err(invalid_item(rollout_id, node_id, "half an issuance stamp")),
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

/// How a lease ends without finishing its target. `error` becomes the item's
/// `last_error`, `None` clearing it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Requeue {
    /// Not attempted: the lease's attempt is given back.
    Release { error: Option<String> },
    /// Attempted and to be tried again: the attempt counts. A check is next
    /// due `next_check_in` from now on the database clock. A first assignment
    /// has no schedule, so its `next_check_at` stays as it is and the
    /// controller paces its retries.
    Retry {
        next_check_in: Duration,
        error: Option<String>,
    },
}

/// End `target`'s lease and return the item to pending. `false` when the
/// token no longer holds the item, in which case nothing changes. A lapsed
/// lease that hasn't been redriven still requeues: nobody else holds the
/// item. Bookkeeping only; it permits no assignment. Issuance is left as it
/// is.
pub async fn requeue(
    executor: impl sqlx::PgExecutor<'_>,
    target: &LeasedTarget,
    requeue: &Requeue,
) -> Result<bool, sqlx::Error> {
    let (refund, next_check_in, error): (i64, _, _) = match requeue {
        Requeue::Release { error } => (1, None, error),
        Requeue::Retry {
            next_check_in,
            error,
        } => (0, Some(next_check_in.as_secs_f64()), error),
    };
    // As the schema's check allows: no NULs, which Postgres text can't hold,
    // at most 4096 characters, and never empty.
    let error = error.as_deref().and_then(|error| {
        let error: String = error.chars().filter(|c| *c != '\0').take(4096).collect();
        (!error.is_empty()).then_some(error)
    });
    let requeued = sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET phase = $4,
             attempts = GREATEST(attempts - $6, 0),
             last_error = $8,
             next_check_at = CASE
                 WHEN $7::float8 IS NULL OR issued_at IS NULL THEN next_check_at
                 ELSE clock_timestamp() + make_interval(secs => $7)
             END,
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
    .bind(next_check_in)
    .bind(error)
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
    redrive(executor, rollout_id, max_attempts, Scope::Any).await
}

/// [`redrive_expired`]; [`Scope::FirstCanary`] redrives only unissued items.
async fn redrive(
    executor: impl sqlx::PgExecutor<'_>,
    rollout_id: RolloutId,
    max_attempts: u64,
    scope: Scope,
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
           AND (NOT $7 OR issued_at IS NULL)
         RETURNING phase"
    ))
    .bind(rollout_id)
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .bind(ItemPhase::Failed.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(LEASE_EXPIRED)
    .bind(ItemPhase::InFlight.as_str())
    .bind(scope == Scope::FirstCanary)
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
