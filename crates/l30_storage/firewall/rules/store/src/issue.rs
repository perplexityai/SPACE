//! Issuing a leased target's first assignment: the node's assignment, its
//! wakeup and the item's issuance stamp commit together or not at all. Only
//! the database changes; the node learns of it when its wakeup is drained,
//! and a delivered wakeup proves nothing about what it enforces.
//!
//! Locks, in order: the rollout row, the lease handoff lock (shared, as a
//! renewal takes it), the item row, the node's `nodes` row (key share), the
//! node's assignment row, and the wakeup's rows. The first three are
//! [`lease_targets`](crate::lease_targets)' order; nothing in the lease verbs
//! takes the rest. The control row isn't locked.

use std::time::Duration;

use jiff::Timestamp;
use pplx_space_accounts::NodeId;
use pplx_space_rollout_queue::HeaderPhase;
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

use crate::lease::{HANDOFF_LOCK, item_timestamp, stamped_work};
use crate::{ItemPhase, LeasedTarget, Work, roll_back, rollout_phase};

/// The wakeup issuance queues for its node, on the issuing transaction.
pub trait Wakeup {
    /// Queue `node`'s wakeup on `tx`, returning how many nodes it queued.
    fn wake(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        node: NodeId,
    ) -> impl Future<Output = Result<u64, sqlx::Error>> + Send;
}

/// What an item's issuance stamp records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stamp {
    pub assignment_generation: u64,
    pub issued_at: Timestamp,
    /// `None` only if a later writer unscheduled it.
    pub next_check_at: Option<Timestamp>,
}

/// What [`issue_first_assignment`] did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Issuance {
    /// This call issued, and committed.
    Issued(Stamp),
    /// The item was already issued; nothing changed.
    AlreadyIssued(Stamp),
    /// Nothing changed.
    Refused(Refusal),
}

/// Why nothing was issued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    /// The lease is for a check; the item was issued when it was leased.
    CheckLease,
    /// The rollout has no such saved target.
    NotATarget,
    /// The rollout isn't in canary or rolling.
    NotIssuing,
    /// The item isn't in flight under this token, or its lease lapsed.
    LeaseLost,
    /// In canary, another target was issued first.
    CanaryChosen,
    /// The node is deregistered, unreachable or stale.
    NodeUnavailable,
    /// The lease lapsed or the node went offline while issuance waited.
    Lapsed,
}

/// Issue `target`'s first assignment: advance its node's assignment to the
/// rollout's revision, queue the node's wakeup through `wakeup`, and stamp the
/// item with the generation, the database time and a first check
/// `first_check_in` after it. Commits only when it returns
/// [`Issuance::Issued`]; the lease stays in flight for the caller to end.
///
/// Authorized under the locks, on the database clock: the rollout in canary
/// or rolling, the item in flight under `target`'s live token and not issued,
/// in canary no other item issued, and the node registered and online. The
/// stamp rechecks the lease and the node after the assignment and wakeup,
/// either of which can wait on a lock.
pub async fn issue_first_assignment(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    first_check_in: Duration,
    wakeup: &mut impl Wakeup,
) -> Result<Issuance, sqlx::Error> {
    // A hint only: a first assignment is rechecked below, a check never issues.
    if target.work != Work::FirstAssignment {
        return Ok(Issuance::Refused(Refusal::CheckLease));
    }
    // Its own transaction, as the lease verbs': a caller's locks would break
    // the lock order.
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    match issue(&mut tx, target, first_check_in, wakeup).await {
        Ok(Issuance::Issued(stamp)) => {
            tx.commit().await?;
            Ok(Issuance::Issued(stamp))
        }
        other => {
            roll_back(tx).await;
            other
        }
    }
}

async fn issue(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &LeasedTarget,
    first_check_in: Duration,
    wakeup: &mut impl Wakeup,
) -> Result<Issuance, sqlx::Error> {
    let header = sqlx::query_as::<_, (bool, bool, uuid::Uuid)>(sql!(
        "SELECT phase IN ($2, $3), phase = $2, to_revision_id
         FROM firewall_rules_rollouts
         WHERE id = $1
         FOR UPDATE"
    ))
    .bind(target.rollout_id)
    .bind(rollout_phase(HeaderPhase::Canary))
    .bind(rollout_phase(HeaderPhase::Rolling))
    .fetch_optional(&mut **tx)
    .await?;
    let Some((issuing, canary, revision)) = header else {
        return Ok(Issuance::Refused(Refusal::NotATarget));
    };
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))"
    ))
    .bind(HANDOFF_LOCK)
    .execute(&mut **tx)
    .await?;
    // Locked before it's read, so the lease is judged on a clock read after
    // any wait for the row.
    sqlx::query(sql!(
        "SELECT 1
         FROM firewall_rules_rollout_items
         WHERE rollout_id = $1
           AND node_id = $2
         FOR UPDATE"
    ))
    .bind(target.rollout_id)
    .bind(target.node_id)
    .fetch_optional(&mut **tx)
    .await?;
    let item = sqlx::query_as::<
        _,
        (
            bool,
            Option<i64>,
            Option<OffsetDateTime>,
            Option<OffsetDateTime>,
        ),
    >(sql!(
        "SELECT phase = $4 AND lease_token = $3 AND lease_expires_at > clock_timestamp(),
                    issued_assignment_generation,
                    issued_at,
                    next_check_at
             FROM firewall_rules_rollout_items
             WHERE rollout_id = $1
               AND node_id = $2"
    ))
    .bind(target.rollout_id)
    .bind(target.node_id)
    .bind(target.lease_token)
    .bind(ItemPhase::InFlight.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    let Some((leased, generation, issued_at, next_check_at)) = item else {
        return Ok(Issuance::Refused(Refusal::NotATarget));
    };
    let timestamp = |what, at| item_timestamp(target.rollout_id, target.node_id, what, at);
    // A replay, or a commit whose response was lost: the stamp says what was
    // issued, whoever holds the lease now.
    if let Work::Check {
        assignment_generation,
        issued_at,
    } = stamped_work(target.rollout_id, target.node_id, generation, issued_at)?
    {
        return Ok(Issuance::AlreadyIssued(Stamp {
            assignment_generation,
            issued_at,
            next_check_at: next_check_at
                .map(|at| timestamp("an out-of-range check time", at))
                .transpose()?,
        }));
    }
    if !issuing {
        return Ok(Issuance::Refused(Refusal::NotIssuing));
    }
    if !leased {
        return Ok(Issuance::Refused(Refusal::LeaseLost));
    }
    // Every item, online or not, due or not: an offline canary still holds
    // the rollout.
    if canary
        && sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM firewall_rules_rollout_items
                 WHERE rollout_id = $1
                   AND issued_at IS NOT NULL)"
        ))
        .bind(target.rollout_id)
        .fetch_one(&mut **tx)
        .await?
    {
        return Ok(Issuance::Refused(Refusal::CanaryChosen));
    }
    // Key share: the node can't be deleted before this commits, and other
    // writers of the row don't wait. Online as the lease verbs judge it.
    let online = sqlx::query_scalar::<_, Option<bool>>(sql!(
        "SELECT nss.reachable AND nss.stale_after > clock_timestamp()
         FROM nodes n
         LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
         WHERE n.id = $1
         FOR KEY SHARE OF n"
    ))
    .bind(target.node_id)
    .fetch_optional(&mut **tx)
    .await?;
    if online.flatten() != Some(true) {
        return Ok(Issuance::Refused(Refusal::NodeUnavailable));
    }
    // The node's own sequence: overflow is an error, not a wrap.
    let generation = sqlx::query_scalar::<_, i64>(sql!(
        "INSERT INTO firewall_rules_node_assignments (node_id, assignment_generation, revision_id)
         VALUES ($1, 1, $2)
         ON CONFLICT (node_id) DO UPDATE
         SET assignment_generation = firewall_rules_node_assignments.assignment_generation + 1,
             revision_id = EXCLUDED.revision_id
         RETURNING assignment_generation"
    ))
    .bind(target.node_id)
    .bind(revision)
    .fetch_one(&mut **tx)
    .await?;
    let queued = wakeup.wake(tx, target.node_id).await?;
    if queued != 1 {
        return Err(sqlx::Error::Protocol(format!(
            "firewall rules wakeup for node {} queued {queued} nodes, not 1",
            target.node_id
        )));
    }
    // After the waits above, on one clock reading: the lease may have lapsed
    // or the node gone offline while they waited. The item lock, held since
    // the item was read, keeps its phase, token and stamp as checked.
    let stamped = sqlx::query_as::<_, (OffsetDateTime, OffsetDateTime)>(sql!(
        "WITH clock AS (SELECT clock_timestamp() AS reading)
         UPDATE firewall_rules_rollout_items i
         SET issued_assignment_generation = $3,
             issued_at = clock.reading,
             next_check_at = clock.reading + make_interval(secs => $4)
         FROM clock
         WHERE i.rollout_id = $1
           AND i.node_id = $2
           AND i.lease_expires_at > clock.reading
           AND EXISTS (
               SELECT 1
               FROM node_status_snapshots nss
               WHERE nss.node_id = $2
                 AND nss.reachable
                 AND nss.stale_after > clock.reading)
         RETURNING i.issued_at, i.next_check_at"
    ))
    .bind(target.rollout_id)
    .bind(target.node_id)
    .bind(generation)
    .bind(first_check_in.as_secs_f64())
    .fetch_optional(&mut **tx)
    .await?;
    let Some((issued_at, next_check_at)) = stamped else {
        return Ok(Issuance::Refused(Refusal::Lapsed));
    };
    Ok(Issuance::Issued(Stamp {
        assignment_generation: u64::try_from(generation).map_err(|_| {
            sqlx::Error::Decode(
                format!(
                    "node {} has a negative assignment generation",
                    target.node_id
                )
                .into(),
            )
        })?,
        issued_at: timestamp("an out-of-range issuance time", issued_at)?,
        next_check_at: Some(timestamp("an out-of-range check time", next_check_at)?),
    }))
}
