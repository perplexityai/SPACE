//! Opening a round that asks an issued target's node what it enforces: the
//! question, bound to the generation the item issued, and the node's wakeup
//! commit together or not at all.
//!
//! Locks, in order: the rollout row (shared, so a pause or abort waits), the
//! lease handoff lock (shared), the item row, the node's assignment row
//! (shared), its observation row, and the wakeup's rows; a writer that takes
//! any of them takes them in this order. The first three are issuance's.
//! Rounds are numbered one past the node's larger pending or accepted round,
//! so none is reused while only acceptance or a larger round replaces a
//! pending one.
//!
//! `open_round` is not the authority for the node-manager epoch: it stamps
//! whatever epoch its first caller passes, and a later mismatch is refused with
//! no recovery. Wire it to a production caller only once a durable node-manager
//! identity owns the observation row and its epoch; it should then refuse a
//! missing row or a different epoch rather than create one.

use pplx_space_postgres_pool::DbError;
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

use crate::fetch::{PendingRound, non_negative, pending_round};
use crate::issue::wake_one;
use crate::leased_item::{HeaderLock, lock_leased_item};
use crate::{LeasedTarget, Wakeup, Work, roll_back};

/// What [`open_round`] did. A round is returned as Fetch reads it: bound to
/// the generation the target's item issued, which an answer must be about,
/// and timed when it was opened, not when the assignment was issued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Opening {
    /// This call opened the round and queued the wakeup, and committed.
    Opened(PendingRound),
    /// The round was already open for this generation; nothing changed.
    AlreadyOpen(PendingRound),
    /// Nothing changed.
    Refused(OpenRefusal),
}

/// Why no round was opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenRefusal {
    /// The rollout has no such saved target.
    NotATarget,
    /// The rollout isn't in canary or rolling.
    NotChecking,
    /// The item isn't in flight under this token, or its lease lapsed.
    LeaseLost,
    /// The item hasn't issued an assignment.
    NotIssued,
    /// The node's assignment is no longer the one the item issued.
    Superseded,
    /// The node's observations belong to another manager epoch.
    EpochMismatch,
    /// The lease lapsed while opening waited.
    Lapsed,
    /// The node's pending round asks about a later generation than the item
    /// issued. Generations only rise, so only a hand-written row leaves one;
    /// it isn't replaced by an older question.
    LaterGeneration,
}

/// Open a round asking `target`'s node to verify the generation its item
/// issued, and queue the node's wakeup through `wakeup`. Commits only when it
/// returns [`Opening::Opened`]; the lease stays in flight for the caller to
/// end.
///
/// Authorized under the locks: the rollout in canary or rolling, the item in
/// flight under `target`'s live token and issued, and the node still assigned
/// the generation and revision it issued. `manager_epoch` must match the
/// node's observations, if it has any. A round already open for that
/// generation is returned as it is, without rechecking the lease after the
/// observation lock, since it changes nothing. One for a later generation is
/// refused; any other pending round is replaced by a new one. The lease is
/// rechecked at the clock reading that times the round, after the wakeup,
/// which can wait on a lock.
pub async fn open_round(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    manager_epoch: u64,
    wakeup: &mut impl Wakeup,
) -> Result<Opening, DbError> {
    let manager_epoch = i64::try_from(manager_epoch).map_err(|_| {
        sqlx::Error::Protocol(format!("manager epoch {manager_epoch} is too large"))
    })?;
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    match open(&mut tx, target, manager_epoch, wakeup).await {
        Ok(Opening::Opened(question)) => {
            tx.commit().await?;
            Ok(Opening::Opened(question))
        }
        other => {
            roll_back(tx).await;
            Ok(other?)
        }
    }
}

async fn open(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &LeasedTarget,
    manager_epoch: i64,
    wakeup: &mut impl Wakeup,
) -> Result<Opening, sqlx::Error> {
    // The header shared: a pause or abort waits, other openings don't.
    let Some(item) = lock_leased_item(tx, target, HeaderLock::Share).await? else {
        return Ok(Opening::Refused(OpenRefusal::NotATarget));
    };
    if !item.working {
        return Ok(Opening::Refused(OpenRefusal::NotChecking));
    }
    if !item.leased {
        return Ok(Opening::Refused(OpenRefusal::LeaseLost));
    }
    let Work::Check {
        assignment_generation,
        ..
    } = item.work(target)?
    else {
        return Ok(Opening::Refused(OpenRefusal::NotIssued));
    };
    let current = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT assignment_generation = $2 AND revision_id = $3
         FROM firewall_rules_node_assignments
         WHERE node_id = $1
         FOR SHARE"
    ))
    .bind(target.node_id)
    // Read from the item's non-negative column, so the cast is lossless.
    .bind(assignment_generation.cast_signed())
    .bind(item.revision)
    .fetch_optional(&mut **tx)
    .await?;
    if current != Some(true) {
        return Ok(Opening::Refused(OpenRefusal::Superseded));
    }
    // Created on first use; a refusal rolls the row back with the rest. The
    // caller supplies the epoch and the first opener's sticks: nothing moves a
    // node to another epoch yet, so `EpochMismatch` has no recovery.
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch)
         VALUES ($1, $2)
         ON CONFLICT (node_id) DO NOTHING"
    ))
    .bind(target.node_id)
    .bind(manager_epoch)
    .execute(&mut **tx)
    .await?;
    let (epoch, round, round_issued_at, bound) =
        sqlx::query_as::<_, (i64, Option<i64>, Option<OffsetDateTime>, Option<i64>)>(sql!(
            "SELECT manager_epoch, pending_round, pending_round_issued_at,
                    pending_assignment_generation
             FROM firewall_rules_node_observations
             WHERE node_id = $1
             FOR UPDATE"
        ))
        .bind(target.node_id)
        .fetch_one(&mut **tx)
        .await?;
    if epoch != manager_epoch {
        return Ok(Opening::Refused(OpenRefusal::EpochMismatch));
    }
    match (round, round_issued_at, bound.map(non_negative).transpose()?) {
        (Some(round), Some(issued_at), Some(bound)) if bound == assignment_generation => {
            return Ok(Opening::AlreadyOpen(pending_round(
                round,
                issued_at,
                Some(assignment_generation),
            )?));
        }
        (_, _, Some(bound)) if bound > assignment_generation => {
            return Ok(Opening::Refused(OpenRefusal::LaterGeneration));
        }
        // Replaced: no round, one for an earlier generation, or an unbound
        // one, left from before rounds named their generation.
        _ => {}
    }
    wake_one(tx, wakeup, target.node_id).await?;
    // After the wakeup, which may have waited, on one clock reading. The item
    // lock, held since the item was read, keeps its token as checked.
    let opened = sqlx::query_as::<_, (i64, OffsetDateTime)>(sql!(
        "WITH clock AS (SELECT clock_timestamp() AS reading)
         UPDATE firewall_rules_node_observations o
         SET pending_round = coalesce(greatest(o.pending_round, o.accepted_round) + 1, 1),
             pending_round_issued_at = clock.reading,
             pending_assignment_generation = $2
         FROM clock
         WHERE o.node_id = $1
           AND EXISTS (
               SELECT 1
               FROM firewall_rules_rollout_items i
               WHERE i.rollout_id = $3
                 AND i.node_id = $1
                 AND i.lease_expires_at > clock.reading)
         RETURNING o.pending_round, o.pending_round_issued_at"
    ))
    .bind(target.node_id)
    .bind(assignment_generation.cast_signed())
    .bind(target.rollout_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((round, issued_at)) = opened else {
        return Ok(Opening::Refused(OpenRefusal::Lapsed));
    };
    Ok(Opening::Opened(pending_round(
        round,
        issued_at,
        Some(assignment_generation),
    )?))
}
