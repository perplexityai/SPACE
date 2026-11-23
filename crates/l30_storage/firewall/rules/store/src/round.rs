//! Opening a round that asks an issued target's node what it enforces: the
//! question, bound to the generation the item issued, and the node's wakeup
//! commit together or not at all.
//!
//! Locks, in order: the rollout row (shared, so a pause or abort waits), the
//! lease handoff lock (shared), the item row, the node's assignment row
//! (shared), its observation row, and the wakeup's rows; a writer of
//! observations takes them in this order. Rounds
//! are numbered one past the node's larger pending or accepted round, so none
//! is reused while only acceptance or a larger round replaces a pending one.

use jiff::Timestamp;
use pplx_space_accounts::NodeId;
use pplx_space_rollout_queue::HeaderPhase;
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

use crate::lease::{HANDOFF_LOCK, stamped_work};
use crate::{ItemPhase, LeasedTarget, Wakeup, Work, roll_back, rollout_phase};

/// A round as the control plane asked it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Question {
    pub round: u64,
    /// The generation the target's item issued, which an answer must be about.
    pub assignment_generation: u64,
    /// When this round was opened, not when the assignment was issued.
    pub issued_at: Timestamp,
}

/// What [`open_round`] did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Opening {
    /// This call opened the round and queued the wakeup, and committed.
    Opened(Question),
    /// The round was already open for this generation; nothing changed.
    AlreadyOpen(Question),
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
/// generation is returned as it is; any other pending round is replaced by a
/// new one. The lease is rechecked at the clock reading that times the round,
/// after the wakeup, which can wait on a lock.
pub async fn open_round(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    manager_epoch: u64,
    wakeup: &mut impl Wakeup,
) -> Result<Opening, sqlx::Error> {
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
            other
        }
    }
}

async fn open(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &LeasedTarget,
    manager_epoch: i64,
    wakeup: &mut impl Wakeup,
) -> Result<Opening, sqlx::Error> {
    let header = sqlx::query_as::<_, (bool, uuid::Uuid)>(sql!(
        "SELECT phase IN ($2, $3), to_revision_id
         FROM firewall_rules_rollouts
         WHERE id = $1
         FOR SHARE"
    ))
    .bind(target.rollout_id)
    .bind(rollout_phase(HeaderPhase::Canary))
    .bind(rollout_phase(HeaderPhase::Rolling))
    .fetch_optional(&mut **tx)
    .await?;
    let Some((checking, revision)) = header else {
        return Ok(Opening::Refused(OpenRefusal::NotATarget));
    };
    // Shared, as issuance and renewal take it.
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
    let item = sqlx::query_as::<_, (bool, Option<i64>, Option<OffsetDateTime>)>(sql!(
        "SELECT phase = $4 AND lease_token = $3 AND lease_expires_at > clock_timestamp(),
                issued_assignment_generation,
                issued_at
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
    let Some((leased, generation, issued_at)) = item else {
        return Ok(Opening::Refused(OpenRefusal::NotATarget));
    };
    if !checking {
        return Ok(Opening::Refused(OpenRefusal::NotChecking));
    }
    if !leased {
        return Ok(Opening::Refused(OpenRefusal::LeaseLost));
    }
    // The stamp, not the lease's hint, says what was issued.
    let (
        Some(generation),
        Work::Check {
            assignment_generation,
            ..
        },
    ) = (
        generation,
        stamped_work(target.rollout_id, target.node_id, generation, issued_at)?,
    )
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
    .bind(generation)
    .bind(revision)
    .fetch_optional(&mut **tx)
    .await?;
    if current != Some(true) {
        return Ok(Opening::Refused(OpenRefusal::Superseded));
    }
    // Created on first use; a refusal rolls the row back with the rest.
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
    match (round, round_issued_at, bound) {
        (Some(round), Some(issued_at), Some(bound)) if bound == generation => {
            return Ok(Opening::AlreadyOpen(question(
                target.node_id,
                round,
                assignment_generation,
                issued_at,
            )?));
        }
        // Generations only rise, so this round asks about a later assignment.
        (_, _, Some(bound)) if bound > generation => {
            return Err(invalid_observation(
                target.node_id,
                "a pending round for a later generation than its assignment",
            ));
        }
        _ => {}
    }
    let queued = wakeup.wake(tx, target.node_id).await?;
    if queued != 1 {
        return Err(sqlx::Error::Protocol(format!(
            "firewall rules wakeup for node {} queued {queued} nodes, not 1",
            target.node_id
        )));
    }
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
    .bind(generation)
    .bind(target.rollout_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((round, issued_at)) = opened else {
        return Ok(Opening::Refused(OpenRefusal::Lapsed));
    };
    Ok(Opening::Opened(question(
        target.node_id,
        round,
        assignment_generation,
        issued_at,
    )?))
}

fn question(
    node: NodeId,
    round: i64,
    assignment_generation: u64,
    issued_at: OffsetDateTime,
) -> Result<Question, sqlx::Error> {
    Ok(Question {
        round: u64::try_from(round)
            .map_err(|_| invalid_observation(node, "a negative pending round"))?,
        assignment_generation,
        issued_at: Timestamp::from_nanosecond(issued_at.unix_timestamp_nanos())
            .map_err(|_| invalid_observation(node, "an out-of-range round time"))?,
    })
}

/// A violation of the schema's or the opener's invariants is a decode error
/// rather than a guess.
fn invalid_observation(node: NodeId, what: &str) -> sqlx::Error {
    sqlx::Error::Decode(format!("firewall rules observation of node {node} has {what}").into())
}
