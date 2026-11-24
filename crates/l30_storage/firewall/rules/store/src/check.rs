//! Scheduling an issued target's due check: in one transaction, ask its node
//! a new question with its wakeup, wait on the question outstanding, or leave
//! the accepted answer for the outcome path. Asking or waiting ends the lease
//! with [`Requeue::Wait`] in the same commit; nothing else writes.
//!
//! Locks, in order: the opener's ([`open_round`](crate::open_round)), then
//! the wakeup's rows, through [`RecheckWakeup::held`] before the wakeup
//! itself; the item row, held since the opener's locks, is written last.
//! Blocking on a trigger row holds the lease handoff lock shared, so the next
//! claim, and every lease writer queued behind it, waits too, as for the
//! opener.
//!
//! Done items are never leased, so nothing here refreshes their evidence.

use std::time::Duration;

use jiff::Timestamp;
use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection};
use time::OffsetDateTime;

use crate::fetch::{PendingRound, non_negative};
use crate::report::{CensusV1, ReportV1, SlotV1};
use crate::round::{Locked, Pending, ask, lock_question};
use crate::{
    CensusGap, LeasedTarget, OpenRefusal, REPORT_MAX_AGE, Requeue, UnknownReason, Wakeup, requeue,
    roll_back,
};

/// How a check paces its node. No production values are approved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckPolicy {
    /// How long after asking or waiting the check is next due.
    pub revisit_in: Duration,
    /// How long after issuance a node that hasn't settled may be asked again.
    pub attempt_deadline: Duration,
}

/// A [`Wakeup`] a check may hold back.
pub trait RecheckWakeup: Wakeup {
    /// Whether `node`'s trigger row exists with `dead_at` set or `attempts`
    /// above 0, claimed or not: queueing would revive it, reset its backoff,
    /// or, on a claimed row, void its claim's failure. Locks the row
    /// `FOR UPDATE` until `tx` ends, so the wakeup takes no stronger lock. A
    /// missing row locks nothing; one created before the wakeup is coalesced
    /// unjudged. Judged after the attempt deadline, so a held first question
    /// waits until a dead row is reaped.
    fn held(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        node: NodeId,
    ) -> impl Future<Output = Result<bool, sqlx::Error>> + Send;
}

/// What [`check_target`] did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Check {
    /// This call opened the round and queued the wakeup; the lease ended
    /// waiting.
    Asked(PendingRound),
    /// The lease ended waiting.
    Waiting(Wait),
    /// The node's answer needs the outcome path. Nothing changed.
    Evidence(Evidence),
    /// Past the attempt deadline, the node hasn't settled. Nothing changed.
    TimedOut,
    /// The item's check isn't due. Nothing changed.
    NotDue,
    /// Nothing changed.
    Refused(OpenRefusal),
}

/// Why a check waited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Wait {
    /// The pending round can still be answered.
    Answer(PendingRound),
    /// The node answered this round still settling, too recently to ask
    /// again.
    Settling { round: u64 },
    /// Asking would revive or reset the node's held wakeup.
    WakeHeld,
}

/// The node's accepted answer about the issued assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Evidence {
    pub round: u64,
    /// When its round was issued: how old the answer is, whatever its verdict.
    pub round_issued_at: Timestamp,
    pub verdict: Verdict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Verdict {
    /// The census is complete and every slot verified or ineligible.
    Match,
    /// A slot enforces other rules with no apply pending.
    Mismatch,
    /// The host names no directory only managed namespaces occupy, so no
    /// census completes until that changes.
    Unowned,
}

/// Ask, wait or return the evidence for `target`'s check, as the module docs
/// say. Commits only when it returns [`Check::Asked`] or [`Check::Waiting`];
/// otherwise the lease stays in flight for the caller.
///
/// Authorized as [`open_round`](crate::open_round) authorizes, then only
/// while the check is due on the database clock. A pending round for the
/// issued generation is the current question. Without one, the accepted
/// answer counts if it names the issued generation, revision and the epoch:
/// a mismatch, `Unowned` or a match is returned at any age; any other
/// answer settles until its round is [`REPORT_MAX_AGE`] old, then is asked
/// again, as is an expired round. Asking again past `attempt_deadline` from
/// issuance is [`Check::TimedOut`]; the first question is always asked.
pub async fn check_target(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    manager_epoch: u64,
    policy: &CheckPolicy,
    wakeup: &mut impl RecheckWakeup,
) -> Result<Check, sqlx::Error> {
    let manager_epoch = i64::try_from(manager_epoch).map_err(|_| {
        sqlx::Error::Protocol(format!("manager epoch {manager_epoch} is too large"))
    })?;
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    match check(&mut tx, target, manager_epoch, policy, wakeup).await {
        Ok(ended @ (Check::Asked(_) | Check::Waiting(_))) => {
            tx.commit().await?;
            Ok(ended)
        }
        other => {
            roll_back(tx).await;
            other
        }
    }
}

/// What the accepted answer says, if it is about the issued assignment.
enum Answer {
    Settled(Verdict),
    Settling,
}

async fn check(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &LeasedTarget,
    manager_epoch: i64,
    policy: &CheckPolicy,
    wakeup: &mut impl RecheckWakeup,
) -> Result<Check, sqlx::Error> {
    let locked = match lock_question(tx, target, manager_epoch).await? {
        Ok(locked) => locked,
        Err(refusal) => return Ok(Check::Refused(refusal)),
    };
    // On one clock reading, under the locks.
    let (due, round, issued_at, document, current, overdue) = sqlx::query_as::<
        _,
        (
            bool,
            Option<i64>,
            Option<OffsetDateTime>,
            Option<String>,
            bool,
            bool,
        ),
    >(sql!(
        "WITH clock AS (SELECT clock_timestamp() AS reading)
         SELECT coalesce(i.next_check_at <= clock.reading, false),
                o.accepted_round,
                o.accepted_round_issued_at,
                o.accepted_report::text,
                coalesce(clock.reading
                         < o.accepted_round_issued_at + make_interval(secs => $3), false),
                clock.reading >= i.issued_at + make_interval(secs => $4)
         FROM clock, firewall_rules_rollout_items i
         JOIN firewall_rules_node_observations o ON o.node_id = i.node_id
         WHERE i.rollout_id = $1
           AND i.node_id = $2"
    ))
    .bind(target.rollout_id)
    .bind(target.node_id)
    .bind(REPORT_MAX_AGE.as_secs_f64())
    .bind(policy.attempt_deadline.as_secs_f64())
    .fetch_one(&mut **tx)
    .await?;
    if !due {
        return Ok(Check::NotDue);
    }
    let again = match locked.pending {
        Pending::Live(question) => return wait(tx, target, policy, Wait::Answer(question)).await,
        // Newer than any answer.
        Pending::Expired => true,
        // The schema sets the three together.
        Pending::None => match (round, issued_at, document) {
            (Some(round), Some(issued_at), Some(document)) => {
                match answer(&document, &locked, manager_epoch)? {
                    Some(Answer::Settled(verdict)) => {
                        return Ok(Check::Evidence(Evidence {
                            round: non_negative(round)?,
                            round_issued_at: Timestamp::from_nanosecond(
                                issued_at.unix_timestamp_nanos(),
                            )
                            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
                            verdict,
                        }));
                    }
                    Some(Answer::Settling) if current => {
                        let round = non_negative(round)?;
                        return wait(tx, target, policy, Wait::Settling { round }).await;
                    }
                    Some(Answer::Settling) => true,
                    None => false,
                }
            }
            _ => false,
        },
    };
    if again && overdue {
        return Ok(Check::TimedOut);
    }
    if wakeup.held(tx, target.node_id).await? {
        return wait(tx, target, policy, Wait::WakeHeld).await;
    }
    let Some(question) = ask(tx, target, locked.assignment_generation, wakeup).await? else {
        return Ok(Check::Refused(OpenRefusal::Lapsed));
    };
    end_lease(tx, target, policy).await?;
    Ok(Check::Asked(question))
}

/// `document`'s answer, if it is about `locked`'s assignment and epoch. A
/// document this version can't read is an error, never no answer.
fn answer(
    document: &str,
    locked: &Locked,
    manager_epoch: i64,
) -> Result<Option<Answer>, sqlx::Error> {
    let report = serde_json::from_str::<ReportV1>(document)
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
    if report.assignment_generation != locked.assignment_generation
        || report.revision_id != locked.revision
        || report.manager_epoch != manager_epoch.cast_unsigned()
    {
        return Ok(None);
    }
    let failed = |slot: &SlotV1| matches!(slot, SlotV1::Failed { .. });
    let verified = |slot: &SlotV1| {
        matches!(
            slot,
            SlotV1::Verified { .. }
                | SlotV1::Unknown {
                    reason: UnknownReason::Ineligible,
                    ..
                }
        )
    };
    Ok(Some(if report.slots.iter().any(failed) {
        Answer::Settled(Verdict::Mismatch)
    } else if report.census_gaps.contains(&CensusGap::Unowned) {
        Answer::Settled(Verdict::Unowned)
    } else if matches!(report.census, CensusV1::Complete) && report.slots.iter().all(verified) {
        Answer::Settled(Verdict::Match)
    } else {
        Answer::Settling
    }))
}

async fn wait(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &LeasedTarget,
    policy: &CheckPolicy,
    wait: Wait,
) -> Result<Check, sqlx::Error> {
    end_lease(tx, target, policy).await?;
    Ok(Check::Waiting(wait))
}

/// Under the item lock held since the opener's locks; the caller rolls back
/// on error.
async fn end_lease(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &LeasedTarget,
    policy: &CheckPolicy,
) -> Result<(), sqlx::Error> {
    let waiting = Requeue::Wait {
        next_check_in: policy.revisit_in,
    };
    if requeue(&mut **tx, target, &waiting).await? {
        return Ok(());
    }
    Err(sqlx::Error::Protocol(format!(
        "firewall rules item {}/{} was checked but didn't requeue",
        target.rollout_id, target.node_id
    )))
}
