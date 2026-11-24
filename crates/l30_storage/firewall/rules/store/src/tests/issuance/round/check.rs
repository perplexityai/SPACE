//! Scheduling due checks against real Postgres, the real trigger queue and
//! the real report store, with the round tests' fixtures. Time passes by
//! moving stored times back on the database clock.

use super::*;
use crate::test_support::make_checks_due;

/// Fixture values only: no production pacing is approved.
const POLICY: CheckPolicy = CheckPolicy {
    revisit_in: Duration::from_secs(150),
    attempt_deadline: Duration::from_secs(900),
};

impl RecheckWakeup for Queue {
    /// The predicate the trait documents, inline until the trigger queue
    /// owns it.
    async fn held(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        node: NodeId,
    ) -> Result<bool, sqlx::Error> {
        let held = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT dead_at IS NOT NULL OR attempts > 0
             FROM node_reconcile_triggers
             WHERE node_id = $1
             FOR UPDATE"
        ))
        .bind(node)
        .fetch_optional(&mut **tx)
        .await?;
        Ok(held == Some(true))
    }
}

async fn check_with(
    pool: &PgPool,
    target: &LeasedTarget,
    epoch: u64,
) -> Result<Check, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    check_target(&mut conn, target, epoch, &POLICY, &mut Queue::Wake).await
}

async fn checked(pool: &PgPool, target: &LeasedTarget) -> Result<Check, sqlx::Error> {
    check_with(pool, target, EPOCH).await
}

fn spawn_check(
    pool: &PgPool,
    target: &LeasedTarget,
) -> tokio::task::JoinHandle<Result<Check, sqlx::Error>> {
    let (pool, target) = (pool.clone(), target.clone());
    tokio::spawn(async move { checked(&pool, &target).await })
}

/// Make `rollout`'s checks due and lease its one target.
async fn due(pool: &PgPool, rollout: RolloutId) -> TestResult<LeasedTarget> {
    make_checks_due(pool, rollout).await?;
    let mut leased = lease(pool, rollout, 1).await?;
    assert_eq!(leased.len(), 1, "{leased:?}");
    Ok(leased.remove(0))
}

/// Run `statement` on `node` with `age` in seconds, to move a stored time
/// back on the database clock.
async fn age(pool: &PgPool, statement: &'static str, node: NodeId, age: Duration) -> TestResult {
    sqlx::query(statement)
        .bind(node)
        .bind(age.as_secs_f64())
        .execute(pool)
        .await?;
    Ok(())
}

const AGE_ROUND: &str = sql!(
    "UPDATE firewall_rules_node_observations
     SET pending_round_issued_at = clock_timestamp() - make_interval(secs => $2)
     WHERE node_id = $1"
);
const AGE_ANSWER: &str = sql!(
    "UPDATE firewall_rules_node_observations
     SET accepted_round_issued_at = clock_timestamp() - make_interval(secs => $2)
     WHERE node_id = $1"
);
const AGE_ISSUE: &str = sql!(
    "UPDATE firewall_rules_rollout_items
     SET issued_at = clock_timestamp() - make_interval(secs => $2)
     WHERE node_id = $1"
);

/// `question`'s answer for `node`, as its worker would report it.
async fn report(
    pool: &PgPool,
    rollout: RolloutId,
    question: PendingRound,
    census: Census,
    slots: &[SlotOutcome],
) -> TestResult<Report> {
    let slots = slots
        .iter()
        .zip(0..)
        .map(|(&outcome, slot_id)| SlotEvidence {
            slot_id,
            slot_generation: 1,
            outcome,
        })
        .collect();
    Ok(Report::new(
        question.round,
        question.assignment_generation.expect("a bound round"),
        RevisionId(revision_of(pool, rollout).await?),
        EPOCH,
        census,
        slots,
    )?)
}

async fn accepted(pool: &PgPool, node: NodeId, report: &Report) -> TestResult {
    let mut conn = pool.acquire().await?;
    assert_eq!(
        accept_report(&mut conn, node, report).await?,
        Acceptance::Accepted
    );
    Ok(())
}

/// `node`'s item phase and attempts, and whether its check is in the future.
async fn schedule(
    pool: &PgPool,
    rollout: RolloutId,
    node: NodeId,
) -> TestResult<(String, i64, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT phase, attempts, next_check_at > clock_timestamp()
         FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(pool)
    .await?)
}

/// Waiting, as an ask or a wait ends a lease: pending, the attempt given back
/// and the check in the future.
fn waited() -> (String, i64, bool) {
    ("pending".to_owned(), 0, true)
}

/// Everything a check may write for `node`, its whole item row included.
async fn state(pool: &PgPool, rollout: RolloutId, node: NodeId) -> TestResult<(Effects, String)> {
    Ok((
        effects(pool, rollout, node).await?,
        row(pool, rollout, node).await?,
    ))
}

/// Catches a check that needs more than the saved stamp and due time, or
/// issues again; a question bound to anything but the issued generation; an
/// ask without exactly one wakeup, or one that keeps the lease, charges the
/// attempt or leaves the item due; a lost reply replayed into a second
/// question; a live round asked again, retimed or rewoken, or waited on
/// with a charge or an immediate rescan; and one waited on before it's due.
#[tokio::test(flavor = "current_thread")]
async fn a_saved_due_check_asks_once_then_waits() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    let stamp = {
        // The issuing controller's connection, gone before any check.
        let mut conn = db.pool.acquire().await?;
        let canary = lease_first_canary(&mut conn, LEASE, SCAN)
            .await?
            .expect("a canary");
        let issued =
            issue_canary_and_release(&mut conn, &canary, FIRST_CHECK_IN, &mut Queue::Wake).await?;
        let Issuance::Issued(stamp) = issued else {
            panic!("the canary wasn't issued: {issued:?}");
        };
        stamp
    };
    let issued = effects(&db.pool, rollout, node).await?;

    let target = due(&db.pool, rollout).await?;
    let Check::Asked(question) = checked(&db.pool, &target).await? else {
        panic!("the due check didn't ask");
    };
    assert_eq!(
        (question.round, question.assignment_generation),
        (1, Some(stamp.assignment_generation))
    );
    let asked = effects(&db.pool, rollout, node).await?;
    assert_eq!(asked.assignment, issued.assignment);
    let stamp_of = |effects: &Effects| effects.stamp.map(|stamp| (stamp.0, stamp.1));
    assert_eq!(stamp_of(&asked), stamp_of(&issued));
    let trigger = |effects: &Effects| effects.trigger.clone().map(|t| (t.0, t.1));
    let (reasons, generation) = trigger(&issued).expect("issuance queued a wakeup");
    assert_eq!(trigger(&asked), Some((reasons, generation + 1)));
    assert_eq!(schedule(&db.pool, rollout, node).await?, waited());
    assert!(lease(&db.pool, rollout, 1).await?.is_empty());

    // The reply was lost, and the check runs again on the same lease.
    assert_eq!(
        checked(&db.pool, &target).await?,
        Check::Refused(OpenRefusal::LeaseLost)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, asked);

    let target = due(&db.pool, rollout).await?;
    assert_eq!(
        checked(&db.pool, &target).await?,
        Check::Waiting(Wait::Answer(question))
    );
    let again = effects(&db.pool, rollout, node).await?;
    assert_eq!(
        (&again.assignment, &again.trigger, &again.observation),
        (&asked.assignment, &asked.trigger, &asked.observation)
    );
    assert_eq!(schedule(&db.pool, rollout, node).await?, waited());
    assert!(lease(&db.pool, rollout, 1).await?.is_empty());

    // Leased before it's due, as an older controller might hold it.
    let early = held(&db.pool, rollout, node).await?;
    let before = state(&db.pool, rollout, node).await?;
    assert_eq!(checked(&db.pool, &early).await?, Check::NotDue);
    assert_eq!(state(&db.pool, rollout, node).await?, before);
    Ok(())
}

/// Drives assignment generation 12 through round 7, an Applying answer and
/// round 8. Catches a settling answer asked again before its round is too
/// old to answer, never, or past the attempt deadline; a new round numbered
/// or bound wrongly; and one that overwrites the answer it follows.
#[tokio::test(flavor = "current_thread")]
async fn a_settling_answer_is_asked_again_once_its_round_is_too_old() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    // The previous assignment, and its unanswered round 6.
    assign(&db.pool, node, 11).await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (
             node_id, manager_epoch, pending_round, pending_round_issued_at,
             pending_assignment_generation)
         VALUES ($1, 1, 6, now(), 11)"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    let target = lease(&db.pool, rollout, 1).await?.remove(0);
    assert_eq!(issued(&db.pool, &target).await?.assignment_generation, 12);
    assert!(requeue(&db.pool, &target, &RELEASE).await?);

    let target = due(&db.pool, rollout).await?;
    let Check::Asked(seventh) = checked(&db.pool, &target).await? else {
        panic!("the due check didn't ask");
    };
    assert_eq!(
        (seventh.round, seventh.assignment_generation),
        (7, Some(12))
    );
    let applying = [SlotOutcome::Verified, SlotOutcome::Applying];
    let applying = report(&db.pool, rollout, seventh, Census::Complete, &applying).await?;
    accepted(&db.pool, node, &applying).await?;
    let answered = effects(&db.pool, rollout, node).await?.observation;

    assert!(lease(&db.pool, rollout, 1).await?.is_empty(), "early tick");
    let target = due(&db.pool, rollout).await?;
    assert_eq!(
        checked(&db.pool, &target).await?,
        Check::Waiting(Wait::Settling { round: 7 })
    );
    assert_eq!(
        effects(&db.pool, rollout, node).await?.observation,
        answered
    );
    assert_eq!(schedule(&db.pool, rollout, node).await?, waited());

    age(&db.pool, AGE_ANSWER, node, REPORT_MAX_AGE).await?;
    age(&db.pool, AGE_ISSUE, node, POLICY.attempt_deadline).await?;
    let target = due(&db.pool, rollout).await?;
    let unsettled = state(&db.pool, rollout, node).await?;
    assert_eq!(checked(&db.pool, &target).await?, Check::TimedOut);
    assert_eq!(state(&db.pool, rollout, node).await?, unsettled);

    // Before the deadline, under the same lease.
    age(&db.pool, AGE_ISSUE, node, POLICY.attempt_deadline / 2).await?;
    let answered = effects(&db.pool, rollout, node).await?.observation;
    let Check::Asked(eighth) = checked(&db.pool, &target).await? else {
        panic!("the old answer wasn't asked again");
    };
    assert_eq!((eighth.round, eighth.assignment_generation), (8, Some(12)));
    let asked = effects(&db.pool, rollout, node).await?;
    assert_eq!(asked.assignment.map(|assignment| assignment.0), Some(12));
    let kept = |o: Option<Observation>| {
        o.map(|o| {
            (
                o.accepted_round,
                o.accepted_round_issued_at,
                o.accepted_report,
            )
        })
    };
    assert_eq!(kept(asked.observation), kept(answered));
    Ok(())
}

/// Catches a check that asks over a mismatch, an unowned census or a match,
/// hiding it before the outcome path reads it, or that writes anything, ends
/// the lease or wakes the node while returning one; a match that stops
/// counting once its round is old, which would ask a late answerer forever,
/// or past the deadline; and a mismatch outranked by the other gaps.
#[tokio::test(flavor = "current_thread")]
async fn settled_answers_are_left_for_the_outcome_path_at_any_age() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    make_checks_due(&db.pool, rollout).await?;
    let unowned = Census::Incomplete {
        gaps: vec![CensusGap::Unowned, CensusGap::ListingFailed],
        extra_namespaces: 2,
    };
    let cases = [
        (
            unowned.clone(),
            vec![
                SlotOutcome::Applying,
                SlotOutcome::Failed(Failure::RulesMismatch),
            ],
            Verdict::Mismatch,
        ),
        (unowned, vec![SlotOutcome::Applying], Verdict::Unowned),
        (
            Census::Complete,
            vec![
                SlotOutcome::Verified,
                SlotOutcome::Unknown(UnknownReason::Ineligible),
            ],
            Verdict::Match,
        ),
    ];
    for (census, slots, verdict) in cases {
        // Asked as the outcome path might, under the lease it still holds.
        let question = opened(&db.pool, &target).await?;
        let answer = report(&db.pool, rollout, question, census, &slots).await?;
        accepted(&db.pool, node, &answer).await?;
        if verdict == Verdict::Match {
            // Answered late and seen later still, past the deadline.
            age(&db.pool, AGE_ANSWER, node, REPORT_MAX_AGE * 2).await?;
            age(&db.pool, AGE_ISSUE, node, POLICY.attempt_deadline).await?;
        }
        let before = state(&db.pool, rollout, node).await?;
        let round_issued_at = before
            .0
            .observation
            .as_ref()
            .and_then(|o| o.accepted_round_issued_at)
            .map(jiff)
            .expect("an accepted round");
        assert_eq!(
            checked(&db.pool, &target).await?,
            Check::Evidence(Evidence {
                round: question.round,
                round_issued_at,
                verdict,
            })
        );
        assert_eq!(state(&db.pool, rollout, node).await?, before, "{verdict:?}");
    }
    Ok(())
}

/// Catches a check run before it's due, which would skip the first check's
/// delay; a first question refused past the deadline, which would time out a
/// node never asked; an unanswered round asked again past the deadline, or
/// with the deadline taken from a round rather than the issuance, so that
/// reopening rolls it forward; a timeout that writes anything or ends the
/// lease; and an expired round that isn't replaced before the deadline.
#[tokio::test(flavor = "current_thread")]
async fn an_unanswered_round_is_asked_again_until_the_attempt_deadline() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    // Still under the issuing lease, before the first check is due.
    let issued = state(&db.pool, rollout, node).await?;
    assert_eq!(checked(&db.pool, &target).await?, Check::NotDue);
    assert_eq!(state(&db.pool, rollout, node).await?, issued);
    assert!(requeue(&db.pool, &target, &RELEASE).await?);

    age(&db.pool, AGE_ISSUE, node, POLICY.attempt_deadline * 2).await?;
    let target = due(&db.pool, rollout).await?;
    let Check::Asked(first) = checked(&db.pool, &target).await? else {
        panic!("the first question wasn't asked");
    };

    age(&db.pool, AGE_ROUND, node, REPORT_MAX_AGE).await?;
    let target = due(&db.pool, rollout).await?;
    let silent = state(&db.pool, rollout, node).await?;
    assert_eq!(checked(&db.pool, &target).await?, Check::TimedOut);
    assert_eq!(state(&db.pool, rollout, node).await?, silent);

    // Before the deadline, under the same lease.
    age(&db.pool, AGE_ISSUE, node, POLICY.attempt_deadline / 2).await?;
    let Check::Asked(second) = checked(&db.pool, &target).await? else {
        panic!("the expired round wasn't asked again");
    };
    assert_eq!(second.round, first.round + 1);
    assert_eq!(schedule(&db.pool, rollout, node).await?, waited());
    Ok(())
}

/// Catches a check that queues the wakeup of a trigger backing off, past its
/// backoff but still counting attempts, claimed after a failure, or
/// dead-lettered, reviving or resetting another reason's delivery; one that
/// opens an unwakeable round instead; a held check that charges the attempt
/// or comes straight back; and repeated checks of a live round that touch
/// the trigger.
#[tokio::test(flavor = "current_thread")]
async fn a_held_wakeup_is_waited_on_and_left_as_it_is() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    assert!(requeue(&db.pool, &target, &RELEASE).await?);
    enqueue_triggers(&db.pool, [node], ReconcileReason::NodeUpdated).await?;
    let trigger = || async {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT to_jsonb(t)::text FROM node_reconcile_triggers t WHERE node_id = $1"
        ))
        .bind(node)
        .fetch_optional(&db.pool)
        .await
    };
    let held = [
        sql!(
            "UPDATE node_reconcile_triggers
             SET attempts = 3, last_error = 'unreachable',
                 next_attempt_at = now() + interval '30 seconds'
             WHERE node_id = $1"
        ),
        sql!(
            "UPDATE node_reconcile_triggers
             SET next_attempt_at = now() - interval '1 second' WHERE node_id = $1"
        ),
        sql!(
            "UPDATE node_reconcile_triggers
             SET claim_token = 'claim', claim_generation = generation, claimed_at = now()
             WHERE node_id = $1"
        ),
        sql!(
            "UPDATE node_reconcile_triggers
             SET claim_token = '', claim_generation = 0, claimed_at = NULL, attempts = 16,
                 dead_at = now(), dead_reason = 'unreachable'
             WHERE node_id = $1"
        ),
    ];
    for hold in held {
        sqlx::query(hold).bind(node).execute(&db.pool).await?;
        let before = trigger().await?;
        for _ in 0..2 {
            let target = due(&db.pool, rollout).await?;
            assert_eq!(
                checked(&db.pool, &target).await?,
                Check::Waiting(Wait::WakeHeld),
                "{hold}"
            );
            assert_eq!(trigger().await?, before, "{hold}");
            let observation = effects(&db.pool, rollout, node).await?.observation;
            assert_eq!(observation.and_then(|o| o.pending_round), None, "{hold}");
            assert_eq!(schedule(&db.pool, rollout, node).await?, waited());
        }
    }

    sqlx::query(sql!(
        "DELETE FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    let target = due(&db.pool, rollout).await?;
    assert!(matches!(checked(&db.pool, &target).await?, Check::Asked(_)));
    let woken = trigger().await?;
    assert!(woken.is_some());
    for _ in 0..2 {
        let target = due(&db.pool, rollout).await?;
        assert!(matches!(
            checked(&db.pool, &target).await?,
            Check::Waiting(Wait::Answer(_))
        ));
        assert_eq!(trigger().await?, woken);
    }
    Ok(())
}

/// Catches a check that asks, waits or ends the lease under stale authority:
/// a paused or aborted rollout, a lost token, a superseded assignment or
/// another manager's observations.
#[tokio::test(flavor = "current_thread")]
async fn a_check_under_stale_authority_changes_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    opened(&db.pool, &target).await?;
    make_checks_due(&db.pool, rollout).await?;
    let before = state(&db.pool, rollout, node).await?;
    for phase in ["paused", "aborted"] {
        set_phase(&db.pool, rollout, phase).await?;
        assert_eq!(
            checked(&db.pool, &target).await?,
            Check::Refused(OpenRefusal::NotChecking),
            "{phase}"
        );
    }
    set_phase(&db.pool, rollout, "rolling").await?;
    let forged = LeasedTarget {
        lease_token: Uuid::now_v7(),
        ..target.clone()
    };
    assert_eq!(
        checked(&db.pool, &forged).await?,
        Check::Refused(OpenRefusal::LeaseLost)
    );
    assert_eq!(
        check_with(&db.pool, &target, EPOCH + 1).await?,
        Check::Refused(OpenRefusal::EpochMismatch)
    );
    let bump = |by: i64| {
        sqlx::query(sql!(
            "UPDATE firewall_rules_node_assignments
             SET assignment_generation = assignment_generation + $2 WHERE node_id = $1"
        ))
        .bind(node)
        .bind(by)
        .execute(&db.pool)
    };
    bump(1).await?;
    assert_eq!(
        checked(&db.pool, &target).await?,
        Check::Refused(OpenRefusal::Superseded)
    );
    bump(-1).await?;
    assert_eq!(state(&db.pool, rollout, node).await?, before);
    Ok(())
}

/// Catches a check that decides before the observation lock: a report that
/// commits ahead of it would be waited past, or asked over, rather than
/// returned.
#[tokio::test(flavor = "current_thread")]
async fn a_report_that_reaches_the_observation_first_is_the_answer() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    let question = opened(&db.pool, &target).await?;
    make_checks_due(&db.pool, rollout).await?;
    let mismatch = [SlotOutcome::Failed(Failure::RulesMismatch)];
    let mismatch = report(&db.pool, rollout, question, Census::Complete, &mismatch).await?;
    let trigger = effects(&db.pool, rollout, node).await?.trigger;

    let mut holder = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_node_observations WHERE node_id = $1 FOR UPDATE"
    ))
    .bind(node)
    .execute(&mut *holder)
    .await?;
    let pool = db.pool.clone();
    let reporting = tokio::spawn(async move {
        let mut conn = pool.acquire().await?;
        accept_report(&mut conn, node, &mismatch).await
    });
    wait_for_lock_waiters(&db.pool, 1).await?;
    let checking = spawn_check(&db.pool, &target);
    wait_for_lock_waiters(&db.pool, 2).await?;
    holder.commit().await?;
    assert_eq!(
        tokio::time::timeout(WAIT, reporting).await???,
        Acceptance::Accepted
    );
    let Check::Evidence(evidence) = tokio::time::timeout(WAIT, checking).await??? else {
        panic!("the check didn't return the report that committed first");
    };
    assert_eq!(
        (evidence.round, evidence.verdict),
        (question.round, Verdict::Mismatch)
    );
    let after = effects(&db.pool, rollout, node).await?;
    assert_eq!(after.trigger, trigger);
    assert_eq!(after.observation.and_then(|o| o.pending_round), None);
    Ok(())
}

/// Catches checks that don't serialize on the item, which would ask twice or
/// wake twice on one lease, and one that doesn't hold the header from its
/// checks to its commit, which would ask after a pause that locked it first.
#[tokio::test(flavor = "current_thread")]
async fn racing_checks_ask_once_and_a_pause_first_stops_them() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    make_checks_due(&db.pool, rollout).await?;
    let before = effects(&db.pool, rollout, node).await?;

    let mut pausing = hold_header(&db.pool, rollout).await?;
    let checking = spawn_check(&db.pool, &target);
    wait_for_lock_waiters(&db.pool, 1).await?;
    set_phase(&mut *pausing, rollout, "paused").await?;
    pausing.commit().await?;
    assert_eq!(
        tokio::time::timeout(WAIT, checking).await???,
        Check::Refused(OpenRefusal::NotChecking)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, before);
    set_phase(&db.pool, rollout, "rolling").await?;

    let holder = hold_header(&db.pool, rollout).await?;
    let racing = [
        spawn_check(&db.pool, &target),
        spawn_check(&db.pool, &target),
    ];
    wait_for_lock_waiters(&db.pool, 2).await?;
    holder.commit().await?;
    let mut outcomes = Vec::new();
    for check in racing {
        outcomes.push(tokio::time::timeout(WAIT, check).await???);
    }
    assert!(
        matches!(
            outcomes[..],
            [Check::Asked(_), Check::Refused(OpenRefusal::LeaseLost)]
                | [Check::Refused(OpenRefusal::LeaseLost), Check::Asked(_)]
        ),
        "{outcomes:?}"
    );
    let generation = |effects: Effects| effects.trigger.map(|t| t.1);
    assert_eq!(
        generation(effects(&db.pool, rollout, node).await?),
        generation(before).map(|g| g + 1)
    );
    Ok(())
}

/// Catches a census incomplete for a reason other than ownership, or a slot
/// unknown for a reason other than ineligibility, taken as a match rather
/// than waited on while it settles.
#[tokio::test(flavor = "current_thread")]
async fn incomplete_answers_settle_rather_than_match() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    assert!(requeue(&db.pool, &target, &RELEASE).await?);
    let incomplete = |gaps, extra_namespaces| Census::Incomplete {
        gaps,
        extra_namespaces,
    };
    let cases = [
        (
            incomplete(vec![CensusGap::ListingFailed], 0),
            SlotOutcome::Verified,
        ),
        (incomplete(Vec::new(), 1), SlotOutcome::Verified),
        (
            Census::Complete,
            SlotOutcome::Unknown(UnknownReason::Unconfirmed),
        ),
    ];
    for (census, outcome) in cases {
        let target = due(&db.pool, rollout).await?;
        let question = opened(&db.pool, &target).await?;
        let answer = report(&db.pool, rollout, question, census, &[outcome]).await?;
        accepted(&db.pool, node, &answer).await?;
        assert_eq!(
            checked(&db.pool, &target).await?,
            Check::Waiting(Wait::Settling {
                round: question.round
            }),
            "{outcome:?}"
        );
    }
    Ok(())
}

/// Catches an accepted answer taken as the current one though it names an
/// earlier generation, another revision or another manager epoch.
#[tokio::test(flavor = "current_thread")]
async fn an_answer_about_another_assignment_is_no_answer() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, stamp) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    assert!(requeue(&db.pool, &target, &RELEASE).await?);
    let generation = stamp.assignment_generation;
    let revision = revision_of(&db.pool, rollout).await?;
    let foreign = [
        (generation - 1, revision, EPOCH),
        (generation, Uuid::now_v7(), EPOCH),
        (generation, revision, EPOCH + 1),
    ];
    for (asked, revision, epoch) in foreign {
        let target = due(&db.pool, rollout).await?;
        let Check::Asked(question) = checked(&db.pool, &target).await? else {
            panic!("the due check didn't ask");
        };
        // A complete match, as a document of this version, but about
        // something else.
        let document = serde_json::json!({
            "schema_version": 1,
            "round": question.round,
            "assignment_generation": asked,
            "revision_id": revision,
            "manager_epoch": epoch,
            "census": "complete",
            "census_gaps": [],
            "extra_namespaces": 0,
            "slots": [{"outcome": "verified", "slot_id": 0, "slot_generation": 1}],
        });
        sqlx::query(sql!(
            "UPDATE firewall_rules_node_observations
             SET accepted_round = pending_round,
                 accepted_round_issued_at = pending_round_issued_at,
                 accepted_report = $2::jsonb,
                 pending_round = NULL,
                 pending_round_issued_at = NULL,
                 pending_assignment_generation = NULL
             WHERE node_id = $1"
        ))
        .bind(node)
        .bind(document.to_string())
        .execute(&db.pool)
        .await?;
    }
    let target = due(&db.pool, rollout).await?;
    assert!(
        matches!(checked(&db.pool, &target).await?, Check::Asked(_)),
        "the last foreign answer was taken as current"
    );
    Ok(())
}
