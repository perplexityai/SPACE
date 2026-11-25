//! Opening a round against real Postgres and the real trigger queue, with the
//! issuance tests' fixtures.

use super::*;

/// The stable worker's manager epoch the fixtures use; there is no
/// production source yet.
const EPOCH: u64 = 1;
/// Bounds every wait on a spawned open.
const WAIT: Duration = Duration::from_secs(10);

async fn open_with(
    pool: &PgPool,
    target: &LeasedTarget,
    mut queue: Queue,
) -> Result<Opening, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    open_round(&mut conn, target, EPOCH, &mut queue).await
}

async fn opened(pool: &PgPool, target: &LeasedTarget) -> TestResult<PendingRound> {
    match open_with(pool, target, Queue::Wake).await? {
        Opening::Opened(question) => Ok(question),
        other => panic!("{target:?} opened nothing: {other:?}"),
    }
}

fn spawn_open(
    pool: &PgPool,
    target: &LeasedTarget,
) -> tokio::task::JoinHandle<Result<Opening, sqlx::Error>> {
    let (pool, target) = (pool.clone(), target.clone());
    tokio::spawn(async move { open_with(&pool, &target, Queue::Wake).await })
}

/// The one target of a rollout in `phase`, issued and still leased.
async fn issued_target(pool: &PgPool, phase: &str) -> TestResult<(RolloutId, LeasedTarget, Stamp)> {
    let rollout = rollout(pool, phase).await?;
    let nodes = online_nodes(pool, 1).await?;
    save_targets(pool, rollout, &nodes).await?;
    let target = lease(pool, rollout, 1).await?.remove(0);
    let stamp = issued(pool, &target).await?;
    Ok((rollout, target, stamp))
}

/// Accept `node`'s pending round with a report, as a later reporter would.
async fn accept(pool: &PgPool, node: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_observations
         SET accepted_round = pending_round,
             accepted_round_issued_at = pending_round_issued_at,
             accepted_report = '{\"enforced\": true}',
             pending_round = NULL,
             pending_round_issued_at = NULL,
             pending_assignment_generation = NULL
         WHERE node_id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Catches openers that serialize on neither the item nor the observation,
/// which would open two rounds; a duplicate or a lost reply that opens again,
/// refreshes the round's time or queues another wakeup; a round bound to
/// anything but the item's generation or timed by its issuance; an open that
/// writes the assignment or the stamp; and one that refuses a canary, which
/// is checked as a rolling target is.
#[tokio::test(flavor = "current_thread")]
async fn racing_and_repeated_opens_converge_on_one_round_and_one_wakeup() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let (rollout, target, stamp) = issued_target(&db.pool, "canary").await?;
    let node = target.node_id;
    // An existing row, so the second opener can't just wait on the first's
    // insert.
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch) VALUES ($1, 1)"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    let before = effects(&db.pool, rollout, node).await?;

    let holder = hold_header(&db.pool, rollout).await?;
    let racing = [spawn_open(&db.pool, &target), spawn_open(&db.pool, &target)];
    wait_for_lock_waiters(&db.pool, 2).await?;
    let start = clock(&db.pool).await?;
    holder.commit().await?;
    let mut outcomes = Vec::new();
    for open in racing {
        outcomes.push(tokio::time::timeout(WAIT, open).await???);
    }
    let end = clock(&db.pool).await?;
    let question = match outcomes[..] {
        [Opening::Opened(question), Opening::AlreadyOpen(again)]
        | [Opening::AlreadyOpen(again), Opening::Opened(question)]
            if again == question =>
        {
            question
        }
        _ => panic!("racing opens didn't converge: {outcomes:?}"),
    };
    assert_eq!(question.round, 1);
    assert_eq!(
        question.assignment_generation,
        Some(stamp.assignment_generation)
    );
    assert!((start..=end).contains(&question.issued_at), "{question:?}");

    let open = effects(&db.pool, rollout, node).await?;
    assert_eq!(
        (&open.assignment, &open.stamp),
        (&before.assignment, &before.stamp)
    );
    let trigger = |effects: &Effects| effects.trigger.clone().map(|t| (t.0, t.1));
    let (reasons, generation) = trigger(&before).expect("issuance queued a wakeup");
    assert_eq!(trigger(&open), Some((reasons, generation + 1)));
    assert_eq!(
        open.observation,
        Some(Observation {
            manager_epoch: 1,
            pending_round: Some(1),
            pending_round_issued_at: Some(OffsetDateTime::from_unix_timestamp_nanos(
                question.issued_at.as_nanosecond()
            )?),
            pending_assignment_generation: Some(i64::try_from(stamp.assignment_generation)?),
            accepted_round: None,
            accepted_round_issued_at: None,
            accepted_report: None,
        })
    );

    // Called again under the same lease, as after a lost response.
    assert_eq!(
        open_with(&db.pool, &target, Queue::Wake).await?,
        Opening::AlreadyOpen(question)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, open);
    Ok(())
}

/// Catches authority taken from the lease or a caller rather than checked
/// when opening: a target the rollout didn't save, a lost token, an
/// unissued item, an assignment whose generation or revision moved on, a
/// rollout that stopped checking, and another manager's observations would
/// each open a round or queue a wakeup; and a pending round for a later
/// generation, which only a hand-written row leaves, would be replaced by an
/// older question or fail the open, which a retrying scheduler would repeat.
#[tokio::test(flavor = "current_thread")]
async fn unauthorized_opens_change_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 3).await?;
    save_targets(&db.pool, rollout, &nodes[..2]).await?;
    let leased = lease(&db.pool, rollout, 10).await?;
    let (target, unissued) = (&leased[0], &leased[1]);
    issued(&db.pool, target).await?;
    let node = target.node_id;
    let set = |statement| {
        let pool = db.pool.clone();
        async move {
            sqlx::query(statement).bind(node).execute(&pool).await?;
            TestResult::Ok(())
        }
    };

    let forged = [
        (
            LeasedTarget {
                node_id: nodes[2],
                ..target.clone()
            },
            OpenRefusal::NotATarget,
        ),
        (
            LeasedTarget {
                rollout_id: RolloutId(Uuid::now_v7()),
                ..target.clone()
            },
            OpenRefusal::NotATarget,
        ),
        (
            LeasedTarget {
                lease_token: Uuid::now_v7(),
                ..target.clone()
            },
            OpenRefusal::LeaseLost,
        ),
        (unissued.clone(), OpenRefusal::NotIssued),
    ];
    for (forged, refusal) in forged {
        let before = effects(&db.pool, rollout, forged.node_id).await?;
        assert_eq!(
            open_with(&db.pool, &forged, Queue::Wake).await?,
            Opening::Refused(refusal),
            "{forged:?}"
        );
        assert_eq!(effects(&db.pool, rollout, forged.node_id).await?, before);
    }

    let moved = [
        (
            sql!(
                "UPDATE firewall_rules_node_assignments
                 SET assignment_generation = assignment_generation + 1 WHERE node_id = $1"
            ),
            sql!(
                "UPDATE firewall_rules_node_assignments
                 SET assignment_generation = assignment_generation - 1 WHERE node_id = $1"
            ),
        ),
        (
            sql!(
                "UPDATE firewall_rules_node_assignments
                 SET revision_id = (SELECT id FROM firewall_rules_revisions
                                    WHERE id <> revision_id LIMIT 1)
                 WHERE node_id = $1"
            ),
            sql!(
                "UPDATE firewall_rules_node_assignments a
                 SET revision_id = r.to_revision_id
                 FROM firewall_rules_rollouts r
                 WHERE a.node_id = $1 AND r.phase = 'rolling'"
            ),
        ),
    ];
    let other = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, '[]'::jsonb)"
    ))
    .bind(other)
    .execute(&db.pool)
    .await?;
    for (change, undo) in moved {
        set(change).await?;
        let before = effects(&db.pool, rollout, node).await?;
        assert_eq!(
            open_with(&db.pool, target, Queue::Wake).await?,
            Opening::Refused(OpenRefusal::Superseded)
        );
        assert_eq!(effects(&db.pool, rollout, node).await?, before);
        set(undo).await?;
    }
    let before = effects(&db.pool, rollout, node).await?;
    for phase in ["paused", "aborted"] {
        set_phase(&db.pool, rollout, phase).await?;
        assert_eq!(
            open_with(&db.pool, target, Queue::Wake).await?,
            Opening::Refused(OpenRefusal::NotChecking),
            "{phase}"
        );
        assert_eq!(effects(&db.pool, rollout, node).await?, before, "{phase}");
    }
    set_phase(&db.pool, rollout, "rolling").await?;

    // Another manager's row, and then a round for a later generation.
    set(sql!(
        "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch) VALUES ($1, 2)"
    ))
    .await?;
    let foreign = effects(&db.pool, rollout, node).await?;
    assert_eq!(
        open_with(&db.pool, target, Queue::Wake).await?,
        Opening::Refused(OpenRefusal::EpochMismatch)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, foreign);
    set(sql!(
        "UPDATE firewall_rules_node_observations
         SET manager_epoch = 1, pending_round = 4, pending_round_issued_at = now(),
             pending_assignment_generation = 9
         WHERE node_id = $1"
    ))
    .await?;
    let later = effects(&db.pool, rollout, node).await?;
    assert_eq!(
        open_with(&db.pool, target, Queue::Wake).await?,
        Opening::Refused(OpenRefusal::LaterGeneration)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, later);
    Ok(())
}

/// Catches an opener that doesn't hold the header and the item from its
/// checks to its commit: a pause that took the header first would still open
/// a round, and a pause or a takeover of the lease that arrives while it waits
/// wouldn't wait for it.
#[tokio::test(flavor = "current_thread")]
async fn the_first_to_lock_wins_a_change_of_intent_or_lease_against_opening() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    let before = effects(&db.pool, rollout, node).await?;

    let mut pausing = hold_header(&db.pool, rollout).await?;
    let opening = spawn_open(&db.pool, &target);
    wait_for_lock_waiters(&db.pool, 1).await?;
    set_phase(&mut *pausing, rollout, "paused").await?;
    pausing.commit().await?;
    assert_eq!(
        tokio::time::timeout(WAIT, opening).await???,
        Opening::Refused(OpenRefusal::NotChecking)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, before);

    // Both wait behind an open that's waiting on the node's trigger row.
    set_phase(&db.pool, rollout, "rolling").await?;
    let locker = hold_trigger(&db.pool, node).await?;
    let opening = spawn_open(&db.pool, &target);
    wait_for_lock_waiters(&db.pool, 1).await?;
    let pool = db.pool.clone();
    // Phase only, which a key-share lock on the header wouldn't block.
    let pausing = tokio::spawn(async move {
        sqlx::query(sql!(
            "UPDATE firewall_rules_rollouts SET phase = 'paused' WHERE id = $1"
        ))
        .bind(rollout)
        .execute(&pool)
        .await
    });
    let pool = db.pool.clone();
    let taking_over = tokio::spawn(async move {
        sqlx::query(sql!(
            "UPDATE firewall_rules_rollout_items
             SET lease_token = gen_random_uuid() WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&pool)
        .await
    });
    wait_for_lock_waiters(&db.pool, 3).await?;
    locker.commit().await?;
    assert!(matches!(
        tokio::time::timeout(WAIT, opening).await???,
        Opening::Opened(_)
    ));
    tokio::time::timeout(WAIT, pausing).await???;
    tokio::time::timeout(WAIT, taking_over).await???;

    Ok(())
}

/// Catches a failure that commits part of an opening: a wakeup that fails, or
/// a lease that lapses while the wakeup runs, leaving a round, a trigger
/// change or a changed observation behind. The lease check must read the
/// clock at the round's write. A wakeup that queues no node or several fails
/// in the check issuance shares, which its own rollback test covers.
#[tokio::test(flavor = "current_thread")]
async fn any_failure_rolls_back_the_whole_opening() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, target, _) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    opened(&db.pool, &target).await?;
    accept(&db.pool, node).await?;
    let before = effects(&db.pool, rollout, node).await?;

    for queue in [Queue::Fails, Queue::ThenLapses] {
        let outcome = open_with(&db.pool, &target, queue).await;
        let lapsed = matches!(queue, Queue::ThenLapses);
        assert!(
            matches!(
                (lapsed, &outcome),
                (true, Ok(Opening::Refused(OpenRefusal::Lapsed))) | (false, Err(_))
            ),
            "{queue:?}: {outcome:?}"
        );
        assert_eq!(effects(&db.pool, rollout, node).await?, before, "{queue:?}");
    }
    Ok(())
}

/// Catches a round numbered from only the pending or only the accepted round,
/// which reissues a number a delayed answer may still carry; an accepted
/// report a new round overwrites; a round returned as already open for
/// another generation; and an unbound round, left from before rounds named
/// their generation, that's returned or refused rather than replaced.
#[tokio::test(flavor = "current_thread")]
async fn each_new_question_takes_a_new_round() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (first, target, stamp) = issued_target(&db.pool, "rolling").await?;
    let node = target.node_id;
    let generation = stamp.assignment_generation;
    let asked = |q: PendingRound| (q.round, q.assignment_generation);

    assert_eq!(
        asked(opened(&db.pool, &target).await?),
        (1, Some(generation))
    );
    accept(&db.pool, node).await?;
    let accepted = |effects: Effects| {
        effects.observation.map(|o| {
            (
                o.accepted_round,
                o.accepted_round_issued_at,
                o.accepted_report,
            )
        })
    };
    let evidence = accepted(effects(&db.pool, first, node).await?);
    assert_eq!(
        asked(opened(&db.pool, &target).await?),
        (2, Some(generation))
    );

    // Round 2 is still pending when the next rollout assigns the node anew.
    assert!(requeue(&db.pool, &target, &RELEASE).await?);
    set_phase(&db.pool, first, "completed").await?;
    let next = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, next, &[node]).await?;
    let target = lease(&db.pool, next, 1).await?.remove(0);
    let generation = issued(&db.pool, &target).await?.assignment_generation;
    assert_eq!(
        asked(opened(&db.pool, &target).await?),
        (3, Some(generation))
    );

    // Round 3 unbound, as 20261123200458's down and up leave a round. The
    // migration that then requires a generation won't apply over one, so its
    // check is dropped first where the schema has it.
    sqlx::query(sql!(
        "ALTER TABLE firewall_rules_node_observations
         DROP CONSTRAINT IF EXISTS firewall_rules_node_observations_pending_bound_check"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_observations
         SET pending_assignment_generation = NULL WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        asked(opened(&db.pool, &target).await?),
        (4, Some(generation))
    );

    assert_eq!(accepted(effects(&db.pool, next, node).await?), evidence);
    Ok(())
}
