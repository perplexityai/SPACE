//! Issuance against real Postgres and the real trigger queue, with the lease
//! tests' fixtures.

use pplx_space_cluster_trigger_queue::{enqueue_triggers, enqueue_triggers_tx};
use pplx_space_state_reconciliation::ReconcileReason;

use super::*;

/// Bound for tests only: there is no production first-check delay yet.
const FIRST_CHECK_IN: Duration = Duration::from_secs(90);
const FIREWALL: &str = "firewall_rules_changed";

/// The controller's wakeup, and faults injected around it.
#[derive(Clone, Copy, Debug)]
enum Queue {
    Wake,
    Fails,
    /// Queues, then reports this count.
    Reports(u64),
    /// Queues, then lapses the item's lease on the same transaction.
    ThenLapses,
}

impl Wakeup for Queue {
    async fn wake(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        node: NodeId,
    ) -> Result<u64, sqlx::Error> {
        if let Self::Fails = self {
            return Err(sqlx::Error::Protocol("injected wakeup failure".into()));
        }
        let queued = enqueue_triggers_tx(tx, [node], ReconcileReason::FirewallRulesChanged).await?;
        match *self {
            Self::Reports(count) => Ok(count),
            Self::ThenLapses => {
                sqlx::query(sql!(
                    "UPDATE firewall_rules_rollout_items
                     SET lease_expires_at = clock_timestamp() - interval '1 second'
                     WHERE node_id = $1 AND phase = 'in_flight'"
                ))
                .bind(node)
                .execute(&mut **tx)
                .await?;
                Ok(queued)
            }
            _ => Ok(queued),
        }
    }
}

async fn issue_with(
    pool: &PgPool,
    target: &LeasedTarget,
    mut queue: Queue,
) -> Result<Issuance, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    issue_first_assignment(&mut conn, target, FIRST_CHECK_IN, &mut queue).await
}

/// Issue with the real wakeup, which must succeed.
async fn issued(pool: &PgPool, target: &LeasedTarget) -> TestResult<Stamp> {
    match issue_with(pool, target, Queue::Wake).await? {
        Issuance::Issued(stamp) => Ok(stamp),
        other => panic!("{target:?} wasn't issued: {other:?}"),
    }
}

/// Everything issuance may write for `node`: its assignment (generation,
/// revision), its item's stamp and schedule, and its trigger row (reasons,
/// generation, attempts, requested and next attempt times).
#[derive(Debug, PartialEq)]
struct Effects {
    assignment: Option<(i64, Uuid)>,
    stamp: Option<(Option<i64>, Option<OffsetDateTime>, Option<OffsetDateTime>)>,
    trigger: Option<(Vec<String>, i64, i32, OffsetDateTime, OffsetDateTime)>,
}

impl Effects {
    const NONE: Self = Self {
        assignment: None,
        stamp: Some((None, None, None)),
        trigger: None,
    };
}

async fn effects(pool: &PgPool, rollout: RolloutId, node: NodeId) -> TestResult<Effects> {
    Ok(Effects {
        assignment: sqlx::query_as(sql!(
            "SELECT assignment_generation, revision_id
             FROM firewall_rules_node_assignments WHERE node_id = $1"
        ))
        .bind(node)
        .fetch_optional(pool)
        .await?,
        stamp: sqlx::query_as(sql!(
            "SELECT issued_assignment_generation, issued_at, next_check_at
             FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
        ))
        .bind(rollout)
        .bind(node)
        .fetch_optional(pool)
        .await?,
        trigger: sqlx::query_as(sql!(
            "SELECT reasons::text[], generation, attempts, requested_at, next_attempt_at
             FROM node_reconcile_triggers WHERE node_id = $1"
        ))
        .bind(node)
        .fetch_optional(pool)
        .await?,
    })
}

async fn revision_of(pool: &PgPool, rollout: RolloutId) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT to_revision_id FROM firewall_rules_rollouts WHERE id = $1"
    ))
    .bind(rollout)
    .fetch_one(pool)
    .await?)
}

/// Give `node` a current assignment at `generation` on a revision of its own.
async fn assign(pool: &PgPool, node: NodeId, generation: i64) -> TestResult {
    let revision = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, '[]'::jsonb)"
    ))
    .bind(revision)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_assignments (node_id, assignment_generation, revision_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (node_id) DO UPDATE SET assignment_generation = $2"
    ))
    .bind(node)
    .bind(generation)
    .bind(revision)
    .execute(pool)
    .await?;
    Ok(())
}

/// Lock `node`'s trigger row in an open transaction, so a wakeup waits on it.
async fn hold_trigger(
    pool: &PgPool,
    node: NodeId,
) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM node_reconcile_triggers WHERE node_id = $1 FOR UPDATE"
    ))
    .bind(node)
    .fetch_one(&mut *tx)
    .await?;
    Ok(tx)
}

fn spawn_issue(
    pool: &PgPool,
    target: &LeasedTarget,
) -> tokio::task::JoinHandle<Result<Issuance, sqlx::Error>> {
    let (pool, target) = (pool.clone(), target.clone());
    tokio::spawn(async move { issue_with(&pool, &target, Queue::Wake).await })
}

async fn clock(pool: &PgPool) -> TestResult<OffsetDateTime> {
    Ok(sqlx::query_scalar(sql!("SELECT clock_timestamp()"))
        .fetch_one(pool)
        .await?)
}

/// Catches a claim that writes delivery state; a generation shared between
/// nodes or taken from anything but the node's assignment; an assignment
/// left on its old revision; a stamp that disagrees with it, isn't the
/// database's time or whose first check isn't one delay later; a missing
/// wakeup; and a release that moves the first check.
#[tokio::test(flavor = "current_thread")]
async fn a_first_issue_advances_the_nodes_own_generation_and_wakes_it_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    save_targets(&db.pool, rollout, &nodes).await?;
    assign(&db.pool, nodes[1], 41).await?;
    let revision = revision_of(&db.pool, rollout).await?;
    let leased = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&leased), nodes);
    assert_eq!(effects(&db.pool, rollout, nodes[0]).await?, Effects::NONE);
    assert_eq!(
        effects(&db.pool, rollout, nodes[1]).await?.stamp,
        Effects::NONE.stamp
    );

    for (target, generation) in leased.iter().zip([1_i64, 42]) {
        let before = clock(&db.pool).await?;
        let stamp = issued(&db.pool, target).await?;
        let after = clock(&db.pool).await?;
        assert!((before..=after).contains(&stamp.issued_at), "{stamp:?}");
        assert_eq!(i64::try_from(stamp.assignment_generation)?, generation);
        assert_eq!(stamp.next_check_at, Some(stamp.issued_at + FIRST_CHECK_IN));
        let issued = effects(&db.pool, rollout, target.node_id).await?;
        assert_eq!(issued.assignment, Some((generation, revision)));
        assert_eq!(
            issued.stamp,
            Some((Some(generation), Some(stamp.issued_at), stamp.next_check_at))
        );
        let (reasons, trigger_generation, ..) = issued.trigger.clone().expect("no wakeup");
        assert_eq!(
            (reasons, trigger_generation),
            (vec![FIREWALL.to_owned()], 0)
        );

        // The driver ends the lease: the attempt comes back, nothing else moves.
        assert!(requeue(&db.pool, target, &RELEASE).await?);
        assert_eq!(effects(&db.pool, rollout, target.node_id).await?, issued);
        assert_eq!(item(&db.pool, rollout, target.node_id).await?.0, 0);
    }
    Ok(())
}

/// Catches a replay, a lost commit response or a crash before the release
/// that issues again, with a second generation, wakeup or first check; a
/// check lease, or a check carrying a stale first-assignment hint, that
/// issues; and a node a later rollout can't move.
#[tokio::test(flavor = "current_thread")]
async fn an_issued_target_is_resolved_from_its_stamp() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let first = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, first, &nodes).await?;
    let target = lease(&db.pool, first, 1).await?.remove(0);
    let stamp = issued(&db.pool, &target).await?;
    let issued_effects = effects(&db.pool, first, node).await?;

    // Called again under the same lease, as after a lost commit response.
    assert_eq!(
        issue_with(&db.pool, &target, Queue::Wake).await?,
        Issuance::AlreadyIssued(stamp)
    );
    // The controller crashed before the release; the lease is redriven.
    lapse(&db.pool, first, node).await?;
    redrive_expired(&db.pool, first, MAX_ATTEMPTS).await?;
    assert_eq!(
        issue_with(&db.pool, &target, Queue::Wake).await?,
        Issuance::AlreadyIssued(stamp)
    );
    assert_eq!(effects(&db.pool, first, node).await?, issued_effects);

    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items SET next_check_at = now() - interval '1 second'
         WHERE rollout_id = $1"
    ))
    .bind(first)
    .execute(&db.pool)
    .await?;
    let due = effects(&db.pool, first, node).await?;
    let check = lease(&db.pool, first, 1).await?.remove(0);
    assert_eq!(
        check.work,
        Work::Check {
            assignment_generation: 1,
            issued_at: stamp.issued_at
        }
    );
    assert_eq!(
        issue_with(&db.pool, &check, Queue::Wake).await?,
        Issuance::Refused(Refusal::CheckLease)
    );
    let hinted = LeasedTarget {
        work: Work::FirstAssignment,
        ..check.clone()
    };
    assert!(matches!(
        issue_with(&db.pool, &hinted, Queue::Wake).await?,
        Issuance::AlreadyIssued(Stamp {
            assignment_generation: 1,
            ..
        })
    ));
    assert_eq!(effects(&db.pool, first, node).await?, due);

    assert!(requeue(&db.pool, &check, &RELEASE).await?);
    set_phase(&db.pool, first, "completed").await?;
    let later = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, later, &nodes).await?;
    let target = lease(&db.pool, later, 1).await?.remove(0);
    assert_eq!(issued(&db.pool, &target).await?.assignment_generation, 2);
    let moved = effects(&db.pool, later, node).await?;
    assert_eq!(
        moved.assignment,
        Some((2, revision_of(&db.pool, later).await?))
    );
    assert_eq!(moved.trigger.map(|trigger| trigger.1), Some(1));
    Ok(())
}

/// Catches a node or lease judged only before issuance waits: a node that
/// goes offline, or a lease that lapses, while the wakeup waits on the
/// node's trigger row would still be stamped, with the assignment and wakeup
/// committed; and an old token that issues or releases once another
/// controller took the item over.
#[tokio::test(flavor = "current_thread")]
async fn a_node_or_lease_that_lapses_while_issuance_waits_issues_nothing() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    assign(&db.pool, node, 5).await?;
    enqueue_triggers(&db.pool, [node], ReconcileReason::NodeUpdated).await?;
    let old = lease(&db.pool, rollout, 1).await?.remove(0);
    let before = effects(&db.pool, rollout, node).await?;

    let locker = hold_trigger(&db.pool, node).await?;
    let issuing = spawn_issue(&db.pool, &old);
    wait_for_lock_waiters(&db.pool, 1).await?;
    set_status(&db.pool, node, false, 60.0).await?;
    locker.commit().await?;
    assert_eq!(issuing.await??, Issuance::Refused(Refusal::Lapsed));
    assert_eq!(effects(&db.pool, rollout, node).await?, before);
    set_status(&db.pool, node, true, 60.0).await?;

    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET lease_expires_at = clock_timestamp() + interval '3 seconds'
         WHERE rollout_id = $1"
    ))
    .bind(rollout)
    .execute(&db.pool)
    .await?;
    let locker = hold_trigger(&db.pool, node).await?;
    let issuing = spawn_issue(&db.pool, &old);
    wait_for_lock_waiters(&db.pool, 1).await?;
    let live = || {
        sqlx::query_scalar::<_, bool>(sql!(
            "SELECT lease_expires_at > clock_timestamp()
             FROM firewall_rules_rollout_items WHERE rollout_id = $1"
        ))
        .bind(rollout)
        .fetch_one(&db.pool)
    };
    // Live while issuance waits, so it passed every check before the wait.
    assert!(live().await?, "the lease lapsed before issuance waited");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while live().await? {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lease didn't lapse"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    locker.commit().await?;
    assert_eq!(issuing.await??, Issuance::Refused(Refusal::Lapsed));
    assert_eq!(effects(&db.pool, rollout, node).await?, before);

    redrive_expired(&db.pool, rollout, MAX_ATTEMPTS).await?;
    let new = lease(&db.pool, rollout, 1).await?.remove(0);
    assert_eq!(
        issue_with(&db.pool, &old, Queue::Wake).await?,
        Issuance::Refused(Refusal::LeaseLost)
    );
    assert!(!requeue(&db.pool, &old, &RELEASE).await?);
    assert_eq!(effects(&db.pool, rollout, node).await?, before);
    assert_eq!(issued(&db.pool, &new).await?.assignment_generation, 6);
    Ok(())
}

/// Catches issuance that doesn't lock the header first or reads it before
/// the lock: a pause that took the header first would still issue; one that
/// waited behind issuance could undo it; and a finished rollout's live lease
/// would still issue.
#[tokio::test(flavor = "current_thread")]
async fn the_first_to_lock_the_header_wins_pause_or_abort_against_issuance() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    let (a, b) = (nodes[0], nodes[1]);
    save_targets(&db.pool, rollout, &nodes).await?;
    let leased = lease(&db.pool, rollout, 10).await?;

    let mut pausing = hold_header(&db.pool, rollout).await?;
    let issuing = spawn_issue(&db.pool, &leased[0]);
    wait_for_lock_waiters(&db.pool, 1).await?;
    set_phase(&mut *pausing, rollout, "paused").await?;
    pausing.commit().await?;
    assert_eq!(issuing.await??, Issuance::Refused(Refusal::NotIssuing));
    assert_eq!(effects(&db.pool, rollout, a).await?, Effects::NONE);

    set_phase(&db.pool, rollout, "rolling").await?;
    enqueue_triggers(&db.pool, [a], ReconcileReason::NodeUpdated).await?;
    let locker = hold_trigger(&db.pool, a).await?;
    let issuing = spawn_issue(&db.pool, &leased[0]);
    wait_for_lock_waiters(&db.pool, 1).await?;
    let pool = db.pool.clone();
    let aborting = tokio::spawn(async move { set_phase(&pool, rollout, "aborted").await });
    wait_for_lock_waiters(&db.pool, 2).await?;
    locker.commit().await?;
    let Issuance::Issued(stamp) = issuing.await?? else {
        panic!("the issuance that locked the header first didn't issue");
    };
    aborting.await??;
    let issued = effects(&db.pool, rollout, a).await?;
    assert_eq!(issued.assignment.map(|assignment| assignment.0), Some(1));
    assert_eq!(
        issued.stamp.and_then(|stamp| stamp.1),
        Some(stamp.issued_at)
    );

    assert_eq!(
        issue_with(&db.pool, &leased[1], Queue::Wake).await?,
        Issuance::Refused(Refusal::NotIssuing)
    );
    assert_eq!(effects(&db.pool, rollout, b).await?, Effects::NONE);
    Ok(())
}

/// Catches a canary pinned by its first lease rather than its first
/// issuance; a pin that looks only at online or due items, which would issue
/// a second canary once the first goes offline; and canary decisions not
/// serialized on the header, which would let two leases both issue.
#[tokio::test(flavor = "current_thread")]
async fn in_canary_the_first_committed_issuance_pins_the_canary() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let first = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 4).await?;
    let (a, b) = (nodes[0], nodes[1]);
    save_targets(&db.pool, first, &nodes[..2]).await?;

    let released = lease(&db.pool, first, 10).await?.remove(0);
    assert_eq!(released.node_id, a);
    assert!(requeue(&db.pool, &released, &RELEASE).await?);
    set_status(&db.pool, a, false, 60.0).await?;
    let canary = lease(&db.pool, first, 10).await?.remove(0);
    assert_eq!(canary.node_id, b);
    issued(&db.pool, &canary).await?;
    assert!(requeue(&db.pool, &canary, &RELEASE).await?);

    // B is offline and not due; A is back, under a lease the pin should have
    // kept it from.
    set_status(&db.pool, b, false, 60.0).await?;
    set_status(&db.pool, a, true, 60.0).await?;
    let second = held(&db.pool, first, a).await?;
    assert_eq!(
        issue_with(&db.pool, &second, Queue::Wake).await?,
        Issuance::Refused(Refusal::CanaryChosen)
    );
    assert_eq!(effects(&db.pool, first, a).await?, Effects::NONE);

    // Two unissued canaries under lease: the one that reaches the header
    // first, then waits on its trigger row, is the only one issued.
    set_phase(&db.pool, first, "aborted").await?;
    let racing = rollout(&db.pool, "canary").await?;
    let (c, d) = (nodes[2], nodes[3]);
    save_targets(&db.pool, racing, &[c, d]).await?;
    let (c_target, d_target) = (
        held(&db.pool, racing, c).await?,
        held(&db.pool, racing, d).await?,
    );
    enqueue_triggers(&db.pool, [c], ReconcileReason::NodeUpdated).await?;
    let locker = hold_trigger(&db.pool, c).await?;
    let issuing_c = spawn_issue(&db.pool, &c_target);
    wait_for_lock_waiters(&db.pool, 1).await?;
    let issuing_d = spawn_issue(&db.pool, &d_target);
    wait_for_lock_waiters(&db.pool, 2).await?;
    locker.commit().await?;
    assert!(matches!(issuing_c.await??, Issuance::Issued(_)));
    assert_eq!(issuing_d.await??, Issuance::Refused(Refusal::CanaryChosen));
    assert_eq!(effects(&db.pool, racing, d).await?, Effects::NONE);
    Ok(())
}

/// Put `node`'s item in flight under a new token, as an older controller's
/// claim might hold it, and return that lease.
async fn held(pool: &PgPool, rollout: RolloutId, node: NodeId) -> TestResult<LeasedTarget> {
    let lease_token = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET phase = 'in_flight', lease_token = $3,
             lease_expires_at = now() + interval '1 hour'
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .bind(lease_token)
    .execute(pool)
    .await?;
    Ok(LeasedTarget {
        rollout_id: rollout,
        node_id: node,
        lease_token,
        attempts: 0,
        work: Work::FirstAssignment,
    })
}

/// Catches eligibility taken from the lease or the trigger queue instead of
/// checked at issuance: an unreachable, stale or deregistered node, or one
/// the rollout didn't save, would be assigned and woken; a deregistered node
/// recreated; or the saved targets changed.
#[tokio::test(flavor = "current_thread")]
async fn ineligible_targets_issue_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 4).await?;
    let unsaved = nodes[3];
    save_targets(&db.pool, rollout, &nodes[..3]).await?;
    let leased = lease(&db.pool, rollout, 10).await?;
    let (unreachable, stale, deregistered) = (nodes[0], nodes[1], nodes[2]);
    set_status(&db.pool, unreachable, false, 60.0).await?;
    set_status(&db.pool, stale, true, -1.0).await?;
    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(deregistered)
        .execute(&db.pool)
        .await?;

    let forged = [
        (
            LeasedTarget {
                node_id: unsaved,
                ..leased[0].clone()
            },
            Refusal::NotATarget,
        ),
        (
            LeasedTarget {
                rollout_id: RolloutId(Uuid::now_v7()),
                ..leased[0].clone()
            },
            Refusal::NotATarget,
        ),
        (
            LeasedTarget {
                lease_token: leased[0].lease_token,
                ..leased[1].clone()
            },
            Refusal::LeaseLost,
        ),
    ];
    let offline = leased
        .iter()
        .map(|target| (target.clone(), Refusal::NodeUnavailable));
    for (target, refusal) in offline.chain(forged) {
        assert_eq!(
            issue_with(&db.pool, &target, Queue::Wake).await?,
            Issuance::Refused(refusal),
            "{target:?}"
        );
    }

    for node in &nodes[..3] {
        assert_eq!(effects(&db.pool, rollout, *node).await?, Effects::NONE);
    }
    assert_eq!(
        effects(&db.pool, rollout, unsaved).await?,
        Effects {
            stamp: None,
            ..Effects::NONE
        }
    );
    let (registered, saved) = sqlx::query_as::<_, (bool, i64)>(sql!(
        "SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $2),
                (SELECT count(*) FILTER (WHERE phase = 'in_flight')
                 FROM firewall_rules_rollout_items WHERE rollout_id = $1)"
    ))
    .bind(rollout)
    .bind(deregistered)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!((registered, saved), (false, 3));
    Ok(())
}

/// Catches a failure that commits part of an issuance: a wakeup that fails,
/// is rejected, queues no node or several, or is followed by a refused stamp,
/// or a generation that overflows, leaving an assignment, a trigger change or
/// a stamp behind; a wakeup that drops the node's other reasons; and a
/// replay that refreshes them.
#[tokio::test(flavor = "current_thread")]
async fn any_failure_rolls_back_the_whole_issuance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    save_targets(&db.pool, rollout, &nodes).await?;
    let leased = lease(&db.pool, rollout, 10).await?;
    let (target, rejected) = (&leased[0], &leased[1]);
    let node = target.node_id;
    assign(&db.pool, node, 7).await?;
    enqueue_triggers(&db.pool, [node], ReconcileReason::NodeUpdated).await?;
    let before = effects(&db.pool, rollout, node).await?;

    for queue in [
        Queue::Fails,
        Queue::Reports(0),
        Queue::Reports(2),
        Queue::ThenLapses,
    ] {
        let outcome = issue_with(&db.pool, target, queue).await;
        let lapsed = matches!(queue, Queue::ThenLapses);
        assert!(
            matches!(
                (lapsed, &outcome),
                (true, Ok(Issuance::Refused(Refusal::Lapsed))) | (false, Err(_))
            ),
            "{queue:?}: {outcome:?}"
        );
        assert_eq!(effects(&db.pool, rollout, node).await?, before, "{queue:?}");
    }

    // The trigger queue wakes only workers and builders.
    sqlx::query(sql!("UPDATE nodes SET role = 'cluster' WHERE id = $1"))
        .bind(rejected.node_id)
        .execute(&db.pool)
        .await?;
    let outcome = issue_with(&db.pool, rejected, Queue::Wake).await;
    let Err(_) = outcome else {
        panic!("a rejected wakeup didn't fail: {outcome:?}");
    };
    assert_eq!(
        effects(&db.pool, rollout, rejected.node_id).await?,
        Effects::NONE
    );

    assign(&db.pool, node, i64::MAX).await?;
    let at_max = effects(&db.pool, rollout, node).await?;
    let outcome = issue_with(&db.pool, target, Queue::Wake).await;
    let Err(_) = outcome else {
        panic!("an overflowing generation didn't fail: {outcome:?}");
    };
    assert_eq!(effects(&db.pool, rollout, node).await?, at_max);

    assign(&db.pool, node, 7).await?;
    let stamp = issued(&db.pool, target).await?;
    assert_eq!(stamp.assignment_generation, 8);
    let issued = effects(&db.pool, rollout, node).await?;
    assert_eq!(
        issued.trigger.as_ref().map(|trigger| trigger.0.clone()),
        Some(vec![FIREWALL.to_owned(), "node_updated".to_owned()])
    );
    assert_eq!(
        issue_with(&db.pool, target, Queue::Wake).await?,
        Issuance::AlreadyIssued(stamp)
    );
    assert_eq!(effects(&db.pool, rollout, node).await?, issued);
    Ok(())
}
