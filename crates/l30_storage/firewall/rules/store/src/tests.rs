use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{
    SEED_USER_ID, TestDb, insert_ready_worker_node, try_pg_test_db,
    try_pg_test_db_with_max_connections,
};
use pplx_space_rollout_queue::LEASE_EXPIRED;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;

const LEASE: Duration = Duration::from_secs(600);
const SCAN: u32 = 100;
const MAX_ATTEMPTS: u64 = 5;

/// A rollout header in `phase`, with the revision it references, created by
/// the seeded user. The schema admits one unfinished header per database.
async fn rollout(pool: &PgPool, phase: &str) -> TestResult<RolloutId> {
    let revision = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, '[]'::jsonb)"
    ))
    .bind(revision)
    .execute(pool)
    .await?;
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_rollouts (
             id, to_revision_id, created_by_user_id, phase, unfinished_slot, reason,
             finished_at
         )
         VALUES ($1, $2, $3, $4,
                 CASE WHEN $4 IN ('completed', 'aborted') THEN NULL ELSE 1 END,
                 'test rollout',
                 CASE WHEN $4 IN ('completed', 'aborted') THEN now() END)"
    ))
    .bind(id)
    .bind(revision)
    .bind(SEED_USER_ID)
    .bind(phase)
    .execute(pool)
    .await?;
    Ok(RolloutId(id))
}

/// Move `rollout` to `phase`, keeping the slot and finish time consistent.
async fn set_phase(pool: &PgPool, rollout: RolloutId, phase: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollouts
         SET phase = $2,
             unfinished_slot = CASE WHEN $2 IN ('completed', 'aborted') THEN NULL ELSE 1 END,
             finished_at = CASE WHEN $2 IN ('completed', 'aborted') THEN now() END
         WHERE id = $1"
    ))
    .bind(rollout)
    .bind(phase)
    .execute(pool)
    .await?;
    Ok(())
}

/// `count` registered nodes with a reachable, fresh status snapshot, in the
/// order a claim scans them.
async fn online_nodes(pool: &PgPool, count: usize) -> TestResult<Vec<NodeId>> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        let node = NodeId::new();
        insert_ready_worker_node(pool, node).await?;
        sqlx::query(sql!(
            "INSERT INTO node_status_snapshots (
                 node_id, observed_at, stale_after, reachable, services, updated_at
             )
             VALUES ($1, now(), now() + interval '1 minute', true, '[]'::jsonb, now())"
        ))
        .bind(node)
        .execute(pool)
        .await?;
        nodes.push(node);
    }
    nodes.sort_by_key(|node| node.as_uuid());
    Ok(nodes)
}

/// Rewrite `node`'s snapshot: `reachable`, going stale `stale_in_secs` from
/// now (negative is already stale).
async fn set_status(
    pool: &PgPool,
    node: NodeId,
    reachable: bool,
    stale_in_secs: f64,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE node_status_snapshots
         SET reachable = $2, stale_after = now() + make_interval(secs => $3)
         WHERE node_id = $1"
    ))
    .bind(node)
    .bind(reachable)
    .bind(stale_in_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn save_targets(pool: &PgPool, rollout: RolloutId, nodes: &[NodeId]) -> TestResult {
    for node in nodes {
        sqlx::query(sql!(
            "INSERT INTO firewall_rules_rollout_items (rollout_id, node_id, phase)
             VALUES ($1, $2, 'pending')"
        ))
        .bind(rollout)
        .bind(*node)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Stand in for the later assignment transaction: stamp `node`'s item as
/// issued at `generation`, with its check `check_in_secs` from now (`None`
/// for unscheduled). Leaves its phase and lease alone.
async fn issue(
    pool: &PgPool,
    rollout: RolloutId,
    node: NodeId,
    generation: i64,
    check_in_secs: Option<f64>,
) -> TestResult<OffsetDateTime> {
    Ok(sqlx::query_scalar::<_, OffsetDateTime>(sql!(
        "UPDATE firewall_rules_rollout_items
         SET issued_assignment_generation = $3,
             issued_at = date_trunc('second', now()) - interval '1 hour',
             next_check_at = now() + make_interval(secs => $4)
         WHERE rollout_id = $1 AND node_id = $2
         RETURNING issued_at"
    ))
    .bind(rollout)
    .bind(node)
    .bind(generation)
    .bind(check_in_secs)
    .fetch_one(pool)
    .await?)
}

async fn lease(
    pool: &PgPool,
    rollout: RolloutId,
    active_limit: u64,
) -> TestResult<Vec<LeasedTarget>> {
    let mut conn = pool.acquire().await?;
    Ok(lease_targets(&mut conn, rollout, active_limit, LEASE, SCAN).await?)
}

fn nodes_of(leased: &[LeasedTarget]) -> Vec<NodeId> {
    leased.iter().map(|target| target.node_id).collect()
}

/// `node`'s attempts and last error.
async fn item(
    pool: &PgPool,
    rollout: RolloutId,
    node: NodeId,
) -> TestResult<(i64, Option<String>)> {
    Ok(sqlx::query_as::<_, (i64, Option<String>)>(sql!(
        "SELECT attempts, last_error
         FROM firewall_rules_rollout_items
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(pool)
    .await?)
}

/// Lapse `node`'s lease on the database clock, as if its controller died.
async fn lapse(pool: &PgPool, rollout: RolloutId, node: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET lease_expires_at = now() - interval '1 second'
         WHERE rollout_id = $1 AND node_id = $2 AND phase = 'in_flight'"
    ))
    .bind(rollout)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Wait, for at most ten seconds, until `count` sessions on this test
/// database wait for a lock: each racing statement is then known to be
/// blocked rather than not started yet.
async fn wait_for_lock_waiters(pool: &PgPool, count: i64) -> TestResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT count(*)
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock'"
        ))
        .fetch_one(pool)
        .await?;
        if waiting >= count {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{waiting} of {count} sessions started waiting for a lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Lock `rollout`'s header in an open transaction, so claims queue behind it.
async fn hold_header(
    pool: &PgPool,
    rollout: RolloutId,
) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_rollouts WHERE id = $1 FOR UPDATE"
    ))
    .bind(rollout)
    .execute(&mut *tx)
    .await?;
    Ok(tx)
}

/// Two claims of `rollout` that both reach the header lock before either
/// takes it, all they lease together.
async fn race(db: &TestDb, rollout: RolloutId, active_limit: u64) -> TestResult<Vec<LeasedTarget>> {
    let holder = hold_header(&db.pool, rollout).await?;
    let claims: Vec<_> = (0..2)
        .map(|_| {
            let pool = db.pool.clone();
            tokio::spawn(async move {
                let mut conn = pool.acquire().await?;
                lease_targets(&mut conn, rollout, active_limit, LEASE, SCAN).await
            })
        })
        .collect();
    wait_for_lock_waiters(&db.pool, 2).await?;
    holder.commit().await?;
    let mut leased = Vec::new();
    for claim in claims {
        leased.extend(claim.await??);
    }
    Ok(leased)
}

/// Catches claims that don't serialize on the header or don't count work
/// already in flight, letting two controllers act on one node or past the
/// active limit; and a failure budget that stops on an unrelated failure.
#[tokio::test(flavor = "current_thread")]
async fn racing_controllers_share_one_node_and_one_slot() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 3).await?;
    let (a, b, failed) = (nodes[0], nodes[1], nodes[2]);
    save_targets(&db.pool, rollout, &[a, failed]).await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items SET phase = 'failed'
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(failed)
    .execute(&db.pool)
    .await?;

    let leased = race(&db, rollout, 10).await?;
    assert_eq!(nodes_of(&leased), [a], "both controllers leased the node");

    assert!(requeue(&db.pool, &leased[0], Requeue::Release).await?);
    save_targets(&db.pool, rollout, &[b]).await?;
    let leased = race(&db, rollout, 1).await?;
    assert_eq!(
        leased.len(),
        1,
        "two leases committed into one slot: {leased:?}"
    );
    Ok(())
}

/// Catches a canary pinned by its first lease rather than its first
/// assignment, which would stall the rollout on a node that died unassigned.
#[tokio::test(flavor = "current_thread")]
async fn before_an_assignment_commits_the_canary_may_move() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    let (a, b) = (nodes[0], nodes[1]);
    save_targets(&db.pool, rollout, &nodes).await?;

    let first = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&first), [a]);
    // Redriven, so A keeps the attempt of a lease that issued nothing.
    lapse(&db.pool, rollout, a).await?;
    redrive_expired(&db.pool, rollout, MAX_ATTEMPTS).await?;
    set_status(&db.pool, a, false, 60.0).await?;

    let second = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&second), [b]);
    Ok(())
}

/// Catches a canary that falls through to another node once the issued one
/// can't be checked, which would put a second canary on the new rules.
#[tokio::test(flavor = "current_thread")]
async fn after_an_assignment_commits_only_the_issued_canary_is_leased() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    let (a, b) = (nodes[0], nodes[1]);
    save_targets(&db.pool, rollout, &nodes).await?;
    // B becomes the canary while A is offline; its assignment commits and
    // its lease ends.
    set_status(&db.pool, a, false, 60.0).await?;
    let canary = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&canary), [b]);
    issue(&db.pool, rollout, b, 7, Some(-1.0)).await?;
    assert!(requeue(&db.pool, &canary[0], Requeue::Release).await?);
    set_status(&db.pool, a, true, 60.0).await?;

    let leased = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&leased), [b]);
    assert!(requeue(&db.pool, &leased[0], Requeue::Release).await?);

    // Each case fails the scan's other filters for B; the pin is decided
    // before them, so it still holds.
    let cases: [(&str, &str); 4] = [
        (
            "offline",
            sql!(
                "UPDATE node_status_snapshots nss
                 SET reachable = false, stale_after = now() - interval '1 second'
                 FROM firewall_rules_rollout_items i
                 WHERE i.rollout_id = $1 AND i.node_id = $2 AND nss.node_id = i.node_id"
            ),
        ),
        (
            "failed",
            sql!(
                "UPDATE firewall_rules_rollout_items SET phase = 'failed'
                 WHERE rollout_id = $1 AND node_id = $2"
            ),
        ),
        (
            "unscheduled",
            sql!(
                "UPDATE firewall_rules_rollout_items SET next_check_at = NULL
                 WHERE rollout_id = $1 AND node_id = $2"
            ),
        ),
        (
            "not due",
            sql!(
                "UPDATE firewall_rules_rollout_items
                 SET next_check_at = now() + interval '1 hour'
                 WHERE rollout_id = $1 AND node_id = $2"
            ),
        ),
    ];
    for (case, statement) in cases {
        sqlx::query(statement)
            .bind(rollout)
            .bind(b)
            .execute(&db.pool)
            .await?;
        let leased = lease(&db.pool, rollout, 10).await?;
        assert!(leased.is_empty(), "B {case}: leased {leased:?}");
        // Back to online, pending and due for the next case.
        set_status(&db.pool, b, true, 60.0).await?;
        sqlx::query(sql!(
            "UPDATE firewall_rules_rollout_items
             SET phase = 'pending', next_check_at = now() - interval '1 second'
             WHERE rollout_id = $1 AND node_id = $2"
        ))
        .bind(rollout)
        .bind(b)
        .execute(&db.pool)
        .await?;
    }
    Ok(())
}

/// Catches a check treated as due while unscheduled or early, which would
/// poll a node in a loop or before its rules could have landed, and a check
/// lease that loses the stamp of what was issued.
#[tokio::test(flavor = "current_thread")]
async fn issued_targets_wait_for_their_scheduled_check() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 4).await?;
    let (unissued, unscheduled, future, due) = (nodes[0], nodes[1], nodes[2], nodes[3]);
    save_targets(&db.pool, rollout, &nodes).await?;
    issue(&db.pool, rollout, unscheduled, 1, None).await?;
    issue(&db.pool, rollout, future, 2, Some(3600.0)).await?;
    let issued_at = issue(&db.pool, rollout, due, 3, Some(-1.0)).await?;

    let leased = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&leased), [unissued, due]);
    assert_eq!(leased[0].work, Work::FirstAssignment);
    assert_eq!(
        leased[1].work,
        Work::Check {
            assignment_generation: 3,
            issued_at
        }
    );
    Ok(())
}

/// Catches an issued node that keeps its lease slot while it waits for
/// proof, which would serialize a rolling rollout behind every check.
#[tokio::test(flavor = "current_thread")]
async fn in_rolling_a_node_awaiting_its_check_frees_its_slot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    let (a, b) = (nodes[0], nodes[1]);
    save_targets(&db.pool, rollout, &nodes).await?;

    let first = lease(&db.pool, rollout, 1).await?;
    assert_eq!(nodes_of(&first), [a]);
    assert!(lease(&db.pool, rollout, 1).await?.is_empty());
    issue(&db.pool, rollout, a, 1, Some(3600.0)).await?;
    assert!(requeue(&db.pool, &first[0], Requeue::Release).await?);

    assert_eq!(nodes_of(&lease(&db.pool, rollout, 1).await?), [b]);
    Ok(())
}

/// Catches an offline node or a header that doesn't lease being leased.
#[tokio::test(flavor = "current_thread")]
async fn only_online_targets_of_a_leasing_rollout_are_leased() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "paused").await?;
    let nodes = online_nodes(&db.pool, 4).await?;
    let (unreachable, stale, deleted, eligible) = (nodes[0], nodes[1], nodes[2], nodes[3]);
    save_targets(&db.pool, rollout, &nodes).await?;
    set_status(&db.pool, unreachable, false, 60.0).await?;
    set_status(&db.pool, stale, true, -1.0).await?;
    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(deleted)
        .execute(&db.pool)
        .await?;

    for phase in ["paused", "completed", "aborted"] {
        set_phase(&db.pool, rollout, phase).await?;
        let leased = lease(&db.pool, rollout, 10).await?;
        assert!(leased.is_empty(), "{phase}: leased {leased:?}");
    }
    set_phase(&db.pool, rollout, "rolling").await?;
    assert_eq!(nodes_of(&lease(&db.pool, rollout, 10).await?), [eligible]);
    Ok(())
}

/// Catches a lease or renewal written for the wrong length, a renewal that
/// reads the clock before it waits for the item lock, reviving a lease
/// another controller may already have been given, and a replaced token
/// that renews.
#[tokio::test(flavor = "current_thread")]
async fn only_a_live_token_renews_even_after_a_lock_wait() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    let mut conn = db.pool.acquire().await?;
    let live_for = || async {
        sqlx::query_scalar::<_, f64>(sql!(
            "SELECT extract(epoch FROM lease_expires_at - clock_timestamp())::float8
             FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
        ))
        .bind(rollout)
        .bind(node)
        .fetch_one(&db.pool)
        .await
    };

    let replaced = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &replaced, Requeue::Release).await?);
    let live = lease(&db.pool, rollout, 1).await?.remove(0);
    let leased_for = live_for().await?;
    assert!(
        leased_for <= LEASE.as_secs_f64(),
        "leased for {leased_for}s"
    );
    assert!(renew_lease(&mut conn, &live, LEASE * 2).await?);
    let renewed_for = live_for().await?;
    assert!(
        renewed_for > LEASE.as_secs_f64() && renewed_for <= (LEASE * 2).as_secs_f64(),
        "renewed for {renewed_for}s"
    );
    assert!(!renew_lease(&mut conn, &replaced, LEASE).await?);

    // Expires 300 ms from now, while the renewal waits on the item lock.
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET lease_expires_at = clock_timestamp() + interval '300 milliseconds'
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .execute(&db.pool)
    .await?;
    let mut locker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_rollout_items
         WHERE rollout_id = $1 AND node_id = $2
         FOR UPDATE"
    ))
    .bind(rollout)
    .bind(node)
    .execute(&mut *locker)
    .await?;
    let pool = db.pool.clone();
    let waiting = live.clone();
    let renewing = tokio::spawn(async move {
        let mut conn = pool.acquire().await?;
        renew_lease(&mut conn, &waiting, LEASE).await
    });
    wait_for_lock_waiters(&db.pool, 1).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while sqlx::query_scalar::<_, bool>(sql!(
        "SELECT lease_expires_at > clock_timestamp()
         FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(&db.pool)
    .await?
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lease didn't lapse"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    locker.commit().await?;
    assert!(
        !renewing.await??,
        "the renewal revived a lease that lapsed while it waited"
    );
    Ok(())
}

/// Catches a release that keeps its attempt or a retry that gives it back,
/// which would misjudge a node's attempt limit; an old token that changes a
/// reclaimed item; a redrive of a live lease; and bookkeeping that writes
/// delivery state: an assignment, issuance, schedule, observation or wakeup
/// would tell the node to act, or say it had, without the assignment
/// transaction.
#[tokio::test(flavor = "current_thread")]
async fn requeues_and_redrives_are_fenced_and_write_no_delivery_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_assignments (node_id, assignment_generation, revision_id)
         SELECT $1, 1, to_revision_id FROM firewall_rules_rollouts WHERE id = $2"
    ))
    .bind(node)
    .bind(rollout)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch) VALUES ($1, 1)"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    issue(&db.pool, rollout, node, 1, Some(-1.0)).await?;
    let delivery = || async {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT jsonb_build_array(
                 (SELECT jsonb_agg(to_jsonb(a)) FROM firewall_rules_node_assignments a),
                 (SELECT jsonb_agg(to_jsonb(o)) FROM firewall_rules_node_observations o),
                 (SELECT jsonb_agg(to_jsonb(t)) FROM node_reconcile_triggers t),
                 (SELECT jsonb_agg(to_jsonb(q)) FROM node_reconcile_queue q),
                 (SELECT jsonb_agg(jsonb_build_array(
                      i.issued_assignment_generation, i.issued_at, i.next_check_at))
                  FROM firewall_rules_rollout_items i)
             )::text"
        ))
        .fetch_one(&db.pool)
        .await
    };
    let before = delivery().await?;

    let mut conn = db.pool.acquire().await?;
    let released = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(renew_lease(&mut conn, &released, LEASE).await?);
    assert!(requeue(&db.pool, &released, Requeue::Release).await?);
    assert_eq!(item(&db.pool, rollout, node).await?.0, 0);

    let retried = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &retried, Requeue::Retry).await?);

    let expired = lease(&db.pool, rollout, 1).await?.remove(0);
    assert_eq!(
        redrive_expired(&db.pool, rollout, 3).await?,
        RedriveReport::default()
    );
    lapse(&db.pool, rollout, node).await?;
    assert_eq!(
        redrive_expired(&db.pool, rollout, 3).await?,
        RedriveReport {
            requeued: 1,
            failed: 0
        }
    );
    assert_eq!(
        item(&db.pool, rollout, node).await?.1.as_deref(),
        Some(LEASE_EXPIRED)
    );

    // The retried and the redriven leases count; the released one doesn't.
    let reclaimed = lease(&db.pool, rollout, 1).await?.remove(0);
    assert_eq!(reclaimed.attempts, 3);
    assert!(!requeue(&db.pool, &expired, Requeue::Release).await?);

    lapse(&db.pool, rollout, node).await?;
    assert_eq!(
        redrive_expired(&db.pool, rollout, 3).await?,
        RedriveReport {
            requeued: 0,
            failed: 1
        }
    );
    assert_eq!(delivery().await?, before);
    Ok(())
}

/// Catches a new rollout leasing a node an earlier, finished rollout's
/// controller may still be working on, and a renewal of that lease.
#[tokio::test(flavor = "current_thread")]
async fn a_finished_rollouts_live_lease_holds_its_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    let first = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, first, &nodes).await?;
    let held = lease(&db.pool, first, 1).await?.remove(0);
    set_phase(&db.pool, first, "aborted").await?;

    let second = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, second, &nodes).await?;
    assert!(lease(&db.pool, second, 1).await?.is_empty());
    let mut conn = db.pool.acquire().await?;
    assert!(!renew_lease(&mut conn, &held, LEASE).await?);
    lapse(&db.pool, first, node).await?;
    assert_eq!(nodes_of(&lease(&db.pool, second, 1).await?), [node]);
    Ok(())
}
