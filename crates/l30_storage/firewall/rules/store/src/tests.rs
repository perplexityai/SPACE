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

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Item {
    phase: String,
    attempts: i64,
    lease_token: Option<Uuid>,
    last_error: Option<String>,
    issued_assignment_generation: Option<i64>,
    issued_at: Option<OffsetDateTime>,
    next_check_at: Option<OffsetDateTime>,
}

async fn item(pool: &PgPool, rollout: RolloutId, node: NodeId) -> TestResult<Item> {
    Ok(sqlx::query_as::<_, Item>(sql!(
        "SELECT phase, attempts, lease_token, last_error, issued_assignment_generation,
                issued_at, next_check_at
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

/// Catches a lease that isn't committed when it returns, or that another
/// connection can't read back: the first thing any controller relies on.
#[tokio::test(flavor = "current_thread")]
async fn a_returned_lease_is_committed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    save_targets(&db.pool, rollout, &nodes).await?;

    let mut conn = db.pool.acquire().await?;
    let leased = lease_targets(&mut conn, rollout, 1, LEASE, SCAN).await?;
    assert_eq!(nodes_of(&leased), nodes);
    assert_eq!(leased[0].work, Work::FirstAssignment);
    assert_eq!(leased[0].attempts, 1);

    let (token, attempts, live_for) = sqlx::query_as::<_, (Option<Uuid>, i64, f64)>(sql!(
        "SELECT lease_token, attempts,
                extract(epoch FROM lease_expires_at - clock_timestamp())::float8
         FROM firewall_rules_rollout_items
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(nodes[0])
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(token, Some(leased[0].lease_token));
    assert_eq!(attempts, 1);
    assert!(
        live_for > 0.0 && live_for <= LEASE.as_secs_f64(),
        "the committed lease runs for {live_for}s"
    );
    Ok(())
}

/// Catches claims that don't serialize on the header or don't count work
/// already in flight, letting two controllers act on one node or past the
/// active limit; and a failure budget that pauses on an unrelated failure.
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

    let phase = sqlx::query_scalar::<_, String>(sql!(
        "SELECT phase FROM firewall_rules_rollouts WHERE id = $1"
    ))
    .bind(rollout)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(phase, "rolling");
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
    lapse(&db.pool, rollout, a).await?;
    assert_eq!(
        redrive_expired(&db.pool, rollout, MAX_ATTEMPTS).await?,
        RedriveReport {
            requeued: 1,
            failed: 0
        }
    );
    set_status(&db.pool, a, false, 60.0).await?;

    let second = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&second), [b]);
    assert_eq!(second[0].work, Work::FirstAssignment);

    // A's token, on A's item or aimed at B's, holds nothing.
    let stale_on_b = LeasedTarget {
        node_id: b,
        ..first[0].clone()
    };
    for stale in [&first[0], &stale_on_b] {
        let mut conn = db.pool.acquire().await?;
        assert!(!renew_lease(&mut conn, stale, LEASE).await?);
        assert!(!requeue(&db.pool, stale, Requeue::Release).await?);
    }
    let b_item = item(&db.pool, rollout, b).await?;
    assert_eq!(
        (b_item.phase.as_str(), b_item.lease_token),
        ("in_flight", Some(second[0].lease_token))
    );
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
    let issued_at = issue(&db.pool, rollout, b, 7, Some(-1.0)).await?;
    assert!(requeue(&db.pool, &canary[0], Requeue::Release).await?);
    set_status(&db.pool, a, true, 60.0).await?;

    // A fresh connection, as a restarted controller's, still finds only B.
    let leased = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&leased), [b]);
    assert_eq!(
        leased[0].work,
        Work::Check {
            assignment_generation: 7,
            issued_at
        }
    );
    assert!(requeue(&db.pool, &leased[0], Requeue::Release).await?);

    // Each case holds B where a check can't take it now.
    let cases: [(&str, &str); 6] = [
        (
            "offline",
            sql!(
                "UPDATE node_status_snapshots nss SET reachable = false
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
        (
            "done",
            sql!(
                "UPDATE firewall_rules_rollout_items SET phase = 'done'
                 WHERE rollout_id = $1 AND node_id = $2"
            ),
        ),
        (
            "leased",
            sql!(
                "UPDATE firewall_rules_rollout_items
                 SET phase = 'in_flight', lease_token = gen_random_uuid(),
                     lease_expires_at = now() + interval '1 hour'
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
        let a_item = item(&db.pool, rollout, a).await?;
        assert_eq!(
            (a_item.phase.as_str(), a_item.attempts),
            ("pending", 0),
            "B {case}"
        );
        // Back to online, pending and due for the next case.
        set_status(&db.pool, b, true, 60.0).await?;
        sqlx::query(sql!(
            "UPDATE firewall_rules_rollout_items
             SET phase = 'pending', lease_token = NULL, lease_expires_at = NULL,
                 next_check_at = now() - interval '1 second'
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
    let waiting = [
        item(&db.pool, rollout, unscheduled).await?,
        item(&db.pool, rollout, future).await?,
    ];

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
    for target in &leased {
        assert!(requeue(&db.pool, target, Requeue::Release).await?);
    }
    for _ in 0..2 {
        let leased = lease(&db.pool, rollout, 10).await?;
        assert_eq!(nodes_of(&leased), [unissued, due]);
        for target in &leased {
            assert!(requeue(&db.pool, target, Requeue::Release).await?);
        }
    }
    assert_eq!(
        [
            item(&db.pool, rollout, unscheduled).await?,
            item(&db.pool, rollout, future).await?,
        ],
        waiting
    );
    let due_item = item(&db.pool, rollout, due).await?;
    assert_eq!(
        (due_item.issued_assignment_generation, due_item.issued_at),
        (Some(3), Some(issued_at))
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

    let second = lease(&db.pool, rollout, 1).await?;
    assert_eq!(nodes_of(&second), [b]);
    assert_eq!(second[0].work, Work::FirstAssignment);
    let a_item = item(&db.pool, rollout, a).await?;
    assert_eq!(
        (a_item.phase.as_str(), a_item.issued_assignment_generation),
        ("pending", Some(1))
    );
    Ok(())
}

/// Catches membership read from the node inventory instead of the saved
/// items, an ineligible node charged an attempt for being looked at, and a
/// lease from a header that doesn't lease.
#[tokio::test(flavor = "current_thread")]
async fn only_saved_online_targets_of_a_leasing_rollout_are_leased() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "paused").await?;
    let nodes = online_nodes(&db.pool, 4).await?;
    let (unreachable, stale, deleted, eligible) = (nodes[0], nodes[1], nodes[2], nodes[3]);
    save_targets(&db.pool, rollout, &nodes).await?;
    // Registered after the rollout saved its targets.
    online_nodes(&db.pool, 1).await?;
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
    for _ in 0..2 {
        let leased = lease(&db.pool, rollout, 10).await?;
        assert_eq!(nodes_of(&leased), [eligible]);
        assert!(requeue(&db.pool, &leased[0], Requeue::Release).await?);
    }
    for node in [unreachable, stale, deleted] {
        let saved = item(&db.pool, rollout, node).await?;
        assert_eq!((saved.phase.as_str(), saved.attempts), ("pending", 0));
    }
    let saved = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*) FROM firewall_rules_rollout_items WHERE rollout_id = $1"
    ))
    .bind(rollout)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(saved, 4);
    Ok(())
}

/// Catches a renewal that reads the clock before it waits for the item
/// lock, reviving a lease another controller may already have been given,
/// and renewals past the shared clamp or under a token that isn't live.
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

    let replaced = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &replaced, Requeue::Release).await?);
    let live = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(renew_lease(&mut conn, &live, MAX_LEASE * 2).await?);
    let live_for = sqlx::query_scalar::<_, f64>(sql!(
        "SELECT extract(epoch FROM lease_expires_at - clock_timestamp())::float8
         FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        live_for <= MAX_LEASE.as_secs_f64(),
        "renewed past the clamp: {live_for}s"
    );
    let wrong = LeasedTarget {
        lease_token: Uuid::now_v7(),
        ..live.clone()
    };
    assert!(!renew_lease(&mut conn, &wrong, LEASE).await?);
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
    assert!(!renew_lease(&mut conn, &live, LEASE).await?);
    Ok(())
}

/// Catches a release refunded twice or a retry refunded at all, which
/// would let a node be leased past its attempt limit; an old token that
/// changes a reclaimed item; and cleanup that clears what was issued.
#[tokio::test(flavor = "current_thread")]
async fn requeues_and_redrives_are_fenced_and_keep_issuance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    issue(&db.pool, rollout, node, 4, Some(-1.0)).await?;
    let issued = item(&db.pool, rollout, node).await?;
    let keeps_issuance = |item: &Item| {
        assert_eq!(
            (
                item.issued_assignment_generation,
                item.issued_at,
                item.next_check_at
            ),
            (
                issued.issued_assignment_generation,
                issued.issued_at,
                issued.next_check_at
            )
        );
    };

    let released = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &released, Requeue::Release).await?);
    assert!(!requeue(&db.pool, &released, Requeue::Release).await?);
    assert_eq!(item(&db.pool, rollout, node).await?.attempts, 0);

    let retried = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &retried, Requeue::Retry).await?);
    let after_retry = item(&db.pool, rollout, node).await?;
    assert_eq!(
        (after_retry.phase.as_str(), after_retry.attempts),
        ("pending", 1)
    );
    keeps_issuance(&after_retry);

    let expired = lease(&db.pool, rollout, 1).await?.remove(0);
    lapse(&db.pool, rollout, node).await?;
    assert_eq!(
        redrive_expired(&db.pool, rollout, 3).await?,
        RedriveReport {
            requeued: 1,
            failed: 0
        }
    );
    let redriven = item(&db.pool, rollout, node).await?;
    assert_eq!(
        (redriven.phase.as_str(), redriven.last_error.as_deref()),
        ("pending", Some(LEASE_EXPIRED))
    );
    keeps_issuance(&redriven);

    let reclaimed = lease(&db.pool, rollout, 1).await?.remove(0);
    assert_eq!(reclaimed.attempts, 3);
    assert!(!requeue(&db.pool, &expired, Requeue::Release).await?);
    let held = item(&db.pool, rollout, node).await?;
    assert_eq!(
        (held.phase.as_str(), held.attempts, held.lease_token),
        ("in_flight", 3, Some(reclaimed.lease_token))
    );

    lapse(&db.pool, rollout, node).await?;
    assert_eq!(
        redrive_expired(&db.pool, rollout, 3).await?,
        RedriveReport {
            requeued: 0,
            failed: 1
        }
    );
    let failed = item(&db.pool, rollout, node).await?;
    assert_eq!(failed.phase, "failed");
    keeps_issuance(&failed);
    Ok(())
}

/// Catches a new rollout leasing a node an earlier, finished rollout's
/// controller may still be working on.
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

    // Held until released.
    let second = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, second, &nodes).await?;
    assert!(lease(&db.pool, second, 1).await?.is_empty());
    let mut conn = db.pool.acquire().await?;
    assert!(!renew_lease(&mut conn, &held, LEASE).await?);
    assert!(requeue(&db.pool, &held, Requeue::Release).await?);
    assert_eq!(nodes_of(&lease(&db.pool, second, 1).await?), [node]);
    set_phase(&db.pool, second, "aborted").await?;

    // Held until it lapses.
    let third = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, third, &nodes).await?;
    assert!(lease(&db.pool, third, 1).await?.is_empty());
    lapse(&db.pool, second, node).await?;
    assert_eq!(nodes_of(&lease(&db.pool, third, 1).await?), [node]);
    Ok(())
}

/// Catches lease bookkeeping that writes delivery state: an assignment, an
/// issuance stamp, an observation or a wakeup would tell the node to act, or
/// say it had, without the assignment transaction.
#[tokio::test(flavor = "current_thread")]
async fn lease_bookkeeping_writes_no_delivery_state() -> TestResult {
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
    let retried = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &retried, Requeue::Retry).await?);
    lease(&db.pool, rollout, 1).await?;
    lapse(&db.pool, rollout, node).await?;
    assert_eq!(
        redrive_expired(&db.pool, rollout, MAX_ATTEMPTS)
            .await?
            .requeued,
        1
    );

    assert_eq!(delivery().await?, before);
    Ok(())
}
