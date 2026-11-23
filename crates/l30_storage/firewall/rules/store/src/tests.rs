use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{
    TestDb, insert_ready_worker_node, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};
use pplx_space_rollout_queue::LEASE_EXPIRED;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;
pub(crate) use crate::test_support::{
    hold_header, rollout, save_targets, set_phase, wait_for_lock_waiters,
};

mod issuance;

const LEASE: Duration = Duration::from_secs(600);
const SCAN: u32 = 100;
const MAX_ATTEMPTS: u64 = 5;
const RELEASE: Requeue = Requeue::Release { error: None };

/// `count` registered nodes with a reachable, fresh status snapshot, in the
/// order a claim scans them.
pub(crate) async fn online_nodes(pool: &PgPool, count: usize) -> TestResult<Vec<NodeId>> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        let node = NodeId::new();
        insert_ready_worker_node(pool, node).await?;
        upsert_snapshot(pool, node, SnapshotSeed::default()).await?;
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
    let seed = SnapshotSeed {
        reachable,
        stale_in_secs,
        ..SnapshotSeed::default()
    };
    Ok(upsert_snapshot(pool, node, seed).await?)
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
) -> TestResult<jiff::Timestamp> {
    let issued_at = sqlx::query_scalar::<_, OffsetDateTime>(sql!(
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
    .await?;
    Ok(jiff::Timestamp::from_nanosecond(
        issued_at.unix_timestamp_nanos(),
    )?)
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

/// `node`'s whole item row, to show a claim left it alone.
async fn row(pool: &PgPool, rollout: RolloutId, node: NodeId) -> TestResult<String> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT to_jsonb(i)::text FROM firewall_rules_rollout_items i
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(pool)
    .await?)
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

    assert!(requeue(&db.pool, &leased[0], &RELEASE).await?);
    save_targets(&db.pool, rollout, &[b]).await?;
    let leased = race(&db, rollout, 1).await?;
    assert_eq!(
        leased.len(),
        1,
        "two leases committed into one slot: {leased:?}"
    );
    let live = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*) FROM firewall_rules_rollout_items
         WHERE rollout_id = $1 AND phase = 'in_flight'
           AND lease_expires_at > clock_timestamp()"
    ))
    .bind(rollout)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(live, 1, "live leases in the database");
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
    assert!(requeue(&db.pool, &canary[0], &RELEASE).await?);
    set_status(&db.pool, a, true, 60.0).await?;

    let leased = lease(&db.pool, rollout, 10).await?;
    assert_eq!(nodes_of(&leased), [b]);
    assert!(requeue(&db.pool, &leased[0], &RELEASE).await?);

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
/// poll a node in a loop or before its rules could have landed; a check
/// lease that loses the stamp of what was issued; new work scanned ahead of
/// due checks, which could crowd out proof of existing work; and a check
/// lease that doesn't use up its slot.
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

    // `unissued` waits for the slot, first behind the check, then while the
    // check's lease is live.
    let leased = lease(&db.pool, rollout, 1).await?;
    assert_eq!(nodes_of(&leased), [due]);
    assert_eq!(
        leased[0].work,
        Work::Check {
            assignment_generation: 3,
            issued_at
        }
    );
    let next = lease(&db.pool, rollout, 1).await?;
    assert!(
        next.is_empty(),
        "{unissued:?} took the check's slot: {next:?}"
    );
    Ok(())
}

/// Catches a slot held by an issued node awaiting its check, which would
/// stall the rollout on a node's proof; one revoked when the leased node goes
/// offline; and one held by a lapsed lease, which no controller can use.
#[tokio::test(flavor = "current_thread")]
async fn in_rolling_only_a_live_lease_holds_a_slot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 3).await?;
    let (a, b, c) = (nodes[0], nodes[1], nodes[2]);
    save_targets(&db.pool, rollout, &nodes).await?;

    let first = lease(&db.pool, rollout, 1).await?;
    assert_eq!(nodes_of(&first), [a]);
    set_status(&db.pool, a, false, 60.0).await?;
    assert!(lease(&db.pool, rollout, 1).await?.is_empty());
    set_status(&db.pool, a, true, 60.0).await?;

    // Released before A proves anything: A stays pending and stamped.
    issue(&db.pool, rollout, a, 1, Some(3600.0)).await?;
    assert!(requeue(&db.pool, &first[0], &RELEASE).await?);
    let released = row(&db.pool, rollout, a).await?;
    let second = lease(&db.pool, rollout, 1).await?;
    assert_eq!(nodes_of(&second), [b]);
    assert_eq!(second[0].work, Work::FirstAssignment);
    assert_eq!(row(&db.pool, rollout, a).await?, released);

    // Lapsed but not redriven: B's row is left as it is.
    lapse(&db.pool, rollout, b).await?;
    let lapsed = row(&db.pool, rollout, b).await?;
    assert_eq!(nodes_of(&lease(&db.pool, rollout, 1).await?), [c]);
    assert_eq!(row(&db.pool, rollout, b).await?, lapsed);
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
    // In whole microseconds, so the bounds compare exactly.
    let clock = || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::int8"
        ))
        .fetch_one(&db.pool)
    };
    let expires = || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT (extract(epoch FROM lease_expires_at) * 1000000)::int8
             FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
        ))
        .bind(rollout)
        .bind(node)
        .fetch_one(&db.pool)
    };

    let replaced = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(requeue(&db.pool, &replaced, &RELEASE).await?);
    let before = clock().await?;
    let live = lease(&db.pool, rollout, 1).await?.remove(0);
    let after = clock().await?;
    let expires_at = expires().await?;
    let lease_us = i64::try_from(LEASE.as_micros())?;
    assert!(
        (before + lease_us..=after + lease_us).contains(&expires_at),
        "lease expires at {expires_at}us, not {lease_us}us after [{before}, {after}]us"
    );
    let before = clock().await?;
    assert!(renew_lease(&mut conn, &live, LEASE * 2).await?);
    let after = clock().await?;
    let expires_at = expires().await?;
    let renew_us = i64::try_from((LEASE * 2).as_micros())?;
    assert!(
        (before + renew_us..=after + renew_us).contains(&expires_at),
        "renewal expires at {expires_at}us, not {renew_us}us after [{before}, {after}]us"
    );
    assert!(!renew_lease(&mut conn, &replaced, LEASE).await?);

    // Hold the item lock and see the renewal wait on it before the lease
    // starts its last 300 ms.
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
    let waiting = live.clone();
    let renewing = tokio::spawn(async move { renew_lease(&mut conn, &waiting, LEASE).await });
    wait_for_lock_waiters(&db.pool, 1).await?;
    // Written under the lock, so the renewal reads it once the lock is
    // released.
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET lease_expires_at = clock_timestamp() + interval '300 milliseconds'
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .execute(&mut *locker)
    .await?;
    let live_now = sql!(
        "SELECT lease_expires_at > clock_timestamp()
         FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
    );
    // Live after the renewal waits, so only a clock read after the wait
    // refuses it.
    assert!(
        sqlx::query_scalar::<_, bool>(live_now)
            .bind(rollout)
            .bind(node)
            .fetch_one(&mut *locker)
            .await?,
        "the lease lapsed before it was seen live during the wait"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while sqlx::query_scalar::<_, bool>(live_now)
        .bind(rollout)
        .bind(node)
        .fetch_one(&mut *locker)
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
/// which would misjudge a node's attempt limit; a requeue that drops its
/// error or keeps a stale one; a retried check due at once, which would poll
/// the node in a loop; an old token that changes a reclaimed item; a redrive
/// of a live lease; and bookkeeping that writes other delivery state: an
/// assignment, issuance, observation or wakeup would tell the node to act, or
/// say it had, without the assignment transaction.
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
    let not_ready = Requeue::Release {
        error: Some("not ready".to_owned()),
    };
    assert!(requeue(&db.pool, &released, &not_ready).await?);
    assert_eq!(
        item(&db.pool, rollout, node).await?,
        (0, Some("not ready".to_owned()))
    );

    let retried = lease(&db.pool, rollout, 1).await?.remove(0);
    let scheduled = sqlx::query_scalar::<_, OffsetDateTime>(sql!(
        "SELECT next_check_at FROM firewall_rules_rollout_items
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(&db.pool)
    .await?;
    let retry = Requeue::Retry {
        next_check_in: Duration::from_secs(3600),
        error: None,
    };
    assert!(requeue(&db.pool, &retried, &retry).await?);
    assert_eq!(item(&db.pool, rollout, node).await?.1, None);
    assert!(lease(&db.pool, rollout, 1).await?.is_empty());
    // Due again, as it was, for the rest of the test.
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items SET next_check_at = $3
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .bind(scheduled)
    .execute(&db.pool)
    .await?;

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
    assert!(!requeue(&db.pool, &expired, &RELEASE).await?);

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
/// controller may still be working on, a renewal of that lease, and a
/// claim that also takes the finished rollout's pending item.
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
    assert!(requeue(&db.pool, &held, &RELEASE).await?);
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

/// Catches a scan that spends its limit on nodes another rollout holds,
/// which would find no work while an eligible node waits behind them.
#[tokio::test(flavor = "current_thread")]
async fn a_node_held_elsewhere_doesnt_use_up_the_scan() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let nodes = online_nodes(&db.pool, 2).await?;
    let (a, b) = (nodes[0], nodes[1]);
    let old = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, old, &[a]).await?;
    assert_eq!(nodes_of(&lease(&db.pool, old, 1).await?), [a]);
    set_phase(&db.pool, old, "aborted").await?;

    let new = rollout(&db.pool, "rolling").await?;
    save_targets(&db.pool, new, &nodes).await?;
    let mut conn = db.pool.acquire().await?;
    let leased = lease_targets(&mut conn, new, 10, LEASE, 1).await?;
    assert_eq!(nodes_of(&leased), [b]);
    Ok(())
}

/// Catches a claim that trusts the scan, or rechecks before its row lock: a
/// node that goes offline or stops being due after it would still be leased
/// and charged an attempt.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_rechecks_eligibility_at_the_write() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "rolling").await?;
    let nodes = online_nodes(&db.pool, 4).await?;
    let (first, offline, not_due, eligible) = (nodes[0], nodes[1], nodes[2], nodes[3]);
    save_targets(&db.pool, rollout, &nodes).await?;
    // Due checks, so the claim, which scans them first, writes `first` first.
    for node in [first, not_due, eligible] {
        issue(&db.pool, rollout, node, 1, Some(-1.0)).await?;
    }

    // The claim scans all four, then waits to write the first, which goes
    // offline during the wait.
    let mut locker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_rollout_items
         WHERE rollout_id = $1 AND node_id = $2
         FOR UPDATE"
    ))
    .bind(rollout)
    .bind(first)
    .execute(&mut *locker)
    .await?;
    let pool = db.pool.clone();
    let claiming = tokio::spawn(async move {
        let mut conn = pool.acquire().await?;
        lease_targets(&mut conn, rollout, 10, LEASE, SCAN).await
    });
    wait_for_lock_waiters(&db.pool, 1).await?;
    set_status(&db.pool, first, false, 60.0).await?;
    set_status(&db.pool, offline, false, 60.0).await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items
         SET next_check_at = now() + interval '1 hour'
         WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(not_due)
    .execute(&db.pool)
    .await?;
    locker.commit().await?;

    assert_eq!(nodes_of(&claiming.await??), [eligible]);
    for node in [first, offline, not_due] {
        assert_eq!(item(&db.pool, rollout, node).await?.0, 0);
        let phase = sqlx::query_scalar::<_, String>(sql!(
            "SELECT phase FROM firewall_rules_rollout_items
             WHERE rollout_id = $1 AND node_id = $2"
        ))
        .bind(rollout)
        .bind(node)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(phase, "pending");
    }
    Ok(())
}
