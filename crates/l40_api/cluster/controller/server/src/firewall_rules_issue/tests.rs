use pplx_space_db_test_fixtures::{
    insert_ready_worker_node, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_firewall_rules_store::RolloutId;
use pplx_space_firewall_rules_store::test_support::{
    hold_header, make_checks_due, rollout, save_targets, wait_for_lock_waiters,
};
use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};
use pplx_space_postgres_pool::Db;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;

/// Bounds every wait on a tick or the loop.
const WAIT: Duration = Duration::from_secs(10);
const CONFIG: Config = Config {
    tick: Duration::from_secs(1),
    lease: Duration::from_secs(600),
    first_check_in: Duration::from_secs(90),
};

/// `count` registered, online nodes in node order, saved as the targets of
/// a new canary.
async fn canary(pool: &PgPool, count: usize) -> TestResult<(RolloutId, Vec<NodeId>)> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        let node = NodeId::new();
        insert_ready_worker_node(pool, node).await?;
        upsert_snapshot(pool, node, SnapshotSeed::default()).await?;
        nodes.push(node);
    }
    nodes.sort_by_key(|node| node.as_uuid());
    let rollout = rollout(pool, "canary").await?;
    save_targets(pool, rollout, &nodes).await?;
    Ok((rollout, nodes))
}

/// Everything a tick may write.
#[derive(Debug, PartialEq)]
struct Snapshot {
    /// Node, generation and revision of each assignment.
    assignments: Vec<(Uuid, i64, Uuid)>,
    items: Vec<Item>,
    /// Node, reasons and generation of each wakeup.
    triggers: Vec<(Uuid, Vec<String>, i64)>,
    observations: i64,
    /// Phase and revision of each rollout.
    rollouts: Vec<(String, Uuid)>,
    adopted: Option<Uuid>,
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Item {
    node_id: Uuid,
    phase: String,
    attempts: i64,
    lease_token: Option<Uuid>,
    lease_expires_at: Option<OffsetDateTime>,
    issued_assignment_generation: Option<i64>,
    issued_at: Option<OffsetDateTime>,
    next_check_at: Option<OffsetDateTime>,
}

async fn snapshot(pool: &PgPool) -> TestResult<Snapshot> {
    Ok(Snapshot {
        assignments: sqlx::query_as(sql!(
            "SELECT node_id, assignment_generation, revision_id
             FROM firewall_rules_node_assignments ORDER BY node_id"
        ))
        .fetch_all(pool)
        .await?,
        items: sqlx::query_as(sql!(
            "SELECT node_id, phase, attempts, lease_token, lease_expires_at,
                    issued_assignment_generation, issued_at, next_check_at
             FROM firewall_rules_rollout_items ORDER BY node_id"
        ))
        .fetch_all(pool)
        .await?,
        triggers: sqlx::query_as(sql!(
            "SELECT node_id, reasons::text[], generation
             FROM node_reconcile_triggers ORDER BY node_id"
        ))
        .fetch_all(pool)
        .await?,
        observations: sqlx::query_scalar(sql!(
            "SELECT count(*) FROM firewall_rules_node_observations"
        ))
        .fetch_one(pool)
        .await?,
        rollouts: sqlx::query_as(sql!(
            "SELECT phase, to_revision_id FROM firewall_rules_rollouts"
        ))
        .fetch_all(pool)
        .await?,
        adopted: sqlx::query_scalar(sql!(
            "SELECT enrollment_default_revision_id FROM firewall_rules_control"
        ))
        .fetch_one(pool)
        .await?,
    })
}

async fn tick_once(pool: &PgPool) -> TestResult<Option<Issuance>> {
    Ok(tokio::time::timeout(WAIT, tick(pool, &CONFIG)).await??)
}

/// Catches a wakeup queued with another reason, which the node wouldn't
/// read as a firewall change. The store's tests cover the rest of issuance.
#[tokio::test(flavor = "current_thread")]
async fn the_wakeup_queues_firewall_rules_changed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    let mut tx = db.pool.begin().await?;
    assert_eq!(FirewallWakeup.wake(&mut *tx, node).await?, 1);
    tx.commit().await?;
    let reasons = sqlx::query_scalar::<_, Vec<String>>(sql!(
        "SELECT reasons::text[] FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        reasons,
        [ReconcileReason::FirewallRulesChanged.as_str().to_owned()]
    );
    Ok(())
}

/// Catches a loop that reads the database with no config, even once, which
/// would make every deployment a firewall producer; one that never ticks;
/// and one that outlives its shutdown token, which would keep issuing after
/// leadership is lost.
#[tokio::test(flavor = "current_thread")]
async fn the_loop_runs_only_when_configured_and_stops_on_shutdown() -> TestResult {
    // Any connection attempt reaches this listener's queue, whatever the
    // pool does after it. Paused, an hour of ticks passes at once.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let options = PgConnectOptions::new()
        .host("127.0.0.1")
        .port(listener.local_addr()?.port());
    tokio::time::pause();
    let shutdown = CancellationToken::new();
    let off = tokio::spawn(run(
        None,
        PgPoolOptions::new().connect_lazy_with(options),
        shutdown.clone(),
    ));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(3600)).await;
    tokio::task::yield_now().await;
    let accepted = listener.accept();
    assert!(
        accepted
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
        "an unconfigured loop connected: {accepted:?}"
    );
    shutdown.cancel();
    tokio::time::timeout(WAIT, off).await??;
    tokio::time::resume();

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (_, nodes) = canary(&db.pool, 1).await?;
    // The first tick runs at once; an hourly one means shutdown can't wait
    // for the next.
    let hourly = Config {
        tick: Duration::from_secs(3600),
        ..CONFIG
    };
    let shutdown = CancellationToken::new();
    let on = tokio::spawn(run(Some(hourly), db.pool.clone(), shutdown.clone()));
    let deadline = tokio::time::Instant::now() + WAIT;
    while snapshot(&db.pool).await?.assignments.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the loop never issued to {}",
            nodes[0]
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    tokio::time::timeout(WAIT, on).await??;
    Ok(())
}

/// Catches a tick that issues more than one canary, keeps the lease, keeps
/// the attempt, moves the first check, advances the rollout or writes an
/// observation; and a later tick that claims the issued canary's due check
/// or, with the canary offline, issues a second one.
#[tokio::test(flavor = "current_thread")]
async fn one_tick_issues_one_canary_and_later_ticks_leave_it_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (rollout, nodes) = canary(&db.pool, 2).await?;
    let (pinned, other) = (nodes[0], nodes[1]);

    let Some(Issuance::Issued(stamp)) = tick_once(&db.pool).await? else {
        panic!("the first tick didn't issue");
    };
    assert_eq!(
        stamp.next_check_at,
        Some(stamp.issued_at + CONFIG.first_check_in)
    );
    let issued = snapshot(&db.pool).await?;
    let revision = issued.rollouts[0].1;
    assert_eq!(issued.assignments, [(pinned.as_uuid(), 1, revision)]);
    let (canary, unissued) = (&issued.items[0], &issued.items[1]);
    let first_check = canary
        .issued_at
        .map(|at| at + time::Duration::try_from(CONFIG.first_check_in).expect("a short delay"));
    assert_eq!(
        (canary.node_id, canary.phase.as_str(), canary.attempts),
        (pinned.as_uuid(), "pending", 0)
    );
    assert_eq!((canary.lease_token, canary.lease_expires_at), (None, None));
    assert_eq!(canary.issued_assignment_generation, Some(1));
    assert!(canary.issued_at.is_some());
    assert_eq!(canary.next_check_at, first_check);
    assert_eq!(
        *unissued,
        Item {
            node_id: other.as_uuid(),
            phase: "pending".to_owned(),
            attempts: 0,
            lease_token: None,
            lease_expires_at: None,
            issued_assignment_generation: None,
            issued_at: None,
            next_check_at: None,
        }
    );
    assert_eq!(
        issued.triggers,
        [(
            pinned.as_uuid(),
            vec!["firewall_rules_changed".to_owned()],
            0
        )]
    );
    assert_eq!(issued.observations, 0);
    assert_eq!(issued.rollouts, [("canary".to_owned(), revision)]);
    assert_eq!(issued.adopted, None);

    make_checks_due(&db.pool, rollout).await?;
    let due = snapshot(&db.pool).await?;
    for (case, reachable) in [("due", true), ("offline", false)] {
        let seed = SnapshotSeed {
            reachable,
            ..SnapshotSeed::default()
        };
        upsert_snapshot(&db.pool, pinned, seed).await?;
        for _ in 0..3 {
            assert_eq!(tick_once(&db.pool).await?, None, "{case}");
        }
        assert_eq!(snapshot(&db.pool).await?, due, "{case}");
    }
    Ok(())
}

/// Catches claims that don't serialize on the header: each tick would lease
/// a target, and issuance would refuse the second.
#[tokio::test(flavor = "current_thread")]
async fn racing_ticks_issue_one_canary() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let (rollout, _) = canary(&db.pool, 2).await?;
    let holder = hold_header(&Db::from(db.pool.clone()), rollout).await?;
    let ticks: Vec<_> = (0..2)
        .map(|_| {
            let pool = db.pool.clone();
            tokio::spawn(async move { tick(&pool, &CONFIG).await })
        })
        .collect();
    wait_for_lock_waiters(&db.pool, 2).await?;
    holder.commit().await?;
    let mut issued = 0;
    for racing in ticks {
        match tokio::time::timeout(WAIT, racing).await??? {
            Some(Issuance::Issued(_)) => issued += 1,
            None => {}
            other => panic!("a racing tick returned {other:?}"),
        }
    }
    assert_eq!(issued, 1);
    let issued = snapshot(&db.pool).await?;
    assert_eq!((issued.assignments.len(), issued.triggers.len()), (1, 1));
    Ok(())
}
