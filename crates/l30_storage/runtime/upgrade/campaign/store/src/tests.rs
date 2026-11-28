use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_content_digest::Digest;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDb, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::Db;
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};

use super::test_support::{CampaignSeed, insert_campaign, insert_campaign_with_kind};
use super::*;

mod kinds;
mod operator;
mod twin_readers;

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

const LEASE: Duration = Duration::from_secs(600);
/// Leases an item may take, as the reconciler allows.
const MAX_ATTEMPTS: u64 = 5;
const SCAN: u32 = 100;

/// Insert a sandbox of `managed_by` in `lifecycle`, published by `node` with
/// a bound execution on environment 0 when `node` is given.
async fn insert_sandbox(
    pool: &PgPool,
    managed_by: SubstrateKind,
    lifecycle: SandboxLifecycle,
    node: Option<NodeId>,
) -> TestResult<Uuid> {
    let sandbox_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         )
         VALUES ($1, $2, $3, $4, $5, 'on_node', 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(lifecycle.as_str())
    .execute(pool)
    .await?;
    if let Some(node) = node {
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, runtime, runtime_version, execution_metadata
             )
             VALUES ($1, $2, 'firecracker', '1.15.1', jsonb_build_object('environment', $3::text))"
        ))
        .bind(sandbox_id)
        .bind(node)
        .bind(environment(0).to_string())
        .execute(pool)
        .await?;
    }
    Ok(sandbox_id)
}

/// A worker node whose status is reachable evidence of its environments.
async fn node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    upsert_snapshot(pool, node, SnapshotSeed::default()).await?;
    Ok(node)
}

/// `count` stopped Kubernetes sandboxes published by `node`, recorded as
/// pending items of `campaign`.
async fn pending_on(
    db: &TestDb,
    campaign: CampaignId,
    node: Option<NodeId>,
    count: usize,
) -> TestResult<Vec<Uuid>> {
    let mut sandboxes = Vec::new();
    for _ in 0..count {
        sandboxes.push(
            insert_sandbox(
                &db.pool,
                SubstrateKind::Kubernetes,
                SandboxLifecycle::Stopped,
                node,
            )
            .await?,
        );
    }
    let mut conn = db.pool.acquire().await?;
    insert_pending(
        &mut *conn,
        campaign,
        SubjectKind::Sandbox,
        &sandboxes,
        MAX_ATTEMPTS,
    )
    .await?;
    Ok(sandboxes)
}

async fn lease(db: &TestDb, campaign: CampaignId) -> TestResult<Vec<LeasedItem>> {
    let mut conn = db.pool.acquire().await?;
    Ok(lease_items(&mut *conn, campaign, LEASE, SCAN).await?)
}

async fn item_phase(pool: &PgPool, campaign: CampaignId, sandbox: Uuid) -> TestResult<String> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT phase FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_kind = 'sandbox' AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

async fn campaign_phase(pool: &PgPool, campaign: CampaignId) -> TestResult<(String, bool)> {
    Ok(sqlx::query_as::<_, (String, bool)>(sql!(
        "SELECT phase, finished_at IS NOT NULL FROM runtime_upgrade_campaigns WHERE id = $1"
    ))
    .bind(campaign)
    .fetch_one(pool)
    .await?)
}

/// Wait, for at most ten seconds, until a session on this test database is
/// waiting for a lock: the point at which a racing statement is known to be
/// blocked rather than not started yet.
async fn wait_for_lock_waiter(pool: &PgPool) -> TestResult {
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
        if waiting > 0 {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no session started waiting for a lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait, on the database clock, until every committed lease of `campaign`
/// has lapsed. Reads past uncommitted writes, so a renewal still open in
/// another transaction doesn't count.
async fn wait_for_committed_lapse(pool: &PgPool, campaign: CampaignId) -> TestResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let live = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM runtime_upgrade_items
                 WHERE campaign_id = $1 AND phase = 'in_flight'
                   AND lease_expires_at > now())"
        ))
        .bind(campaign)
        .fetch_one(pool)
        .await?;
        if !live {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the committed lease didn't lapse"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Lapse every lease of `campaign`, as if its workers died.
async fn lapse_leases(pool: &PgPool, campaign: CampaignId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = now() - interval '1 second'
         WHERE campaign_id = $1 AND phase = 'in_flight'"
    ))
    .bind(campaign)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unfinished_campaigns_are_listed_oldest_first() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rolling = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Completed,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let paused = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Paused,
            kind: CampaignKind::Templates,
            per_node: 2,
            per_cluster: 5,
            failure_budget: 7,
            include_running: true,
            ..CampaignSeed::default()
        },
    )
    .await?;

    let campaigns = list_unfinished(&db.pool).await?;
    assert_eq!(
        campaigns
            .iter()
            .map(|campaign| campaign.id)
            .collect::<Vec<_>>(),
        vec![rolling, paused]
    );
    assert_eq!(
        campaigns[1],
        Campaign {
            id: paused,
            from: environment(0),
            to: environment(1),
            kind: CampaignKind::Templates,
            phase: CampaignPhase::Paused,
            max_in_flight_per_node: 2,
            max_in_flight_per_cluster: 5,
            failure_budget: 7,
            include_running: true,
            generation: 0,
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unknown_phase_fails_the_campaign_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'warming' WHERE id = $1"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;

    let error = list_unfinished(&db.pool)
        .await
        .expect_err("an unknown phase must fail the read");
    assert!(
        matches!(&error, ReadCampaignError::UnknownValue { campaign_id, .. } if *campaign_id == campaign),
        "{error:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn items_go_only_into_unfinished_sandbox_campaigns_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let templates = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::Templates,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let aborted = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Aborted,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let sandboxes = [Uuid::now_v7(), Uuid::now_v7()];
    let mut conn = db.pool.acquire().await?;

    assert_eq!(
        insert_pending(
            &mut *conn,
            campaign,
            SubjectKind::Sandbox,
            &sandboxes,
            MAX_ATTEMPTS
        )
        .await?,
        2
    );
    assert_eq!(
        insert_pending(
            &mut *conn,
            campaign,
            SubjectKind::Sandbox,
            &sandboxes,
            MAX_ATTEMPTS
        )
        .await?,
        0
    );
    assert_eq!(
        insert_pending(
            &mut *conn,
            templates,
            SubjectKind::Sandbox,
            &sandboxes,
            MAX_ATTEMPTS
        )
        .await?,
        0
    );
    assert_eq!(
        insert_pending(
            &mut *conn,
            aborted,
            SubjectKind::Sandbox,
            &sandboxes,
            MAX_ATTEMPTS
        )
        .await?,
        0
    );
    assert_eq!(
        item_counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 2,
            ..ItemCounts::default()
        }
    );
    assert_eq!(
        item_counts(&db.pool, templates).await?,
        ItemCounts::default()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_skipped_item_with_leases_left_is_made_pending_again() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let sandboxes = pending_on(&db, campaign, Some(node), 4).await?;
    let leased = lease(&db, campaign).await?;
    let outcome_of = |sandbox: Uuid| match sandboxes.iter().position(|id| *id == sandbox) {
        Some(0) => ItemOutcome::Skipped(ItemError::new("the sandbox had started")),
        Some(1) => ItemOutcome::Done,
        Some(2) => ItemOutcome::Failed(ItemError::new("the boot failed")),
        _ => ItemOutcome::Retry(ItemError::new("busy")),
    };
    for item in &leased {
        finish_item(&db.pool, item, &outcome_of(item.subject_id)).await?;
    }
    // A fifth sandbox, skipped after using up its leases.
    let exhausted = pending_on(&db, campaign, Some(node), 1).await?[0];
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items SET phase = 'skipped', attempts = $3
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(exhausted)
    .bind(MAX_ATTEMPTS as i64)
    .execute(&db.pool)
    .await?;

    // Recording all five again revives only the skipped one with leases
    // left.
    let mut all = sandboxes.clone();
    all.push(exhausted);
    assert_eq!(
        insert_pending(&db.pool, campaign, SubjectKind::Sandbox, &all, MAX_ATTEMPTS).await?,
        1
    );
    let rows = sqlx::query_as::<_, (Uuid, String, i64, Option<String>)>(sql!(
        "SELECT subject_id, phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .fetch_all(&db.pool)
    .await?;
    let row = |sandbox: Uuid| {
        rows.iter()
            .find(|row| row.0 == sandbox)
            .map(|row| (row.1.clone(), row.2, row.3.clone()))
    };
    assert_eq!(
        row(sandboxes[0]),
        Some(("pending".to_owned(), 1, None)),
        "a revived item keeps its attempts"
    );
    assert_eq!(row(exhausted).map(|row| row.0), Some("skipped".to_owned()));
    assert_eq!(row(sandboxes[1]).map(|row| row.0), Some("done".to_owned()));
    assert_eq!(
        row(sandboxes[2]).map(|row| row.0),
        Some("failed".to_owned())
    );
    assert_eq!(
        row(sandboxes[3]),
        Some(("pending".to_owned(), 1, Some("busy".to_owned()))),
        "a pending item keeps its attempts and error"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn leasing_stays_under_the_cluster_cap_counting_items_already_in_flight() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 1,
            per_cluster: 3,
            ..CampaignSeed::default()
        },
    )
    .await?;
    // One item per node, so only the cluster cap binds.
    for _ in 0..5 {
        let node = node(&db.pool).await?;
        pending_on(&db, campaign, Some(node), 1).await?;
    }

    let first = lease(&db, campaign).await?;
    assert_eq!(first.len(), 3);
    assert!(
        first
            .iter()
            .all(|item| item.attempts == 1 && item.node_id.is_some())
    );
    assert!(lease(&db, campaign).await?.is_empty(), "the cap is full");

    assert!(finish_item(&db.pool, &first[0], &ItemOutcome::Done).await?);
    let second = lease(&db, campaign).await?;
    assert_eq!(second.len(), 1, "one slot freed");
    assert_eq!(
        item_counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 1,
            in_flight: 3,
            done: 1,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lease_that_waits_on_another_lease_of_its_campaign_counts_what_that_one_took()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_cluster: 1,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 2).await?;
    // The first lease's transaction stays open, holding the campaign row.
    let mut first = db.pool.begin().await?;
    assert_eq!(
        lease_items(&mut *first, campaign, LEASE, SCAN).await?.len(),
        1
    );

    let pool = db.pool.clone();
    let second = tokio::spawn(async move {
        let mut conn = pool.acquire().await.map_err(DbError::from)?;
        lease_items(&mut *conn, campaign, LEASE, SCAN).await
    });
    wait_for_lock_waiter(&db.pool).await?;
    first.commit().await?;
    let second = second.await??;
    assert!(
        second.is_empty(),
        "the second lease went past the cap the first one filled: {second:?}"
    );
    assert_eq!(
        item_counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 1,
            in_flight: 1,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn leasing_stays_under_the_per_node_cap_and_takes_other_nodes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 2,
            per_cluster: 10,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let busy = node(&db.pool).await?;
    let quiet = node(&db.pool).await?;
    pending_on(&db, campaign, Some(busy), 4).await?;
    pending_on(&db, campaign, Some(quiet), 1).await?;
    // Published by no node: counts toward the cluster cap only.
    let unpublished = pending_on(&db, campaign, None, 1).await?;

    let leased = lease(&db, campaign).await?;
    let on = |node| {
        leased
            .iter()
            .filter(|item| item.node_id == Some(node))
            .count()
    };
    assert_eq!(on(busy), 2);
    assert_eq!(on(quiet), 1);
    assert!(
        leased
            .iter()
            .any(|item| item.subject_id == unpublished[0] && item.node_id.is_none())
    );
    assert_eq!(leased.len(), 4);
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "every node is at its cap"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lease_counts_a_sandbox_on_the_node_that_published_it_while_the_lease_waited()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(60),
            per_node: 1,
            per_cluster: 10,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let first = node(&db.pool).await?;
    let second = node(&db.pool).await?;
    let moving = pending_on(&db, campaign, Some(first), 1).await?.remove(0);
    assert_eq!(lease(&db, campaign).await?.len(), 1);
    let waiting = pending_on(&db, campaign, Some(second), 1).await?.remove(0);
    // Another campaign's renewal, open in a transaction, holds the handoff
    // lock the lease waits for, but not this campaign's row.
    let other = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(61),
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, other, None, 1).await?;
    let held = lease(&db, other).await?.remove(0);
    let mut renewal = db.pool.begin().await?;
    assert!(renew_lease(&mut *renewal, &held, LEASE).await?);

    let pool = db.pool.clone();
    let leasing = tokio::spawn(async move {
        let mut conn = pool.acquire().await.map_err(DbError::from)?;
        lease_items(&mut *conn, campaign, LEASE, SCAN).await
    });
    wait_for_lock_waiter(&db.pool).await?;
    // The in-flight sandbox is published on the second node while the lease
    // waits, so that node is at its cap.
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states SET node_id = $2 WHERE sandbox_id = $1"
    ))
    .bind(moving)
    .bind(second)
    .execute(&db.pool)
    .await?;
    renewal.commit().await?;
    let leased = leasing.await??;
    assert!(leased.is_empty(), "the second node's cap held: {leased:?}");
    assert_eq!(item_phase(&db.pool, campaign, waiting).await?, "pending");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_lease_scan_considers_only_pending_items() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let mut sandboxes = pending_on(&db, campaign, None, 4).await?;
    sandboxes.sort();
    // The two lowest subjects are done, ahead of the pending ones in the
    // scan's order.
    let mut conn = db.pool.acquire().await?;
    for leased in lease_items(&mut *conn, campaign, LEASE, 2).await? {
        assert!(sandboxes[..2].contains(&leased.subject_id));
        finish_item(&db.pool, &leased, &ItemOutcome::Done).await?;
    }

    let leased = lease_items(&mut *conn, campaign, LEASE, 2).await?;
    let mut leased: Vec<_> = leased.iter().map(|item| item.subject_id).collect();
    leased.sort();
    assert_eq!(
        leased,
        sandboxes[2..],
        "a scan of two finds both pending items"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_canary_leases_one_item_at_a_time_and_stops_once_one_is_done() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            per_node: 5,
            per_cluster: 5,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    pending_on(&db, campaign, Some(node), 3).await?;

    let first = lease(&db, campaign).await?;
    assert_eq!(first.len(), 1);
    assert!(lease(&db, campaign).await?.is_empty());
    // A failed canary item lets another one go.
    assert!(
        finish_item(
            &db.pool,
            &first[0],
            &ItemOutcome::Failed(ItemError::new("boot failed"))
        )
        .await?
    );
    let second = lease(&db, campaign).await?;
    assert_eq!(second.len(), 1);
    assert!(finish_item(&db.pool, &second[0], &ItemOutcome::Done).await?);
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "a done canary waits for an operator's resume"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nothing_is_leased_from_a_campaign_that_isnt_leasing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = node(&db.pool).await?;
    for seed in [
        CampaignSeed {
            phase: CampaignPhase::Paused,
            ..CampaignSeed::default()
        },
        CampaignSeed {
            kind: CampaignKind::Templates,
            ..CampaignSeed::default()
        },
    ] {
        let campaign = insert_campaign(&db.pool, seed).await?;
        // Items seeded directly: the store refuses sandbox items in a
        // template campaign.
        let sandbox = insert_sandbox(
            &db.pool,
            SubstrateKind::Kubernetes,
            SandboxLifecycle::Stopped,
            Some(node),
        )
        .await?;
        sqlx::query(sql!(
            "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase)
             VALUES ($1, 'sandbox', $2, 'pending')"
        ))
        .bind(campaign)
        .bind(sandbox)
        .execute(&db.pool)
        .await?;
        assert!(lease(&db, campaign).await?.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lease_runs_on_the_database_clock() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    pending_on(&db, campaign, None, 1).await?;
    // Inside a transaction whose now() is a tenth of a second old, a lease
    // on the transaction clock would end before an hour from the lease.
    // Instants in whole microseconds, Postgres's resolution, so the bounds
    // compare exactly.
    let mut caller = db.pool.begin().await?;
    let started = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::int8 FROM pg_sleep(0.1)"
    ))
    .fetch_one(&mut *caller)
    .await?;
    let expires_at = async |caller: &mut sqlx::PgConnection| {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT (extract(epoch FROM lease_expires_at) * 1000000)::int8
             FROM runtime_upgrade_items WHERE campaign_id = $1"
        ))
        .bind(campaign)
        .fetch_one(caller)
        .await
    };
    let clock = async |caller: &mut sqlx::PgConnection| {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::int8"
        ))
        .fetch_one(caller)
        .await
    };
    // Longer than MAX_LEASE: clamped.
    let leased = lease_items(&mut *caller, campaign, Duration::from_secs(10 * 3600), SCAN)
        .await?
        .remove(0);
    let leased_by = clock(&mut caller).await?;
    let expires = expires_at(&mut caller).await?;
    // An hour, the documented bound.
    assert_eq!(MAX_LEASE, Duration::from_secs(3600));
    let hour = 3_600_000_000;
    assert!(
        (started + hour..=leased_by + hour).contains(&expires),
        "lease expires at {expires}us, not an hour after a clock read in [{started}, {leased_by}]us"
    );
    // A renewal is clamped the same way, and writes a new expiry: from a
    // shorter live lease, one that writes nothing stays a minute out.
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() + interval '1 minute'
         WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .execute(&mut *caller)
    .await?;
    let before = clock(&mut caller).await?;
    assert!(renew_lease(&mut *caller, &leased, Duration::from_secs(10 * 3600)).await?);
    let after = clock(&mut caller).await?;
    let renewed = expires_at(&mut caller).await?;
    caller.rollback().await?;
    assert!(
        (before + hour..=after + hour).contains(&renewed),
        "renewal expires at {renewed}us, not an hour after a clock read in [{before}, {after}]us"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_the_lease_holder_renews_or_records_an_outcome() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let sandbox = pending_on(&db, campaign, None, 1).await?[0];
    let held = lease(&db, campaign).await?.remove(0);
    let stranger = LeasedItem {
        lease_token: uuid::Uuid::now_v7(),
        ..held.clone()
    };

    assert!(!renew_lease(&db.pool, &stranger, LEASE).await?);
    assert!(!finish_item(&db.pool, &stranger, &ItemOutcome::Done).await?);
    assert!(renew_lease(&db.pool, &held, LEASE).await?);
    assert!(
        finish_item(
            &db.pool,
            &held,
            &ItemOutcome::Retry(ItemError::new("the sandbox is starting"))
        )
        .await?
    );
    assert_eq!(item_phase(&db.pool, campaign, sandbox).await?, "pending");
    // Released: the old token records nothing more.
    assert!(!finish_item(&db.pool, &held, &ItemOutcome::Done).await?);
    assert!(!renew_lease(&db.pool, &held, LEASE).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_expired_lease_isnt_renewed_but_still_records_its_outcome() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let sandbox = pending_on(&db, campaign, None, 1).await?[0];
    let held = lease(&db, campaign).await?.remove(0);
    lapse_leases(&db.pool, campaign).await?;

    assert!(
        !renew_lease(&db.pool, &held, LEASE).await?,
        "an expired lease is over on the database clock"
    );
    // Not re-driven yet, so the holder's finished work still counts.
    assert!(finish_item(&db.pool, &held, &ItemOutcome::Done).await?);
    assert_eq!(item_phase(&db.pool, campaign, sandbox).await?, "done");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_renewal_in_a_transaction_opened_before_the_lease_lapsed_is_refused() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    pending_on(&db, campaign, None, 1).await?;
    // The caller's transaction starts first, so its now() is from before the
    // lease is granted.
    let mut caller = db.pool.begin().await?;
    sqlx::query(sql!("SELECT now()"))
        .execute(&mut *caller)
        .await?;
    let held = lease(&db, campaign).await?.remove(0);
    // The lease lapses now, after the caller's transaction began.
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items SET lease_expires_at = clock_timestamp()
         WHERE campaign_id = $1 AND phase = 'in_flight'"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;
    let after_caller_began = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT lease_expires_at > now()
         FROM runtime_upgrade_items WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .fetch_one(&mut *caller)
    .await?;
    assert!(
        after_caller_began,
        "the lease lapsed after the caller's transaction began"
    );

    assert!(
        !renew_lease(&mut *caller, &held, LEASE).await?,
        "the lease lapsed before the renewal ran"
    );
    caller.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lease_waits_for_a_renewal_in_an_open_transaction_and_then_skips_the_subject()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let subject = Uuid::now_v7();
    let holder = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(48),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(49),
            ..CampaignSeed::default()
        },
    )
    .await?;
    for target in [holder, campaign] {
        insert_pending(
            &db.pool,
            target,
            SubjectKind::Sandbox,
            &[subject],
            MAX_ATTEMPTS,
        )
        .await?;
    }
    let held = lease(&db, holder).await?.remove(0);
    // The renewal is written inside a transaction that stays open past the
    // committed lease's expiry. The caller's transaction is open before the
    // short deadline is set, so only the renewal itself runs inside it; a
    // slow run that misses it starts over with a fresh transaction.
    let mut attempts = 0;
    let caller = loop {
        attempts += 1;
        let mut caller = db.pool.begin().await?;
        lapse_in(&db.pool, holder, 500).await?;
        if renew_lease(&mut *caller, &held, LEASE).await? {
            break caller;
        }
        caller.rollback().await?;
        assert!(
            attempts < 5,
            "the renewal never ran before the committed lease expired"
        );
    };
    wait_for_committed_lapse(&db.pool, holder).await?;

    let pool = db.pool.clone();
    let leasing = tokio::spawn(async move {
        let mut conn = pool.acquire().await.map_err(DbError::from)?;
        lease_items(&mut *conn, campaign, LEASE, SCAN).await
    });
    wait_for_lock_waiter(&db.pool).await?;
    caller.commit().await?;
    let leased = leasing.await??;
    assert!(
        leased.is_empty(),
        "the lease saw the committed renewal: {leased:?}"
    );
    Ok(())
}

/// Set the committed leases of `campaign` to lapse `millis` from now on the
/// database clock.
async fn lapse_in(pool: &PgPool, campaign: CampaignId, millis: i32) -> TestResult {
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() + make_interval(secs => $2 / 1000.0)
         WHERE campaign_id = $1 AND phase = 'in_flight'"
    ))
    .bind(campaign)
    .bind(millis)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lease_that_lapses_inside_a_callers_transaction_counts_as_lapsed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let subject = Uuid::now_v7();
    let aborted = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(56),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(57),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let stuck = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(58),
            ..CampaignSeed::default()
        },
    )
    .await?;
    for target in [aborted, campaign] {
        insert_pending(
            &db.pool,
            target,
            SubjectKind::Sandbox,
            &[subject],
            MAX_ATTEMPTS,
        )
        .await?;
    }
    insert_pending(
        &db.pool,
        stuck,
        SubjectKind::Sandbox,
        &[Uuid::now_v7()],
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(lease(&db, aborted).await?.len(), 1);
    assert_eq!(lease(&db, stuck).await?.len(), 1);

    // The caller's transaction starts first, and both leases are then set
    // to lapse after its now().
    let mut caller = db.pool.begin().await?;
    let started = sqlx::query_scalar::<_, f64>(sql!("SELECT extract(epoch FROM now())::float8"))
        .fetch_one(&mut *caller)
        .await?;
    for target in [aborted, stuck] {
        lapse_in(&db.pool, target, 300).await?;
    }
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'aborted', finished_at = now() WHERE id = $1"
    ))
    .bind(aborted)
    .execute(&db.pool)
    .await?;
    let earliest = sqlx::query_scalar::<_, f64>(sql!(
        "SELECT extract(epoch FROM min(lease_expires_at))::float8
         FROM runtime_upgrade_items
         WHERE campaign_id = ANY($1) AND phase = 'in_flight'"
    ))
    .bind([aborted, stuck])
    .fetch_one(&db.pool)
    .await?;
    assert!(
        earliest > started,
        "the leases lapse after the caller's transaction starts"
    );
    wait_for_committed_lapse(&db.pool, aborted).await?;
    wait_for_committed_lapse(&db.pool, stuck).await?;

    assert_eq!(
        lease_items(&mut *caller, campaign, LEASE, SCAN)
            .await?
            .len(),
        1,
        "the aborted campaign's lapsed lease still held the subject"
    );
    assert_eq!(
        redrive_expired(&mut *caller, stuck, MAX_ATTEMPTS).await?,
        RedriveReport {
            requeued: 1,
            failed: 0
        },
        "the lapsed lease wasn't redriven"
    );
    assert_eq!(
        release_lapsed_in_finished(&mut *caller).await?,
        1,
        "the finished campaign's lapsed lease wasn't released"
    );
    caller.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_renewal_that_waits_on_a_locked_item_past_its_lease_isnt_renewed() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let subject = Uuid::now_v7();
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(59),
            ..CampaignSeed::default()
        },
    )
    .await?;
    insert_pending(
        &db.pool,
        campaign,
        SubjectKind::Sandbox,
        &[subject],
        MAX_ATTEMPTS,
    )
    .await?;
    let held = lease(&db, campaign).await?.remove(0);
    lapse_in(&db.pool, campaign, 300).await?;
    // Recording the in-flight subject again changes nothing, but its
    // conflict check locks the item row until this transaction ends.
    let mut locker = db.pool.begin().await?;
    assert_eq!(
        insert_pending(
            &mut *locker,
            campaign,
            SubjectKind::Sandbox,
            &[subject],
            MAX_ATTEMPTS,
        )
        .await?,
        0
    );

    let pool = db.pool.clone();
    let renewing = tokio::spawn(async move { renew_lease(&pool, &held, LEASE).await });
    wait_for_lock_waiter(&db.pool).await?;
    wait_for_committed_lapse(&db.pool, campaign).await?;
    locker.commit().await?;
    assert!(
        !renewing.await??,
        "the renewal extended a lease that lapsed while it waited"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failure_committed_while_a_lease_waits_counts_against_the_budget() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(54),
            per_cluster: 2,
            failure_budget: 1,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 1).await?;
    let failing = lease(&db, campaign).await?.remove(0);
    pending_on(&db, campaign, None, 1).await?;
    // Another campaign's open renewal holds the handoff lock, so the lease
    // below reads its in-flight count and then waits.
    let other = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(55),
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, other, None, 1).await?;
    let renewing = lease(&db, other).await?.remove(0);
    let mut caller = db.pool.begin().await?;
    assert!(renew_lease(&mut *caller, &renewing, LEASE).await?);

    let pool = db.pool.clone();
    let leasing = tokio::spawn(async move {
        let mut conn = pool.acquire().await.map_err(DbError::from)?;
        lease_items(&mut *conn, campaign, LEASE, SCAN).await
    });
    wait_for_lock_waiter(&db.pool).await?;
    finish_item(
        &db.pool,
        &failing,
        &ItemOutcome::Failed(ItemError::new("boot failed")),
    )
    .await?;
    caller.commit().await?;
    let leased = leasing.await??;
    assert!(
        leased.is_empty(),
        "the failure that freed the slot reached the budget: {leased:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn renewals_of_one_kind_dont_wait_on_each_other() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let mut held = Vec::new();
    for byte in [52, 53] {
        let campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                from: environment(byte),
                ..CampaignSeed::default()
            },
        )
        .await?;
        pending_on(&db, campaign, None, 1).await?;
        held.push(lease(&db, campaign).await?.remove(0));
    }
    let mut first = db.pool.begin().await?;
    assert!(renew_lease(&mut *first, &held[0], LEASE).await?);
    // Still inside the first renewal's transaction, so it holds its locks.
    let second = tokio::time::timeout(
        Duration::from_secs(5),
        renew_lease(&db.pool, &held[1], LEASE),
    )
    .await;
    first.rollback().await?;
    assert!(
        second.expect("the second renewal didn't wait for the first")?,
        "the second renewal renewed"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completion_inside_a_transaction_older_than_the_campaign_succeeds() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    // The caller's transaction starts first, so its now() is from before the
    // campaign's created_at.
    let mut caller = db.pool.begin().await?;
    sqlx::query(sql!("SELECT now()"))
        .execute(&mut *caller)
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;

    assert!(complete_if_drained(&mut *caller, campaign).await?);
    caller.commit().await?;
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Completed);
    assert!(record.finished_at.expect("finished") >= record.created_at);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn outcomes_store_their_phase_and_error() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 4).await?;
    let leased = lease(&db, campaign).await?;
    let long = "x".repeat(ItemError::MAX_CHARS + 10);
    let outcomes = [
        (ItemOutcome::Done, "done", None),
        (
            ItemOutcome::Skipped(ItemError::new("deleted")),
            "skipped",
            Some("deleted".to_owned()),
        ),
        (
            ItemOutcome::Failed(ItemError::new(&long)),
            "failed",
            Some("x".repeat(ItemError::MAX_CHARS)),
        ),
        (ItemOutcome::Retry(ItemError::new("")), "pending", None),
    ];
    assert_eq!(leased.len(), outcomes.len(), "every outcome gets an item");
    for (item, (outcome, phase, error)) in leased.iter().zip(outcomes) {
        assert!(finish_item(&db.pool, item, &outcome).await?);
        let stored = sqlx::query_as::<_, (String, Option<String>, bool, i64)>(sql!(
            "SELECT phase, last_error, lease_token IS NULL AND lease_expires_at IS NULL,
                    attempts::bigint
             FROM runtime_upgrade_items WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(campaign)
        .bind(item.subject_id)
        .fetch_one(&db.pool)
        .await?;
        // Every outcome but a release keeps the lease's attempt.
        assert_eq!(stored, (phase.to_owned(), error, true, 1));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_release_gives_its_attempt_back_and_a_retry_keeps_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 2,
            per_cluster: 2,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 2).await?;
    let leased = lease(&db, campaign).await?;
    assert!(leased.iter().all(|item| item.attempts == 1));

    let released = ItemOutcome::Release(ItemError::new("the sandbox is starting"));
    let retried = ItemOutcome::Retry(ItemError::new("the node is unavailable"));
    assert!(finish_item(&db.pool, &leased[0], &released).await?);
    assert!(finish_item(&db.pool, &leased[1], &retried).await?);

    let attempts = |sandbox: Uuid| {
        sqlx::query_as::<_, (String, i64, Option<String>)>(sql!(
            "SELECT phase, attempts, last_error FROM runtime_upgrade_items
             WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(campaign)
        .bind(sandbox)
        .fetch_one(&db.pool)
    };
    assert_eq!(
        attempts(leased[0].subject_id).await?,
        (
            "pending".to_owned(),
            0,
            Some("the sandbox is starting".to_owned())
        )
    );
    assert_eq!(
        attempts(leased[1].subject_id).await?,
        (
            "pending".to_owned(),
            1,
            Some("the node is unavailable".to_owned())
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lapsed_leases_are_redriven_until_their_attempts_run_out() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 2,
            per_cluster: 2,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let sandboxes = pending_on(&db, campaign, None, 2).await?;
    let first = lease(&db, campaign).await?;
    assert_eq!(first.len(), 2);
    // Only lapsed leases are re-driven.
    assert_eq!(
        redrive_expired(&db.pool, campaign, 2).await?,
        RedriveReport::default()
    );

    lapse_leases(&db.pool, campaign).await?;
    assert_eq!(
        redrive_expired(&db.pool, campaign, 2).await?,
        RedriveReport {
            requeued: 2,
            failed: 0
        }
    );
    // The lapsed worker can't record an outcome over the re-drive.
    assert!(!finish_item(&db.pool, &first[0], &ItemOutcome::Done).await?);
    let second = lease(&db, campaign).await?;
    assert!(second.iter().all(|item| item.attempts == 2));
    // The re-drive's lease is the one that counts now.
    assert!(finish_item(&db.pool, &second[0], &ItemOutcome::Done).await?);

    lapse_leases(&db.pool, campaign).await?;
    assert_eq!(
        redrive_expired(&db.pool, campaign, 2).await?,
        RedriveReport {
            requeued: 0,
            failed: 1
        }
    );
    let last_error = sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(second[1].subject_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(last_error.as_deref(), Some(super::items::LEASE_EXPIRED));
    let mut phases = vec![
        item_phase(&db.pool, campaign, sandboxes[0]).await?,
        item_phase(&db.pool, campaign, sandboxes[1]).await?,
    ];
    phases.sort();
    assert_eq!(phases, ["done", "failed"]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_pauses_itself_when_failures_reach_its_budget() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 3,
            per_cluster: 3,
            failure_budget: 2,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 3).await?;
    let leased = lease(&db, campaign).await?;
    let failed = ItemOutcome::Failed(ItemError::new("boot failed"));
    finish_item(&db.pool, &leased[0], &failed).await?;
    // Skips and retries don't count against the budget.
    finish_item(
        &db.pool,
        &leased[1],
        &ItemOutcome::Skipped(ItemError::new("gone")),
    )
    .await?;
    assert!(!pause_if_over_budget(&Db::from(db.pool.clone()), campaign).await?);

    finish_item(&db.pool, &leased[2], &failed).await?;
    assert!(pause_if_over_budget(&Db::from(db.pool.clone()), campaign).await?);
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        ("paused".to_owned(), false)
    );
    // Already paused: nothing more to do.
    assert!(!pause_if_over_budget(&Db::from(db.pool.clone()), campaign).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_operator_phase_isnt_overwritten_by_the_budget() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            failure_budget: 1,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 1).await?;
    let leased = lease(&db, campaign).await?;
    finish_item(
        &db.pool,
        &leased[0],
        &ItemOutcome::Failed(ItemError::new("x")),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'aborted', finished_at = now() WHERE id = $1"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;

    assert!(!pause_if_over_budget(&Db::from(db.pool.clone()), campaign).await?);
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        ("aborted".to_owned(), true)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_a_drained_rolling_campaign_completes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let canary = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            ..CampaignSeed::default()
        },
    )
    .await?;
    // The canary's one item is done, so it waits for the operator's resume.
    pending_on(&db, canary, None, 1).await?;
    let canary_item = lease(&db, canary).await?;
    finish_item(&db.pool, &canary_item[0], &ItemOutcome::Done).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(9),
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 1).await?;
    let mut conn = db.pool.acquire().await?;

    assert!(
        !complete_if_drained(&mut *conn, canary).await?,
        "a canary waits for resume"
    );
    assert!(
        !complete_if_drained(&mut *conn, campaign).await?,
        "an item is pending"
    );
    let leased = lease(&db, campaign).await?;
    assert!(
        !complete_if_drained(&mut *conn, campaign).await?,
        "an item is in flight"
    );
    finish_item(
        &db.pool,
        &leased[0],
        &ItemOutcome::Failed(ItemError::new("x")),
    )
    .await?;

    assert!(complete_if_drained(&mut *conn, campaign).await?);
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        ("completed".to_owned(), true)
    );
    assert!(!complete_if_drained(&mut *conn, campaign).await?);
    Ok(())
}

/// Seed `campaign`'s item for `sandbox` as skipped, `attempts` leases in.
async fn skipped(pool: &PgPool, campaign: CampaignId, sandbox: Uuid, attempts: i64) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase, attempts)
         VALUES ($1, 'sandbox', $2, 'skipped', $3)"
    ))
    .bind(campaign)
    .bind(sandbox)
    .bind(attempts)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retiring_skipped_items_leaves_other_sandboxes_phases_and_campaigns_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let other = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(3),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let pending = pending_on(&db, campaign, None, 1).await?[0];
    let sandbox = Uuid::now_v7();
    let unlisted = Uuid::now_v7();
    skipped(&db.pool, campaign, sandbox, 1).await?;
    skipped(&db.pool, campaign, unlisted, 1).await?;
    skipped(&db.pool, other, sandbox, 1).await?;

    assert_eq!(
        retire_skipped(
            &db.pool,
            campaign,
            SubjectKind::Sandbox,
            &[pending, sandbox],
        )
        .await?,
        1
    );
    assert_eq!(
        retire_skipped(&db.pool, campaign, SubjectKind::Sandbox, &[sandbox],).await?,
        0,
        "already retired"
    );
    let attempts = |campaign: CampaignId, sandbox: Uuid| {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT attempts FROM runtime_upgrade_items WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(campaign)
        .bind(sandbox)
        .fetch_one(&db.pool)
    };
    assert_eq!(attempts(campaign, sandbox).await?, RETIRED_ATTEMPTS as i64);
    assert_eq!(attempts(other, sandbox).await?, 1);
    assert_eq!(attempts(campaign, unlisted).await?, 1, "not given, so kept");
    assert_eq!(attempts(campaign, pending).await?, 0);
    assert_eq!(item_phase(&db.pool, campaign, pending).await?, "pending");

    // A later, higher limit doesn't bring a retired item back.
    for max_attempts in [MAX_ATTEMPTS + 1, u64::MAX] {
        assert_eq!(
            insert_pending(
                &db.pool,
                campaign,
                SubjectKind::Sandbox,
                &[sandbox],
                max_attempts
            )
            .await?,
            0
        );
        assert_eq!(item_phase(&db.pool, campaign, sandbox).await?, "skipped");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_items_on_reachable_nodes_with_environment_evidence_are_leased() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 5,
            per_cluster: 10,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let fresh = SnapshotSeed::default();
    let mut expected = Vec::new();
    let mut gated = Vec::new();
    // In subject order, gated items first, so one leased after them shows
    // the scan limit counts only what may be leased.
    let mut seeded = Vec::new();
    for (snapshot, leases) in [
        (
            Some(SnapshotSeed {
                reachable: false,
                ..fresh
            }),
            false,
        ),
        (
            Some(SnapshotSeed {
                stale_in_secs: -1.0,
                ..fresh
            }),
            false,
        ),
        (None, false),
        (Some(fresh), true),
    ] {
        let node = NodeId::new();
        insert_ready_worker_node(&db.pool, node).await?;
        if let Some(snapshot) = snapshot {
            upsert_snapshot(&db.pool, node, snapshot).await?;
        }
        seeded.push((node, leases));
    }
    for (node, leases) in seeded {
        let sandbox = pending_on(&db, campaign, Some(node), 1).await?[0];
        if leases {
            expected.push(sandbox);
        } else {
            gated.push(sandbox);
        }
    }
    // No published node: the upgrade refuses it itself.
    expected.extend(pending_on(&db, campaign, None, 1).await?);
    expected.sort();

    let mut conn = db.pool.acquire().await?;
    let mut leased: Vec<Uuid> = lease_items(&mut *conn, campaign, LEASE, 2)
        .await?
        .into_iter()
        .map(|item| item.subject_id)
        .collect();
    leased.sort();

    assert_eq!(
        leased, expected,
        "an unreachable node, a stale one and one with no status get nothing"
    );
    for sandbox in gated {
        assert_eq!(item_phase(&db.pool, campaign, sandbox).await?, "pending");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_gated_node_is_leased_from_once_it_qualifies_again() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    let unreachable = SnapshotSeed {
        reachable: false,
        ..SnapshotSeed::default()
    };
    upsert_snapshot(&db.pool, node, unreachable).await?;
    let sandbox = pending_on(&db, campaign, Some(node), 1).await?[0];

    assert!(lease(&db, campaign).await?.is_empty());
    upsert_snapshot(&db.pool, node, SnapshotSeed::default()).await?;
    let leased = lease(&db, campaign).await?;
    assert_eq!(
        leased
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        vec![sandbox]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_over_its_budget_doesnt_complete_or_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 2,
            per_cluster: 2,
            failure_budget: 1,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, campaign, None, 2).await?;
    let first = lease_items(&db.pool, campaign, LEASE, 1).await?;
    finish_item(
        &db.pool,
        &first[0],
        &ItemOutcome::Failed(ItemError::new("x")),
    )
    .await?;

    // Not yet paused, but the budget is spent: nothing more goes out.
    assert!(lease(&db, campaign).await?.is_empty());
    assert_eq!(campaign_phase(&db.pool, campaign).await?.0, "rolling");
    // Drained of everything but the pending item, still over budget.
    sqlx::query(sql!(
        "DELETE FROM runtime_upgrade_items WHERE campaign_id = $1 AND phase = 'pending'"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;
    assert!(!complete_if_drained(&db.pool, campaign).await?);
    assert!(pause_if_over_budget(&Db::from(db.pool.clone()), campaign).await?);
    Ok(())
}

fn new_campaign(from: u8, kind: CampaignKind) -> NewCampaign {
    NewCampaign {
        from: environment(from),
        to: environment(200),
        kind,
        canary: false,
        max_in_flight_per_node: 1,
        max_in_flight_per_cluster: 4,
        failure_budget: 2,
        include_running: false,
        reason: "move off a blocked environment".to_owned(),
        created_by: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_unfinished_campaign_per_kind_and_source() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let first = create_campaign(&db.pool, &new_campaign(10, CampaignKind::Sandboxes)).await?;
    let second = create_campaign(&db.pool, &new_campaign(10, CampaignKind::Sandboxes)).await;
    assert!(
        matches!(&second, Err(CreateCampaignError::AlreadyUnfinished(id)) if *id == first),
        "{second:?}"
    );
    // Another kind or another source is free.
    create_campaign(&db.pool, &new_campaign(10, CampaignKind::Templates)).await?;
    create_campaign(&db.pool, &new_campaign(11, CampaignKind::Sandboxes)).await?;

    // Once the first finishes, the source is free again.
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'aborted', finished_at = now() WHERE id = $1"
    ))
    .bind(first)
    .execute(&db.pool)
    .await?;
    let next = create_campaign(
        &db.pool,
        &NewCampaign {
            canary: true,
            ..new_campaign(10, CampaignKind::Sandboxes)
        },
    )
    .await?;
    let campaigns = list_unfinished(&db.pool).await?;
    let created = campaigns
        .iter()
        .find(|campaign| campaign.id == next)
        .expect("listed");
    assert_eq!(created.phase, CampaignPhase::Canary);
    assert_eq!(
        (
            created.max_in_flight_per_node,
            created.max_in_flight_per_cluster,
            created.failure_budget
        ),
        (1, 4, 2)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn of_two_concurrent_creates_for_one_source_exactly_one_succeeds() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = new_campaign(20, CampaignKind::Sandboxes);
    // Hold the first create's transaction open across the second, so the
    // second runs while the first hasn't committed.
    let mut first_tx = db.pool.begin().await?;
    let first = create_campaign(&mut *first_tx, &campaign).await?;
    let second = create_campaign(&db.pool, &campaign);
    tokio::pin!(second);
    // The second blocks on the advisory lock the first still holds.
    tokio::select! {
        second = &mut second => panic!("the second create didn't wait for the first: {second:?}"),
        waited = wait_for_lock_waiter(&db.pool) => waited?,
    }
    first_tx.commit().await?;
    let second = second.await;
    assert!(
        matches!(&second, Err(CreateCampaignError::AlreadyUnfinished(id)) if *id == first),
        "the second waited on the lock and then saw the first: {second:?}"
    );
    let unfinished = list_unfinished(&db.pool).await?;
    assert_eq!(
        unfinished
            .iter()
            .filter(|c| c.from == environment(20))
            .count(),
        1
    );
    Ok(())
}

#[test]
fn a_campaign_with_impossible_pacing_or_reason_is_refused_before_the_database() {
    for (campaign, why, refusal) in [
        (
            NewCampaign {
                max_in_flight_per_cluster: 0,
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "no cluster room",
            "max_in_flight_per_cluster must be at least 1",
        ),
        (
            NewCampaign {
                to: environment(10),
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "same source and target",
            "the source and target must differ",
        ),
        (
            NewCampaign {
                max_in_flight_per_node: 0,
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "no per-node room",
            "max_in_flight_per_node must be at least 1 and at most max_in_flight_per_cluster",
        ),
        (
            NewCampaign {
                max_in_flight_per_node: 5,
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "per-node above per-cluster",
            "max_in_flight_per_node must be at least 1 and at most max_in_flight_per_cluster",
        ),
        (
            NewCampaign {
                failure_budget: 0,
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "no budget",
            "failure_budget must be at least 1",
        ),
        (
            NewCampaign {
                include_running: true,
                ..new_campaign(10, CampaignKind::Templates)
            },
            "include_running on a template campaign",
            "include_running applies only to sandbox campaigns",
        ),
        (
            NewCampaign {
                reason: String::new(),
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "empty reason",
            "the reason must be 1 to 1024 characters with no NUL",
        ),
        (
            NewCampaign {
                reason: "x".repeat(1025),
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "long reason",
            "the reason must be 1 to 1024 characters with no NUL",
        ),
        (
            NewCampaign {
                reason: "a\0b".to_owned(),
                ..new_campaign(10, CampaignKind::Sandboxes)
            },
            "NUL in the reason",
            "the reason must be 1 to 1024 characters with no NUL",
        ),
    ] {
        assert_eq!(super::create::validate(&campaign), Err(refusal), "{why}");
    }
    super::create::validate(&NewCampaign {
        reason: "é".repeat(1024),
        ..new_campaign(10, CampaignKind::Sandboxes)
    })
    .expect("a 1024-character reason fits");
    super::create::validate(&NewCampaign {
        max_in_flight_per_node: 4,
        ..new_campaign(10, CampaignKind::Sandboxes)
    })
    .expect("a node may take the whole cluster's room");
}

#[tokio::test(flavor = "current_thread")]
async fn a_verb_that_fails_after_locking_the_campaign_releases_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    pending_on(&db, campaign, None, 1).await?;
    // Leasing fails after the campaign is locked, when it moves the item.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_lease() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'leases are down'; END $$;
         CREATE TRIGGER refuse_lease BEFORE UPDATE ON runtime_upgrade_items
             FOR EACH ROW EXECUTE FUNCTION refuse_lease();"
    ))
    .execute(&db.pool)
    .await?;
    let mut held = db.pool.acquire().await?;

    let refused = lease_items(&mut *held, campaign, LEASE, SCAN)
        .await
        .expect_err("the trigger refuses the lease");
    // The trigger fires on the item update, after the campaign is locked.
    let LeaseError::Database(refused) = refused else {
        panic!("{refused:?}");
    };
    let refused = sqlx::Error::from(refused);
    assert!(
        matches!(
            &refused,
            sqlx::Error::Database(error) if error.message() == "leases are down"
        ),
        "{refused:?}"
    );

    // `held` is still checked out and unused: its transaction must already
    // have ended, or this lock waits on it.
    let locked = sqlx::query_scalar::<_, i32>(sql!(
        "SELECT 1 FROM runtime_upgrade_campaigns WHERE id = $1 FOR UPDATE NOWAIT"
    ))
    .bind(campaign)
    .fetch_one(&db.pool)
    .await;
    locked.unwrap_or_else(|error| panic!("the campaign is still locked: {error}"));
    drop(held);
    Ok(())
}

#[test]
fn an_item_error_fits_the_column() {
    assert_eq!(ItemError::new("").as_deref(), None);
    assert_eq!(ItemError::new("\0").as_deref(), None);
    assert_eq!(ItemError::new("a\0b").as_deref(), Some("ab"));
    let long = "é".repeat(ItemError::MAX_CHARS + 1);
    assert_eq!(
        ItemError::new(&long)
            .as_deref()
            .map(|error| error.chars().count()),
        Some(ItemError::MAX_CHARS)
    );
}

#[test]
fn vocabularies_round_trip_and_refuse_strangers() {
    for phase in CampaignPhase::ALL {
        assert_eq!(phase.as_str().parse::<CampaignPhase>(), Ok(*phase));
    }
    for phase in ItemPhase::ALL {
        assert_eq!(phase.as_str().parse::<ItemPhase>(), Ok(*phase));
    }
    for kind in CampaignKind::ALL {
        assert_eq!(kind.as_str().parse::<CampaignKind>(), Ok(*kind));
    }
    for kind in SubjectKind::ALL {
        assert_eq!(kind.as_str().parse::<SubjectKind>(), Ok(*kind));
    }
    assert_eq!(
        "Rolling".parse::<CampaignPhase>(),
        Err(UnknownValue {
            column: "campaign phase",
            value: "Rolling".to_owned(),
        })
    );
    assert_eq!(
        CampaignPhase::ALL
            .iter()
            .filter(|phase| phase.leases())
            .collect::<Vec<_>>(),
        [&CampaignPhase::Canary, &CampaignPhase::Rolling]
    );
}

#[test]
fn counts_and_ids_read_back_as_stored() {
    let counts = ItemCounts {
        pending: 1,
        in_flight: 2,
        done: 3,
        failed: 4,
        skipped: 5,
    };
    assert_eq!(
        ItemPhase::ALL
            .iter()
            .map(|phase| counts.get(*phase))
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    let id = uuid::Uuid::now_v7();
    assert_eq!(CampaignId(id).to_string(), id.to_string());
}
