use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    Digest, EnvironmentReference, NodeId, SandboxId, SandboxLifecycle, SubstrateKind,
};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDb, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::test_support::{CampaignSeed, insert_campaign};
use super::*;

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
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
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

async fn node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    Ok(node)
}

/// `count` stopped Kubernetes sandboxes published by `node`, recorded as
/// pending items of `campaign`.
async fn pending_on(
    db: &TestDb,
    campaign: CampaignId,
    node: Option<NodeId>,
    count: usize,
) -> TestResult<Vec<SandboxId>> {
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
    insert_pending_sandboxes(&mut *conn, campaign, &sandboxes, MAX_ATTEMPTS).await?;
    Ok(sandboxes)
}

async fn lease(db: &TestDb, campaign: CampaignId) -> TestResult<Vec<LeasedItem>> {
    let mut conn = db.pool.acquire().await?;
    Ok(lease_sandbox_items(&mut *conn, campaign, LEASE, SCAN).await?)
}

async fn item_phase(pool: &PgPool, campaign: CampaignId, sandbox: SandboxId) -> TestResult<String> {
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
async fn candidates_are_published_sandboxes_of_the_substrate_and_lifecycles_without_items()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    let k8s = SubstrateKind::Kubernetes;
    let mut stopped = Vec::new();
    for _ in 0..3 {
        stopped.push(insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?);
    }
    stopped.sort();
    let started = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Started, Some(node)).await?;
    // Out of scope: another substrate, another lifecycle, or never published.
    insert_sandbox(
        &db.pool,
        SubstrateKind::Space,
        SandboxLifecycle::Stopped,
        Some(node),
    )
    .await?;
    insert_sandbox(&db.pool, k8s, SandboxLifecycle::Suspended, Some(node)).await?;
    insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, None).await?;

    let stopped_only = [SandboxLifecycle::Stopped];
    let page = sandbox_candidates(
        &db.pool,
        campaign,
        k8s,
        &stopped_only,
        None,
        2,
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(
        page.iter()
            .map(|candidate| candidate.sandbox_id)
            .collect::<Vec<_>>(),
        stopped[..2]
    );
    assert_eq!(page[0].runtime_version.as_deref(), Some("1.15.1"));
    assert_eq!(
        page[0].execution_metadata.as_deref(),
        Some(&serde_json::json!({ "environment": environment(0).to_string() }))
    );
    let rest = sandbox_candidates(
        &db.pool,
        campaign,
        k8s,
        &stopped_only,
        Some(stopped[1]),
        2,
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(
        rest.iter()
            .map(|candidate| candidate.sandbox_id)
            .collect::<Vec<_>>(),
        stopped[2..]
    );

    // A sandbox the campaign already has is no longer a candidate.
    let mut conn = db.pool.acquire().await?;
    insert_pending_sandboxes(&mut *conn, campaign, &stopped[..1], MAX_ATTEMPTS).await?;
    let with_running = [SandboxLifecycle::Stopped, SandboxLifecycle::Started];
    let mut all = sandbox_candidates(
        &db.pool,
        campaign,
        k8s,
        &with_running,
        None,
        10,
        MAX_ATTEMPTS,
    )
    .await?
    .into_iter()
    .map(|candidate| candidate.sandbox_id)
    .collect::<Vec<_>>();
    all.sort();
    let mut expected = vec![stopped[1], stopped[2], started];
    expected.sort();
    assert_eq!(all, expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidates_bound_to_another_environment_are_left_out() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    let k8s = SubstrateKind::Kubernetes;
    let on_source = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?;
    let mut by_metadata = Vec::new();
    for metadata in [
        serde_json::json!({ "environment": environment(9).to_string() }),
        // Not a string: unbound, so the caller attributes it.
        serde_json::json!({ "environment": 7 }),
        serde_json::json!({}),
    ] {
        let sandbox = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_observed_states SET execution_metadata = $2 WHERE sandbox_id = $1"
        ))
        .bind(sandbox)
        .bind(metadata)
        .execute(&db.pool)
        .await?;
        by_metadata.push(sandbox);
    }

    let mut found: Vec<SandboxId> = sandbox_candidates(
        &db.pool,
        campaign,
        k8s,
        &[SandboxLifecycle::Stopped],
        None,
        10,
        MAX_ATTEMPTS,
    )
    .await?
    .into_iter()
    .map(|candidate| candidate.sandbox_id)
    .collect();
    found.sort();
    let mut expected = vec![on_source, by_metadata[1], by_metadata[2]];
    expected.sort();
    assert_eq!(found, expected);
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
    let sandboxes = [SandboxId::new(), SandboxId::new()];
    let mut conn = db.pool.acquire().await?;

    assert_eq!(
        insert_pending_sandboxes(&mut *conn, campaign, &sandboxes, MAX_ATTEMPTS).await?,
        2
    );
    assert_eq!(
        insert_pending_sandboxes(&mut *conn, campaign, &sandboxes, MAX_ATTEMPTS).await?,
        0
    );
    assert_eq!(
        insert_pending_sandboxes(&mut *conn, templates, &sandboxes, MAX_ATTEMPTS).await?,
        0
    );
    assert_eq!(
        insert_pending_sandboxes(&mut *conn, aborted, &sandboxes, MAX_ATTEMPTS).await?,
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
async fn a_skipped_item_with_leases_left_is_found_again_and_made_pending() -> TestResult {
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
    let outcome_of = |sandbox: SandboxId| match sandboxes.iter().position(|id| *id == sandbox) {
        Some(0) => ItemOutcome::Skipped(ItemError::new("the sandbox had started")),
        Some(1) => ItemOutcome::Done,
        Some(2) => ItemOutcome::Failed(ItemError::new("the boot failed")),
        _ => ItemOutcome::Retry(ItemError::new("busy")),
    };
    for item in &leased {
        finish_item(&db.pool, item, &outcome_of(item.sandbox_id)).await?;
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

    // Only the skipped sandbox with leases left is a candidate again; the
    // others keep their items.
    let found: Vec<SandboxId> = sandbox_candidates(
        &db.pool,
        campaign,
        SubstrateKind::Kubernetes,
        &[SandboxLifecycle::Stopped],
        None,
        10,
        MAX_ATTEMPTS,
    )
    .await?
    .into_iter()
    .map(|candidate| candidate.sandbox_id)
    .collect();
    assert_eq!(found, vec![sandboxes[0]]);

    // Recording all five again revives only the skipped one with leases
    // left.
    let mut all = sandboxes.clone();
    all.push(exhausted);
    assert_eq!(
        insert_pending_sandboxes(&db.pool, campaign, &all, MAX_ATTEMPTS).await?,
        1
    );
    let rows = sqlx::query_as::<_, (SandboxId, String, i64, Option<String>)>(sql!(
        "SELECT subject_id, phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .fetch_all(&db.pool)
    .await?;
    let row = |sandbox: SandboxId| {
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
            .any(|item| item.sandbox_id == unpublished[0] && item.node_id.is_none())
    );
    assert_eq!(leased.len(), 4);
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "every node is at its cap"
    );
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
    for leased in lease_sandbox_items(&mut *conn, campaign, LEASE, 2).await? {
        assert!(sandboxes[..2].contains(&leased.sandbox_id));
        finish_item(&db.pool, &leased, &ItemOutcome::Done).await?;
    }

    let leased = lease_sandbox_items(&mut *conn, campaign, LEASE, 2).await?;
    let mut leased: Vec<_> = leased.iter().map(|item| item.sandbox_id).collect();
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
    let mut conn = db.pool.acquire().await?;
    // Longer than MAX_LEASE: clamped.
    lease_sandbox_items(&mut *conn, campaign, Duration::from_secs(10 * 3600), SCAN).await?;

    let remaining = sqlx::query_scalar::<_, f64>(sql!(
        "SELECT extract(epoch FROM lease_expires_at - now())::float8
         FROM runtime_upgrade_items WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .fetch_one(&db.pool)
    .await?;
    // An hour, the documented bound.
    assert_eq!(MAX_LEASE, Duration::from_secs(3600));
    assert!(
        remaining <= 3600.0 && remaining > 3540.0,
        "lease runs {remaining}s from the database's now()"
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
    for (item, (outcome, phase, error)) in leased.iter().zip(outcomes) {
        assert!(finish_item(&db.pool, item, &outcome).await?);
        let stored = sqlx::query_as::<_, (String, Option<String>, bool)>(sql!(
            "SELECT phase, last_error, lease_token IS NULL AND lease_expires_at IS NULL
             FROM runtime_upgrade_items WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(campaign)
        .bind(item.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(stored, (phase.to_owned(), error, true));
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

    let attempts = |sandbox: SandboxId| {
        sqlx::query_as::<_, (String, i64, Option<String>)>(sql!(
            "SELECT phase, attempts, last_error FROM runtime_upgrade_items
             WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(campaign)
        .bind(sandbox)
        .fetch_one(&db.pool)
    };
    assert_eq!(
        attempts(leased[0].sandbox_id).await?,
        (
            "pending".to_owned(),
            0,
            Some("the sandbox is starting".to_owned())
        )
    );
    assert_eq!(
        attempts(leased[1].sandbox_id).await?,
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
    .bind(second[1].sandbox_id)
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
    assert!(!pause_if_over_budget(&db.pool, campaign).await?);

    finish_item(&db.pool, &leased[2], &failed).await?;
    assert!(pause_if_over_budget(&db.pool, campaign).await?);
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        ("paused".to_owned(), false)
    );
    // Already paused: nothing more to do.
    assert!(!pause_if_over_budget(&db.pool, campaign).await?);
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

    assert!(!pause_if_over_budget(&db.pool, campaign).await?);
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
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
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

    lease_sandbox_items(&mut *held, campaign, LEASE, SCAN)
        .await
        .expect_err("the trigger refuses the lease");

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
