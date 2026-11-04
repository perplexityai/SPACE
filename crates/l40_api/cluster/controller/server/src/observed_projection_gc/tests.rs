use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;
use std::sync::{Arc, Mutex};

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use sqlx::PgPool;
use uuid::Uuid;

use pplx_space_test_env::TestResult;

#[derive(Clone)]
struct FakeSink {
    seen: Arc<Mutex<Option<PruneRequest>>>,
    report: TickReport,
}

impl ProjectionGcSink for FakeSink {
    async fn prune_observed_projections(
        &self,
        request: PruneRequest,
    ) -> Result<TickReport, sqlx::Error> {
        *self.seen.lock().unwrap() = Some(request);
        Ok(self.report)
    }
}

#[test]
fn default_config_is_self_healing() {
    let config = ReconcilerConfig::default();

    assert!(config.enabled);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.max_per_tick, DEFAULT_MAX_PER_TICK);
    assert_eq!(
        config.stale_suspended_age.get(),
        Duration::from_secs(DEFAULT_STALE_SUSPENDED_AGE_SECS)
    );
    assert_eq!(
        config.orphan_age.get(),
        Duration::from_secs(DEFAULT_ORPHAN_AGE_SECS)
    );
    assert_eq!(config.scan_window, DEFAULT_SCAN_WINDOW);
    assert_eq!(config.windows_per_tick, DEFAULT_WINDOWS_PER_TICK);
}

#[tokio::test(flavor = "current_thread")]
async fn tick_passes_bounded_prune_request_to_sink() {
    let seen = Arc::new(Mutex::new(None));
    let sink = FakeSink {
        seen: seen.clone(),
        report: TickReport {
            stale_suspended_observed: 1,
            orphaned_observed: 2,
            orphaned_facet_status: 3,
            orphaned_facet_conditions: 4,
        },
    };
    let config = ReconcilerConfig {
        enabled: true,
        tick: Duration::from_secs(30),
        stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(10)),
        orphan_age: PositiveDuration::new_panic(Duration::from_secs(20)),
        max_per_tick: 7,
        scan_window: 11,
        windows_per_tick: 3,
    };

    let report = Reconciler::new(config, sink).tick().await;
    let request = seen.lock().unwrap().expect("request captured");

    assert_eq!(report.stale_suspended_observed, 1);
    assert_eq!(request.stale_suspended_age.get(), Duration::from_secs(10));
    assert_eq!(request.orphan_age.get(), Duration::from_secs(20));
    assert_eq!(request.max_per_tick, 7);
    assert_eq!(request.scan_window, 11);
    assert_eq!(request.windows_per_tick, 3);
}

fn prune_request() -> PruneRequest {
    PruneRequest {
        stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(60)),
        orphan_age: PositiveDuration::new_panic(Duration::from_secs(60)),
        max_per_tick: 20,
        scan_window: DEFAULT_SCAN_WINDOW,
        windows_per_tick: DEFAULT_WINDOWS_PER_TICK,
    }
}

/// Full-table window bounds for tests that drive one windowed delete
/// directly.
fn full_range() -> (Uuid, Uuid) {
    (Uuid::nil(), Uuid::max())
}

async fn insert_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '127.0.0.1', 'worker', 'space')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_suspended_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    insert_sandbox_with_state(pool, sandbox_id, "suspended", 120).await
}

async fn insert_sandbox_with_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    age_minutes: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle,
             network_policy_generation, created_at, updated_at
         )
         VALUES (
             $1, $2::uuid, $3::uuid, 'space', $4,
             1, NOW() - make_interval(mins => $5::INTEGER),
             NOW() - make_interval(mins => $5::INTEGER)
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .bind(i32::try_from(age_minutes)?)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_observed_sandbox(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> TestResult {
    sqlx::query(
        sql!("INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, runtime, observed_at, updated_at
         )
         VALUES ($1, $2, 'suspended', 'firecracker', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours')"),
    )
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_template_inventory(
    pool: &PgPool,
    node_id: NodeId,
    template_name: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id)
         VALUES (space_uuid_v7(), $1, $2::uuid)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(template_name)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, observed_at, updated_at
         )
         VALUES ($1, $2, '', NOW(), NOW())"
    ))
    .bind(node_id)
    .bind(template_name)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_facet_status_and_condition(
    pool: &PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_at, updated_at
         )
         VALUES ($1, $2, $3, NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours')"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(pool)
    .await?;

    sqlx::query(
        sql!("INSERT INTO node_facet_conditions (
             node_id, facet, element, condition_type, condition_status,
             reason, message, last_transition_at, updated_at
         )
         VALUES (
             $1, $2, $3, 'ApplyFailed', 'True',
             'ReconcileFailed', 'test failure', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'
         )"),
    )
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_quarantine_recovery_facet(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    age_minutes: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         VALUES (
             $1,
             'sandbox-desired-state',
             $2,
             '{\"quarantine_recovery\": {\"schema_version\": 1}}'::JSONB,
             NOW() - make_interval(mins => $3::INTEGER),
             NOW() - make_interval(mins => $3::INTEGER)
         )"
    ))
    .bind(node_id)
    .bind(sandbox_id.to_string())
    .bind(i32::try_from(age_minutes)?)
    .execute(pool)
    .await?;
    Ok(())
}

async fn facet_status_exists(
    pool: &PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> TestResult<bool> {
    let exists = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM node_facet_observed_status
             WHERE node_id = $1
               AND facet = $2
               AND element = $3
         )"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

async fn facet_condition_exists(
    pool: &PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> TestResult<bool> {
    let exists = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM node_facet_conditions
             WHERE node_id = $1
               AND facet = $2
               AND element = $3
         )"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

async fn observed_exists(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    let exists = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandbox_observed_states
             WHERE sandbox_id = $1
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Selection is unordered (the LIMIT short-circuits the candidate scan),
/// so the per-tick contract is: exactly `max_per_tick` eligible rows go per
/// tick, ineligible rows are untouched, and repeated ticks reach every
/// eligible row — progress, not pick order, is the invariant.
#[tokio::test]
async fn stale_suspended_gc_bounds_each_tick_and_reaches_all_eligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let eligible_ids = [SandboxId::new(), SandboxId::new(), SandboxId::new()];
    let too_new_id = SandboxId::new();
    let active_id = SandboxId::new();

    insert_node(&db.pool, node_id).await?;
    for (sandbox_id, lifecycle, age_minutes) in [
        (eligible_ids[0], "suspended", 300),
        (eligible_ids[1], "suspended", 240),
        (eligible_ids[2], "suspended", 240),
        (too_new_id, "suspended", 30),
        (active_id, "started", 300),
    ] {
        insert_sandbox_with_state(&db.pool, sandbox_id, lifecycle, age_minutes).await?;
        insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    }

    let request = PruneRequest {
        stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(60 * 60)),
        orphan_age: PositiveDuration::new_panic(Duration::from_secs(24 * 60 * 60)),
        max_per_tick: 2,
        scan_window: DEFAULT_SCAN_WINDOW,
        windows_per_tick: DEFAULT_WINDOWS_PER_TICK,
    };
    let (from, to) = full_range();
    let deleted = delete_stale_suspended_observed(&db.pool, request, 2, from, to).await?;

    assert_eq!(
        deleted,
        (2, 0, 0),
        "first tick deletes exactly max_per_tick"
    );
    let mut survivors = 0;
    for id in eligible_ids {
        if observed_exists(&db.pool, id).await? {
            survivors += 1;
        }
    }
    assert_eq!(
        survivors, 1,
        "exactly one eligible row awaits the next tick"
    );
    assert!(observed_exists(&db.pool, too_new_id).await?);
    assert!(observed_exists(&db.pool, active_id).await?);

    let deleted = delete_stale_suspended_observed(&db.pool, request, 2, from, to).await?;
    assert_eq!(deleted, (1, 0, 0), "second tick drains the backlog");
    for id in eligible_ids {
        assert!(
            !observed_exists(&db.pool, id).await?,
            "every eligible row is reached across ticks"
        );
    }
    assert!(observed_exists(&db.pool, too_new_id).await?);
    assert!(observed_exists(&db.pool, active_id).await?);

    Ok(())
}

#[tokio::test]
async fn stale_suspended_gc_removes_bare_and_prefixed_sandbox_facets() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let bare_element = sandbox_id.to_string();
    let network_policy_element = format!("sandbox:{sandbox_id}");

    insert_node(&db.pool, node_id).await?;
    insert_suspended_sandbox(&db.pool, sandbox_id).await?;
    insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    insert_facet_status_and_condition(&db.pool, node_id, "sandbox-desired-state", &bare_element)
        .await?;
    insert_facet_status_and_condition(&db.pool, node_id, "sandbox-inventory", &bare_element)
        .await?;
    insert_facet_status_and_condition(&db.pool, node_id, "network-policy", &network_policy_element)
        .await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.stale_suspended_observed, 1);
    assert_eq!(report.orphaned_facet_status, 3);
    assert_eq!(report.orphaned_facet_conditions, 3);
    assert!(!facet_status_exists(&db.pool, node_id, "sandbox-desired-state", &bare_element).await?);
    assert!(
        !facet_condition_exists(&db.pool, node_id, "sandbox-desired-state", &bare_element).await?
    );
    assert!(!facet_status_exists(&db.pool, node_id, "sandbox-inventory", &bare_element).await?);
    assert!(!facet_condition_exists(&db.pool, node_id, "sandbox-inventory", &bare_element).await?);
    assert!(
        !facet_status_exists(&db.pool, node_id, "network-policy", &network_policy_element).await?
    );

    Ok(())
}

#[tokio::test]
async fn stale_suspended_gc_preserves_fresh_quarantine_recovery_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    insert_node(&db.pool, node_id).await?;
    insert_suspended_sandbox(&db.pool, sandbox_id).await?;
    insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    insert_quarantine_recovery_facet(&db.pool, node_id, sandbox_id, 0).await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.stale_suspended_observed, 0);
    assert!(observed_exists(&db.pool, sandbox_id).await?);
    assert!(
        facet_status_exists(
            &db.pool,
            node_id,
            "sandbox-desired-state",
            &sandbox_id.to_string()
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
async fn stale_suspended_gc_removes_expired_quarantine_recovery_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    insert_node(&db.pool, node_id).await?;
    insert_suspended_sandbox(&db.pool, sandbox_id).await?;
    insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    insert_quarantine_recovery_facet(&db.pool, node_id, sandbox_id, 120).await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.stale_suspended_observed, 1);
    assert!(!observed_exists(&db.pool, sandbox_id).await?);
    assert!(
        !facet_status_exists(
            &db.pool,
            node_id,
            "sandbox-desired-state",
            &sandbox_id.to_string()
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
async fn orphaned_observed_gc_removes_bare_and_prefixed_sandbox_facets() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let bare_element = sandbox_id.to_string();
    let network_policy_element = format!("sandbox:{sandbox_id}");

    insert_node(&db.pool, node_id).await?;
    insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    insert_facet_status_and_condition(&db.pool, node_id, "sandbox-inventory", &bare_element)
        .await?;
    insert_facet_status_and_condition(&db.pool, node_id, "network-policy", &network_policy_element)
        .await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.orphaned_observed, 1);
    assert_eq!(report.orphaned_facet_status, 2);
    assert_eq!(report.orphaned_facet_conditions, 2);
    assert!(!facet_status_exists(&db.pool, node_id, "sandbox-inventory", &bare_element).await?);
    assert!(
        !facet_status_exists(&db.pool, node_id, "network-policy", &network_policy_element).await?
    );

    Ok(())
}

#[tokio::test]
async fn orphaned_observed_gc_preserves_fresh_quarantine_recovery_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    insert_node(&db.pool, node_id).await?;
    insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    insert_quarantine_recovery_facet(&db.pool, node_id, sandbox_id, 0).await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.orphaned_observed, 0);
    assert!(observed_exists(&db.pool, sandbox_id).await?);
    assert!(
        facet_status_exists(
            &db.pool,
            node_id,
            "sandbox-desired-state",
            &sandbox_id.to_string()
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
async fn orphaned_facet_gc_removes_bare_template_facets_without_inventory() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();

    insert_node(&db.pool, node_id).await?;
    insert_facet_status_and_condition(
        &db.pool,
        node_id,
        "template-desired-state",
        "orphan-template",
    )
    .await?;
    insert_facet_status_and_condition(
        &db.pool,
        node_id,
        "network-policy",
        "template:orphan-template",
    )
    .await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.orphaned_facet_status, 2);
    assert_eq!(report.orphaned_facet_conditions, 2);
    assert!(
        !facet_status_exists(
            &db.pool,
            node_id,
            "template-desired-state",
            "orphan-template"
        )
        .await?
    );
    assert!(
        !facet_status_exists(
            &db.pool,
            node_id,
            "network-policy",
            "template:orphan-template"
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn orphaned_facet_gc_keeps_bare_template_facets_with_inventory() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();

    insert_node(&db.pool, node_id).await?;
    insert_template_inventory(&db.pool, node_id, "live-template").await?;
    insert_facet_status_and_condition(
        &db.pool,
        node_id,
        "template-base-snapshot-policy",
        "live-template",
    )
    .await?;

    let report = ObservedProjectionDbStore::new(db.pool.clone())
        .prune_observed_projections(prune_request())
        .await?;

    assert_eq!(report.orphaned_facet_status, 0);
    assert_eq!(report.orphaned_facet_conditions, 0);
    assert!(
        facet_status_exists(
            &db.pool,
            node_id,
            "template-base-snapshot-policy",
            "live-template"
        )
        .await?
    );
    assert!(
        facet_condition_exists(
            &db.pool,
            node_id,
            "template-base-snapshot-policy",
            "live-template"
        )
        .await?
    );

    Ok(())
}

/// With a scan window smaller than the table, one prune call sweeps only the
/// first windows; the persisted lap cursor makes later calls reach the rest,
/// and a finished lap starts over. This is the windowed-sweep contract that
/// replaced the full-table pass.
#[tokio::test]
async fn windowed_prune_reaches_all_rows_across_calls() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let eligible_ids = [SandboxId::new(), SandboxId::new(), SandboxId::new()];
    let active_id = SandboxId::new();

    insert_node(&db.pool, node_id).await?;
    for (sandbox_id, lifecycle) in [
        (eligible_ids[0], "suspended"),
        (eligible_ids[1], "suspended"),
        (eligible_ids[2], "suspended"),
        (active_id, "started"),
    ] {
        insert_sandbox_with_state(&db.pool, sandbox_id, lifecycle, 300).await?;
        insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    }

    let store = ObservedProjectionDbStore::new(db.pool.clone());
    let request = PruneRequest {
        stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(60 * 60)),
        orphan_age: PositiveDuration::new_panic(Duration::from_secs(24 * 60 * 60)),
        max_per_tick: 20,
        scan_window: 1,
        windows_per_tick: 1,
    };

    // One 1-row window per call: four observed rows take at most four calls
    // to lap, plus one call that observes the empty tail and resets the lap.
    let mut deleted_total = 0;
    for _ in 0..6 {
        deleted_total += store
            .prune_observed_projections(request)
            .await?
            .stale_suspended_observed;
    }
    assert_eq!(
        deleted_total, 3,
        "every eligible row is reached across calls"
    );
    for id in eligible_ids {
        assert!(!observed_exists(&db.pool, id).await?);
    }
    assert!(
        observed_exists(&db.pool, active_id).await?,
        "ineligible rows survive the full lap"
    );

    Ok(())
}

/// `max_per_tick` bounds one prune call's deletes even when the per-call
/// window budget could sweep further.
#[tokio::test]
async fn windowed_prune_respects_max_per_tick_across_windows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let eligible_ids = [SandboxId::new(), SandboxId::new(), SandboxId::new()];

    insert_node(&db.pool, node_id).await?;
    for sandbox_id in eligible_ids {
        insert_sandbox_with_state(&db.pool, sandbox_id, "suspended", 300).await?;
        insert_observed_sandbox(&db.pool, node_id, sandbox_id).await?;
    }

    let store = ObservedProjectionDbStore::new(db.pool.clone());
    let request = PruneRequest {
        stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(60 * 60)),
        orphan_age: PositiveDuration::new_panic(Duration::from_secs(24 * 60 * 60)),
        max_per_tick: 2,
        scan_window: 1,
        windows_per_tick: 10,
    };

    let report = store.prune_observed_projections(request).await?;
    assert_eq!(
        report.stale_suspended_observed, 2,
        "one call stops at max_per_tick despite remaining window budget"
    );

    let report = store.prune_observed_projections(request).await?;
    assert_eq!(
        report.stale_suspended_observed, 1,
        "the next call finishes the backlog"
    );

    Ok(())
}

/// The observed sweeps delete facet-status rows as side effects of each
/// stale observed row. Those must not consume the facet lap's own budget:
/// under ordinary suspend churn they would zero it every tick and the facet
/// cursor would never advance.
#[tokio::test]
async fn facet_sweep_budget_survives_observed_churn() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let churn_ids = [Uuid::now_v7(), Uuid::now_v7()];
    let orphan_element = Uuid::now_v7().to_string();

    insert_node(&db.pool, node_id).await?;
    // Two stale observed rows whose facet side-deletes alone consume
    // max_per_tick (2) worth of status rows.
    for sandbox_id in churn_ids {
        insert_suspended_sandbox(&db.pool, SandboxId::from_uuid(sandbox_id)).await?;
        insert_observed_sandbox(&db.pool, node_id, SandboxId::from_uuid(sandbox_id)).await?;
        insert_facet_status_and_condition(
            &db.pool,
            node_id,
            "sandbox-inventory",
            &sandbox_id.to_string(),
        )
        .await?;
    }
    // One aged facet row with no observed backing: only the facet lap can
    // remove it.
    insert_facet_status_and_condition(&db.pool, node_id, "sandbox-inventory", &orphan_element)
        .await?;

    let store = ObservedProjectionDbStore::new(db.pool.clone());
    let request = PruneRequest {
        stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(60)),
        orphan_age: PositiveDuration::new_panic(Duration::from_secs(60)),
        max_per_tick: 2,
        scan_window: DEFAULT_SCAN_WINDOW,
        windows_per_tick: DEFAULT_WINDOWS_PER_TICK,
    };
    let report = store.prune_observed_projections(request).await?;

    assert_eq!(report.stale_suspended_observed, 2);
    assert!(
        !facet_status_exists(&db.pool, node_id, "sandbox-inventory", &orphan_element).await?,
        "facet lap must run with its own budget despite observed-churn side-deletes"
    );
    assert_eq!(
        report.orphaned_facet_status, 3,
        "two churn side-deletes plus the orphaned facet row"
    );

    Ok(())
}
