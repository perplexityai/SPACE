//! Plan-cost regressions for hot campaign item reads:
//! [`crate::items::candidates`]'s rolling and canary candidate selectors and
//! [`crate::sandboxes`]'s [`STARTED_PENDING_TARGET_FIRST`] /
//! [`STARTED_PENDING_TARGET_AFTER`] keyset walk.
//!
//! Functional ordering, wrap-around fairness, cluster-cap short-circuiting, and
//! target-ready recovery are covered by [`super::tests`]; this module bounds
//! how many rows and buffer blocks the statements handle as a campaign's item
//! history (`'done'` + `'pending'`) grows, under both PostgreSQL plan cache
//! modes (`force_custom_plan` and `force_generic_plan`).

use pplx_space_accounts::NodeId;
use pplx_space_content_digest::Digest;
use pplx_space_db_test_fixtures::plan::{PLAN_CACHE_MODES, explain_prepared};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_node_model::SubstrateKind;
use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use crate::items::candidates::{
    NODE_BOUND_CANARY_AFTER, NODE_BOUND_CANARY_FIRST, NODE_BOUND_ROLLING_AFTER,
    NODE_BOUND_ROLLING_FIRST, NODELESS_CANARY_AFTER, NODELESS_CANARY_FIRST, NODELESS_ROLLING_AFTER,
    NODELESS_ROLLING_FIRST,
};
use crate::items::{
    STARTED_HOLD_PREFIX, TARGET_START_SETTLED, TARGET_START_SETTLED_PREFIX, TARGET_STARTING,
    TARGET_STARTING_PREFIX, UNSTARTED_PREFIX,
};
use crate::sandboxes::{
    STARTED_PENDING_TARGET_AFTER, STARTED_PENDING_TARGET_FIRST, started_pending_target_sandboxes,
};
use crate::test_support::{CampaignSeed, insert_campaign};
use crate::{
    CampaignId, CampaignKind, CampaignPhase, ItemPhase, SubjectKind, lease_items, lease_items_after,
};

/// Items per campaign in the smaller fixture (`3/4` `'done'`, `1/4` `'pending'`).
const ITEMS: usize = 240;

/// Additional finished/background campaigns so `runtime_upgrade_items` and
/// `sandboxes` are large enough that PostgreSQL doesn't prefer a tiny-table
/// sequential scan over index probes at the smaller size.
const OTHER_CAMPAIGNS: u8 = 6;

/// How many times larger the second fixture is (`1,440` items per campaign).
const SCALE: usize = 6;

/// `LIMIT` bound in each prepared statement.
const PAGE_LIMIT: u32 = 8;

/// Upper bound on total rows handled across all plan nodes for one `LIMIT 8`
/// rolling candidate read. Index-ordered probes handle ~10–120 rows regardless
/// of campaign size, whereas scanning and sorting the campaign's `'pending'`
/// set (360 rows at the larger size) or `'done'` + `'pending'` set (1,440 rows)
/// exceeds this bound.
const MAX_BOUNDED_ROWS: f64 = 160.0;

/// Upper bound on per-row join/loop work (`PlanNode::repeated_rows_handled`)
/// for one `LIMIT 8` canary candidate read over a ready untouched backlog.
/// Keyset-windowed untouched selection probes `sandbox_observed_states` and
/// `node_status_snapshots` for at most `PAGE_LIMIT` rows (~16–32 repeated rows
/// regardless of backlog size), whereas joining the campaign's untouched
/// backlog before `LIMIT` handles >1,000 repeated rows at the larger size.
const MAX_CANARY_REPEATED_ROWS: f64 = 64.0;

/// Constant-overhead bound above `2 * pending_count` for total rows handled by
/// a canary read: `'done'` rows (`3 * total / 4`) stay untouched via
/// `runtime_upgrade_items_campaign_phase_subject_idx`, `started_pending`
/// inspects `'pending'` rows at most once (`<= 2 * pending_count` across a
/// `Bitmap Index Scan` + `Bitmap Heap Scan` pair), and `untouched` stops at
/// `LIMIT`. Scanning and sorting the untouched `'pending'` backlog adds at
/// least `2 * pending_count` more rows (`>= 3 * pending_count` total even under
/// `Index Scan`) and exceeds this bound at `ITEMS * SCALE`.
const MAX_CANARY_UNTOUCHED_OVERHEAD: usize = 160;

/// Upper bound on block-touch growth between `ITEMS` and `ITEMS * SCALE` for
/// canary reads (`started_pending` is linear in `'pending'`, while a correlated
/// rescan of `'pending'` per outer row grows quadratically above `SCALE`).
const MAX_CANARY_LINEAR_GROWTH: f64 = (SCALE as f64) * 1.5;

/// Upper bound on total rows handled across all plan nodes for one `LIMIT 8`
/// `started_pending_target_sandboxes` read (~312 rows across its lateral probe
/// for 15 `Index Only Scan` tuples at both fixture sizes, vs. >5,000 rows if
/// `'pending'` is scanned at the larger size).
const MAX_STARTED_TARGET_ROWS: f64 = 400.0;

/// Maximum block-touch growth between `ITEMS` and `ITEMS * SCALE`. Pure
/// `LIMIT`-bounded index walks stay near `1.0x` (with small B-tree height
/// slack), whereas full-campaign scans grow toward `SCALE` (`6.0x`).
const MAX_BOUNDED_GROWTH: f64 = 2.2;

struct BoundedCase {
    label: &'static str,
    prepare: String,
    execute: String,
    max_rows: f64,
    max_growth: f64,
}

struct Measured {
    label: &'static str,
    plan_cache_mode: &'static str,
    max_growth: f64,
    blocks: f64,
    outline: String,
}

struct CanaryCase {
    label: &'static str,
    prepare: String,
    execute: String,
}

struct Fixture {
    template_campaign: CampaignId,
    template_cursor: Uuid,
    sandbox_campaign: CampaignId,
    sandbox_cursor: Uuid,
    rejected_prefix_cursor: Uuid,
    unready_next_cursor: Uuid,
    started_target_cursor: Uuid,
}

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

#[tokio::test]
async fn rolling_campaign_item_reads_stay_bounded_by_limit() -> TestResult {
    let (small, large) = tokio::try_join!(measure_rolling(ITEMS), measure_rolling(ITEMS * SCALE))?;
    let (Some(small), Some(large)) = (small, large) else {
        return Ok(());
    };
    assert_bounded_growth(&small, &large);
    Ok(())
}

#[tokio::test]
async fn canary_campaign_item_reads_avoid_backlog_joins_and_sorts() -> TestResult {
    let (small, large) = tokio::try_join!(measure_canary(ITEMS), measure_canary(ITEMS * SCALE))?;
    let (Some(small), Some(large)) = (small, large) else {
        return Ok(());
    };
    assert_bounded_growth(&small, &large);
    Ok(())
}

#[tokio::test]
async fn started_pending_target_reads_stay_bounded_by_limit() -> TestResult {
    let (small, large) = tokio::try_join!(
        measure_started_pending_target(ITEMS),
        measure_started_pending_target(ITEMS * SCALE),
    )?;
    let (Some(small), Some(large)) = (small, large) else {
        return Ok(());
    };
    assert_bounded_growth(&small, &large);
    Ok(())
}

fn assert_bounded_growth(small: &[Measured], large: &[Measured]) {
    for (small, large) in small.iter().zip(large) {
        let growth = large.blocks / small.blocks.max(1.0);
        assert!(
            growth <= small.max_growth,
            "{} ({}): touched {} blocks at {ITEMS} items and {} at {} items, \
             {growth:.2}x growth (max {:.2}x). Plan at larger size:{}",
            small.label,
            small.plan_cache_mode,
            small.blocks,
            large.blocks,
            ITEMS * SCALE,
            small.max_growth,
            large.outline,
        );
    }
}

async fn measure_rolling(items_per_campaign: usize) -> TestResult<Option<Vec<Measured>>> {
    let Some(test_db) = try_pg_test_db().await? else {
        return Ok(None);
    };
    let pool = &test_db.pool;
    let fixture = seed(pool, items_per_campaign, CampaignPhase::Rolling, true).await?;
    analyze_campaign_tables(pool).await?;

    let measured = explain_bounded_cases(pool, items_per_campaign, rolling_cases(&fixture)).await?;

    let mut tpl_cursor = Some(fixture.template_cursor);
    let tpl_leased = lease_items_after(
        pool,
        fixture.template_campaign,
        &mut tpl_cursor,
        std::time::Duration::from_secs(60),
        PAGE_LIMIT,
    )
    .await?;
    assert_eq!(tpl_leased.len(), PAGE_LIMIT as usize);

    let mut sbx_cursor = Some(fixture.sandbox_cursor);
    let sbx_leased = lease_items_after(
        pool,
        fixture.sandbox_campaign,
        &mut sbx_cursor,
        std::time::Duration::from_secs(60),
        PAGE_LIMIT,
    )
    .await?;
    assert_eq!(sbx_leased.len(), PAGE_LIMIT as usize);

    // Over an unready range (`reachable = false`), `lease_items_after` scans at
    // most `PAGE_LIMIT` pending rows and advances `after` past them so the next
    // tick continues forward rather than rescanning from the same cursor.
    let mut unready_cursor = Some(fixture.rejected_prefix_cursor);
    let unready_leased = lease_items_after(
        pool,
        fixture.sandbox_campaign,
        &mut unready_cursor,
        std::time::Duration::from_secs(60),
        PAGE_LIMIT,
    )
    .await?;
    assert!(unready_leased.is_empty());
    assert_eq!(unready_cursor, Some(fixture.unready_next_cursor));

    Ok(Some(measured))
}

async fn measure_canary(items_per_campaign: usize) -> TestResult<Option<Vec<Measured>>> {
    let Some(test_db) = try_pg_test_db().await? else {
        return Ok(None);
    };
    let pool = &test_db.pool;
    let fixture = seed(pool, items_per_campaign, CampaignPhase::Canary, false).await?;
    analyze_campaign_tables(pool).await?;

    let pending_count = items_per_campaign - (items_per_campaign * 3) / 4;
    let max_total_rows = (2 * pending_count + MAX_CANARY_UNTOUCHED_OVERHEAD) as f64;
    let mut measured = Vec::new();
    for case in canary_cases(&fixture) {
        for plan_cache_mode in PLAN_CACHE_MODES {
            let plan =
                explain_prepared(pool, &case.prepare, &case.execute, plan_cache_mode).await?;
            let outline = plan.outline();
            assert!(plan.rows_handled() > 0.0, "{}: no rows parsed", case.label);
            let repeated = plan.repeated_rows_handled();
            assert!(
                repeated <= MAX_CANARY_REPEATED_ROWS,
                "{} ({plan_cache_mode}, {items_per_campaign} items): handled {repeated} repeated join/loop rows, \
                 over {MAX_CANARY_REPEATED_ROWS}. Plan outline:{outline}",
                case.label,
            );
            let handled = plan.rows_handled();
            assert!(
                handled <= max_total_rows,
                "{} ({plan_cache_mode}, {items_per_campaign} items): handled {handled} total rows, \
                 over {max_total_rows}. Plan outline:{outline}",
                case.label,
            );
            measured.push(Measured {
                label: case.label,
                plan_cache_mode,
                max_growth: MAX_CANARY_LINEAR_GROWTH,
                blocks: plan.blocks_touched(),
                outline,
            });
        }
    }

    // Over an unready range (`reachable = false`), `lease_items_after` in
    // `canary` advances `after` across the `PAGE_LIMIT` window without leasing,
    // while `lease_items` (`after = None`) walks straight to the first ready
    // untouched item.
    let mut unready_cursor = Some(fixture.rejected_prefix_cursor);
    let unready_leased = lease_items_after(
        pool,
        fixture.sandbox_campaign,
        &mut unready_cursor,
        std::time::Duration::from_secs(60),
        PAGE_LIMIT,
    )
    .await?;
    assert!(unready_leased.is_empty());
    assert_eq!(unready_cursor, Some(fixture.unready_next_cursor));

    let first_tpl = lease_items(
        pool,
        fixture.template_campaign,
        std::time::Duration::from_secs(60),
        PAGE_LIMIT,
    )
    .await?;
    assert_eq!(first_tpl.len(), 1);

    let first_sbx = lease_items(
        pool,
        fixture.sandbox_campaign,
        std::time::Duration::from_secs(60),
        PAGE_LIMIT,
    )
    .await?;
    assert_eq!(first_sbx.len(), 1);

    Ok(Some(measured))
}

async fn measure_started_pending_target(
    items_per_campaign: usize,
) -> TestResult<Option<Vec<Measured>>> {
    let Some(test_db) = try_pg_test_db().await? else {
        return Ok(None);
    };
    let pool = &test_db.pool;
    let fixture = seed(pool, items_per_campaign, CampaignPhase::Rolling, true).await?;
    analyze_campaign_tables(pool).await?;

    let measured = explain_bounded_cases(
        pool,
        items_per_campaign,
        started_pending_target_cases(&fixture),
    )
    .await?;

    let target_first = started_pending_target_sandboxes(
        pool,
        fixture.sandbox_campaign,
        SubstrateKind::Kubernetes,
        &[],
        None,
        PAGE_LIMIT,
    )
    .await?;
    assert!(target_first.gate_open);
    assert_eq!(target_first.probed, PAGE_LIMIT as usize);
    assert_eq!(
        target_first.last_probed,
        Some(fixture.rejected_prefix_cursor)
    );
    assert!(target_first.items.is_empty());

    let target_after = started_pending_target_sandboxes(
        pool,
        fixture.sandbox_campaign,
        SubstrateKind::Kubernetes,
        &[],
        Some(fixture.started_target_cursor),
        PAGE_LIMIT,
    )
    .await?;
    assert!(target_after.gate_open);
    assert_eq!(target_after.probed, PAGE_LIMIT as usize);
    assert_eq!(target_after.items.len(), PAGE_LIMIT as usize);

    Ok(Some(measured))
}

async fn analyze_campaign_tables(pool: &PgPool) -> TestResult<()> {
    sqlx::query(sql!(
        "ANALYZE runtime_upgrade_campaigns, runtime_upgrade_items, sandboxes,
                 sandbox_observed_states, node_status_snapshots, snapshots"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn explain_bounded_cases(
    pool: &PgPool,
    items_per_campaign: usize,
    cases: Vec<BoundedCase>,
) -> TestResult<Vec<Measured>> {
    let mut measured = Vec::with_capacity(cases.len() * PLAN_CACHE_MODES.len());
    for case in cases {
        for plan_cache_mode in PLAN_CACHE_MODES {
            let plan =
                explain_prepared(pool, &case.prepare, &case.execute, plan_cache_mode).await?;
            assert!(plan.rows_handled() > 0.0, "{}: no rows parsed", case.label);
            let handled = plan.rows_handled();
            assert!(
                handled <= case.max_rows,
                "{} ({plan_cache_mode}, {items_per_campaign} items): handled {handled} total rows, \
                 over {}. Plan outline:{}",
                case.label,
                case.max_rows,
                plan.outline(),
            );
            measured.push(Measured {
                label: case.label,
                plan_cache_mode,
                max_growth: case.max_growth,
                blocks: plan.blocks_touched(),
                outline: plan.outline(),
            });
        }
    }
    Ok(measured)
}

fn rolling_cases(fixture: &Fixture) -> Vec<BoundedCase> {
    let limit = i64::from(PAGE_LIMIT);
    let tpl_id = fixture.template_campaign.0;
    let tpl_cursor = fixture.template_cursor;
    let sbx_id = fixture.sandbox_campaign.0;
    let sbx_cursor = fixture.sandbox_cursor;
    let rejected_cursor = fixture.rejected_prefix_cursor;
    let target_settled = TARGET_START_SETTLED;
    let target_settled_prefix = TARGET_START_SETTLED_PREFIX;
    vec![
        BoundedCase {
            label: "nodeless rolling first page",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint) AS {NODELESS_ROLLING_FIRST}"
            ),
            execute: format!(
                "EXECUTE plan_under_test('{tpl_id}', '{}', {limit}, '{}', 32)",
                ItemPhase::Pending.as_str(),
                ItemPhase::InFlight.as_str(),
            ),
            max_rows: MAX_BOUNDED_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
        BoundedCase {
            label: "nodeless rolling wrap-around cursor",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, uuid) AS {NODELESS_ROLLING_AFTER}"
            ),
            execute: format!(
                "EXECUTE plan_under_test('{tpl_id}', '{}', {limit}, '{}', 32, '{tpl_cursor}')",
                ItemPhase::Pending.as_str(),
                ItemPhase::InFlight.as_str(),
            ),
            max_rows: MAX_BOUNDED_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
        BoundedCase {
            label: "node-bound rolling first page",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, boolean, text, text) AS {}",
                NODE_BOUND_ROLLING_FIRST.as_str(),
            ),
            execute: format!(
                "EXECUTE plan_under_test('{sbx_id}', '{}', {limit}, '{}', 32, true, '{target_settled}', '{target_settled_prefix}')",
                ItemPhase::Pending.as_str(),
                ItemPhase::InFlight.as_str(),
            ),
            max_rows: MAX_BOUNDED_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
        BoundedCase {
            label: "node-bound rolling wrap-around cursor",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, uuid, boolean, text, text) AS {}",
                NODE_BOUND_ROLLING_AFTER.as_str(),
            ),
            execute: format!(
                "EXECUTE plan_under_test('{sbx_id}', '{}', {limit}, '{}', 32, '{sbx_cursor}', true, '{target_settled}', '{target_settled_prefix}')",
                ItemPhase::Pending.as_str(),
                ItemPhase::InFlight.as_str(),
            ),
            max_rows: MAX_BOUNDED_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
        BoundedCase {
            label: "node-bound rolling after cursor over unready range",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, uuid, boolean, text, text) AS {}",
                NODE_BOUND_ROLLING_AFTER.as_str(),
            ),
            execute: format!(
                "EXECUTE plan_under_test('{sbx_id}', '{}', {limit}, '{}', 32, '{rejected_cursor}', true, '{target_settled}', '{target_settled_prefix}')",
                ItemPhase::Pending.as_str(),
                ItemPhase::InFlight.as_str(),
            ),
            max_rows: MAX_BOUNDED_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
    ]
}

fn canary_cases(fixture: &Fixture) -> Vec<CanaryCase> {
    let limit = i64::from(PAGE_LIMIT);
    let tpl_id = fixture.template_campaign.0;
    let tpl_cursor = fixture.template_cursor;
    let sbx_id = fixture.sandbox_campaign.0;
    let sbx_cursor = fixture.sandbox_cursor;
    let rejected_cursor = fixture.rejected_prefix_cursor;
    let unstarted = UNSTARTED_PREFIX;
    let started_hold = STARTED_HOLD_PREFIX;
    let target_settled = TARGET_START_SETTLED;
    let target_settled_prefix = TARGET_START_SETTLED_PREFIX;
    let pending = ItemPhase::Pending.as_str();
    let in_flight = ItemPhase::InFlight.as_str();
    vec![
        CanaryCase {
            label: "nodeless canary first page over untouched backlog",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, boolean, text, text) AS {NODELESS_CANARY_FIRST}"
            ),
            execute: format!(
                "EXECUTE plan_under_test('{tpl_id}', '{pending}', {limit}, '{in_flight}', 1, false, '{unstarted}', '{started_hold}')"
            ),
        },
        CanaryCase {
            label: "nodeless canary wrap-around cursor over untouched backlog",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, boolean, text, uuid, text) AS {NODELESS_CANARY_AFTER}"
            ),
            execute: format!(
                "EXECUTE plan_under_test('{tpl_id}', '{pending}', {limit}, '{in_flight}', 1, false, '{unstarted}', '{tpl_cursor}', '{started_hold}')"
            ),
        },
        CanaryCase {
            label: "node-bound canary first page over untouched backlog",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, boolean, text, text, text, text) AS {}",
                NODE_BOUND_CANARY_FIRST.as_str(),
            ),
            execute: format!(
                "EXECUTE plan_under_test('{sbx_id}', '{pending}', {limit}, '{in_flight}', 1, true, '{unstarted}', '{started_hold}', '{target_settled}', '{target_settled_prefix}')"
            ),
        },
        CanaryCase {
            label: "node-bound canary wrap-around cursor over untouched backlog",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, boolean, text, uuid, text, text, text) AS {}",
                NODE_BOUND_CANARY_AFTER.as_str(),
            ),
            execute: format!(
                "EXECUTE plan_under_test('{sbx_id}', '{pending}', {limit}, '{in_flight}', 1, true, '{unstarted}', '{sbx_cursor}', '{started_hold}', '{target_settled}', '{target_settled_prefix}')"
            ),
        },
        CanaryCase {
            label: "node-bound canary after cursor over unready range",
            prepare: format!(
                "PREPARE plan_under_test(uuid, text, bigint, text, bigint, boolean, text, uuid, text, text, text) AS {}",
                NODE_BOUND_CANARY_AFTER.as_str(),
            ),
            execute: format!(
                "EXECUTE plan_under_test('{sbx_id}', '{pending}', {limit}, '{in_flight}', 1, true, '{unstarted}', '{rejected_cursor}', '{started_hold}', '{target_settled}', '{target_settled_prefix}')"
            ),
        },
    ]
}

fn started_pending_target_cases(fixture: &Fixture) -> Vec<BoundedCase> {
    let limit = i64::from(PAGE_LIMIT);
    let sbx_id = fixture.sandbox_campaign.0;
    let rejected_cursor = fixture.rejected_prefix_cursor;
    let target_cursor = fixture.started_target_cursor;
    let unstarted = UNSTARTED_PREFIX;
    let target_settled = TARGET_START_SETTLED;
    let target_settled_prefix = TARGET_START_SETTLED_PREFIX;
    let target_starting = TARGET_STARTING;
    let target_starting_prefix = TARGET_STARTING_PREFIX;
    let starting_lifecycle = SandboxLifecycle::Starting.as_str();
    let started_args_prefix = format!(
        "'{sbx_id}', '{}', ARRAY['stopped','started','paused'], 'stopped', 'disk', 'stop', 'disk', \
         'sandbox', 'pending', 'ready', '{unstarted}', 'in_flight', 'completed', \
         ARRAY[]::uuid[], ARRAY['canary','rolling'], 'canary', 'done', 'failed', {limit}, \
         '{target_settled}', '{target_settled_prefix}', '{target_starting}', '{target_starting_prefix}', '{starting_lifecycle}'",
        SubstrateKind::Kubernetes.as_str(),
    );
    vec![
        BoundedCase {
            label: "started_pending_target_sandboxes first page",
            prepare: format!(
                "PREPARE plan_under_test(\
                 uuid, text, text[], text, text, text, text, text, text, text, text, text, text, \
                 uuid[], text[], text, text, text, bigint, text, text, text, text, text\
                 ) AS {}",
                STARTED_PENDING_TARGET_FIRST.as_str(),
            ),
            execute: format!("EXECUTE plan_under_test({started_args_prefix})"),
            max_rows: MAX_STARTED_TARGET_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
        BoundedCase {
            label: "started_pending_target_sandboxes rejected prefix cursor",
            prepare: format!(
                "PREPARE plan_under_test(\
                 uuid, text, text[], text, text, text, text, text, text, text, text, text, text, \
                 uuid[], text[], text, text, text, bigint, text, text, text, text, text, uuid\
                 ) AS {}",
                STARTED_PENDING_TARGET_AFTER.as_str(),
            ),
            execute: format!("EXECUTE plan_under_test({started_args_prefix}, '{rejected_cursor}')"),
            max_rows: MAX_STARTED_TARGET_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
        BoundedCase {
            label: "started_pending_target_sandboxes target tail cursor",
            prepare: format!(
                "PREPARE plan_under_test(\
                 uuid, text, text[], text, text, text, text, text, text, text, text, text, text, \
                 uuid[], text[], text, text, text, bigint, text, text, text, text, text, uuid\
                 ) AS {}",
                STARTED_PENDING_TARGET_AFTER.as_str(),
            ),
            execute: format!("EXECUTE plan_under_test({started_args_prefix}, '{target_cursor}')"),
            max_rows: MAX_STARTED_TARGET_ROWS,
            max_growth: MAX_BOUNDED_GROWTH,
        },
    ]
}

async fn seed(
    pool: &PgPool,
    items_per_campaign: usize,
    phase: CampaignPhase,
    include_started_target: bool,
) -> TestResult<Fixture> {
    let worker = NodeId::new();
    insert_ready_worker_node(pool, worker).await?;
    upsert_snapshot(pool, worker, SnapshotSeed::default()).await?;

    let unreachable_worker = NodeId::new();
    insert_ready_worker_node(pool, unreachable_worker).await?;
    upsert_snapshot(
        pool,
        unreachable_worker,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;

    let from_env = environment(1);
    let to_env = environment(2);

    let seed_cfg = CampaignSeed {
        from: from_env,
        to: to_env,
        phase,
        per_node: 32,
        per_cluster: 32,
        ..CampaignSeed::default()
    };
    let template_campaign = insert_campaign(
        pool,
        CampaignSeed {
            kind: CampaignKind::Templates,
            ..seed_cfg.clone()
        },
    )
    .await?;
    let sandbox_campaign = insert_campaign(
        pool,
        CampaignSeed {
            kind: CampaignKind::SandboxForceRestarts,
            ..seed_cfg
        },
    )
    .await?;

    let template_pending = seed_template_campaign_rows(
        pool,
        template_campaign,
        items_per_campaign,
        phase != CampaignPhase::Canary,
    )
    .await?;
    let (sandbox_pending, started_target) = seed_sandbox_campaign_rows(
        pool,
        sandbox_campaign,
        (worker, unreachable_worker),
        items_per_campaign,
        (from_env, to_env),
        include_started_target,
    )
    .await?;

    seed_background_campaigns(pool, (worker, unreachable_worker), (from_env, to_env)).await?;

    // Place wrap-around cursors 3 items before the end of each pending ring so
    // `*_AFTER` queries exercise both branches (`> cursor` and `<= cursor`)
    // under `LIMIT 8`.
    let template_cursor = template_pending[template_pending.len() - 3];
    let sandbox_cursor = sandbox_pending[sandbox_pending.len() - 3];
    let rejected_prefix_cursor = sandbox_pending[(PAGE_LIMIT as usize) - 1];
    let unready_next_cursor = sandbox_pending[(2 * PAGE_LIMIT as usize) - 1];
    let started_target_cursor = if started_target.is_empty() {
        sandbox_cursor
    } else {
        sandbox_pending[sandbox_pending.len() - started_target.len() - 1]
    };

    Ok(Fixture {
        template_campaign,
        template_cursor,
        sandbox_campaign,
        sandbox_cursor,
        rejected_prefix_cursor,
        unready_next_cursor,
        started_target_cursor,
    })
}

async fn seed_template_campaign_rows(
    pool: &PgPool,
    campaign: CampaignId,
    total: usize,
    use_done_history: bool,
) -> TestResult<Vec<Uuid>> {
    let history_count = (total * 3) / 4;
    let history_phase = if use_done_history {
        ItemPhase::Done.as_str()
    } else {
        ItemPhase::Skipped.as_str()
    };
    let mut ids: Vec<Uuid> = (0..total).map(|_| Uuid::now_v7()).collect();
    ids.sort_unstable();
    let phases: Vec<&str> = (0..total)
        .map(|idx| {
            if idx < history_count {
                history_phase
            } else {
                ItemPhase::Pending.as_str()
            }
        })
        .collect();
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, last_error
         )
         SELECT $1, $2, u.subject_id, u.phase,
                CASE WHEN u.phase = 'pending' THEN 0 ELSE 1 END,
                CASE WHEN u.phase = 'skipped' THEN 'already on target' ELSE NULL END
         FROM UNNEST($3::uuid[], $4::text[]) AS u(subject_id, phase)"
    ))
    .bind(campaign)
    .bind(SubjectKind::TemplateRelease.as_str())
    .bind(&ids)
    .bind(&phases)
    .execute(pool)
    .await?;
    Ok(ids[history_count..].to_vec())
}

async fn seed_sandbox_campaign_rows(
    pool: &PgPool,
    campaign: CampaignId,
    (worker, unreachable_worker): (NodeId, NodeId),
    total: usize,
    (from_env, to_env): (EnvironmentReference, EnvironmentReference),
    include_started_target: bool,
) -> TestResult<(Vec<Uuid>, Vec<Uuid>)> {
    let done_count = (total * 3) / 4;
    let pending_count = total - done_count;
    let target_ready_tail = if include_started_target {
        16.min(pending_count)
    } else {
        0
    };
    let mut ids: Vec<Uuid> = (0..total).map(|_| Uuid::now_v7()).collect();
    ids.sort_unstable();

    let mut phases = Vec::with_capacity(total);
    let mut last_errors: Vec<Option<&str>> = Vec::with_capacity(total);
    let mut env_refs: Vec<String> = Vec::with_capacity(total);
    let mut node_ids: Vec<Uuid> = Vec::with_capacity(total);
    let mut started_target = Vec::new();
    let from_str = from_env.to_string();
    let to_str = to_env.to_string();

    for (idx, id) in ids.iter().copied().enumerate() {
        if idx < done_count {
            if include_started_target {
                phases.push(ItemPhase::Done.as_str());
                last_errors.push(None);
            } else {
                phases.push(ItemPhase::Skipped.as_str());
                last_errors.push(Some("already on target"));
            }
            env_refs.push(to_str.clone());
            node_ids.push(worker.as_uuid());
        } else {
            phases.push(ItemPhase::Pending.as_str());
            let pending_idx = idx - done_count;
            if include_started_target && pending_idx >= pending_count - target_ready_tail {
                last_errors.push(Some("run started"));
                env_refs.push(to_str.clone());
                node_ids.push(worker.as_uuid());
                started_target.push(id);
            } else if pending_idx >= PAGE_LIMIT as usize
                && pending_idx < (3 * PAGE_LIMIT as usize).min(pending_count)
            {
                last_errors.push(None);
                env_refs.push(from_str.clone());
                node_ids.push(unreachable_worker.as_uuid());
            } else {
                last_errors.push(None);
                env_refs.push(from_str.clone());
                node_ids.push(worker.as_uuid());
            }
        }
    }

    insert_sandbox_rows(
        pool,
        campaign,
        &ids,
        &node_ids,
        &env_refs,
        &phases,
        &last_errors,
    )
    .await?;

    Ok((ids[done_count..].to_vec(), started_target))
}

async fn seed_background_campaigns(
    pool: &PgPool,
    (worker, _unreachable_worker): (NodeId, NodeId),
    (_from_env, to_env): (EnvironmentReference, EnvironmentReference),
) -> TestResult<()> {
    let mut other_campaigns = Vec::with_capacity(usize::from(OTHER_CAMPAIGNS));
    for idx in 0..OTHER_CAMPAIGNS {
        let other = insert_campaign(
            pool,
            CampaignSeed {
                from: environment(10 + idx),
                to: environment(30 + idx),
                kind: CampaignKind::SandboxForceRestarts,
                phase: CampaignPhase::Completed,
                per_node: 16,
                per_cluster: 32,
                ..CampaignSeed::default()
            },
        )
        .await?;
        other_campaigns.push(other.0);
    }

    let total = usize::from(OTHER_CAMPAIGNS) * ITEMS;
    let mut campaign_ids = Vec::with_capacity(total);
    let mut ids = Vec::with_capacity(total);
    for &campaign_id in &other_campaigns {
        let mut batch: Vec<Uuid> = (0..ITEMS).map(|_| Uuid::now_v7()).collect();
        batch.sort_unstable();
        campaign_ids.extend(std::iter::repeat_n(campaign_id, ITEMS));
        ids.extend(batch);
    }
    let node_ids = vec![worker.as_uuid(); total];
    let env_refs = vec![to_env.to_string(); total];

    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, network_policy_generation
         )
         SELECT u.s_id, $2, $3, $4, $5, 'on_node', u.n_id, 1
         FROM UNNEST($1::uuid[], $6::uuid[]) AS u(s_id, n_id)"
    ))
    .bind(&ids)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(SandboxLifecycle::Started.as_str())
    .bind(&node_ids)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, runtime, runtime_version, execution_metadata
         )
         SELECT u.s_id, u.n_id, 'firecracker', '1.15.1',
                jsonb_build_object('environment', u.env_ref)
         FROM UNNEST($1::uuid[], $2::uuid[], $3::text[]) AS u(s_id, n_id, env_ref)"
    ))
    .bind(&ids)
    .bind(&node_ids)
    .bind(&env_refs)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, last_error
         )
         SELECT u.c_id, $1, u.s_id, $2, 1, NULL
         FROM UNNEST($3::uuid[], $4::uuid[]) AS u(c_id, s_id)"
    ))
    .bind(SubjectKind::Sandbox.as_str())
    .bind(ItemPhase::Done.as_str())
    .bind(&campaign_ids)
    .bind(&ids)
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_sandbox_rows(
    pool: &PgPool,
    campaign: CampaignId,
    ids: &[Uuid],
    node_ids: &[Uuid],
    env_refs: &[String],
    phases: &[&str],
    last_errors: &[Option<&str>],
) -> TestResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, network_policy_generation
         )
         SELECT u.s_id, $2, $3, $4, $5, 'on_node', u.n_id, 1
         FROM UNNEST($1::uuid[], $6::uuid[]) AS u(s_id, n_id)"
    ))
    .bind(ids)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(SandboxLifecycle::Started.as_str())
    .bind(node_ids)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, runtime, runtime_version, execution_metadata
         )
         SELECT u.s_id, u.n_id, 'firecracker', '1.15.1',
                jsonb_build_object('environment', u.env_ref)
         FROM UNNEST($1::uuid[], $2::uuid[], $3::text[]) AS u(s_id, n_id, env_ref)"
    ))
    .bind(ids)
    .bind(node_ids)
    .bind(env_refs)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, last_error
         )
         SELECT $1, $2, u.s_id, u.phase,
                CASE WHEN u.phase = 'done' OR u.last_error IS NOT NULL THEN 1 ELSE 0 END,
                u.last_error
         FROM UNNEST($3::uuid[], $4::text[], $5::text[]) AS u(s_id, phase, last_error)"
    ))
    .bind(campaign)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(ids)
    .bind(phases)
    .bind(last_errors)
    .execute(pool)
    .await?;

    Ok(())
}
