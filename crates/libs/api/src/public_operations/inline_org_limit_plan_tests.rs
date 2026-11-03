//! Plan-cost regression for [`INLINE_ORG_AT_LIMIT`].
//!
//! The limit's behaviour is covered in `tests`; this covers how much its count
//! reads. A cached statement can switch to a generic plan, which knows no org,
//! so the count has to stay cheap without knowing how big the org is. Its input
//! is the org's running operations, and a plan linear in those reads each in a
//! few nodes. What made the count slow was work outside them:
//!
//! * reading every running operation in the fleet through the state index;
//! * reading the org's retained operation history through
//!   `idx_public_operations_tenant_created`;
//! * scanning the table whole.
//!
//! [`the_count_reads_only_the_orgs_running_operations`] bounds the rows every
//! plan node handles, under both plan modes, by a few passes over that input,
//! and [`seed`] sizes the fixture so each of the three costs above exceeds that
//! bound on its own. It reads rows off the plan through
//! [`super::plan_explain`].

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_public_api::OperationState;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::plan_explain::{PLAN_CACHE_MODES, PlanNode, explain_prepared};
use super::{ADMISSION_CLAIM_TOKEN_PREFIX, INLINE_ORG_AT_LIMIT, TEARDOWN_KINDS};

/// How many plan nodes may handle each of the org's running operations: its
/// index entry, its heap row, and the filter that drops it or the aggregate
/// that counts it.
const PASSES: u64 = 3;

/// The staleness bound the count is asked with, in seconds.
const CLAIM_STALE_AFTER_SECS: i64 = 60;

/// The small orgs filling the fleet, each with this many operations running
/// under a live admission claim and this much retained history.
const SMALL_ORGS: i32 = 300;
const SMALL_ORG_RUNNING: i32 = 40;
const SMALL_ORG_HISTORY: i32 = 20;

/// The org counted: running operations under a live admission claim, a stale
/// one, a dispatcher claim, and live-claimed teardowns; queued operations; and
/// retained history, every row of which carries an admission claim.
const ORG_LIVE_CLAIMS: i32 = 50;
const ORG_STALE_CLAIMS: i32 = 50;
const ORG_DISPATCHER_CLAIMS: i32 = 50;
const ORG_TEARDOWNS: i32 = 25;
const ORG_QUEUED: i32 = 100;
const ORG_HISTORY: i32 = 30_000;

struct Fixture {
    org_id: OrgId,
    /// The org's `running` operations, however claimed.
    org_running: u64,
    /// Every `running` operation in the fleet.
    fleet_running: u64,
}

/// The count's bound: [`PASSES`] over the org's running operations. Each slow
/// plan the module docs list exceeds it on [`seed`]'s fixture, which the test
/// checks before it measures.
#[tokio::test(flavor = "current_thread")]
async fn the_count_reads_only_the_orgs_running_operations() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fixture = seed(pool).await?;
    let bound = PASSES * fixture.org_running;
    let operations: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM public_operations"))
        .fetch_one(pool)
        .await?;
    for (cost, rows) in [
        (
            "every running operation in the fleet",
            fixture.fleet_running,
        ),
        ("the org's operation history", u64::try_from(ORG_HISTORY)?),
        ("a scan of `public_operations`", u64::try_from(operations)?),
    ] {
        assert!(
            rows > bound,
            "fixture too small: {cost} reads {rows} rows, within the bound of {bound}"
        );
    }

    for plan_cache_mode in PLAN_CACHE_MODES {
        let plan = explain_count(pool, &fixture, plan_cache_mode).await?;
        let handled = plan.rows_handled();
        let outline = plan.outline();
        assert!(
            handled <= bound as f64,
            "the count must handle no more rows than {PASSES} passes over the org's {} \
             running operations ({bound}) ({plan_cache_mode}); it handled {handled}. Rows \
             handled by node, children included:{outline}",
            fixture.org_running,
        );
    }
    Ok(())
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` the count for the fixture's org under a
/// forced `plan_cache_mode`, with the limit at the org's live admission claims.
async fn explain_count(
    pool: &PgPool,
    fixture: &Fixture,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    let prepare = format!(
        "PREPARE at_limit(uuid, bigint, text, text, bigint, text[]) AS {INLINE_ORG_AT_LIMIT}"
    );
    let teardowns = TEARDOWN_KINDS
        .iter()
        .map(|kind| format!("'{}'", kind.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let execute = format!(
        "EXECUTE at_limit('{}', {ORG_LIVE_CLAIMS}, '{}', '{ADMISSION_CLAIM_TOKEN_PREFIX}', \
         {CLAIM_STALE_AFTER_SECS}, ARRAY[{teardowns}])",
        fixture.org_id,
        OperationState::Running.as_str(),
    );
    explain_prepared(pool, &prepare, &execute, plan_cache_mode).await
}

/// A fleet of [`SMALL_ORGS`] small orgs and the counted org, analyzed so both
/// plan modes see its statistics.
async fn seed(pool: &PgPool) -> TestResult<Fixture> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         SELECT CASE WHEN g = 0 THEN $1 ELSE gen_random_uuid() END,
                'limit-plan-' || g, 'limit-plan-' || g
           FROM generate_series(0, $2) g"
    ))
    .bind(org_id)
    .bind(SMALL_ORGS)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         SELECT CASE WHEN o.id = $1 THEN $2 ELSE gen_random_uuid() END,
                o.slug || '@example.test', 'hash', 'Plan User', o.id, 'member'
           FROM organizations o
          WHERE o.slug LIKE 'limit-plan-%'"
    ))
    .bind(org_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    // Small orgs: operations running under live admission claims, and short
    // histories.
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, actor_user_id, kind, target_type, target_id, state,
              claim_token, claimed_at)
         SELECT gen_random_uuid(), u.org_id, u.id, 'create_sandbox', 'sandbox',
                'sbx_' || gen_random_uuid()::text,
                CASE WHEN g <= $2 THEN 'running' ELSE 'succeeded' END,
                $4 || gen_random_uuid()::text, NOW()
           FROM users u, generate_series(1, $2 + $3) g
          WHERE u.email LIKE 'limit-plan-%' AND u.org_id <> $1"
    ))
    .bind(org_id)
    .bind(SMALL_ORG_RUNNING)
    .bind(SMALL_ORG_HISTORY)
    .bind(ADMISSION_CLAIM_TOKEN_PREFIX)
    .execute(pool)
    .await?;
    // The counted org, one row per (kind, state, token, claim time) group.
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, actor_user_id, kind, target_type, target_id, state,
              claim_token, claimed_at)
         SELECT gen_random_uuid(), $1, $2, rows.kind, 'sandbox',
                'sbx_' || gen_random_uuid()::text, rows.state,
                CASE WHEN rows.token = '' THEN '' ELSE rows.token || gen_random_uuid()::text END,
                NOW() - rows.age
           FROM (VALUES
                    ('create_sandbox', 'running', $3, interval '0', $4),
                    ('create_sandbox', 'running', $3, interval '1 hour', $5),
                    ('create_sandbox', 'running', 'dispatcher-', interval '0', $6),
                    ('delete_sandbox', 'running', $3, interval '0', $7),
                    ('create_sandbox', 'queued', '', interval '0', $8),
                    ('create_sandbox', 'succeeded', $3, interval '0', $9)
                ) AS rows(kind, state, token, age, n),
                generate_series(1, rows.n)"
    ))
    .bind(org_id)
    .bind(user_id)
    .bind(ADMISSION_CLAIM_TOKEN_PREFIX)
    .bind(ORG_LIVE_CLAIMS)
    .bind(ORG_STALE_CLAIMS)
    .bind(ORG_DISPATCHER_CLAIMS)
    .bind(ORG_TEARDOWNS)
    .bind(ORG_QUEUED)
    .bind(ORG_HISTORY)
    .execute(pool)
    .await?;
    sqlx::query(sql!("ANALYZE public_operations"))
        .execute(pool)
        .await?;
    let (org_running, fleet_running): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*) FILTER (WHERE org_id = $1), COUNT(*)
           FROM public_operations
          WHERE state = 'running'"
    ))
    .bind(org_id)
    .fetch_one(pool)
    .await?;
    Ok(Fixture {
        org_id,
        org_running: u64::try_from(org_running)?,
        fleet_running: u64::try_from(fleet_running)?,
    })
}
