//! Plan-cost regression for [`SANDBOXES_HELD`].
//!
//! The count's behaviour is covered in `tests`; this covers how much it
//! reads. A cached statement can switch to a generic plan, which knows no
//! org, so a plan has to stay cheap without knowing how big the org is. The
//! count's inputs are the org's sandbox rows and the org's in-flight
//! operations, and a plan linear in those reads each of them in a few nodes:
//! scanned, filtered, joined or hashed, and appended. What made the count
//! slow was work outside them:
//!
//! * comparing each in-flight create with each of the org's rows, which a
//!   generic plan of a text comparison does in a nested loop, so the work
//!   is their product;
//! * reading the org's retained operation history through
//!   `idx_public_operations_tenant_created`, a bitmap intersected with the
//!   in-flight state index;
//! * reading every in-flight operation in the cluster, every org's and of
//!   every kind, through the state index alone;
//! * scanning either table whole.
//!
//! [`the_count_reads_only_the_orgs_rows_and_in_flight_operations`]
//! bounds the rows every plan node handles, under both plan modes, by a few
//! passes over those inputs, and [`seed`] sizes the fixture so each of the
//! four costs above exceeds that bound on its own. It reads rows off the
//! plan through [`super::super::plan_explain`].

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::super::plan_explain::{PLAN_CACHE_MODES, PlanNode, explain_prepared};
use super::SANDBOXES_HELD;

/// How many plan nodes may handle each input row. The count's linear plans
/// handle an org row in its scan (the bitmap index entry and the heap row),
/// the append, and, when the anti-join hashes the org's rows, a second scan
/// and the hash; an in-flight operation in its index entry, its heap row,
/// the kind filter and the anti-join.
const PASSES: u64 = 6;

/// The small orgs filling the cluster, each with this many sandboxes, one
/// in-flight create, this many other in-flight operations and this much
/// retained history.
const SMALL_ORGS: i32 = 300;
const SMALL_ORG_SANDBOXES: i32 = 80;
const SMALL_ORG_BACKLOG: i32 = 60;
const SMALL_ORG_HISTORY: i32 = 20;

/// The org counted: live rows, `deleted` tombstones, in-flight creates of
/// which some already have their `creating` row, other in-flight
/// operations, and retained history.
const ORG_LIVE: i32 = 1_000;
const ORG_TOMBSTONES: i32 = 500;
const ORG_CREATES: i32 = 200;
const ORG_CREATES_WITH_ROWS: i32 = 50;
const ORG_BACKLOG: i32 = 100;
const ORG_HISTORY: i32 = 30_000;

struct Fixture {
    org_id: OrgId,
    user_id: UserId,
    /// Every row of the org's, live or not.
    org_rows: u64,
    /// The org's `queued` or `running` operations, of every kind.
    org_in_flight: u64,
    /// Every `queued` or `running` operation in the cluster.
    cluster_in_flight: u64,
}

/// The count's bound: [`PASSES`] over the org's rows and the org's
/// in-flight operations. Each slow plan the module docs list exceeds it on
/// [`seed`]'s fixture, which the test checks before it measures.
///
/// Asked about the org whose count was slow: big next to the others, with
/// many creates in flight, so a create compared with every row costs far
/// more than either alone, and with an operation history and a cluster
/// in-flight set each far longer than its own in-flight set.
#[tokio::test(flavor = "current_thread")]
async fn the_count_reads_only_the_orgs_rows_and_in_flight_operations() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fixture = seed(pool).await?;
    let bound = PASSES * (fixture.org_rows + fixture.org_in_flight);
    let creates_without_rows = u64::try_from(ORG_CREATES - ORG_CREATES_WITH_ROWS)?;
    let org_live = u64::try_from(ORG_LIVE + ORG_CREATES_WITH_ROWS)?;
    let sandboxes: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes"))
        .fetch_one(pool)
        .await?;
    for (cost, rows) in [
        (
            "each create compared with each row",
            creates_without_rows * org_live,
        ),
        ("the org's operation history", u64::try_from(ORG_HISTORY)?),
        (
            "every in-flight operation in the cluster",
            fixture.cluster_in_flight,
        ),
        ("a scan of `sandboxes`", u64::try_from(sandboxes)?),
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
            "the count must handle no more rows than {PASSES} passes over the org's {} rows \
             and its {} in-flight operations ({bound}) ({plan_cache_mode}); it handled \
             {handled}. Rows handled by node, children included:{outline}",
            fixture.org_rows,
            fixture.org_in_flight,
        );
    }
    Ok(())
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` the count for the fixture's org under a
/// forced `plan_cache_mode`.
async fn explain_count(
    pool: &PgPool,
    fixture: &Fixture,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    let prepare = format!("PREPARE held(uuid, uuid, text, text, text) AS {SANDBOXES_HELD}");
    let execute = format!(
        "EXECUTE held('{}', '{}', '{}', '{}', '{}')",
        fixture.org_id,
        fixture.user_id,
        super::ApiProduct::Native.as_str(),
        pplx_space_public_api::OperationKind::CreateSandbox.as_str(),
        pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX,
    );
    explain_prepared(pool, &prepare, &execute, plan_cache_mode).await
}

/// A cluster of [`SMALL_ORGS`] small orgs and the counted org, analyzed so
/// both plan modes see its statistics.
async fn seed(pool: &PgPool) -> TestResult<Fixture> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         SELECT CASE WHEN g = 0 THEN $1 ELSE gen_random_uuid() END,
                'plan-' || g, 'plan-' || g
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
          WHERE o.slug LIKE 'plan-%'"
    ))
    .bind(org_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    // Small orgs: sandboxes, one in-flight create each, a backlog of other
    // in-flight operations, and short histories.
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(), u.id, u.org_id, 'k8s', 'started', 1
           FROM users u, generate_series(1, $2)
          WHERE u.email LIKE 'plan-%' AND u.org_id <> $1"
    ))
    .bind(org_id)
    .bind(SMALL_ORG_SANDBOXES)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, actor_user_id, kind, target_type, target_id, state)
         SELECT gen_random_uuid(), u.org_id, u.id, 'create_sandbox', 'sandbox',
                'sbx_' || gen_random_uuid()::text,
                CASE WHEN g = 0 THEN 'queued' ELSE 'succeeded' END
           FROM users u, generate_series(0, $2) g
          WHERE u.email LIKE 'plan-%' AND u.org_id <> $1
         UNION ALL
         SELECT gen_random_uuid(), u.org_id, u.id,
                (ARRAY['delete_sandbox', 'suspend_sandbox'])[1 + g % 2], 'sandbox',
                'sbx_' || gen_random_uuid()::text,
                CASE WHEN g % 2 = 0 THEN 'queued' ELSE 'running' END
           FROM users u, generate_series(1, $3) g
          WHERE u.email LIKE 'plan-%' AND u.org_id <> $1"
    ))
    .bind(org_id)
    .bind(SMALL_ORG_HISTORY)
    .bind(SMALL_ORG_BACKLOG)
    .execute(pool)
    .await?;
    // The counted org: live rows and tombstones, the rows of the creates
    // past their insert, and those creates' operations.
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(), CASE WHEN g > $3 + $4 THEN 'in-flight' END, $2, $1, 'k8s',
                CASE WHEN g <= $3 THEN 'started'
                     WHEN g <= $3 + $4 THEN 'deleted'
                     ELSE 'creating' END,
                1
           FROM generate_series(1, $3 + $4 + $5) g"
    ))
    .bind(org_id)
    .bind(user_id)
    .bind(ORG_LIVE)
    .bind(ORG_TOMBSTONES)
    .bind(ORG_CREATES_WITH_ROWS)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, actor_user_id, kind, target_type, target_id, state)
         SELECT gen_random_uuid(), $1, $2, 'create_sandbox', 'sandbox',
                'sbx_' || id::text, 'running'
           FROM sandboxes
          WHERE org_id = $1 AND name = 'in-flight'
         UNION ALL
         SELECT gen_random_uuid(), $1, $2, 'create_sandbox', 'sandbox',
                'sbx_' || gen_random_uuid()::text, 'queued'
           FROM generate_series(1, $3 - $4)
         UNION ALL
         SELECT gen_random_uuid(), $1, $2,
                (ARRAY['delete_sandbox', 'suspend_sandbox'])[1 + g % 2],
                'sandbox', 'sbx_' || gen_random_uuid()::text, 'queued'
           FROM generate_series(1, $5) g
         UNION ALL
         SELECT gen_random_uuid(), $1, $2,
                (ARRAY['create_sandbox', 'delete_sandbox', 'suspend_sandbox'])[1 + g % 3],
                'sandbox', 'sbx_' || gen_random_uuid()::text, 'succeeded'
           FROM generate_series(1, $6) g"
    ))
    .bind(org_id)
    .bind(user_id)
    .bind(ORG_CREATES)
    .bind(ORG_CREATES_WITH_ROWS)
    .bind(ORG_BACKLOG)
    .bind(ORG_HISTORY)
    .execute(pool)
    .await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;
    sqlx::query(sql!("ANALYZE public_operations"))
        .execute(pool)
        .await?;
    let (org_in_flight, cluster_in_flight): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*) FILTER (WHERE org_id = $1), COUNT(*)
           FROM public_operations
          WHERE state IN ('queued', 'running')"
    ))
    .bind(org_id)
    .fetch_one(pool)
    .await?;
    Ok(Fixture {
        org_id,
        user_id,
        org_rows: u64::try_from(ORG_LIVE + ORG_TOMBSTONES + ORG_CREATES_WITH_ROWS)?,
        org_in_flight: u64::try_from(org_in_flight)?,
        cluster_in_flight: u64::try_from(cluster_in_flight)?,
    })
}
