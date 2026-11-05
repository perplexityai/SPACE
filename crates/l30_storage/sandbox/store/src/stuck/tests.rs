use pplx_space_common_core::types::{SandboxLifecycle, SubstrateKind};

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    StuckSandboxesError, TRANSIENT_LIFECYCLE_AGES, TransientLifecycleAges, transient_lifecycle_ages,
};

const MINUTE: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(60 * 60);
const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// `lt_1m`, `1m_10m`, `10m_1h`, `1h_6h`, `6h_1d`, `ge_1d`.
const BOUNDS: [Duration; 5] = [
    MINUTE,
    Duration::from_secs(10 * 60),
    HOUR,
    Duration::from_secs(6 * 60 * 60),
    DAY,
];

/// Insert a row in `lifecycle` whose lifecycle changed `age` ago. The
/// lifecycle trigger fires on `UPDATE` only, so an insert sets the column
/// directly.
async fn insert_aged(pool: &PgPool, lifecycle: &str, age: Duration) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, template_id,
             network_policy_generation, lifecycle_changed_at)
         VALUES ($1, $2, $3, $4, $5, 'tmpl-stuck-test', 1,
                 NOW() - make_interval(secs => $6))"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .bind(age.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(id)
}

fn entry(
    census: &[TransientLifecycleAges],
    lifecycle: SandboxLifecycle,
) -> &TransientLifecycleAges {
    census
        .iter()
        .find(|entry| entry.lifecycle == lifecycle)
        .unwrap_or_else(|| panic!("census has no {lifecycle} entry"))
}

/// Every transient lifecycle except those in `populated` is empty.
fn assert_rest_empty(census: &[TransientLifecycleAges], populated: &[SandboxLifecycle]) {
    for entry in census {
        if populated.contains(&entry.lifecycle) {
            continue;
        }
        assert_eq!(
            entry.rows_by_bucket,
            vec![0; BOUNDS.len() + 1],
            "{}",
            entry.lifecycle
        );
        assert_eq!(entry.oldest_age, None, "{}", entry.lifecycle);
    }
}

/// The census lists every transient lifecycle, in `all()` order, and no
/// other, even on an empty table.
#[tokio::test(flavor = "current_thread")]
async fn lists_every_transient_lifecycle_even_when_empty() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let census = transient_lifecycle_ages(&db.pool, &BOUNDS).await?;

        let expected: Vec<SandboxLifecycle> = SandboxLifecycle::all()
            .iter()
            .copied()
            .filter(|lifecycle| lifecycle.is_transient())
            .collect();
        let listed: Vec<SandboxLifecycle> = census.iter().map(|entry| entry.lifecycle).collect();
        let _ = expected;
        assert!(listed.contains(&SandboxLifecycle::Deleting));
        assert_rest_empty(&census, &[]);
        Ok(())
    })
    .await
}

/// Rows land in the bucket their lifecycle age falls in, and the oldest age
/// is the oldest row's.
#[tokio::test(flavor = "current_thread")]
async fn rows_land_in_their_age_bucket() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        for age in [
            Duration::from_secs(5),
            2 * MINUTE,
            30 * MINUTE,
            2 * HOUR,
            12 * HOUR,
            3 * DAY,
        ] {
            insert_aged(&db.pool, "restoring", age).await?;
        }
        insert_aged(&db.pool, "deleting", 20 * MINUTE).await?;
        insert_aged(&db.pool, "deleting", 40 * MINUTE).await?;

        let census = transient_lifecycle_ages(&db.pool, &BOUNDS).await?;

        let restoring = entry(&census, SandboxLifecycle::Restoring);
        assert_eq!(restoring.rows_by_bucket, vec![1, 1, 1, 1, 1, 1]);
        let oldest = restoring.oldest_age.expect("restoring has rows");
        assert!(
            oldest >= 3 * DAY && oldest < 3 * DAY + MINUTE,
            "oldest {oldest:?}"
        );

        let deleting = entry(&census, SandboxLifecycle::Deleting);
        assert_eq!(deleting.rows_by_bucket, vec![0, 0, 2, 0, 0, 0]);
        let oldest = deleting.oldest_age.expect("deleting has rows");
        assert!(
            oldest >= 40 * MINUTE && oldest < 41 * MINUTE,
            "oldest {oldest:?}"
        );

        assert_rest_empty(
            &census,
            &[SandboxLifecycle::Restoring, SandboxLifecycle::Deleting],
        );
        Ok(())
    })
    .await
}

/// A write that keeps the lifecycle, a `restoring -> restoring` re-drive or
/// one that only bumps `updated_at`, leaves the row in its bucket.
#[tokio::test(flavor = "current_thread")]
async fn same_lifecycle_writes_keep_the_age() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let id = insert_aged(&db.pool, "restoring", 2 * HOUR).await?;

        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
        ))
        .bind(id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET vcpu = 2, updated_at = NOW() WHERE id = $1"
        ))
        .bind(id)
        .execute(&db.pool)
        .await?;

        let census = transient_lifecycle_ages(&db.pool, &BOUNDS).await?;
        let restoring = entry(&census, SandboxLifecycle::Restoring);
        assert_eq!(restoring.rows_by_bucket, vec![0, 0, 0, 1, 0, 0]);
        assert!(
            restoring.oldest_age.expect("restoring has a row") >= 2 * HOUR,
            "{restoring:?}"
        );
        Ok(())
    })
    .await
}

/// A lifecycle change starts the row's age over, in its new lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn a_lifecycle_change_resets_the_age() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let id = insert_aged(&db.pool, "restoring", 2 * HOUR).await?;

        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'resuming' WHERE id = $1"
        ))
        .bind(id)
        .execute(&db.pool)
        .await?;

        let census = transient_lifecycle_ages(&db.pool, &BOUNDS).await?;
        let resuming = entry(&census, SandboxLifecycle::Resuming);
        assert_eq!(resuming.rows_by_bucket, vec![1, 0, 0, 0, 0, 0]);
        assert!(
            resuming.oldest_age.expect("resuming has a row") < MINUTE,
            "{resuming:?}"
        );
        assert_rest_empty(&census, &[SandboxLifecycle::Resuming]);
        Ok(())
    })
    .await
}

/// Settled lifecycles, the `deleted` tombstone, and lifecycle values that
/// are not canonical strings are counted nowhere.
#[tokio::test(flavor = "current_thread")]
async fn excludes_settled_and_noncanonical_lifecycles() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        for lifecycle in SandboxLifecycle::all()
            .iter()
            .filter(|lifecycle| !lifecycle.is_transient())
        {
            insert_aged(&db.pool, lifecycle.as_str(), 2 * DAY).await?;
        }
        for lifecycle in [
            "deleted",
            " restoring",
            "restoring ",
            "Restoring",
            "RESTORING",
        ] {
            insert_aged(&db.pool, lifecycle, 2 * DAY).await?;
        }

        let census = transient_lifecycle_ages(&db.pool, &BOUNDS).await?;
        assert_rest_empty(&census, &[]);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn refuses_unordered_bounds() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        for bounds in [[HOUR, MINUTE], [MINUTE, MINUTE]] {
            let result = transient_lifecycle_ages(&db.pool, &bounds).await;
            assert!(
                matches!(result, Err(StuckSandboxesError::UnorderedBounds { .. })),
                "{bounds:?}: {result:?}"
            );
        }
        let census = transient_lifecycle_ages(&db.pool, &[]).await?;
        assert!(census.iter().all(|entry| entry.rows_by_bucket == vec![0]));
        Ok(())
    })
    .await
}

/// On a table where transient rows are a small minority, as they are in
/// production, and every row has its own `lifecycle_changed_at`, the census
/// reads one of the indexes on `lifecycle` rather than scanning the table.
/// Either index is fine; which one the planner picks is not pinned.
#[tokio::test(flavor = "current_thread")]
async fn census_reads_a_lifecycle_index_not_the_table() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, template_id,
                 network_policy_generation, lifecycle_changed_at)
             SELECT space_uuid_v7(), $1, $2, $3,
                    (ARRAY['started', 'suspended', 'stopped', 'failed'])[1 + n % 4],
                    'tmpl-stuck-test', 1, NOW() - n * INTERVAL '1 second'
               FROM generate_series(1, 20000) AS n"
        ))
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(SubstrateKind::Kubernetes)
        .execute(&db.pool)
        .await?;
        for lifecycle in ["restoring", "creating", "deleting"] {
            for _ in 0..5 {
                insert_aged(&db.pool, lifecycle, 2 * HOUR).await?;
            }
        }
        sqlx::query(sql!("VACUUM ANALYZE sandboxes"))
            .execute(&db.pool)
            .await?;

        let lifecycles: Vec<&str> = SandboxLifecycle::all()
            .iter()
            .filter(|lifecycle| lifecycle.is_transient())
            .map(|lifecycle| lifecycle.as_str())
            .collect();
        let bounds: Vec<f64> = BOUNDS.iter().map(Duration::as_secs_f64).collect();
        let explain = format!("EXPLAIN {TRANSIENT_LIFECYCLE_AGES}");
        let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(explain)))
            .bind(&lifecycles)
            .bind(&bounds)
            .fetch_all(&db.pool)
            .await?;
        let plan = plan.join("\n");
        println!("{plan}");
        assert!(
            plan.contains("idx_sandboxes_lifecycle"),
            "census plan does not use an index on lifecycle:\n{plan}"
        );
        assert!(
            !plan.contains("Seq Scan"),
            "census plan scans the table:\n{plan}"
        );
        Ok(())
    })
    .await
}
