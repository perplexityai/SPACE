use std::time::Duration;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{LIST_SQL, SCAN_EPOCH_SQL, list, scan_epoch};

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A `full` snapshot in `state`, dated `age` ago. `route` adds its storage
/// location and `encrypted` its encrypted-fields sidecar.
async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
    age: Duration,
    route: Option<&str>,
    encrypted: bool,
) -> TestResult<(SnapshotId, Option<Uuid>)> {
    let id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', $3, NOW() - make_interval(secs => $4::BIGINT), $5::UUID)"
    ))
    .bind(id)
    .bind(sandbox_id)
    .bind(state)
    .bind(age.as_secs().cast_signed())
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    let Some(route) = route else {
        return Ok((id, None));
    };
    let location_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3::UUID)
         RETURNING id"
    ))
    .bind(id)
    .bind(route)
    .bind(SEED_ORG_ID)
    .fetch_one(pool)
    .await?;
    if encrypted {
        let master_key_id: Uuid = sqlx::query_scalar(sql!(
            "INSERT INTO org_master_keys (org_id, provider, key_identifier)
             VALUES ($1::UUID, 'test', 'test-key')
             RETURNING id"
        ))
        .bind(SEED_ORG_ID)
        .fetch_one(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_location_encrypted_fields
                 (snapshot_storage_location_id, org_id, master_key_id, wrapped_dek,
                  storage_connection_string)
             VALUES ($1, $2::UUID, $3, '\\x01', '\\x02')"
        ))
        .bind(location_id)
        .bind(SEED_ORG_ID)
        .bind(master_key_id)
        .execute(pool)
        .await?;
    }
    Ok((id, Some(location_id)))
}

#[tokio::test(flavor = "current_thread")]
async fn claims_page_in_id_order_with_their_location_and_age() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    assert_eq!(scan_epoch(pool).await?, None);

    let age = Duration::from_secs(3_600);
    let (plain, plain_location) =
        insert_snapshot(pool, sandbox, "deleting", age, Some("s3://b/plain"), false).await?;
    let (sealed, sealed_location) =
        insert_snapshot(pool, sandbox, "deleting", age, Some("s3://b/sealed"), true).await?;
    let (bare, _) = insert_snapshot(pool, sandbox, "deleting", age, None, false).await?;
    // Neither a `ready` row nor a claim above the bound belongs to the epoch.
    insert_snapshot(pool, sandbox, "ready", age, Some("s3://b/ready"), false).await?;
    let epoch = scan_epoch(pool).await?.expect("three claims");
    assert_eq!(epoch, bare);
    insert_snapshot(pool, sandbox, "deleting", age, None, false).await?;

    let rows = list(pool, 10, None, epoch).await?;
    assert_eq!(
        rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        [plain, sealed, bare]
    );
    assert!(rows.iter().all(|r| r.sandbox_id == sandbox));
    assert_eq!(rows[0].storage_location_id, plain_location);
    assert_eq!(
        rows[0].storage_connection_string.as_deref(),
        Some("s3://b/plain")
    );
    assert!(!rows[0].has_encrypted_fields);
    assert_eq!(rows[1].storage_location_id, sealed_location);
    assert!(rows[1].has_encrypted_fields);
    assert_eq!(rows[2].storage_location_id, None);
    assert_eq!(rows[2].storage_connection_string, None);
    assert!(!rows[2].has_encrypted_fields);
    // Ages come from the database clock, whole seconds.
    assert!(rows.iter().all(|r| (3_600..3_660).contains(&r.age_secs)));

    let first = list(pool, 1, None, epoch).await?;
    assert_eq!(first[0].snapshot_id, plain);
    let rest = list(pool, 10, Some(plain), epoch).await?;
    assert_eq!(
        rest.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        [sealed, bare]
    );
    assert!(list(pool, 10, Some(bare), epoch).await?.is_empty());
    Ok(())
}

/// Claimed recovery must not scale with the snapshot table. Statistics
/// sampled while a tick holds many claims, followed by those claims
/// finishing forward, leave a few old (low UUIDv7) claims under a large
/// `deleting` estimate. Unfenced, `ORDER BY id DESC LIMIT 1` then walks the
/// primary key backward and filters every unclaimed row before it reaches
/// one, which is how the epoch query exceeds the statement timeout.
#[tokio::test(flavor = "current_thread")]
async fn claimed_recovery_reads_claims_not_the_table_under_stale_statistics() -> TestResult {
    const ROWS: i64 = 50_000;
    const SAMPLED_CLAIMS: i64 = 1_000;
    const LIVE_CLAIMS: usize = 10;
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
         SELECT ('20000000-0000-7000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
                $1, 'full',
                CASE WHEN g <= $3 THEN 'deleting' ELSE 'ready' END,
                NOW() - make_interval(secs => ($2 - g + 1)::double precision)
           FROM generate_series(1, $2) g"
    ))
    .bind(sandbox)
    .bind(ROWS)
    .bind(SAMPLED_CLAIMS)
    .execute(pool)
    .await?;
    sqlx::query(sql!("ANALYZE snapshots")).execute(pool).await?;
    let claims: Vec<SnapshotId> = sqlx::query_scalar(sql!(
        "SELECT id FROM snapshots WHERE state = 'deleting' ORDER BY id LIMIT $1"
    ))
    .bind(LIVE_CLAIMS as i64)
    .fetch_all(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE state = 'deleting' AND id > $1"
    ))
    .bind(claims[LIVE_CLAIMS - 1])
    .execute(pool)
    .await?;

    let through = scan_epoch(pool).await?.expect("live claims");
    assert_eq!(through, claims[LIVE_CLAIMS - 1]);
    let page = list(pool, ROWS, None, through).await?;
    assert_eq!(
        page.iter().map(|c| c.snapshot_id).collect::<Vec<_>>(),
        claims
    );

    let filtered = |plan: Vec<String>| -> i64 {
        plan.iter()
            .filter_map(|line| line.split("Rows Removed by Filter: ").nth(1))
            .map(|n| n.trim().parse::<i64>().expect("filtered row count"))
            .sum()
    };
    let explain = |statement: &str| {
        format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {statement}")
    };
    let epoch_plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(explain(SCAN_EPOCH_SQL)))
        .fetch_all(pool)
        .await?;
    let list_plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(explain(LIST_SQL)))
        .bind(ROWS)
        .bind(None::<SnapshotId>)
        .bind(through)
        .fetch_all(pool)
        .await?;
    for plan in [epoch_plan, list_plan] {
        let shown = plan.join("\n");
        assert!(filtered(plan) < SAMPLED_CLAIMS, "{shown}");
    }
    Ok(())
}
