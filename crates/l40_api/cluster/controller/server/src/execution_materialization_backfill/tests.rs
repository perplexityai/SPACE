use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use super::*;

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by, network_policy_generation
         )
         VALUES ($1, '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001', 'started', 'unplaced', 'k8s', 1)"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    active: bool,
    pod_uid: Option<&str>,
    stamped: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
             pod_materialized_at
         )
         VALUES ($1, $2, $3, 'space', $4, 'running', $5,
                 CASE WHEN $6 THEN TIMESTAMPTZ '2026-01-01T00:00:00Z' END)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "space-sbx-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(pod_uid)
    .bind(active)
    .bind(stamped)
    .execute(pool)
    .await?;
    Ok(())
}

async fn stamp(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT pod_materialized_at::TEXT
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// A lap over the active rows, in windows that resume where the last one
/// ended, stamps each that recorded a pod and has no stamp. It leaves alone an
/// active row with no pod or a blank one, an existing stamp, and retired rows,
/// which keep their `pod_uid`. Its end starts the next lap from the top, which
/// finds nothing left to stamp.
#[tokio::test(flavor = "current_thread")]
async fn a_lap_stamps_only_active_rows_that_recorded_a_pod_without_a_stamp() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let mut sandboxes = Vec::new();
    // (active pod UID, already stamped, a retired row beside it)
    for (pod_uid, stamped, retired) in [
        (Some("pod-a"), false, true),
        (None, false, false),
        (Some("  "), false, false),
        (Some("pod-d"), true, false),
        (Some("pod-e"), false, false),
    ] {
        let sandbox_id = insert_sandbox(pool).await?;
        if retired {
            insert_execution(pool, sandbox_id, 1, false, Some("retired-pod"), false).await?;
        }
        insert_execution(pool, sandbox_id, 2, true, pod_uid, stamped).await?;
        sandboxes.push(sandbox_id);
    }

    let store = BackfillStore::new(pool.clone());
    let config = ReconcilerConfig {
        tick: Duration::from_secs(1),
        window_rows: 2,
        windows_per_tick: 2,
    };
    let (cursor, first) = run_tick(&store, &config, lap_start()).await?;
    assert_ne!(
        cursor,
        lap_start(),
        "two windows of two active rows don't reach the end"
    );
    let (cursor, second) = run_tick(&store, &config, cursor).await?;
    assert_eq!(
        cursor,
        lap_start(),
        "the lap reached the end and starts over"
    );
    assert_eq!(first + second, 2);

    let mut stamped = Vec::new();
    for sandbox_id in &sandboxes {
        stamped.push(stamp(pool, *sandbox_id, 2).await?.is_some());
    }
    assert_eq!(stamped, vec![true, false, false, true, true]);
    assert!(
        stamp(pool, sandboxes[3], 2)
            .await?
            .is_some_and(|stamp| stamp.starts_with("2026-01-01")),
        "an existing stamp isn't moved"
    );
    assert_eq!(
        stamp(pool, sandboxes[0], 1).await?,
        None,
        "a retired row keeps its pod UID and needs no stamp"
    );

    let (_, again) = run_tick(&store, &config, lap_start()).await?;
    assert_eq!(again, 0, "a second lap has nothing left to stamp");
    Ok(())
}
