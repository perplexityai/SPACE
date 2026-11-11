//! Postgres-backed truth-table tests for the shared portability predicate
//! defined in `pplx_space_sandbox_portability`.
//!
//! Each test inserts a sandbox row with a controlled (lifecycle,
//! `placement_changed_at`) tuple plus zero or one `snapshots` /
//! `snapshot_storage_locations` rows, then evaluates the predicate via a
//! direct `SELECT ...` against the live Postgres so the SQL fragment is
//! actually parsed and executed by the same database engine that runs in
//! production. The end-to-end callsite tests (`drain_guard_postgres_tests`
//! in `lifecycle.rs`) cover the predicate's wiring through
//! `fetch_active_node_drain_state`; these tests pin the predicate itself.

#![cfg(test)]

use pplx_space_common_core::types::{SandboxId, SnapshotId};

use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_sandbox_portability::{
    fresh_ready_snapshot_exists, portable_for_drain_completion, portable_for_restore_affinity,
};
use sqlx::PgPool;
use time::{Duration as TimeDuration, OffsetDateTime};
use uuid::Uuid;

use pplx_space_test_env::TestResult;

async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    placement_changed_at: Option<OffsetDateTime>,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             placement_changed_at,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'unplaced', 'k8s',
             $3,
             $4, 1
         )",
    )
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(placement_changed_at)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a snapshot row at the given timestamp, plus the
/// `snapshot_storage_locations` join row that the predicate requires.
async fn insert_snapshot_with_location(
    pool: &PgPool,
    sandbox_id: SandboxId,
    snapshot_type: &str,
    snapshot_state: &str,
    timestamp: OffsetDateTime,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(snapshot_type)
    .bind(snapshot_state)
    .bind(timestamp)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)",
    )
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

async fn eval_portable_for_drain_completion(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<bool> {
    let predicate = portable_for_drain_completion("s");
    let query = format!("SELECT {predicate} FROM sandboxes s WHERE s.id = $1");
    let result: Option<bool> = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
        .bind(sandbox_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.unwrap_or(false))
}

async fn eval_portable_for_restore_affinity(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<bool> {
    let predicate = portable_for_restore_affinity("s");
    let query = format!("SELECT {predicate} FROM sandboxes s WHERE s.id = $1");
    let result: Option<bool> = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
        .bind(sandbox_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.unwrap_or(false))
}

async fn eval_fresh_ready_snapshot_exists(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<bool> {
    let predicate = fresh_ready_snapshot_exists("s");
    let query = format!("SELECT {predicate} FROM sandboxes s WHERE s.id = $1");
    let result: Option<bool> = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
        .bind(sandbox_id)
        .fetch_optional(pool)
        .await?;
    Ok(result.unwrap_or(false))
}

/// Drives the full `lifecycle × snapshot-state × freshness ×
/// placement_changed_at IS NULL` truth table on a single fixture. Each row
/// is checked against both portability predicates plus the inner freshness
/// fragment so a regression in any one of them surfaces here.
#[tokio::test(flavor = "current_thread")]
async fn portability_predicates_truth_table() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let now = OffsetDateTime::now_utc();
    let fresh_ts = now - TimeDuration::seconds(30);
    let stale_ts = now - TimeDuration::hours(2);
    let placement_changed = now - TimeDuration::hours(1);

    // `SnapshotSetup` encodes the inserted snapshot rows; `Case` records the
    // 4-dimensional input plus the expected predicate outputs so each row in
    // the truth table reads as a single line.
    #[derive(Debug, Clone, Copy)]
    enum SnapshotSetup {
        None,
        FullCreatingFresh,
        FullReadyStale,
        FullReadyFresh,
        DiskReadyFresh,
    }

    #[derive(Debug, Clone, Copy)]
    struct Case {
        lifecycle: &'static str,
        placement_changed_at: Option<OffsetDateTime>,
        snapshot: SnapshotSetup,
        expect_drain: bool,
        expect_restore: bool,
        expect_fresh: bool,
    }

    let case =
        |lifecycle, placement_changed_at, snapshot, expect_drain, expect_restore, expect_fresh| {
            Case {
                lifecycle,
                placement_changed_at,
                snapshot,
                expect_drain,
                expect_restore,
                expect_fresh,
            }
        };

    let cases = &[
        // === lifecycle = 'suspended', placement_changed_at IS NULL ===
        // Legacy rows: any ready full snapshot counts as fresh.
        case("suspended", None, SnapshotSetup::None, false, false, false),
        case(
            "suspended",
            None,
            SnapshotSetup::FullReadyFresh,
            true,
            true,
            true,
        ),
        case(
            "suspended",
            None,
            SnapshotSetup::FullReadyStale,
            true,
            true,
            true,
        ),
        case(
            "suspended",
            None,
            SnapshotSetup::FullCreatingFresh,
            false,
            false,
            false,
        ),
        case(
            "suspended",
            None,
            SnapshotSetup::DiskReadyFresh,
            false,
            false,
            false,
        ),
        // === lifecycle = 'suspended', placement_changed_at set ===
        // Freshness gate kicks in: stale snapshots no longer count.
        case(
            "suspended",
            Some(placement_changed),
            SnapshotSetup::None,
            false,
            false,
            false,
        ),
        case(
            "suspended",
            Some(placement_changed),
            SnapshotSetup::FullReadyFresh,
            true,
            true,
            true,
        ),
        case(
            "suspended",
            Some(placement_changed),
            SnapshotSetup::FullReadyStale,
            false,
            false,
            false,
        ),
        case(
            "suspended",
            Some(placement_changed),
            SnapshotSetup::FullCreatingFresh,
            false,
            false,
            false,
        ),
        case(
            "suspended",
            Some(placement_changed),
            SnapshotSetup::DiskReadyFresh,
            false,
            false,
            false,
        ),
        // === lifecycle = 'restoring' ===
        // Restore-affinity widens to include 'restoring'; drain-completion stays strict.
        case(
            "restoring",
            Some(placement_changed),
            SnapshotSetup::FullReadyFresh,
            false,
            true,
            true,
        ),
        case(
            "restoring",
            Some(placement_changed),
            SnapshotSetup::FullReadyStale,
            false,
            false,
            false,
        ),
        case(
            "restoring",
            Some(placement_changed),
            SnapshotSetup::None,
            false,
            false,
            false,
        ),
        // === lifecycle = 'started' (live VM state is canonical, snapshot is stale) ===
        case(
            "started",
            Some(placement_changed),
            SnapshotSetup::FullReadyFresh,
            false,
            false,
            true,
        ),
        case(
            "started",
            Some(placement_changed),
            SnapshotSetup::None,
            false,
            false,
            false,
        ),
        // === lifecycle = 'paused' (same reasoning as 'started') ===
        case(
            "paused",
            Some(placement_changed),
            SnapshotSetup::FullReadyFresh,
            false,
            false,
            true,
        ),
        // === lifecycle = 'suspending' (transitional; no portable snapshot yet) ===
        case(
            "suspending",
            Some(placement_changed),
            SnapshotSetup::None,
            false,
            false,
            false,
        ),
        case(
            "suspending",
            Some(placement_changed),
            SnapshotSetup::FullReadyFresh,
            false,
            false,
            true,
        ),
        // === lifecycle = 'creating' (no snapshot path applies) ===
        case("creating", None, SnapshotSetup::None, false, false, false),
    ];

    for (index, case) in cases.iter().enumerate() {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, case.lifecycle, case.placement_changed_at).await?;
        match case.snapshot {
            SnapshotSetup::None => {}
            SnapshotSetup::FullCreatingFresh => {
                insert_snapshot_with_location(pool, sandbox_id, "full", "creating", fresh_ts)
                    .await?;
            }
            SnapshotSetup::FullReadyStale => {
                insert_snapshot_with_location(pool, sandbox_id, "full", "ready", stale_ts).await?;
            }
            SnapshotSetup::FullReadyFresh => {
                insert_snapshot_with_location(pool, sandbox_id, "full", "ready", fresh_ts).await?;
            }
            SnapshotSetup::DiskReadyFresh => {
                insert_snapshot_with_location(pool, sandbox_id, "disk", "ready", fresh_ts).await?;
            }
        }

        let fresh = eval_fresh_ready_snapshot_exists(pool, sandbox_id).await?;
        let drain = eval_portable_for_drain_completion(pool, sandbox_id).await?;
        let restore = eval_portable_for_restore_affinity(pool, sandbox_id).await?;
        assert_eq!(
            (fresh, drain, restore),
            (case.expect_fresh, case.expect_drain, case.expect_restore),
            "case[{index}] {case:?}"
        );
    }

    Ok(())
}

/// Two ready full snapshots whose timestamps straddle `placement_changed_at`
/// still let portability pass: the fresh-ready check is an `EXISTS`, so a
/// stale row alongside a fresh row must not poison the result.
#[tokio::test(flavor = "current_thread")]
async fn portability_passes_when_any_snapshot_is_fresh_even_if_others_are_stale() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let now = OffsetDateTime::now_utc();
    let placement_changed = now - TimeDuration::hours(1);
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "suspended", Some(placement_changed)).await?;
    insert_snapshot_with_location(
        pool,
        sandbox_id,
        "full",
        "ready",
        now - TimeDuration::hours(3),
    )
    .await?;
    insert_snapshot_with_location(
        pool,
        sandbox_id,
        "full",
        "ready",
        now - TimeDuration::seconds(30),
    )
    .await?;

    assert!(eval_portable_for_drain_completion(pool, sandbox_id).await?);
    assert!(eval_portable_for_restore_affinity(pool, sandbox_id).await?);
    Ok(())
}

/// Boundary case: a ready snapshot whose `timestamp` exactly equals
/// `placement_changed_at` must count as fresh — the predicate is `>=`, not
/// `>`. A snapshot registered "at the same moment as" the placement update
/// is the typical path during a normal suspend → upload → register flow.
#[tokio::test(flavor = "current_thread")]
async fn portability_treats_equal_timestamps_as_fresh() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let placement_changed = OffsetDateTime::now_utc() - TimeDuration::hours(1);
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "suspended", Some(placement_changed)).await?;
    insert_snapshot_with_location(pool, sandbox_id, "full", "ready", placement_changed).await?;

    assert!(eval_portable_for_drain_completion(pool, sandbox_id).await?);
    assert!(eval_portable_for_restore_affinity(pool, sandbox_id).await?);
    Ok(())
}

/// A ready full snapshot without a matching `snapshot_storage_locations`
/// row is **not** portable. The snapshot row alone proves nothing about S3
/// presence; the location row is the durability signal the drain path
/// gates on.
#[tokio::test(flavor = "current_thread")]
async fn portability_requires_snapshot_storage_location_row() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let placement_changed = OffsetDateTime::now_utc() - TimeDuration::hours(1);
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "suspended", Some(placement_changed)).await?;

    let snapshot_id = SnapshotId::new();
    sqlx::query(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
         VALUES ($1, $2, 'full', 'ready', NOW())",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    // Deliberately omit the snapshot_storage_locations insert.

    assert!(!eval_portable_for_drain_completion(pool, sandbox_id).await?);
    assert!(!eval_portable_for_restore_affinity(pool, sandbox_id).await?);
    assert!(!eval_fresh_ready_snapshot_exists(pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn portability_requires_backup_for_the_current_local_artifact() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    let previous_artifact_id = Uuid::now_v7();
    let current_artifact_id = Uuid::now_v7();
    insert_sandbox(pool, sandbox_id, "suspended", None).await?;
    sqlx::query(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .bind(current_artifact_id)
    .execute(pool)
    .await?;

    let previous_backup_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES ($1, $2, 'full', 'ready', NOW(), $3, 'full', 'suspend')",
    )
    .bind(previous_backup_id)
    .bind(sandbox_id)
    .bind(previous_artifact_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/previous-artifact')",
    )
    .bind(previous_backup_id)
    .execute(pool)
    .await?;

    assert!(!eval_fresh_ready_snapshot_exists(pool, sandbox_id).await?);

    let current_backup_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES ($1, $2, 'full', 'ready', NOW(), $3, 'full', 'suspend')",
    )
    .bind(current_backup_id)
    .bind(sandbox_id)
    .bind(current_artifact_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/current-artifact')",
    )
    .bind(current_backup_id)
    .execute(pool)
    .await?;

    assert!(eval_fresh_ready_snapshot_exists(pool, sandbox_id).await?);
    assert!(eval_portable_for_drain_completion(pool, sandbox_id).await?);
    Ok(())
}
