//! # Test matrix
//!
//! ## C — `ReconcilerConfig`
//! - C1 `c1_default_is_disabled_with_24h_min_age`
//!
//! ## T — Tick behavior (fake sink)
//! - T1 `t1_no_stuck_rows_emits_empty_report`
//! - T2 `t2_stuck_rows_reaped_into_report`
//! - T3 `t3_db_error_does_not_panic_and_reaps_nothing`
//! - T4 `t4_tick_passes_configured_min_age_to_sink`
//!
//! ## R — Postgres-backed sink (CAS + predicate semantics)
//! - R1 `r1_old_creating_full_snapshot_reaped`
//! - R2 `r2_fresh_creating_row_left_alone`
//! - R3 `r3_non_creating_and_disk_rows_left_alone`
//! - R4 `r4_existing_failure_reason_preserved`
//! - R5 `r5_recent_attempt_heartbeat_protects_old_row`
//! - R6 `r6_stale_attempt_heartbeat_reaped`

use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};

use std::sync::Mutex as StdMutex;
use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;

use super::*;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSinkState {
    /// Rows to return from the next `reap_stuck_creating` call.
    claim: Vec<ReapedSnapshot>,
    /// When true, the next `reap_stuck_creating` returns an error.
    db_error: bool,
    /// Every `min_age` the reconciler asked about, in order.
    min_age_calls: Vec<PositiveDuration>,
}

struct FakeSink(StdMutex<FakeSinkState>);

impl FakeSink {
    fn new() -> Self {
        Self(StdMutex::new(FakeSinkState::default()))
    }

    fn set_claim(&self, rows: Vec<ReapedSnapshot>) {
        self.0.lock().unwrap().claim = rows;
    }

    fn set_db_error(&self) {
        self.0.lock().unwrap().db_error = true;
    }

    fn min_age_calls(&self) -> Vec<PositiveDuration> {
        self.0.lock().unwrap().min_age_calls.clone()
    }
}

impl ReaperSink for FakeSink {
    async fn reap_stuck_creating(
        &self,
        min_age: PositiveDuration,
    ) -> Result<Vec<ReapedSnapshot>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.min_age_calls.push(min_age);
        if state.db_error {
            state.db_error = false;
            return Err(sqlx::Error::PoolClosed);
        }
        Ok(std::mem::take(&mut state.claim))
    }
}

fn enabled_config(min_age_secs: u64) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        tick: DEFAULT_TICK,
        min_age: PositiveDuration::new(Duration::from_secs(min_age_secs)).unwrap(),
    }
}

fn reaped_row() -> ReapedSnapshot {
    ReapedSnapshot {
        snapshot_id: SnapshotId::new(),
        sandbox_id: SandboxId::new(),
        source_node_id: Some(NodeId::new()),
    }
}

// -----------------------------------------------------------------
// C — Config
// -----------------------------------------------------------------

#[test]
fn c1_default_is_disabled_with_24h_min_age() {
    let config = ReconcilerConfig::default();
    assert!(!config.enabled, "reaper must default to disabled");
    assert_eq!(
        config.min_age.get(),
        Duration::from_secs(DEFAULT_MIN_AGE_SECS)
    );
    // The default must sit comfortably above the worker's ~10.13h full
    // retry window so a slow-but-alive upload is never reaped.
    assert!(
        config.min_age.get() >= Duration::from_secs(36_455),
        "default min_age must exceed the worker retry window"
    );
}

// -----------------------------------------------------------------
// T — Tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t1_no_stuck_rows_emits_empty_report() {
    let sink = FakeSink::new();
    let reconciler = Reconciler::new(enabled_config(86_400), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t2_stuck_rows_reaped_into_report() {
    let sink = FakeSink::new();
    sink.set_claim(vec![reaped_row(), reaped_row(), reaped_row()]);
    let reconciler = Reconciler::new(enabled_config(86_400), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.reaped, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn t3_db_error_does_not_panic_and_reaps_nothing() {
    let sink = FakeSink::new();
    sink.set_db_error();
    let reconciler = Reconciler::new(enabled_config(86_400), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t4_tick_passes_configured_min_age_to_sink() {
    let sink = FakeSink::new();
    let reconciler = Reconciler::new(enabled_config(12_345), sink);
    let _ = reconciler.tick().await;
    let calls = reconciler.sink.min_age_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].get(), Duration::from_secs(12_345));
}

// -----------------------------------------------------------------
// R — Postgres-backed sink
// -----------------------------------------------------------------

mod postgres_backed {

    use super::*;
    use crate::stuck_snapshot_reaper::store::SnapshotDbStore;
    use pplx_space_common_core::types::SnapshotId;
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
    use sqlx::PgPool;

    use pplx_space_test_env::TestResult;

    async fn insert_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
        // `network_policy_generation` is explicit because migration 034 drops
        // the column DEFAULT after backfill; the snapshots composite FK
        // `(sandbox_id, org_id)` needs the sandbox to carry the seed org.
        sqlx::query(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
             VALUES ($1, $2::uuid, $3::uuid, 'space', 'suspended', 1)",
        )
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// One `snapshots` row to seed. Built via [`SnapshotSpec::creating_full`]
    /// (the common case) with field overrides through struct-update syntax, so
    /// the insert helper takes a single value rather than a positional
    /// argument soup.
    #[derive(Clone)]
    struct SnapshotSpec {
        snapshot_id: SnapshotId,
        sandbox_id: SandboxId,
        snapshot_type: &'static str,
        state: &'static str,
        /// `timestamp = NOW() - age_offset_secs`.
        age_offset_secs: i64,
        /// `last_attempt_at = NOW() - last_attempt_offset_secs`, or NULL when
        /// `None` (no recorded attempt heartbeat — pre-heartbeat-writer rows).
        last_attempt_offset_secs: Option<i64>,
        source_node_id: Option<NodeId>,
        failure_reason: Option<&'static str>,
    }

    impl SnapshotSpec {
        /// A `type='full'`, `state='creating'` row whose `timestamp` is
        /// `age_offset_secs` in the past, with no attempt heartbeat (NULL
        /// `last_attempt_at`) and no source node / failure reason.
        fn creating_full(
            snapshot_id: SnapshotId,
            sandbox_id: SandboxId,
            age_offset_secs: i64,
        ) -> Self {
            Self {
                snapshot_id,
                sandbox_id,
                snapshot_type: "full",
                state: "creating",
                age_offset_secs,
                last_attempt_offset_secs: None,
                source_node_id: None,
                failure_reason: None,
            }
        }
    }

    async fn insert_snapshot(pool: &PgPool, spec: SnapshotSpec) -> TestResult {
        sqlx::query(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp, last_attempt_at,
                 source_node_id, failure_reason)
             VALUES ($1, $2, $3::uuid, $4, $5,
                     NOW() - make_interval(secs => $6::BIGINT),
                     CASE WHEN $7::BIGINT IS NULL THEN NULL
                          ELSE NOW() - make_interval(secs => $7::BIGINT) END,
                     $8, $9)",
        )
        .bind(spec.snapshot_id)
        .bind(spec.sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(spec.snapshot_type)
        .bind(spec.state)
        .bind(spec.age_offset_secs)
        .bind(spec.last_attempt_offset_secs)
        .bind(spec.source_node_id)
        .bind(spec.failure_reason)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn snapshot_state(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult<String> {
        Ok(
            sqlx::query_scalar("SELECT state FROM snapshots WHERE id = $1")
                .bind(snapshot_id)
                .fetch_one(pool)
                .await?,
        )
    }

    /// A `creating` full snapshot older than `min_age` is flipped to
    /// `failed`, returned with its sandbox/source-node identity, and stamped
    /// with the reap reason.
    #[tokio::test(flavor = "current_thread")]
    async fn r1_old_creating_full_snapshot_reaped() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        let node_id = NodeId::new();
        insert_sandbox(&db.pool, sandbox_id).await?;
        // Stuck for 30h — past the 24h default.
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                source_node_id: Some(node_id),
                ..SnapshotSpec::creating_full(snapshot_id, sandbox_id, 108_000)
            },
        )
        .await?;

        let store = SnapshotDbStore::new(db.pool.clone());
        let min_age = PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap();
        let reaped = store.reap_stuck_creating(min_age).await?;

        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].snapshot_id, snapshot_id);
        assert_eq!(reaped[0].sandbox_id, sandbox_id);
        assert_eq!(reaped[0].source_node_id, Some(node_id));

        assert_eq!(snapshot_state(&db.pool, snapshot_id).await?, "failed");
        let reason: Option<String> =
            sqlx::query_scalar("SELECT failure_reason FROM snapshots WHERE id = $1")
                .bind(snapshot_id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(reason.as_deref(), Some(REAP_FAILURE_REASON));
        Ok(())
    }

    /// A `creating` row younger than `min_age` (a slow-but-alive upload) is
    /// never reaped.
    #[tokio::test(flavor = "current_thread")]
    async fn r2_fresh_creating_row_left_alone() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        insert_sandbox(&db.pool, sandbox_id).await?;
        // Only 1h old — well within the worker retry window.
        insert_snapshot(
            &db.pool,
            SnapshotSpec::creating_full(snapshot_id, sandbox_id, 3_600),
        )
        .await?;

        let store = SnapshotDbStore::new(db.pool.clone());
        let min_age = PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap();
        let reaped = store.reap_stuck_creating(min_age).await?;

        assert!(reaped.is_empty(), "fresh upload must not be reaped");
        assert_eq!(snapshot_state(&db.pool, snapshot_id).await?, "creating");
        Ok(())
    }

    /// Terminal rows (`ready` / `failed`) and stuck *disk* snapshots are out
    /// of scope — only old `creating` *full* rows are reaped.
    #[tokio::test(flavor = "current_thread")]
    async fn r3_non_creating_and_disk_rows_left_alone() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        insert_sandbox(&db.pool, sandbox_id).await?;

        let ready = SnapshotId::new();
        let failed = SnapshotId::new();
        let disk = SnapshotId::new();
        // All old enough to age out if the predicate were wrong.
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                state: "ready",
                ..SnapshotSpec::creating_full(ready, sandbox_id, 108_000)
            },
        )
        .await?;
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                state: "failed",
                ..SnapshotSpec::creating_full(failed, sandbox_id, 108_000)
            },
        )
        .await?;
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                snapshot_type: "disk",
                ..SnapshotSpec::creating_full(disk, sandbox_id, 108_000)
            },
        )
        .await?;

        let store = SnapshotDbStore::new(db.pool.clone());
        let min_age = PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap();
        let reaped = store.reap_stuck_creating(min_age).await?;

        assert!(reaped.is_empty(), "no full creating rows to reap");
        assert_eq!(snapshot_state(&db.pool, ready).await?, "ready");
        assert_eq!(snapshot_state(&db.pool, failed).await?, "failed");
        assert_eq!(snapshot_state(&db.pool, disk).await?, "creating");
        Ok(())
    }

    /// A `failure_reason` recorded by an earlier best-effort worker attempt is
    /// preserved (COALESCE), not overwritten by the reap reason.
    #[tokio::test(flavor = "current_thread")]
    async fn r4_existing_failure_reason_preserved() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        insert_sandbox(&db.pool, sandbox_id).await?;
        let prior = "worker reported: vol-mgr upload permanently failed";
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                failure_reason: Some(prior),
                ..SnapshotSpec::creating_full(snapshot_id, sandbox_id, 108_000)
            },
        )
        .await?;

        let store = SnapshotDbStore::new(db.pool.clone());
        let min_age = PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap();
        let reaped = store.reap_stuck_creating(min_age).await?;

        assert_eq!(reaped.len(), 1);
        assert_eq!(snapshot_state(&db.pool, snapshot_id).await?, "failed");
        let reason: Option<String> =
            sqlx::query_scalar("SELECT failure_reason FROM snapshots WHERE id = $1")
                .bind(snapshot_id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(reason.as_deref(), Some(prior));
        Ok(())
    }

    /// A row whose `timestamp` is ancient but whose `last_attempt_at` is recent
    /// (the upload recovered and resumed after a long node outage) is NOT
    /// reaped — the heartbeat anchor pauses the reaper's clock alongside the
    /// worker's retry clock.
    #[tokio::test(flavor = "current_thread")]
    async fn r5_recent_attempt_heartbeat_protects_old_row() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        insert_sandbox(&db.pool, sandbox_id).await?;
        // Created 30h ago, but a retry attempt heartbeated 1h ago.
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                last_attempt_offset_secs: Some(3_600),
                ..SnapshotSpec::creating_full(snapshot_id, sandbox_id, 108_000)
            },
        )
        .await?;

        let store = SnapshotDbStore::new(db.pool.clone());
        let min_age = PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap();
        let reaped = store.reap_stuck_creating(min_age).await?;

        assert!(
            reaped.is_empty(),
            "a recently-heartbeated upload must not be reaped despite an old timestamp"
        );
        assert_eq!(snapshot_state(&db.pool, snapshot_id).await?, "creating");
        Ok(())
    }

    /// A row whose `last_attempt_at` is itself older than `min_age` (the worker
    /// is truly gone — no recent attempt) IS reaped, confirming the heartbeat,
    /// not `timestamp`, is the anchor when present.
    #[tokio::test(flavor = "current_thread")]
    async fn r6_stale_attempt_heartbeat_reaped() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        insert_sandbox(&db.pool, sandbox_id).await?;
        // Created 40h ago, last heartbeat 30h ago — both past the 24h default.
        insert_snapshot(
            &db.pool,
            SnapshotSpec {
                last_attempt_offset_secs: Some(108_000),
                ..SnapshotSpec::creating_full(snapshot_id, sandbox_id, 144_000)
            },
        )
        .await?;

        let store = SnapshotDbStore::new(db.pool.clone());
        let min_age = PositiveDuration::new(Duration::from_secs(DEFAULT_MIN_AGE_SECS)).unwrap();
        let reaped = store.reap_stuck_creating(min_age).await?;

        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].snapshot_id, snapshot_id);
        assert_eq!(snapshot_state(&db.pool, snapshot_id).await?, "failed");
        Ok(())
    }
}
