use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{SnapshotId, SnapshotState};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use std::time::Duration;

use time::OffsetDateTime;

use super::{
    FinalizedAttempt, StuckFullUpload, fail_stuck_full_uploads, finalize_attempt,
    in_flight_idle_for, touch_attempt,
};

async fn insert_snapshot_with_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state)
         VALUES ($1, $2, 'full', $3)"
    ))
    .bind(SnapshotId::new())
    .bind(sandbox_id)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// An upload is in flight when the sandbox has a `creating` snapshot that is
/// not a fork copy: not when it has no snapshots, only `ready` ones, or only a
/// `creating` fork copy.
#[tokio::test(flavor = "current_thread")]
async fn in_flight_idle_for_classifies_states() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 'suspending', 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;

    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_none(),
        "no snapshots → not in flight"
    );

    insert_snapshot_with_state(&db.pool, sandbox_id, "ready").await?;
    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_none(),
        "only 'ready' snapshots → not in flight"
    );

    crate::disk_artifact::insert_creating_fork_copy(
        &db.pool,
        SnapshotId::new(),
        sandbox_id,
        OrgId::from_uuid(SEED_ORG_ID),
    )
    .await?;
    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_none(),
        "only a 'creating' fork copy → not in flight"
    );

    insert_snapshot_with_state(&db.pool, sandbox_id, "creating").await?;
    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_some(),
        "one 'creating' snapshot → in flight"
    );
    Ok(())
}

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// One `snapshots` row to seed: a `creating` full upload `age_secs` old with
/// no heartbeat, no source node and no failure reason, unless overridden.
#[derive(Clone)]
struct Upload {
    snapshot_id: SnapshotId,
    sandbox_id: SandboxId,
    kind: &'static str,
    state: &'static str,
    /// `timestamp = NOW() - age_secs`.
    age_secs: i64,
    /// `last_attempt_at = NOW() - heartbeat_secs`, or NULL when `None`.
    heartbeat_secs: Option<i64>,
    source_node_id: Option<NodeId>,
    size_bytes: Option<i64>,
    failure_reason: Option<&'static str>,
}

impl Upload {
    fn creating_full(sandbox_id: SandboxId, age_secs: i64) -> Self {
        Self {
            snapshot_id: SnapshotId::new(),
            sandbox_id,
            kind: "full",
            state: "creating",
            age_secs,
            heartbeat_secs: None,
            source_node_id: None,
            size_bytes: None,
            failure_reason: None,
        }
    }

    async fn insert(&self, pool: &PgPool) -> TestResult<SnapshotId> {
        sqlx::query(sql!(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp, last_attempt_at,
                 source_node_id, size_bytes, failure_reason)
             VALUES ($1, $2, $3::UUID, $4, $5,
                     NOW() - make_interval(secs => $6::BIGINT),
                     CASE WHEN $7::BIGINT IS NULL THEN NULL
                          ELSE NOW() - make_interval(secs => $7::BIGINT) END,
                     $8, $9, $10)"
        ))
        .bind(self.snapshot_id)
        .bind(self.sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(self.kind)
        .bind(self.state)
        .bind(self.age_secs)
        .bind(self.heartbeat_secs)
        .bind(self.source_node_id)
        .bind(self.size_bytes)
        .bind(self.failure_reason)
        .execute(pool)
        .await?;
        Ok(self.snapshot_id)
    }
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct StoredUpload {
    state: String,
    size_bytes: Option<i64>,
    failure_reason: Option<String>,
    /// As written, not as an age, so comparisons don't depend on when it's
    /// read.
    last_attempt_at: Option<OffsetDateTime>,
}

async fn stored(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult<StoredUpload> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, size_bytes, failure_reason, last_attempt_at
           FROM snapshots
          WHERE id = $1"
    ))
    .bind(snapshot_id)
    .fetch_one(pool)
    .await?)
}

const DAY: Duration = Duration::from_hours(24);
const REASON: &str = "reaped: stuck in creating";

/// A `creating` full upload quiet past `min_age` is failed with the reason and
/// returned with its sandbox and source node.
#[tokio::test(flavor = "current_thread")]
async fn fail_stuck_full_uploads_fails_a_quiet_upload() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let node_id = NodeId::new();
    let snapshot_id = Upload {
        source_node_id: Some(node_id),
        ..Upload::creating_full(sandbox_id, 108_000)
    }
    .insert(&db.pool)
    .await?;

    let failed = fail_stuck_full_uploads(&db.pool, DAY, REASON).await?;

    assert_eq!(
        failed,
        vec![StuckFullUpload {
            snapshot_id,
            sandbox_id,
            source_node_id: Some(node_id),
        }]
    );
    let row = stored(&db.pool, snapshot_id).await?;
    assert_eq!(row.state, "failed");
    assert_eq!(row.failure_reason.as_deref(), Some(REASON));
    Ok(())
}

/// The losing cases: an upload active inside `min_age` (by creation time or by
/// a recent heartbeat on an old row), a full row no longer `creating`, and a
/// `creating` disk row all match nothing and stay as they were.
#[tokio::test(flavor = "current_thread")]
async fn fail_stuck_full_uploads_leaves_active_terminal_and_disk_rows() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let fresh = Upload::creating_full(sandbox_id, 3_600)
        .insert(&db.pool)
        .await?;
    let heartbeated = Upload {
        heartbeat_secs: Some(3_600),
        ..Upload::creating_full(sandbox_id, 108_000)
    }
    .insert(&db.pool)
    .await?;
    let ready = Upload {
        state: "ready",
        ..Upload::creating_full(sandbox_id, 108_000)
    }
    .insert(&db.pool)
    .await?;
    let failed = Upload {
        state: "failed",
        ..Upload::creating_full(sandbox_id, 108_000)
    }
    .insert(&db.pool)
    .await?;
    let disk = Upload {
        kind: "disk",
        ..Upload::creating_full(sandbox_id, 108_000)
    }
    .insert(&db.pool)
    .await?;

    assert!(
        fail_stuck_full_uploads(&db.pool, DAY, REASON)
            .await?
            .is_empty()
    );

    for (snapshot_id, state) in [
        (fresh, "creating"),
        (heartbeated, "creating"),
        (ready, "ready"),
        (failed, "failed"),
        (disk, "creating"),
    ] {
        let row = stored(&db.pool, snapshot_id).await?;
        assert_eq!(row.state, state);
        assert_eq!(row.failure_reason, None);
    }
    Ok(())
}

/// A failure reason an earlier attempt stored is kept, and a heartbeat that is
/// itself older than `min_age` doesn't protect the row.
#[tokio::test(flavor = "current_thread")]
async fn fail_stuck_full_uploads_keeps_a_stored_reason_and_ages_on_the_heartbeat() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let prior = "worker reported: upload permanently failed";
    let snapshot_id = Upload {
        heartbeat_secs: Some(108_000),
        failure_reason: Some(prior),
        ..Upload::creating_full(sandbox_id, 144_000)
    }
    .insert(&db.pool)
    .await?;

    let failed = fail_stuck_full_uploads(&db.pool, DAY, REASON).await?;

    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].snapshot_id, snapshot_id);
    let row = stored(&db.pool, snapshot_id).await?;
    assert_eq!(row.state, "failed");
    assert_eq!(row.failure_reason.as_deref(), Some(prior));
    Ok(())
}

/// Finalizing a `creating` upload `ready` records its size, clears a stored
/// failure reason, and returns its identity; finalizing it `ready` again
/// succeeds again.
#[tokio::test(flavor = "current_thread")]
async fn finalize_attempt_closes_a_creating_upload_and_repeats() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let node_id = NodeId::new();
    let snapshot_id = Upload {
        source_node_id: Some(node_id),
        failure_reason: Some("earlier attempt failed"),
        ..Upload::creating_full(sandbox_id, 60)
    }
    .insert(&db.pool)
    .await?;
    let expected = FinalizedAttempt {
        sandbox_id,
        source_node_id: Some(node_id),
        local_artifact_id: None,
        artifact_kind: None,
    };

    for _ in 0..2 {
        let finalized =
            finalize_attempt(&db.pool, snapshot_id, SnapshotState::Ready, Some(128), None).await?;
        assert_eq!(finalized.as_ref(), Some(&expected));
        let row = stored(&db.pool, snapshot_id).await?;
        assert_eq!(row.state, "ready");
        assert_eq!(row.size_bytes, Some(128));
        assert_eq!(row.failure_reason, None);
    }
    Ok(())
}

/// Finalizing `failed` keeps the stored size when none is given and records
/// the reason, or keeps the stored one when none is given.
#[tokio::test(flavor = "current_thread")]
async fn finalize_attempt_failed_keeps_size_and_stored_reason() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let snapshot_id = Upload {
        size_bytes: Some(64),
        failure_reason: Some("earlier attempt failed"),
        ..Upload::creating_full(sandbox_id, 60)
    }
    .insert(&db.pool)
    .await?;

    finalize_attempt(&db.pool, snapshot_id, SnapshotState::Failed, None, None).await?;
    let row = stored(&db.pool, snapshot_id).await?;
    assert_eq!(row.state, "failed");
    assert_eq!(row.size_bytes, Some(64));
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("earlier attempt failed")
    );

    finalize_attempt(
        &db.pool,
        snapshot_id,
        SnapshotState::Failed,
        None,
        Some("upload failed"),
    )
    .await?;
    let row = stored(&db.pool, snapshot_id).await?;
    assert_eq!(row.failure_reason.as_deref(), Some("upload failed"));
    Ok(())
}

/// The losing cases: an upload already closed in the other terminal state and
/// a missing row match nothing, and the closed row stays as it was.
#[tokio::test(flavor = "current_thread")]
async fn finalize_attempt_refuses_a_crossed_terminal_state_and_a_missing_row() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let snapshot_id = Upload {
        state: "failed",
        failure_reason: Some("upload failed"),
        ..Upload::creating_full(sandbox_id, 60)
    }
    .insert(&db.pool)
    .await?;
    let before = stored(&db.pool, snapshot_id).await?;

    assert_eq!(
        finalize_attempt(&db.pool, snapshot_id, SnapshotState::Ready, Some(128), None).await?,
        None
    );
    assert_eq!(stored(&db.pool, snapshot_id).await?, before);
    assert_eq!(
        finalize_attempt(
            &db.pool,
            SnapshotId::new(),
            SnapshotState::Ready,
            None,
            None
        )
        .await?,
        None
    );
    Ok(())
}

/// A heartbeat stamps a `creating` upload with the database clock.
#[tokio::test(flavor = "current_thread")]
async fn touch_attempt_stamps_a_creating_upload() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let snapshot_id = Upload {
        heartbeat_secs: Some(3_600),
        ..Upload::creating_full(sandbox_id, 7_200)
    }
    .insert(&db.pool)
    .await?;
    let seeded = stored(&db.pool, snapshot_id)
        .await?
        .last_attempt_at
        .ok_or("seeded heartbeat missing")?;

    assert!(touch_attempt(&db.pool, snapshot_id).await?);
    // Seeded an hour before the insert's clock; the stamp is the touch's
    // clock, which is no earlier than the insert's.
    let stamped = stored(&db.pool, snapshot_id).await?.last_attempt_at;
    assert!(stamped.is_some_and(|stamped| stamped >= seeded + time::Duration::HOUR));
    Ok(())
}

/// The losing cases: a closed upload and a missing row match nothing, and the
/// closed row's heartbeat stays as it was.
#[tokio::test(flavor = "current_thread")]
async fn touch_attempt_leaves_a_closed_upload_and_a_missing_row() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let snapshot_id = Upload {
        state: "ready",
        heartbeat_secs: Some(3_600),
        ..Upload::creating_full(sandbox_id, 7_200)
    }
    .insert(&db.pool)
    .await?;
    let before = stored(&db.pool, snapshot_id).await?.last_attempt_at;
    assert!(before.is_some());

    assert!(!touch_attempt(&db.pool, snapshot_id).await?);
    assert_eq!(stored(&db.pool, snapshot_id).await?.last_attempt_at, before);
    assert!(!touch_attempt(&db.pool, SnapshotId::new()).await?);
    Ok(())
}
