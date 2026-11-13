//! [`restore_pin_state`] and [`pin_restore_source`] against Postgres: an
//! operator can pin either side of an unordered history, and nothing a
//! later capture provably follows.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEED_ORG, SEED_USER_ID as SEED_USER, try_pg_test_db,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{pin_restore_source, restore_pin_state};

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, template_id, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, 'tmpl-restore-pin-test', 1)"
    ))
    .bind(id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A full backup of `artifact` with `origin`, registered `minutes_ago`.
async fn insert_backup(
    pool: &PgPool,
    sandbox: SandboxId,
    artifact: Uuid,
    origin: &str,
    generation: Option<i64>,
    minutes_ago: i32,
) -> TestResult {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, artifact_snapshot_name, generation, org_id
         )
         VALUES ($1, $2, 'full', 'ready', NOW() - make_interval(mins => $3::INT), $4, 'full',
                 $5, CASE WHEN $5 = 'snapshot' THEN 'named' END, $6, $7)"
    ))
    .bind(snapshot)
    .bind(sandbox)
    .bind(minutes_ago)
    .bind(artifact)
    .bind(origin)
    .bind(generation)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://test/restore-pin?region=us-east-1', $2)"
    ))
    .bind(snapshot)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    Ok(())
}

/// An unordered history lists the latest capture of each side, and an
/// operator can pin either: the capture with a generation, or the later one
/// without. An outranked capture and a named snapshot's backup aren't listed
/// and can't be pinned, and neither can any capture once the row's
/// `lifecycle_changed_at` moved, or while it's pinned or not `suspended`.
#[tokio::test(flavor = "current_thread")]
async fn an_operator_pins_either_side_of_an_unordered_history_and_nothing_older() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let (older, stamped, unstamped, named) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    insert_backup(pool, sandbox, older, "suspend", Some(4), 50).await?;
    insert_backup(pool, sandbox, stamped, "suspend", Some(5), 40).await?;
    insert_backup(pool, sandbox, unstamped, "suspend", None, 30).await?;
    insert_backup(pool, sandbox, named, "snapshot", None, 20).await?;

    let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert!(state.unordered);
    assert_eq!(state.pinned_artifact, None);
    let listed: Vec<Uuid> = state
        .captures
        .iter()
        .map(|capture| capture.local_artifact_id.as_uuid())
        .collect();
    assert_eq!(listed, [unstamped, stamped]);
    let token = state.lifecycle_changed_at.expect("lifecycle_changed_at");

    for refused in [older, named] {
        assert!(
            !pin_restore_source(pool, sandbox, LocalArtifactId::from_uuid(refused), token).await?
        );
    }
    let moved = token - time::Duration::seconds(1);
    assert!(
        !pin_restore_source(pool, sandbox, LocalArtifactId::from_uuid(unstamped), moved).await?
    );
    assert!(pin_restore_source(pool, sandbox, LocalArtifactId::from_uuid(unstamped), token).await?);
    let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert_eq!(state.pinned_artifact.map(|a| a.as_uuid()), Some(unstamped));
    // Pinned: a second pin is refused.
    assert!(!pin_restore_source(pool, sandbox, LocalArtifactId::from_uuid(stamped), token).await?);

    let other = insert_sandbox(pool, "suspended").await?;
    insert_backup(pool, other, stamped, "suspend", Some(5), 40).await?;
    insert_backup(pool, other, unstamped, "suspend", None, 30).await?;
    let token = restore_pin_state(pool, other)
        .await?
        .and_then(|state| state.lifecycle_changed_at)
        .expect("lifecycle_changed_at");
    assert!(pin_restore_source(pool, other, LocalArtifactId::from_uuid(stamped), token).await?);

    let started = insert_sandbox(pool, "started").await?;
    insert_backup(pool, started, unstamped, "suspend", None, 30).await?;
    let token = restore_pin_state(pool, started)
        .await?
        .and_then(|state| state.lifecycle_changed_at)
        .expect("lifecycle_changed_at");
    assert!(
        !pin_restore_source(pool, started, LocalArtifactId::from_uuid(unstamped), token).await?
    );
    Ok(())
}

/// A capture lists the generation any backup of its artifact records, as its
/// rank reads it, though its newest backup, from before backups inherited
/// it, records none.
#[tokio::test(flavor = "current_thread")]
async fn a_capture_lists_the_generation_its_artifact_records() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let artifact = Uuid::now_v7();
    insert_backup(pool, sandbox, artifact, "suspend", Some(5), 30).await?;
    insert_backup(pool, sandbox, artifact, "suspend", None, 10).await?;
    let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    let generations: Vec<Option<i64>> = state
        .captures
        .iter()
        .map(|capture| capture.generation)
        .collect();
    assert_eq!(generations, [Some(5)]);
    Ok(())
}

/// A capture outranked only by a later execution's failed upload can be
/// pinned, and so can one followed only by a failed upload of the same rank:
/// that upload won't land, and the operator chooses. One outranked or
/// followed by an upload still in flight can't.
#[tokio::test(flavor = "current_thread")]
async fn a_capture_outranked_only_by_a_failed_upload_is_pinnable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (later upload's state, its generation, the ready capture is pinnable)
    for (state, generation, pinnable) in [
        ("failed", 6_i64, true),
        ("creating", 6, false),
        ("failed", 5, true),
        ("creating", 5, false),
    ] {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        let ready = Uuid::now_v7();
        insert_backup(pool, sandbox, ready, "suspend", Some(5), 30).await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
                 artifact_origin, generation, org_id
             )
             VALUES ($1, $2, 'full', $3, NOW() - INTERVAL '10 minutes', $4, 'full', 'suspend', $6,
                     $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(sandbox)
        .bind(state)
        .bind(Uuid::now_v7())
        .bind(SEED_ORG)
        .bind(generation)
        .execute(pool)
        .await?;
        let state_now = restore_pin_state(pool, sandbox).await?.expect("sandbox");
        let listed = state_now
            .captures
            .iter()
            .any(|capture| capture.local_artifact_id.as_uuid() == ready);
        assert_eq!(listed, pinnable, "{state}, {generation}");
        let token = state_now
            .lifecycle_changed_at
            .expect("lifecycle_changed_at");
        assert_eq!(
            pin_restore_source(pool, sandbox, LocalArtifactId::from_uuid(ready), token).await?,
            pinnable,
            "{state}, {generation}"
        );
    }
    Ok(())
}
