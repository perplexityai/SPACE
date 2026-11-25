use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxActivationAttemptId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::super::ownership_test_fixture::{
    execution_key_share_blocked, execution_state, execution_version, hold_execution,
    insert_execution, insert_sandbox, is_lock_not_available,
};
use super::{lock_active_nowait, note_failed_cleanup_attempt, retire_reverted_restore_execution};

#[tokio::test(flavor = "current_thread")]
async fn note_failed_cleanup_attempt_bumps_only_a_failed_kubernetes_rows_active_execution()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let failed = insert_sandbox(pool, "failed").await?;
    let failed_execution = insert_execution(pool, failed, 1, "running", true, None).await?;
    let started = insert_sandbox(pool, "started").await?;
    let started_execution = insert_execution(pool, started, 1, "running", true, None).await?;
    let retired = insert_sandbox(pool, "failed").await?;
    let retired_execution = insert_execution(pool, retired, 1, "failed", false, None).await?;
    let versions = [
        execution_version(pool, failed_execution.id).await?,
        execution_version(pool, started_execution.id).await?,
        execution_version(pool, retired_execution.id).await?,
    ];

    // The losing cases: another substrate, a lifecycle other than `failed`,
    // no active execution. Nothing changes.
    assert_eq!(
        note_failed_cleanup_attempt(pool, failed, SubstrateKind::Space).await?,
        0
    );
    assert_eq!(
        note_failed_cleanup_attempt(pool, started, SubstrateKind::Kubernetes).await?,
        0
    );
    assert_eq!(
        note_failed_cleanup_attempt(pool, retired, SubstrateKind::Kubernetes).await?,
        0
    );
    assert_eq!(
        [
            execution_version(pool, failed_execution.id).await?,
            execution_version(pool, started_execution.id).await?,
            execution_version(pool, retired_execution.id).await?,
        ],
        versions
    );

    assert_eq!(
        note_failed_cleanup_attempt(pool, failed, SubstrateKind::Kubernetes).await?,
        1
    );
    assert_ne!(
        execution_version(pool, failed_execution.id).await?,
        versions[0]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retire_reverted_restore_execution_fences_on_the_recorded_identity() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "suspended").await?;
    let execution = insert_execution(pool, sandbox_id, 2, "running", true, Some(" uid-2 ")).await?;
    let attempt = SandboxActivationAttemptId::from_uuid(Uuid::now_v7());
    let route = SandboxActivationAttemptId::from_uuid(Uuid::now_v7());
    let other = SandboxActivationAttemptId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2, route_activation_id = $3
          WHERE id = $1"
    ))
    .bind(execution.id)
    .bind(attempt)
    .bind(route)
    .execute(pool)
    .await?;
    let before = execution_version(pool, execution.id).await?;
    let pod_name = execution.pod_name.as_str();

    // The losing cases: any one field of the identity differs.
    for (pod_name, generation, attempt_id, pod_uid, route_id) in [
        (
            "sbx-other",
            Some(2),
            Some(attempt),
            Some("uid-2"),
            Some(route),
        ),
        (pod_name, Some(3), Some(attempt), Some("uid-2"), Some(route)),
        (pod_name, None, Some(attempt), Some("uid-2"), Some(route)),
        (pod_name, Some(2), Some(other), Some("uid-2"), Some(route)),
        (pod_name, Some(2), None, Some("uid-2"), Some(route)),
        (
            pod_name,
            Some(2),
            Some(attempt),
            Some("uid-other"),
            Some(route),
        ),
        (pod_name, Some(2), Some(attempt), None, Some(route)),
        (pod_name, Some(2), Some(attempt), Some("uid-2"), Some(other)),
        (pod_name, Some(2), Some(attempt), Some("uid-2"), None),
    ] {
        assert!(
            !retire_reverted_restore_execution(
                pool, sandbox_id, pod_name, generation, attempt_id, pod_uid, route_id,
            )
            .await?
        );
    }
    assert_eq!(execution_version(pool, execution.id).await?, before);

    // The stored UID is compared trimmed.
    assert!(
        retire_reverted_restore_execution(
            pool,
            sandbox_id,
            pod_name,
            Some(2),
            Some(attempt),
            Some("uid-2"),
            Some(route),
        )
        .await?
    );
    assert_eq!(
        execution_state(pool, execution.id).await?,
        (false, "replaced".to_string())
    );
    // Retired, it no longer matches `active`.
    let retired = execution_version(pool, execution.id).await?;
    assert!(
        !retire_reverted_restore_execution(
            pool,
            sandbox_id,
            pod_name,
            Some(2),
            Some(attempt),
            Some("uid-2"),
            Some(route),
        )
        .await?
    );
    assert_eq!(execution_version(pool, execution.id).await?, retired);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lock_active_nowait_locks_the_active_execution_for_update_or_fails_fast() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "restoring").await?;
    let retired = insert_execution(pool, sandbox_id, 1, "replaced", false, None).await?;
    let active = insert_execution(pool, sandbox_id, 2, "running", true, None).await?;

    let mut tx = pool.begin().await?;
    lock_active_nowait(&mut tx, sandbox_id).await?;
    // `FOR UPDATE`: even a key-share lock waits, until the transaction ends.
    assert!(execution_key_share_blocked(pool, active.id).await?);
    assert!(!execution_key_share_blocked(pool, retired.id).await?);
    tx.commit().await?;
    assert!(!execution_key_share_blocked(pool, active.id).await?);

    // `NOWAIT`: a held row fails the read at once with `lock_not_available`.
    let holder = hold_execution(pool, active.id).await?;
    let mut tx = pool.begin().await?;
    let error = lock_active_nowait(&mut tx, sandbox_id)
        .await
        .expect_err("a held execution fails the lock");
    assert!(is_lock_not_available(&error), "{error}");
    tx.rollback().await?;
    holder.rollback().await?;

    // No active execution: nothing to lock, and no error.
    let bare = insert_sandbox(pool, "restoring").await?;
    let mut tx = pool.begin().await?;
    lock_active_nowait(&mut tx, bare).await?;
    tx.rollback().await?;
    Ok(())
}
