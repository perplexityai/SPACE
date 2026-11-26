use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_sandbox_model::{
    SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId, SandboxTransitionReceiptKind,
};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use sqlx::types::Json;
use uuid::Uuid;

use super::super::ownership_test_fixture::{
    SeededExecution, execution_key_share_blocked, execution_state, execution_version,
    insert_execution, insert_sandbox, sandbox_key_share_blocked,
};
use super::{
    ActivationRollbackCandidateRow, LOCK_ACTIVATION_ROLLBACK_CANDIDATES,
    lock_activation_rollback_candidates, retire_rolled_back_execution,
};
use crate::ingest::TRANSITION_RECEIPT_RECORD_COLUMNS;

/// The statement reads the receipts in the publish path's recordset shape:
/// its `jsonb_to_recordset` column list is ingest's, column for column.
#[test]
fn the_receipt_recordset_columns_are_ingests() {
    let (_, recordset) = LOCK_ACTIVATION_ROLLBACK_CANDIDATES
        .split_once("AS input(")
        .expect("the statement reads a recordset");
    let (column_list, _) = recordset.split_once(')').expect("the column list closes");
    let columns = |list: &str| {
        list.split(',')
            .map(|column| column.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        columns(column_list),
        columns(TRANSITION_RECEIPT_RECORD_COLUMNS)
    );
}

/// A registered worker node.
async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

/// A sandbox in `lifecycle` active on `node_id`, with an active execution at
/// generation 4 holding a fresh activation attempt.
async fn seed(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
) -> TestResult<(SandboxId, SeededExecution, SandboxActivationAttemptId)> {
    let sandbox_id = insert_sandbox(pool, lifecycle).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    let execution = insert_execution(pool, sandbox_id, 4, "running", true, None).await?;
    let attempt = SandboxActivationAttemptId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET activation_attempt_id = $2 WHERE id = $1"
    ))
    .bind(execution.id)
    .bind(attempt)
    .execute(pool)
    .await?;
    Ok((sandbox_id, execution, attempt))
}

fn receipt(
    sandbox_id: SandboxId,
    receipt_id: SandboxCleanupEpisodeId,
    generation: i64,
    attempt: SandboxActivationAttemptId,
) -> serde_json::Value {
    serde_json::json!({
        "sandbox_id": sandbox_id.as_uuid(),
        "published_lifecycle": "stopped",
        "transition_receipt_id": receipt_id.as_uuid(),
        "transition_receipt_kind": "activation_rollback",
        "transition_receipt_target_state": "stopped",
        "transition_receipt_execution_generation": generation,
        "transition_receipt_reason_code": "activation_failed",
        "transition_receipt_activation_attempt_id": attempt.as_uuid(),
        "failure_reason": "boot failed",
    })
}

#[tokio::test(flavor = "current_thread")]
async fn lock_activation_rollback_candidates_locks_the_sandbox_and_its_fenced_execution()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = insert_node(pool).await?;
    let (fenced, fenced_execution, fenced_attempt) = seed(pool, node_id, " Starting").await?;
    let (stale, stale_execution, _) = seed(pool, node_id, "starting").await?;
    let (quarantined, quarantined_execution, quarantined_attempt) =
        seed(pool, node_id, "quarantined").await?;
    let (wrong_generation, wrong_generation_execution, wrong_generation_attempt) =
        seed(pool, node_id, "starting").await?;
    let (unpublished, unpublished_execution, unpublished_attempt) =
        seed(pool, node_id, "starting").await?;
    let (untargeted, untargeted_execution, untargeted_attempt) =
        seed(pool, node_id, "starting").await?;
    let (retired, retired_execution, retired_attempt) = seed(pool, node_id, "starting").await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'replaced', terminated_at = NOW()
          WHERE id = $1"
    ))
    .bind(retired_execution.id)
    .execute(pool)
    .await?;
    let mut unpublished_receipt = receipt(
        unpublished,
        SandboxCleanupEpisodeId::new(),
        4,
        unpublished_attempt,
    );
    unpublished_receipt["published_lifecycle"] = "started".into();
    let mut untargeted_receipt = receipt(
        untargeted,
        SandboxCleanupEpisodeId::new(),
        4,
        untargeted_attempt,
    );
    untargeted_receipt["published_lifecycle"] = "started".into();
    untargeted_receipt["transition_receipt_target_state"] = "started".into();
    let receipt_id = SandboxCleanupEpisodeId::new();
    let rows = serde_json::Value::Array(vec![
        receipt(fenced, receipt_id, 4, fenced_attempt),
        // The losing cases: another activation attempt of the same
        // generation, and an excluded lifecycle. Neither is selected or
        // locked.
        receipt(
            stale,
            SandboxCleanupEpisodeId::new(),
            4,
            SandboxActivationAttemptId::new(),
        ),
        receipt(
            quarantined,
            SandboxCleanupEpisodeId::new(),
            4,
            quarantined_attempt,
        ),
        // Another generation, a published lifecycle that is not the target,
        // a target outside the converging states, and a retired execution.
        receipt(
            wrong_generation,
            SandboxCleanupEpisodeId::new(),
            3,
            wrong_generation_attempt,
        ),
        unpublished_receipt,
        untargeted_receipt,
        receipt(retired, SandboxCleanupEpisodeId::new(), 4, retired_attempt),
    ]);

    let mut tx = pool.begin().await?;
    let candidates = lock_activation_rollback_candidates(
        &mut *tx,
        node_id,
        Json(rows.clone()),
        SandboxTransitionReceiptKind::ActivationRollback,
    )
    .await?;
    assert_eq!(
        candidates,
        [ActivationRollbackCandidateRow {
            sandbox_id: fenced,
            receipt_id,
            execution_generation: 4,
            activation_attempt_id: fenced_attempt,
            target_state: "stopped".to_string(),
            reason_code: "activation_failed".to_string(),
            prior_lifecycle: "starting".to_string(),
            failure_message: Some("boot failed".to_string()),
        }]
    );
    // `FOR UPDATE OF s, execution`: both rows, so that even a key-share lock
    // waits on each.
    assert!(sandbox_key_share_blocked(pool, fenced).await?);
    assert!(execution_key_share_blocked(pool, fenced_execution.id).await?);
    for (sandbox_id, execution) in [
        (stale, &stale_execution),
        (quarantined, &quarantined_execution),
        (wrong_generation, &wrong_generation_execution),
        (unpublished, &unpublished_execution),
        (untargeted, &untargeted_execution),
        (retired, &retired_execution),
    ] {
        assert!(!sandbox_key_share_blocked(pool, sandbox_id).await?);
        assert!(!execution_key_share_blocked(pool, execution.id).await?);
    }
    tx.commit().await?;
    assert!(!sandbox_key_share_blocked(pool, fenced).await?);
    assert!(!execution_key_share_blocked(pool, fenced_execution.id).await?);

    // Another receipt kind, or another node, selects nothing.
    let mut tx = pool.begin().await?;
    for (node, kind) in [
        (node_id, SandboxTransitionReceiptKind::QuarantineRecovery),
        (
            NodeId::new(),
            SandboxTransitionReceiptKind::ActivationRollback,
        ),
    ] {
        let candidates =
            lock_activation_rollback_candidates(&mut *tx, node, Json(rows.clone()), kind).await?;
        assert!(candidates.is_empty());
    }
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retire_rolled_back_execution_fences_on_the_generation_and_attempt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = insert_node(pool).await?;
    let (sandbox_id, execution, attempt) = seed(pool, node_id, "starting").await?;
    let before = execution_version(pool, execution.id).await?;

    // The losing cases: another generation, or another attempt of this one.
    assert!(!retire_rolled_back_execution(pool, sandbox_id, 3, "replaced", attempt).await?);
    assert!(
        !retire_rolled_back_execution(
            pool,
            sandbox_id,
            4,
            "replaced",
            SandboxActivationAttemptId::new(),
        )
        .await?
    );
    assert_eq!(execution_version(pool, execution.id).await?, before);

    assert!(retire_rolled_back_execution(pool, sandbox_id, 4, "replaced", attempt).await?);
    assert_eq!(
        execution_state(pool, execution.id).await?,
        (false, "replaced".to_string())
    );
    // Retired, it no longer matches `active`.
    let retired = execution_version(pool, execution.id).await?;
    assert!(!retire_rolled_back_execution(pool, sandbox_id, 4, "suspended", attempt).await?);
    assert_eq!(execution_version(pool, execution.id).await?, retired);
    Ok(())
}
