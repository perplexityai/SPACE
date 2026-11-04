use pplx_space_common_core::{
    sql,
    types::{
        AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
        AutoLifecyclePolicy, AutoLifecycleStage, AutoLifecycleWakePolicy, AutoLifecycleWakeSource,
        SandboxActivationAttemptId, SandboxQuarantineSource,
    },
};
use pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db, try_pg_test_db};
use pplx_space_placement::test_support::stored_lifecycle_matrix;

use super::*;

use pplx_space_test_env::TestResult;

async fn insert_node(pool: &sqlx::PgPool, node_id: NodeId) -> TestResult {
    insert_ready_worker_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name)
         VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(format!("node-{node_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_active_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    generation: i64,
) -> TestResult<SandboxActivationAttemptId> {
    insert_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_generation,
             auto_lifecycle_json
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'on_node', $2, $2, 'k8s', 1, $3
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(Json(delete_policy()))
    .execute(pool)
    .await?;
    insert_execution(pool, sandbox_id, generation, true).await
}

async fn insert_execution(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    active: bool,
) -> TestResult<SandboxActivationAttemptId> {
    let route_activation_id = SandboxActivationAttemptId::new();
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, state, active,
             route_activation_id, k8s_node_name
         )
         SELECT $1, $2, $3, 'space', 'running', $4, $5, node_k8s.k8s_node_name
           FROM sandboxes
           JOIN node_k8s ON node_k8s.node_id = sandboxes.active_node_id
          WHERE sandboxes.id = $1"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "space-sbx-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(active)
    .bind(route_activation_id)
    .execute(pool)
    .await?;
    Ok(route_activation_id)
}

async fn seed_orphan_quarantine(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    last_observed_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'orphaned', terminated_at = NOW()
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'quarantined', placement = 'unplaced',
                active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (
             sandbox_id, quarantine_id, entered_from_lifecycle, source,
             reason_code, evidence, next_action, last_observed_node_id
         )
         VALUES ($1, $2, 'started', $3, $4, '{}',
                 'requires_operator_attention', $5)"
    ))
    .bind(sandbox_id)
    .bind(uuid::Uuid::now_v7())
    .bind(SandboxQuarantineSource::ClusterController.as_str())
    .bind(SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact.as_str())
    .bind(last_observed_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn delete_policy() -> AutoLifecyclePolicy {
    AutoLifecyclePolicy {
        condition_sets: [(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]
        .into(),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 900,
            action: AutoLifecycleIdleAction::Delete,
        }],
        ..Default::default()
    }
}

fn matching_policy_fingerprint() -> AutoLifecyclePolicyFingerprint {
    AutoLifecyclePolicyFingerprint::for_policy(&delete_policy())
}

fn completion(
    receipt_id: AutoLifecycleDeleteReceiptId,
    sandbox_id: SandboxId,
    generation: i64,
    route_activation_id: SandboxActivationAttemptId,
    policy_fingerprint: AutoLifecyclePolicyFingerprint,
) -> AutoLifecycleDeleteCompletion {
    AutoLifecycleDeleteCompletion {
        receipt_id: receipt_id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        execution_generation: generation,
        route_activation_id: route_activation_id.to_string(),
        auto_lifecycle_policy_fingerprint: policy_fingerprint.to_string(),
    }
}

async fn sandbox_exists(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar::<_, bool>(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn matching_generation_and_node_deletes_and_acknowledges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert_eq!(consumption.deleted_sandboxes.len(), 1);
    assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dormant_sandbox_uses_exact_execution_node_as_ownership_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended', placement = 'unplaced',
                active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert_eq!(consumption.deleted_sandboxes.len(), 1);
    assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn policy_without_delete_is_acknowledged_without_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let disabled = AutoLifecyclePolicy::disabled();
    sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Json(disabled.clone()))
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            AutoLifecyclePolicyFingerprint::for_policy(&disabled),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(consumption.deleted_sandboxes.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_receipt_does_not_poison_valid_receipts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let malformed_receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let valid_receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let mut malformed = completion(
        malformed_receipt_id,
        SandboxId::new(),
        7,
        SandboxActivationAttemptId::new(),
        matching_policy_fingerprint(),
    );
    malformed.sandbox_id = "not-a-sandbox-id".to_string();
    let valid = completion(
        valid_receipt_id,
        SandboxId::new(),
        7,
        SandboxActivationAttemptId::new(),
        matching_policy_fingerprint(),
    );

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &[malformed, valid])
            .await?;
    tx.commit().await?;

    assert_eq!(
        consumption
            .acknowledged_receipt_ids
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([malformed_receipt_id, valid_receipt_id])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unparseable_receipt_id_is_not_reported_as_acknowledged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut malformed = completion(
        AutoLifecycleDeleteReceiptId::generate(),
        SandboxId::new(),
        7,
        SandboxActivationAttemptId::new(),
        matching_policy_fingerprint(),
    );
    malformed.receipt_id = "not-a-receipt-id".to_string();

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &[malformed]).await?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Unacknowledgeable,
            AutoLifecycleDeleteCompletionReason::MalformedReceipt,
        )]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_receipts_do_not_consume_the_valid_receipt_budget() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut completions = (0..AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE)
        .map(|index| {
            let mut malformed = completion(
                AutoLifecycleDeleteReceiptId::generate(),
                SandboxId::new(),
                7,
                SandboxActivationAttemptId::new(),
                matching_policy_fingerprint(),
            );
            malformed.receipt_id = format!("malformed-{index}");
            malformed
        })
        .collect::<Vec<_>>();
    let valid_receipt_id = AutoLifecycleDeleteReceiptId::generate();
    completions.push(completion(
        valid_receipt_id,
        SandboxId::new(),
        7,
        SandboxActivationAttemptId::new(),
        matching_policy_fingerprint(),
    ));

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &completions).await?;
    tx.commit().await?;

    assert!(
        consumption
            .acknowledged_receipt_ids
            .contains(&valid_receipt_id),
        "malformed receipts must not prevent a valid sibling from being evaluated"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn forward_incompatible_policy_defers_without_aborting_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET auto_lifecycle_json = jsonb_set(
                auto_lifecycle_json,
                '{stages,0,action}',
                '\"future_delete\"'::jsonb
            )
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let receipt = completion(
        AutoLifecycleDeleteReceiptId::generate(),
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[receipt]).await?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::PolicyUnreadable,
        )],
        "one forward-incompatible policy must defer only its receipt"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn quarantined_matching_generation_and_source_node_is_deleted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'orphaned', terminated_at = NOW()
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'quarantined', placement = 'unplaced',
                active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (
             sandbox_id, quarantine_id, entered_from_lifecycle, source,
             reason_code, evidence, next_action, last_observed_node_id
         )
         VALUES ($1, $2, 'started', $3, $4, '{}', 'requires_operator_attention', $5)"
    ))
    .bind(sandbox_id)
    .bind(uuid::Uuid::now_v7())
    .bind(SandboxQuarantineSource::ClusterController.as_str())
    .bind(SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact.as_str())
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn quarantined_null_source_node_is_held_without_poisoning_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'orphaned', terminated_at = NOW()
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'quarantined', placement = 'unplaced',
                active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (
             sandbox_id, quarantine_id, entered_from_lifecycle, source,
             reason_code, evidence, next_action, last_observed_node_id
         )
         VALUES ($1, $2, 'started', $3, $4, '{}',
                 'requires_operator_attention', NULL)"
    ))
    .bind(sandbox_id)
    .bind(uuid::Uuid::now_v7())
    .bind(SandboxQuarantineSource::ClusterController.as_str())
    .bind(SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact.as_str())
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
            AutoLifecycleDeleteCompletionReason::QuarantineHold,
        )]
    );
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_receipt_does_not_acknowledge_current_same_sandbox_receipt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let stale_receipt_id = AutoLifecycleDeleteReceiptId::from_uuid(uuid::Uuid::from_u128(1));
    let current_receipt_id = AutoLifecycleDeleteReceiptId::from_uuid(uuid::Uuid::from_u128(2));
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let stale = completion(
        stale_receipt_id,
        sandbox_id,
        6,
        SandboxActivationAttemptId::new(),
        matching_policy_fingerprint(),
    );
    let current = completion(
        current_receipt_id,
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[stale, current.clone()])
            .await?;
    tx.commit().await?;

    assert_eq!(
        consumption.acknowledged_receipt_ids,
        vec![stale_receipt_id],
        "the unevaluated current receipt must remain in the node outbox"
    );
    assert!(consumption.completion_metrics.contains(&(
        AutoLifecycleDeleteCompletionDisposition::Deferred,
        AutoLifecycleDeleteCompletionReason::DuplicateSandboxReceipt,
    )));
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);

    let mut retry_tx = db.pool.begin().await?;
    let retry = consume_auto_lifecycle_delete_completions(
        &mut retry_tx,
        node_id,
        std::slice::from_ref(&current),
    )
    .await?;
    retry_tx.commit().await?;

    assert_eq!(retry.acknowledged_receipt_ids, vec![current_receipt_id]);
    assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn contended_sandbox_lock_defers_without_stalling_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let receipt = completion(
        receipt_id,
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *blocker)
        .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[receipt]),
    )
    .await
    .expect("receipt consumption must not wait for a contended sandbox row")?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::LockContended,
        )]
    );
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    blocker.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn contended_execution_lock_defers_without_stalling_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let receipt = completion(
        AutoLifecycleDeleteReceiptId::generate(),
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_one(&mut *blocker)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[receipt]),
    )
    .await
    .expect("receipt consumption must not wait for contended execution evidence")?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::LockContended,
        )]
    );
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    blocker.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn contended_quarantine_lock_defers_without_stalling_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    seed_orphan_quarantine(&db.pool, sandbox_id, Some(node_id)).await?;
    let receipt = completion(
        AutoLifecycleDeleteReceiptId::generate(),
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1
           FROM sandbox_quarantine_state
          WHERE sandbox_id = $1
          FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_one(&mut *blocker)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[receipt]),
    )
    .await
    .expect("receipt consumption must not wait for contended quarantine ownership")?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::LockContended,
        )]
    );
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    blocker.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retryable_finalize_failure_defers_without_aborting_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let receipt = completion(
        AutoLifecycleDeleteReceiptId::generate(),
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );
    sqlx::query(sql!(
        "CREATE FUNCTION fail_auto_lifecycle_delete_finalize_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION USING
                 ERRCODE = '40P01',
                 MESSAGE = 'injected auto-lifecycle delete deadlock';
         END;
         $$"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER fail_auto_lifecycle_delete_finalize_for_test
         BEFORE DELETE ON sandboxes
         FOR EACH ROW
         EXECUTE FUNCTION fail_auto_lifecycle_delete_finalize_for_test()"
    ))
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let result = consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[receipt]).await;

    assert!(
        result.is_ok(),
        "a retryable finalize failure must defer only this receipt: {result:?}"
    );
    let consumption = result?;
    tx.commit().await?;
    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::FinalizeContended,
        )]
    );
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_generation_is_acknowledged_without_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    insert_execution(&db.pool, sandbox_id, 8, true).await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(consumption.deleted_sandboxes.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn same_generation_newer_route_incarnation_is_acknowledged_without_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            SandboxActivationAttemptId::new(),
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(consumption.deleted_sandboxes.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn changed_cluster_policy_is_acknowledged_without_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let stale_policy = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![AutoLifecycleWakeSource::Access],
        },
        ..AutoLifecyclePolicy::disabled()
    };

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            AutoLifecyclePolicyFingerprint::for_policy(&stale_policy),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(consumption.deleted_sandboxes.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_node_is_acknowledged_without_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let owner_node_id = NodeId::new();
    let publishing_node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, owner_node_id, 7).await?;
    insert_node(&db.pool, publishing_node_id).await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        publishing_node_id,
        &[completion(
            AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids.len(), 1);
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ownership_and_route_absence_dispositions_are_explicit() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let missing_route_sandbox = SandboxId::new();
    let missing_route_node = NodeId::new();
    let missing_route_activation =
        seed_active_sandbox(&db.pool, missing_route_sandbox, missing_route_node, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET route_activation_id = NULL
          WHERE sandbox_id = $1"
    ))
    .bind(missing_route_sandbox)
    .execute(&db.pool)
    .await?;

    let different_active_sandbox = SandboxId::new();
    let execution_node = NodeId::new();
    let different_active_node = NodeId::new();
    let different_active_route =
        seed_active_sandbox(&db.pool, different_active_sandbox, execution_node, 7).await?;
    insert_node(&db.pool, different_active_node).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, target_node_id = $2
          WHERE id = $1"
    ))
    .bind(different_active_sandbox)
    .bind(different_active_node)
    .execute(&db.pool)
    .await?;

    let awaiting_sandbox = SandboxId::new();
    let awaiting_node = NodeId::new();
    let awaiting_route = seed_active_sandbox(&db.pool, awaiting_sandbox, awaiting_node, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = NULL, target_node_id = NULL, placement = 'unplaced'
          WHERE id = $1"
    ))
    .bind(awaiting_sandbox)
    .execute(&db.pool)
    .await?;

    let absent_sandbox = SandboxId::new();
    let absent_node = NodeId::new();
    let absent_route = seed_active_sandbox(&db.pool, absent_sandbox, absent_node, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'created', active_node_id = NULL,
                target_node_id = NULL, placement = 'unplaced'
          WHERE id = $1"
    ))
    .bind(absent_sandbox)
    .execute(&db.pool)
    .await?;

    for (sandbox_id, node_id, route_activation_id, disposition, reason) in [
        (
            missing_route_sandbox,
            missing_route_node,
            missing_route_activation,
            AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
            AutoLifecycleDeleteCompletionReason::MissingRouteIdentity,
        ),
        (
            different_active_sandbox,
            execution_node,
            different_active_route,
            AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
            AutoLifecycleDeleteCompletionReason::DifferentActiveNode,
        ),
        (
            awaiting_sandbox,
            awaiting_node,
            awaiting_route,
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::AwaitingOwnershipEvidence,
        ),
        (
            absent_sandbox,
            absent_node,
            absent_route,
            AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
            AutoLifecycleDeleteCompletionReason::OwnershipAbsent,
        ),
    ] {
        let mut tx = db.pool.begin().await?;
        let consumption = consume_auto_lifecycle_delete_completions(
            &mut tx,
            node_id,
            &[completion(
                AutoLifecycleDeleteReceiptId::generate(),
                sandbox_id,
                7,
                route_activation_id,
                matching_policy_fingerprint(),
            )],
        )
        .await?;
        tx.commit().await?;

        assert_eq!(
            consumption.completion_metrics,
            vec![(disposition, reason)],
            "sandbox_id={sandbox_id}"
        );
        assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn quarantine_from_other_source_is_acknowledged_without_deleting() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'orphaned', terminated_at = NOW()
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'quarantined', placement = 'unplaced',
                active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (
             sandbox_id, quarantine_id, entered_from_lifecycle, source,
             reason_code, evidence, next_action, last_observed_node_id
         )
         VALUES ($1, $2, 'started', $3, $4, '{}', 'requires_operator_attention', $5)"
    ))
    .bind(sandbox_id)
    .bind(uuid::Uuid::now_v7())
    .bind(SandboxQuarantineSource::SbxManager.as_str())
    .bind(SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact.as_str())
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

/// A matching delete completion deletes the row only out of a settled
/// lifecycle its publisher owns: with the row active on the publishing node,
/// out of `created`, `started`, `paused`, `suspended`, `stopped` and `failed`;
/// with no active node, only out of the dormant `stopped` and `suspended`.
/// Every other stored lifecycle keeps the row and its lifecycle. The read
/// parses the column through `StoredSandboxLifecycle`, which normalizes case
/// and padding, so a padded `suspended` is deleted like a canonical one and an
/// unknown value is acknowledged without a delete. Columns are the row's
/// placement; `A`: deleted; `.`: kept.
#[tokio::test(flavor = "current_thread")]
async fn delete_completion_applies_only_from_settled_owned_lifecycles() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              on_node  unplaced
        "created            A        .",
        "creating           .        .",
        "starting           .        .",
        "started            A        .",
        "restoring          .        .",
        "pausing            .        .",
        "paused             A        .",
        "resuming           .        .",
        "suspending         .        .",
        "suspended          A        A",
        "stopping           .        .",
        "stopped            A        A",
        "deleting           .        .",
        "checkpointing      .        .",
        "quarantined        .        .",
        "failed             A        .",
        "deleted            .        .",
        "cleanup_required   .        .",
        "bogus              .        .",
        "_Suspended_        A        A",
    ];
    let db = pg_test_db().await?;

    let mut mismatches = Vec::new();
    for (stored, cells) in stored_lifecycle_matrix(["on_node", "unplaced"], EXPECTED) {
        for (placement, deletes) in cells {
            let deletes = deletes.applies();
            let sandbox_id = SandboxId::new();
            let node_id = NodeId::new();
            let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
            sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                .bind(sandbox_id)
                .bind(stored)
                .execute(&db.pool)
                .await?;
            if placement == "unplaced" {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET active_node_id = NULL, target_node_id = NULL, placement = 'unplaced'
                      WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&db.pool)
                .await?;
            }

            let mut tx = db.pool.begin().await?;
            consume_auto_lifecycle_delete_completions(
                &mut tx,
                node_id,
                &[completion(
                    AutoLifecycleDeleteReceiptId::generate(),
                    sandbox_id,
                    7,
                    route_activation_id,
                    matching_policy_fingerprint(),
                )],
            )
            .await?;
            tx.commit().await?;

            let lifecycle: Option<String> =
                sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                    .bind(sandbox_id)
                    .fetch_optional(&db.pool)
                    .await?;
            let expected = (!deletes).then(|| stored.to_owned());
            if lifecycle != expected {
                mismatches.push(format!("{placement} {stored}: row now {lifecycle:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stored_tombstones_are_acknowledged_without_poisoning_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for lifecycle in StoredSandboxLifecycle::tombstones_as_str() {
        let sandbox_id = SandboxId::new();
        let node_id = NodeId::new();
        let receipt_id = AutoLifecycleDeleteReceiptId::generate();
        let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox_id)
            .bind(lifecycle)
            .execute(&db.pool)
            .await?;

        let mut tx = db.pool.begin().await?;
        let consumption = consume_auto_lifecycle_delete_completions(
            &mut tx,
            node_id,
            &[completion(
                receipt_id,
                sandbox_id,
                7,
                route_activation_id,
                matching_policy_fingerprint(),
            )],
        )
        .await?;
        tx.commit().await?;

        assert_eq!(
            consumption.acknowledged_receipt_ids,
            vec![receipt_id],
            "lifecycle={lifecycle}"
        );
        assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_lifecycle_is_acknowledged_without_poisoning_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'future_lifecycle' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            receipt_id,
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(consumption.deleted_sandboxes.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn missing_or_ahead_lineage_is_acknowledged_without_deleting() -> TestResult {
    for delete_execution in [true, false] {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox_id = SandboxId::new();
        let node_id = NodeId::new();
        let receipt_id = AutoLifecycleDeleteReceiptId::generate();
        let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
        if delete_execution {
            sqlx::query(sql!(
                "DELETE FROM sandbox_k8s_executions WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }

        let receipt_generation = if delete_execution { 7 } else { 8 };
        let mut tx = db.pool.begin().await?;
        let consumption = consume_auto_lifecycle_delete_completions(
            &mut tx,
            node_id,
            &[completion(
                receipt_id,
                sandbox_id,
                receipt_generation,
                route_activation_id,
                matching_policy_fingerprint(),
            )],
        )
        .await?;
        tx.commit().await?;

        assert_eq!(
            consumption.acknowledged_receipt_ids,
            vec![receipt_id],
            "delete_execution={delete_execution}"
        );
        assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn central_lifecycle_transition_defers_acknowledgement_and_delete() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'starting' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn creating_snapshot_defers_acknowledgement_and_delete() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (sandbox_id, type, state, source_node_id)
         VALUES ($1, 'full', 'creating', $2)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn already_absent_sandbox_is_acknowledged_idempotently() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        NodeId::new(),
        &[completion(
            receipt_id,
            SandboxId::new(),
            7,
            SandboxActivationAttemptId::new(),
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(consumption.acknowledged_receipt_ids, vec![receipt_id]);
    assert!(consumption.deleted_sandboxes.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publication_processes_a_bounded_receipt_batch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let completions = (0..AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE + 1)
        .map(|_| {
            completion(
                AutoLifecycleDeleteReceiptId::generate(),
                SandboxId::new(),
                7,
                SandboxActivationAttemptId::new(),
                matching_policy_fingerprint(),
            )
        })
        .collect::<Vec<_>>();

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &completions).await?;
    tx.commit().await?;

    assert_eq!(
        consumption.acknowledged_receipt_ids.len(),
        AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE
    );
    assert_eq!(consumption.deferred_by_batch_limit, 1);
    assert!(consumption.deleted_sandboxes.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publication_preserves_publisher_fairness_before_lock_ordering() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_ids = (1..=AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE + 1)
        .map(|value| SandboxId::from_uuid(uuid::Uuid::from_u128(value as u128)))
        .collect::<Vec<_>>();
    let publisher_first = *sandbox_ids.last().expect("non-empty sandbox ids");
    let deferred_by_limit = *sandbox_ids
        .get(AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE - 1)
        .expect("batch-limited sandbox id");
    let completions = std::iter::once(publisher_first)
        .chain(
            sandbox_ids
                .iter()
                .take(AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE)
                .copied(),
        )
        .map(|sandbox_id| {
            completion(
                AutoLifecycleDeleteReceiptId::generate(),
                sandbox_id,
                7,
                SandboxActivationAttemptId::new(),
                matching_policy_fingerprint(),
            )
        })
        .collect::<Vec<_>>();
    let publisher_first_receipt = AutoLifecycleDeleteReceiptId::from_str(
        &completions
            .first()
            .expect("publisher-first receipt")
            .receipt_id,
    )?;
    let deferred_receipt = AutoLifecycleDeleteReceiptId::from_str(
        &completions
            .iter()
            .find(|completion| completion.sandbox_id == deferred_by_limit.to_string())
            .expect("batch-limited receipt")
            .receipt_id,
    )?;

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &completions).await?;
    tx.commit().await?;

    assert!(
        consumption
            .acknowledged_receipt_ids
            .contains(&publisher_first_receipt),
        "the receiver must preserve the publisher's fair selection order"
    );
    assert!(
        !consumption
            .acknowledged_receipt_ids
            .contains(&deferred_receipt)
    );
    assert_eq!(consumption.deferred_by_batch_limit, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unsupported_fingerprint_version_defers_instead_of_acknowledging() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let receipt_id = AutoLifecycleDeleteReceiptId::generate();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    let mut receipt = completion(
        receipt_id,
        sandbox_id,
        7,
        route_activation_id,
        matching_policy_fingerprint(),
    );
    receipt.auto_lifecycle_policy_fingerprint = receipt
        .auto_lifecycle_policy_fingerprint
        .replace("v1:", "v2:");

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, node_id, &[receipt]).await?;
    tx.commit().await?;

    // Acknowledging would tell the publisher to prune a receipt this build
    // simply cannot read, losing the delete until orphan reconciliation.
    assert!(consumption.acknowledged_receipt_ids.is_empty());
    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::Deferred,
            AutoLifecycleDeleteCompletionReason::UnsupportedFingerprintVersion,
        )]
    );
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_request_is_truncated_before_the_tail_is_parsed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    const OVER_LIMIT: usize = 3;
    let mut completions = (0..AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT)
        .map(|_| {
            completion(
                AutoLifecycleDeleteReceiptId::generate(),
                SandboxId::new(),
                7,
                SandboxActivationAttemptId::new(),
                matching_policy_fingerprint(),
            )
        })
        .collect::<Vec<_>>();
    // Unparseable identities in the tail: reaching them at all would surface a
    // malformed-receipt metric.
    completions.extend((0..OVER_LIMIT).map(|_| AutoLifecycleDeleteCompletion {
        receipt_id: "not-a-receipt-id".to_string(),
        sandbox_id: "not-a-sandbox-id".to_string(),
        execution_generation: -1,
        route_activation_id: "not-an-activation-id".to_string(),
        auto_lifecycle_policy_fingerprint: "not-a-fingerprint".to_string(),
    }));

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &completions).await?;
    tx.commit().await?;

    assert_eq!(consumption.deferred_by_request_limit, OVER_LIMIT);
    assert!(
        !consumption.completion_metrics.iter().any(|(_, reason)| {
            *reason == AutoLifecycleDeleteCompletionReason::MalformedReceipt
        }),
        "receipts past the request cap must not be parsed"
    );
    assert_eq!(
        consumption.acknowledged_receipt_ids.len(),
        AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE
    );
    assert_eq!(
        consumption.deferred_by_batch_limit,
        AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT
            - AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ownership_evidence_deferral_expires_into_acknowledgement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;
    // Started with no active node and a placement change older than the grace:
    // nothing repopulates active_node_id once the runtime is gone, so the
    // deferral has to terminate.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = NULL, target_node_id = NULL, placement = 'unplaced',
                placement_changed_at = NOW() - $2::INTERVAL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(format!(
        "{} seconds",
        AUTO_LIFECYCLE_DELETE_COMPLETION_OWNERSHIP_GRACE.as_secs() + 60
    ))
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let consumption = consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    tx.commit().await?;

    assert_eq!(
        consumption.completion_metrics,
        vec![(
            AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
            AutoLifecycleDeleteCompletionReason::OwnershipEvidenceExpired,
        )]
    );
    assert_eq!(consumption.acknowledged_receipt_ids.len(), 1);
    // Weak evidence never authorizes the delete; the row is left for orphan
    // reconciliation.
    assert!(sandbox_exists(&db.pool, sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn receipt_consumption_bounds_lock_waits_for_the_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = seed_active_sandbox(&db.pool, sandbox_id, node_id, 7).await?;

    let mut tx = db.pool.begin().await?;
    consume_auto_lifecycle_delete_completions(
        &mut tx,
        node_id,
        &[completion(
            AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            7,
            route_activation_id,
            matching_policy_fingerprint(),
        )],
    )
    .await?;
    // pg_settings reports lock_timeout in milliseconds. The finalize path holds
    // the batch's row locks with plain UPDATE/DELETE statements, so an unbounded
    // wait there stalls the whole node publication.
    let lock_timeout =
        sqlx::query_scalar::<_, String>(sql!("SELECT setting FROM pg_settings WHERE name = $1"))
            .bind("lock_timeout")
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;

    assert_eq!(
        lock_timeout,
        AUTO_LIFECYCLE_DELETE_COMPLETION_LOCK_TIMEOUT
            .as_millis()
            .to_string()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_sandbox_receipts_do_not_consume_batch_budget() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_ids = (1..=AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE)
        .map(|value| SandboxId::from_uuid(uuid::Uuid::from_u128(value as u128)))
        .collect::<Vec<_>>();
    let completions = sandbox_ids
        .iter()
        .copied()
        .flat_map(|sandbox_id| {
            let count = if Some(&sandbox_id) == sandbox_ids.first() {
                2
            } else {
                1
            };
            std::iter::repeat_with(move || {
                completion(
                    AutoLifecycleDeleteReceiptId::generate(),
                    sandbox_id,
                    7,
                    SandboxActivationAttemptId::new(),
                    matching_policy_fingerprint(),
                )
            })
            .take(count)
        })
        .collect::<Vec<_>>();

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &completions).await?;
    tx.commit().await?;

    assert_eq!(
        consumption.acknowledged_receipt_ids.len(),
        AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE
    );
    assert_eq!(consumption.deferred_by_batch_limit, 0);
    assert!(consumption.completion_metrics.contains(&(
        AutoLifecycleDeleteCompletionDisposition::Deferred,
        AutoLifecycleDeleteCompletionReason::DuplicateSandboxReceipt,
    )));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_receipts_for_a_displaced_sandbox_count_against_the_budget() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let displaced_sandbox = SandboxId::new();
    let completions = (0..AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE)
        .map(|_| SandboxId::new())
        .chain([displaced_sandbox, displaced_sandbox])
        .map(|sandbox_id| {
            completion(
                AutoLifecycleDeleteReceiptId::generate(),
                sandbox_id,
                7,
                SandboxActivationAttemptId::new(),
                matching_policy_fingerprint(),
            )
        })
        .collect::<Vec<_>>();

    let mut tx = db.pool.begin().await?;
    let consumption =
        consume_auto_lifecycle_delete_completions(&mut tx, NodeId::new(), &completions).await?;
    tx.commit().await?;

    // Neither receipt for the displaced sandbox was evaluated, so both belong to
    // the budget that turned them away rather than to each other.
    assert_eq!(consumption.deferred_by_batch_limit, 2);
    assert!(!consumption.completion_metrics.contains(&(
        AutoLifecycleDeleteCompletionDisposition::Deferred,
        AutoLifecycleDeleteCompletionReason::DuplicateSandboxReceipt,
    )));
    Ok(())
}
