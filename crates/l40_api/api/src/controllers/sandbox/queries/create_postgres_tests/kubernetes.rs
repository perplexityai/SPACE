//! The create test in the parent module whose outcome differs by substrate,
//! run against the Kubernetes substrate.

use super::*;
use crate::controllers::substrate::kubernetes::tests::substrate_with_store;

/// Twin of `postgres_failed_pooled_create_finalizes_after_claim_cleanup`,
/// renamed because the outcome differs. On direct the claim cleanup finds no
/// `node_direct`
/// endpoint to roll back through and the create finalizes by deleting the
/// row. The Kubernetes cleanup (`cleanup_failed_pooled_prepare`) marks the
/// claimed warm-pool slot `cleanup_pending` and defers to the reconciler, so
/// the rollback keeps the row for it.
#[tokio::test(flavor = "current_thread")]
async fn postgres_failed_pooled_create_defers_cleanup_to_the_reconciler() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("pooled-sandbox-cleanup", None);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    let claim = pooled_claim(sandbox_id, node_id);
    let release_id = insert_template_release(&db.pool, "seed", &claim.content_hash).await?;
    let pool_config_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;
    let mut tx = db.pool.begin().await?;
    insert_pooled_sandbox(&mut tx, &input, &create_owner, &claim, "k8s").await?;
    tx.commit().await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             id, config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, claim_id, ready_at, claimed_at
         )
         VALUES (
             $1, $2, $3, $4, 'seed', $5, 4, 2048, $6, 'space', $7,
             $8::inet, 'worker-1', 'claimed', $9, NOW(), NOW()
         )"
    ))
    .bind(claim.pool_slot_id)
    .bind(pool_config_id)
    .bind(release_id)
    .bind(claim.sandbox_id)
    .bind(&claim.content_hash)
    .bind(&claim.pod_name)
    .bind(claim.pod_uid.as_ref().map(PodUid::as_str))
    .bind(&claim.pod_ip)
    .bind(claim.claim_id)
    .execute(&db.pool)
    .await?;
    let substrate = substrate_with_store(db.pool.clone());

    let cleanup = SandboxController::rollback_failed_pooled_create_with_substrate(
        &substrate, &db.pool, &claim,
    )
    .await?;

    let sandbox_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandboxes
             WHERE id = $1
         )"
    ))
    .bind(claim.sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(cleanup, PooledPrepareFailureCleanup::DeferredToReconciler);
    assert!(sandbox_exists);
    let (slot_state, _) = pooled_slot_and_sandbox_state(&db.pool, sandbox_id).await?;
    assert_eq!(slot_state, "cleanup_pending");
    Ok(())
}
