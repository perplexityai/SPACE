use super::*;
use crate::tests::queued_ref::{
    CALLER_TRACEPARENT, QueuedRefProgress, queue_claimed_retrying_ref, queued_ref_progress,
};

/// Inventory a node publishes for the first time resets its refs' retry
/// progress and makes them due now, even a ref a reconciler holds (which
/// keeps its claim), and saves the publisher's trace context for the pass
/// that applies them.
#[tokio::test(flavor = "current_thread")]
async fn first_seen_inventory_enqueue_resets_a_claimed_refs_retries_and_saves_the_trace()
-> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let element = sandbox_id.to_string();
    queue_claimed_retrying_ref(&db.pool, node_id, FACET_SANDBOX_DESIRED_STATE, &element).await?;

    let mut conn = db.pool.acquire().await?;
    pplx_space_observability::test_trace::in_remote_parent_span(
        CALLER_TRACEPARENT,
        queue_policy_reconcile_refs_for_new_inventory(
            &mut conn,
            node_id,
            vec![(FACET_SANDBOX_DESIRED_STATE, element.clone())],
            ReconcileReason::NodeSandboxInventoryPublished,
        ),
    )
    .await?;
    drop(conn);

    assert_eq!(
        queued_ref_progress(&db.pool, node_id, FACET_SANDBOX_DESIRED_STATE, &element).await?,
        (
            QueuedRefProgress {
                generation: 4,
                attempts: 0,
                last_error: String::new(),
                claim_token: "claimant".to_string(),
                requested_now: true,
                due_now: true,
            },
            true,
        )
    );
    Ok(())
}
