use pplx_space_common_core::types::NodeId;

use std::collections::BTreeMap;

use pplx_space_common_svc::types::{
    FACET_NETWORK_POLICY, FACET_NODE_STATUS, FACET_SANDBOX_DESIRED_STATE, FACET_TEMPLATE_INVENTORY,
};
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};

use super::*;

use pplx_space_test_env::TestResult;

async fn insert_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    insert_ready_worker_node(pool, node_id).await
}

async fn insert_queue_row(
    pool: &PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
    state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id, facet, element, claim_token, claimed_at,
             next_attempt_at, dead_at, requested_at, updated_at
         )
         VALUES (
             $1, $2, $3,
             CASE WHEN $4 IN ('claimed', 'stale') THEN 'claim-token' ELSE '' END,
             CASE
                 WHEN $4 = 'claimed' THEN NOW()
                 WHEN $4 = 'stale' THEN NOW() - (($5 + 1) * INTERVAL '1 second')
                 ELSE NULL
             END,
             CASE WHEN $4 = 'backoff' THEN NOW() + INTERVAL '60 seconds' ELSE NOW() END,
             CASE WHEN $4 = 'dead' THEN NOW() ELSE NULL END,
             NOW(),
             NOW()
         )"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(state)
    .bind(NODE_RECONCILE_CLAIM_STALE_SECS)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_queue_metrics_cover_depth_and_stale_claim_counts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    insert_queue_row(&db.pool, node_id, FACET_NETWORK_POLICY, "ready", "ready").await?;
    insert_queue_row(&db.pool, node_id, FACET_NETWORK_POLICY, "stale", "stale").await?;
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "claimed",
        "claimed",
    )
    .await?;
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_TEMPLATE_INVENTORY,
        "backoff",
        "backoff",
    )
    .await?;
    insert_queue_row(&db.pool, node_id, FACET_NODE_STATUS, "dead", "dead").await?;

    let depth = node_queue_depth_counts(&db.pool)
        .await?
        .into_iter()
        .map(|(facet, state, count)| ((facet, state), count))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        depth.get(&(FACET_NETWORK_POLICY.to_string(), "ready".to_string())),
        Some(&1)
    );
    assert_eq!(
        depth.get(&(FACET_NETWORK_POLICY.to_string(), "claimed".to_string())),
        Some(&1)
    );
    assert_eq!(
        depth.get(&(
            FACET_SANDBOX_DESIRED_STATE.to_string(),
            "claimed".to_string()
        )),
        Some(&1)
    );
    assert_eq!(
        depth.get(&(FACET_TEMPLATE_INVENTORY.to_string(), "backoff".to_string())),
        Some(&1)
    );
    assert_eq!(
        depth.get(&(FACET_NODE_STATUS.to_string(), "dead".to_string())),
        Some(&1)
    );

    let stale_claims = node_queue_stale_claim_counts(&db.pool)
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(stale_claims.len(), 1);
    assert_eq!(stale_claims.get(FACET_NETWORK_POLICY), Some(&1));

    emit_node_queue_depth(&db.pool).await;
    emit_node_queue_stale_claims(&db.pool).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_queue_lag_only_includes_ready_nodes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let ready_node_id = NodeId::new();
    let inactive_node_id = NodeId::new();
    insert_node(&db.pool, ready_node_id).await?;
    insert_node(&db.pool, inactive_node_id).await?;
    sqlx::query(sql!("UPDATE nodes SET state = 'offline' WHERE id = $1"))
        .bind(inactive_node_id)
        .execute(&db.pool)
        .await?;
    insert_queue_row(
        &db.pool,
        ready_node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "ready",
        "ready",
    )
    .await?;
    insert_queue_row(
        &db.pool,
        inactive_node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "inactive",
        "ready",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET requested_at = NOW() - INTERVAL '6 hours'
         WHERE node_id = $1"
    ))
    .bind(inactive_node_id)
    .execute(&db.pool)
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    assert!(
        claim_lag < 60.0,
        "inactive node ref inflated actionable lag to {claim_lag}s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retrying_ref_does_not_inflate_claim_stage_lag() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    // A ref that has failed applies and is cycling through capped backoff
    // keeps its original `requested_at`, so its age reflects the retry
    // schedule (~40 minutes to the attempts cap), not claim latency.
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "retrying",
        "backoff",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET attempts = 12,
             requested_at = NOW() - INTERVAL '40 minutes'
         WHERE node_id = $1
           AND element = 'retrying'"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    // A freshly enqueued ref on the same facet: this is the one whose age
    // answers "is cluster intent reaching nodes?".
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "fresh",
        "ready",
    )
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    let retry_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Retry).await?;
    assert!(
        claim_lag < 60.0,
        "retrying ref inflated claim-stage lag to {claim_lag}s"
    );
    assert!(
        retry_lag > 2_000.0,
        "retry-stage lag lost the backoff-aged ref: {retry_lag}s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retry_ref_past_its_backoff_window_surfaces_as_claim_lag() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    // A ref that failed one apply and whose backoff expired 20 minutes ago
    // with nobody claiming it: the node's node-ctl has stopped claiming.
    // `attempts` cannot fall back to 0 without a completion that is never
    // coming, so keying the claim clock off `attempts` would hide this
    // stall forever while the dead-letter counter also stayed quiet.
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "stalled-retry",
        "ready",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET attempts = 1,
             requested_at = NOW() - INTERVAL '25 minutes',
             next_attempt_at = NOW() - INTERVAL '20 minutes'
         WHERE node_id = $1
           AND element = 'stalled-retry'"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    assert!(
        (1_100.0..1_300.0).contains(&claim_lag),
        "claim lag must be the ~1200s wait since the backoff window opened, got {claim_lag}s"
    );
    // The same row still reports its full lifetime on the retry clock.
    let retry_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Retry).await?;
    assert!(
        retry_lag > 1_400.0,
        "retry lag must be the ref's total lifetime, got {retry_lag}s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ref_inside_its_backoff_window_is_not_claim_lag() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    // Still inside its backoff window: nothing is supposed to claim it yet,
    // so it must not read as a claim stall.
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "backing-off",
        "backoff",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET attempts = 9,
             requested_at = NOW() - INTERVAL '30 minutes',
             next_attempt_at = NOW() + INTERVAL '4 minutes'
         WHERE node_id = $1
           AND element = 'backing-off'"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    assert!(
        claim_lag <= 0.0,
        "a ref inside its backoff window inflated claim lag to {claim_lag}s"
    );
    let retry_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Retry).await?;
    assert!(
        retry_lag > 1_700.0,
        "retry lag lost the backing-off ref: {retry_lag}s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn re_enqueueing_an_unclaimed_ref_does_not_restart_claim_lag() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    // The revival upsert re-stamps `next_attempt_at = NOW()` on an unclaimed
    // never-failed row while preserving `requested_at`. A claim clock keyed
    // off `next_attempt_at` would reset here on every republish, letting a
    // frequently-updated element mask a node that has stopped claiming.
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "republished",
        "ready",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET requested_at = NOW() - INTERVAL '10 minutes',
             next_attempt_at = NOW(),
             attempts = 0
         WHERE node_id = $1
           AND element = 'republished'"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    assert!(
        claim_lag > 500.0,
        "re-enqueue restarted the claim clock: {claim_lag}s, expected ~600s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn actively_claimed_ref_is_not_claim_lag() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    // A node holds a fresh claim and is applying. `ClaimNodeReconcileRefs`
    // would not hand this row out, so a long-but-healthy apply must not read
    // as a queue stall; `reconcile.queue.stale_claims` owns the stuck-claim
    // case.
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "in-flight",
        "claimed",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET requested_at = NOW() - INTERVAL '30 minutes',
             next_attempt_at = NOW() - INTERVAL '30 minutes'
         WHERE node_id = $1
           AND element = 'in-flight'"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    assert!(
        claim_lag <= 0.0,
        "an actively claimed ref inflated claim lag to {claim_lag}s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_claim_counts_as_claim_lag_from_when_it_went_stale() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    // Claimed 10 minutes ago and never completed: the claim went stale
    // `NODE_RECONCILE_CLAIM_STALE_SECS` after it was taken, and the claim RPC
    // has been offering the row again ever since. The clock runs from
    // staleness, not from the original request.
    insert_queue_row(
        &db.pool,
        node_id,
        FACET_SANDBOX_DESIRED_STATE,
        "stale-claim",
        "claimed",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET requested_at = NOW() - INTERVAL '60 minutes',
             claimed_at = NOW() - INTERVAL '10 minutes'
         WHERE node_id = $1
           AND element = 'stale-claim'"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let claim_lag = lag_for(&db.pool, FACET_SANDBOX_DESIRED_STATE, QueueLagStage::Claim).await?;
    let expected = 600.0 - NODE_RECONCILE_CLAIM_STALE_SECS as f64;
    assert!(
        (claim_lag - expected).abs() < 60.0,
        "stale claim should age from staleness (~{expected}s), got {claim_lag}s"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn every_facet_emits_both_stages_when_queue_is_empty() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for facet in RECONCILE_FACETS {
        for stage in [QueueLagStage::Claim, QueueLagStage::Retry] {
            // `lag_for` panics unless the facet emitted a row at all, so
            // this covers both halves of the zero-fill contract: the series
            // exists, and it carries no lag.
            let lag = lag_for(&db.pool, facet.as_str(), stage).await?;
            assert!(
                lag <= 0.0,
                "facet {facet} stage {stage} must emit zero with an empty queue, got {lag}s"
            );
        }
    }
    Ok(())
}

async fn lag_for(pool: &PgPool, facet: &str, stage: QueueLagStage) -> TestResult<f64> {
    let (_, claim_seconds, retry_seconds) = node_queue_lag_seconds(pool)
        .await?
        .into_iter()
        .find(|(row_facet, _, _)| row_facet == facet)
        .unwrap_or_else(|| panic!("known facet {facet} must emit a lag value"));
    Ok(match stage {
        QueueLagStage::Claim => claim_seconds,
        QueueLagStage::Retry => retry_seconds,
    })
}
