//! The `:drop-memory` route against Postgres: the switch, the request's
//! shape, eligibility and the enqueued operation. The drop itself is
//! `controllers::sandbox::memory_drop`'s and the executor's.

use std::str::FromStr;

use axum::http::{Method, StatusCode};
use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_node_model::SubstrateKind;
use pplx_space_public_api::{OperationId, PublicSandboxId};
use pplx_space_sql_locator::sql;
use serde_json::json;
use uuid::Uuid;

use crate::routes::admin::test_fixtures::{
    AdminRouterHarness, TestResult, admin_router_harness, admin_router_harness_with,
};

async fn enabled_harness() -> TestResult<Option<AdminRouterHarness>> {
    admin_router_harness_with(|state| state.memory_drop_enabled = true).await
}

/// A sandbox the drop accepts: Kubernetes, suspended and unplaced off a node
/// that's gone, pinned to a durable full suspend snapshot.
async fn droppable_sandbox(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    user_id: UserId,
) -> TestResult<PublicSandboxId> {
    let id = Uuid::now_v7();
    let artifact_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             target_node_id, network_policy_generation,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin)
         VALUES ($1, $2, $3, $4, 'suspended', 'unplaced', $5, 1, $6, 'full',
                 'suspend')"
    ))
    .bind(id)
    .bind(user_id)
    .bind(org_id)
    .bind(SubstrateKind::Kubernetes)
    .bind(NodeId::new())
    .bind(artifact_id)
    .execute(pool)
    .await?;
    // The last run's node, which has no `node_k8s` mapping: gone.
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
             state, active, terminated_at)
         VALUES ($1, 1, $2, 'space-sandboxes', 'worker-gone', 'terminated',
                 FALSE, NOW() - INTERVAL '1 hour')"
    ))
    .bind(id)
    .bind(format!("sbx-{id}"))
    .execute(pool)
    .await?;
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, timestamp, local_artifact_id,
             artifact_kind, artifact_origin, org_id, vcpu, memory_mib)
         VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '1 hour', $3, 'full',
                 'suspend', $4, 2, 512)"
    ))
    .bind(snapshot_id)
    .bind(id)
    .bind(artifact_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
            (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot_id)
    .bind(format!(
        "s3://test-bucket/suspend_snapshots/00/{id}/{snapshot_id}?region=us-east-1"
    ))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(PublicSandboxId::from_uuid(id))
}

fn path(sandbox: PublicSandboxId) -> String {
    format!("/sandboxes/{sandbox}:drop-memory")
}

#[tokio::test]
async fn the_switch_off_refuses_with_a_503() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    let (status, body) = harness
        .as_operator(
            Method::POST,
            &path(sandbox),
            &[("idempotency-key", "drop-1")],
            Some(json!({"reason": "node retired"})),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let events = harness.audit_events(&path(sandbox)).await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["status_code"], 503);
    assert_eq!(events[0]["target_id"], sandbox.to_string());
    Ok(())
}

#[tokio::test]
async fn only_operators_drop_memory() -> TestResult {
    let Some(harness) = enabled_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    harness
        .assert_refused_to_non_operators(
            Method::POST,
            &path(sandbox),
            Some(json!({"reason": "node retired"})),
        )
        .await;
    Ok(())
}

#[tokio::test]
async fn a_drop_needs_a_reason_and_an_idempotency_key() -> TestResult {
    let Some(harness) = enabled_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    let key = [("idempotency-key", "drop-1")];
    for body in [
        None,
        Some(json!({})),
        Some(json!({"reason": "  "})),
        Some(json!({"reason": "why", "extra": true})),
    ] {
        let (status, error) = harness
            .as_operator(Method::POST, &path(sandbox), &key, body.clone())
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}: {error}");
    }
    let (status, error) = harness
        .as_operator(
            Method::POST,
            &path(sandbox),
            &[],
            Some(json!({"reason": "node retired"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    let missing = PublicSandboxId::from_uuid(Uuid::now_v7());
    let (status, _) = harness
        .as_operator(
            Method::POST,
            &path(missing),
            &key,
            Some(json!({"reason": "node retired"})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

/// A sandbox another drop holds sealed is refused with a retryable 503, not
/// a 409: the refusal ends when that drop commits or its seal lapses.
#[tokio::test]
async fn a_sealed_sandbox_is_refused_as_retryable() -> TestResult {
    let Some(harness) = enabled_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    // The write keeps the row `suspended`, so the trigger keeps the lease.
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET restore_lease_token = gen_random_uuid(),
             restore_lease_expires_at = NOW() + INTERVAL '1 minute'
         WHERE id = $1"
    ))
    .bind(sandbox.as_uuid())
    .execute(&harness.db.pool)
    .await?;
    let (status, error) = harness
        .as_operator(
            Method::POST,
            &path(sandbox),
            &[("idempotency-key", "drop-1")],
            Some(json!({"reason": "node retired"})),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{error}");
    assert!(
        error["error"]
            .as_str()
            .is_some_and(|message| message.contains("memory_drop_in_progress")),
        "{error}"
    );
    assert_eq!(error["retryable"], true, "{error}");
    let operations: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM public_operations WHERE target_id = $1"
    ))
    .bind(sandbox.to_string())
    .fetch_one(&harness.db.pool)
    .await?;
    assert_eq!(operations, 0);
    Ok(())
}

#[tokio::test]
async fn an_ineligible_sandbox_is_refused_with_the_reason() -> TestResult {
    let Some(harness) = enabled_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started', placement = 'placed' WHERE id = $1"
    ))
    .bind(sandbox.as_uuid())
    .execute(&harness.db.pool)
    .await?;
    let (status, error) = harness
        .as_operator(
            Method::POST,
            &path(sandbox),
            &[("idempotency-key", "drop-1")],
            Some(json!({"reason": "node retired"})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert!(
        error["error"]
            .as_str()
            .is_some_and(|message| message.ends_with("not_suspended")),
        "{error}"
    );
    assert_eq!(error["retryable"], false, "{error}");
    let operations: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM public_operations WHERE target_id = $1"
    ))
    .bind(sandbox.to_string())
    .fetch_one(&harness.db.pool)
    .await?;
    assert_eq!(operations, 0);
    Ok(())
}

/// An eligible drop enqueues one `drop_sandbox_memory` operation under the
/// sandbox's organization, carrying the reason; a replay returns it, and the
/// same key with another reason is a conflict.
#[tokio::test]
async fn an_eligible_drop_enqueues_one_operation() -> TestResult {
    let Some(harness) = enabled_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    let key = [("idempotency-key", "drop-1")];
    let body = json!({"reason": "node retired"});
    let (status, accepted) = harness
        .as_operator(Method::POST, &path(sandbox), &key, Some(body.clone()))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    assert_eq!(accepted["sandbox_id"], sandbox.to_string());
    let operation_id = accepted["operation_id"].clone();

    let rows: Vec<(Uuid, String, Uuid, serde_json::Value)> = sqlx::query_as(sql!(
        "SELECT operation_id, kind, org_id, payload_json
           FROM public_operations WHERE target_id = $1"
    ))
    .bind(sandbox.to_string())
    .fetch_all(&harness.db.pool)
    .await?;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let (id, kind, org_id, payload) = &rows[0];
    let accepted_id = operation_id.as_str().expect("operation id");
    assert_eq!(*id, OperationId::from_str(accepted_id)?.as_uuid());
    assert_eq!(kind, "drop_sandbox_memory");
    assert_eq!(*org_id, harness.tenant_org.as_uuid());
    assert_eq!(
        *payload,
        json!({"sandbox_id": sandbox.to_string(), "reason": "node retired"})
    );

    let (status, replay) = harness
        .as_operator(Method::POST, &path(sandbox), &key, Some(body))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
    assert_eq!(replay["operation_id"], operation_id);
    let (status, conflict) = harness
        .as_operator(
            Method::POST,
            &path(sandbox),
            &key,
            Some(json!({"reason": "another reason"})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");

    let events = harness.audit_events(&path(sandbox)).await;
    let statuses: Vec<_> = events
        .iter()
        .map(|event| event["status_code"].clone())
        .collect();
    assert_eq!(statuses, vec![json!(409), json!(202), json!(202)]);
    Ok(())
}

/// A sandbox already dropped isn't refused: the operation finds it done, so
/// a retry under a new key answers as the first request did.
#[tokio::test]
async fn an_already_dropped_sandbox_is_accepted() -> TestResult {
    let Some(harness) = enabled_harness().await? else {
        return Ok(());
    };
    let sandbox =
        droppable_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = gen_random_uuid(),
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox.as_uuid())
    .execute(&harness.db.pool)
    .await?;
    let (status, accepted) = harness
        .as_operator(
            Method::POST,
            &path(sandbox),
            &[("idempotency-key", "drop-2")],
            Some(json!({"reason": "node retired"})),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    Ok(())
}
