use super::*;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use pplx_space_common_core::{
    sql,
    types::{AutoLifecyclePolicy, AutoLifecycleStage, AutoLifecycleWakeSource, NodeId, SandboxId},
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_node_api_sim::SimWorkerHarness;
use serde_json::{Value, json};
use sqlx::{PgPool, types::Json as SqlJson};
use tower::ServiceExt;

const KEY: &str = "timeout-test-client-key";

#[test]
fn deadline_allows_five_seconds_of_skew_and_never_precedes_observation() {
    let observed = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap()
        + time::Duration::milliseconds(123);
    for (timeout, remaining) in [(1, 0), (5, 0), (6, 1), (600, 595), (u32::MAX, u32::MAX - 5)] {
        let deadline = conservative_deadline(observed, timeout).unwrap();
        assert_eq!(
            deadline,
            observed + time::Duration::seconds(i64::from(remaining))
        );
        assert!(deadline >= observed);
    }
}

async fn app(pool: &PgPool) -> TestDbResult<Router> {
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'timeout-test', $3, ARRAY['native'])"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(pool)
    .await?;
    let state = AppState::from_pool(pool.clone()).await?;
    Ok(Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state))
}

async fn send(
    app: &Router,
    method: &str,
    id: SandboxId,
    key: Option<&str>,
    body: Value,
) -> TestDbResult<(StatusCode, Value)> {
    let path = format!(
        "/v1/sandboxes/{}{}",
        pplx_space_public_api::PublicSandboxId::from(id),
        if method == "POST" { ":set_timeout" } else { "" }
    );
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(serde_json::to_vec(&body)?))?)
        .await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn seed(
    pool: &PgPool,
    lifecycle: &str,
    policy: &AutoLifecyclePolicy,
    node: Option<NodeId>,
) -> TestDbResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, active_node_id,
             auto_lifecycle_json, next_auto_event_at, template_id, network_policy_generation)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'seed', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .bind(if node.is_some() {
        "on_node"
    } else {
        "unplaced"
    })
    .bind(node)
    .bind(SqlJson(policy))
    .bind(OffsetDateTime::now_utc().unix_timestamp() - 100)
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test]
async fn timeout_validation_scope_and_policy_matrix() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let app = app(&db.pool).await?;
    let default = crate::e2b::auto_lifecycle_policy(21_600, true, false);
    let id = seed(&db.pool, "suspended", &default, None).await?;
    for timeout in [
        json!(null),
        json!(0),
        json!("0s"),
        json!("1.5s"),
        json!("4294967296s"),
        json!("bad"),
    ] {
        let (status, body) = send(
            &app,
            "POST",
            id,
            Some("invalid"),
            json!({"timeout":timeout}),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_request");
    }
    for body in [json!({}), json!({"timeout":"1s"})] {
        let (status, body) = send(&app, "POST", id, None, body).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_request");
    }
    let foreign = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000002', 'suspended', 1)"
    ))
    .bind(foreign)
    .bind(SEED_USER_ID)
    .execute(&db.pool)
    .await?;
    for target in [foreign, SandboxId::new()] {
        let (status, body) = send(
            &app,
            "POST",
            target,
            Some("foreign"),
            json!({"timeout":"1m"}),
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["code"], "not_found");
    }
    let mut multi = default.clone();
    multi.stages.push(AutoLifecycleStage {
        at: 30_000,
        ..multi.stages[0].clone()
    });
    let mut non_access = default.clone();
    non_access
        .condition_sets
        .values_mut()
        .next()
        .unwrap()
        .activity_source =
        Some(pplx_space_common_core::types::AutoLifecycleActivitySource::Command);
    let mut custom_wake = default.clone();
    custom_wake.wake.on = vec![AutoLifecycleWakeSource::Command];
    for (lifecycle, policy, expected) in [
        ("deleting", default.clone(), StatusCode::PRECONDITION_FAILED),
        ("failed", default.clone(), StatusCode::PRECONDITION_FAILED),
        (
            "quarantined",
            default.clone(),
            StatusCode::PRECONDITION_FAILED,
        ),
        ("stopped", default.clone(), StatusCode::PRECONDITION_FAILED),
        ("deleted", default.clone(), StatusCode::PRECONDITION_FAILED),
        (
            "suspended",
            AutoLifecyclePolicy::disabled(),
            StatusCode::PRECONDITION_FAILED,
        ),
        ("suspended", multi, StatusCode::PRECONDITION_FAILED),
        ("suspended", non_access, StatusCode::PRECONDITION_FAILED),
        ("suspended", custom_wake, StatusCode::PRECONDITION_FAILED),
    ] {
        let id = seed(&db.pool, lifecycle, &policy, None).await?;
        let (status, body) =
            send(&app, "POST", id, Some("rejected"), json!({"timeout":"1m"})).await?;
        assert_eq!(status, expected, "{lifecycle}: {body}");
        assert_eq!(
            body["code"],
            if expected == StatusCode::NOT_FOUND {
                "not_found"
            } else {
                "failed_precondition"
            }
        );
        let SqlJson(stored): SqlJson<AutoLifecyclePolicy> = sqlx::query_scalar(sql!(
            "SELECT auto_lifecycle_json FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(stored, policy);
    }
    let id = seed(&db.pool, "suspended", &default, None).await?;
    let sandbox = fetch_sandbox(&db.pool, id).await?.unwrap();
    let mut replacement = default.clone();
    replacement.wake.on = vec![AutoLifecycleWakeSource::Command];
    sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(SqlJson(&replacement))
    .execute(&db.pool)
    .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let error = state
        .sandbox_controller
        .set_access_idle_timeout(
            &db.pool,
            &sandbox,
            90,
            prost_types::Timestamp {
                seconds: OffsetDateTime::now_utc().unix_timestamp(),
                nanos: 0,
            },
            tokio::time::Instant::now() + NODE_RPC_TIMEOUT,
            Some(&default),
        )
        .await
        .expect_err("a concurrent policy change must not be simplified");
    assert_eq!(error.status(), StatusCode::PRECONDITION_FAILED);
    let SqlJson(stored): SqlJson<AutoLifecyclePolicy> = sqlx::query_scalar(sql!(
        "SELECT auto_lifecycle_json FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(stored, replacement);
    Ok(())
}

#[tokio::test]
async fn dormant_timeout_persists_without_waking_and_caches_retries() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let app = app(&db.pool).await?;
    for (lifecycle, pause, resume, initial_timeout) in [
        ("paused", true, true, 21_600),
        ("suspended", false, false, 21_600),
        ("created", true, false, 21_600),
        ("started", true, false, 21_600),
        ("suspended", true, true, u64::from(u32::MAX) + 1),
    ] {
        let policy = crate::e2b::access_idle_policy(initial_timeout, pause, resume);
        let id = seed(&db.pool, lifecycle, &policy, None).await?;
        if lifecycle == "started" {
            sqlx::query(sql!(
                "UPDATE sandboxes SET managed_by = 'k8s' WHERE id = $1"
            ))
            .bind(id)
            .execute(&db.pool)
            .await?;
        }
        let key = id.to_string();
        let (status, first) = send(&app, "POST", id, Some(&key), json!({"timeout":"1.5m"})).await?;
        assert_eq!(status, StatusCode::OK, "{first}");
        assert_eq!(first["idle_timeout"], "90s");
        assert!(first["expires_at"].is_null());
        let (status, public) = send(&app, "GET", id, None, json!(null)).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(public["idle_timeout"], "90s");
        assert_eq!(
            public["idle_action"],
            if pause { "suspend" } else { "delete" }
        );
        let (stored_lifecycle, SqlJson(stored)): (String, SqlJson<AutoLifecyclePolicy>) =
            sqlx::query_as(sql!(
                "SELECT lifecycle, auto_lifecycle_json FROM sandboxes WHERE id = $1"
            ))
            .bind(id)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(stored_lifecycle, lifecycle);
        assert_eq!(stored.wake, policy.wake);
        let (_, retry) = send(&app, "POST", id, Some(&key), json!({"timeout":"90s"})).await?;
        assert_eq!(retry, first);
        let (status, conflict) =
            send(&app, "POST", id, Some(&key), json!({"timeout":"91s"})).await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(conflict["code"], "idempotency_conflict");
    }
    Ok(())
}

#[tokio::test]
async fn running_timeout_restarts_clock_without_using_stale_publication() -> TestDbResult<()> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let node = NodeId::new();
    let worker = SimWorkerHarness::builder(node.to_string())
        .with_internal_api()
        .start()
        .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node)
    .bind(worker.endpoint())
    .execute(&db.pool)
    .await?;
    let app = app(&db.pool).await?;
    for (pause, initial_timeout) in [(false, 300), (true, 90)] {
        let policy = crate::e2b::auto_lifecycle_policy(initial_timeout, pause, pause);
        let id = seed(&db.pool, "started", &policy, Some(node)).await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_identities
             (sandbox_id, route_generation, current_node_id, substrate_kind, execution_generation, activation_attempt_id)
             VALUES ($1, 7, $2, 'space', 7, $3)"
        )).bind(id).bind(node).bind(uuid::Uuid::now_v7()).execute(&db.pool).await?;
        let key = id.to_string();
        worker.state().ensure_default_running(&key).await?;
        worker
            .state()
            .set_route_identity_provenance(&key, Some(7), Some(7), None)
            .await?;
        let (_, before) = send(&app, "GET", id, None, json!(null)).await?;
        let started = OffsetDateTime::now_utc();
        let (status, response) =
            send(&app, "POST", id, Some(&key), json!({"timeout":"90s"})).await?;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_ne!(response["expires_at"], before["expires_at"]);
        let expires = OffsetDateTime::parse(
            response["expires_at"].as_str().unwrap(),
            &time::format_description::well_known::Rfc3339,
        )?;
        assert!(expires >= conservative_deadline(started, 90).unwrap());
        let record = worker.state().get(&key).await?;
        use prost::Message;
        let applied = crate::controllers::auto_lifecycle::policy_from_proto(
            &pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy::decode(
                record.access_idle_policy.as_slice(),
            )?,
        )?;
        assert_eq!(applied.stages[0].at, 90);
        assert_eq!(applied.stages[0].action, policy.stages[0].action);
        assert_eq!(applied.wake, policy.wake);
        let published = OffsetDateTime::now_utc().unix_timestamp() + 90;
        sqlx::query(sql!(
            "UPDATE sandboxes SET next_auto_event_at = $2 WHERE id = $1"
        ))
        .bind(id)
        .bind(published)
        .execute(&db.pool)
        .await?;
        let (_, public) = send(&app, "GET", id, None, json!(null)).await?;
        assert_eq!(public["idle_timeout"], "90s");
        assert_eq!(
            public["idle_action"],
            if pause { "suspend" } else { "delete" }
        );
        assert_ne!(public["expires_at"], before["expires_at"]);
        assert!(expires.unix_timestamp() <= published);
        worker
            .state()
            .apply_access_deadline(&key, Some(published + 1000), None)
            .await?;
        let (status, retry) = send(&app, "POST", id, Some(&key), json!({"timeout":"90s"})).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(retry, response);
        assert_eq!(
            worker.state().get(&key).await?.access_extended_until,
            Some(published + 1000)
        );
        assert_eq!(
            applied.wake.on,
            if pause {
                vec![AutoLifecycleWakeSource::Access]
            } else {
                vec![]
            }
        );
    }
    Ok(())
}
