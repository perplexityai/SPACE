//! The Direct node registry reads and writes only Direct (`'space'`) nodes.
//!
//! Each case pairs a Direct node with an otherwise identical node whose
//! `managed_by` is `'k8s'`. Both keep a `node_direct` row, so only the
//! `managed_by` predicates decide which one a registry statement acts on.

use super::*;

async fn insert_registered_node(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    managed_by: &str,
    endpoint: &str,
    retired_age_secs: Option<i64>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.70'::INET, 'worker', $2, 'registered')"
    ))
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, updated_at, deleted_at)
         VALUES (
             $1, 'registry-node', $2,
             NOW() - (COALESCE($3, 0) * INTERVAL '1 second'),
             CASE WHEN $3::BIGINT IS NULL THEN NULL ELSE NOW() END
         )"
    ))
    .bind(node_id)
    .bind(endpoint)
    .bind(retired_age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn registration(
    pool: &sqlx::PgPool,
    node_id: NodeId,
) -> Result<(String, Option<String>, bool), sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT n.managed_by, nd.name, nd.deleted_at IS NOT NULL
         FROM nodes n
         JOIN node_direct nd ON nd.node_id = n.id
         WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await
}

async fn response_json(
    response: axum::response::Response,
) -> TestResult<(StatusCode, serde_json::Value)> {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 100_000).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

#[tokio::test(flavor = "current_thread")]
async fn node_registry_writes_only_direct_nodes() -> TestResult {
    // `AppState::from_pool` chooses its substrate from the process
    // environment; the registry routes need Direct.
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_K8S_CONTROL_PLANE",
        Some(std::ffi::OsStr::new("false")),
    )]) {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let actor = sqlx::query_as::<_, crate::models::User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(pool)
    .await?;
    let state = AppState::from_pool(pool.clone()).await?;
    let create = |request: serde_json::Value| {
        let state = state.clone();
        let actor = actor.clone();
        async move {
            let request: CreateNodeRequest = serde_json::from_value(request)?;
            let response = create_node(State(state), Extension(AuthUser(actor)), Json(request))
                .await
                .into_response();
            response_json(response).await
        }
    };

    // Re-registering a retired endpoint reactivates the newest retired Direct
    // node at that endpoint, never a newer retired Kubernetes one.
    let endpoint = "10.0.0.71:9802";
    let retired_direct = NodeId::new();
    let retired_kubernetes = NodeId::new();
    insert_registered_node(pool, retired_direct, "space", endpoint, Some(7200)).await?;
    insert_registered_node(pool, retired_kubernetes, "k8s", endpoint, Some(3600)).await?;
    let (status, body) = create(serde_json::json!({
        "node_api_endpoint": endpoint,
        "role": "worker",
        "name": "reactivated",
    }))
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], retired_direct.to_string());
    assert_eq!(
        registration(pool, retired_direct).await?,
        ("space".into(), Some("reactivated".into()), false)
    );
    assert_eq!(
        registration(pool, retired_kubernetes).await?,
        ("k8s".into(), Some("registry-node".into()), true)
    );

    // A new endpoint registers a new Direct node.
    let (status, body) = create(serde_json::json!({
        "node_api_endpoint": "10.0.0.72:9802",
        "role": "worker",
    }))
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let registered: NodeId = serde_json::from_value(body["id"].clone())?;
    assert_eq!(registration(pool, registered).await?.0, "space");

    // Updates and deletes reach only the Direct twin. A `verify` update reads
    // the node before it dials the node's endpoint. The Kubernetes twin's is a
    // closed local port, so an update that wrongly dials it fails at once with
    // a gateway error instead of the 404.
    let live_direct = NodeId::new();
    let live_kubernetes = NodeId::new();
    insert_registered_node(pool, live_direct, "space", "10.0.0.73:9802", None).await?;
    insert_registered_node(pool, live_kubernetes, "k8s", "127.0.0.1:1", None).await?;
    for (node_id, verify, expected_status, expected_name) in [
        (
            live_kubernetes,
            false,
            StatusCode::NOT_FOUND,
            "registry-node",
        ),
        (
            live_kubernetes,
            true,
            StatusCode::NOT_FOUND,
            "registry-node",
        ),
        (live_direct, false, StatusCode::OK, "renamed"),
    ] {
        let request: UpdateNodeRequest =
            serde_json::from_value(serde_json::json!({ "name": "renamed", "verify": verify }))?;
        let response = update_node(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(node_id),
            Json(request),
        )
        .await
        .into_response();
        let (status, body) = response_json(response).await?;
        assert_eq!(status, expected_status, "{body}");
        assert_eq!(
            registration(pool, node_id).await?.1.as_deref(),
            Some(expected_name)
        );
    }

    assert_eq!(hard_delete_direct_node(pool, live_kubernetes).await?, 0);
    assert_eq!(registration(pool, live_kubernetes).await?.0, "k8s");
    assert_eq!(hard_delete_direct_node(pool, live_direct).await?, 1);
    Ok(())
}
