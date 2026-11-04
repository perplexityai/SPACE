//! Route tests for `GET /runtime-environments/nodes`: conformance and paging.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn node_status_compares_each_published_policy_a_page_at_a_time() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    h.set_default(e0).await;
    h.set_default(e1).await;
    h.set_state(e0, json!({"state": "blocked", "reason": "0-day"}))
        .await;

    let node = |n: u128| NodeId::from_uuid(Uuid::from_u128(n));
    let (conforming, lagging, silent, unreadable, stale, deleted) =
        (node(1), node(2), node(3), node(4), node(5), node(6));
    for id in [conforming, lagging, silent, unreadable, stale, deleted] {
        insert_ready_worker_node(&h.db.pool, id).await?;
    }
    publish_policy(
        &h.db.pool,
        conforming,
        false,
        Some(published(&applied_observation(&[e1], Some(e1), 3))),
    )
    .await?;
    publish_policy(
        &h.db.pool,
        lagging,
        false,
        Some(published(&applied_observation(&[e0, e1], Some(e0), 2))),
    )
    .await?;
    publish_policy(&h.db.pool, silent, false, None).await?;
    publish_policy(
        &h.db.pool,
        unreadable,
        false,
        Some(json!({"schema_version": 99})),
    )
    .await?;
    publish_policy(
        &h.db.pool,
        stale,
        true,
        Some(published(&observation(&[e1], Some(e1), false))),
    )
    .await?;
    // Soft deletion is a direct-substrate concept.
    sqlx::query(sql!("UPDATE nodes SET managed_by = 'space' WHERE id = $1"))
        .bind(deleted)
        .execute(&h.db.pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, deleted_at)
         VALUES ($1, 'gone', 'http://127.0.0.1:1', NOW())"
    ))
    .bind(deleted)
    .execute(&h.db.pool)
    .await?;

    let (status, body) = h.get("/admin/api/v1/runtime-environments/nodes").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], 3);
    assert_eq!(body["default_environment_ref"], e1.to_string());
    assert_eq!(body["next_after"], Value::Null);
    let nodes = body["nodes"].as_array().unwrap();
    let ids: Vec<_> = nodes
        .iter()
        .map(|n| n["node_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        [conforming, lagging, silent, unreadable, stale].map(|id| id.to_string()),
        "soft-deleted nodes are left out"
    );
    // Nodes on the applied schema report the generation they applied; one
    // on its startup policy (the stale node's schema 1) reports none.
    assert_eq!(nodes[0]["applied_generation"], 3);
    assert_eq!(nodes[0]["applied_current"], true);
    assert_eq!(nodes[1]["applied_generation"], 2);
    assert_eq!(nodes[1]["applied_current"], false);
    assert_eq!(nodes[4]["applied_generation"], Value::Null);
    assert_eq!(nodes[4]["applied_current"], Value::Null);
    assert_eq!(nodes[0]["conformance"], "conforms");
    assert_eq!(nodes[0]["status_fresh"], true);
    assert_eq!(nodes[0]["role"], "worker");
    assert!(nodes[0]["policy_received_at"].is_string());
    assert_eq!(
        nodes[0]["enforced"]["default_environment_reference"],
        e1.to_string()
    );
    assert_eq!(nodes[1]["conformance"], "diverges");
    assert_eq!(nodes[1]["default_differs"], true);
    assert_eq!(nodes[1]["executes_outside_policy"], json!([e0.to_string()]));
    for unknown in &nodes[2..4] {
        assert_eq!(unknown["conformance"], "unknown", "{unknown}");
        assert_eq!(unknown["enforced"], Value::Null);
        assert_eq!(unknown["policy_received_at"], Value::Null);
    }
    assert_eq!(nodes[4]["conformance"], "conforms");
    assert_eq!(nodes[4]["status_fresh"], false);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_status_pages_hold_at_most_one_hundred_nodes() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let total = NODE_PAGE_SIZE + 1;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         SELECT space_uuid_v7(), '127.0.0.1', 'worker', 'k8s', 'ready'
         FROM generate_series(1, $1)"
    ))
    .bind(i32::try_from(total)?)
    .execute(&h.db.pool)
    .await?;

    let (status, first) = h.get("/admin/api/v1/runtime-environments/nodes").await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let first_nodes = first["nodes"].as_array().unwrap();
    assert_eq!(first_nodes.len(), NODE_PAGE_SIZE);
    let cursor = first["next_after"].as_str().unwrap();
    assert_eq!(cursor, first_nodes.last().unwrap()["node_id"]);

    let (status, second) = h
        .get(&format!(
            "/admin/api/v1/runtime-environments/nodes?after={cursor}"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    let second_nodes = second["nodes"].as_array().unwrap();
    assert_eq!(second_nodes.len(), 1);
    assert_eq!(second["next_after"], Value::Null);
    assert!(second_nodes[0]["node_id"].as_str().unwrap() > cursor);

    // Exactly one full page is the last page.
    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(Uuid::parse_str(
            second_nodes[0]["node_id"].as_str().unwrap(),
        )?)
        .execute(&h.db.pool)
        .await?;
    let (status, only) = h.get("/admin/api/v1/runtime-environments/nodes").await;
    assert_eq!(status, StatusCode::OK, "{only}");
    assert_eq!(only["nodes"].as_array().unwrap().len(), NODE_PAGE_SIZE);
    assert_eq!(only["next_after"], Value::Null);

    let (status, _) = h
        .get("/admin/api/v1/runtime-environments/nodes?after=not-a-uuid")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}
