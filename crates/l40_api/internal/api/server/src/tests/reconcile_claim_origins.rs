use super::*;

/// Queue one network-policy ref for `node_id`, first requested at
/// `requested_at`, with `traceparent` saved.
async fn queue_ref(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    element: &str,
    requested_at: time::OffsetDateTime,
    traceparent: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id, facet, element, requested_at, updated_at, traceparent
         )
         VALUES ($1, 'network-policy', $2, $3, NOW(), $4)"
    ))
    .bind(node_id)
    .bind(element)
    .bind(requested_at)
    .bind(traceparent)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claim_returns_saved_origins_in_claim_order() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    let first = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let second = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    let tie_low = "00-11111111111111111111111111111111-1111111111111111-01";
    let tie_high = "00-22222222222222222222222222222222-2222222222222222-01";
    let element = || format!("sandbox:{}", SandboxId::new());
    let mut tied = [element(), element()];
    tied.sort();
    let now = time::OffsetDateTime::now_utc();
    let ago = |seconds: i64| now - time::Duration::seconds(seconds);

    // Inserted out of order: the claim's order is `requested_at`, then facet,
    // then element, and the origins follow it.
    queue_ref(pool, node_id, &element(), ago(10), Some(first)).await?;
    queue_ref(pool, node_id, &tied[1], ago(15), Some(tie_high)).await?;
    queue_ref(pool, node_id, &element(), ago(30), Some(first)).await?;
    queue_ref(pool, node_id, &tied[0], ago(15), Some(tie_low)).await?;
    queue_ref(pool, node_id, &element(), ago(20), Some(second)).await?;
    queue_ref(pool, node_id, &element(), ago(5), None).await?;
    queue_ref(pool, node_id, &element(), ago(1), Some("not-a-traceparent")).await?;

    let claim = fixture
        .service
        .claim_node_reconcile_refs(request_from_node(
            ClaimNodeReconcileRefsRequest {
                node_id: node_id.to_string(),
                limit: 0,
                role: None,
            },
            TEST_WORKER_IP,
        ))
        .await?
        .into_inner();

    assert_eq!(claim.refs.len(), 7);
    // A traceparent shared by two refs keeps its first ref's place; a ref
    // saved without one, or with an invalid one, adds nothing.
    assert_eq!(
        claim.origin_traceparents,
        [first, second, tie_low, tie_high]
    );
    Ok(())
}
