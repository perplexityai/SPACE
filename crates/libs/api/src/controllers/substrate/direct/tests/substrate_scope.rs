//! Direct's node reads select only Direct (`'space'`) nodes, its create-target
//! claim and release touch only Direct sandboxes, and a stored route
//! identity's substrate is read by its exact label.
//!
//! Each node test registers a Direct node and an otherwise identical twin
//! whose `managed_by` is `'k8s'`. The twin keeps its own live `node_direct`
//! row, so only the `managed_by` predicate can exclude it.

use std::collections::HashSet;

use super::*;

async fn relabel_as_kubernetes(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!("UPDATE nodes SET managed_by = 'k8s' WHERE id = $1"))
        .bind(node_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Two ready workers with the same runtime facts; the second is relabelled
/// as a Kubernetes node.
async fn insert_worker_twins(pool: &PgPool) -> TestResult<(NodeId, NodeId)> {
    let direct = NodeId::new();
    let kubernetes = NodeId::new();
    insert_ready_worker(pool, direct, "127.0.0.1:1", "9.0.0").await?;
    insert_ready_worker(pool, kubernetes, "127.0.0.1:2", "9.0.0").await?;
    relabel_as_kubernetes(pool, kubernetes).await?;
    Ok((direct, kubernetes))
}

fn node_ids<'a>(nodes: impl IntoIterator<Item = &'a DirectNode>) -> HashSet<NodeId> {
    nodes.into_iter().map(|node| node.id).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn worker_selection_reads_only_direct_nodes() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (direct, kubernetes) = insert_worker_twins(&fixture.pool).await?;
    for node_id in [direct, kubernetes] {
        insert_template_inventory(&fixture.pool, node_id, "seed", "hash-seed", "9.0.0").await?;
    }
    sqlx::query(sql!(
        "UPDATE node_template_inventory SET pool_current = 1 WHERE template_name = 'seed'"
    ))
    .execute(&fixture.pool)
    .await?;
    let only_direct = HashSet::from([direct]);

    let ready = fixture.substrate.random_ready_nodes("worker").await?;
    assert_eq!(node_ids(&ready), only_direct, "cached-ready node selection");

    assert_eq!(
        fixture.substrate.eligible_start_node_count().await?,
        1,
        "eligible start node count"
    );

    let cached = fixture
        .substrate
        .cached_worker_create_candidates("seed", Some("hash-seed"), "fc", CreateBootMode::Cold)
        .await?;
    assert_eq!(
        node_ids(&cached),
        only_direct,
        "cached-ready create candidates"
    );

    let pull_on_create = fixture
        .substrate
        .pull_on_create_worker_candidates("fc")
        .await?;
    assert_eq!(
        node_ids(&pull_on_create),
        only_direct,
        "pull-on-create candidates"
    );

    let pooled = fixture
        .substrate
        .pooled_worker_candidates("seed", Some("hash-seed"), "fc")
        .await?;
    assert_eq!(
        pooled
            .iter()
            .map(|candidate| candidate.id)
            .collect::<HashSet<_>>(),
        only_direct,
        "pool candidates"
    );

    // With every heartbeat stale, create falls back to the last-known
    // inventory statement.
    sqlx::query(sql!(
        "UPDATE node_status_snapshots SET stale_after = NOW() - INTERVAL '1 minute'"
    ))
    .execute(&fixture.pool)
    .await?;
    let last_known = fixture
        .substrate
        .cached_worker_create_candidates("seed", Some("hash-seed"), "fc", CreateBootMode::Cold)
        .await?;
    assert_eq!(
        node_ids(&last_known),
        only_direct,
        "last-known create candidates"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn placement_lookups_read_only_direct_nodes() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (direct, kubernetes) = insert_worker_twins(&fixture.pool).await?;
    let on_direct = SandboxId::new();
    let on_kubernetes = SandboxId::new();
    for (sandbox_id, node_id) in [(on_direct, direct), (on_kubernetes, kubernetes)] {
        insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET active_node_id = target_node_id WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&fixture.pool)
        .await?;
    }

    assert_eq!(fixture.substrate.sandbox_node(on_direct).await?.id, direct);
    assert!(matches!(
        fixture.substrate.sandbox_node(on_kubernetes).await,
        Err(SubstrateError::Unavailable { .. })
    ));

    assert_eq!(
        fixture
            .substrate
            .direct_provenance_source_node(on_direct)
            .await?
            .map(|node| node.id),
        Some(direct)
    );
    assert!(
        fixture
            .substrate
            .direct_provenance_source_node(on_kubernetes)
            .await?
            .is_none()
    );

    assert_eq!(
        fixture.substrate.direct_node_by_id(direct).await?.id,
        direct
    );
    assert!(matches!(
        fixture.substrate.direct_node_by_id(kubernetes).await,
        Err(SubstrateError::Unavailable { .. })
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_read_only_direct_nodes() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (direct, kubernetes) = insert_worker_twins(&fixture.pool).await?;

    // Source-node leg: each sandbox's snapshot is held by one of the twins
    // and has no durable copy, so only its holder can restore it.
    let held_by_direct = SandboxId::new();
    let held_by_kubernetes = SandboxId::new();
    for (sandbox_id, node_id) in [(held_by_direct, direct), (held_by_kubernetes, kubernetes)] {
        insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
        insert_observed_source(&fixture.pool, sandbox_id, node_id).await?;
        insert_full_snapshot(&fixture.pool, sandbox_id, node_id, "9.0.0", "failed").await?;
    }
    let candidates = fixture
        .substrate
        .direct_restore_candidates(held_by_direct)
        .await?;
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (candidate.kind, candidate.node.id))
            .collect::<Vec<_>>(),
        vec![(DirectRestoreCandidateKind::SourceNode, direct)]
    );
    assert!(
        fixture
            .substrate
            .direct_restore_candidates(held_by_kubernetes)
            .await?
            .is_empty()
    );

    // Remote leg: a durable copy from a third node can restore on either
    // twin's runtime.
    let source = NodeId::new();
    insert_ready_worker(&fixture.pool, source, "127.0.0.1:3", "9.0.0").await?;
    let durable = SandboxId::new();
    insert_sandbox(&fixture.pool, durable, Some(source)).await?;
    insert_ready_snapshot(
        &fixture.pool,
        durable,
        source,
        "9.0.0",
        Some("s3://synthetic-bucket/durable"),
    )
    .await?;
    let candidates = fixture.substrate.direct_restore_candidates(durable).await?;
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (candidate.kind, candidate.node.id))
            .collect::<Vec<_>>(),
        vec![(DirectRestoreCandidateKind::RemoteSnapshot, direct)]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn builder_lookup_reads_only_direct_nodes() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // The Direct builder points at a listener, so a dial is observable. Live
    // registrations need distinct endpoints.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let direct_endpoint = listener.local_addr()?.to_string();
    let direct = NodeId::new();
    let kubernetes = NodeId::new();
    for (node_id, managed_by, endpoint) in [
        (direct, "space", direct_endpoint.as_str()),
        (kubernetes, "k8s", "127.0.0.1:1"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'builder', $2, 'registered')"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(&fixture.pool)
        .await?;
    }

    let refused = match fixture.substrate.builder_client_for_node(kubernetes).await {
        Ok(_) => panic!("a Kubernetes builder must not be dialed through Direct"),
        Err(error) => error,
    };
    assert_eq!(
        refused.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        refused.to_string().contains("is not registered"),
        "{refused}"
    );

    // Aborted when dropped, including when the bound below fails the test.
    let substrate = fixture.substrate.clone();
    let mut dial = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        substrate.builder_client_for_node(direct).await
    }));
    // Past the lookup the client either dials or fails on transport
    // configuration first; either way it was not refused as unregistered.
    let outcome: TestResult<Option<_>> = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            accepted = listener.accept() => {
                accepted?;
                Ok(None)
            }
            dialed = &mut dial => Ok(Some(dialed?)),
        }
    })
    .await
    .expect("the Direct builder is dialed or fails past the lookup");
    drop(dial);
    if let Some(Err(error)) = outcome? {
        assert!(!error.to_string().contains("is not registered"), "{error}");
    }
    Ok(())
}

/// Only the exact labels name a substrate; any other text, including another
/// spelling of one, is an invalid route identity.
#[test]
fn route_identity_substrate_kind_reads_only_the_exact_labels() {
    use pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind;
    for (stored, expected) in [
        ("space", Some(SandboxSubstrateKind::Direct)),
        ("k8s", Some(SandboxSubstrateKind::Kubernetes)),
        ("direct", None),
        ("kubernetes", None),
        ("SPACE", None),
        ("K8S", None),
        (" k8s", None),
        ("space ", None),
        ("", None),
    ] {
        assert_eq!(
            route_identity_substrate_kind(stored),
            expected,
            "{stored:?}"
        );
    }
}

async fn create_target(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<NodeId>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT target_node_id FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Direct claims and releases a create target only on its own rows: an
/// otherwise identical `'k8s'` row awaiting creation is refused and left as
/// it was.
#[tokio::test(flavor = "current_thread")]
async fn create_target_claims_only_direct_rows() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let node = NodeId::new();
    insert_ready_worker(pool, node, "127.0.0.1:1", "9.0.0").await?;
    let direct = SandboxId::new();
    let kubernetes = SandboxId::new();
    for (sandbox_id, managed_by) in [(direct, "space"), (kubernetes, "k8s")] {
        insert_sandbox(pool, sandbox_id, None).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'creating', managed_by = $2 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(managed_by)
        .execute(pool)
        .await?;
    }

    fixture.substrate.claim_create_target(direct, node).await?;
    assert!(matches!(
        fixture
            .substrate
            .claim_create_target(kubernetes, node)
            .await,
        Err(SubstrateError::Conflict { .. })
    ));
    assert_eq!(create_target(pool, direct).await?, Some(node));
    assert_eq!(create_target(pool, kubernetes).await?, None);

    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(kubernetes)
    .bind(node)
    .execute(pool)
    .await?;
    for sandbox_id in [direct, kubernetes] {
        fixture
            .substrate
            .release_create_target(sandbox_id, node)
            .await?;
    }
    assert_eq!(create_target(pool, direct).await?, None);
    assert_eq!(create_target(pool, kubernetes).await?, Some(node));
    Ok(())
}
