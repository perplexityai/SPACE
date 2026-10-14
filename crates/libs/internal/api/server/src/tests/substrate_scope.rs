//! Node-registry statements scope by the substrate value they bind. Each case
//! seeds one node per substrate with both a `node_direct` and a `node_k8s`
//! row, so the joins cannot do the excluding and only `managed_by` differs.

use super::*;
use crate::template_releases_store::{TemplateReadScope, TemplateReleasesStore};

async fn insert_node_with_both_registrations(
    pool: &sqlx::PgPool,
    managed_by: &str,
    address: &str,
    k8s_node_name: &str,
) -> TestResult<NodeId> {
    let id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, 'worker', $3, 'ready')"
    ))
    .bind(id)
    .bind(address)
    .bind(managed_by)
    .execute(pool)
    .await?;
    insert_data_plane_direct_node_endpoint(pool, id, &format!("{k8s_node_name}:9802")).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(id)
    .bind(k8s_node_name)
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test(flavor = "current_thread")]
async fn node_lookups_select_only_their_substrate() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let address = "192.0.2.40";
    let direct =
        insert_node_with_both_registrations(pool, "space", address, "substrate-scope-direct")
            .await?;
    let kubernetes =
        insert_node_with_both_registrations(pool, "k8s", address, "substrate-scope-kubernetes")
            .await?;
    let listed = |rows: Vec<NodeRow>| -> Vec<String> {
        rows.into_iter()
            .map(|row| row.id)
            .filter(|id| *id == direct.to_string() || *id == kubernetes.to_string())
            .collect()
    };

    // Direct listing: only the Direct node.
    assert_eq!(
        listed(fixture.service.fetch_nodes(None, false).await?),
        vec![direct.to_string()]
    );
    // Kubernetes listing: only the Kubernetes node.
    assert_eq!(
        listed(node_inventory::list_kubernetes(pool, None, false).await?),
        vec![kubernetes.to_string()]
    );

    // A Kubernetes node name resolves only to a Kubernetes registration. With
    // no remote address the miss cannot bootstrap a new row.
    let resolved = fixture
        .service
        .resolve_origin_node_id(
            "substrate-scope-kubernetes",
            Some(SpaceNodeRole::Worker),
            None,
        )
        .await?;
    assert_eq!(resolved.id, kubernetes);
    assert!(!resolved.bootstrapped);
    let err = fixture
        .service
        .resolve_origin_node_id("substrate-scope-direct", Some(SpaceNodeRole::Worker), None)
        .await
        .expect_err("a Direct node's name is not a Kubernetes registration");
    assert_eq!(err.code(), Code::NotFound);

    // Both substrates are valid registered origins by id, and both share the
    // caller address's template scope.
    for node_id in [direct, kubernetes] {
        let origin = fixture
            .service
            .resolve_origin_node_id(&node_id.to_string(), Some(SpaceNodeRole::Worker), None)
            .await?;
        assert_eq!(origin.id, node_id);
    }
    let TemplateReadScope::NodeScoped { mut node_ids } = TemplateReleasesStore::new(pool.clone())
        .node_scope_for_address(address.parse()?)
        .await?
    else {
        panic!("an address lookup is node-scoped");
    };
    node_ids.sort();
    let mut expected = vec![direct, kubernetes];
    expected.sort();
    assert_eq!(node_ids, expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_k8s_origin_node_writes_and_updates_only_kubernetes_rows() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let role = SpaceNodeRole::Worker;
    let row = |node_id: NodeId| {
        sqlx::query_as::<_, (String, Option<String>)>(sql!(
            "SELECT managed_by, host(address) FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(pool)
    };

    // A fresh registration is written as Kubernetes-managed.
    let fresh = fixture
        .service
        .bootstrap_k8s_origin_node("substrate-scope-fresh", role, "192.0.2.41".parse()?)
        .await?;
    assert!(fresh.bootstrapped);
    assert_eq!(
        row(fresh.id).await?,
        ("k8s".to_owned(), Some("192.0.2.41".to_owned()))
    );

    // An existing row at the derived id is refreshed only when it is a
    // Kubernetes registration; a Direct row there is refused and left as is.
    for (managed_by, refreshed) in [("k8s", true), ("space", false)] {
        let node_name = format!("substrate-scope-existing-{managed_by}");
        let node_id = space_k8s_node_id(&node_name, role.as_str());
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '192.0.2.42'::INET, 'worker', $2, 'ready')"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(pool)
        .await?;

        let result = fixture
            .service
            .bootstrap_k8s_origin_node(&node_name, role, "192.0.2.43".parse()?)
            .await;
        if refreshed {
            assert_eq!(result?.id, node_id);
            assert_eq!(
                row(node_id).await?,
                ("k8s".to_owned(), Some("192.0.2.43".to_owned()))
            );
        } else {
            let err = result.expect_err("a Direct row at the derived id must not be adopted");
            assert_eq!(err.code(), Code::FailedPrecondition);
            assert_eq!(
                row(node_id).await?,
                ("space".to_owned(), Some("192.0.2.42".to_owned()))
            );
        }
    }
    Ok(())
}
