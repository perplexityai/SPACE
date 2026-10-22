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

    // A Kubernetes node name resolves only to a Kubernetes registration; a
    // miss is NotFound rather than a new row.
    let resolved = fixture
        .service
        .resolve_origin_node_id("substrate-scope-kubernetes", Some(SpaceNodeRole::Worker))
        .await?;
    assert_eq!(resolved.id, kubernetes);
    let err = fixture
        .service
        .resolve_origin_node_id("substrate-scope-direct", Some(SpaceNodeRole::Worker))
        .await
        .expect_err("a Direct node's name is not a Kubernetes registration");
    assert_eq!(err.code(), Code::NotFound);

    // Both substrates are valid registered origins by id, and both share the
    // caller address's template scope.
    for node_id in [direct, kubernetes] {
        let origin = fixture
            .service
            .resolve_origin_node_id(&node_id.to_string(), Some(SpaceNodeRole::Worker))
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
