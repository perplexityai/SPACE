//! The admin Direct node registry's `nodes` statements against Postgres, each
//! one's losing case included, and the registry-row reads.

use std::collections::HashSet;

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use pplx_space_postgres_pool::DbErrorKind;
use sqlx::PgPool;

use super::*;
use crate::tests::direct_seed::{direct_node, endpoint, k8s_name, node};

async fn insert_node(
    pool: &PgPool,
    node_id: NodeId,
    managed_by: SubstrateKind,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.60'::INET, 'worker', $2, 'registered')"
    ))
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    Ok(())
}

/// The node's `node_direct` row at `endpoint`, deleted when `retired`.
async fn insert_registration(
    pool: &PgPool,
    node_id: NodeId,
    endpoint: &str,
    retired: bool,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, deleted_at)
         VALUES ($1, 'registry-node', $2, CASE WHEN $3::BOOL THEN NOW() END)"
    ))
    .bind(node_id)
    .bind(endpoint)
    .bind(retired)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every column of the node's `nodes` row, to show what a statement left.
async fn node_row(pool: &PgPool, node_id: NodeId) -> TestDbResult<Option<serde_json::Value>> {
    Ok(sqlx::query_scalar::<_, serde_json::Value>(sql!(
        "SELECT to_jsonb(n) FROM nodes n WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(pool)
    .await?)
}

fn write(managed_by: SubstrateKind) -> RegistryNodeWrite<'static> {
    RegistryNodeWrite {
        address: Some("10.0.0.61"),
        role: NodeRole::Builder,
        managed_by,
        state: Some("ready"),
        stamp_heartbeat: true,
    }
}

fn update() -> RegistryNodeUpdate<'static> {
    RegistryNodeUpdate {
        address: Some("10.0.0.62"),
        role: None,
        state: Some("cordoned"),
        stamp_heartbeat: false,
    }
}

/// Every column of every `nodes` row, to show a statement changed none.
async fn all_nodes(pool: &PgPool) -> TestDbResult<serde_json::Value> {
    Ok(sqlx::query_scalar::<_, serde_json::Value>(sql!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(n) ORDER BY n.id), '[]') FROM nodes n"
    ))
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn reactivate_retired_node_without_a_retired_node_writes_nothing() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let retired = NodeId::new();
    insert_node(pool, retired, SubstrateKind::Space).await?;
    insert_registration(pool, retired, "10.0.0.62:9802", true).await?;
    let before = all_nodes(pool).await?;

    // Neither another id nor another endpoint matches, and the retired
    // node's endpoint matches only when the caller asks it to.
    for (endpoint, match_endpoint) in [("10.0.0.99:9802", true), ("10.0.0.62:9802", false)] {
        assert_eq!(
            reactivate_retired_node(
                pool,
                NodeId::new(),
                endpoint,
                match_endpoint,
                &write(SubstrateKind::Space)
            )
            .await?,
            None
        );
    }
    assert_eq!(all_nodes(pool).await?, before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivate_retired_node_skips_a_live_registration_at_the_endpoint() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let live = NodeId::new();
    insert_node(pool, live, SubstrateKind::Space).await?;
    insert_registration(pool, live, "10.0.0.60:9802", false).await?;
    let before = all_nodes(pool).await?;

    for node_id in [live, NodeId::new()] {
        assert_eq!(
            reactivate_retired_node(
                pool,
                node_id,
                "10.0.0.60:9802",
                true,
                &write(SubstrateKind::Space)
            )
            .await?,
            None
        );
    }
    assert_eq!(all_nodes(pool).await?, before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivate_retired_node_skips_another_substrates_retired_node() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let kubernetes = NodeId::new();
    insert_node(pool, kubernetes, SubstrateKind::Kubernetes).await?;
    insert_registration(pool, kubernetes, "10.0.0.61:9802", true).await?;
    let before = all_nodes(pool).await?;

    // Neither by its id nor by its endpoint.
    for node_id in [kubernetes, NodeId::new()] {
        assert_eq!(
            reactivate_retired_node(
                pool,
                node_id,
                "10.0.0.61:9802",
                true,
                &write(SubstrateKind::Space)
            )
            .await?,
            None
        );
    }
    assert_eq!(all_nodes(pool).await?, before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivate_retired_node_resets_a_deleted_state() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let retired = NodeId::new();
    insert_node(pool, retired, SubstrateKind::Space).await?;
    insert_registration(pool, retired, "10.0.0.62:9802", true).await?;
    sqlx::query(sql!("UPDATE nodes SET state = 'deleted' WHERE id = $1"))
        .bind(retired)
        .execute(pool)
        .await?;

    let row = reactivate_retired_node(
        pool,
        retired,
        "10.0.0.62:9802",
        false,
        &RegistryNodeWrite {
            state: None,
            ..write(SubstrateKind::Space)
        },
    )
    .await?
    .expect("the retired node matches by id");
    assert_eq!(row.id, retired);
    assert_eq!(row.state.as_deref(), Some("registered"));
    assert_eq!(row.address.as_deref(), Some("10.0.0.61"));
    assert_eq!(row.role, NodeRole::Builder);
    assert_eq!(row.managed_by, NodeManagedBy::Space);
    assert!(row.last_heartbeat.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_node_of_an_existing_id_fails_and_keeps_the_row() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_node(pool, node_id, SubstrateKind::Space).await?;
    let before = node_row(pool, node_id).await?;

    let error = register_node(pool, node_id, &write(SubstrateKind::Space))
        .await
        .expect_err("an existing id fails the insert");
    assert_eq!(error.kind(), DbErrorKind::UniqueViolation);
    assert_eq!(error.constraint(), Some("nodes_pkey"));
    assert_eq!(node_row(pool, node_id).await?, before);

    // A new id registers; an empty address stores none, no state stores
    // `registered`, and no stamp leaves the heartbeat unset.
    let fresh = NodeId::new();
    let row = register_node(
        pool,
        fresh,
        &RegistryNodeWrite {
            address: Some(""),
            role: NodeRole::Worker,
            managed_by: SubstrateKind::Space,
            state: None,
            stamp_heartbeat: false,
        },
    )
    .await?;
    assert_eq!(row.id, fresh);
    assert_eq!(row.address, None);
    assert_eq!(row.state.as_deref(), Some("registered"));
    assert_eq!(row.last_heartbeat, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_registered_node_skips_a_node_the_substrate_does_not_manage() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let kubernetes = NodeId::new();
    insert_node(pool, kubernetes, SubstrateKind::Kubernetes).await?;
    insert_registration(pool, kubernetes, "10.0.0.60:9802", false).await?;
    let before = node_row(pool, kubernetes).await?;

    assert_eq!(
        update_registered_node(pool, kubernetes, SubstrateKind::Space, &update()).await?,
        None
    );
    assert_eq!(node_row(pool, kubernetes).await?, before);
    let missing = NodeId::new();
    assert_eq!(
        update_registered_node(pool, missing, SubstrateKind::Space, &update()).await?,
        None
    );
    assert_eq!(node_row(pool, missing).await?, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_registered_node_writes_whatever_the_registration() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let live = NodeId::new();
    insert_node(pool, live, SubstrateKind::Space).await?;
    insert_registration(pool, live, "10.0.0.60:9802", false).await?;
    let retired = NodeId::new();
    insert_node(pool, retired, SubstrateKind::Space).await?;
    insert_registration(pool, retired, "10.0.0.61:9802", true).await?;
    let unregistered = NodeId::new();
    insert_node(pool, unregistered, SubstrateKind::Space).await?;

    for node_id in [live, retired, unregistered] {
        let row = update_registered_node(pool, node_id, SubstrateKind::Space, &update())
            .await?
            .expect("a Direct node is updated whatever its registration");
        assert_eq!(row.id, node_id);
        assert_eq!(row.address.as_deref(), Some("10.0.0.62"));
        assert_eq!(row.role, NodeRole::Worker, "no role keeps it");
        assert_eq!(row.state.as_deref(), Some("cordoned"));
        assert_eq!(row.last_heartbeat, None, "no stamp keeps it");
    }
    Ok(())
}

fn ids<I: IntoIterator<Item = NodeId>>(ids: I) -> HashSet<NodeId> {
    ids.into_iter().collect()
}

#[tokio::test(flavor = "current_thread")]
async fn live_worker_and_builder_ids_keep_other_substrates_and_live_direct_registry_rows()
-> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let k8s_worker = node(pool, "worker", "k8s", Some("ready")).await?;
    let k8s_builder = node(pool, "builder", "k8s", Some("ready")).await?;
    node(pool, "cluster", "k8s", Some("ready")).await?;
    let direct_worker = direct_node(pool, "worker", "draining", "worker", false).await?;
    let direct_builder = direct_node(pool, "builder", "registered", "builder", false).await?;
    direct_node(pool, "worker", "registered", "deleted", true).await?;
    direct_node(pool, "cluster", "registered", "cluster", false).await?;
    // A Direct node with no registry row is no live Direct node.
    node(pool, "worker", "space", None).await?;

    let listed = live_worker_and_builder_ids(pool, SubstrateKind::Space).await?;
    assert_eq!(listed.len(), 4, "each node once: {listed:?}");
    assert_eq!(
        ids(listed),
        ids([k8s_worker, k8s_builder, direct_worker, direct_builder])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tombstones_are_deleted_nodes_with_a_deleted_registry_row() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    direct_node(pool, "worker", "deleted", "tombstone-1", true).await?;
    direct_node(pool, "builder", "deleted", "tombstone-2", true).await?;
    direct_node(pool, "worker", "registered", "deleted-row", true).await?;
    direct_node(pool, "worker", "deleted", "live-row", false).await?;
    direct_node(pool, "worker", "registered", "live", false).await?;
    node(pool, "worker", "k8s", Some("deleted")).await?;

    assert_eq!(count_tombstones(pool, SubstrateKind::Space).await?, 2);
    assert_eq!(count_tombstones(pool, SubstrateKind::Kubernetes).await?, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn registry_nodes_read_every_substrate_and_filter_deleted_registry_rows() -> TestDbResult<()>
{
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let k8s_worker = node(pool, "worker", "k8s", Some("ready")).await?;
    k8s_name(pool, k8s_worker, "kind-worker").await?;
    let direct_b = direct_node(pool, "worker", "registered", "b-worker", false).await?;
    let direct_a = direct_node(pool, "worker", "registered", "a-worker", false).await?;
    let deleted = direct_node(pool, "worker", "deleted", "c-worker", true).await?;
    let builder = direct_node(pool, "builder", "registered", "builder", false).await?;
    let bare = node(pool, "worker", "space", None).await?;

    let k8s_row = registry_node(pool, k8s_worker)
        .await?
        .expect("a Kubernetes node is read");
    assert_eq!(k8s_row.id, k8s_worker);
    assert_eq!(k8s_row.name, None);
    assert_eq!(k8s_row.address.as_deref(), Some("10.0.0.1"));
    assert_eq!(k8s_row.node_api_endpoint, None);
    assert_eq!(k8s_row.role, NodeRole::Worker);
    assert_eq!(k8s_row.managed_by, NodeManagedBy::Kubernetes);
    assert_eq!(k8s_row.state.as_deref(), Some("ready"));
    assert_eq!(k8s_row.metadata, None);
    assert_eq!(k8s_row.updated_at, None);
    assert_eq!(k8s_row.deleted_at, None);
    assert_eq!(k8s_row.node_name.as_deref(), Some("kind-worker"));

    let deleted_row = registry_node(pool, deleted)
        .await?
        .expect("a deleted Direct node is read by id");
    assert_eq!(deleted_row.name.as_deref(), Some("c-worker"));
    assert_eq!(
        deleted_row.node_api_endpoint.as_deref(),
        Some(endpoint("c-worker").as_str())
    );
    assert_eq!(deleted_row.managed_by, NodeManagedBy::Space);
    assert_eq!(
        deleted_row.metadata.map(|metadata| metadata.0),
        Some(serde_json::json!({"rack": "r1"}))
    );
    assert!(deleted_row.updated_at.is_some());
    assert!(deleted_row.deleted_at.is_some());
    assert_eq!(deleted_row.node_name, None);
    assert_eq!(registry_node(pool, NodeId::new()).await?, None);

    let listed = |rows: Vec<RegistryNodeRow>| rows.into_iter().map(|row| row.id).collect();
    // By role, then name (a Kubernetes node by its node name), then endpoint.
    let live: Vec<NodeId> = listed(registry_nodes(pool, None, false).await?);
    assert_eq!(live, vec![builder, direct_a, direct_b, k8s_worker, bare]);
    let all: Vec<NodeId> = listed(registry_nodes(pool, None, true).await?);
    assert_eq!(
        all,
        vec![builder, direct_a, direct_b, deleted, k8s_worker, bare]
    );
    let builders: Vec<NodeId> = listed(registry_nodes(pool, Some(NodeRole::Builder), true).await?);
    assert_eq!(builders, vec![builder]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn listed_nodes_read_the_substrates_registry_rows_as_text() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let k8s_worker = node(pool, "worker", "k8s", Some("ready")).await?;
    k8s_name(pool, k8s_worker, "kind-worker").await?;
    node(pool, "worker", "space", None).await?;
    let direct_b = direct_node(pool, "worker", "registered", "b-worker", false).await?;
    let direct_a = direct_node(pool, "worker", "registered", "a-worker", false).await?;
    let deleted = direct_node(pool, "worker", "deleted", "c-worker", true).await?;
    let builder = direct_node(pool, "builder", "registered", "builder", false).await?;

    let rows = listed_nodes(pool, None, false, SubstrateKind::Space).await?;
    let listed: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
    assert_eq!(
        listed,
        [builder, direct_a, direct_b]
            .map(|id| id.to_string())
            .to_vec()
    );
    let a = &rows[1];
    assert_eq!(a.name.as_deref(), Some("a-worker"));
    assert_eq!(a.address.as_deref(), Some("10.0.0.1"));
    assert_eq!(a.node_api_endpoint, endpoint("a-worker"));
    assert_eq!(a.role, "worker");
    assert_eq!(a.managed_by, "space");
    assert_eq!(a.state.as_deref(), Some("registered"));
    assert_eq!(a.last_heartbeat, None);
    assert_eq!(a.deleted_at, None);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&a.metadata_json)?,
        serde_json::json!({"rack": "r1"})
    );

    let all: Vec<String> = listed_nodes(pool, None, true, SubstrateKind::Space)
        .await?
        .into_iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(
        all,
        [builder, direct_a, direct_b, deleted]
            .map(|id| id.to_string())
            .to_vec()
    );
    let workers: Vec<String> =
        listed_nodes(pool, Some(NodeRole::Worker), false, SubstrateKind::Space)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect();
    assert_eq!(
        workers,
        [direct_a, direct_b].map(|id| id.to_string()).to_vec()
    );
    assert!(
        listed_nodes(pool, None, true, SubstrateKind::Kubernetes)
            .await?
            .is_empty()
    );
    Ok(())
}
