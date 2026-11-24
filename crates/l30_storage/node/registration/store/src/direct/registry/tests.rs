//! The admin Direct node registry's `nodes` statements against Postgres, each
//! one's losing case included.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use pplx_space_postgres_pool::DbErrorKind;
use sqlx::PgPool;

use super::*;

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
