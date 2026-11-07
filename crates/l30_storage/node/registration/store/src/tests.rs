//! Recording a node's EC2 instance through registration, against Postgres:
//! which reports write it, which leave it alone, and that an instance alone
//! neither dates the row's Kubernetes metadata nor clears drain state.

use pplx_space_common_core::types::{NodeId, NodeRole};
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db};
use pplx_space_sql_locator::sql;
use sqlx::{PgConnection, PgPool};

use super::*;

const NAME: &str = "space-test-instance-node";

fn registration<'a>(
    node_id: NodeId,
    uid: &'a str,
    instance_id: Option<&'a str>,
) -> K8sNodeRegistration<'a> {
    K8sNodeRegistration {
        node_id,
        name: NAME,
        uid,
        role: NodeRole::Worker,
        address: Some("10.0.0.10"),
        state: "ready",
        manage_state: true,
        instance_id,
    }
}

/// Register in a transaction of its own, committed only on success.
async fn register(
    pool: &PgPool,
    registration: K8sNodeRegistration<'_>,
) -> TestDbResult<Result<bool, K8sNodeRegistrationError>> {
    let mut tx = pool.begin().await?;
    let result = register_k8s_node(&mut tx, registration, || {}).await;
    if result.is_ok() {
        tx.commit().await?;
    }
    Ok(result)
}

async fn registered(pool: &PgPool, registration: K8sNodeRegistration<'_>) -> TestDbResult<bool> {
    Ok(register(pool, registration).await??)
}

#[derive(Debug, PartialEq, Eq)]
struct NodeK8s {
    k8s_node_uid: Option<String>,
    instance_id: Option<String>,
    updated_at: String,
    /// The row version: unchanged means no statement wrote the row.
    version: String,
}

async fn node_k8s(executor: &mut PgConnection, node_id: NodeId) -> TestDbResult<Option<NodeK8s>> {
    let row: Option<(Option<String>, Option<String>, String)> = sqlx::query_as(sql!(
        "SELECT k8s_node_uid, instance_id, updated_at::TEXT FROM node_k8s WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(&mut *executor)
    .await?;
    let Some((k8s_node_uid, instance_id, updated_at)) = row else {
        return Ok(None);
    };
    // A schema variant can make `node_k8s` a view, which has no `xmin`.
    let version = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT xmin::TEXT FROM {} WHERE node_id = $1",
        pplx_space_db_test_fixtures::physical_table("node_k8s")?
    ))))
    .bind(node_id)
    .fetch_one(&mut *executor)
    .await?;
    Ok(Some(NodeK8s {
        k8s_node_uid,
        instance_id,
        updated_at,
        version,
    }))
}

async fn db() -> TestDbResult<TestDb> {
    pg_test_db().await
}

/// A node's first instance is recorded on a new row and on a row registered
/// before instances were recorded.
#[tokio::test]
async fn a_first_instance_is_recorded() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;

    let new_node = NodeId::new();
    assert!(registered(&db.pool, registration(new_node, "uid-1", Some("i-0a"))).await?);
    assert_eq!(
        node_k8s(&mut conn, new_node)
            .await?
            .unwrap()
            .instance_id
            .as_deref(),
        Some("i-0a")
    );

    let older_row = NodeId::new();
    assert!(registered(&db.pool, registration(older_row, "uid-1", None)).await?);
    let before = node_k8s(&mut conn, older_row).await?.unwrap();
    assert_eq!(before.instance_id, None);
    assert!(registered(&db.pool, registration(older_row, "uid-1", Some("i-0a"))).await?);
    let after = node_k8s(&mut conn, older_row).await?.unwrap();
    assert_eq!(after.instance_id.as_deref(), Some("i-0a"));
    // An instance report doesn't date the row's Kubernetes metadata, which
    // absence demotion reads.
    assert_eq!(after.updated_at, before.updated_at);
    Ok(())
}

/// The recorded instance again, or no instance (a caller that has none),
/// writes nothing, and no instance keeps the recorded one through a UID
/// change.
#[tokio::test]
async fn the_same_instance_or_none_writes_nothing() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
    let before = node_k8s(&mut conn, node_id).await?.unwrap();

    for instance_id in [Some("i-0a"), None] {
        assert!(
            !registered(&db.pool, registration(node_id, "uid-1", instance_id)).await?,
            "{instance_id:?}"
        );
        assert_eq!(
            node_k8s(&mut conn, node_id).await?.unwrap(),
            before,
            "{instance_id:?}"
        );
    }

    registered(&db.pool, registration(node_id, "uid-2", None)).await?;
    let after = node_k8s(&mut conn, node_id).await?.unwrap();
    assert_eq!(after.k8s_node_uid.as_deref(), Some("uid-2"));
    assert_eq!(after.instance_id.as_deref(), Some("i-0a"));
    Ok(())
}

/// Another instance replaces the recorded one, with or without a UID change,
/// and only a UID change dates the row.
#[tokio::test]
async fn another_instance_replaces_the_recorded_one() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    for uid in ["uid-1", "uid-2"] {
        let node_id = NodeId::new();
        registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
        let before = node_k8s(&mut conn, node_id).await?.unwrap();
        // `NOW()` is the transaction's start, so a later one differs.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        assert!(registered(&db.pool, registration(node_id, uid, Some("i-0b"))).await?);

        let after = node_k8s(&mut conn, node_id).await?.unwrap();
        assert_eq!(
            (after.k8s_node_uid.as_deref(), after.instance_id.as_deref()),
            (Some(uid), Some("i-0b")),
            "{uid}"
        );
        assert_eq!(
            after.updated_at == before.updated_at,
            uid == "uid-1",
            "{uid}"
        );
    }
    Ok(())
}

/// A tombstoned node refuses before anything is written, an instance
/// included.
#[tokio::test]
async fn a_tombstoned_node_refuses_without_recording_an_instance() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
    let before = node_k8s(&mut conn, node_id).await?.unwrap();
    sqlx::query(sql!(
        "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role, reason)
         VALUES ($1, $2, 'worker', 'admin-terminate')"
    ))
    .bind(node_id)
    .bind(NAME)
    .execute(&mut *conn)
    .await?;

    let result = register(&db.pool, registration(node_id, "uid-2", Some("i-0b"))).await?;
    assert!(
        matches!(result, Err(K8sNodeRegistrationError::Tombstoned)),
        "{result:?}"
    );
    assert_eq!(node_k8s(&mut conn, node_id).await?.unwrap(), before);
    Ok(())
}

/// A UID change clears drain state and an instance change alone does not.
#[tokio::test]
async fn an_instance_change_alone_keeps_drain_state() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    for (uid, instance_id) in [
        ("uid-1", "i-0a"),
        ("uid-2", "i-0a"),
        ("uid-1", "i-0b"),
        ("uid-2", "i-0b"),
    ] {
        let node_id = NodeId::new();
        registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
        sqlx::query(sql!(
            "UPDATE nodes
             SET drain_phase = 'draining', drain_mode = 'gentle',
                 drain_started_at = NOW(), drain_last_error = 'seeded'
             WHERE id = $1"
        ))
        .bind(node_id)
        .execute(&mut *conn)
        .await?;

        let mut cleared = false;
        let mut tx = db.pool.begin().await?;
        register_k8s_node(
            &mut tx,
            registration(node_id, uid, Some(instance_id)),
            || cleared = true,
        )
        .await?;
        tx.commit().await?;

        let drain: (Option<String>, Option<String>) = sqlx::query_as(sql!(
            "SELECT drain_phase, drain_mode FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(&mut *conn)
        .await?;
        let uid_changed = uid != "uid-1";
        assert_eq!(cleared, uid_changed, "{uid} {instance_id}");
        assert_eq!(
            drain,
            if uid_changed {
                (None, None)
            } else {
                (Some("draining".to_string()), Some("gentle".to_string()))
            },
            "{uid} {instance_id}"
        );
        assert_eq!(
            node_k8s(&mut conn, node_id)
                .await?
                .unwrap()
                .instance_id
                .as_deref(),
            Some(instance_id),
            "{uid} {instance_id}"
        );
    }
    Ok(())
}
