use super::*;

#[tokio::test]
async fn node_store_requires_current_name_and_non_null_uid()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_common_svc::sql;
    use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db};
    let db = pg_test_db().await?;
    let id = NodeId::new();
    insert_ready_worker_node(&db.pool, id).await?;
    let store = PgCredentialStore::new(db.pool.clone());
    let name = "worker".parse().unwrap();
    let uid = "node-uid".parse().unwrap();
    assert_eq!(store.registered_node(&name, &uid).await?, None);
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'worker')"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;
    assert_eq!(store.registered_node(&name, &uid).await?, None);
    for (updated_name, updated_uid, expected) in [
        ("worker", "node-uid", Some(id)),
        ("worker", "replacement-uid", None),
        ("another-worker", "node-uid", None),
    ] {
        sqlx::query(sql!(
            "UPDATE node_k8s SET k8s_node_name = $1, k8s_node_uid = $2 WHERE node_id = $3"
        ))
        .bind(updated_name)
        .bind(updated_uid)
        .bind(id)
        .execute(&db.pool)
        .await?;
        assert_eq!(store.registered_node(&name, &uid).await?, expected);
    }
    Ok(())
}
