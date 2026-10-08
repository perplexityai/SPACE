use pplx_space_common_core::types::{NodeId, SnapshotId};

use pplx_space_common_svc::core::types::{OrgId, SandboxId, TemplateName};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, pg_test_db,
};
use uuid::Uuid;

use super::*;

#[tokio::test]
async fn reads_sandbox_template_and_per_org_vend_authority() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let snapshot_id = SnapshotId::new();
    let template = TemplateName::parse("storage-vend-template")?;
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'worker-a', 'worker-a-uid')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE organizations
         SET snapshot_base_object_storage_uri =
             's3://tenant-snapshots/acme?region=us-east-1'
         WHERE id = $1"
    ))
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, active_node_id, network_policy_generation)
         VALUES ($1, $2, $3, $4, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, $2, $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(template.as_str())
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let role = "arn:aws:iam::123456789012:role/acme-snapshots";
    sqlx::query(sql!(
        "INSERT INTO org_storage_vend_identities (org_id, provider, identity)
         VALUES ($1, 'aws-sts', $2)"
    ))
    .bind(SEED_ORG_ID)
    .bind(role)
    .execute(&db.pool)
    .await?;

    let store = PgStorageVendStore::new(db.pool.clone());
    let sandbox = store
        .sandbox_context(sandbox_id, snapshot_id)
        .await?
        .expect("sandbox context");
    assert_eq!(sandbox.organization_id, OrgId::from_uuid(SEED_ORG_ID));
    assert_eq!(sandbox.nodes[0].uid.as_str(), "worker-a-uid");
    assert_eq!(sandbox.snapshot_storage[0].bucket, "tenant-snapshots");
    assert_eq!(
        store.template_owner(&template).await?,
        Some(Some(OrgId::from_uuid(SEED_ORG_ID)))
    );
    assert_eq!(
        store
            .org_vend_identity(OrgId::from_uuid(SEED_ORG_ID))
            .await?
            .expect("org vend identity"),
        StorageVendIdentity {
            provider: StorageVendProvider::AwsSts,
            identity: role.to_string(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn template_release_is_published_once_a_storage_location_is_registered() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("storage-vend-release")?;
    let hash: ContentHash = format!("sha256:{}", "ab".repeat(32)).parse()?;
    let unregistered: ContentHash = format!("sha256:{}", "cd".repeat(32)).parse()?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, $2, $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(template.as_str())
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, org_id)
         VALUES ($1, $2, 'fc', $3, $4)"
    ))
    .bind(release_id)
    .bind(template.as_str())
    .bind(hash.to_string())
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let store = PgStorageVendStore::new(db.pool.clone());

    // A release row without artifacts has not been published yet.
    assert!(!store.template_release_published(&template, &hash).await?);

    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         VALUES ($1, 's3://artifacts/templates/storage-vend-release/x?region=us-east-1', $2)"
    ))
    .bind(release_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    assert!(store.template_release_published(&template, &hash).await?);
    assert!(
        !store
            .template_release_published(&template, &unregistered)
            .await?
    );

    // A release registered before the `sha256:` prefix was required is stored
    // bare, and is just as published.
    let bare_hex = "ef".repeat(32);
    let bare_release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, org_id)
         VALUES ($1, $2, 'fc', $3, $4)"
    ))
    .bind(bare_release_id)
    .bind(template.as_str())
    .bind(&bare_hex)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         VALUES ($1, 's3://artifacts/templates/storage-vend-release/y?region=us-east-1', $2)"
    ))
    .bind(bare_release_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let legacy: ContentHash = format!("sha256:{bare_hex}").parse()?;
    assert!(store.template_release_published(&template, &legacy).await?);
    Ok(())
}

#[tokio::test]
async fn reads_unplaced_node_history_and_the_recorded_snapshot_uri() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let execution_node_id = NodeId::new();
    let snapshot_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let snapshot_id = SnapshotId::new();
    insert_ready_worker_node(&db.pool, execution_node_id).await?;
    insert_ready_worker_node(&db.pool, snapshot_node_id).await?;
    for (node_id, name, uid) in [
        (execution_node_id, "worker-execution", "execution-uid"),
        (snapshot_node_id, "worker-snapshot", "snapshot-uid"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, $3)"
        ))
        .bind(node_id)
        .bind(name)
        .bind(uid)
        .execute(&db.pool)
        .await?;
    }
    sqlx::query(sql!(
        "UPDATE organizations
         SET snapshot_base_object_storage_uri = 'not a storage uri'
         WHERE id = $1"
    ))
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active
         ) VALUES ($1, 7, 'retired-sandbox', 'space', 'worker-execution', 'terminated', false)"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, org_id)
         VALUES ($1, $2, 'disk', 'ready', $3, $4)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id.as_uuid())
    .bind(snapshot_node_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, org_id
         ) VALUES ($1, $2, 's3://historical-snapshots/old-prefix?region=us-east-1', $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;

    let context = PgStorageVendStore::new(db.pool.clone())
        .sandbox_context(sandbox_id, snapshot_id)
        .await?
        .expect("sandbox context");
    let node_uids: Vec<_> = context.nodes.iter().map(|node| node.uid.as_str()).collect();
    assert!(node_uids.contains(&"execution-uid"));
    assert!(node_uids.contains(&"snapshot-uid"));
    assert_eq!(context.snapshot_storage.len(), 1);
    assert_eq!(context.snapshot_storage[0].bucket, "historical-snapshots");
    assert!(context.snapshot_storage_configuration_invalid);
    Ok(())
}
