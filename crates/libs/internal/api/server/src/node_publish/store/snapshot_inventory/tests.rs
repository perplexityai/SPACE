use super::*;
use pplx_space_common_core::types::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db};
use pplx_space_sbx_mgr_client::proto::{SnapshotDetail, SnapshotLocation, SnapshotType};
use pplx_space_test_env::TestResult;

#[tokio::test]
async fn snapshot_arrays_preserve_nullable_neighbors_and_the_accepted_scope() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let rich_id = SandboxId::new();
    let sparse_id = SandboxId::new();
    let outside_id = SandboxId::new();
    let scope = [rich_id, sparse_id];
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id)
         SELECT UNNEST($1::UUID[]), $2"
    ))
    .bind([rich_id, sparse_id, outside_id].as_slice())
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let rich = SnapshotDetail {
        id: "inventory/description".to_owned(),
        r#type: SnapshotType::Full.into(),
        state: "available".to_owned(),
        timestamp: Some("reported-time".to_owned()),
        source_node_id: Some("reported-origin".to_owned()),
        label: Some("manual capture".to_owned()),
        retention_tier: Some("archive".to_owned()),
        vcpu: 2,
        memory_mib: 512,
        size_bytes: Some(4096),
        template_id: Some("captured-template".to_owned()),
        content_hash: Some("captured-content".to_owned()),
        locations: vec![SnapshotLocation {
            location_type: "future-location".to_owned(),
            volume_id: Some("synthetic-volume".to_owned()),
            state: "future-state".to_owned(),
            size_bytes: u64::MAX,
            ..Default::default()
        }],
        ..Default::default()
    };
    let sparse = SnapshotDetail {
        id: rich.id.clone(),
        r#type: SnapshotType::Disk.into(),
        state: "pending".to_owned(),
        ..Default::default()
    };
    let claims = [
        SnapshotClaim::try_from_proto(rich_id, &rich)?,
        SnapshotClaim::try_from_proto(sparse_id, &sparse)?,
        SnapshotClaim::try_from_proto(outside_id, &rich)?,
    ];
    let mut conn = db.pool.acquire().await?;
    replace(&mut conn, &scope, &claims).await?;
    let descriptions: Vec<(SandboxId, serde_json::Value)> = sqlx::query_as(sql!(
        "SELECT sandbox_id, jsonb_build_array(
             snapshot_id, type, state, timestamp, source_node_id, label,
             retention_tier, vcpu, memory_mib, size_bytes, template_id, content_hash,
             locations
         ) FROM sandbox_snapshot_inventory ORDER BY sandbox_id"
    ))
    .fetch_all(&mut *conn)
    .await?;
    assert_eq!(descriptions.len(), 2);
    for (sandbox_id, description) in descriptions {
        let expected = if sandbox_id == rich_id {
            serde_json::json!([
                "inventory/description", "full", "available", "reported-time",
                "reported-origin", "manual capture", "archive", 2, 512, 4096,
                "captured-template", "captured-content", [{
                    "location_type": "future-location", "node_id": null,
                    "node_address": null, "volume_id": "synthetic-volume",
                    "state": "future-state", "size_bytes": u64::MAX
                }]
            ])
        } else {
            assert_eq!(sandbox_id, sparse_id);
            serde_json::json!([
                "inventory/description",
                "disk",
                "pending",
                null,
                null,
                null,
                null,
                0,
                0,
                0,
                null,
                null,
                []
            ])
        };
        assert_eq!(description, expected);
    }

    // Missing scope must not prune anything. Missing claims prune only scope.
    replace(&mut conn, &[], &claims).await?;
    let count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandbox_snapshot_inventory"))
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 2);
    replace(&mut conn, &scope, &claims[..1]).await?;
    let kept: Vec<SandboxId> = sqlx::query_scalar(sql!(
        "SELECT sandbox_id FROM sandbox_snapshot_inventory ORDER BY sandbox_id"
    ))
    .fetch_all(&mut *conn)
    .await?;
    assert_eq!(kept, [rich_id]);
    replace(&mut conn, &[outside_id], &claims[2..]).await?;
    replace(&mut conn, &scope, &[]).await?;
    let remaining: Vec<SandboxId> = sqlx::query_scalar(sql!(
        "SELECT sandbox_id FROM sandbox_snapshot_inventory ORDER BY sandbox_id"
    ))
    .fetch_all(&mut *conn)
    .await?;
    assert_eq!(remaining, [outside_id]);
    Ok(())
}
