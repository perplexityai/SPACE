use pplx_space_common_core::types::OrgId;
use pplx_space_common_core::types::{SandboxId, SnapshotId};
use uuid::Uuid;

use super::*;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db};

use pplx_space_test_env::TestResult;

fn snapshot_row(id: SnapshotId, created_at: OffsetDateTime) -> SnapshotRow {
    SnapshotRow {
        id,
        sandbox_id: SandboxId::new(),
        snapshot_type: "full".to_string(),
        state: "ready".to_string(),
        created_at,
        label: None,
        retention_tier: None,
        size_bytes: None,
        vcpu: None,
        memory_mib: None,
        template_id: None,
        content_hash: None,
        failure_reason: None,
    }
}

#[test]
fn list_snapshots_query_defaults_and_caps_limit() {
    assert_eq!(
        ListSnapshotsQuery {
            cursor: None,
            limit: None,
        }
        .limit(),
        DEFAULT_SNAPSHOT_LIST_LIMIT
    );
    assert_eq!(
        ListSnapshotsQuery {
            cursor: None,
            limit: Some(0),
        }
        .limit(),
        DEFAULT_SNAPSHOT_LIST_LIMIT
    );
    assert_eq!(
        ListSnapshotsQuery {
            cursor: None,
            limit: Some(MAX_SNAPSHOT_LIST_LIMIT + 1),
        }
        .limit(),
        MAX_SNAPSHOT_LIST_LIMIT
    );
}

#[test]
fn snapshot_page_cursor_requires_overfetch_row() {
    let first = Uuid::now_v7();
    let second = SnapshotId::new();
    let third = Uuid::now_v7();
    let timestamp = OffsetDateTime::from_unix_timestamp(1_765_800_000).expect("valid timestamp");

    let (rows, next_cursor) = snapshot_page_rows(
        vec![
            snapshot_row(SnapshotId::from_uuid(first), timestamp),
            snapshot_row(second, timestamp - time::Duration::seconds(1)),
        ],
        2,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(next_cursor.is_none());

    let (rows, next_cursor) = snapshot_page_rows(
        vec![
            snapshot_row(SnapshotId::from_uuid(first), timestamp),
            snapshot_row(second, timestamp - time::Duration::seconds(1)),
            snapshot_row(
                SnapshotId::from_uuid(third),
                timestamp - time::Duration::seconds(2),
            ),
        ],
        2,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        next_cursor
            .as_ref()
            .and_then(|cursor| decode_cursor(CursorKind::Snapshot, cursor.as_str()).ok())
            .and_then(|payload| parse_snapshot_cursor(&payload).ok()),
        Some((timestamp - time::Duration::seconds(1), second))
    );
}

#[test]
fn snapshot_cursor_round_trips_timestamp_and_id() {
    let timestamp = OffsetDateTime::from_unix_timestamp(1_765_800_000).expect("valid timestamp");
    let snapshot_id = SnapshotId::new();
    let cursor = snapshot_cursor(timestamp, snapshot_id).expect("cursor");
    let payload = decode_cursor(CursorKind::Snapshot, cursor.as_str()).unwrap();

    assert_eq!(
        parse_snapshot_cursor(&payload).expect("parsed cursor"),
        (timestamp, snapshot_id)
    );
}

#[test]
fn public_snapshot_maps_closed_state_and_type() {
    let snapshot_id = SnapshotId::new();
    let sandbox_id = SandboxId::new();
    let created_at = OffsetDateTime::from_unix_timestamp(1_765_800_000).expect("valid timestamp");

    let snapshot = public_snapshot_from_row(SnapshotRow {
        id: snapshot_id,
        sandbox_id,
        snapshot_type: "full".to_string(),
        state: "ready".to_string(),
        created_at,
        label: Some("checkpoint".to_string()),
        retention_tier: None,
        size_bytes: Some(-1),
        vcpu: Some(2),
        memory_mib: Some(1024),
        template_id: Some("seed".to_string()),
        content_hash: Some("sha256:abc".to_string()),
        failure_reason: None,
    })
    .expect("snapshot");

    assert_eq!(snapshot.id, PublicSnapshotId::from(snapshot_id));
    assert_eq!(snapshot.sandbox_id, PublicSandboxId::from(sandbox_id));
    assert_eq!(snapshot.snapshot_type, PublicSnapshotType::Full);
    assert_eq!(snapshot.state, PublicSnapshotState::Ready);
    assert_eq!(snapshot.size_bytes, None);
    assert_eq!(snapshot.resources.vcpu, Some(2));
}

#[tokio::test]
async fn public_snapshot_queries_hide_fork_origin_rows() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = SEED_ORG_ID;
    let sandbox_id = SandboxId::new();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_override_json, network_policy_generation
         ) VALUES (
             $1, '00000000-0000-0000-0000-000000000010', $2,
             'suspended', 'unplaced', 'k8s', $3, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&db.pool)
    .await?;
    let public_snapshot_id = SnapshotId::new();
    let fork_snapshot_id = SnapshotId::new();
    for (id, origin) in [(public_snapshot_id, "snapshot"), (fork_snapshot_id, "fork")] {
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, local_artifact_id,
                 artifact_kind, artifact_origin,
                 artifact_snapshot_name
             ) VALUES ($1, $2, 'disk', 'ready', $3, 'disk', $4, $5)"
        ))
        .bind(id)
        .bind(sandbox_id)
        .bind((origin == "snapshot").then(Uuid::now_v7))
        .bind(origin)
        .bind((origin == "snapshot").then_some("checkpoint"))
        .execute(&db.pool)
        .await?;
    }
    let public_sandbox_id = PublicSandboxId::from(sandbox_id);

    let listed = fetch_snapshot_rows(
        &db.pool,
        OrgId::from_uuid(org_id),
        public_sandbox_id,
        None,
        100,
    )
    .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, public_snapshot_id);
    assert!(
        fetch_snapshot_row(
            &db.pool,
            OrgId::from_uuid(org_id),
            public_sandbox_id,
            fork_snapshot_id
        )
        .await?
        .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn public_snapshot_queries_hide_deleting_rows() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = SEED_ORG_ID;
    let sandbox_id = SandboxId::new();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_override_json, network_policy_generation
         ) VALUES (
             $1, '00000000-0000-0000-0000-000000000010', $2,
             'suspended', 'unplaced', 'k8s', $3, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&db.pool)
    .await?;
    let ready_snapshot_id = SnapshotId::new();
    let deleting_snapshot_id = SnapshotId::new();
    for (id, state) in [
        (ready_snapshot_id, "ready"),
        (deleting_snapshot_id, "deleting"),
    ] {
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, local_artifact_id,
                 artifact_kind, artifact_origin, artifact_snapshot_name
             ) VALUES ($1, $2, 'disk', $3, $4, 'disk', 'snapshot', 'checkpoint')"
        ))
        .bind(id)
        .bind(sandbox_id)
        .bind(state)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
    }
    let public_sandbox_id = PublicSandboxId::from(sandbox_id);

    // A claimed row is de-advertised: absent from the list and a 404 (None)
    // on point lookup, never a parse failure on the closed wire enum.
    let listed = fetch_snapshot_rows(
        &db.pool,
        OrgId::from_uuid(org_id),
        public_sandbox_id,
        None,
        100,
    )
    .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, ready_snapshot_id);
    assert!(
        fetch_snapshot_row(
            &db.pool,
            OrgId::from_uuid(org_id),
            public_sandbox_id,
            deleting_snapshot_id
        )
        .await?
        .is_none()
    );
    Ok(())
}
