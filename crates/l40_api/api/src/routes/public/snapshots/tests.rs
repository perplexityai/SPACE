use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use uuid::Uuid;

use super::*;

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
