use pplx_space_common_core::types::{LocalArtifactId, OrgId, SandboxId, SnapshotId, UserId};

use super::*;
use crate::fork_operations::{ForkRequest, ForkTargetConfig};
use pplx_space_db_test_fixtures::try_pg_test_db;

#[test]
fn leader_selection_prefers_an_existing_leader() {
    let earlier_unclaimed = OperationId::new();
    let existing_leader = OperationId::new();
    assert_eq!(
        choose_capture_leader(
            [
                (earlier_unclaimed, None),
                (existing_leader, Some(existing_leader)),
            ],
            OperationId::new(),
        ),
        existing_leader
    );
}

#[test]
fn leader_selection_uses_the_earliest_unclaimed_operation() {
    let first = OperationId::new();
    let second = OperationId::new();
    assert_eq!(
        choose_capture_leader([(first, None), (second, None)], OperationId::new()),
        first
    );
}

#[test]
fn published_artifact_leader_is_not_a_capture_candidate() {
    let mut candidate = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
        "e2b.example.test".to_string(),
    );
    candidate.freeze_source(
        7,
        ForkSourceLifecycle::Started,
        ForkTargetConfig {
            volume_encryption: false,
            egress_profiles: Default::default(),
            template: "seed:latest".parse().unwrap(),
            source_template_id: Some("seed".to_string()),
            runtime: None,
            env: Default::default(),
            metadata: Default::default(),
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
            auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            network_policy: None,
        },
        None,
    );
    candidate.resolve_fork_snapshot(SnapshotId::new(), Uuid::now_v7());

    assert!(!is_capture_candidate(
        &candidate,
        7,
        ForkSourceLifecycle::Started
    ));
}

#[test]
fn capture_candidate_must_match_the_frozen_source_lifecycle() {
    let mut candidate = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
        "e2b.example.test".to_string(),
    );
    candidate.freeze_source(
        7,
        ForkSourceLifecycle::Paused,
        ForkTargetConfig {
            volume_encryption: false,
            egress_profiles: Default::default(),
            template: "seed:latest".parse().unwrap(),
            source_template_id: Some("seed".to_string()),
            runtime: None,
            env: Default::default(),
            metadata: Default::default(),
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
            auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            network_policy: None,
        },
        None,
    );

    assert!(!is_capture_candidate(
        &candidate,
        7,
        ForkSourceLifecycle::Started
    ));
    assert!(is_capture_candidate(
        &candidate,
        7,
        ForkSourceLifecycle::Paused
    ));
}

#[test]
fn capture_leader_must_still_match_the_followers_frozen_source() {
    let mut leader = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
        "e2b.example.test".to_string(),
    );
    leader.freeze_source(
        8,
        ForkSourceLifecycle::Started,
        ForkTargetConfig {
            volume_encryption: false,
            egress_profiles: Default::default(),
            template: "seed:latest".parse().unwrap(),
            source_template_id: Some("seed".to_string()),
            runtime: None,
            env: Default::default(),
            metadata: Default::default(),
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
            auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            network_policy: None,
        },
        None,
    );

    assert!(!capture_leader_matches_source(
        &leader,
        7,
        ForkSourceLifecycle::Started
    ));
    assert!(capture_leader_matches_source(
        &leader,
        8,
        ForkSourceLifecycle::Started
    ));
}

#[test]
fn capture_followers_only_wait_for_non_terminal_leaders() {
    assert!(is_capture_pending_state(OperationState::Queued));
    assert!(is_capture_pending_state(OperationState::Running));
    for state in [
        OperationState::Succeeded,
        OperationState::Failed,
        OperationState::Canceled,
        OperationState::Expired,
    ] {
        assert!(!is_capture_pending_state(state));
    }
}

/// A leader state this build does not know is an error, not a terminal leader.
///
/// `OperationState` is open, so a plain `parse()` turns an unknown persisted
/// value into `Other(_)`; classified as "not pending" that would read as
/// `TerminalWithoutArtifact`, and a follower would clear a live newer leader's
/// claim and start a duplicate capture.
#[test]
fn an_unknown_leader_state_is_invalid_rather_than_terminal() {
    assert!(matches!(
        capture_leader_state_observation("running".to_string()),
        Ok(CaptureLeaderObservation::Pending)
    ));
    assert!(matches!(
        capture_leader_state_observation("failed".to_string()),
        Ok(CaptureLeaderObservation::TerminalWithoutArtifact)
    ));
    assert!(matches!(
        capture_leader_state_observation("hibernating".to_string()),
        Err(ForkOperationStoreError::InvalidLeaderState(state)) if state == "hibernating"
    ));
}

#[tokio::test]
async fn retiring_a_published_copy_removes_the_snapshot_and_orphans_its_location() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let source_sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, $2, $3)"
    ))
    .bind(org_id)
    .bind(format!("Fork copy retirement {org_id}"))
    .bind(format!(
        "fork-copy-retirement-{}",
        org_id.as_uuid().simple()
    ))
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Fork copy retirement', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("fork-copy-retirement-{user_id}@example.com"))
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, network_policy_generation
         ) VALUES ($1, $2, $3, 'suspended', 'unplaced', 1)"
    ))
    .bind(source_sandbox_id.as_uuid())
    .bind(user_id)
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap();

    let store = ForkOperationStore::new(db.pool.clone());
    let snapshot_id = SnapshotId::new();
    let storage_location_id = Uuid::now_v7();
    store
        .prepare_copied_artifact(PrepareCopiedArtifact {
            snapshot_id,
            storage_location_id,
            owner_sandbox_id: source_sandbox_id,
            org_id,
            destination_storage_uri: "s3://fork-copy-retirement/artifact",
        })
        .await
        .unwrap();
    assert!(
        store
            .mark_copied_artifact_ready(snapshot_id, source_sandbox_id, 128)
            .await
            .unwrap()
    );
    let target_sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, network_policy_generation
         ) VALUES ($1, $2, $3, 'started', 'on_node', 1)"
    ))
    .bind(target_sandbox_id.as_uuid())
    .bind(user_id)
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(
        store
            .rebind_copied_artifact_owner(snapshot_id, source_sandbox_id, target_sandbox_id,)
            .await
            .unwrap()
    );
    let rebound_owner: Uuid =
        sqlx::query_scalar(sql!("SELECT sandbox_id FROM snapshots WHERE id = $1"))
            .bind(snapshot_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(rebound_owner, target_sandbox_id.as_uuid());

    store
        .retire_copied_artifact(snapshot_id, storage_location_id)
        .await
        .unwrap();

    let snapshot_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM snapshots WHERE id = $1)"
    ))
    .bind(snapshot_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    let location_orphaned: bool = sqlx::query_scalar(sql!(
        "SELECT orphaned_at IS NOT NULL
         FROM snapshot_storage_locations WHERE id = $1"
    ))
    .bind(storage_location_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(!snapshot_exists);
    assert!(location_orphaned);

    store
        .prepare_copied_artifact(PrepareCopiedArtifact {
            snapshot_id,
            storage_location_id,
            owner_sandbox_id: source_sandbox_id,
            org_id,
            destination_storage_uri: "s3://fork-copy-retirement/artifact",
        })
        .await
        .unwrap();
    let location_reattached: bool = sqlx::query_scalar(sql!(
        "SELECT snapshot_id = $2 AND orphaned_at IS NULL
         FROM snapshot_storage_locations WHERE id = $1"
    ))
    .bind(storage_location_id)
    .bind(snapshot_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(location_reattached);
}

#[tokio::test]
async fn live_capture_resolves_its_published_snapshot_artifact() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let sandbox_id = SandboxId::new();
    let snapshot_id = SnapshotId::new();
    let local_artifact_id = LocalArtifactId::new();
    let storage_location_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)"
    ))
    .bind(org_id)
    .bind(format!("Fork artifact selection {org_id}"))
    .bind(format!(
        "fork-artifact-selection-{}",
        org_id.as_uuid().simple()
    ))
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Fork artifact selection', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("fork-artifact-selection-{user_id}@example.com"))
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, network_policy_generation
         ) VALUES ($1, $2, $3, 'suspended', 'unplaced', 1)"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, artifact_kind, artifact_origin,
             local_artifact_id, artifact_snapshot_name, org_id
         ) VALUES ($1, $2, 'disk', 'ready', 'disk', 'snapshot', $3, $4, $5)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(local_artifact_id)
    .bind("fork-selection-regression")
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string
         ) VALUES ($1, $2, 's3://fork-artifact-selection/artifact')"
    ))
    .bind(storage_location_id)
    .bind(snapshot_id)
    .execute(&db.pool)
    .await
    .unwrap();

    let artifact = ForkOperationStore::new(db.pool.clone())
        .ready_artifact_for_local_id(sandbox_id, local_artifact_id)
        .await
        .unwrap()
        .expect("the exact operation snapshot must resolve after upload");
    assert_eq!(artifact.snapshot_id, snapshot_id);
    assert_eq!(artifact.storage_location_id, storage_location_id);

    let fenced = ForkOperationStore::new(db.pool.clone())
        .ready_artifact(snapshot_id, storage_location_id)
        .await
        .unwrap()
        .expect("the persisted snapshot and location must remain resolvable");
    assert_eq!(fenced, artifact);
}
