use pplx_space_accounts::{OrgId, UserId};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};

use super::*;
use crate::fork_operations::{ForkRequest, ForkTargetConfig};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sql_locator::sql;
use sqlx::types::Json;

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
            auto_lifecycle: pplx_space_auto_lifecycle::AutoLifecyclePolicy::disabled(),
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
            auto_lifecycle: pplx_space_auto_lifecycle::AutoLifecyclePolicy::disabled(),
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
            auto_lifecycle: pplx_space_auto_lifecycle::AutoLifecyclePolicy::disabled(),
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

/// A suspended source sandbox in its own org, pinned to `pinned` when given,
/// last placed `placement_minutes_ago` when given.
async fn insert_suspended_source(
    pool: &PgPool,
    pinned: Option<LocalArtifactId>,
    placement_minutes_ago: Option<i32>,
) -> (OrgId, SandboxId) {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)"
    ))
    .bind(org_id)
    .bind(format!("Fork source {org_id}"))
    .bind(format!("fork-source-{}", org_id.as_uuid().simple()))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Fork source', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("fork-source-{user_id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, network_policy_generation,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin, placement_changed_at
         ) VALUES ($1, $2, $3, 'suspended', 'unplaced', 1, $4,
                   CASE WHEN $4::UUID IS NOT NULL THEN 'full' END,
                   CASE WHEN $4::UUID IS NOT NULL THEN 'suspend' END,
                   NOW() - make_interval(mins => $5::INT))"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .bind(pinned)
    .bind(placement_minutes_ago)
    .execute(pool)
    .await
    .unwrap();
    (org_id, sandbox_id)
}

/// A full suspend backup of `artifact` with a storage location, registered
/// `minutes_ago`.
async fn insert_suspend_backup(
    pool: &PgPool,
    (org_id, sandbox_id): (OrgId, SandboxId),
    artifact: LocalArtifactId,
    state: &str,
    generation: Option<i64>,
    minutes_ago: i32,
) -> SnapshotId {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, artifact_kind, artifact_origin,
             local_artifact_id, generation, org_id
         ) VALUES ($1, $2, 'full', $3, NOW() - make_interval(mins => $4::INT), 'full',
                   'suspend', $5, $6, $7)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(minutes_ago)
    .bind(artifact)
    .bind(generation)
    .bind(org_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (id, snapshot_id, storage_connection_string)
         VALUES ($1, $2, 's3://fork-source/artifact')"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot_id)
    .execute(pool)
    .await
    .unwrap();
    snapshot_id
}

/// A pinned source forks from its pinned capture even when a failed capture
/// of a later execution outranks it, as its restore loads the pin: an
/// operator pins such a capture through the admin route. The newest ready
/// backup of the pin is the one forked, here a re-upload registered after
/// the failed capture.
#[tokio::test]
async fn a_pinned_source_forks_from_its_pin_whatever_outranks_it() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let pinned = LocalArtifactId::new();
    let source = insert_suspended_source(&db.pool, Some(pinned), None).await;
    insert_suspend_backup(&db.pool, source, pinned, "ready", Some(5), 30).await;
    insert_suspend_backup(
        &db.pool,
        source,
        LocalArtifactId::new(),
        "failed",
        Some(6),
        20,
    )
    .await;
    let reupload = insert_suspend_backup(&db.pool, source, pinned, "ready", Some(5), 10).await;

    let (snapshot_id, _) = ForkOperationStore::new(db.pool.clone())
        .suspended_source_artifact(source.1)
        .await
        .unwrap()
        .expect("a pinned source forks from its pin");
    assert_eq!(snapshot_id, reupload);
}

/// An unpinned source forks only from a capture first registered since its
/// last placement, as its restore requires. A re-upload registered after the
/// placement of a capture taken before it isn't fresh; a capture taken after
/// it is.
#[tokio::test]
async fn an_unpinned_source_forks_only_from_a_capture_since_its_placement() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let store = ForkOperationStore::new(db.pool.clone());
    let source = insert_suspended_source(&db.pool, None, Some(15)).await;
    let capture = LocalArtifactId::new();
    insert_suspend_backup(&db.pool, source, capture, "ready", Some(5), 30).await;
    insert_suspend_backup(&db.pool, source, capture, "ready", Some(5), 10).await;
    assert_eq!(
        store.suspended_source_artifact(source.1).await.unwrap(),
        None
    );

    let mut fresh_artifacts = [LocalArtifactId::new(), LocalArtifactId::new()];
    fresh_artifacts.sort_by_key(|id| id.as_uuid());
    let [low_fresh_artifact, high_fresh_artifact] = fresh_artifacts;
    let fresh =
        insert_suspend_backup(&db.pool, source, low_fresh_artifact, "ready", Some(6), 5).await;
    let (snapshot_id, _) = store
        .suspended_source_artifact(source.1)
        .await
        .unwrap()
        .expect("a capture since the placement is fresh");
    assert_eq!(snapshot_id, fresh);

    // A named snapshot registered after the suspend capture (without a
    // generation, as nodes emit named snapshots) doesn't hide or reorder the
    // unpinned fork source, and a tied suspend capture breaks ties on higher
    // local_artifact_id before (timestamp, id).
    let named_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, artifact_kind, artifact_origin,
             local_artifact_id, artifact_snapshot_name, org_id
         ) VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '1 minute', 'full',
                   'snapshot', $3, 'named-after-suspend', $4)"
    ))
    .bind(named_id)
    .bind(source.1)
    .bind(LocalArtifactId::new())
    .bind(source.0)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (id, snapshot_id, storage_connection_string)
         VALUES ($1, $2, 's3://fork-source/named')"
    ))
    .bind(Uuid::now_v7())
    .bind(named_id)
    .execute(&db.pool)
    .await
    .unwrap();
    let (snapshot_id, _) = store
        .suspended_source_artifact(source.1)
        .await
        .unwrap()
        .expect("a later named snapshot does not hide the suspend capture");
    assert_eq!(snapshot_id, fresh);

    let tied_high =
        insert_suspend_backup(&db.pool, source, high_fresh_artifact, "ready", Some(6), 5).await;
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = (SELECT timestamp FROM snapshots WHERE id = $1)
          WHERE id = $2"
    ))
    .bind(fresh)
    .bind(tied_high)
    .execute(&db.pool)
    .await
    .unwrap();
    insert_suspend_backup(&db.pool, source, low_fresh_artifact, "ready", Some(6), 2).await;
    let (snapshot_id, _) = store
        .suspended_source_artifact(source.1)
        .await
        .unwrap()
        .expect("higher local_artifact_id wins a tied capture");
    assert_eq!(snapshot_id, tied_high);
}

/// An unpinned source forks from the capture its placement-fresh restore
/// loads. A later capture of the same execution whose upload failed doesn't
/// outrank it, so it doesn't hold the fork back, as it doesn't hold the
/// restore; a later execution's failed capture does, as it does there.
#[tokio::test]
async fn a_failed_later_capture_of_the_same_execution_doesnt_hold_the_fork() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let store = ForkOperationStore::new(db.pool.clone());
    // (the failed capture's generation, the ready capture forks)
    for (failed_generation, forks) in [(6_i64, true), (7, false)] {
        let source = insert_suspended_source(&db.pool, None, Some(15)).await;
        let ready = insert_suspend_backup(
            &db.pool,
            source,
            LocalArtifactId::new(),
            "ready",
            Some(6),
            10,
        )
        .await;
        insert_suspend_backup(
            &db.pool,
            source,
            LocalArtifactId::new(),
            "failed",
            Some(failed_generation),
            5,
        )
        .await;
        let picked = store
            .suspended_source_artifact(source.1)
            .await
            .unwrap()
            .map(|(snapshot_id, _)| snapshot_id);
        assert_eq!(
            picked,
            forks.then_some(ready),
            "failed generation {failed_generation}"
        );
    }
}

/// An unpinned source whose history is unordered has no fork source, as its
/// restore refuses it: a capture without a generation registered after one
/// with a generation may be the latest state or a re-upload of an older
/// capture. Once a pin names a capture, the source forks from it.
#[tokio::test]
async fn an_unpinned_source_with_an_unordered_history_has_no_fork_source() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let store = ForkOperationStore::new(db.pool.clone());
    let source = insert_suspended_source(&db.pool, None, None).await;
    let ranked = LocalArtifactId::new();
    insert_suspend_backup(&db.pool, source, ranked, "ready", Some(5), 30).await;
    let unranked =
        insert_suspend_backup(&db.pool, source, LocalArtifactId::new(), "ready", None, 10).await;
    assert_eq!(
        store.suspended_source_artifact(source.1).await.unwrap(),
        None
    );

    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = local_artifact_id,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         FROM snapshots
         WHERE sandboxes.id = $1 AND snapshots.id = $2"
    ))
    .bind(source.1)
    .bind(unranked)
    .execute(&db.pool)
    .await
    .unwrap();
    let (snapshot_id, _) = store
        .suspended_source_artifact(source.1)
        .await
        .unwrap()
        .expect("a pinned capture forks");
    assert_eq!(snapshot_id, unranked);
}

/// Forks of one source elect one capture leader whichever id form each
/// names the source by: a bare-UUID row from before the `sbx_` form and a
/// newer `sbx_` row, in either order, are candidates for the same leader.
#[tokio::test]
async fn capture_leader_election_spans_both_target_id_forms() {
    let Some(db) = try_pg_test_db().await.unwrap() else {
        return;
    };
    let org_id = OrgId::from_uuid(pplx_space_db_test_fixtures::SEED_ORG_ID);
    let frozen = |source| {
        let mut payload = ForkSandboxPayloadV1::new(
            source,
            ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
            "e2b.example.test".to_string(),
        );
        payload.freeze_source(
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
                auto_lifecycle: pplx_space_auto_lifecycle::AutoLifecyclePolicy::disabled(),
                network_policy: None,
            },
            None,
        );
        payload
    };
    let store = ForkOperationStore::new(db.pool.clone());
    for earlier_is_public in [false, true] {
        let source = SandboxId::new();
        let bare = source.to_string();
        let public = pplx_space_public_api::PublicSandboxId::from(source).to_string();
        let (earlier_target, claimed_target) = if earlier_is_public {
            (public, bare)
        } else {
            (bare, public)
        };
        let earlier = OperationId::new();
        let claimed = OperationId::new();
        let claim_token = format!("capture-election-{claimed}");
        let payload_json = serde_json::to_value(ForkSandboxPayload::v1(frozen(source))).unwrap();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, kind, target_type, target_id, state,
                 payload_json, created_at
             ) VALUES ($1, $2, 'fork_sandbox', 'sandbox', $3, 'queued', $4,
                       NOW() - INTERVAL '1 minute')"
        ))
        .bind(earlier.as_uuid())
        .bind(org_id)
        .bind(&earlier_target)
        .bind(Json(&payload_json))
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, kind, target_type, target_id, state,
                 payload_json, claim_token, claim_generation
             ) VALUES ($1, $2, 'fork_sandbox', 'sandbox', $3, 'running', $4, $5, 1)"
        ))
        .bind(claimed.as_uuid())
        .bind(org_id)
        .bind(&claimed_target)
        .bind(Json(&payload_json))
        .bind(&claim_token)
        .execute(&db.pool)
        .await
        .unwrap();
        let operation = ClaimedPublicOperation {
            id: claimed,
            org_id,
            actor_user_id: None,
            api_product: crate::public_operations::ApiProduct::Native,
            kind: pplx_space_public_api::OperationKind::ForkSandbox,
            target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
            target_id: Some(claimed_target.clone()),
            payload: payload_json.clone(),
            attempts: 1,
            claim_generation: 1,
            traceparent: None,
            age_at_first_claim_secs: None,
        };

        let mut payload = frozen(source);
        let leader = store
            .select_capture_leader(&claim_token, &operation, &mut payload)
            .await
            .unwrap();

        assert_eq!(
            leader, earlier,
            "the earlier fork must lead the later one (earlier_is_public: {earlier_is_public})"
        );
        assert_eq!(payload.capture_operation_id(), Some(earlier));
    }
}
