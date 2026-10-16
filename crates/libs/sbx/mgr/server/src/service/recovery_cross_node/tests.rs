use pplx_space_common_core::types::{LocalArtifactId, SandboxId, SnapshotId};

use super::*;
use crate::service::tests::service_tests::service_with_runtime_probe;

/// The reclamation fences wait rather than discarding a finished transfer.
///
/// The reclamation boundary is reached only after the whole staging transfer has
/// completed — up to two three-hour volume-manager waits plus a rootfs pull. A
/// non-blocking acquisition here turned any momentary lock holder into a
/// `RestoreOperationBusy` for the user-facing `Start` RPC, throwing that work away
/// with no retry of either fence.
#[tokio::test]
async fn the_reclamation_fences_wait_instead_of_discarding_a_finished_transfer() {
    let canonical_id = SandboxId::new().to_string();
    let cleanup_locks = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    let cleanup_lock = Arc::new(tokio::sync::Mutex::new(()));
    let staging_lock =
        crate::auto_lifecycle::AutoLifecycleCoordinator::default().staging_lock(&canonical_id);

    let held_cleanup = Arc::clone(&cleanup_lock).lock_owned().await;
    let cleanup_owner = crate::service::cleanup_support::CleanupLockGuard::new(
        Arc::clone(&cleanup_locks),
        canonical_id.clone(),
        Arc::clone(&cleanup_lock),
    );
    let mut acquisition = Box::pin(acquire_canonical_reclamation_fences(
        cleanup_owner,
        Arc::clone(&staging_lock),
    ));
    assert!(
        futures_util::poll!(&mut acquisition).is_pending(),
        "a held cleanup lock must make the reclamation wait, not fail"
    );
    drop(held_cleanup);
    let (cleanup_fence, staging_guard) = acquisition.await;

    // And both are held afterwards, so the destructive boundary below the call
    // site runs under cleanup-then-staging ownership.
    Arc::clone(&cleanup_lock)
        .try_lock_owned()
        .expect_err("the reclamation must hold the cleanup fence it acquired");
    Arc::clone(&staging_lock)
        .try_lock_owned()
        .expect_err("the reclamation must hold the staging lock it acquired");
    drop((cleanup_fence, staging_guard));

    // The staging half separately. Asserting only the cleanup half leaves the
    // regression this exists to catch — a non-blocking staging acquisition —
    // invisible: the future is `Pending` because cleanup is held, and both guards
    // genuinely are held at the end, so a green test would ship a discarded
    // multi-hour transfer.
    let held_staging = Arc::clone(&staging_lock).lock_owned().await;
    let cleanup_owner = crate::service::cleanup_support::CleanupLockGuard::new(
        cleanup_locks,
        canonical_id,
        Arc::clone(&cleanup_lock),
    );
    let mut acquisition = Box::pin(acquire_canonical_reclamation_fences(
        cleanup_owner,
        Arc::clone(&staging_lock),
    ));
    assert!(
        futures_util::poll!(&mut acquisition).is_pending(),
        "a held staging lock must make the reclamation wait, not fail"
    );
    drop(held_staging);
    drop(acquisition.await);
}

#[test]
fn interrupted_restore_uses_the_incoming_authoritative_source() {
    let persisted: ExactStorageUri =
        "s3://test-bucket/suspend_snapshots/persisted?region=us-east-1"
            .parse()
            .unwrap();
    let incoming = "s3://test-bucket/suspend_snapshots/incoming?region=us-east-1";

    let selected = select_interrupted_restore_source(Some(incoming), Some(persisted))
        .unwrap()
        .unwrap();
    assert_eq!(selected.as_wire(), incoming);
}

#[test]
fn interrupted_restore_persists_new_source_for_uri_less_retry() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let persisted: ExactStorageUri = "s3://test-bucket/suspend_snapshots/source-a?region=us-east-1"
        .parse()
        .unwrap();
    let selected: ExactStorageUri = "s3://test-bucket/suspend_snapshots/source-b?region=us-east-1"
        .parse()
        .unwrap();
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, created_at, updated_at,
             restore_origin, restore_phase, restore_storage_uri
         ) VALUES (?1, 'sandbox1', 'restoring', 'seed', '1785456000', '1785456000',
                   'remote', 'metadata_staging', ?2)",
        rusqlite::params![sandbox_id.to_string(), persisted.as_wire()],
    )
    .unwrap();

    persist_selected_interrupted_restore_source(&conn, sandbox_id, &persisted, &selected).unwrap();

    let durable_source: ExactStorageUri = conn
        .query_row(
            "SELECT restore_storage_uri FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
        .parse()
        .unwrap();
    let next_retry = select_interrupted_restore_source(None, Some(durable_source))
        .unwrap()
        .unwrap();
    assert_eq!(next_retry, selected);
}

#[test]
fn canonical_root_reclaim_requires_unchanged_provenance() {
    let source_a: ExactStorageUri = "s3://test-bucket/suspend/source-a?region=us-east-1"
        .parse()
        .unwrap();
    let source_b: ExactStorageUri = "s3://test-bucket/suspend/source-b?region=us-east-1"
        .parse()
        .unwrap();

    assert_eq!(
        canonical_root_reclaim_decision(&source_a, Some(&source_a), true).unwrap(),
        CanonicalRootReclaimDecision::Reclaim
    );
    canonical_root_reclaim_decision(&source_a, Some(&source_b), true).unwrap_err();
    canonical_root_reclaim_decision(&source_a, None, true).unwrap_err();
    assert_eq!(
        canonical_root_reclaim_decision(&source_a, None, false).unwrap(),
        CanonicalRootReclaimDecision::AlreadyVacant
    );
}

#[test]
fn canonical_root_reclaim_requires_the_staged_source_to_remain_exact() {
    let source_b: ExactStorageUri = "s3://test-bucket/suspend/source-b?region=us-east-1"
        .parse()
        .unwrap();
    let source_c: ExactStorageUri = "s3://test-bucket/suspend/source-c?region=us-east-1"
        .parse()
        .unwrap();

    ensure_staged_restore_source(&source_b, Some(&source_b)).unwrap();
    ensure_staged_restore_source(&source_b, Some(&source_c)).unwrap_err();
    ensure_staged_restore_source(&source_b, None).unwrap_err();
}

#[test]
fn canonical_conflict_failure_does_not_change_transfer_scope() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, user, created_at, updated_at)
         VALUES (?1, 'sandbox1', 'restoring', 'seed',
                 'user:019f8000-0000-7000-8000-000000000001', '1785456000', '1785456000')",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let scope = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        crate::db::insert_restore_transfer_scope(
            &conn,
            &sandbox_id.to_string(),
            scope,
            None,
            None,
            "/space/cache/.restore-scope",
        )
        .unwrap()
    );

    persist_cross_node_staging_failure(
        &conn,
        sandbox_id,
        "canonical-root classification",
        &crate::snapshot_restore::RestoreError::CanonicalRootConflict { sandbox_id },
        // No recovery claim: this is the ordinary restore path.
        None,
    )
    .unwrap();

    let retained = crate::db::restore_transfer_scope(&conn, &sandbox_id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(retained.transfer_scope_id, scope);
    let failure_reason: String = conn
        .query_row(
            "SELECT failure_reason FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(failure_reason.contains("canonical cache root"));
}

#[tokio::test]
async fn exchanged_restore_cleanup_failure_retains_scope_until_retry() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, user, created_at, updated_at)
         VALUES (?1, 'sandbox1', 'restoring', 'seed',
                 'user:019f8000-0000-7000-8000-000000000001', '1785456000', '1785456000')",
        [&sandbox_id],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET restore_origin = 'remote',
                restore_phase = 'artifact_staging',
                restore_storage_uri = 's3://test-bucket/suspend/source?region=us-east-1'
          WHERE id = ?1",
        [&sandbox_id],
    )
    .unwrap();
    let generation = pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap();
    let attempt_a = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str("019c0000-0000-7000-8000-000000000001").unwrap(),
    );
    let attempt_b = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str("019c0000-0000-7000-8000-000000000002").unwrap(),
    );
    let attempt_c = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str("019c0000-0000-7000-8000-000000000003").unwrap(),
    );
    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        crate::db::insert_restore_transfer_scope(
            &conn,
            &sandbox_id,
            transfer_scope_id,
            Some(generation),
            Some(attempt_b),
            "/space/cache/restore-staging/exchanged",
        )
        .unwrap()
    );
    let db = std::sync::Arc::new(std::sync::Mutex::new(conn));

    let failed = finalize_exchanged_restore_after_cleanup(
        &db,
        std::future::ready(Err(crate::snapshot_restore::RestoreError::Other(
            "injected remnant cleanup failure".to_string(),
        ))),
        |conn| {
            crate::db::publish_restored_suspended_without_artifact(conn, &sandbox_id).map_err(
                |error| {
                    crate::snapshot_restore::RestoreError::Other(format!(
                        "finalize restored sandbox: {error}"
                    ))
                },
            )
        },
    )
    .await;
    assert!(failed.is_err());
    {
        let conn = db.lock().unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT state FROM sandboxes WHERE id = ?1",
                [&sandbox_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            .as_str(),
            SandboxState::Restoring.as_str(),
        );
        assert_eq!(
            crate::db::restore_transfer_scope(&conn, &sandbox_id)
                .unwrap()
                .unwrap()
                .transfer_scope_id,
            transfer_scope_id,
            "failed physical cleanup must retain the exact durable retry owner"
        );
    }

    let retained_b = {
        let conn = db.lock().unwrap();
        crate::db::restore_transfer_scope(&conn, &sandbox_id)
            .unwrap()
            .unwrap()
    };
    let retained_c =
        claim_exchanged_restore_cleanup(&db, &sandbox_id, &retained_b, generation, attempt_c)
            .unwrap();
    assert_eq!(retained_c.transfer_scope_id, transfer_scope_id);
    assert_eq!(retained_c.staging_dir, retained_b.staging_dir);
    assert_eq!(retained_c.activation_attempt_id, Some(attempt_c));
    assert!(
        claim_exchanged_restore_cleanup(&db, &sandbox_id, &retained_c, generation, attempt_a,)
            .is_err(),
        "delayed A must remain fenced after C takes over B's failed cleanup"
    );

    finalize_exchanged_restore_after_cleanup(&db, std::future::ready(Ok(())), |conn| {
        crate::db::publish_restored_suspended_without_artifact(conn, &sandbox_id).map_err(|error| {
            crate::snapshot_restore::RestoreError::Other(format!(
                "finalize restored sandbox: {error}"
            ))
        })
    })
    .await
    .unwrap();
    let conn = db.lock().unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&sandbox_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
        .as_str(),
        SandboxState::Suspended.as_str(),
    );
    assert!(
        crate::db::restore_transfer_scope(&conn, &sandbox_id)
            .unwrap()
            .is_none(),
        "successful retry must retire the durable scope with lifecycle finalization"
    );
}

fn restore_artifact_as_new_context(
    artifact_snapshot_id: SnapshotId,
    artifact_kind: pplx_space_common_core::types::ArtifactKind,
) -> RestoreArtifactAsNewContext {
    RestoreArtifactAsNewContext {
        source_sandbox_id: SandboxId::new(),
        artifact_snapshot_id,
        target_owner: format!("user:{}", uuid::Uuid::now_v7()).parse().unwrap(),
        artifact_kind,
        target_disk_io_limits: None,
    }
}

#[test]
fn interrupted_restore_as_new_reuses_published_canonical_staging() {
    assert_eq!(
        restore_as_new_metadata_destination(true, true),
        RestoreAsNewMetadataDestination::Canonical,
    );
}

#[test]
fn interrupted_restore_as_new_publishes_metadata_and_finalizes_as_stopped() {
    let sandbox_dir = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
         VALUES (?1, 'sandbox1', 'restoring', 'seed', '1785456000', '1785456000')",
        [&sandbox_id],
    )
    .unwrap();
    let (mut meta, _) = disk_artifact_meta();
    rebind_artifact_snapshot_identity(
        &mut meta,
        &sandbox_id,
        &"user:019f8000-0000-7000-8000-000000000005".parse().unwrap(),
    )
    .unwrap();

    let result = finalize_recovered_published_restore(
        &conn,
        &sandbox_id,
        sandbox_dir.path(),
        &meta,
        true,
        CrossNodeRestoreTarget::SuspendedCheckpoint,
    );

    assert!(
        result.is_ok(),
        "disk restore-as-new must finalize: {result:?}"
    );
    assert_eq!(
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&sandbox_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        SandboxState::Stopped.as_str(),
    );
    let published: pplx_space_common_core::types::SandboxArtifactMetadata = serde_json::from_slice(
        &std::fs::read(sandbox_dir.path().join("metadata.json"))
            .expect("restore-as-new recovery must publish sandbox metadata"),
    )
    .unwrap();
    assert_eq!(published.sandbox_id.to_string(), sandbox_id);
}

fn disk_artifact_meta() -> (
    crate::snapshot_upload::SnapshotMeta,
    pplx_space_common_core::types::SnapshotName,
) {
    let capture_name: pplx_space_common_core::types::SnapshotName =
        "d-operation-019f8000-0000-7000-8000-000000000006"
            .parse()
            .unwrap();
    let execution = pplx_space_common_core::types::SnapshotExecutionMetadata::new(
        pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
            std::env::consts::ARCH.parse().unwrap(),
            vec![],
        ),
    )
    .unwrap();
    (
        crate::snapshot_upload::SnapshotMeta {
            version: crate::snapshot_upload::SNAPSHOT_METADATA_VERSION,
            execution: Some(execution),
            sandbox_id: "019f8000-0000-7000-8000-000000000002".to_string(),
            size_bytes: 0,
            created_at: "2026-07-31T00:00:00Z".to_string(),
            source: crate::snapshot_upload::SnapshotSource {
                runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.to_string(),
                runtime_version: None,
                state_format_version: None,
                template_id: "seed".to_string(),
                content_hash: None,
                rootfs_size_mib: None,
                rootfs_version: None,
                kernel_version: None,
                kernel_digest: None,
                disk_io_max_operations_per_sec: None,
                disk_io_max_bandwidth_mib_per_sec: None,
            },
            sandbox: Some(crate::snapshot_upload::SnapshotSandbox {
                owner: "user:019f8000-0000-7000-8000-000000000003".to_string(),
                snapshots_enabled: true,
                process_env: std::collections::BTreeMap::new(),
                auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            }),
            rootfs_diff_format: None,
            rootfs_send_source_id: None,
            encrypted: false,
            local_artifact_id: Some(LocalArtifactId::new()),
            artifact_kind: Some(pplx_space_common_core::types::ArtifactKind::Disk),
            artifact_origin: Some(pplx_space_common_core::types::ArtifactOrigin::Snapshot),
            artifact_snapshot_name: Some(capture_name.clone()),
        },
        capture_name,
    )
}

/// A `stop`-origin disk artifact: what a drained node publishes for a stopped
/// resident, and the only artifact shape a stopped materialization may pin.
fn evacuated_stop_disk_meta() -> crate::snapshot_upload::SnapshotMeta {
    let (mut meta, _) = disk_artifact_meta();
    meta.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Stop);
    meta.artifact_snapshot_name = None;
    meta
}

fn insert_restoring_row(conn: &rusqlite::Connection, sandbox_id: &str) {
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
         VALUES (?1, 'sandbox1', 'restoring', 'seed', 'now', 'now')",
        [sandbox_id],
    )
    .unwrap();
}

fn current_pin(conn: &rusqlite::Connection, sandbox_id: &str) -> Option<String> {
    conn.query_row(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = ?1",
        [sandbox_id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn a_stop_origin_disk_artifact_is_the_pin_a_stopped_materialization_takes() {
    let meta = evacuated_stop_disk_meta();

    assert_eq!(
        stopped_materialization_pin(&meta, meta.local_artifact_id.unwrap()).unwrap(),
        meta.local_artifact_id.unwrap()
    );
}

/// Every artifact shape that is *not* an evacuated stopped disk, refused.
///
/// Each of these is a real artifact the selector could hand this path if it
/// picked the wrong row, and each would leave the sandbox pinned to bytes that
/// are not the disk it was evacuated with: a fork's immutable source, a
/// suspend checkpoint that also carries VM memory, a user's named snapshot, or
/// a legacy sidecar from before typed artifacts existed.
#[test]
fn a_stopped_materialization_refuses_every_artifact_that_is_not_its_own_disk() {
    use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin};

    type MetaMutation = Box<dyn Fn(&mut crate::snapshot_upload::SnapshotMeta)>;
    let cases: Vec<(&str, MetaMutation)> = vec![
        (
            "a fork's immutable source disk",
            Box::new(|meta: &mut crate::snapshot_upload::SnapshotMeta| {
                meta.artifact_origin = Some(ArtifactOrigin::Fork);
            }),
        ),
        (
            "a named user snapshot",
            Box::new(|meta: &mut crate::snapshot_upload::SnapshotMeta| {
                meta.artifact_origin = Some(ArtifactOrigin::Snapshot);
            }),
        ),
        (
            "a suspend checkpoint carrying VM memory",
            Box::new(|meta: &mut crate::snapshot_upload::SnapshotMeta| {
                meta.artifact_kind = Some(ArtifactKind::Full);
                meta.artifact_origin = Some(ArtifactOrigin::Suspend);
            }),
        ),
        (
            "a disk artifact whose sidecar names no local artifact",
            Box::new(|meta: &mut crate::snapshot_upload::SnapshotMeta| {
                meta.local_artifact_id = None;
            }),
        ),
        (
            "an encrypted capture, which stages no plaintext rootfs",
            Box::new(|meta: &mut crate::snapshot_upload::SnapshotMeta| {
                meta.encrypted = true;
            }),
        ),
        (
            "a legacy sidecar from before typed artifacts",
            Box::new(|meta: &mut crate::snapshot_upload::SnapshotMeta| {
                meta.artifact_kind = None;
                meta.artifact_origin = None;
            }),
        ),
    ];

    for (description, mutate) in cases {
        let mut meta = evacuated_stop_disk_meta();
        mutate(&mut meta);
        let expected = meta.local_artifact_id.unwrap_or_default();
        let error = stopped_materialization_pin(&meta, expected).expect_err(&format!(
            "{description} must not be pinned as an evacuated stopped disk"
        ));
        assert!(
            matches!(
                error,
                crate::snapshot_restore::RestoreError::StoppedMaterializationArtifactUnsuitable { .. }
            ),
            "{description} produced the wrong refusal: {error:?}"
        );
    }
}

/// `stopped_materialization_pin` refuses a sidecar naming a different artifact
/// than the caller expects.
///
/// It is the check `run_cross_node_restore` runs on the sidecar as soon as it
/// lands, ahead of the rootfs transfer and ahead of the finalize; that ordering
/// is only exercised end to end, since driving the restore needs object
/// storage. This test pins the comparison itself.
/// Putting the comparison here rather than at the reply is what keeps a
/// disagreement from costing a multi-gigabyte pull and leaving a committed row
/// pinned to an artifact the boundary then refuses to vouch for — recoverable
/// only by a caller that corrects its request.
#[test]
fn the_stopped_pin_refuses_an_unexpected_artifact() {
    let meta = evacuated_stop_disk_meta();
    let staged = meta.local_artifact_id.unwrap();
    let asked_for = pplx_space_common_core::types::LocalArtifactId::new();

    let error = stopped_materialization_pin(&meta, asked_for)
        .expect_err("a sidecar naming a different artifact must not be pinned");

    match error {
        crate::snapshot_restore::RestoreError::StoppedMaterializationArtifactMismatch {
            expected,
            staged: got,
        } => {
            assert_eq!(expected, asked_for);
            assert_eq!(got, staged);
        }
        other => panic!("expected a mismatch refusal, got {other:?}"),
    }
}

/// The row is left untouched by that refusal — the point of catching it early.
#[test]
fn an_unexpected_artifact_leaves_the_row_alone() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_restoring_row(&conn, &sandbox_id);
    let meta = evacuated_stop_disk_meta();

    transition_restored_sandbox_to_stopped(
        &conn,
        &sandbox_id,
        &meta,
        pplx_space_common_core::types::LocalArtifactId::new(),
    )
    .expect_err("a mismatch must not publish");

    assert_eq!(
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&sandbox_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        SandboxState::Restoring.as_str(),
        "the row must still be restoring, not committed to the wrong artifact"
    );
    assert_eq!(
        current_pin(&conn, &sandbox_id),
        None,
        "and it must not have been pinned"
    );
}

#[test]
fn materializing_a_stopped_sandbox_lands_stopped_with_its_pin() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_restoring_row(&conn, &sandbox_id);
    let meta = evacuated_stop_disk_meta();

    transition_restored_sandbox_to_stopped(
        &conn,
        &sandbox_id,
        &meta,
        meta.local_artifact_id.unwrap(),
    )
    .unwrap();

    assert_eq!(
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&sandbox_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        SandboxState::Stopped.as_str(),
    );
    assert_eq!(
        current_pin(&conn, &sandbox_id),
        meta.local_artifact_id.map(|id| id.to_string()),
    );
    // The pin row never shares the suspend checkpoint's path, so nothing that
    // later stamps or reaps it can reach `{id}/snapshot`.
    let row = crate::db::get_local_artifact(&conn, meta.local_artifact_id.unwrap())
        .unwrap()
        .expect("the pin names a local artifact row");
    assert_eq!(
        row.local_path,
        pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
            &sandbox_id,
            &format!("d-stop-{}", meta.local_artifact_id.unwrap()),
        ),
    );
    assert_ne!(
        row.local_path,
        format!("{CACHE_DIR}/{sandbox_id}/snapshot"),
        "the pin must not sit on the suspend checkpoint's path"
    );
}

/// The three finalize arms, selected on one fixture.
///
/// The same staged disk sidecar reaches all three; what differs is the
/// request. A fork takes the restore-as-new arm and drops the pin, a stopped
/// target takes the evacuation arm and keeps it, and the historical
/// suspend-checkpoint arm refuses the disk outright because a checkpoint
/// restore has to carry VM memory. Driving them from one fixture is what makes
/// a mis-wired dispatch visible: pick the wrong arm and the assertion for the
/// arm you meant fails.
#[test]
fn each_finalize_arm_is_selected_by_the_request_not_by_the_artifact() {
    let conn = crate::db::test_conn();
    let sandbox_dir = tempfile::tempdir().unwrap();
    let meta = evacuated_stop_disk_meta();

    let forked = SandboxId::new().to_string();
    insert_restoring_row(&conn, &forked);
    let mut forked_meta = meta.clone();
    rebind_artifact_snapshot_identity(
        &mut forked_meta,
        &forked,
        &"user:019f8000-0000-7000-8000-000000000005".parse().unwrap(),
    )
    .unwrap();
    finalize_recovered_published_restore(
        &conn,
        &forked,
        sandbox_dir.path(),
        &forked_meta,
        true,
        CrossNodeRestoreTarget::SuspendedCheckpoint,
    )
    .expect("a fork request must still take the restore-as-new arm");
    assert_eq!(
        current_pin(&conn, &forked),
        None,
        "the fork arm must leave the staged disk unpinned"
    );

    let evacuated = SandboxId::new().to_string();
    insert_restoring_row(&conn, &evacuated);
    finalize_recovered_published_restore(
        &conn,
        &evacuated,
        sandbox_dir.path(),
        &meta,
        false,
        CrossNodeRestoreTarget::StoppedDiskArtifact {
            expected: meta.local_artifact_id.unwrap(),
        },
    )
    .expect("a stopped target must take the evacuation arm");
    assert_eq!(
        current_pin(&conn, &evacuated),
        meta.local_artifact_id.map(|id| id.to_string()),
        "the evacuation arm must keep the pin"
    );

    let checkpointed = SandboxId::new().to_string();
    insert_restoring_row(&conn, &checkpointed);
    let error = finalize_recovered_published_restore(
        &conn,
        &checkpointed,
        sandbox_dir.path(),
        &meta,
        false,
        CrossNodeRestoreTarget::SuspendedCheckpoint,
    )
    .expect_err("the checkpoint arm must not accept a disk artifact");
    assert!(
        error.to_string().contains("must identify a full artifact"),
        "unexpected checkpoint-arm refusal: {error}"
    );
    assert_eq!(
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&checkpointed],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        SandboxState::Restoring.as_str(),
    );
}

#[derive(Clone, Default)]
struct EventFieldCapture {
    fields: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

impl<S> tracing_subscriber::Layer<S> for EventFieldCapture
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        event.record(&mut visitor);
        self.fields
            .lock()
            .expect("capture mutex poisoned")
            .extend(visitor.into_fields());
    }
}

#[test]
fn interrupted_restore_completion_log_redacts_storage_uri() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let storage_uri =
        "s3://bucket/snapshot?endpoint=https%3A%2F%2Faccess%3Asecret%40storage.example";
    let capture = EventFieldCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    tracing::subscriber::with_default(subscriber, || {
        log_interrupted_restore_completion("sandbox", storage_uri);
    });

    let expected_len = storage_uri.len().to_string();
    let fields = capture.fields.lock().expect("capture mutex poisoned");
    assert_eq!(
        fields
            .iter()
            .find(|(name, _)| name == "storage_uri_len")
            .map(|(_, value)| value.as_str()),
        Some(expected_len.as_str())
    );
    assert!(fields.iter().all(|(name, _)| name != "storage_uri"));
    assert!(fields.iter().all(|(_, value)| !value.contains("secret")));
}

#[test]
fn quarantine_recovery_requires_matching_artifact_identity_shape() {
    let artifact_id = LocalArtifactId::new();
    assert_eq!(
        validate_quarantine_recovery_artifact_identity(
            "sandbox",
            Some(artifact_id),
            Some(artifact_id),
        )
        .unwrap(),
        Some(artifact_id)
    );
    assert_eq!(
        validate_quarantine_recovery_artifact_identity("sandbox", None, None).unwrap(),
        None
    );

    validate_quarantine_recovery_artifact_identity("sandbox", Some(artifact_id), None).unwrap_err();
    validate_quarantine_recovery_artifact_identity("sandbox", None, Some(artifact_id)).unwrap_err();
    validate_quarantine_recovery_artifact_identity(
        "sandbox",
        Some(artifact_id),
        Some(LocalArtifactId::new()),
    )
    .unwrap_err();
}

#[test]
fn restore_artifact_as_new_context_requires_the_exact_snapshot_uri() {
    let snapshot_id = SnapshotId::new();
    let context = restore_artifact_as_new_context(
        snapshot_id,
        pplx_space_common_core::types::ArtifactKind::Disk,
    );
    let matching_uri = format!(
        "s3://space-test/suspend_snapshots/00/{}/{snapshot_id}?region=us-east-1",
        context.source_sandbox_id
    );
    validate_restore_artifact_as_new_context(&context, &matching_uri).unwrap();

    let mismatched_uri = format!(
        "s3://space-test/suspend_snapshots/00/{}/{}?region=us-east-1",
        context.source_sandbox_id,
        uuid::Uuid::now_v7(),
    );
    let error = validate_restore_artifact_as_new_context(&context, &mismatched_uri)
        .expect_err("mismatched URI must be rejected");
    assert!(matches!(
        error,
        crate::snapshot_restore::RestoreError::ArtifactSnapshotUriMismatch {
            expected,
            ..
        } if expected == snapshot_id
    ));

    let mismatched_source_uri = format!(
        "s3://space-test/suspend_snapshots/00/{}/{snapshot_id}?region=us-east-1",
        SandboxId::new()
    );
    let error = validate_restore_artifact_as_new_context(&context, &mismatched_source_uri)
        .expect_err("mismatched source sandbox URI must be rejected");
    assert!(matches!(
        error,
        crate::snapshot_restore::RestoreError::ArtifactSourceUriMismatch {
            expected,
            ..
        } if expected == context.source_sandbox_id
    ));
}

#[test]
fn full_artifact_restore_as_new_is_reserved_but_not_routed_through_disk_restore() {
    let (meta, _) = disk_artifact_meta();
    let context = restore_artifact_as_new_context(
        SnapshotId::new(),
        pplx_space_common_core::types::ArtifactKind::Full,
    );
    let error = validate_artifact_restore_as_new(&meta, &context, &context.source_sandbox_id)
        .expect_err("full restore semantics are not implemented in the disk PR");
    assert!(error.to_string().contains("not implemented"));
}

#[test]
fn restore_artifact_as_new_rejects_legacy_metadata_without_owner_fields() {
    let owner = "user:019f8000-0000-7000-8000-000000000004".parse().unwrap();
    let error = rebind_artifact_snapshot_owner(None, &owner)
        .expect_err("artifact restore must not silently substitute the guest owner");
    assert!(matches!(
        error,
        crate::snapshot_restore::RestoreError::MissingSandboxOwnershipMetadata
    ));
}

#[test]
fn restore_artifact_as_new_rebind_clears_source_snapshot_identity() {
    let (mut meta, _) = disk_artifact_meta();
    let source_execution = meta.execution.clone();

    rebind_artifact_snapshot_identity(
        &mut meta,
        "019f8000-0000-7000-8000-000000000004",
        &"user:019f8000-0000-7000-8000-000000000005".parse().unwrap(),
    )
    .unwrap();

    assert_eq!(meta.sandbox_id, "019f8000-0000-7000-8000-000000000004");
    assert_eq!(meta.execution, source_execution);
    assert_eq!(meta.local_artifact_id, None);
    assert_eq!(
        meta.artifact_origin,
        Some(pplx_space_common_core::types::ArtifactOrigin::Snapshot)
    );
    assert_eq!(meta.artifact_snapshot_name, None);
    assert_eq!(
        meta.sandbox.as_ref().map(|sandbox| sandbox.owner.as_str()),
        Some("user:019f8000-0000-7000-8000-000000000005")
    );
}

#[test]
fn restore_artifact_as_new_rebind_keeps_source_sidecar_until_artifacts_are_staged() {
    let temp = tempfile::tempdir().unwrap();
    let metadata_path = temp.path().join("snapshot.json");
    let (mut meta, _) = disk_artifact_meta();
    std::fs::write(&metadata_path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();

    rebind_artifact_snapshot_identity(
        &mut meta,
        "019f8000-0000-7000-8000-000000000004",
        &"user:019f8000-0000-7000-8000-000000000005".parse().unwrap(),
    )
    .unwrap();

    let preserved: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
    assert_eq!(preserved.sandbox_id, "019f8000-0000-7000-8000-000000000002");

    publish_rebound_artifact_snapshot_metadata(&meta, temp.path()).unwrap();
    let published: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
    assert_eq!(published.sandbox_id, "019f8000-0000-7000-8000-000000000004");
    assert_eq!(published.local_artifact_id, None);
}

#[test]
fn restore_artifact_as_new_accepts_only_bound_disk_artifacts() {
    let (mut meta, _) = disk_artifact_meta();
    let mut context = restore_artifact_as_new_context(
        SnapshotId::new(),
        pplx_space_common_core::types::ArtifactKind::Disk,
    );
    context.source_sandbox_id = meta.sandbox_id.parse().unwrap();
    validate_artifact_restore_as_new(&meta, &context, &context.source_sandbox_id)
        .expect("disk artifact is accepted");

    meta.artifact_kind = Some(pplx_space_common_core::types::ArtifactKind::Full);
    let error = validate_artifact_restore_as_new(&meta, &context, &context.source_sandbox_id)
        .expect_err("full checkpoint must not enter the disk-only restore path");
    assert!(error.to_string().contains("does not match requested disk"));

    meta.artifact_kind = Some(pplx_space_common_core::types::ArtifactKind::Disk);
    meta.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Suspend);
    let error = validate_artifact_restore_as_new(&meta, &context, &context.source_sandbox_id)
        .expect_err("suspend checkpoints are not disk-only fork artifacts");
    assert!(error.to_string().contains("snapshot or fork disk artifact"));

    meta.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Fork);
    meta.sandbox_id = SandboxId::new().to_string();
    let error = validate_artifact_restore_as_new(&meta, &context, &context.source_sandbox_id)
        .expect_err("projected fork identity must stay bound to the target");
    assert!(matches!(
        error,
        crate::snapshot_restore::RestoreError::SnapshotIdentityMismatch { .. }
    ));
}

#[test]
fn projected_fork_metadata_is_validated_against_the_target_identity() {
    let (mut meta, _) = disk_artifact_meta();
    let context = restore_artifact_as_new_context(
        SnapshotId::new(),
        pplx_space_common_core::types::ArtifactKind::Disk,
    );
    let target_sandbox_id = SandboxId::new();
    meta.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Fork);
    meta.sandbox_id = target_sandbox_id.to_string();

    validate_artifact_restore_as_new(&meta, &context, &target_sandbox_id)
        .expect("a projected fork artifact belongs to the restore target");
}

#[test]
fn restore_artifact_as_new_keeps_unnamed_fork_metadata_for_child_publication() {
    let (mut meta, _) = disk_artifact_meta();
    meta.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Fork);
    meta.artifact_snapshot_name = None;
    let target = "019f8000-0000-7000-8000-000000000004";

    rebind_artifact_snapshot_identity(
        &mut meta,
        target,
        &"user:019f8000-0000-7000-8000-000000000005".parse().unwrap(),
    )
    .unwrap();

    assert_eq!(
        meta.artifact_origin,
        Some(pplx_space_common_core::types::ArtifactOrigin::Fork)
    );
    assert_eq!(meta.artifact_snapshot_name, None);
}

#[test]
fn restore_artifact_as_new_publishes_execution_metadata_for_cold_start() {
    let temp = tempfile::tempdir().unwrap();
    let (mut meta, _) = disk_artifact_meta();
    let expected_execution = meta
        .execution
        .as_ref()
        .expect("test artifact has execution metadata")
        .execution()
        .clone();

    rebind_artifact_snapshot_identity(
        &mut meta,
        "019f8000-0000-7000-8000-000000000004",
        &"user:019f8000-0000-7000-8000-000000000005".parse().unwrap(),
    )
    .unwrap();
    publish_rebound_sandbox_artifact_metadata(&meta, temp.path()).unwrap();

    let published: pplx_space_common_core::types::SandboxArtifactMetadata =
        serde_json::from_slice(&std::fs::read(temp.path().join("metadata.json")).unwrap()).unwrap();
    assert_eq!(
        published.sandbox_id.to_string(),
        "019f8000-0000-7000-8000-000000000004"
    );
    assert_eq!(published.execution, expected_execution);
}

/// A renewal closure that reports how many heartbeats it saw and can be told to
/// lose the claim or fail against SQLite.
struct RenewalStub {
    calls: std::sync::atomic::AtomicUsize,
    verdict: Result<bool, rusqlite::Error>,
    fail_first: u32,
}

impl RenewalStub {
    fn new(verdict: Result<bool, rusqlite::Error>) -> Self {
        Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
            verdict,
            fail_first: 0,
        }
    }

    /// Fails its first `times` renewals, then reports the claim still held.
    fn failing_times(times: u32) -> Self {
        Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
            verdict: Ok(true),
            fail_first: times,
        }
    }

    fn renew(&self) -> Result<bool, crate::snapshot_restore::RestoreError> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if u32::try_from(call).unwrap_or(u32::MAX) < self.fail_first {
            return Err(
                crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim {
                    source: rusqlite::Error::QueryReturnedNoRows,
                },
            );
        }
        match &self.verdict {
            Ok(renewed) => Ok(*renewed),
            Err(_) => Err(
                crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim {
                    source: rusqlite::Error::QueryReturnedNoRows,
                },
            ),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_renews_across_heartbeats_until_it_completes() {
    let sandbox_id = SandboxId::new();
    let renewal = RenewalStub::new(Ok(true));
    let provenance = drive_under_quarantine_recovery_claim(
        async {
            tokio::time::sleep(std::time::Duration::from_secs(11 * 60)).await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        std::time::Duration::from_secs(60 * 60),
        tokio_util::sync::CancellationToken::new(),
        || renewal.renew(),
    )
    .await
    .unwrap();

    assert_eq!(provenance, Some(()));
    // The immediate first tick is consumed before the loop, so an eleven-minute
    // operation sees the five- and ten-minute renewals and nothing else.
    assert_eq!(renewal.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_stops_at_the_operation_deadline() {
    let sandbox_id = SandboxId::new();
    let renewal = RenewalStub::new(Ok(true));
    let timeout = std::time::Duration::from_secs(30 * 60);
    let error = drive_under_quarantine_recovery_claim(
        async {
            std::future::pending::<()>().await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        timeout,
        tokio_util::sync::CancellationToken::new(),
        || renewal.renew(),
    )
    .await
    .expect_err("a stalled operation must stop at its deadline");

    assert!(matches!(
        error,
        crate::snapshot_restore::RestoreError::QuarantineRecoveryTimedOut {
            sandbox_id: timed_out,
            timeout: reported,
        } if timed_out == sandbox_id && reported == timeout
    ));
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_aborts_when_the_claim_is_lost() {
    let sandbox_id = SandboxId::new();
    let renewal = RenewalStub::new(Ok(false));
    let error = drive_under_quarantine_recovery_claim(
        async {
            std::future::pending::<()>().await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        std::time::Duration::from_secs(60 * 60),
        tokio_util::sync::CancellationToken::new(),
        || renewal.renew(),
    )
    .await
    .expect_err("a lost claim must abort the operation");

    assert!(matches!(
        error,
        crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded {
            sandbox_id: superseded,
        } if superseded == sandbox_id
    ));
    assert_eq!(renewal.calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_propagates_a_failed_renewal_as_a_typed_error() {
    let sandbox_id = SandboxId::new();
    let renewal = RenewalStub::new(Err(rusqlite::Error::QueryReturnedNoRows));
    let error = drive_under_quarantine_recovery_claim(
        async {
            std::future::pending::<()>().await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        std::time::Duration::from_secs(60 * 60),
        tokio_util::sync::CancellationToken::new(),
        || renewal.renew(),
    )
    .await
    .expect_err("a failed renewal must not be reported as a lost claim");

    assert!(
        matches!(
            error,
            crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim { .. }
        ),
        "renewal failure must stay distinguishable from supersession, got {error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_abandons_itself_at_shutdown() {
    let sandbox_id = SandboxId::new();
    let renewal = RenewalStub::new(Ok(true));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(90)).await;
        trigger.cancel();
    });

    let error = drive_under_quarantine_recovery_claim(
        async {
            std::future::pending::<()>().await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        std::time::Duration::from_secs(8 * 60 * 60),
        shutdown,
        || renewal.renew(),
    )
    .await
    .expect_err("shutdown must end the operation instead of running to its deadline");

    assert!(
        matches!(
            error,
            crate::snapshot_restore::RestoreError::QuarantineRecoveryAbandonedAtShutdown {
                sandbox_id: abandoned,
            } if abandoned == sandbox_id
        ),
        "expected an abandoned-at-shutdown verdict, got {error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_survives_transient_renewal_failures() {
    let sandbox_id = SandboxId::new();
    // Fails the first renewal, then succeeds. A multi-hour transfer must not be
    // thrown away for a transient SQLITE_BUSY on a shared database.
    //
    // One, not two: the budget is two consecutive failures, and at a 300s
    // heartbeat two of them is a 600s outage rather than a blip. Aborting there
    // is deliberate — it keeps the abort a full heartbeat inside the 900s lease,
    // so the operation can never continue with no lease at all.
    let renewal = RenewalStub::failing_times(1);
    let provenance = drive_under_quarantine_recovery_claim(
        async {
            tokio::time::sleep(std::time::Duration::from_secs(21 * 60)).await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        std::time::Duration::from_secs(8 * 60 * 60),
        tokio_util::sync::CancellationToken::new(),
        || renewal.renew(),
    )
    .await
    .expect("transient renewal failures must not abort the operation");

    assert_eq!(provenance, Some(()));
    assert!(renewal.calls() >= 3, "expected renewals to be retried");
}

#[tokio::test(start_paused = true)]
async fn claimed_operation_gives_up_after_repeated_renewal_failures() {
    let sandbox_id = SandboxId::new();
    let renewal = RenewalStub::failing_times(u32::MAX);
    let error = drive_under_quarantine_recovery_claim(
        async {
            std::future::pending::<()>().await;
            Ok(Some(()))
        },
        sandbox_id,
        std::time::Duration::from_secs(5 * 60),
        std::time::Duration::from_secs(8 * 60 * 60),
        tokio_util::sync::CancellationToken::new(),
        || renewal.renew(),
    )
    .await
    .expect_err("a database that never answers must eventually end the operation");

    assert!(
        matches!(
            error,
            crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim { .. }
        ),
        "the underlying renewal error must be preserved, got {error:?}"
    );
    assert_eq!(
        renewal.calls(),
        QUARANTINE_RECOVERY_RENEWAL_FAILURES_BEFORE_ABORT as usize
    );
}

/// A superseded claim must leave no adopted sandbox row behind.
///
/// The adoption used to insert the orphan's sandbox row through a function that
/// opened and committed a transaction of its own, and only then run the
/// generation CAS. A task whose lease had lapsed and been re-claimed by another
/// pass would therefore materialize a `restoring` row for an episode it no
/// longer owned, fail the CAS, and leave that row behind. Both writes are now one
/// transaction, so the refusal rolls the insert back with it.
///
/// The adopted id differs from the episode's here, which keeps this on the plain
/// insert branch. Adoption of an existing synthetic-orphan row has its own
/// preconditions and is not covered by this test.
#[test]
fn a_superseded_recovery_claim_leaves_no_adopted_row() {
    let conn = crate::db::test_conn();
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let sandbox_id = crate::db::tests::setup_quarantine_recovery_episode(&conn, reason_code);
    let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let stale = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("the first claim should succeed");

    // Supersede it the way production does: the lease lapses and the next pass
    // takes the episode, minting a fresh claim token.
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET claim_token = ?1, claim_expires_at = ?2, next_action_after = NULL
          WHERE sandbox_id = ?3",
        rusqlite::params![
            uuid::Uuid::now_v7().to_string(),
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 900)
                .to_string(),
            sandbox_id.to_string()
        ],
    )
    .unwrap();

    let orphan_id = SandboxId::new();
    let (mut meta, _) = disk_artifact_meta();
    meta.sandbox_id = orphan_id.to_string();
    let restore_source = pplx_space_common_core::types::ExactStorageUri::parse(
        "s3://bucket/snapshots/orphan/vmstate?region=us-east-1",
    )
    .expect("the fixture URI should parse");
    let plan = crate::snapshot_restore::QuarantineRecoveryPlan {
        recovery_id: pplx_space_common_core::types::SandboxCleanupEpisodeId::new(),
        snapshot_id: SnapshotId::new(),
        storage_uri: "s3://bucket/snapshots/orphan/vmstate?region=us-east-1".to_string(),
        local_artifact_id: None,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap(),
        reason_code,
    };
    let recovery = super::QuarantineRecoveryContext {
        plan: &plan,
        claim: &stale,
    };

    let outcome = super::adopt_restoring_row_under_recovery_claim(
        &conn,
        orphan_id,
        &orphan_id.to_string(),
        super::RestoringRowInput::new(&meta, &restore_source, None),
        Some(&recovery),
        false,
    );
    assert!(
        matches!(
            outcome,
            Err(crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded { .. })
        ),
        "a stale claim must be refused, got {outcome:?}"
    );
    let adopted: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sandboxes WHERE id = ?1",
            [orphan_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        adopted, 0,
        "the refused CAS must roll the adoption back; before this was one \
         transaction the row committed on its own and outlived the failure"
    );
}

/// The committing path of the same helper, which is what proves the transaction
/// is usable rather than merely present.
///
/// The row insert underneath used to open a transaction of its own. Nesting that
/// inside the claim transaction fails on the committing path and on no other, so
/// a refusal-only test would have passed over an adoption that could never
/// succeed.
#[test]
fn an_adoption_inside_the_claim_transaction_commits_the_row() {
    let conn = crate::db::test_conn();
    let orphan_id = SandboxId::new();
    let (mut meta, _) = disk_artifact_meta();
    meta.sandbox_id = orphan_id.to_string();
    let restore_source = pplx_space_common_core::types::ExactStorageUri::parse(
        "s3://bucket/snapshots/orphan/vmstate?region=us-east-1",
    )
    .expect("the fixture URI should parse");

    // No recovery context: this is the insert-and-commit half, isolated from the
    // claim CAS so a nested transaction would surface as a failure here.
    super::adopt_restoring_row_under_recovery_claim(
        &conn,
        orphan_id,
        &orphan_id.to_string(),
        super::RestoringRowInput::new(&meta, &restore_source, None),
        None,
        false,
    )
    .expect("the adoption should commit inside the claim transaction");

    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [orphan_id.to_string()],
            |row| row.get(0),
        )
        .expect("the adopted row should exist after the commit");
    assert_eq!(state, "restoring");
}

/// The ordinary cross-node retry must not take the node database's write lock.
///
/// With `retrying_interrupted_restore` and no recovery context the call writes
/// nothing — no insert, no CAS — so all it needs is an identity comparison.
/// Wrapping it in `BEGIN IMMEDIATE` took the write lock and committed empty,
/// which serializes every ordinary retry behind a no-op.
///
/// The oracle is a second connection holding the write lock: the no-write path
/// has to succeed anyway, and would fail with `SQLITE_BUSY` if it still asked
/// for a transaction.
#[test]
fn an_ordinary_cross_node_retry_takes_no_write_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sbxmgr.db");
    let mut conn = rusqlite::Connection::open(&path).unwrap();
    crate::db::create_test_schema(&mut conn);
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();

    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker.busy_timeout(std::time::Duration::ZERO).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let sandbox_id = SandboxId::new();
    let (mut meta, _) = disk_artifact_meta();
    meta.sandbox_id = sandbox_id.to_string();
    let restore_source = pplx_space_common_core::types::ExactStorageUri::parse(
        "s3://bucket/snapshots/orphan/vmstate?region=us-east-1",
    )
    .expect("the fixture URI should parse");

    super::adopt_restoring_row_under_recovery_claim(
        &conn,
        sandbox_id,
        &sandbox_id.to_string(),
        super::RestoringRowInput::new(&meta, &restore_source, None),
        None,
        true,
    )
    .expect("a retry that writes nothing must not need the write lock");

    blocker.execute_batch("ROLLBACK").unwrap();
}

/// A sandbox that left an activatable state after the claim must be refused
/// before the adoption rewrites it back.
///
/// The generation CAS requires `state = 'restoring'`, and the fence relaxes its
/// own activatable check because the CAS is stricter. But the upsert between them
/// admits a row in `'failed'` and rewrites it to `'restoring'`, so the CAS would
/// pass on a value the same transaction had just written. The state is therefore
/// read before the upsert.
#[test]
fn a_sandbox_that_left_an_activatable_state_is_refused_before_adoption() {
    let conn = crate::db::test_conn();
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let sandbox_id = crate::db::tests::setup_quarantine_recovery_episode(&conn, reason_code);
    let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let claim = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("claiming a fresh episode should succeed");

    // A concurrent path fails the sandbox after the claim was taken.
    conn.execute(
        "UPDATE sandboxes SET state = 'failed' WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    let (mut meta, _) = disk_artifact_meta();
    meta.sandbox_id = sandbox_id.to_string();
    let restore_source = pplx_space_common_core::types::ExactStorageUri::parse(
        "s3://bucket/snapshots/orphan/vmstate?region=us-east-1",
    )
    .expect("the fixture URI should parse");
    let plan = crate::snapshot_restore::QuarantineRecoveryPlan {
        recovery_id: pplx_space_common_core::types::SandboxCleanupEpisodeId::new(),
        snapshot_id: SnapshotId::new(),
        storage_uri: "s3://bucket/snapshots/orphan/vmstate?region=us-east-1".to_string(),
        local_artifact_id: None,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap(),
        reason_code,
    };
    let recovery = super::QuarantineRecoveryContext {
        plan: &plan,
        claim: &claim,
    };

    let outcome = super::adopt_restoring_row_under_recovery_claim(
        &conn,
        sandbox_id,
        &sandbox_id.to_string(),
        super::RestoringRowInput::new(&meta, &restore_source, None),
        Some(&recovery),
        false,
    );
    assert!(
        matches!(
            outcome,
            Err(crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded { .. })
        ),
        "a non-activatable sandbox must be refused, got {outcome:?}"
    );
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "failed",
        "and the refusal must happen before anything rewrites the state back to \
         restoring, which is what made the CAS's own conjunct self-satisfying"
    );
}

/// The renewal budget must abort before the lease can lapse, whatever the
/// constants are set to.
///
/// The existing count assertion computes its expectation from the constant, so it
/// holds for any value — reverting the budget to 3 broke nothing. This asserts the
/// property the choice was made for, directly against the three constants: the
/// deciding strike lands at `budget × heartbeat`, the first tick is consumed at
/// t=0, and that has to leave at least one heartbeat of lease.
#[test]
fn the_renewal_budget_aborts_a_full_heartbeat_inside_the_lease() {
    let heartbeat = QUARANTINE_RECOVERY_CLAIM_HEARTBEAT.as_secs();
    let lease = crate::db::QUARANTINE_RECOVERY_CLAIM_LEASE.as_secs();
    let budget = u64::from(QUARANTINE_RECOVERY_RENEWAL_FAILURES_BEFORE_ABORT);
    let abort_at = budget * heartbeat;
    assert!(
        abort_at + heartbeat <= lease,
        "the deciding renewal strike lands at {abort_at}s (budget {budget} × \
         heartbeat {heartbeat}s) and must leave at least one heartbeat of the \
         {lease}s lease. Otherwise a late tick — the interval is Delay-behaved and \
         renewal is a blocking write on a shared mutex — decides after expiry, and \
         the operation keeps running with no lease at all, which is the overlap the \
         Ok(false) arm refuses to allow for even one heartbeat."
    );
}

/// A staging failure recorded under a superseded claim must not land.
///
/// Every caller of this helper runs after a transfer that can outlive the claim that
/// authorized it. The state guard alone does not protect the row, because a new owner
/// that reclaimed the episode has put it back in `restoring` — which is exactly the
/// state the unfenced statement matches on.
#[test]
fn a_superseded_claim_cannot_persist_a_canonical_publication_failure() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = crate::db::tests::setup_quarantine_recovery_candidate(&conn);
    let claimed = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("claiming a fresh episode should succeed");
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        rusqlite::params![sandbox_id.to_string()],
    )
    .unwrap();

    // Control: the live claim can record its own failure, so the refusal below is the
    // fence and not a statement that never matches.
    persist_cross_node_staging_failure(
        &conn,
        sandbox_id,
        "canonical-root publication",
        &crate::snapshot_restore::RestoreError::CanonicalRootConflict { sandbox_id },
        Some(&claimed),
    )
    .unwrap();
    let owned: Option<String> = conn
        .query_row(
            "SELECT failure_reason FROM sandboxes WHERE id = ?1",
            rusqlite::params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(owned.is_some(), "a live claim must record its own failure");

    // The episode moves to another claimant while the transfer is still running.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_token = ?1 WHERE sandbox_id = ?2",
        rusqlite::params![uuid::Uuid::now_v7().to_string(), sandbox_id.to_string()],
    )
    .unwrap();
    // A different stage, so the two writes are distinguishable. With the same stage the
    // text is byte-identical and the comparison below cannot tell a write that was
    // refused from a write that landed and was then reported as refused — which is what
    // a write-then-check implementation would do, and is the defect this fences.
    let error = persist_cross_node_staging_failure(
        &conn,
        sandbox_id,
        "artifact staging",
        &crate::snapshot_restore::RestoreError::CanonicalRootConflict { sandbox_id },
        Some(&claimed),
    )
    .expect_err("a superseded claim must not be able to record a failure");
    assert!(
        matches!(
            error,
            crate::snapshot_restore::RestoreError::RestorePhaseTransitionLost { .. }
        ),
        "the refusal must be reported as a lost transition, not as success: {error}"
    );
    let after: Option<String> = conn
        .query_row(
            "SELECT failure_reason FROM sandboxes WHERE id = ?1",
            rusqlite::params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        after, owned,
        "the superseded write must leave the reason the owning claim wrote"
    );
}

/// Every staging-failure write in this file has to be able to fence, and every caller
/// that holds a claim has to pass it.
///
/// The fenced variant was introduced for one of the two call sites and the other kept
/// the unfenced one — same function, same post-transfer window, one protected and one
/// not. Two invariants keep that from returning: the unfenced statement has exactly one
/// use, inside the helper's `None` arm, and no caller inside a function that has
/// `recovery` in scope passes a literal `None` for the claim. The second is the one
/// that matters now: after the refactor a new site cannot reach the unfenced statement
/// directly, but it can still hardcode `None` and be exactly as wrong.
#[test]
fn the_only_unfenced_staging_failure_write_is_the_no_claim_arm() {
    let source = include_str!("../recovery_cross_node.rs");
    let lines: Vec<&str> = source.lines().collect();
    let mut unfenced = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if !line.contains("mark_cross_node_restore_staging_failed(") {
            continue;
        }
        // The `None =>` arm selecting it is on the call's own line, or the line above
        // it when rustfmt has split the arm.
        let selected_by_no_claim = line.trim_start().starts_with("None =>")
            || (index > 0 && lines[index - 1].trim_start().starts_with("None =>"));
        if !selected_by_no_claim {
            unfenced.push(format!(
                "recovery_cross_node.rs:{}: {}",
                index + 1,
                line.trim()
            ));
        }
    }
    assert!(
        unfenced.is_empty(),
        "these use the unfenced staging-failure write outside the no-claim arm, so a \
         claim that moved during the transfer can still stamp a failure on the row its \
         new owner is restoring:\n{}",
        unfenced.join("\n")
    );

    // A call that hardcodes `None` inside a function that has a claim available is the
    // same defect reached without touching the unfenced statement at all.
    let mut claimless = Vec::new();
    let mut recovery_in_scope = false;
    for (index, line) in lines.iter().enumerate() {
        if let Some(name) = line
            .strip_prefix("    async fn ")
            .or(line.strip_prefix("    fn "))
        {
            let _ = name;
            recovery_in_scope = false;
        }
        if line.contains("recovery: Option<") || line.contains("recovery.map(|context|") {
            recovery_in_scope = true;
        }
        if !line.contains("persist_cross_node_staging_failure(") || line.starts_with("fn ") {
            continue;
        }
        // The claim is the last argument, within a few lines of the call.
        let passes_none = lines[index..(index + 8).min(lines.len())]
            .iter()
            .any(|argument| argument.trim() == "None,");
        if passes_none && recovery_in_scope {
            claimless.push(format!(
                "recovery_cross_node.rs:{}: passes None for the claim",
                index + 1
            ));
        }
    }
    assert!(
        claimless.is_empty(),
        "these call the staging-failure helper with no claim from a function that has \
         one, so the write is unfenced in exactly the window the fence exists for:\n{}",
        claimless.join("\n")
    );
}

// ---------------------------------------------------------------------------
// `run_cross_node_restore` refusal arms.
//
// Every case below is refused before the restore reads node paths or connects to
// volume-manager, node-manager or cluster-api. Each asserts the caller-visible
// outcome and what the refusal left in `sandboxes` and
// `sandbox_restore_transfer_scopes`. The fixtures also make the regression each
// test guards against stop at a database refusal, a live-runtime refusal or the
// staging-path string compare, so a broken guard fails the test without reaching
// the host either.
// ---------------------------------------------------------------------------

fn attempt(sequence: u8) -> pplx_space_common_core::types::SandboxActivationAttemptId {
    pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str(&format!("019c0000-0000-7000-8000-0000000000{sequence:02x}"))
            .unwrap(),
    )
}

fn generation(value: i64) -> pplx_space_common_core::types::ExecutionGeneration {
    pplx_space_common_core::types::ExecutionGeneration::new(value).unwrap()
}

/// A staging path that `remote_restore_metadata_staging_dir` never produces, so
/// scope cleanup refuses it before any volume-manager or node-manager call.
const UNOWNED_STAGING_DIR: &str = "/space/cache/restore-staging/not-owned-by-this-scope";

/// Persist the row an interrupted remote restore leaves behind: `restoring`,
/// remote origin, metadata staging, no runtime fields and no artifact.
fn insert_interrupted_remote_restore(conn: &rusqlite::Connection) -> (String, ExactStorageUri) {
    let sandbox_id = SandboxId::new().to_string();
    let (mut meta, _) = disk_artifact_meta();
    meta.sandbox_id = sandbox_id.clone();
    meta.local_artifact_id = None;
    meta.artifact_kind = None;
    meta.artifact_origin = None;
    meta.artifact_snapshot_name = None;
    let source = ExactStorageUri::parse(&format!(
        "s3://snapshots/suspend/{sandbox_id}/attempt-1?region=us-east-1"
    ))
    .unwrap();
    super::insert_restoring_sandbox_row_from_snapshot(conn, &sandbox_id, &meta, &source, None)
        .unwrap();
    let row = crate::db::get_sandbox(conn, &sandbox_id).unwrap().unwrap();
    assert!(
        crate::db::sandbox_is_retryable_interrupted_restore(&row),
        "fixture must be a retryable interrupted restore"
    );
    (sandbox_id, source)
}

fn transfer_scope(
    service: &SandboxManagerService,
    sandbox_id: &str,
) -> Option<crate::db::RestoreTransferScopeRow> {
    crate::db::restore_transfer_scope(&service.db.lock().unwrap(), sandbox_id).unwrap()
}

/// Every persisted column of a `sandboxes` row, so "untouched" means the whole
/// row and not only the fields a test thought to name.
fn sandbox_columns(
    conn: &rusqlite::Connection,
    sandbox_id: &str,
) -> Option<Vec<rusqlite::types::Value>> {
    use rusqlite::OptionalExtension as _;
    conn.query_row(
        "SELECT * FROM sandboxes WHERE id = ?1",
        [sandbox_id],
        |row| {
            (0..row.as_ref().column_count())
                .map(|index| row.get(index))
                .collect()
        },
    )
    .optional()
    .unwrap()
}

fn restore_error(
    result: Result<Option<CrossNodeRestoreProvenance>, crate::snapshot_restore::RestoreError>,
) -> crate::snapshot_restore::RestoreError {
    match result {
        Err(error) => error,
        Ok(provenance) => panic!(
            "expected a refusal, got Ok({:?})",
            provenance.map(|provenance| provenance.storage_uri)
        ),
    }
}

#[tokio::test]
async fn cross_node_restore_rejects_malformed_identity_and_source_before_any_write() {
    let service = service_with_runtime_probe(crate::db::test_conn(), false);
    let valid_id = SandboxId::new().to_string();

    let error = restore_error(
        service
            .run_cross_node_restore(CrossNodeRestoreRequest {
                sandbox_id: "not-a-sandbox-id",
                storage_uri: Some("s3://snapshots/suspend/x?region=us-east-1"),
                execution_generation: None,
                activation_attempt_id: None,
                recovery: None,
                restore_artifact_as_new: None,
                target: CrossNodeRestoreTarget::SuspendedCheckpoint,
                staging_admission: CrossNodeRestoreStagingAdmission::Foreground,
            })
            .await,
    );
    assert!(
        matches!(
            &error,
            crate::snapshot_restore::RestoreError::InvalidSandboxId { value, .. }
                if value == "not-a-sandbox-id"
        ),
        "got {error:?}"
    );
    assert!(transfer_scope(&service, "not-a-sandbox-id").is_none());

    let error = restore_error(
        service
            .run_cross_node_restore(CrossNodeRestoreRequest {
                sandbox_id: &valid_id,
                storage_uri: Some("not a storage uri"),
                execution_generation: None,
                activation_attempt_id: None,
                recovery: None,
                restore_artifact_as_new: None,
                target: CrossNodeRestoreTarget::SuspendedCheckpoint,
                staging_admission: CrossNodeRestoreStagingAdmission::Foreground,
            })
            .await,
    );
    assert!(
        matches!(
            &error,
            crate::snapshot_restore::RestoreError::InvalidRestoreSource { uri, .. }
                if uri == "not a storage uri"
        ),
        "got {error:?}"
    );
    assert!(
        transfer_scope(&service, &valid_id).is_none(),
        "a malformed source must be refused before a transfer scope is minted"
    );
    assert!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &valid_id)
            .unwrap()
            .is_none()
    );
}

/// Only a retryable interrupted remote restore may be restored over. Any other
/// existing row owns the sandbox, and the request is declined without touching
/// the row or its transfer scope.
///
/// Every row also carries a transfer scope owned by another attempt. Declining
/// never reads it. A regression that stopped declining would try to retire it as
/// a rowless scope, which refuses because the row exists, so that regression
/// fails here on a database refusal before any metadata pull.
#[tokio::test]
async fn cross_node_restore_declines_an_existing_row_that_is_not_an_interrupted_remote_restore() {
    let mut cases = Vec::new();
    // `Restoring` is excluded: whether it is declined depends on the row's restore
    // fields, which the table below varies one condition at a time.
    for state in SandboxState::all()
        .into_iter()
        .filter(|state| *state != SandboxState::Restoring)
    {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &sandbox_id, state);
        cases.push((state.to_string(), conn, sandbox_id));
    }
    // A `restoring` row that fails exactly one condition of a retryable interrupted
    // remote restore.
    for (name, update) in [
        (
            "restoring: local activation",
            "UPDATE sandboxes
                SET restore_origin = 'local', restore_phase = 'activation',
                    restore_storage_uri = NULL
              WHERE id = ?1",
        ),
        (
            "restoring: no restore operation",
            "UPDATE sandboxes
                SET restore_origin = NULL, restore_phase = NULL, restore_storage_uri = NULL
              WHERE id = ?1",
        ),
        (
            "restoring: runtime fields",
            "UPDATE sandboxes SET uid = 10000, slot = 0 WHERE id = ?1",
        ),
        (
            "restoring: local artifact",
            "UPDATE sandboxes
                SET current_local_artifact_id = '019c0000-0000-7000-8000-0000000000aa',
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?1",
        ),
        (
            "restoring: cleanup intent",
            "UPDATE sandboxes
                SET cleanup_completion = 'mark_suspended', cleanup_keep_filesystem = 1
              WHERE id = ?1",
        ),
    ] {
        let conn = crate::db::test_conn();
        let (sandbox_id, _) = insert_interrupted_remote_restore(&conn);
        assert_eq!(conn.execute(update, [&sandbox_id]).unwrap(), 1, "{name}");
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Restoring, "{name}");
        assert!(
            !crate::db::sandbox_is_retryable_interrupted_restore(&row),
            "{name}: fixture must not be a retryable interrupted restore"
        );
        cases.push((name.to_string(), conn, sandbox_id));
    }

    let mut mismatches = Vec::new();
    for (name, conn, sandbox_id) in cases {
        assert!(
            crate::db::insert_restore_transfer_scope(
                &conn,
                &sandbox_id,
                pplx_space_common_core::types::TransferScopeId::new(),
                Some(generation(7)),
                Some(attempt(2)),
                UNOWNED_STAGING_DIR,
            )
            .unwrap(),
            "{name}"
        );
        let row_before = sandbox_columns(&conn, &sandbox_id);
        let scope_before = crate::db::restore_transfer_scope(&conn, &sandbox_id).unwrap();
        let service = service_with_runtime_probe(conn, false);

        let outcome = service
            .run_cross_node_restore(CrossNodeRestoreRequest {
                sandbox_id: &sandbox_id,
                storage_uri: Some("s3://snapshots/suspend/x?region=us-east-1"),
                execution_generation: Some(generation(7)),
                activation_attempt_id: Some(attempt(3)),
                recovery: None,
                restore_artifact_as_new: None,
                target: CrossNodeRestoreTarget::SuspendedCheckpoint,
                staging_admission: CrossNodeRestoreStagingAdmission::Foreground,
            })
            .await;

        if !matches!(outcome, Ok(None)) {
            mismatches.push(format!("{name}: expected Ok(None), got {outcome:?}"));
        }
        let db = service.db.lock().unwrap();
        if sandbox_columns(&db, &sandbox_id) != row_before {
            mismatches.push(format!("{name}: the sandbox row changed"));
        }
        if crate::db::restore_transfer_scope(&db, &sandbox_id).unwrap() != scope_before {
            mismatches.push(format!("{name}: the transfer scope changed"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

/// A user-facing restore waits a bounded time for the per-sandbox staging lock,
/// then refuses as a retryable `Unavailable` rather than queueing indefinitely.
#[tokio::test(start_paused = true)]
async fn foreground_cross_node_restore_refuses_while_staging_is_held() {
    let service = service_with_runtime_probe(crate::db::test_conn(), false);
    let sandbox_id = SandboxId::new().to_string();
    let _held = Arc::clone(&service.auto_lifecycle.staging_lock(&sandbox_id))
        .lock_owned()
        .await;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3600),
        service.run_cross_node_restore(CrossNodeRestoreRequest {
            sandbox_id: &sandbox_id,
            storage_uri: Some("s3://snapshots/suspend/x?region=us-east-1"),
            execution_generation: Some(generation(7)),
            activation_attempt_id: Some(attempt(1)),
            recovery: None,
            restore_artifact_as_new: None,
            target: CrossNodeRestoreTarget::SuspendedCheckpoint,
            staging_admission: CrossNodeRestoreStagingAdmission::Foreground,
        }),
    )
    .await
    .expect("foreground admission must be bounded");

    let error = restore_error(result);
    let crate::snapshot_restore::RestoreError::AdmissionRejected(status) = &error else {
        panic!("expected an admission refusal, got {error:?}");
    };
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(
        status
            .message()
            .contains("activation admission is busy; retry"),
        "{status:?}"
    );
    assert!(transfer_scope(&service, &sandbox_id).is_none());
}

/// Ownership of a retained interrupted-restore transfer scope, at the service
/// layer.
///
/// The attempt-ordering CAS itself is pinned in `db/tests.rs`
/// (`retryable_restore_scope_handoff_rejects_a_after_b_and_admits_c`,
/// `authoritative_restore_attempt_claims_legacy_unowned_retryable_scope`); this
/// table adds generation ordering, the same-attempt resume, and that the takeover
/// is persisted before cleanup runs while the scope id and staging path are
/// retained. Cleanup here always refuses the unowned staging path before any
/// remote call, so a takeover surfaces as that refusal with the new owner
/// already recorded.
#[tokio::test]
async fn retryable_scope_takeover_requires_a_current_or_newer_attempt() {
    // (case, incoming generation and attempt, taken over); the scope is owned by
    // generation 7, attempt 2.
    let cases: [(&str, (i64, u8), bool); 4] = [
        ("older generation", (6, 3), false),
        ("same attempt", (7, 2), true),
        ("newer attempt", (7, 3), true),
        ("newer generation", (8, 1), true),
    ];
    for (name, (incoming_generation, incoming_attempt), taken_over) in cases {
        let conn = crate::db::test_conn();
        let (sandbox_id, _) = insert_interrupted_remote_restore(&conn);
        let scope_id = pplx_space_common_core::types::TransferScopeId::new();
        assert!(
            crate::db::insert_restore_transfer_scope(
                &conn,
                &sandbox_id,
                scope_id,
                Some(generation(7)),
                Some(attempt(2)),
                UNOWNED_STAGING_DIR,
            )
            .unwrap()
        );
        let service = service_with_runtime_probe(conn, false);
        let observed = transfer_scope(&service, &sandbox_id).unwrap();

        let result = service
            .claim_and_clean_retryable_restore_transfer_scope(
                &sandbox_id,
                &observed,
                generation(incoming_generation),
                attempt(incoming_attempt),
            )
            .await;

        let expected_owner = if taken_over {
            assert!(
                matches!(
                    &result,
                    Err(crate::snapshot_restore::RestoreError::Other(_))
                ),
                "{name}: got {result:?}"
            );
            (
                Some(generation(incoming_generation)),
                Some(attempt(incoming_attempt)),
            )
        } else {
            assert!(matches!(result, Ok(None)), "{name}: got {result:?}");
            (Some(generation(7)), Some(attempt(2)))
        };
        let scope = transfer_scope(&service, &sandbox_id).expect("scope is retained");
        assert_eq!(scope.transfer_scope_id, scope_id, "{name}");
        assert_eq!(scope.staging_dir, UNOWNED_STAGING_DIR, "{name}");
        assert_eq!(
            (scope.execution_generation, scope.activation_attempt_id),
            expected_owner,
            "{name}"
        );
        assert_eq!(
            crate::db::get_sandbox(&service.db.lock().unwrap(), &sandbox_id)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Restoring,
            "{name}"
        );
    }
}

/// The takeover CAS re-checks the row, so a row that gained runtime state after
/// the caller read the scope is not cleaned underneath it.
#[tokio::test]
async fn retryable_scope_takeover_refuses_a_row_that_gained_runtime_state() {
    let conn = crate::db::test_conn();
    let (sandbox_id, _) = insert_interrupted_remote_restore(&conn);
    let scope_id = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        crate::db::insert_restore_transfer_scope(
            &conn,
            &sandbox_id,
            scope_id,
            Some(generation(7)),
            Some(attempt(2)),
            UNOWNED_STAGING_DIR,
        )
        .unwrap()
    );
    let service = service_with_runtime_probe(conn, false);
    let observed = transfer_scope(&service, &sandbox_id).unwrap();
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET uid = 1000 WHERE id = ?1",
            [&sandbox_id],
        )
        .unwrap();

    let claimed = service
        .claim_and_clean_retryable_restore_transfer_scope(
            &sandbox_id,
            &observed,
            generation(7),
            attempt(3),
        )
        .await
        .unwrap();

    assert_eq!(claimed, None);
    let scope = transfer_scope(&service, &sandbox_id).unwrap();
    assert_eq!(scope.transfer_scope_id, scope_id);
    assert_eq!(scope.activation_attempt_id, Some(attempt(2)));
}

/// A rowless scope owned by another attempt must be retired before a new one is
/// minted. When its cleanup cannot be confirmed, the old scope stays as the
/// durable reference to the predecessor's staging and the restore is refused.
///
/// If the retire failure were swallowed, `insert_restore_transfer_scope`'s
/// `ON CONFLICT DO NOTHING` would still refuse to mint over the retained scope,
/// with the same outcome. That conflict guard, not the error propagation, is what
/// keeps the old scope in that case, so this test pins only that the reference
/// outlives a cleanup that did not happen.
#[tokio::test]
async fn rowless_restore_keeps_a_superseded_scope_it_cannot_retire() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    let scope_id = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        crate::db::insert_restore_transfer_scope(
            &conn,
            &sandbox_id,
            scope_id,
            Some(generation(7)),
            Some(attempt(2)),
            UNOWNED_STAGING_DIR,
        )
        .unwrap()
    );
    let scope_before = crate::db::restore_transfer_scope(&conn, &sandbox_id).unwrap();
    let service = service_with_runtime_probe(conn, false);

    let error = restore_error(
        service
            .run_cross_node_restore(CrossNodeRestoreRequest {
                sandbox_id: &sandbox_id,
                storage_uri: Some("s3://snapshots/suspend/x?region=us-east-1"),
                execution_generation: Some(generation(7)),
                activation_attempt_id: Some(attempt(3)),
                recovery: None,
                restore_artifact_as_new: None,
                target: CrossNodeRestoreTarget::SuspendedCheckpoint,
                staging_admission: CrossNodeRestoreStagingAdmission::Foreground,
            })
            .await,
    );

    assert!(
        matches!(&error, crate::snapshot_restore::RestoreError::Other(_)),
        "got {error:?}"
    );
    assert_eq!(transfer_scope(&service, &sandbox_id), scope_before);
    assert!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &sandbox_id)
            .unwrap()
            .is_none()
    );
}

fn claim_orphan_episode(
    conn: &rusqlite::Connection,
) -> (SandboxId, crate::db::ClaimedQuarantineRecoveryCandidate) {
    let sandbox_id = crate::db::tests::setup_quarantine_recovery_episode(
        conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    let candidate = crate::db::scan_quarantine_recovery_candidates(conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let claimed = crate::db::claim_quarantine_recovery_candidate(
        conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("the claim should succeed");
    (sandbox_id, claimed)
}

fn orphan_plan(
    reason_code: pplx_space_common_core::types::SandboxQuarantineReasonCode,
) -> crate::snapshot_restore::QuarantineRecoveryPlan {
    crate::snapshot_restore::QuarantineRecoveryPlan {
        recovery_id: pplx_space_common_core::types::SandboxCleanupEpisodeId::new(),
        snapshot_id: SnapshotId::new(),
        storage_uri: "s3://bucket/snapshots/orphan/vmstate?region=us-east-1".to_string(),
        local_artifact_id: None,
        execution_generation: generation(7),
        reason_code,
    }
}

/// Fails to compile when a reason code is added, until the sweep below lists it.
fn _all_reasons_listed(reason: pplx_space_common_core::types::SandboxQuarantineReasonCode) {
    use pplx_space_common_core::types::SandboxQuarantineReasonCode as Reason;
    match reason {
        Reason::Unspecified
        | Reason::RuntimeProbeInconsistent
        | Reason::SuspendSnapshotPublicationFailed
        | Reason::OrphanedSandboxRowMissing
        | Reason::CreateInterrupted
        | Reason::ColdActivationInterrupted
        | Reason::RestoreActivationInterrupted
        | Reason::ColdActivationPrepareFailed
        | Reason::ColdActivationMetadataFailed
        | Reason::ColdActivationEncryptedRootfsPrepareFailed
        | Reason::ColdActivationFirecrackerLaunchFailed
        | Reason::ColdActivationSidecarLaunchFailed
        | Reason::RestorePrepareFailed
        | Reason::RestoreMetadataFailed
        | Reason::RestoreEncryptedDevicesPrepareFailed
        | Reason::RestoreFirecrackerLaunchFailed
        | Reason::RestoreNetGatewayLaunchFailed
        | Reason::RestoreSnapshotLoadFailed
        | Reason::RestoreSpacedHostLaunchFailed
        | Reason::RestoreResumeFailed
        | Reason::PauseFailed
        | Reason::CheckpointResumeFailed
        | Reason::ResidentResumeFailed
        | Reason::SnapshotRollbackCleanupFailed
        | Reason::NodePublishedQuarantined
        | Reason::OrphanedExecutionMissingArtifact
        | Reason::OrphanedExecutionLegacyDeletingState => {}
    }
}

/// Only an orphaned-row plan may restore over a quarantined row; a plan for any
/// other reason is declined without staging, even when the local episode is an
/// orphan.
///
/// The service reports every runtime as live, which these declines never
/// consult. A regression that admitted the plan would stop at the live-runtime
/// refusal before probing the canonical cache root.
#[tokio::test]
async fn orphan_restore_declines_a_plan_for_a_different_quarantine_reason() {
    use pplx_space_common_core::types::SandboxQuarantineReasonCode as Reason;
    // Written out because the enum has no `all()`; `_all_reasons_listed` keeps
    // the list complete. `OrphanedSandboxRowMissing` is excluded because it is the
    // plan reason that proceeds.
    let reasons = [
        Reason::Unspecified,
        Reason::RuntimeProbeInconsistent,
        Reason::SuspendSnapshotPublicationFailed,
        Reason::CreateInterrupted,
        Reason::ColdActivationInterrupted,
        Reason::RestoreActivationInterrupted,
        Reason::ColdActivationPrepareFailed,
        Reason::ColdActivationMetadataFailed,
        Reason::ColdActivationEncryptedRootfsPrepareFailed,
        Reason::ColdActivationFirecrackerLaunchFailed,
        Reason::ColdActivationSidecarLaunchFailed,
        Reason::RestorePrepareFailed,
        Reason::RestoreMetadataFailed,
        Reason::RestoreEncryptedDevicesPrepareFailed,
        Reason::RestoreFirecrackerLaunchFailed,
        Reason::RestoreNetGatewayLaunchFailed,
        Reason::RestoreSnapshotLoadFailed,
        Reason::RestoreSpacedHostLaunchFailed,
        Reason::RestoreResumeFailed,
        Reason::PauseFailed,
        Reason::CheckpointResumeFailed,
        Reason::ResidentResumeFailed,
        Reason::SnapshotRollbackCleanupFailed,
        Reason::NodePublishedQuarantined,
        Reason::OrphanedExecutionMissingArtifact,
        Reason::OrphanedExecutionLegacyDeletingState,
    ];
    let mut mismatches = Vec::new();
    for reason in reasons {
        let conn = crate::db::test_conn();
        let (sandbox_id, claimed) = claim_orphan_episode(&conn);
        let sandbox_id = sandbox_id.to_string();
        let row_before = sandbox_columns(&conn, &sandbox_id);
        let service = service_with_runtime_probe(conn, true);

        let outcome = service
            .run_quarantined_orphan_restore(&claimed, &orphan_plan(reason))
            .await;

        if !matches!(outcome, Ok(None)) {
            mismatches.push(format!("{reason:?}: expected Ok(None), got {outcome:?}"));
        }
        let db = service.db.lock().unwrap();
        if sandbox_columns(&db, &sandbox_id) != row_before {
            mismatches.push(format!("{reason:?}: the sandbox row changed"));
        }
        if crate::db::restore_transfer_scope(&db, &sandbox_id)
            .unwrap()
            .is_some()
        {
            mismatches.push(format!("{reason:?}: a transfer scope was minted"));
        }
        if !crate::db::quarantine_recovery_claim_is_current(&db, &claimed).unwrap() {
            mismatches.push(format!("{reason:?}: the recovery claim was lost"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

/// A row that holds a local artifact is not an orphan, so the restore is declined
/// rather than staged over it. As above, a live runtime stops a regression before
/// the canonical cache root is probed.
#[tokio::test]
async fn orphan_restore_declines_a_row_that_now_holds_a_local_artifact() {
    let conn = crate::db::test_conn();
    let (sandbox_id, claimed) = claim_orphan_episode(&conn);
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?2",
        rusqlite::params![LocalArtifactId::new().to_string(), sandbox_id.to_string()],
    )
    .unwrap();
    let service = service_with_runtime_probe(conn, true);

    let outcome = service
        .run_quarantined_orphan_restore(
            &claimed,
            &orphan_plan(
                pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
            ),
        )
        .await
        .unwrap();

    assert!(outcome.is_none());
    assert!(transfer_scope(&service, &sandbox_id.to_string()).is_none());
}
