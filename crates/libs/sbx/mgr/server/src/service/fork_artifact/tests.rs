use pplx_space_common_core::types::LocalArtifactId;

use super::*;
use std::time::Duration;

#[tokio::test]
async fn cancelled_copy_attempt_removes_its_staging_directory() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("attempt");
    tokio::fs::create_dir_all(&path).await.unwrap();
    tokio::fs::write(path.join("rootfs.ext4"), b"partial")
        .await
        .unwrap();

    let attempt = CopyStagingAttempt::new(path.clone());
    drop(attempt);

    for _ in 0..100 {
        if !path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("cancelled copy staging was not removed");
}

#[tokio::test]
async fn fork_copy_transfer_deadline_bounds_a_stalled_rpc() {
    let result = bounded_transfer(
        std::future::pending::<Result<(), Status>>(),
        Duration::from_millis(10),
        "pull",
    )
    .await;

    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
}

fn suspended_metadata(sandbox_id: SandboxId) -> crate::snapshot_upload::SnapshotMeta {
    crate::snapshot_upload::SnapshotMeta {
        version: 1,
        execution: None,
        sandbox_id: sandbox_id.to_string(),
        size_bytes: 900,
        created_at: "2026-08-04T00:00:00Z".to_string(),
        source: crate::snapshot_upload::SnapshotSource {
            runtime: "fc".to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: "seed".to_string(),
            content_hash: Some("content-hash".to_string()),
            rootfs_size_mib: Some(1024),
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
        },
        sandbox: None,
        rootfs_diff_format: None,
        rootfs_send_source_id: None,
        encrypted: false,
        local_artifact_id: Some(LocalArtifactId::new()),
        artifact_kind: Some(ArtifactKind::Full),
        artifact_origin: Some(ArtifactOrigin::Suspend),
        artifact_snapshot_name: Some("s-donor".parse().expect("valid snapshot name")),
    }
}

#[test]
fn copied_metadata_keeps_only_portable_disk_identity() {
    let source_sandbox_id = SandboxId::new();
    let destination_sandbox_id = SandboxId::new();
    let projected = project_disk_metadata(
        suspended_metadata(source_sandbox_id),
        source_sandbox_id,
        destination_sandbox_id,
        42,
        ArtifactOrigin::Fork,
    )
    .expect("suspended disk is forkable");
    assert_eq!(projected.sandbox_id, destination_sandbox_id.to_string());
    assert_eq!(projected.size_bytes, 42);
    assert_eq!(projected.artifact_kind, Some(ArtifactKind::Disk));
    assert_eq!(projected.artifact_origin, Some(ArtifactOrigin::Fork));
    assert_eq!(projected.local_artifact_id, None);
    assert_eq!(projected.artifact_snapshot_name, None);
}

#[test]
fn projected_metadata_keeps_the_group_readable_mode_after_atomic_replace() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.json");
    write_projected_metadata(&path, b"{}").unwrap();
    let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, FORK_COPY_FILE_MODE);
}

#[test]
fn copied_metadata_rejects_encrypted_artifacts() {
    let source_sandbox_id = SandboxId::new();
    let mut metadata = suspended_metadata(source_sandbox_id);
    metadata.encrypted = true;
    let error = rootfs_artifact(&metadata)
        .expect_err("encrypted metadata must fail before the rootfs pull");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let error = project_disk_metadata(
        metadata,
        source_sandbox_id,
        SandboxId::new(),
        42,
        ArtifactOrigin::Fork,
    )
    .expect_err("encrypted artifact is not a disk-only fork source");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn copied_metadata_rejects_artifacts_without_a_declared_kind() {
    let source_sandbox_id = SandboxId::new();
    let mut metadata = suspended_metadata(source_sandbox_id);
    metadata.artifact_kind = None;

    let error =
        rootfs_artifact(&metadata).expect_err("untyped metadata must fail before the rootfs pull");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let error = project_disk_metadata(
        metadata,
        source_sandbox_id,
        SandboxId::new(),
        42,
        ArtifactOrigin::Fork,
    )
    .expect_err("untyped metadata is not a disk-only fork source");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn copied_metadata_preserves_btrfs_send_streams() {
    let source_sandbox_id = SandboxId::new();
    let mut metadata = suspended_metadata(source_sandbox_id);
    metadata.rootfs_diff_format =
        Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.to_string());
    let artifact = rootfs_artifact(&metadata).expect("btrfs metadata is supported");
    let projected = project_disk_metadata(
        metadata,
        source_sandbox_id,
        SandboxId::new(),
        42,
        ArtifactOrigin::Fork,
    )
    .expect("a btrfs send stream is a supported disk artifact");
    assert_eq!(artifact, SnapshotArtifact::RootfsBtrfsSend);
    assert_eq!(
        projected.rootfs_diff_format.as_deref(),
        Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1)
    );
}

#[test]
fn copy_staging_uses_the_shared_writable_sandbox_cache() {
    let operation_id = Uuid::parse_str("019fb707-99ea-7aa3-9430-6886f423bc13").unwrap();
    assert_eq!(
        copy_staging_dir(operation_id),
        std::path::Path::new(
            "/space/cache/firecracker/sandboxes/.fork-copies/019fb707-99ea-7aa3-9430-6886f423bc13"
        )
    );
}

#[tokio::test]
async fn retry_cleanup_removes_all_staged_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let staging = root.path().join("fork-copy");
    tokio::fs::create_dir_all(&staging).await.unwrap();
    tokio::fs::write(staging.join("rootfs"), b"artifact")
        .await
        .unwrap();
    remove_staging_dir(&staging).await.unwrap();
    assert!(!staging.exists());
    remove_staging_dir(&staging).await.unwrap();
}

#[test]
fn overlapping_retries_use_distinct_attempt_directories() {
    let operation_id = Uuid::now_v7();
    let first = copy_attempt_staging_dir(operation_id, Uuid::now_v7());
    let second = copy_attempt_staging_dir(operation_id, Uuid::now_v7());
    assert_ne!(first, second);
    assert_eq!(first.parent(), second.parent());
}

#[test]
fn active_copy_operation_is_visible_to_the_stale_sweeper() {
    let operations = Arc::new(CopyOperationReservations::default());
    let path = std::path::PathBuf::from("/tmp/fork-copy-operation");
    let guard = ActiveCopyOperation::register(Arc::clone(&operations), path.clone()).unwrap();
    assert!(is_copy_operation_active(&operations, &path));
    drop(guard);
    assert!(!is_copy_operation_active(&operations, &path));
}

#[test]
fn overlapping_copy_attempts_keep_the_operation_active_until_the_last_drop() {
    let operations = Arc::new(CopyOperationReservations::default());
    let path = std::path::PathBuf::from("/tmp/overlapping-fork-copy-operation");
    let first = ActiveCopyOperation::register(Arc::clone(&operations), path.clone()).unwrap();
    let second = ActiveCopyOperation::register(Arc::clone(&operations), path.clone()).unwrap();

    drop(first);
    assert!(is_copy_operation_active(&operations, &path));
    drop(second);
    assert!(!is_copy_operation_active(&operations, &path));
}

#[test]
fn stale_copy_delete_reservation_blocks_same_operation_retry() {
    let operations = Arc::new(CopyOperationReservations::default());
    let path = std::path::PathBuf::from("/tmp/retrying-fork-copy-operation");
    let delete_lease = CopyOperationDeleteLease::acquire(Arc::clone(&operations), path.clone())
        .expect("inactive operation should be reservable for deletion");

    assert!(
        ActiveCopyOperation::register(Arc::clone(&operations), path.clone()).is_none(),
        "a retry must not register while stale cleanup owns the operation key"
    );

    drop(delete_lease);
    assert!(
        ActiveCopyOperation::register(operations, path).is_some(),
        "a retry may register after stale cleanup releases the operation key"
    );
}

#[test]
fn active_copy_tracking_is_scoped_to_the_service_state() {
    let first_service = Arc::new(CopyOperationReservations::default());
    let second_service = Arc::new(CopyOperationReservations::default());
    let path = std::path::PathBuf::from("/tmp/service-owned-fork-copy-operation");
    let guard = ActiveCopyOperation::register(Arc::clone(&first_service), path.clone()).unwrap();

    assert!(is_copy_operation_active(&first_service, &path));
    assert!(!is_copy_operation_active(&second_service, &path));

    drop(guard);
}

#[test]
fn source_not_found_is_a_terminal_copy_error() {
    let error = map_pull_error(
        "metadata",
        Status::not_found("source object no longer exists"),
    );

    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn copy_admission_allows_only_one_in_flight_copy() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let waiting = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first = acquire_copy_permit(
        Arc::clone(&semaphore),
        Arc::clone(&waiting),
        Arc::clone(&active),
    )
    .await
    .unwrap();

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            acquire_copy_permit(
                Arc::clone(&semaphore),
                Arc::clone(&waiting),
                Arc::clone(&active),
            )
        )
        .await
        .is_err(),
        "a second copy must wait for the active copy"
    );

    drop(first);
    let _second = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        acquire_copy_permit(semaphore, waiting, active),
    )
    .await
    .expect("the queued copy should proceed after release")
    .unwrap();
}

#[test]
fn copy_admission_reserves_the_full_staged_artifact() {
    use pplx_space_common_core::host::disk::DiskStatus;
    use pplx_space_common_core::host::disk_admission::{
        CacheDiskAdmissionConfig, CacheDiskMeasurement,
    };

    let decision = copy_disk_admission_decision(
        CacheDiskAdmissionConfig {
            enabled: true,
            min_available_fraction: 0.20,
            warning_available_fraction: 0.30,
            min_available_bytes: 0,
            allocation_reservation_bytes: 10,
        },
        CacheDiskMeasurement {
            status: DiskStatus {
                total_bytes: 100,
                free_bytes: 25,
                available_bytes: 25,
            },
            btrfs: None,
        },
        10,
    );

    assert!(
        !decision.allows_new_allocation(),
        "staging must not consume bytes protected by the hard floor"
    );
}
