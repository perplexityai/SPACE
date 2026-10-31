use pplx_space_common_core::types::JobId;

use tonic::Status;

use super::backup_submission::{
    artifact_backup_execution, captured_suspend_snapshot_metadata, suspend_artifact_metadata,
    suspend_snapshot_metadata,
};
use super::{
    ArtifactBackupSubmissionError, OrgSnapshotConfigResolution, SuspendRequestContext,
    artifact_backup_format, artifact_backup_lookup_error, current_instance_backup_precondition,
    encrypted_snapshot_prepare_error_status, ensure_completed_suspend_retry_format,
    pause_reconciliation_outcome, require_artifact_backup_files, reupload_format_from_sidecar,
    teardown_artifact_backup_status,
};
use crate::auto_lifecycle::AutoLifecycleCoordinator;
use crate::guest_filesystem_flush::GuestSnapshotPreparationBudget;
use crate::service::SandboxManagerService;
use crate::service::suspend_snapshot_publication::publish_suspend_snapshot_dir;
use crate::service::suspend_snapshot_publication::write_full_suspend_artifact;
use crate::state::SandboxState;
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, HostCpu, LocalArtifactId, SandboxId, SandboxOwner, SandboxRuntimeLabel,
};
use pplx_space_events::SandboxEventTrigger;
use pplx_space_node_runtime_core::vm_state::VmState;
use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;
use std::collections::BTreeMap;
use tokio::sync::oneshot;

// Regression: a forwarded node-api caller (peer principal `space-nodeapi`) must
// not be able to set `authority=local`. `Local` pins a node-local, cluster-immune
// policy, so before this gate any node-api-allowlisted ServiceAccount could pin an
// allow-all policy that a later `authority=cluster` reconcile could never override.
#[tokio::test]
async fn set_network_policy_rejects_local_authority_from_forwarded_service_peer() {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::host::linux::identities::users;
    use pplx_space_common_svc::PeerIdentity;
    use pplx_space_sbx_mgr_client::proto::SetNetworkPolicyRequest;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
    let service = SandboxManagerService::for_test(conn);

    let allow_all = pplx_space_net_common::NetworkPolicyV1::allow_all();
    let make_request = |uid: u32, gid: u32| {
        let mut request = tonic::Request::new(SetNetworkPolicyRequest {
            sandbox_id: sandbox_id.clone(),
            policy: Some(pplx_space_net_common::proto::NetworkPolicyV1::from(
                &allow_all,
            )),
            inherit_template: false,
            policy_generation: 0,
            authority: pplx_space_net_common::proto::PolicyAuthority::Local as i32,
            // Keep the update on the async path: this test exercises the peer
            // gate, not a gateway round-trip.
            sync_to_gateway: false,
        });
        request
            .extensions_mut()
            .insert(PeerIdentity::new(uid, gid, None));
        request
    };

    // Forwarded node-api service peer: refused before any DB mutation.
    let error = service
        .set_network_policy(make_request(
            users::SPACE_NODEAPI.uid,
            users::SPACE_NODEAPI.primary_group.gid,
        ))
        .await
        .expect_err("authority=local must be refused for a forwarded service peer");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(
        error.message().contains("authority=local"),
        "unexpected message: {}",
        error.message()
    );
    let row = service.lookup_sandbox(&sandbox_id).unwrap();
    assert!(
        row.network_policy_override.is_none(),
        "refused request must not mutate desired policy"
    );
    assert_eq!(
        row.network_policy_authority,
        crate::db::PolicyAuthority::Cluster
    );

    // On-node operator (root, uid 0): may set administrator-owned policy.
    service
        .set_network_policy(make_request(0, 0))
        .await
        .expect("root operator may set authority=local");
    let row = service.lookup_sandbox(&sandbox_id).unwrap();
    assert_eq!(
        row.network_policy_authority,
        crate::db::PolicyAuthority::Local
    );
    assert!(row.network_policy_override.is_some());
}

// Auto-lifecycle carries the same cluster-immune `authority=local` semantics as
// network policy, so a forwarded service peer must not be able to set it either.
#[tokio::test]
async fn set_auto_lifecycle_policy_rejects_local_authority_from_forwarded_service_peer() {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::host::linux::identities::users;
    use pplx_space_common_svc::PeerIdentity;
    use pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyRequest;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
    let service = SandboxManagerService::for_test(conn);

    let make_request = |uid: u32, gid: u32| {
        let mut request = tonic::Request::new(SetAutoLifecyclePolicyRequest {
            sandbox_id: sandbox_id.clone(),
            policy: Some(crate::auto_lifecycle::policy_to_proto(
                &AutoLifecyclePolicy::disabled(),
            )),
            authority: pplx_space_net_common::proto::PolicyAuthority::Local as i32,
        });
        request
            .extensions_mut()
            .insert(PeerIdentity::new(uid, gid, None));
        request
    };

    // Forwarded node-api service peer: refused before any DB mutation.
    let error = service
        .set_auto_lifecycle_policy(make_request(
            users::SPACE_NODEAPI.uid,
            users::SPACE_NODEAPI.primary_group.gid,
        ))
        .await
        .expect_err("authority=local must be refused for a forwarded service peer");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(
        error.message().contains("authority=local"),
        "unexpected message: {}",
        error.message()
    );
    let row = service.lookup_sandbox(&sandbox_id).unwrap();
    assert_eq!(
        row.auto_lifecycle_authority,
        crate::db::PolicyAuthority::Cluster
    );

    // On-node operator (root, uid 0): may set administrator-owned policy.
    service
        .set_auto_lifecycle_policy(make_request(0, 0))
        .await
        .expect("root operator may set authority=local");
    let row = service.lookup_sandbox(&sandbox_id).unwrap();
    assert_eq!(
        row.auto_lifecycle_authority,
        crate::db::PolicyAuthority::Local
    );
}

#[tokio::test]
async fn template_notification_matches_inherited_sandboxes_of_both_authorities() {
    use crate::service::activation_support::internal_request;
    use crate::service::tests::service_tests::insert_test_sandbox;
    let conn = crate::db::test_conn();
    let override_json =
        serde_json::to_string(&pplx_space_net_common::NetworkPolicy::default()).unwrap();
    for authority in ["cluster", "local"] {
        for (template, pool, override_policy) in [
            ("seed", None, None),
            ("other", None, None),
            ("seed", Some("seed"), None),
            ("seed", None, Some(override_json.as_str())),
        ] {
            let id = uuid::Uuid::now_v7().to_string();
            insert_test_sandbox(&conn, &id, crate::state::SandboxState::Stopped);
            conn.execute(
                "UPDATE sandboxes SET template = ?1, pool_template = ?2,
                    network_policy_override_json = ?3, network_policy_authority = ?4 WHERE id = ?5",
                crate::db::fixture::params![template, pool, override_policy, authority, id],
            )
            .unwrap();
        }
    }
    let service = crate::service::SandboxManagerService::for_test(conn);
    let response = service
        .template_network_policy_updated(internal_request(
            crate::proto::TemplateNetworkPolicyUpdatedRequest {
                template: "seed".into(),
                policy: Some(crate::service::network_policy::network_policy_to_proto(
                    &pplx_space_net_common::NetworkPolicy::default(),
                )),
                authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
                generation: 1,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.matched_sandboxes, 2);
    assert_eq!(response.notified_gateways, 0);
}

#[tokio::test(start_paused = true)]
async fn artifact_alias_waits_on_canonical_lock_and_rereads_after_admission() {
    use crate::service::tests::service_tests::{insert_test_sandbox, service_for_teardown_test};
    let id = "01939e3e-1111-7000-8000-000000000001";
    let db = crate::db::test_conn();
    insert_test_sandbox(&db, id, SandboxState::Suspended);
    let service = service_for_teardown_test(db);
    let lock = service.auto_lifecycle.artifact_lock(id).lock_owned().await;
    let pending = service.lock_artifact_for_sandbox("01939e3e1111");
    tokio::pin!(pending);
    assert!(futures_util::poll!(&mut pending).is_pending());
    crate::db::test_fixtures::state_write_fixtures::apply_edge(
        &service.db_conn().unwrap(),
        id,
        &crate::db::edges::AUTO_LIFECYCLE_STOP_SUSPENDED,
    );
    drop(lock);
    let (row, guard) = pending.await.unwrap();
    assert_eq!(row.id, id);
    assert_eq!(row.state, SandboxState::Stopped);
    service
        .auto_lifecycle
        .artifact_lock(id)
        .try_lock()
        .unwrap_err();
    let label = id.parse::<SandboxId>().unwrap().label().to_string();
    let blocked = service.lock_artifact_for_sandbox(&label);
    tokio::pin!(blocked);
    assert!(futures_util::poll!(&mut blocked).is_pending());
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert_eq!(
        blocked.await.err().expect("bounded admission").code(),
        tonic::Code::Unavailable
    );
    drop(guard);
}

#[tokio::test]
async fn staging_guard_preserves_files_without_confirmed_remote_cleanup() {
    enum Finish {
        UnconfirmedCleanup,
        Preserve,
        Abandon,
    }
    for finish in [
        Finish::UnconfirmedCleanup,
        Finish::Preserve,
        Finish::Abandon,
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("staging");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("memory"), b"capture in progress").unwrap();
        let mut guard = super::LocalArtifactStagingGuard::new(path.clone());
        match finish {
            Finish::UnconfirmedCleanup => {
                let channel = pplx_space_common_grpc::connect_uds_channel_lazy(
                    &root.path().join("absent-node-manager.sock"),
                    tonic::transport::Endpoint::from_static("http://[::]:50051")
                        .timeout(std::time::Duration::from_secs(1)),
                );
                let mut client = pplx_space_node_mgr_client::NodeManagerGrpcClient::new(
                    pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
                );
                guard.cleanup(&mut client).await;
            }
            Finish::Preserve | Finish::Abandon => {
                if matches!(finish, Finish::Preserve) {
                    guard.disarm();
                }
                drop(guard);
            }
        }
        assert_eq!(
            std::fs::read(path.join("memory")).unwrap(),
            b"capture in progress"
        );
    }
}

#[test]
fn encrypted_snapshot_lifecycle_prepare_preserves_invalid_key_marker() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE
            .parse()
            .unwrap(),
    );
    let error = crate::snapshot_encryption::EncryptedSnapshotPrepareError::Rpc(
        tonic::Status::with_metadata(
            tonic::Code::FailedPrecondition,
            "provider rejected lifecycle key",
            metadata,
        ),
    );

    let status = encrypted_snapshot_prepare_error_status(error);

    assert!(pplx_space_credmgr_client::status_is_invalid_key_reference(
        &status
    ));
    assert!(status.message().contains("provider rejected lifecycle key"));
}

#[test]
fn completed_suspend_retry_respects_an_explicit_format_contract() {
    for recorded in [
        SuspendSnapshotFormat::Full,
        SuspendSnapshotFormat::BtrfsSend,
    ] {
        assert!(
            ensure_completed_suspend_retry_format(SuspendSnapshotFormat::Unspecified, recorded)
                .is_ok(),
            "an unspecified retry must reuse the already-committed artifact",
        );
        ensure_completed_suspend_retry_format(recorded, recorded).expect("matching retry format");
    }

    let error = ensure_completed_suspend_retry_format(
        SuspendSnapshotFormat::BtrfsSend,
        SuspendSnapshotFormat::Full,
    )
    .expect_err("an explicit btrfs-send retry must reject a committed full artifact");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let error = ensure_completed_suspend_retry_format(
        SuspendSnapshotFormat::Full,
        SuspendSnapshotFormat::BtrfsSend,
    )
    .expect_err("an explicit full retry must reject a committed btrfs-send artifact");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn only_auto_lifecycle_suspend_uses_extended_guest_preparation_budget() {
    let context =
        |trigger| SuspendRequestContext::new("sandbox".to_string(), true, 0, true, trigger);

    assert_eq!(
        context(Some(SandboxEventTrigger::AutoLifecycle)).guest_preparation_budget(),
        GuestSnapshotPreparationBudget::AutoLifecycle
    );
    assert_eq!(
        context(Some(SandboxEventTrigger::System)).guest_preparation_budget(),
        GuestSnapshotPreparationBudget::Standard
    );
    assert_eq!(
        context(Some(SandboxEventTrigger::User)).guest_preparation_budget(),
        GuestSnapshotPreparationBudget::Standard
    );
    assert_eq!(
        context(None).guest_preparation_budget(),
        GuestSnapshotPreparationBudget::Standard
    );
}

#[test]
fn active_artifact_backup_jobs_reuse_the_persisted_attempt() {
    use pplx_space_common_jobs::JobStatus;
    let payload = serde_json::json!({"snapshot_id": "attempt-1"}).to_string();
    for status in [
        JobStatus::Pending,
        JobStatus::Running,
        JobStatus::Cancelling,
        JobStatus::Completed,
        JobStatus::Failed,
        JobStatus::Cancelled,
    ] {
        for has_failure in [false, true] {
            let reusable =
                !has_failure && matches!(status, JobStatus::Pending | JobStatus::Running);
            assert_eq!(
                SandboxManagerService::reusable_artifact_backup_attempt(
                    status,
                    has_failure,
                    &payload
                )
                .unwrap(),
                reusable.then(|| "attempt-1".to_string()),
                "{status:?}, has_failure={has_failure}"
            );
        }
    }
}

#[test]
fn current_instance_backup_preserves_scoped_drain_signals() {
    use pplx_space_common_core::artifact_backup_signals::{
        CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER,
        CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER,
    };
    use pplx_space_common_core::node_drain_signals::{
        REUPLOAD_SANDBOX_RESTORING_MARKER, REUPLOAD_SANDBOX_UNKNOWN_MARKER,
    };
    use pplx_space_common_core::types::ArtifactKind;
    for kind in [ArtifactKind::Disk, ArtifactKind::Full] {
        for state in SandboxState::all() {
            let result = current_instance_backup_precondition(state, kind);
            let valid = matches!(
                (state, kind),
                (SandboxState::Stopped, ArtifactKind::Disk)
                    | (SandboxState::Suspended, ArtifactKind::Full)
            );
            assert_eq!(result.is_ok(), valid, "{state:?}/{kind:?}");
            if let Err(error) = result {
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                for (marker, expected) in [
                    (
                        REUPLOAD_SANDBOX_RESTORING_MARKER,
                        state == SandboxState::Restoring && kind == ArtifactKind::Full,
                    ),
                    (
                        CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER,
                        state == SandboxState::Failed,
                    ),
                    (
                        CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER,
                        state == SandboxState::Suspended && kind == ArtifactKind::Disk,
                    ),
                ] {
                    assert_eq!(
                        error.message().contains(marker),
                        expected,
                        "{state:?}/{kind:?}"
                    );
                }
            }
        }
    }
    for full_current in [false, true] {
        for code in [
            tonic::Code::NotFound,
            tonic::Code::Unavailable,
            tonic::Code::Internal,
        ] {
            let status =
                artifact_backup_lookup_error(tonic::Status::new(code, "lookup"), full_current);
            let terminal = full_current && code == tonic::Code::NotFound;
            assert_eq!(
                status.message().contains(REUPLOAD_SANDBOX_UNKNOWN_MARKER),
                terminal
            );
            assert_eq!(
                status.code(),
                if terminal {
                    tonic::Code::FailedPrecondition
                } else {
                    code
                }
            );
        }
    }
}

#[tokio::test]
async fn artifact_backup_file_proof_distinguishes_absence_from_io_failure() {
    use pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER;
    use pplx_space_common_core::types::ArtifactKind;
    let inspector = crate::retained_source::FakeInspector::new(
        crate::retained_source::RetainedSourceCheck::Absent,
    );
    let sandbox_id = "01900000-0000-7000-8000-000000000001";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_str().unwrap();
    let format = crate::proto::SuspendSnapshotFormat::Full;
    let files = crate::snapshot_upload::artifact_files_for_kind(ArtifactKind::Full, format);
    for file in &files {
        std::fs::write(directory.path().join(file.local_name), b"captured").unwrap();
    }
    require_artifact_backup_files(
        &inspector,
        sandbox_id,
        path,
        ArtifactKind::Full,
        format,
        super::artifact_backup::BackupFilesPolicy::Backup {
            full_current_instance: true,
        },
    )
    .await
    .unwrap();
    let first = directory.path().join(files[0].local_name);
    std::fs::remove_file(&first).unwrap();
    for full_current in [false, true] {
        let missing = require_artifact_backup_files(
            &inspector,
            sandbox_id,
            path,
            ArtifactKind::Full,
            format,
            super::artifact_backup::BackupFilesPolicy::Backup {
                full_current_instance: full_current,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            missing.message().contains(REUPLOAD_ARTIFACT_MISSING_MARKER),
            full_current
        );
    }
    // A symlink loop produces an actual stat error, independent of permissions
    // or whether the test runs as root. It must never be mistaken for absence.
    std::os::unix::fs::symlink(&first, &first).unwrap();
    let inaccessible = require_artifact_backup_files(
        &inspector,
        sandbox_id,
        path,
        ArtifactKind::Full,
        format,
        super::artifact_backup::BackupFilesPolicy::Backup {
            full_current_instance: true,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(inaccessible.code(), tonic::Code::Internal);
    assert!(
        !inaccessible
            .message()
            .contains(REUPLOAD_ARTIFACT_MISSING_MARKER)
    );
}

#[test]
fn disk_artifact_backup_uses_the_codec_witnessed_by_its_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let artifact_dir = tmp.path().join("artifact");
    std::fs::create_dir(&artifact_dir).unwrap();
    std::fs::write(
        artifact_dir.join(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME),
        b"btrfs-send-stream",
    )
    .unwrap();
    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: LocalArtifactId::new(),
        sandbox_id: "01900000-0000-7000-8000-000000000001".to_string(),
        artifact_kind: pplx_space_common_core::types::ArtifactKind::Disk,
        artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Snapshot,
        snapshot_name: None,
        local_path: artifact_dir.to_string_lossy().into_owned(),
    };

    assert_eq!(
        artifact_backup_format(&artifact).unwrap(),
        SuspendSnapshotFormat::BtrfsSend,
        "a disk artifact containing rootfs.btrfs-send must not be uploaded as rootfs.ext4",
    );
    std::fs::remove_file(artifact_dir.join(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME))
        .unwrap();
    // A bundle left by an earlier binary still witnesses the codec. Its entry
    // is enough; nothing inside it is read.
    {
        use std::os::unix::fs::PermissionsExt;
        let bundle = artifact_dir.join(crate::retained_source::BUNDLE_DIRECTORY);
        std::fs::create_dir(&bundle).unwrap();
        std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            artifact_backup_format(&artifact).unwrap(),
            SuspendSnapshotFormat::BtrfsSend
        );
        std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(0o750)).unwrap();
    }
}

#[tokio::test]
async fn encrypted_backup_admission_requires_the_published_container_files() {
    use super::artifact_backup::BackupFilesPolicy;
    use pplx_space_common_core::types::ArtifactKind;

    let sandbox_id = "01900000-0000-7000-8000-000000000001";
    for format in [
        SuspendSnapshotFormat::Full,
        SuspendSnapshotFormat::BtrfsSend,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let metadata = serde_json::json!({
            "version": 1,
            "sandbox_id": sandbox_id,
            "size_bytes": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "source": {"runtime": "fc", "template_id": "seed"},
            "encrypted": true,
        });
        let sidecar = directory.path().join("snapshot.json");
        std::fs::write(&sidecar, serde_json::to_vec(&metadata).unwrap()).unwrap();
        // Encrypted capture publishes these two containers, with no plaintext
        // memory, vmstate, rootfs file, or retained btrfs-send source.
        for file in ["snapshot.luks", "cow.luks"] {
            std::fs::write(directory.path().join(file), b"encrypted capture").unwrap();
        }
        let inspector = crate::retained_source::FakeInspector::new(
            crate::retained_source::RetainedSourceCheck::Absent,
        );
        for policy in [
            BackupFilesPolicy::Backup {
                full_current_instance: false,
            },
            BackupFilesPolicy::Backup {
                full_current_instance: true,
            },
            BackupFilesPolicy::Reupload,
        ] {
            require_artifact_backup_files(
                &inspector,
                sandbox_id,
                path,
                ArtifactKind::Full,
                format,
                policy,
            )
            .await
            .unwrap();
            for file in ["snapshot.luks", "cow.luks"] {
                let file_path = directory.path().join(file);
                std::fs::remove_file(&file_path).unwrap();
                let error = require_artifact_backup_files(
                    &inspector,
                    sandbox_id,
                    path,
                    ArtifactKind::Full,
                    format,
                    policy,
                )
                .await
                .unwrap_err();
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert!(error.message().contains(file));
                std::fs::write(file_path, b"encrypted capture").unwrap();
            }
        }
        assert!(inspector.calls().is_empty());

        for bytes in [b"{\"encrypted\":".as_slice(), b"{\"encrypted\":\"true\"}"] {
            std::fs::write(&sidecar, bytes).unwrap();
            let error = require_artifact_backup_files(
                &inspector,
                sandbox_id,
                path,
                ArtifactKind::Full,
                format,
                BackupFilesPolicy::Backup {
                    full_current_instance: false,
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
        }
    }
}

#[tokio::test]
async fn retained_source_backup_precondition_follows_node_manager() {
    use crate::retained_source::{FakeInspector, RetainedSourceCheck};
    use pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER;
    use pplx_space_common_core::types::ArtifactKind;
    let sandbox_id = "01900000-0000-7000-8000-000000000001";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_str().unwrap();
    std::fs::write(directory.path().join("vmstate"), b"state").unwrap();
    std::fs::write(directory.path().join("memory"), b"memory").unwrap();
    let source_id = uuid::Uuid::now_v7();
    let meta = serde_json::json!({
        "version": crate::snapshot_upload::SNAPSHOT_METADATA_VERSION,
        "sandbox_id": sandbox_id,
        "size_bytes": 12,
        "created_at": "2026-05-25T00:00:00Z",
        "source": {"runtime": "fc", "template_id": "seed"},
        "rootfs_diff_format": crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1,
        "rootfs_send_source_id": source_id,
    });
    std::fs::write(
        directory
            .path()
            .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    let format = SuspendSnapshotFormat::BtrfsSend;
    let inspector = FakeInspector::new(RetainedSourceCheck::Present { rootfs_bytes: 1 });
    let require = |full_current| {
        require_artifact_backup_files(
            &inspector,
            sandbox_id,
            path,
            ArtifactKind::Full,
            format,
            super::artifact_backup::BackupFilesPolicy::Backup {
                full_current_instance: full_current,
            },
        )
    };

    require(true).await.unwrap();
    let call = inspector.calls().pop().unwrap();
    assert_eq!(call.source_id, source_id);
    assert_eq!(call.artifact_dir, directory.path());

    for check in [
        RetainedSourceCheck::Absent,
        RetainedSourceCheck::Invalid {
            reason: "lineage changed".to_owned(),
        },
    ] {
        inspector.answer(check);
        let terminal = require(true).await.unwrap_err();
        assert_eq!(terminal.code(), tonic::Code::FailedPrecondition);
        assert!(
            terminal
                .message()
                .contains(REUPLOAD_ARTIFACT_MISSING_MARKER)
        );
    }
    inspector.answer(RetainedSourceCheck::Unavailable {
        reason: "unimplemented".to_owned(),
    });
    let retry = require(true).await.unwrap_err();
    assert_eq!(retry.code(), tonic::Code::Internal);
    assert!(!retry.message().contains(REUPLOAD_ARTIFACT_MISSING_MARKER));
}

fn insert_suspended_test_sandbox(conn: &crate::db::Connection, sandbox_id: &str) {
    let uuid8 = SandboxRuntimeLabel::parse("sandbox1").unwrap();
    let owner = SandboxOwner::parse("user").unwrap();
    let process_env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: sandbox_id,
            uuid8: &uuid8,
            state: SandboxState::Suspended,
            template: "seed",
            template_hash: Some("hash"),
            user: &owner,
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &process_env,
            auto_lifecycle: &auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
}

/// A suspend this host captured records this host's CPU; metadata rebuilt for
/// an artifact whose sidecar is missing records none, because the host
/// rebuilding it may not be the one that captured it. Non-Linux hosts report
/// no CPU, so the first comparison is only meaningful on Linux, where an
/// unidentified CPU fails the test rather than letting it compare `None`
/// with `None`.
#[test]
fn only_a_captured_suspend_records_this_hosts_cpu() {
    #[cfg(target_os = "linux")]
    assert!(
        HostCpu::current().is_some(),
        "this Linux host's CPU is unidentified"
    );
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();

    for format in [
        SuspendSnapshotFormat::Full,
        SuspendSnapshotFormat::BtrfsSend,
    ] {
        let host_cpu = |metadata: crate::snapshot_upload::SnapshotMeta| {
            metadata
                .execution
                .and_then(|execution| execution.host_cpu().cloned())
        };
        assert_eq!(
            host_cpu(captured_suspend_snapshot_metadata(&row, format).unwrap()),
            HostCpu::current().cloned(),
            "{format:?}"
        );
        for encrypted in [false, true] {
            assert_eq!(
                host_cpu(
                    suspend_artifact_metadata(
                        &row,
                        format,
                        LocalArtifactId::new(),
                        encrypted,
                        None
                    )
                    .unwrap()
                ),
                HostCpu::current().cloned(),
                "{format:?}, encrypted: {encrypted}"
            );
        }
        assert_eq!(
            host_cpu(suspend_snapshot_metadata(&row, format).unwrap()),
            None,
            "{format:?}"
        );
    }
}

/// A disk artifact's backup reaches the cluster API without a capture CPU,
/// whatever its metadata says; a full artifact keeps the CPU it recorded.
#[test]
fn artifact_backup_execution_keeps_a_capture_cpu_only_for_full_artifacts() {
    use pplx_space_common_core::types::{
        Arch, ArtifactKind, ExecutionMetadata, SnapshotExecutionMetadata,
    };

    let host_cpu = HostCpu::parse("genuineintel-6-85-7").unwrap();
    let execution = SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
        Arch::X86_64,
        vec![],
    ))
    .unwrap()
    .with_host_cpu(Some(host_cpu.clone()));

    assert_eq!(
        artifact_backup_execution(execution.clone(), ArtifactKind::Full).host_cpu(),
        Some(&host_cpu)
    );
    assert_eq!(
        artifact_backup_execution(execution, ArtifactKind::Disk).host_cpu(),
        None
    );
}

#[test]
fn current_instance_handler_refuses_incomplete_suspend_without_publishing_a_pin() {
    use pplx_space_common_core::node_drain_signals::{
        REUPLOAD_ARTIFACT_MISSING_MARKER, REUPLOAD_SANDBOX_RESTORING_MARKER,
        REUPLOAD_SANDBOX_UNKNOWN_MARKER,
    };

    {
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> =
            ["SPACE_INTERNAL_API_ENDPOINT", "SPACE_INTERNAL_TOKEN"]
                .iter()
                .map(|key| (*key, None))
                .collect();
        vars.extend([
            (
                "SPACE_INTERNAL_API_ENDPOINT",
                Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
            ),
            (
                "SPACE_INTERNAL_TOKEN",
                Some(std::ffi::OsStr::new("backup-handler-test")),
            ),
        ]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                for (case, marker) in [
                    ("missing", REUPLOAD_ARTIFACT_MISSING_MARKER),
                    ("restoring", REUPLOAD_SANDBOX_RESTORING_MARKER),
                    ("absent", REUPLOAD_SANDBOX_UNKNOWN_MARKER),
                    ("unsupported", "snapshot restore requires file-backed memory; set hugepages = false"),
                ] {
                    let conn = crate::db::test_conn();
                    let id = SandboxId::new().to_string();
                    let cache_path = std::path::Path::new(super::CACHE_DIR).join(&id);
                    assert!(!cache_path.try_exists().unwrap());
                    if case != "absent" {
                        insert_suspended_test_sandbox(&conn, &id);
                    }
                    if case == "restoring" {
                        conn.execute("UPDATE sandboxes SET state = 'restoring' WHERE id = ?1", [&id])
                            .unwrap();
                    }
                    let mut service = SandboxManagerService::for_test(conn);
                    service.config.hugepages = case == "unsupported";
                    let error = service
                        .enqueue_artifact_backup(crate::service::activation_support::internal_request(
                            crate::proto::EnqueueArtifactBackupRequest {
                                sandbox_id: id.clone(),
                                expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                                source: Some(crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(
                                    crate::proto::CurrentInstanceSource {},
                                )),
                            },
                        ))
                        .await
                        .unwrap_err();
                    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{case}: {error}");
                    assert!(error.message().contains(marker), "{case}: {error}");
                    let conn = service.db_conn().unwrap();
                    if let Some(row) = crate::db::get_sandbox(&conn, &id).unwrap() {
                        assert!(row.current_local_artifact_id.is_none(), "{case}");
                    }
                    assert!(!cache_path.try_exists().unwrap());
                }
            });
    };
}

/// Sidecar absent and no rootfs artifact on disk → `Ok(Full)`.
/// Pre-codec snapshots wrote no sidecar; the historical full-rootfs
/// shape is the safe fallback when the artifacts don't witness a
/// codec either.
#[test]
fn reupload_sidecar_missing_resolves_to_full() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let format = reupload_format_from_sidecar(snapshot_dir.to_str().unwrap())
        .expect("missing sidecar must resolve to Full, not error");
    assert_eq!(format, SuspendSnapshotFormat::Full);
}

/// Sidecar absent with `rootfs.ext4` on disk → `Ok(Full)`: the
/// historical artifact is the witness, and it takes precedence over
/// the btrfs-send probe.
#[test]
fn reupload_sidecar_missing_with_ext4_artifact_resolves_to_full() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(snapshot_dir.join("rootfs.ext4"), b"ext4").unwrap();
    let format = reupload_format_from_sidecar(snapshot_dir.to_str().unwrap())
        .expect("sidecar-less full snapshot must resolve, not error");
    assert_eq!(format, SuspendSnapshotFormat::Full);
}

/// Regression: a sidecar-less snapshot whose rootfs artifact is
/// `rootfs.btrfs-send` must resolve to `BtrfsSend`, not `Full`.
/// The first days of the btrfs-send rollout suspended sandboxes
/// before suspend wrote a local `snapshot.json`, so "no sidecar"
/// does not imply the historical full-rootfs shape. Falling back
/// to `Full` makes the caller's artifact-presence check probe
/// `rootfs.ext4` (which never existed for this codec) and return
/// the terminal `REUPLOAD_ARTIFACT_MISSING_MARKER` for a complete,
/// healthy snapshot: the eviction legacy-repair lane re-skips the
/// row forever, and the drain promoter marks the sandbox failed.
#[test]
fn reupload_sidecar_missing_with_btrfs_send_artifact_resolves_to_btrfs_send() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(snapshot_dir.join("vmstate"), b"v").unwrap();
    std::fs::write(snapshot_dir.join("memory"), b"m").unwrap();
    std::fs::write(
        snapshot_dir.join(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME),
        b"btrfs-send-stream",
    )
    .unwrap();
    let format = reupload_format_from_sidecar(snapshot_dir.to_str().unwrap())
        .expect("sidecar-less btrfs-send snapshot must resolve, not error");
    assert_eq!(
        format,
        SuspendSnapshotFormat::BtrfsSend,
        "a sidecar-less snapshot with rootfs.btrfs-send on disk must \
         reupload under the BtrfsSend artifact set",
    );
}

/// Sidecar parses → use whatever codec was recorded.
#[test]
fn reupload_sidecar_btrfs_send_marker_resolves_to_btrfs_send() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let meta_json = r#"{
        "version": 1,
        "sandbox_id": "019e31d9-79a6-7132-b90f-007f1c62ddea",
        "size_bytes": 0,
        "created_at": "2026-05-25T00:00:00Z",
        "source": { "runtime": "fc", "template_id": "seed" },
        "rootfs_diff_format": "btrfs-send-v1"
    }"#;
    std::fs::write(snapshot_dir.join("snapshot.json"), meta_json).unwrap();
    let format = reupload_format_from_sidecar(snapshot_dir.to_str().unwrap()).unwrap();
    assert_eq!(format, SuspendSnapshotFormat::BtrfsSend);
}

/// Regression: a present-but-unparseable sidecar (truncated by a
/// crash mid-`std::fs::write`) **must not** silently degrade to
/// `Full`. The earlier implementation did, and the caller's
/// downstream artifact-presence check then probed `rootfs.ext4`,
/// which `encode_rootfs_as_btrfs_send` already deleted for a
/// btrfs-send snapshot — turning corruption into a terminal
/// `REUPLOAD_ARTIFACT_MISSING_MARKER` that the cluster controller
/// classifies as "mark sandbox failed."
///
/// The fix surfaces the parse error as `Status::internal` so the
/// reconciler stays in the retry path. This test pins that
/// invariant: the function must return an error (not `Ok(Full)`)
/// when the sidecar is unparseable.
#[test]
fn reupload_sidecar_corrupt_json_is_internal_error_not_silent_full() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(snapshot_dir.join("snapshot.json"), b"{\"version\":1,\"sand").unwrap();
    let err = reupload_format_from_sidecar(snapshot_dir.to_str().unwrap())
        .expect_err("corrupt sidecar must NOT silently resolve to Full");
    assert_eq!(
        err.code(),
        tonic::Code::Internal,
        "parse failure must surface as Internal so the reconciler retries \
         instead of marking the sandbox failed; got {:?}",
        err.code(),
    );
    assert!(
        err.message()
            .contains("snapshot.json present but unparseable"),
        "error message must name the failure mode precisely: {}",
        err.message(),
    );
}

/// The reupload give-up marker is scoped to the `restoring` state only.
/// The `restoring` rejection must render `REUPLOAD_SANDBOX_RESTORING_MARKER`
/// byte-identically to the historical message for compatibility with older
/// sandbox-managers, while a running state (`started`/`paused`)
/// must NOT carry the marker — abandoning a running VM's live state for an
/// older snapshot would lose work. Mirrors the producer's per-state branch.
#[test]
fn reupload_restoring_message_carries_marker_running_states_do_not() {
    use pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_RESTORING_MARKER;

    // Mirror the producer's per-state rendering.
    let render = |state: SandboxState| -> String {
        if state == SandboxState::Restoring {
            format!("cannot reupload snapshot: {REUPLOAD_SANDBOX_RESTORING_MARKER}")
        } else {
            format!("cannot reupload snapshot: sandbox is {state}, must be suspended")
        }
    };

    let restoring = render(SandboxState::Restoring);
    assert_eq!(
        restoring, "cannot reupload snapshot: sandbox is restoring, must be suspended",
        "the restoring message must stay byte-identical to the historical phrase",
    );
    assert!(
        restoring.contains(REUPLOAD_SANDBOX_RESTORING_MARKER),
        "the restoring message must carry the give-up marker",
    );

    // Running states must not carry the marker (no give-up → no data loss).
    for state in [SandboxState::Started, SandboxState::Paused] {
        assert!(
            !render(state).contains(REUPLOAD_SANDBOX_RESTORING_MARKER),
            "a {state} reupload refusal must NOT carry the give-up marker",
        );
    }
}

#[test]
fn pause_reconciliation_maps_runtime_observations() {
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::VmStateObservation::Observed(VmState::Paused,)
        ),
        super::PauseReconciliationOutcome::Paused
    );
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::VmStateObservation::Observed(VmState::Running,)
        ),
        super::PauseReconciliationOutcome::Running
    );
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::VmStateObservation::Unobservable(
                "probe failed".to_string(),
            )
        ),
        super::PauseReconciliationOutcome::Unknown("probe failed".to_string())
    );
}

#[test]
fn pause_reconciliation_does_not_guess_unknown_state() {
    for observation in [
        crate::runtime_reconciliation::VmStateObservation::Unobservable("probe failed".to_string()),
        crate::runtime_reconciliation::VmStateObservation::Observed(VmState::NotStarted),
        crate::runtime_reconciliation::VmStateObservation::Observed(VmState::Shutdown),
    ] {
        assert!(
            matches!(
                pause_reconciliation_outcome(&observation),
                super::PauseReconciliationOutcome::Unknown(_)
            ),
            "unknown pause outcomes must remain retryable with evidence"
        );
    }
}

#[tokio::test]
async fn suspended_artifact_adoption_race_uses_existing_artifact_id() {
    let conn = crate::db::test_conn();
    let sandbox_id = "sandbox-1";
    let snapshot_dir = "/cache/sandbox-1/snapshot";
    insert_suspended_test_sandbox(&conn, sandbox_id);

    let first = LocalArtifactId::new();
    let second = LocalArtifactId::new();
    assert!(
        crate::db::adopt_suspended_local_artifact(&conn, sandbox_id, first, snapshot_dir).unwrap()
    );
    let service = SandboxManagerService::for_test(conn);

    let resolved = service
        .adopt_or_get_suspended_local_artifact(sandbox_id, second, snapshot_dir)
        .unwrap();

    assert_eq!(resolved, first);
    let db = service.db.lock().expect("mutex poisoned");
    assert!(
        crate::db::get_local_artifact(&db, second)
            .unwrap()
            .is_none(),
        "failed adoption must not create a second artifact row"
    );
}

#[tokio::test]
async fn restore_consumer_cannot_observe_suspend_staging_before_publication() {
    let coordinator = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let artifact_lock = coordinator.artifact_lock(sandbox_id);
    let publication_guard = artifact_lock.lock().await;
    let tmp = tempfile::tempdir().expect("create tempdir");
    let snapshot_dir = tmp.path().join("snapshot");
    let staging_dir = tmp.path().join("staging");
    let local_artifact_id = LocalArtifactId::new();
    write_full_suspend_artifact(
        &staging_dir,
        sandbox_id,
        local_artifact_id,
        b"published-state",
    );

    let (attempted_tx, attempted_rx) = oneshot::channel();
    let consumer_lock = coordinator.artifact_lock(sandbox_id);
    let consumer_snapshot_dir = snapshot_dir.clone();
    let consumer = tokio::spawn(async move {
        attempted_tx.send(()).unwrap();
        let _guard = consumer_lock.lock().await;
        std::fs::read(consumer_snapshot_dir.join("vmstate")).unwrap()
    });
    attempted_rx.await.unwrap();

    publish_suspend_snapshot_dir(
        &staging_dir,
        &snapshot_dir,
        local_artifact_id,
        &Default::default(),
    )
    .unwrap();
    drop(publication_guard);

    assert_eq!(consumer.await.unwrap(), b"published-state");
}

#[tokio::test]
async fn paused_suspend_serializes_resume_behind_wake_and_artifact_locks() {
    let coordinator = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let wake_lock = coordinator.wake_lock(sandbox_id);
    let artifact_lock = coordinator.artifact_lock(sandbox_id);
    let suspend_wake_guard = wake_lock.lock().await;
    let suspend_artifact_guard = artifact_lock.lock().await;

    let (attempted_tx, attempted_rx) = oneshot::channel();
    let (resumed_tx, mut resumed_rx) = oneshot::channel();
    let resumed_wake_lock = coordinator.wake_lock(sandbox_id);
    let resumed_artifact_lock = coordinator.artifact_lock(sandbox_id);
    let resume = tokio::spawn(async move {
        attempted_tx.send(()).unwrap();
        let _guard = resumed_wake_lock.lock().await;
        let _guard = resumed_artifact_lock.lock().await;
        resumed_tx.send(()).unwrap();
    });
    attempted_rx.await.unwrap();

    drop(suspend_artifact_guard);
    assert!(
        matches!(
            resumed_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "resume must remain blocked while suspend retains wake serialization",
    );
    drop(suspend_wake_guard);
    resumed_rx.await.unwrap();
    resume.await.unwrap();
}

#[tokio::test]
async fn permanently_refused_teardown_backup_marks_the_suspending_row_terminal() {
    // A permanent job failure with no recovery attempt left ends the
    // artifact's upload: an earlier upload of it has failed permanently too,
    // so the join below never mints a new attempt, the suspend can never
    // publish, and the row can never leave `suspending` on its own. Nothing else on the node
    // separates that row from a suspend still uploading, which is the
    // distinction the cluster-deleted eviction needs before it may reclaim
    // anything — so the refusal has to be recorded on the row. A retriable
    // exhaustion is a different story: the next poll does mint a fresh
    // attempt, so it must leave the row unmarked.
    use crate::service::tests::service_tests::{
        insert_stranded_teardown_suspend_sandbox, prepare_teardown_suspend_request,
    };
    use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin, ExecutionGeneration};

    for (failure, expected_marker) in [
        (pplx_space_common_jobs::JobFailure::Retriable, None),
        (
            pplx_space_common_jobs::JobFailure::Permanent { reason: None },
            Some(pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON),
        ),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let execution_generation = ExecutionGeneration::new(7).unwrap();
        let staged =
            insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, execution_generation);
        // The artifact's recovery attempt is the upload this teardown joins.
        if let pplx_space_common_jobs::JobFailure::Permanent { reason } = failure {
            let earlier = JobId::new();
            pplx_space_common_jobs::db::insert_job(
                conn.raw(),
                pplx_space_common_jobs::db::InsertJob {
                    job_id: &earlier,
                    kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                    payload: "{}",
                    subject_id: Some(&sandbox_id_string),
                    dedup_key: Some(&staged.to_string()),
                    max_attempts: 1,
                    traceparent: None,
                    priority: pplx_space_common_jobs::JobPriority::default(),
                },
            )
            .unwrap();
            pplx_space_common_jobs::db::fail_job_permanent(
                conn.raw(),
                &earlier,
                "cluster-api rejected BeginArtifactBackup (NotFound): sandbox not found",
                reason,
            )
            .unwrap();
        }
        let backup_job_id = JobId::new();
        pplx_space_common_jobs::db::insert_job(
            conn.raw(),
            pplx_space_common_jobs::db::InsertJob {
                job_id: &backup_job_id,
                kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                payload: "{}",
                subject_id: Some(&sandbox_id_string),
                dedup_key: Some(&staged.to_string()),
                max_attempts: 1,
                traceparent: None,
                priority: pplx_space_common_jobs::JobPriority::default(),
            },
        )
        .unwrap();
        match failure {
            pplx_space_common_jobs::JobFailure::Permanent { reason } => {
                pplx_space_common_jobs::db::fail_job_permanent(
                    conn.raw(),
                    &backup_job_id,
                    "cluster-api rejected BeginArtifactBackup (NotFound): sandbox not found",
                    reason,
                )
                .unwrap();
            }
            pplx_space_common_jobs::JobFailure::Retriable => {
                pplx_space_common_jobs::db::fail_job_retriable_exhausted(
                    conn.raw(),
                    &backup_job_id,
                    "cluster-api BeginArtifactBackup (Unavailable): connection refused",
                )
                .unwrap();
            }
        }
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        let service = SandboxManagerService::for_test(conn);
        let artifact = crate::db::LocalArtifactRow {
            local_artifact_id: staged,
            sandbox_id: sandbox_id_string.clone(),
            artifact_kind: ArtifactKind::Full,
            artifact_origin: ArtifactOrigin::Suspend,
            snapshot_name: None,
            local_path: format!("/var/lib/space/sandboxes/{sandbox_id_string}/suspend"),
        };

        let error = service
            .ensure_teardown_suspend_backup(
                &row,
                super::TeardownSuspendBackup {
                    execution_generation,
                    artifact: &artifact,
                    format: SuspendSnapshotFormat::Full,
                    origin: super::TeardownBackupOrigin::Captured,
                },
            )
            .await
            .expect_err("a terminally failed backup cannot make the suspend durable");

        service
            .record_teardown_suspend_driver_failure(
                sandbox_id,
                execution_generation,
                &error,
                Some(&OrgSnapshotConfigResolution::ClusterRowMissing),
            )
            .unwrap();
        let terminal = matches!(
            failure,
            pplx_space_common_jobs::JobFailure::Permanent { .. }
        );
        {
            let db = service.db.lock().unwrap();
            let status = crate::db::latest_teardown_suspend_operation(&db, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .status;
            assert_eq!(
                status,
                if terminal {
                    crate::db::TeardownSuspendOperationStatus::Failed
                } else {
                    crate::db::TeardownSuspendOperationStatus::Running
                }
            );
        }
        let marked = {
            let db = service.db.lock().unwrap();
            crate::db::get_sandbox(&db, &sandbox_id_string)
                .unwrap()
                .unwrap()
        };
        assert_eq!(marked.state, SandboxState::Suspending);
        assert_eq!(
            marked.failure_reason.as_deref(),
            expected_marker,
            "{failure:?}"
        );

        // A confirmed missing sandbox ends polling without granting release proof.
        let response = service
            .prepare_teardown_suspend(crate::service::activation_support::internal_request(
                prepare_teardown_suspend_request(
                    sandbox_id_string,
                    execution_generation.get(),
                    crate::proto::TeardownSuspendAction::BeginOrJoin,
                ),
            ))
            .await;
        if terminal {
            assert_eq!(
                response.unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        } else {
            assert_eq!(
                crate::proto::TeardownSuspendOutcome::try_from(
                    response.unwrap().into_inner().outcome
                )
                .unwrap(),
                crate::proto::TeardownSuspendOutcome::Pending
            );
        }
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }
}

#[tokio::test]
async fn teardown_suspend_terminal_driver_failures_stop_polling_without_release_proof() {
    use crate::service::tests::service_tests::{
        insert_test_sandbox, prepare_teardown_suspend_request,
    };
    use pplx_space_common_core::types::ExecutionGeneration;
    // A deleting sandbox keeps polling for its delete receipt; see
    // `teardown_suspend_keeps_polling_until_a_deleting_sandbox_commits_its_delete`.
    for state in [Some(SandboxState::Quarantined), None] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Paused);
        let generation = ExecutionGeneration::new(7).unwrap();
        crate::db::begin_teardown_suspend_operation(&conn, &id, generation).unwrap();
        // Reproduce legacy jobs without invoking the fixed transition writers.
        if let Some(state) = state {
            conn.execute(
                "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
                crate::db::fixture::params![state.as_str(), id],
            )
            .unwrap();
        } else {
            conn.execute("DELETE FROM sandboxes WHERE id = ?1", [&id])
                .unwrap();
        }
        let service = SandboxManagerService::for_test(conn);
        service
            .drive_teardown_suspend(sandbox_id, generation, SandboxEventTrigger::System)
            .await
            .unwrap_err();
        {
            let db = service.db.lock().unwrap();
            assert_eq!(
                crate::db::latest_teardown_suspend_operation(&db, &id)
                    .unwrap()
                    .unwrap()
                    .status,
                crate::db::TeardownSuspendOperationStatus::Failed,
                "{state:?}"
            );
        }
        for action in [
            crate::proto::TeardownSuspendAction::BeginOrJoin,
            crate::proto::TeardownSuspendAction::ObserveOnly,
        ] {
            let error = service
                .prepare_teardown_suspend(crate::service::activation_support::internal_request(
                    prepare_teardown_suspend_request(id.clone(), generation.get(), action),
                ))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{state:?}");
        }
    }
}

/// A started sandbox whose generation-7 suspend job is running, moved to
/// Deleting by the production transition, which closes that job.
fn deleting_sandbox_with_closed_teardown_suspend(
    conn: &crate::db::Connection,
) -> (String, pplx_space_common_core::types::ExecutionGeneration) {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let generation = ExecutionGeneration::new(7).unwrap();
    insert_test_sandbox(conn, &id, SandboxState::Started);
    // The delete's receipt covers the row's route execution generation.
    crate::db::update_space_api_route_identity(
        conn,
        sandbox_id,
        &pplx_space_common_core::types::SpaceApiRouteIdentity {
            execution_generation: Some(generation),
            ..Default::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    crate::db::begin_teardown_suspend_operation(conn, &id, generation).unwrap();
    assert_eq!(
        crate::db::latest_teardown_suspend_operation(conn, &id)
            .unwrap()
            .unwrap()
            .status,
        crate::db::TeardownSuspendOperationStatus::Running
    );
    crate::db::test_fixtures::state_write_fixtures::apply_edge(
        conn,
        &id,
        &crate::db::edges::DELETE_ADMISSION,
    );
    // The transition fails the job before any shim request arrives.
    assert_eq!(
        crate::db::latest_teardown_suspend_operation(conn, &id)
            .unwrap()
            .unwrap()
            .status,
        crate::db::TeardownSuspendOperationStatus::Failed
    );
    (id, generation)
}

/// The production delete: it commits the receipt and removes the row in one
/// transaction.
fn commit_sandbox_delete(conn: &crate::db::Connection, id: &str) {
    crate::db::set_cleanup_intent(conn, id, false, crate::db::CleanupCompletion::DeleteRow)
        .unwrap();
    crate::db::complete_row_intent_cleanup(conn, id, crate::db::CleanupCompletion::DeleteRow, None)
        .unwrap();
}

#[tokio::test]
async fn teardown_suspend_keeps_polling_until_a_deleting_sandbox_commits_its_delete() {
    use crate::service::tests::service_tests::prepare_teardown_suspend_request;
    let conn = crate::db::test_conn();
    let (id, generation) = deleting_sandbox_with_closed_teardown_suspend(&conn);
    let service = SandboxManagerService::for_test(conn);
    let actions = [
        crate::proto::TeardownSuspendAction::BeginOrJoin,
        crate::proto::TeardownSuspendAction::ObserveOnly,
    ];

    // The shim releases a stopped task once, on containerd's exit path, and
    // nothing retries a refusal. While the delete is in flight it must hear
    // Pending, not FailedPrecondition.
    for action in actions {
        let response = service
            .prepare_teardown_suspend(crate::service::activation_support::internal_request(
                prepare_teardown_suspend_request(id.clone(), generation.get(), action),
            ))
            .await
            .expect("a deleting sandbox keeps the caller polling")
            .into_inner();
        assert_eq!(
            crate::proto::TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            crate::proto::TeardownSuspendOutcome::Pending,
            "{action:?}"
        );
    }
    // The job stays closed, so no driver restarts for the deleting row.
    assert!(service.teardown_suspend_drivers.is_empty());
    {
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&db, &id)
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Failed
        );
        commit_sandbox_delete(&db, &id);
    }

    for action in actions {
        let response = service
            .prepare_teardown_suspend(crate::service::activation_support::internal_request(
                prepare_teardown_suspend_request(id.clone(), generation.get(), action),
            ))
            .await
            .expect("the delete receipt releases the execution")
            .into_inner();
        assert_eq!(
            crate::proto::TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            crate::proto::TeardownSuspendOutcome::Deleted,
            "{action:?}"
        );
    }
}

#[tokio::test]
async fn teardown_suspend_terminal_decision_releases_a_delete_committed_after_the_first_snapshot() {
    let conn = crate::db::test_conn();
    let (id, generation) = deleting_sandbox_with_closed_teardown_suspend(&conn);
    let service = SandboxManagerService::for_test(conn);
    {
        let db = service.db.lock().unwrap();
        // The handler's first snapshot: the row is deleting and no receipt
        // covers the execution, so it goes on to the terminal decision.
        let evidence = crate::db::teardown_release_evidence(&db, &id, generation).unwrap();
        assert_eq!(evidence.suspend.unwrap().state, SandboxState::Deleting);
        assert!(!evidence.deleted);
        // The delete commits before the terminal decision's snapshot. The
        // failed job stays behind as history for the missing row.
        commit_sandbox_delete(&db, &id);
        assert!(crate::db::get_sandbox(&db, &id).unwrap().is_none());
        assert!(
            crate::db::latest_teardown_suspend_operation(&db, &id)
                .unwrap()
                .is_some()
        );
    }

    // Refusing this missing row would strand the pod: the shim's exit-path
    // release is not retried.
    assert_eq!(
        service
            .close_terminal_teardown_suspend(&id, generation)
            .unwrap(),
        super::TerminalTeardownSuspend::Deleted
    );
    // The receipt is generation-fenced: it does not release a newer execution.
    assert_eq!(
        service
            .close_terminal_teardown_suspend(
                &id,
                pplx_space_common_core::types::ExecutionGeneration::new(8).unwrap()
            )
            .unwrap(),
        super::TerminalTeardownSuspend::Open
    );
}

#[tokio::test]
async fn teardown_suspend_preserves_retryable_and_superseded_driver_work() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
    let service = SandboxManagerService::for_test(conn);
    for error in [
        Status::unavailable("retry later"),
        Status::internal("local IO failed"),
        Status::aborted("generation suspend no longer owns durable suspend artifact"),
        Status::not_found("runtime RPC object missing"),
    ] {
        service
            .record_teardown_suspend_driver_failure(sandbox_id, generation, &error, None)
            .unwrap();
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
    }
    let error = service
        .drive_teardown_suspend(
            sandbox_id,
            ExecutionGeneration::new(6).unwrap(),
            SandboxEventTrigger::System,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    {
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
        db.execute(
            "UPDATE sandboxes SET state = 'quarantined' WHERE id = ?1",
            [sandbox_id.to_string()],
        )
        .unwrap();
        db.execute_batch(
            "CREATE TRIGGER reject_terminal_job BEFORE UPDATE ON jobs
            WHEN NEW.status = 'failed' BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END",
        )
        .unwrap();
    }
    let error = service
        .drive_teardown_suspend(sandbox_id, generation, SandboxEventTrigger::System)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        error.message(),
        "cannot continue teardown suspend from quarantined",
        "bookkeeping failure must preserve the original driver error"
    );
}

#[tokio::test]
async fn teardown_suspend_registration_failure_closes_only_the_requested_generation() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    let failed_artifact = insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
    let service = SandboxManagerService::for_test(conn);
    let missing = teardown_artifact_backup_status(
        ArtifactBackupSubmissionError::Begin {
            error: pplx_space_common_jobs::JobError::permanent("registration refused"),
            superseded_generation: None,
        },
        failed_artifact,
    );
    let transient = teardown_artifact_backup_status(
        ArtifactBackupSubmissionError::Begin {
            error: pplx_space_common_jobs::JobError::retriable("connection unavailable"),
            superseded_generation: None,
        },
        failed_artifact,
    );
    service
        .record_teardown_suspend_driver_failure(
            sandbox_id,
            generation,
            &transient,
            Some(&OrgSnapshotConfigResolution::ClusterRowMissing),
        )
        .unwrap();
    service
        .record_teardown_suspend_driver_failure(
            sandbox_id,
            ExecutionGeneration::new(6).unwrap(),
            &missing,
            Some(&OrgSnapshotConfigResolution::ClusterRowMissing),
        )
        .unwrap();
    {
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .failure_reason
                .is_none(),
            "transient and stale-generation refusals must not mark the current artifact"
        );
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
    }
    // A missing origin node or an inconclusive verification must remain retryable.
    for resolution in [
        OrgSnapshotConfigResolution::Absent,
        OrgSnapshotConfigResolution::LookupFailed("connection unavailable".to_string()),
    ] {
        service
            .record_teardown_suspend_driver_failure(
                sandbox_id,
                generation,
                &missing,
                Some(&resolution),
            )
            .unwrap();
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .failure_reason
                .is_none(),
            "inconclusive absence verification must not authorize eviction"
        );
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
    }
    {
        let db = service.db.lock().unwrap();
        crate::db::stage_teardown_suspend_artifact(
            &db,
            &sandbox_id.to_string(),
            generation,
            LocalArtifactId::new(),
            "/synthetic/replacement",
        )
        .unwrap();
    }
    service
        .record_teardown_suspend_driver_failure(
            sandbox_id,
            generation,
            &missing,
            Some(&OrgSnapshotConfigResolution::ClusterRowMissing),
        )
        .unwrap();
    {
        let db = service.db.lock().unwrap();
        let status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        assert!(
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .failure_reason
                .is_none(),
            "an old failure must not mark a replacement artifact"
        );
        assert_eq!(
            status.operation.unwrap().status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
        crate::db::stage_teardown_suspend_artifact(
            &db,
            &sandbox_id.to_string(),
            generation,
            failed_artifact,
            "/synthetic/original",
        )
        .unwrap();
        db.execute_batch(
            "CREATE TRIGGER reject_terminal_job BEFORE UPDATE ON jobs
             WHEN NEW.status = 'failed' BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END",
        )
        .unwrap();
    }
    service
        .record_teardown_suspend_driver_failure(
            sandbox_id,
            generation,
            &missing,
            Some(&OrgSnapshotConfigResolution::ClusterRowMissing),
        )
        .unwrap_err();
    {
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .failure_reason
                .is_none(),
            "job failure rollback must also roll back eviction evidence"
        );
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
        db.execute_batch("DROP TRIGGER reject_terminal_job")
            .unwrap();
    }
    service
        .record_teardown_suspend_driver_failure(
            sandbox_id,
            generation,
            &missing,
            Some(&OrgSnapshotConfigResolution::ClusterRowMissing),
        )
        .unwrap();
    let db = service.db.lock().unwrap();
    assert_eq!(
        crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .status,
        crate::db::TeardownSuspendOperationStatus::Failed
    );
    assert_eq!(
        crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .failure_reason
            .as_deref(),
        Some(pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON),
        "a closed backup driver must leave the evidence required by cluster-deleted eviction"
    );
}

#[tokio::test]
async fn teardown_suspend_pause_failure_recovery_allows_same_generation_retry() {
    use crate::service::tests::service_tests::{
        insert_test_sandbox, prepare_teardown_suspend_request,
    };
    use pplx_space_common_core::types::ExecutionGeneration;

    for target in [
        crate::db::PauseFailedRecoveryTarget::Started,
        crate::db::PauseFailedRecoveryTarget::Paused,
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        let generation = ExecutionGeneration::new(7).unwrap();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &pplx_space_common_core::types::SpaceApiRouteIdentity {
                execution_generation: Some(generation),
                ..Default::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        crate::db::begin_teardown_suspend_operation(&conn, &id, generation).unwrap();
        let original = crate::db::latest_teardown_suspend_operation(&conn, &id)
            .unwrap()
            .unwrap();
        crate::db::test_fixtures::state_write_fixtures::apply_edge(
            &conn,
            &id,
            &crate::db::edges::PAUSE_BEGIN,
        );
        crate::db::mark_quarantined_from_state(
            &conn,
            &id,
            SandboxState::Pausing,
            pplx_space_common_core::types::SandboxQuarantineSource::SbxManager,
            pplx_space_common_core::types::SandboxQuarantineReasonCode::PauseFailed,
            "ambiguous pause response",
            pplx_space_common_core::types::SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        assert_eq!(
            service
                .close_terminal_teardown_suspend(&id, generation)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        {
            let db = service.db.lock().unwrap();
            assert!(crate::db::recover_pause_failed_quarantine(&db, &sandbox_id, target).unwrap());
        }
        let response = service
            .prepare_teardown_suspend(crate::service::activation_support::internal_request(
                prepare_teardown_suspend_request(
                    id.clone(),
                    generation.get(),
                    crate::proto::TeardownSuspendAction::ObserveOnly,
                ),
            ))
            .await
            .expect("recovery must make this execution eligible to suspend again")
            .into_inner();
        assert_eq!(
            crate::proto::TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            crate::proto::TeardownSuspendOutcome::Pending,
            "recovery alone supplies no runtime-release proof"
        );
        let db = service.db.lock().unwrap();
        for observed_generation in [None, Some(6), Some(8)] {
            db.execute(
                "UPDATE sandboxes SET space_api_route_execution_generation = ?1 WHERE id = ?2",
                crate::db::fixture::params![observed_generation, id],
            )
            .unwrap();
            crate::db::begin_teardown_suspend_operation(&db, &id, generation).unwrap_err();
        }
        db.execute(
            "UPDATE sandboxes SET space_api_route_execution_generation = ?1 WHERE id = ?2",
            crate::db::fixture::params![generation.get(), id],
        )
        .unwrap();
        crate::db::begin_teardown_suspend_operation(&db, &id, generation).unwrap();
        let retry = crate::db::latest_teardown_suspend_operation(&db, &id)
            .unwrap()
            .unwrap();
        assert_eq!(retry.execution_generation, generation);
        assert_eq!(
            retry.status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
        assert_ne!(retry.job_id, original.job_id);
        assert_eq!(
            pplx_space_common_jobs::db::get_job(db.raw(), &original.job_id)
                .unwrap()
                .unwrap()
                .status,
            "failed",
            "retry must retain terminal history"
        );
    }
}

use crate::service::tests::service_tests::service_with_runtime_liveness_and_absent_sockets;

fn stored_sandbox(service: &SandboxManagerService, id: &str) -> Option<crate::db::SandboxRow> {
    let db = service.db.lock().unwrap();
    crate::db::get_sandbox(&db, id).unwrap()
}

/// Suspend admission skips its plaintext-COW probe, which inspects the host
/// cache directory, for a sandbox with an encrypted volume.
fn insert_encrypted_test_sandbox(conn: &crate::db::Connection, id: &str, state: SandboxState) {
    crate::service::tests::service_tests::insert_test_sandbox(conn, id, state);
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [id],
    )
    .unwrap();
}

async fn drain_service_tasks(service: &SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}

/// Node-manager peer that confirms every `PauseSandbox` call and records it.
/// Any other node-manager method is refused, so the test fails instead of
/// reaching unexpected node work.
#[derive(Clone, Default)]
struct PauseConfirmingNodeManager {
    requests: std::sync::Arc<
        std::sync::Mutex<Vec<pplx_space_node_mgr_client::proto::PauseSandboxRequest>>,
    >,
}

impl tonic::server::NamedService for PauseConfirmingNodeManager {
    const NAME: &'static str = "space.node.v1.NodeManager";
}

struct ConfirmPause(
    std::sync::Arc<std::sync::Mutex<Vec<pplx_space_node_mgr_client::proto::PauseSandboxRequest>>>,
);

impl tonic::server::UnaryService<pplx_space_node_mgr_client::proto::PauseSandboxRequest>
    for ConfirmPause
{
    type Response = pplx_space_node_mgr_client::proto::PauseSandboxResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, Status>>;

    fn call(
        &mut self,
        request: tonic::Request<pplx_space_node_mgr_client::proto::PauseSandboxRequest>,
    ) -> Self::Future {
        self.0.lock().unwrap().push(request.into_inner());
        std::future::ready(Ok(tonic::Response::new(
            pplx_space_node_mgr_client::proto::PauseSandboxResponse {},
        )))
    }
}

impl<B> tower::Service<http::Request<B>> for PauseConfirmingNodeManager
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() != "/space.node.v1.NodeManager/PauseSandbox" {
            let refused = Status::unimplemented(format!(
                "unexpected node-manager call {}",
                request.uri().path()
            ));
            return Box::pin(std::future::ready(Ok(refused.into_http())));
        }
        let requests = std::sync::Arc::clone(&self.requests);
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(ConfirmPause(requests), request).await)
        })
    }
}

#[tokio::test]
async fn confirmed_node_pause_reports_paused_and_carries_the_execution_fence() {
    use pplx_space_node_mgr_client::proto::ExecutionFence;

    let sandbox_id = SandboxId::new();
    let db = crate::db::SharedConnection::new(crate::db::test_conn());

    let node_manager = PauseConfirmingNodeManager::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(node_manager.clone())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = pplx_space_node_mgr_client::NodeManagerGrpcClient::new(
        pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
    );
    let fence = ExecutionFence {
        execution_id: uuid::Uuid::now_v7().to_string(),
        previous_id: None,
    };

    let connector = crate::node_manager_transport::fake::fixed_client(client.clone());
    let state = super::pause_with_reconciliation(
        &mut client,
        Some(fence.clone()),
        &db,
        &sandbox_id,
        "pause failed",
        GuestSnapshotPreparationBudget::Standard,
        &connector,
        None,
    )
    .await;

    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(state, Ok(SandboxState::Paused));
    // Every node call carries the caller's execution fence so node-manager
    // can refuse a pause aimed at a replaced execution.
    let requests = node_manager.requests.lock().unwrap();
    assert!(!requests.is_empty(), "the pause must reach node-manager");
    for request in requests.iter() {
        assert_eq!(request.sandbox_id, sandbox_id.to_string());
        assert_eq!(request.execution_fence.as_ref(), Some(&fence));
    }
}

#[tokio::test]
async fn pause_refuses_rows_that_cannot_enter_pausing_before_writing_state() {
    use crate::service::activation_support::internal_request;
    use crate::service::tests::service_tests::insert_test_sandbox;

    let sockets = tempfile::tempdir().unwrap();
    for state in SandboxState::all()
        .into_iter()
        .filter(|state| *state != SandboxState::Started)
    {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, state);
        let service = service_with_runtime_liveness_and_absent_sockets(conn, false, sockets.path());

        let error = service
            .pause(internal_request(
                pplx_space_sbx_mgr_client::proto::PauseRequest {
                    sandbox_id: id.clone(),
                },
            ))
            .await
            .expect_err("only a started sandbox can pause");

        // A transient state is another operation in flight: retryable.
        let in_flight = matches!(
            state,
            SandboxState::Creating
                | SandboxState::Starting
                | SandboxState::Stopping
                | SandboxState::Deleting
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
                | SandboxState::Suspending
                | SandboxState::Restoring
        );
        let expected = if in_flight {
            tonic::Code::Unavailable
        } else {
            tonic::Code::FailedPrecondition
        };
        assert_eq!(error.code(), expected, "{state:?}");
        assert_eq!(
            stored_sandbox(&service, &id).unwrap().state,
            state,
            "a refused pause must not write state ({state:?})"
        );
        drain_service_tasks(&service).await;
    }
}

#[tokio::test]
async fn suspend_refuses_rows_that_are_neither_started_nor_paused() {
    use crate::service::activation_support::internal_request;
    use pplx_space_common_core::node_drain_signals::SUSPEND_SANDBOX_FAILED_MARKER;

    let sockets = tempfile::tempdir().unwrap();
    for state in SandboxState::all()
        .into_iter()
        .filter(|state| !matches!(state, SandboxState::Started | SandboxState::Paused))
    {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_encrypted_test_sandbox(&conn, &id, state);
        let service = service_with_runtime_liveness_and_absent_sockets(conn, false, sockets.path());

        let error = service
            .suspend(internal_request(
                pplx_space_sbx_mgr_client::proto::SuspendRequest {
                    sandbox_id: id.clone(),
                    blocking: true,
                    format: SuspendSnapshotFormat::Unspecified as i32,
                },
            ))
            .await
            .expect_err("only a started or paused sandbox can suspend");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{state:?}");
        // Node drain gives up on a failed sandbox only when it sees this marker.
        assert_eq!(
            error.message().contains(SUSPEND_SANDBOX_FAILED_MARKER),
            state == SandboxState::Failed,
            "{state:?}: {}",
            error.message()
        );
        let row = stored_sandbox(&service, &id).unwrap();
        assert_eq!(row.state, state, "a refused suspend must not write state");
        assert!(matches!(
            service.try_cleanup_fence(&id),
            crate::service::cleanup_support::CleanupLockTryResult::Acquired(_)
        ));
        drain_service_tasks(&service).await;
    }
}

#[tokio::test]
async fn paused_suspend_admission_requires_a_free_row_and_the_owning_generation() {
    use crate::service::cleanup_support::CleanupLockTryResult;
    use pplx_space_common_core::types::ExecutionGeneration;

    enum Admission {
        NodeLocal,
        Teardown(i64),
    }
    let running_generation = ExecutionGeneration::new(7).unwrap();
    let sockets = tempfile::tempdir().unwrap();
    for (case, admission, cleanup_intent, admitted) in [
        (
            "a persisted cleanup intent owns the row",
            Admission::NodeLocal,
            true,
            false,
        ),
        (
            "an older generation no longer owns the runtime",
            Admission::Teardown(6),
            false,
            false,
        ),
        (
            "the running generation suspends",
            Admission::Teardown(7),
            false,
            true,
        ),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_encrypted_test_sandbox(&conn, &id, SandboxState::Paused);
        if cleanup_intent {
            assert!(
                crate::db::set_cleanup_intent(
                    &conn,
                    &id,
                    false,
                    crate::db::CleanupCompletion::DeleteRow
                )
                .unwrap()
            );
        }
        let context = match admission {
            Admission::NodeLocal => SuspendRequestContext::new(id.clone(), true, 0, true, None),
            Admission::Teardown(generation) => {
                crate::db::begin_teardown_suspend_operation(&conn, &id, running_generation)
                    .unwrap();
                SuspendRequestContext::for_teardown(
                    id.clone(),
                    ExecutionGeneration::new(generation).unwrap(),
                )
            }
        };
        let service = service_with_runtime_liveness_and_absent_sockets(conn, false, sockets.path());
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        let artifact = service.auto_lifecycle.artifact_lock(&id).lock_owned().await;

        let result = service
            .prepare_suspend_snapshot_publication_after_locks(&context, &wake, &artifact)
            .await;

        let row = stored_sandbox(&service, &id).unwrap();
        match result {
            Ok((local_artifact_id, publication)) => {
                assert!(admitted, "{case}");
                local_artifact_id.parse::<LocalArtifactId>().unwrap();
                assert_eq!(row.state, SandboxState::Suspending, "{case}");
                let operation = {
                    let db = service.db.lock().unwrap();
                    crate::db::latest_teardown_suspend_operation(&db, &id)
                        .unwrap()
                        .unwrap()
                };
                assert_eq!(operation.execution_generation, running_generation);
                assert_eq!(
                    operation.status,
                    crate::db::TeardownSuspendOperationStatus::Running,
                    "{case}"
                );
                drop(publication);
            }
            Err(error) => {
                assert!(!admitted, "{case}: {error:?}");
                assert_eq!(error.code(), tonic::Code::Aborted, "{case}");
                assert_eq!(row.state, SandboxState::Paused, "{case}");
                assert_eq!(
                    row.cleanup_completion,
                    cleanup_intent.then_some(crate::db::CleanupCompletion::DeleteRow),
                    "{case}"
                );
                assert!(matches!(
                    service.try_cleanup_fence(&id),
                    CleanupLockTryResult::Acquired(_)
                ));
            }
        }
        drop((wake, artifact));
        drain_service_tasks(&service).await;
    }
}

#[tokio::test]
async fn suspend_completion_refuses_a_superseded_row_before_runtime_cleanup() {
    use crate::service::cleanup_support::CleanupLockTryResult;
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::{ExecutionGeneration, SpaceApiRouteIdentity};

    #[derive(Debug)]
    enum Supersession {
        CleanupActive,
        Deleted,
        RolledBackToPaused,
        RecreatedRuntimeLabel,
        NewerExecution,
    }
    let sockets = tempfile::tempdir().unwrap();
    for (supersession, expected) in [
        (Supersession::CleanupActive, tonic::Code::Unavailable),
        (Supersession::Deleted, tonic::Code::NotFound),
        (Supersession::RolledBackToPaused, tonic::Code::Aborted),
        (Supersession::RecreatedRuntimeLabel, tonic::Code::Aborted),
        (Supersession::NewerExecution, tonic::Code::Aborted),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Paused);
        let route_identity = |generation| SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(generation).unwrap()),
            ..Default::default()
        };
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &route_identity(7),
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        assert!(crate::db::begin_suspend(&conn, &id).unwrap());
        let mut captured = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        match supersession {
            Supersession::CleanupActive => {}
            Supersession::Deleted => {
                crate::db::test_fixtures::state_write_fixtures::apply_edge(
                    &conn,
                    &id,
                    &crate::db::edges::DELETE_ADMISSION,
                );
                commit_sandbox_delete(&conn, &id);
                assert!(crate::db::get_sandbox(&conn, &id).unwrap().is_none());
            }
            Supersession::RolledBackToPaused => {
                assert!(crate::db::rollback_suspending_to_paused(&conn, &id).unwrap());
            }
            Supersession::RecreatedRuntimeLabel => {
                captured.uuid8 = SandboxRuntimeLabel::parse("stale001").unwrap();
            }
            Supersession::NewerExecution => {
                // A newer execution suspended again after this attempt rolled back.
                assert!(crate::db::rollback_suspending_to_paused(&conn, &id).unwrap());
                crate::db::update_space_api_route_identity(
                    &conn,
                    sandbox_id,
                    &route_identity(8),
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                )
                .unwrap();
                assert!(crate::db::begin_suspend(&conn, &id).unwrap());
            }
        }
        let before = crate::db::get_sandbox(&conn, &id).unwrap();
        let service = service_with_runtime_liveness_and_absent_sockets(conn, false, sockets.path());
        // A superseded completion must leave the current execution's snapshot
        // scheduling in place.
        service
            .snapshot_scheduler
            .add_sandbox(id.clone(), captured.template.clone())
            .await;
        // Fail closed if a guard regresses: with shutdown begun, runtime
        // cleanup is never dispatched to node-manager.
        service.shutdown_token.cancel();
        let held_fence = match supersession {
            Supersession::CleanupActive => match service.try_cleanup_fence(&id) {
                CleanupLockTryResult::Acquired(fence) => Some(fence),
                CleanupLockTryResult::Busy => panic!("fence is free"),
            },
            _ => None,
        };
        let snapshot_dir = sockets.path().join(&id).join("snapshot");

        let error = service
            .finalize_suspended_runtime(
                &captured,
                LocalArtifactId::new(),
                &snapshot_dir.to_string_lossy(),
                None,
                None,
            )
            .await
            .expect_err("a superseded completion must not tear down the runtime");

        assert_eq!(error.code(), expected, "{supersession:?}");
        let after = stored_sandbox(&service, &id);
        assert_eq!(
            after
                .as_ref()
                .map(|row| (row.state, row.cleanup_completion)),
            before
                .as_ref()
                .map(|row| (row.state, row.cleanup_completion)),
            "a refused completion must not change the row ({supersession:?})"
        );
        assert!(
            service.snapshot_scheduler.is_active(&id).await,
            "a refused completion must keep snapshot scheduling ({supersession:?})"
        );
        let cleanup_jobs: i64 = service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE kind = ?1 AND subject_id = ?2",
                crate::db::fixture::params![crate::db::cleanup_jobs::KIND, id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            cleanup_jobs, 0,
            "a refused completion must not persist cleanup work ({supersession:?})"
        );
        drop(held_fence);
        drain_service_tasks(&service).await;
    }
}

#[tokio::test]
async fn suspend_publication_failure_persists_only_against_the_suspending_row() {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::SandboxQuarantineReasonCode;

    let reject_state = |conn: &crate::db::Connection, state: SandboxState| {
        conn.execute_batch(&format!(
            "CREATE TRIGGER reject_{state} BEFORE UPDATE OF state ON sandboxes
             WHEN NEW.state = '{state}'
             BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END"
        ))
        .unwrap();
    };
    let sockets = tempfile::tempdir().unwrap();
    for (case, runtime_live, state, rejected, expected_state) in [
        (
            "a live rollback that lost to another transition leaves that row alone",
            true,
            SandboxState::Paused,
            None,
            SandboxState::Paused,
        ),
        (
            "a live runtime whose rollback cannot persist is quarantined",
            true,
            SandboxState::Suspending,
            Some(SandboxState::Paused),
            SandboxState::Quarantined,
        ),
        (
            "a dead runtime whose quarantine cannot persist stays suspending",
            false,
            SandboxState::Suspending,
            Some(SandboxState::Quarantined),
            SandboxState::Suspending,
        ),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, state);
        if let Some(rejected) = rejected {
            reject_state(&conn, rejected);
        }
        let service =
            service_with_runtime_liveness_and_absent_sockets(conn, runtime_live, sockets.path());

        service.record_suspend_snapshot_publication_failure_after_begin(&id, "capture failed");

        let row = stored_sandbox(&service, &id).unwrap();
        let reason_code = {
            let db = service.db.lock().unwrap();
            crate::db::current_quarantine_reason_wire_value(&db, &id).unwrap()
        };
        assert_eq!(row.state, expected_state, "{case}");
        if expected_state == SandboxState::Quarantined {
            assert_eq!(
                row.failure_reason.as_deref(),
                Some("capture failed"),
                "{case}"
            );
            assert_eq!(
                reason_code.as_deref(),
                Some(SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.as_str()),
                "{case}"
            );
        } else {
            // Periodic publication recovery owns a row left suspending; a row
            // another transition owns records nothing from this failure.
            assert_eq!(row.failure_reason, None, "{case}");
            assert_eq!(reason_code, None, "{case}");
        }
        drain_service_tasks(&service).await;
    }
}

#[tokio::test]
async fn suspend_publication_failure_after_staging_preserves_only_owned_evidence() {
    use super::SuspendSnapshotStagingDisposition as Disposition;
    use crate::service::suspend_snapshot_publication::{
        suspend_snapshot_staging_dir, suspend_snapshot_staging_parent_dir,
    };
    use crate::service::tests::service_tests::insert_test_sandbox;

    #[derive(Debug)]
    enum Row {
        Absent,
        At(SandboxState),
        /// A row whose stored policy document this build cannot decode.
        Undecodable,
    }
    #[derive(Debug)]
    enum Disk {
        Empty,
        CompleteCurrentStaging,
        /// The staging parent cannot be listed.
        UnlistableStaging,
    }
    for (case, runtime_live, row, disk, unreadable_journal, expected, final_state) in [
        (
            "a live runtime rolls back to paused",
            true,
            Row::At(SandboxState::Suspending),
            Disk::CompleteCurrentStaging,
            false,
            Disposition::Cleanup,
            Some(SandboxState::Paused),
        ),
        (
            "another transition already moved the row",
            false,
            Row::At(SandboxState::Paused),
            Disk::CompleteCurrentStaging,
            false,
            Disposition::Cleanup,
            Some(SandboxState::Paused),
        ),
        (
            "the sandbox was deleted",
            false,
            Row::Absent,
            Disk::CompleteCurrentStaging,
            false,
            Disposition::Cleanup,
            None,
        ),
        (
            "an unreadable row keeps existing staging",
            false,
            Row::Undecodable,
            Disk::CompleteCurrentStaging,
            false,
            Disposition::Preserve,
            None,
        ),
        (
            "an unreadable row with no staging",
            false,
            Row::Undecodable,
            Disk::Empty,
            false,
            Disposition::Cleanup,
            None,
        ),
        (
            "unlistable staging is quarantined",
            false,
            Row::At(SandboxState::Suspending),
            Disk::UnlistableStaging,
            false,
            Disposition::Cleanup,
            Some(SandboxState::Quarantined),
        ),
        (
            "no usable artifact is quarantined",
            false,
            Row::At(SandboxState::Suspending),
            Disk::Empty,
            false,
            Disposition::Cleanup,
            Some(SandboxState::Quarantined),
        ),
        (
            "an unprovable durability mode keeps the complete capture",
            false,
            Row::At(SandboxState::Suspending),
            Disk::CompleteCurrentStaging,
            true,
            Disposition::Preserve,
            Some(SandboxState::Suspending),
        ),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        match row {
            Row::Absent => {}
            Row::At(state) => insert_test_sandbox(&conn, &id, state),
            Row::Undecodable => {
                insert_test_sandbox(&conn, &id, SandboxState::Suspending);
                conn.execute(
                    "UPDATE sandboxes SET auto_lifecycle_json = 'undecodable' WHERE id = ?1",
                    [&id],
                )
                .unwrap();
            }
        }
        if unreadable_journal {
            conn.execute(
                "INSERT INTO jobs (job_id, kind, subject_id, payload)
                 VALUES (?1, ?2, ?3, 'malformed')",
                crate::db::fixture::params![
                    uuid::Uuid::now_v7().to_string(),
                    crate::db::TEARDOWN_SUSPEND_JOB_KIND,
                    id
                ],
            )
            .unwrap();
        }
        let cache = tempfile::tempdir().unwrap();
        let sandbox_dir = cache.path().join(&id);
        let local_artifact_id = LocalArtifactId::new();
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id);
        match disk {
            Disk::Empty => {}
            Disk::CompleteCurrentStaging => {
                write_full_suspend_artifact(&staging_dir, &id, local_artifact_id, b"capture");
            }
            Disk::UnlistableStaging => {
                std::fs::create_dir_all(&sandbox_dir).unwrap();
                std::fs::write(suspend_snapshot_staging_parent_dir(&sandbox_dir), b"").unwrap();
            }
        }
        let service =
            service_with_runtime_liveness_and_absent_sockets(conn, runtime_live, cache.path());

        let disposition = service.record_suspend_snapshot_publication_failure_after_staging(
            &id,
            &sandbox_dir,
            &staging_dir,
            "publish failed",
            &Default::default(),
        );

        assert_eq!(disposition, expected, "{case}");
        if matches!(row, Row::Undecodable) {
            let raw_state: String = service
                .db
                .lock()
                .unwrap()
                .query_row("SELECT state FROM sandboxes WHERE id = ?1", [&id], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(raw_state, SandboxState::Suspending.as_str(), "{case}");
        } else {
            assert_eq!(
                stored_sandbox(&service, &id).map(|row| row.state),
                final_state,
                "{case}"
            );
        }
        if matches!(disk, Disk::CompleteCurrentStaging) {
            // Removal is the caller's job, on a Cleanup disposition.
            assert!(staging_dir.exists(), "{case}");
        }
        drain_service_tasks(&service).await;
    }
}

#[tokio::test]
async fn teardown_suspend_driver_completes_a_generation_whose_artifact_is_already_durable() {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let generation = ExecutionGeneration::new(7).unwrap();
    let cache = tempfile::tempdir().unwrap();
    let snapshot_dir = cache.path().join(&id).join("snapshot");
    let snapshot_dir = snapshot_dir.to_string_lossy();
    let local_artifact_id = LocalArtifactId::new();
    insert_test_sandbox(&conn, &id, SandboxState::Paused);
    crate::db::begin_teardown_suspend(&conn, &id, generation).unwrap();
    crate::db::stage_teardown_suspend_artifact(
        &conn,
        &id,
        generation,
        local_artifact_id,
        &snapshot_dir,
    )
    .unwrap();
    crate::db::mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string()).unwrap();
    crate::db::complete_row_intent_cleanup(
        &conn,
        &id,
        crate::db::CleanupCompletion::MarkSuspended,
        Some((local_artifact_id, &snapshot_dir)),
    )
    .unwrap();
    let service = service_with_runtime_liveness_and_absent_sockets(conn, false, cache.path());

    // Driving the same generation again is idempotent.
    for pass in ["first", "repeated"] {
        service
            .drive_teardown_suspend(sandbox_id, generation, SandboxEventTrigger::System)
            .await
            .unwrap_or_else(|error| panic!("{pass}: {error:?}"));

        let operation = {
            let db = service.db.lock().unwrap();
            crate::db::latest_teardown_suspend_operation(&db, &id)
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            operation.status,
            crate::db::TeardownSuspendOperationStatus::Completed,
            "{pass}"
        );
        assert_eq!(
            operation.published_artifact_id,
            Some(local_artifact_id),
            "{pass}"
        );
        let row = stored_sandbox(&service, &id).unwrap();
        assert_eq!(row.state, SandboxState::Suspended, "{pass}");
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    }
    drain_service_tasks(&service).await;
}

#[tokio::test]
async fn teardown_suspend_without_a_capture_quarantines_as_an_only_copy() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
    // The publication stopped the runtime before anything was staged.
    conn.execute(
        "UPDATE sandboxes SET current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL, current_local_artifact_origin = NULL
         WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let service = SandboxManagerService::for_test(conn);

    service
        .drive_teardown_suspend(sandbox_id, generation, SandboxEventTrigger::System)
        .await
        .unwrap_err();

    let db = service.db.lock().unwrap();
    let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(
        row.failure_reason.as_deref(),
        Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    );
    assert_eq!(
        crate::db::latest_teardown_suspend_operation(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .status,
        crate::db::TeardownSuspendOperationStatus::Failed
    );
}
