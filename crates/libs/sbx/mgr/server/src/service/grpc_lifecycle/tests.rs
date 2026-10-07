use pplx_space_common_core::types::JobId;

use tonic::Status;

use super::{
    ArtifactBackupSubmissionError, OrgSnapshotConfigResolution, SuspendRequestContext,
    artifact_backup_format, artifact_backup_lookup_error, current_instance_backup_precondition,
    encrypted_snapshot_prepare_error_status, ensure_completed_suspend_retry_format,
    pause_reconciliation_outcome, publish_suspend_snapshot_dir, require_artifact_backup_files,
    reupload_format_from_sidecar, teardown_artifact_backup_status,
};
use crate::auto_lifecycle::AutoLifecycleCoordinator;
use crate::guest_filesystem_flush::GuestSnapshotPreparationBudget;
use crate::service::SandboxManagerService;
use crate::service::suspend_snapshot_publication::write_full_suspend_artifact;
use crate::state::SandboxState;
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, LocalArtifactId, SandboxId, SandboxOwner, SandboxRuntimeLabel,
};
use pplx_space_events::SandboxEventTrigger;
use pplx_space_node_runtime_core::fc_api::FirecrackerVmState;
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
    let service = SandboxManagerService::new(conn);

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
    let service = SandboxManagerService::new(conn);

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
                rusqlite::params![template, pool, override_policy, authority, id],
            )
            .unwrap();
        }
    }
    let service = crate::service::SandboxManagerService::new(conn);
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
    crate::db::update_state(&service.db_conn().unwrap(), id, SandboxState::Stopped)
        .unwrap()
        .require_applied()
        .unwrap();
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

#[test]
fn artifact_backup_file_proof_distinguishes_absence_from_io_failure() {
    use pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER;
    use pplx_space_common_core::types::ArtifactKind;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_str().unwrap();
    let format = crate::proto::SuspendSnapshotFormat::Full;
    let files = crate::snapshot_upload::artifact_files_for_kind(ArtifactKind::Full, format);
    for file in &files {
        std::fs::write(directory.path().join(file.local_name), b"captured").unwrap();
    }
    require_artifact_backup_files(path, ArtifactKind::Full, format, true).unwrap();
    let first = directory.path().join(files[0].local_name);
    std::fs::remove_file(&first).unwrap();
    for full_current in [false, true] {
        let missing = require_artifact_backup_files(path, ArtifactKind::Full, format, full_current)
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
    let inaccessible =
        require_artifact_backup_files(path, ArtifactKind::Full, format, true).unwrap_err();
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
}

fn insert_suspended_test_sandbox(conn: &rusqlite::Connection, sandbox_id: &str) {
    let uuid8 = SandboxRuntimeLabel::parse("sandbox1").unwrap();
    let owner = SandboxOwner::parse("user").unwrap();
    let process_env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
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
                    let mut service = SandboxManagerService::new(conn);
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
fn pause_reconciliation_maps_firecracker_observations() {
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::FirecrackerVmStateObservation::Observed(
                FirecrackerVmState::Paused,
            )
        ),
        super::PauseReconciliationOutcome::Paused
    );
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::FirecrackerVmStateObservation::Observed(
                FirecrackerVmState::Running,
            )
        ),
        super::PauseReconciliationOutcome::Running
    );
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::FirecrackerVmStateObservation::Observed(
                FirecrackerVmState::NotStarted,
            )
        ),
        super::PauseReconciliationOutcome::Unknown("Firecracker reported NotStarted".to_string())
    );
    assert_eq!(
        pause_reconciliation_outcome(
            &crate::runtime_reconciliation::FirecrackerVmStateObservation::Unobservable(
                "probe failed".to_string(),
            )
        ),
        super::PauseReconciliationOutcome::Unknown("probe failed".to_string())
    );
}

#[test]
fn pause_reconciliation_does_not_guess_unknown_state() {
    for observation in [
        crate::runtime_reconciliation::FirecrackerVmStateObservation::Unobservable(
            "probe failed".to_string(),
        ),
        crate::runtime_reconciliation::FirecrackerVmStateObservation::Observed(
            FirecrackerVmState::NotStarted,
        ),
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
    let service = SandboxManagerService::new(conn);

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

    publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, local_artifact_id).unwrap();
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
    // A permanent job failure ends the artifact's upload: the join below
    // never mints a new attempt, so the suspend can never publish and the
    // row can never leave `suspending` on its own. Nothing else on the node
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
        let backup_job_id = JobId::new();
        pplx_space_common_jobs::db::insert_job(
            &conn,
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
                    &conn,
                    &backup_job_id,
                    "cluster-api rejected BeginArtifactBackup (NotFound): sandbox not found",
                    reason,
                )
                .unwrap();
            }
            pplx_space_common_jobs::JobFailure::Retriable => {
                pplx_space_common_jobs::db::fail_job_retriable_exhausted(
                    &conn,
                    &backup_job_id,
                    "cluster-api BeginArtifactBackup (Unavailable): connection refused",
                )
                .unwrap();
            }
        }
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        let service = SandboxManagerService::new(conn);
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
    for state in [
        Some(SandboxState::Quarantined),
        Some(SandboxState::Deleting),
        None,
    ] {
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
                rusqlite::params![state.as_str(), id],
            )
            .unwrap();
        } else {
            conn.execute("DELETE FROM sandboxes WHERE id = ?1", [&id])
                .unwrap();
        }
        let service = SandboxManagerService::new(conn);
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

#[tokio::test]
async fn teardown_suspend_preserves_retryable_and_superseded_driver_work() {
    use crate::service::tests::service_tests::insert_stranded_teardown_suspend_sandbox;
    use pplx_space_common_core::types::ExecutionGeneration;
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation);
    let service = SandboxManagerService::new(conn);
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
    let service = SandboxManagerService::new(conn);
    let missing = teardown_artifact_backup_status(
        ArtifactBackupSubmissionError::Begin(pplx_space_common_jobs::JobError::permanent(
            "registration refused",
        )),
        failed_artifact,
    );
    let transient = teardown_artifact_backup_status(
        ArtifactBackupSubmissionError::Begin(pplx_space_common_jobs::JobError::retriable(
            "connection unavailable",
        )),
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
        assert_eq!(
            crate::db::update_state(&conn, &id, SandboxState::Pausing).unwrap(),
            crate::db::SandboxStateWriteOutcome::Applied
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
        let service = SandboxManagerService::new(conn);
        assert_eq!(
            service
                .reject_terminal_teardown_suspend(&id, generation)
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
                rusqlite::params![observed_generation, id],
            )
            .unwrap();
            crate::db::begin_teardown_suspend_operation(&db, &id, generation).unwrap_err();
        }
        db.execute(
            "UPDATE sandboxes SET space_api_route_execution_generation = ?1 WHERE id = ?2",
            rusqlite::params![generation.get(), id],
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
            pplx_space_common_jobs::db::get_job(&db, &original.job_id)
                .unwrap()
                .unwrap()
                .status,
            "failed",
            "retry must retain terminal history"
        );
    }
}
