use super::*;

#[test]
fn completed_push_reuse_is_limited_to_immutable_snapshot_artifacts() {
    let generic = PushFileRequest::default();
    assert_eq!(
        push_duplicate_policy(&generic),
        pplx_space_common_jobs::DuplicatePolicy::ReuseExisting
    );

    let snapshot = PushFileRequest {
        subject_id: "sandbox:019ff811-2061-7240-b337-3fefb9f82b64".into(),
        s3_uri: "s3://bucket/prefix/suspend_snapshots/05/019ff811-2061-7240-b337-3fefb9f82b64/019ff84a-67b2-7aa3-b52e-45015b02e12d/memfile?region=us-east-1".into(),
        ..Default::default()
    };
    assert_eq!(
        push_duplicate_policy(&snapshot),
        pplx_space_common_jobs::DuplicatePolicy::ReuseExistingOrCompleted
    );

    for artifact in [
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::RootfsBtrfsSend,
        SnapshotArtifact::Memory,
        SnapshotArtifact::VmState,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
        SnapshotArtifact::Metadata,
    ] {
        let artifact_snapshot = PushFileRequest {
            s3_uri: snapshot
                .s3_uri
                .replace("/memfile?", &format!("/{}?", artifact.file_name())),
            ..snapshot.clone()
        };
        assert_eq!(
            push_duplicate_policy(&artifact_snapshot),
            pplx_space_common_jobs::DuplicatePolicy::ReuseExistingOrCompleted,
            "{} should use completed-transfer reuse",
            artifact.file_name()
        );
    }

    let changed_generic_path = PushFileRequest {
        subject_id: snapshot.subject_id,
        s3_uri: "s3://bucket/arbitrary/output".into(),
        ..Default::default()
    };
    assert_eq!(
        push_duplicate_policy(&changed_generic_path),
        pplx_space_common_jobs::DuplicatePolicy::ReuseExisting
    );
}
use std::sync::Arc;

#[derive(Clone)]
struct BlockingExecutor;

#[async_trait::async_trait]
impl JobExecutor for BlockingExecutor {
    async fn execute(
        &self,
        _job: &pplx_space_common_jobs::Job,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<i64, pplx_space_common_jobs::JobError> {
        std::future::pending().await
    }
}

fn request_from_peer<T>(message: T, uid: u32, gid: u32) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(uid, gid, Some(42)));
    request
}

fn root_request<T>(message: T) -> Request<T> {
    request_from_peer(message, 0, 0)
}

fn service_request<T>(
    message: T,
    user: pplx_space_common_svc::core::host::linux::identities::LinuxUser,
) -> Request<T> {
    request_from_peer(message, user.uid, user.primary_group.gid)
}

async fn test_service(temp: &tempfile::TempDir) -> VolumeManagerService {
    let db_path = temp.path().join("volume-manager.db");
    crate::run::migrate_volume_manager_db(&db_path)
        .await
        .unwrap();
    VolumeManagerService::new(
        db_path.to_str().expect("utf8 db path"),
        temp.path().join("staging"),
        VolumeManagerServiceConfig {
            push_concurrency: 1,
            pull_concurrency: 1,
            pull_priority_bands: Vec::new(),
            transfer_timeout: Duration::from_secs(1),
            max_attempts: 1,
            executor: TransferExecutorConfig::default(),
        },
    )
    .expect("create volume-manager service")
}

#[tokio::test]
async fn btrfs_resubmission_reuses_live_or_completed_jobs_but_restages_terminal_failures() {
    use pplx_space_common_jobs::{JobStatus, db as jobs};

    for status in [
        JobStatus::Pending,
        JobStatus::Running,
        JobStatus::Cancelling,
        JobStatus::Completed,
        JobStatus::Failed,
        JobStatus::Cancelled,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let service = test_service(&temp).await;
        let request = crate::proto::PushBtrfsSendRequest {
            sandbox_id: uuid::Uuid::now_v7().to_string(),
            source_id: uuid::Uuid::now_v7().to_string(),
            artifact_dir: temp.path().join("artifact").to_string_lossy().into_owned(),
            s3_uri: "s3://bucket/snapshot/rootfs.btrfs-send".into(),
        };
        let mut staged = Vec::new();
        // Exercise real submission and SQLite ownership, replacing only the
        // privileged COW operation. The worker is not polled between these
        // ready submissions and the explicit durable state transitions.
        let mut stage =
            |request: pplx_space_node_mgr_client::proto::BtrfsStageSendSourceRequest| {
                staged.push(request.lease_id.parse::<uuid::Uuid>().unwrap());
                std::future::ready(Ok(()))
            };
        let first = service
            .submit_btrfs_push_with_stager(request.clone(), &mut stage)
            .await
            .unwrap();
        let first_id: JobId = first.job_id.parse().unwrap();
        let should_reuse = {
            let conn = service.lock_db().unwrap();
            match status {
                JobStatus::Pending => true,
                JobStatus::Running => {
                    assert!(jobs::mark_job_running(&conn, &first_id).unwrap());
                    true
                }
                JobStatus::Cancelling | JobStatus::Cancelled => {
                    jobs::request_job_cancellation(&conn, &first_id).unwrap();
                    if status == JobStatus::Cancelled {
                        assert!(jobs::mark_job_cancelled(&conn, &first_id).unwrap());
                    }
                    status == JobStatus::Cancelling
                }
                JobStatus::Completed => {
                    assert!(jobs::mark_job_running(&conn, &first_id).unwrap());
                    assert!(jobs::complete_job(&conn, &first_id, 42).unwrap());
                    true
                }
                JobStatus::Failed => {
                    jobs::fail_job_permanent(&conn, &first_id, "upload failed", None).unwrap();
                    false
                }
            }
        };
        let retry = service
            .submit_btrfs_push_with_stager(request, &mut stage)
            .await
            .unwrap();
        assert_eq!(first.job_id == retry.job_id, should_reuse, "{status:?}");
        assert_eq!(staged.len(), if should_reuse { 1 } else { 2 }, "{status:?}");
        let retry_id: JobId = retry.job_id.parse().unwrap();
        let job = service.worker.get_job(&retry_id).unwrap().unwrap();
        let payload: crate::btrfs_push::Payload = serde_json::from_str(&job.payload).unwrap();
        assert_eq!(payload.send_lease, *staged.last().unwrap());
        if !should_reuse {
            assert_ne!(staged[0], staged[1]);
            assert_eq!(job.status, "pending");
        }
    }
}

fn plaintext_pull_artifact_request(subject_id: &str, artifact_id: &str) -> PullFilesAsyncRequest {
    PullFilesAsyncRequest {
        subject_id: subject_id.to_string(),
        artifact_id: artifact_id.to_string(),
        files: vec![PullFileBatchEntry {
            s3_uri: "s3://bucket/snap/payload?region=us-east-2".to_string(),
            local_path: "/space/snap/payload".to_string(),
            role: "payload".to_string(),
            ..Default::default()
        }],
        encryption: None,
    }
}

async fn pull_artifact_as(
    service: &VolumeManagerService,
    subject_id: &str,
    artifact_id: &str,
    user: pplx_space_common_svc::core::host::linux::identities::LinuxUser,
) -> Result<String, Status> {
    VolumeManager::pull_files_async(
        service,
        service_request(
            plaintext_pull_artifact_request(subject_id, artifact_id),
            user,
        ),
    )
    .await
    .map(|response| response.into_inner().job_id)
}

fn plaintext_artifact_payload(subject_id: &str, artifact_id: &str) -> ArtifactPayload {
    ArtifactPayload {
        subject_id: subject_id.to_string(),
        artifact_id: artifact_id.to_string(),
        entries: vec![entry(
            "payload",
            "s3://bucket/snap/payload?region=us-east-2",
        )],
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
    }
}

#[tokio::test]
async fn closing_transfer_admission_rejects_new_work_and_bounds_inflight_wait() {
    let admission = TransferAdmission::open();
    let lease = admission.enter().expect("admit predecessor submission");

    admission.close();

    let rejected = match admission.enter() {
        Ok(_) => panic!("handoff must reject new work"),
        Err(status) => status,
    };
    assert_eq!(rejected.code(), tonic::Code::Unavailable);
    assert!(
        !admission.wait_for_idle(Duration::from_millis(20)).await,
        "an admitted submission must hold the handoff drain open"
    );

    drop(lease);
    assert!(
        admission.wait_for_idle(Duration::from_secs(1)).await,
        "handoff drain must finish after the admitted submission exits"
    );
}

#[tokio::test]
async fn closing_admission_rejects_mutations() {
    let temp = tempfile::tempdir().expect("tempdir");
    let service = test_service(&temp).await;
    service.transfer_admission().close();

    let attach = VolumeManager::attach_volume(
        &service,
        root_request(AttachVolumeRequest {
            sandbox_id: "sandbox-test".to_string(),
            user_email: "test@example.com".to_string(),
            volume: Some(VolumeSpec {
                name: "data".to_string(),
                s3_bucket: "bucket".to_string(),
                s3_prefix: String::new(),
                s3_region: "us-east-1".to_string(),
                read_only: true,
                byok_key_id: None,
            }),
        }),
    )
    .await
    .expect_err("handoff must reject attach");
    assert_eq!(attach.code(), tonic::Code::Unavailable);

    let detach = VolumeManager::detach_volume(
        &service,
        root_request(DetachVolumeRequest {
            sandbox_id: "sandbox-test".to_string(),
            volume_name: "data".to_string(),
        }),
    )
    .await
    .expect_err("handoff must reject detach");
    assert_eq!(detach.code(), tonic::Code::Unavailable);

    let cancel = VolumeManager::cancel_transfer(
        &service,
        root_request(CancelTransferRequest {
            selector: Some(CancelTransferSelector::JobId("job-test".to_string())),
        }),
    )
    .await
    .expect_err("handoff must reject cancellation before reading its selector");
    assert_eq!(cancel.code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn rpc_authorization_rejects_group_members_and_separates_subject_kinds() {
    use pplx_space_common_svc::core::host::linux::identities::{groups, users};

    let temp = tempfile::tempdir().expect("tempdir");
    let service = test_service(&temp).await;
    let sandbox_subject = "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2";
    let template_subject = "template:seed";

    let rejected = VolumeManager::submit_pull_file(
        &service,
        request_from_peer(
            PullFileRequest {
                subject_id: sandbox_subject.to_string(),
                s3_uri: "s3://other-tenant/snapshot?region=us-east-1".to_string(),
                local_path: "/tmp/caller-chosen".to_string(),
                ..Default::default()
            },
            42_424,
            groups::SPACE_SBXCLIENTS.gid,
        ),
    )
    .await
    .expect_err("socket-group membership alone must not authorize transfers");
    assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
    assert_eq!(service.active_transfer_count().expect("active jobs"), 0);

    let missing_identity = VolumeManager::list_transfer_jobs(
        &service,
        Request::new(ListTransferJobsRequest {
            subject_id: Some(sandbox_subject.to_string()),
            ..Default::default()
        }),
    )
    .await
    .expect_err("requests without UDS peer credentials must fail closed");
    assert_eq!(missing_identity.code(), tonic::Code::Unauthenticated);

    VolumeManager::list_transfer_jobs(
        &service,
        service_request(
            ListTransferJobsRequest {
                subject_id: Some(sandbox_subject.to_string()),
                ..Default::default()
            },
            users::SPACE_SBXMGR,
        ),
    )
    .await
    .expect("sandbox-manager may list sandbox transfers");

    let sandbox_cross_kind = VolumeManager::list_transfer_jobs(
        &service,
        service_request(
            ListTransferJobsRequest {
                subject_id: Some(template_subject.to_string()),
                ..Default::default()
            },
            users::SPACE_SBXMGR,
        ),
    )
    .await
    .expect_err("sandbox-manager must not list template transfers");
    assert_eq!(sandbox_cross_kind.code(), tonic::Code::PermissionDenied);

    VolumeManager::list_transfer_jobs(
        &service,
        service_request(
            ListTransferJobsRequest {
                subject_id: Some(template_subject.to_string()),
                ..Default::default()
            },
            users::SPACE_TMPLMGR,
        ),
    )
    .await
    .expect("template-manager may list template transfers");

    let template_cross_kind = VolumeManager::list_transfer_jobs(
        &service,
        service_request(
            ListTransferJobsRequest {
                subject_id: Some(sandbox_subject.to_string()),
                ..Default::default()
            },
            users::SPACE_TMPLMGR,
        ),
    )
    .await
    .expect_err("template-manager must not list sandbox transfers");
    assert_eq!(template_cross_kind.code(), tonic::Code::PermissionDenied);

    let unscoped = VolumeManager::list_transfer_jobs(
        &service,
        service_request(ListTransferJobsRequest::default(), users::SPACE_SBXMGR),
    )
    .await
    .expect_err("service callers must scope job listings");
    assert_eq!(unscoped.code(), tonic::Code::PermissionDenied);

    VolumeManager::list_transfer_jobs(&service, root_request(ListTransferJobsRequest::default()))
        .await
        .expect("root retains unscoped diagnostic access");
}

#[tokio::test]
async fn pull_artifact_dedup_never_returns_the_other_manager_class_job() {
    use pplx_space_common_svc::core::host::linux::identities::users;

    const SANDBOX_SUBJECT: &str = "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2";
    const TEMPLATE_SUBJECT: &str = "template:seed";
    let temp = tempfile::tempdir().expect("tempdir");
    let service = test_service(&temp).await;

    // A new server must recover a same-class job written with the old
    // subject-agnostic key, while letting the other class submit its own job.
    let legacy_artifact_id = "019e9000-0000-7000-8000-000000000002";
    let legacy_payload = plaintext_artifact_payload(SANDBOX_SUBJECT, legacy_artifact_id);
    let legacy_key = pull_artifact_dedup_key(legacy_artifact_id, MaterializationMode::Disabled);
    let legacy_job = service
        .worker
        .submit(
            PULL_ARTIFACT_KIND,
            &legacy_payload.to_json(),
            Some(SANDBOX_SUBJECT),
            Some(&legacy_key),
        )
        .expect("seed pre-upgrade job");

    let template_job = pull_artifact_as(
        &service,
        TEMPLATE_SUBJECT,
        legacy_artifact_id,
        users::SPACE_TMPLMGR,
    )
    .await
    .expect("template-manager must make progress beside a sandbox legacy job");
    assert_ne!(template_job, legacy_job.to_string());

    let sandbox_job = pull_artifact_as(
        &service,
        SANDBOX_SUBJECT,
        legacy_artifact_id,
        users::SPACE_SBXMGR,
    )
    .await
    .expect("sandbox-manager must recover its legacy job");
    assert_eq!(sandbox_job, legacy_job.to_string());

    let template_retry = pull_artifact_as(
        &service,
        TEMPLATE_SUBJECT,
        legacy_artifact_id,
        users::SPACE_TMPLMGR,
    )
    .await
    .expect("template retry must recover its class-scoped job");
    assert_eq!(template_retry, template_job);

    // Concurrent first submissions from the two manager classes must never
    // converge on one winner.
    let concurrent_artifact_id = "019e9000-0000-7000-8000-000000000003";
    let (sandbox_first, template_first) = tokio::join!(
        pull_artifact_as(
            &service,
            SANDBOX_SUBJECT,
            concurrent_artifact_id,
            users::SPACE_SBXMGR,
        ),
        pull_artifact_as(
            &service,
            TEMPLATE_SUBJECT,
            concurrent_artifact_id,
            users::SPACE_TMPLMGR,
        ),
    );
    let sandbox_first = sandbox_first.expect("sandbox submission");
    let template_first = template_first.expect("template submission");
    assert_ne!(sandbox_first, template_first);

    // Simulate an unauthorized active job remaining the submit winner after
    // recovery rejects it. The post-submit load and authorization must reject
    // it rather than returning its id.
    let winner_artifact_id = "019e9000-0000-7000-8000-000000000004";
    let template_subject = parse_subject(TEMPLATE_SUBJECT).expect("template subject");
    let typed_artifact = parse_artifact_id(winner_artifact_id).expect("artifact id");
    let winner_key = pull_artifact_dedup_key(
        &subject_class_artifact_dedup_key(&template_subject, &typed_artifact),
        MaterializationMode::Disabled,
    );
    let wrong_class_payload = plaintext_artifact_payload(SANDBOX_SUBJECT, winner_artifact_id);
    service
        .worker
        .submit(
            PULL_ARTIFACT_KIND,
            &wrong_class_payload.to_json(),
            Some(SANDBOX_SUBJECT),
            Some(&winner_key),
        )
        .expect("seed concurrent wrong-class winner");

    let error = pull_artifact_as(
        &service,
        TEMPLATE_SUBJECT,
        winner_artifact_id,
        users::SPACE_TMPLMGR,
    )
    .await
    .expect_err("post-submit winner must be re-authorized");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn timed_out_admission_wait_can_continue_until_the_submission_is_durable() {
    let admission = TransferAdmission::open();
    let lease = admission.enter().expect("admit predecessor submission");
    admission.close();

    assert!(
        !admission.wait_for_idle(Duration::from_millis(20)).await,
        "the warning deadline must not manufacture an idle admission"
    );

    let continued_wait = tokio::spawn({
        let admission = admission.clone();
        async move { admission.wait_for_idle(Duration::from_secs(1)).await }
    });
    assert!(
        !continued_wait.is_finished(),
        "ownership must remain blocked after the warning deadline"
    );

    drop(lease);
    assert!(
        continued_wait.await.expect("continued admission waiter"),
        "a later durable-registration boundary must release the waiter"
    );
}

#[tokio::test]
async fn transfer_admission_does_not_lose_final_idle_notification() {
    for _ in 0..1_000 {
        let admission = TransferAdmission::open();
        let lease = admission.enter().expect("admit predecessor submission");
        admission.close();

        let waiter = tokio::spawn({
            let admission = admission.clone();
            async move { admission.wait_for_idle(Duration::from_secs(1)).await }
        });
        tokio::task::yield_now().await;
        drop(lease);

        assert!(waiter.await.expect("idle waiter task"));
    }
}

#[test]
fn parse_subject_accepts_new_kinded_form() {
    let s = parse_subject("template:seed").expect("new kinded form must parse");
    assert!(matches!(s, SubjectId::Template(_)));
    let s = parse_subject("sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2")
        .expect("new kinded form must parse");
    assert!(matches!(s, SubjectId::Sandbox(_)));
}

#[test]
fn parse_subject_accepts_bare_sandbox_uuid() {
    // Sandbox-manager sends a bare sandbox UUID in `subject_id` (same
    // wire bytes the previous vol-mgr image decoded as `sandbox_id`).
    // The RPC boundary MUST accept the bare-UUID form so:
    //  - In-flight transfers from a same-node sbx-mgr pod — old or new
    //    image — succeed.
    //  - A future release that flips sandbox-manager over to the
    //    explicit `"sandbox:<uuid>"` form lands without coordination.
    // Mirrors the executor's `parse_subject_id_with_legacy_fallback`
    // so the gRPC boundary and the SQLite-recovery boundary share the
    // same acceptance rule.
    let s = parse_subject("019d84bd-280d-7331-99c6-b63377f3a3a2")
        .expect("bare sandbox UUID must parse");
    assert!(matches!(s, SubjectId::Sandbox(_)));
}

#[test]
fn parse_subject_rejects_malformed_input() {
    assert!(parse_subject("").is_err(), "empty must fail");
    assert!(
        parse_subject("not-a-uuid-or-prefix").is_err(),
        "non-UUID without prefix must fail"
    );
    assert!(
        parse_subject("unknown:value").is_err(),
        "unknown kind prefix must fail"
    );
    assert!(
        parse_subject("template:..").is_err(),
        "invalid template name must fail"
    );
}

#[test]
fn artifact_mode_graduates_memory_and_reserves_assignment_for_rootfs() {
    let sandbox = parse_subject("sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2").expect("sandbox");
    for rootfs_assignment in [
        MaterializationMode::Disabled,
        MaterializationMode::Shadow,
        MaterializationMode::Enabled,
    ] {
        assert_eq!(
            artifact_materialization_mode(&sandbox, true, false, rootfs_assignment),
            MaterializationMode::Enabled,
            "memory must ignore the rootfs assignment"
        );
        assert_eq!(
            artifact_materialization_mode(&sandbox, false, true, rootfs_assignment),
            rootfs_assignment,
            "rootfs must use its assignment"
        );
        assert_eq!(
            artifact_materialization_mode(&sandbox, true, true, rootfs_assignment),
            MaterializationMode::Enabled,
            "a mixed artifact must keep memory enabled independently of rootfs"
        );
    }
    let template = parse_subject("template:seed").expect("template");
    assert_eq!(
        artifact_materialization_mode(&template, true, false, MaterializationMode::Enabled),
        MaterializationMode::Disabled
    );
}

#[test]
fn rootfs_mode_is_enabled_only_for_sandboxes() {
    let sandbox = parse_subject("sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2").expect("sandbox");
    let template = parse_subject("template:seed").expect("template");

    assert_eq!(
        rootfs_materialization_mode(&sandbox),
        MaterializationMode::Enabled
    );
    assert_eq!(
        rootfs_materialization_mode(&template),
        MaterializationMode::Disabled
    );
}

fn valid_dec_cfg() -> BatchDecryptionConfig {
    BatchDecryptionConfig {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".to_string(),
        provider: "in-memory".to_string(),
        master_key_identifier: "test/key-1".to_string(),
        wrapped_dek: vec![1, 2, 3, 4],
    }
}

#[test]
fn parse_batch_decryption_config_accepts_well_formed() {
    let enc = parse_batch_decryption_config(valid_dec_cfg()).expect("valid cfg");
    assert_eq!(
        enc.organization_id.to_string(),
        "019e31d9-79a6-7132-b90f-007f1c62ddea"
    );
    assert_eq!(enc.provider, KekProvider::InMemory);
    assert_eq!(enc.master_key_identifier.as_str(), "test/key-1");
    assert_eq!(enc.wrapped_dek, vec![1, 2, 3, 4]);
}

#[test]
fn parse_batch_decryption_config_rejects_malformed_org_id() {
    let mut cfg = valid_dec_cfg();
    cfg.organization_id = "not-a-uuid".to_string();
    let err = parse_batch_decryption_config(cfg).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn parse_batch_decryption_config_rejects_unknown_provider() {
    let mut cfg = valid_dec_cfg();
    cfg.provider = "ms-azure-kv".to_string();
    let err = parse_batch_decryption_config(cfg).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn parse_batch_decryption_config_rejects_empty_wrapped_dek() {
    // A zero-length wrapped DEK would unwrap to an empty plaintext and
    // produce a deterministic encrypt result, defeating envelope
    // encryption. Fail loudly at the boundary rather than queueing a job
    // that surfaces as an obscure auth failure on the next dequeue.
    let mut cfg = valid_dec_cfg();
    cfg.wrapped_dek = Vec::new();
    let err = parse_batch_decryption_config(cfg).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn parse_artifact_id_round_trips_and_rejects_garbage() {
    let id = ArtifactId::new();
    assert_eq!(parse_artifact_id(&format!("  {id}  ")).unwrap(), id);
    assert_eq!(
        parse_artifact_id("not-a-uuid").unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn pull_batch_entry_uses_explicit_artifact_instead_of_opaque_role() {
    let payload = pull_batch_entry_payload(
        PullFileBatchEntry {
            s3_uri: "s3://bucket/snapshot?region=us-east-2".to_string(),
            local_path: "/space/cache/opaque-output".to_string(),
            file_mode: None,
            role: "caller-chosen-aad-role".to_string(),
            snapshot_artifact: ProtoSnapshotArtifact::Memory as i32,
        },
        0,
    )
    .unwrap();

    assert_eq!(payload.snapshot_artifact, Some(SnapshotArtifact::Memory));
    assert_eq!(payload.role, "caller-chosen-aad-role");
}

#[test]
fn push_batch_entry_uses_explicit_artifact_instead_of_opaque_role() {
    let payload = push_batch_entry_payload(
        PushFileBatchEntry {
            local_path: "/space/cache/opaque-input".to_string(),
            s3_uri: "s3://bucket/snapshot?region=us-east-2".to_string(),
            role: "caller-chosen-aad-role".to_string(),
            snapshot_artifact: ProtoSnapshotArtifact::Memory as i32,
        },
        2,
    )
    .unwrap();

    assert_eq!(payload.snapshot_artifact, Some(SnapshotArtifact::Memory));
    assert_eq!(payload.role, "caller-chosen-aad-role");
}

#[test]
fn batch_entry_rejects_unknown_explicit_artifact() {
    let error = match pull_batch_entry_payload(
        PullFileBatchEntry {
            snapshot_artifact: i32::MAX,
            ..Default::default()
        },
        3,
    ) {
        Ok(_) => panic!("unknown snapshot artifact must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("files[3].snapshot_artifact"));
    assert!(error.message().contains(&i32::MAX.to_string()));
}

#[test]
fn batch_entry_without_explicit_artifact_keeps_legacy_role_fallback() {
    let payload = pull_batch_entry_payload(
        PullFileBatchEntry {
            role: "memfile".to_string(),
            snapshot_artifact: ProtoSnapshotArtifact::Unspecified as i32,
            ..Default::default()
        },
        0,
    )
    .unwrap();

    assert_eq!(payload.snapshot_artifact, Some(SnapshotArtifact::Memory));
}

#[test]
fn validate_entry_roles_accepts_distinct_and_rejects_duplicate_or_empty() {
    validate_entry_roles(["memfile", "rootfs", "metadata.json"].into_iter())
        .expect("distinct well-formed roles must pass");

    // Duplicate roles would share a per-chunk AAD under the one artifact
    // DEK, making the two entries cryptographically interchangeable.
    let dup = validate_entry_roles(["memfile", "memfile"].into_iter()).unwrap_err();
    assert_eq!(dup.code(), tonic::Code::InvalidArgument);

    let empty = validate_entry_roles(["memfile", ""].into_iter()).unwrap_err();
    assert_eq!(empty.code(), tonic::Code::InvalidArgument);
}

fn enc(wrapped_dek: Vec<u8>) -> TransferEncryption {
    TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek,
    }
}

fn entry(role: &str, s3_uri: &str) -> ArtifactEntryPayload {
    ArtifactEntryPayload {
        snapshot_artifact: snapshot_artifact_from_entry_role(role),
        role: role.to_string(),
        local_path: format!("/space/snap/{role}"),
        s3_uri: s3_uri.to_string(),
        staged_path: None,
        file_mode: None,
    }
}

fn persisted_job(status: &str, payload: &ArtifactPayload) -> pplx_space_common_jobs::Job {
    pplx_space_common_jobs::Job {
        job_id: "019d84bd-280d-7331-99c6-b63377f3a3a3".parse().unwrap(),
        kind: PUSH_ARTIFACT_KIND.to_string(),
        status: status.to_string(),
        payload: payload.to_json(),
        subject_id: Some(payload.subject_id.clone()),
        dedup_key: Some(payload.artifact_id.clone()),
        failure: None,
        attempts: 1,
        max_attempts: 5,
        bytes_processed: 0,
        error: None,
        created_at: String::new(),
        started_at: None,
        completed_at: None,
        traceparent: None,
        lease_owner: None,
        priority: JobPriority::default(),
    }
}

fn sample_artifact() -> ArtifactPayload {
    ArtifactPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        artifact_id: "019e9000-0000-7000-8000-000000000001".to_string(),
        entries: vec![entry("memfile", "s3://b/snap/memfile?region=us-east-2")],
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: Some(enc(vec![1, 2, 3, 4])),
    }
}

#[test]
fn pull_job_failure_status_preserves_typed_reason_and_retry_semantics() {
    let mut job = persisted_job("failed", &sample_artifact());
    job.error = Some("object not found".to_string());

    job.failure = Some(JobFailure::Permanent {
        reason: Some(JobFailureReason::NotFound),
    });
    assert_eq!(pull_job_failure_status(&job).code(), tonic::Code::NotFound);

    job.failure = Some(JobFailure::Permanent {
        reason: Some(JobFailureReason::PermissionDenied),
    });
    assert_eq!(
        pull_job_failure_status(&job).code(),
        tonic::Code::PermissionDenied
    );

    job.failure = Some(JobFailure::Permanent { reason: None });
    assert_eq!(
        pull_job_failure_status(&job).code(),
        tonic::Code::FailedPrecondition
    );

    job.failure = Some(JobFailure::Retriable);
    assert_eq!(
        pull_job_failure_status(&job).code(),
        tonic::Code::Unavailable
    );
}

fn test_worker() -> (
    Arc<Mutex<rusqlite::Connection>>,
    BackgroundWorker<BlockingExecutor>,
) {
    test_worker_with_active_limit(1)
}

fn test_worker_with_active_limit(
    active_job_limit: usize,
) -> (
    Arc<Mutex<rusqlite::Connection>>,
    BackgroundWorker<BlockingExecutor>,
) {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for migration in sqlx::migrate!("../../../../../db/common")
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
    let db = Arc::new(Mutex::new(conn));
    let worker = BackgroundWorker::new(
        Arc::clone(&db),
        BlockingExecutor,
        WorkerConfig::new(1, Duration::from_secs(60), 3)
            .with_kind_concurrency(PULL_KIND, 2)
            .with_kind_active_job_limit(PULL_KIND, active_job_limit)
            .with_kind_priority_bands(PULL_KIND, [PriorityBand::new(JobPriority::new(100), 1, 1)]),
    );
    (db, worker)
}

#[test]
fn transfer_scope_lookup_is_not_hidden_by_newer_terminal_jobs() {
    let (db, _worker) = test_worker();
    let scope = TransferScopeId::new();
    let payload = TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: "/space/cache/snapshot.luks".to_string(),
        s3_uri: "s3://bucket/snapshot.luks?region=us-east-2".to_string(),
        transfer_scope_id: Some(scope),
        snapshot_artifact: Some(SnapshotArtifact::Rootfs),
        staged_path: None,
        raw: true,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::VerifiedReplica,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    };
    let conn = db.lock().unwrap();
    pplx_space_common_jobs::db::insert_job(
        &conn,
        pplx_space_common_jobs::db::InsertJob {
            job_id: &"019d84bd-280d-7331-99c6-b63377f3a3a5".parse().unwrap(),
            kind: PULL_KIND,
            payload: &payload.to_json(),
            subject_id: Some(&payload.subject_id),
            dedup_key: None,
            max_attempts: 3,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();
    conn.execute(
        "UPDATE jobs SET created_at = '2000-01-01 00:00:00' WHERE job_id = '019d84bd-280d-7331-99c6-b63377f3a3a5'",
        [],
    )
    .unwrap();
    conn.execute_batch(
        "WITH RECURSIVE seq(value) AS (
             VALUES(1)
             UNION ALL
             SELECT value + 1 FROM seq WHERE value < 10000
         )
         INSERT INTO jobs (job_id, kind, status, payload, created_at)
         SELECT printf('terminal-%05d', value), 'pull', 'completed', '{}',
                printf('2026-01-01 00:00:%05d', value)
           FROM seq;",
    )
    .unwrap();

    assert_eq!(
        transfer_scope_job_ids(&conn, scope).unwrap(),
        vec![
            "019d84bd-280d-7331-99c6-b63377f3a3a5"
                .parse::<pplx_space_common_jobs::JobId>()
                .unwrap()
        ]
    );
}

#[tokio::test]
async fn submit_pull_does_not_reuse_job_with_different_materialization() {
    let (db, worker) = test_worker_with_active_limit(2);
    let req = PullFileRequest {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: "s3://bucket/snapshots/a/snapshot.luks?region=us-east-2".to_string(),
        local_path: "/space/cache/sandboxes/sbx/snapshot/snapshot.luks".to_string(),
        raw: true,
        file_mode: Some(0o600),
        existing_file_policy: ExistingFilePolicy::VerifiedReplica as i32,
        priority: 0,
        snapshot_artifact: ProtoSnapshotArtifact::Unspecified as i32,
        transfer_scope_id: None,
    };
    let file_mode = Some(FileMode::new(0o600).unwrap());
    let payload = TransferPayload {
        subject_id: req.subject_id.clone(),
        local_path: req.local_path.clone(),
        s3_uri: req.s3_uri.clone(),
        transfer_scope_id: None,
        snapshot_artifact: snapshot_artifact_from_local_path(Path::new(&req.local_path)),
        staged_path: None,
        raw: req.raw,
        file_mode,
        existing_file_policy: PullExistingPolicy::VerifiedReplica,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    }
    .to_json();
    let dk = dedup_key(
        "pull",
        &req.subject_id,
        &req.local_path,
        &req.s3_uri,
        req.raw,
        TransferDedupOptions {
            file_mode,
            existing_file_policy: PullExistingPolicy::VerifiedReplica,
            snapshot_artifact: None,
            materialization_mode: MaterializationMode::Disabled,
            encryption: None,
        },
    );
    {
        let conn = db.lock().unwrap();
        pplx_space_common_jobs::db::insert_job(
            &conn,
            pplx_space_common_jobs::db::InsertJob {
                job_id: &"019d84bd-280d-7331-99c6-b63377f3a3a6".parse().unwrap(),
                kind: "pull",
                payload: &payload,
                subject_id: Some(&req.subject_id),
                dedup_key: Some(&dk),
                max_attempts: 3,
                traceparent: None,
                priority: JobPriority::default(),
            },
        )
        .unwrap();
    }

    let returned = submit_pull(&worker, &req, MaterializationMode::Enabled).unwrap();

    assert_ne!(
        returned.to_string(),
        "019d84bd-280d-7331-99c6-b63377f3a3a6",
        "an enabled pull must not reuse an in-flight disabled pull"
    );
    let conn = db.lock().unwrap();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count, 2,
        "different materialization postconditions need distinct jobs"
    );
    let persisted: String = conn
        .query_row(
            "SELECT payload FROM jobs WHERE job_id = ?1",
            ["019d84bd-280d-7331-99c6-b63377f3a3a6"],
            |row| row.get(0),
        )
        .unwrap();
    let persisted = TransferPayload::from_json(&persisted).unwrap();
    assert_eq!(
        persisted.materialization_mode,
        MaterializationMode::Disabled,
        "a later assignment must not mutate a queued job"
    );
}

#[tokio::test]
async fn pull_artifact_does_not_reuse_job_with_different_materialization() {
    let (db, worker) = test_worker_with_active_limit(2);
    let mut disabled = sample_artifact();
    disabled.encryption = None;
    let disabled_key =
        pull_artifact_dedup_key(&disabled.artifact_id, MaterializationMode::Disabled);
    let disabled_job = worker
        .submit(
            PULL_ARTIFACT_KIND,
            &disabled.to_json(),
            Some(&disabled.subject_id),
            Some(&disabled_key),
        )
        .unwrap();

    let mut shadow = disabled.clone();
    shadow.materialization_mode = MaterializationMode::Shadow;
    let shadow_key = pull_artifact_dedup_key(&shadow.artifact_id, MaterializationMode::Shadow);
    let shadow_job = worker
        .submit(
            PULL_ARTIFACT_KIND,
            &shadow.to_json(),
            Some(&shadow.subject_id),
            Some(&shadow_key),
        )
        .unwrap();

    let mut enabled = disabled.clone();
    enabled.materialization_mode = MaterializationMode::Enabled;
    let enabled_key = pull_artifact_dedup_key(&enabled.artifact_id, MaterializationMode::Enabled);
    let enabled_job = worker
        .submit(
            PULL_ARTIFACT_KIND,
            &enabled.to_json(),
            Some(&enabled.subject_id),
            Some(&enabled_key),
        )
        .unwrap();

    assert_eq!(
        disabled_key, disabled.artifact_id,
        "disabled artifact pulls must preserve the legacy dedup key"
    );
    assert_ne!(
        shadow_job, disabled_job,
        "a shadow artifact pull must not reuse an in-flight disabled pull"
    );
    assert_ne!(
        enabled_job, disabled_job,
        "an enabled artifact pull must not reuse an in-flight disabled pull"
    );
    assert_ne!(
        enabled_job, shadow_job,
        "enabled and shadow artifact pulls have different postconditions"
    );
    let count: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count, 3,
        "different artifact materialization postconditions need distinct jobs"
    );
}

#[tokio::test]
async fn pull_artifact_mode_change_still_rejects_divergent_retry() {
    let (_db, worker) = test_worker();
    let mut disabled = sample_artifact();
    disabled.encryption = None;
    let disabled_key =
        pull_artifact_dedup_key(&disabled.artifact_id, MaterializationMode::Disabled);
    worker
        .submit(
            PULL_ARTIFACT_KIND,
            &disabled.to_json(),
            Some(&disabled.subject_id),
            Some(&disabled_key),
        )
        .unwrap();

    let divergent = vec![entry("memfile", "s3://b/snap/OTHER?region=us-east-2")];
    let error = classify_pull_artifact_retry(
        &worker,
        ArtifactRetryIdentity::legacy(&disabled.artifact_id),
        MaterializationMode::Shadow,
        &divergent,
        None,
        None,
        |_| true,
    )
    .expect_err("mode changes must not bypass artifact-id retry validation");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn pull_artifact_retry_validates_other_modes_before_reusing_current_mode() {
    let (_db, worker) = test_worker_with_active_limit(2);
    let mut disabled = sample_artifact();
    disabled.encryption = None;
    let disabled_key =
        pull_artifact_dedup_key(&disabled.artifact_id, MaterializationMode::Disabled);
    worker
        .submit(
            PULL_ARTIFACT_KIND,
            &disabled.to_json(),
            Some(&disabled.subject_id),
            Some(&disabled_key),
        )
        .unwrap();

    let mut divergent_shadow = disabled.clone();
    divergent_shadow.materialization_mode = MaterializationMode::Shadow;
    divergent_shadow.entries = vec![entry("memfile", "s3://b/snap/OTHER?region=us-east-2")];
    let shadow_key =
        pull_artifact_dedup_key(&divergent_shadow.artifact_id, MaterializationMode::Shadow);
    worker
        .submit(
            PULL_ARTIFACT_KIND,
            &divergent_shadow.to_json(),
            Some(&divergent_shadow.subject_id),
            Some(&shadow_key),
        )
        .unwrap();

    let error = classify_pull_artifact_retry(
        &worker,
        ArtifactRetryIdentity::legacy(&disabled.artifact_id),
        MaterializationMode::Disabled,
        &disabled.entries,
        None,
        None,
        |_| true,
    )
    .expect_err("a current-mode hit must not hide a divergent artifact mode");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn pull_artifact_retry_only_reuses_the_requested_materialization_mode() {
    let (_db, worker) = test_worker();
    let mut disabled = sample_artifact();
    disabled.encryption = None;
    let disabled_key =
        pull_artifact_dedup_key(&disabled.artifact_id, MaterializationMode::Disabled);
    let disabled_job = worker
        .submit(
            PULL_ARTIFACT_KIND,
            &disabled.to_json(),
            Some(&disabled.subject_id),
            Some(&disabled_key),
        )
        .unwrap();

    assert!(
        classify_pull_artifact_retry(
            &worker,
            ArtifactRetryIdentity::legacy(&disabled.artifact_id),
            MaterializationMode::Shadow,
            &disabled.entries,
            None,
            None,
            |_| true,
        )
        .unwrap()
        .is_none(),
        "an equivalent artifact with a different layout contract must not be reused"
    );
    assert_eq!(
        classify_pull_artifact_retry(
            &worker,
            ArtifactRetryIdentity::legacy(&disabled.artifact_id),
            MaterializationMode::Disabled,
            &disabled.entries,
            None,
            None,
            |_| true,
        )
        .unwrap()
        .map(|job| job.job_id),
        Some(disabled_job),
        "an equivalent artifact with the requested layout contract remains idempotent"
    );
}

#[tokio::test]
async fn pull_artifact_retry_blocks_incompatible_legacy_live_job() {
    let (db, worker) = test_worker();
    let mut legacy = sample_artifact();
    legacy.encryption = None;
    legacy.entries = vec![entry("rootfs", "s3://b/snap/rootfs?region=us-east-2")];
    legacy.materialization_mode = MaterializationMode::Enabled;
    let legacy_key = pull_artifact_dedup_key(&legacy.artifact_id, MaterializationMode::Enabled);
    worker
        .submit(
            PULL_ARTIFACT_KIND,
            &legacy.to_json(),
            Some(&legacy.subject_id),
            Some(&legacy_key),
        )
        .unwrap();
    let scoped_assignment = MaterializationAssignment {
        mode: MaterializationMode::Enabled,
        rootfs_mode: Some(MaterializationMode::Enabled),
    };

    let error = classify_pull_artifact_retry(
        &worker,
        ArtifactRetryIdentity::legacy(&legacy.artifact_id),
        scoped_assignment,
        &legacy.entries,
        None,
        None,
        |_| true,
    )
    .expect_err("a scoped retry must not enqueue beside an incompatible legacy live job");

    assert_eq!(error.code(), tonic::Code::Unavailable);
    let count: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1, "the retry must not enqueue duplicate work");
}

#[tokio::test]
async fn pull_artifact_retry_reuses_compatible_legacy_dense_job() {
    let (_db, worker) = test_worker();
    let mut legacy = sample_artifact();
    legacy.encryption = None;
    legacy.entries = vec![entry("rootfs", "s3://b/snap/rootfs?region=us-east-2")];
    let legacy_key = pull_artifact_dedup_key(&legacy.artifact_id, MaterializationMode::Disabled);
    let legacy_job = worker
        .submit(
            PULL_ARTIFACT_KIND,
            &legacy.to_json(),
            Some(&legacy.subject_id),
            Some(&legacy_key),
        )
        .unwrap();
    let scoped_assignment = MaterializationAssignment {
        mode: MaterializationMode::Disabled,
        rootfs_mode: Some(MaterializationMode::Disabled),
    };

    let reused = classify_pull_artifact_retry(
        &worker,
        ArtifactRetryIdentity::legacy(&legacy.artifact_id),
        scoped_assignment,
        &legacy.entries,
        None,
        None,
        |_| true,
    )
    .unwrap()
    .expect("a dense scoped retry is compatible with the legacy dense job");

    assert_eq!(reused.job_id, legacy_job);
}

#[tokio::test]
async fn pull_artifact_retry_allows_scoped_job_after_incompatible_legacy_completes() {
    let (db, worker) = test_worker();
    let mut legacy = sample_artifact();
    legacy.encryption = None;
    legacy.entries = vec![entry("rootfs", "s3://b/snap/rootfs?region=us-east-2")];
    legacy.materialization_mode = MaterializationMode::Enabled;
    let legacy_key = pull_artifact_dedup_key(&legacy.artifact_id, MaterializationMode::Enabled);
    let legacy_job = worker
        .submit(
            PULL_ARTIFACT_KIND,
            &legacy.to_json(),
            Some(&legacy.subject_id),
            Some(&legacy_key),
        )
        .unwrap();
    {
        let conn = db.lock().unwrap();
        assert!(pplx_space_common_jobs::db::mark_job_running(&conn, &legacy_job).unwrap());
        assert!(pplx_space_common_jobs::db::complete_job(&conn, &legacy_job, 0).unwrap());
    }
    let scoped_assignment = MaterializationAssignment {
        mode: MaterializationMode::Enabled,
        rootfs_mode: Some(MaterializationMode::Enabled),
    };

    assert!(
        classify_pull_artifact_retry(
            &worker,
            ArtifactRetryIdentity::legacy(&legacy.artifact_id),
            scoped_assignment,
            &legacy.entries,
            None,
            None,
            |_| true,
        )
        .unwrap()
        .is_none(),
        "an incompatible legacy job must stop blocking once it is terminal"
    );
}

#[tokio::test]
async fn submit_pull_persists_explicit_snapshot_artifact_for_opaque_path() {
    let (_db, worker) = test_worker();
    let req = PullFileRequest {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: "s3://bucket/snapshots/a/memfile?region=us-east-2".to_string(),
        local_path: "/space/cache/replacements/download-7b47".to_string(),
        snapshot_artifact: ProtoSnapshotArtifact::Memory as i32,
        ..Default::default()
    };

    let job_id = submit_pull(&worker, &req, MaterializationMode::Disabled).unwrap();
    let job = worker.get_job(&job_id).unwrap().unwrap();
    let payload = TransferPayload::from_json(&job.payload).unwrap();

    assert_eq!(payload.snapshot_artifact, Some(SnapshotArtifact::Memory));
}

#[tokio::test]
async fn submit_pull_persists_and_versions_rootfs_assignment() {
    let (_db, worker) = test_worker();
    let req = PullFileRequest {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: "s3://bucket/snapshots/a/rootfs?region=us-east-2".to_string(),
        local_path: "/space/cache/sandboxes/sbx/snapshot/rootfs".to_string(),
        snapshot_artifact: ProtoSnapshotArtifact::Unspecified as i32,
        ..Default::default()
    };
    let assignment = MaterializationAssignment {
        mode: MaterializationMode::Enabled,
        rootfs_mode: Some(MaterializationMode::Enabled),
    };

    let job_id = submit_pull(&worker, &req, assignment).unwrap();
    let job = worker.get_job(&job_id).unwrap().unwrap();
    let payload = TransferPayload::from_json(&job.payload).unwrap();
    assert_eq!(
        payload.rootfs_materialization_mode,
        Some(MaterializationMode::Enabled)
    );

    let legacy_key = dedup_key(
        PULL_KIND,
        &req.subject_id,
        &req.local_path,
        &req.s3_uri,
        req.raw,
        TransferDedupOptions {
            materialization_mode: MaterializationMode::Enabled,
            ..Default::default()
        },
    );
    assert!(
        worker
            .find_job_by_dedup_key(PULL_KIND, &legacy_key)
            .unwrap()
            .is_none(),
        "a rootfs request must not reuse a job keyed before scoped assignments existed"
    );
}

#[tokio::test]
async fn submit_pull_preserves_unspecified_artifact_for_canonical_path() {
    let (_db, worker) = test_worker();
    let req = PullFileRequest {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: "s3://bucket/snapshots/a/snapshot.luks?region=us-east-2".to_string(),
        local_path: "/space/cache/sandboxes/sbx/snapshot/snapshot.luks".to_string(),
        snapshot_artifact: ProtoSnapshotArtifact::Unspecified as i32,
        ..Default::default()
    };

    let job_id = submit_pull(&worker, &req, MaterializationMode::Disabled).unwrap();
    let job = worker.get_job(&job_id).unwrap().unwrap();
    let payload = TransferPayload::from_json(&job.payload).unwrap();

    assert_eq!(payload.snapshot_artifact, None);
}

#[tokio::test]
async fn protected_priority_pull_is_admitted_when_default_band_is_full() {
    let (db, worker) = test_worker();
    let request = |name: &str, priority: i32| PullFileRequest {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: format!("s3://bucket/snapshots/a/{name}?region=us-east-2"),
        local_path: format!("/space/cache/sandboxes/sbx/snapshot/{name}"),
        priority,
        ..Default::default()
    };

    let first_normal = submit_pull(
        &worker,
        &request("memory", 0),
        MaterializationMode::Disabled,
    );
    let second_normal = submit_pull(
        &worker,
        &request("rootfs", 0),
        MaterializationMode::Disabled,
    );
    let reserved = submit_pull(
        &worker,
        &request("control.json", 100),
        MaterializationMode::Disabled,
    );

    first_normal.unwrap();
    assert_eq!(
        second_normal.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    let reserved = reserved.unwrap();
    assert_eq!(
        db.lock()
            .unwrap()
            .query_row(
                "SELECT priority FROM jobs WHERE job_id = ?1",
                [reserved],
                |row| row.get::<_, i32>(0),
            )
            .unwrap(),
        100
    );
}

#[test]
fn classify_artifact_retry_reuses_equivalent_live_job() {
    let art = sample_artifact();
    let kek = art.encryption.as_ref().unwrap();
    let kek_id = Some((
        &kek.organization_id,
        &kek.provider,
        &kek.master_key_identifier,
    ));
    // Same entries + KEK identity, push side (no wrap supplied) → reuse.
    let out = classify_artifact_retry(
        persisted_job("running", &art),
        &art.artifact_id,
        &art.entries,
        kek_id,
        None,
    )
    .expect("equivalent retry must not error")
    .expect("equivalent retry must reuse the existing job");
    assert_eq!(
        out.0.job_id.to_string(),
        "019d84bd-280d-7331-99c6-b63377f3a3a3"
    );
}

#[test]
fn classify_artifact_retry_mints_fresh_when_prior_attempt_failed() {
    let art = sample_artifact();
    // A terminally-failed prior attempt must NOT be returned as success.
    let out = classify_artifact_retry(
        persisted_job("failed", &art),
        &art.artifact_id,
        &art.entries,
        None,
        None,
    )
    .expect("failed prior attempt is not an error");
    assert!(out.is_none(), "failed prior attempt must mint a fresh job");
}

#[test]
fn classify_artifact_retry_rejects_divergent_entries() {
    let art = sample_artifact();
    let kek = art.encryption.as_ref().unwrap();
    let kek_id = Some((
        &kek.organization_id,
        &kek.provider,
        &kek.master_key_identifier,
    ));
    // Same artifact_id, different S3 destination for the entry → reject.
    let diverged = vec![entry("memfile", "s3://b/snap/OTHER?region=us-east-2")];
    match classify_artifact_retry(
        persisted_job("running", &art),
        &art.artifact_id,
        &diverged,
        kek_id,
        None,
    ) {
        Err(e) => assert_eq!(e.code(), tonic::Code::FailedPrecondition),
        Ok(_) => panic!("divergent entries must be rejected, not reused"),
    }
}

#[test]
fn artifact_retry_rejects_divergent_semantic_artifact() {
    let art = sample_artifact();
    let mut diverged = art.entries.clone();
    diverged[0].snapshot_artifact = Some(SnapshotArtifact::VmState);
    let encryption = art.encryption.as_ref().unwrap();

    assert!(!artifact_request_matches(
        &art,
        &diverged,
        Some((
            &encryption.organization_id,
            &encryption.provider,
            &encryption.master_key_identifier,
        )),
        None,
    ));
}

#[test]
fn artifact_retry_accepts_legacy_missing_identity_when_role_is_canonical() {
    let mut persisted = sample_artifact();
    persisted.entries[0].snapshot_artifact = None;
    let requested = sample_artifact();
    let encryption = persisted.encryption.as_ref().unwrap();

    assert!(artifact_request_matches(
        &persisted,
        &requested.entries,
        Some((
            &encryption.organization_id,
            &encryption.provider,
            &encryption.master_key_identifier,
        )),
        None,
    ));
}

#[test]
fn artifact_retry_rejects_missing_identity_for_opaque_role() {
    let mut persisted = sample_artifact();
    persisted.entries[0].role = "opaque-aad-role".to_string();
    persisted.entries[0].snapshot_artifact = None;
    let mut requested = persisted.entries.clone();
    requested[0].snapshot_artifact = Some(SnapshotArtifact::Memory);
    let encryption = persisted.encryption.as_ref().unwrap();

    assert!(!artifact_request_matches(
        &persisted,
        &requested,
        Some((
            &encryption.organization_id,
            &encryption.provider,
            &encryption.master_key_identifier,
        )),
        None,
    ));
}

#[test]
fn classify_artifact_retry_rejects_divergent_wrap_on_pull() {
    let art = sample_artifact();
    let kek = art.encryption.as_ref().unwrap();
    let kek_id = Some((
        &kek.organization_id,
        &kek.provider,
        &kek.master_key_identifier,
    ));
    // Pull supplies the wrap; a mismatch against the persisted wrap (e.g. a
    // stale id reused with a different DEK) must surface, not silently
    // execute the persisted wrap.
    match classify_artifact_retry(
        persisted_job("completed", &art),
        &art.artifact_id,
        &art.entries,
        kek_id,
        Some(&[9, 9, 9, 9]),
    ) {
        Err(e) => assert_eq!(e.code(), tonic::Code::FailedPrecondition),
        Ok(_) => panic!("wrap mismatch must be rejected, not reused"),
    }

    // Matching wrap → reuse.
    assert!(
        classify_artifact_retry(
            persisted_job("completed", &art),
            &art.artifact_id,
            &art.entries,
            kek_id,
            Some(&[1, 2, 3, 4]),
        )
        .unwrap()
        .is_some()
    );
}
