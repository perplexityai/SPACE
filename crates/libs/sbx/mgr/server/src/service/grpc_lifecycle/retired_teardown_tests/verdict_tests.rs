//! Recovering the cluster's superseded verdict when it reached the teardown
//! drive through a failed upload job, including off the sandbox's node.

use super::*;

/// A node the sandbox has moved off can't see the sandbox through the
/// placement-filtered storage lookup, but its retired execution's refused
/// backup still probes registration, learns it was superseded, and releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_execution_off_its_node_still_releases() {
    with_cluster_api(RETIRING, async || {
        let refused = refused_teardown_with(|staged| {
            serde_json::to_string(&bound_upload_payload(staged)).unwrap()
        })
        .await;
        let service = &refused.service;
        tokio::time::timeout(
            Duration::from_secs(5),
            service.settle_teardown_suspend_drive(
                refused.sandbox_id,
                generation(GENERATION),
                Err(refused.error.clone()),
            ),
        )
        .await
        .expect("the release should complete")
        .expect("a retired execution's teardown must be released");
        assert_eq!(
            *refused.dispatched.lock().unwrap(),
            vec![(refused.sandbox_id.to_string(), false)],
            "the resident runtime must be torn down"
        );
        assert_eq!(
            poll_outcome(service, refused.sandbox_id).await,
            TeardownSuspendOutcome::Deleted
        );
        drain(service).await;
    })
    .await;
}

/// The drive can meet the refusal itself, while the upload job it enqueued is
/// still live. On a node the sandbox has moved off, the refusal's own verdict
/// releases the runtime on that first drive, though the job can't be probed
/// yet and the storage lookup can't see the sandbox, and cancels the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_refusal_off_its_node_releases_on_the_first_drive() {
    with_cluster_api(RETIRING, async || {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let staged =
            insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
        let upload_job = insert_upload_job(
            &conn,
            &sandbox_id.to_string(),
            staged,
            &serde_json::to_string(&bound_upload_payload(staged)).unwrap(),
        );
        let (service, dispatched) = service_recording_cleanup(conn);
        let error = super::super::teardown_artifact_backup_status(
            super::super::ArtifactBackupSubmissionError::Begin {
                error: pplx_space_common_jobs::JobError::permanent(
                    "cluster-api rejected begin_artifact_backup (FailedPrecondition): retired",
                ),
                superseded_generation: Some(generation(GENERATION)),
            },
            staged,
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            service.settle_teardown_suspend_drive(sandbox_id, generation(GENERATION), Err(error)),
        )
        .await
        .expect("the release should complete")
        .expect("a retired execution's teardown must be released");
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec![(sandbox_id.to_string(), false)],
            "the resident runtime must be torn down"
        );
        let upload = {
            let db = service.db.lock().unwrap();
            pplx_space_common_jobs::db::get_job(db.raw(), &upload_job)
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            upload.status,
            pplx_space_common_jobs::JobStatus::Cancelled.as_str(),
            "the discarded capture's upload must not register it later"
        );
        assert_eq!(
            poll_outcome(&service, sandbox_id).await,
            TeardownSuspendOutcome::Deleted
        );
        drain(&service).await;
    })
    .await;
}

/// A probe cluster-api can't answer is no verdict, and the lookup that can't
/// see a sandbox placed on another node is no proof it's gone: the teardown
/// stays running, nothing is released, and the next drive asks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanswered_probe_off_its_node_keeps_the_teardown_running() {
    with_cluster_api(
        RetiringClusterApi {
            unavailable: true,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                service.settle_teardown_suspend_drive(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(refused.error.clone()),
                ),
            )
            .await
            .expect("the drive should settle");
            assert!(result.is_err(), "the drive must stay failed");
            assert!(refused.dispatched.lock().unwrap().is_empty());
            let status = {
                let db = service.db.lock().unwrap();
                crate::db::latest_teardown_suspend_operation(&db, &refused.sandbox_id.to_string())
                    .unwrap()
                    .unwrap()
                    .status
            };
            assert_eq!(
                status,
                crate::db::TeardownSuspendOperationStatus::Running,
                "an unanswered probe must not close the teardown"
            );
            drain(service).await;
        },
    )
    .await;
}

/// The re-sent registration carries the refused generation back across the
/// wire, to the probe and to a direct registration: a generation-bound upload
/// reads as superseded at exactly its own generation, and an unbound one,
/// which the cluster admits, reads as not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resent_registration_recovers_the_superseded_generation() {
    with_cluster_api(RETIRING, async || {
        let artifact: LocalArtifactId = "01939e3e-0000-7000-8000-0000000000bb".parse().unwrap();
        let mut payload = bound_upload_payload(artifact);
        assert_eq!(
            crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload).await,
            RegistrationProbe::Superseded(generation(GENERATION))
        );
        let refusal =
            crate::snapshot_upload_jobs::begin_prepared_upload_naming_supersession(&payload)
                .await
                .expect_err("the cluster refuses a superseded registration");
        assert_eq!(refusal.superseded_generation, Some(generation(GENERATION)));
        assert_eq!(
            refusal.error.kind(),
            pplx_space_common_jobs::JobFailureKind::Permanent
        );
        payload.execution_generation = None;
        assert_eq!(
            crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload).await,
            RegistrationProbe::NotSuperseded
        );
    })
    .await;
}

/// The teardown driver's probe asks for the registration's verdict rather than
/// registering again: a cluster that refuses only verdict-only requests still
/// names the superseded generation to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_probe_asks_for_a_verdict_only() {
    let verdict_refusing = RetiringClusterApi {
        refuses_only_verdicts: true,
        ..RETIRING
    };
    with_cluster_api(verdict_refusing, async || {
        let artifact: LocalArtifactId = "01939e3e-0000-7000-8000-0000000000bc".parse().unwrap();
        assert_eq!(
            crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(
                &bound_upload_payload(artifact)
            )
            .await,
            RegistrationProbe::Superseded(generation(GENERATION))
        );
    })
    .await;
}

/// A teardown whose capture the cluster refused as superseded, its upload
/// failed and its row marked as permanently refused, after one more drive's
/// `ensure_teardown_suspend_backup` against `cluster`.
struct RedrivenRefusal {
    service: SandboxManagerService,
    sandbox_id: SandboxId,
    refused: JobId,
    /// The newest upload under the artifact's key after the drive.
    newest: JobId,
    /// The drive's result, or `None` while its new upload still runs after
    /// a few seconds: the test serves no vol-mgr.
    result: Option<Result<(), tonic::Status>>,
    dispatched: Dispatched,
    _snapshot_root: tempfile::TempDir,
}

async fn redrive_refusal() -> RedrivenRefusal {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let refused = insert_upload_job(
        &conn,
        &sandbox_id.to_string(),
        staged,
        &serde_json::to_string(&bound_upload_payload(staged)).unwrap(),
    );
    crate::snapshot_upload_jobs::record_failure_scope(
        &conn,
        &refused,
        crate::snapshot_upload::UploadFailureScope::Artifact,
    )
    .unwrap();
    pplx_space_common_jobs::db::fail_job_permanent(
        conn.raw(),
        &refused,
        "cluster-api rejected begin_artifact_backup (FailedPrecondition): execution retired",
        None,
    )
    .unwrap();
    assert!(
        crate::db::mark_teardown_suspend_backup_permanently_failed(
            &conn,
            &sandbox_id.to_string(),
            staged
        )
        .unwrap()
    );
    // A new attempt prepares its upload from the capture on disk.
    let snapshot_root = tempfile::tempdir().unwrap();
    let snapshot_dir = snapshot_root.path().join("suspend");
    crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
        &snapshot_dir,
        &sandbox_id.to_string(),
        staged,
        b"refused-capture",
    );
    conn.execute(
        "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
        crate::db::fixture::params![snapshot_dir.to_str().unwrap(), staged.to_string()],
    )
    .unwrap();
    let row = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
        .unwrap()
        .unwrap();
    let (service, dispatched) = service_recording_cleanup(conn);
    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: staged,
        sandbox_id: row.id.clone(),
        artifact_kind: ArtifactKind::Full,
        artifact_origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        local_path: snapshot_dir.to_str().unwrap().to_string(),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        service.ensure_teardown_suspend_backup(
            &row,
            TeardownSuspendBackup {
                execution_generation: generation(GENERATION),
                artifact: &artifact,
                format: SuspendSnapshotFormat::Full,
                origin: super::super::TeardownBackupOrigin::Captured,
            },
        ),
    )
    .await
    .ok();
    let newest = service
        .snapshot_upload_worker
        .find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &crate::snapshot_upload_jobs::artifact_backup_dedup_key(staged),
        )
        .unwrap()
        .unwrap()
        .job_id;
    RedrivenRefusal {
        service,
        sandbox_id,
        refused,
        newest,
        result,
        dispatched,
        _snapshot_root: snapshot_root,
    }
}

/// Whether the sandbox's row carries the terminal teardown-backup marker.
fn marked_permanently_refused(service: &SandboxManagerService, sandbox_id: SandboxId) -> bool {
    let row = crate::db::get_sandbox(&service.db.lock().unwrap(), &sandbox_id.to_string())
        .unwrap()
        .expect("the sandbox row stays");
    row.failure_reason.as_deref()
        == Some(pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON)
}

/// The cluster refused this teardown's capture as superseded, and every
/// execution that superseded it then ended without running, so the cluster
/// now admits the capture. The next drive asks again before rejoining the
/// refused upload and registers the capture with a new one, instead of
/// replaying the refusal and holding the runtime for good, and the row is no
/// longer marked as permanently refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lapsed_supersession_registers_the_capture_again() {
    let lapsed = RetiringClusterApi {
        supersession_lapsed: true,
        ..RETIRING
    };
    with_cluster_api(lapsed, async || {
        let drive = redrive_refusal().await;
        assert_ne!(
            drive.newest, drive.refused,
            "the drive must start a new upload, not rejoin the refused one"
        );
        let newest = drive
            .service
            .snapshot_upload_worker
            .get_job(&drive.newest)
            .unwrap()
            .unwrap()
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .unwrap();
        assert_eq!(
            newest.execution_generation,
            Some(generation(GENERATION)),
            "the new attempt registers under the generation, so the fence runs on it too"
        );
        assert!(drive.dispatched.lock().unwrap().is_empty());
        assert!(
            !marked_permanently_refused(&drive.service, drive.sandbox_id),
            "a refusal that no longer holds mustn't leave the teardown marked terminal"
        );
        drain(&drive.service).await;
    })
    .await;
}

/// The probe can be admitted while the registration itself is still refused,
/// as when a newer execution starts between them. The new attempt's refusal
/// reaches the settle with its superseded generation, which releases the
/// retired runtime instead of registering its capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_new_attempt_after_an_admitted_probe_releases() {
    let racing = RetiringClusterApi {
        refuses_only_registrations: true,
        ..RETIRING
    };
    with_cluster_api(racing, async || {
        let drive = redrive_refusal().await;
        assert_ne!(
            drive.newest, drive.refused,
            "the admitted probe starts a new upload"
        );
        let error = drive
            .result
            .expect("the new attempt's registration is refused at once")
            .expect_err("a refused registration can't make the suspend durable");
        tokio::time::timeout(
            Duration::from_secs(5),
            drive.service.settle_teardown_suspend_drive(
                drive.sandbox_id,
                generation(GENERATION),
                Err(error),
            ),
        )
        .await
        .expect("the release should complete")
        .expect("a retired execution's teardown must be released");
        assert_eq!(
            *drive.dispatched.lock().unwrap(),
            vec![(drive.sandbox_id.to_string(), false)],
            "the resident runtime must be torn down"
        );
        drain(&drive.service).await;
    })
    .await;
}

/// A server that predates verdict-only requests registers the probe instead
/// of answering it. Its admission doesn't come from the fence, so the drive
/// rejoins the refused upload instead of starting one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registered_probe_starts_no_new_upload() {
    let predating = RetiringClusterApi {
        supersession_lapsed: true,
        predates_verdicts: true,
        ..RETIRING
    };
    with_cluster_api(predating, async || {
        let drive = redrive_refusal().await;
        assert_eq!(drive.newest, drive.refused);
        assert!(
            marked_permanently_refused(&drive.service, drive.sandbox_id),
            "the refusal still stands"
        );
        drain(&drive.service).await;
    })
    .await;
}

/// The fence doesn't run on a replay of an attempt that has a row, such as one
/// a server that predated verdict-only requests registered for an earlier
/// probe, or on bytes another attempt registered. Such an admission says
/// nothing about whether a new attempt would be fenced, so the drive rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_admission_without_the_fence_starts_no_new_upload() {
    let unfenced = RetiringClusterApi {
        supersession_lapsed: true,
        admits_unfenced: true,
        ..RETIRING
    };
    with_cluster_api(unfenced, async || {
        let drive = redrive_refusal().await;
        assert_eq!(drive.newest, drive.refused);
        assert!(marked_permanently_refused(&drive.service, drive.sandbox_id));
        drain(&drive.service).await;
    })
    .await;
}

/// A refusal of the artifact doesn't spend the attempts an attempt's own
/// permanent failures get: the cluster can stop refusing, and the upload
/// that follows still gets its recovery attempt.
#[tokio::test]
async fn an_artifact_refusal_leaves_the_attempt_budget_whole() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let mut newest = None;
    for scope in [
        crate::snapshot_upload::UploadFailureScope::Artifact,
        crate::snapshot_upload::UploadFailureScope::Attempt,
    ] {
        let job_id = insert_upload_job(&conn, &sandbox_id.to_string(), staged, "{}");
        crate::snapshot_upload_jobs::record_failure_scope(&conn, &job_id, scope).unwrap();
        pplx_space_common_jobs::db::fail_job_permanent(conn.raw(), &job_id, "refused", None)
            .unwrap();
        newest = Some(job_id);
    }
    let service = SandboxManagerService::for_test(conn);
    let job = service
        .snapshot_upload_worker
        .find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &crate::snapshot_upload_jobs::artifact_backup_dedup_key(staged),
        )
        .unwrap()
        .unwrap();
    assert_eq!(Some(job.job_id), newest);
    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: staged,
        sandbox_id: sandbox_id.to_string(),
        artifact_kind: ArtifactKind::Full,
        artifact_origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        local_path: "/var/lib/space/sandboxes/s/suspend".to_string(),
    };
    let backup = TeardownSuspendBackup {
        execution_generation: generation(GENERATION),
        artifact: &artifact,
        format: SuspendSnapshotFormat::Full,
        origin: super::super::TeardownBackupOrigin::Captured,
    };
    assert_eq!(
        service.teardown_use_of_upload(&backup, &job).unwrap(),
        super::super::KeyedUploadUse::NewAttempt,
        "only the attempt's own failure counts against its budget"
    );
    // A payload the count can't read is an attempt's failure, not an error.
    let unreadable = {
        let db = service.db.lock().unwrap();
        insert_upload_job(&db, &sandbox_id.to_string(), staged, "not json")
    };
    pplx_space_common_jobs::db::fail_job_permanent(
        service.db.lock().unwrap().raw(),
        &unreadable,
        "refused",
        None,
    )
    .unwrap();
    assert_eq!(
        service.teardown_use_of_upload(&backup, &job).unwrap(),
        super::super::KeyedUploadUse::Join,
        "two attempts' permanent failures spend the budget"
    );
}

/// Clearing the terminal teardown-backup marker tells a row that still stages
/// the artifact as `suspending` apart from one that moved on, such as one a
/// cluster-deleted eviction claimed while the probe was out.
#[test]
fn clearing_the_marker_tells_a_row_that_moved_on_apart() {
    use crate::db::TeardownBackupMarkerClear::{Cleared, MovedOn, Unmarked};
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let clear =
        || crate::db::clear_teardown_suspend_backup_permanently_failed(&conn, &id, staged).unwrap();
    assert!(
        crate::db::mark_teardown_suspend_backup_permanently_failed(&conn, &id, staged).unwrap()
    );
    assert_eq!(clear(), Cleared);
    assert_eq!(clear(), Unmarked);
    assert_eq!(
        crate::db::clear_teardown_suspend_backup_permanently_failed(
            &conn,
            &id,
            LocalArtifactId::new()
        )
        .unwrap(),
        MovedOn,
        "a row staging another artifact isn't this capture's"
    );
    conn.execute(
        "UPDATE sandboxes SET state = 'deleting' WHERE id = ?1",
        crate::db::fixture::params![id],
    )
    .unwrap();
    assert_eq!(clear(), MovedOn);
}
