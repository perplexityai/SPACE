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

/// A drive whose re-ask went unanswered carries no answer the settle can
/// trust, so the settle asks again and releases the retired execution then.
/// An answer the drive did get stands: the settle doesn't ask twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_drive_verdict_is_asked_again_at_the_settle() {
    with_cluster_api(RETIRING, async || {
        for (carried, released) in [
            (RegistrationProbe::Unanswered, true),
            (RegistrationProbe::NotSuperseded, false),
        ] {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let error = super::super::teardown_suspend::teardown_backup_verdict_status(
                super::super::teardown_artifact_backup_status(
                    super::super::ArtifactBackupSubmissionError::Begin {
                        error: pplx_space_common_jobs::JobError::permanent(
                            "cluster-api rejected begin_artifact_backup (FailedPrecondition)",
                        ),
                        superseded_generation: None,
                    },
                    refused.staged,
                ),
                carried,
            );
            let settled = tokio::time::timeout(
                Duration::from_secs(5),
                service.settle_teardown_suspend_drive(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(error),
                ),
            )
            .await
            .expect("the settle answers");
            assert_eq!(
                settled.is_ok(),
                released,
                "carried {carried:?}: {settled:?}"
            );
            let expected: Vec<(String, bool)> = if released {
                vec![(refused.sandbox_id.to_string(), false)]
            } else {
                vec![]
            };
            assert_eq!(
                *refused.dispatched.lock().unwrap(),
                expected,
                "carried {carried:?}"
            );
            drain(service).await;
        }
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
    /// The refused capture and the directory holding its files.
    staged: LocalArtifactId,
    capture_dir: std::path::PathBuf,
    _snapshot_root: tempfile::TempDir,
}

/// Where a refused teardown's row stands when the next drive runs.
#[derive(Clone, Copy)]
enum RefusedRow {
    /// `suspending`, its capture staged and its runtime resident.
    Suspending,
    /// `suspended`, as startup cleanup recovery left a `suspending` row
    /// before it deferred to an active teardown: the runtime retired and the
    /// marker cleared, while the teardown still waits on the refused upload.
    SuspendedByRestart,
}

async fn redrive_refusal() -> RedrivenRefusal {
    redrive_refusal_of(RefusedRow::Suspending).await
}

async fn redrive_refusal_of(shape: RefusedRow) -> RedrivenRefusal {
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
    let origin = match shape {
        RefusedRow::Suspending => super::super::TeardownBackupOrigin::Captured,
        RefusedRow::SuspendedByRestart => {
            conn.execute(
                "UPDATE sandboxes SET state = 'suspended', failure_reason = NULL WHERE id = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
            )
            .unwrap();
            // The drive takes the previously-suspended arm for such a row.
            super::super::TeardownBackupOrigin::PreviouslySuspended
        }
    };
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
                origin,
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
        staged,
        capture_dir: snapshot_dir,
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
/// longer marked as permanently refused. That holds for a row startup
/// recovery marked `suspended` too, which takes the previously-suspended arm:
/// its refused upload was still this generation's own capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lapsed_supersession_registers_the_capture_again() {
    let lapsed = RetiringClusterApi {
        supersession_lapsed: true,
        ..RETIRING
    };
    with_cluster_api(lapsed, async || {
        for shape in [RefusedRow::Suspending, RefusedRow::SuspendedByRestart] {
            assert_lapsed_refusal_registers_again(shape).await;
        }
    })
    .await;
}

/// One shape of [`lapsed_supersession_registers_the_capture_again`].
async fn assert_lapsed_refusal_registers_again(shape: RefusedRow) {
    let drive = redrive_refusal_of(shape).await;
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
    // The admitted registration is recorded, which is what makes the held
    // teardowns' empty record evidence rather than a log that never writes.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !admitted_registrations().contains(&drive.staged.to_string()) {
        assert!(
            std::time::Instant::now() < deadline,
            "the lapsed refusal's new registration was never admitted: recorded {:?}",
            admitted_registrations(),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(drive.dispatched.lock().unwrap().is_empty());
    assert!(
        !marked_permanently_refused(&drive.service, drive.sandbox_id),
        "a refusal that no longer holds mustn't leave the teardown marked terminal"
    );
    drain(&drive.service).await;
}

/// A teardown whose row startup recovery marked `suspended` while the cluster
/// still refuses its capture as superseded is held, safely: the refused capture
/// is never registered or adopted, nothing is deleted, and the pod stays held.
///
/// The pod is what keeps a wake from restoring that capture: a restore routed
/// to this node boots from the local suspend files, and the control plane
/// defers every wake while a prior generation's pod lingers
/// (`restore_deferred_on_a_lingering_pod_mints_no_execution` in the API's
/// Kubernetes tests). So the teardown must keep answering `PENDING`, the one
/// answer that doesn't release the pod, and the settle must neither release
/// the row nor tear anything down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_superseded_suspended_teardown_is_held_with_its_capture_unregistered() {
    with_cluster_api(RETIRING, async || {
        let drive = redrive_refusal_of(RefusedRow::SuspendedByRestart).await;
        assert_eq!(
            drive.newest, drive.refused,
            "a standing refusal starts no upload"
        );
        assert_held_and_unregistered(drive).await;
    })
    .await;
}

/// The race into the same hold: the probe is admitted, so the drive starts a
/// new upload under the generation, and the cluster retires the generation
/// before that upload's registration. The registration is refused, not
/// admitted, and the teardown is held as when the refusal never lapsed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_suspended_teardown_superseded_before_its_new_registration_is_held() {
    let racing = RetiringClusterApi {
        refuses_only_registrations: true,
        ..RETIRING
    };
    with_cluster_api(racing, async || {
        let drive = redrive_refusal_of(RefusedRow::SuspendedByRestart).await;
        assert_ne!(
            drive.newest, drive.refused,
            "the admitted probe starts a new upload"
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
            "the new attempt registers under the generation, so the fence refuses it"
        );
        assert_held_and_unregistered(drive).await;
    })
    .await;
}

/// Settle `drive` and check the hold: the settle fails without tearing
/// anything down, the teardown keeps running and answers `PENDING`, the row
/// and its capture are intact, and no registration of the capture was
/// admitted. The settle's error says why the teardown is held.
async fn assert_held_and_unregistered(drive: RedrivenRefusal) {
    let error = drive
        .result
        .expect("a refused registration answers at once")
        .expect_err("a refused backup can't make the suspend durable");
    let settled = tokio::time::timeout(
        Duration::from_secs(5),
        drive.service.settle_teardown_suspend_drive(
            drive.sandbox_id,
            generation(GENERATION),
            Err(error),
        ),
    )
    .await
    .expect("the settle should finish")
    .expect_err("the teardown stays failed while the refusal stands");
    assert!(
        settled
            .message()
            .contains("held until the cluster admits its capture"),
        "{settled:?}"
    );
    assert!(
        drive.dispatched.lock().unwrap().is_empty(),
        "nothing is torn down"
    );
    {
        let db = drive.service.db.lock().unwrap();
        let id = drive.sandbox_id.to_string();
        assert!(
            crate::db::read::sandbox_view(&db, &id)
                .teardown()
                .running_for(generation(GENERATION))
                .unwrap(),
            "the teardown keeps running, so the pod stays held"
        );
        let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Suspended, "the row is kept");
        assert_eq!(
            row.current_local_artifact_id,
            Some(drive.staged),
            "the row still pins the capture"
        );
        let status = crate::db::read::sandbox_view(&db, &id)
            .teardown()
            .status()
            .unwrap()
            .unwrap();
        assert!(!status.backup_completed, "the capture isn't backed");
    }
    assert!(
        std::fs::read_dir(&drive.capture_dir)
            .unwrap()
            .next()
            .is_some(),
        "the capture's files are kept"
    );
    // Any line naming the capture counts, so a torn line can't hide one.
    let staged = drive.staged.to_string();
    assert!(
        !admitted_registrations()
            .iter()
            .any(|line| line.contains(&staged)),
        "no registration of the refused capture was admitted"
    );
    assert_eq!(
        poll_outcome(&drive.service, drive.sandbox_id).await,
        TeardownSuspendOutcome::Pending,
        "the teardown's answer doesn't release the pod"
    );
    drain(&drive.service).await;
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

/// A drive that re-asked the cluster about a refused upload and rejoined it
/// carries the answer on its error, so the settle needn't ask again. A
/// refusal the drive met directly keeps its superseded generation.
#[test]
fn a_rejoined_refusal_carries_its_verdict_to_the_settle() {
    use super::super::teardown_suspend::{
        teardown_backup_drive_verdict, teardown_backup_verdict_status,
    };
    for verdict in [
        RegistrationProbe::Superseded(generation(GENERATION)),
        RegistrationProbe::Admitted,
        RegistrationProbe::NotSuperseded,
        RegistrationProbe::Unanswered,
    ] {
        let status = teardown_backup_verdict_status(tonic::Status::internal("refused"), verdict);
        assert_eq!(teardown_backup_drive_verdict(&status), Some(verdict));
    }
    assert_eq!(
        teardown_backup_drive_verdict(&tonic::Status::internal("refused")),
        None,
        "a drive that didn't ask carries no verdict"
    );
}

/// An admission clears a refusal only for a new attempt that registers under
/// this generation. On a row startup recovery marked `suspended`, a capture
/// whose record names another generation, or can't be read, would register
/// unfenced, so the drive rejoins the refused upload instead, though the
/// cluster now admits the probe: nothing registers, and the settle gets the
/// refusal with the verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_admission_clears_no_refusal_for_an_unfenced_attempt() {
    use super::super::teardown_suspend::teardown_backup_drive_verdict;
    let lapsed = RetiringClusterApi {
        supersession_lapsed: true,
        ..RETIRING
    };
    with_cluster_api(lapsed, async || {
        for readable in [true, false] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new();
            let staged = insert_stranded_teardown_suspend_sandbox(
                &conn,
                sandbox_id,
                generation(GENERATION),
            );
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
            let snapshot_root = tempfile::tempdir().unwrap();
            let snapshot_dir = snapshot_root.path().join("suspend");
            crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
                &snapshot_dir,
                &sandbox_id.to_string(),
                staged,
                b"refused-capture",
            );
            if readable {
                crate::capture_record::write(&snapshot_dir, staged, generation(GENERATION - 1))
                    .unwrap();
            } else {
                std::fs::write(
                    snapshot_dir.join(crate::capture_record::CAPTURE_RECORD_FILENAME),
                    b"not a record",
                )
                .unwrap();
            }
            conn.execute(
                "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
                crate::db::fixture::params![snapshot_dir.to_str().unwrap(), staged.to_string()],
            )
            .unwrap();
            conn.execute(
                "UPDATE sandboxes SET state = 'suspended', failure_reason = NULL WHERE id = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
            )
            .unwrap();
            let row = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            let (service, _) = service_recording_cleanup(conn);
            let artifact = crate::db::LocalArtifactRow {
                local_artifact_id: staged,
                sandbox_id: sandbox_id.to_string(),
                artifact_kind: ArtifactKind::Full,
                artifact_origin: ArtifactOrigin::Suspend,
                snapshot_name: None,
                local_path: snapshot_dir.to_str().unwrap().to_string(),
            };
            let error = tokio::time::timeout(
                Duration::from_secs(3),
                service.ensure_teardown_suspend_backup(
                    &row,
                    TeardownSuspendBackup {
                        execution_generation: generation(GENERATION),
                        artifact: &artifact,
                        format: SuspendSnapshotFormat::Full,
                        origin: super::super::TeardownBackupOrigin::PreviouslySuspended,
                    },
                ),
            )
            .await
            .expect("the drive answers at once")
            .expect_err("a rejoined refusal can't make the suspend durable");
            assert_eq!(
                teardown_backup_drive_verdict(&error),
                Some(RegistrationProbe::Admitted),
                "record readable {readable}: the drive rejoins the refusal: {error:?}"
            );
            assert!(
                !admitted_registrations()
                    .iter()
                    .any(|line| line.contains(&staged.to_string())),
                "record readable {readable}: nothing registers"
            );
            drain(&service).await;
        }
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

/// Clearing the terminal teardown-backup marker tells a row that still holds
/// the artifact, `suspending` or `suspended`, apart from one that moved on,
/// such as one a cluster-deleted eviction claimed while the probe was out.
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
    // Startup cleanup recovery can mark the row suspended under the running
    // teardown, clearing the marker; it still holds the capture.
    crate::db::test_fixtures::state_write_fixtures::force_state(
        &conn,
        &id,
        SandboxState::Suspended,
    );
    assert_eq!(
        clear(),
        Unmarked,
        "a suspended row pinning the capture holds it"
    );
    conn.execute(
        "UPDATE sandboxes SET state = 'deleting' WHERE id = ?1",
        crate::db::fixture::params![id],
    )
    .unwrap();
    assert_eq!(clear(), MovedOn);
}
