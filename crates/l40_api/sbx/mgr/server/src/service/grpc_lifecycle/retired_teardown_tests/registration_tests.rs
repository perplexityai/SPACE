//! Which execution generation a backup registration carries, so the cluster's
//! fence can refuse a superseded runtime's capture however it's requested.

use super::*;

/// Only this execution's own capture is fenced. A suspended row the teardown
/// merely binds registers its own attempt unfenced, since a refusal there
/// could only strand the pod.
#[test]
fn only_a_captured_teardown_artifact_carries_its_generation() {
    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: LocalArtifactId::new(),
        sandbox_id: SandboxId::new().to_string(),
        artifact_kind: ArtifactKind::Full,
        artifact_origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        local_path: "/var/lib/space/sandboxes/s/suspend".to_string(),
    };
    let backup = |origin| TeardownSuspendBackup {
        execution_generation: generation(GENERATION),
        artifact: &artifact,
        format: SuspendSnapshotFormat::Full,
        origin,
    };
    assert_eq!(
        backup(super::super::TeardownBackupOrigin::Captured).registration_generation(),
        Some(generation(GENERATION))
    );
    assert_eq!(
        backup(super::super::TeardownBackupOrigin::PreviouslySuspended).registration_generation(),
        None
    );
}

/// A node-local suspend registers under the Kubernetes execution its runtime
/// belongs to, so an orphaned restore pod's VM can't publish by idling into
/// auto-suspend. An older route identity that names its runtime only by pod
/// UID is Kubernetes too. A runtime with no Kubernetes execution registers
/// unfenced, even when its route identity carries a generation.
#[test]
fn node_local_suspend_registers_under_its_kubernetes_execution() {
    let conn = crate::db::test_conn();
    let k8s_sandbox = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, k8s_sandbox, generation(GENERATION));
    let row = crate::db::get_sandbox(&conn, &k8s_sandbox.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(
        super::super::node_local_registration_generation(&row),
        Some(generation(GENERATION))
    );

    let mut pod_uid_only = row.clone();
    pod_uid_only.space_api_route_identity.substrate_kind = None;
    assert!(pod_uid_only.space_api_route_identity.pod_uid.is_some());
    assert_eq!(
        super::super::node_local_registration_generation(&pod_uid_only),
        Some(generation(GENERATION))
    );

    let mut space_runtime = row.clone();
    space_runtime.space_api_route_identity.substrate_kind =
        Some(pplx_space_common_core::types::NodeManagedBy::Space);
    assert!(space_runtime.space_api_route_identity.pod_uid.is_some());
    assert_eq!(
        super::super::node_local_registration_generation(&space_runtime),
        None
    );

    let unrouted = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &unrouted,
        SandboxState::Started,
    );
    let row = crate::db::get_sandbox(&conn, &unrouted).unwrap().unwrap();
    assert_eq!(super::super::node_local_registration_generation(&row), None);
}

/// The node-local suspend path end to end, up to the runtime it keeps: the
/// upload it persists carries the sandbox's Kubernetes execution generation,
/// the cluster's refusal of that generation comes back as a backup-fence
/// failure, and that failure rolls the suspend back to `paused` with the
/// runtime still resident.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_local_suspend_of_a_superseded_execution_rolls_back() {
    with_cluster_api(RETIRING, async || {
        use crate::service::tests::service_tests::{insert_test_sandbox, runtime_fields};
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        let mut fields = runtime_fields(generation(GENERATION), 7);
        fields.space_api_route_identity.substrate_kind =
            Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes);
        crate::db::update_runtime_fields(&conn, sandbox_id.parse().unwrap(), fields).unwrap();
        let service = service_with_runtime_probe(conn, true);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let snapshot_root = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let snapshot_dir = snapshot_root.path().join("snapshot");
        crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"node-local-capture",
        );

        let refused = service
            .enqueue_node_local_suspend_backup(
                &sandbox,
                local_artifact_id,
                snapshot_dir.to_str().unwrap(),
                SuspendSnapshotFormat::Full,
            )
            .await
            .expect_err("the cluster refuses a superseded execution's capture");
        assert!(
            refused
                .metadata()
                .contains_key(super::super::AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA),
            "a refusal must disarm auto-lifecycle: {refused:?}"
        );
        let payload: String = service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT payload FROM jobs WHERE kind = ?1 AND subject_id = ?2",
                crate::db::fixture::params![
                    crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                    sandbox_id
                ],
                |row| row.get(0),
            )
            .unwrap();
        let payload: crate::snapshot_upload::SnapshotUploadJobs =
            serde_json::from_str(&payload).unwrap();
        assert_eq!(payload.execution_generation, Some(generation(GENERATION)));

        service
            .require_node_local_backup_handoff(&sandbox_id, local_artifact_id, Err(refused))
            .expect_err("a refused handoff must stop before runtime finalization");
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert_eq!(row.pid, Some(std::process::id()));
        drain(&service).await;
    })
    .await;
}

/// Re-requesting a refused capture's backup by its artifact id, as node-drain
/// promotion does, starts a fresh attempt under the generation of the
/// execution that captured it, so the control plane refuses it again rather
/// than registering it unfenced. The capture recorded that generation in its
/// record, so it holds after the job store prunes the refused upload and
/// the teardown that staged it is no longer running; a capture staged before
/// the record existed takes it from that upload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_rerequested_by_artifact_keeps_its_execution_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            for (recorded, upload_retained) in [(true, false), (false, true)] {
                let refused = refused_teardown_with(|staged| {
                    serde_json::to_string(&bound_upload_payload(staged)).unwrap()
                })
                .await;
                let service = &refused.service;
                let sandbox_id = refused.sandbox_id.to_string();
                let snapshot_root = tempfile::tempdir().unwrap();
                stage_refused_capture(
                    &refused,
                    &snapshot_root.path().join("suspend"),
                    recorded,
                    upload_retained,
                );
                {
                    let db = service.db.lock().unwrap();
                    // The teardown failed and waits for its next drive, so the
                    // running-teardown fallback can't supply the generation.
                    let teardown_job: JobId = db
                            .query_row(
                                "SELECT job_id FROM jobs WHERE kind = ?1 AND subject_id = ?2",
                                crate::db::fixture::params![
                                    crate::db::TEARDOWN_SUSPEND_JOB_KIND,
                                    sandbox_id
                                ],
                                |row| row.get(0),
                            )
                            .unwrap();
                        pplx_space_common_jobs::db::fail_job_permanent(
                            db.raw(),
                            &teardown_job,
                            "teardown backup refused",
                            None,
                        )
                        .unwrap();
                }

                let error = service
                    .enqueue_artifact_backup(internal_request(
                        crate::proto::EnqueueArtifactBackupRequest {
                            sandbox_id: sandbox_id.clone(),
                            expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                            source: Some(
                                crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                    refused.staged.to_string(),
                                ),
                            ),
                        },
                    ))
                    .await
                    .expect_err("the cluster refuses the retired execution's capture again");
                assert_eq!(
                    pplx_space_node_api_client::cluster::backup_execution_superseded_generation(
                        &error
                    ),
                    Some(generation(GENERATION)),
                    "recorded {recorded}, upload retained {upload_retained}: the caller must see \
                     the superseded verdict: {error}"
                );
                let latest = service
                    .snapshot_upload_worker
                    .find_job_by_dedup_key(
                        crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                        &refused.staged.to_string(),
                    )
                    .unwrap()
                    .unwrap();
                let payload = latest
                    .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                    .unwrap();
                assert_ne!(
                    payload.snapshot_id,
                    bound_upload_payload(refused.staged).snapshot_id,
                    "the re-request must start a fresh attempt: {error}"
                );
                assert_eq!(payload.execution_generation, Some(generation(GENERATION)));
                drain(service).await;
            }
        },
    )
    .await;
}

/// Eviction's re-upload of a suspended row's unproven artifact registers a
/// refused teardown capture under the generation that captured it. A restart
/// can mark the row `suspended` with that capture still pinned, and a
/// re-upload naming no generation would register it unfenced. The generation
/// comes from the capture's record or, before the record existed, from the
/// refused upload; an unrecorded artifact with no earlier upload belongs to a
/// node-local suspend and still re-uploads unfenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eviction_reupload_of_a_refused_capture_keeps_its_execution_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            for (recorded, upload_retained, carried) in [
                (true, false, Some(generation(GENERATION))),
                (false, true, Some(generation(GENERATION))),
                (false, false, None),
            ] {
                let refused = refused_teardown_with(|staged| {
                    serde_json::to_string(&bound_upload_payload(staged)).unwrap()
                })
                .await;
                let service = &refused.service;
                let sandbox_id = refused.sandbox_id.to_string();
                stage_refused_capture(
                    &refused,
                    &crate::service::suspend_snapshot_publication::canonical_suspend_snapshot_dir(
                        &service.cache_root().join(&sandbox_id),
                    ),
                    recorded,
                    upload_retained,
                );
                // Startup recovery marked the row suspended with the capture
                // still pinned.
                service
                    .db
                    .lock()
                    .unwrap()
                    .execute(
                        "UPDATE sandboxes SET state = 'suspended' WHERE id = ?1",
                        [&sandbox_id],
                    )
                    .unwrap();

                let job_id = service
                    .reupload_legacy_sandbox_for_eviction(&sandbox_id)
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "recorded {recorded}, upload retained {upload_retained}: the \
                             re-upload must be queued: {error}"
                        )
                    });
                let payload = service
                    .snapshot_upload_worker
                    .get_job(&job_id)
                    .unwrap()
                    .unwrap()
                    .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                    .unwrap();
                assert_eq!(
                    payload.meta.local_artifact_id,
                    Some(refused.staged),
                    "the re-upload must back up the pinned capture"
                );
                assert_eq!(
                    payload.execution_generation, carried,
                    "recorded {recorded}, upload retained {upload_retained}"
                );
                drain(service).await;
            }
        },
    )
    .await;
}

/// A teardown's own capture on a row startup recovery marked `suspended`
/// never registers unfenced. Every new attempt the drive starts registers
/// under the teardown's generation whenever the capture's record or its
/// earlier upload names that generation: after a retriable failure, and after
/// the refused upload is pruned. The control plane, which superseded the
/// generation, refuses it each time. A capture with no such evidence, as an
/// earlier node-local suspend's has, still registers unfenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_suspended_teardown_capture_never_registers_unfenced() {
    with_cluster_api(RETIRING, async || {
        for (recorded, upload_retained, fenced) in [
            (true, false, true),
            (false, true, true),
            (false, false, false),
        ] {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id = refused.sandbox_id.to_string();
            let snapshot_dir =
                crate::service::suspend_snapshot_publication::canonical_suspend_snapshot_dir(
                    &service.cache_root().join(&sandbox_id),
                );
            stage_refused_capture(&refused, &snapshot_dir, recorded, upload_retained);
            {
                let db = service.db.lock().unwrap();
                // Startup recovery marked the row suspended with the capture
                // still pinned, and its teardown still running.
                db.execute(
                    "UPDATE sandboxes SET state = 'suspended', failure_reason = NULL WHERE id = ?1",
                    [&sandbox_id],
                )
                .unwrap();
                // A retained upload that failed retriably gets a new attempt.
                db.execute(
                    "UPDATE jobs SET error_kind = 'retriable' WHERE dedup_key = ?1",
                    [refused.staged.to_string()],
                )
                .unwrap();
            }
            let row = crate::db::get_sandbox(&service.db.lock().unwrap(), &sandbox_id)
                .unwrap()
                .unwrap();
            let artifact = crate::db::LocalArtifactRow {
                local_artifact_id: refused.staged,
                sandbox_id: sandbox_id.clone(),
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
                        origin: super::super::TeardownBackupOrigin::PreviouslySuspended,
                    },
                ),
            )
            .await;
            let staged = refused.staged.to_string();
            let admitted = admitted_registrations()
                .iter()
                .any(|line| line.contains(&staged));
            assert_eq!(
                admitted, !fenced,
                "recorded {recorded}, upload retained {upload_retained}: {result:?}"
            );
            if fenced {
                result
                    .expect("a refused registration answers at once")
                    .expect_err("a refused registration can't make the suspend durable");
            }
            drain(service).await;
        }
    })
    .await;
}

/// Evidence that can't be read can't vouch for a previously-suspended
/// capture's generation. A drive that would start a new attempt for it fails
/// rather than register it unfenced, and the stub admits nothing. A drive
/// that joins an upload already registered still joins it: joining registers
/// nothing new.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_capture_record_registers_nothing_but_an_upload_still_joins() {
    with_cluster_api(RETIRING, async || {
        for joins_completed_upload in [false, true] {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id = refused.sandbox_id.to_string();
            let snapshot_dir =
                crate::service::suspend_snapshot_publication::canonical_suspend_snapshot_dir(
                    &service.cache_root().join(&sandbox_id),
                );
            stage_refused_capture(&refused, &snapshot_dir, false, joins_completed_upload);
            {
                let db = service.db.lock().unwrap();
                db.execute(
                    "UPDATE sandboxes SET state = 'suspended', failure_reason = NULL WHERE id = ?1",
                    [&sandbox_id],
                )
                .unwrap();
                if joins_completed_upload {
                    // An upload that completed, whose payload no longer
                    // decodes.
                    db.execute(
                        "UPDATE jobs SET status = 'completed', error_kind = NULL, error = NULL,
                                payload = 'not a payload'
                          WHERE dedup_key = ?1",
                        [refused.staged.to_string()],
                    )
                    .unwrap();
                }
            }
            if !joins_completed_upload {
                std::fs::write(
                    snapshot_dir.join(crate::capture_record::CAPTURE_RECORD_FILENAME),
                    b"not a record",
                )
                .unwrap();
            }
            let row = crate::db::get_sandbox(&service.db.lock().unwrap(), &sandbox_id)
                .unwrap()
                .unwrap();
            let artifact = crate::db::LocalArtifactRow {
                local_artifact_id: refused.staged,
                sandbox_id: sandbox_id.clone(),
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
                        origin: super::super::TeardownBackupOrigin::PreviouslySuspended,
                    },
                ),
            )
            .await
            .expect("the drive answers at once");
            let staged = refused.staged.to_string();
            assert!(
                !admitted_registrations()
                    .iter()
                    .any(|line| line.contains(&staged)),
                "joins a completed upload {joins_completed_upload}: {result:?}"
            );
            if joins_completed_upload {
                result.expect("the drive joins the completed upload");
            } else {
                let error = result.expect_err("an unvouched capture starts no attempt");
                assert_eq!(error.code(), tonic::Code::Internal, "{error:?}");
            }
            drain(service).await;
        }
    })
    .await;
}

/// A capture whose earlier upload was admitted stays bound to the execution
/// that took it: the re-request carries that generation, and the node passes
/// on whatever the control plane answers. (The control plane admits it, since
/// an earlier attempt registered these bytes; this stub refuses every
/// generation-bound registration.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rerequested_admitted_backup_keeps_its_execution_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                let mut admitted = bound_upload_payload(staged);
                admitted.job_ids = vec![JobId::new()];
                serde_json::to_string(&admitted).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id = refused.sandbox_id.to_string();
            let snapshot_root = tempfile::tempdir().unwrap();
            let snapshot_dir = snapshot_root.path().join("suspend");
            crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
                &snapshot_dir,
                &sandbox_id,
                refused.staged,
                b"admitted-capture",
            );
            service
                .db
                .lock()
                .unwrap()
                .execute(
                    "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
                    crate::db::fixture::params![
                        snapshot_dir.to_str().unwrap(),
                        refused.staged.to_string()
                    ],
                )
                .unwrap();

            let error = service
                .enqueue_artifact_backup(internal_request(
                    crate::proto::EnqueueArtifactBackupRequest {
                        sandbox_id: sandbox_id.clone(),
                        expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                        source: Some(
                            crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                refused.staged.to_string(),
                            ),
                        ),
                    },
                ))
                .await
                .expect_err("the cluster refuses the superseded execution's capture");
            assert_eq!(
                pplx_space_node_api_client::cluster::backup_execution_superseded_generation(&error),
                Some(generation(GENERATION)),
                "the caller must see the superseded verdict: {error}"
            );
            let latest = service
                .snapshot_upload_worker
                .find_job_by_dedup_key(
                    crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                    &refused.staged.to_string(),
                )
                .unwrap()
                .unwrap();
            let payload = latest
                .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                .unwrap();
            assert_ne!(
                payload.snapshot_id,
                bound_upload_payload(refused.staged).snapshot_id,
                "the re-request must start a fresh attempt"
            );
            assert_eq!(payload.execution_generation, Some(generation(GENERATION)));
            drain(service).await;
        },
    )
    .await;
}

/// A teardown capture requested by artifact id before its own upload job
/// exists registers under the teardown's generation, not the route's: the one
/// the capture recorded, or, for a capture from before the
/// record existed, the running teardown's while the row still pins it,
/// including after a restart left it in cleanup. A recorded capture carries
/// its generation whatever the row's state. A suspended row's unrecorded
/// artifact belongs to an earlier node-local suspend, so its request names no
/// generation and isn't refused as superseded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_request_of_a_staged_capture_carries_the_teardown_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            for (state, recorded, carried) in [
                ("suspending", true, Some(generation(GENERATION))),
                ("suspending", false, Some(generation(GENERATION))),
                ("cleanup_required", false, Some(generation(GENERATION))),
                ("suspended", true, Some(generation(GENERATION))),
                ("suspended", false, None),
            ] {
                let conn = crate::db::test_conn();
                let sandbox_id = SandboxId::new();
                let sandbox_id_string = sandbox_id.to_string();
                let staged = insert_stranded_teardown_suspend_sandbox(
                    &conn,
                    sandbox_id,
                    generation(GENERATION),
                );
                let snapshot_root = tempfile::tempdir().unwrap();
                let snapshot_dir = snapshot_root.path().join("suspend");
                crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
                    &snapshot_dir,
                    &sandbox_id_string,
                    staged,
                    b"staged-capture",
                );
                conn.execute(
                    "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
                    crate::db::fixture::params![snapshot_dir.to_str().unwrap(), staged.to_string()],
                )
                .unwrap();
                if recorded {
                    record_capture_generation(&snapshot_dir, staged);
                }
                // The route names another generation, so only the teardown's
                // own can be what the request carries.
                conn.execute(
                    "UPDATE sandboxes
                        SET state = ?1, space_api_route_execution_generation = ?2
                      WHERE id = ?3",
                    crate::db::fixture::params![state, GENERATION + 92, sandbox_id_string],
                )
                .unwrap();
                let (service, _dispatched) = service_recording_cleanup(conn);

                let result = service
                    .enqueue_artifact_backup(internal_request(
                        crate::proto::EnqueueArtifactBackupRequest {
                            sandbox_id: sandbox_id_string.clone(),
                            expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                            source: Some(
                                crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                    staged.to_string(),
                                ),
                            ),
                        },
                    ))
                    .await;
                let verdict = result.as_ref().err().and_then(|error| {
                    pplx_space_node_api_client::cluster::backup_execution_superseded_generation(error)
                });
                assert_eq!(verdict, carried, "{state}, recorded {recorded}: {result:?}");
                drain(&service).await;
            }
        },
    )
    .await;
}

/// Write `refused`'s capture into `snapshot_dir` and point its local artifact
/// row there. The capture records [`GENERATION`] when `recorded`, and its
/// refused upload is pruned unless `upload_retained`.
fn stage_refused_capture(
    refused: &RefusedTeardown,
    snapshot_dir: &std::path::Path,
    recorded: bool,
    upload_retained: bool,
) {
    crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
        snapshot_dir,
        &refused.sandbox_id.to_string(),
        refused.staged,
        b"refused-capture",
    );
    if recorded {
        record_capture_generation(snapshot_dir, refused.staged);
    }
    let db = refused.service.db.lock().unwrap();
    db.execute(
        "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
        crate::db::fixture::params![snapshot_dir.to_str().unwrap(), refused.staged.to_string()],
    )
    .unwrap();
    if !upload_retained {
        db.execute(
            "DELETE FROM jobs WHERE dedup_key = ?1",
            [refused.staged.to_string()],
        )
        .unwrap();
    }
}

/// Record [`GENERATION`] as the capture's execution generation in the
/// capture's record at `snapshot_dir`, as a teardown suspend's capture does.
fn record_capture_generation(snapshot_dir: &std::path::Path, artifact: LocalArtifactId) {
    crate::capture_record::write(snapshot_dir, artifact, generation(GENERATION)).unwrap();
}
