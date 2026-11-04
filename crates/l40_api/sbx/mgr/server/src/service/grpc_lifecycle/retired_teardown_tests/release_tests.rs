//! Settling a refused teardown with a release: the claim, the holds that keep
//! the runtime resident, and the eviction that tears it down.

use super::*;

/// Settling with the release the cluster's verdict selects (injected here;
/// `retired_execution_off_its_node_still_releases` drives the verdict itself)
/// while the sandbox row still exists. The runtime is torn down, the capture
/// never becomes a pending backup, and the shim's next poll releases the pod.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_refused_for_the_retired_generation_releases_the_teardown() {
    with_cluster_api(RETIRING, async || {
        let refused = refused_teardown().await;
        let service = &refused.service;
        let sandbox_id_string = refused.sandbox_id.to_string();

        let capture = crate::service::tests::service_tests::SandboxEventCapture::default();
        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        {
            use tracing_subscriber::layer::SubscriberExt;
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(capture.clone()),
            );
            tokio::time::timeout(
                Duration::from_secs(5),
                service.settle_teardown_suspend_driver_failure(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(refused.error.clone()),
                    None,
                    Some(refused.staged),
                ),
            )
            .await
            .expect("the release should complete")
            .expect("a retired execution's teardown must be released");
        }
        let emitted = counters.finish();
        assert!(
            emitted
                .iter()
                .any(|counter| counter.name == "sandbox.evict.evicted"
                    && counter
                        .tags
                        .iter()
                        .any(|tag| tag == "reason:execution_retired")),
            "the release counts as an execution_retired eviction: {emitted:?}"
        );
        let events = capture.events.lock().unwrap().clone();
        assert!(
            events.iter().any(|event| {
                event.get("event_type").map(String::as_str) == Some("local_eviction_completed")
                    && event.get("reason").map(String::as_str) == Some("execution_retired")
                    && event.get("sandbox_id") == Some(&sandbox_id_string)
            }),
            "the release emits its local_eviction_completed event: {events:?}"
        );

        assert_eq!(
            *refused.dispatched.lock().unwrap(),
            vec![(sandbox_id_string.clone(), false)],
            "the resident runtime must be torn down, not just forgotten"
        );
        {
            let db = service.db.lock().unwrap();
            assert!(
                crate::db::get_sandbox(&db, &sandbox_id_string)
                    .unwrap()
                    .is_none()
            );
        }
        let pending = service.local_artifacts().list_pending_backups().unwrap();
        assert!(
            pending
                .iter()
                .all(|backup| backup.local_artifact_id != refused.staged),
            "the discarded capture must not block a drain as a pending backup: {pending:?}"
        );
        assert_eq!(
            poll_outcome(service, refused.sandbox_id).await,
            TeardownSuspendOutcome::Deleted,
            "the shim must be released instead of polling forever"
        );
        drain(service).await;
    })
    .await;
}

/// Without a refusal for this execution the failure takes the existing
/// handling: the runtime stays and the row keeps waiting for its backup.
#[tokio::test]
async fn backup_refusal_without_a_release_verdict_releases_nothing() {
    let refused = refused_teardown().await;
    let service = &refused.service;
    let result = service
        .settle_teardown_suspend_driver_failure(
            refused.sandbox_id,
            generation(GENERATION),
            Err(refused.error.clone()),
            Some(&OrgSnapshotConfigResolution::Absent),
            None,
        )
        .await;
    assert!(result.is_err(), "the drive must stay failed");
    assert!(
        refused.dispatched.lock().unwrap().is_empty(),
        "the runtime must not be torn down"
    );
    let row = {
        let db = service.db.lock().unwrap();
        crate::db::get_sandbox(&db, &refused.sandbox_id.to_string())
            .unwrap()
            .unwrap()
    };
    assert_eq!(row.state, SandboxState::Suspending);
    drain(service).await;
}

/// A release for a row that moved on changes nothing and doesn't ask the
/// cluster whether the sandbox is restorable, so no hold is counted; the
/// drive's failure takes the ordinary handling: a quarantined row's teardown
/// job is closed rather than left running.
#[tokio::test]
async fn release_for_a_row_that_moved_on_records_the_drive_failure() {
    let refused = refused_teardown().await;
    let service = &refused.service;
    let sandbox_id_string = refused.sandbox_id.to_string();
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'quarantined' WHERE id = ?1",
            crate::db::fixture::params![sandbox_id_string],
        )
        .unwrap();

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let result = service
        .settle_teardown_suspend_driver_failure(
            refused.sandbox_id,
            generation(GENERATION),
            Err(refused.error.clone()),
            Some(&OrgSnapshotConfigResolution::Absent),
            Some(refused.staged),
        )
        .await;
    let emitted = counters.finish();
    assert!(result.is_err(), "the drive must stay failed");
    assert!(refused.dispatched.lock().unwrap().is_empty());
    assert!(
        !emitted
            .iter()
            .any(|counter| counter.name == "sandbox.evict.skipped"),
        "a row that moved on isn't a held release: {emitted:?}"
    );
    let status = {
        let db = service.db.lock().unwrap();
        crate::db::latest_teardown_suspend_operation(&db, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .status
    };
    assert_eq!(status, crate::db::TeardownSuspendOperationStatus::Failed);
    drain(service).await;
}

/// A release held back by another artifact's pending backup is counted once,
/// as that hold, before the cluster is asked whether the sandbox is restorable
/// (the stub would answer no), and nothing is torn down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_release_counts_a_skip() {
    let no_backed_copy = RetiringClusterApi {
        backed_copy: false,
        ..RETIRING
    };
    with_cluster_api(no_backed_copy, async || {
        let refused = refused_teardown().await;
        let service = &refused.service;
        let named = insert_unstamped_checkpoint(service, refused.sandbox_id);

        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        let result = service
            .settle_teardown_suspend_driver_failure(
                refused.sandbox_id,
                generation(GENERATION),
                Err(refused.error.clone()),
                Some(&OrgSnapshotConfigResolution::Absent),
                Some(refused.staged),
            )
            .await;
        let emitted = counters.finish();
        assert!(result.is_err(), "a held release leaves the drive failed");
        assert!(refused.dispatched.lock().unwrap().is_empty());
        let skips: Vec<_> = emitted
            .iter()
            .filter(|counter| counter.name == "sandbox.evict.skipped")
            .collect();
        assert_eq!(skips.len(), 1, "one hold, counted once: {emitted:?}");
        assert!(
            skips[0]
                .tags
                .iter()
                .any(|tag| tag == "reason:local_artifact_awaiting_backup"),
            "the hold is counted as the pending artifact: {emitted:?}"
        );
        assert!(
            !emitted
                .iter()
                .any(|counter| counter.name == "sandbox.evict.evicted"),
            "nothing was evicted: {emitted:?}"
        );
        assert!(
            !checkpoint_stamped(service, named),
            "an artifact the cluster can't show backed stays unstamped"
        );
        drain(service).await;
    })
    .await;
}

/// A named checkpoint of the sandbox with no `backup_completed_at` stamp, as a
/// restore from another node's backup leaves one.
fn insert_unstamped_checkpoint(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
) -> LocalArtifactId {
    let named = LocalArtifactId::new();
    let sandbox_id_string = sandbox_id.to_string();
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO local_artifacts (
                 local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                 snapshot_name, local_path, created_at
             ) VALUES (?1, ?2, 'full', 'snapshot', 'checkpoint', ?3, '0')",
            crate::db::fixture::params![
                named.to_string(),
                sandbox_id_string,
                format!("/var/lib/space/sandboxes/{sandbox_id_string}/snapshots/checkpoint"),
            ],
        )
        .unwrap();
    named
}

fn checkpoint_stamped(service: &SandboxManagerService, artifact: LocalArtifactId) -> bool {
    service
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT backup_completed_at IS NOT NULL FROM local_artifacts
              WHERE local_artifact_id = ?1",
            crate::db::fixture::params![artifact.to_string()],
            |row| row.get(0),
        )
        .unwrap()
}

/// An unstamped artifact the cluster shows backed up doesn't hold the release:
/// it's proven by the exact-artifact readiness check, stamped, and the retired
/// execution's runtime is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_proves_an_unstamped_artifact_backed() {
    let artifact_backed = RetiringClusterApi {
        artifact_backed: true,
        ..RETIRING
    };
    with_cluster_api(artifact_backed, async || {
        let refused = refused_teardown().await;
        let service = &refused.service;
        let sandbox_id_string = refused.sandbox_id.to_string();
        insert_unstamped_checkpoint(service, refused.sandbox_id);

        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        tokio::time::timeout(
            Duration::from_secs(5),
            service.settle_teardown_suspend_driver_failure(
                refused.sandbox_id,
                generation(GENERATION),
                Err(refused.error.clone()),
                None,
                Some(refused.staged),
            ),
        )
        .await
        .expect("the release should complete")
        .expect("a proven artifact doesn't hold the release");
        let emitted = counters.finish();
        assert!(
            !emitted
                .iter()
                .any(|counter| counter.name == "sandbox.evict.skipped"),
            "nothing held the release: {emitted:?}"
        );
        assert_eq!(
            *refused.dispatched.lock().unwrap(),
            vec![(sandbox_id_string, false)],
            "the retired execution's runtime is torn down"
        );
        drain(service).await;
    })
    .await;
}

/// Without a ready snapshot of the sandbox to restore from, the refused capture
/// may be its only copy: the release is held, nothing is torn down or deleted,
/// the teardown stays running, and the capture still can't register.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_without_a_backed_copy_is_held() {
    with_cluster_api(
        RetiringClusterApi {
            backed_copy: false,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id_string = refused.sandbox_id.to_string();
            let counters = pplx_space_metrics::test_capture::capture_counters_guard();
            let result = service
                .settle_teardown_suspend_drive(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(refused.error.clone()),
                )
                .await;
            let emitted = counters.finish();
            assert!(result.is_err(), "a held release leaves the teardown failed");
            assert!(
                refused.dispatched.lock().unwrap().is_empty(),
                "the runtime stays"
            );
            assert!(
                emitted
                    .iter()
                    .any(|counter| counter.name == "sandbox.evict.skipped"
                        && counter
                            .tags
                            .iter()
                            .any(|tag| tag == "reason:no_backed_copy")),
                "the hold is counted: {emitted:?}"
            );
            assert!(
                !emitted
                    .iter()
                    .any(|counter| counter.name == "sandbox.evict.evicted"),
                "nothing was evicted: {emitted:?}"
            );
            {
                let db = service.db.lock().unwrap();
                let row = crate::db::get_sandbox(&db, &sandbox_id_string)
                    .unwrap()
                    .unwrap();
                assert_eq!(row.state, SandboxState::Suspending);
                assert_eq!(row.current_local_artifact_id, Some(refused.staged));
                assert!(
                    crate::db::get_local_artifact(&db, refused.staged)
                        .unwrap()
                        .is_some(),
                    "the capture is kept"
                );
                assert_eq!(
                    crate::db::latest_teardown_suspend_operation(&db, &sandbox_id_string)
                        .unwrap()
                        .unwrap()
                        .status,
                    crate::db::TeardownSuspendOperationStatus::Running,
                    "the next drive asks again"
                );
            }
            assert_eq!(
                crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(
                    &bound_upload_payload(refused.staged)
                )
                .await,
                RegistrationProbe::Superseded(generation(GENERATION)),
                "the held capture's upload stays bound to its generation"
            );
            drain(service).await;
        },
    )
    .await;
}

/// A winning claim can't stop an upload job that's already running, so it marks
/// the job `cancelling` rather than leaving it running or marking it done. The
/// job worker's own tests cover finishing a `cancelling` job as cancelled.
#[test]
fn release_claim_marks_a_running_upload_to_finish_cancelled() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let job_id = insert_upload_job(&conn, &sandbox_id_string, staged, "{}");
    assert!(pplx_space_common_jobs::db::mark_job_running(conn.raw(), &job_id).unwrap());

    assert!(
        crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap()
    );
    let job = pplx_space_common_jobs::db::get_job(conn.raw(), &job_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        job.status,
        pplx_space_common_jobs::JobStatus::Cancelling.as_str(),
        "a running upload is told to stop, not marked done"
    );
}

/// The claim re-asserts the execution the refusal was about: a different
/// generation or a re-staged artifact means the row moved on, and nothing
/// changes.
#[test]
fn release_claim_refuses_a_row_that_moved_on() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));

    for (claimed_generation, claimed_artifact) in [
        (generation(GENERATION + 1), staged),
        (generation(GENERATION - 1), staged),
        (generation(GENERATION), LocalArtifactId::new()),
    ] {
        assert!(
            !crate::db::claim_retired_teardown_execution_for_release(
                &conn,
                &sandbox_id_string,
                claimed_generation,
                claimed_artifact,
            )
            .unwrap()
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Suspending);
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
    }

    assert!(
        crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap()
    );
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::Deleting);
    assert_ne!(
        crate::db::latest_teardown_suspend_operation(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .status,
        crate::db::TeardownSuspendOperationStatus::Running,
        "the claim closes the teardown job it took over"
    );
}

/// The probe re-sends only a terminal upload bound to the generation being
/// driven whose registration was never admitted: a live job still owns its
/// attempt, and a payload persisted before the fence, one bound to another
/// execution, or one that already has transfers carries no verdict about this
/// execution.
#[tokio::test]
async fn only_an_upload_bound_to_the_generation_is_probed() {
    let staged_id = "01939e3e-0000-7000-8000-0000000000aa";
    let staged: LocalArtifactId = staged_id.parse().unwrap();
    let mut payload = crate::snapshot_upload_jobs::tests::reclaim_fixture_payload(
        staged_id,
        std::path::PathBuf::from("/var/lib/space/sandboxes/s/suspend"),
    );
    let sandbox_id = payload.meta.sandbox_id.clone();

    for (bound_to, admitted, probed) in [
        (Some(generation(GENERATION)), false, true),
        (Some(generation(GENERATION)), true, false),
        (Some(generation(GENERATION + 1)), false, false),
        (None, false, false),
    ] {
        payload.execution_generation = bound_to;
        payload.job_ids = if admitted {
            vec![JobId::new()]
        } else {
            Vec::new()
        };
        let conn = crate::db::test_conn();
        let job_id = insert_upload_job(
            &conn,
            &sandbox_id,
            staged,
            &serde_json::to_string(&payload).unwrap(),
        );
        let service = SandboxManagerService::for_test(conn);
        // Only a payload that would be probed once terminal shows that
        // liveness alone holds the probe back.
        if probed {
            assert!(
                service
                    .teardown_backup_payload_bound_to(generation(GENERATION), staged)
                    .is_none(),
                "a live job still owns its attempt"
            );
        }
        pplx_space_common_jobs::db::fail_job_permanent(
            service.db.lock().unwrap().raw(),
            &job_id,
            "refused",
            None,
        )
        .unwrap();
        assert_eq!(
            service
                .teardown_backup_payload_bound_to(generation(GENERATION), staged)
                .is_some(),
            probed,
            "upload bound to {bound_to:?}, admitted {admitted}"
        );
    }

    let service = SandboxManagerService::for_test(crate::db::test_conn());
    assert!(
        service
            .teardown_backup_payload_bound_to(generation(GENERATION), staged)
            .is_none(),
        "no upload job, no probe"
    );
}

/// The release deletes every local artifact of the sandbox, so it waits while
/// another one is still awaiting its backup: those bytes may be the only copy.
#[test]
fn release_claim_waits_for_other_local_artifacts_to_back_up() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let named = LocalArtifactId::new();
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at
         ) VALUES (?1, ?2, 'full', 'snapshot', 'checkpoint', ?3, '0')",
        crate::db::fixture::params![
            named.to_string(),
            sandbox_id_string,
            format!("/var/lib/space/sandboxes/{sandbox_id_string}/snapshots/checkpoint"),
        ],
    )
    .unwrap();

    assert!(
        !crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap(),
        "an unbacked named snapshot must keep the row"
    );
    assert_eq!(
        crate::db::other_local_artifacts_awaiting_backup(&conn, &sandbox_id_string, staged)
            .unwrap()
            .iter()
            .map(|artifact| artifact.local_artifact_id)
            .collect::<Vec<_>>(),
        vec![named],
        "the hold names the unstamped artifact"
    );
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::Suspending);

    crate::db::mark_local_artifact_backup_completed(&conn, &named.to_string()).unwrap();
    assert!(
        crate::db::other_local_artifacts_awaiting_backup(&conn, &sandbox_id_string, staged)
            .unwrap()
            .is_empty()
    );
    assert!(
        crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap()
    );
}
