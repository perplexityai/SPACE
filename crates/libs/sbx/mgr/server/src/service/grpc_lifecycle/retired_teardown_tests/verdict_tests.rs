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
