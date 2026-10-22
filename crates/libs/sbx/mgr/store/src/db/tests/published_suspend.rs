use super::*;

#[test]
fn published_suspend_rollback_retains_the_new_artifact_only_for_its_owner() {
    for conflict in [
        "none",
        "state",
        "runtime",
        "generation",
        "predecessor",
        "intent",
    ] {
        let conn = test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, "capture1", SandboxState::Suspending),
        );
        let predecessor = LocalArtifactId::new();
        conn.execute(
            "UPDATE sandboxes SET suspend_predecessor_artifact_id = ?1 WHERE id = ?2",
            params![predecessor.to_string(), id],
        )
        .unwrap();
        let mut expected = get_sandbox(&conn, &id).unwrap().unwrap();
        // The publication context was read before begin_suspend moved this pointer.
        expected.current_local_artifact_id = Some(predecessor);
        expected.current_local_artifact_kind = Some(ArtifactKind::Full);
        expected.current_local_artifact_origin = Some(ArtifactOrigin::Suspend);
        match conflict {
            "state" => {
                conn.execute(
                    "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                    [&id],
                )
                .unwrap();
            }
            "runtime" => expected.node_execution_id = Some(SandboxActivationAttemptId::new()),
            "generation" => {
                expected.space_api_route_identity.execution_generation =
                    Some(ExecutionGeneration::new(9).unwrap())
            }
            "predecessor" => expected.current_local_artifact_id = Some(LocalArtifactId::new()),
            "intent" => {
                set_cleanup_intent(&conn, &id, true, CleanupCompletion::MarkStopped).unwrap();
            }
            "none" => {}
            _ => unreachable!(),
        }
        let before = database_rows(&conn);
        let artifact = LocalArtifactId::new();
        assert_eq!(
            rollback_published_suspend_to_paused(&conn, &expected, artifact, "/cache/snapshot")
                .unwrap(),
            conflict == "none",
            "{conflict}"
        );
        if conflict != "none" {
            assert_eq!(
                database_rows(&conn),
                before,
                "rejection must be atomic: {conflict}"
            );
        } else {
            let row = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(row.state, SandboxState::Paused);
            assert_eq!(row.current_local_artifact_id, Some(artifact));
            assert_eq!(row.cleanup_completion, None);
            assert_eq!(
                get_local_artifact(&conn, artifact)
                    .unwrap()
                    .unwrap()
                    .local_path,
                "/cache/snapshot"
            );
            assert!(
                !rollback_published_suspend_to_paused(
                    &conn,
                    &expected,
                    LocalArtifactId::new(),
                    "/other"
                )
                .unwrap()
            );
        }
    }
}

#[test]
fn refused_suspend_backup_finishes_only_its_running_cleanup_and_retains_the_artifact() {
    use cleanup_jobs::{CleanupWork, GuestFlush};
    use pplx_space_common_jobs::JobStatus;
    for conflict in [
        "none",
        "state",
        "runtime",
        "generation",
        "artifact",
        "job",
        "job_pending",
    ] {
        let conn = test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, "capture1", SandboxState::Suspending),
        );
        let artifact = LocalArtifactId::new();
        pin_suspended_artifact_for_cleanup(&conn, &id, artifact, "/cache/snapshot").unwrap();
        let mut expected = get_sandbox(&conn, &id).unwrap().unwrap();
        let work = CleanupWork {
            version: 1,
            sandbox_id,
            uuid8: expected.uuid8.clone(),
            execution_generation: expected.space_api_route_identity.execution_generation,
            cleanup_id: None,
            completion: CleanupCompletion::MarkSuspended,
            keep_filesystem: true,
            immediate_kill: true,
            guest_flush: GuestFlush::Unrequested,
            published_artifact: Some((artifact, "/cache/snapshot".to_owned())),
        };
        let mut wrong_work = work.clone();
        wrong_work.sandbox_id = SandboxId::new();
        let tx = Transaction::deferred(&conn).unwrap();
        let job = cleanup_jobs::persist(
            &tx,
            if conflict == "job" {
                wrong_work
            } else {
                work.clone()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        if conflict != "job_pending" {
            assert!(cleanup_jobs::mark_running(&conn, &job).unwrap());
        }
        match conflict {
            "state" => {
                conn.execute(
                    "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                    [&id],
                )
                .unwrap();
            }
            "runtime" => expected.node_execution_id = Some(SandboxActivationAttemptId::new()),
            "generation" => {
                expected.space_api_route_identity.execution_generation =
                    Some(ExecutionGeneration::new(9).unwrap())
            }
            "artifact" => expected.current_local_artifact_id = Some(LocalArtifactId::new()),
            _ => {}
        }
        let before = database_rows(&conn);
        let target = expected.current_local_artifact_id.unwrap();
        assert_eq!(
            quarantine_refused_suspend_backup(
                &conn,
                &expected,
                target,
                "registration superseded",
                Some(&job)
            )
            .unwrap(),
            conflict == "none",
            "{conflict}"
        );
        if conflict != "none" {
            assert_eq!(database_rows(&conn), before, "{conflict}");
        } else {
            let row = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(row.state, SandboxState::Quarantined);
            assert_eq!(row.cleanup_completion, None);
            assert_eq!(row.current_local_artifact_id, Some(artifact));
            assert!(get_local_artifact(&conn, artifact).unwrap().is_some());
            assert_eq!(
                cleanup_jobs::get_job(&conn, &job).unwrap().unwrap().status,
                JobStatus::Failed.as_str()
            );
            assert!(
                !cleanup_jobs::current(&conn, &work).unwrap(),
                "restart discovery has no remaining cleanup authority"
            );
            assert_eq!(
                current_quarantine_reason_wire_value(&conn, &id)
                    .unwrap()
                    .as_deref(),
                Some(SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.as_str())
            );
        }
    }
}
