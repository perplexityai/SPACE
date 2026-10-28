//! A sandbox-manager restart while a teardown suspend waits on its capture's
//! backup leaves the row to that teardown.

use super::*;

/// The teardown still owns a `suspending` row whose capture it staged and
/// whose runtime is live: it backs the capture up, then finalizes the runtime.
/// Startup recovery must leave the row alone. Marking it `suspended` retires
/// the runtime behind the teardown's back, which keeps the teardown, and the
/// pod waiting on it, running.
#[test]
fn startup_recovery_leaves_a_suspending_row_to_its_active_teardown() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));

    let records = crate::db::recover_interrupted_startup_states(&conn).unwrap();
    let record = records
        .iter()
        .find(|record| record.id == id)
        .expect("startup recovery reports the interrupted suspend");
    assert_eq!(
        record.outcome,
        crate::db::StartupRecoveryOutcome::Deferred,
        "the store's startup pass defers to the teardown"
    );
    let db = crate::db::SharedConnection::new(conn);
    // The test wrapper reports every runtime live.
    let targets = SandboxManagerService::recover_required_cleanup_targets(
        &db,
        crate::service::recovery::CleanupRecoveryAuthority::Startup,
    )
    .unwrap();
    assert!(
        targets.iter().all(|target| target.sandbox_id != sandbox_id),
        "startup cleanup discovery leaves the row to its teardown"
    );

    let db = db.lock().unwrap();
    let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Suspending);
    assert_eq!(row.cleanup_completion, None);
    assert!(
        crate::db::read::sandbox_view(&db, &id)
            .teardown()
            .running_for(generation(GENERATION))
            .unwrap()
    );
}

/// Without an active teardown, a `suspending` row with its full suspend
/// artifact pinned is finished as before: startup marks it suspended.
#[test]
fn startup_recovery_still_finishes_a_suspending_row_without_a_teardown() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    crate::db::fail_teardown_suspend_operation(
        &conn,
        &id,
        generation(GENERATION),
        crate::db::TeardownSuspendFailure::Failed,
    )
    .unwrap();

    let records = crate::db::recover_interrupted_startup_states(&conn).unwrap();
    let record = records.iter().find(|record| record.id == id).unwrap();
    assert_eq!(
        record.outcome,
        crate::db::StartupRecoveryOutcome::CleanupRequired {
            keep_filesystem: true,
            completion: crate::db::CleanupCompletion::MarkSuspended,
        }
    );
}

/// After a host reboot the runtime is gone, so the teardown has nothing left
/// to finalize. The store's pass, which can't see the runtime, still defers
/// the row, and startup cleanup discovery then marks it suspended as before,
/// persisting the intent, with startup's own reason rather than any marker
/// the row carried. Its teardown then continues on the shim's next poll.
#[test]
fn startup_cleanup_marks_a_teardown_owned_row_suspended_once_its_runtime_is_gone() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));

    let records = crate::db::recover_interrupted_startup_states(&conn).unwrap();
    let record = records.iter().find(|record| record.id == id).unwrap();
    assert_eq!(record.outcome, crate::db::StartupRecoveryOutcome::Deferred);
    let db = crate::db::SharedConnection::new(conn);
    let targets = SandboxManagerService::recover_required_cleanup_targets_with(
        &db,
        crate::service::recovery::CleanupRecoveryAuthority::Startup,
        &|_| false,
    )
    .unwrap();
    let target = targets
        .iter()
        .find(|target| target.sandbox_id == sandbox_id)
        .expect("startup cleanup takes the row whose runtime is gone");
    assert_eq!(
        target.completion,
        crate::db::CleanupCompletion::MarkSuspended
    );
    assert!(target.keep_filesystem, "the capture stays for the teardown");
    let db = db.lock().unwrap();
    let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
    assert_eq!(
        (row.state, row.cleanup_completion),
        (
            SandboxState::CleanupRequired,
            Some(crate::db::CleanupCompletion::MarkSuspended)
        ),
        "the intent is persisted, so the periodic reconciler retries a cleanup that doesn't finish"
    );
    assert!(
        row.failure_reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("startup recovery:")),
        "the persisted reason is startup's: {:?}",
        row.failure_reason
    );
}

/// A teardown job that doesn't decode, or a second active one, can't drive
/// its row, so startup finishes the row as it would without a teardown. It
/// mustn't fail startup either, which would keep sbx-manager down for the
/// whole node.
#[test]
fn startup_recovery_finishes_a_suspending_row_whose_teardown_job_does_not_decode() {
    for second_job in [false, true] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
        if second_job {
            pplx_space_common_jobs::db::insert_job(
                conn.raw(),
                pplx_space_common_jobs::db::InsertJob {
                    job_id: &JobId::new(),
                    kind: crate::db::TEARDOWN_SUSPEND_JOB_KIND,
                    payload: "{}",
                    subject_id: Some(&id),
                    dedup_key: None,
                    max_attempts: 1,
                    traceparent: None,
                    priority: pplx_space_common_jobs::JobPriority::default(),
                },
            )
            .unwrap();
        } else {
            conn.execute(
                "UPDATE jobs SET payload = 'not json' WHERE kind = ?1 AND subject_id = ?2",
                crate::db::fixture::params![crate::db::TEARDOWN_SUSPEND_JOB_KIND, id],
            )
            .unwrap();
        }

        let records = crate::db::recover_interrupted_startup_states(&conn)
            .expect("one unreadable teardown job mustn't fail startup recovery");
        let record = records.iter().find(|record| record.id == id).unwrap();
        assert_eq!(
            record.outcome,
            crate::db::StartupRecoveryOutcome::CleanupRequired {
                keep_filesystem: true,
                completion: crate::db::CleanupCompletion::MarkSuspended,
            },
            "second_job={second_job}"
        );
    }
}

/// Only the shim's poll used to start a teardown's driver, and a pod
/// force-deleted while sandbox-manager was down never polls again. The
/// resumer drives a `suspending` row its teardown owns without one, so the
/// row startup recovery left to its teardown can't keep its runtime resident
/// with no owner. A `suspended` row under a running teardown is not resumed:
/// that hold stands until the cluster stops refusing its capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_resumer_starts_the_driver_of_a_teardown_owned_suspending_row() {
    with_cluster_api(RETIRING, async || {
        let conn = crate::db::test_conn();
        let owned = SandboxId::new();
        insert_stranded_teardown_suspend_sandbox(&conn, owned, generation(GENERATION));
        let held = SandboxId::new();
        insert_stranded_teardown_suspend_sandbox(&conn, held, generation(GENERATION));
        crate::db::test_fixtures::state_write_fixtures::force_state(
            &conn,
            &held.to_string(),
            SandboxState::Suspended,
        );
        let ended = SandboxId::new();
        insert_stranded_teardown_suspend_sandbox(&conn, ended, generation(GENERATION));
        crate::db::fail_teardown_suspend_operation(
            &conn,
            &ended.to_string(),
            generation(GENERATION),
            crate::db::TeardownSuspendFailure::Failed,
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, true);

        assert_eq!(
            service.resume_teardown_suspend_drivers(),
            vec![owned],
            "only the suspending row its teardown owns is resumed"
        );
        // A later pass finds the driver running, or settled and waiting out
        // its retry pacing; either way it takes nothing else.
        assert!(
            service
                .resume_teardown_suspend_drivers()
                .iter()
                .all(|resumed| *resumed == owned),
            "a later pass resumes nothing else"
        );
        drain(&service).await;
    })
    .await;
}

/// The resumer's scan takes only the job a poll would resume. It skips a row
/// it can't attribute to one teardown (a second active teardown job, even
/// when the first doesn't decode, or one whose payload doesn't decode), a
/// job still `pending`, and a job for a generation the row's runtime no
/// longer belongs to.
#[test]
fn the_resumer_skips_a_suspending_row_without_one_readable_teardown() {
    let conn = crate::db::test_conn();
    let owned = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, owned, generation(GENERATION));
    let doubled = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, doubled, generation(GENERATION));
    pplx_space_common_jobs::db::insert_job(
        conn.raw(),
        pplx_space_common_jobs::db::InsertJob {
            job_id: &JobId::new(),
            kind: crate::db::TEARDOWN_SUSPEND_JOB_KIND,
            payload: "{}",
            subject_id: Some(&doubled.to_string()),
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: pplx_space_common_jobs::JobPriority::default(),
        },
    )
    .unwrap();
    // Two active jobs, the first of them unreadable, are still two.
    let doubled_unreadable = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, doubled_unreadable, generation(GENERATION));
    conn.execute(
        "UPDATE jobs SET payload = 'not json' WHERE kind = ?1 AND subject_id = ?2",
        crate::db::fixture::params![
            crate::db::TEARDOWN_SUSPEND_JOB_KIND,
            doubled_unreadable.to_string()
        ],
    )
    .unwrap();
    pplx_space_common_jobs::db::insert_job(
        conn.raw(),
        pplx_space_common_jobs::db::InsertJob {
            job_id: &JobId::new(),
            kind: crate::db::TEARDOWN_SUSPEND_JOB_KIND,
            payload: &format!("{{\"execution_generation\":{GENERATION}}}"),
            subject_id: Some(&doubled_unreadable.to_string()),
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: pplx_space_common_jobs::JobPriority::default(),
        },
    )
    .unwrap();
    let unreadable = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, unreadable, generation(GENERATION));
    conn.execute(
        "UPDATE jobs SET payload = 'not json' WHERE kind = ?1 AND subject_id = ?2",
        crate::db::fixture::params![crate::db::TEARDOWN_SUSPEND_JOB_KIND, unreadable.to_string()],
    )
    .unwrap();

    // Not the job a poll would resume: still pending, or for a generation the
    // row's runtime no longer belongs to.
    let pending = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, pending, generation(GENERATION));
    conn.execute(
        "UPDATE jobs SET status = 'pending' WHERE kind = ?1 AND subject_id = ?2",
        crate::db::fixture::params![crate::db::TEARDOWN_SUSPEND_JOB_KIND, pending.to_string()],
    )
    .unwrap();
    let stale = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, stale, generation(GENERATION));
    conn.execute(
        "UPDATE sandboxes SET space_api_route_execution_generation = ?1 WHERE id = ?2",
        crate::db::fixture::params![GENERATION + 1, stale.to_string()],
    )
    .unwrap();

    let rows = crate::db::scan::teardown_owned_suspending(&conn).unwrap();
    assert_eq!(
        rows,
        vec![crate::db::TeardownOwnedSuspendingRow {
            sandbox_id: owned.to_string(),
            execution_generation: generation(GENERATION),
        }]
    );
}

/// Startup leaves a row only to a teardown that will drive it: its one active
/// job `running`, for the generation the row's runtime belongs to, as the
/// resumer requires. A job still `pending`, or one for an older generation,
/// isn't one a poll or the resumer would drive, so startup finishes the row as
/// it would without a teardown rather than leave it with no owner.
#[test]
fn startup_recovery_leaves_a_row_only_to_a_teardown_that_will_drive_it() {
    for stale_job in ["pending", "older_generation"] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
        if stale_job == "pending" {
            conn.execute(
                "UPDATE jobs SET status = 'pending' WHERE kind = ?1 AND subject_id = ?2",
                crate::db::fixture::params![crate::db::TEARDOWN_SUSPEND_JOB_KIND, id],
            )
            .unwrap();
        } else {
            conn.execute(
                "UPDATE sandboxes SET space_api_route_execution_generation = ?1 WHERE id = ?2",
                crate::db::fixture::params![GENERATION + 1, id],
            )
            .unwrap();
        }

        let records = crate::db::recover_interrupted_startup_states(&conn).unwrap();
        let record = records.iter().find(|record| record.id == id).unwrap();
        assert_eq!(
            record.outcome,
            crate::db::StartupRecoveryOutcome::CleanupRequired {
                keep_filesystem: true,
                completion: crate::db::CleanupCompletion::MarkSuspended,
            },
            "{stale_job}"
        );
    }
}
