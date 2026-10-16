use super::*;
use crate::service::cleanup_support::CleanupCompletion;
use crate::service::tests::service_tests::insert_test_sandbox;

#[tokio::test(start_paused = true)]
async fn inventory_stream_cancels_while_waiting_for_storage_capacity() {
    use tokio_stream::StreamExt;
    for reason in ["deadline", "shutdown", "reader"] {
        let service = SandboxManagerService::for_test(crate::db::test_conn());
        let stream_capacity = service.cleanup_target_streams.available_permits();
        let cache = tempfile::tempdir().unwrap();
        let permits = service.cleanup_io.available_permits();
        let _held = service
            .cleanup_io
            .clone()
            .acquire_many_owned(u32::try_from(permits).unwrap())
            .await
            .unwrap();
        let mut stream = Some(
            service
                .start_cleanup_target_stream(
                    cache.path().into(),
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .unwrap(),
        );
        // Polling once enters the page's semaphore wait, before cancellation.
        tokio::task::yield_now().await;
        match reason {
            "deadline" => tokio::time::advance(Duration::from_secs(10)).await,
            "shutdown" => service.shutdown_token.cancel(),
            "reader" => drop(stream.take()),
            _ => unreachable!(),
        }
        // The tracker also owns the service's long-lived scheduler. Wait for
        // stream capacity itself before canceling unrelated background work.
        let _streams_done = tokio::time::timeout(
            Duration::from_secs(1),
            service
                .cleanup_target_streams
                .clone()
                .acquire_many_owned(u32::try_from(stream_capacity).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        if let Some(mut stream) = stream {
            assert_eq!(
                stream.next().await.unwrap().unwrap_err().code(),
                if reason == "deadline" {
                    tonic::Code::DeadlineExceeded
                } else {
                    tonic::Code::Cancelled
                }
            );
            assert!(stream.next().await.is_none());
        }
        assert_eq!(service.cleanup_io.available_permits(), 0);
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }
}

#[tokio::test]
async fn selected_cleanup_rechecks_template_and_owner_after_acquiring_admission_fence() {
    use crate::service::cleanup_support::CleanupLockTryResult;
    use crate::service::tests::service_tests::service_for_teardown_test;
    use std::future::Future;
    for completion in [CleanupCompletion::MarkStopped, CleanupCompletion::DeleteRow] {
        for (change, require_template_owner, filter_template, expected_code) in [
            ("template", false, true, tonic::Code::Aborted),
            ("owner", true, false, tonic::Code::PermissionDenied),
            ("absent", false, true, tonic::Code::Aborted),
            ("absent", true, false, tonic::Code::PermissionDenied),
        ] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new();
            insert_test_sandbox(&conn, &id.to_string(), SandboxState::Started);
            conn.execute(
                "UPDATE sandboxes SET user = ?1 WHERE id = ?2",
                crate::db::fixture::params![
                    SandboxOwnerKind::TemplateManager
                        .reserved_owner_label()
                        .unwrap(),
                    id.to_string()
                ],
            )
            .unwrap();
            let service = service_for_teardown_test(conn);
            let selected_template =
                crate::db::cleanup_target(&service.db_conn().unwrap(), &id.to_string())
                    .unwrap()
                    .unwrap()
                    .template;
            let CleanupLockTryResult::Acquired(fence) = service.try_cleanup_fence(&id.to_string())
            else {
                panic!("unused cleanup fence")
            };
            let mut admission = Box::pin(service.admit_selected_cleanup(
                id,
                completion,
                false,
                true,
                filter_template.then_some(selected_template.as_str()),
                require_template_owner,
            ));
            std::future::poll_fn(|cx| {
                assert!(admission.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let sql = match change {
                "template" => "UPDATE sandboxes SET template = 'replacement' WHERE id = ?1",
                "owner" => "UPDATE sandboxes SET user = 'root@local' WHERE id = ?1",
                "absent" => "DELETE FROM sandboxes WHERE id = ?1",
                _ => unreachable!(),
            };
            service
                .db_conn()
                .unwrap()
                .execute(sql, [id.to_string()])
                .unwrap();
            drop(fence);
            let error = admission.await.err().unwrap().into_status();
            assert_eq!(error.code(), expected_code, "{change}");
            {
                let db = service.db_conn().unwrap();
                assert_eq!(
                    crate::db::current_state_label(&db, &id.to_string())
                        .unwrap()
                        .as_deref(),
                    (change != "absent").then_some("started")
                );
                assert!(
                    crate::db::cleanup_episode(&db, &id.to_string())
                        .unwrap()
                        .is_none()
                );
            }
            service.shutdown_token.cancel();
            service.task_tracker.close();
            service.task_tracker.wait().await;
        }
    }
}

#[tokio::test]
async fn streamed_inventory_includes_rowless_targets_and_preserves_bad_metadata() {
    let conn = crate::db::test_conn();
    let persisted = SandboxId::new().to_string();
    let malformed = SandboxId::new().to_string();
    let rowless = SandboxId::new().to_string();
    for id in [&persisted, &malformed] {
        insert_test_sandbox(&conn, id, SandboxState::Started);
    }
    conn.execute(
        "UPDATE sandboxes SET user = X'00' WHERE id = ?1",
        [&malformed],
    )
    .unwrap();
    let service = SandboxManagerService::for_test(conn);
    let cache = tempfile::tempdir().unwrap();
    for id in [&persisted, &rowless] {
        std::fs::create_dir(cache.path().join(id)).unwrap();
    }
    std::fs::create_dir(cache.path().join(".transfer-staging")).unwrap();
    let mut cursor = CleanupInventoryCursor::default();
    let mut found = Vec::new();
    while !cursor.complete {
        let (next, batch) = cursor.next_batch(&service, cache.path()).unwrap();
        assert!(batch.targets.len() <= BATCH_SIZE);
        found.extend(batch.targets);
        cursor = next;
    }
    found.sort_by(|a, b| a.sandbox_id.cmp(&b.sandbox_id));
    assert_eq!(found.len(), 3);
    for target in found {
        assert_eq!(target.metadata_valid, target.sandbox_id != malformed);
        if target.sandbox_id == rowless || target.sandbox_id == malformed {
            assert!(target.template.is_none());
        } else {
            assert!(target.template.is_some());
        }
    }
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}

#[tokio::test]
async fn inventory_stream_owns_capacity_and_reports_failure_instead_of_clean_eof() {
    use tokio_stream::StreamExt;
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let cache = tempfile::tempdir().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let capacity = service.cleanup_target_streams.available_permits();
    let streams = (0..capacity)
        .map(|_| {
            service
                .start_cleanup_target_stream(cache.path().into(), deadline)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        service
            .start_cleanup_target_stream(cache.path().into(), deadline)
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    drop(streams);
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    assert_eq!(service.cleanup_target_streams.available_permits(), capacity);

    for failure in ["deadline", "database"] {
        let service = SandboxManagerService::for_test(crate::db::test_conn());
        let deadline = if failure == "deadline" {
            tokio::time::Instant::now()
        } else {
            service
                .db_conn()
                .unwrap()
                .execute("DROP TABLE sandboxes", [])
                .unwrap();
            deadline
        };
        let mut stream = service
            .start_cleanup_target_stream(cache.path().into(), deadline)
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            if failure == "deadline" {
                tonic::Code::DeadlineExceeded
            } else {
                tonic::Code::Internal
            }
        );
        assert!(stream.next().await.is_none());
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }
    service.shutdown_token.cancel();
    assert_eq!(
        service
            .start_cleanup_target_stream(cache.path().into(), deadline)
            .err()
            .unwrap()
            .code(),
        tonic::Code::Unavailable
    );
}

#[test]
fn runtime_probe_does_not_hold_database_mutex() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Started);
    let db = crate::db::SharedConnection::new(conn);
    let cache = tempfile::tempdir().unwrap();
    let targets = resolve_lifecycle_target_blocking(
        &db,
        cache.path(),
        id.parse().unwrap(),
        None,
        false,
        true,
        |_| {
            let conn = db
                .try_lock()
                .expect("filesystem probes must not hold SQLite");
            assert!(crate::db::sandbox_exists(&conn, &id).unwrap());
            true
        },
    )
    .unwrap();
    assert_eq!(targets.unwrap().0, id);
}

#[tokio::test]
async fn normalized_selectors_reach_fenced_stop_and_delete_completion() {
    use crate::service::cleanup::SynchronousCleanupRequest;
    use crate::service::tests::service_tests::service_for_teardown_test;
    let canonical = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    for selector in [
        canonical.to_owned(),
        canonical.to_uppercase(),
        canonical.replace('-', ""),
        format!("{{{canonical}}}"),
        format!("urn:uuid:{canonical}"),
    ] {
        for completion in [CleanupCompletion::MarkStopped, CleanupCompletion::DeleteRow] {
            let conn = crate::db::test_conn();
            insert_test_sandbox(&conn, canonical, SandboxState::Started);
            let service = service_for_teardown_test(conn);
            let cache = tempfile::tempdir().unwrap();
            let target = resolve_lifecycle_target_blocking(
                &service.db,
                cache.path(),
                selector.parse().unwrap(),
                None,
                false,
                false,
                |_| true,
            )
            .unwrap();
            let (id, runtime) = target.as_ref().unwrap();
            assert_eq!(id, canonical);
            let stop = matches!(completion, CleanupCompletion::MarkStopped);
            let admission = service
                .admit_lifecycle_cleanup(
                    id.parse().unwrap(),
                    if stop {
                        SandboxState::Stopping
                    } else {
                        SandboxState::Deleting
                    },
                    stop,
                    completion,
                    false,
                )
                .await
                .unwrap();
            service
                .run_cleanup_to_completion_with_node_dispatch_and_admission(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: runtime.as_ref().unwrap().as_str(),
                        keep_filesystem: stop,
                        context: "normalized selector cleanup",
                        completion,
                        published_artifact: None,
                        immediate_kill: true,
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    Some(admission),
                    None,
                    || async { Ok(()) },
                    |_| async { Ok(()) },
                )
                .await
                .unwrap();
            let row = crate::db::get_sandbox(&service.db.lock().unwrap(), canonical).unwrap();
            if stop {
                assert_eq!(row.unwrap().state, SandboxState::Stopped);
            } else {
                assert!(row.is_none());
            }
            service.shutdown_token.cancel();
            service.task_tracker.close();
            service.task_tracker.wait().await;
        }
    }
}

#[test]
fn exact_requests_use_indexed_lookups_including_disk_only_and_absent_targets() {
    static QUERIES: Mutex<Vec<String>> = Mutex::new(Vec::new());
    use crate::db::fixture::{TraceEvent, TraceEventCodes};
    fn trace(event: TraceEvent<'_>) {
        if let TraceEvent::Stmt(statement, _) = event {
            QUERIES
                .lock()
                .unwrap()
                .push(statement.expanded_sql().unwrap());
        }
    }
    for (metadata, disk) in [(true, false), (true, true), (false, true), (false, false)] {
        let conn = crate::db::test_conn();
        let cache = tempfile::tempdir().unwrap();
        let target = SandboxId::new().to_string();
        for _ in 0..128 {
            insert_test_sandbox(
                &conn,
                &SandboxId::new().to_string(),
                SandboxState::Suspended,
            );
        }
        if metadata {
            insert_test_sandbox(&conn, &target, SandboxState::Started);
        }
        // Unrelated future metadata cannot poison an exact lookup.
        crate::db::seed_historical_lifecycle_rows(&conn, |conn| {
            conn.execute(
                "UPDATE sandboxes SET state = 'future_state' WHERE id != ?1",
                [&target],
            )
            .unwrap();
        });
        if metadata && !disk {
            // Canonical DB hits need no readable cache entry.
            std::os::unix::fs::symlink(&target, cache.path().join(&target)).unwrap();
        }
        if disk {
            std::fs::create_dir(cache.path().join(&target)).unwrap();
        }
        conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(trace));
        let db = crate::db::SharedConnection::new(conn);
        for selector in [&target, &target.to_uppercase(), &target.replace('-', "")] {
            let resolved = resolve_lifecycle_target_blocking(
                &db,
                cache.path(),
                selector.parse().unwrap(),
                None,
                false,
                false,
                |_| true,
            )
            .unwrap();
            assert_eq!(resolved.is_some(), metadata || disk);
        }
        let conn = db.lock().unwrap();
        conn.trace_v2(TraceEventCodes::empty(), None);
        for sql in QUERIES.lock().unwrap().drain(..) {
            let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            for detail in plan.query_map([], |row| row.get::<_, String>(3)).unwrap() {
                assert!(
                    !detail.unwrap().contains("SCAN sandboxes"),
                    "exact request scanned inventory: {sql}"
                );
            }
        }
    }
}

#[test]
fn exact_cleanup_rejects_target_metadata_damage_and_database_failure() {
    for damage in ["uuid8 = 'invalid/label'", "user = ''", "template = x'ff'"] {
        let conn = crate::db::test_conn();
        let broken = SandboxId::new().to_string();
        let healthy = SandboxId::new().to_string();
        for id in [&broken, &healthy] {
            insert_test_sandbox(&conn, id, SandboxState::Started);
        }
        conn.execute(
            &format!("UPDATE sandboxes SET {damage} WHERE id = ?1"),
            [&broken],
        )
        .unwrap();
        let db = crate::db::SharedConnection::new(conn);
        let cache = tempfile::tempdir().unwrap();
        for running in [false, true] {
            let resolve = |id: &str| {
                resolve_lifecycle_target_blocking(
                    &db,
                    cache.path(),
                    id.parse().unwrap(),
                    None,
                    false,
                    running,
                    |_| true,
                )
            };
            resolve(&broken).unwrap_err();
            assert_eq!(resolve(&healthy).unwrap().unwrap().0, healthy);
        }
        db.lock()
            .unwrap()
            .execute_batch("ALTER TABLE sandboxes RENAME COLUMN template TO removed_template")
            .unwrap();
        resolve_lifecycle_target_blocking(
            &db,
            cache.path(),
            healthy.parse().unwrap(),
            None,
            false,
            false,
            |_| true,
        )
        .unwrap_err();
    }
}

// A stop decides its guest flush under the admission fence, from the state it
// supersedes: neither a pause still preparing its own flush nor a completed
// pause proves that every acknowledged write was flushed, while a suspended
// sandbox has no VM to flush.
#[tokio::test]
async fn a_stops_guest_flush_is_decided_from_the_state_it_supersedes() {
    use crate::db::cleanup_jobs::{self, GuestFlush};
    use crate::service::tests::service_tests::service_for_teardown_test;
    for (state, expected) in [
        (SandboxState::Started, GuestFlush::BeforeTermination),
        (SandboxState::Pausing, GuestFlush::BeforeTermination),
        (SandboxState::Paused, GuestFlush::BeforeTermination),
        (SandboxState::Suspended, GuestFlush::Unrequested),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        insert_test_sandbox(&conn, &id.to_string(), state);
        let service = service_for_teardown_test(conn);
        // Keep the executor from taking the job before it is read.
        service.shutdown_token.cancel();
        let admission = service
            .admit_selected_cleanup(id, CleanupCompletion::MarkStopped, false, true, None, false)
            .await
            .unwrap_or_else(|error| panic!("{state:?}: {}", error.into_status()));
        let persisted = {
            let db = service.db_conn().unwrap();
            let job = pplx_space_common_jobs::db::find_active_job(
                db.raw(),
                cleanup_jobs::KIND,
                &id.to_string(),
            )
            .unwrap()
            .expect("the stop's cleanup is persisted");
            let payload = pplx_space_common_jobs::db::get_job(db.raw(), &job)
                .unwrap()
                .unwrap()
                .payload;
            cleanup_jobs::decode(&payload).unwrap().guest_flush
        };
        assert_eq!(persisted, expected, "{state:?}");
        drop(admission);
    }
}

// A paused VM is resumed so the flush can reach its guest: writes can
// complete after the pause's own flush, and stopping the paused VM would
// discard them. A VM whose state cannot be read is still offered the flush.
#[test]
fn a_stops_flush_resumes_a_paused_vm_first() {
    use crate::service::cleanup::BeforeStopFlush;
    use pplx_space_node_runtime_core::fc_api::FirecrackerVmState;
    assert_eq!(
        BeforeStopFlush::for_vm_state(Some(FirecrackerVmState::Paused)),
        BeforeStopFlush::ResumeThenFlush
    );
    assert_eq!(
        BeforeStopFlush::for_vm_state(Some(FirecrackerVmState::Running)),
        BeforeStopFlush::Flush
    );
    assert_eq!(BeforeStopFlush::for_vm_state(None), BeforeStopFlush::Flush);
    assert_eq!(
        BeforeStopFlush::for_vm_state(Some(FirecrackerVmState::NotStarted)),
        BeforeStopFlush::NotRunning
    );
}

// A stalled step ends the stop's flush attempt at its one budget, so the stop
// proceeds to terminate: a resume that never answers, or a flush that never
// finishes after a resume that did.
#[tokio::test(start_paused = true)]
async fn a_stops_flush_attempt_ends_at_its_budget_whichever_step_stalls() {
    use crate::service::cleanup::{StopFlushError, within_stop_flush_budget};
    let budget = std::time::Duration::from_secs(30);
    let flushed = std::sync::atomic::AtomicBool::new(false);

    let stalled_resume = within_stop_flush_budget(
        budget,
        std::future::pending::<Result<(), StopFlushError>>(),
        || async {
            flushed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<(), StopFlushError>(())
        },
    )
    .await;
    assert!(matches!(stalled_resume, Err(StopFlushError::TimedOut(spent)) if spent == budget));
    assert!(
        !flushed.load(std::sync::atomic::Ordering::SeqCst),
        "no flush follows a resume that never answered"
    );

    let stalled_flush = within_stop_flush_budget(budget, async { Ok(()) }, || {
        std::future::pending::<Result<(), StopFlushError>>()
    })
    .await;
    assert!(matches!(stalled_flush, Err(StopFlushError::TimedOut(_))));

    let failed_resume = within_stop_flush_budget(
        budget,
        async { Err(StopFlushError::VmNotRunning) },
        || async { Ok::<(), StopFlushError>(()) },
    )
    .await;
    assert!(matches!(failed_resume, Err(StopFlushError::VmNotRunning)));

    let failed_flush = within_stop_flush_budget(budget, async { Ok(()) }, || async {
        Err(crate::guest_filesystem_flush::GuestFlushError::Guest(
            tonic::Status::unavailable("guest unreachable"),
        ))
    })
    .await;
    assert!(matches!(
        failed_flush,
        Err(StopFlushError::Flush(
            crate::guest_filesystem_flush::GuestFlushError::Guest(_)
        ))
    ));

    within_stop_flush_budget(budget, async { Ok(()) }, || async {
        Ok::<(), StopFlushError>(())
    })
    .await
    .expect("a prompt preparation and flush complete");
}
