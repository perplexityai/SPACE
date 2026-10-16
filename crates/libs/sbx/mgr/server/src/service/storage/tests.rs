use super::*;
use crate::service::drain::with_state_writer;
use crate::service::tests::service_tests::{insert_test_sandbox, service_for_teardown_test};

#[tokio::test]
async fn cancelled_local_eviction_retains_writer_until_cleanup_persistence_finishes() {
    const ID: &str = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, ID, crate::state::SandboxState::Suspended);
    let mut service = service_for_teardown_test(conn);
    // Remove unrelated background readers before holding the database mutex.
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    service.shutdown_token = tokio_util::sync::CancellationToken::new();
    service.cleanup_io = Arc::new(tokio::sync::Semaphore::new(1));
    let capacity = service.cleanup_io.clone().acquire_owned().await.unwrap();

    // No task-local writer: eviction must propagate the lease it acquires itself.
    let waker = futures_util::task::noop_waker();
    let mut context = std::task::Context::from_waker(&waker);
    let mut eviction = Box::pin(service.evict_local_sandbox(
        ID,
        crate::service::eviction::EvictionClaim::SuspendedPinned(None),
        None,
    ));
    assert!(eviction.as_mut().poll(&mut context).is_pending());
    assert_eq!(
        service.lookup_sandbox(ID).unwrap().state,
        crate::state::SandboxState::Deleting,
    );
    let drain_pending = {
        let db = service.db.lock().unwrap();
        drop(capacity);
        // The storage permit is available, but the submitted blocking mutation
        // cannot finish until we release the database mutex.
        assert!(eviction.as_mut().poll(&mut context).is_pending());
        assert_eq!(service.cleanup_io.available_permits(), 0);
        service.begin_draining_for_restart();
        drop(eviction);
        let mut drained = Box::pin(service.wait_for_state_transitions_drained());
        let pending = drained.as_mut().poll(&mut context).is_pending();
        drop(db);
        pending
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.wait_for_state_transitions_drained(),
    )
    .await
    .unwrap();
    assert!(
        drain_pending,
        "cancelled eviction must not detach its database writer"
    );
    assert_eq!(service.cleanup_io.available_permits(), 1);
}

#[tokio::test]
async fn artifact_work_preserves_ownership_without_starving_cleanup_storage() {
    let mut service = service_for_teardown_test(crate::db::test_conn());
    service.cleanup_io = Arc::new(tokio::sync::Semaphore::new(1));
    service.artifact_io = Arc::new(tokio::sync::Semaphore::new(1));
    let lock = service.auto_lifecycle.artifact_lock("blocking-owner");
    let guard = lock.lock().await;
    let writer = service.state_writer_lease("Rollback").unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let worker = service.clone();
    let task = tokio::spawn(with_state_writer(writer, async move {
        worker
            .blocking_artifact_io(move |_| {
                let _guard = guard;
                started.send(()).unwrap();
                wait.recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                Ok(())
            })
            .await
    }));
    ready.await.unwrap();
    let storage_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.storage(|db| {
            insert_test_sandbox(
                db,
                "019d84bd-280d-7331-99c6-b63377f3a3a2",
                crate::state::SandboxState::Stopped,
            );
            Ok(())
        }),
    )
    .await;
    assert!(
        storage_result.is_ok(),
        "artifact I/O must not starve cleanup storage"
    );
    storage_result.unwrap().unwrap();
    service.begin_draining_for_restart();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(service.active_state_transition_count(), 1);
    assert_eq!(service.artifact_io.available_permits(), 0);
    lock.try_lock()
        .map(|_| ())
        .expect_err("artifact exclusion must survive");

    release.send(()).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.wait_for_state_transitions_drained(),
    )
    .await
    .unwrap();
    assert_eq!(service.artifact_io.available_permits(), 1);
    drop(lock.try_lock().unwrap());
}

#[tokio::test]
async fn network_paced_exports_leave_artifact_work_available() {
    let service = service_for_teardown_test(crate::db::test_conn());
    let writer = service.state_writer_lease("ExportSnapshot").unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let worker = service.clone();
    let task = tokio::spawn(with_state_writer(writer, async move {
        worker
            .blocking_export_io(move |_| {
                started.send(()).unwrap();
                wait.recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                Ok(())
            })
            .await
    }));
    ready.await.unwrap();
    // Saturate the rest of the export lane, then prove actual artifact work runs.
    let remaining = service
        .export_io
        .clone()
        .try_acquire_many_owned((EXPORT_IO_CONCURRENCY - 1) as u32)
        .unwrap();
    service.export_io.try_acquire().unwrap_err();
    let work = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.blocking_artifact_io(|_| Ok(())),
    )
    .await;
    service.begin_draining_for_restart();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(service.active_state_transition_count(), 1);
    release.send(()).unwrap();
    drop(remaining);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.wait_for_state_transitions_drained(),
    )
    .await
    .unwrap();
    work.expect("exports must not occupy artifact-work permits")
        .unwrap();
}

#[tokio::test]
async fn storage_waits_off_runtime_and_preserves_transaction_rollback_and_poison_errors() {
    const ID: &str = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let mut service = service_for_teardown_test(crate::db::test_conn());
    // The fixture starts watchdogs that also take the DB mutex inline. Stop
    // those unrelated readers before testing this storage boundary's progress.
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    service.shutdown_token = tokio_util::sync::CancellationToken::new();
    let locked = service.db.clone();
    let (holding, held) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _db = locked.lock().unwrap();
        holding.send(()).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    });
    held.await.unwrap();
    let worker = service.clone();
    let (polled, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        polled.send(()).unwrap();
        worker.storage(|db| Ok(db.is_autocommit())).await
    });
    ready.await.unwrap();
    // On the single-thread test runtime this cannot progress if storage tries
    // to acquire the std mutex inline.
    tokio::task::yield_now().await;
    release.send(()).unwrap();
    holder.join().unwrap();
    assert!(task.await.unwrap().unwrap());

    let error = service
        .storage(|db| {
            let tx = crate::db::Transaction::deferred(db).unwrap();
            insert_test_sandbox(&tx, ID, crate::state::SandboxState::Created);
            Err::<(), _>(Status::aborted("abort transaction"))
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(
        !service
            .storage(|db| crate::db::sandbox_exists(db, ID)
                .map_err(|error| Status::internal(error.to_string())))
            .await
            .unwrap()
    );

    let poisoned = service.db.clone();
    assert!(
        std::thread::spawn(move || {
            let _db = poisoned.lock().unwrap();
            panic!("inject database mutex poison");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        service.storage(|_| Ok(())).await.unwrap_err().code(),
        tonic::Code::Internal,
    );
    assert!(service.db.is_poisoned(), "poison must not silently reset");
    assert!(service.shutdown_token.is_cancelled());
    assert!(service.is_draining_for_restart());
    assert!(service.critical_worker_failed());
    assert!(service.state_writer_lease("Start").is_err());
}
