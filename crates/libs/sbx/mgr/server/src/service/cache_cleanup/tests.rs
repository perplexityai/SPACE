use pplx_space_common_core::types::LocalArtifactId;

use super::*;

#[test]
fn poisoned_inventory_connection_returns_error_without_running_the_mutation() {
    let db = std::sync::Arc::new(std::sync::Mutex::new(crate::db::test_conn()));
    let poison = db.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.lock().unwrap();
            panic!("poison fixture");
        })
        .join()
        .is_err()
    );
    let mut called = false;
    let result = db.with_connection(|_| {
        called = true;
    });
    assert!(matches!(result, Err(InventoryError::DatabaseMutexPoisoned)));
    assert!(!called);
    assert!(db.is_poisoned());
}
use crate::service::cleanup_support::{
    CleanupCompletion, CleanupLockGuard, CleanupLockMap, CleanupLockTryResult,
    OwnedCleanupFenceGuard,
};

use crate::service::suspend_snapshot_publication::{
    canonical_suspend_snapshot_dir, write_full_suspend_artifact,
};
use crate::service::tests::service_tests::insert_test_sandbox;

#[test]
fn orphan_materialization_requires_observed_authority_without_overwriting_a_new_owner() {
    for (live, authority, row_appears) in [
        (false, Ok(None), false),
        (true, Ok(None), false),
        (
            false,
            Err(Status::unavailable("receiver restarting")),
            false,
        ),
        (true, Err(Status::unavailable("receiver restarting")), false),
        (false, inventory_execution_for_test(SandboxId::new()), false),
        (false, inventory_execution_for_test(SandboxId::new()), true),
    ] {
        let db = Mutex::new(crate::db::test_conn());
        let cache = tempfile::tempdir().unwrap();
        let run = tempfile::tempdir().unwrap();
        let id = SandboxId::new();
        std::fs::create_dir(cache.path().join(id.to_string())).unwrap();
        if live {
            let runtime = run.path().join(id.to_string());
            std::fs::create_dir(&runtime).unwrap();
            std::os::unix::fs::symlink(cache.path(), runtime.join("fc.pid")).unwrap();
        }
        let appeared = std::cell::Cell::new(None);
        let observed = authority.as_ref().ok().copied().flatten();
        let report = reconcile_sandbox_inventory_ids(
            &db,
            SandboxInventoryPaths::new(cache.path(), run.path()),
            vec![id],
            if live { vec![id] } else { Vec::new() },
            if live { vec![id] } else { Vec::new() },
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(inventory_cleanup_guard_for_test, |_| Some(()), &|_| {
                let conn = db
                    .try_lock()
                    .expect("receiver inspection must not hold SQLite");
                if row_appears {
                    insert_test_sandbox(&conn, &id.to_string(), SandboxState::Created);
                    appeared.set(crate::db::node_execution_fence(&conn, &id.to_string()).unwrap());
                }
                authority.clone()
            }),
        )
        .unwrap();
        let conn = db.lock().unwrap();
        let row = crate::db::get_sandbox(&conn, &id.to_string()).unwrap();
        if row_appears {
            assert!(report.changed_ids().is_empty());
            assert_eq!(row.unwrap().state, SandboxState::Created);
            assert_eq!(
                crate::db::node_execution_fence(&conn, &id.to_string()).unwrap(),
                appeared.get()
            );
        } else if authority.is_err() || (live && observed.is_none()) {
            assert!(
                row.is_none(),
                "unconfirmed authority must not poison a durable cleanup retry"
            );
            assert!(report.changed_ids().is_empty());
        } else {
            assert_eq!(report.changed_ids(), vec![id]);
            assert_eq!(row.unwrap().state, SandboxState::Quarantined);
            let fence = crate::db::node_execution_fence(&conn, &id.to_string())
                .unwrap()
                .unwrap();
            if let Some(observed) = observed {
                assert_eq!(fence, observed);
            }
            let retired: bool = conn
                .query_row(
                    "SELECT node_execution_retired FROM sandboxes WHERE id = ?1",
                    [id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retired, observed.is_none());
        }
    }
}

#[tokio::test]
async fn orphan_authority_reaches_cleanup_and_is_never_refreshed_on_rejection() {
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    use pplx_space_node_mgr_client::proto::{
        CleanupSandboxResponse, InspectSandboxRequest, InspectSandboxResponse,
    };

    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("node.sock");
    let cache = root.path().join("cache");
    let run = root.path().join("run");
    let id = SandboxId::new();
    std::fs::create_dir(&cache).unwrap();
    std::fs::create_dir_all(run.join(id.to_string())).unwrap();
    std::os::unix::fs::symlink(&cache, run.join(id.to_string()).join("fc.pid")).unwrap();
    let db = Arc::new(Mutex::new(crate::db::test_conn()));
    let receiver = Arc::new(Mutex::new(Err(Status::unavailable("receiver restarting"))));
    let inspections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let inspect = tower::service_fn({
        let receiver = receiver.clone();
        let inspections = inspections.clone();
        let db = db.clone();
        move |request: Request<InspectSandboxRequest>| {
            drop(
                db.try_lock()
                    .expect("receiver inspection must not hold SQLite"),
            );
            assert!(request.get_ref().execution_only);
            inspections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let authority: Result<Option<ExecutionFence>, Status> =
                receiver.lock().unwrap().clone();
            async move {
                Ok::<_, Status>(Response::new(InspectSandboxResponse {
                    execution_fence: authority?.map(Into::into),
                    ..Default::default()
                }))
            }
        }
    });
    let cleanup = tower::service_fn({
        let receiver = receiver.clone();
        move |request: Request<CleanupSandboxRequest>| {
            let authority = receiver.lock().unwrap().clone();
            async move {
                let authority = authority?.unwrap();
                let requested =
                    ExecutionFence::try_from(request.get_ref().execution_fence.as_ref().unwrap())?;
                if !requested.owns(authority.current()) {
                    return Err(Status::failed_precondition("execution mismatch"));
                }
                Ok::<_, Status>(Response::new(CleanupSandboxResponse {
                    retained_execution_id: Some(authority.current().to_string()),
                    ..Default::default()
                }))
            }
        }
    });
    let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        let inspect = inspect.clone();
        let cleanup = cleanup.clone();
        async move {
            let response = if request.uri().path().ends_with("/InspectSandbox") {
                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(inspect, request)
                    .await
            } else {
                assert!(request.uri().path().ends_with("/CleanupSandbox"));
                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(cleanup, request)
                    .await
            };
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(tonic::transport::Server::builder().serve_with_incoming(
        rpc,
        tokio_stream::wrappers::UnixListenerStream::new(listener),
    ));
    let channel = pplx_space_common_grpc::connect_uds_channel("orphan-authority", &socket)
        .await
        .unwrap();
    let mut client = NodeManagerGrpcClient::new(
        pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
    );
    let reconcile = || {
        let (db, client, cache, run) = (db.clone(), client.clone(), cache.clone(), run.clone());
        tokio::task::spawn_blocking(move || {
            let observe = orphan_execution_observer(client, CancellationToken::new());
            reconcile_sandbox_inventory_ids(
                db.as_ref(),
                SandboxInventoryPaths::new(&cache, &run),
                Vec::new(),
                vec![id],
                vec![id],
                ReconcileSource::Test,
                InventoryOwnershipGuards::new(
                    inventory_cleanup_guard_for_test,
                    |_| Some(()),
                    &observe,
                ),
            )
            .unwrap()
        })
    };
    assert!(reconcile().await.unwrap().changed_ids().is_empty());
    assert!(!crate::db::sandbox_exists(&db.lock().unwrap(), &id.to_string()).unwrap());
    let original = ExecutionFence::new(
        SandboxActivationAttemptId::new(),
        Some(SandboxActivationAttemptId::new()),
    );
    *receiver.lock().unwrap() = Ok(Some(original));
    assert_eq!(reconcile().await.unwrap().changed_ids(), vec![id]);
    assert_eq!(
        crate::db::node_execution_fence(&db.lock().unwrap(), &id.to_string()).unwrap(),
        Some(original)
    );
    super::super::cleanup_support::cleanup_sandbox_via_node(
        &db,
        &mut client,
        &id.to_string(),
        true,
        "orphan-test",
    )
    .await
    .unwrap();
    let acknowledged =
        crate::db::node_execution_fence(&db.lock().unwrap(), &id.to_string()).unwrap();
    assert_eq!(
        acknowledged,
        Some(ExecutionFence::new(original.current(), None))
    );
    *receiver.lock().unwrap() = Ok(Some(ExecutionFence::new(
        SandboxActivationAttemptId::new(),
        None,
    )));
    for _ in 0..2 {
        let error = super::super::cleanup_support::cleanup_sandbox_via_node(
            &db,
            &mut client,
            &id.to_string(),
            true,
            "orphan-test",
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            crate::db::node_execution_fence(&db.lock().unwrap(), &id.to_string()).unwrap(),
            acknowledged
        );
    }
    assert_eq!(
        inspections.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "cleanup must not refresh authority after rejection"
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[test]
fn inventory_preserves_malformed_rows_without_blocking_healthy_progress() {
    let conn = crate::db::test_conn();
    let cache = tempfile::tempdir().unwrap();
    let run = tempfile::tempdir().unwrap();
    let malformed = SandboxId::new();
    let stale = SandboxId::new();
    for id in [malformed, stale] {
        insert_test_sandbox(&conn, &id.to_string(), SandboxState::Stopped);
    }
    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'invalid' WHERE id = ?1",
        [malformed.to_string()],
    )
    .unwrap();
    let report =
        reconcile_sandbox_inventory_in(&conn, cache.path(), run.path(), ReconcileSource::Test)
            .unwrap();
    assert_eq!(report.changed_ids(), vec![stale]);
    assert!(crate::db::sandbox_exists(&conn, &malformed.to_string()).unwrap());
    assert!(!crate::db::sandbox_exists(&conn, &stale.to_string()).unwrap());
}

#[cfg(unix)]
#[test]
fn inventory_does_not_treat_an_unreadable_cache_path_as_absence() {
    let conn = crate::db::test_conn();
    let cache = tempfile::tempdir().unwrap();
    let run = tempfile::tempdir().unwrap();
    let id = SandboxId::new();
    insert_test_sandbox(&conn, &id.to_string(), SandboxState::Stopped);
    let root = cache.path().join(id.to_string());
    std::os::unix::fs::symlink(&root, &root).unwrap();
    let report =
        reconcile_sandbox_inventory_in(&conn, cache.path(), run.path(), ReconcileSource::Test)
            .unwrap();
    assert!(report.changed_ids().is_empty());
    assert!(crate::db::sandbox_exists(&conn, &id.to_string()).unwrap());
}

struct SecondCallMutatingInventoryDb<Mutation> {
    conn: std::cell::RefCell<rusqlite::Connection>,
    calls: std::cell::Cell<usize>,
    mutation: Mutation,
}

impl<Mutation> InventoryDbAccess for SecondCallMutatingInventoryDb<Mutation>
where
    Mutation: Fn(&rusqlite::Connection),
{
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Connection) -> T,
    ) -> Result<T, InventoryError> {
        let call = self.calls.get();
        self.calls.set(call + 1);
        let conn = self.conn.borrow_mut();
        if call == 1 {
            (self.mutation)(&conn);
        }
        Ok(operation(&conn))
    }
}

fn try_cleanup_guard(locks: &CleanupLockMap, sandbox_id: &str) -> CleanupLockTryResult {
    let lock = {
        let mut entries = locks.lock().expect("cleanup lock map poisoned");
        Arc::clone(
            entries
                .entry(sandbox_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    };
    CleanupLockGuard::new(Arc::clone(locks), sandbox_id.to_string(), lock).try_acquire_owned()
}

fn acquired_cleanup_guard(locks: &CleanupLockMap, sandbox_id: &str) -> OwnedCleanupFenceGuard {
    match try_cleanup_guard(locks, sandbox_id) {
        CleanupLockTryResult::Acquired(guard) => guard,
        CleanupLockTryResult::Busy => panic!("cleanup lock must be free"),
    }
}

#[tokio::test]
async fn row_cleanup_retires_policy_only_after_identity_is_deleted() {
    use crate::service::cleanup::SynchronousCleanupRequest;

    for (completion, replacement, activation_episode) in [
        (CleanupCompletion::DeleteRow, false, false),
        (CleanupCompletion::MarkStopped, false, false),
        (CleanupCompletion::DeleteRow, true, false),
        (CleanupCompletion::MarkStopped, true, true),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        let state = if activation_episode {
            SandboxState::Stopped
        } else if completion.deletes_row() {
            SandboxState::Deleting
        } else {
            SandboxState::Stopping
        };
        insert_test_sandbox(&conn, &id, state);
        let cleanup_id = activation_episode.then(|| {
            let episode = crate::db::begin_activation(
                &conn,
                &id,
                state,
                crate::db::ActivationKind::ColdStart,
                None,
                None,
            )
            .unwrap()
            .unwrap();
            crate::db::request_cleanup_episode(
                &conn,
                &id,
                episode.id,
                SandboxQuarantineReasonCode::ColdActivationInterrupted,
                "activation interrupted",
            )
            .unwrap();
            episode.id
        });
        let service = SandboxManagerService::for_test(conn);
        let queue = &service.network_policy_reconcile_queue;
        queue.enqueue(id.clone());
        let old_attempt = queue.take_pending(&Default::default(), 1).pop().unwrap();
        queue.enqueue(id.clone());
        let uuid8 = service.lookup_sandbox(&id).unwrap().uuid8;
        service
            .run_cleanup_to_completion_with_node_dispatch_and_admission(
                SynchronousCleanupRequest {
                    sandbox_id: &id,
                    uuid8: uuid8.as_str(),
                    keep_filesystem: !completion.deletes_row(),
                    context: "policy retirement test",
                    completion,
                    published_artifact: None,
                    immediate_kill: false,
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                },
                None,
                cleanup_id,
                || async { Ok(()) },
                |()| async {
                    if replacement {
                        // Model a newer identity visible when the old remote
                        // cleanup returns; its maps and obligation must survive.
                        service.db.lock().unwrap().execute(
                            "UPDATE sandboxes SET state = 'started', space_api_route_execution_generation = 2 WHERE id = ?1",
                            [&id],
                        ).unwrap();
                        service.watchdog.rearm_sandbox_from_db(&id).await;
                        service.snapshot_scheduler.add_sandbox(id.clone(), "seed".into()).await;
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        let deleted = completion.deletes_row() && !replacement;
        assert_eq!(queue.drain_pending().contains(&id), !deleted);
        queue.finish_attempt(old_attempt, Some("late failure"), Duration::ZERO);
        assert_eq!(
            queue.promote_due_retries(tokio::time::Instant::now()),
            usize::from(!deleted)
        );
        assert_eq!(service.lookup_sandbox(&id).is_ok(), !deleted);
        if replacement {
            assert!(service.watchdog.get_health(&id).await.is_some());
            assert!(service.snapshot_scheduler.is_active(&id).await);
        }
    }
}

#[tokio::test]
async fn inventory_retires_registries_atomically_and_retries_busy_registrations() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let service = SandboxManagerService::for_test(conn);
    let cache = tempfile::tempdir().unwrap();
    let run = tempfile::tempdir().unwrap();
    service.network_policy_reconcile_queue.enqueue(id.clone());
    service.retire_sandbox_registrations(&id).await;
    assert!(
        service
            .network_policy_reconcile_queue
            .drain_pending()
            .contains(&id),
        "stopped row must retain its policy obligation"
    );
    service.watchdog.rearm_sandbox_from_db(&id).await;
    service
        .snapshot_scheduler
        .add_sandbox(id.clone(), "seed".into())
        .await;
    service.auto_lifecycle.record_wake_completed(&id);
    service.network_policy_reconcile_queue.enqueue(id.clone());
    let wake = service.auto_lifecycle.wake_lock(&id);
    let wake_guard = wake.lock().await;
    let reconcile = || {
        reconcile_sandbox_inventory_ids(
            service.db.as_ref(),
            SandboxInventoryPaths::new(cache.path(), run.path()),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                |id: &str| service.try_cleanup_fence(id),
                |id: SandboxId| service.auto_lifecycle.try_staging_guard(&id.to_string()),
                &inventory_execution_for_test,
            )
            .with_retirement(|id: &str| {
                let retire = service.try_registration_retirement(id)?;
                let db = &service.db;
                Some(move || {
                    assert!(matches!(db.try_lock(), Err(std::sync::TryLockError::WouldBlock)),
                        "same-ID insertion must remain excluded until old policy obligations are retired");
                    retire();
                })
            }),
        )
        .unwrap()
    };
    let busy = service.snapshot_scheduler.try_retirement(&id).unwrap();
    assert!(reconcile().changed_ids().is_empty());
    assert!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &id)
            .unwrap()
            .is_some()
    );
    assert!(service.watchdog.get_health(&id).await.is_some());
    drop(busy);

    assert_eq!(reconcile().removed_ids.len(), 1);
    assert!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &id)
            .unwrap()
            .is_none()
    );
    assert!(service.watchdog.get_health(&id).await.is_none());
    assert!(service.watchdog.sidecar_info_for_test(&id).await.is_none());
    assert!(!service.snapshot_scheduler.is_active(&id).await);
    assert!(
        service
            .auto_lifecycle
            .since_last_wake_completed(&id)
            .is_none()
    );
    assert!(
        service
            .network_policy_reconcile_queue
            .take_pending(&Default::default(), 1)
            .is_empty()
    );
    assert!(Arc::ptr_eq(&wake, &service.auto_lifecycle.wake_lock(&id)));
    drop(wake_guard);
}

#[test]
fn inventory_does_not_delete_starting_activation_episode_before_runtime_appears() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Created);

    let episode = crate::db::begin_activation(
        &conn,
        &sandbox_id_string,
        SandboxState::Created,
        crate::db::ActivationKind::ColdStart,
        None,
        None,
    )
    .expect("activation claim")
    .expect("activation must claim the created row");

    let report = reconcile_sandbox_inventory_in(
        &conn,
        cache_dir.path(),
        run_dir.path(),
        ReconcileSource::Test,
    )
    .expect("inventory reconcile");

    assert!(report.changed_ids().is_empty());
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .expect("read sandbox")
        .expect("inventory must preserve the activation-owned row");
    assert_eq!(row.state, SandboxState::Starting);
    let current_episode = crate::db::cleanup_episode(&conn, &sandbox_id_string)
        .expect("read cleanup episode")
        .expect("inventory must preserve the activation episode");
    assert_eq!(current_episode.id, episode.id);
    assert!(
        current_episode.requested_at.is_none(),
        "inventory must not turn a live activation into cleanup work"
    );
}

#[test]
fn orphan_materialization_rechecks_filesystem_after_acquiring_database_mutex() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_root = cache_dir.path().join(sandbox_id.to_string());
    std::fs::create_dir_all(sandbox_root.join("instance")).expect("create sandbox cache");
    std::fs::write(sandbox_root.join("instance/rootfs.ext4"), b"rootfs")
        .expect("write rootfs evidence");
    let db = SecondCallMutatingInventoryDb {
        conn: std::cell::RefCell::new(conn),
        calls: std::cell::Cell::new(0),
        mutation: move |_: &rusqlite::Connection| {
            std::fs::remove_dir_all(&sandbox_root).expect("remove stale filesystem evidence");
        },
    };

    let report = reconcile_sandbox_inventory_ids(
        &db,
        SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
        vec![sandbox_id],
        Vec::new(),
        Vec::new(),
        ReconcileSource::Test,
        InventoryOwnershipGuards::new(
            inventory_cleanup_guard_for_test,
            |_: SandboxId| Some(()),
            &inventory_execution_for_test,
        ),
    )
    .expect("inventory reconcile");

    assert!(report.changed_ids().is_empty());
    assert!(
        crate::db::get_sandbox(&db.conn.borrow(), &sandbox_id.to_string())
            .expect("read sandbox")
            .is_none(),
        "evidence removed while waiting for SQLite must not be materialized"
    );
}

#[test]
fn stale_row_rechecks_filesystem_after_acquiring_database_mutex() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Stopped);
    let sandbox_root = cache_dir.path().join(&sandbox_id_string);
    let db = SecondCallMutatingInventoryDb {
        conn: std::cell::RefCell::new(conn),
        calls: std::cell::Cell::new(0),
        mutation: move |_: &rusqlite::Connection| {
            std::fs::create_dir_all(&sandbox_root).expect("restore filesystem evidence");
        },
    };

    let report = reconcile_sandbox_inventory_ids(
        &db,
        SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        ReconcileSource::Test,
        InventoryOwnershipGuards::new(
            inventory_cleanup_guard_for_test,
            |_: SandboxId| Some(()),
            &inventory_execution_for_test,
        ),
    )
    .expect("inventory reconcile");

    assert!(report.changed_ids().is_empty());
    assert!(
        crate::db::get_sandbox(&db.conn.borrow(), &sandbox_id_string)
            .expect("read sandbox")
            .is_some(),
        "filesystem evidence restored while waiting for SQLite must preserve the row"
    );
}

#[test]
fn nested_materialization_database_failure_is_classified_as_database_error() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let snapshot_dir = canonical_suspend_snapshot_dir(&cache_dir.path().join(&sandbox_id_string));
    write_full_suspend_artifact(
        &snapshot_dir,
        &sandbox_id_string,
        LocalArtifactId::new(),
        b"complete-suspend",
    );
    let db = SecondCallMutatingInventoryDb {
        conn: std::cell::RefCell::new(conn),
        calls: std::cell::Cell::new(0),
        mutation: |conn: &rusqlite::Connection| {
            conn.execute_batch("DROP TABLE local_artifacts")
                .expect("break nested artifact registration");
        },
    };
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();

    reconcile_sandbox_inventory_ids(
        &db,
        SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
        vec![sandbox_id],
        Vec::new(),
        Vec::new(),
        ReconcileSource::Test,
        InventoryOwnershipGuards::new(
            inventory_cleanup_guard_for_test,
            |_: SandboxId| Some(()),
            &inventory_execution_for_test,
        ),
    )
    .expect("inventory reconcile");
    let counters = capture.finish();

    assert!(counters.iter().any(|counter| {
        counter.name == "sandbox.worker.inventory_reconcile"
            && counter.tags.iter().any(|tag| tag == "outcome:failed")
            && counter
                .tags
                .iter()
                .any(|tag| tag == "reason:database_error")
    }));
}

#[test]
fn inventory_live_cleanup_lock_defers_one_sandbox_without_blocking_another() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let locked_id = SandboxId::new();
    let unrelated_id = SandboxId::new();
    insert_test_sandbox(&conn, &locked_id.to_string(), SandboxState::Stopped);
    insert_test_sandbox(&conn, &unrelated_id.to_string(), SandboxState::Stopped);
    let locks = Arc::new(Mutex::new(BTreeMap::new()));
    let owner = acquired_cleanup_guard(&locks, &locked_id.to_string());

    let report = reconcile_sandbox_inventory_ids(
        &conn,
        SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        ReconcileSource::Test,
        InventoryOwnershipGuards::new(
            |sandbox_id: &str| try_cleanup_guard(&locks, sandbox_id),
            |_: SandboxId| Some(()),
            &inventory_execution_for_test,
        ),
    )
    .expect("inventory reconcile");

    assert_eq!(report.changed_ids(), vec![unrelated_id]);
    assert!(
        crate::db::get_sandbox(&conn, &locked_id.to_string())
            .expect("read locked sandbox")
            .is_some()
    );
    assert!(
        crate::db::get_sandbox(&conn, &unrelated_id.to_string())
            .expect("read unrelated sandbox")
            .is_none()
    );

    drop(owner);
    assert!(
        locks.lock().expect("cleanup lock map poisoned").is_empty(),
        "RAII owners must prune both the busy contender and completed lock"
    );
}

#[test]
fn inventory_preserves_a_row_with_malformed_cleanup_intent() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Stopped);
    conn.execute(
        "UPDATE sandboxes SET cleanup_completion = 'mark_stopped' WHERE id = ?1",
        [&sandbox_id_string],
    )
    .expect("write partial cleanup intent");
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();

    let report = reconcile_sandbox_inventory_in(
        &conn,
        cache_dir.path(),
        run_dir.path(),
        ReconcileSource::Test,
    )
    .expect("inventory reconcile");
    let counters = capture.finish();

    assert!(report.changed_ids().is_empty());
    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id_string)
            .expect("read sandbox")
            .is_some(),
        "inventory must not guess the missing half of cleanup intent"
    );
    assert!(counters.iter().any(|counter| {
        counter.name == "sandbox.worker.inventory_reconcile"
            && counter.tags.iter().any(|tag| tag == "outcome:deferred")
            && counter
                .tags
                .iter()
                .any(|tag| tag == "reason:malformed_cleanup_intent")
    }));
}

#[test]
fn inventory_preserves_unfinished_teardown_without_a_live_lock() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Deleting);
    let stopping_id = SandboxId::new();
    let stopping_id_string = stopping_id.to_string();
    insert_test_sandbox(&conn, &stopping_id_string, SandboxState::Stopping);
    let persisted_intent_id = SandboxId::new();
    let persisted_intent_id_string = persisted_intent_id.to_string();
    insert_test_sandbox(&conn, &persisted_intent_id_string, SandboxState::Stopped);
    crate::db::set_cleanup_intent(
        &conn,
        &persisted_intent_id_string,
        false,
        crate::db::CleanupCompletion::DeleteRow,
    )
    .expect("persist cleanup intent");

    let report = reconcile_sandbox_inventory_in(
        &conn,
        cache_dir.path(),
        run_dir.path(),
        ReconcileSource::Test,
    )
    .expect("inventory reconcile");

    assert!(report.changed_ids().is_empty());
    assert_eq!(
        crate::db::get_sandbox(&conn, &sandbox_id_string)
            .expect("read sandbox")
            .expect("deleting row is durable lifecycle ownership")
            .state,
        SandboxState::Deleting
    );
    assert_eq!(
        crate::db::get_sandbox(&conn, &stopping_id_string)
            .expect("read sandbox")
            .expect("stopping row is durable lifecycle ownership")
            .state,
        SandboxState::Stopping
    );
    assert!(
        crate::db::get_sandbox(&conn, &persisted_intent_id_string)
            .expect("read cleanup-intent sandbox")
            .is_some()
    );
}

#[test]
fn inventory_still_removes_unowned_starting_row() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Starting);

    let report = reconcile_sandbox_inventory_in(
        &conn,
        cache_dir.path(),
        run_dir.path(),
        ReconcileSource::Test,
    )
    .expect("inventory reconcile");

    assert_eq!(report.changed_ids(), vec![sandbox_id]);
    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id_string)
            .expect("read sandbox")
            .is_none()
    );
}

#[test]
fn rowless_restore_scope_defers_materialization_until_retired() {
    let conn = crate::db::test_conn();
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    let sandbox_root = cache_dir.path().join(&sandbox_id_string);
    std::fs::create_dir_all(sandbox_root.join("instance")).expect("create sandbox cache");
    std::fs::write(sandbox_root.join("instance/rootfs.ext4"), b"rootfs")
        .expect("write rootfs evidence");
    assert!(
        crate::db::insert_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
            None,
            None,
            "/space/cache/.restore-staging",
        )
        .expect("insert restore transfer scope")
    );

    let deferred = reconcile_sandbox_inventory_in(
        &conn,
        cache_dir.path(),
        run_dir.path(),
        ReconcileSource::Test,
    )
    .expect("inventory reconcile");
    assert!(deferred.changed_ids().is_empty());
    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id_string)
            .expect("read sandbox")
            .is_none()
    );

    assert!(
        crate::db::retire_rowless_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
        )
        .expect("retire restore transfer scope")
    );
    let materialized = reconcile_sandbox_inventory_in(
        &conn,
        cache_dir.path(),
        run_dir.path(),
        ReconcileSource::Test,
    )
    .expect("inventory reconcile after scope retirement");
    assert_eq!(materialized.changed_ids(), vec![sandbox_id]);
    assert_eq!(
        crate::db::get_sandbox(&conn, &sandbox_id_string)
            .expect("read materialized sandbox")
            .expect("orphan must be classified after ownership retires")
            .state,
        SandboxState::Quarantined
    );
}

#[test]
fn inventory_keeps_a_quarantined_row_without_local_evidence() {
    // A quarantine is held for operator or cluster disposition, so missing
    // cache and runtime evidence is not proof that its row is stale.
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::tests::setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let sandbox_id_string = sandbox_id.to_string();
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .expect("read sandbox")
        .expect("quarantined row");
    assert_eq!(row.state, SandboxState::Quarantined);
    assert!(row.current_local_artifact_id.is_some());
    assert_eq!(
        crate::db::current_quarantine_reason_wire_value(&conn, &sandbox_id_string)
            .expect("read quarantine reason")
            .as_deref(),
        Some(SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed.as_str())
    );
    let before = crate::db::tests::database_rows(&conn);
    let cache_dir = tempfile::tempdir().expect("create cache directory");
    let run_dir = tempfile::tempdir().expect("create run directory");
    let paths = SandboxInventoryPaths::new(cache_dir.path(), run_dir.path());
    // Every candidate takes its cleanup fence first, so the fence requests are
    // the candidates: skipping the hold before the fence is what keeps a pass
    // from contending for it.
    let fenced = std::sync::Mutex::new(Vec::new());
    let reconcile = |conn: &rusqlite::Connection| {
        reconcile_sandbox_inventory_ids(
            conn,
            paths,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                |id: &str| {
                    fenced.lock().unwrap().push(id.to_string());
                    inventory_cleanup_guard_for_test(id)
                },
                |_: SandboxId| Some(()),
                &inventory_execution_for_test,
            ),
        )
        .expect("inventory reconcile")
    };

    let report = reconcile(&conn);

    assert!(report.changed_ids().is_empty());
    assert!(
        fenced.lock().unwrap().is_empty(),
        "inventory must not take the quarantined row's cleanup fence"
    );
    assert_eq!(
        crate::db::tests::database_rows(&conn),
        before,
        "inventory must keep the quarantined row, its hold, history and artifact pin"
    );
    // The guarded delete refuses the row as well, so the hold does not depend
    // on inventory's candidate selection.
    assert_eq!(
        crate::db::delete_sandbox_if_inventory_witness_matches(&conn, &row)
            .expect("guarded inventory delete"),
        crate::db::InventoryStaleRowMutation::Deferred(
            crate::db::InventoryOwnershipReason::LifecycleOwned
        )
    );
    assert_eq!(crate::db::tests::database_rows(&conn), before);
    // The control: a stale row beside it is fenced and removed.
    let stale = SandboxId::new();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &stale.to_string(),
        SandboxState::Stopped,
    );
    assert_eq!(reconcile(&conn).changed_ids(), vec![stale]);
    assert_eq!(*fenced.lock().unwrap(), vec![stale.to_string()]);
}
