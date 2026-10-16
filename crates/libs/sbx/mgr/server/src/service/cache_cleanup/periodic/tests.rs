use super::*;
use crate::db::test_fixtures::database_rows;
use crate::service::tests::service_tests::insert_test_sandbox;

#[tokio::test]
async fn periodic_inventory_progresses_in_pages_without_treating_other_pages_as_absent() {
    let cache = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let mut ids = Vec::new();
    for _ in 0..DIRECTORY_BATCH_SIZE + 1 {
        let id = SandboxId::new();
        std::fs::create_dir(cache.path().join(id.to_string())).unwrap();
        insert_test_sandbox(&conn, &id.to_string(), SandboxState::Stopped);
        ids.push(id);
    }
    let stale = SandboxId::new();
    insert_test_sandbox(&conn, &stale.to_string(), SandboxState::Stopped);
    let service = SandboxManagerService::for_test(conn);
    let mut scan = PeriodicInventoryScan::default();
    let shutdown = CancellationToken::new();
    let paths = SandboxInventoryPaths::new(cache.path(), runtime.path());
    let batch = service
        .discover_periodic_inventory_in(&mut scan, paths, &shutdown)
        .unwrap();
    service
        .reconcile_periodic_inventory_batch(batch, &Default::default())
        .unwrap();
    assert!(crate::db::sandbox_exists(&service.db_conn().unwrap(), &stale.to_string()).unwrap());
    let batch = service
        .discover_periodic_inventory_in(&mut scan, paths, &shutdown)
        .unwrap();
    service
        .reconcile_periodic_inventory_batch(batch, &Default::default())
        .unwrap();
    assert!(!crate::db::sandbox_exists(&service.db_conn().unwrap(), &stale.to_string()).unwrap());
    for id in ids {
        assert_eq!(
            crate::db::get_sandbox(&service.db_conn().unwrap(), &id.to_string())
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Stopped
        );
        assert!(cache.path().join(id.to_string()).is_dir());
    }
}

#[test]
fn directory_entry_errors_consume_budget_without_rewinding_or_losing_other_entries() {
    let dir = tempfile::tempdir().unwrap();
    for index in 0..DIRECTORY_BATCH_SIZE + 1 {
        std::fs::create_dir(dir.path().join(index.to_string())).unwrap();
    }
    let mut entries = std::iter::once(Err(std::io::Error::other("unreadable entry")))
        .chain(std::fs::read_dir(dir.path()).unwrap());
    let (first, exhausted) = read_directory_batch(&mut entries);
    assert!(!exhausted);
    assert_eq!(first.len(), DIRECTORY_BATCH_SIZE - 1);
    let (second, exhausted) = read_directory_batch(&mut entries);
    assert!(exhausted);
    assert_eq!(second.len(), 2);
    assert_eq!(
        first
            .into_iter()
            .chain(second)
            .map(|entry| entry.file_name())
            .collect::<BTreeSet<_>>()
            .len(),
        DIRECTORY_BATCH_SIZE + 1
    );
}

#[test]
fn directory_discovery_bounds_each_batch_and_wraps_without_retaining_names() {
    let dir = tempfile::tempdir().unwrap();
    for index in 0..DIRECTORY_BATCH_SIZE + 1 {
        std::fs::create_dir(dir.path().join(index.to_string())).unwrap();
    }
    let mut scan = DirectoryScan::default();
    let first = scan.next_batch(dir.path()).unwrap();
    assert_eq!(first.len(), DIRECTORY_BATCH_SIZE);
    let second = scan.next_batch(dir.path()).unwrap();
    assert_eq!(second.len(), 1);
    let names = first
        .into_iter()
        .chain(second)
        .map(|entry| entry.file_name())
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), DIRECTORY_BATCH_SIZE + 1);
    assert_eq!(
        scan.next_batch(dir.path()).unwrap().len(),
        DIRECTORY_BATCH_SIZE
    );
}

#[tokio::test]
async fn periodic_inventory_names_only_rowless_cache_directories_for_inspection() {
    let cache = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let (owned, orphan) = (SandboxId::new(), SandboxId::new());
    for id in [owned, orphan] {
        std::fs::create_dir(cache.path().join(id.to_string())).unwrap();
    }
    insert_test_sandbox(&conn, &owned.to_string(), SandboxState::Suspended);
    let service = SandboxManagerService::for_test(conn);
    let batch = service
        .discover_periodic_inventory_in(
            &mut PeriodicInventoryScan::default(),
            SandboxInventoryPaths::new(cache.path(), runtime.path()),
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(
        batch.orphan_candidates,
        vec![cache.path().join(orphan.to_string())]
    );
}

/// Discovers and reconciles one periodic page. The tests that use it create no
/// cache directories, so the page has no orphan candidates whose retained
/// sources would need inspecting.
fn reconcile_one_periodic_page(service: &SandboxManagerService, paths: SandboxInventoryPaths<'_>) {
    let batch = service
        .discover_periodic_inventory_in(
            &mut PeriodicInventoryScan::default(),
            paths,
            &CancellationToken::new(),
        )
        .unwrap();
    assert!(batch.orphan_candidates.is_empty());
    service
        .reconcile_periodic_inventory_batch(batch, &Default::default())
        .unwrap();
}

#[tokio::test]
async fn orphan_quarantine_survives_inventory_once_its_runtime_is_gone() {
    let cache = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let paths = SandboxInventoryPaths::new(cache.path(), runtime.path());
    let id = SandboxId::new();
    let sandbox_run_dir = runtime.path().join(id.to_string());
    std::fs::create_dir(&sandbox_run_dir).unwrap();
    std::os::unix::fs::symlink(cache.path(), sandbox_run_dir.join("fc.pid")).unwrap();
    let conn = crate::db::test_conn();
    let receiver = ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    );
    // A live runtime without a cache root is materialized for a cleanup that
    // ends in quarantine. Completing it leaves no intent or episode behind.
    let report = reconcile_sandbox_inventory_ids(
        &conn,
        paths,
        Vec::new(),
        vec![id],
        vec![id],
        ReconcileSource::Test,
        InventoryOwnershipGuards::new(
            inventory_cleanup_guard_for_test,
            |_: SandboxId| Some(()),
            &|_| Ok(Some(receiver)),
        ),
    )
    .unwrap();
    assert_eq!(report.changed_ids(), vec![id]);
    crate::db::complete_row_intent_cleanup(
        &conn,
        &id.to_string(),
        crate::db::CleanupCompletion::Quarantine,
        None,
    )
    .unwrap();
    let held = crate::db::get_sandbox(&conn, &id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(held.state, SandboxState::Quarantined);
    assert!(held.cleanup_completion.is_none());
    assert!(
        crate::db::cleanup_episode(&conn, &id.to_string())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        crate::db::current_quarantine_reason_wire_value(&conn, &id.to_string())
            .unwrap()
            .as_deref(),
        Some(SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str())
    );
    std::fs::remove_dir_all(&sandbox_run_dir).unwrap();
    let service = crate::service::tests::service_tests::service_with_runtime_probe(conn, false);
    let before = database_rows(&service.db_conn().unwrap());

    reconcile_one_periodic_page(&service, paths);
    assert_eq!(
        database_rows(&service.db_conn().unwrap()),
        before,
        "periodic inventory must keep the quarantine and publish no deletion"
    );
    let report = reconcile_sandbox_inventory_in(
        &service.db_conn().unwrap(),
        cache.path(),
        runtime.path(),
        ReconcileSource::Test,
    )
    .unwrap();
    assert!(report.changed_ids().is_empty());
    assert_eq!(database_rows(&service.db_conn().unwrap()), before);
}

/// Whether a row with no cache root, runtime directory or durable cleanup
/// owner survives an inventory pass.
fn kept_without_local_evidence(state: SandboxState) -> bool {
    match state {
        // These states may legitimately exist before, or after, the cache root.
        SandboxState::Creating
        | SandboxState::Created
        | SandboxState::Suspended
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed => true,
        // An in-flight stop or delete owns the row's removal.
        SandboxState::Stopping | SandboxState::Deleting => true,
        // Only an explicit disposition (cluster-deleted eviction, on-node
        // Delete, or quarantine recovery) releases a quarantine hold.
        SandboxState::Quarantined => true,
        // Every other state requires local evidence: the row is stale.
        SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopped
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Failed => false,
    }
}

#[tokio::test]
async fn startup_and_periodic_inventory_remove_only_rows_whose_state_requires_local_evidence() {
    let cache = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let paths = SandboxInventoryPaths::new(cache.path(), runtime.path());
    for periodic in [false, true] {
        let conn = crate::db::test_conn();
        let rows = SandboxState::all()
            .into_iter()
            .map(|state| {
                let id = SandboxId::new();
                insert_test_sandbox(&conn, &id.to_string(), state);
                (state, id)
            })
            .collect::<Vec<_>>();
        for (state, id) in &rows {
            assert!(
                crate::db::sandbox_exists(&conn, &id.to_string()).unwrap(),
                "{state} row must exist before the pass"
            );
        }
        let expected_removed = rows
            .iter()
            .filter(|(state, _)| !kept_without_local_evidence(*state))
            .map(|(state, _)| state.as_str())
            .collect::<BTreeSet<_>>();
        assert!(!expected_removed.is_empty());

        let service = crate::service::tests::service_tests::service_with_runtime_probe(conn, false);
        let removed_or_published = if periodic {
            reconcile_one_periodic_page(&service, paths);
            crate::db::list_node_dirty_refs(&service.db_conn().unwrap(), 1024)
                .unwrap()
                .into_iter()
                .filter(|dirty| dirty.element_kind == "sandbox_inventory")
                .map(|dirty| SandboxId::from_str(&dirty.element_value).unwrap())
                .collect::<BTreeSet<_>>()
        } else {
            reconcile_sandbox_inventory_in(
                &service.db_conn().unwrap(),
                cache.path(),
                runtime.path(),
                ReconcileSource::Test,
            )
            .unwrap()
            .changed_ids()
            .into_iter()
            .collect()
        };

        let entry = if periodic { "periodic" } else { "startup" };
        let removed_or_published = rows
            .iter()
            .filter(|(_, id)| removed_or_published.contains(id))
            .map(|(state, _)| state.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(removed_or_published, expected_removed, "{entry} pass");
        let conn = service.db_conn().unwrap();
        for (state, id) in rows {
            let row = crate::db::get_sandbox(&conn, &id.to_string()).unwrap();
            if kept_without_local_evidence(state) {
                assert_eq!(
                    row.map(|row| row.state),
                    Some(state),
                    "{entry} pass must keep the {state} row unchanged"
                );
            } else {
                assert!(
                    row.is_none(),
                    "{entry} pass must remove the stale {state} row"
                );
            }
        }
    }
}
