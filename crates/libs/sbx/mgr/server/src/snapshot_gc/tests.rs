use super::*;
use crate::db::Connection;
use crate::db::fixture::params;
use std::fs;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

fn open_test_db() -> Connection {
    crate::db::test_conn()
}

fn test_ownership() -> GcOwnership {
    GcOwnership {
        coordinator: crate::auto_lifecycle::AutoLifecycleCoordinator::default(),
        drain: Arc::new(crate::service::drain::DrainState::default()),
        shutdown: CancellationToken::new(),
        node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
    }
}

const TEST_ART_ID: &str = "01939e3e-0000-7000-8000-000000000001";

fn insert_artifact_row(
    conn: &Connection,
    id: &str,
    sandbox_id: &str,
    local_path: &str,
    created_at_secs: u64,
    last_touched_at_secs: u64,
    backup_completed_at_secs: Option<u64>,
) {
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, last_touched_at,
             backup_completed_at
         ) VALUES (?1, ?2, 'full', 'suspend', NULL, ?3, ?4, ?5, ?6)",
        params![
            id,
            sandbox_id,
            local_path,
            created_at_secs.to_string(),
            last_touched_at_secs.to_string(),
            backup_completed_at_secs.map(|s| s.to_string()),
        ],
    )
    .unwrap();
}

fn insert_sandbox_row(conn: &Connection, id: &str, current_local_artifact_id: Option<&str>) {
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin
         ) VALUES (?1, ?2, 'suspended', 'seed', 'u', 'fc', '0', '0', '{}', '{}', '{}', ?3, ?4, ?5)",
        params![
            id,
            "uuid8",
            current_local_artifact_id,
            current_local_artifact_id.map(|_| "full"),
            current_local_artifact_id.map(|_| "suspend"),
        ],
    )
    .unwrap();
}

fn make_artifact_dir(root: &TempDir, name: &str, bytes: usize) -> String {
    let path = root.path().join(name);
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("memory"), vec![0u8; bytes]).unwrap();
    path.to_string_lossy().into_owned()
}

#[test]
fn artifact_size_counts_allocated_blocks_for_sparse_files() {
    let root = TempDir::new().unwrap();
    let artifact_dir = root.path().join("artifact");
    fs::create_dir(&artifact_dir).unwrap();
    let memory_path = artifact_dir.join("memory");
    let mut memory = fs::File::create(&memory_path).unwrap();
    memory.write_all(&[0xA5; 4096]).unwrap();
    memory.set_len(8 * 1024 * 1024).unwrap();
    memory.sync_all().unwrap();

    let metadata = fs::metadata(&memory_path).unwrap();
    let allocated_bytes = metadata.blocks().saturating_mul(512);
    // Linux exercises sparse allocation; APFS can report the full logical size.
    #[cfg(target_os = "linux")]
    assert!(allocated_bytes < metadata.len(), "fixture must be sparse");
    assert_eq!(dir_size_bytes(&artifact_dir), allocated_bytes);
}

fn test_cfg() -> GcConfig {
    GcConfig {
        enabled: true,
        // Always engaged, drain to empty — the fraction-model
        // equivalent of the old 1-byte budget.
        engage_fraction: 0.0,
        target_fraction: 0.0,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
        idle: Duration::from_secs(0),
    }
}

/// A fully-occupied filesystem snapshot: with `target_fraction`
/// 0 every byte is over target, so the planner wants everything
/// eligible gone.
fn full_disk() -> crate::disk_usage::DiskUsage {
    crate::disk_usage::DiskUsage {
        used_bytes: 1 << 40,
        total_bytes: 1 << 40,
    }
}

/// Paused, like the sibling reaper tests. A reaped candidate's staging
/// directory is handed to node-manager, which here is an absent socket dialed
/// once. Nothing here reads elapsed time — eligibility is decided from the
/// `now_secs` this test passes in — so the clock only has to not be real.
#[tokio::test(start_paused = true)]
async fn malformed_artifact_times_are_retained_without_blocking_valid_siblings() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let malformed = make_artifact_dir(&tmp, "malformed", 4096);
    let valid = make_artifact_dir(&tmp, "valid", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &malformed, 0, 0, Some(0));
    insert_artifact_row(
        &conn,
        "01939e3e-0000-7000-8000-000000000002",
        "sbx1",
        &valid,
        0,
        0,
        Some(0),
    );
    conn.execute(
        "UPDATE local_artifacts SET last_touched_at = 'invalid' WHERE local_artifact_id = ?1",
        [TEST_ART_ID],
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    // A timestamp corrupted between planning and execution must also fail closed.
    execute_one(
        &artifacts,
        TEST_ART_ID,
        "sbx1",
        GcEligibility {
            now_secs: now_secs(),
            idle_secs: 3600,
            grace_secs: 60,
        },
    )
    .unwrap_err();
    let report = gc_tick(
        artifacts,
        db,
        test_cfg(),
        now_secs(),
        full_disk(),
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 1);
    assert!(Path::new(&malformed).exists());
    assert!(!Path::new(&valid).exists());
}

/// Bounds the reaper's served node-manager call on real time. The test's
/// clock is paused and auto-advances while the call waits on its socket, so a
/// tokio timeout would fire early.
const NODE_MANAGER_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// `future`'s output, or a panic once [`NODE_MANAGER_WAIT`] of real time has
/// passed. The watchdog thread exits as soon as `future` completes.
async fn within_node_manager_wait<F: std::future::Future>(future: F) -> F::Output {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let (expired, timed_out) = tokio::sync::oneshot::channel::<()>();
    std::thread::spawn(move || {
        if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
            finished.recv_timeout(NODE_MANAGER_WAIT)
        {
            let _ = expired.send(());
        }
    });
    let output = tokio::select! {
        output = future => output,
        _ = timed_out => panic!("waited longer than {NODE_MANAGER_WAIT:?} on node-manager"),
    };
    drop(done);
    output
}

/// Backup-completed + idle + not pinned → reaped. The row is gone, and the
/// artifact directory moves to its `.gc-pending-<id>` staging sibling, which
/// node-manager is asked to delete.
#[tokio::test(start_paused = true)]
async fn reaps_idle_uploaded_unpinned_artifact() {
    use pplx_space_node_mgr_client::proto::{
        DeleteCacheArtifactRequest, DeleteCacheArtifactResponse,
    };

    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art1", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &path, 0, 0, Some(0));
    insert_sandbox_row(&conn, "sbx1", None);
    let deletions = Arc::new(Mutex::new(Vec::new()));
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary("DeleteCacheArtifact", {
            let deletions = Arc::clone(&deletions);
            move |request: DeleteCacheArtifactRequest| {
                deletions.lock().unwrap().push(request.local_path);
                Ok(DeleteCacheArtifactResponse::default())
            }
        })
        .serve();

    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let ownership = GcOwnership {
        node_manager: node_manager.connector(),
        ..test_ownership()
    };
    let mut scan = ArtifactScan::default();
    let report = within_node_manager_wait(gc_tick(
        artifacts,
        db.clone(),
        test_cfg(),
        now_secs(),
        full_disk(),
        &ownership,
        &mut scan,
    ))
    .await
    .unwrap();
    assert!(report.engaged);
    assert_eq!(report.deleted, 1);
    assert!(report.bytes_freed >= 4096);
    assert!(!std::path::Path::new(&path).exists());
    let staging = format!("{path}.gc-pending-{TEST_ART_ID}");
    // The fake acknowledges without deleting, so the staging directory stays.
    assert!(std::path::Path::new(&staging).is_dir());
    assert_eq!(*deletions.lock().unwrap(), vec![staging]);
    assert_eq!(node_manager.calls(), vec!["DeleteCacheArtifact".to_owned()]);
    let conn = db.lock().expect("mutex poisoned");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM local_artifacts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

/// Below the engage fraction the tick is a no-op even with a
/// fully eligible artifact present — the disk gate decides
/// whether the reaper acts at all, and the expensive planning
/// walk is skipped entirely.
#[tokio::test]
async fn does_not_reap_below_engage_fraction() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art1", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &path, 0, 0, Some(0));
    insert_sandbox_row(&conn, "sbx1", None);

    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let mut cfg = test_cfg();
    cfg.engage_fraction = 0.50;
    cfg.target_fraction = 0.40;
    let disk = crate::disk_usage::DiskUsage {
        used_bytes: 30,
        total_bytes: 100,
    };
    let report = gc_tick(
        artifacts,
        db.clone(),
        cfg,
        now_secs(),
        disk,
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert!(!report.engaged);
    assert_eq!(report.deleted, 0);
    assert_eq!(report.candidates_planned, 0);
    assert!(std::path::Path::new(&path).exists());
}

/// Artifact whose `backup_completed_at` is NULL → never reaped,
/// even when idle + not pinned + over budget. The reaper does
/// not key off `jobs.subject_id` (which is `sandbox.id`, not
/// `local_artifact_id`); only the per-artifact marker decides.
#[tokio::test]
async fn does_not_reap_when_backup_not_completed() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art1", 4096);
    insert_artifact_row(&conn, "art1", "sbx1", &path, 0, 0, None);
    insert_sandbox_row(&conn, "sbx1", None);

    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let report = gc_tick(
        artifacts,
        db.clone(),
        test_cfg(),
        now_secs(),
        full_disk(),
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(std::path::Path::new(&path).exists());
}

/// A committed artifact and a suspend rollback predecessor are both retained,
/// even when idle + uploaded + over budget.
#[tokio::test]
async fn does_not_reap_committed_or_suspend_predecessor_pin() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art1", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &path, 0, 0, Some(0));
    insert_sandbox_row(&conn, "sbx1", Some(TEST_ART_ID));

    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let report = gc_tick(
        artifacts,
        db.clone(),
        test_cfg(),
        now_secs(),
        full_disk(),
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(std::path::Path::new(&path).exists());

    db.lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes
                SET suspend_predecessor_artifact_id = current_local_artifact_id,
                    current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL
              WHERE id = 'sbx1'",
            [],
        )
        .unwrap();
    let page = ArtifactScan::default()
        .next_page(&db.lock().unwrap())
        .unwrap();
    assert_eq!(page.artifacts.len(), 1);
    assert!(page.artifacts[0].pinned_current);
    let artifacts = LocalArtifacts::new(db.clone());
    let report = gc_tick(
        artifacts,
        db.clone(),
        test_cfg(),
        now_secs(),
        full_disk(),
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(std::path::Path::new(&path).exists());
}

/// Artifact whose last_touched_at is inside the idle window →
/// never reaped, even when uploaded + not pinned + over budget.
#[tokio::test]
async fn does_not_reap_when_not_idle() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art1", 4096);
    let now = now_secs();
    insert_artifact_row(&conn, "art1", "sbx1", &path, now, now, Some(now));
    insert_sandbox_row(&conn, "sbx1", None);

    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let mut cfg = test_cfg();
    cfg.idle = Duration::from_secs(3600);
    let report = gc_tick(
        artifacts,
        db.clone(),
        cfg,
        now,
        full_disk(),
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(std::path::Path::new(&path).exists());
}

/// The reaper loop is spawned after other startup tasks. It must
/// not run stuck-state recovery itself: a restore can acquire a
/// legitimate `RestoreLease` first, and resetting that live
/// `restoring` row to `ready` would let delete/GC race the file
/// read. Stuck-state recovery belongs in synchronous startup
/// before lease acquisition is possible.
#[tokio::test]
async fn run_loop_startup_does_not_reset_live_restore_lease() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art-live-restore", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &path, 0, 0, Some(0));
    insert_sandbox_row(&conn, "sbx1", Some(TEST_ART_ID));

    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let artifact_id = TEST_ART_ID.parse::<LocalArtifactId>().unwrap();
    let lease = artifacts.check_out_for_restore(artifact_id).unwrap();

    let mut cfg = test_cfg();
    cfg.enabled = false;
    let ownership = test_ownership();
    let shutdown = ownership.shutdown.clone();
    let reaper = run_loop(
        artifacts.clone(),
        db.clone(),
        Arc::new(crate::service::fork_artifact::CopyOperationReservations::default()),
        cfg,
        ownership,
    );
    tokio::pin!(reaper);
    // Enter startup before cancelling: a spawned-but-unpolled loop would not
    // exercise the live-lease regression. Disabled pressure GC still owns
    // periodic orphan recovery, so it must remain pending until shutdown.
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(reaper.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), reaper)
        .await
        .expect("reaper must finish its bounded discovery page after cancellation");

    let restore_attempt = artifacts.check_out_for_restore(artifact_id);
    assert!(
        matches!(
            restore_attempt,
            Err(CheckoutError::NotReady {
                state: crate::local_artifacts::ArtifactState::Restoring,
                ..
            })
        ),
        "reaper startup must not reset a live restore lease"
    );

    drop(lease);
    let _lease_after_drop = artifacts.check_out_for_restore(artifact_id).unwrap();
}

/// Startup sweep discovers `.gc-pending-<uuid>` directories
/// nested under the cache root and ignores unrelated dirs. The
/// actual delete is routed through node-mgr (so subvolumes work
/// too); this unit test only covers the discovery side.
#[test]
fn sweep_discovers_gc_pending_orphans_and_skips_unrelated() {
    let tmp = TempDir::new().unwrap();
    let sandbox_dir = tmp.path().join("sbx-abc/snapshot");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    let orphan = tmp.path().join("sbx-abc/snapshot.gc-pending-deadbeef");
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("memory"), b"stale").unwrap();
    let snapshots = tmp.path().join("sbx-abc/snapshots");
    std::fs::create_dir_all(&snapshots).unwrap();
    // Staging is only ever a sibling of an artifact: outside the shapes
    // node-mgr can delete, nothing is a candidate or even read.
    let unrelated = tmp.path().join("sbx-abc/instance");
    std::fs::create_dir_all(&unrelated).unwrap();
    std::fs::create_dir(unrelated.join("stray.gc-pending-held")).unwrap();

    // A wide first directory spans pages without growing retained discovery.
    for index in 0..270 {
        std::fs::create_dir(snapshots.join(format!("item.gc-pending-{index}"))).unwrap();
    }
    let outside = TempDir::new().unwrap();
    std::fs::create_dir(outside.path().join("outside.gc-pending-held")).unwrap();
    std::os::unix::fs::symlink(outside.path(), tmp.path().join("escape")).unwrap();
    let active = crate::service::fork_artifact::CopyOperationReservations::default();
    let mut scan = OrphanScan::default();
    let mut found = std::collections::HashSet::new();
    for _ in 0..4 {
        let page = scan
            .next_page(tmp.path(), OrphanKind::GcPending, &active, now_secs())
            .unwrap();
        assert!(page.len() <= crate::db::snapshot_gc::PAGE_SIZE);
        found.extend(page);
    }
    assert_eq!(found.len(), 271);
    assert!(found.contains(&orphan));
    assert!(found.iter().all(|path| path.starts_with(tmp.path())));
}

#[test]
fn gc_pending_reaping_requires_absent_registry_and_pin() {
    let db = crate::db::SharedConnection::new(open_test_db());
    let artifacts = LocalArtifacts::new(db.clone());
    let root = TempDir::new().unwrap();
    let path = root
        .path()
        .join(format!("snapshot.gc-pending-{TEST_ART_ID}"));
    fs::create_dir(&path).unwrap();
    assert!(gc_pending_is_unowned(&artifacts, &path));
    assert!(!gc_pending_is_unowned(
        &artifacts,
        &root.path().join("snapshot.gc-pending-invalid")
    ));
    {
        let conn = db.lock().unwrap();
        insert_artifact_row(&conn, TEST_ART_ID, "sbx-kept", "/missing", 0, 0, None);
        insert_sandbox_row(&conn, "sbx-kept", Some(TEST_ART_ID));
    }
    for state in ["ready", "restoring", "deleting"] {
        db.lock()
            .unwrap()
            .execute("UPDATE local_artifacts SET state = ?1", [state])
            .unwrap();
        assert!(!gc_pending_is_unowned(&artifacts, &path), "{state}");
    }
    db.lock()
        .unwrap()
        .execute("DELETE FROM local_artifacts", [])
        .unwrap();
    assert!(
        !gc_pending_is_unowned(&artifacts, &path),
        "dangling pin retains evidence"
    );
    db.lock()
        .unwrap()
        .execute("DELETE FROM sandboxes", [])
        .unwrap();
    assert!(
        gc_pending_is_unowned(&artifacts, &path),
        "completed cleanup releases evidence"
    );
    db.lock()
        .unwrap()
        .execute("DROP TABLE local_artifacts", [])
        .unwrap();
    assert!(
        !gc_pending_is_unowned(&artifacts, &path),
        "database errors retain evidence"
    );
    assert!(path.exists());
}

/// Race re-check: candidate selected by the planner with stale
/// last_touched_at, then bumped to "now" between plan and
/// execute. The execute_one re-check inside the txn must skip
/// the delete and surface the not_idle reason.
#[test]
fn sweep_discovers_stale_fork_copy_operations_in_the_produced_layout() {
    let tmp = TempDir::new().unwrap();
    let sandbox_cache_root = tmp.path().join("firecracker/sandboxes");
    let current = crate::service::fork_artifact::copy_staging_root_under(&sandbox_cache_root)
        .join("current-operation");
    let wrong_anchor = crate::service::fork_artifact::copy_staging_root_under(tmp.path())
        .join("wrong-anchor-operation");
    let unrelated = tmp.path().join("keep/current-operation");
    let misleading = tmp.path().join("keep/.fork-copies/unrelated-operation");
    std::fs::create_dir_all(&current).unwrap();
    std::fs::create_dir_all(&wrong_anchor).unwrap();
    std::fs::create_dir_all(&unrelated).unwrap();
    std::fs::create_dir_all(&misleading).unwrap();
    let now = now_secs().saturating_add(FORK_COPY_ORPHAN_MIN_AGE_SECS + 1);

    let mut orphans = OrphanScan::default()
        .next_page(
            &crate::service::fork_artifact::copy_staging_root_under(&sandbox_cache_root),
            OrphanKind::ForkCopy,
            &crate::service::fork_artifact::CopyOperationReservations::default(),
            now,
        )
        .unwrap();
    orphans.sort();
    assert_eq!(orphans.len(), 1);
    assert!(
        orphans
            .iter()
            .any(|path| path.ends_with("current-operation"))
    );
    assert!(
        !orphans
            .iter()
            .any(|path| path.to_string_lossy().contains("wrong-anchor"))
    );
    assert!(
        !orphans
            .iter()
            .any(|path| path.to_string_lossy().contains("/keep/"))
    );
}

#[test]
fn stale_fork_copy_is_rechecked_for_activity_before_delete() {
    let active = Arc::new(crate::service::fork_artifact::CopyOperationReservations::default());
    let path = std::path::PathBuf::from("/space/cache/.fork-copies/operation");

    let guard = crate::service::fork_artifact::ActiveCopyOperation::register(
        Arc::clone(&active),
        path.clone(),
    )
    .unwrap();
    assert!(
        crate::service::fork_artifact::CopyOperationDeleteLease::acquire(
            Arc::clone(&active),
            path.clone(),
        )
        .is_none()
    );
    drop(guard);
    assert!(
        crate::service::fork_artifact::CopyOperationDeleteLease::acquire(active, path).is_some()
    );
}

#[tokio::test]
async fn completed_restore_after_planning_still_honors_grace_when_idle_is_zero() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "art1", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &path, 0, 0, Some(0));
    insert_sandbox_row(&conn, "sbx1", None);
    let now = now_secs();
    let observed = ArtifactScan::default().next_page(&conn).unwrap();
    assert_eq!(observed.artifacts[0].last_touched_at_secs, 0);
    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    artifacts
        .check_out_for_restore(TEST_ART_ID.parse().unwrap())
        .unwrap()
        .release()
        .unwrap();
    let (outcome, staging) = execute_one(
        &artifacts,
        TEST_ART_ID,
        "sbx1",
        GcEligibility {
            now_secs: now,
            idle_secs: 0,
            grace_secs: 3600,
        },
    )
    .unwrap();
    assert_eq!(outcome, EvictionOutcome::SkippedNotIdleAtExecute);
    assert!(staging.is_none(), "skip path must not return a staging dir");
    assert!(std::path::Path::new(&path).exists());
}

#[tokio::test(start_paused = true)]
async fn gc_defers_to_publication_staging_drain_and_shutdown_ownership() {
    let conn = open_test_db();
    let tmp = TempDir::new().unwrap();
    let path = make_artifact_dir(&tmp, "artifact", 4096);
    insert_artifact_row(&conn, TEST_ART_ID, "sbx1", &path, 0, 0, Some(0));
    let db = crate::db::SharedConnection::new(conn);
    let artifacts = LocalArtifacts::new(db.clone());
    let ownership = test_ownership();
    for lock in [
        ownership.coordinator.artifact_lock("sbx1"),
        ownership.coordinator.staging_lock("sbx1"),
    ] {
        let guard = lock.lock_owned().await;
        let report = gc_tick(
            artifacts.clone(),
            db.clone(),
            test_cfg(),
            now_secs(),
            full_disk(),
            &ownership,
            &mut ArtifactScan::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_not_ready, 1);
        assert!(Path::new(&path).exists());
        drop(guard);
    }
    let closed = GcOwnership {
        coordinator: ownership.coordinator.clone(),
        drain: crate::service::tests::closed_drain_for_test(),
        shutdown: CancellationToken::new(),
        node_manager: ownership.node_manager.clone(),
    };
    let report = gc_tick(
        artifacts.clone(),
        db.clone(),
        test_cfg(),
        now_secs(),
        full_disk(),
        &closed,
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(Path::new(&path).exists());
    ownership.shutdown.cancel();
    let report = gc_tick(
        artifacts.clone(),
        db.clone(),
        test_cfg(),
        now_secs(),
        full_disk(),
        &ownership,
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(Path::new(&path).exists());
    let report = gc_tick(
        artifacts,
        db,
        test_cfg(),
        now_secs(),
        full_disk(),
        &test_ownership(),
        &mut ArtifactScan::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 1);
    assert!(!Path::new(&path).exists());
}
