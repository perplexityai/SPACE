use super::*;
use crate::service::tests::service_tests::insert_test_sandbox;
use pplx_space_common_core::types::LocalArtifactId;
use std::collections::HashSet;

fn sweep(service: &SandboxManagerService, root: &Path) -> HashSet<PathBuf> {
    let mut scan = StagingCleanupScan::default();
    let mut found = HashSet::new();
    for _ in 0..128 {
        let batch = service
            .abandoned_suspend_staging_batch(root, &mut scan)
            .unwrap();
        assert!(batch.len() <= DELETE_BATCH);
        found.extend(batch);
        if scan.sandboxes.is_none() {
            return found;
        }
    }
    panic!("finite fixture must finish its sweep");
}

#[tokio::test]
async fn staging_cleanup_requires_stable_unowned_lifecycle() {
    use SandboxState::*;
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let mut expected = HashSet::new();
    let mut paused = None;
    for state in [
        Creating,
        Created,
        Starting,
        Started,
        Stopping,
        Stopped,
        Deleting,
        Pausing,
        Paused,
        Resuming,
        Checkpointing,
        Suspending,
        Suspended,
        Restoring,
        CleanupRequired,
        CleanupFailed,
        Quarantined,
        Failed,
    ] {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, state);
        let artifact_id = LocalArtifactId::new();
        let path = suspend_snapshot_publication::suspend_snapshot_staging_dir(
            &cache.path().join(&id),
            artifact_id,
        );
        std::fs::create_dir_all(&path).unwrap();
        let retired = path.with_file_name(format!("{RETIRED_PREFIX}{artifact_id}"));
        // These are recovery evidence, not safe garbage, in transitional or
        // quarantined states even when their lifecycle locks are currently free.
        match state {
            Started | Paused | Stopped | Suspended => {
                expected.insert(retired.clone());
            }
            Creating | Created | Starting | Stopping | Deleting | Pausing | Resuming
            | Checkpointing | Suspending | Restoring | CleanupRequired | CleanupFailed
            | Quarantined | Failed => {}
        }
        if state == Paused {
            paused = Some((id, retired));
        }
    }
    let unknown = suspend_snapshot_publication::suspend_snapshot_staging_dir(
        &cache.path().join(SandboxId::new().to_string()),
        LocalArtifactId::new(),
    );
    std::fs::create_dir_all(&unknown).unwrap();
    let service = SandboxManagerService::for_test(conn);
    let (id, path) = paused.unwrap();
    for owner in 0..3 {
        let _wake = (owner == 0).then(|| {
            service
                .auto_lifecycle
                .wake_lock(&id)
                .try_lock_owned()
                .unwrap()
        });
        let _artifact = (owner == 1).then(|| {
            service
                .auto_lifecycle
                .artifact_lock(&id)
                .try_lock_owned()
                .unwrap()
        });
        let _cleanup = (owner == 2).then(|| match service.try_cleanup_fence(&id) {
            CleanupLockTryResult::Acquired(guard) => guard,
            _ => panic!("free fixture fence"),
        });
        let mut unowned = expected.clone();
        unowned.remove(&path);
        assert_eq!(sweep(&service, cache.path()), unowned);
    }
    assert_eq!(sweep(&service, cache.path()), expected);
}

#[tokio::test]
async fn staging_cleanup_rediscovery_survives_rollback_failed_deletion_and_restart() {
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Suspending);
    let root = cache.path().join(&id);
    let snapshots = suspend_snapshot_publication::suspend_snapshot_staging_parent_dir(&root);
    std::fs::create_dir_all(&snapshots).unwrap();
    // Exceed both limits of the old one-shot sweep. Non-staging entries must
    // not starve the tail, and successful capture history must remain untouched.
    for n in 0..1100 {
        std::fs::write(snapshots.join(format!("d-1m-{n}")), b"history").unwrap();
    }
    let expected: HashSet<_> = (0..80)
        .map(|_| {
            let artifact_id = LocalArtifactId::new();
            let path =
                suspend_snapshot_publication::suspend_snapshot_staging_dir(&root, artifact_id);
            suspend_snapshot_publication::write_full_suspend_artifact(
                &path,
                &id,
                artifact_id,
                b"old attempt",
            );
            path.with_file_name(format!("{RETIRED_PREFIX}{artifact_id}"))
        })
        .collect();
    let service = SandboxManagerService::for_test(conn);
    assert!(sweep(&service, cache.path()).is_empty());
    assert!(crate::db::rollback_suspending_to_paused(&service.db.lock().unwrap(), &id).unwrap());
    // Simulate an unacknowledged deletion by leaving the selected files in
    // place. Both another sweep and a restarted scan must rediscover them.
    for state in [
        SandboxState::Paused,
        SandboxState::Started,
        SandboxState::Paused,
    ] {
        crate::db::tests::state_write_fixtures::force_state(
            &service.db.lock().unwrap(),
            &id,
            state,
        );
        assert_eq!(sweep(&service, cache.path()), expected);
        assert!(
            suspend_snapshot_publication::find_complete_suspend_snapshot_staging_dir(
                &root,
                &Default::default()
            )
            .unwrap()
            .is_none(),
            "later recovery must never adopt an artifact pending deletion"
        );
    }
    // A replacement capture keeps its own name; old retired work remains
    // discoverable even while the replacement owns the lifecycle.
    let fresh_id = LocalArtifactId::new();
    let fresh = suspend_snapshot_publication::suspend_snapshot_staging_dir(&root, fresh_id);
    suspend_snapshot_publication::write_full_suspend_artifact(
        &fresh,
        &id,
        fresh_id,
        b"new attempt",
    );
    crate::db::tests::state_write_fixtures::force_state(
        &service.db.lock().unwrap(),
        &id,
        SandboxState::Suspending,
    );
    let _wake = service
        .auto_lifecycle
        .wake_lock(&id)
        .try_lock_owned()
        .unwrap();
    assert_eq!(sweep(&service, cache.path()), expected);
    assert_eq!(
        suspend_snapshot_publication::find_complete_suspend_snapshot_staging_dir(
            &root,
            &Default::default()
        )
        .unwrap()
        .unwrap()
        .local_artifact_id,
        fresh_id
    );
    for path in &expected {
        std::fs::remove_dir_all(path).unwrap();
    }
    assert!(sweep(&service, cache.path()).is_empty());
    assert_eq!(std::fs::read_dir(snapshots).unwrap().count(), 1101);
}

/// Publication retires a superseded snapshot into this lane while the sandbox
/// is still `Suspending` and its locks are held. Once retired, the entry is
/// garbage the sweep collects regardless of state: only retiring a live
/// `.suspend-publish-*` directory needs the lifecycle gate above.
#[tokio::test]
async fn publication_retirement_is_owned_by_the_sweep_in_any_lifecycle_state() {
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Suspending);
    let sandbox_dir = cache.path().join(&id);
    let snapshot_dir = suspend_snapshot_publication::canonical_suspend_snapshot_dir(&sandbox_dir);
    let (old_id, new_id) = (LocalArtifactId::new(), LocalArtifactId::new());
    suspend_snapshot_publication::write_full_suspend_artifact(&snapshot_dir, &id, old_id, b"old");
    let staging = suspend_snapshot_publication::suspend_snapshot_staging_dir(&sandbox_dir, new_id);
    suspend_snapshot_publication::write_full_suspend_artifact(&staging, &id, new_id, b"new");
    let service = SandboxManagerService::for_test(conn);
    let _wake = service
        .auto_lifecycle
        .wake_lock(&id)
        .try_lock_owned()
        .unwrap();
    let _artifact = service
        .auto_lifecycle
        .artifact_lock(&id)
        .try_lock_owned()
        .unwrap();

    suspend_snapshot_publication::publish_suspend_snapshot_dir(
        &staging,
        &snapshot_dir,
        new_id,
        &Default::default(),
    )
    .unwrap();

    assert!(
        !suspend_snapshot_publication::retired_suspend_snapshot_dir(&sandbox_dir, new_id).exists()
    );
    let swept = sweep(&service, cache.path());
    assert_eq!(swept.len(), 1);
    let garbage = swept.into_iter().next().unwrap();
    assert_eq!(std::fs::read(garbage.join("vmstate")).unwrap(), b"old");
}

/// A publication interrupted after moving the canonical snapshot aside may be
/// rolled back by recovery without restoring the hold. Under the guards, with
/// no canonical snapshot and a stable lifecycle state, the sweep gives the
/// hold an owner: back to the canonical path when the suspended row still
/// pins its artifact, otherwise into the garbage lane.
#[tokio::test]
async fn sweep_owns_the_hold_of_a_rolled_back_publication() {
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let live = SandboxId::new().to_string();
    let pinned = SandboxId::new().to_string();
    let unreadable = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &live, SandboxState::Paused);
    insert_test_sandbox(&conn, &pinned, SandboxState::Suspended);
    insert_test_sandbox(&conn, &unreadable, SandboxState::Suspended);
    let (old_id, new_id) = (LocalArtifactId::new(), LocalArtifactId::new());
    conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = ?1",
        crate::db::fixture::params![pinned, old_id.to_string()],
    )
    .unwrap();

    // Rolled back to a live VM: the held snapshot is superseded.
    let live_dir = cache.path().join(&live);
    let live_hold = suspend_snapshot_publication::retired_suspend_snapshot_dir(&live_dir, new_id);
    suspend_snapshot_publication::write_full_suspend_artifact(&live_hold, &live, old_id, b"old");
    // Rolled back while suspended on the held artifact: the hold is the
    // snapshot a wake needs.
    let pinned_dir = cache.path().join(&pinned);
    let pinned_hold =
        suspend_snapshot_publication::retired_suspend_snapshot_dir(&pinned_dir, new_id);
    suspend_snapshot_publication::write_full_suspend_artifact(
        &pinned_hold,
        &pinned,
        old_id,
        b"old",
    );
    // Rolled back while suspended, but the hold's sidecar cannot be parsed:
    // its identity is unknown, so it is neither restored nor released.
    let unreadable_dir = cache.path().join(&unreadable);
    let unreadable_hold =
        suspend_snapshot_publication::retired_suspend_snapshot_dir(&unreadable_dir, new_id);
    suspend_snapshot_publication::write_full_suspend_artifact(
        &unreadable_hold,
        &unreadable,
        old_id,
        b"old",
    );
    std::fs::write(
        unreadable_hold.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        b"{not json",
    )
    .unwrap();
    // A restore interrupted after creating the canonical directory but before
    // any metadata arrived: not replacement evidence, so the pinned hold stays.
    let interrupted = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &interrupted, SandboxState::Suspended);
    conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = ?1",
        crate::db::fixture::params![interrupted, old_id.to_string()],
    )
    .unwrap();
    let interrupted_dir = cache.path().join(&interrupted);
    let interrupted_hold =
        suspend_snapshot_publication::retired_suspend_snapshot_dir(&interrupted_dir, new_id);
    suspend_snapshot_publication::write_full_suspend_artifact(
        &interrupted_hold,
        &interrupted,
        old_id,
        b"old",
    );
    let empty_canonical =
        suspend_snapshot_publication::canonical_suspend_snapshot_dir(&interrupted_dir);
    std::fs::create_dir_all(&empty_canonical).unwrap();
    let service = SandboxManagerService::for_test(conn);

    let swept = sweep(&service, cache.path());
    assert!(
        interrupted_hold.exists(),
        "an empty canonical directory is not evidence"
    );
    assert!(empty_canonical.exists());
    assert!(unreadable_hold.exists(), "unknown identity keeps the hold");
    assert!(
        !suspend_snapshot_publication::canonical_suspend_snapshot_dir(&unreadable_dir).exists()
    );
    assert!(!live_hold.exists());
    assert_eq!(swept.len(), 1);
    let released = swept.into_iter().next().unwrap();
    assert!(released.starts_with(&live_dir));
    assert_eq!(std::fs::read(released.join("vmstate")).unwrap(), b"old");

    assert!(!pinned_hold.exists());
    let canonical = suspend_snapshot_publication::canonical_suspend_snapshot_dir(&pinned_dir);
    assert_eq!(std::fs::read(canonical.join("vmstate")).unwrap(), b"old");
    suspend_snapshot_publication::validate_complete_suspend_snapshot_dir(
        &canonical,
        &Default::default(),
    )
    .unwrap();
}

/// A readable canonical identity is not replacement evidence on its own: a
/// restore writes metadata before the VM files. The hold is released only
/// when the canonical snapshot names a different artifact that the suspended
/// row pins; a canonical copy of the held artifact itself, or one the row does
/// not pin, keeps the hold.
#[tokio::test]
async fn sweep_keeps_holds_without_durable_supersession() {
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let same = SandboxId::new().to_string();
    let unpinned = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &same, SandboxState::Suspended);
    insert_test_sandbox(&conn, &unpinned, SandboxState::Suspended);
    let (a, b) = (LocalArtifactId::new(), LocalArtifactId::new());
    for id in [&same, &unpinned] {
        conn.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?2,
                 current_local_artifact_kind = 'full',
                 current_local_artifact_origin = 'suspend'
             WHERE id = ?1",
            crate::db::fixture::params![id, a.to_string()],
        )
        .unwrap();
    }

    // A restore of the pinned artifact wrote only metadata at the canonical
    // path while the hold still carries the complete copy.
    let same_dir = cache.path().join(&same);
    let same_hold = suspend_snapshot_publication::retired_suspend_snapshot_dir(&same_dir, b);
    suspend_snapshot_publication::write_full_suspend_artifact(&same_hold, &same, a, b"a");
    let same_canonical = suspend_snapshot_publication::canonical_suspend_snapshot_dir(&same_dir);
    std::fs::create_dir_all(&same_canonical).unwrap();
    std::fs::copy(
        same_hold.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        same_canonical.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
    )
    .unwrap();

    // The canonical snapshot names B, but the row still pins A: B never
    // committed durably.
    let unpinned_dir = cache.path().join(&unpinned);
    let unpinned_hold =
        suspend_snapshot_publication::retired_suspend_snapshot_dir(&unpinned_dir, b);
    suspend_snapshot_publication::write_full_suspend_artifact(&unpinned_hold, &unpinned, a, b"a");
    suspend_snapshot_publication::write_full_suspend_artifact(
        &suspend_snapshot_publication::canonical_suspend_snapshot_dir(&unpinned_dir),
        &unpinned,
        b,
        b"b",
    );
    let service = SandboxManagerService::for_test(conn);

    let swept = sweep(&service, cache.path());
    assert!(swept.is_empty());
    assert!(same_hold.exists());
    assert!(unpinned_hold.exists());
}

/// A publication retry restores its hold to the canonical path and moves it
/// aside again before replacing it. A sweep that observed the restored
/// snapshot must not release the hold on that stale observation: while the
/// lifecycle locks are held, holds are left alone and only already-retired
/// garbage is collected.
#[tokio::test]
async fn sweep_leaves_holds_alone_while_a_publication_may_be_in_flight() {
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Suspended);
    let sandbox_dir = cache.path().join(&id);
    let (old_id, new_id) = (LocalArtifactId::new(), LocalArtifactId::new());
    // The replacement committed durably: the suspended row pins it.
    conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = ?1",
        crate::db::fixture::params![id, new_id.to_string()],
    )
    .unwrap();
    suspend_snapshot_publication::write_full_suspend_artifact(
        &suspend_snapshot_publication::canonical_suspend_snapshot_dir(&sandbox_dir),
        &id,
        new_id,
        b"new",
    );
    let hold = suspend_snapshot_publication::retired_suspend_snapshot_dir(&sandbox_dir, new_id);
    suspend_snapshot_publication::write_full_suspend_artifact(&hold, &id, old_id, b"old");
    let garbage = suspend_snapshot_publication::suspend_snapshot_staging_parent_dir(&sandbox_dir)
        .join(format!("{RETIRED_PREFIX}{}", LocalArtifactId::new()));
    suspend_snapshot_publication::write_full_suspend_artifact(&garbage, &id, old_id, b"gc");
    let service = SandboxManagerService::for_test(conn);

    let wake = service
        .auto_lifecycle
        .wake_lock(&id)
        .try_lock_owned()
        .unwrap();
    let swept = sweep(&service, cache.path());
    assert_eq!(swept, HashSet::from([garbage.clone()]));
    assert!(
        hold.exists(),
        "a hold is released only under the lifecycle guards"
    );
    drop(wake);

    // The batch lists garbage without deleting it, so the lane entry repeats.
    let mut swept = sweep(&service, cache.path());
    assert!(swept.remove(&garbage));
    assert!(!hold.exists());
    assert_eq!(swept.len(), 1);
    let released = swept.into_iter().next().unwrap();
    assert_eq!(std::fs::read(released.join("vmstate")).unwrap(), b"old");
}

/// A hold is the canonical snapshot moved aside by a publication. Without a
/// canonical snapshot it is the rollback candidate that publication restores,
/// and the sweep leaves it alone. With one, it is garbage even when a later
/// publication has replaced the artifact that displaced it: a release that
/// failed after commit must not strand the hold until sandbox deletion.
#[tokio::test]
async fn sweep_releases_holds_once_a_canonical_snapshot_exists() {
    let cache = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let committed = SandboxId::new().to_string();
    let in_flight = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &committed, SandboxState::Suspended);
    insert_test_sandbox(&conn, &in_flight, SandboxState::Suspending);
    let (a, b, c) = (
        LocalArtifactId::new(),
        LocalArtifactId::new(),
        LocalArtifactId::new(),
    );

    // A -> B committed but B's release failed; then B -> C committed and
    // the suspended row pins C.
    conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = ?1",
        crate::db::fixture::params![committed, c.to_string()],
    )
    .unwrap();
    let committed_dir = cache.path().join(&committed);
    suspend_snapshot_publication::write_full_suspend_artifact(
        &suspend_snapshot_publication::canonical_suspend_snapshot_dir(&committed_dir),
        &committed,
        c,
        b"c",
    );
    let stale_hold = suspend_snapshot_publication::retired_suspend_snapshot_dir(&committed_dir, b);
    let fresh_hold = suspend_snapshot_publication::retired_suspend_snapshot_dir(&committed_dir, c);
    suspend_snapshot_publication::write_full_suspend_artifact(&stale_hold, &committed, a, b"a");
    suspend_snapshot_publication::write_full_suspend_artifact(&fresh_hold, &committed, b, b"b");

    // A publication of B interrupted between its renames and still owned by
    // recovery (the row is suspending): no canonical, and not the sweep's.
    let in_flight_dir = cache.path().join(&in_flight);
    let kept = suspend_snapshot_publication::retired_suspend_snapshot_dir(&in_flight_dir, b);
    suspend_snapshot_publication::write_full_suspend_artifact(&kept, &in_flight, a, b"a");
    let service = SandboxManagerService::for_test(conn);

    let swept = sweep(&service, cache.path());
    assert!(
        !stale_hold.exists(),
        "the hold B's failed release left behind"
    );
    assert!(
        !fresh_hold.exists(),
        "the hold displaced by the canonical artifact"
    );
    assert!(
        kept.exists(),
        "no canonical snapshot and a publication in flight: the rollback candidate stays"
    );
    let mut contents: Vec<Vec<u8>> = swept
        .iter()
        .map(|garbage| {
            assert!(
                garbage
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with(RETIRED_PREFIX)
            );
            std::fs::read(garbage.join("vmstate")).unwrap()
        })
        .collect();
    contents.sort();
    assert_eq!(contents, vec![b"a".to_vec(), b"b".to_vec()]);
}

/// An ordinary retired snapshot is gone as soon as its publication commits,
/// as before the lane existed. One this service cannot delete stays in the
/// lane for node-manager and the sweep; it never lingers under another name.
#[tokio::test]
async fn immediate_reclaim_removes_ordinary_entries_and_leaves_the_rest_in_the_lane() {
    use std::os::unix::fs::PermissionsExt;
    let cache = tempfile::tempdir().unwrap();
    let sandbox_dir = cache.path().join("sandbox");
    let lane = suspend_snapshot_publication::suspend_snapshot_staging_parent_dir(&sandbox_dir);
    let ordinary = lane.join(format!("{RETIRED_PREFIX}{}", LocalArtifactId::new()));
    let stubborn = lane.join(format!("{RETIRED_PREFIX}{}", LocalArtifactId::new()));
    std::fs::create_dir_all(&ordinary).unwrap();
    std::fs::write(ordinary.join("vmstate"), b"old").unwrap();
    let bundle = stubborn.join("rootfs.btrfs-source");
    std::fs::create_dir_all(&bundle).unwrap();
    std::fs::write(bundle.join("source.json"), b"{}").unwrap();
    // Read-only directory: unlinking its entries fails for a normal user, as
    // a retained subvolume's do.
    std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(0o555)).unwrap();
    // Node-manager refuses the delete the stubborn entry falls back to.
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();

    // At capacity nothing is spawned; the entries join the backlog. The
    // slots here are held by the test rather than by workers, so no worker
    // exit drains them; the next admitted reclaim does, before it returns.
    // Enqueue and a worker's exit are serialized by the backlog lock, so a
    // worker that saw an empty backlog cannot exit after an entry lands.
    let backlog: Vec<_> = (0..3)
        .map(|n| {
            let dir = lane.join(format!("{RETIRED_PREFIX}{}", LocalArtifactId::new()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("vmstate"), format!("{n}")).unwrap();
            dir
        })
        .collect();
    let held: Vec<_> = (0..IMMEDIATE_RECLAIM_CONCURRENCY)
        .map(|_| IMMEDIATE_RECLAIM_SLOTS.try_acquire().unwrap())
        .collect();
    for dir in &backlog {
        assert!(
            reclaim_garbage_now("sandbox".into(), dir.clone(), node_manager.connector()).is_none()
        );
        assert!(dir.exists());
    }
    drop(held);

    reclaim_garbage_now("sandbox".into(), ordinary.clone(), node_manager.connector())
        .unwrap()
        .await
        .unwrap();
    for dir in &backlog {
        assert!(!dir.exists(), "backlog drained once a slot was free");
    }
    reclaim_garbage_now("sandbox".into(), stubborn.clone(), node_manager.connector())
        .unwrap()
        .await
        .unwrap();

    assert!(!ordinary.exists());
    let remaining: Vec<_> = std::fs::read_dir(&lane)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    for entry in &remaining {
        assert_eq!(
            entry, &stubborn,
            "only the entry this user cannot delete remains"
        );
        std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    assert!(remaining.len() <= 1);
}

/// A backlogged entry falls back to node-manager through the connector that
/// submitted it, not through the connector of whichever worker drains it.
#[tokio::test]
async fn immediate_reclaim_backlog_uses_each_entrys_own_node_manager() {
    use pplx_space_node_mgr_client::proto::{
        DeleteCacheArtifactRequest, DeleteCacheArtifactResponse,
    };
    use std::sync::{Arc, Mutex};

    fn recording_node_manager(
        deletes: &Arc<Mutex<Vec<String>>>,
    ) -> crate::node_manager_transport::fake::ServedNodeManager {
        let deletes = Arc::clone(deletes);
        crate::node_manager_transport::fake::FakeNodeManager::default()
            .unary(
                "DeleteCacheArtifact",
                move |request: DeleteCacheArtifactRequest| {
                    deletes.lock().unwrap().push(request.local_path);
                    Ok(DeleteCacheArtifactResponse::default())
                },
            )
            .serve()
    }

    let cache = tempfile::tempdir().unwrap();
    let lane = suspend_snapshot_publication::suspend_snapshot_staging_parent_dir(
        &cache.path().join("sandbox"),
    );
    std::fs::create_dir_all(&lane).unwrap();
    // A regular file is not a directory, so the local removal fails even for
    // root and every entry falls back to node-manager.
    let entry = |name: &str| {
        let path = lane.join(format!("{RETIRED_PREFIX}{name}"));
        std::fs::write(&path, b"retired").unwrap();
        path
    };
    let (first, second, drainer) = (entry("first"), entry("second"), entry("drainer"));
    let deletes = [(); 3].map(|()| Arc::new(Mutex::new(Vec::new())));
    let node_managers = deletes.each_ref().map(recording_node_manager);

    let held: Vec<_> = (0..IMMEDIATE_RECLAIM_CONCURRENCY)
        .map(|_| IMMEDIATE_RECLAIM_SLOTS.try_acquire().unwrap())
        .collect();
    for (path, node_manager) in [(&first, &node_managers[0]), (&second, &node_managers[1])] {
        assert!(
            reclaim_garbage_now("sandbox".into(), path.clone(), node_manager.connector()).is_none(),
            "both slots are held, so the entry is backlogged"
        );
    }
    drop(held);
    reclaim_garbage_now(
        "sandbox".into(),
        drainer.clone(),
        node_managers[2].connector(),
    )
    .unwrap()
    .await
    .unwrap();

    let deleted = deletes.map(|deletes| deletes.lock().unwrap().clone());
    assert_eq!(
        deleted,
        [first, second, drainer].map(|path| vec![path.to_string_lossy().into_owned()])
    );
}
