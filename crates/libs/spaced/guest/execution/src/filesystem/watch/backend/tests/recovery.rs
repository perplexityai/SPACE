//! Recovery of directories a watch skipped for lack of permission, and the
//! directory budget those skips must not consume.

use super::*;

// A recursive watch over a root holding `blocked`, a mode-000 directory the
// kernel refuses to watch. A security policy can let a walk list such a
// directory anyway; the scanner here reports `blocked` the way that walk
// would, so each scan that reaches it attempts, and is refused, the watch.
#[cfg(target_os = "linux")]
struct RefusedWatch {
    blocked: PathBuf,
    scans: Arc<std::sync::atomic::AtomicUsize>,
    reserved: Arc<std::sync::atomic::AtomicUsize>,
    released: Arc<std::sync::atomic::AtomicUsize>,
    settled: Arc<tokio::sync::Semaphore>,
    shutdown: CancellationToken,
    _temp_dir: tempfile::TempDir,
}

#[cfg(target_os = "linux")]
impl RefusedWatch {
    async fn start() -> Self {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::AtomicUsize;
        let temp_dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp_dir.path()).unwrap();
        let blocked = root.join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let filter = WatchFilter::compile(
            super::super::super::filter::WatchFilterConfig::new(Vec::new(), Vec::new(), false)
                .unwrap(),
        )
        .unwrap();
        let scans = Arc::new(AtomicUsize::new(0));
        let scan_spawner: DirectoryScanSpawner = {
            let scans = Arc::clone(&scans);
            let blocked = blocked.clone();
            Arc::new(move |root, filter, candidates| {
                scans.fetch_add(1, Ordering::SeqCst);
                let blocked = blocked.clone();
                tokio::spawn(async move {
                    let mut result = scan_directory_candidates(&root, &filter, &candidates)?;
                    if candidates.contains(&root) || candidates.contains(&blocked) {
                        result.directories.push(blocked);
                    }
                    Ok(result)
                })
            })
        };
        let reserved = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let settled = Arc::new(tokio::sync::Semaphore::new(0));
        let shutdown = CancellationToken::new();
        spawn_notify_task_with_scanner(
            NotifyTaskConfig {
                tasks: tokio_util::task::TaskTracker::new(),
                root: root.clone(),
                recursive: true,
                include_metadata_events: false,
                filter,
                registration: RegisteredWatch::prepare(
                    WatchRegistrationPlan {
                        root: root.clone(),
                        descendants: vec![],
                        skipped: vec![],
                    },
                    tokio_util::task::TaskTracker::new(),
                )
                .await
                .unwrap(),
                shutdown: shutdown.clone(),
            },
            |event| std::future::ready(event.is_ok()),
            {
                let reserved = Arc::clone(&reserved);
                let released = Arc::clone(&released);
                let settled = Arc::clone(&settled);
                move |reserve, release, discovering| {
                    reserved.fetch_add(reserve, Ordering::SeqCst);
                    released.fetch_add(release, Ordering::SeqCst);
                    if discovering == Some(false) {
                        settled.add_permits(1);
                    }
                    std::future::ready(DirectoryAdjustment::Applied)
                }
            },
            scan_spawner,
        );
        Self {
            blocked,
            scans,
            reserved,
            released,
            settled,
            shutdown,
            _temp_dir: temp_dir,
        }
    }

    fn counts(&self) -> (usize, usize, usize) {
        (
            self.scans.load(Ordering::SeqCst),
            self.reserved.load(Ordering::SeqCst),
            self.released.load(Ordering::SeqCst),
        )
    }

    async fn settle(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.settled.acquire())
            .await
            .unwrap_or_else(|_| {
                let (scans, reserved, released) = self.counts();
                panic!(
                    "discovery never settled: {scans} scans, {reserved} directory charges, \
                     {released} released"
                )
            })
            .unwrap()
            .forget();
    }

    fn chmod_blocked(&self, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&self.blocked, std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

#[cfg(target_os = "linux")]
impl Drop for RefusedWatch {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.chmod_blocked(0o700);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_listed_directory_whose_watch_is_refused_is_not_rescanned_or_recharged() {
    if !pplx_space_test_env::unprivileged_or_skip("be refused an inotify watch") {
        return;
    }
    let watch = RefusedWatch::start().await;
    watch.settle().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // One verification scan and one refused charge, released; nothing after.
    // A rescan would find `blocked` again and charge it again.
    assert_eq!(watch.counts(), (1, 1, 1));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn permission_changes_that_leave_a_directory_unreadable_charge_nothing() {
    if !pplx_space_test_env::unprivileged_or_skip("be refused an inotify watch") {
        return;
    }
    let available = WATCH_DIRECTORY_SLOTS.available_permits();
    let watch = RefusedWatch::start().await;
    watch.settle().await;
    let changes = 5;
    for change in 0..changes {
        // Both modes keep `blocked` unreadable, so every walk is refused.
        watch.chmod_blocked(if change % 2 == 0 { 0o200 } else { 0o000 });
        watch.settle().await;
    }
    // Every change walked `blocked` again and attempted its watch...
    let (scans, reserved, released) = watch.counts();
    assert_eq!(scans, 1 + changes);
    assert_eq!(reserved, 1 + changes);
    // ...and none of those attempts kept a charge.
    assert_eq!(released, reserved);
    // The root holds the only process-wide charge.
    assert_eq!(WATCH_DIRECTORY_SLOTS.available_permits(), available - 1);
}

#[test]
fn skipped_directories_track_only_what_a_permission_change_can_recover() {
    let root = PathBuf::from("/watched");
    let metadata = |path: PathBuf| {
        Event::new(EventKind::Modify(ModifyKind::Metadata(
            notify::event::MetadataKind::Any,
        )))
        .add_path(path)
    };
    let mut skipped = SkippedDirectories::default();
    skipped.remember([root.join("a"), root.join("b/c"), root.join("d")]);

    // Only a metadata change on a remembered path is a hit, and a hit is taken.
    let write = Event::new(EventKind::Modify(ModifyKind::Data(
        notify::event::DataChange::Any,
    )))
    .add_path(root.join("a"));
    assert!(skipped.take_hits(&write).is_empty());
    assert!(
        skipped
            .take_hits(&metadata(root.join("elsewhere")))
            .is_empty()
    );
    assert_eq!(
        skipped.take_hits(&metadata(root.join("a"))),
        vec![root.join("a")]
    );
    assert!(skipped.take_hits(&metadata(root.join("a"))).is_empty());

    // Removing an ancestor forgets what was under it; registering forgets it.
    skipped.forget_descendants(&[root.join("b")]);
    assert!(skipped.take_hits(&metadata(root.join("b/c"))).is_empty());
    skipped.forget(&[root.join("d")]);
    assert!(skipped.take_hits(&metadata(root.join("d"))).is_empty());

    // Past the cap new skips are dropped rather than failing the watch.
    skipped.remember((0..=MAX_SKIPPED_DIRECTORIES).map(|index| root.join(index.to_string())));
    assert_eq!(skipped.paths.len(), MAX_SKIPPED_DIRECTORIES);
    assert!(skipped.overflowed);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_directory_made_readable_while_its_scan_runs_is_walked_again() {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    let filter = WatchFilter::compile(
        super::super::super::filter::WatchFilterConfig::new(Vec::new(), Vec::new(), false).unwrap(),
    )
    .unwrap();
    // Each scan reads the tree as soon as it starts and reports only when
    // released, so a permission change can land between its read and result.
    let scanned = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let scan_spawner: DirectoryScanSpawner = {
        let scanned = Arc::clone(&scanned);
        let release = Arc::clone(&release);
        Arc::new(move |root, filter, candidates| {
            let result = scan_directory_candidates(&root, &filter, &candidates);
            let scanned = Arc::clone(&scanned);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                scanned.add_permits(1);
                release.acquire().await.unwrap().forget();
                result
            })
        })
    };
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = CancellationToken::new();
    spawn_notify_task_with_scanner(
        NotifyTaskConfig {
            tasks: tokio_util::task::TaskTracker::new(),
            root: root.clone(),
            recursive: true,
            include_metadata_events: true,
            filter,
            registration: RegisteredWatch::prepare(
                WatchRegistrationPlan {
                    root: root.clone(),
                    descendants: vec![],
                    skipped: vec![],
                },
                tokio_util::task::TaskTracker::new(),
            )
            .await
            .unwrap(),
            shutdown: shutdown.clone(),
        },
        move |event| {
            let event_tx = event_tx.clone();
            async move { event_tx.send(event).is_ok() }
        },
        |_, _, _| std::future::ready(DirectoryAdjustment::Applied),
        scan_spawner,
    );
    let scan_read = || async {
        tokio::time::timeout(Duration::from_secs(5), scanned.acquire())
            .await
            .expect("scan started")
            .unwrap()
            .forget();
    };
    // The verification scan of the root.
    scan_read().await;
    release.add_permits(1);

    // `late` is created unreadable, so its scan skips it. It is made readable
    // before that scan reports, and before it was ever remembered.
    let late = root.join("late");
    std::fs::DirBuilder::new()
        .mode(0o000)
        .create(&late)
        .unwrap();
    scan_read().await;
    std::fs::set_permissions(&late, std::fs::Permissions::from_mode(0o700)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = event_rx.recv().await.unwrap().expect("healthy watch");
            if event.relative_path() == Path::new("late") && event.kind() == FileEventKind::Chmod {
                break;
            }
        }
    })
    .await
    .expect("the permission change reaches the watch before the scan reports");
    release.add_permits(64);

    let probe = late.join("probe");
    let watched = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            std::fs::write(&probe, b"x").unwrap();
            let arrived = tokio::time::timeout(Duration::from_millis(100), async {
                loop {
                    let event = event_rx.recv().await.unwrap().expect("healthy watch");
                    if event.relative_path() == Path::new("late/probe") {
                        break;
                    }
                }
            })
            .await;
            if arrived.is_ok() {
                break;
            }
        }
    })
    .await;
    assert!(
        watched.is_ok(),
        "`late` is watched after its permissions are restored"
    );
    shutdown.cancel();
}

// A recursive watch planned and scanned the way production does it, with a
// signal for each time discovery settles. Waiting for it before a permission
// change keeps the startup scan from finding the directory readable first.
#[cfg(target_os = "linux")]
struct SettlingWatch {
    root: PathBuf,
    events: tokio::sync::mpsc::UnboundedReceiver<Result<WatchEvent, Status>>,
    settled: Arc<tokio::sync::Semaphore>,
    // Times the root reported discovery under way, whose end the ledger
    // records as a possible loss.
    discovery_starts: Arc<std::sync::atomic::AtomicUsize>,
    shutdown: CancellationToken,
}

#[cfg(target_os = "linux")]
impl SettlingWatch {
    async fn start(root: PathBuf) -> Self {
        let filter = WatchFilter::compile(
            super::super::super::filter::WatchFilterConfig::new(Vec::new(), Vec::new(), false)
                .unwrap(),
        )
        .unwrap();
        let tasks = tokio_util::task::TaskTracker::new();
        let plan = super::super::super::paths::plan_watch_registration(
            &root,
            true,
            &filter,
            &super::super::super::paths::new_watch_directory_scan_slots(),
            &tasks,
        )
        .await
        .unwrap();
        let registration = RegisteredWatch::prepare(plan, tasks.clone()).await.unwrap();
        let scan_spawner: DirectoryScanSpawner = Arc::new(|root, filter, candidates| {
            tokio::task::spawn_blocking(move || {
                scan_directory_candidates(&root, &filter, &candidates)
            })
        });
        let settled = Arc::new(tokio::sync::Semaphore::new(0));
        let discovery_starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (event_tx, events) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        spawn_notify_task_with_scanner(
            NotifyTaskConfig {
                tasks,
                root: root.clone(),
                recursive: true,
                include_metadata_events: false,
                filter,
                registration,
                shutdown: shutdown.clone(),
            },
            move |event| {
                let event_tx = event_tx.clone();
                async move { event_tx.send(event).is_ok() }
            },
            {
                let settled = Arc::clone(&settled);
                let discovery_starts = Arc::clone(&discovery_starts);
                move |_, _, discovering| {
                    match discovering {
                        Some(false) => settled.add_permits(1),
                        Some(true) => {
                            discovery_starts.fetch_add(1, Ordering::SeqCst);
                        }
                        None => {}
                    }
                    std::future::ready(DirectoryAdjustment::Applied)
                }
            },
            scan_spawner,
        );
        let watch = Self {
            root,
            events,
            settled,
            discovery_starts,
            shutdown,
        };
        watch.settle().await;
        watch
    }

    async fn settle(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.settled.acquire())
            .await
            .expect("discovery settles")
            .unwrap()
            .forget();
    }

    /// Writes `relative` until the watch reports it: discovery registers a
    /// directory asynchronously, so one write can land before its watch.
    async fn observes(&mut self, relative: &str) -> bool {
        let path = self.root.join(relative);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                std::fs::write(&path, b"x").unwrap();
                let arrived = tokio::time::timeout(Duration::from_millis(100), async {
                    loop {
                        let event = self.events.recv().await.unwrap().expect("healthy watch");
                        if event.relative_path() == Path::new(relative) {
                            break;
                        }
                    }
                })
                .await;
                if arrived.is_ok() {
                    break;
                }
            }
        })
        .await
        .is_ok()
    }
}

#[cfg(target_os = "linux")]
impl Drop for SettlingWatch {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Builds a tree under a fresh root, starts a watch over it, makes `path`
/// accessible, and reports whether a write to `probe` is then observed. `None`
/// when running as root, which reads every directory.
#[cfg(target_os = "linux")]
async fn observed_after_chmod(build: impl FnOnce(&Path), path: &str, probe: &str) -> Option<bool> {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return None;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    build(&root);
    let mut watch = SettlingWatch::start(root.clone()).await;
    std::fs::set_permissions(root.join(path), std::fs::Permissions::from_mode(0o755)).unwrap();
    Some(watch.observes(probe).await)
}

#[cfg(target_os = "linux")]
fn set_mode(root: &Path, path: &str, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root.join(path), std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn only_a_recheck_that_brings_a_directory_back_reports_discovery() {
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    std::fs::create_dir(root.join("locked")).unwrap();
    set_mode(&root, "locked", 0o000);
    let mut watch = SettlingWatch::start(root.clone()).await;
    let starts = || watch.discovery_starts.load(Ordering::SeqCst);
    let before = starts();

    // Still unreadable: the recheck finds nothing, so nothing could be lost.
    set_mode(&root, "locked", 0o000);
    watch.settle().await;
    assert_eq!(starts(), before);

    // Readable: the recheck brings it back, and files written while it was
    // unwatched went unreported.
    set_mode(&root, "locked", 0o755);
    assert!(watch.observes("locked/probe").await);
    assert!(watch.discovery_starts.load(Ordering::SeqCst) > before);
}

#[cfg(target_os = "linux")]
#[test]
fn a_change_above_a_remembered_skip_walks_it_again() {
    let root = PathBuf::from("/watched");
    let remembered = root.join("a/b");
    let mut skipped = SkippedDirectories::default();
    skipped.remember([remembered.clone(), root.join("a0")]);
    let metadata = |path: PathBuf| {
        Event::new(EventKind::Modify(ModifyKind::Metadata(
            notify::event::MetadataKind::Any,
        )))
        .add_path(path)
    };

    // `a/b` was remembered after `a` lost search permission and got it back;
    // the change that brought it back is on `a`. `a0` is a sibling, not below.
    assert_eq!(
        skipped.take_hits(&metadata(root.join("a"))),
        vec![remembered]
    );
    assert_eq!(skipped.paths, BTreeSet::from([root.join("a0")]));
}

#[tokio::test]
async fn a_change_above_a_skip_during_its_scan_rechecks_it() {
    let root = PathBuf::from("/watched");
    let recheck = root.join("a/b");
    let mut scan = scan_of(
        BTreeSet::from([root.clone(), recheck.clone()]),
        BTreeSet::from([recheck.clone()]),
    );
    let metadata = |path: PathBuf| {
        Event::new(EventKind::Modify(ModifyKind::Metadata(
            notify::event::MetadataKind::Any,
        )))
        .add_path(path)
    };
    for index in 0..=DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY {
        scan.note_metadata_change(&metadata(root.join(format!("other-{index}"))));
    }
    assert!(scan.metadata_changes_overflowed);

    // Past the bound, a change above the scan's own recheck is still seen.
    assert!(!scan.changed_since_scan(&recheck));
    scan.note_metadata_change(&metadata(root.join("a")));
    assert!(scan.changed_since_scan(&recheck));
}

#[test]
fn a_kernel_queue_overflow_ends_the_watch_like_a_channel_overflow() {
    let overflow = Ok(Event::new(EventKind::Other).set_flag(notify::event::Flag::Rescan));
    assert_eq!(raw_disposition(&overflow), RawDisposition::Lost);
    let open = Ok(Event::new(EventKind::Access(
        notify::event::AccessKind::Any,
    )));
    assert_eq!(raw_disposition(&open), RawDisposition::Drop);
    let created = Ok(Event::new(EventKind::Create(CreateKind::Folder)));
    assert_eq!(raw_disposition(&created), RawDisposition::Forward);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_unreadable_directory_is_watched_once_it_becomes_readable() {
    let observed = observed_after_chmod(
        |root| {
            std::fs::create_dir(root.join("locked")).unwrap();
            set_mode(root, "locked", 0o000);
        },
        "locked",
        "locked/probe",
    )
    .await;
    assert_ne!(
        observed,
        Some(false),
        "a file in the restored directory is reported"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn children_of_an_unsearchable_directory_are_watched_once_it_is_searchable() {
    let observed = observed_after_chmod(
        |root| {
            std::fs::create_dir_all(root.join("listable/child")).unwrap();
            set_mode(root, "listable", 0o400);
        },
        "listable",
        "listable/child/probe",
    )
    .await;
    assert_ne!(
        observed,
        Some(false),
        "a file in the existing child is reported"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn recovering_more_directories_than_the_discovery_queue_holds_keeps_the_watch() {
    let children = DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY + 1;
    let probe = format!("listable/{}/probe", children - 1);
    let observed = observed_after_chmod(
        |root| {
            for index in 0..children {
                std::fs::create_dir_all(root.join("listable").join(index.to_string())).unwrap();
            }
            set_mode(root, "listable", 0o400);
        },
        "listable",
        &probe,
    )
    .await;
    assert_ne!(
        observed,
        Some(false),
        "a file in the last recovered child is reported"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_subtree_of_an_unreadable_parent_is_watched_once_it_is_readable() {
    let observed = observed_after_chmod(
        |root| {
            std::fs::create_dir_all(root.join("parent/child")).unwrap();
            set_mode(root, "parent", 0o000);
        },
        "parent",
        "parent/child/probe",
    )
    .await;
    assert_ne!(
        observed,
        Some(false),
        "a file in the nested child is reported"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_directory_hidden_by_an_unsearchable_ancestor_is_watched_once_it_is_searchable() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    let ancestor = root.join("a");
    std::fs::create_dir_all(ancestor.join("b")).unwrap();
    let mut watch = SettlingWatch::start(root.clone()).await;

    // A process already inside `a/b` creates `c` after `a` loses search
    // permission. The create event comes from `b`'s watch, but looking up
    // `a/b/c` fails because of `a`, two levels up.
    let inside = std::fs::File::open(ancestor.join("b")).unwrap();
    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o600)).unwrap();
    nix::sys::stat::mkdirat(
        &inside,
        "c",
        nix::sys::stat::Mode::from_bits_truncate(0o755),
    )
    .unwrap();
    watch.settle().await;

    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        watch.observes("a/b/c/probe").await,
        "`a/b/c` is watched once `a` is searchable again"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_remembered_root_that_turns_unreadable_keeps_its_watch() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    std::fs::create_dir(root.join("child")).unwrap();
    // A root it can list but not search is watched, and remembered for the
    // children it cannot reach.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o400)).unwrap();
    let mut watch = SettlingWatch::start(root.clone()).await;

    // Its next permission change walks it again, and that walk is refused.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000)).unwrap();
    watch.settle().await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        watch.observes("child/probe").await,
        "the watch survives the refused walk and reaches `child` once the root is searchable"
    );
}

#[test]
fn due_rechecks_wait_for_room_and_are_then_queued() {
    let root = PathBuf::from("/watched");
    let mut pending = PendingDirectoryDiscovery::default();
    let full = (0..DIRECTORY_RECHECK_CAPACITY)
        .map(|index| root.join(format!("recheck-{index}")))
        .collect::<Vec<_>>();
    assert!(pending.extend_best_effort(full).is_empty());
    let mut skipped = SkippedDirectories::default();
    let late = root.join("late");
    skipped.defer(pending.extend_best_effort(vec![late.clone()]));
    assert_eq!(skipped.due, BTreeSet::from([late.clone()]));

    // While the queue is full the recheck stays due rather than lost.
    skipped.queue_due(&mut pending);
    assert!(skipped.due.contains(&late));
    // Once a scan takes the queue, the due recheck is queued for the next one.
    let (taken, _) = pending.take();
    assert!(!taken.contains(&late));
    skipped.queue_due(&mut pending);
    assert!(skipped.due.is_empty());
    assert!(pending.take().0.contains(&late));
}

#[test]
fn forgetting_a_path_also_drops_its_due_recheck() {
    let root = PathBuf::from("/watched");
    let mut skipped = SkippedDirectories::default();
    skipped.defer(vec![root.join("a/b"), root.join("c")]);
    skipped.forget_descendants(&[root.join("a")]);
    skipped.forget(&[root.join("c")]);
    assert!(skipped.due.is_empty());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_refused_watch_remembers_the_ancestor_that_blocks_it() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    let ancestor = root.join("a");
    std::fs::create_dir_all(ancestor.join("b/c")).unwrap();
    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o600)).unwrap();
    let remembered = refused_with_blocking_ancestors(&root, vec![ancestor.join("b/c")]).await;
    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(remembered, vec![ancestor.clone(), ancestor.join("b/c")]);

    // A directory refused for its own mode can still be looked up, so only it
    // is remembered, not an ancestor that blocks nothing.
    let locked = root.join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let remembered = refused_with_blocking_ancestors(&root, vec![locked.clone()]).await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(remembered, vec![locked]);
}

#[test]
fn an_invalidated_recheck_does_not_overflow_discovery() {
    let root = PathBuf::from("/watched");
    let mut pending = PendingDirectoryDiscovery::default();
    assert!(pending.queue(
        &root,
        (0..DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY).map(|index| root.join(index.to_string()))
    ));
    let mut skipped = SkippedDirectories::default();
    let recheck = root.join("recheck");
    let invalidated = BTreeSet::from([recheck.clone()]);

    // Discovery's queue is full, yet the invalidated recheck is still kept.
    assert!(requeue_invalidated(
        &mut pending,
        &mut skipped,
        invalidated.clone(),
        &invalidated,
    ));
    assert!(pending.rechecks.contains(&recheck) || skipped.due.contains(&recheck));
    // An invalidated candidate discovery depends on still overflows it.
    assert!(!requeue_invalidated(
        &mut pending,
        &mut skipped,
        BTreeSet::from([root.join("required")]),
        &BTreeSet::new(),
    ));
}

fn scan_of(candidates: BTreeSet<PathBuf>, rechecks: BTreeSet<PathBuf>) -> DirectoryScan {
    DirectoryScan {
        task: tokio::spawn(async {
            Ok(DirectoryScanResult {
                directories: Vec::new(),
                skipped: Vec::new(),
                incomplete: false,
            })
        }),
        candidate_roots: Arc::new(candidates),
        invalidated_roots: HashSet::new(),
        invalidated_candidate_roots: BTreeSet::new(),
        narrowed_removals: 0,
        rechecks,
        metadata_changed: HashSet::new(),
        metadata_changes_overflowed: false,
    }
}

#[tokio::test]
async fn removing_a_file_does_not_discard_a_walk_of_its_tree() {
    let candidate = PathBuf::from("/watched/restored");
    let mut scan = scan_of(BTreeSet::from([candidate.clone()]), BTreeSet::new());
    let removed = vec![candidate.join("build/tmp")];

    let file = Event::new(EventKind::Remove(RemoveKind::File));
    scan.invalidate(directory_removals(&file, &removed), []);
    assert!(scan.invalidated_candidate_roots.is_empty());

    // A removed directory may have been in the walk's results; only its own
    // subtree is discarded, so churn below doesn't restart the whole walk.
    let folder = Event::new(EventKind::Remove(RemoveKind::Folder));
    scan.invalidate(directory_removals(&folder, &removed), []);
    assert_eq!(scan.invalidated_roots, HashSet::from([removed[0].clone()]));
    assert!(scan.invalidated_candidate_roots.is_empty());
}

#[tokio::test]
async fn a_metadata_storm_rechecks_each_skip_once() {
    let root = PathBuf::from("/watched");
    let rechecked = root.join("rechecked");
    let mut scan = scan_of(
        BTreeSet::from([root.clone(), rechecked.clone()]),
        BTreeSet::from([rechecked.clone()]),
    );
    for index in 0..=DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY {
        let event = Event::new(EventKind::Modify(ModifyKind::Metadata(
            notify::event::MetadataKind::Any,
        )))
        .add_path(root.join(index.to_string()));
        scan.note_metadata_change(&event);
    }
    assert!(scan.metadata_changes_overflowed);

    // Too many changes to track: a skip may have changed, unless this scan
    // was already its recheck.
    assert!(scan.changed_since_scan(&root.join("other")));
    assert!(!scan.changed_since_scan(&rechecked));

    // A recheck's own changes are still tracked once the storm overflows.
    let restored = Event::new(EventKind::Modify(ModifyKind::Metadata(
        notify::event::MetadataKind::Any,
    )))
    .add_path(rechecked.clone());
    scan.note_metadata_change(&restored);
    assert!(scan.changed_since_scan(&rechecked));
}

#[test]
fn a_recheck_whose_walk_ran_verifies_what_it_found() {
    let root = PathBuf::from("/watched");
    let recheck = root.join("restored");
    let rechecks = BTreeSet::from([recheck.clone()]);
    let mut pending = PendingDirectoryDiscovery::default();

    // More than discovery's queue holds, yet one candidate covers them all.
    let found = (0..=DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY)
        .map(|index| recheck.join(index.to_string()))
        .collect();
    assert!(queue_verification(
        &mut pending,
        found,
        &rechecks,
        &walked_rechecks(&rechecks, &[])
    ));
    assert_eq!(pending.candidates, rechecks);
    assert!(pending.rechecks.is_empty());
}

#[test]
fn a_directory_discovered_under_a_recovered_subtree_is_verified_on_its_own() {
    let root = PathBuf::from("/watched");
    let recheck = root.join("restored");
    let created = recheck.join("build/new");
    let rechecks = BTreeSet::from([recheck.clone()]);
    let candidates = BTreeSet::from([recheck.clone(), created.clone()]);
    let mut pending = PendingDirectoryDiscovery::default();

    // `created` was reported by its own event, so walking all of `restored`
    // again would cost far more than walking `created/child`.
    assert!(queue_verification(
        &mut pending,
        vec![created.join("child")],
        &candidates,
        &walked_rechecks(&rechecks, &[]),
    ));
    assert_eq!(pending.candidates, BTreeSet::from([created.join("child")]));
}

#[test]
fn a_refused_recheck_leaves_what_was_walked_beside_it_to_be_verified() {
    let root = PathBuf::from("/watched");
    let rechecks = BTreeSet::from([root.clone()]);
    let found = root.join("new/child");
    let mut pending = PendingDirectoryDiscovery::default();

    // The root's own walk was refused; another candidate's walk found `found`.
    assert!(queue_verification(
        &mut pending,
        vec![found.clone()],
        &rechecks,
        &walked_rechecks(&rechecks, std::slice::from_ref(&root)),
    ));
    assert_eq!(pending.candidates, BTreeSet::from([found]));
    assert!(pending.rechecks.is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn a_refused_root_walk_still_walks_the_candidates_it_folded_in() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp_dir.path()).unwrap();
    let created = root.join("new");
    std::fs::create_dir(&created).unwrap();
    // Searchable but not listable: `new` can still be looked up and watched.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o300)).unwrap();
    let filter = WatchFilter::compile(
        super::super::super::filter::WatchFilterConfig::new(Vec::new(), Vec::new(), false).unwrap(),
    )
    .unwrap();
    let scan = scan_directory_candidates(
        &root,
        &filter,
        &BTreeSet::from([root.clone(), created.clone()]),
    );
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let scan = scan.expect("a refused root walk does not end discovery");
    assert_eq!(scan.skipped, vec![root]);
    assert_eq!(scan.directories, vec![created]);
    assert!(!scan.incomplete);
}
