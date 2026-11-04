use notify::event::{ModifyKind, RemoveKind, RenameMode};
#[cfg(target_os = "linux")]
use std::time::Duration;

use super::*;
use pplx_space_common_core::types::GuestPath;

#[cfg(unix)]
#[tokio::test]
async fn path_loss_is_scoped_to_matching_consumers_and_monitoring_continues() {
    use std::os::unix::ffi::OsStringExt;

    use super::super::WatchManager;
    use futures_util::StreamExt;
    use pplx_space_common_core::types::{MAX_GUEST_PATH_BYTES, RelativeGlobPattern};
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::GetModifiedFilesRequest;

    for (name, loses_identity, displayed) in [
        (std::ffi::OsString::from("good-first"), false, true),
        ("line\nbreak".into(), true, false),
        ("control\u{85}".into(), true, false),
        (
            format!("{}leaf", "segment/".repeat(MAX_GUEST_PATH_BYTES / 8 + 1)).into(),
            true,
            false,
        ),
        (std::ffi::OsString::from_vec(vec![0xff]), true, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let shared = directory.path().join("shared");
        let private = directory.path().join("private");
        std::fs::create_dir(&shared).unwrap();
        std::fs::create_dir(&private).unwrap();
        let shared_path = GuestPath::parse(shared.display().to_string()).unwrap();
        let private_path = GuestPath::parse(private.display().to_string()).unwrap();
        let manager = WatchManager::new(CancellationToken::new());
        let (all, _) = manager
            .create_watcher(shared_path.clone(), true, true, vec![], vec![], Some(false))
            .await
            .unwrap();
        let included = vec![RelativeGlobPattern::parse("good*").unwrap()];
        let (filtered, _) = manager
            .create_watcher(
                shared_path.clone(),
                true,
                true,
                included.clone(),
                vec![],
                Some(false),
            )
            .await
            .unwrap();
        let mut stream = manager
            .watch_stream(shared_path.clone(), true, true, vec![], vec![], Some(false))
            .await
            .unwrap();
        let mut filtered_stream = manager
            .watch_stream(
                shared_path.clone(),
                true,
                true,
                included,
                vec![],
                Some(false),
            )
            .await
            .unwrap();
        manager
            .enable_file_changes(vec![shared_path, private_path], vec![], vec![], Some(false))
            .await
            .unwrap();
        // OS delivery is tested separately. Inject after registration to pin
        // conversion, consumer filtering, and ledger continuity deterministically.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while manager
                .inner
                .lock()
                .await
                .roots
                .values()
                .any(|root| root.discovering)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let filter = WatchFilter::compile(
            super::super::filter::WatchFilterConfig::new(vec![], vec![], false).unwrap(),
        )
        .unwrap();
        let ready = now_unix_ms().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while now_unix_ms().unwrap() <= ready {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let observed = now_unix_ms().unwrap();
        for (index, (root, name)) in [
            (&shared, name),
            (&private, "private-file".into()),
            (&shared, "good-later".into()),
        ]
        .into_iter()
        .enumerate()
        {
            let (key, generation) = {
                let state = manager.inner.lock().await;
                let (key, watch) = state
                    .roots
                    .iter()
                    .find(|(key, _)| key.root.ends_with(root.file_name().unwrap()))
                    .unwrap();
                (key.clone(), watch.generation)
            };
            let raw = Event::new(EventKind::Create(CreateKind::File)).add_path(key.root.join(name));
            let events = file_events_from_notify(&key.root, raw, true, &filter)
                .expect("a filename must not retire the backend");
            for event in events {
                let event = WatchEvent::new(
                    event.absolute_path().to_path_buf(),
                    event.relative_path().to_path_buf(),
                    event.kind(),
                    observed,
                )
                .with_is_regular_file(Some(true));
                assert!(manager.dispatch_root_event(&key, generation, event).await);
            }
            if index == 0 {
                let events = manager.get_watcher_events(&all).await;
                if loses_identity {
                    assert_eq!(events.unwrap_err().code(), tonic::Code::ResourceExhausted);
                } else {
                    assert_eq!(events.unwrap().len(), 1);
                }
                let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(event.is_err(), loses_identity);
            }
        }
        assert_eq!(
            manager.get_watcher_events(&all).await.unwrap()[0].path,
            "good-later"
        );
        let filtered = manager.get_watcher_events(&filtered).await.unwrap();
        assert_eq!(filtered.last().unwrap().path, "good-later");
        tokio::time::timeout(std::time::Duration::from_secs(1), filtered_stream.next())
            .await
            .unwrap()
            .unwrap()
            .expect("filtered stream remains healthy");
        let query = |cursor: u64| GetModifiedFilesRequest {
            cursor: Some(cursor.to_string()),
            ..Default::default()
        };
        let response = manager.get_modified_files(query(observed)).await.unwrap();
        assert!(response.monitoring_enabled);
        assert_eq!(response.overflowed, loses_identity);
        assert_eq!(response.files.len(), if displayed { 3 } else { 2 });
        assert!(
            response
                .files
                .iter()
                .any(|file| file.path == "private-file")
        );
        let later = manager
            .get_modified_files(query(observed + 1))
            .await
            .unwrap();
        assert!(
            !later.overflowed,
            "loss must not poison later observation ranges"
        );
        assert_eq!(manager.inner.lock().await.roots.len(), 2);
        drop(stream);
        drop(filtered_stream);
        manager.shutdown().await;
    }
}

#[cfg(unix)]
#[test]
fn non_utf8_events_are_reported_unless_intentionally_excluded() {
    use std::os::unix::ffi::OsStringExt;

    use super::super::filter::WatchFilterConfig;
    use pplx_space_common_core::types::RelativeGlobPattern;

    let root = Path::new("/watched");
    let invalid_name = std::ffi::OsString::from_vec(vec![0xff]);
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(
            vec![],
            vec![RelativeGlobPattern::parse("excluded/**").unwrap()],
            false,
        )
        .unwrap(),
    )
    .unwrap();
    for excluded in [false, true] {
        let parent = if excluded {
            root.join("excluded")
        } else {
            root.to_path_buf()
        };
        let invalid = parent.join(&invalid_name);
        let valid = parent.join("valid");
        for (kind, paths) in [
            (EventKind::Create(CreateKind::File), vec![invalid.clone()]),
            (
                EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Any)),
                vec![invalid.clone()],
            ),
            (EventKind::Remove(RemoveKind::File), vec![invalid.clone()]),
            (
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                vec![invalid.clone(), valid.clone()],
            ),
            (
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                vec![valid, invalid.clone()],
            ),
        ] {
            let mut raw = Event::new(kind);
            raw.paths = paths;
            let result = file_events_from_notify(root, raw, false, &filter);
            if excluded {
                assert!(result.unwrap().is_empty(), "kind={kind:?}");
            } else {
                let events = result.expect("non-UTF-8 paths remain observable");
                assert!(
                    events
                        .iter()
                        .any(|event| event.to_proto().path.contains('\u{fffd}')),
                    "kind={kind:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn nested_watchers_keep_their_own_relative_paths_and_excludes() {
    use super::super::WatchManager;
    use futures_util::StreamExt;
    use pplx_space_common_core::types::RelativeGlobPattern;
    for exclude in [None, Some("child/**")] {
        let directory = tempfile::tempdir().expect("directory");
        let child = directory.path().join("child");
        std::fs::create_dir(&child).expect("child");
        let parent = GuestPath::parse(directory.path().display().to_string()).unwrap();
        let child_root = GuestPath::parse(child.display().to_string()).unwrap();
        let excludes: Vec<_> = exclude
            .into_iter()
            .map(|pattern| RelativeGlobPattern::parse(pattern).unwrap())
            .collect();
        let manager = WatchManager::new(CancellationToken::new());
        // Routing and relative filtering operate on ready roots; OS delivery
        // has separate backend tests and must not gate this policy matrix.
        for root in [&parent, &child_root] {
            let mut state = manager.inner.lock().await;
            let generation = super::super::state::next_root_generation_locked(&mut state);
            state.roots.insert(
                super::super::state::WatchRootKey {
                    root: std::fs::canonicalize(root.expose()).expect("root"),
                    recursive: true,
                    include_metadata_events: false,
                    filter_config: super::super::filter::WatchFilterConfig::new(
                        vec![],
                        excludes.clone(),
                        true,
                    )
                    .expect("config")
                    .backend_config(),
                },
                super::super::state::WatchRoot {
                    generation,
                    consumers: Default::default(),
                    directory_count: 1,
                    discovering: false,
                    shutdown: CancellationToken::new(),
                },
            );
        }
        manager
            .create_watcher(parent, true, false, vec![], excludes.clone(), None)
            .await
            .expect("parent watcher");
        let (id, _) = manager
            .create_watcher(
                child_root.clone(),
                true,
                false,
                vec![],
                excludes.clone(),
                None,
            )
            .await
            .expect("child watcher");
        let mut stream = manager
            .watch_stream(child_root, true, false, vec![], excludes, None)
            .await
            .unwrap();
        // Feed the real backend classifier after registration, independent of
        // OS callback startup latency. Relative excludes belong to each root.
        let raw = Event::new(EventKind::Create(CreateKind::File))
            .add_path(std::fs::canonicalize(&child).unwrap().join("file"));
        let backends: Vec<_> = manager
            .inner
            .lock()
            .await
            .roots
            .iter()
            .map(|(key, root)| (key.clone(), root.generation))
            .collect();
        for (key, generation) in backends {
            let filter = WatchFilter::compile(key.filter_config.clone()).unwrap();
            for event in file_events_from_notify(&key.root, raw.clone(), false, &filter).unwrap() {
                manager.dispatch_root_event(&key, generation, event).await;
            }
        }
        let event = futures_util::poll!(stream.next()).map(|event| event.unwrap().unwrap());
        assert!(
            matches!(&event, std::task::Poll::Ready(event) if event.path == "file"),
            "child event must use its own root: {event:?}"
        );
        let events = manager.get_watcher_events(&id).await.unwrap();
        assert!(!events.is_empty());
        assert!(events.iter().all(|event| event.path == "file"));
        manager.shutdown().await;
    }
}

#[tokio::test]
async fn raw_event_batches_leave_ready_work_for_the_next_shutdown_check() {
    // One extra ready event represents a producer replenishing the bounded
    // channel while the batch is drained, without a timing-dependent race.
    let (tx, mut rx) = mpsc::channel(RAW_EVENT_BUFFER_CAPACITY + 1);
    for _ in 0..=RAW_EVENT_BUFFER_CAPACITY {
        tx.try_send(Ok(Event::new(EventKind::Any)))
            .expect("capacity");
    }
    let first = rx.recv().await.expect("first event");
    let batch = take_raw_event_batch(first, &mut rx);
    assert_eq!(batch.len(), RAW_EVENT_BUFFER_CAPACITY);
    rx.try_recv().expect("ready work remains").expect("event");
}

#[test]
fn removed_subtree_is_eligible_for_manual_reregistration() {
    let root = PathBuf::from("/watched");
    let removed_root = root.join("recreated");
    let nested = removed_root.join("nested");
    let retained = root.join("retained");
    let mut registered = HashSet::from([
        root.clone(),
        removed_root.clone(),
        nested.clone(),
        retained.clone(),
    ]);
    let event = Event::new(EventKind::Remove(RemoveKind::Folder)).add_path(removed_root.clone());

    let removed = take_removed_directories(&root, &event, &mut registered);

    assert!(removed.registered.contains(&removed_root));
    assert!(removed.registered.contains(&nested));
    assert_eq!(removed.registered.len(), 2);
    assert_eq!(registered, HashSet::from([root, retained]));
}

#[test]
fn rename_both_unregisters_old_subtree_and_scans_only_new_path() {
    let root = PathBuf::from("/watched");
    let old_path = root.join("old");
    let new_path = root.join("new");
    let mut registered = HashSet::from([root.clone(), old_path.clone()]);
    let event = Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
        .add_path(old_path.clone())
        .add_path(new_path.clone());

    let removed = take_removed_directories(&root, &event, &mut registered);

    assert_eq!(removed.registered, vec![old_path]);
    assert_eq!(removed.registered.len(), 1);
    assert_eq!(registered, HashSet::from([root]));
    assert_eq!(event.paths.last(), Some(&new_path));
}

#[test]
fn known_file_creation_does_not_queue_directory_discovery() {
    let root = PathBuf::from("/watched");
    let event = Event::new(EventKind::Create(CreateKind::File)).add_path(root.join("file"));

    assert!(directory_candidates(&event).is_empty());
}

#[test]
fn directory_discovery_coalesces_nested_candidates() {
    let root = PathBuf::from("/watched");
    let parent = root.join("parent");

    assert_eq!(
        coalesce_directory_roots(vec![
            parent.join("child/grandchild"),
            root.join("sibling"),
            parent.join("child"),
            parent.clone(),
        ]),
        vec![parent, root.join("sibling")],
    );
}

#[test]
fn rename_candidate_that_is_a_file_does_not_scan() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let file = temp_dir.path().join("renamed");
    std::fs::write(&file, b"contents").expect("create file");
    let event =
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::To))).add_path(file.clone());
    let filter = WatchFilter::compile(
        super::super::filter::WatchFilterConfig::new(Vec::new(), Vec::new(), false)
            .expect("filter config"),
    )
    .expect("filter");

    let candidates = directory_candidates(&event).into_iter().collect();
    let scan = scan_directory_candidates(temp_dir.path(), &filter, &candidates).unwrap();

    assert!(scan.directories.is_empty());
    assert!(!scan.incomplete);
}

#[tokio::test]
async fn removal_invalidation_retains_only_bounded_scan_roots() {
    let root = PathBuf::from("/watched");
    let candidate = root.join("created");
    let candidates = Arc::new(BTreeSet::from([candidate.clone()]));
    let mut scan = DirectoryScan {
        task: tokio::spawn(async {
            Ok(DirectoryScanResult {
                directories: Vec::new(),
                skipped: Vec::new(),
                incomplete: false,
            })
        }),
        candidate_roots: candidates,
        invalidated_roots: HashSet::new(),
        invalidated_candidate_roots: BTreeSet::new(),
        narrowed_removals: 0,
        rechecks: BTreeSet::new(),
        metadata_changed: HashSet::new(),
        metadata_changes_overflowed: false,
    };

    scan.invalidate(
        &[root.join("unrelated-file"), candidate.join("removed")],
        [root.join("known-directory")],
    );
    // Below the candidate, only the removed subtree's results are stale.
    assert_eq!(
        scan.invalidated_roots,
        HashSet::from([candidate.join("removed"), root.join("known-directory")])
    );
    assert!(scan.invalidated_candidate_roots.is_empty());

    // Removing the candidate itself discards its walk and queues it again.
    scan.invalidate(std::slice::from_ref(&candidate), []);
    assert!(scan.invalidated_roots.contains(&candidate));
    assert_eq!(
        scan.invalidated_candidate_roots,
        BTreeSet::from([candidate.clone()])
    );

    // Removed registered directories don't count toward the bound...
    scan.invalidated_candidate_roots.clear();
    scan.invalidate(
        &[],
        (0..DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY).map(|index| root.join(format!("gone-{index}"))),
    );
    scan.invalidate(&[candidate.join("early")], []);
    assert!(scan.invalidated_candidate_roots.is_empty());

    // ...but past it, a removal below the candidate walks it again instead.
    for index in 0..DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY {
        scan.invalidate(&[candidate.join(format!("churn-{index}"))], []);
    }
    scan.invalidate(&[candidate.join("late")], []);
    assert_eq!(
        scan.invalidated_candidate_roots,
        BTreeSet::from([candidate])
    );
}

#[test]
fn directory_discovery_overflow_is_terminal_and_bounded() {
    let root = PathBuf::from("/watched");
    let mut pending = PendingDirectoryDiscovery::default();
    assert!(!pending.queue(
        &root,
        (0..=DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY).map(|index| root.join(index.to_string()))
    ));
    assert_eq!(
        pending.candidates.len(),
        DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY
    );
    assert!(!pending.extend_bounded([root.join("requeued")]));
    assert_eq!(
        pending.candidates.len(),
        DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn moved_subtrees_wait_for_admission_while_existing_events_progress() {
    for allow_subtree in [false, true] {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let root = temp_dir.path().to_path_buf();
        let shutdown = CancellationToken::new();
        let filter = WatchFilter::compile(
            super::super::filter::WatchFilterConfig::new(Vec::new(), Vec::new(), false)
                .expect("filter config"),
        )
        .expect("filter");
        let scan_started = Arc::new(tokio::sync::Semaphore::new(0));
        let release_scan = Arc::new(tokio::sync::Semaphore::new(0));
        let scan_spawner: DirectoryScanSpawner = {
            let scan_started = Arc::clone(&scan_started);
            let release_scan = Arc::clone(&release_scan);
            Arc::new(move |root, filter, candidates| {
                let scan_started = Arc::clone(&scan_started);
                let release_scan = Arc::clone(&release_scan);
                tokio::spawn(async move {
                    scan_started.add_permits(1);
                    let _permit = release_scan.acquire().await.expect("scan release");
                    scan_directory_candidates(&root, &filter, &candidates)
                })
            })
        };
        let discovery_complete = Arc::new(tokio::sync::Semaphore::new(0));
        let completed = discovery_complete.clone();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
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
                .expect("registration"),
                shutdown: shutdown.clone(),
            },
            move |event| {
                let event_tx = event_tx.clone();
                async move { event_tx.send(event).is_ok() }
            },
            move |reserve, _, discovering| {
                if discovering == Some(false) {
                    completed.add_permits(1);
                }
                async move {
                    if !allow_subtree && reserve > 1 {
                        DirectoryAdjustment::BudgetExceeded
                    } else {
                        DirectoryAdjustment::Applied
                    }
                }
            },
            scan_spawner,
        );

        let outside = tempfile::tempdir().expect("outside tree");
        std::fs::create_dir_all(outside.path().join("tree/nested/leaf")).expect("prebuilt tree");
        let created_directory = root.join("directory");
        tokio::fs::rename(outside.path().join("tree"), &created_directory)
            .await
            .expect("move subtree into watch");
        let _started = tokio::time::timeout(Duration::from_secs(5), scan_started.acquire())
            .await
            .expect("directory scan did not start")
            .expect("scan-start semaphore closed");

        tokio::fs::write(
            created_directory.join("nested/leaf/unadmitted"),
            b"outside budget",
        )
        .await
        .expect("write inside moved subtree");
        let created_file = root.join("during-scan");
        tokio::fs::write(&created_file, b"contents")
            .await
            .expect("create file during scan");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = event_rx
                    .recv()
                    .await
                    .expect("watch event channel closed")
                    .expect("watch backend error");
                assert!(
                    event.relative_path() == Path::new("directory")
                        || !event.relative_path().starts_with("directory"),
                    "subtree was registered before admission"
                );
                if event.relative_path() == Path::new("during-scan") {
                    break;
                }
            }
        })
        .await
        .expect("file event was blocked behind directory scan");

        release_scan.add_permits(1);
        if allow_subtree {
            let _complete =
                tokio::time::timeout(Duration::from_secs(5), discovery_complete.acquire())
                    .await
                    .expect("discovery finishes")
                    .expect("completion");
            tokio::fs::write(created_directory.join("nested/leaf/observed"), b"admitted")
                .await
                .expect("write after admission");
            tokio::time::timeout(Duration::from_secs(5), async {
                while event_rx
                    .recv()
                    .await
                    .expect("event")
                    .expect("healthy backend")
                    .relative_path()
                    != "directory/nested/leaf/observed"
                {}
            })
            .await
            .expect("descendant observed after bounded registration");
            tokio::fs::remove_dir_all(&root)
                .await
                .expect("remove watched root");
        }
        let expected = if allow_subtree {
            tonic::Code::NotFound
        } else {
            tonic::Code::ResourceExhausted
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Err(status) = event_rx.recv().await.expect("root loss delivered") {
                    assert_eq!(status.code(), expected);
                    break;
                }
            }
        })
        .await
        .expect("lost root terminates incomplete watch");
        shutdown.cancel();
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn registration_does_not_follow_directory_symlinks() {
    let watched = tempfile::tempdir().expect("watched root");
    let external = tempfile::tempdir().expect("external tree");
    let outside_file = external.path().join("outside");
    std::fs::write(&outside_file, b"before").expect("external file");
    let link = watched.path().join("link");
    std::os::unix::fs::symlink(external.path(), &link).expect("directory symlink");
    let root = watched.path().to_path_buf();
    let mut registration = RegisteredWatch::prepare(
        WatchRegistrationPlan {
            root: root.clone(),
            descendants: vec![],
            skipped: vec![],
        },
        tokio_util::task::TaskTracker::new(),
    )
    .await
    .expect("registration");
    assert_eq!(registration.directory_count(), 1);
    std::fs::write(&outside_file, b"after").expect("external modification");
    let marker = root.join("marker");
    std::fs::write(&marker, b"inside").expect("in-root marker");
    // Both writes precede the marker on the same inotify queue. A followed
    // symlink therefore exposes the external event before this positive check.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = registration
                .raw_rx
                .recv()
                .await
                .expect("watch open")
                .expect("event");
            assert!(
                event
                    .paths
                    .iter()
                    .all(|path| !path.starts_with(&link) && !path.starts_with(external.path())),
                "external tree escaped registration budget: {event:?}"
            );
            if event.paths.contains(&marker) {
                break;
            }
        }
    })
    .await
    .expect("in-root event delivered");
}

#[tokio::test]
async fn failed_registration_and_cancelled_operation_retain_physical_admission() {
    let tasks = tokio_util::task::TaskTracker::new();
    let directory = tempfile::tempdir().unwrap();
    let mut registration = RegisteredWatch::prepare(
        WatchRegistrationPlan {
            root: directory.path().to_path_buf(),
            descendants: vec![],
            skipped: vec![],
        },
        tasks.clone(),
    )
    .await
    .expect("registered root");
    let _result = apply_discovered_directories(
        directory.path(),
        &mut registration.watcher,
        &mut HashSet::new(),
        &mut SkippedDirectories::default(),
        vec![PathBuf::from(format!(
            "/spaced-missing-{}",
            uuid::Uuid::now_v7()
        ))],
        &mut |_, _, _| std::future::ready(DirectoryAdjustment::Applied),
    )
    .await;
    assert_eq!(
        registration
            .watcher
            .inner
            .as_ref()
            .expect("backend")
            .2
            .num_permits(),
        2,
        "notify failure does not prove that its kernel registration was rolled back"
    );
    // Use a private admission token so other concurrent watchers cannot affect
    // the ownership assertion; the same owner carries production admission.
    let admission = Arc::new(tokio::sync::Semaphore::new(1));
    registration.watcher.inner.as_mut().expect("backend").1 =
        admission.clone().try_acquire_owned().expect("admission");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let caller = tokio::spawn(async move {
        registration
            .watcher
            .update(move |_| {
                started_tx.send(()).expect("caller waiting");
                release_rx.recv().expect("release");
                Ok(())
            })
            .await
    });
    started_rx.await.expect("blocking operation started");
    caller.abort();
    caller.await.expect_err("caller cancelled");
    assert_eq!(admission.available_permits(), 0);
    tasks.close();
    assert!(futures_util::poll!(Box::pin(tasks.wait())).is_pending());
    release_tx.send(()).expect("release blocking owner");
    let _returned = tokio::time::timeout(std::time::Duration::from_secs(5), admission.acquire())
        .await
        .expect("retirement completes")
        .expect("admission open");
    tokio::time::timeout(std::time::Duration::from_secs(5), tasks.wait())
        .await
        .expect("drain includes backend teardown");
}

#[test]
fn discovery_distinguishes_a_required_root_from_a_vanished_child() {
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().join("root");
    let filter = WatchFilter::compile(
        super::super::filter::WatchFilterConfig::new(vec![], vec![], false).unwrap(),
    )
    .unwrap();
    for root_exists in [false, true] {
        if root_exists {
            std::fs::create_dir(&root).unwrap();
        }
        let candidate = if root_exists {
            root.join("missing-child")
        } else {
            root.clone()
        };
        let scan = scan_directory_candidates(&root, &filter, &BTreeSet::from([candidate]));
        if root_exists {
            assert!(!scan.unwrap().incomplete, "vanished children are tolerated");
        } else {
            assert!(matches!(scan, Err(status) if status.code() == tonic::Code::NotFound));
        }
    }
}

#[tokio::test]
async fn verification_root_loss_is_terminal_without_a_backend_event() {
    for replace_with_file in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let filter = WatchFilter::compile(
            super::super::filter::WatchFilterConfig::new(vec![], vec![], false).unwrap(),
        )
        .unwrap();
        let plan = super::super::paths::plan_watch_registration(
            &root,
            true,
            &filter,
            &super::super::paths::new_watch_directory_scan_slots(),
            &tokio_util::task::TaskTracker::new(),
        )
        .await
        .unwrap();
        let mut registration = RegisteredWatch::prepare(plan, tokio_util::task::TaskTracker::new())
            .await
            .unwrap();
        // Suppress OS delivery to prove that verification itself detects loss.
        let (_raw_tx, raw_rx) = mpsc::channel(1);
        registration.raw_rx = raw_rx;
        std::fs::remove_dir(&root).unwrap();
        if replace_with_file {
            std::fs::write(&root, b"replacement").unwrap();
        }
        let shutdown = CancellationToken::new();
        let tasks = tokio_util::task::TaskTracker::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn_notify_task(
            NotifyTaskConfig {
                tasks: tasks.clone(),
                root: root.clone(),
                recursive: true,
                include_metadata_events: false,
                filter,
                registration,
                shutdown: shutdown.clone(),
            },
            super::super::paths::new_watch_directory_scan_slots(),
            move |event| {
                let sent = tx.send(event).is_ok();
                async move { sent }
            },
            |_, _, discovering| {
                assert_ne!(
                    discovering,
                    Some(false),
                    "lost coverage must never finish discovery"
                );
                async { DirectoryAdjustment::Applied }
            },
        );
        let status = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .err()
            .expect("verification must fail");
        assert_eq!(
            status.code(),
            if replace_with_file {
                tonic::Code::InvalidArgument
            } else {
                tonic::Code::NotFound
            }
        );
        tasks.close();
        tokio::time::timeout(std::time::Duration::from_secs(5), tasks.wait())
            .await
            .unwrap();
        assert!(shutdown.is_cancelled());
        if replace_with_file {
            std::fs::remove_file(&root).unwrap();
        }
        std::fs::create_dir(&root).unwrap();
        assert!(
            rx.recv().await.is_none(),
            "recreation cannot revive a lost session"
        );
    }
}

#[tokio::test]
async fn registration_rejects_a_root_lost_after_planning() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    let filter = WatchFilter::compile(
        super::super::filter::WatchFilterConfig::new(vec![], vec![], false).unwrap(),
    )
    .unwrap();
    for replace_with_file in [false, true] {
        std::fs::create_dir(&root).unwrap();
        let plan = super::super::paths::plan_watch_registration(
            &root,
            true,
            &filter,
            &super::super::paths::new_watch_directory_scan_slots(),
            &tokio_util::task::TaskTracker::new(),
        )
        .await
        .unwrap();
        std::fs::remove_dir(&root).unwrap();
        if replace_with_file {
            std::fs::write(&root, b"replacement").unwrap();
        }
        assert!(
            RegisteredWatch::prepare(plan, tokio_util::task::TaskTracker::new())
                .await
                .is_err()
        );
        if replace_with_file {
            std::fs::remove_file(&root).unwrap();
        }
    }
    std::fs::create_dir(&root).unwrap();
    let plan = super::super::paths::plan_watch_registration(
        &root,
        true,
        &filter,
        &super::super::paths::new_watch_directory_scan_slots(),
        &tokio_util::task::TaskTracker::new(),
    )
    .await
    .unwrap();
    assert!(
        RegisteredWatch::prepare(plan, tokio_util::task::TaskTracker::new())
            .await
            .is_ok(),
        "fresh setup can recover"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn registration_skips_a_descendant_that_turned_unreadable_after_planning() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_path_buf();
    let locked = root.join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let registration = RegisteredWatch::prepare(
        WatchRegistrationPlan {
            root: root.clone(),
            descendants: vec![locked.clone()],
            skipped: vec![],
        },
        tokio_util::task::TaskTracker::new(),
    )
    .await
    .expect("an unreadable descendant does not fail registration");
    assert_eq!(registration.directories, vec![root.clone()]);
    // The refused descendant is remembered for its next permission change,
    // and its charge is released.
    // It was refused for its own mode, so it alone is remembered: a permission
    // change on it is what brings it back.
    assert_eq!(registration.skipped, vec![locked.clone()]);
    assert_eq!(registration.directory_count(), 1);
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
}

mod recovery;
