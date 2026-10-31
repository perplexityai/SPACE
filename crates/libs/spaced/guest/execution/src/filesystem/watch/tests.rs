use super::*;
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;

async fn modified_files(manager: &WatchManager) -> GetModifiedFilesResponse {
    manager
        .get_modified_files(GetModifiedFilesRequest::default())
        .await
        .expect("query")
}

#[tokio::test]
async fn admitted_pull_consumer_already_has_query_and_expiry_ownership() {
    let directory = tempfile::tempdir().expect("directory");
    let root = std::fs::canonicalize(directory.path()).expect("root");
    let config = WatchFilterConfig::new(vec![], vec![], false).expect("config");
    let manager = WatchManager::new(CancellationToken::new());
    let (_, consumer_id) = manager
        .add_consumer(
            WatchRootKey {
                root: root.clone(),
                recursive: false,
                include_metadata_events: false,
                filter_config: config.clone(),
            },
            WatchConsumer::Pull {
                root,
                recursive: false,
                events: VecDeque::new(),
                retained_bytes: 0,
                overflowed: false,
                last_access: Instant::now() - PULL_WATCHER_TTL - Duration::from_secs(1),
                filter: WatchFilter::compile(config).expect("filter"),
            },
        )
        .await
        .expect("admitted");
    // Cancellation after admission must not strand a consumer outside the
    // expiry index. The expired query must remove the owning root too.
    assert_eq!(
        manager
            .get_watcher_events(&format!("w{consumer_id}"))
            .await
            .expect_err("expired watcher")
            .code(),
        tonic::Code::NotFound,
    );
    assert!(manager.inner.lock().await.roots.is_empty());
    manager.shutdown().await;
}

#[tokio::test]
async fn byte_overflow_recovers_and_shutdown_closes_all_watch_owners() {
    use futures_util::StreamExt;
    let directory = tempfile::tempdir().unwrap();
    let root = GuestPath::parse(directory.path().display().to_string()).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    let (id, _) = manager
        .create_watcher(root.clone(), true, false, vec![], vec![], None)
        .await
        .unwrap();
    let mut overflow_stream = manager
        .watch_stream(root.clone(), true, false, vec![], vec![], None)
        .await
        .unwrap();
    let (key, generation) = {
        let state = manager.inner.lock().await;
        let (key, root) = state.roots.iter().next().unwrap();
        (key.clone(), root.generation)
    };
    for index in 0..40 {
        let relative = format!("{}-{index}", "x".repeat(3000));
        let event = WatchEvent::new(
            key.root.join(&relative),
            PathBuf::from(relative),
            FileEventKind::Created,
            1_000,
        );
        manager.dispatch_root_event(&key, generation, event).await;
    }
    assert_eq!(
        manager.get_watcher_events(&id).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    let stream_error = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Err(error) = overflow_stream.next().await.expect("explicit overflow") {
                break error;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(stream_error.code(), tonic::Code::ResourceExhausted);
    let event = WatchEvent::new(
        key.root.join("recent"),
        PathBuf::from("recent"),
        FileEventKind::Created,
        1_001,
    );
    manager.dispatch_root_event(&key, generation, event).await;
    assert_eq!(manager.get_watcher_events(&id).await.unwrap().len(), 1);
    let mut stream = manager
        .watch_stream(root.clone(), true, false, vec![], vec![], None)
        .await
        .unwrap();
    manager
        .enable_file_changes(vec![root.clone()], vec![], vec![], None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), manager.shutdown())
        .await
        .unwrap();
    assert!(stream.next().await.is_none());
    assert!(manager.inner.lock().await.roots.is_empty());
    manager
        .get_watcher_events(&id)
        .await
        .expect_err("watcher removed");
    manager
        .enable_file_changes(vec![root], vec![], vec![], None)
        .await
        .expect_err("manager shut down");
}

#[tokio::test]
async fn cancelled_or_superseded_setup_cannot_own_a_replacement_ledger() {
    for cancel in [true, false] {
        let directory = tempfile::tempdir().expect("directory");
        let root = GuestPath::parse(directory.path().display().to_string()).expect("path");
        let manager = WatchManager::new(CancellationToken::new());
        let mut first =
            Box::pin(manager.enable_file_changes(vec![root.clone()], vec![], vec![], None));
        assert!(futures_util::poll!(&mut first).is_pending());
        if cancel {
            drop(first);
            assert!(!modified_files(&manager).await.monitoring_enabled);
        } else {
            manager.disable_file_changes().await;
            manager
                .enable_file_changes(vec![root.clone()], vec![], vec![], None)
                .await
                .expect("replacement");
            assert_eq!(first.await.unwrap_err().code(), tonic::Code::Aborted);
        }
        manager
            .enable_file_changes(vec![root], vec![], vec![], None)
            .await
            .expect("enabled");
        assert!(modified_files(&manager).await.monitoring_enabled);
        manager.disable_file_changes().await;
        assert!(
            manager.inner.lock().await.roots.is_empty(),
            "teardown leaves no orphan consumers"
        );
    }
}

#[tokio::test]
async fn failed_ledger_registration_returns_capacity_without_removing_other_watchers() {
    let directory = tempfile::tempdir().expect("directory");
    let root = GuestPath::parse(directory.path().display().to_string()).expect("path");
    let manager = WatchManager::new(CancellationToken::new());
    let mut watchers = Vec::new();
    for _ in 0..MAX_ACTIVE_WATCHER_CONSUMERS - 1 {
        watchers.push(
            manager
                .create_watcher(root.clone(), false, false, vec![], vec![], None)
                .await
                .unwrap()
                .0,
        );
    }
    let other = tempfile::tempdir().expect("other root");
    let error = manager
        .enable_file_changes(
            vec![
                root.clone(),
                GuestPath::parse(other.path().display().to_string()).unwrap(),
            ],
            vec![],
            vec![],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(!modified_files(&manager).await.monitoring_enabled);
    watchers.push(
        manager
            .create_watcher(root, false, false, vec![], vec![], None)
            .await
            .expect("failed setup released its capacity")
            .0,
    );
    for watcher in watchers {
        manager
            .get_watcher_events(&watcher)
            .await
            .expect("existing watcher survives");
        manager.remove_watcher(&watcher).await.expect("remove");
    }
}

#[tokio::test]
async fn ledger_is_not_published_while_enable_can_be_cancelled() {
    let directory = tempfile::tempdir().expect("directory");
    let manager = WatchManager::new(CancellationToken::new());
    let mut enable = Box::pin(async {
        loop {
            let result = manager
                .enable_file_changes(
                    vec![GuestPath::parse(directory.path().display().to_string()).expect("path")],
                    Vec::new(),
                    Vec::new(),
                    None,
                )
                .await;
            if !result
                .as_ref()
                .is_err_and(|status| status.code() == tonic::Code::ResourceExhausted)
            {
                break result;
            }
            tokio::task::yield_now().await;
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let std::task::Poll::Ready(result) = futures_util::poll!(&mut enable) {
                result.expect("enable");
                break;
            }
            let response = modified_files(&manager).await;
            assert!(
                !response.monitoring_enabled,
                "cancelling enable here would leave an incomplete published ledger"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("setup completes");
    assert!(modified_files(&manager).await.monitoring_enabled);
    manager.disable_file_changes().await;
}

#[tokio::test]
async fn ledger_backend_loss_remains_visible_until_reenabled() {
    let directory = tempfile::tempdir().expect("directory");
    let root = GuestPath::parse(directory.path().display().to_string()).expect("root");
    let manager = WatchManager::new(CancellationToken::new());
    manager
        .enable_file_changes(vec![root.clone()], vec![], vec![], None)
        .await
        .expect("enable");
    let (key, generation) = {
        let state = manager.inner.lock().await;
        let (key, root) = state.roots.iter().next().expect("root");
        (key.clone(), root.generation)
    };
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
    .expect("initial registration verified");
    for (discovering, cursor, overflowed) in [
        (true, u64::MAX, true),
        (false, 0, true),
        (false, u64::MAX, false),
    ] {
        assert_eq!(
            manager
                .adjust_root_directories(&key, generation, 0, 0, Some(discovering))
                .await,
            DirectoryAdjustment::Applied
        );
        let response = manager
            .get_modified_files(GetModifiedFilesRequest {
                cursor: Some(cursor.to_string()),
                ..Default::default()
            })
            .await
            .expect("discovery query");
        assert!(
            response.monitoring_enabled,
            "discovery does not disable the ledger"
        );
        assert_eq!(
            response.overflowed, overflowed,
            "discovery={discovering}, cursor={cursor}"
        );
    }
    manager
        .dispatch_root_error(
            &key,
            generation,
            Status::resource_exhausted("backend overflow"),
        )
        .await;
    for disable in [false, true] {
        if disable {
            manager.disable_file_changes().await;
        }
        let response = modified_files(&manager).await;
        assert!(!response.monitoring_enabled);
        assert!(
            response.overflowed,
            "disable cannot acknowledge lost observations"
        );
    }
    manager
        .enable_file_changes(vec![root], vec![], vec![], None)
        .await
        .expect("reenable");
    let response = modified_files(&manager).await;
    assert!(response.monitoring_enabled);
    manager.shutdown().await;
}

// Recursive watch setup fails fast once its manager's scans are all busy. That
// budget belongs to the manager, so one manager's scans must not refuse
// another's setup, as they did when the budget was shared process-wide.
#[tokio::test]
async fn watch_managers_do_not_share_a_directory_scan_budget() {
    let directory = tempfile::tempdir().unwrap();
    let root = GuestPath::parse(directory.path().display().to_string()).unwrap();
    let busy = WatchManager::new(CancellationToken::new());
    let _busy_scans = Arc::clone(&busy.scan_slots)
        .acquire_many_owned(
            u32::try_from(busy.scan_slots.available_permits()).expect("scan budget"),
        )
        .await
        .unwrap();
    let refused = busy
        .create_watcher(root.clone(), true, false, vec![], vec![], None)
        .await
        .expect_err("busy manager refuses a recursive watch");
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::is_refused_before_start(&refused));

    let other = WatchManager::new(CancellationToken::new());
    other
        .create_watcher(root, true, false, vec![], vec![], None)
        .await
        .expect("another manager keeps its own scan budget");
    other.shutdown().await;
    busy.shutdown().await;
}

/// The next event `matches` accepts, or `None` if none arrives in time.
#[cfg(target_os = "linux")]
async fn next_event_matching(
    stream: &mut WatchStream,
    within: Duration,
    matches: impl Fn(&FileEvent) -> bool,
) -> Option<FileEvent> {
    use futures_util::StreamExt;
    tokio::time::timeout(within, async {
        loop {
            let event = stream
                .next()
                .await
                .expect("watch stays open")
                .expect("watch keeps delivering events");
            if matches(&event) {
                return event;
            }
        }
    })
    .await
    .ok()
}

#[cfg(target_os = "linux")]
async fn event_arrives_at(stream: &mut WatchStream, relative: &str, within: Duration) -> bool {
    next_event_matching(stream, within, |event| event.path == relative)
        .await
        .is_some()
}

fn is_coverage(event: &FileEvent) -> bool {
    matches!(
        event.kind(),
        FileEventKind::Unwatched | FileEventKind::Rewatched
    )
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn recursive_watch_skips_unreadable_subtrees_and_watches_the_rest() {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    // Neither listable nor searchable, and listable but not searchable: both
    // shapes a package cache can leave under a home directory.
    std::fs::create_dir_all(root.join("no-access/inner")).unwrap();
    std::fs::create_dir_all(root.join("no-search/inner")).unwrap();
    std::fs::create_dir(root.join("sibling")).unwrap();
    let mode = |path: &str, mode| {
        std::fs::set_permissions(root.join(path), std::fs::Permissions::from_mode(mode)).unwrap();
    };
    mode("no-access", 0o000);
    mode("no-search", 0o400);

    let manager = WatchManager::new(CancellationToken::new());
    let mut stream = manager
        .watch_stream(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![],
            vec![],
            Some(false),
        )
        .await
        .expect("an unreadable descendant does not fail the watch");
    std::fs::write(root.join("sibling/first"), b"x").unwrap();
    assert!(event_arrives_at(&mut stream, "sibling/first", Duration::from_secs(5)).await);

    // Discovery after registration skips a new unreadable directory the same
    // way instead of ending the watch. Discovery scans run in event order, so
    // once `later` is watched the scan that met `late` has completed.
    std::fs::DirBuilder::new()
        .mode(0o000)
        .create(root.join("sibling/late"))
        .unwrap();
    std::fs::create_dir(root.join("sibling/later")).unwrap();
    let discovered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            std::fs::write(root.join("sibling/later/probe"), b"x").unwrap();
            if event_arrives_at(
                &mut stream,
                "sibling/later/probe",
                Duration::from_millis(100),
            )
            .await
            {
                break;
            }
        }
    })
    .await;
    assert!(
        discovered.is_ok(),
        "the watch discovers `later` after skipping `late`"
    );

    manager.shutdown().await;
    for path in ["no-access", "no-search", "sibling/late"] {
        mode(path, 0o700);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_unreadable_watch_root_is_permission_denied() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let locked = directory.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let root = GuestPath::parse(locked.display().to_string()).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    for recursive in [false, true] {
        let Err(stream_error) = manager
            .watch_stream(root.clone(), recursive, false, vec![], vec![], None)
            .await
        else {
            panic!("an unreadable root cannot be watched (recursive={recursive})");
        };
        let watcher_error = manager
            .create_watcher(root.clone(), recursive, false, vec![], vec![], None)
            .await
            .expect_err("an unreadable root cannot be watched");
        for error in [stream_error, watcher_error] {
            assert_eq!(
                error.code(),
                tonic::Code::PermissionDenied,
                "recursive={recursive}: {error:?}"
            );
        }
    }
    manager.shutdown().await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(target_os = "linux")]
async fn next_coverage_event(stream: &mut WatchStream) -> FileEvent {
    next_event_matching(stream, Duration::from_secs(5), is_coverage)
        .await
        .expect("coverage is reported")
}

#[test]
fn a_coverage_seed_fits_any_consumer_and_says_when_it_is_incomplete() {
    let root = Path::new("/watched");
    let skips = |count: usize| {
        (0..count)
            .map(|index| root.join(format!("cache-{index:04}")))
            .collect::<BTreeSet<_>>()
    };
    let listed = |events: &[WatchEvent]| {
        events
            .iter()
            .map(|event| (event.kind(), event.relative_path().display().to_string()))
            .collect::<Vec<_>>()
    };
    assert!(coverage_seed(root, &BTreeSet::new(), false).is_empty());
    assert_eq!(
        listed(&coverage_seed(root, &skips(2), false)),
        [
            (FileEventKind::Unwatched, "cache-0000".to_string()),
            (FileEventKind::Unwatched, "cache-0001".to_string()),
        ]
    );
    // A root that stopped tracking skips says so even with none listed.
    assert_eq!(
        listed(&coverage_seed(root, &BTreeSet::new(), true)),
        [(FileEventKind::Unwatched, ".".to_string())]
    );
    // More skips than a quarter of the smallest buffer: the seed stops short,
    // so a pull watcher keeps room for its history, and ends with the root.
    let seed = coverage_seed(root, &skips(4_096), false);
    assert!(seed.len() <= STREAM_BUFFER_CAPACITY / 4, "{}", seed.len());
    let bytes = seed.iter().map(WatchEvent::retained_bytes).sum::<usize>();
    assert!(bytes <= WATCH_CONSUMER_BYTE_CAPACITY / 4, "{bytes}");
    assert_eq!(
        listed(&seed[seed.len() - 1..]),
        [(FileEventKind::Unwatched, ".".to_string())]
    );
    let mut queue = VecDeque::new();
    let (mut retained_bytes, mut overflowed) = (0, false);
    for event in &seed {
        queue_pull_event(&mut queue, &mut retained_bytes, &mut overflowed, event);
    }
    assert!(!overflowed && queue.len() == seed.len());
}

#[test]
fn a_coverage_step_collapses_each_kind_past_its_own_budget() {
    let root = Path::new("/watched");
    let path = |index: usize| root.join(format!("cache-{index:04}"));
    let event = |kind, index| coverage_event(root, &path(index), kind, 0).unwrap();
    let mut batch = CoverageBatch::new(root, 0);
    for event in budgeted_rewatches(root, &(0..300).map(path).collect(), 0) {
        batch.push_unbudgeted(event);
    }
    for index in 300..600 {
        batch.push(event(FileEventKind::Unwatched, index));
    }
    let events = batch.finish();
    let of_kind = |kind| {
        events
            .iter()
            .filter(|event| event.kind() == kind)
            .collect::<Vec<_>>()
    };
    // Rewatches don't crowd out new skips, and past its budget each kind
    // collapses to the root: a rescan of the whole watch, and an incomplete
    // list.
    for kind in [FileEventKind::Rewatched, FileEventKind::Unwatched] {
        let events = of_kind(kind);
        assert!(
            (2..=CoverageBatch::EVENTS).contains(&events.len()),
            "{kind:?}"
        );
        assert_eq!(events.last().unwrap().relative_path(), Path::new("."));
    }
    // A step within budget names every rewatch.
    let few = budgeted_rewatches(root, &(0..3).map(path).collect(), 0);
    assert_eq!(few.len(), 3);
    assert!(
        few.iter()
            .all(|event| event.relative_path() != Path::new("."))
    );
    // One the wire can't name asks for a rescan of the whole watch instead.
    let unnamed = PathBuf::from(std::ffi::OsString::from_vec(b"/watched/\xff".to_vec()));
    let events = budgeted_rewatches(root, &BTreeSet::from([unnamed]), 0);
    assert_eq!(
        events
            .iter()
            .map(|event| event.relative_path())
            .collect::<Vec<_>>(),
        [Path::new(".")]
    );
}

/// The next `count` coverage events, sorted.
#[cfg(target_os = "linux")]
async fn next_coverage_events(
    stream: &mut WatchStream,
    count: usize,
) -> Vec<(FileEventKind, String)> {
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let event = next_coverage_event(stream).await;
        events.push((event.kind(), event.path));
    }
    events.sort();
    events
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn every_consumer_learns_which_subtrees_are_unwatched_and_when_they_return() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    // Neither listable nor searchable, and listable but not searchable: the
    // second is watched itself, and only its subdirectories are missing.
    let locked = root.join("locked");
    let unsearchable = root.join("unsearchable");
    for (path, mode) in [(&locked, 0o000), (&unsearchable, 0o400)] {
        std::fs::create_dir_all(path.join("inner")).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    let both = |kind| {
        vec![
            (kind, "locked".to_string()),
            (kind, "unsearchable".to_string()),
        ]
    };
    let guest_root = GuestPath::parse(root.display().to_string()).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    // An include glob selects no directory, yet coverage still reaches the
    // consumer: the skipped subtree may hide files it matches.
    let watch = || {
        manager.watch_stream(
            guest_root.clone(),
            true,
            false,
            vec![RelativeGlobPattern::parse("**/*.rs".to_string()).unwrap()],
            vec![],
            Some(false),
        )
    };
    let mut first = watch().await.unwrap();
    let (watcher_id, _) = manager
        .create_watcher(guest_root.clone(), true, false, vec![], vec![], Some(false))
        .await
        .unwrap();
    manager
        .enable_file_changes(vec![guest_root.clone()], vec![], vec![], Some(false))
        .await
        .unwrap();

    assert_eq!(
        next_coverage_events(&mut first, 2).await,
        both(FileEventKind::Unwatched)
    );
    // A consumer joining the shared watch is told about the skips first.
    let mut late = watch().await.unwrap();
    assert_eq!(
        next_coverage_events(&mut late, 2).await,
        both(FileEventKind::Unwatched)
    );
    // Unlike `overflowed`, which completing discovery sets once, a
    // directory that stays unreadable doesn't ask for a full rescan on every
    // query: a query after the last loss is clean but still lists the skips.
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        let mut cursor = None;
        loop {
            let response = manager
                .get_modified_files(GetModifiedFilesRequest {
                    cursor: cursor.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            if cursor.is_some() && !response.overflowed {
                return response;
            }
            cursor = response.cursor;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a query after discovery isn't overflowed");
    assert_eq!(
        response.unwatched_paths,
        [
            locked.display().to_string(),
            unsearchable.display().to_string()
        ]
    );
    assert!(!response.unwatched_paths_truncated);
    assert!(
        response.files.is_empty(),
        "a skip isn't a modified file: {:?}",
        response.files
    );

    // A pull watcher that lost its history is told again what's unwatched,
    // since the rescan it's asked for can't see that.
    {
        let mut state = manager.inner.lock().await;
        let reference = state.pull_watchers[&watcher_id].clone();
        let Some(WatchConsumer::Pull { overflowed, .. }) = state
            .roots
            .get_mut(&reference.root)
            .and_then(|root| root.consumers.get_mut(&reference.consumer_id))
        else {
            panic!("pull watcher registered");
        };
        *overflowed = true;
    }
    assert_eq!(
        manager
            .get_watcher_events(&watcher_id)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );

    for path in [&locked, &unsearchable] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    for stream in [&mut first, &mut late] {
        assert_eq!(
            next_coverage_events(stream, 2).await,
            both(FileEventKind::Rewatched)
        );
    }
    let mut pulled = manager
        .get_watcher_events(&watcher_id)
        .await
        .unwrap()
        .into_iter()
        .filter(is_coverage)
        .map(|event| (event.kind(), event.path))
        .collect::<Vec<_>>();
    pulled.sort();
    assert_eq!(
        pulled,
        [
            both(FileEventKind::Unwatched),
            both(FileEventKind::Rewatched)
        ]
        .concat()
    );
    // The ledger's own backend may report a moment after the streams'.
    let restored = tokio::time::timeout(Duration::from_secs(5), async {
        while !modified_files(&manager).await.unwatched_paths.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(restored.is_ok(), "rewatched paths leave unwatched_paths");
    // A query from before the skips ended learns that changes were missed,
    // even with nothing left listed.
    let since_skipped = manager
        .get_modified_files(GetModifiedFilesRequest {
            cursor: response.cursor.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(since_skipped.unwatched_paths.is_empty());
    assert!(since_skipped.overflowed);

    manager.shutdown().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn more_skips_than_a_stream_buffers_collapse_to_the_root_instead_of_ending_it() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    // More unreadable directories than the stream's 1,024 events and 64 KiB.
    let locked = (0..1_100)
        .map(|index| root.join(format!("cache/{index:04}")))
        .collect::<Vec<_>>();
    for path in &locked {
        std::fs::create_dir_all(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
    }
    let manager = WatchManager::new(CancellationToken::new());
    let mut stream = manager
        .watch_stream(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let marker = next_event_matching(&mut stream, Duration::from_secs(5), |event| {
        event.path == "." && event.kind() == FileEventKind::Unwatched
    })
    .await;
    assert!(marker.is_some(), "the root says its list is incomplete");
    // The stream outlives the burst.
    std::fs::write(root.join("probe"), b"x").unwrap();
    assert!(event_arrives_at(&mut stream, "probe", Duration::from_secs(5)).await);

    manager.shutdown().await;
    for path in &locked {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_skip_removed_between_ledger_queries_reports_the_loss() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let locked = root.join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    manager
        .enable_file_changes(
            vec![GuestPath::parse(root.display().to_string()).unwrap()],
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    // A query after discovery that still lists the skip.
    let listed = tokio::time::timeout(Duration::from_secs(5), async {
        let mut cursor = None;
        loop {
            let response = manager
                .get_modified_files(GetModifiedFilesRequest {
                    cursor: cursor.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            if cursor.is_some() && !response.overflowed && !response.unwatched_paths.is_empty() {
                return response;
            }
            cursor = response.cursor;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the skip is listed");
    // Removing it walks nothing, yet the next query learns changes were missed.
    std::fs::remove_dir(&locked).unwrap();
    let after = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = manager
                .get_modified_files(GetModifiedFilesRequest {
                    cursor: listed.cursor.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            if response.unwatched_paths.is_empty() {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the removed skip leaves the list");
    assert!(after.overflowed);

    manager.shutdown().await;
}

#[tokio::test]
async fn a_rescan_of_the_whole_watch_restates_the_remaining_skips() {
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    let (watcher_id, _) = manager
        .create_watcher(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let (key, generation) = {
        let state = manager.inner.lock().await;
        let reference = state.pull_watchers[&watcher_id].clone();
        let generation = state.roots[&reference.root].generation;
        (reference.root, generation)
    };
    // More directories return at once than one step names, while one stays
    // left out.
    let returned = (0..300)
        .map(|index| root.join(format!("gone/{index:03}")))
        .collect::<BTreeSet<_>>();
    let remaining = root.join("still-locked");
    manager
        .inner
        .lock()
        .await
        .roots
        .get_mut(&key)
        .unwrap()
        .unwatched
        .extend(returned.iter().cloned().chain([remaining]));
    assert!(
        manager
            .apply_root_coverage(
                &key,
                generation,
                CoverageChange {
                    rewatched: returned,
                    ..Default::default()
                },
            )
            .await
    );
    let events = manager.get_watcher_events(&watcher_id).await.unwrap();
    let rescan = events
        .iter()
        .position(|event| event.kind() == FileEventKind::Rewatched && event.path == ".")
        .expect("a rescan of the whole watch");
    // After it, the consumer is told what's still left out, which replaces
    // its list.
    let restated = events[rescan + 1..]
        .iter()
        .map(|event| (event.kind(), event.path.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(restated, [(FileEventKind::Unwatched, "still-locked")]);

    manager.shutdown().await;
}

#[tokio::test]
async fn the_root_returning_with_other_directories_restates_the_remaining_skips() {
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    let (watcher_id, _) = manager
        .create_watcher(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let (key, generation) = {
        let state = manager.inner.lock().await;
        let reference = state.pull_watchers[&watcher_id].clone();
        let generation = state.roots[&reference.root].generation;
        (reference.root, generation)
    };
    let (returned, remaining) = (root.join("returned"), root.join("still-locked"));
    manager
        .inner
        .lock()
        .await
        .roots
        .get_mut(&key)
        .unwrap()
        .unwatched
        .extend([returned.clone(), remaining]);
    manager.get_watcher_events(&watcher_id).await.unwrap();
    // The root sorts before what it returns with, so its rescan isn't last.
    assert!(
        manager
            .apply_root_coverage(
                &key,
                generation,
                CoverageChange {
                    rewatched: BTreeSet::from([root.clone(), returned]),
                    ..Default::default()
                },
            )
            .await
    );
    let events = manager
        .get_watcher_events(&watcher_id)
        .await
        .unwrap()
        .into_iter()
        .map(|event| (event.kind(), event.path))
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        [
            (FileEventKind::Rewatched, ".".to_string()),
            (FileEventKind::Rewatched, "returned".to_string()),
            (FileEventKind::Unwatched, "still-locked".to_string()),
        ]
    );

    manager.shutdown().await;
}

// A consumer told its list was incomplete learns that it's complete again from
// a rescan of the whole watch and a restated list, even when the root itself
// didn't return: skips the list never named may have ended meanwhile.
#[tokio::test]
async fn a_list_that_turns_complete_is_restated_with_a_rescan_of_the_whole_watch() {
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let manager = WatchManager::new(CancellationToken::new());
    let (watcher_id, _) = manager
        .create_watcher(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let (key, generation) = {
        let state = manager.inner.lock().await;
        let reference = state.pull_watchers[&watcher_id].clone();
        let generation = state.roots[&reference.root].generation;
        (reference.root, generation)
    };
    let remaining = root.join("still-locked");
    assert!(
        manager
            .apply_root_coverage(
                &key,
                generation,
                CoverageChange {
                    unwatched: BTreeSet::from([remaining]),
                    truncated: true,
                    ..Default::default()
                },
            )
            .await
    );
    manager.get_watcher_events(&watcher_id).await.unwrap();
    assert!(
        manager
            .apply_root_coverage(
                &key,
                generation,
                CoverageChange {
                    complete: true,
                    ..Default::default()
                },
            )
            .await
    );
    let events = manager
        .get_watcher_events(&watcher_id)
        .await
        .unwrap()
        .into_iter()
        .map(|event| (event.kind(), event.path))
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        [
            (FileEventKind::Rewatched, ".".to_string()),
            (FileEventKind::Unwatched, "still-locked".to_string()),
        ]
    );

    manager.shutdown().await;
}

// A directory listed but not searchable is reported, and made searchable it is
// walked again. With more unreadable directories under it than one walk keeps,
// that walk can't tell whether it ended, so it must stay where its next
// permission change finds it, or a caller is never told to rescan it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_skip_whose_walk_overflowed_is_rewatched_on_a_later_permission_change() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let crowded = root.join("crowded");
    let children = (0..MAX_SKIPPED_DIRECTORIES + 50)
        .map(|index| crowded.join(format!("{index:05}")))
        .collect::<Vec<_>>();
    for child in &children {
        std::fs::create_dir_all(child).unwrap();
        mode(child, 0o000);
    }
    mode(&crowded, 0o400);
    let manager = WatchManager::new(CancellationToken::new());
    // A consumer that selects files still gets every coverage event, and the
    // removals below don't fill its stream.
    let mut stream = manager
        .watch_stream(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![RelativeGlobPattern::parse("**/*.rs".to_string()).unwrap()],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let coverage_of = |kind: FileEventKind, path: &'static str| {
        move |event: &FileEvent| event.kind() == kind && event.path == path
    };
    assert!(
        next_event_matching(
            &mut stream,
            Duration::from_secs(10),
            coverage_of(FileEventKind::Unwatched, "crowded"),
        )
        .await
        .is_some(),
        "a directory whose entries can't be inspected is reported"
    );

    // Searchable now, but its walk meets more unreadable children than it
    // keeps, so the list is incomplete and `crowded` stays listed.
    mode(&crowded, 0o700);
    assert!(
        next_event_matching(
            &mut stream,
            Duration::from_secs(10),
            coverage_of(FileEventKind::Unwatched, "."),
        )
        .await
        .is_some(),
        "an overflowed walk reports an incomplete list"
    );

    // Drains what the watch reports for `for_how_long`, noting whether it
    // said `crowded` is watched again.
    let mut early_rewatch = false;
    let mut drain = async |stream: &mut WatchStream, for_how_long| {
        use futures_util::StreamExt;
        let _ = tokio::time::timeout(for_how_long, async {
            while let Some(event) = stream.next().await {
                let event = event.expect("watch keeps delivering events");
                early_rewatch |= coverage_of(FileEventKind::Rewatched, "crowded")(&event);
            }
        })
        .await;
    };
    // Let the scans that keep up with `crowded` finish: a removal during one
    // would walk it again.
    drain(&mut stream, Duration::from_secs(2)).await;
    // Removing the children walks nothing, so the next permission change on
    // `crowded` is the only cue to walk it again. Each removal is an event on
    // `crowded`, so they come in chunks the watch's raw-event buffer can
    // drain.
    for chunk in children.chunks(512) {
        for child in chunk {
            std::fs::remove_dir(child).unwrap();
        }
        drain(&mut stream, Duration::from_millis(100)).await;
    }
    drain(&mut stream, Duration::from_millis(500)).await;
    assert!(
        !early_rewatch,
        "nothing but a walk of `crowded` ends its skip"
    );
    mode(&crowded, 0o755);
    assert!(
        next_event_matching(
            &mut stream,
            Duration::from_secs(20),
            coverage_of(FileEventKind::Rewatched, "crowded"),
        )
        .await
        .is_some(),
        "the skip ends once a walk can tell"
    );

    manager.shutdown().await;
}

// The root's return asks for a rescan of the whole watch even when the walk
// that sees it can't keep every unreadable directory under it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_root_returning_over_more_skips_than_a_walk_keeps_asks_for_a_rescan() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let children = (0..MAX_SKIPPED_DIRECTORIES + 50)
        .map(|index| root.join(format!("{index:05}")))
        .collect::<Vec<_>>();
    for child in &children {
        std::fs::create_dir(child).unwrap();
        mode(child, 0o000);
    }
    let manager = WatchManager::new(CancellationToken::new());
    let mut stream = manager
        .watch_stream(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            false,
            vec![RelativeGlobPattern::parse("**/*.rs".to_string()).unwrap()],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let root_marker =
        |kind: FileEventKind| move |event: &FileEvent| event.kind() == kind && event.path == ".";
    // Past the cap on reported skips, the list is incomplete.
    assert!(
        next_event_matching(
            &mut stream,
            Duration::from_secs(10),
            root_marker(FileEventKind::Unwatched)
        )
        .await
        .is_some()
    );
    // The root turns unsearchable, then searchable again; the walk that sees
    // it return still meets more unreadable directories than it keeps.
    mode(&root, 0o600);
    tokio::time::sleep(Duration::from_millis(500)).await;
    mode(&root, 0o700);
    let rescan = next_event_matching(
        &mut stream,
        Duration::from_secs(20),
        root_marker(FileEventKind::Rewatched),
    )
    .await;
    manager.shutdown().await;
    for child in &children {
        mode(child, 0o700);
    }
    assert!(
        rescan.is_some(),
        "the root's return asks for a rescan of the whole watch"
    );
}

/// Polls the ledger until `done` accepts a reply, or fails after a while.
#[cfg(target_os = "linux")]
async fn ledger_until(
    manager: &WatchManager,
    done: impl Fn(&GetModifiedFilesResponse) -> bool,
) -> GetModifiedFilesResponse {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let response = modified_files(manager).await;
            if done(&response) {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the ledger reaches the expected coverage")
}

// A root that turns unreadable hides every skip under it, so its list is
// incomplete; once it's readable again and a walk of it lists every skip, the
// list is complete again.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_unwatched_list_is_complete_again_once_the_root_is_walked_cleanly() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let locked = root.join("locked");
    std::fs::create_dir(&locked).unwrap();
    mode(&locked, 0o000);
    let manager = WatchManager::new(CancellationToken::new());
    manager
        .enable_file_changes(
            vec![GuestPath::parse(root.display().to_string()).unwrap()],
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let listed = locked.display().to_string();
    ledger_until(&manager, |response| {
        response.unwatched_paths == [listed.clone()] && !response.unwatched_paths_truncated
    })
    .await;

    mode(&root, 0o600);
    ledger_until(&manager, |response| response.unwatched_paths_truncated).await;
    mode(&root, 0o700);
    let complete = ledger_until(&manager, |response| !response.unwatched_paths_truncated).await;
    assert_eq!(complete.unwatched_paths, [listed]);

    manager.shutdown().await;
    mode(&locked, 0o700);
}

// A walk of the returning root that can't keep every skip it meets hasn't
// listed them all, so the list stays incomplete.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_unwatched_list_stays_incomplete_when_the_root_walk_is_truncated() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let children = (0..MAX_SKIPPED_DIRECTORIES + 50)
        .map(|index| root.join(format!("{index:05}")))
        .collect::<Vec<_>>();
    for child in &children {
        std::fs::create_dir(child).unwrap();
        mode(child, 0o000);
    }
    let manager = WatchManager::new(CancellationToken::new());
    manager
        .enable_file_changes(
            vec![GuestPath::parse(root.display().to_string()).unwrap()],
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    ledger_until(&manager, |response| response.unwatched_paths_truncated).await;
    // A stream with the ledger's options shares its backend, so it hears the
    // root's return in the same step that updates the ledger's list.
    let mut stream = manager
        .watch_stream(
            GuestPath::parse(root.display().to_string()).unwrap(),
            true,
            true,
            vec![],
            vec![],
            Some(false),
        )
        .await
        .unwrap();
    let coverage_of =
        |kind: FileEventKind| move |event: &FileEvent| event.kind() == kind && event.path == ".";

    mode(&root, 0o600);
    tokio::time::sleep(Duration::from_millis(500)).await;
    mode(&root, 0o700);
    let returned = next_event_matching(
        &mut stream,
        Duration::from_secs(15),
        coverage_of(FileEventKind::Rewatched),
    )
    .await
    .is_some();
    // The restated list that follows says it's still incomplete.
    let restated_incomplete = next_event_matching(
        &mut stream,
        Duration::from_secs(5),
        coverage_of(FileEventKind::Unwatched),
    )
    .await
    .is_some();
    let response = modified_files(&manager).await;
    manager.shutdown().await;
    for child in &children {
        mode(child, 0o700);
    }
    assert!(returned, "the root's return is reported");
    assert!(restated_incomplete);
    assert!(response.unwatched_paths_truncated);
}
