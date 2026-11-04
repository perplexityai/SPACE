use std::path::Path;
use std::time::Duration;

use pplx_space_common_core::types::RelativeGlobPattern;

use super::super::paths::consumer_accepts_event;
use super::*;

const GENERATION: WatchRootGeneration = WatchRootGeneration(1);

fn glob(value: &str) -> RelativeGlobPattern {
    RelativeGlobPattern::parse(value).expect("test glob is valid")
}

fn key(path: &str, recursive: bool, include_metadata_events: bool) -> WatchRootKey {
    WatchRootKey {
        root: PathBuf::from(path),
        recursive,
        include_metadata_events,
        filter_config: WatchFilterConfig::new(Vec::new(), Vec::new(), false)
            .expect("filter config"),
    }
}

#[test]
fn include_globs_do_not_split_backend_roots() {
    let first = WatchFilterConfig::new(vec![glob("**/*.rs")], Vec::new(), false)
        .expect("filter config")
        .backend_config();
    let second = WatchFilterConfig::new(vec![glob("**/*.txt")], Vec::new(), false)
        .expect("filter config")
        .backend_config();

    assert_eq!(first, second);
}

#[test]
fn non_recursive_consumer_only_matches_direct_children() {
    let root = Path::new("/tmp/root");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    assert!(consumer_accepts_event(
        "/tmp/root/file.txt",
        root,
        false,
        &filter,
        true
    ));
    assert!(!consumer_accepts_event(
        "/tmp/root/subdir/file.txt",
        root,
        false,
        &filter,
        true
    ));
    assert!(consumer_accepts_event(
        "/tmp/root/subdir/file.txt",
        root,
        true,
        &filter,
        true
    ));
    assert!(!consumer_accepts_event(
        "/tmp/other/file.txt",
        root,
        true,
        &filter,
        true
    ));
}

#[test]
fn expired_pull_watcher_reaper_removes_consumer_and_root() {
    let root_key = key("/tmp/root", true, true);
    let shutdown = CancellationToken::new();
    let consumer_id = 7;
    let mut consumers = HashMap::new();
    consumers.insert(
        consumer_id,
        WatchConsumer::Pull {
            retained_bytes: 0,
            root: root_key.root.clone(),
            recursive: true,
            events: VecDeque::new(),
            overflowed: false,
            last_access: Instant::now() - PULL_WATCHER_TTL - Duration::from_secs(1),
            filter: WatchFilter::compile(root_key.filter_config.clone()).expect("filter"),
        },
    );
    let mut state = WatchManagerState::default();
    state.roots.insert(
        root_key.clone(),
        WatchRoot {
            generation: GENERATION,
            consumers,
            directory_count: 1,
            discovering: false,
            shutdown: shutdown.clone(),
        },
    );
    state.pull_watchers.insert(
        "w1".into(),
        WatchConsumerRef {
            root: root_key.clone(),
            consumer_id,
        },
    );

    let root_shutdowns = reap_expired_pull_watchers_locked(&mut state);

    assert!(state.pull_watchers.is_empty());
    assert!(!state.roots.contains_key(&root_key));
    assert_eq!(root_shutdowns.len(), 1);
}

#[test]
fn dynamic_directories_consume_the_shared_watch_budget() {
    let root_key = key("/tmp/root", true, true);
    let mut state = WatchManagerState::default();
    state.roots.insert(
        root_key.clone(),
        WatchRoot {
            generation: GENERATION,
            consumers: HashMap::new(),
            directory_count: MAX_WATCHED_DIRECTORIES - 1,
            discovering: false,
            shutdown: CancellationToken::new(),
        },
    );

    assert_eq!(
        adjust_directories_locked(&mut state, &root_key, GENERATION, 1, 0),
        DirectoryAdjustment::Applied
    );
    assert_eq!(
        adjust_directories_locked(&mut state, &root_key, GENERATION, 1, 0),
        DirectoryAdjustment::BudgetExceeded
    );
    assert_eq!(
        state.roots[&root_key].directory_count,
        MAX_WATCHED_DIRECTORIES
    );

    assert_eq!(
        adjust_directories_locked(&mut state, &root_key, GENERATION, 1, 1),
        DirectoryAdjustment::Applied
    );
    assert_eq!(
        state.roots[&root_key].directory_count,
        MAX_WATCHED_DIRECTORIES
    );

    assert_eq!(
        adjust_directories_locked(&mut state, &root_key, GENERATION, 0, 1),
        DirectoryAdjustment::Applied
    );

    assert_eq!(
        state.roots[&root_key].directory_count,
        MAX_WATCHED_DIRECTORIES - 1
    );
    assert_eq!(
        adjust_directories_locked(&mut state, &root_key, GENERATION, 1, 0),
        DirectoryAdjustment::Applied
    );
}

#[test]
fn stale_backend_generation_cannot_mutate_or_remove_recreated_root() {
    let root_key = key("/tmp/root", true, true);
    let stale_generation = WatchRootGeneration(1);
    let current_generation = WatchRootGeneration(2);
    let mut state = WatchManagerState::default();
    state.roots.insert(
        root_key.clone(),
        WatchRoot {
            generation: current_generation,
            consumers: HashMap::new(),
            directory_count: 7,
            discovering: false,
            shutdown: CancellationToken::new(),
        },
    );

    assert_eq!(
        adjust_directories_locked(&mut state, &root_key, stale_generation, 3, 2),
        DirectoryAdjustment::RootGone
    );
    assert!(root_for_backend_mut(&mut state, &root_key, stale_generation).is_none());
    assert!(remove_root_for_backend(&mut state, &root_key, stale_generation).is_none());
    assert_eq!(state.roots[&root_key].directory_count, 7);
    assert_eq!(state.roots[&root_key].generation, current_generation);
}
