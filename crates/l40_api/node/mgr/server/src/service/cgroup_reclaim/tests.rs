use super::*;
use std::fs;

#[test]
fn reclaim_listed_reclaims_only_untracked_unpopulated() {
    let root = tempfile::tempdir().unwrap();

    // Tracked sandbox: skipped regardless of state.
    let tracked_dir = root.path().join("tracked");
    fs::create_dir(&tracked_dir).unwrap();

    // Untracked + unpopulated: reclaimed. (No cgroup.events file => the
    // kernel `populated` flag reads as absent/false; a regular fs would let
    // a stray control file block rmdir, unlike a real cgroupfs.)
    let leak_dir = root.path().join("leak");
    fs::create_dir(&leak_dir).unwrap();

    // Untracked + populated: must be left alone (safety).
    let live_dir = root.path().join("live");
    fs::create_dir(&live_dir).unwrap();
    fs::write(live_dir.join("cgroup.events"), "populated 1\n").unwrap();

    let tracked: HashSet<String> = ["tracked".to_string()].into_iter().collect();
    let dirs = vec![
        ("tracked".to_string(), tracked_dir.clone()),
        ("leak".to_string(), leak_dir.clone()),
        ("live".to_string(), live_dir.clone()),
    ];

    let stats = reclaim_listed(dirs, &tracked);

    assert_eq!(stats.reclaimed, 1);
    assert_eq!(stats.leaked, 1);
    assert_eq!(stats.failed, 0);
    assert!(!leak_dir.exists(), "leaked cgroup should be reclaimed");
    assert!(tracked_dir.exists(), "tracked cgroup must be untouched");
    assert!(
        live_dir.exists(),
        "untracked but populated cgroup must not be removed"
    );
}
