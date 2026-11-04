use pplx_space_common_core::types::RelativeGlobPattern;

use super::super::filter::{WatchFilter, WatchFilterConfig};
use super::*;

fn glob(value: &str) -> RelativeGlobPattern {
    RelativeGlobPattern::parse(value).expect("test glob is valid")
}

#[tokio::test]
async fn registration_plans_bound_nonrecursive_and_filtered_walks() {
    let directory = tempfile::tempdir().expect("directory");
    for path in ["keep/nested", "target/nested"] {
        std::fs::create_dir_all(directory.path().join(path)).expect("tree");
    }
    for (recursive, excluded, count) in [(false, false, 1), (true, false, 5), (true, true, 3)] {
        let excludes = if excluded {
            vec![glob("target"), glob("target/**")]
        } else {
            vec![]
        };
        let filter =
            WatchFilter::compile(WatchFilterConfig::new(vec![], excludes, false).expect("config"))
                .expect("filter");
        let plan = plan_watch_registration(
            directory.path(),
            recursive,
            &filter,
            &new_watch_directory_scan_slots(),
            &tokio_util::task::TaskTracker::new(),
        )
        .await
        .expect("plan");
        assert_eq!(plan.len(), count);
        assert_eq!(plan.root, directory.path());
        assert_eq!(
            plan.descendants.contains(&directory.path().join("target")),
            recursive && !excluded
        );
    }
}

#[test]
fn consumer_include_globs_still_apply_after_backend_filter() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(vec![glob("**/*.rs")], Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    assert!(consumer_accepts_event(
        temp_dir.path().join("src/lib.rs"),
        temp_dir.path(),
        true,
        &filter,
        true,
    ));
    assert!(!consumer_accepts_event(
        temp_dir.path().join("src/lib.txt"),
        temp_dir.path(),
        true,
        &filter,
        true,
    ));
}

#[test]
fn directory_walk_treats_a_disappeared_start_as_an_empty_tree() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let removed = temp_dir.path().join("removed");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    let directories = collect_watch_directories(temp_dir.path(), &removed, &filter)
        .expect("a disappeared candidate is an ordinary traversal race")
        .directories;

    assert!(directories.is_empty());
}

#[test]
fn directory_walk_rejects_a_wide_frontier_and_preserves_filtered_capacity() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    for index in 0..24 {
        std::fs::create_dir(temp_dir.path().join(format!("child-{index}"))).expect("child");
    }
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), false).expect("filter config"),
    )
    .expect("filter");
    assert_eq!(
        collect_watch_directories_with_limit(temp_dir.path(), temp_dir.path(), &filter, 8)
            .expect_err("wide traversal must reject before retaining the entire frontier")
            .code(),
        tonic::Code::ResourceExhausted,
    );

    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), vec![glob("child-*")], false).expect("filter config"),
    )
    .expect("filter");
    assert_eq!(
        collect_watch_directories_with_limit(temp_dir.path(), temp_dir.path(), &filter, 1)
            .expect("excluded directories consume no traversal capacity")
            .directories,
        vec![temp_dir.path().to_path_buf()],
    );
}

#[cfg(unix)]
#[test]
fn unreadable_directories_take_no_directory_budget() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let root = temp_dir.path();
    std::fs::create_dir(root.join("readable")).expect("readable child");
    let locked = root.join("locked");
    std::fs::create_dir(&locked).expect("locked child");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    let walk = collect_watch_directories_with_limit(root, root, &filter, 2)
        .expect("the watchable tree fits the budget");
    assert_eq!(walk.skipped, vec![locked.clone()]);
    let mut directories = walk.directories;

    directories.sort();
    assert_eq!(directories, vec![root.to_path_buf(), root.join("readable")]);
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).expect("chmod");
}

#[cfg(unix)]
#[test]
fn a_refused_lookup_remembers_the_ancestor_that_blocks_it() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused a lookup") {
        return;
    }
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let root = temp_dir.path();
    let blocker = root.join("a");
    let start = blocker.join("b/c");
    std::fs::create_dir_all(&start).expect("tree");
    // Readable but not searchable: nothing below `a` can be looked up, so
    // `a/b` is no better a place to wait for a permission change than `c`.
    std::fs::set_permissions(&blocker, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    let walk = collect_watch_directories(root, &start, &filter);
    std::fs::set_permissions(&blocker, std::fs::Permissions::from_mode(0o700)).expect("chmod");

    let walk = walk.expect("a refused descendant is skipped");
    assert!(walk.directories.is_empty());
    assert_eq!(walk.skipped, vec![blocker]);
}

#[cfg(unix)]
#[test]
fn a_walk_reports_a_root_it_cannot_list_only_when_asked() {
    use std::os::unix::fs::PermissionsExt;
    if !pplx_space_test_env::unprivileged_or_skip("be refused an unreadable directory") {
        return;
    }
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let root = temp_dir.path().join("root");
    std::fs::create_dir(&root).expect("root");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o300)).expect("chmod");
    let reported = collect_watch_directories_or_root_refusal(&root, &root, &filter);
    let failed = collect_watch_directories(&root, &root, &filter);
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).expect("chmod");

    // Discovery keeps the watch and waits for the root's next change...
    assert!(matches!(reported, Ok(None)));
    // ...while setting a watch up refuses the root.
    assert_eq!(
        failed.expect_err("an unreadable root fails").code(),
        tonic::Code::PermissionDenied
    );
    let walk = collect_watch_directories_or_root_refusal(&root, &root, &filter)
        .expect("a readable root walks")
        .expect("a readable root is walked");
    assert_eq!(walk.directories, vec![root]);
}

#[tokio::test]
async fn required_root_cannot_disappear_or_be_filtered_out_of_a_plan() {
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().join("root");
    for replacement in [None, Some(b"file".as_slice())] {
        std::fs::create_dir(&root).expect("root");
        let normalized = normalize_watch_root(&GuestPath::parse(root.to_str().unwrap()).unwrap())
            .await
            .unwrap();
        std::fs::remove_dir(&root).unwrap();
        if let Some(contents) = replacement {
            std::fs::write(&root, contents).unwrap();
        }
        let filter =
            WatchFilter::compile(WatchFilterConfig::new(vec![], vec![], false).unwrap()).unwrap();
        assert!(
            plan_watch_registration(
                &normalized,
                true,
                &filter,
                &new_watch_directory_scan_slots(),
                &tokio_util::task::TaskTracker::new()
            )
            .await
            .is_err(),
            "a vanished mandatory root must fail setup"
        );
        if replacement.is_some() {
            std::fs::remove_file(&root).unwrap();
        }
    }
    std::fs::create_dir(&root).unwrap();
    let filter =
        WatchFilter::compile(WatchFilterConfig::new(vec![], vec![glob(".")], false).unwrap())
            .unwrap();
    let plan = plan_watch_registration(
        &root,
        true,
        &filter,
        &new_watch_directory_scan_slots(),
        &tokio_util::task::TaskTracker::new(),
    )
    .await
    .unwrap();
    assert_eq!(plan.len(), 1, "filters must retain root lifecycle coverage");

    // A glob that excludes only the root must not hide existing children.
    std::fs::create_dir_all(root.join("existing/nested")).unwrap();
    let plan = plan_watch_registration(
        &root,
        true,
        &filter,
        &new_watch_directory_scan_slots(),
        &tokio_util::task::TaskTracker::new(),
    )
    .await
    .unwrap();
    assert_eq!(plan.root, root);
    let mut descendants = plan.descendants;
    descendants.sort();
    assert_eq!(
        descendants,
        vec![root.join("existing"), root.join("existing/nested")]
    );
}

#[tokio::test]
async fn default_roots_require_every_selected_directory() {
    let temp = tempfile::tempdir().unwrap();
    let workdir = temp.path().join("workspace");
    let temp_root = temp.path().to_str().unwrap();
    for (value, expected) in [
        (workdir.to_str().unwrap(), tonic::Code::NotFound),
        ("", tonic::Code::FailedPrecondition),
    ] {
        assert_eq!(
            default_ledger_roots_for(value, temp_root)
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    std::fs::write(&workdir, b"not a directory").unwrap();
    assert_eq!(
        default_ledger_roots_for(workdir.to_str().unwrap(), temp_root)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    std::fs::remove_file(&workdir).unwrap();
    std::fs::create_dir(&workdir).unwrap();
    let roots = default_ledger_roots_for(workdir.to_str().unwrap(), temp_root)
        .await
        .unwrap();
    assert_eq!(roots.len(), 2);
    assert!(roots.contains(&std::fs::canonicalize(&workdir).unwrap()));
    assert_eq!(
        default_ledger_roots_for(temp_root, temp_root)
            .await
            .unwrap()
            .len(),
        1
    );
    let error = default_ledger_roots_for(
        workdir.to_str().unwrap(),
        temp.path().join("missing-tmp").to_str().unwrap(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
}
