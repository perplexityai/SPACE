use tempfile::tempdir;

use super::*;

fn glob(value: &str) -> RelativeGlobPattern {
    RelativeGlobPattern::parse(value).expect("test glob is valid")
}

#[test]
fn include_globs_filter_relative_paths() {
    let root = tempdir().expect("temp dir");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(vec![glob("**/*.rs")], Vec::new(), false).expect("filter config"),
    )
    .expect("filter");

    assert!(filter.matches_path(root.path(), &root.path().join("src/lib.rs")));
    assert!(!filter.matches_path(root.path(), &root.path().join("src/lib.txt")));
}

#[test]
fn exclude_globs_win_over_include_globs() {
    let root = tempdir().expect("temp dir");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(vec![glob("**/*.rs")], vec![glob("target/**")], false)
            .expect("filter config"),
    )
    .expect("filter");

    assert!(!filter.matches_path(root.path(), &root.path().join("target/build.rs")));
}

#[test]
fn default_excludes_reject_cache_directories() {
    let root = tempdir().expect("temp dir");
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), true).expect("filter config"),
    )
    .expect("filter");

    assert!(!filter.matches_path(root.path(), &root.path().join("pkg/__pycache__/x.pyc")));
    assert!(filter.matches_path(root.path(), &root.path().join("pkg/main.py")));
}

#[test]
fn browser_scratch_defaults_are_scoped_to_tmp() {
    let filter = WatchFilter::compile(
        WatchFilterConfig::new(Vec::new(), Vec::new(), true).expect("filter config"),
    )
    .expect("filter");

    assert!(!filter.matches_path(Path::new("/tmp"), Path::new("/tmp/playwright/browser.log")));
    assert!(filter.matches_path(
        Path::new("/home/user"),
        Path::new("/home/user/playwright/test.ts")
    ));
}

#[test]
fn absolute_globs_are_rejected() {
    let error = parse_watch_globs(vec!["/tmp/**".into()], "include_globs")
        .expect_err("absolute glob should fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}
