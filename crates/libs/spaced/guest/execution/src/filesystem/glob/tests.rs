use std::os::unix::fs::symlink;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use super::{GlobQuery, glob};

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    for directory in [
        "nested/deep",
        "node_modules/pkg",
        "a/node_modules",
        ".cache",
    ] {
        std::fs::create_dir_all(path.join(directory)).unwrap();
    }
    for file in [
        "root.py",
        ".hidden.py",
        "nested/child.py",
        "nested/deep/x.py",
        "node_modules/pkg/i.py",
        "a/node_modules/j.py",
        "a/b.py",
        "a.py",
        "lit.py",
        "lit.txt",
        "lit.{py,txt}",
        ".cache/c.py",
    ] {
        std::fs::write(path.join(file), "abc").unwrap();
    }
    symlink("root.py", path.join("link.py")).unwrap();
    symlink("nested", path.join("linkdir")).unwrap();
    symlink("..", path.join("nested/deep/back")).unwrap();
    symlink("missing", path.join("dangling.py")).unwrap();
    nix::unistd::mkfifo(&path.join("fifo.py"), nix::sys::stat::Mode::S_IRWXU).unwrap();
    root
}

fn query(root: &Path, pattern: &str, skip_dirs: &[&str], max_results: usize) -> GlobQuery {
    GlobQuery {
        root: root.to_path_buf(),
        pattern: pattern.to_owned(),
        skip_dirs: skip_dirs
            .iter()
            .map(|directory| (*directory).to_owned())
            .collect(),
        max_results,
        deadline: Instant::now() + Duration::from_secs(30),
    }
}

fn paths(root: &Path, pattern: &str, skip_dirs: &[&str]) -> Vec<String> {
    glob(query(root, pattern, skip_dirs, 0))
        .unwrap()
        .matches
        .into_iter()
        .map(|entry| entry.path)
        .collect()
}

#[test]
fn recursive_glob_follows_links_includes_hidden_and_sorts_by_component() {
    let root = fixture();
    assert_eq!(
        paths(root.path(), "**/*.py", &[]),
        [
            ".cache/c.py",
            ".hidden.py",
            "a/b.py",
            "a/node_modules/j.py",
            "a.py",
            "link.py",
            "linkdir/child.py",
            "linkdir/deep/x.py",
            "lit.py",
            "nested/child.py",
            "nested/deep/x.py",
            "node_modules/pkg/i.py",
            "root.py",
        ]
    );
}

#[test]
fn skip_dirs_exclude_contents_at_any_depth() {
    let root = fixture();
    assert_eq!(
        paths(root.path(), "**/*.py", &["node_modules", ".cache"]),
        [
            ".hidden.py",
            "a/b.py",
            "a.py",
            "link.py",
            "linkdir/child.py",
            "linkdir/deep/x.py",
            "lit.py",
            "nested/child.py",
            "nested/deep/x.py",
            "root.py",
        ]
    );
}

#[test]
fn patterns_are_anchored_and_single_stars_stay_in_one_directory() {
    let root = fixture();
    assert_eq!(
        paths(root.path(), "*.py", &[]),
        [".hidden.py", "a.py", "link.py", "lit.py", "root.py"]
    );
    assert_eq!(
        paths(root.path(), "./nested/*.py", &[]),
        ["nested/child.py"]
    );
    assert_eq!(paths(root.path(), "/nested/*.py", &[]), ["nested/child.py"]);
    assert_eq!(
        paths(root.path(), "nested/?????.py", &[]),
        ["nested/child.py"]
    );
    assert_eq!(paths(root.path(), "[ar]*.py", &[]), ["a.py", "root.py"]);
}

#[test]
fn braces_are_literal_and_directory_only_patterns_match_no_files() {
    let root = fixture();
    assert_eq!(paths(root.path(), "lit.{py,txt}", &[]), ["lit.{py,txt}"]);
    assert!(paths(root.path(), "nested/", &[]).is_empty());
}

#[test]
fn reaching_the_limit_reports_truncation() {
    let root = fixture();
    let response = glob(query(root.path(), "*.py", &[], 2)).unwrap();
    let matched: Vec<_> = response
        .matches
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(matched, [".hidden.py", "a.py"]);
    assert!(response.truncated);
    assert!(!glob(query(root.path(), "*.py", &[], 0)).unwrap().truncated);
}

#[test]
fn matches_carry_size_and_modification_time() {
    let root = fixture();
    let response = glob(query(root.path(), "root.py", &[], 0)).unwrap();
    let [entry] = response.matches.as_slice() else {
        panic!("expected one match: {response:?}");
    };
    assert_eq!(entry.size_bytes, 3);
    assert!(entry.modified_at.is_some_and(|time| time.seconds > 0));
}

#[test]
fn missing_or_file_roots_have_no_matches_and_bad_globs_are_rejected() {
    let root = fixture();
    let missing = glob(query(&root.path().join("absent"), "**", &[], 0)).unwrap();
    assert!(missing.matches.is_empty() && !missing.truncated);
    assert!(
        glob(query(&root.path().join("root.py"), "**", &[], 0))
            .unwrap()
            .matches
            .is_empty()
    );
    let error = glob(query(root.path(), "[", &[], 0)).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn an_expired_walk_stops_as_truncated() {
    let root = tempfile::tempdir().unwrap();
    for index in 0..2048 {
        std::fs::write(root.path().join(format!("{index:04}.txt")), "").unwrap();
    }
    let mut expired = query(root.path(), "*.txt", &[], 0);
    expired.deadline = Instant::now();
    let response = glob(expired).unwrap();
    assert!(response.truncated);
    assert!(response.matches.len() < 2048);
}

/// Cross-checks against ripgrep where one is installed; the assertions above
/// pin the same behavior without it.
#[test]
fn agrees_with_ripgrep_when_available() {
    let Ok(version) = Command::new("rg").arg("--version").output() else {
        return;
    };
    if !version.status.success() {
        return;
    }
    let root = fixture();
    for (pattern, skip_dirs) in [
        ("**/*.py", &[][..]),
        ("**/*.py", &["node_modules"][..]),
        ("*.py", &[][..]),
        ("**", &[".cache", "deep"][..]),
        ("nested/**", &[][..]),
        ("lit.{py,txt}", &[][..]),
        ("**/deep/*", &[][..]),
    ] {
        let mut command = Command::new("rg");
        command.current_dir(root.path()).args([
            "--no-config",
            "--files",
            "--follow",
            "--null",
            "--sort",
            "path",
            "--hidden",
            "--no-ignore",
            "--no-messages",
            "--glob",
        ]);
        command.arg(format!(
            "/{}",
            pattern.replace('{', "\\{").replace('}', "\\}")
        ));
        for directory in skip_dirs {
            command.args(["--glob", &format!("!**/{directory}/**")]);
        }
        let output = command.output().unwrap();
        let expected: Vec<String> = String::from_utf8(output.stdout)
            .unwrap()
            .split('\0')
            .filter(|path| !path.is_empty())
            .filter(|path| {
                std::fs::metadata(root.path().join(path)).is_ok_and(|metadata| metadata.is_file())
            })
            .map(str::to_owned)
            .collect();
        assert_eq!(
            paths(root.path(), pattern, skip_dirs),
            expected,
            "{pattern} {skip_dirs:?}"
        );
    }
}
