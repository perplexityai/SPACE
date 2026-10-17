use super::*;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn lossy_names_keep_distinct_file_identity_and_classify_hard_links() {
    use std::os::unix::ffi::OsStringExt;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let source = root.join("source");
    std::fs::write(&source, b"before").unwrap();
    let names = [vec![0xff], vec![0xfe], "\u{fffd}".as_bytes().to_vec()];
    let mut ledger = Ledger::new(1, 1_000);
    for name in names {
        let relative = PathBuf::from(std::ffi::OsString::from_vec(name));
        let path = root.join(&relative);
        std::fs::hard_link(&source, &path).unwrap();
        std::fs::write(&path, b"after").unwrap();
        let event = WatchEvent::new(path, relative, FileEventKind::Modified, 1_100);
        let is_file = super::super::classify_ledger_event_regular_file(&event).await;
        assert!(is_file, "stat must use the raw path, not its display name");
        ledger.record_event(root, &event, is_file);
    }
    let response = modified_files_since(&mut ledger, 1_000);
    assert_eq!(response.files.len(), 3);
    assert!(response.files.iter().all(|file| file.path == "\u{fffd}"));
    assert!(
        response.overflowed,
        "lossy display names are not exact inventory identifiers"
    );
    assert_eq!(std::fs::read(source).unwrap(), b"after");

    let relative = PathBuf::from(std::ffi::OsString::from_vec(vec![0xff]));
    ledger.record_event(
        root,
        &WatchEvent::new(
            root.join(&relative),
            relative,
            FileEventKind::Removed,
            1_200,
        ),
        false,
    );
    assert_eq!(modified_files_since(&mut ledger, 1_000).files.len(), 2);
}

fn file_event(path: &str, kind: FileEventKind, observed_unix_ms: u64) -> WatchEvent {
    let relative = path.strip_prefix('/').unwrap_or(path);
    WatchEvent::new(
        PathBuf::from(path),
        PathBuf::from(relative),
        kind,
        observed_unix_ms,
    )
}

fn modified_files_since(ledger: &mut Ledger, since_unix_ms: u64) -> GetModifiedFilesResponse {
    ledger
        .modified_files_response(GetModifiedFilesRequest {
            since: Some(unix_ms_to_timestamp(since_unix_ms)),
            ..Default::default()
        })
        .expect("modified files")
}

#[test]
fn long_paths_overflow_before_the_event_count_limit() {
    let mut ledger = Ledger::new(1, 1_000);
    for index in 0..400 {
        let path = format!("/{}-{index}", "x".repeat(3900));
        ledger.record_event(
            Path::new("/"),
            &file_event(&path, FileEventKind::Created, 1_100),
            true,
        );
    }
    let response = modified_files_since(&mut ledger, 1_100);
    assert!(
        response.overflowed,
        "long paths must exhaust the byte budget before 16k events"
    );
    assert!(response.files.len() < 400);
    ledger.record_event(
        Path::new("/"),
        &file_event("/recent", FileEventKind::Created, 1_200),
        true,
    );
    assert!(
        modified_files_since(&mut ledger, 1_100)
            .files
            .iter()
            .any(|file| file.path == "recent")
    );
    for file in modified_files_since(&mut ledger, 1_100).files {
        ledger.record_event(
            Path::new("/"),
            &file_event(&format!("/{}", file.path), FileEventKind::Removed, 1_300),
            false,
        );
    }
    assert_eq!(
        ledger.file_bytes, 0,
        "eviction/removal must refund all file storage"
    );
}

#[test]
fn removed_file_disappears_from_modified_snapshot() {
    let root = Path::new("/root");
    let path = "/root/file.txt";
    let mut ledger = Ledger::new(1, 1_000);

    for kind in [
        FileEventKind::Created,
        FileEventKind::Modified,
        FileEventKind::Renamed,
        FileEventKind::Chmod,
        FileEventKind::Modified,
    ] {
        ledger.record_event(root, &file_event(path, kind, 1_100), true);
    }
    let response = modified_files_since(&mut ledger, 1_000);
    assert_eq!(response.files.len(), 1);

    ledger.record_event(
        root,
        &file_event(path, FileEventKind::Removed, 1_200),
        false,
    );
    let response = modified_files_since(&mut ledger, 1_000);
    assert!(response.files.is_empty());
    assert_eq!(
        ledger.file_bytes, 0,
        "removal must refund kind allocation too"
    );
}

#[test]
fn non_regular_paths_are_not_reported_as_modified_files() {
    let mut ledger = Ledger::new(1, 1_000);
    ledger.record_event(
        Path::new("/root"),
        &file_event("/root/dir", FileEventKind::Created, 1_100),
        false,
    );
    let response = modified_files_since(&mut ledger, 1_000);

    assert!(response.files.is_empty());
}

#[test]
fn same_relative_path_under_different_roots_does_not_collide() {
    let mut ledger = Ledger::new(1, 1_000);
    for (root, observed) in [("/home", 1_100), ("/tmp", 1_200)] {
        ledger.record_event(
            Path::new(root),
            &file_event(
                &format!("{root}/test.txt"),
                FileEventKind::Modified,
                observed,
            ),
            true,
        );
    }
    let response = modified_files_since(&mut ledger, 1_000);
    assert_eq!(
        response
            .files
            .iter()
            .map(|file| (file.root.as_str(), file.path.as_str()))
            .collect::<Vec<_>>(),
        vec![("/home", "test.txt"), ("/tmp", "test.txt")]
    );
}

#[test]
fn modified_file_filters_reject_conflicts_and_invalid_time_values() {
    let mut ledger = Ledger::new(1, 1_000);
    let error = ledger
        .modified_files_response(GetModifiedFilesRequest {
            window: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            since: Some(unix_ms_to_timestamp(1_000)),
            cursor: None,
            user: None,
        })
        .expect_err("conflicting filters should fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    for (seconds, nanos) in [(i64::MAX, 1_000_000), (-1, 0), (0, -1), (0, 1_000_000_000)] {
        for window in [false, true] {
            let request = GetModifiedFilesRequest {
                window: window.then_some(prost_types::Duration { seconds, nanos }),
                since: (!window).then_some(prost_types::Timestamp { seconds, nanos }),
                ..Default::default()
            };
            assert_eq!(
                ledger
                    .modified_files_response(request)
                    .expect_err("invalid time")
                    .code(),
                tonic::Code::InvalidArgument,
            );
        }
    }
}

#[test]
fn expiring_all_observations_advances_the_floor_and_releases_files() {
    let mut ledger = Ledger::new(1, 1_000);
    ledger.record_event(
        Path::new("/"),
        &file_event("/old", FileEventKind::Created, 1_100),
        true,
    );
    for event in &mut ledger.events {
        event.observed_at = Instant::now() - LEDGER_RETENTION - Duration::from_secs(1);
    }
    let response = modified_files_since(&mut ledger, 1_000);
    assert!(
        response.files.is_empty(),
        "expired observations must release indexed files"
    );
    assert!(
        response.overflowed,
        "history loss must not reset to the original observation floor"
    );
    assert_eq!(ledger.file_bytes, 0);
}

#[test]
fn backwards_wall_observations_preserve_the_loss_fence() {
    for expire_previous in [false, true] {
        let mut ledger = Ledger::new(1, 1_000);
        let root = Path::new("/root");
        ledger.record_event(
            root,
            &file_event("/root/first", FileEventKind::Modified, 2_000),
            true,
        );
        let cursor = if expire_previous {
            ledger.discard_oldest_observation();
            2_001
        } else {
            2_000
        };
        ledger.record_event(
            root,
            &file_event("/root/changed", FileEventKind::Modified, 1_900),
            true,
        );
        let response = modified_files_since(&mut ledger, cursor);
        assert!(
            response.overflowed,
            "regressed observation must invalidate the previous cursor and retention floor"
        );
        assert!(
            response
                .cursor
                .expect("cursor")
                .parse::<u64>()
                .expect("time")
                >= cursor
        );
    }
}
