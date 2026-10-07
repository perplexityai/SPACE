use super::*;
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
