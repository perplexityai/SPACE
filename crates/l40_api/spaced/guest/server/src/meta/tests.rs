use super::*;
#[cfg(target_os = "linux")]
use std::io;

#[cfg(target_os = "linux")]
#[test]
fn flush_filesystem_helper_reports_syncfs_success() {
    flush_filesystem_at_path_with_syncfs("/", |_| Ok(())).expect("flush helper succeeds");
}

#[cfg(target_os = "linux")]
#[test]
fn flush_filesystem_helper_reports_syncfs_failure() {
    let error =
        flush_filesystem_at_path_with_syncfs("/", |_| Err(io::Error::from_raw_os_error(libc::EIO)))
            .expect_err("syncfs failure is returned");

    assert_eq!(error.raw_os_error(), Some(libc::EIO));
}

#[tokio::test]
async fn cancelled_flush_retains_execution_slot_until_worker_finishes() {
    let slots = Arc::new(Semaphore::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let flush = tokio::spawn(run_filesystem_flush(slots.clone(), move || {
        let _ = started_tx.send(());
        release_rx.recv().unwrap();
        Ok(())
    }));
    started_rx.await.unwrap();
    flush.abort();
    let _ = flush.await;
    let rejected = run_filesystem_flush(slots.clone(), || Ok(())).await;
    // Release even when the assertion fails so the runtime can shut down.
    release_tx.send(()).unwrap();
    assert_eq!(rejected.unwrap_err().code(), tonic::Code::ResourceExhausted);
    drop(
        tokio::time::timeout(Duration::from_secs(1), slots.acquire())
            .await
            .unwrap()
            .unwrap(),
    );
    run_filesystem_flush(slots, || Ok(()))
        .await
        .expect("settled flush releases admission");
}

/// Only diagnostic-draining Pings read memory; an unavailable reading does
/// not fail the health check.
/// The handler is called directly because a drain takes the process-wide
/// warning record, which another test in the same binary may be reading.
#[tokio::test]
async fn bridge_ping_reports_used_memory_without_depending_on_it() {
    let sources: [fn() -> Option<u64>; 4] = [
        || Some(512 << 20),
        || Some(0),
        || None,
        || panic!("non-draining Ping must not read memory"),
    ];
    for (source, (drain_diagnostics, expected)) in sources.into_iter().zip([
        (true, Some(512 << 20)),
        (true, Some(0)),
        (true, None),
        (false, None),
    ]) {
        let service = SpacedGuestService::with_config(
            CancellationToken::new(),
            SpacedGuestServiceConfig {
                host_connector_type: HostConnectorType::Uds(PathBuf::from("unused.sock")),
                local_socket_path: None,
                bridge_socket_path: None,
                guest_memory_used_source: source,
                ..Default::default()
            },
        )
        .expect("guest service");
        let response = BridgeGuestMeta::ping(
            &service,
            Request::new(BridgeGuestPingRequest { drain_diagnostics }),
        )
        .await
        .expect("ping succeeds")
        .into_inner();
        assert_eq!(
            response.memory_used_bytes, expected,
            "drain_diagnostics={drain_diagnostics}"
        );
    }
}
