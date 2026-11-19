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
#[tokio::test(start_paused = true)]
async fn bridge_ping_reports_used_memory_without_depending_on_it() {
    let sources: [fn() -> Option<u64>; 5] = [
        || Some(512 << 20),
        || Some(0),
        || None,
        || panic!("memory reader failed"),
        || Some(1),
    ];
    for (source, (drain_diagnostics, expected)) in sources.into_iter().zip([
        (true, Some(512 << 20)),
        (true, Some(0)),
        (true, None),
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

#[tokio::test(start_paused = true)]
async fn memory_read_timeout_or_cancellation_keeps_admission_until_worker_finishes() {
    for cancel in [false, true] {
        let slots = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let outcome = {
            let read = read_guest_memory(slots.clone(), move || {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
                Some(42)
            });
            tokio::pin!(read);
            tokio::select! {
                _ = started_rx => {}
                _ = &mut read => panic!("reader returned before its release"),
            }
            if cancel {
                None
            } else {
                tokio::time::advance(GUEST_MEMORY_READ_TIMEOUT).await;
                Some(futures_util::poll!(&mut read))
            }
        };
        let retry = read_guest_memory(slots.clone(), || Some(0)).await;
        // Release before asserting so a regression cannot strand the worker.
        release_tx.send(()).unwrap();
        drop(slots.acquire().await.unwrap());
        let fresh = read_guest_memory(slots, || Some(7)).await;

        if let Some(outcome) = outcome {
            assert_eq!(outcome, std::task::Poll::Ready(None));
        }
        assert_eq!(retry, None, "an outstanding read keeps admission");
        assert_eq!(fresh, Some(7), "a completed read releases admission");
    }
}
