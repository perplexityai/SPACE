use super::*;
#[cfg(target_os = "linux")]
use std::io;

#[tokio::test]
async fn sandbox_trust_install_is_opt_in_and_validates_before_filesystem_changes() {
    let shutdown = CancellationToken::new();
    let service = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            filesystem_execution: FilesystemExecutionMode::Direct,
            local_socket_path: None,
            bridge_socket_path: None,
            ..Default::default()
        },
    )
    .unwrap();
    let error = BridgeGuestMeta::install_sandbox_trust(
        &service,
        Request::new(InstallSandboxTrustRequest {
            sandbox_id: uuid::Uuid::now_v7().to_string(),
            certificate_der: Vec::new(),
        }),
    )
    .await
    .unwrap_err();
    shutdown.cancel();
    assert_eq!(
        error.code(),
        if cfg!(feature = "split-trust-prototype") {
            tonic::Code::InvalidArgument
        } else {
            tonic::Code::Unimplemented
        }
    );
}

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
