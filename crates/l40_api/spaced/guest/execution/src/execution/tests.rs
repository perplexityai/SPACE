use super::*;

#[test]
fn identity_configuration_is_checked_before_construction() {
    for execution in [
        FilesystemExecutionMode::IdentityWorkers,
        FilesystemExecutionMode::Direct,
    ] {
        let result = ExecutionService::with_config(
            CancellationToken::new(),
            ExecutionServiceConfig {
                filesystem_execution: execution,
                identity_policy: IdentityPolicy::Restricted,
                ..Default::default()
            },
        );
        match execution {
            FilesystemExecutionMode::Direct => assert!(matches!(
                result,
                Err(CreateExecutionServiceError::RestrictedRequiresIdentityWorkers)
            )),
            FilesystemExecutionMode::IdentityWorkers => {
                #[cfg(target_os = "linux")]
                assert!(matches!(
                    result,
                    Err(CreateExecutionServiceError::MissingWorkerExecutable)
                ));
                #[cfg(not(target_os = "linux"))]
                assert!(matches!(
                    result,
                    Err(CreateExecutionServiceError::UnsupportedIdentityWorkers)
                ));
            }
        }
    }
}

#[tokio::test]
#[allow(
    deprecated,
    reason = "legacy filesystem RPCs must also reject new work"
)]
async fn closed_services_refuse_every_operation_through_existing_handles() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        self as protocol, filesystem_server::Filesystem,
    };

    enum Close {
        Explicit,
        CallerCancellation,
        Cancel,
        Drain,
    }
    for close in [
        Close::Explicit,
        Close::CallerCancellation,
        Close::Cancel,
        Close::Drain,
    ] {
        let shutdown = CancellationToken::new();
        let service = ExecutionService::with_config(
            shutdown.clone(),
            ExecutionServiceConfig {
                identity_policy: IdentityPolicy::Guest,
                filesystem_execution: FilesystemExecutionMode::Direct,
                ..Default::default()
            },
        )
        .unwrap();
        let environment =
            EnvMap::try_from(HashMap::from([("INITIAL".into(), "kept".into())])).unwrap();
        service
            .configure_process_env(environment.clone())
            .await
            .unwrap();
        let existing = service.clone();
        let files = service.filesystem_service();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("preserved");
        tokio::fs::write(&path, b"original").await.unwrap();
        match close {
            Close::Explicit => {
                service.close();
                service.close();
            }
            Close::CallerCancellation => shutdown.cancel(),
            Close::Cancel => service.cancel(),
            Close::Drain => service.drain().await.expect("complete drain"),
        }
        let caller_cancelled = matches!(close, Close::CallerCancellation);
        assert_eq!(
            shutdown.is_cancelled(),
            caller_cancelled,
            "service closure must not cancel the caller's token"
        );
        macro_rules! refused {
            ($($call:expr),+ $(,)?) => {$(
                let error = $call.await.err().expect(concat!(stringify!($call), " must refuse"));
                assert_eq!(error.code(), tonic::Code::Unavailable, "{}", stringify!($call));
                assert!(pplx_space_common_grpc::is_refused_before_start(&error));
            )+};
        }
        refused!(
            existing.start(Request::new(StartRequest::default())),
            existing.connect_process(Request::new(ConnectProcessRequest::default())),
            existing.list_processes(Request::new(ListProcessesRequest::default())),
            existing.send_signal(Request::new(SendSignalRequest::default())),
            existing.send_input(Request::new(SendInputRequest::default())),
            existing.close_stdin(Request::new(CloseStdinRequest::default())),
            existing.resize_pty(Request::new(ResizePtyRequest::default())),
            existing.configure_process_env(EnvMap::empty()),
            files.stat(Request::new(protocol::StatRequest::default())),
            files.list(Request::new(protocol::ListRequest::default())),
            files.stat_info(Request::new(protocol::StatRequest::default())),
            files.list_info(Request::new(protocol::ListRequest::default())),
            files.read(Request::new(protocol::ReadRequest::default())),
            files.write_batch(Request::new(protocol::WriteBatchRequest::default())),
            files.mkdir(Request::new(protocol::MkdirRequest::default())),
            files.remove(Request::new(protocol::RemoveRequest {
                path: path.display().to_string(),
                ..Default::default()
            })),
            files.rename(Request::new(protocol::RenameRequest::default())),
            files
                .normalize_watch_root(Request::new(protocol::NormalizeWatchRootRequest::default())),
            files.watch(Request::new(protocol::WatchRequest::default())),
            files.create_watcher(Request::new(protocol::CreateWatcherRequest::default())),
            files.get_watcher_events(Request::new(protocol::GetWatcherEventsRequest::default())),
            files.remove_watcher(Request::new(protocol::RemoveWatcherRequest::default())),
            files.enable_file_changes(Request::new(protocol::EnableFileChangesRequest::default())),
            files
                .disable_file_changes(Request::new(protocol::DisableFileChangesRequest::default())),
            files.get_modified_files(Request::new(protocol::GetModifiedFilesRequest::default())),
        );
        for through_handle in [false, true] {
            let mut polled = false;
            let input = futures_util::stream::poll_fn(|_| {
                polled = true;
                std::task::Poll::Ready(None)
            });
            if through_handle {
                refused!(files.write_file(Request::new(input)));
            } else {
                refused!(existing.write_file(Request::new(input)));
            }
            assert!(!polled, "closed uploads must not consume caller input");
        }
        assert_eq!(*service.inner.process_env.read().await, environment);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"original");
        service.drain().await.expect("complete drain");
        assert_eq!(
            shutdown.is_cancelled(),
            caller_cancelled,
            "draining a closed service must not cancel the caller's token"
        );
    }
}

#[tokio::test]
async fn close_allows_an_admitted_upload_to_finish() {
    use futures_util::StreamExt;
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteOpen, WriteRequest, write_request::Message,
    };
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .unwrap()
        .unwrap();
    let service = ExecutionService::with_config(
        CancellationToken::new(),
        ExecutionServiceConfig {
            filesystem_execution: FilesystemExecutionMode::Direct,
            identity_policy: IdentityPolicy::Guest,
            default_operation_user: GuestUserName::parse(user.name).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("graceful");
    let (finish, end) = tokio::sync::oneshot::channel::<()>();
    let input = futures_util::stream::iter([
        Ok(WriteRequest {
            message: Some(Message::Open(WriteOpen {
                path: path.display().to_string(),
                ..Default::default()
            })),
        }),
        Ok(WriteRequest {
            message: Some(Message::Data(b"complete".to_vec())),
        }),
    ])
    .chain(
        futures_util::stream::once(async {
            end.await.unwrap();
            None
        })
        .filter_map(std::future::ready),
    );
    let mut upload = Box::pin(service.write_file(Request::new(input)));
    assert!(futures_util::poll!(&mut upload).is_pending());
    service.close();
    assert!(
        futures_util::poll!(&mut upload).is_pending(),
        "close must not cancel admitted input"
    );
    finish.send(()).unwrap();
    upload.await.unwrap();
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"complete");
    service.drain().await.expect("complete drain");
}

// A FIFO makes the physical syscall outlive cancellation without depending on
// slow disks. Drain must retain the syscall and observe completion after release.
#[tokio::test]
async fn drain_retains_cancelled_direct_io_and_temporary_file_cleanup() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteBatchEntry, WriteBatchRequest, filesystem_server::Filesystem,
    };
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .unwrap()
        .unwrap();
    let service = ExecutionService::with_config(
        CancellationToken::new(),
        ExecutionServiceConfig {
            filesystem_execution: FilesystemExecutionMode::Direct,
            identity_policy: IdentityPolicy::Guest,
            default_operation_user: GuestUserName::parse(user.name).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("started");
    let fifo = directory.path().join("blocked");
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let mut reader = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&fifo)
        .unwrap();
    let files = service.filesystem_service();
    let request = Request::new(WriteBatchRequest {
        entries: vec![
            WriteBatchEntry {
                path: marker.display().to_string(),
                data: b"first effect".to_vec(),
                append: false,
            },
            WriteBatchEntry {
                path: fifo.display().to_string(),
                data: vec![b'x'; 1024 * 1024],
                append: true,
            },
        ],
        idempotency_key: Some("drain-blocked-io".into()),
        ..Default::default()
    });
    let caller = tokio::spawn(async move { files.write_batch(request).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match reader.read(&mut [0; 1]) {
                Ok(1) => break,
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                other => panic!("FIFO probe: {other:?}"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(
        service.drain_with_timeout(Duration::ZERO).await,
        Err(crate::IncompleteExecutionDrain)
    );
    // Consume the remaining bytes only after proving that drain cannot finish.
    let release = async {
        let mut buffer = [0; 64 * 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("FIFO release: {error}"),
            }
            tokio::task::yield_now().await;
        }
    };
    let (drained, ()) = tokio::join!(service.drain(), release);
    drained.expect("drain observes physical I/O and replacement cleanup");
    drop(reader);
    let mut names = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["blocked", "started"]);
    assert_eq!(tokio::fs::read(&marker).await.unwrap(), b"first effect");
}
