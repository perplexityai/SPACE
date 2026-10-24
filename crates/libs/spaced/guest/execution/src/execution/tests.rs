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
        Drain,
    }
    for close in [Close::Explicit, Close::CallerCancellation, Close::Drain] {
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
            Close::Drain => service.drain().await,
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
        service.drain().await;
        assert_eq!(
            shutdown.is_cancelled(),
            caller_cancelled,
            "draining a closed service must not cancel the caller's token"
        );
    }
}
