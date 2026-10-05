use crate::time::timestamp_from_millis;
use futures_util::StreamExt;
use pplx_space_spaced_common::proto::bridge_guest::filesystem_server::{
    Filesystem, FilesystemServer,
};
use pplx_space_spaced_common::proto::bridge_guest::meta_server::{Meta, MetaServer};
use pplx_space_spaced_common::proto::bridge_guest::process_server::{Process, ProcessServer};
use pplx_space_spaced_common::proto::bridge_guest::{
    CloseStdinRequest, CloseStdinResponse, ConfigureProcessEnvRequest, ConfigureProcessEnvResponse,
    ConnectProcessRequest, CreateWatcherRequest, CreateWatcherResponse, DisableFileChangesRequest,
    DisableFileChangesResponse, EnableFileChangesRequest, EnableFileChangesResponse, FileChunk,
    FileEvent, FileType, FlushFilesystemsRequest, FlushFilesystemsResponse,
    GetModifiedFilesRequest, GetModifiedFilesResponse, GetWatcherEventsRequest,
    GetWatcherEventsResponse, LegacyDirEntry, LegacyStatResponse, ListProcessesRequest,
    ListRequest, MkdirRequest, MkdirResponse, NormalizeWatchRootRequest,
    NormalizeWatchRootResponse, NotifyResumeRequest, NotifyResumeResponse, PingRequest,
    PingResponse, ProcessEvent, ProcessInfo, ReadRequest, RemoveRequest, RemoveResponse,
    RemoveWatcherRequest, RemoveWatcherResponse, RenameRequest, RenameResponse, ResizePtyRequest,
    ResizePtyResponse, SendInputRequest, SendInputResponse, SendSignalRequest, SendSignalResponse,
    StartRequest, StatRequest, WaitReadyRequest, WaitReadyResponse, WatchRequest,
    WriteBatchRequest, WriteBatchResponse, WriteRequest, WriteResponse,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(crate) struct MockBridgeGuest {
    pub(crate) version: &'static str,
    pub(crate) ready: bool,
    pub(crate) interactive_process_control_calls: Option<Arc<AtomicUsize>>,
    pub(crate) input_reply_error: Option<tonic::Code>,
    pub(crate) resize_fail_once: bool,
    pub(crate) metadata_open_error: Option<tonic::Code>,
    pub(crate) list_body_error: bool,
    pub(crate) read_continue: Option<Arc<tokio::sync::Notify>>,
    pub(crate) flush_calls: Arc<AtomicUsize>,
    pub(crate) env_reply_failure: Option<(Arc<AtomicUsize>, tonic::Code)>,
    pub(crate) lost_reply_calls: Option<Arc<AtomicUsize>>,
}

impl Default for MockBridgeGuest {
    fn default() -> Self {
        Self {
            version: "0.1.4",
            ready: true,
            interactive_process_control_calls: None,
            input_reply_error: None,
            resize_fail_once: false,
            metadata_open_error: None,
            list_body_error: false,
            read_continue: None,
            flush_calls: Arc::default(),
            lost_reply_calls: None,
            env_reply_failure: None,
        }
    }
}

impl MockBridgeGuest {
    fn lose_reply_after_effect(&self) -> Result<(), Status> {
        if let Some(calls) = &self.lost_reply_calls {
            calls.fetch_add(1, Ordering::SeqCst);
            return Err(Status::unavailable("reply lost after effect"));
        }
        Ok(())
    }

    fn record_interactive_process_control_call(&self) {
        if let Some(calls) = &self.interactive_process_control_calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tonic::async_trait]
impl Filesystem for MockBridgeGuest {
    // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: retire old-guest mode, not
    // canonical-response/error coverage, with the host metadata fallback.
    type ListInfoStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<pplx_space_spaced_common::proto::operations::FileInfo, Status>,
                > + Send,
        >,
    >;
    async fn stat_info(
        &self,
        request: Request<StatRequest>,
    ) -> Result<Response<pplx_space_spaced_common::proto::operations::StatResponse>, Status> {
        if let Some(code) = self.metadata_open_error {
            return Err(Status::new(code, "StatInfo rejected"));
        }
        Ok(Response::new(
            pplx_space_spaced_common::proto::operations::StatResponse {
                info: Some(pplx_space_spaced_common::proto::operations::FileInfo {
                    path: request.into_inner().path,
                    file_type: FileType::File.into(),
                    size_bytes: 321,
                    ..Default::default()
                }),
            },
        ))
    }
    async fn list_info(
        &self,
        _: Request<ListRequest>,
    ) -> Result<Response<Self::ListInfoStream>, Status> {
        if let Some(code) = self.metadata_open_error {
            return Err(Status::new(code, "ListInfo rejected"));
        }
        let mut entries = vec![Ok(pplx_space_spaced_common::proto::operations::FileInfo {
            path: "/tmp/test-dir/canonical.txt".into(),
            name: Some("canonical.txt".into()),
            file_type: FileType::File.into(),
            ..Default::default()
        })];
        if self.list_body_error {
            entries.push(Err(Status::unimplemented("failure after stream opened")));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(entries))))
    }
    type ListStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<LegacyDirEntry, Status>> + Send>>;
    type ReadStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileChunk, Status>> + Send>>;
    type WatchStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileEvent, Status>> + Send>>;

    async fn stat(
        &self,
        request: Request<StatRequest>,
    ) -> Result<Response<LegacyStatResponse>, Status> {
        assert_eq!(request.into_inner().path, "/tmp/test-file");
        Ok(Response::new(LegacyStatResponse {
            file_type: FileType::File.into(),
            size_bytes: 123,
            mode: 0o100644,
            modified_at: Some(timestamp_from_millis(1_000)),
            owner_uid: 1000,
            owner_gid: 1000,
            symlink_target: None,
        }))
    }

    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        assert_eq!(request.into_inner().path, "/tmp/test-dir");
        let entries = vec![
            Ok(LegacyDirEntry {
                name: "a.txt".into(),
                path: "/tmp/test-dir/a.txt".into(),
                file_type: FileType::File.into(),
                size_bytes: 5,
                mode: 0o100644,
                modified_at: Some(timestamp_from_millis(1_000)),
                owner_uid: 1000,
                owner_gid: 1000,
                symlink_target: None,
            }),
            Ok(LegacyDirEntry {
                name: "nested".into(),
                path: "/tmp/test-dir/nested".into(),
                file_type: FileType::Directory.into(),
                size_bytes: 0,
                mode: 0o040755,
                modified_at: Some(timestamp_from_millis(1_000)),
                owner_uid: 1000,
                owner_gid: 1000,
                symlink_target: None,
            }),
        ];
        Ok(Response::new(
            Box::pin(tokio_stream::iter(entries)) as Self::ListStream
        ))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();
        assert_eq!(request.path, "/tmp/test-file");
        assert_eq!(request.offset, 1);
        assert_eq!(request.length, Some(3));
        let read_continue = self.read_continue.clone();
        let first = futures_util::stream::once(async {
            Ok(FileChunk {
                data: b"bc".to_vec().into(),
            })
        });
        let last = futures_util::stream::once(async move {
            if let Some(read_continue) = read_continue {
                read_continue.notified().await;
            }
            Ok(FileChunk {
                data: b"d".to_vec().into(),
            })
        });
        Ok(Response::new(Box::pin(first.chain(last))))
    }

    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.path, "/tmp/test-dir");
        assert!(request.parents);
        assert!(request.exist_ok);
        Ok(Response::new(MkdirResponse {}))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.path, "/tmp/test-dir");
        assert!(request.recursive);
        assert!(request.missing_ok);
        self.lose_reply_after_effect()?;
        Ok(Response::new(RemoveResponse {}))
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.old_path, "/tmp/old");
        assert_eq!(request.new_path, "/tmp/new");
        assert_eq!(request.idempotency_key.as_deref(), Some("key-1"));
        Ok(Response::new(RenameResponse {}))
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let open = stream
            .message()
            .await?
            .expect("open message")
            .message
            .expect("open payload");
        match open {
            pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Open(open) => {
                assert_eq!(open.path, "/tmp/test-file");
                assert!(!open.append);
            }
            _ => panic!("first message must be open"),
        }
        let mut data = Vec::new();
        while let Some(message) = stream.message().await? {
            match message.message.expect("data payload") {
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Data(
                    chunk,
                ) => {
                    data.extend_from_slice(&chunk);
                }
                _ => panic!("subsequent message must be data"),
            }
        }
        assert_eq!(data, b"hello");
        Ok(Response::new(WriteResponse { bytes_written: 5 }))
    }

    async fn write_batch(
        &self,
        _request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        Ok(Response::new(WriteBatchResponse { files_written: 0 }))
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        assert_eq!(request.into_inner().path, "/tmp/test-dir");
        let events = vec![Ok(FileEvent {
            path: "created.txt".into(),
            kind: pplx_space_spaced_common::proto::bridge_guest::FileEventKind::Created.into(),
            observed_at: Some(timestamp_from_millis(1)),
        })];
        Ok(Response::new(
            Box::pin(tokio_stream::iter(events)) as Self::WatchStream
        ))
    }

    async fn create_watcher(
        &self,
        request: Request<CreateWatcherRequest>,
    ) -> Result<Response<CreateWatcherResponse>, Status> {
        let request = request.into_inner();
        self.lose_reply_after_effect()?;
        Ok(Response::new(CreateWatcherResponse {
            watcher_id: "watcher-1".into(),
            root: request.root,
        }))
    }

    async fn normalize_watch_root(
        &self,
        request: Request<NormalizeWatchRootRequest>,
    ) -> Result<Response<NormalizeWatchRootResponse>, Status> {
        Ok(Response::new(NormalizeWatchRootResponse {
            root: request.into_inner().path,
        }))
    }

    async fn get_watcher_events(
        &self,
        _request: Request<GetWatcherEventsRequest>,
    ) -> Result<Response<GetWatcherEventsResponse>, Status> {
        self.lose_reply_after_effect()?;
        Ok(Response::new(GetWatcherEventsResponse {
            events: Vec::new(),
        }))
    }

    async fn remove_watcher(
        &self,
        _request: Request<RemoveWatcherRequest>,
    ) -> Result<Response<RemoveWatcherResponse>, Status> {
        Ok(Response::new(RemoveWatcherResponse {}))
    }

    async fn enable_file_changes(
        &self,
        _request: Request<EnableFileChangesRequest>,
    ) -> Result<Response<EnableFileChangesResponse>, Status> {
        Ok(Response::new(EnableFileChangesResponse {
            observed_since: Some(timestamp_from_millis(1)),
        }))
    }

    async fn disable_file_changes(
        &self,
        _request: Request<DisableFileChangesRequest>,
    ) -> Result<Response<DisableFileChangesResponse>, Status> {
        Ok(Response::new(DisableFileChangesResponse {}))
    }

    async fn get_modified_files(
        &self,
        _request: Request<GetModifiedFilesRequest>,
    ) -> Result<Response<GetModifiedFilesResponse>, Status> {
        Ok(Response::new(GetModifiedFilesResponse {
            files: Vec::new(),
            observed_since: Some(timestamp_from_millis(1)),
            retained_since: Some(timestamp_from_millis(1)),
            cursor: Some("1".into()),
            monitoring_enabled: true,
            overflowed: false,
        }))
    }
}

#[tonic::async_trait]
impl Process for MockBridgeGuest {
    type StartStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
    type ConnectProcessStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
    type ListProcessesStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessInfo, Status>> + Send>>;

    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<Self::StartStream>, Status> {
        let request = request.into_inner();
        assert_eq!(
            request.command,
            vec!["echo".to_string(), "hello".to_string()]
        );
        assert_eq!(request.idempotency_key.as_deref(), Some("start-key"));
        assert!(!request.stdin);
        // Echo a caller-assigned id so tests can prove it survived the host
        // client's request build instead of being silently dropped.
        let assigned_process_id = request
            .process_id
            .clone()
            .unwrap_or_else(|| "proc-1".to_string());
        let events = vec![
            Ok(ProcessEvent {
                process_id: assigned_process_id.clone(),
                pid: Some(123),
                event: Some(
                    pplx_space_spaced_common::proto::bridge_guest::process_event::Event::Started(
                        pplx_space_spaced_common::proto::bridge_guest::ProcessStarted {
                            start_time: Some(timestamp_from_millis(1_000)),
                        },
                    ),
                ),
            }),
            Ok(ProcessEvent {
                process_id: assigned_process_id.clone(),
                pid: Some(123),
                event: Some(
                    pplx_space_spaced_common::proto::bridge_guest::process_event::Event::Stdout(
                        pplx_space_spaced_common::proto::bridge_guest::ProcessOutput {
                            data: b"hello\n".to_vec(),
                        },
                    ),
                ),
            }),
            Ok(ProcessEvent {
                process_id: assigned_process_id.clone(),
                pid: Some(123),
                event: Some(
                    pplx_space_spaced_common::proto::bridge_guest::process_event::Event::Exit(
                        pplx_space_spaced_common::proto::bridge_guest::ProcessExit {
                            outcome: Some(
                                pplx_space_spaced_common::proto::bridge_guest::process_exit::Outcome::Exited(
                                    pplx_space_spaced_common::proto::bridge_guest::ProcessExited {
                                        exit_code: 0,
                                    },
                                ),
                            ),
                        },
                    ),
                ),
            }),
        ];
        Ok(Response::new(
            Box::pin(tokio_stream::iter(events)) as Self::StartStream
        ))
    }

    async fn connect_process(
        &self,
        request: Request<ConnectProcessRequest>,
    ) -> Result<Response<Self::ConnectProcessStream>, Status> {
        assert_eq!(request.into_inner().process_id, "proc-1");
        let events = vec![Ok(ProcessEvent {
            process_id: "proc-1".into(),
            pid: Some(123),
            event: Some(
                pplx_space_spaced_common::proto::bridge_guest::process_event::Event::Connected(
                    pplx_space_spaced_common::proto::bridge_guest::ProcessConnected {},
                ),
            ),
        })];
        Ok(Response::new(
            Box::pin(tokio_stream::iter(events)) as Self::ConnectProcessStream
        ))
    }

    async fn list_processes(
        &self,
        _request: Request<ListProcessesRequest>,
    ) -> Result<Response<Self::ListProcessesStream>, Status> {
        let processes = vec![Ok(ProcessInfo {
            process_id: "proc-1".into(),
            pid: 123,
            command: vec!["sleep".into(), "60".into()],
            cwd: Some("/workspace".into()),
            env: std::collections::HashMap::from([("FOO".into(), "bar".into())]),
            user: Some("user".into()),
            tag: Some("server".into()),
            pty: Some(false),
        })];
        Ok(Response::new(
            Box::pin(tokio_stream::iter(processes)) as Self::ListProcessesStream
        ))
    }

    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.process_id, "proc-1");
        assert_eq!(request.signal, 9);
        Ok(Response::new(SendSignalResponse {}))
    }

    async fn send_input(
        &self,
        request: Request<SendInputRequest>,
    ) -> Result<Response<SendInputResponse>, Status> {
        self.record_interactive_process_control_call();
        if let Some(code) = self.input_reply_error {
            return Err(Status::new(code, "reply lost after accepting stdin"));
        }
        let request = request.into_inner();
        assert_eq!(request.process_id, "proc-1");
        assert_eq!(request.data, b"hello stdin\n");
        Ok(Response::new(SendInputResponse {}))
    }

    async fn close_stdin(
        &self,
        request: Request<CloseStdinRequest>,
    ) -> Result<Response<CloseStdinResponse>, Status> {
        self.record_interactive_process_control_call();
        assert_eq!(request.into_inner().process_id, "proc-1");
        Ok(Response::new(CloseStdinResponse {}))
    }

    async fn resize_pty(
        &self,
        request: Request<ResizePtyRequest>,
    ) -> Result<Response<ResizePtyResponse>, Status> {
        self.record_interactive_process_control_call();
        if self.resize_fail_once
            && self
                .interactive_process_control_calls
                .as_ref()
                .unwrap()
                .load(Ordering::SeqCst)
                == 1
        {
            return Err(Status::unavailable("lost first resize reply"));
        }
        assert_eq!(request.into_inner().process_id, "proc-1");
        Ok(Response::new(ResizePtyResponse {}))
    }
}

#[tonic::async_trait]
impl Meta for MockBridgeGuest {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {}))
    }

    async fn wait_ready(
        &self,
        _request: Request<WaitReadyRequest>,
    ) -> Result<Response<WaitReadyResponse>, Status> {
        Ok(Response::new(WaitReadyResponse {
            ready: self.ready,
            guest_uptime: self.ready.then_some(prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
            version: self.version.to_string(),
            compatibility_family:
                pplx_space_spaced_common::spaced_compatibility_family_for_version(self.version)
                    .expect("test version is valid SemVer"),
            mem_total_bytes: None,
            vcpu_count: None,
        }))
    }

    async fn configure_process_env(
        &self,
        _request: Request<ConfigureProcessEnvRequest>,
    ) -> Result<Response<ConfigureProcessEnvResponse>, Status> {
        if let Some((calls, code)) = &self.env_reply_failure
            && calls.fetch_add(1, Ordering::SeqCst) == 0
        {
            return Err(Status::new(*code, "environment reply lost"));
        }
        Ok(Response::new(ConfigureProcessEnvResponse {}))
    }

    async fn notify_resume(
        &self,
        _request: Request<NotifyResumeRequest>,
    ) -> Result<Response<NotifyResumeResponse>, Status> {
        Ok(Response::new(NotifyResumeResponse {
            clock_sync_status:
                pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Synced.into(),
            ready: true,
        }))
    }

    async fn flush_filesystems(
        &self,
        _request: Request<FlushFilesystemsRequest>,
    ) -> Result<Response<FlushFilesystemsResponse>, Status> {
        self.flush_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(FlushFilesystemsResponse {}))
    }
}

pub(crate) async fn start_mock_bridge_guest(
    socket_path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    start_mock_bridge_guest_with_service(socket_path, MockBridgeGuest::default()).await
}

pub(crate) async fn start_mock_bridge_guest_with_service(
    socket_path: std::path::PathBuf,
    service: MockBridgeGuest,
) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).expect("guest bridge socket bound");
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(MetaServer::new(service.clone()))
            .add_service(FilesystemServer::new(service.clone()))
            .add_service(ProcessServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await;
    })
}
