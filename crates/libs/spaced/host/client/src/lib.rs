//! Typed convenience client for the spaced host socket.
//!
//! Handles can be cloned or shared by reference. File listings, reads, and process
//! output are streaming: callers choose their own collection limits. Opening a
//! stream may retry; consuming it does not silently restart it. Process starts
//! generate an idempotency key when absent; interactive effects are single-attempt.
//! Dropping a process response stream does not terminate the guest process.

use futures_util::StreamExt;
use pplx_space_common_core::types::{
    GuestPath, GuestUserName, IdempotencyKey, Pii, ProcessCommand, ProcessId, ProcessSignal,
    ReadyTimeout, RelativeGlobPattern, WatchRoot,
};
use prost::Message as _;
use proto::{
    ConfigureProcessEnvRequest, ConnectProcessRequest, CreateWatcherRequest,
    DisableFileChangesRequest, EnableFileChangesRequest, FlushGuestFilesystemsRequest,
    GetModifiedFilesRequest, GetStatusRequest, GetWatcherEventsRequest, IsConnectedRequest,
    KillRequest, ListProcessesRequest, ListRequest, MkdirRequest, NormalizeWatchRootRequest,
    NotifyResumeRequest, PingGuestRequest, PingRequest, PrepareSuspendRequest, RemoveRequest,
    RemoveWatcherRequest, RenameRequest, ResumeReason, SendSignalRequest, StartRequest,
    StatRequest, WaitReadyRequest, WatchRequest, WriteBatchEntry, WriteBatchRequest, WriteRequest,
};
use tonic::Response;

mod config;

pub mod operations {
    pub use pplx_space_spaced_common::proto::operations::*;
}

pub use pplx_space_spaced_common::proto::host as proto;

pub use config::SpacedHostClientConfig;
pub use pplx_space_spaced_common::ConnectError;
use pplx_space_spaced_common::{
    invalid_argument_status, parse_env, proto_duration, proto_env, proto_globs,
    proto_idempotency_key,
};

pub type RawSpacedHostMetaClient = proto::meta_client::MetaClient<tonic::transport::Channel>;
pub type RawSpacedHostFilesystemClient =
    proto::filesystem_client::FilesystemClient<tonic::transport::Channel>;
pub type RawSpacedHostProcessClient =
    proto::process_client::ProcessClient<tonic::transport::Channel>;
pub type RawSpacedHostNetworkingClient =
    proto::networking_client::NetworkingClient<tonic::transport::Channel>;

pub use pplx_space_spaced_common::{
    DEFAULT_COMMAND_RETRY_POLICY, DEFAULT_CONNECT_RETRY_POLICY, RetryPolicy,
};

const PROCESS_LIST_MAX_RECORDS: usize = 128;
const PROCESS_LIST_MAX_ENCODED_BYTES: usize = 4 * 1024 * 1024;
const PROCESS_LIST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub fn default_host_uds_path() -> std::path::PathBuf {
    pplx_space_common_core::host::linux::paths::default_host_uds_path(
        pplx_space_common_core::host::linux::paths::SPACED_HOST_SOCKET_FILE,
    )
}

fn idempotency_key_or_generated(
    key: Option<String>,
    required: bool,
) -> Result<Option<IdempotencyKey>, tonic::Status> {
    match key {
        Some(key) => IdempotencyKey::parse(key)
            .map(Some)
            .map_err(invalid_argument_status),
        None if required => IdempotencyKey::parse(uuid::Uuid::now_v7().to_string())
            .map(Some)
            .map_err(invalid_argument_status),
        None => Ok(None),
    }
}

fn connect_process_request(
    process_id: &ProcessId,
    timeout: Option<ReadyTimeout>,
    output_cursor: Option<u64>,
) -> ConnectProcessRequest {
    ConnectProcessRequest {
        process_id: process_id.as_str().to_string(),
        timeout: timeout.map(proto_duration),
        output_cursor,
    }
}

#[derive(Clone, Debug)]
pub struct SpacedHostClient {
    meta: RawSpacedHostMetaClient,
    filesystem: RawSpacedHostFilesystemClient,
    process: RawSpacedHostProcessClient,
    networking: RawSpacedHostNetworkingClient,
    config: SpacedHostClientConfig,
}

#[derive(Clone, Debug)]
pub struct StartProcessOptions {
    pub command: ProcessCommand,
    pub cwd: Option<GuestPath>,
    pub env: pplx_space_common_core::types::EnvMap,
    pub user: Option<GuestUserName>,
    pub timeout: Option<std::time::Duration>,
    pub background: bool,
    pub idempotency_key: Option<IdempotencyKey>,
    pub tag: Option<String>,
    pub stdin: bool,
    pub pty: Option<proto::PtyConfig>,
    pub clean_env: bool,
    /// Caller-assigned process identity. `None` lets the guest mint one.
    pub process_id: Option<ProcessId>,
}

impl StartProcessOptions {
    /// A validated command is required; all other options are opt-in.
    pub fn new(command: ProcessCommand) -> Self {
        Self {
            command,
            cwd: None,
            env: Default::default(),
            user: None,
            timeout: None,
            background: false,
            idempotency_key: None,
            process_id: None,
            tag: None,
            stdin: false,
            pty: None,
            clean_env: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct WatchOptions {
    pub recursive: bool,
    pub include_metadata_events: bool,
    pub include_globs: Vec<RelativeGlobPattern>,
    pub exclude_globs: Vec<RelativeGlobPattern>,
    pub use_default_excludes: Option<bool>,
    pub user: Option<GuestUserName>,
}

// Generated tonic clients differ only in their type and cache field here.
macro_rules! client_call {
    ($method:ident, $field:ident, $client:ty) => {
        client_call!($method, $field, $client, true);
    };
    ($method:ident, $field:ident, $client:ty, $replay:literal) => {
        async fn $method<T, F, Fut>(
            &self,
            command: &'static str,
            mut call: F,
        ) -> Result<T, tonic::Status>
        where
            F: FnMut($client) -> Fut,
            Fut: std::future::Future<Output = Result<Response<T>, tonic::Status>>,
        {
            pplx_space_spaced_common::retry_status(
                "spaced host",
                command,
                if $replay {
                    self.config.command_retry_policy
                } else {
                    RetryPolicy::none()
                },
                || {
                    let response = call(self.$field.clone());
                    async move { response.await.map(Response::into_inner) }
                },
            )
            .await
        }
    };
}

impl SpacedHostClient {
    client_call!(call_process, process, RawSpacedHostProcessClient, false);
    client_call!(call_meta, meta, RawSpacedHostMetaClient);
    client_call!(call_filesystem, filesystem, RawSpacedHostFilesystemClient);
    client_call!(
        call_networking_once,
        networking,
        RawSpacedHostNetworkingClient,
        false
    );
    client_call!(call_meta_once, meta, RawSpacedHostMetaClient, false);
    client_call!(
        call_filesystem_once,
        filesystem,
        RawSpacedHostFilesystemClient,
        false
    );

    pub async fn new_with_config(config: SpacedHostClientConfig) -> Result<Self, ConnectError> {
        let channel = pplx_space_spaced_common::connect_uds_channel_with_retry(
            "spaced host service",
            &config.socket_path,
            config.connect_retry_policy,
        )
        .await?;
        Ok(Self::from_channel(channel, config))
    }

    /// Connect to the expected host process. The authenticated channel is
    /// pinned to that connection and will not reconnect to a replacement PID;
    /// callers must construct a new client after re-attesting the endpoint.
    pub async fn new_with_config_and_expected_peer(
        config: SpacedHostClientConfig,
        expected_peer: pplx_space_spaced_common::ExpectedUdsPeer,
    ) -> Result<Self, ConnectError> {
        let channel = pplx_space_spaced_common::connect_uds_channel_with_expected_peer_and_retry(
            "spaced host service",
            &config.socket_path,
            expected_peer,
            config.connect_retry_policy,
        )
        .await?;
        Ok(Self::from_channel(channel, config))
    }

    fn from_channel(channel: tonic::transport::Channel, config: SpacedHostClientConfig) -> Self {
        let meta = RawSpacedHostMetaClient::new(channel.clone());
        let filesystem = RawSpacedHostFilesystemClient::new(channel.clone());
        let process = RawSpacedHostProcessClient::new(channel.clone());
        let networking = RawSpacedHostNetworkingClient::new(channel);
        Self {
            meta,
            filesystem,
            process,
            networking,
            config,
        }
    }

    pub async fn ping(&self) -> Result<proto::PingResponse, tonic::Status> {
        self.call_meta("ping", |mut client| async move {
            client.ping(PingRequest {}).await
        })
        .await
    }

    pub async fn is_connected(&self) -> Result<proto::IsConnectedResponse, tonic::Status> {
        self.call_meta("is_connected", |mut client| async move {
            client.is_connected(IsConnectedRequest {}).await
        })
        .await
    }

    pub async fn ping_guest(&self) -> Result<proto::PingGuestResponse, tonic::Status> {
        self.call_meta("ping_guest", |mut client| async move {
            client.ping_guest(PingGuestRequest {}).await
        })
        .await
    }

    pub async fn get_status(&self) -> Result<proto::GetStatusResponse, tonic::Status> {
        self.call_meta("get_status", |mut client| async move {
            client.get_status(GetStatusRequest {}).await
        })
        .await
    }

    pub async fn wait_ready(
        &self,
        timeout: std::time::Duration,
    ) -> Result<proto::WaitReadyResponse, tonic::Status> {
        let timeout = ReadyTimeout::new(timeout).map_err(invalid_argument_status)?;
        self.call_meta("wait_ready", |mut client| async move {
            client
                .wait_ready(WaitReadyRequest {
                    timeout: Some(proto_duration(timeout)),
                })
                .await
        })
        .await
    }

    pub async fn configure_process_env(
        &self,
        env: std::collections::HashMap<String, String>,
    ) -> Result<(), tonic::Status> {
        let env = parse_env(env);
        self.call_meta_once("configure_process_env", |mut client| {
            let env = proto_env(&env);
            async move {
                client
                    .configure_process_env(ConfigureProcessEnvRequest { env })
                    .await
            }
        })
        .await
        .map(|_: proto::ConfigureProcessEnvResponse| ())
    }

    pub async fn notify_resume(
        &self,
        reason: ResumeReason,
    ) -> Result<proto::NotifyResumeResponse, tonic::Status> {
        let now = pplx_space_spaced_common::system_time_to_proto(std::time::SystemTime::now())?;
        self.call_meta_once("notify_resume", |mut client| async move {
            client
                .notify_resume(NotifyResumeRequest {
                    reason: reason.into(),
                    host_time: Some(now),
                })
                .await
        })
        .await
    }

    /// Prepare for a VM memory snapshot, including the final guest filesystem flush.
    ///
    /// Call this instead of `flush_guest_filesystems` for suspend or full
    /// checkpoint paths; do not call both for one snapshot.
    pub async fn prepare_suspend(&self, timeout: std::time::Duration) -> Result<(), tonic::Status> {
        self.call_meta_once("prepare_suspend", |mut client| async move {
            let mut request = tonic::Request::new(PrepareSuspendRequest {});
            request.set_timeout(timeout.min(std::time::Duration::from_secs(180)));
            client.prepare_suspend(request).await
        })
        .await
        .map(|_: proto::PrepareSuspendResponse| ())
    }

    /// Flush guest filesystems without preparing bridge connections for a VM snapshot.
    ///
    /// Use this for disk-only snapshots of a still-running VM. Memory-bearing
    /// snapshots must call `prepare_suspend` instead.
    pub async fn flush_guest_filesystems(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), tonic::Status> {
        self.call_meta_once("flush_guest_filesystems", |mut client| async move {
            let mut request = tonic::Request::new(FlushGuestFilesystemsRequest {});
            request.set_timeout(timeout.min(std::time::Duration::from_secs(180)));
            client.flush_guest_filesystems(request).await
        })
        .await
        .map(|_: proto::FlushGuestFilesystemsResponse| ())
    }

    pub async fn stat(&self, path: String) -> Result<proto::StatResponse, tonic::Status> {
        self.stat_as_user(path, None).await
    }

    pub async fn stat_as_user(
        &self,
        path: String,
        user: Option<GuestUserName>,
    ) -> Result<proto::StatResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem("stat", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .stat(StatRequest {
                        path: path.expose().to_string(),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn list_stream_with_depth_as_user(
        &self,
        path: String,
        depth: Option<u32>,
        user: Option<GuestUserName>,
    ) -> Result<tonic::Streaming<proto::FileInfo>, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        if depth == Some(0) {
            return Err(tonic::Status::invalid_argument(
                "depth must be greater than 0",
            ));
        }
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "list",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = path.clone();
                let user = user.clone();
                async move {
                    client
                        .list(ListRequest {
                            path: path.expose().to_string(),
                            depth,
                            user: user.map(|user| user.as_str().to_string()),
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn read_stream_as_user(
        &self,
        path: String,
        offset: u64,
        length: Option<u64>,
        user: Option<GuestUserName>,
    ) -> Result<tonic::Streaming<proto::FileChunk>, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "read",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = path.clone();
                let user = user.clone();
                async move {
                    client
                        .read(proto::ReadRequest {
                            path: path.expose().to_string(),
                            offset,
                            length,
                            user: user.map(|user| user.as_str().to_string()),
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn write(
        &self,
        path: String,
        data: Vec<u8>,
        append: bool,
    ) -> Result<proto::WriteResponse, tonic::Status> {
        self.write_with_idempotency(path, data, append, None, None)
            .await
    }

    pub async fn write_with_idempotency(
        &self,
        path: String,
        data: Vec<u8>,
        append: bool,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::WriteResponse, tonic::Status> {
        const CHUNK_SIZE: usize = 64 * 1024;
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, append)?;
        let data = std::sync::Arc::new(data);

        pplx_space_spaced_common::retry_status(
            "spaced host",
            "write",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = path.clone();
                let idempotency_key = idempotency_key.clone();
                let user = user.clone();
                let data = data.clone();
                async move {
                    let open = WriteRequest {
                        message: Some(proto::write_request::Message::Open(proto::WriteOpen {
                            path: path.expose().to_string(),
                            append,
                            idempotency_key: proto_idempotency_key(idempotency_key),
                            user: user.map(|user| user.as_str().to_string()),
                        })),
                    };
                    // Retries share the supplied payload; only the next frame
                    // is copied, not the complete upload on every attempt.
                    let chunks =
                        futures_util::stream::unfold((data, 0), |(data, offset)| async move {
                            let chunk = data.get(offset..)?.chunks(CHUNK_SIZE).next()?.to_vec();
                            let next = offset + chunk.len();
                            Some((
                                WriteRequest {
                                    message: Some(proto::write_request::Message::Data(chunk)),
                                },
                                (data, next),
                            ))
                        });
                    let messages = futures_util::stream::once(async move { open }).chain(chunks);
                    client.write(messages).await.map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn write_stream_with_idempotency<S>(
        &self,
        path: String,
        append: bool,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
        data: S,
    ) -> Result<proto::WriteResponse, tonic::Status>
    where
        S: futures_util::Stream<Item = Vec<u8>> + Send + 'static,
    {
        use futures_util::StreamExt;

        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, append)?;
        let open = WriteRequest {
            message: Some(proto::write_request::Message::Open(proto::WriteOpen {
                path: path.expose().to_string(),
                append,
                idempotency_key: proto_idempotency_key(idempotency_key),
                user: user.map(|user| user.as_str().to_string()),
            })),
        };
        let messages =
            futures_util::stream::once(async move { open }).chain(data.map(|chunk| WriteRequest {
                message: Some(proto::write_request::Message::Data(chunk)),
            }));
        self.filesystem
            .clone()
            .write(messages)
            .await
            .map(Response::into_inner)
    }

    pub async fn mkdir(
        &self,
        path: String,
        parents: bool,
        exist_ok: bool,
        user: Option<GuestUserName>,
    ) -> Result<proto::MkdirResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem_once("mkdir", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .mkdir(MkdirRequest {
                        path: path.expose().to_string(),
                        parents,
                        exist_ok,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn remove(
        &self,
        path: String,
        recursive: bool,
        missing_ok: bool,
    ) -> Result<proto::RemoveResponse, tonic::Status> {
        self.remove_as_user(path, recursive, missing_ok, None).await
    }

    pub async fn remove_as_user(
        &self,
        path: String,
        recursive: bool,
        missing_ok: bool,
        user: Option<GuestUserName>,
    ) -> Result<proto::RemoveResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem_once("remove", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .remove(RemoveRequest {
                        path: path.expose().to_string(),
                        recursive,
                        missing_ok,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn rename_as_user(
        &self,
        old_path: String,
        new_path: String,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::RenameResponse, tonic::Status> {
        let old_path = GuestPath::parse(old_path).map_err(invalid_argument_status)?;
        let new_path = GuestPath::parse(new_path).map_err(invalid_argument_status)?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, true)?;
        self.call_filesystem("rename", |mut client| {
            let old_path = old_path.clone();
            let new_path = new_path.clone();
            let idempotency_key = idempotency_key.clone();
            let user = user.clone();
            async move {
                client
                    .rename(RenameRequest {
                        old_path: old_path.expose().to_string(),
                        new_path: new_path.expose().to_string(),
                        idempotency_key: proto_idempotency_key(idempotency_key),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn write_batch_as_user(
        &self,
        entries: Vec<WriteBatchEntry>,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::WriteBatchResponse, tonic::Status> {
        let entries = entries
            .into_iter()
            .map(|entry| {
                let path = GuestPath::parse(entry.path).map_err(invalid_argument_status)?;
                Ok((path, entry.data, entry.append))
            })
            .collect::<Result<Vec<_>, tonic::Status>>()?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, true)?;
        self.call_filesystem("write_batch", |mut client| {
            let entries = entries.clone();
            let idempotency_key = idempotency_key.clone();
            let user = user.clone();
            async move {
                client
                    .write_batch(WriteBatchRequest {
                        entries: entries
                            .into_iter()
                            .map(|(path, data, append)| WriteBatchEntry {
                                path: path.expose().to_string(),
                                data,
                                append,
                            })
                            .collect(),
                        idempotency_key: proto_idempotency_key(idempotency_key),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn watch_stream(
        &self,
        path: String,
        recursive: bool,
    ) -> Result<tonic::Streaming<proto::FileEvent>, tonic::Status> {
        self.watch_stream_with_options(
            path,
            WatchOptions {
                recursive,
                include_metadata_events: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn watch_stream_with_options(
        &self,
        path: String,
        options: WatchOptions,
    ) -> Result<tonic::Streaming<proto::FileEvent>, tonic::Status> {
        let root = WatchRoot::new(GuestPath::parse(path).map_err(invalid_argument_status)?);
        let include_globs = options.include_globs;
        let exclude_globs = options.exclude_globs;
        let recursive = options.recursive;
        let include_metadata_events = options.include_metadata_events;
        let use_default_excludes = options.use_default_excludes;
        let user = options.user;
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "watch",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = root.guest_path().expose().to_string();
                let include_globs = include_globs.clone();
                let exclude_globs = exclude_globs.clone();
                let user = user.clone();
                async move {
                    client
                        .watch(WatchRequest {
                            path,
                            recursive,
                            include_metadata_events,
                            include_globs: proto_globs(&include_globs),
                            exclude_globs: proto_globs(&exclude_globs),
                            use_default_excludes,
                            user: user.map(|user| user.as_str().to_string()),
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn create_watcher_with_options(
        &self,
        root: String,
        options: WatchOptions,
    ) -> Result<proto::CreateWatcherResponse, tonic::Status> {
        let root = WatchRoot::new(GuestPath::parse(root).map_err(invalid_argument_status)?);
        let include_globs = options.include_globs;
        let exclude_globs = options.exclude_globs;
        let recursive = options.recursive;
        let include_metadata_events = options.include_metadata_events;
        let use_default_excludes = options.use_default_excludes;
        let user = options.user;
        self.call_filesystem_once("create_watcher", |mut client| {
            let root = root.guest_path().expose().to_string();
            let include_globs = include_globs.clone();
            let exclude_globs = exclude_globs.clone();
            let user = user.clone();
            async move {
                client
                    .create_watcher(CreateWatcherRequest {
                        root,
                        recursive,
                        include_metadata_events,
                        include_globs: proto_globs(&include_globs),
                        exclude_globs: proto_globs(&exclude_globs),
                        use_default_excludes,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn normalize_watch_root_as_user(
        &self,
        path: String,
        user: Option<GuestUserName>,
    ) -> Result<proto::NormalizeWatchRootResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem("normalize_watch_root", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .normalize_watch_root(NormalizeWatchRootRequest {
                        path: path.expose().to_string(),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn get_watcher_events(
        &self,
        watcher_id: String,
    ) -> Result<proto::GetWatcherEventsResponse, tonic::Status> {
        self.call_filesystem_once("get_watcher_events", |mut client| {
            let watcher_id = watcher_id.clone();
            async move {
                client
                    .get_watcher_events(GetWatcherEventsRequest { watcher_id })
                    .await
            }
        })
        .await
    }

    pub async fn remove_watcher(
        &self,
        watcher_id: String,
    ) -> Result<proto::RemoveWatcherResponse, tonic::Status> {
        self.call_filesystem_once("remove_watcher", |mut client| {
            let watcher_id = watcher_id.clone();
            async move {
                client
                    .remove_watcher(RemoveWatcherRequest { watcher_id })
                    .await
            }
        })
        .await
    }

    pub async fn enable_file_changes_with_options_as_user(
        &self,
        roots: Vec<String>,
        include_globs: Vec<RelativeGlobPattern>,
        exclude_globs: Vec<RelativeGlobPattern>,
        use_default_excludes: Option<bool>,
        user: Option<GuestUserName>,
    ) -> Result<proto::EnableFileChangesResponse, tonic::Status> {
        let roots = roots
            .into_iter()
            .map(GuestPath::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid_argument_status)?;
        self.call_filesystem_once("enable_file_changes", |mut client| {
            let roots = roots
                .iter()
                .map(|root| root.expose().to_string())
                .collect::<Vec<_>>();
            let include_globs = include_globs.clone();
            let exclude_globs = exclude_globs.clone();
            let user = user.clone();
            async move {
                client
                    .enable_file_changes(EnableFileChangesRequest {
                        roots,
                        include_globs: proto_globs(&include_globs),
                        exclude_globs: proto_globs(&exclude_globs),
                        use_default_excludes,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn disable_file_changes_as_user(
        &self,
        user: Option<GuestUserName>,
    ) -> Result<proto::DisableFileChangesResponse, tonic::Status> {
        self.call_filesystem_once("disable_file_changes", |mut client| {
            let user = user.clone();
            async move {
                client
                    .disable_file_changes(DisableFileChangesRequest {
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn get_modified_files_as_user(
        &self,
        window: Option<prost_types::Duration>,
        since: Option<prost_types::Timestamp>,
        cursor: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::GetModifiedFilesResponse, tonic::Status> {
        self.call_filesystem("get_modified_files", |mut client| {
            let cursor = cursor.clone();
            let user = user.clone();
            async move {
                client
                    .get_modified_files(GetModifiedFilesRequest {
                        window,
                        since,
                        cursor,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn start_process_stream_with_options(
        &self,
        options: StartProcessOptions,
    ) -> Result<tonic::Streaming<proto::ProcessEvent>, tonic::Status> {
        let tag = options.tag.clone();
        let command = options.command;
        let cwd = options.cwd;
        let env = options.env;
        let user = options.user;
        let timeout = options
            .timeout
            .map(ReadyTimeout::new)
            .transpose()
            .map_err(invalid_argument_status)?;
        let background = options.background;
        let stdin = options.stdin;
        let pty = options.pty;
        let clean_env = options.clean_env;
        let assigned_process_id = options.process_id;
        let idempotency_key = Some(match options.idempotency_key {
            Some(key) => key,
            None => IdempotencyKey::parse(uuid::Uuid::now_v7().to_string())
                .map_err(invalid_argument_status)?,
        });
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "process_start",
            self.config.command_retry_policy,
            || {
                let mut client = self.process.clone();
                let command = Pii::new(&command).expose_pii().argv().to_vec();
                let cwd = cwd.as_ref().map(|cwd| cwd.expose().to_string());
                let env = proto_env(&env);
                let user = user.as_ref().map(|user| user.as_str().to_string());
                let idempotency_key = idempotency_key.clone();
                let tag = tag.clone();
                let process_id = assigned_process_id
                    .as_ref()
                    .map(|process_id| process_id.to_string());
                async move {
                    client
                        .start(StartRequest {
                            command,
                            cwd,
                            env,
                            user,
                            timeout: timeout.map(proto_duration),
                            background,
                            idempotency_key: proto_idempotency_key(idempotency_key),
                            tag,
                            stdin,
                            pty,
                            clean_env,
                            process_id,
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn connect_process_stream_with_cursor(
        &self,
        process_id: String,
        timeout: Option<std::time::Duration>,
        output_cursor: Option<u64>,
    ) -> Result<tonic::Streaming<proto::ProcessEvent>, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let timeout = timeout
            .map(ReadyTimeout::new)
            .transpose()
            .map_err(invalid_argument_status)?;
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "process_connect",
            self.config.command_retry_policy,
            || {
                let mut client = self.process.clone();
                let process_id = process_id.clone();
                async move {
                    client
                        .connect_process(connect_process_request(
                            &process_id,
                            timeout,
                            output_cursor,
                        ))
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn list_processes(&self) -> Result<Vec<proto::ProcessInfo>, tonic::Status> {
        tokio::time::timeout(
            PROCESS_LIST_TIMEOUT,
            pplx_space_spaced_common::retry_status(
                "spaced host",
                "process_list",
                self.config.command_retry_policy,
                || {
                    let mut client = self.process.clone();
                    async move {
                        let mut stream = client
                            .list_processes(ListProcessesRequest {})
                            .await?
                            .into_inner();
                        let mut processes = Vec::new();
                        let mut encoded_bytes = 0usize;
                        while let Some(process) = stream.message().await? {
                            if processes.len() == PROCESS_LIST_MAX_RECORDS {
                                return Err(tonic::Status::resource_exhausted(
                                    "spaced host returned too many process records",
                                ));
                            }
                            encoded_bytes = encoded_bytes
                                .checked_add(process.encoded_len())
                                .filter(|size| *size <= PROCESS_LIST_MAX_ENCODED_BYTES)
                                .ok_or_else(|| {
                                    tonic::Status::resource_exhausted(
                                        "spaced host process metadata is too large",
                                    )
                                })?;
                            processes.push(process);
                        }
                        Ok(processes)
                    }
                },
            ),
        )
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("spaced host process list timed out"))?
    }

    /// Attempts a process signal without automatic replay.
    ///
    /// Use this for security-sensitive control paths where an ambiguous
    /// transport failure must not replay a state-changing request.
    pub async fn send_signal(
        &self,
        process_id: String,
        signal: i32,
    ) -> Result<proto::SendSignalResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let signal = ProcessSignal::try_from_i32(signal).map_err(invalid_argument_status)?;
        let mut client = self.process.clone();
        client
            .send_signal(SendSignalRequest {
                process_id: process_id.as_str().to_string(),
                signal: signal.as_i32(),
            })
            .await
            .map(Response::into_inner)
    }

    /// Attempts a process stdin write without replay so an ambiguous failure cannot
    /// duplicate secret input.
    pub async fn send_input(
        &self,
        process_id: String,
        data: Vec<u8>,
    ) -> Result<proto::SendInputResponse, tonic::Status> {
        let mut request = proto::SendInputRequest { process_id, data };
        let process_id = ProcessId::parse(std::mem::take(&mut request.process_id))
            .map_err(invalid_argument_status)?;
        request.process_id = process_id.as_str().to_string();
        let mut client = self.process.clone();
        client.send_input(request).await.map(Response::into_inner)
    }

    /// Attempts to close process stdin without replay so an ambiguous failure cannot
    /// replay process state transitions.
    pub async fn close_stdin(
        &self,
        process_id: String,
    ) -> Result<proto::CloseStdinResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let mut client = self.process.clone();
        client
            .close_stdin(proto::CloseStdinRequest {
                process_id: process_id.as_str().to_string(),
            })
            .await
            .map(Response::into_inner)
    }

    pub async fn resize_pty(
        &self,
        process_id: String,
        size: proto::PtySize,
    ) -> Result<proto::ResizePtyResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        self.call_process("process_resize_pty", |mut client| {
            let process_id = process_id.clone();
            async move {
                client
                    .resize_pty(proto::ResizePtyRequest {
                        process_id: process_id.as_str().to_string(),
                        size: Some(size),
                    })
                    .await
            }
        })
        .await
    }

    /// Attempts to force-stop a process without replay. This is intended for bounded
    /// cleanup after an earlier control request had an ambiguous outcome.
    pub async fn kill_process(
        &self,
        process_id: String,
    ) -> Result<proto::KillResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let mut client = self.process.clone();
        client
            .kill(KillRequest {
                process_id: process_id.as_str().to_string(),
            })
            .await
            .map(Response::into_inner)
    }

    /// Open the in-VM loopback bridge for `port`, blocking server-side for
    /// up to `wait_timeout` for the listener to appear.
    ///
    /// Bypasses the standard `command_retry_policy` because (a) the
    /// service-side wait already absorbs transient "listener not yet up"
    /// states by design, and (b) re-sending the same `wait_timeout` on
    /// each retry would compound the budget (2s × N) without changing the
    /// outcome — `WaitingForListener` is a "the port is armed; reconnect"
    /// signal, not a transient transport error.
    pub async fn open_loopback_bridge(
        &self,
        port: u32,
        wait_timeout: Option<std::time::Duration>,
    ) -> Result<proto::OpenLoopbackBridgeResponse, tonic::Status> {
        let wait_timeout_proto = wait_timeout
            .map(pplx_space_spaced_common::duration_to_proto)
            .transpose()?;
        let mut client = self.networking.clone();
        client
            .open_loopback_bridge(proto::OpenLoopbackBridgeRequest {
                port,
                wait_timeout: wait_timeout_proto,
            })
            .await
            .map(Response::into_inner)
    }

    /// Close authorization for `port` with a single dispatch. Client construction
    /// retries connection establishment, but a lost close response is ambiguous:
    /// replay could close a replacement bridge opened on the same port.
    pub async fn close_loopback_bridge(
        &self,
        port: u32,
    ) -> Result<proto::CloseLoopbackBridgeResponse, tonic::Status> {
        self.call_networking_once("close_loopback_bridge", move |mut client| async move {
            client
                .close_loopback_bridge(proto::CloseLoopbackBridgeRequest { port })
                .await
        })
        .await
    }

    /// Make the guest forget its cached link-layer address of its gateway.
    /// `Unimplemented` from a spaced-host or guest that predates the call.
    pub async fn flush_gateway_neighbor(&self) -> Result<(), tonic::Status> {
        self.call_networking_once("flush_gateway_neighbor", move |mut client| async move {
            client
                .flush_gateway_neighbor(proto::FlushGatewayNeighborRequest {})
                .await
        })
        .await
        .map(drop)
    }
}

#[cfg(test)]
mod tests;
