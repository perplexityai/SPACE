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

pub fn default_host_uds_path() -> std::path::PathBuf {
    pplx_space_common_core::host::linux::paths::default_host_uds_path(
        pplx_space_common_core::host::linux::paths::SPACED_HOST_SOCKET_FILE,
    )
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

mod file_watch;
mod filesystem;
mod meta;
mod networking;
mod process;

#[allow(unused_imports)]
use filesystem::*;
#[allow(unused_imports)]
use process::*;

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
}

#[cfg(test)]
mod tests;
