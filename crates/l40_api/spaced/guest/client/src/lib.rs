//! Client for guest-local metadata and activity requests.
//!
//! This is not the host filesystem/process API. Calls use the configured local
//! Unix socket and bounded retry policy. Activity requests cross the guest/host
//! bridge and remain subject to host authorization and lifecycle decisions.

use proto::{
    ActivityState, PingRequest, RequestActiveForRequest, RequestShutdownRequest,
    SetActivityStateRequest,
};
use tonic::Response;

mod config;

pub use pplx_space_spaced_common::proto::guest as proto;

pub use config::SpacedGuestClientConfig;
pub use pplx_space_spaced_common::ConnectError;

pub type RawSpacedGuestClient = proto::meta_client::MetaClient<tonic::transport::Channel>;

pub use pplx_space_spaced_common::{
    DEFAULT_COMMAND_RETRY_POLICY, DEFAULT_CONNECT_RETRY_POLICY, RetryPolicy,
};

pub fn default_guest_uds_path() -> std::path::PathBuf {
    pplx_space_common_core::guest::linux::paths::default_guest_uds_path(
        pplx_space_common_core::guest::linux::paths::SPACED_GUEST_SOCKET_FILE,
    )
}

use pplx_space_spaced_common::validate_optional_reason;

#[derive(Clone, Debug)]
pub struct SpacedGuestClient {
    inner: RawSpacedGuestClient,
    config: SpacedGuestClientConfig,
}

impl SpacedGuestClient {
    pub async fn new() -> Result<Self, ConnectError> {
        Self::new_with_config(SpacedGuestClientConfig::default()).await
    }

    pub async fn new_with_config(config: SpacedGuestClientConfig) -> Result<Self, ConnectError> {
        let channel = pplx_space_spaced_common::connect_uds_channel_with_retry(
            "spaced guest service",
            &config.socket_path,
            config.connect_retry_policy,
        )
        .await?;
        let inner = RawSpacedGuestClient::new(channel);
        Ok(Self { inner, config })
    }

    pub async fn ping(&mut self) -> Result<proto::PingResponse, tonic::Status> {
        self.call("ping", |mut client| async move {
            client.ping(PingRequest {}).await
        })
        .await
    }

    pub async fn set_activity_state(
        &mut self,
        state: ActivityState,
    ) -> Result<proto::SetActivityStateResponse, tonic::Status> {
        self.call("set_activity_state", |mut client| async move {
            client
                .set_activity_state(SetActivityStateRequest {
                    state: state.into(),
                })
                .await
        })
        .await
    }

    pub async fn notify_idle(&mut self) -> Result<proto::SetActivityStateResponse, tonic::Status> {
        self.set_activity_state(ActivityState::Idle).await
    }

    pub async fn notify_active(
        &mut self,
    ) -> Result<proto::SetActivityStateResponse, tonic::Status> {
        self.set_activity_state(ActivityState::Active).await
    }

    pub async fn request_active_for(
        &mut self,
        hold_for: std::time::Duration,
    ) -> Result<proto::RequestActiveForResponse, tonic::Status> {
        self.call("request_active_for", |mut client| async move {
            client
                .request_active_for(RequestActiveForRequest {
                    hold_for: Some(pplx_space_spaced_common::duration_to_proto(hold_for)?),
                })
                .await
        })
        .await
    }

    pub async fn request_shutdown(
        &mut self,
    ) -> Result<proto::RequestShutdownResponse, tonic::Status> {
        self.request_shutdown_with_reason(None).await
    }

    pub async fn request_shutdown_with_reason(
        &mut self,
        reason: Option<String>,
    ) -> Result<proto::RequestShutdownResponse, tonic::Status> {
        let reason = validate_optional_reason(reason)?;
        self.call("request_shutdown", |mut client| {
            let reason = reason.clone();
            async move {
                client
                    .request_shutdown(RequestShutdownRequest { reason })
                    .await
            }
        })
        .await
    }

    async fn call<T, F, Fut>(
        &mut self,
        command: &'static str,
        mut call: F,
    ) -> Result<T, tonic::Status>
    where
        F: FnMut(RawSpacedGuestClient) -> Fut,
        Fut: std::future::Future<Output = Result<Response<T>, tonic::Status>>,
    {
        pplx_space_spaced_common::retry_status(
            "spaced guest",
            command,
            self.config.command_retry_policy,
            || {
                let client = self.inner.clone();
                let response = call(client);
                async move { response.await.map(Response::into_inner) }
            },
        )
        .await
    }
}
