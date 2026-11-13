//! Spaced transport connectors, listeners, lazy clients, and retry policy.
//! The wire types are in `pplx-space-spaced-protocol` and
//! `pplx-space-spaced-guest-protocol`.
//!
//! Authenticated channels are generation-scoped. Retry hooks invalidate failed
//! channels even on the final attempt; mutations must opt out of replay unless
//! their owning service supplies idempotency.

/// HTTP/2 frame size the spaced channels advertise, so a 64 KiB file chunk
/// travels as one DATA frame instead of five.
pub const BULK_HTTP2_MAX_FRAME_SIZE: u32 = 256 * 1024;

mod error;
pub mod lazy_client;
pub mod retry_policy;
pub mod server;
pub mod uds;
pub mod vsock;

pub use error::ConnectError;
pub use lazy_client::{LazyClient, LazyClientLease};
pub use retry_policy::{
    DEFAULT_COMMAND_RETRY_POLICY, DEFAULT_CONNECT_RETRY_POLICY, REQUEST_DEADLINE_EXCEEDED_MESSAGE,
    RetryContext, RetryPolicy, retry_status, retry_status_with_context,
    retry_status_with_context_and_retry_hook, status_is_request_deadline_exceeded,
};
pub use server::{BindUdsError, bind_uds, bind_uds_with_mode};
pub use uds::{
    ExpectedUdsPeer, PinnedConnection, UdsConnectError, connect_uds_channel,
    connect_uds_channel_with_expected_peer, connect_uds_channel_with_expected_peer_and_retry,
    connect_uds_channel_with_retry, default_bridge_guest_uds_path, default_bridge_host_uds_path,
};
pub use vsock::{
    BRIDGE_GUEST_PING_MAX_DECODE_BYTES, FirecrackerVsockConnectError, VsockConnectError,
    bind_vsock, connect_firecracker_vsock_channel_with_expected_peer, connect_vsock_channel,
    connect_vsock_channel_with_retry, firecracker_vsock_handshake, firecracker_vsock_port_path,
};

/// Stamps out a `SpacedClientConfig`-style struct with the standard socket-path
/// + connect/command retry policy builders. Used by `spaced-host-client` and
///   `spaced-guest-client` to avoid duplicating the identical config shape.
///
/// Usage:
/// ```ignore
/// pplx_space_spaced_client::spaced_client_config!(
///     SpacedHostClientConfig,
///     crate::default_host_uds_path,
/// );
/// ```
#[macro_export]
macro_rules! spaced_client_config {
    ($name:ident, $default_path:path $(,)?) => {
        #[derive(Clone, Debug)]
        pub struct $name {
            pub socket_path: ::std::path::PathBuf,
            pub connect_retry_policy: $crate::RetryPolicy,
            pub command_retry_policy: $crate::RetryPolicy,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    socket_path: $default_path(),
                    connect_retry_policy: $crate::DEFAULT_CONNECT_RETRY_POLICY,
                    command_retry_policy: $crate::DEFAULT_COMMAND_RETRY_POLICY,
                }
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self::default()
            }

            pub fn with_socket_path(
                mut self,
                socket_path: impl ::std::convert::Into<::std::path::PathBuf>,
            ) -> Self {
                self.socket_path = socket_path.into();
                self
            }

            pub fn with_connect_retry_policy(mut self, retry_policy: $crate::RetryPolicy) -> Self {
                self.connect_retry_policy = retry_policy;
                self
            }

            pub fn with_command_retry_policy(mut self, retry_policy: $crate::RetryPolicy) -> Self {
                self.command_retry_policy = retry_policy;
                self
            }
        }
    };
}

/// Whether a hop should retry `status` itself: a transport-shaped failure,
/// unless the server refused the request before starting it. Such a refusal
/// (an admission limit, a guest shutting down) goes back to the caller, who
/// decides whether and when to retry.
pub fn status_is_retryable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::DeadlineExceeded
    ) && !status_is_refused_before_start(status)
}

fn status_is_refused_before_start(status: &tonic::Status) -> bool {
    status
        .metadata()
        .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        == Some(pplx_space_common_core::spaced::REQUEST_REFUSED)
}

#[cfg(test)]
mod tests;
