//! Shared spaced wire types, transport connectors, and retry policy.
//!
//! Protobufs are the compatibility boundary; validators are repeated at each
//! trust boundary. Authenticated channels are generation-scoped. Retry hooks
//! invalidate failed channels even on the final attempt; mutations must opt out
//! of replay unless their owning service supplies idempotency.

pub mod proto {
    pub mod host {
        tonic::include_proto!("space.spaced.host.v1");
        pub use super::operations::*;
    }

    pub mod guest {
        tonic::include_proto!("space.spaced.guest.v1");
        pub use super::activity::*;
    }

    pub mod worker {
        include!(concat!(
            env!("OUT_DIR"),
            "/worker/space.spaced.worker.v1.rs"
        ));
    }
    pub mod activity {
        tonic::include_proto!("space.spaced.activity.v1");
    }

    pub mod operations {
        tonic::include_proto!("space.spaced.operations.v1");
    }

    pub mod bridge_guest {
        // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: also requires MIN_HOST 0.1.4;
        // remove this allowance with the generated legacy endpoints.
        #![allow(deprecated)]
        tonic::include_proto!("space.spaced.bridge.guest.v1");

        pub use super::operations::*;
    }

    pub mod bridge_host {
        #![allow(clippy::large_enum_variant)]

        tonic::include_proto!("space.spaced.bridge.host.v1");

        pub use super::activity::{
            Accepted, ActiveForAccepted, ActivityState, Denied, RequestActiveForRequest,
            RequestActiveForResponse, RequestShutdownRequest, RequestShutdownResponse,
            SetActivityStateRequest, SetActivityStateResponse, request_active_for_response,
            request_shutdown_response, set_activity_state_response,
        };
        pub use super::operations::*;
    }

    pub mod host_control {
        tonic::include_proto!("space.spaced.host.control.v1");

        pub use super::activity::{
            Accepted, ActiveForAccepted, ActivityState, Denied, RequestActiveForRequest,
            RequestActiveForResponse, RequestShutdownRequest, RequestShutdownResponse,
            SetActivityStateRequest, SetActivityStateResponse, request_active_for_response,
            request_shutdown_response, set_activity_state_response,
        };
    }
}

impl std::fmt::Debug for proto::operations::SendInputRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SendInputRequest")
            .field("process_id", &self.process_id)
            .field("data", &"<redacted>")
            .finish()
    }
}

/// HTTP/2 frame size the spaced channels advertise, so a 64 KiB file chunk
/// travels as one DATA frame instead of five.
pub const BULK_HTTP2_MAX_FRAME_SIZE: u32 = 256 * 1024;

pub mod admission_queue;
mod compatibility;
mod error;
pub mod filesystem_limits;
pub mod lazy_client;
pub mod process_limits;
pub mod retry_policy;
pub mod server;
pub mod uds;
pub mod validate;
pub mod vsock;
mod wire;

pub use compatibility::{
    GuestCompatibilityError, SUPPORTED_SPACED_GUEST_VERSIONS, validate_spaced_guest_compatibility,
};
pub use error::{ConnectError, invalid_argument_status};
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
pub use validate::{
    validate_duration, validate_optional_duration, validate_optional_reason,
    validate_optional_timeout, validate_timeout,
};
pub use vsock::{
    BRIDGE_GUEST_PING_MAX_DECODE_BYTES, BRIDGE_GUEST_VSOCK_PORT, BRIDGE_HOST_VSOCK_PORT,
    FirecrackerVsockConnectError, VsockConnectError, bind_vsock,
    connect_firecracker_vsock_channel_with_expected_peer, connect_vsock_channel,
    connect_vsock_channel_with_retry, firecracker_vsock_handshake, firecracker_vsock_port_path,
};
pub use wire::{
    duration_to_proto, parse_env, parse_globs, proto_duration, proto_env, proto_globs,
    proto_idempotency_key, system_time_to_proto,
};

/// Stamps out a `SpacedClientConfig`-style struct with the standard socket-path
/// + connect/command retry policy builders. Used by `spaced-host-client` and
///   `spaced-guest-client` to avoid duplicating the identical config shape.
///
/// Usage:
/// ```ignore
/// pplx_space_spaced_common::spaced_client_config!(
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

pub fn spaced_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn spaced_compatibility_family() -> String {
    spaced_compatibility_family_for_version(spaced_version())
        .expect("CARGO_PKG_VERSION is valid SemVer")
}

pub fn spaced_compatibility_family_for_version(version: &str) -> Result<String, semver::Error> {
    let version = semver::Version::parse(version)?;
    Ok(compatibility::compatibility_family(&version))
}

// SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: remove with the host interactive
// control gate and its version tests once no older guest can run or be restored.
pub fn spaced_guest_supports_interactive_process_control(
    version: &str,
) -> Result<bool, semver::Error> {
    const MIN_INTERACTIVE_PROCESS_CONTROL_VERSION: semver::Version = semver::Version::new(0, 1, 2);

    let version = semver::Version::parse(version)?;
    Ok(version >= MIN_INTERACTIVE_PROCESS_CONTROL_VERSION)
}

#[cfg(test)]
mod tests;
