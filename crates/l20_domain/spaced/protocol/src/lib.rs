//! Spaced host, guest, bridge-host and host-control wire types, and the
//! host's guest-compatibility rules. The guest bridge's own contracts are in
//! `pplx-space-spaced-guest-protocol`; the transport is in
//! `pplx-space-spaced-client`.
//!
//! Protobufs are the compatibility boundary; validators are repeated at each
//! trust boundary.

pub mod proto {
    pub mod host {
        tonic::include_proto!("space.spaced.host.v1");
        pub use pplx_space_spaced_guest_protocol::proto::operations::*;
    }

    pub mod guest {
        tonic::include_proto!("space.spaced.guest.v1");
        pub use super::activity::*;
    }

    pub mod activity {
        tonic::include_proto!("space.spaced.activity.v1");
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
        pub use pplx_space_spaced_guest_protocol::proto::operations::*;
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

mod compatibility;
pub use compatibility::spaced_guest_requires_platform_descriptor;

pub use compatibility::{
    GuestCompatibilityError, SUPPORTED_SPACED_GUEST_VERSIONS, validate_spaced_guest_compatibility,
};

pub fn spaced_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
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

/// Maximum operations qualified by an image, intersected with each connection's
/// advertised operations. New wire operations receive no implicit permission.
pub fn allowed_guest_operations(
    agent: pplx_space_execution_spec::guest_image::QualifiedGuestAgent,
) -> &'static [pplx_space_spaced_guest_protocol::proto::platform::GuestOperation] {
    use pplx_space_execution_spec::guest_image::QualifiedGuestAgent;
    use pplx_space_spaced_guest_protocol::proto::platform::GuestOperation::*;
    match agent {
        QualifiedGuestAgent::Absent => &[],
        QualifiedGuestAgent::LinuxV1 => &[
            Process,
            ProcessEnvironment,
            Files,
            FileWatch,
            Flush,
            Resume,
            LoopbackForward,
            InteractiveProcessControl,
        ],
        QualifiedGuestAgent::WindowsV1 => {
            &[Process, Files, Flush, Resume, InteractiveProcessControl]
        }
    }
}
