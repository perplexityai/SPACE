//! Guest bridge and filesystem-worker wire contracts, including process,
//! filesystem, metadata, and networking services, plus their shared operations.
//! Host/control protocols and transport connectors belong to spaced-common.

pub mod proto {
    pub mod platform {
        tonic::include_proto!("space.spaced.platform.v1");
    }
    pub mod operations {
        tonic::include_proto!("space.spaced.operations.v1");
    }
    pub mod bridge_guest {
        // Legacy stat/list messages remain part of the deployed wire contract.
        #![allow(deprecated)]
        tonic::include_proto!("space.spaced.bridge.guest.v1");
        pub use super::operations::*;
    }
    pub mod worker {
        tonic::include_proto!("space.spaced.worker.v1");
    }
}

/// Host and guest endpoints of the shared vsock bridge.
pub const BRIDGE_HOST_VSOCK_PORT: u32 = 5001;
pub const BRIDGE_GUEST_VSOCK_PORT: u32 = 5002;

pub mod filesystem_limits;
pub mod platform;
pub mod process_limits;
pub mod validate;
pub mod wire;
pub use validate::*;
pub use wire::*;

pub fn invalid_argument_status(error: impl std::fmt::Display) -> tonic::Status {
    tonic::Status::invalid_argument(error.to_string())
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

#[cfg(test)]
mod tests;

pub mod admission_queue;

/// SemVer compatibility family used by guest artifacts and readiness.
pub fn spaced_compatibility_family_for_version(version: &str) -> Result<String, semver::Error> {
    let version = semver::Version::parse(version)?;
    Ok(compatibility_family(&version))
}

pub fn compatibility_family(version: &semver::Version) -> String {
    if version.major != 0 {
        version.major.to_string()
    } else if version.minor != 0 {
        format!("0.{}", version.minor)
    } else {
        format!("0.0.{}", version.patch)
    }
}

pub fn spaced_compatibility_family() -> String {
    spaced_compatibility_family_for_version(env!("CARGO_PKG_VERSION"))
        .expect("CARGO_PKG_VERSION is valid SemVer")
}
