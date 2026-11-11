//! Negotiated guest operations. Platform semantics belong to trusted image policy.
use crate::proto::platform::*;
use tonic::Status;

pub const HOST_CONTRACT_REVISION: u32 = 1;
const MIN_NON_LINUX_HOST_CONTRACT_REVISION: u32 = 1;

/// Validated peer claims; callers must still intersect them with image policy.
#[derive(Clone, Debug)]
pub struct GuestPlatform(GuestPlatformDescriptor);

impl GuestPlatform {
    pub fn parse(wire: GuestPlatformDescriptor, expected: GuestOs) -> Result<Self, Status> {
        if expected == GuestOs::Unspecified || wire.os != expected as i32 {
            return Err(Status::failed_precondition(
                "guest platform contract mismatch",
            ));
        }
        if wire.operations.len() > 64 {
            return Err(Status::failed_precondition("too many guest capabilities"));
        }
        Ok(Self(wire))
    }

    pub fn supports(&self, operation: GuestOperation) -> bool {
        operation != GuestOperation::Unspecified && self.0.operations.contains(&(operation as i32))
    }

    pub fn wire(&self) -> GuestPlatformDescriptor {
        self.0.clone()
    }

    /// Existing Linux operations; this does not qualify VM snapshots.
    pub fn linux() -> Self {
        Self(GuestPlatformDescriptor {
            os: GuestOs::Linux as i32,
            operations: [
                GuestOperation::Process,
                GuestOperation::ProcessEnvironment,
                GuestOperation::InteractiveProcessControl,
                GuestOperation::Files,
                GuestOperation::FileWatch,
                GuestOperation::Flush,
                GuestOperation::Resume,
                GuestOperation::LoopbackForward,
            ]
            .map(|op| op as i32)
            .to_vec(),
        })
    }

    /// Non-Linux implementations must opt into operations as they are qualified.
    pub fn for_target() -> Self {
        if cfg!(target_os = "linux") {
            Self::linux()
        } else {
            Self(GuestPlatformDescriptor {
                os: target_os() as i32,
                operations: Vec::new(),
            })
        }
    }
}

pub const fn target_os() -> GuestOs {
    if cfg!(target_os = "linux") {
        GuestOs::Linux
    } else if cfg!(target_os = "windows") {
        GuestOs::Windows
    } else {
        GuestOs::Unspecified
    }
}

/// Linux retains its legacy handshake; non-Linux requires at least revision 1.
/// Future host revisions retain this additive baseline.
pub fn require_host_contract(os: GuestOs, revision: u32) -> Result<(), Status> {
    if os == GuestOs::Unspecified
        || (os != GuestOs::Linux && revision < MIN_NON_LINUX_HOST_CONTRACT_REVISION)
    {
        return Err(Status::failed_precondition(
            "host lacks the guest platform contract",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
