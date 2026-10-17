use std::path::Path;

use pplx_space_common_core::types::GuestUserName;
use tokio::process::Command;
use tonic::Status;

#[derive(Debug, thiserror::Error)]
#[error("user switching is not supported on this platform: {0}")]
pub(crate) struct ResolveGuestAccountError(GuestUserName);

impl ResolveGuestAccountError {
    pub(crate) fn into_status(self) -> Status {
        Status::failed_precondition(self.to_string())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GuestAccount {
    name: GuestUserName,
}

impl GuestAccount {
    pub(crate) fn resolve(name: GuestUserName) -> Result<Self, ResolveGuestAccountError> {
        Err(ResolveGuestAccountError(name))
    }

    pub(crate) fn name(&self) -> &GuestUserName {
        &self.name
    }

    pub(crate) fn ownership(&self) -> Result<(u32, u32), Status> {
        Err(Status::failed_precondition(
            "user switching is not supported on this platform",
        ))
    }

    pub(crate) fn home(&self) -> &Path {
        Path::new("/")
    }

    pub(crate) fn shell(&self) -> &Path {
        Path::new("")
    }

    pub(crate) fn configure_child_credentials(&self, _command: &mut Command) -> Result<(), Status> {
        Err(Status::failed_precondition(format!(
            "user switching is not supported on this platform: {}",
            self.name
        )))
    }
}
