use std::path::{Path, PathBuf};

use pplx_space_common_core::types::GuestUserName;
use tokio::process::Command;
use tonic::Status;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveGuestAccountError {
    #[error("failed to resolve user {user}: {source}")]
    Lookup {
        user: GuestUserName,
        #[source]
        source: nix::Error,
    },
    #[error("user not found: {0}")]
    NotFound(GuestUserName),
}

impl ResolveGuestAccountError {
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::NotFound(user) => Status::invalid_argument(format!("user not found: {user}")),
            error => Status::internal(error.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GuestAccount {
    name: GuestUserName,
    home: PathBuf,
    shell: PathBuf,
    uid: libc::uid_t,
    gid: libc::gid_t,
}

impl GuestAccount {
    pub(crate) fn resolve(name: GuestUserName) -> Result<Self, ResolveGuestAccountError> {
        let resolved = nix::unistd::User::from_name(name.as_str())
            .map_err(|source| ResolveGuestAccountError::Lookup {
                user: name.clone(),
                source,
            })?
            .ok_or_else(|| ResolveGuestAccountError::NotFound(name.clone()))?;
        Ok(Self {
            name,
            home: resolved.dir,
            shell: resolved.shell,
            uid: resolved.uid.as_raw(),
            gid: resolved.gid.as_raw(),
        })
    }

    pub(crate) fn name(&self) -> &GuestUserName {
        &self.name
    }

    pub(crate) fn ownership(&self) -> Result<(u32, u32), Status> {
        Ok((self.uid, self.gid))
    }

    pub(crate) fn home(&self) -> &Path {
        &self.home
    }

    pub(crate) fn shell(&self) -> &Path {
        &self.shell
    }

    pub(crate) fn configure_child_credentials(&self, _command: &mut Command) -> Result<(), Status> {
        use nix::unistd::{Gid, Uid};

        if Uid::effective().as_raw() == self.uid && Gid::effective().as_raw() == self.gid {
            Ok(())
        } else {
            Err(Status::failed_precondition(format!(
                "credential switching is implemented for the Linux guest only: {}",
                self.name
            )))
        }
    }
}
