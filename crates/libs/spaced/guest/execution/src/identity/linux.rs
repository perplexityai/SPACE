use std::ffi::CString;
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::GuestUserName;
use tokio::process::Command;
use tonic::Status;

fn set_process_gid(gid: libc::gid_t) -> std::io::Result<()> {
    nix::unistd::setgid(nix::unistd::Gid::from_raw(gid)).map_err(std::io::Error::from)
}

fn set_process_uid(uid: libc::uid_t) -> std::io::Result<()> {
    nix::unistd::setuid(nix::unistd::Uid::from_raw(uid)).map_err(std::io::Error::from)
}

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
    #[error("failed to resolve supplementary groups for {user}: {source}")]
    SupplementaryGroups {
        user: GuestUserName,
        #[source]
        source: nix::Error,
    },
    #[error("guest user name contains an interior NUL byte: {0}")]
    InteriorNul(GuestUserName),
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
    supplementary_groups: Vec<nix::unistd::Gid>,
}

impl GuestAccount {
    pub(crate) fn resolve(name: GuestUserName) -> Result<Self, ResolveGuestAccountError> {
        let resolved = nix::unistd::User::from_name(name.as_str())
            .map_err(|source| ResolveGuestAccountError::Lookup {
                user: name.clone(),
                source,
            })?
            .ok_or_else(|| ResolveGuestAccountError::NotFound(name.clone()))?;
        let c_name = CString::new(name.as_str())
            .map_err(|_| ResolveGuestAccountError::InteriorNul(name.clone()))?;
        let supplementary_groups =
            nix::unistd::getgrouplist(&c_name, resolved.gid).map_err(|source| {
                ResolveGuestAccountError::SupplementaryGroups {
                    user: name.clone(),
                    source,
                }
            })?;
        Ok(Self {
            name,
            home: resolved.dir,
            shell: resolved.shell,
            uid: resolved.uid.as_raw(),
            gid: resolved.gid.as_raw(),
            supplementary_groups,
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

    pub(crate) fn assume_process_credentials(&self) -> std::io::Result<()> {
        nix::unistd::setgroups(&self.supplementary_groups)
            .map_err(|error| std::io::Error::from_raw_os_error(error as i32))?;
        // SAFETY: this runs in the worker subprocess before constructing its
        // Tokio runtime, so no sibling thread can observe partial credentials.
        set_process_gid(self.gid)?;
        set_process_uid(self.uid)?;
        Ok(())
    }

    pub(crate) fn configure_child_credentials(&self, command: &mut Command) -> Result<(), Status> {
        use nix::unistd::{Gid, Uid};

        // An unprivileged test harness can already run as the requested
        // identity, but cannot call setgroups. Reusing it is safe only if its
        // effective groups already match the account, including supplementary
        // access. Production root always installs the requested credentials.
        if !Uid::effective().is_root()
            && Uid::effective().as_raw() == self.uid
            && Gid::effective().as_raw() == self.gid
        {
            let current = nix::unistd::getgroups().map_err(|error| {
                Status::internal(format!("reading current supplementary groups: {error}"))
            })?;
            if !super::supplementary_groups_match(
                current,
                &self.supplementary_groups,
                Gid::from_raw(self.gid),
            ) {
                return Err(Status::failed_precondition(
                    "current supplementary groups do not match requested guest user",
                ));
            }
            return Ok(());
        }

        let uid = self.uid;
        let gid = self.gid;
        let supplementary_groups = self.supplementary_groups.clone();
        // SAFETY: the closure executes after fork and before exec. It uses only
        // precomputed IDs and async-signal-safe credential syscalls.
        unsafe {
            command.pre_exec(move || {
                nix::unistd::setgroups(&supplementary_groups)
                    .map_err(|error| std::io::Error::from_raw_os_error(error as i32))?;
                set_process_gid(gid)?;
                set_process_uid(uid)?;
                Ok(())
            });
        }
        Ok(())
    }
}
