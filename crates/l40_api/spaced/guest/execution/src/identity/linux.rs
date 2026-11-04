use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
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

    pub(crate) fn validate_policy(&self, policy: crate::IdentityPolicy) -> Result<(), Status> {
        if policy == crate::IdentityPolicy::Restricted && self.uid == 0 {
            return Err(Status::failed_precondition(
                "restricted identities require a non-root account",
            ));
        }
        Ok(())
    }

    pub(crate) fn assume_process_credentials(
        &self,
        policy: crate::IdentityPolicy,
    ) -> std::io::Result<()> {
        if policy == crate::IdentityPolicy::Restricted {
            return assume_restricted_credentials(self.uid, self.gid, &self.supplementary_groups);
        }
        nix::unistd::setgroups(&self.supplementary_groups)
            .map_err(|error| std::io::Error::from_raw_os_error(error as i32))?;
        // SAFETY: this runs in the worker subprocess before constructing its
        // Tokio runtime, so no sibling thread can observe partial credentials.
        set_process_gid(self.gid)?;
        set_process_uid(self.uid)?;
        Ok(())
    }

    pub(crate) fn configure_child_credentials(
        &self,
        command: &mut Command,
        policy: crate::IdentityPolicy,
        cwd: &Path,
    ) -> Result<(), Status> {
        use nix::unistd::{Gid, Uid};

        self.validate_policy(policy)?;
        if policy == crate::IdentityPolicy::Restricted {
            let cwd = CString::new(cwd.as_os_str().as_bytes())
                .map_err(|_| Status::invalid_argument("working directory contains a NUL byte"))?;
            let uid = self.uid;
            let gid = self.gid;
            let groups = self.supplementary_groups.clone();
            // Command::current_dir runs before pre_exec. Enter the directory
            // after dropping authority so every ancestor is checked as the user.
            // SAFETY: all storage is prepared before fork; the hook only uses
            // credential/capability syscalls and async-signal-safe chdir.
            unsafe {
                command.pre_exec(move || {
                    assume_restricted_credentials(uid, gid, &groups)?;
                    nix::unistd::chdir(cwd.as_c_str()).map_err(std::io::Error::from)
                });
            }
            return Ok(());
        }
        command.current_dir(cwd);

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

// Called in a forked child or after the worker entrypoint checks thread count.
// no_new_privs persists across subsequent execs.
// Bounding capabilities are a ceiling, not granted authority: clearing all
// active sets and forbidding exec gains leaves no capabilities to recover.
fn assume_restricted_credentials(
    uid: libc::uid_t,
    gid: libc::gid_t,
    groups: &[nix::unistd::Gid],
) -> std::io::Result<()> {
    use nix::unistd::{Gid, Uid};
    use rustix::thread::{CapabilitySet, CapabilitySets};

    // UID 0 retains ownership and signal authority even without capabilities.
    if uid == 0 {
        return Err(std::io::Error::from_raw_os_error(libc::EPERM));
    }
    rustix::thread::set_no_new_privs(true)?;
    nix::unistd::setgroups(groups)?;
    let uid = Uid::from_raw(uid);
    let gid = Gid::from_raw(gid);
    nix::unistd::setresgid(gid, gid, gid)?;
    nix::unistd::setresuid(uid, uid, uid)?;
    rustix::thread::clear_ambient_capability_set()?;
    let empty = CapabilitySets {
        effective: CapabilitySet::empty(),
        permitted: CapabilitySet::empty(),
        inheritable: CapabilitySet::empty(),
    };
    rustix::thread::set_capabilities(None, empty)?;
    let uids = nix::unistd::getresuid()?;
    let gids = nix::unistd::getresgid()?;
    if [uids.real, uids.effective, uids.saved] != [uid; 3]
        || [gids.real, gids.effective, gids.saved] != [gid; 3]
        || rustix::thread::capabilities(None)? != empty
        || !rustix::thread::no_new_privs()?
    {
        return Err(std::io::Error::from_raw_os_error(libc::EPERM));
    }
    Ok(())
}
