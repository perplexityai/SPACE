mod archive;
mod dependencies;
mod install;

use std::path::{Component, Path, PathBuf};
use std::process::Command;

pub use archive::GuestBundle;
pub use install::install_current_guest;
pub(crate) use install::install_working_directory;

use crate::command_as_root;

pub(super) const REQUIRED_PATHS: &[&str] = &[
    "bin/bash",
    "bin/sh",
    "etc/group",
    "etc/gshadow",
    "etc/passwd",
    "etc/shadow",
    "etc/ssh",
    "etc/systemd/system",
    "home/user",
    "root/.ssh",
    "sbin/init",
    "usr/bin/bash",
    "usr/bin/sh",
    "usr/bin/tar",
    "usr/lib/systemd",
    "usr/local/bin/spaced",
    "usr/local/bin/spaced-svc",
    "usr/sbin/sshd",
];

pub(super) const OPTIONAL_PATHS: &[&str] = &[
    "bin/mkdir",
    "bin/mknod",
    "bin/rm",
    "etc/fstab",
    "etc/hostname",
    "etc/hosts",
    "etc/ld.so.cache",
    "etc/ld.so.conf",
    "etc/ld.so.conf.d",
    "etc/nsswitch.conf",
    "etc/pam.conf",
    "etc/pam.d",
    "etc/resolv.conf",
    "etc/space",
    "etc/spaced",
    "etc/sudoers",
    "etc/sudoers.d",
    "etc/tmpfiles.d",
    "usr/bin/env",
    "usr/bin/scp",
    "usr/bin/ssh-keygen",
    "usr/bin/sudo",
    "usr/bin/systemctl",
    "usr/lib/openssh",
    "usr/libexec/sudo",
    "usr/local/sbin/space-configure-apt-mirror",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestBundleOperation {
    Inspect,
    Archive,
    Extract,
    Install,
}

impl GuestBundleOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect_guest_bundle",
            Self::Archive => "archive_guest_bundle",
            Self::Extract => "extract_guest_bundle",
            Self::Install => "install_guest_bundle",
        }
    }
}

impl std::fmt::Display for GuestBundleOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GuestBundleError {
    #[error("seed guest bundle is missing {path}")]
    MissingPath { path: PathBuf },
    #[error("{operation} failed for {path}: {source}")]
    Io {
        operation: GuestBundleOperation,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} exited unsuccessfully with code {code:?}")]
    Command {
        operation: GuestBundleOperation,
        code: Option<i32>,
    },
    #[error("guest bundle dependency path {path:?} is unsafe")]
    UnsafeDependency { path: String },
    #[error("guest bundle dependency {dependency} was not found")]
    MissingDependency { dependency: String },
    #[error("guest bundle target path {path} is unsafe")]
    UnsafeTarget { path: PathBuf },
    #[error("failed to parse ELF file {path}: {source}")]
    InvalidElf {
        path: PathBuf,
        #[source]
        source: object::Error,
    },
}

pub(super) fn normalize_relative_path(path: &Path) -> Result<PathBuf, GuestBundleError> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir if normalized.pop() => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(GuestBundleError::UnsafeDependency {
                    path: path.display().to_string(),
                });
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(GuestBundleError::UnsafeDependency {
            path: path.display().to_string(),
        });
    }
    Ok(normalized)
}

pub(super) fn privileged_command(program: &str, as_root: bool) -> Command {
    if as_root {
        command_as_root(program)
    } else {
        Command::new(program)
    }
}

pub(super) fn run_status(
    operation: GuestBundleOperation,
    command: &mut Command,
) -> Result<(), GuestBundleError> {
    let status = command.status().map_err(|source| GuestBundleError::Io {
        operation,
        path: PathBuf::new(),
        source,
    })?;
    if status.success() {
        Ok(())
    } else {
        Err(GuestBundleError::Command {
            operation,
            code: status.code(),
        })
    }
}

pub(super) fn path_arg(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
use dependencies::{add_dynamic_dependency_paths, parse_dynamic_dependencies};
#[cfg(test)]
use install::install_current_guest_with_privilege;

#[cfg(test)]
mod tests;
