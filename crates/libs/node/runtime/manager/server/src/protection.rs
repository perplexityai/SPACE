//! Whether the paths the runtime manager relies on are protected from other
//! local accounts.
//!
//! The runtime manager reads the releases and kernels a node image ships,
//! asks node-manager to publish into a separate store, keeps private job
//! state, and reads
//! kernels that are staged into the shared cache. Each is safe only if no other account can
//! write it and no other account can rename it, or any directory above it,
//! out of the way. Provisioning establishes that; this module checks it, so a
//! node that was provisioned wrongly or not yet says so. It reports and never
//! repairs.

use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::{Component, Path, PathBuf};

const STICKY: u32 = 0o1000;
const GROUP_WRITE: u32 = 0o020;
const OTHER_WRITE: u32 = 0o002;
const GROUP_OR_OTHER_ANY: u32 = 0o077;
const GROUP_OR_OTHER_WRITE: u32 = GROUP_WRITE | OTHER_WRITE;
/// Entries walked beneath one protected root. A node carries a handful of
/// releases and kernels; the bound keeps a pathological tree from stalling a
/// status call.
const MAX_AUDITED_ENTRIES: usize = 4096;
/// Content problems reported per root before the rest are summarized.
const MAX_REPORTED_CONTENT_PROBLEMS: usize = 8;

/// What a protected path is for, which decides who may write it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PathRole {
    /// A root of releases or kernels in the node store: root-owned and
    /// writable by no other account, the runtime manager included, since
    /// install candidates and execution artifacts must both be immutable to
    /// unprivileged services. A compatibility link is audited at its target.
    InstallRoot,
    /// Staged into the shared cache by root-run node bootstrap: root-owned and
    /// writable by nobody else.
    RootManagedCache,
    /// The runtime manager's own state directory, which also holds its
    /// socket: owned by it and writable by no other account, its files
    /// private to it.
    StateDirectory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Protection {
    Protected,
    /// Not provisioned yet.
    Absent,
    /// Each entry names one way another account could write or displace it.
    Unprotected(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PathReport {
    pub(crate) path: PathBuf,
    pub(crate) role: PathRole,
    pub(crate) protection: Protection,
}

/// The accounts the audit trusts.
#[derive(Clone, Debug)]
pub(crate) struct AuditIdentity {
    /// Owner every directory above a protected path must have: root.
    pub(crate) trusted_uid: u32,
    /// The runtime manager's own account and primary group.
    pub(crate) service_uid: u32,
    pub(crate) service_gid: u32,
    /// Where the ancestor walk starts; `/` outside tests.
    pub(crate) audit_from: PathBuf,
}

impl AuditIdentity {
    pub(crate) fn of_this_process() -> Self {
        Self {
            trusted_uid: 0,
            service_uid: nix::unistd::geteuid().as_raw(),
            service_gid: nix::unistd::getegid().as_raw(),
            audit_from: PathBuf::from("/"),
        }
    }
}

/// Check `path` for `role`. Blocking: it stats every directory from
/// [`AuditIdentity::audit_from`] down to `path`.
pub(crate) fn audit(path: &Path, role: PathRole, identity: &AuditIdentity) -> PathReport {
    let report = |protection| PathReport {
        path: path.to_path_buf(),
        role,
        protection,
    };
    let target = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return report(Protection::Absent);
        }
        Err(error) => {
            return report(Protection::Unprotected(vec![format!(
                "{}: cannot inspect: {error}",
                path.display()
            )]));
        }
    };

    let mut problems = Vec::new();
    audit_ancestors(path, identity, &mut problems);
    // Only the link's directory can replace a link, and the ancestor walk
    // has audited it; what it names is audited as the root itself.
    let (path, target) = if role == PathRole::InstallRoot && target.file_type().is_symlink() {
        match std::fs::canonicalize(path).and_then(|resolved| {
            std::fs::symlink_metadata(&resolved).map(|metadata| (resolved, metadata))
        }) {
            Ok((resolved, metadata)) => {
                audit_ancestors(&resolved, identity, &mut problems);
                (resolved, metadata)
            }
            Err(error) => {
                problems.push(format!("{}: cannot resolve: {error}", path.display()));
                return report(Protection::Unprotected(problems));
            }
        }
    } else {
        (path.to_path_buf(), target)
    };
    let path = path.as_path();
    audit_target(path, &target, role, identity, &mut problems);
    if target.is_dir() && !target.file_type().is_symlink() {
        audit_contents(path, &target, role, identity, &mut problems);
    }
    report(if problems.is_empty() {
        Protection::Protected
    } else {
        Protection::Unprotected(problems)
    })
}

/// Every directory above `path` must be owned by the trusted account and must
/// not let anyone else rename the next directory down. Group write is allowed
/// only with the sticky bit and a trusted-owned child, which is how the
/// shared cache stays writable by its services without exposing the entries
/// root created in it.
fn audit_ancestors(path: &Path, identity: &AuditIdentity, problems: &mut Vec<String>) {
    let Ok(relative) = path.strip_prefix(&identity.audit_from) else {
        problems.push(format!(
            "{} is outside the audited tree {}",
            path.display(),
            identity.audit_from.display()
        ));
        return;
    };
    let mut ancestor = identity.audit_from.clone();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            problems.push(format!("{} is not a normalized path", path.display()));
            return;
        };
        let child = ancestor.join(name);
        match std::fs::symlink_metadata(&ancestor) {
            Ok(metadata) => {
                check_ancestor(&ancestor, &metadata, &child, identity, problems);
            }
            Err(error) => {
                problems.push(format!("{}: cannot inspect: {error}", ancestor.display()));
                return;
            }
        }
        ancestor = child;
    }
}

fn check_ancestor(
    ancestor: &Path,
    metadata: &std::fs::Metadata,
    child: &Path,
    identity: &AuditIdentity,
    problems: &mut Vec<String>,
) {
    let shown = ancestor.display();
    if metadata.file_type().is_symlink() {
        problems.push(format!("{shown} is a symlink"));
        return;
    }
    if metadata.uid() != identity.trusted_uid {
        problems.push(format!(
            "{shown} is owned by uid {}, so its owner can replace {}",
            metadata.uid(),
            child.display()
        ));
    }
    let mode = metadata.mode();
    if mode & OTHER_WRITE != 0 {
        problems.push(format!("{shown} is writable by every account"));
    }
    if mode & GROUP_WRITE != 0 {
        if mode & STICKY == 0 {
            problems.push(format!(
                "{shown} is group-writable without the sticky bit, so group gid {} can rename {}",
                metadata.gid(),
                child.display()
            ));
        } else {
            match std::fs::symlink_metadata(child) {
                Ok(child_metadata) if child_metadata.uid() != identity.trusted_uid => {
                    problems.push(format!(
                        "{} is owned by uid {} inside group-writable {shown}",
                        child.display(),
                        child_metadata.uid()
                    ));
                }
                Ok(_) => {}
                // A missing child is the target's own problem; the target
                // check reports it.
                Err(_) => {}
            }
        }
    }
}

/// A protected root is only as safe as what is inside it: a kernel image
/// another account can write is as exposed as a writable directory. Walks the
/// tree without following symlinks (the version aliases) or leaving the root's
/// filesystem, and requires every directory and file to be owned by an account
/// allowed to write there and closed to group and other writes.
fn audit_contents(
    root: &Path,
    root_metadata: &std::fs::Metadata,
    role: PathRole,
    identity: &AuditIdentity,
    problems: &mut Vec<String>,
) {
    let owner_allowed = |uid: u32| match role {
        PathRole::InstallRoot => uid == identity.trusted_uid,
        PathRole::RootManagedCache => uid == identity.trusted_uid,
        PathRole::StateDirectory => uid == identity.service_uid,
    };
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    let mut visited = 0usize;
    while let Some(dir) = pending.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                found.push(format!("{}: cannot list: {error}", dir.display()));
                continue;
            }
        };
        for entry in entries {
            visited += 1;
            if visited > MAX_AUDITED_ENTRIES {
                found.push(format!(
                    "{} has more than {MAX_AUDITED_ENTRIES} entries; the rest were not audited",
                    root.display()
                ));
                pending.clear();
                break;
            }
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(error) => {
                    found.push(format!("{}: cannot list: {error}", dir.display()));
                    break;
                }
            };
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) => {
                    found.push(format!("{}: cannot inspect: {error}", path.display()));
                    continue;
                }
            };
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                continue;
            }
            let shown = path.display();
            if metadata.dev() != root_metadata.dev() {
                found.push(format!("{shown} is on another filesystem"));
                continue;
            }
            // The runtime manager's socket lives beside its state; who may
            // connect is the socket's mode and group, by design.
            let socket = role == PathRole::StateDirectory && file_type.is_socket();
            if !file_type.is_dir() && !file_type.is_file() && !socket {
                found.push(format!("{shown} is not a regular file or directory"));
                continue;
            }
            if !owner_allowed(metadata.uid()) {
                found.push(format!(
                    "{shown} is owned by uid {}, which can rewrite it",
                    metadata.uid()
                ));
            }
            if socket {
                continue;
            }
            if metadata.mode() & GROUP_OR_OTHER_WRITE != 0 {
                found.push(format!(
                    "{shown} has mode {:o}, writable by accounts other than its owner",
                    metadata.mode() & 0o7777
                ));
            }
            if role == PathRole::StateDirectory
                && file_type.is_file()
                && metadata.mode() & GROUP_OR_OTHER_ANY != 0
            {
                found.push(format!(
                    "{shown} has mode {:o}; state must be private to the runtime manager",
                    metadata.mode() & 0o7777
                ));
            }
            if file_type.is_dir() {
                pending.push(path);
            }
        }
    }
    let hidden = found.len().saturating_sub(MAX_REPORTED_CONTENT_PROBLEMS);
    problems.extend(found.into_iter().take(MAX_REPORTED_CONTENT_PROBLEMS));
    if hidden > 0 {
        problems.push(format!("and {hidden} more under {}", root.display()));
    }
}

fn audit_target(
    path: &Path,
    metadata: &std::fs::Metadata,
    role: PathRole,
    identity: &AuditIdentity,
    problems: &mut Vec<String>,
) {
    let shown = path.display();
    if metadata.file_type().is_symlink() {
        problems.push(format!("{shown} is a symlink"));
        return;
    }
    if !metadata.is_dir() {
        problems.push(format!("{shown} is not a directory"));
        return;
    }
    let mode = metadata.mode();
    match role {
        PathRole::InstallRoot => {
            if metadata.uid() != identity.trusted_uid {
                problems.push(format!(
                    "{shown} is owned by uid {}, not root",
                    metadata.uid()
                ));
            }
            if mode & GROUP_WRITE != 0 {
                problems.push(format!(
                    "{shown} is writable by group gid {}; only root may write the node store",
                    metadata.gid()
                ));
            }
            if mode & OTHER_WRITE != 0 {
                problems.push(format!("{shown} is writable by every account"));
            }
        }
        PathRole::RootManagedCache => {
            if metadata.uid() != identity.trusted_uid {
                problems.push(format!("{shown} is owned by uid {}", metadata.uid()));
            }
            if mode & GROUP_OR_OTHER_WRITE != 0 {
                problems.push(format!(
                    "{shown} has mode {:o}, writable by accounts other than its owner",
                    mode & 0o7777
                ));
            }
        }
        PathRole::StateDirectory => {
            if metadata.uid() != identity.service_uid {
                problems.push(format!(
                    "{shown} is owned by uid {}, not the runtime manager's uid {}",
                    metadata.uid(),
                    identity.service_uid
                ));
            }
            // Clients traverse it to reach the socket; nobody else writes it.
            if mode & GROUP_OR_OTHER_WRITE != 0 {
                problems.push(format!(
                    "{shown} has mode {:o}, writable by accounts other than the runtime manager",
                    mode & 0o7777
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests;
