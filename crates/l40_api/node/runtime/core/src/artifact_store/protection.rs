use super::*;

/// Only execution-cache namespaces may live below shared sticky parents.
/// Durable publication and removal keep the stricter store-root contract.
#[derive(Clone, Copy)]
pub(super) enum RootProtection {
    Store,
    ExecutionCache,
    ExecutionCacheParent,
    ExecutionAncestors,
}

impl RootProtection {
    pub(super) fn check(self, root: &Path, owner: u32) -> Result<(), PublishError> {
        check_directories(root, owner, self).map_err(PublishError::from)
    }

    pub(super) fn verify(self, root: &Path, owner: u32) -> Result<(), VerifyError> {
        check_directories(root, owner, self).map_err(VerifyError::from)
    }
}

/// Execution consumers check the entire path, including protected sticky cache
/// ancestors. Store publication keeps its narrower, stricter layout contract.
pub fn verify_execution_directory(root: &Path, owner: u32) -> Result<(), VerifyError> {
    RootProtection::ExecutionAncestors.verify(root, owner)
}

/// Refuse a store root that another account could write or swap. Returns
/// the offending path and the problem.
pub(super) fn check_root(root: &Path, owner: u32) -> Result<(), DirectoryProtectionError> {
    check_directories(root, owner, RootProtection::Store)
}

fn check_directories(
    root: &Path,
    owner: u32,
    protection: RootProtection,
) -> Result<(), DirectoryProtectionError> {
    check_directories_with(root, owner, protection, |path| {
        std::fs::symlink_metadata(path)
    })
}

fn check_directories_with(
    root: &Path,
    owner: u32,
    protection: RootProtection,
    mut metadata: impl FnMut(&Path) -> std::io::Result<std::fs::Metadata>,
) -> Result<(), DirectoryProtectionError> {
    let depth = match protection {
        RootProtection::ExecutionAncestors => usize::MAX,
        _ => 3,
    };
    for path in root.ancestors().take(depth) {
        let metadata = metadata(path).map_err(|source| DirectoryProtectionError::Io {
            path: path.to_owned(),
            source,
        })?;
        let unsafe_path = |problem| DirectoryProtectionError::Unsafe {
            path: path.to_owned(),
            problem,
        };
        if metadata.file_type().is_symlink() {
            return Err(unsafe_path("a symlink"));
        }
        if !metadata.is_dir() {
            return Err(unsafe_path("not a directory"));
        }
        // Root-controlled ancestors also protect a store owned by a less
        // privileged publisher (including test fixtures). The leaf must still
        // belong to that publisher. Production execution uses owner = root.
        let trusted_root_ancestor = matches!(protection, RootProtection::ExecutionAncestors)
            && path != root
            && metadata.uid() == 0;
        if metadata.uid() != owner && !trusted_root_ancestor {
            return Err(unsafe_path("owned by another account"));
        }
        let permits_sticky = match protection {
            RootProtection::Store => false,
            RootProtection::ExecutionCache | RootProtection::ExecutionAncestors => path != root,
            RootProtection::ExecutionCacheParent => true,
        };
        if metadata.permissions().mode() & 0o022 != 0
            && !(permits_sticky && metadata.permissions().mode() & 0o1000 != 0)
        {
            return Err(unsafe_path("writable by its group or others"));
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(super) enum DirectoryProtectionError {
    #[error("artifact directory {} is {problem}", path.display())]
    Unsafe {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("inspect artifact directory {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl From<DirectoryProtectionError> for PublishError {
    fn from(error: DirectoryProtectionError) -> Self {
        match error {
            DirectoryProtectionError::Unsafe { path, problem } => {
                Self::UnsafeRoot { path, problem }
            }
            DirectoryProtectionError::Io { path, source } => Self::Io {
                action: "inspect artifact directory",
                path,
                source,
            },
        }
    }
}

impl From<DirectoryProtectionError> for VerifyError {
    fn from(error: DirectoryProtectionError) -> Self {
        match error {
            DirectoryProtectionError::Unsafe { path, problem } => {
                Self::UnsafeRoot { path, problem }
            }
            DirectoryProtectionError::Io { path, source } => Self::Io {
                action: "inspect artifact directory",
                path,
                source,
            },
        }
    }
}

impl From<DirectoryProtectionError> for RemoveError {
    fn from(error: DirectoryProtectionError) -> Self {
        match error {
            DirectoryProtectionError::Unsafe { path, problem } => {
                Self::UnsafeRoot { path, problem }
            }
            DirectoryProtectionError::Io { path, source } => Self::Io {
                action: "inspect artifact directory",
                path,
                source,
            },
        }
    }
}

#[cfg(test)]
mod tests;
