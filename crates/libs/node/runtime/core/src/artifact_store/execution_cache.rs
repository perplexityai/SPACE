//! Independently replaceable, verity-sealed copies on the execution filesystem.

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum StageExecutionError {
    #[error("compiled execution integrity catalog: {0}")]
    Catalog(#[from] &'static crate::catalog::CatalogError),
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error(transparent)]
    Verify(#[from] VerifyError),
    #[error("execution artifact has no trusted verity pin: {0}")]
    MissingPin(Digest),
    #[error("invalid execution source path {}", .0.display())]
    InvalidSource(PathBuf),
    #[error("execution-filesystem support probe was interrupted; retry discovery")]
    SupportProbePoisoned,
    #[error("authenticate sealed execution file {}: {source}", path.display())]
    Verity {
        path: PathBuf,
        #[source]
        source: crate::verity::VerityError,
    },
}

impl StageExecutionError {
    pub fn is_unsupported(&self) -> bool {
        matches!(
            self,
            Self::Verity {
                source: crate::verity::VerityError::Unsupported,
                ..
            }
        )
    }
    pub fn is_transient(&self) -> bool {
        match self {
            Self::SupportProbePoisoned => true,
            Self::Publish(error) => error.is_transient(),
            Self::Verify(error) => error.is_transient(),
            Self::Verity {
                source: crate::verity::VerityError::Io { source, .. },
                ..
            } => pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source),
            Self::Catalog(_)
            | Self::MissingPin(_)
            | Self::InvalidSource(_)
            | Self::Verity { .. } => false,
        }
    }
}

/// The caller names the source's trust domain, independently of its location.
#[derive(Clone, Copy)]
pub enum KernelSource<'a> {
    Store(&'a Path),
    ExecutionCache(&'a Path),
}

impl<'a> KernelSource<'a> {
    fn image(self) -> &'a Path {
        match self {
            Self::Store(path) | Self::ExecutionCache(path) => path,
        }
    }

    fn protection(self) -> RootProtection {
        match self {
            Self::Store(_) => RootProtection::Store,
            Self::ExecutionCache(_) => RootProtection::ExecutionCache,
        }
    }
}

struct SealedSource<'a> {
    directory: &'a Path,
    protection: RootProtection,
}

struct SealedPlan {
    file: PlannedFile,
    expected: crate::verity::VerityDigest,
}

impl SealedPlan {
    fn from_catalog(file: PlannedFile) -> Result<Self, StageExecutionError> {
        let expected = crate::catalog::compiled_verity_digest(file.sha256)?
            .ok_or(StageExecutionError::MissingPin(file.sha256))?;
        Ok(Self { file, expected })
    }
}

impl ArtifactStore {
    pub fn execution_release_path(parent: &Path, pin: &PinnedRelease) -> PathBuf {
        parent
            .join("managed/releases")
            .join(managed_release_directory(&pin.release, pin.digest()))
    }

    /// Stage both binaries on the verity-capable execution filesystem. The
    /// durable source may remain on a filesystem without verity support.
    pub fn stage_release_at(
        &self,
        pin: &PinnedRelease,
        source: &Path,
        parent: &Path,
    ) -> Result<PathBuf, StageExecutionError> {
        let files = pin
            .binaries
            .iter()
            .map(|file| {
                SealedPlan::from_catalog(PlannedFile {
                    path: file.path,
                    sha256: file.sha256,
                    size: None,
                    mode: 0o755,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let root = self.prepare_execution_namespace(parent, "releases")?;
        let target = self.stage_sealed_files(
            &root,
            &managed_release_directory(&pin.release, pin.digest()),
            SealedSource {
                directory: source,
                protection: RootProtection::Store,
            },
            &files,
            pin.digest(),
        )?;
        Ok(target)
    }

    pub fn stage_sealed_kernel_at(
        &self,
        pin: &PinnedKernel,
        source: KernelSource<'_>,
        parent: &Path,
    ) -> Result<PathBuf, StageExecutionError> {
        let image = source.image();
        let directory = image
            .parent()
            .ok_or_else(|| StageExecutionError::InvalidSource(image.to_owned()))?;
        let root = self.prepare_execution_namespace(parent, "kernels")?;
        let target = self.stage_sealed_files(
            &root,
            &managed_kernel_directory(&pin.version, pin.sha256),
            SealedSource {
                directory,
                protection: source.protection(),
            },
            &[SealedPlan::from_catalog(PlannedFile {
                path: KERNEL_IMAGE,
                sha256: pin.sha256,
                size: Some(pin.size),
                mode: 0o644,
            })?],
            pin.sha256,
        )?;
        Ok(target.join(KERNEL_IMAGE))
    }

    pub(super) fn prepare_execution_namespace(
        &self,
        parent: &Path,
        namespace: &str,
    ) -> Result<PathBuf, PublishError> {
        RootProtection::ExecutionCacheParent.check(parent, self.owner)?;
        let managed = parent.join("managed");
        let root = managed.join(namespace);
        for path in [&managed, &root] {
            match std::fs::DirBuilder::new().mode(0o755).create(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => return Err(io_publish("create execution namespace", path, source)),
            }
            RootProtection::ExecutionCache.check(path, self.owner)?;
        }
        Ok(root)
    }

    // Memoize within one inventory rebuild, keyed by the destination device.
    // A later rebuild probes again, including after mounts or host support change.
    fn require_verity_support(&self, root: &Path) -> Result<(), StageExecutionError> {
        let device = std::fs::metadata(root)
            .map_err(|error| io_publish("inspect execution filesystem", root, error))?
            .dev();
        let unsupported = || StageExecutionError::Verity {
            path: root.to_owned(),
            source: crate::verity::VerityError::Unsupported,
        };
        let mut support = self
            .verity_support
            .lock()
            .map_err(|_| StageExecutionError::SupportProbePoisoned)?;
        if let Some(supported) = support.get(&device) {
            return if *supported {
                Ok(())
            } else {
                Err(unsupported())
            };
        }
        let probe = root.join(format!("{INCOMING_PREFIX}verity-probe-{}", unique_suffix()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&probe)
            .map_err(|error| io_publish("create verity probe", &probe, error))?;
        let _cleanup = IncomingWork(probe.clone());
        let path = probe.join("probe");
        drop(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .map_err(|error| io_publish("create verity probe file", &path, error))?,
        );
        let result = crate::verity::open_readonly(&path).and_then(|file| {
            match crate::verity::measure(&file) {
                Err(crate::verity::VerityError::Unsealed) => crate::verity::enable(&file),
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            }
        });
        match result {
            Ok(()) => {
                support.insert(device, true);
                Ok(())
            }
            Err(crate::verity::VerityError::Unsupported) => {
                support.insert(device, false);
                Err(unsupported())
            }
            Err(source) => Err(StageExecutionError::Verity { path, source }),
        }
    }

    fn stage_sealed_files(
        &self,
        root: &Path,
        directory: &str,
        source: SealedSource<'_>,
        files: &[SealedPlan],
        digest: Digest,
    ) -> Result<PathBuf, StageExecutionError> {
        let _lock =
            lock_root(root).map_err(|error| io_publish("lock execution cache", root, error))?;
        clear_root_leftovers(root)
            .map_err(|source| io_publish("clear interrupted execution cache", root, source))?;
        let target = root.join(directory);
        reclaim_empty_execution_target(&target, files, self.owner)?;
        let exists = exists(&target)
            .map_err(|error| io_publish("inspect execution cache", &target, error))?;
        if !exists {
            self.require_verity_support(root)?;
            let source_parent = source
                .directory
                .parent()
                .ok_or_else(|| StageExecutionError::InvalidSource(source.directory.to_owned()))?;
            let source_name = source
                .directory
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| StageExecutionError::InvalidSource(source.directory.to_owned()))?;
            source.protection.check(source.directory, self.owner)?;
            self.publish(
                root,
                directory,
                PublishSource::Store {
                    root: source_parent,
                    directory: source_name,
                },
                &files
                    .iter()
                    .map(|plan| plan.file.clone())
                    .collect::<Vec<_>>(),
                digest,
                RootProtection::ExecutionCache,
            )?;
        }
        // Seal existing copies in place, preserving inode identity and links.
        // Publication can be interrupted before sealing; no inventory exposes
        // this namespace until every required measurement has been checked.
        let mut sealed = false;
        for plan in files {
            let file = &plan.file;
            let path = target.join(file.path);
            verify_execution_directory(path.parent().unwrap_or(&target), self.owner)?;
            let opened = crate::verity::open_readonly(&path).map_err(|source| {
                StageExecutionError::Verity {
                    path: path.clone(),
                    source,
                }
            })?;
            let metadata = opened
                .metadata()
                .map_err(|source| io_verify("inspect execution file", &path, source))?;
            if !metadata.is_file()
                || metadata.uid() != self.owner
                || metadata.mode() & 0o022 != 0
                || metadata.mode() & file.mode & 0o111 != file.mode & 0o111
            {
                return Err(VerifyError::InvalidFile {
                    path,
                    problem: "unsafe execution file ownership, permissions or type",
                }
                .into());
            }
            match crate::verity::measure(&opened) {
                Ok(actual) if actual == plan.expected => {}
                Ok(actual) => {
                    return Err(StageExecutionError::Verity {
                        path,
                        source: crate::verity::VerityError::Mismatch {
                            expected: plan.expected,
                            actual,
                        },
                    });
                }
                Err(crate::verity::VerityError::Unsealed) => {
                    crate::verity::seal(&opened, plan.expected).map_err(|source| {
                        StageExecutionError::Verity {
                            path: path.clone(),
                            source,
                        }
                    })?;
                    opened
                        .sync_all()
                        .map_err(|source| io_verify("sync sealed file", &path, source))?;
                    sealed = true;
                }
                Err(source) => return Err(StageExecutionError::Verity { path, source }),
            }
        }
        if sealed {
            sync_dir(&target)
                .map_err(|source| io_publish("sync sealed directory", &target, source))?;
        }
        Ok(target)
    }
}

// Only an empty planned directory skeleton is disposable. Never unlink an
// existing file, follow a symlink, or descend into an unexpected directory.
fn reclaim_empty_execution_target(
    target: &Path,
    files: &[SealedPlan],
    owner: u32,
) -> Result<(), PublishError> {
    if !exists(target).map_err(|error| io_publish("inspect execution cache", target, error))? {
        return Ok(());
    }
    RootProtection::ExecutionCache.check(target, owner)?;
    let mut directories = std::collections::BTreeSet::from([target.to_owned()]);
    for plan in files {
        let path = Path::new(plan.file.path);
        for parent in path
            .ancestors()
            .skip(1)
            .filter(|path| !path.as_os_str().is_empty())
        {
            directories.insert(target.join(parent));
        }
    }
    let mut existing = Vec::new();
    for directory in &directories {
        let metadata = match std::fs::symlink_metadata(directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_publish("inspect execution directory", directory, error)),
        };
        if !metadata.is_dir() {
            return Ok(());
        }
        RootProtection::ExecutionCache.check(directory, owner)?;
        for entry in std::fs::read_dir(directory)
            .map_err(|error| io_publish("inspect execution directory", directory, error))?
        {
            let entry =
                entry.map_err(|error| io_publish("inspect execution entry", directory, error))?;
            if !directories.contains(&entry.path())
                || !entry
                    .file_type()
                    .map_err(|error| io_publish("inspect execution entry", &entry.path(), error))?
                    .is_dir()
            {
                return Ok(());
            }
        }
        existing.push(directory);
    }
    // Children follow their parents in path order. Every entry was checked
    // before mutation, so a partial release or an unknown entry stays intact.
    for directory in existing.into_iter().rev() {
        std::fs::remove_dir(directory)
            .map_err(|error| io_publish("reclaim empty execution directory", directory, error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
