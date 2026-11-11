//! Independently replaceable, verity-sealed copies on the execution filesystem.

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum StageExecutionError {
    #[error("fs-verity API unavailable on execution filesystem {}", .0.display())]
    UnsupportedBackend(PathBuf),
    #[error("catalog-only staging requires a filesystem without fs-verity: {}", .0.display())]
    FallbackOnCapableBackend(PathBuf),
    #[error("inspect fs-verity API at {}: {source}", path.display())]
    BackendInspection {
        path: PathBuf,
        #[source]
        source: crate::verity::VerityError,
    },
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
    #[error("execution-filesystem support cache lock was poisoned; retry discovery")]
    SupportCachePoisoned,
    #[error("authenticate sealed execution file {}: {source}", path.display())]
    Verity {
        path: PathBuf,
        #[source]
        source: crate::verity::VerityError,
    },
}

impl StageExecutionError {
    /// Only capability detection may authorize legacy execution. File-level
    /// failures after selecting the sealed backend must never downgrade it.
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::UnsupportedBackend(_))
    }
    pub fn is_transient(&self) -> bool {
        match self {
            Self::SupportCachePoisoned => true,
            Self::Publish(error) => error.is_transient(),
            Self::Verify(error) => error.is_transient(),
            Self::BackendInspection {
                source: crate::verity::VerityError::Io { source, .. },
                ..
            }
            | Self::Verity {
                source: crate::verity::VerityError::Io { source, .. },
                ..
            } => pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source),
            Self::UnsupportedBackend(_)
            | Self::FallbackOnCapableBackend(_)
            | Self::Catalog(_)
            | Self::MissingPin(_)
            | Self::InvalidSource(_)
            | Self::BackendInspection { .. }
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

enum SealedSource<'a> {
    Directory {
        directory: &'a Path,
        protection: RootProtection,
    },
    Kernel(KernelSource<'a>),
}

impl<'a> SealedSource<'a> {
    fn directory(&self) -> Result<&'a Path, VerifyError> {
        match self {
            Self::Directory { directory, .. } => Ok(directory),
            Self::Kernel(source) => kernel_image_directory(source.image()),
        }
    }

    fn protection(&self) -> RootProtection {
        match self {
            Self::Directory { protection, .. } => *protection,
            Self::Kernel(source) => source.protection(),
        }
    }

    // An absent source must not cause cache creation or a capability query.
    // Trust checks belong to the selected backend, not capability detection.
    fn require_if_uncached(&self, target: &Path) -> Result<(), StageExecutionError> {
        if !exists(target).map_err(|error| io_publish("inspect execution cache", target, error))? {
            let directory = self.directory()?;
            std::fs::symlink_metadata(directory)
                .map_err(|error| io_publish("inspect execution source", directory, error))?;
        }
        Ok(())
    }

    // Recheck source protection under the namespace lock before publication.
    fn check_if_uncached(&self, target: &Path, owner: u32) -> Result<(), StageExecutionError> {
        if !exists(target).map_err(|error| io_publish("inspect execution cache", target, error))? {
            self.protection().check(self.directory()?, owner)?;
        }
        Ok(())
    }
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
    pub fn execution_release_path(
        parent: &Path,
        pin: &PinnedRelease,
    ) -> Result<PathBuf, PublishError> {
        let root = parent.join("managed/releases");
        existing_artifact_path(&root, &release_directory_names(&pin.release, pin.digest()))
            .map_err(|error| io_publish("locate execution release", &root, error))
    }

    /// Stage both binaries on the verity-capable execution filesystem. The
    /// durable source may remain on a filesystem without verity support.
    pub fn stage_release_at(
        &self,
        pin: &PinnedRelease,
        source: &Path,
        parent: &Path,
    ) -> Result<PathBuf, StageExecutionError> {
        let source = SealedSource::Directory {
            directory: source,
            protection: RootProtection::Store,
        };
        let target = Self::execution_release_path(parent, pin)?;
        source.require_if_uncached(&target)?;
        self.require_verity_api(parent)?;
        source.check_if_uncached(&target, self.owner)?;
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
            target
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| StageExecutionError::InvalidSource(target.clone()))?,
            source,
            &files,
            pin.digest(),
        )?;
        Ok(target)
    }

    /// Publish authenticated fresh inodes on the same unsupported filesystem
    /// the launcher will inspect. Never infer source-file integrity from the
    /// capability of a different filesystem or accept a caller downgrade flag.
    pub(crate) fn stage_catalog_release_at(
        &self,
        pin: &PinnedRelease,
        source: &Path,
        parent: &Path,
    ) -> Result<PathBuf, StageExecutionError> {
        let root = self.prepare_execution_namespace(parent, "releases")?;
        match self.require_verity_api(&root) {
            Err(StageExecutionError::UnsupportedBackend(_)) => {}
            Err(error) => return Err(error),
            Ok(()) => return Err(StageExecutionError::FallbackOnCapableBackend(root)),
        }
        RootProtection::Store.check(source, self.owner)?;
        let target = Self::execution_release_path(parent, pin)?;
        let invalid = || StageExecutionError::InvalidSource(source.to_owned());
        let source_parent = source.parent().ok_or_else(invalid)?;
        let source_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(invalid)?;
        let directory = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(invalid)?;
        let files = pin
            .binaries
            .iter()
            .map(|file| PlannedFile {
                path: file.path,
                sha256: file.sha256,
                size: None,
                mode: 0o755,
            })
            .collect::<Vec<_>>();
        let _lock =
            lock_root(&root).map_err(|error| io_publish("lock execution cache", &root, error))?;
        clear_root_leftovers(&root)
            .map_err(|error| io_publish("clear interrupted execution cache", &root, error))?;
        Ok(self
            .publish(
                &root,
                directory,
                PublishSource::Store {
                    root: source_parent,
                    directory: source_name,
                },
                &files,
                pin.digest(),
                RootProtection::ExecutionCache,
            )?
            .path)
    }

    pub fn stage_sealed_kernel_at(
        &self,
        pin: &PinnedKernel,
        source: KernelSource<'_>,
        parent: &Path,
    ) -> Result<PathBuf, StageExecutionError> {
        self.stage_sealed_boot_file(
            SealedSource::Kernel(source),
            Self::execution_kernel_path(parent, pin)?,
            parent,
            "kernels",
            PlannedFile {
                path: KERNEL_IMAGE,
                sha256: pin.sha256,
                size: Some(pin.size),
                mode: 0o644,
            },
        )
    }

    pub fn execution_boot_artifact_path(
        parent: &Path,
        pin: &crate::catalog::PinnedBootArtifact,
    ) -> PathBuf {
        parent
            .join("managed")
            .join(crate::artifact_layout::BOOT_ARTIFACT_NAMESPACE)
            .join(boot_artifact_directory(pin.identity()))
            .join(BOOT_ARTIFACT_IMAGE)
    }

    pub fn stage_sealed_boot_artifact(
        &self,
        pin: &crate::catalog::PinnedBootArtifact,
        parent: &Path,
    ) -> Result<PathBuf, StageExecutionError> {
        let directory = self
            .boot_artifacts_root()
            .join(boot_artifact_directory(pin.identity()));
        let source = SealedSource::Directory {
            directory: &directory,
            protection: RootProtection::Store,
        };
        self.stage_sealed_boot_file(
            source,
            Self::execution_boot_artifact_path(parent, pin),
            parent,
            crate::artifact_layout::BOOT_ARTIFACT_NAMESPACE,
            super::boot_artifact::boot_file(pin),
        )
    }

    fn stage_sealed_boot_file(
        &self,
        source: SealedSource<'_>,
        target: PathBuf,
        parent: &Path,
        namespace: &str,
        file: PlannedFile,
    ) -> Result<PathBuf, StageExecutionError> {
        source.require_if_uncached(&target)?;
        self.require_verity_api(parent)?;
        source.check_if_uncached(&target, self.owner)?;
        let root = self.prepare_execution_namespace(parent, namespace)?;
        let directory = target
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .ok_or_else(|| StageExecutionError::InvalidSource(target.clone()))?;
        let image = file.path;
        let digest = file.sha256;
        self.stage_sealed_files(
            &root,
            directory,
            source,
            &[SealedPlan::from_catalog(file)?],
            digest,
        )
        .map(|path| path.join(image))
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

    // Query an existing directory, never an artifact or a newly created probe.
    // Missing cache directories inherit their nearest existing ancestor's
    // filesystem. This read-only query may follow configured symlinks; it grants
    // no path trust. Publication checks the selected backend's destination policy.
    // Only definitive supported/unsupported observations are memoized within
    // this inventory rebuild; inspection failures cannot authorize fallback.
    fn require_verity_api(&self, root: &Path) -> Result<(), StageExecutionError> {
        let (path, directory) = root
            .ancestors()
            .find_map(|path| {
                match std::fs::File::options()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
                    .open(path)
                {
                    Ok(file) => Some(Ok((path, file))),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => Some(Err(io_publish("open execution filesystem", path, error))),
                }
            })
            .ok_or_else(|| StageExecutionError::InvalidSource(root.to_owned()))??;
        let device = directory
            .metadata()
            .map_err(|error| io_publish("inspect execution filesystem", path, error))?
            .dev();
        let mut support = self
            .verity_support
            .lock()
            .map_err(|_| StageExecutionError::SupportCachePoisoned)?;
        let supported = match support.get(&device) {
            Some(supported) => *supported,
            None => {
                let supported =
                    crate::verity::filesystem_has_verity_api(&directory).map_err(|source| {
                        StageExecutionError::BackendInspection {
                            path: path.to_owned(),
                            source,
                        }
                    })?;
                support.insert(device, supported);
                supported
            }
        };
        if supported {
            Ok(())
        } else {
            Err(StageExecutionError::UnsupportedBackend(path.to_owned()))
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
        let target = root.join(directory);
        // The caller checked source presence before preparing this namespace.
        // Query its actual filesystem too; it may differ from the cache parent.
        self.require_verity_api(root)?;
        let _lock =
            lock_root(root).map_err(|error| io_publish("lock execution cache", root, error))?;
        clear_root_leftovers(root)
            .map_err(|source| io_publish("clear interrupted execution cache", root, source))?;
        let planned_files: Vec<_> = files.iter().map(|plan| plan.file.clone()).collect();
        reclaim_empty_execution_target(&target, &planned_files, self.owner)?;
        let exists = exists(&target)
            .map_err(|error| io_publish("inspect execution cache", &target, error))?;
        let publication = if exists {
            PublishOutcome::AlreadyPresent
        } else {
            // A complete cache copy needs no source. An empty skeleton reclaimed
            // above does: validate its exact input layout before copying bytes.
            let source_directory = source.directory()?;
            source.protection().check(source_directory, self.owner)?;
            let source_parent = source_directory
                .parent()
                .ok_or_else(|| StageExecutionError::InvalidSource(source_directory.to_owned()))?;
            let source_name = source_directory
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| StageExecutionError::InvalidSource(source_directory.to_owned()))?;
            self.publish(
                root,
                directory,
                PublishSource::Store {
                    root: source_parent,
                    directory: source_name,
                },
                &planned_files,
                digest,
                RootProtection::ExecutionCache,
            )?
            .outcome
        };
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
                    // Sealing is irreversible. Recheck pre-existing mutable copies
                    // through the descriptor we will seal. Fresh publication already
                    // verified these bytes while copying under this namespace lock;
                    // only root can replace the protected destination before sealing.
                    if metadata.len() > self.max_file_bytes
                        || file.size.is_some_and(|size| metadata.len() != size)
                    {
                        return Err(VerifyError::InvalidFile {
                            path,
                            problem: "unsealed execution file has an invalid size",
                        }
                        .into());
                    }
                    if publication == PublishOutcome::AlreadyPresent {
                        let found =
                            sha256_reader((&opened).take(self.max_file_bytes.saturating_add(1)))
                                .map_err(|source| {
                                    io_verify("hash unsealed execution file", &path, source)
                                })?;
                        if found != file.sha256 {
                            return Err(VerifyError::Mismatch {
                                path,
                                found,
                                pinned: file.sha256,
                            }
                            .into());
                        }
                    }
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
pub(super) fn reclaim_empty_execution_target(
    target: &Path,
    files: &[PlannedFile],
    owner: u32,
) -> Result<(), PublishError> {
    if !exists(target).map_err(|error| io_publish("inspect execution cache", target, error))? {
        return Ok(());
    }
    RootProtection::ExecutionCache.check(target, owner)?;
    let mut directories = std::collections::BTreeSet::from([target.to_owned()]);
    for file in files {
        let path = Path::new(file.path);
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
