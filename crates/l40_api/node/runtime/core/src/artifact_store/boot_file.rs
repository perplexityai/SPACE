//! One publication/removal protocol for single-file boot inputs. Kernel aliases
//! remain part of the plan, so migration never strands historical directories.
use super::*;

pub(super) struct BootFilePlan<'a> {
    root: PathBuf,
    version: &'a ArtifactVersion,
    directories: Vec<String>,
    namespace: String,
    file: PlannedFile,
}

impl<'a> BootFilePlan<'a> {
    pub(super) fn kernel(store: &ArtifactStore, pin: &'a PinnedKernel) -> Self {
        Self {
            root: store.kernels_root.clone(),
            version: &pin.version,
            directories: kernel_directory_names(&pin.version, pin.sha256).to_vec(),
            namespace: kernel_staging(pin.runtime),
            file: PlannedFile {
                path: KERNEL_IMAGE,
                sha256: pin.sha256,
                size: Some(pin.size),
                mode: 0o644,
            },
        }
    }

    pub(super) fn auxiliary(
        store: &ArtifactStore,
        pin: &'a crate::catalog::PinnedBootArtifact,
    ) -> Self {
        Self {
            root: store.boot_artifacts_root(),
            version: &pin.identity().version,
            directories: vec![boot_artifact_directory(pin.identity())],
            namespace: boot_artifact_staging(pin.identity().kind),
            file: super::boot_artifact::boot_file(pin),
        }
    }

    fn locate(&self, root: &Path) -> Result<PathBuf, PublishError> {
        existing_artifact_path(root, &self.directories)
            .map_err(|source| io_publish("inspect", root, source))
    }
}

impl ArtifactStore {
    pub(super) fn publish_boot_file(
        &self,
        plan: &BootFilePlan<'_>,
        staged: &StagedName,
    ) -> Result<Publication, PublishError> {
        if !store_version(plan.version) {
            return Err(PublishError::InvalidVersion {
                version: plan.version.clone(),
            });
        }
        RootProtection::Store.check(&plan.root, self.owner)?;
        let _lock =
            lock_root(&plan.root).map_err(|source| io_publish("lock", &plan.root, source))?;
        let target = plan.locate(&plan.root)?;
        let files = std::slice::from_ref(&plan.file);
        if exists(&target).map_err(|source| io_publish("inspect", &target, source))? {
            return self.already_present(&target, files, plan.file.sha256, RootProtection::Store);
        }
        self.publish(
            &plan.root,
            plan.directories
                .first()
                .expect("boot-file plan has a primary directory"),
            PublishSource::Staging {
                namespace: &plan.namespace,
                staged,
                directory: plan.version.as_str(),
            },
            files,
            plan.file.sha256,
            RootProtection::Store,
        )
    }

    pub(super) fn stage_boot_file(
        &self,
        plan: &BootFilePlan<'_>,
        cache: &Path,
    ) -> Result<Publication, PublishError> {
        RootProtection::Store.check(&plan.root, self.owner)?;
        let _source_lock =
            lock_root(&plan.root).map_err(|source| io_publish("lock", &plan.root, source))?;
        RootProtection::ExecutionCache.check(cache, self.owner)?;
        let _cache_lock = lock_root(cache).map_err(|source| io_publish("lock", cache, source))?;
        let target = plan.locate(cache)?;
        let source = plan.locate(&plan.root)?;
        let files = std::slice::from_ref(&plan.file);
        // Retain the retryable cache-reconstruction error for a missing image
        // whose directory is obstructed. Never delete anything in that directory.
        if matches!(std::fs::symlink_metadata(target.join(plan.file.path)), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            && std::fs::symlink_metadata(&target).is_ok()
        {
            RootProtection::ExecutionCache.check(&target, self.owner)?;
            std::fs::remove_dir(&target).map_err(|source| {
                io_publish("remove empty execution cache directory", &target, source)
            })?;
        }
        self.publish(
            cache,
            target
                .file_name()
                .and_then(|s| s.to_str())
                .expect("catalog directory name"),
            PublishSource::Store {
                root: &plan.root,
                directory: source
                    .file_name()
                    .and_then(|s| s.to_str())
                    .expect("catalog directory name"),
            },
            files,
            plan.file.sha256,
            RootProtection::ExecutionCache,
        )
    }

    pub(super) fn remove_boot_file(
        &self,
        root: &Path,
        version: &ArtifactVersion,
        digest: Digest,
        image_name: &str,
        directories: &[String],
    ) -> Result<RemoveOutcome, RemoveError> {
        if !store_version(version) {
            return Err(RemoveError::InvalidVersion {
                version: version.clone(),
            });
        }
        check_root(root, self.owner).map_err(RemoveError::from)?;
        let _lock = lock_root(root).map_err(|source| io("lock", root, source))?;
        let mut verified = Vec::new();
        for name in directories {
            let target = root.join(name);
            if !exists(&target).map_err(|source| io("inspect", &target, source))? {
                continue;
            }
            let image = target.join(image_name);
            let metadata = std::fs::symlink_metadata(&image)
                .map_err(|source| removal_evidence_error("inspect", &image, source))?;
            if !metadata.is_file() || metadata.len() > self.max_file_bytes {
                return Err(RemoveError::IdentityChanged { path: image });
            }
            let found = sha256_file(&image)
                .map_err(|source| removal_evidence_error("hash", &image, source))?;
            if found != digest {
                return Err(RemoveError::IdentityChanged { path: image });
            }
            verified.push(target);
        }
        // Validate every alias before deleting any: a refused removal is non-mutating.
        for target in &verified {
            remove_directory(root, target)?;
        }
        Ok(if verified.is_empty() {
            RemoveOutcome::Absent
        } else {
            RemoveOutcome::Removed
        })
    }
}
