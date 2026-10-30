//! Auxiliary boot files use the same root-owned copy-and-verify publication as
//! kernels. Their directory names and transport never establish trust.
use super::*;
use crate::catalog::PinnedBootArtifact;
use pplx_space_common_core::types::{ArtifactComponentKind, ComponentIdentity};

pub use crate::catalog::BOOT_ARTIFACT_IMAGE;

pub fn boot_artifact_staging(kind: ArtifactComponentKind) -> String {
    format!("components/{kind}")
}

pub fn boot_artifact_directory(identity: &ComponentIdentity) -> String {
    format!(
        "{}-{}",
        identity.kind,
        ManagedDirectoryName::kernel(&identity.version, identity.digest)
    )
}

impl ArtifactStore {
    pub fn boot_artifacts_root(&self) -> PathBuf {
        self.kernels_root.with_file_name("boot-artifacts")
    }

    pub fn verify_boot_artifact(
        &self,
        pin: &PinnedBootArtifact,
    ) -> Result<VerifiedArtifact, VerifyError> {
        let root = self.boot_artifacts_root();
        RootProtection::Store.verify(&root, self.owner)?;
        let _lock = lock_root(&root).map_err(|source| io_verify("lock", &root, source))?;
        self.verify_files(
            &root.join(boot_artifact_directory(pin.identity())),
            &[boot_file(pin)],
            pin.identity().digest,
            RootProtection::Store,
        )
    }

    pub fn publish_boot_artifact(
        &self,
        pin: &PinnedBootArtifact,
        staged: &StagedName,
    ) -> Result<Publication, PublishError> {
        let identity = pin.identity();
        if !store_version(&identity.version) {
            return Err(PublishError::InvalidVersion {
                version: identity.version.clone(),
            });
        }
        let root = self.boot_artifacts_root();
        RootProtection::Store.check(&root, self.owner)?;
        let _lock = lock_root(&root).map_err(|source| io_publish("lock", &root, source))?;
        self.publish(
            &root,
            &boot_artifact_directory(identity),
            PublishSource::Staging {
                namespace: &boot_artifact_staging(identity.kind),
                staged,
                directory: identity.version.as_str(),
            },
            &[boot_file(pin)],
            identity.digest,
            RootProtection::Store,
        )
    }

    /// Copy onto the jail filesystem even without fs-verity. Publication hashes
    /// the copy and never overwrites an existing inode; launch authenticates it
    /// again before linking the opened inode into the jail.
    pub fn stage_boot_artifact(
        &self,
        pin: &PinnedBootArtifact,
        parent: &Path,
    ) -> Result<Publication, PublishError> {
        let source = self.boot_artifacts_root();
        RootProtection::Store.check(&source, self.owner)?;
        let _source_lock =
            lock_root(&source).map_err(|error| io_publish("lock", &source, error))?;
        let cache = self.prepare_execution_namespace(parent, "boot-artifacts")?;
        let _cache_lock = lock_root(&cache).map_err(|error| io_publish("lock", &cache, error))?;
        let directory = boot_artifact_directory(pin.identity());
        let files = [boot_file(pin)];
        super::execution_cache::reclaim_empty_execution_target(
            &cache.join(&directory),
            &files,
            self.owner,
        )?;
        self.publish(
            &cache,
            &directory,
            PublishSource::Store {
                root: &source,
                directory: &directory,
            },
            &files,
            pin.identity().digest,
            RootProtection::ExecutionCache,
        )
    }

    pub fn remove_boot_artifact(
        &self,
        identity: &ComponentIdentity,
    ) -> Result<RemoveOutcome, RemoveError> {
        if !store_version(&identity.version) {
            return Err(RemoveError::InvalidVersion {
                version: identity.version.clone(),
            });
        }
        let root = self.boot_artifacts_root();
        check_root(&root, self.owner).map_err(RemoveError::from)?;
        let _lock = lock_root(&root).map_err(|source| io("lock", &root, source))?;
        let target = root.join(boot_artifact_directory(identity));
        if !exists(&target).map_err(|source| io("inspect", &target, source))? {
            return Ok(RemoveOutcome::Absent);
        }
        let image = target.join(BOOT_ARTIFACT_IMAGE);
        let metadata = std::fs::symlink_metadata(&image)
            .map_err(|source| removal_evidence_error("inspect", &image, source))?;
        if !metadata.is_file() || metadata.len() > self.max_file_bytes {
            return Err(RemoveError::IdentityChanged { path: image });
        }
        let digest =
            sha256_file(&image).map_err(|source| removal_evidence_error("hash", &image, source))?;
        if digest != identity.digest {
            return Err(RemoveError::IdentityChanged { path: image });
        }
        remove_directory(&root, &target)?;
        Ok(RemoveOutcome::Removed)
    }
}

pub(super) fn boot_file(pin: &PinnedBootArtifact) -> PlannedFile {
    PlannedFile {
        path: BOOT_ARTIFACT_IMAGE,
        sha256: pin.identity().digest,
        size: Some(pin.size()),
        mode: 0o644,
    }
}
