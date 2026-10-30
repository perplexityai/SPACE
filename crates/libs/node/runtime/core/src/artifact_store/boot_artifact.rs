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
        crate::artifact_layout::boot_artifacts_root(&self.kernels_root)
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
        self.publish_boot_file(&BootFilePlan::auxiliary(self, pin), staged)
    }

    /// Copy and authenticate fresh inodes on the execution filesystem.
    pub fn stage_boot_artifact(
        &self,
        pin: &PinnedBootArtifact,
        parent: &Path,
    ) -> Result<Publication, PublishError> {
        let cache = self
            .prepare_execution_namespace(parent, crate::artifact_layout::BOOT_ARTIFACT_NAMESPACE)?;
        self.stage_boot_file(&BootFilePlan::auxiliary(self, pin), &cache)
    }

    pub fn remove_boot_artifact(
        &self,
        identity: &ComponentIdentity,
    ) -> Result<RemoveOutcome, RemoveError> {
        self.remove_boot_file(
            &self.boot_artifacts_root(),
            &identity.version,
            identity.digest,
            BOOT_ARTIFACT_IMAGE,
            &[boot_artifact_directory(identity)],
        )
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
