//! Store and execution-cache namespaces shared by publishers and inventory.
use std::path::{Path, PathBuf};

pub const BOOT_ARTIFACT_NAMESPACE: &str = "boot-artifacts";

pub fn boot_artifacts_root(kernels_root: &Path) -> PathBuf {
    kernels_root.with_file_name(BOOT_ARTIFACT_NAMESPACE)
}
