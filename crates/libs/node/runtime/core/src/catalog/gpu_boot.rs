//! Source-built boot inputs use a distinct identity from upstream CPU releases.
//! These pins authenticate artifacts; they do not qualify device ownership or reuse.
use super::{CatalogError, PinnedBootArtifact};
use pplx_space_common_core::types::{Arch, ArtifactComponentKind, ComponentIdentity};

pub(crate) const RUNTIME_VERSION: &str = "54.0.0+space.1";
pub(super) const RUNTIME_SHA256: &str =
    "e255b8139b69f21fe204e2e9436139f289a7ead8e632b0d197d4528a86506de4";
pub(super) const KERNEL_VERSION: &str = "6.1.155-space-gpu";
pub(super) const KERNEL_SHA256: &str =
    "5654b6098b1753729a8df4b19a7a19f48d84d511a704cf61b65773c4e5582c9b";
pub(super) const INITRAMFS_VERSION: &str = "6.1.155-nvidia-580.178.04";
pub(super) const INITRAMFS_SHA256: &str =
    "7bb3035adffbb30fb55d6429f93315d2ceb1be7818e9247ef13e3423cbfb0be0";

pub(super) fn initramfs() -> Result<PinnedBootArtifact, CatalogError> {
    let invalid = |field, value| CatalogError::Compiled {
        entry: "signed GPU initramfs".to_owned(),
        field,
        value,
    };
    PinnedBootArtifact::new(
        ComponentIdentity {
            kind: ArtifactComponentKind::Initrd,
            version: INITRAMFS_VERSION
                .parse()
                .map_err(|_| invalid("version", INITRAMFS_VERSION))?,
            digest: super::sha256_hex(INITRAMFS_SHA256)
                .ok_or_else(|| invalid("sha256", INITRAMFS_SHA256))?,
        },
        Arch::X86_64,
        31_737_680,
    )
    .map_err(|_| invalid("boot artifact", INITRAMFS_VERSION))
}
