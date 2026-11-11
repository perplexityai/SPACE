//! Official Cloud Hypervisor EDK2 release. Firmware is an immutable boot input,
//! distinct from both a guest kernel and the tenant's writable system disk.
use super::{CatalogError, PinnedBootArtifact};
use pplx_space_common_core::types::{Arch, ArtifactComponentKind, ComponentIdentity};

pub(super) const VERSION: &str = "ch-97eeb7b09";

pub(super) fn firmware() -> Result<PinnedBootArtifact, CatalogError> {
    const SHA256: &str = "dc2fc8f0e43b96712d9fccc52a3a590769606412b3e1bc911d217addd3bef624";
    // https://github.com/cloud-hypervisor/edk2/releases/tag/ch-97eeb7b09
    let invalid = |field, value| CatalogError::Compiled {
        entry: "CH EDK2 firmware".into(),
        field,
        value,
    };
    PinnedBootArtifact::new(
        ComponentIdentity {
            kind: ArtifactComponentKind::Firmware,
            version: VERSION.parse().map_err(|_| invalid("version", VERSION))?,
            digest: super::sha256_hex(SHA256).ok_or_else(|| invalid("sha256", SHA256))?,
        },
        Arch::X86_64,
        4_194_304,
    )
    .map_err(|_| invalid("firmware", VERSION))
}
