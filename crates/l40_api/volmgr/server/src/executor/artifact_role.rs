//! Which snapshot artifact a transfer carries, and the materialization mode and block size it uses.

use super::*;

pub(super) const MEMORY_SPARSE_BLOCK_BYTES: usize = 4 * 1024;

pub(super) const ROOTFS_SPARSE_BLOCK_BYTES: usize = 64 * 1024;

pub(super) fn transfer_artifact_name(local_path: &Path) -> Option<&str> {
    let name = local_path.file_name()?.to_str()?;
    let name = name.strip_prefix('.').unwrap_or(name);
    Some(name.split(".restore-").next().unwrap_or(name))
}

pub(crate) fn snapshot_artifact_from_local_path(local_path: &Path) -> Option<SnapshotArtifact> {
    match transfer_artifact_name(local_path)? {
        "snapshot.json" => Some(SnapshotArtifact::Metadata),
        "vmstate" => Some(SnapshotArtifact::VmState),
        "memory" => Some(SnapshotArtifact::Memory),
        "rootfs.ext4" => Some(SnapshotArtifact::Rootfs),
        "rootfs.btrfs-send" => Some(SnapshotArtifact::RootfsBtrfsSend),
        "snapshot.luks" => Some(SnapshotArtifact::EncryptedSnapshotVolume),
        "cow.luks" => Some(SnapshotArtifact::EncryptedRootfsCow),
        _ => None,
    }
}

pub(crate) fn snapshot_artifact_from_entry_role(role: &str) -> Option<SnapshotArtifact> {
    [
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::RootfsBtrfsSend,
        SnapshotArtifact::Memory,
        SnapshotArtifact::VmState,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
        SnapshotArtifact::Metadata,
    ]
    .into_iter()
    .find(|artifact| artifact.file_name() == role)
}

pub(super) fn transfer_artifact_tag(artifact: Option<SnapshotArtifact>) -> &'static str {
    match artifact {
        Some(SnapshotArtifact::Metadata) => "metadata",
        Some(SnapshotArtifact::VmState) => "vmstate",
        Some(SnapshotArtifact::Memory) => "memory",
        Some(SnapshotArtifact::Rootfs) => "rootfs_ext4",
        Some(SnapshotArtifact::RootfsBtrfsSend) => "rootfs_btrfs_send",
        Some(SnapshotArtifact::EncryptedSnapshotVolume) => "encrypted_snapshot_volume",
        Some(SnapshotArtifact::EncryptedRootfsCow) => "encrypted_rootfs_cow",
        None => "other",
    }
}

pub(super) fn effective_materialization_mode(
    artifact: Option<SnapshotArtifact>,
    requested: MaterializationMode,
) -> MaterializationMode {
    if matches!(
        artifact,
        Some(
            SnapshotArtifact::Memory
                | SnapshotArtifact::Rootfs
                | SnapshotArtifact::EncryptedSnapshotVolume
                | SnapshotArtifact::EncryptedRootfsCow
        )
    ) {
        requested
    } else {
        MaterializationMode::Disabled
    }
}

pub(super) fn execution_materialization_mode(
    artifact: Option<SnapshotArtifact>,
    requested: MaterializationMode,
) -> MaterializationMode {
    // LUKS backings contain literal zero regions outside their written encrypted
    // sectors. Exact-zero materialization preserves the ciphertext and logical
    // length, including when retrying jobs persisted before sparse support.
    let requested = if matches!(
        artifact,
        Some(
            SnapshotArtifact::Memory
                | SnapshotArtifact::EncryptedSnapshotVolume
                | SnapshotArtifact::EncryptedRootfsCow
        )
    ) {
        MaterializationMode::Enabled
    } else {
        requested
    };
    effective_materialization_mode(artifact, requested)
}

pub(super) fn materialization_block_bytes(artifact: Option<SnapshotArtifact>) -> usize {
    match artifact {
        Some(SnapshotArtifact::Rootfs) => ROOTFS_SPARSE_BLOCK_BYTES,
        _ => MEMORY_SPARSE_BLOCK_BYTES,
    }
}
