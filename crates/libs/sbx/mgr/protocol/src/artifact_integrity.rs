//! Content manifests at snapshot publication and durable-copy RPC boundaries.
//! Empty means legacy; incomplete or malformed manifests never fall back to it.
use crate::proto::SnapshotArtifactDigest;
use pplx_space_common_core::types::{ArtifactKind, Digest, SnapshotArtifact};
use std::collections::BTreeMap;

type Manifest = BTreeMap<SnapshotArtifact, (Digest, u64)>;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("invalid artifact identity: {0}")]
    Artifact(#[from] pplx_space_common_core::types::SnapshotArtifactParseError),
    #[error("invalid content identity for {0}")]
    Identity(SnapshotArtifact),
    #[error("duplicate content identity for {0}")]
    Duplicate(SnapshotArtifact),
    #[error("artifact set {actual:?} is incomplete or unsupported; expected {expected}")]
    Incomplete {
        actual: Vec<&'static str>,
        expected: &'static str,
    },
}

pub fn parse_copy_source(entries: &[SnapshotArtifactDigest]) -> Result<Manifest, ManifestError> {
    parse(entries, ManifestPurpose::CopySource)
}

pub fn parse_copied_disk(entries: &[SnapshotArtifactDigest]) -> Result<Manifest, ManifestError> {
    parse(entries, ManifestPurpose::CopiedDisk)
}

pub fn parse_backup(
    entries: &[SnapshotArtifactDigest],
    kind: ArtifactKind,
) -> Result<Manifest, ManifestError> {
    parse(entries, ManifestPurpose::Backup(kind))
}

enum ManifestPurpose {
    CopySource,
    CopiedDisk,
    Backup(ArtifactKind),
}

fn parse(
    entries: &[SnapshotArtifactDigest],
    purpose: ManifestPurpose,
) -> Result<Manifest, ManifestError> {
    let mut manifest = Manifest::new();
    for entry in entries {
        let artifact = entry.artifact.parse::<SnapshotArtifact>()?;
        let digest = entry
            .digest
            .parse::<Digest>()
            .map_err(|_| ManifestError::Identity(artifact))?;
        if !matches!(digest, Digest::Sha256(_))
            || digest.to_string() != entry.digest
            || entry.size_bytes > i64::MAX as u64
        {
            return Err(ManifestError::Identity(artifact));
        }
        if manifest
            .insert(artifact, (digest, entry.size_bytes))
            .is_some()
        {
            return Err(ManifestError::Duplicate(artifact));
        }
    }
    if !manifest.is_empty() {
        let has = |artifact| manifest.contains_key(&artifact);
        let one_disk = has(SnapshotArtifact::Rootfs) ^ has(SnapshotArtifact::RootfsBtrfsSend);
        let disk = manifest.len() == 2 && one_disk;
        let full = manifest.len() == 4
            && one_disk
            && has(SnapshotArtifact::Memory)
            && has(SnapshotArtifact::VmState);
        let encrypted = manifest.len() == 3
            && has(SnapshotArtifact::EncryptedSnapshotVolume)
            && has(SnapshotArtifact::EncryptedRootfsCow);
        let (valid, expected) = match purpose {
            ManifestPurpose::CopySource => (
                disk || full,
                "metadata.json and exactly one of rootfs/rootfs.btrfs-send, optionally with both memfile and binfile",
            ),
            ManifestPurpose::CopiedDisk => (
                disk,
                "metadata.json and exactly one of rootfs/rootfs.btrfs-send",
            ),
            ManifestPurpose::Backup(ArtifactKind::Disk) => (
                disk || encrypted,
                "metadata.json and either one of rootfs/rootfs.btrfs-send or both snapshot.luks and cow.luks",
            ),
            ManifestPurpose::Backup(ArtifactKind::Full) => (
                full || encrypted,
                "metadata.json and either {memfile, binfile, one of rootfs/rootfs.btrfs-send} or {snapshot.luks, cow.luks}",
            ),
        };
        if !has(SnapshotArtifact::Metadata) || !valid {
            return Err(ManifestError::Incomplete {
                actual: manifest
                    .keys()
                    .map(|artifact| artifact.file_name())
                    .collect(),
                expected,
            });
        }
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests;
