use super::*;
use pplx_space_common_core::types::{Arch, ExecutionMetadata};

#[test]
fn approval_requires_a_sealed_unambiguous_full_artifact() {
    let manifest = RecoveryManifest {
        validation: SealedValidation {
            sealed_full_suspend: true,
            evidence_reference: "synthetic verification".into(),
        },
        execution: SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
            Arch::Aarch64,
            vec![],
        ))
        .unwrap(),
        metadata_binding_digest: format!("sha256:{}", "11".repeat(32)).parse().unwrap(),
        vcpu: 2,
        memory_mib: 1024,
        template_id: "synthetic-template".parse().unwrap(),
        content_hash: None,
        artifacts: [
            SnapshotArtifactFile::Rootfs,
            SnapshotArtifactFile::Memory,
            SnapshotArtifactFile::VmState,
        ]
        .into_iter()
        .map(|artifact| SnapshotArtifactDigest {
            artifact,
            digest: format!("sha256:{}", "22".repeat(32)).parse().unwrap(),
            size_bytes: 32,
        })
        .collect(),
    };
    manifest.validate().unwrap();
    for case in [
        "unsealed",
        "no-validation",
        "missing-file",
        "duplicate-role",
        "generated-btrfs",
        "empty-file",
        "overflow",
        "no-resources",
    ] {
        let mut invalid = manifest.clone();
        match case {
            "unsealed" => invalid.validation.sealed_full_suspend = false,
            "no-validation" => invalid.validation.evidence_reference.clear(),
            "missing-file" => {
                invalid.artifacts.pop();
            }
            "duplicate-role" => invalid.artifacts[0].artifact = SnapshotArtifactFile::Memory,
            "generated-btrfs" => {
                invalid.artifacts[0].artifact = SnapshotArtifactFile::RootfsBtrfsSend
            }
            "empty-file" => invalid.artifacts[0].size_bytes = 0,
            "overflow" => invalid.artifacts[0].size_bytes = i64::MAX as u64,
            "no-resources" => invalid.vcpu = 0,
            _ => unreachable!(),
        }
        assert!(invalid.validate().is_err(), "{case}");
    }
}
