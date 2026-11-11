use prost::Message as _;

use pplx_space_common_core::types::SnapshotArtifact as CoreArtifact;

use super::proto::{PullFileBatchEntry, PullFileRequest, PushFileBatchEntry, SnapshotArtifact};
use super::{snapshot_artifact_from_proto, snapshot_artifact_to_proto};

#[test]
fn batch_entry_semantic_artifact_wire_shape_is_pinned() {
    let push = PushFileBatchEntry {
        local_path: "/space/snapshot/input".to_string(),
        s3_uri: "s3://bucket/snapshot?region=us-east-2".to_string(),
        role: "opaque-aad-role".to_string(),
        snapshot_artifact: SnapshotArtifact::Memory as i32,
    };
    let pull = PullFileBatchEntry {
        s3_uri: "s3://bucket/snapshot?region=us-east-2".to_string(),
        local_path: "/space/snapshot/output".to_string(),
        file_mode: Some(0o600),
        role: "opaque-aad-role".to_string(),
        snapshot_artifact: SnapshotArtifact::Memory as i32,
    };
    let single_pull = PullFileRequest {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: "s3://bucket/snapshot?region=us-east-2".to_string(),
        local_path: "/space/snapshot/output".to_string(),
        snapshot_artifact: SnapshotArtifact::Memory as i32,
        ..Default::default()
    };

    insta::assert_snapshot!(format!(
        "push: {}\npull: {}\nsingle_pull: {}",
        hex::encode(push.encode_to_vec()),
        hex::encode(pull.encode_to_vec()),
        hex::encode(single_pull.encode_to_vec()),
    ));
}

#[test]
fn snapshot_artifact_proto_conversion_covers_the_closed_set() {
    let artifacts = [
        (CoreArtifact::Rootfs, SnapshotArtifact::Rootfs, 1),
        (
            CoreArtifact::RootfsBtrfsSend,
            SnapshotArtifact::RootfsBtrfsSend,
            2,
        ),
        (CoreArtifact::Memory, SnapshotArtifact::Memory, 3),
        (CoreArtifact::VmState, SnapshotArtifact::VmState, 4),
        (
            CoreArtifact::EncryptedSnapshotVolume,
            SnapshotArtifact::EncryptedSnapshotVolume,
            5,
        ),
        (
            CoreArtifact::EncryptedRootfsCow,
            SnapshotArtifact::EncryptedRootfsCow,
            6,
        ),
        (CoreArtifact::Metadata, SnapshotArtifact::Metadata, 7),
    ];

    assert_eq!(SnapshotArtifact::Unspecified as i32, 0);
    for (core, proto, discriminant) in artifacts {
        assert_eq!(snapshot_artifact_to_proto(core), proto);
        assert_eq!(proto as i32, discriminant);
        assert_eq!(snapshot_artifact_from_proto(proto), Some(core),);
    }
    assert_eq!(
        snapshot_artifact_from_proto(SnapshotArtifact::Unspecified),
        None
    );
}
