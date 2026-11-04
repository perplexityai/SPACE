use super::*;

#[test]
fn snapshot_artifact_directory_is_shared_with_its_backup_reader() {
    use std::os::unix::fs::MetadataExt;

    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().canonicalize().unwrap().join("artifact");
    let reader_gid = nix::unistd::getegid().as_raw();
    let mut guard = create_snapshot_artifact_directory(destination.clone(), reader_gid).unwrap();
    let metadata = std::fs::metadata(&destination).unwrap();
    assert_eq!(metadata.gid(), reader_gid);
    assert_eq!(metadata.mode() & 0o7777, 0o2770);
    assert!(create_snapshot_artifact_directory(destination.clone(), reader_gid).is_err());
    assert!(
        destination.is_dir(),
        "a collision must not remove the first artifact"
    );
    guard.disarm();
    drop(guard);
    assert!(destination.is_dir());
    let incomplete = destination.with_file_name("incomplete");
    drop(create_snapshot_artifact_directory(incomplete.clone(), reader_gid).unwrap());
    assert!(!incomplete.exists());
}

#[test]
fn named_snapshot_send_source_is_derived_from_sandbox_identity() {
    let sandbox_id = "01900000-0000-7000-8000-000000000001";
    let snapshot_name = "d-operation-01900000-0000-7000-8000-000000000002";

    assert_eq!(
        named_snapshot_send_source(sandbox_id, snapshot_name).unwrap(),
        PathBuf::from(format!(
            "/space/cache/firecracker/sandboxes/{sandbox_id}/snapshots/{snapshot_name}"
        )),
    );
    assert_eq!(
        named_snapshot_send_source(sandbox_id, "../instance")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
        "the privileged encoder must derive its source instead of accepting an arbitrary path",
    );
}

#[test]
fn named_snapshot_stream_uses_restore_wire_basename() {
    let artifact_dir =
        Path::new("/space/cache/firecracker/sandboxes/source/snapshots/operation-artifact");

    assert_eq!(
        named_snapshot_wire_source(artifact_dir),
        artifact_dir.join("instance.ro"),
        "the receive side renames instance.ro into the target instance",
    );
}

#[test]
fn named_snapshot_send_requires_the_template_instance_snapshot_lineage_chain() {
    let template_uuid = uuid::Uuid::now_v7();
    let instance_uuid = uuid::Uuid::now_v7();
    let instance = pplx_space_common_core::host::btrfs::SubvolumeInfo {
        uuid: Some(instance_uuid),
        parent_uuid: Some(template_uuid),
        received_uuid: None,
    };
    let snapshot = pplx_space_common_core::host::btrfs::SubvolumeInfo {
        uuid: Some(uuid::Uuid::now_v7()),
        parent_uuid: Some(instance_uuid),
        received_uuid: None,
    };

    validate_named_snapshot_lineage(template_uuid, &instance, &snapshot).unwrap();

    let unrelated_snapshot = pplx_space_common_core::host::btrfs::SubvolumeInfo {
        parent_uuid: Some(uuid::Uuid::now_v7()),
        ..snapshot
    };
    assert_eq!(
        validate_named_snapshot_lineage(template_uuid, &instance, &unrelated_snapshot)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
        "an unrelated named snapshot must fall back instead of being encoded against the wrong parent",
    );
}

#[test]
fn lineage_precondition_carries_bounded_cause_metadata() {
    let status = lineage_precondition(
        LineageFallbackCause::InstanceNotSubvolume,
        "not a subvolume".to_string(),
    );
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        status.metadata().get(LINEAGE_CAUSE_METADATA).unwrap(),
        "instance_not_subvolume"
    );
}
