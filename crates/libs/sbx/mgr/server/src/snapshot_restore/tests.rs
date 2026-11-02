use pplx_space_common_core::types::LocalArtifactId;

use super::*;
use crate::snapshot_upload::build_storage_uri_for_tests;
use pplx_space_common_svc::bearer_token::BearerTokenError;
use pplx_space_node_api_client::cluster::ConnectClusterApiError;

#[test]
fn disk_restore_stages_only_the_rootfs() {
    assert_eq!(
        full_codec_artifacts(false),
        vec![(SnapshotArtifact::Rootfs, "rootfs.ext4")]
    );
    assert_eq!(full_codec_artifacts(true).len(), 3);
}

#[test]
fn btrfs_disk_restore_stages_only_the_rootfs_diff() {
    assert_eq!(
        btrfs_restore_artifacts(ArtifactKind::Disk),
        vec![(
            SnapshotArtifact::RootfsBtrfsSend,
            crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME,
        )]
    );
    assert_eq!(btrfs_restore_artifacts(ArtifactKind::Full).len(), 3);
}

// A Direct node has no SPACE_NODE_ID; its node-id file names it.
#[test]
fn quarantine_recovery_plan_request_names_the_node_from_the_node_id_file() {
    let dir = tempfile::tempdir().unwrap();
    let node_id_path = dir.path().join("node-id");
    let node_id = pplx_space_common_core::types::NodeId::new().to_string();
    std::fs::write(&node_id_path, format!("{node_id}\n")).unwrap();

    let request = quarantine_recovery_plan_request(
        "sandbox-under-test",
        &crate::service::NodeIdentitySource::node_id_file(node_id_path),
        None,
    )
    .expect("the node-id file identifies this node");

    assert_eq!(request.node_id, node_id);
    assert_eq!(
        request.role,
        Some(pplx_space_node_api_client::proto::NodeRole::Worker as i32)
    );
}

#[test]
fn recovery_plan_parses_legacy_empty_artifact_identity() {
    assert_eq!(parse_recovery_local_artifact_id("").unwrap(), None);
    assert_eq!(parse_recovery_local_artifact_id("   ").unwrap(), None);

    let artifact_id = LocalArtifactId::new();
    assert_eq!(
        parse_recovery_local_artifact_id(&artifact_id.to_string()).unwrap(),
        Some(artifact_id)
    );
    parse_recovery_local_artifact_id("not-an-artifact-id").unwrap_err();
}

#[test]
fn missing_cluster_api_configuration_defers_restore() {
    for error in [
        ConnectClusterApiError::MissingEndpoint,
        ConnectClusterApiError::MissingToken(BearerTokenError::EmptyEnv {
            env_var: "SPACE_INTERNAL_TOKEN",
        }),
    ] {
        assert!(matches!(
            restore_error_from_cluster_connect(error),
            RestoreError::ClusterApiUnconfigured(_)
        ));
    }
}

#[test]
fn verified_artifact_location_recovers_exact_snapshot_source() {
    let artifact_uri_wire = "s3://snapshots/prefix/metadata.json?region=us-east-1&endpoint=http://object-store:9000&path_style=true";
    let artifact_uri = ExactStorageUri::parse(artifact_uri_wire).unwrap();

    let source =
        restore_source_from_artifact_location(&artifact_uri, SnapshotArtifact::Metadata).unwrap();

    assert_eq!(
        source.as_wire(),
        "s3://snapshots/prefix?region=us-east-1&endpoint=http://object-store:9000&path_style=true"
    );
}

#[test]
fn volmgr_admission_rejection_remains_resource_exhausted() {
    let mut status = tonic::Status::resource_exhausted("pull admission limit reached");
    status
        .metadata_mut()
        .insert("x-space-retry-after-ms", "250".parse().unwrap());
    let error = EnsureFileReplicaError::Submit(status);

    let restore_error = restore_error_from_ensure_file_replica("rootfs", error);

    let RestoreError::AdmissionRejected(status) = restore_error else {
        panic!("expected admission rejection");
    };
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("rootfs"));
    assert_eq!(
        status.metadata().get("x-space-retry-after-ms").unwrap(),
        "250"
    );
}

#[test]
fn template_admission_rejection_remains_resource_exhausted() {
    let status = tonic::Status::resource_exhausted("pull admission limit reached");

    let restore_error = template_pull_error("app-runtime", "sha256:test", status);

    let RestoreError::AdmissionRejected(status) = restore_error else {
        panic!("expected admission rejection");
    };
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("app-runtime@sha256:test"));
}

#[test]
fn template_lookup_failure_is_typed_and_keeps_its_message() {
    let status = tonic::Status::not_found("template release not found");

    let restore_error = template_pull_error("app-runtime", "sha256:test", status);

    let RestoreError::ParentTemplateLookup { ref source, .. } = restore_error else {
        panic!("expected a parent-template lookup failure");
    };
    assert_eq!(source.code(), tonic::Code::NotFound);
    assert_eq!(restore_error.kind_label(), "parent_template_lookup");
    assert!(
        restore_error
            .to_string()
            .starts_with("tmplmgr GetTemplate(app-runtime@sha256:test) failed: ")
    );
    assert_eq!(restore_error.redacted_detail(), None);
}

#[test]
fn snapshot_artifacts_use_high_priority_pulls() {
    let artifacts = [
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::RootfsBtrfsSend,
        SnapshotArtifact::Memory,
        SnapshotArtifact::VmState,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
        SnapshotArtifact::Metadata,
    ];

    for artifact in artifacts {
        assert_eq!(
            snapshot_pull_priority(artifact),
            RESTORE_PRIORITY,
            "{artifact:?}"
        );
    }
}

fn restore_key_for(storage_uri: &str, artifact: SnapshotArtifact) -> String {
    let config = SnapshotUploadConfig::from_uri(storage_uri).expect("valid storage uri");
    let dir_prefix = config.key_prefix.trim_matches('/');
    let file_name = artifact.file_name();
    if dir_prefix.is_empty() {
        file_name.to_string()
    } else {
        format!("{dir_prefix}/{file_name}")
    }
}

fn upload_key_for(sandbox_id: &str, snapshot_id: &str, artifact: SnapshotArtifact) -> String {
    // Mirrors what `snapshot_upload::submit_suspend_snapshot` writes for
    // the bucket-root case (empty cluster key_prefix).
    pplx_space_common_core::types::snapshot_object_key("", sandbox_id, snapshot_id, artifact)
}

#[test]
fn restore_key_round_trips_with_upload() {
    // Bucket-root case (no cluster-wide key prefix): the URI registered
    // by upload, parsed back by restore, must produce the same key the
    // upload wrote to. Both bugs surfaced by the snapshot-S3 test were
    // round-trip failures here: the key was double-prefixed and the
    // sandbox-id format was renormalized between sides.
    let sandbox_id = "019e31d9-79a6-7132-b90f-007f1c62ddea";
    let snapshot_id = "019253ab-cdef-7000-8123-000000000001";
    let storage_uri =
        build_storage_uri_for_tests(sandbox_id, snapshot_id, "test-bucket", "us-east-1", "");
    for artifact in [
        SnapshotArtifact::VmState,
        SnapshotArtifact::Memory,
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::Metadata,
    ] {
        assert_eq!(
            restore_key_for(&storage_uri, artifact),
            upload_key_for(sandbox_id, snapshot_id, artifact),
            "restore key for {artifact:?} must match upload key",
        );
    }
}

fn meta_with_diff_format(diff_format: Option<&str>) -> crate::snapshot_upload::SnapshotMeta {
    let format_field = match diff_format {
        Some(s) => format!(r#","rootfs_diff_format":"{s}""#),
        None => String::new(),
    };
    let json = format!(
        r#"{{"version":1,"sandbox_id":"sbx","size_bytes":0,"created_at":"2026-05-25T00:00:00Z",
             "source":{{"runtime":"fc","template_id":"t"}}{format_field}}}"#
    );
    serde_json::from_str(&json).expect("test meta JSON must parse")
}

#[test]
fn codec_from_meta_preserves_known_formats_and_rejects_unknown_markers() {
    for (marker, expected) in [
        (None, Some(RestoreCodec::Full)),
        (
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1),
            Some(RestoreCodec::BtrfsSendV1),
        ),
        (Some("future-codec-v9"), None),
        (Some(""), None),
    ] {
        let result = codec_from_meta(&meta_with_diff_format(marker));
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None => {
                result.unwrap_err();
            }
        }
    }
}

fn disk_artifact_meta(sandbox_id: SandboxId) -> crate::snapshot_upload::SnapshotMeta {
    serde_json::from_value(serde_json::json!({
        "version": 1,
        "sandbox_id": sandbox_id.to_string(),
        "size_bytes": 0,
        "created_at": "2026-08-05T00:00:00Z",
        "artifact_kind": "disk",
        "source": {
            "runtime": "fc",
            "template_id": "seed"
        }
    }))
    .unwrap()
}

#[test]
fn fork_metadata_is_bound_to_the_new_sandbox_identity() {
    let source_sandbox_id = SandboxId::new();
    let target_sandbox_id = SandboxId::new();

    let source_snapshot = disk_artifact_meta(source_sandbox_id);
    validate_restore_metadata_identity(
        &source_snapshot,
        &target_sandbox_id.to_string(),
        &source_sandbox_id.to_string(),
    )
    .unwrap();

    let mut fork_artifact = disk_artifact_meta(target_sandbox_id);
    fork_artifact.artifact_origin = Some(ArtifactOrigin::Fork);
    validate_restore_metadata_identity(
        &fork_artifact,
        &target_sandbox_id.to_string(),
        &source_sandbox_id.to_string(),
    )
    .expect("fork artifacts are projected onto the new sandbox identity");
}

fn write_test_replica_provenance(
    path: &Path,
    sandbox_id: SandboxId,
    config: &SnapshotUploadConfig,
    artifact: SnapshotArtifact,
) {
    let size = std::fs::metadata(path).unwrap().len();
    std::fs::write(
        verified_replica_provenance_path(path),
        serde_json::to_vec(&pplx_space_volmgr_client::VerifiedReplicaProvenance {
            version: pplx_space_volmgr_client::VERIFIED_REPLICA_PROVENANCE_VERSION,
            subject_id: format!("sandbox:{sandbox_id}"),
            s3_uri: restore_artifact_uri(config, artifact.file_name()),
            raw: false,
            file_mode: None,
            snapshot_artifact: Some(artifact),
            object_size: size,
            object_etag: Some("test-etag".to_string()),
            local_size: size,
        })
        .unwrap(),
    )
    .unwrap();
}

fn write_test_published_metadata(
    root: &Path,
    subject_sandbox_id: SandboxId,
    metadata_sandbox_id: SandboxId,
    storage_uri: &str,
) -> crate::snapshot_upload::SnapshotMeta {
    let snapshot_dir = root.join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let meta = disk_artifact_meta(metadata_sandbox_id);
    let metadata_path = snapshot_dir.join("snapshot.json");
    std::fs::write(&metadata_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    write_test_replica_provenance(
        &metadata_path,
        subject_sandbox_id,
        &SnapshotUploadConfig::from_uri(storage_uri).unwrap(),
        SnapshotArtifact::Metadata,
    );
    meta
}

#[test]
fn published_restore_is_recoverable_after_state_commit_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let storage_uri = build_storage_uri_for_tests(
        &sandbox_id_string,
        "019253ab-cdef-7000-8123-000000000001",
        "test-bucket",
        "us-east-1",
        "",
    );
    let meta: crate::snapshot_upload::SnapshotMeta = serde_json::from_value(serde_json::json!({
        "version": 1,
        "sandbox_id": sandbox_id_string,
        "size_bytes": 0,
        "created_at": "2026-07-30T00:00:00Z",
        "source": {
            "runtime": "fc",
            "template_id": "seed"
        }
    }))
    .unwrap();
    let staging_dir = tmp.path().join(".restore");
    let snapshot_dir = staging_dir.join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let metadata = serde_json::to_vec(&meta).unwrap();
    let metadata_path = snapshot_dir.join("snapshot.json");
    std::fs::write(&metadata_path, &metadata).unwrap();
    let config = SnapshotUploadConfig::from_uri(&storage_uri).unwrap();
    write_test_replica_provenance(
        &metadata_path,
        sandbox_id,
        &config,
        SnapshotArtifact::Metadata,
    );
    let mut staged = RestoreMetadataStaged {
        sandbox_dir: staging_dir,
        snapshot_dir,
        instance_dir: tmp.path().join(".restore/instance"),
        config,
        storage_uri: storage_uri.clone(),
        meta,
        codec: RestoreCodec::Full,
        transfer_scope_id: Some(pplx_space_common_core::types::TransferScopeId::new()),
        credential_subject_id: sandbox_id.to_string(),
        filesystem_scope_id: None,
    };
    let canonical_dir = tmp.path().join(&sandbox_id_string);

    publish_remote_restore_metadata_to(&mut staged, sandbox_id, canonical_dir.clone()).unwrap();
    let wrong_storage_uri = build_storage_uri_for_tests(
        &sandbox_id_string,
        "019253ab-cdef-7000-8123-000000000002",
        "test-bucket",
        "us-east-1",
        "",
    );
    let error = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &wrong_storage_uri.parse().unwrap(),
        None,
    )
    .unwrap_err();
    assert!(matches!(error, RestoreError::CanonicalRootConflict { .. }));
    assert!(canonical_dir.join("snapshot/snapshot.json").is_file());
    assert_eq!(
        published_remote_restore_source_at(&canonical_dir, sandbox_id)
            .unwrap()
            .unwrap()
            .as_wire(),
        storage_uri,
        "reclamation must fence against the exact source that owns the root"
    );
    let error = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &storage_uri.parse().unwrap(),
        None,
    )
    .unwrap_err();
    assert!(matches!(error, RestoreError::CanonicalRootConflict { .. }));

    let config = SnapshotUploadConfig::from_uri(&storage_uri).unwrap();
    for (artifact, local_name) in full_codec_artifacts(true) {
        let path = canonical_dir.join("snapshot").join(local_name);
        std::fs::write(&path, artifact.file_name()).unwrap();
        write_test_replica_provenance(&path, sandbox_id, &config, artifact);
    }
    let instance_dir = canonical_dir.join("instance");
    std::fs::create_dir_all(&instance_dir).unwrap();
    std::fs::write(instance_dir.join("rootfs.ext4"), b"rootfs").unwrap();

    let recovered = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &storage_uri.parse().unwrap(),
        None,
    )
    .unwrap()
    .expect("a complete published restore must retain recoverable ownership");

    assert_eq!(recovered.storage_uri, storage_uri);
    assert_eq!(recovered.meta.sandbox_id, sandbox_id_string);
}

#[test]
fn complete_encrypted_published_restore_is_recoverable_without_plaintext_rootfs() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let storage_uri = build_storage_uri_for_tests(
        &sandbox_id.to_string(),
        "019253ab-cdef-7000-8123-000000000004",
        "test-bucket",
        "us-east-1",
        "",
    );
    let config = SnapshotUploadConfig::from_uri(&storage_uri).unwrap();
    let canonical_dir = tmp.path().join(sandbox_id.to_string());
    let snapshot_dir = canonical_dir.join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();

    let mut meta = disk_artifact_meta(sandbox_id);
    meta.encrypted = true;
    let metadata_path = snapshot_dir.join("snapshot.json");
    std::fs::write(&metadata_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    write_test_replica_provenance(
        &metadata_path,
        sandbox_id,
        &config,
        SnapshotArtifact::Metadata,
    );
    for artifact in crate::snapshot_upload::encrypted_vm_artifacts() {
        let path = snapshot_dir.join(artifact.local_name);
        std::fs::write(&path, artifact.kind.file_name()).unwrap();
        write_test_replica_provenance(&path, sandbox_id, &config, artifact.kind);
    }

    let recovered = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &storage_uri.parse().unwrap(),
        None,
    )
    .expect("a complete encrypted publication must not conflict")
    .expect("a complete encrypted publication must retain recoverable ownership");

    assert!(recovered.meta.encrypted);
    assert!(!canonical_dir.join("instance/rootfs.ext4").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn canonical_restore_exchange_publishes_replacement_and_scopes_the_old_root() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let old_storage_uri = build_storage_uri_for_tests(
        &sandbox_id.to_string(),
        "019253ab-cdef-7000-8123-000000000008",
        "test-bucket",
        "us-east-1",
        "",
    );
    let replacement_storage_uri = build_storage_uri_for_tests(
        &sandbox_id.to_string(),
        "019253ab-cdef-7000-8123-000000000009",
        "test-bucket",
        "us-east-1",
        "",
    );
    let staging_dir = tmp.path().join(".restore-scope");
    let canonical_dir = tmp.path().join(sandbox_id.to_string());
    std::fs::create_dir_all(staging_dir.join("instance")).unwrap();
    let meta = write_test_published_metadata(
        &staging_dir,
        sandbox_id,
        sandbox_id,
        &replacement_storage_uri,
    );
    write_test_published_metadata(&canonical_dir, sandbox_id, sandbox_id, &old_storage_uri);
    std::fs::write(staging_dir.join("replacement"), b"source-b").unwrap();
    std::fs::write(canonical_dir.join("remnant"), b"source-a").unwrap();
    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    let execution_generation = pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap();
    let attempt_a = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str("019c0000-0000-7000-8000-000000000001").unwrap(),
    );
    let activation_attempt_id =
        pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
            uuid::Uuid::parse_str("019c0000-0000-7000-8000-000000000002").unwrap(),
        );
    let attempt_c = pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str("019c0000-0000-7000-8000-000000000003").unwrap(),
    );
    let mut staged = RestoreMetadataStaged {
        sandbox_dir: staging_dir.clone(),
        snapshot_dir: staging_dir.join("snapshot"),
        instance_dir: staging_dir.join("instance"),
        config: SnapshotUploadConfig::from_uri(&replacement_storage_uri).unwrap(),
        storage_uri: replacement_storage_uri.clone(),
        meta,
        codec: RestoreCodec::Full,
        transfer_scope_id: Some(transfer_scope_id),
        credential_subject_id: sandbox_id.to_string(),
        filesystem_scope_id: None,
    };

    write_canonical_restore_exchange_receipt(
        &staged,
        sandbox_id,
        transfer_scope_id,
        execution_generation,
        activation_attempt_id,
        &old_storage_uri.parse().unwrap(),
        &replacement_storage_uri.parse().unwrap(),
    )
    .unwrap();

    exchange_remote_restore_metadata_to(&mut staged, sandbox_id, canonical_dir.clone()).unwrap();

    assert_eq!(staged.sandbox_dir, canonical_dir);
    assert!(staged.sandbox_dir.join("replacement").is_file());
    assert!(!staged.sandbox_dir.join("remnant").exists());
    assert!(
        staging_dir.join("remnant").is_file(),
        "the old root must move under the existing durable transfer scope"
    );
    assert_eq!(
        canonical_restore_exchange_old_source_at(
            &staged.sandbox_dir,
            sandbox_id,
            transfer_scope_id,
            execution_generation,
            activation_attempt_id,
            &replacement_storage_uri.parse().unwrap(),
        )
        .unwrap()
        .unwrap()
        .as_wire(),
        old_storage_uri
    );
    assert!(
        canonical_restore_exchange_old_source_at(
            &staged.sandbox_dir,
            sandbox_id,
            transfer_scope_id,
            execution_generation,
            attempt_c,
            &replacement_storage_uri.parse().unwrap(),
        )
        .is_ok(),
        "a strictly newer cleanup owner must be able to use the immutable exchange receipt"
    );
    assert!(
        canonical_restore_exchange_old_source_at(
            &staged.sandbox_dir,
            sandbox_id,
            transfer_scope_id,
            execution_generation,
            attempt_a,
            &replacement_storage_uri.parse().unwrap(),
        )
        .is_err(),
        "a delayed predecessor must not use a successor's exchange receipt"
    );
    assert_eq!(
        reclaimable_published_remote_restore_source_at(&staging_dir, sandbox_id)
            .unwrap()
            .unwrap()
            .as_wire(),
        old_storage_uri,
        "post-exchange cleanup must revalidate the exact old source"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn same_source_exchange_receipt_recovers_cleanup_after_commit_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let storage_uri = build_storage_uri_for_tests(
        &sandbox_id.to_string(),
        "019253ab-cdef-7000-8123-00000000000b",
        "test-bucket",
        "us-east-1",
        "",
    );
    let config = SnapshotUploadConfig::from_uri(&storage_uri).unwrap();
    let staging_dir = tmp.path().join(".restore-scope");
    let canonical_dir = tmp.path().join(sandbox_id.to_string());
    std::fs::create_dir_all(staging_dir.join("instance")).unwrap();
    let meta = write_test_published_metadata(&staging_dir, sandbox_id, sandbox_id, &storage_uri);
    for (artifact, local_name) in full_codec_artifacts(true) {
        let path = staging_dir.join("snapshot").join(local_name);
        std::fs::write(&path, artifact.file_name()).unwrap();
        write_test_replica_provenance(&path, sandbox_id, &config, artifact);
    }
    std::fs::write(staging_dir.join("instance/rootfs.ext4"), b"rootfs").unwrap();

    write_test_published_metadata(&canonical_dir, sandbox_id, sandbox_id, &storage_uri);
    std::fs::write(canonical_dir.join("partial"), b"old incomplete root").unwrap();

    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    let execution_generation = pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap();
    let activation_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    let parsed_storage_uri = storage_uri.parse().unwrap();
    let mut staged = RestoreMetadataStaged {
        sandbox_dir: staging_dir.clone(),
        snapshot_dir: staging_dir.join("snapshot"),
        instance_dir: staging_dir.join("instance"),
        config,
        storage_uri: storage_uri.clone(),
        meta,
        codec: RestoreCodec::Full,
        transfer_scope_id: Some(transfer_scope_id),
        credential_subject_id: sandbox_id.to_string(),
        filesystem_scope_id: None,
    };
    write_canonical_restore_exchange_receipt(
        &staged,
        sandbox_id,
        transfer_scope_id,
        execution_generation,
        activation_attempt_id,
        &parsed_storage_uri,
        &parsed_storage_uri,
    )
    .unwrap();
    exchange_remote_restore_metadata_to(&mut staged, sandbox_id, canonical_dir.clone()).unwrap();

    recover_published_remote_restore_at(&canonical_dir, sandbox_id, &parsed_storage_uri, None)
        .unwrap()
        .expect("the complete replacement must be recoverable after restart");
    let old_source = canonical_restore_exchange_old_source_at(
        &canonical_dir,
        sandbox_id,
        transfer_scope_id,
        execution_generation,
        activation_attempt_id,
        &parsed_storage_uri,
    )
    .expect("the same-source receipt must authorize scoped cleanup")
    .expect("the exchange receipt must identify the old root");
    assert_eq!(old_source, parsed_storage_uri);
    assert_eq!(
        reclaimable_published_remote_restore_source_at(&staging_dir, sandbox_id)
            .unwrap()
            .unwrap(),
        parsed_storage_uri,
        "cleanup must still validate the old root's verified provenance"
    );

    std::fs::remove_dir_all(&staging_dir).unwrap();
    assert!(!staging_dir.exists());
    assert!(
        recover_published_remote_restore_at(&canonical_dir, sandbox_id, &parsed_storage_uri, None,)
            .unwrap()
            .is_some(),
        "scoped cleanup must leave the complete canonical replacement recoverable"
    );
}

#[test]
fn published_restore_source_rejects_mismatched_snapshot_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let storage_uri = build_storage_uri_for_tests(
        &sandbox_id.to_string(),
        "019253ab-cdef-7000-8123-00000000000a",
        "test-bucket",
        "us-east-1",
        "",
    );
    write_test_published_metadata(tmp.path(), sandbox_id, SandboxId::new(), &storage_uri);

    assert!(matches!(
        reclaimable_published_remote_restore_source_at(tmp.path(), sandbox_id),
        Err(RestoreError::SnapshotIdentityMismatch { .. })
    ));
}

#[test]
fn published_restore_as_new_requires_complete_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new();
    let source_sandbox_id = SandboxId::new();
    let storage_uri = build_storage_uri_for_tests(
        &source_sandbox_id.to_string(),
        "019253ab-cdef-7000-8123-000000000003",
        "test-bucket",
        "us-east-1",
        "",
    );
    let mut meta = disk_artifact_meta(source_sandbox_id);
    let canonical_dir = tmp.path().join(sandbox_id.to_string());
    let snapshot_dir = canonical_dir.join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let metadata = serde_json::to_vec(&meta).unwrap();
    let metadata_path = snapshot_dir.join("snapshot.json");
    std::fs::write(&metadata_path, &metadata).unwrap();
    let config = SnapshotUploadConfig::from_uri(&storage_uri).unwrap();
    write_test_replica_provenance(
        &metadata_path,
        sandbox_id,
        &config,
        SnapshotArtifact::Metadata,
    );

    let recovered = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &storage_uri.parse().unwrap(),
        Some((source_sandbox_id, ArtifactKind::Disk)),
    )
    .unwrap();

    assert!(
        recovered.is_none(),
        "partial restore-as-new publication must be rematerialized"
    );

    meta.sandbox_id = sandbox_id.to_string();
    std::fs::write(&metadata_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    let recovered = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &storage_uri.parse().unwrap(),
        Some((source_sandbox_id, ArtifactKind::Disk)),
    )
    .unwrap();

    assert!(recovered.is_none());

    let instance_dir = canonical_dir.join("instance");
    std::fs::create_dir_all(&instance_dir).unwrap();
    let rootfs_path = snapshot_dir.join("rootfs.ext4");
    std::fs::write(&rootfs_path, b"rootfs").unwrap();
    std::fs::write(instance_dir.join("rootfs.ext4"), b"rootfs").unwrap();
    write_test_replica_provenance(&rootfs_path, sandbox_id, &config, SnapshotArtifact::Rootfs);

    let recovered = recover_published_remote_restore_at(
        &canonical_dir,
        sandbox_id,
        &storage_uri.parse().unwrap(),
        Some((source_sandbox_id, ArtifactKind::Disk)),
    )
    .unwrap();

    assert!(recovered.is_some());
}

#[test]
fn pull_replacement_commits_new_bytes_over_existing_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    std::fs::write(&local_path, b"stale").unwrap();
    let mut replacement =
        PullReplacementGuard::for_existing(&local_path, "s3://bucket/sbx/snap-1/memfile");
    let download_path = replacement.download_path(&local_path);
    std::fs::write(&download_path, b"newer").unwrap();

    replacement.commit(&local_path).unwrap();

    assert_eq!(std::fs::read(&local_path).unwrap(), b"newer");
    assert!(!download_path.exists());
}

#[test]
fn pull_replacement_keeps_uncommitted_download_for_reuse_on_drop() {
    // An uncommitted temp is a same-bytes cache entry keyed by the
    // source URI: the next attempt for the same URI commits it via
    // vol-mgr's skip-present path instead of re-downloading. Only
    // the sweep removes temps, and only for superseded URIs.
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    std::fs::write(&local_path, b"stale").unwrap();
    let download_path = {
        let replacement =
            PullReplacementGuard::for_existing(&local_path, "s3://bucket/sbx/snap-1/memfile");
        let download_path = replacement.download_path(&local_path);
        std::fs::write(&download_path, b"newer").unwrap();
        download_path
    };

    assert_eq!(std::fs::read(&local_path).unwrap(), b"stale");
    assert!(
        download_path.exists(),
        "uncommitted temp must survive for the next attempt to reuse"
    );
}

#[test]
fn pull_replacement_temp_name_is_deterministic_per_source_uri() {
    // Determinism is the dedup linkage: vol-mgr's transfer dedup
    // key includes local_path, so identical (artifact, source URI)
    // attempts collapse onto one in-flight download — and a
    // completed temp is found at the same path on retry.
    let local_path = Path::new("/a/snapshot/memory");
    let uri_a = "s3://bucket/sbx/snap-1/memfile";
    let uri_b = "s3://bucket/sbx/snap-2/memfile";
    let attempt_one = PullReplacementGuard::for_existing(local_path, uri_a);
    let attempt_two = PullReplacementGuard::for_existing(local_path, uri_a);
    let other_generation = PullReplacementGuard::for_existing(local_path, uri_b);

    assert_eq!(
        attempt_one.download_path(local_path),
        attempt_two.download_path(local_path),
        "same source URI must map to the same temp across attempts"
    );
    assert_ne!(
        attempt_one.download_path(local_path),
        other_generation.download_path(local_path),
        "a different snapshot generation must get its own temp"
    );
    // The name keeps the `.{name}.restore-<uuid>` shape existing
    // residue tooling (cache-usage classifier, reclaim script
    // globs) matches on.
    let spare = attempt_one.spare_basename().unwrap();
    let suffix = spare.strip_prefix(".memory.restore-").unwrap();
    // Golden value: pins both the derivation (uuidv5 of the URI)
    // and the frozen namespace const — accidentally changing either
    // would silently orphan every cached temp in the fleet.
    assert_eq!(suffix, "87b38d7b-7989-5e06-b307-870aebfb8ee5");
    // Strict shape, not just parseable: downstream residue tooling
    // matches hyphenated lowercase hex of exactly this layout.
    assert_eq!(suffix.len(), 36, "hyphenated uuid length: {suffix}");
    assert!(
        suffix.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit() && !b.is_ascii_uppercase(),
        }),
        "lowercase hyphenated hex shape: {suffix}"
    );
}

#[test]
fn sweep_removes_only_superseded_temps_of_this_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    std::fs::write(&local_path, b"current").unwrap();
    let superseded = tmp
        .path()
        .join(".memory.restore-019eb9b6-8492-75b1-b089-935a68444ddf");
    let superseded_dec = tmp
        .path()
        .join(".memory.restore-019eba94-fb5a-73e0-98cd-7f82fe3f70d9.dec.tmp");
    // vol-mgr streams downloads into `{dest}.partial` before the
    // rename, so leaked temps exist in this third form too.
    let superseded_partial = tmp
        .path()
        .join(".memory.restore-019eba97-0c4a-7f63-a060-e5c4c3dd65a5.partial");
    let superseded_provenance = verified_replica_provenance_path(&superseded);
    let other_artifact = tmp
        .path()
        .join(".vmstate.restore-019eb9f8-a373-7a62-8791-d8f79ad11111");
    for path in [
        &superseded,
        &superseded_dec,
        &superseded_partial,
        &superseded_provenance,
        &other_artifact,
    ] {
        std::fs::write(path, b"leaked").unwrap();
    }

    sweep_stale_pull_replacements("sbx-test", &local_path, None);

    assert!(local_path.exists(), "the real artifact must survive");
    assert!(!superseded.exists());
    assert!(!superseded_dec.exists());
    assert!(!superseded_partial.exists());
    assert!(!superseded_provenance.exists());
    assert!(
        other_artifact.exists(),
        "temps of other artifacts must not be touched by this artifact's sweep"
    );
}

#[test]
fn sweep_spares_the_current_source_uris_temp_and_derivatives() {
    // The current generation's temp may be a finished download the
    // attempt is about to commit, or the working file of the
    // in-flight job it deduped onto — including the `.partial` /
    // `.dec.tmp` stages vol-mgr is still writing.
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    std::fs::write(&local_path, b"current").unwrap();
    let guard = PullReplacementGuard::for_existing(&local_path, "s3://bucket/sbx/snap-2/memfile");
    let spare = guard.spare_basename().unwrap();
    let current = tmp.path().join(&spare);
    let current_partial = tmp.path().join(format!("{spare}.partial"));
    let current_dec = tmp.path().join(format!("{spare}.dec.tmp"));
    let current_provenance = verified_replica_provenance_path(&current);
    let superseded = tmp
        .path()
        .join(".memory.restore-019eb9b6-8492-75b1-b089-935a68444ddf");
    for path in [
        &current,
        &current_partial,
        &current_dec,
        &current_provenance,
        &superseded,
    ] {
        std::fs::write(path, b"x").unwrap();
    }

    sweep_stale_pull_replacements("sbx-test", &local_path, Some(&spare));

    assert!(current.exists(), "current-generation temp must be spared");
    assert!(
        current_partial.exists(),
        ".partial derivative must be spared"
    );
    assert!(current_dec.exists(), ".dec.tmp derivative must be spared");
    assert!(
        current_provenance.exists(),
        "verified replica provenance must be spared with its temp"
    );
    assert!(
        !superseded.exists(),
        "superseded-generation temp must be removed"
    );
}

#[test]
fn missing_artifact_adopts_completed_same_uri_temp() {
    // A prior attempt finished the download but was abandoned
    // before commit, and the artifact itself is gone (e.g. a
    // partially-wiped dir): the rename adopts the finished bytes
    // and the follow-up vol-mgr pull validates them via
    // skip-present instead of re-downloading.
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    let uri = "s3://bucket/sbx/snap-1/memfile";
    let temp = PullReplacementGuard::for_existing(&local_path, uri).download_path(&local_path);
    std::fs::write(&temp, b"downloaded").unwrap();

    adopt_completed_replacement_temp("sbx-test", &local_path, uri);

    assert_eq!(std::fs::read(&local_path).unwrap(), b"downloaded");
    assert!(!temp.exists(), "temp must be renamed, not copied");
}

#[test]
fn completed_temp_adoption_moves_verified_replica_provenance() {
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    let uri = "s3://bucket/sbx/snap-1/memfile";
    let temp = PullReplacementGuard::for_existing(&local_path, uri).download_path(&local_path);
    let temp_provenance = verified_replica_provenance_path(&temp);
    let local_provenance = verified_replica_provenance_path(&local_path);
    std::fs::write(&temp, b"downloaded").unwrap();
    std::fs::write(&temp_provenance, b"verified-replica").unwrap();

    let adopted = adopt_completed_replacement_temp("sbx-test", &local_path, uri);

    assert!(adopted);
    assert_eq!(std::fs::read(&local_path).unwrap(), b"downloaded");
    assert_eq!(
        std::fs::read(&local_provenance).unwrap(),
        b"verified-replica"
    );
    assert!(!temp.exists());
    assert!(!temp_provenance.exists());
}

#[tokio::test]
async fn admission_rejection_preserves_completed_restore_work_for_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path().join("sbx");
    let snapshot_dir = sandbox_dir.join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let local_path = snapshot_dir.join("memory");
    std::fs::write(&local_path, b"downloaded").unwrap();
    std::fs::write(
        verified_replica_provenance_path(&local_path),
        b"verified-replica",
    )
    .unwrap();
    let storage_uri = build_storage_uri_for_tests(
        "019e31d9-79a6-7132-b90f-007f1c62ddea",
        "019253ab-cdef-7000-8123-000000000001",
        "test-bucket",
        "us-east-1",
        "",
    );
    let stage = RestoreMetadataStaged {
        sandbox_dir: sandbox_dir.clone(),
        snapshot_dir,
        instance_dir: sandbox_dir.join("instance"),
        config: SnapshotUploadConfig::from_uri(&storage_uri).unwrap(),
        storage_uri,
        meta: meta_with_diff_format(None),
        codec: RestoreCodec::Full,
        transfer_scope_id: None,
        credential_subject_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".to_string(),
        filesystem_scope_id: None,
    };
    let error = RestoreError::AdmissionRejected(tonic::Status::resource_exhausted(
        "pull admission limit reached",
    ));

    let target = pplx_space_node_mgr_client::execution_fence::InstanceTarget::Execution(
        pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            None,
        ),
    );
    handle_restore_stage_failure(
        &stage,
        target,
        &error,
        &crate::node_manager_transport::fake::unbound_in_absent_dir(),
    )
    .await;

    assert!(
        sandbox_dir.exists(),
        "admission backpressure must not erase completed restore work"
    );
    let s3_uri = restore_artifact_uri(&stage.config, SnapshotArtifact::Memory.file_name());
    let retry_path =
        PullReplacementGuard::for_existing(&local_path, &s3_uri).download_path(&local_path);
    assert!(!local_path.exists());
    assert!(retry_path.exists());
    assert!(verified_replica_provenance_path(&retry_path).exists());

    let adopted = adopt_completed_replacement_temp("sbx", &local_path, &s3_uri);

    assert!(adopted);
    assert_eq!(std::fs::read(&local_path).unwrap(), b"downloaded");
    assert_eq!(
        std::fs::read(verified_replica_provenance_path(&local_path)).unwrap(),
        b"verified-replica"
    );
}

#[test]
fn adopted_artifact_is_validated_in_place_not_via_a_fresh_temp() {
    // Review-pointed bug: adoption renamed the temp into place, but
    // the pull still targeted the (now missing) temp path — so
    // vol-mgr's skip-present validated the wrong path, re-downloaded
    // the full artifact, and the commit discarded the adopted
    // bytes. After adoption the pull must land on the canonical
    // path itself, where skip-present (S3 HEAD + zstd-magic check)
    // validates the adopted bytes without a transfer.
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    let uri = "s3://bucket/sbx/snap-1/memfile";
    let temp = PullReplacementGuard::for_existing(&local_path, uri).download_path(&local_path);
    std::fs::write(&temp, b"downloaded").unwrap();

    let adopted = adopt_completed_replacement_temp("sbx-test", &local_path, uri);
    assert!(adopted);
    let replacement = replacement_for_pull(&local_path, uri, adopted);

    assert_eq!(
        replacement.download_path(&local_path),
        local_path,
        "after adoption the pull must target the adopted file in place"
    );
}

#[test]
fn adoption_is_a_noop_without_a_matching_temp_or_with_artifact_present() {
    let tmp = tempfile::tempdir().unwrap();
    let local_path = tmp.path().join("memory");
    let uri = "s3://bucket/sbx/snap-1/memfile";

    // No temp, no artifact: nothing materializes.
    adopt_completed_replacement_temp("sbx-test", &local_path, uri);
    assert!(!local_path.exists());

    // Artifact present: an existing temp is left for the normal
    // replacement flow (commit by rename after validation).
    std::fs::write(&local_path, b"current").unwrap();
    let temp = PullReplacementGuard::for_existing(&local_path, uri).download_path(&local_path);
    std::fs::write(&temp, b"newer").unwrap();
    adopt_completed_replacement_temp("sbx-test", &local_path, uri);
    assert_eq!(std::fs::read(&local_path).unwrap(), b"current");
    assert!(temp.exists());
}

#[test]
fn sweep_is_a_noop_when_parent_dir_is_missing() {
    // First pull into a fresh snapshot dir: nothing to sweep, no panic.
    sweep_stale_pull_replacements(
        "sbx-test",
        Path::new("/nonexistent-space-sweep-test/snapshot/memory"),
        None,
    );
}

#[test]
fn sandbox_cache_dir_guard_wipes_dir_when_armed_at_drop() {
    // Models the inter-phase failure window in run_cross_node_restore:
    // phase 1 has returned Ok(staged), the dir exists on disk, but a
    // ? on `SandboxId::from_str` / `restored_snapshot_sandbox_fields`
    // / `insert_sandbox` short-circuits before the guard is disarmed.
    // The Drop must wipe the dir so the periodic reconciler doesn't
    // adopt it as a state=Failed orphan.
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path().join("019e31d9-79a6-7132-b90f-007f1c62ddea");
    std::fs::create_dir_all(sandbox_dir.join("snapshot")).unwrap();
    std::fs::create_dir_all(sandbox_dir.join("snapshots")).unwrap();
    std::fs::write(sandbox_dir.join("snapshot/snapshot.json"), b"{}").unwrap();

    {
        let _guard = SandboxCacheDirGuard::for_restore_staging(&sandbox_dir, None);
        assert!(sandbox_dir.exists());
    }

    assert!(
        !sandbox_dir.exists(),
        "armed guard must wipe the sandbox cache root on Drop",
    );
}

#[test]
fn sandbox_cache_dir_guard_leaves_dir_when_disarmed() {
    // Happy path: the row insert succeeded, the guard is disarmed,
    // Drop must NOT wipe the dir (phase 2 needs to read from it).
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path().join("019e31d9-79a6-7132-b90f-007f1c62ddea");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    std::fs::write(sandbox_dir.join("marker"), b"keep").unwrap();

    {
        let mut guard = SandboxCacheDirGuard::for_restore_staging(&sandbox_dir, None);
        guard.disarm();
    }

    assert!(
        sandbox_dir.exists(),
        "disarmed guard must NOT wipe the sandbox cache root",
    );
    assert!(sandbox_dir.join("marker").exists());
}

#[test]
fn sandbox_cache_dir_guard_defers_scoped_staging_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path().join(".sandbox.restore-scope");
    std::fs::create_dir_all(&sandbox_dir).unwrap();
    std::fs::write(sandbox_dir.join("marker"), b"keep").unwrap();

    {
        let _guard = SandboxCacheDirGuard::for_restore_staging(
            &sandbox_dir,
            Some(pplx_space_common_core::types::TransferScopeId::new()),
        );
    }

    assert!(sandbox_dir.join("marker").exists());
}

#[test]
fn metadata_admission_rejection_keeps_attributed_restore_files() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path().join("019e31d9-79a6-7132-b90f-007f1c62ddea");
    let preserved = sandbox_dir.join("snapshot/.memory.restore-preserved");
    std::fs::create_dir_all(preserved.parent().unwrap()).unwrap();
    std::fs::write(&preserved, b"downloaded").unwrap();

    {
        let mut guard = SandboxCacheDirGuard::for_restore_staging(&sandbox_dir, None);
        let result = preserve_attributed_metadata_retry(
            &mut guard,
            true,
            Err(RestoreError::AdmissionRejected(
                tonic::Status::resource_exhausted("metadata pool full"),
            )),
        );
        assert!(matches!(result, Err(RestoreError::AdmissionRejected(_))));
    }

    assert!(
        preserved.exists(),
        "an existing Restoring row attributes the retry files, so metadata backpressure must not delete them"
    );
}

#[tokio::test]
async fn cleanup_partial_restore_artifacts_requires_acknowledgement() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = "019e31d9-79a6-7132-b90f-007f1c62ddea";
    let sandbox_dir = tmp.path().join(sandbox_id);
    let snapshot_dir = sandbox_dir.join("snapshot");
    let instance_dir = sandbox_dir.join("instance");
    let snapshots_dir = sandbox_dir.join("snapshots");
    for d in [&snapshot_dir, &instance_dir, &snapshots_dir] {
        std::fs::create_dir_all(d).unwrap();
    }
    std::fs::write(snapshot_dir.join("snapshot.json"), b"{}").unwrap();

    use crate::btrfs_via_nodemgr::BtrfsViaNodemgrError;
    for error in [
        BtrfsViaNodemgrError::Connect("no socket".to_string()),
        BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsDeleteSandboxInstance",
            status: tonic::Status::unavailable("receive still owns the target"),
        },
        BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsDeleteSandboxInstance",
            status: tonic::Status::internal("subvolume deletion failed"),
        },
    ] {
        cleanup_partial_restore_artifacts_with(
            &sandbox_dir,
            sandbox_id,
            None,
            std::future::ready(Err(error)),
        )
        .await;
        assert!(snapshot_dir.join("snapshot.json").exists());
        assert!(instance_dir.exists());
        assert!(snapshots_dir.exists());
    }
    // Dropping a waiter before acknowledgement cannot permit removal either.
    {
        use futures_util::FutureExt;
        let cleanup = cleanup_partial_restore_artifacts_with(
            &sandbox_dir,
            sandbox_id,
            None,
            std::future::pending(),
        );
        assert!(cleanup.now_or_never().is_none());
        assert!(snapshot_dir.join("snapshot.json").exists());
    }
    cleanup_partial_restore_artifacts_with(
        &sandbox_dir,
        sandbox_id,
        Some(sandbox_id.parse().unwrap()),
        async { panic!("scope cleanup must wait for the durable owner's transfer cancellation") },
    )
    .await;
    assert!(instance_dir.exists());
    assert!(snapshot_dir.join("snapshot.json").exists());
    let retained = snapshots_dir.join("d-1m-retained");
    std::fs::create_dir(&retained).unwrap();
    std::fs::write(retained.join("rootfs.ext4"), b"retained disk snapshot").unwrap();
    // Acknowledgement covers only the requested artifacts, not every file
    // under the sandbox root. A retry must not erase unrelated snapshots.
    for _ in 0..2 {
        cleanup_partial_restore_artifacts_with(
            &sandbox_dir,
            sandbox_id,
            None,
            std::future::ready(Ok(())),
        )
        .await;
        assert_eq!(
            std::fs::read(retained.join("rootfs.ext4")).unwrap(),
            b"retained disk snapshot"
        );
    }
}

#[test]
fn restore_key_round_trips_with_cluster_prefix() {
    let sandbox_id = "019e31d9-79a6-7132-b90f-007f1c62ddea";
    let snapshot_id = "019253ab-cdef-7000-8123-000000000001";
    let storage_uri = build_storage_uri_for_tests(
        sandbox_id,
        snapshot_id,
        "test-bucket",
        "us-east-1",
        "clusters/abc",
    );
    for artifact in [
        SnapshotArtifact::VmState,
        SnapshotArtifact::Memory,
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::Metadata,
    ] {
        let key = restore_key_for(&storage_uri, artifact);
        let expected = format!(
            "clusters/abc/{}",
            upload_key_for(sandbox_id, snapshot_id, artifact),
        );
        assert_eq!(key, expected, "{artifact:?}");
    }
}

fn staging_err(msg: &str) -> RestoreError {
    RestoreError::Other(msg.to_string())
}

#[tokio::test(start_paused = true)]
async fn btrfs_staging_receives_as_soon_as_its_inputs_are_ready() {
    // The memory download is not an input to filesystem reconstruction. Both
    // must finish before staging returns, including when receive is slower.
    for (template_ms, rootfs_ms, memory_ms, receive_ms, expected_ms) in [
        (10, 20, Some(100), 30, 100),
        (40, 20, Some(100), 80, 120),
        (10, 20, None, 30, 50),
    ] {
        let started = tokio::time::Instant::now();
        let transfer = |ms| -> BoxFuture<'_, Result<(), RestoreError>> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })
        };
        let receive = Box::pin(async move {
            assert_eq!(
                started.elapsed(),
                Duration::from_millis(template_ms.max(rootfs_ms)),
                "receive must wait only for its template and rootfs stream"
            );
            tokio::time::sleep(Duration::from_millis(receive_ms)).await;
            Ok(())
        });
        run_btrfs_staging(
            transfer(template_ms),
            vec![transfer(rootfs_ms)],
            memory_ms.into_iter().map(transfer).collect(),
            receive,
        )
        .await
        .unwrap();
        assert_eq!(started.elapsed(), Duration::from_millis(expected_ms));
    }
}

#[tokio::test(start_paused = true)]
async fn btrfs_staging_joins_failures_before_cleanup_and_preserves_error_order() {
    use std::sync::atomic::{AtomicU8, Ordering};

    // Bits name template, rootfs, runtime download, and receive failures.
    // Combined failures pin the existing diagnostic/cleanup precedence.
    for (failures, expected) in [
        (0, None),
        (1, Some("template")),
        (2, Some("rootfs")),
        (4, Some("runtime")),
        (8, Some("receive")),
        (7, Some("template")),
        (6, Some("runtime")),
        (12, Some("runtime")),
    ] {
        let completed = AtomicU8::new(0);
        let stage = |bit, name, ms| -> BoxFuture<'_, Result<(), RestoreError>> {
            let completed = &completed;
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                completed.fetch_or(bit, Ordering::Relaxed);
                if failures & bit != 0 {
                    Err(staging_err(name))
                } else {
                    Ok(())
                }
            })
        };
        let result = run_btrfs_staging(
            stage(1, "template", 10),
            vec![stage(2, "rootfs", 20)],
            vec![stage(4, "runtime", 100)],
            stage(8, "receive", 30),
        )
        .await;
        assert_eq!(
            completed.load(Ordering::Relaxed),
            if failures & 3 == 0 { 15 } else { 7 },
            "every transfer must finish, and receive requires both inputs"
        );
        match expected {
            None => result.unwrap(),
            Some(expected) => {
                assert!(result.unwrap_err().to_string().contains(expected));
            }
        }
    }
}

#[tokio::test]
async fn staging_transfers_overlap_template_ensure_with_pulls() {
    // Cross-dependency only satisfiable under concurrency: the template
    // ensure blocks until a pull has started. The pre-parallel ordering
    // (template ensure strictly before any pull) deadlocks here.
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let template: BoxFuture<'_, Result<(), RestoreError>> = Box::pin(async move {
        started_rx
            .await
            .map_err(|_| staging_err("pull never started"))
    });
    let pull: BoxFuture<'_, Result<(), RestoreError>> = Box::pin(async move {
        let _ = started_tx.send(());
        Ok(())
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        run_staging_transfers(Some(template), vec![pull]),
    )
    .await
    .expect("template ensure must overlap the artifact pulls, not precede them")
    .expect("staging should succeed");
}

#[tokio::test]
async fn staging_transfers_run_all_pulls_to_completion_despite_failure() {
    // No cancel-on-first-error: a failing sibling must not tear down the
    // other transfers (cancelling mid-pull strands replacement temps and
    // loses the dedupe cooperation retried restores rely on).
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let second_completed = Arc::new(AtomicBool::new(false));
    let third_completed = Arc::new(AtomicBool::new(false));

    let first: BoxFuture<'_, Result<(), RestoreError>> =
        Box::pin(async { Err(staging_err("first pull failed")) });
    let second = {
        let flag = Arc::clone(&second_completed);
        let fut: BoxFuture<'_, Result<(), RestoreError>> = Box::pin(async move {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            flag.store(true, Ordering::SeqCst);
            Ok(())
        });
        fut
    };
    let third = {
        let flag = Arc::clone(&third_completed);
        let fut: BoxFuture<'_, Result<(), RestoreError>> = Box::pin(async move {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            flag.store(true, Ordering::SeqCst);
            Err(staging_err("third pull failed"))
        });
        fut
    };

    let result = run_staging_transfers(None, vec![first, second, third]).await;

    assert!(
        second_completed.load(Ordering::SeqCst),
        "sibling pull was cancelled by an earlier failure"
    );
    assert!(
        third_completed.load(Ordering::SeqCst),
        "sibling pull was cancelled by an earlier failure"
    );
    // First error in submission order wins, not completion order.
    let err = result.expect_err("staging must fail when a pull fails");
    assert!(
        err.to_string().contains("first pull failed"),
        "expected the first submitted pull's error, got: {err}"
    );
}

#[tokio::test]
async fn staging_transfer_errors_prefer_template_ensure() {
    // The template gates the receive/clone step, so its failure is the
    // actionable diagnostic even when a pull also failed.
    let template: BoxFuture<'_, Result<(), RestoreError>> =
        Box::pin(async { Err(staging_err("template ensure failed")) });
    let pull: BoxFuture<'_, Result<(), RestoreError>> =
        Box::pin(async { Err(staging_err("pull failed")) });

    let err = run_staging_transfers(Some(template), vec![pull])
        .await
        .expect_err("staging must fail");
    assert!(
        err.to_string().contains("template ensure failed"),
        "expected the template error to take priority, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Plan-response resolution
//
// Every rejection branch here decides whether the node acts on a central plan or
// declines and reschedules. They sat behind `connect_from_env` and so had no
// coverage at all; each one now names the field and value it rejected, which is
// the difference between a diagnosable loop and a silent one.
// ---------------------------------------------------------------------------

fn plan_response(
    recovery_id: &str,
    snapshot_id: &str,
    local_artifact_id: &str,
    storage_uri: &str,
    execution_generation: i64,
    reason_code: &str,
    target_state: i32,
) -> pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanResponse {
    pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanResponse {
        recovery_id: recovery_id.to_string(),
        execution_generation,
        reason_code: reason_code.to_string(),
        target_state,
        suspend_snapshot: Some(
            pplx_space_node_api_client::proto::QuarantineRecoverySuspendSnapshot {
                snapshot_id: snapshot_id.to_string(),
                local_artifact_id: local_artifact_id.to_string(),
                storage_uri: storage_uri.to_string(),
                bytes_uploaded: 4096,
            },
        ),
    }
}

fn valid_plan_response() -> pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanResponse {
    plan_response(
        &pplx_space_common_core::types::SandboxCleanupEpisodeId::new().to_string(),
        &uuid::Uuid::now_v7().to_string(),
        &LocalArtifactId::new().to_string(),
        "s3://snapshots/suspend/sandbox/snap?region=us-east-1",
        7,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed.as_str(),
        pplx_space_node_api_client::proto::QuarantineRecoveryTargetState::Suspended as i32,
    )
}

fn malformed_reason(
    response: pplx_space_node_api_client::proto::GetQuarantineRecoveryPlanResponse,
) -> crate::db::QuarantineRecoveryNoProgressReason {
    match resolve_quarantine_recovery_plan_response("sandbox-under-test", response)
        .expect("a malformed plan is a no-progress result, not an error")
    {
        QuarantineRecoveryPlanResolution::NoProgress(reason) => reason,
        QuarantineRecoveryPlanResolution::Available(_) => {
            panic!("expected the plan to be rejected")
        }
    }
}

#[test]
fn a_well_formed_plan_resolves_to_an_actionable_plan() {
    // Control: without this the rejection assertions below could all pass because
    // the parser rejects everything.
    let resolution =
        resolve_quarantine_recovery_plan_response("sandbox-under-test", valid_plan_response())
            .unwrap();
    let QuarantineRecoveryPlanResolution::Available(plan) = resolution else {
        panic!("a well-formed plan must be actionable");
    };
    assert_eq!(plan.execution_generation.get(), 7);
    assert_eq!(
        plan.reason_code,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed
    );
}

#[test]
fn every_unusable_plan_field_is_rejected_as_malformed() {
    use crate::db::QuarantineRecoveryNoProgressReason as Reason;

    let mut no_snapshot = valid_plan_response();
    no_snapshot.suspend_snapshot = None;
    assert_eq!(malformed_reason(no_snapshot), Reason::PlanMalformed);

    let mut bad_recovery_id = valid_plan_response();
    bad_recovery_id.recovery_id = "not-a-uuid".to_string();
    assert_eq!(malformed_reason(bad_recovery_id), Reason::PlanMalformed);

    let mut bad_snapshot_id = valid_plan_response();
    bad_snapshot_id
        .suspend_snapshot
        .as_mut()
        .unwrap()
        .snapshot_id = "not-a-uuid".to_string();
    assert_eq!(malformed_reason(bad_snapshot_id), Reason::PlanMalformed);

    let mut bad_artifact_id = valid_plan_response();
    bad_artifact_id
        .suspend_snapshot
        .as_mut()
        .unwrap()
        .local_artifact_id = "not-a-uuid".to_string();
    assert_eq!(malformed_reason(bad_artifact_id), Reason::PlanMalformed);

    // Zero is not a valid execution generation; accepting it would fence a
    // recovery against a generation that cannot exist.
    let mut bad_generation = valid_plan_response();
    bad_generation.execution_generation = 0;
    assert_eq!(malformed_reason(bad_generation), Reason::PlanMalformed);

    let mut bad_reason = valid_plan_response();
    bad_reason.reason_code = "not_a_reason_this_release_knows".to_string();
    assert_eq!(malformed_reason(bad_reason), Reason::PlanMalformed);
}

#[test]
fn a_plan_targeting_an_unsupported_state_is_declined_not_attempted() {
    let mut unspecified = valid_plan_response();
    unspecified.target_state =
        pplx_space_node_api_client::proto::QuarantineRecoveryTargetState::Unspecified as i32;
    assert_eq!(
        malformed_reason(unspecified),
        crate::db::QuarantineRecoveryNoProgressReason::UnsupportedTargetState,
        "a target state this release cannot honor must be declined distinctly from \
         a malformed field, so a newer control plane reads as skew rather than corruption"
    );
}

#[test]
fn a_plan_for_a_never_recoverable_reason_is_declined() {
    let mut ineligible = valid_plan_response();
    // A reason this release parses but never recovers automatically.
    ineligible.reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::PauseFailed
            .as_str()
            .to_string();
    assert_eq!(
        malformed_reason(ineligible),
        crate::db::QuarantineRecoveryNoProgressReason::PlanReasonIneligible
    );
}

#[test]
fn a_legacy_snapshot_without_artifact_identity_is_still_actionable() {
    // Empty local_artifact_id is the documented legacy shape, not a malformation.
    let mut legacy = valid_plan_response();
    legacy.suspend_snapshot.as_mut().unwrap().local_artifact_id = String::new();
    let resolution =
        resolve_quarantine_recovery_plan_response("sandbox-under-test", legacy).unwrap();
    let QuarantineRecoveryPlanResolution::Available(plan) = resolution else {
        panic!("a legacy snapshot must remain actionable");
    };
    assert!(plan.local_artifact_id.is_none());
}

/// A malformed plan is logged once per attempt per episode, and every value in the
/// detail arrives from the control plane unvalidated and unlengthed. The bound has
/// to sit on the whole rendered detail: the parse errors render the offending value
/// a second time themselves, so bounding only the field leaves the `{error}` beside
/// it free to log the original in full.
#[test]
fn a_malformed_plan_detail_is_bounded_before_it_reaches_a_log() {
    let short = super::bounded_log_detail("invalid recovery reason \"nope\"");
    assert_eq!(short, "invalid recovery reason \"nope\"");

    // The shape the production sites build: a bounded-looking field followed by a
    // parse error whose own Display embeds the same unbounded value.
    let hostile = "x".repeat(4096);
    let reason_code: Result<pplx_space_common_core::types::SandboxQuarantineReasonCode, _> =
        hostile.parse();
    let error = reason_code.expect_err("a 4 KiB reason code is not a known vocabulary member");
    let detail = format!("invalid recovery reason {hostile:?}: {error}");
    assert!(
        detail.len() > 8000,
        "the unbounded detail must carry the value twice for this to be a real test: {} bytes",
        detail.len()
    );

    let bounded = super::bounded_log_detail(&detail);
    assert!(
        bounded.len() < 512,
        "an unbounded detail must not reach the log line: {} bytes",
        bounded.len()
    );
    assert!(
        bounded.starts_with("invalid recovery reason"),
        "the diagnosis must survive the truncation: {bounded}"
    );
    assert!(
        bounded.contains("bytes)"),
        "the real length must survive the truncation: {bounded}"
    );

    // Truncation is on a character boundary, so a multi-byte detail cannot panic
    // the very log line reporting it as malformed.
    let multibyte = "é".repeat(400);
    let bounded = super::bounded_log_detail(&multibyte);
    assert!(bounded.contains("800 bytes"), "{bounded}");
}

/// The bound has to hold on the line the node actually logs, not on the helper.
///
/// A previous round bounded the interpolated field and left the parse error beside
/// it free to render the same value in full — `unknown {kind} '{value}'` — so the
/// 4 KiB string reached the log anyway and the helper's own test still passed. This
/// drives the production path with a hostile plan and reads the emitted field.
#[test]
fn a_hostile_plan_field_is_bounded_on_the_line_that_is_logged() {
    use tracing_subscriber::layer::SubscriberExt;

    let hostile = "x".repeat(4096);
    let response = plan_response(
        &pplx_space_common_core::types::SandboxCleanupEpisodeId::new().to_string(),
        &uuid::Uuid::now_v7().to_string(),
        &LocalArtifactId::new().to_string(),
        "s3://snapshots/suspend/sandbox/snap?region=us-east-1",
        7,
        &hostile,
        pplx_space_node_api_client::proto::QuarantineRecoveryTargetState::Suspended as i32,
    );

    let capture = crate::service::tests::service_tests::LogFieldCapture::new("detail");
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    let resolution =
        resolve_quarantine_recovery_plan_response("sandbox-under-test", response).unwrap();
    drop(guard);

    assert!(
        matches!(
            resolution,
            QuarantineRecoveryPlanResolution::NoProgress(
                crate::db::QuarantineRecoveryNoProgressReason::PlanMalformed
            )
        ),
        "a 4 KiB reason code is malformed, and the control is that this path ran at all"
    );
    let logged = capture.values.lock().unwrap().clone();
    let detail = logged
        .iter()
        .find(|value| value.starts_with("invalid recovery reason"))
        .expect("the malformed-plan line must have been emitted");
    assert!(
        detail.len() < 512,
        "the logged line must be bounded, however many times the value appears in \
         it: {} bytes",
        detail.len()
    );
    assert!(
        !detail.contains(&hostile),
        "the unbounded value must not appear in the logged line"
    );
    assert!(
        detail.contains("bytes)"),
        "the real length must survive the truncation: {detail}"
    );
}

#[test]
fn restored_sidecar_is_replaced_without_writing_the_pulled_file() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp
        .path()
        .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let mut meta = meta_with_diff_format(Some("btrfs-send-v1"));
    meta.rootfs_send_source_id = Some(uuid::Uuid::now_v7());
    std::fs::write(&path, serde_json::to_vec(&meta).unwrap()).unwrap();
    // Volume-manager owns the pulled file; emulate the closest a single-user
    // test can get to a file this service may read but not open for writing.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();

    meta.rootfs_send_source_id = None;
    replace_restored_sidecar(tmp.path(), &meta).unwrap();

    let written: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(written.rootfs_send_source_id, None);
    assert!(!tmp.path().join(".snapshot.json.restore").exists());
}

#[rstest::rstest]
#[case::root("root@local", true)]
#[case::operator("deploy@local", true)]
#[case::tenant("user:00000000-0000-0000-0000-000000000001", false)]
#[tokio::test]
async fn restore_template_lookup_uses_validated_owner(
    #[case] owner: &str,
    #[case] local: bool,
    #[values(false, true)] receipt: bool,
) {
    use pplx_space_common_svc::template_lookup::TemplateLookupScope;
    use pplx_space_template_mgr_client::proto::GetTemplateResponse;
    let id = pplx_space_common_core::types::SandboxId::new();
    let expected = if local {
        TemplateLookupScope::LocalExecution
    } else {
        TemplateLookupScope::Sandbox {
            id,
            caller_node: None,
        }
    };
    let templates = crate::template_resolver::fake::FakeTemplateResolver::new(move || {
        let mut response = tonic::Response::new(GetTemplateResponse {
            template_dir: "/synthetic/cached-base".into(),
            ..Default::default()
        });
        if receipt {
            expected.receipt(&mut response);
        }
        Ok(response)
    });

    let path = ensure_local_template(
        &templates,
        &id.to_string(),
        &owner.parse().unwrap(),
        "base",
        "sha256:cached",
    )
    .await
    .unwrap();

    assert_eq!(path, "/synthetic/cached-base");
    let lookup = templates.lookups().pop().unwrap();
    let mut request = tonic::Request::new(lookup.request);
    *request.metadata_mut() = lookup.metadata;
    assert_eq!(
        TemplateLookupScope::from_request(&request).unwrap(),
        Some(expected)
    );
}

// A stopped materialization refused because the staged artifact isn't the
// evacuated disk the caller named is evidence disagreeing with itself, the
// same class as the other identity mismatches, not a filesystem or database
// failure.
#[test]
fn a_refused_stopped_materialization_is_a_source_mismatch_stage_failure() {
    let unsuitable = RestoreError::StoppedMaterializationArtifactUnsuitable {
        kind: Some(ArtifactKind::Full),
        origin: Some(ArtifactOrigin::Suspend),
        local_artifact_id: Some(LocalArtifactId::new()),
    };
    let mismatch = RestoreError::StoppedMaterializationArtifactMismatch {
        expected: LocalArtifactId::new(),
        staged: LocalArtifactId::new(),
    };
    for error in [unsuitable, mismatch] {
        assert_eq!(
            error.stage_error_kind().as_str(),
            pplx_space_metrics::snapshot::RestoreStageErrorKind::SourceMismatch.as_str(),
            "{error}"
        );
    }
}

// The two stopped-materialization refusals are the only stage-in failures a
// retry can't help, so they're the only ones the `Start` paths report as
// `FailedPrecondition`. A sibling source-mismatch variant stays retryable.
#[test]
fn only_the_stopped_refusals_are_classified_as_permanent() {
    let unsuitable = RestoreError::StoppedMaterializationArtifactUnsuitable {
        kind: None,
        origin: None,
        local_artifact_id: None,
    };
    let mismatch = RestoreError::StoppedMaterializationArtifactMismatch {
        expected: LocalArtifactId::new(),
        staged: LocalArtifactId::new(),
    };
    assert!(unsuitable.is_stopped_materialization_refusal());
    assert!(mismatch.is_stopped_materialization_refusal());
    assert!(!RestoreError::MissingSandboxOwnershipMetadata.is_stopped_materialization_refusal());
}
