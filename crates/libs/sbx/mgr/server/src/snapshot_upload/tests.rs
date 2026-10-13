use pplx_space_common_core::types::JobId;

use super::*;

fn fixture_config() -> SnapshotUploadConfig {
    SnapshotUploadConfig {
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: String::new(),
        endpoint: None,
        path_style: false,
    }
}

#[test]
fn object_store_identity_excludes_only_the_object_prefix() {
    let source = fixture_config();
    let mut destination = source.clone();
    destination.key_prefix = "fork/destination".to_string();
    assert!(source.same_store(&destination));

    for different in [
        SnapshotUploadConfig {
            bucket: "other-bucket".to_string(),
            ..destination.clone()
        },
        SnapshotUploadConfig {
            region: "us-west-2".to_string(),
            ..destination.clone()
        },
        SnapshotUploadConfig {
            endpoint: Some("http://object-store.test".to_string()),
            ..destination.clone()
        },
        SnapshotUploadConfig {
            path_style: true,
            ..destination
        },
    ] {
        assert!(!source.same_store(&different));
    }
}

fn fixture_meta(rootfs_diff_format: Option<&str>) -> SnapshotMeta {
    SnapshotMeta {
        version: 1,
        execution: None,
        sandbox_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".to_string(),
        size_bytes: 0,
        created_at: "2026-05-25T00:00:00Z".to_string(),
        source: SnapshotSource {
            runtime: "fc".to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: "seed".to_string(),
            content_hash: None,
            rootfs_size_mib: None,
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
        },
        sandbox: None,
        rootfs_diff_format: rootfs_diff_format.map(str::to_string),
        rootfs_send_source_id: None,
        encrypted: false,
        local_artifact_id: None,
        artifact_kind: None,
        artifact_origin: None,
        artifact_snapshot_name: None,
    }
}

/// A deferred artifact as sandbox-manager sees it: VM state and a retained
/// bundle it cannot traverse. Access is restored on drop for cleanup.
struct DeferredArtifact {
    _temp: tempfile::TempDir,
    dir: std::path::PathBuf,
    source_id: uuid::Uuid,
}

impl DeferredArtifact {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("snapshot");
        let bundle = dir.join(crate::retained_source::BUNDLE_DIRECTORY);
        std::fs::create_dir_all(bundle.join("source")).unwrap();
        std::fs::write(bundle.join("source/rootfs.ext4"), b"jail-owned rootfs").unwrap();
        std::fs::set_permissions(&bundle, std::fs::Permissions::from_mode(0o000)).unwrap();
        std::fs::write(dir.join("memory"), b"memory").unwrap();
        std::fs::write(dir.join("vmstate"), b"state").unwrap();
        Self {
            _temp: temp,
            dir,
            source_id: uuid::Uuid::now_v7(),
        }
    }

    fn meta(&self) -> SnapshotMeta {
        let mut meta = fixture_meta(Some(ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1));
        meta.artifact_kind = Some(ArtifactKind::Full);
        meta.rootfs_send_source_id = Some(self.source_id);
        meta
    }

    /// Node-manager's answer for this artifact's retained source.
    fn retained(&self, rootfs_bytes: u64, stream: bool) -> RetainedUpload {
        RetainedUpload {
            source: crate::retained_source::RetainedSource {
                sandbox_id: self.meta().sandbox_id,
                artifact_dir: self.dir.clone(),
                source_id: self.source_id,
            },
            rootfs_bytes,
            stream,
        }
    }

    fn prepare(&self, retained: Option<RetainedUpload>) -> Result<SnapshotUploadJobs, String> {
        prepare_suspend_snapshot(
            &self.meta(),
            &uuid::Uuid::now_v7().to_string(),
            &self.dir,
            &fixture_config(),
            ArtifactKind::Full,
            SuspendSnapshotFormat::BtrfsSend,
            retained,
        )
    }
}

impl DeferredArtifact {
    /// A materialized stream beside the retained source.
    fn with_stream(self, stream: &[u8]) -> Self {
        std::fs::write(self.dir.join(ROOTFS_BTRFS_SEND_FILENAME), stream).unwrap();
        self
    }

    fn staged_metadata(prepared: &SnapshotUploadJobs) -> serde_json::Value {
        let path = prepared
            .metadata_staging_dir
            .as_ref()
            .expect("retained artifacts stage their uploaded metadata separately")
            .join(SNAPSHOT_METADATA_FILENAME);
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    fn remove_artifact_dir(&self) {
        self.restore_access();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }

    fn restore_access(&self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(
            self.dir.join(crate::retained_source::BUNDLE_DIRECTORY),
            std::fs::Permissions::from_mode(0o750),
        );
    }
}

impl Drop for DeferredArtifact {
    fn drop(&mut self) {
        self.restore_access();
    }
}

#[test]
fn deferred_send_prepares_from_the_inspected_size_and_keeps_metadata_outside_the_artifact() {
    let artifact = DeferredArtifact::new();
    let prepared = artifact
        .prepare(Some(artifact.retained(1000, true)))
        .unwrap();
    assert!(prepared.deferred_metadata);
    assert_eq!(
        prepared.meta.rootfs_send_source_id,
        Some(artifact.source_id)
    );
    assert_eq!(
        prepared.meta.size_bytes,
        1000 + b"memory".len() as u64 + b"state".len() as u64
    );
    assert!(prepared.job_ids.is_empty());
    let metadata = prepared.metadata_staging_dir.as_ref().unwrap();
    assert!(!metadata.starts_with(&artifact.dir));
    assert!(metadata.join(SNAPSHOT_METADATA_FILENAME).is_file());
    assert!(!artifact.dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());
    // Teardown can remove the artifact after VM staging. The metadata
    // worker must have a surviving, separately owned directory.
    artifact.remove_artifact_dir();
    assert!(metadata.is_dir());
    cleanup_metadata_staging_dir(&prepared);
    assert!(!metadata.exists());
}

#[test]
fn evidence_for_another_source_does_not_prepare_this_artifact() {
    let artifact = DeferredArtifact::new();
    let other = DeferredArtifact::new();
    let error = artifact
        .prepare(Some(other.retained(1000, true)))
        .unwrap_err();
    assert!(error.contains("was inspected as"), "{error}");
}

#[test]
fn retained_source_without_streaming_prepares_for_materialization() {
    let artifact = DeferredArtifact::new();
    let prepared = artifact
        .prepare(Some(artifact.retained(1000, false)))
        .unwrap();
    // The worker regenerates rootfs.btrfs-send from this exact source and
    // uploads the file. The payload keeps the identity it needs for that;
    // the staged copy, rewritten after materialization, never carries it.
    assert!(!prepared.deferred_metadata);
    assert_eq!(
        prepared.meta.rootfs_send_source_id,
        Some(artifact.source_id)
    );
    let staged = DeferredArtifact::staged_metadata(&prepared);
    assert!(staged.get("rootfs_send_source_id").is_none(), "{staged}");
    cleanup_metadata_staging_dir(&prepared);
}

#[test]
fn retained_artifact_with_a_stream_uploads_the_file_and_strips_its_identity_remotely() {
    let artifact = DeferredArtifact::new().with_stream(&[0u8; 333]);
    let prepared = artifact
        .prepare(Some(artifact.retained(1000, true)))
        .unwrap();
    // A stream already on disk has a known length: no deferral.
    assert!(!prepared.deferred_metadata);
    assert_eq!(
        prepared.meta.rootfs_send_source_id,
        Some(artifact.source_id)
    );
    let vm = (b"memory".len() + b"state".len()) as u64;
    let staged = DeferredArtifact::staged_metadata(&prepared);
    assert_eq!(staged["size_bytes"], 333 + vm);
    assert!(staged.get("rootfs_send_source_id").is_none(), "{staged}");
    // The artifact's own sidecar counts the stream while it exists and keeps
    // the identity; reclaim switches it to the image when the stream goes.
    let local = write_snapshot_metadata(
        &artifact.meta(),
        &artifact.dir,
        &vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend),
        None,
    )
    .unwrap();
    assert_eq!(local.size_bytes, 333 + vm);
    let written: SnapshotMeta = serde_json::from_slice(
        &std::fs::read(artifact.dir.join(SNAPSHOT_METADATA_FILENAME)).unwrap(),
    )
    .unwrap();
    assert_eq!(written.rootfs_send_source_id, Some(artifact.source_id));
    cleanup_metadata_staging_dir(&prepared);
}

#[test]
fn retained_source_must_be_inspected_before_preparation() {
    let artifact = DeferredArtifact::new();
    let error = artifact.prepare(None).unwrap_err();
    assert!(error.contains(&artifact.source_id.to_string()), "{error}");
    assert!(error.contains("not inspected"), "{error}");
}

#[test]
fn btrfs_send_without_stream_or_identity_is_missing_its_stream() {
    let artifact = DeferredArtifact::new();
    let mut meta = artifact.meta();
    meta.rootfs_send_source_id = None;
    let error = prepare_suspend_snapshot(
        &meta,
        &uuid::Uuid::now_v7().to_string(),
        &artifact.dir,
        &fixture_config(),
        ArtifactKind::Full,
        SuspendSnapshotFormat::BtrfsSend,
        Some(artifact.retained(1, true)),
    )
    .unwrap_err();
    assert!(error.contains(ROOTFS_BTRFS_SEND_FILENAME), "{error}");
}

#[test]
fn metadata_sizing_names_the_path_it_could_not_stat() {
    let artifact = DeferredArtifact::new();
    std::fs::remove_file(artifact.dir.join("memory")).unwrap();
    let error = write_snapshot_metadata(
        &artifact.meta(),
        &artifact.dir,
        &vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend),
        Some(1),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains(&artifact.dir.join("memory").display().to_string()),
        "{error}"
    );
    // A retained source without node-manager's size cannot be sized here.
    std::fs::write(artifact.dir.join("memory"), b"memory").unwrap();
    let error = write_snapshot_metadata(
        &artifact.meta(),
        &artifact.dir,
        &vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend),
        None,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        SnapshotMetadataWriteError::UnsizedRetainedSource { source_id, .. }
            if source_id == artifact.source_id
    ));
}

#[test]
fn disk_artifacts_preserve_the_persisted_rootfs_codec() {
    let full = artifact_files_for_kind(ArtifactKind::Disk, SuspendSnapshotFormat::Full);
    assert_eq!(full.len(), 1);
    assert_eq!(full[0].kind, SnapshotArtifact::Rootfs);
    assert_eq!(full[0].local_name, ROOTFS_FULL_FILENAME);

    let btrfs_send = artifact_files_for_kind(ArtifactKind::Disk, SuspendSnapshotFormat::BtrfsSend);
    assert_eq!(btrfs_send.len(), 1);
    assert_eq!(btrfs_send[0].kind, SnapshotArtifact::RootfsBtrfsSend);
    assert_eq!(btrfs_send[0].local_name, ROOTFS_BTRFS_SEND_FILENAME);
}

#[test]
fn upload_artifacts_append_metadata_after_vm_artifacts() {
    // The metadata document is the last entry in the upload set for
    // every codec, sitting after the VM artifacts in the order
    // `submit_suspend_snapshot` queues them. The durable worker polls
    // this whole set before finalizing `ready`, so metadata's
    // presence here is what gates readiness on its upload (a `ready`
    // row must never point at a snapshot whose `metadata.json` is
    // still missing).
    for format in [
        SuspendSnapshotFormat::Full,
        SuspendSnapshotFormat::BtrfsSend,
    ] {
        let vm = vm_artifacts_for_format(format);
        let full = upload_artifacts_for_kind(ArtifactKind::Full, format);
        assert_eq!(full.len(), vm.len() + 1);
        assert_eq!(
            full[..vm.len()]
                .iter()
                .map(|a| a.local_name)
                .collect::<Vec<_>>(),
            vm.iter().map(|a| a.local_name).collect::<Vec<_>>(),
            "VM artifacts must precede metadata, unchanged in order",
        );
        let last = full.last().unwrap();
        assert_eq!(last.kind, SnapshotArtifact::Metadata);
        assert_eq!(last.local_name, SNAPSHOT_METADATA_FILENAME);
    }
}

#[test]
fn write_snapshot_metadata_sizes_from_local_artifacts_at_submit_time() {
    // Regression: `snapshot.json` used to be written by the durable
    // worker *after* every artifact upload finished, with
    // `size_bytes` summed from vol-mgr's reported transfer bytes.
    // That late `std::fs::write` targeted the per-sandbox
    // `snapshot_dir`, which a concurrent reclamation could remove
    // between the artifact uploads and the metadata write — surfacing
    // as `write snapshot.json failed: No such file or directory`.
    //
    // The fix computes the logical size from the local artifact files
    // and writes the sidecar at submit time, so it rides the same
    // staged push path as the VM artifacts. This test pins the
    // submit-time sizing + write.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let vm = vm_artifacts_for_format(SuspendSnapshotFormat::Full);
    std::fs::write(dir.join("vmstate"), vec![0u8; 10]).unwrap();
    std::fs::write(dir.join("memory"), vec![0u8; 20]).unwrap();
    std::fs::write(dir.join("rootfs.ext4"), vec![0u8; 30]).unwrap();

    let meta = fixture_meta(None);
    let metadata_path = dir.join(SNAPSHOT_METADATA_FILENAME);
    std::fs::write(&metadata_path, b"previous metadata").unwrap();
    // Restored metadata can be readable but not writable by this service.
    // A reader that already opened it must retain the previous complete bytes.
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&metadata_path, std::fs::Permissions::from_mode(0o444)).unwrap();
    let mut previous = std::fs::File::open(&metadata_path).unwrap();
    let out = write_snapshot_metadata(&meta, dir, &vm, None).unwrap();
    let mut previous_bytes = Vec::new();
    previous.read_to_end(&mut previous_bytes).unwrap();
    assert_eq!(previous_bytes, b"previous metadata");
    assert_eq!(out.size_bytes, 60, "size is the sum of local VM artifacts");

    let written = std::fs::read_to_string(dir.join(SNAPSHOT_METADATA_FILENAME)).unwrap();
    let parsed: SnapshotMeta = serde_json::from_str(&written).unwrap();
    assert_eq!(parsed.size_bytes, 60);
    assert_eq!(parsed.sandbox_id, meta.sandbox_id);
}

#[test]
fn write_snapshot_metadata_errors_when_an_artifact_is_missing() {
    // A missing VM artifact at submit time must surface as an error
    // rather than silently writing a zero/partial size.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let vm = vm_artifacts_for_format(SuspendSnapshotFormat::Full);
    std::fs::write(dir.join("vmstate"), vec![0u8; 10]).unwrap();
    // memory + rootfs.ext4 intentionally absent.
    let err = write_snapshot_metadata(&fixture_meta(None), dir, &vm, None).unwrap_err();
    let err = err.to_string();
    assert!(
        err.contains("memory"),
        "error names the missing artifact: {err}"
    );
}

#[test]
fn prepare_persists_exact_artifact_intent_without_remote_transfer_handles() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::write(dir.join("vmstate"), vec![0u8; 10]).unwrap();
    std::fs::write(dir.join("memory"), vec![0u8; 20]).unwrap();
    std::fs::write(dir.join("rootfs.ext4"), vec![0u8; 30]).unwrap();

    let local_artifact_id = LocalArtifactId::new();
    let mut meta = fixture_meta(None);
    meta.local_artifact_id = Some(local_artifact_id);
    meta.artifact_kind = Some(ArtifactKind::Full);
    meta.artifact_origin = Some(ArtifactOrigin::Suspend);

    let prepared = prepare_suspend_snapshot(
        &meta,
        "019e31d9-79a6-7132-b90f-0000000bbbbb",
        dir,
        &fixture_config(),
        ArtifactKind::Full,
        SuspendSnapshotFormat::Full,
        None,
    )
    .unwrap();

    assert!(prepared.job_ids.is_empty());
    assert!(prepared.includes_metadata_job);
    assert_eq!(prepared.meta.local_artifact_id, Some(local_artifact_id));
    assert_eq!(prepared.meta.size_bytes, 60);
    assert!(dir.join(SNAPSHOT_METADATA_FILENAME).exists());

    let recovered: SnapshotUploadJobs =
        serde_json::from_str(&serde_json::to_string(&prepared).unwrap()).unwrap();
    assert!(recovered.job_ids.is_empty());
    assert_eq!(recovered.snapshot_id, prepared.snapshot_id);
    assert_eq!(recovered.storage_uri, prepared.storage_uri);
    assert_eq!(recovered.meta.local_artifact_id, Some(local_artifact_id));
}

#[test]
fn snapshot_meta_artifact_identity_is_typed_but_wire_compatible() {
    let local_artifact_id = LocalArtifactId::new();
    let snapshot_name = SnapshotName::parse("manual-snapshot").unwrap();
    let mut meta = fixture_meta(None);
    meta.local_artifact_id = Some(local_artifact_id);
    meta.artifact_kind = Some(ArtifactKind::Full);
    meta.artifact_origin = Some(ArtifactOrigin::Snapshot);
    meta.artifact_snapshot_name = Some(snapshot_name.clone());

    let json = serde_json::to_string(&meta).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        value["local_artifact_id"].as_str(),
        Some(local_artifact_id.to_string().as_str())
    );
    assert_eq!(
        value["artifact_snapshot_name"].as_str(),
        Some(snapshot_name.as_str())
    );

    let parsed: SnapshotMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.local_artifact_id, Some(local_artifact_id));
    assert_eq!(parsed.artifact_snapshot_name, Some(snapshot_name));
}

#[test]
fn legacy_payload_deserializes_without_metadata_job_flag() {
    // A `SnapshotUploadJobs` serialized by code that predates the
    // staged metadata upload carries only the three VM artifact
    // job_ids and no `includes_metadata_job` field. It MUST
    // deserialize as a legacy job (flag = false) so startup rejects its
    // active form before cutover while terminal evidence stays readable.
    let legacy = r#"{
        "meta": {
            "version": 1,
            "sandbox_id": "019e31d9-79a6-7132-b90f-007f1c62ddea",
            "size_bytes": 0,
            "created_at": "2026-05-25T00:00:00Z",
            "source": {"runtime": "fc", "template_id": "seed"}
        },
        "snapshot_id": "019e31d9-79a6-7132-b90f-0000000aaaaa",
        "snapshot_dir": "/space/cache/sbx/snapshot",
        "config": {"bucket": "b", "region": "r", "key_prefix": "", "endpoint": null, "path_style": false},
        "storage_uri": "s3://b/suspend_snapshots/00/sbx/snap",
        "job_ids": [
            "019e31d9-79a6-7132-b90f-000000000001",
            "019e31d9-79a6-7132-b90f-000000000002",
            "019e31d9-79a6-7132-b90f-000000000003"
        ]
    }"#;
    let jobs: SnapshotUploadJobs = serde_json::from_str(legacy).unwrap();
    assert!(
        !jobs.includes_metadata_job,
        "legacy payloads default to no metadata job",
    );
    assert!(
        jobs.metadata_staging_dir.is_none(),
        "legacy payloads have no owned metadata staging directory"
    );
    assert_eq!(jobs.job_ids.len(), 3);
}

#[test]
fn new_payload_round_trips_metadata_job_flag() {
    let jobs = SnapshotUploadJobs {
        meta: fixture_meta(None),
        snapshot_id: "snap-1".to_string(),
        snapshot_dir: std::path::PathBuf::from("/space/cache/sbx/snapshot"),
        config: fixture_config(),
        storage_uri: "s3://b/x".to_string(),
        job_ids: (0..4).map(|_| JobId::new()).collect(),
        metadata_staging_dir: Some(std::path::PathBuf::from(
            "/space/cache/sbx/.artifact-metadata-snap-1",
        )),
        includes_metadata_job: true,
        deferred_metadata: false,
    };
    let round: SnapshotUploadJobs =
        serde_json::from_str(&serde_json::to_string(&jobs).unwrap()).unwrap();
    assert!(round.includes_metadata_job);
    assert_eq!(round.metadata_staging_dir, jobs.metadata_staging_dir);
}

#[test]
fn cleanup_metadata_staging_dir_removes_only_owned_staging_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("d-stop-artifact");
    let staging_dir = tmp.path().join(".artifact-metadata-snap-1");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::create_dir_all(&staging_dir).unwrap();
    std::fs::write(staging_dir.join(SNAPSHOT_METADATA_FILENAME), "{}").unwrap();

    let jobs = SnapshotUploadJobs {
        meta: fixture_meta(None),
        snapshot_id: "snap-1".to_string(),
        snapshot_dir: snapshot_dir.clone(),
        config: fixture_config(),
        storage_uri: "s3://b/x".to_string(),
        job_ids: Vec::new(),
        metadata_staging_dir: Some(staging_dir.clone()),
        includes_metadata_job: true,
        deferred_metadata: false,
    };

    cleanup_metadata_staging_dir(&jobs);

    assert!(
        snapshot_dir.exists(),
        "artifact directory is not owned here"
    );
    assert!(
        !staging_dir.exists(),
        "metadata staging directory should be removed"
    );
}

#[test]
fn metric_tag_rootfs_varies_per_codec() {
    // Regression: the metric tag for the rootfs slot is what
    // dashboards split on per codec. A positional const used to
    // emit `artifact:rootfs` for the btrfs-send codec too —
    // silently aggregating both codecs under the Full tile and
    // zeroing out every dashboard that splits on
    // `(codec, artifact)`. Pin the per-codec tag values directly
    // off the artifact builder so renaming a kind doesn't drift
    // the metric vocabulary.
    let full = vm_artifacts_for_format(SuspendSnapshotFormat::Full);
    let btrfs = vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend);

    assert_eq!(
        full.iter().map(|a| a.metric_tag()).collect::<Vec<_>>(),
        vec!["vmstate", "memory", "rootfs"],
    );
    assert_eq!(
        btrfs.iter().map(|a| a.metric_tag()).collect::<Vec<_>>(),
        vec!["vmstate", "memory", "rootfs.btrfs-send"],
    );
}

#[test]
fn format_from_meta_dispatches_on_diff_format() {
    // Mirrors `vm_artifacts_for_format` semantics: an absent or
    // unknown marker resolves to Full (forward-compat), the
    // explicit `btrfs-send-v1` marker resolves to BtrfsSend.
    assert_eq!(
        format_from_meta(&fixture_meta(None)),
        SuspendSnapshotFormat::Full,
    );
    assert_eq!(
        format_from_meta(&fixture_meta(Some(ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1))),
        SuspendSnapshotFormat::BtrfsSend,
    );
    assert_eq!(
        format_from_meta(&fixture_meta(Some("future-codec-v9"))),
        SuspendSnapshotFormat::Full,
    );
}

#[test]
fn storage_uri_is_stable_for_a_given_attempt() {
    let cfg = fixture_config();
    let uri_a = build_storage_uri("sbx-1", "snap-1", &cfg);
    let uri_b = build_storage_uri("sbx-1", "snap-1", &cfg);
    assert_eq!(uri_a, uri_b);
    let shard = snapshot_shard("sbx-1");
    assert_eq!(
        uri_a,
        format!("s3://test-bucket/suspend_snapshots/{shard:02x}/sbx-1/snap-1?region=us-east-1"),
        "storage URI must encode the per-attempt \
         suspend_snapshots/<shard>/<sandbox_id>/<snapshot_id> prefix",
    );
}

#[test]
fn storage_uri_distinct_per_snapshot_attempt() {
    let cfg = fixture_config();
    let a = build_storage_uri("sbx-1", "snap-1", &cfg);
    let b = build_storage_uri("sbx-1", "snap-2", &cfg);
    assert_ne!(a, b, "two attempts on the same sandbox must not collide");
}

#[test]
fn storage_uri_honours_key_prefix() {
    let mut cfg = fixture_config();
    cfg.key_prefix = "clusters/abc".to_string();
    let shard = snapshot_shard("sbx-1");
    assert_eq!(
        build_storage_uri("sbx-1", "snap-1", &cfg),
        format!(
            "s3://test-bucket/clusters/abc/suspend_snapshots/{shard:02x}/sbx-1/snap-1?region=us-east-1"
        ),
    );
}
