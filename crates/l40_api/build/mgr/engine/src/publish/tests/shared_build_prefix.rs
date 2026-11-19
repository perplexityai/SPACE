//! A release and its twins share one build prefix, keyed by template and
//! content hash only, so the publisher must never replace the control objects
//! another publication already put there.

use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pplx_space_object_store::{ObjectMeta, PutIfAbsent};

/// Objects by key. `put_bytes` replaces, as S3 does; `put_bytes_if_absent`
/// creates only.
#[derive(Clone, Default)]
struct SharedBucket {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl SharedBucket {
    fn get(&self, key: &str) -> Option<Bytes> {
        self.objects.lock().unwrap().get(key).cloned()
    }
}

#[async_trait]
impl ObjectStore for SharedBucket {
    async fn head(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        Ok(self.get(target.key).map(|bytes| ObjectMeta {
            size: bytes.len() as u64,
            etag: None,
        }))
    }

    async fn put_bytes(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<(), ObjectStoreError> {
        self.objects
            .lock()
            .unwrap()
            .insert(target.key.to_string(), body);
        Ok(())
    }

    async fn put_bytes_if_absent(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<PutIfAbsent, ObjectStoreError> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(target.key) {
            return Ok(PutIfAbsent::AlreadyExists);
        }
        objects.insert(target.key.to_string(), body);
        Ok(PutIfAbsent::Created)
    }

    async fn get_bytes(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        self.get(target.key)
            .map(|bytes| bytes.to_vec())
            .ok_or_else(|| ObjectStoreError::NotFound {
                key: target.key.to_string(),
            })
    }

    async fn put_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        src: &Path,
    ) -> Result<u64, ObjectStoreError> {
        let bytes = std::fs::read(src)?;
        let size = bytes.len() as u64;
        self.put_bytes(creds, target, Bytes::from(bytes)).await?;
        Ok(size)
    }

    async fn get_file(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        _dst: &Path,
    ) -> Result<u64, ObjectStoreError> {
        Err(ObjectStoreError::NotFound {
            key: target.key.to_string(),
        })
    }
}

fn config() -> ObjectStoreConfig {
    ObjectStoreConfig {
        bucket: "templates-bucket".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: String::new(),
        endpoint: None,
        path_style: false,
    }
}

fn credentials() -> ObjectCredentials {
    ObjectCredentials {
        access_key_id: "synthetic-access-key".to_string(),
        secret_access_key: "synthetic-secret-key".to_string(),
        session_token: None,
    }
}

fn key(artifact: TemplateArtifact) -> String {
    template_object_key("", &sample_template(), sample_hash(), artifact)
}

/// A build directory whose control objects describe `arch`'s execution,
/// with `build_date` in the manifest, so two builds of one execution can
/// still differ byte for byte.
fn build_dir(arch: Arch, build_date: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("build dir");
    write_metadata(dir.path(), arch);
    let manifest = serde_json::json!({
        "schema_version": TEMPLATE_MANIFEST_SCHEMA_VERSION,
        "name": "code-runtime",
        "content_hash": sample_hash().to_string(),
        "build_date": build_date,
        "execution": execution_for(arch),
    });
    std::fs::write(
        dir.path().join(TemplateArtifact::Manifest.file_name()),
        manifest.to_string(),
    )
    .expect("write manifest");
    std::fs::write(
        dir.path().join(TemplateArtifact::RootfsZstd.file_name()),
        format!("rootfs compressed on {build_date}"),
    )
    .expect("write rootfs");
    dir
}

async fn upload(bucket: &SharedBucket, dir: &Path) -> Result<(), PublishError> {
    upload_build_artifacts(
        bucket,
        &credentials(),
        &config(),
        &sample_template(),
        sample_hash(),
        dir,
    )
    .await
}

fn local(dir: &Path, artifact: TemplateArtifact) -> Bytes {
    Bytes::from(std::fs::read(dir.join(artifact.file_name())).unwrap())
}

#[tokio::test]
async fn a_twin_publish_leaves_the_original_publications_objects_in_place() {
    let bucket = SharedBucket::default();
    let original = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    upload(&bucket, original.path()).await.expect("original");

    let twin = build_dir(Arch::Aarch64, "2026-11-02T00:00:00Z");
    let refused = upload(&bucket, twin.path()).await.unwrap_err();

    assert!(
        matches!(
            refused,
            PublishError::BuildPrefixHeldByAnotherExecution { ref artifact, .. }
                if artifact == TemplateArtifact::Manifest.file_name()
        ),
        "{refused:?}"
    );
    for artifact in TemplateArtifact::PUBLISHED {
        assert_eq!(
            bucket.get(&key(artifact)),
            Some(local(original.path(), artifact)),
            "{} must stay the original's",
            artifact.file_name()
        );
    }
}

#[tokio::test]
async fn a_twin_publish_is_refused_when_only_the_metadata_names_another_execution() {
    let bucket = SharedBucket::default();
    let original = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    upload(&bucket, original.path()).await.expect("original");
    // Pull falls back to metadata.json for the binding, so it's guarded too.
    let twin = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    write_metadata(twin.path(), Arch::Aarch64);

    let refused = upload(&bucket, twin.path()).await.unwrap_err();

    assert!(
        matches!(
            refused,
            PublishError::BuildPrefixHeldByAnotherExecution { ref artifact, .. }
                if artifact == TemplateArtifact::Metadata.file_name()
        ),
        "{refused:?}"
    );
    assert_eq!(
        bucket.get(&key(TemplateArtifact::Metadata)),
        Some(local(original.path(), TemplateArtifact::Metadata))
    );
    assert_eq!(
        bucket.get(&key(TemplateArtifact::RootfsZstd)),
        Some(local(original.path(), TemplateArtifact::RootfsZstd))
    );
}

#[tokio::test]
async fn a_refused_rebuild_replaces_nothing_before_the_refusal() {
    let bucket = SharedBucket::default();
    let original = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    upload(&bucket, original.path()).await.expect("original");
    // An older build-manager, which wrote metadata first and unconditionally,
    // put another execution's metadata beside the original's manifest.
    let foreign = build_dir(Arch::Aarch64, "2026-11-01T00:00:00Z");
    bucket.objects.lock().unwrap().insert(
        key(TemplateArtifact::Metadata),
        local(foreign.path(), TemplateArtifact::Metadata),
    );

    // A rebuild of the original's execution has a new manifest and rootfs.
    let rebuild = build_dir(Arch::X86_64, "2026-11-02T00:00:00Z");
    let refused = upload(&bucket, rebuild.path()).await.unwrap_err();

    assert!(
        matches!(
            refused,
            PublishError::BuildPrefixHeldByAnotherExecution { ref artifact, .. }
                if artifact == TemplateArtifact::Metadata.file_name()
        ),
        "{refused:?}"
    );
    // The manifest still pins the rootfs that's stored beside it.
    assert_eq!(
        bucket.get(&key(TemplateArtifact::Manifest)),
        Some(local(original.path(), TemplateArtifact::Manifest))
    );
    assert_eq!(
        bucket.get(&key(TemplateArtifact::RootfsZstd)),
        Some(local(original.path(), TemplateArtifact::RootfsZstd))
    );
}

#[tokio::test]
async fn a_metadata_only_prefix_refuses_another_execution_before_it_writes() {
    let bucket = SharedBucket::default();
    // A publish died after metadata.json and before manifest.json.
    let interrupted = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    bucket.objects.lock().unwrap().insert(
        key(TemplateArtifact::Metadata),
        local(interrupted.path(), TemplateArtifact::Metadata),
    );

    let other = build_dir(Arch::Aarch64, "2026-11-02T00:00:00Z");
    let refused = upload(&bucket, other.path()).await.unwrap_err();
    assert!(
        matches!(
            refused,
            PublishError::BuildPrefixHeldByAnotherExecution { ref artifact, .. }
                if artifact == TemplateArtifact::Metadata.file_name()
        ),
        "{refused:?}"
    );
    assert_eq!(bucket.get(&key(TemplateArtifact::Manifest)), None);

    // So the interrupted execution's retry still completes.
    upload(&bucket, interrupted.path()).await.expect("retry");
    for artifact in TemplateArtifact::PUBLISHED {
        assert_eq!(
            bucket.get(&key(artifact)),
            Some(local(interrupted.path(), artifact))
        );
    }
}

#[tokio::test]
async fn a_stored_control_object_that_does_not_parse_is_never_replaced() {
    let bucket = SharedBucket::default();
    let garbage = Bytes::from_static(b"not a manifest");
    bucket
        .objects
        .lock()
        .unwrap()
        .insert(key(TemplateArtifact::Manifest), garbage.clone());

    let build = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    let refused = upload(&bucket, build.path()).await.unwrap_err();

    assert!(
        matches!(
            refused,
            PublishError::BuildPrefixHeldByAnotherExecution { .. }
        ),
        "{refused:?}"
    );
    assert_eq!(bucket.get(&key(TemplateArtifact::Manifest)), Some(garbage));
    assert_eq!(bucket.get(&key(TemplateArtifact::RootfsZstd)), None);
}

#[tokio::test]
async fn a_rebuild_of_the_same_execution_replaces_the_first_builds_objects() {
    let bucket = SharedBucket::default();
    let first = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    upload(&bucket, first.path()).await.expect("first");

    let rebuild = build_dir(Arch::X86_64, "2026-11-02T00:00:00Z");
    upload(&bucket, rebuild.path()).await.expect("rebuild");

    // The manifest pins the compressed rootfs digest, so a rebuild's rootfs
    // must travel with the rebuild's manifest.
    for artifact in TemplateArtifact::PUBLISHED {
        assert_eq!(
            bucket.get(&key(artifact)),
            Some(local(rebuild.path(), artifact)),
            "{} must be the rebuild's",
            artifact.file_name()
        );
    }
}

#[tokio::test]
async fn a_retried_upload_of_one_build_completes() {
    let bucket = SharedBucket::default();
    let build = build_dir(Arch::X86_64, "2026-11-01T00:00:00Z");
    // An earlier attempt put the control objects and died before the rootfs.
    for artifact in [TemplateArtifact::Metadata, TemplateArtifact::Manifest] {
        bucket
            .objects
            .lock()
            .unwrap()
            .insert(key(artifact), local(build.path(), artifact));
    }

    upload(&bucket, build.path()).await.expect("retry");

    for artifact in TemplateArtifact::PUBLISHED {
        assert_eq!(
            bucket.get(&key(artifact)),
            Some(local(build.path(), artifact))
        );
    }
}
