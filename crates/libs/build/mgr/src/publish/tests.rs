use super::*;
use pplx_space_common_core::types::{
    Arch, ExecutionMetadata, TEMPLATE_MANIFEST_SCHEMA_VERSION, TEMPLATE_METADATA_SCHEMA_VERSION,
};

#[cfg(unix)]
mod republication;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

fn execution_for(arch: Arch) -> ExecutionMetadata {
    ExecutionMetadata::initial_firecracker(arch, vec![])
}

fn write_manifest(dir: &Path, schema_version: Option<u32>, arch: Arch) {
    let mut value = serde_json::json!({
        "name": "code-runtime",
        "content_hash": sample_hash().to_string(),
        "execution": execution_for(arch),
    });
    if let Some(version) = schema_version {
        value["schema_version"] = version.into();
    }
    std::fs::write(
        dir.join(TemplateArtifact::Manifest.file_name()),
        value.to_string(),
    )
    .expect("write manifest fixture");
}

fn write_metadata(dir: &Path, arch: Arch) {
    let value = serde_json::json!({
        "schema_version": TEMPLATE_METADATA_SCHEMA_VERSION,
        "id": "code-runtime",
        "name": "code-runtime",
        "execution": execution_for(arch),
    });
    std::fs::write(
        dir.join(TemplateArtifact::Metadata.file_name()),
        value.to_string(),
    )
    .expect("write metadata fixture");
}

fn write_ready_compressed_rootfs(dir: &Path) {
    std::fs::write(
        dir.join(TemplateArtifact::RootfsZstd.file_name()),
        b"compressed-rootfs",
    )
    .expect("write compressed rootfs fixture");
    std::fs::write(
        dir.join(TemplateArtifact::Manifest.file_name()),
        serde_json::json!({
            "files": {
                (TEMPLATE_ROOTFS_ZSTD_FILE): {
                    "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "size": 17,
                    "compression": {
                        "codec": TEMPLATE_ROOTFS_ZSTD_CODEC,
                        "level": TEMPLATE_ROOTFS_ZSTD_LEVEL,
                    },
                },
            },
        })
        .to_string(),
    )
    .expect("write ready manifest fixture");
}

#[tokio::test]
async fn publish_acquires_credentials_only_after_compressed_rootfs_is_ready() {
    let dir = tempfile::tempdir().expect("tempdir");
    let build_dir = dir.path().to_path_buf();
    let acquired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acquired_in_task = std::sync::Arc::clone(&acquired);
    let task = tokio::spawn(async move {
        acquire_after_artifacts_ready(&build_dir, || async move {
            acquired_in_task.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, CredentialExchangeError>(())
        })
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !acquired.load(std::sync::atomic::Ordering::SeqCst),
        "credential exchange must not run while compression is still pending"
    );
    write_ready_compressed_rootfs(dir.path());

    task.await
        .expect("readiness task must not panic")
        .expect("ready artifacts permit credential exchange");
    assert!(acquired.load(std::sync::atomic::Ordering::SeqCst));
}

fn loaded_execution(
    dir: &Path,
) -> Result<pplx_space_execution_common::proto::ExecutionMetadata, PublishError> {
    block_on(load_build_execution_metadata(dir))
}

const PROTO_X86_64: i32 = 1;

/// The registration payload must come from the digest-stable
/// `manifest.json`, not the rebuildable `metadata.json`: a same-rootfs
/// rebuild can refresh `metadata.json` while template-manager retains the
/// hash-addressed manifest, and registration is first-write-wins, so
/// freezing a `metadata.json` value could disagree with the manifest the
/// node inventory publishes.
#[test]
fn release_execution_metadata_comes_from_digest_stable_manifest() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_manifest(
        dir.path(),
        Some(TEMPLATE_MANIFEST_SCHEMA_VERSION),
        Arch::X86_64,
    );
    write_metadata(dir.path(), Arch::Aarch64);

    let execution = loaded_execution(dir.path()).expect("v2 manifest must load");
    assert_eq!(
        execution.guest.map(|guest| guest.architecture),
        Some(PROTO_X86_64),
        "execution metadata must be sourced from manifest.json"
    );
}

/// A manifest whose schema predates execution capture (or carries an
/// unsupported version) must fail the publish rather than trust a field
/// the schema does not vouch for — even when a readable `metadata.json`
/// sits next to it. Registration is the only writer of release execution
/// metadata, so registering without it would create a permanently
/// metadata-less release.
#[test]
fn release_execution_metadata_honors_manifest_schema_validation() {
    for schema_version in [None, Some(99)] {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(dir.path(), schema_version, Arch::X86_64);
        write_metadata(dir.path(), Arch::X86_64);

        assert!(
            matches!(
                loaded_execution(dir.path()),
                Err(PublishError::ManifestExecutionMetadata { .. })
            ),
            "schema_version {schema_version:?} must fail the publish"
        );
    }
}

/// No manifest at all fails the publish; `metadata.json` alone must not
/// be consulted.
#[test]
fn release_execution_metadata_fails_without_manifest() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_metadata(dir.path(), Arch::X86_64);

    assert!(matches!(
        loaded_execution(dir.path()),
        Err(PublishError::ReadArtifact { .. })
    ));
}

fn sample_template() -> TemplateName {
    TemplateName::parse("code-runtime").expect("valid template name")
}

fn sample_hash() -> ContentHash {
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        .parse()
        .expect("valid content hash")
}

fn sample_template_request() -> RegisterRequest {
    RegisterRequest::Template(RegisterTemplateRequest {
        system_publication_grant_id: None,
        name: sample_template().as_str().to_string(),
        description: None,
        org_id: None,
    })
}

/// A republished release uploads nothing, so it registers tags only: neither
/// the current object store as a location nor execution metadata from the
/// manifest that was not uploaded, and the same holds when no cold store is
/// configured at all. Only a first publication registers where it uploaded,
/// if anywhere, and what.
#[test]
fn republication_registers_tags_only() {
    let current = "s3://bucket/templates/code-runtime/sha256:ab?region=us-east-1";
    let execution = || {
        pplx_space_execution_common::proto::ExecutionMetadata::from(&execution_for(Arch::X86_64))
    };

    let first = release_registration(false, Some(current), execution());
    assert_eq!(first.storage_locations, vec![current.to_string()]);
    assert!(first.execution.is_some());

    let first_without_store = release_registration(false, None, execution());
    assert!(first_without_store.storage_locations.is_empty());
    assert!(first_without_store.execution.is_some());

    for uploaded_to in [Some(current), None] {
        let republished = release_registration(true, uploaded_to, execution());
        assert!(republished.storage_locations.is_empty());
        assert!(
            republished.execution.is_none(),
            "a republish must not describe artifacts it did not upload"
        );
    }
}

/// Another publisher can register the same hash between this build's
/// publication lookup and its credential exchange, and under node-scoped
/// lookups the builder may not even see its own published output. Credential
/// Manager then refuses the write with a machine-readable reason, and the
/// build converges on tags-only registration instead of failing. Unrelated
/// denials still fail the publication.
#[tokio::test]
async fn publication_converges_on_tags_only_when_the_release_is_published_meanwhile() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_ready_compressed_rootfs(dir.path());
    let published_meanwhile = || {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
            pplx_space_credmgr_client::CREDMGR_ERROR_REASON_RELEASE_ALREADY_PUBLISHED
                .parse()
                .unwrap(),
        );
        CredentialExchangeError::Exchange(tonic::Status::with_metadata(
            tonic::Code::PermissionDenied,
            "template release is already published and cannot be rewritten",
            metadata,
        ))
    };

    let outcome =
        acquire_publication_credentials(dir.path(), || async { Err(published_meanwhile()) })
            .await
            .expect("an already-published refusal is an outcome, not a failure");
    assert!(matches!(outcome, Publication::AlreadyPublished));

    let unrelated = acquire_publication_credentials(dir.path(), || async {
        Err(CredentialExchangeError::Exchange(
            tonic::Status::permission_denied("caller is not authorized"),
        ))
    })
    .await
    .expect_err("an unrelated denial must still fail the publication");
    assert!(matches!(
        unrelated,
        PublishError::CredentialExchange(CredentialExchangeError::Exchange(status))
            if status.code() == tonic::Code::PermissionDenied
    ));

    let outcome = acquire_publication_credentials(dir.path(), || async {
        Ok(ObjectCredentials {
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            session_token: None,
        })
    })
    .await
    .expect("credentials permit the upload");
    assert!(matches!(outcome, Publication::Upload(_)));
}

#[test]
fn registration_requests_carry_publication_authority() {
    let org_id = "00000000-0000-0000-0000-000000000003";
    let grant_id = "018f0000-0000-7000-8000-000000000004";
    for (publication, expected_org, expected_grant) in [
        (
            TemplatePublication::Tenant(TemplateOrg::parse(org_id).unwrap()),
            Some(org_id),
            None,
        ),
        (
            TemplatePublication::System(Some(grant_id.parse().unwrap())),
            None,
            Some(grant_id),
        ),
        (TemplatePublication::System(None), None, None),
    ] {
        let template = template_registration_request(&sample_template(), publication);
        let release = release_registration_request(
            &sample_template(),
            sample_hash(),
            &["latest".to_string()],
            vec![],
            publication,
            None,
        );
        assert_eq!(template.org_id.as_deref(), expected_org);
        assert_eq!(release.org_id.as_deref(), expected_org);
        assert_eq!(
            template.system_publication_grant_id.as_deref(),
            expected_grant
        );
        assert_eq!(
            release.system_publication_grant_id.as_deref(),
            expected_grant
        );
    }
}

fn sample_release_request() -> RegisterRequest {
    RegisterRequest::Release(RegisterTemplateReleaseRequest {
        system_publication_grant_id: None,
        template_name: sample_template().as_str().to_string(),
        runtime: RUNTIME_FIRECRACKER_ALIAS.to_string(),
        hash: sample_hash().to_string(),
        tags: vec![sample_hash().to_string(), "latest".to_string()],
        storage_connection_strings: Vec::new(),
        org_id: None,
        execution: None,
    })
}

#[test]
fn register_retry_schedule_retries_pre_handler_auth_rejections_through_backoff() {
    for request in [sample_template_request(), sample_release_request()] {
        let mut schedule = RegisterRetrySchedule::new();
        let status = tonic::Status::unauthenticated("token validation failed");

        let delays: Vec<Duration> = REGISTER_RETRY_BACKOFF
            .iter()
            .map(|_| {
                schedule
                    .next_delay(&request, &status)
                    .expect("auth rejection within the schedule should yield a delay")
            })
            .collect();

        assert_eq!(delays, REGISTER_RETRY_BACKOFF);
        assert!(
            schedule.next_delay(&request, &status).is_none(),
            "an exhausted schedule must surface the failure instead of retrying forever"
        );
    }
}

#[test]
fn register_retry_schedule_never_replays_template_after_ambiguous_unavailable() {
    // Same ambiguity as the release case below, different side effect:
    // the RegisterTemplate upsert forces `desired_state = 'present'` and
    // clears pending delete metadata on conflict, so a replay after an
    // ambiguous commit can resurrect a template that a concurrent delete
    // marked during the backoff window.
    let mut schedule = RegisterRetrySchedule::new();
    assert!(
        schedule
            .next_delay(
                &sample_template_request(),
                &tonic::Status::unavailable("transport error"),
            )
            .is_none()
    );
}

#[test]
fn register_retry_schedule_never_replays_release_after_ambiguous_unavailable() {
    // UNAVAILABLE is ambiguous: the server may have committed the release
    // and its last-writer-wins tag writes before the client saw the
    // failure. Replaying would move a mutable tag (e.g. `latest`) back to
    // this build even if a concurrent build advanced it in the meantime,
    // so the release must surface the failure instead of retrying.
    let mut schedule = RegisterRetrySchedule::new();
    assert!(
        schedule
            .next_delay(
                &sample_release_request(),
                &tonic::Status::unavailable("transport error"),
            )
            .is_none()
    );
}

#[test]
fn register_retry_schedule_does_not_retry_permanent_statuses() {
    for status in [
        tonic::Status::already_exists("template exists"),
        tonic::Status::invalid_argument("bad template name"),
        tonic::Status::permission_denied("service account not allowed"),
    ] {
        for request in [sample_template_request(), sample_release_request()] {
            let mut schedule = RegisterRetrySchedule::new();
            assert!(
                schedule.next_delay(&request, &status).is_none(),
                "{:?} on {} must surface immediately without a retry",
                status.code(),
                request.operation()
            );
        }
    }
}

#[test]
fn register_retry_schedule_stays_transient_aware_after_permanent_status() {
    // A permanent status must not consume a backoff slot.
    let mut schedule = RegisterRetrySchedule::new();
    let request = sample_template_request();
    assert!(
        schedule
            .next_delay(&request, &tonic::Status::invalid_argument("bad request"))
            .is_none()
    );
    assert_eq!(
        schedule.next_delay(
            &request,
            &tonic::Status::unauthenticated("token validation failed")
        ),
        Some(REGISTER_RETRY_BACKOFF[0])
    );
}

#[test]
fn release_tags_always_include_full_digest_once() {
    let hash = sample_hash();
    let tags = release_tags(
        hash,
        &[
            hash.to_string(),
            "latest".to_string(),
            " latest ".to_string(),
            String::new(),
        ],
    );
    assert_eq!(tags, vec![hash.to_string(), "latest".to_string()]);
}

#[test]
fn storage_uri_uses_canonical_digest_prefix() {
    let config = ObjectStoreConfig {
        bucket: "bucket".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: "prefix".to_string(),
        endpoint: None,
        path_style: false,
    };

    let uri = build_storage_uri(&config, &sample_template(), sample_hash());
    let parsed =
        pplx_space_common_core::types::StorageUri::parse(&uri).expect("storage uri should parse");

    assert_eq!(parsed.bucket, "bucket");
    assert_eq!(
        parsed.key_prefix,
        "prefix/templates/code-runtime/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(parsed.region, "us-east-1");
    assert!(!parsed.path_style);
}

#[test]
fn storage_uri_keeps_explicit_path_style_override() {
    let config = ObjectStoreConfig {
        bucket: "bucket".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: String::new(),
        endpoint: Some("https://minio.internal".to_string()),
        path_style: false,
    };

    let uri = build_storage_uri(&config, &sample_template(), sample_hash());
    let parsed =
        pplx_space_common_core::types::StorageUri::parse(&uri).expect("storage uri should parse");

    assert_eq!(parsed.endpoint.as_deref(), Some("https://minio.internal"));
    assert!(!parsed.path_style);
}
