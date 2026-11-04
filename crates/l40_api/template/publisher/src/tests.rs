use std::collections::BTreeMap;

use super::*;

fn seed_hash() -> String {
    format!("sha256:{}", "ab".repeat(32))
}

#[test]
fn pin_parses_name_and_digest() {
    let parsed: TemplatePin = format!("seed@{}", seed_hash())
        .parse()
        .expect("pin should parse");

    assert_eq!(parsed.template.as_str(), "seed");
    assert_eq!(parsed.content_hash.to_string(), seed_hash());
}

/// The rendered form is what a chart writes into the Job's args, so it has to
/// be the form this parser accepts.
#[test]
fn pin_display_round_trips_through_parse() {
    let wire = format!("code-runtime@{}", seed_hash());
    let parsed: TemplatePin = wire.parse().expect("pin should parse");

    assert_eq!(parsed.to_string(), wire);
    assert_eq!(
        parsed.to_string().parse::<TemplatePin>().expect("re-parse"),
        parsed,
    );
}

#[test]
fn pin_rejects_malformed_input() {
    for bad in [
        // Separator but no digest.
        "seed@".to_string(),
        // Digest but no name.
        format!("@{}", seed_hash()),
        // Not a digest.
        "seed@latest".to_string(),
        // Too short even to be an abbreviated digest.
        "seed@sha256:abcd".to_string(),
    ] {
        assert!(
            bad.parse::<TemplatePin>().is_err(),
            "expected '{bad}' to be rejected",
        );
    }
}

/// The shared reference grammar accepts a tag and an abbreviated digest; a pin
/// must not. Either would let the artifacts a pin resolves to change after the
/// values file was reviewed, which is the whole point of pinning a hash.
#[test]
fn pin_rejects_references_that_are_not_a_full_digest() {
    // A 12-hex digest is a *valid* abbreviated digest to the shared grammar, so
    // it reaches this crate's own constraint rather than being rejected as
    // malformed upstream — which is exactly the arm worth pinning.
    for mutable in ["seed", "seed:latest", "seed@sha256:abcdefabcdef"] {
        let error = mutable
            .parse::<TemplatePin>()
            .expect_err("a non-digest reference must be rejected");

        assert!(
            matches!(error, TemplatePinParseError::NotAFullDigest { .. }),
            "'{mutable}' was rejected as {error:?} rather than as a non-digest pin",
        );
    }
}

/// The `@` split has to survive the `:` inside a digest.
#[test]
fn pin_splits_on_the_first_at_not_the_digest_colon() {
    let parsed: TemplatePin = format!("app-runtime@{}", seed_hash())
        .parse()
        .expect("pin should parse");

    assert_eq!(parsed.template.as_str(), "app-runtime");
    assert_eq!(parsed.content_hash.to_string(), seed_hash());
}

/// With only `--bucket` and `--region`, the source defaults to the destination.
fn minimal_config() -> TemplatePublishRunConfig {
    TemplatePublishRunConfig {
        templates: vec![test_pin()],
        bucket: "dest-bucket".to_string(),
        region: "us-east-1".to_string(),
        source_bucket: None,
        source_region: None,
    }
}

/// A cluster that already holds its own artifacts configures no source, and the
/// copy step must then compare an object against itself and skip.
#[test]
fn source_defaults_to_the_destination() {
    let config = minimal_config();

    assert_eq!(config.source(), config.destination());
}

#[test]
fn source_overrides_apply_independently() {
    let config = TemplatePublishRunConfig {
        source_bucket: Some("upstream-bucket".to_string()),
        ..minimal_config()
    };

    assert_eq!(
        config.source(),
        BucketLocation {
            bucket: "upstream-bucket".to_string(),
            // Unset, so it falls back to the destination region rather than
            // silently defaulting to something else.
            region: "us-east-1".to_string(),
        },
    );
}

/// The value template-manager reads back to find the artifacts. Real S3 carries
/// no endpoint and no path-style flag; both would send volume-manager somewhere
/// that does not exist.
#[test]
fn storage_uri_addresses_the_destination_build_directory() {
    let config = minimal_config();
    let pin = &config.templates[0];

    assert_eq!(
        config.destination().storage_uri(pin),
        format!(
            "s3://dest-bucket/templates/seed/{}?region=us-east-1",
            seed_hash()
        ),
    );
}

fn manifest(name: &str, content_hash: &str) -> TemplateManifest {
    TemplateManifest {
        name: name.to_string(),
        content_hash: content_hash.to_string(),
        ..TemplateManifest::default()
    }
}

#[test]
fn manifest_matching_the_pin_verifies() {
    let pinned = test_pin();

    assert_eq!(
        manifest_verdict(&manifest("seed", &seed_hash()), &pinned),
        ManifestVerdict::Matches,
    );
}

/// The failure this check exists for: a values file naming a hash whose
/// artifacts belong to a different build. Registering it would point a release
/// row at bytes that are not the release.
#[test]
fn manifest_with_a_different_hash_is_a_mismatch() {
    let pinned = test_pin();
    let other = format!("sha256:{}", "cd".repeat(32));

    assert_eq!(
        manifest_verdict(&manifest("seed", &other), &pinned),
        ManifestVerdict::Mismatch,
    );
}

#[test]
fn manifest_for_a_different_template_is_a_mismatch() {
    let pinned = test_pin();

    assert_eq!(
        manifest_verdict(&manifest("code-runtime", &seed_hash()), &pinned),
        ManifestVerdict::Mismatch,
    );
}

/// Distinguished from a mismatch so the operator is told the manifest cannot be
/// checked rather than that the pin is wrong — but still rejected by the caller,
/// because an unverifiable pin defeats the check.
#[test]
fn manifest_without_a_content_hash_is_unstamped_not_a_mismatch() {
    let pinned = test_pin();

    assert_eq!(
        manifest_verdict(&manifest("seed", ""), &pinned),
        ManifestVerdict::Unstamped,
    );
}

/// A digest the manifest spells unparseably is a mismatch, not an accidental
/// pass from comparing two strings that differ.
#[test]
fn manifest_with_an_unparseable_content_hash_is_a_mismatch() {
    let pinned = test_pin();

    assert_eq!(
        manifest_verdict(&manifest("seed", "sha256:not-hex"), &pinned),
        ManifestVerdict::Mismatch,
    );
}

#[test]
fn manifest_without_a_name_still_verifies_by_hash() {
    let pinned = test_pin();

    assert_eq!(
        manifest_verdict(&manifest("", &seed_hash()), &pinned),
        ManifestVerdict::Matches,
    );
}

#[test]
fn outcome_labels_are_stable() {
    for (outcome, label) in [
        (PinOutcome::AlreadyPublished, "already_published"),
        (PinOutcome::Registered, "registered"),
        (PinOutcome::CopiedAndRegistered, "copied_and_registered"),
    ] {
        assert_eq!(outcome.as_str(), label);
        assert_eq!(outcome.to_string(), label);
    }
}

// ── The copy/verify matrix ───────────────────────────────────────────────────
//
// This is the logic a live deploy would otherwise be the only thing to
// exercise: which of the four (destination, source) presence combinations copies,
// which skips, and which fails. The fake below records every call so a test can
// assert the *absence* of a copy, which is the half that matters for idempotence.

use std::sync::Mutex;

use pplx_space_common_core::types::TemplateArtifact;
use pplx_space_object_store::{ObjectMeta, ObjectStore, ObjectStoreError, ObjectTarget};

/// Object store backed by a `bucket/key -> size` map.
#[derive(Default)]
struct FakeStore {
    objects: Mutex<BTreeMap<String, u64>>,
    manifest: Mutex<Option<Vec<u8>>>,
    calls: Mutex<Vec<String>>,
}

impl FakeStore {
    /// Owned entries so callers can build them from `keys_for` without keeping
    /// a temporary alive just to borrow out of it.
    fn with_objects(entries: Vec<(String, String, u64)>) -> Self {
        let store = Self::default();
        for (bucket, key, size) in entries {
            store
                .objects
                .lock()
                .expect("objects lock")
                .insert(format!("{bucket}/{key}"), size);
        }
        store
    }

    fn set_manifest(&self, body: &str) {
        *self.manifest.lock().expect("manifest lock") = Some(body.as_bytes().to_vec());
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn copies(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|call| call.starts_with("copy "))
            .collect()
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("calls lock").push(call);
    }
}

#[async_trait::async_trait]
impl ObjectStore for FakeStore {
    async fn head(
        &self,
        _creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        let addressed = format!("{}/{}", target.bucket, target.key);
        self.record(format!("head {addressed}"));
        Ok(self
            .objects
            .lock()
            .expect("objects lock")
            .get(&addressed)
            .map(|size| ObjectMeta {
                size: *size,
                etag: None,
            }))
    }

    async fn copy_object(
        &self,
        _creds: &pplx_space_object_store::ObjectCredentials,
        source: ObjectTarget<'_>,
        dest: ObjectTarget<'_>,
        source_size: u64,
    ) -> Result<(), ObjectStoreError> {
        self.record(format!(
            "copy {}/{} -> {}/{}",
            source.bucket, source.key, dest.bucket, dest.key
        ));
        self.objects
            .lock()
            .expect("objects lock")
            .insert(format!("{}/{}", dest.bucket, dest.key), source_size);
        Ok(())
    }

    async fn put_bytes(
        &self,
        _creds: &pplx_space_object_store::ObjectCredentials,
        _target: ObjectTarget<'_>,
        _body: bytes::Bytes,
    ) -> Result<(), ObjectStoreError> {
        panic!("the publisher never puts bytes; it copies")
    }

    async fn get_bytes(
        &self,
        _creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        self.record(format!("get {}/{}", target.bucket, target.key));
        self.manifest
            .lock()
            .expect("manifest lock")
            .clone()
            .ok_or_else(|| ObjectStoreError::NotFound {
                key: target.key.to_string(),
            })
    }

    async fn put_file(
        &self,
        _creds: &pplx_space_object_store::ObjectCredentials,
        _target: ObjectTarget<'_>,
        _src: &std::path::Path,
    ) -> Result<u64, ObjectStoreError> {
        panic!("the publisher never uploads from disk")
    }

    async fn get_file(
        &self,
        _creds: &pplx_space_object_store::ObjectCredentials,
        _target: ObjectTarget<'_>,
        _dst: &std::path::Path,
    ) -> Result<u64, ObjectStoreError> {
        panic!("the publisher never downloads to disk")
    }
}

/// Static credentials so the fake paths never touch the AWS chain.
fn test_credentials() -> Credentials {
    let credentials = Credentials::default();
    credentials
        .0
        .set(pplx_space_object_store::ObjectCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
        })
        .expect("fresh cell");
    credentials
}

fn locations(source_bucket: &str) -> (BucketLocation, BucketLocation) {
    let destination = BucketLocation {
        bucket: "dest".to_string(),
        region: "us-east-1".to_string(),
    };
    let source = BucketLocation {
        bucket: source_bucket.to_string(),
        ..destination.clone()
    };
    (destination, source)
}

fn test_pin() -> TemplatePin {
    format!("seed@{}", seed_hash()).parse().expect("pin")
}

/// Every published artifact of `pin` in `location`, each at `size` bytes.
fn objects_in(
    location: &BucketLocation,
    pin: &TemplatePin,
    size: u64,
) -> Vec<(String, String, u64)> {
    keys_for(location, pin)
        .into_iter()
        .map(|(bucket, key)| (bucket, key, size))
        .collect()
}

fn keys_for(location: &BucketLocation, pin: &TemplatePin) -> Vec<(String, String)> {
    TemplateArtifact::PUBLISHED
        .iter()
        .map(|artifact| {
            (
                location.bucket.clone(),
                template_object_key("", &pin.template, pin.content_hash, *artifact),
            )
        })
        .collect()
}

/// An empty destination must receive every artifact from the populated source.
#[tokio::test]
async fn every_artifact_is_copied_when_the_destination_is_empty() {
    let pin = test_pin();
    let (destination, source) = locations("upstream");
    let store = FakeStore::with_objects(objects_in(&source, &pin, 10));

    let copied = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect("copy should succeed");

    assert!(copied);
    assert_eq!(
        store.copies().len(),
        TemplateArtifact::PUBLISHED.len(),
        "calls: {:?}",
        store.calls(),
    );
}

/// Ordering is the invariant: `manifest.json` describes the other artifacts, so
/// it must be the last one to land.
#[tokio::test]
async fn the_manifest_is_the_last_artifact_copied() {
    let pin = test_pin();
    let (destination, source) = locations("upstream");
    let store = FakeStore::with_objects(objects_in(&source, &pin, 10));

    ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect("copy should succeed");

    let last = store.copies().pop().expect("at least one copy");
    assert!(
        last.contains("manifest.json"),
        "manifest was not copied last; copies: {:?}",
        store.copies(),
    );
}

/// Idempotence: a second run over a destination that already matches copies
/// nothing at all.
#[tokio::test]
async fn nothing_is_copied_when_the_destination_already_matches() {
    let pin = test_pin();
    let (destination, source) = locations("upstream");
    let mut present = objects_in(&source, &pin, 10);
    present.extend(objects_in(&destination, &pin, 10));
    let store = FakeStore::with_objects(present);

    let copied = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect("no-op should succeed");

    assert!(!copied);
    assert!(store.copies().is_empty(), "calls: {:?}", store.calls());
}

/// A destination object of a different size is a prior copy that did not finish,
/// so it is replaced rather than accepted.
#[tokio::test]
async fn a_truncated_destination_object_is_recopied() {
    let pin = test_pin();
    let (destination, source) = locations("upstream");
    let mut present = objects_in(&source, &pin, 10);
    present.extend(objects_in(&destination, &pin, 4));
    let store = FakeStore::with_objects(present);

    let copied = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect("recopy should succeed");

    assert!(copied);
    assert_eq!(store.copies().len(), TemplateArtifact::PUBLISHED.len());
}

/// A cluster whose own builder published the release and whose configured source
/// never had it: the destination is authoritative and must not be disturbed.
#[tokio::test]
async fn an_artifact_absent_upstream_but_present_locally_is_kept() {
    let pin = test_pin();
    let (destination, source) = locations("upstream");
    let store = FakeStore::with_objects(objects_in(&destination, &pin, 10));

    let copied = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect("keeping the local copy should succeed");

    assert!(!copied);
    assert!(store.copies().is_empty(), "calls: {:?}", store.calls());
}

/// Absent on both sides is the wrong-pin case, and it must name the source so
/// the operator checks the bucket the artifacts were supposed to come from.
#[tokio::test]
async fn an_artifact_absent_on_both_sides_fails_naming_the_source() {
    let pin = test_pin();
    let (destination, source) = locations("upstream");
    let store = FakeStore::default();

    let error = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect_err("a pin with no artifacts anywhere must fail");

    let rendered = error.to_string();
    assert!(
        rendered.contains("upstream"),
        "error should name the source bucket, got: {rendered}",
    );
    assert!(store.copies().is_empty());
}

/// Source defaulted to the destination: the two keys address one object, so
/// there is nothing to compare and nothing to copy — but presence still has
/// to be confirmed.
#[tokio::test]
async fn a_self_sourced_cluster_confirms_presence_without_copying() {
    let pin = test_pin();
    let (destination, source) = locations("dest");
    let store = FakeStore::with_objects(objects_in(&destination, &pin, 10));

    let copied = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect("a self-sourced cluster with every artifact present should pass");

    assert!(!copied);
    assert!(store.copies().is_empty(), "calls: {:?}", store.calls());
    assert_eq!(
        store.calls().len(),
        TemplateArtifact::PUBLISHED.len(),
        "each artifact should be probed exactly once; calls: {:?}",
        store.calls(),
    );
}

/// The defect this exists for: the builder uploads `manifest.json` *before* the
/// rootfs, so a publish that died in between leaves a manifest that verifies
/// against a rootfs that is not there. A self-sourced cluster that skipped
/// probing would register a `latest` no worker can pull — and every later run
/// would skip it, because `latest` now resolves.
#[tokio::test]
async fn a_self_sourced_cluster_rejects_a_half_published_build() {
    let pin = test_pin();
    let (destination, source) = locations("dest");
    let present: Vec<(String, String, u64)> = objects_in(&destination, &pin, 10)
        .into_iter()
        .filter(|(_, key, _)| !key.ends_with("rootfs.ext4.zst"))
        .collect();
    let store = FakeStore::with_objects(present);

    let error = ensure_artifacts(&store, &test_credentials(), &destination, &source, &pin)
        .await
        .expect_err("a build missing its rootfs must not be registered");

    let rendered = error.to_string();
    assert!(
        rendered.contains("rootfs.ext4.zst"),
        "the error should name the missing artifact, got: {rendered}",
    );
}

#[tokio::test]
async fn verify_manifest_accepts_a_manifest_naming_the_pin() {
    let pin = test_pin();
    let (destination, _) = locations("dest");
    let store = FakeStore::default();
    store.set_manifest(&format!(
        r#"{{"name":"seed","content_hash":"{}"}}"#,
        seed_hash()
    ));

    verify_manifest(&store, &test_credentials(), &destination, &pin)
        .await
        .expect("a matching manifest should verify");
}

/// A bootstrapped release has to be indistinguishable from a builder-published
/// one, and registration is first-write-wins on `(template_name, hash)` — so a
/// release registered without execution metadata never gains it. The builder
/// reads it out of the hash-addressed manifest; so does this.
#[tokio::test]
async fn verify_manifest_returns_the_execution_metadata_to_register() {
    let pin = test_pin();
    let (destination, _) = locations("dest");
    let store = FakeStore::default();
    store.set_manifest(&format!(
        r#"{{"schema_version":2,
             "name":"seed",
             "content_hash":"{}",
             "execution":{{
               "runtime":{{"kind":"firecracker","version":"1.15.1"}},
               "guest":{{"os":"linux","architecture":"x86_64"}},
               "state_format":{{"format":"firecracker-snapshot","version":"9.0.0"}}
             }}}}"#,
        seed_hash()
    ));

    let execution = verify_manifest(&store, &test_credentials(), &destination, &pin)
        .await
        .expect("a matching manifest should verify")
        .expect("a schema-2 manifest carries execution metadata");

    assert_eq!(
        execution
            .runtime
            .as_ref()
            .map(|runtime| runtime.version.clone()),
        Some("1.15.1".to_string()),
    );
}

/// A manifest whose execution block fails schema validation must not fail the
/// publish: the release is still correct and pullable, and the node side treats
/// absent metadata as something to fill in later.
#[tokio::test]
async fn a_schema_invalid_execution_block_does_not_fail_the_publish() {
    let pin = test_pin();
    let (destination, _) = locations("dest");
    let store = FakeStore::default();
    // schema_version 2 with no execution block is exactly the invalid shape.
    store.set_manifest(&format!(
        r#"{{"schema_version":2,"name":"seed","content_hash":"{}"}}"#,
        seed_hash()
    ));

    let execution = verify_manifest(&store, &test_credentials(), &destination, &pin)
        .await
        .expect("an unvouchable execution block must not fail the publish");

    assert!(execution.is_none());
}

#[tokio::test]
async fn verify_manifest_rejects_a_manifest_for_another_build() {
    let pin = test_pin();
    let (destination, _) = locations("dest");
    let store = FakeStore::default();
    store.set_manifest(&format!(
        r#"{{"name":"seed","content_hash":"sha256:{}"}}"#,
        "cd".repeat(32)
    ));

    let error = verify_manifest(&store, &test_credentials(), &destination, &pin)
        .await
        .expect_err("a manifest for another build must be rejected");

    assert!(
        matches!(error, RunTemplatePublishError::ManifestMismatch { .. }),
        "got {error:?}",
    );
}

#[tokio::test]
async fn verify_manifest_rejects_a_body_that_is_not_json() {
    let pin = test_pin();
    let (destination, _) = locations("dest");
    let store = FakeStore::default();
    store.set_manifest("not json at all");

    let error = verify_manifest(&store, &test_credentials(), &destination, &pin)
        .await
        .expect_err("a malformed manifest must be rejected");

    assert!(
        matches!(error, RunTemplatePublishError::ManifestMalformed { .. }),
        "got {error:?}",
    );
}
