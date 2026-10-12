mod catalog;

use super::*;
use pplx_space_node_api_client::proto::{FindTemplateReleaseResponse, TemplateRelease};

struct AuthorizedReleaseFixture {
    org: OrgId,
    hash: String,
    response: FindTemplateReleaseResponse,
}

#[rstest::fixture]
fn authorized_release_fixture() -> AuthorizedReleaseFixture {
    let org = OrgId::new();
    let hash = format!("sha256:{}", "a".repeat(64));
    let response = FindTemplateReleaseResponse {
        authorized_org_id: Some(org.to_string()),
        release: Some(TemplateRelease {
            template_name: "base".into(),
            runtime: DEFAULT_RUNTIME.into(),
            hash: hash.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    AuthorizedReleaseFixture {
        org,
        hash,
        response,
    }
}

#[rstest::rstest]
fn scoped_release_response_must_acknowledge_the_org_and_exact_release(
    authorized_release_fixture: AuthorizedReleaseFixture,
) {
    let AuthorizedReleaseFixture {
        org,
        hash,
        response: valid,
    } = authorized_release_fixture;
    assert_eq!(
        parse_release_response("base", &hash, Some(org), false, valid.clone())
            .unwrap()
            .hash,
        hash
    );
    let mut legacy_response = valid.clone();
    legacy_response.authorized_org_id = None;
    for requested_hash in ["", hash.as_str()] {
        let release =
            parse_release_response("base", requested_hash, None, false, legacy_response.clone())
                .expect("unscoped callers accept releases from an older ClusterApi");
        assert_eq!(release.hash, hash);
    }

    let mut generic = legacy_response.clone();
    generic.template_visibility = TemplateVisibility::Generic.into();
    assert!(matches!(
        parse_release_response("base", &hash, None, true, generic.clone()),
        Err(FindReleaseError::InvalidResponse)
    ));
    generic.generic_authorized = true;
    parse_release_response("base", &hash, None, true, generic)
        .expect("generic-only catalog acknowledgement permits prefetch");

    for mutation in 0..6 {
        let mut response = valid.clone();
        match mutation {
            0 => response.authorized_org_id = None,
            1 => response.authorized_org_id = Some(OrgId::new().to_string()),
            2 => response.release.as_mut().unwrap().hash = format!("sha256:{}", "b".repeat(64)),
            3 => response.release.as_mut().unwrap().template_name = "other".into(),
            4 => response.release.as_mut().unwrap().runtime = "other".into(),
            5 => response.release = None,
            _ => unreachable!(),
        }
        assert!(matches!(
            parse_release_response("base", &hash, Some(org), false, response),
            Err(FindReleaseError::InvalidResponse)
        ));
    }
}
use pplx_space_common_core::types::TEMPLATE_ROOTFS_ZSTD_FILE;

#[test]
fn volmgr_admission_rejection_is_retryable() {
    let status = Status::resource_exhausted("pull admission limit reached");

    let disposition = pull_status_disposition(&status);

    assert_eq!(disposition, PullStatusDisposition::Retryable);
}

#[test]
fn downstream_status_classification_preserves_permanent_failures() {
    let cluster_status = Status::permission_denied("cluster metadata access denied");
    let FindReleaseError::Status(cluster_status) = classify_find_release_status(cluster_status)
    else {
        panic!("permanent cluster status must be preserved");
    };
    assert_eq!(cluster_status.code(), tonic::Code::PermissionDenied);

    let volume_status = Status::failed_precondition("object store is misconfigured");
    assert_eq!(
        pull_status_disposition(&volume_status),
        PullStatusDisposition::Failed
    );
}

#[test]
fn replicated_pull_prefers_retryable_failure_regardless_of_uri_order() {
    for (first, second) in [
        (
            PullFailure::StorageUnavailable,
            PullFailure::StorageObjectMissing,
        ),
        (
            PullFailure::StorageObjectMissing,
            PullFailure::StorageUnavailable,
        ),
    ] {
        assert_eq!(
            prefer_retryable_failure(first, second),
            PullFailure::StorageUnavailable
        );
    }

    let permanent = Status::permission_denied("replica access denied");
    assert!(matches!(
        finish_pull_failures(PullFailure::StorageObjectMissing, Some(permanent)),
        PullError::Failed(status) if status.code() == tonic::Code::PermissionDenied
    ));
    assert!(matches!(
        finish_pull_failures(
            PullFailure::StorageUnavailable,
            Some(Status::permission_denied("replica access denied"))
        ),
        PullError::Failure(PullFailure::StorageUnavailable)
    ));
}

#[test]
fn template_control_requests_use_protected_priority() {
    let metadata = build_pull_file_request(
        "template:seed".to_string(),
        "s3://bucket/metadata.json".to_string(),
        "/cache/metadata.json".to_string(),
        PullOptions::control("metadata"),
    );
    let manifest = build_pull_file_request(
        "template:seed".to_string(),
        "s3://bucket/manifest.json".to_string(),
        "/cache/manifest.json".to_string(),
        PullOptions::control("manifest"),
    );
    let rootfs = build_pull_file_request(
        "template:seed".to_string(),
        "s3://bucket/rootfs.ext4.zst".to_string(),
        "/cache/rootfs.ext4.zst".to_string(),
        PullOptions::bulk("rootfs_zstd", true),
    );

    assert_eq!(metadata.priority, RESTORE_PRIORITY);
    assert_eq!(manifest.priority, RESTORE_PRIORITY);
    assert_eq!(rootfs.priority, 0);
}

#[test]
fn retryable_outcome_preserves_status_for_coalesced_callers() {
    let mut status = Status::resource_exhausted("pull admission limit reached");
    status
        .metadata_mut()
        .insert("x-space-retry-after-ms", "250".parse().unwrap());
    let outcome = PullOutcome::Retryable(status);

    let restored = PullOutcomeSnapshot::from(&outcome).into_outcome();

    let PullOutcome::Retryable(status) = restored else {
        panic!("expected retryable pull outcome");
    };
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(status.message(), "pull admission limit reached");
    assert_eq!(
        status.metadata().get("x-space-retry-after-ms").unwrap(),
        "250"
    );
}

#[test]
fn lookup_tag_returns_req_tag_or_default() {
    // req_tag is used when set.
    assert_eq!(lookup_tag("v1"), "v1");
    assert_eq!(lookup_tag("latest"), "latest");
    assert_eq!(lookup_tag("prod"), "prod");
    // Empty falls back to "latest".
    assert_eq!(lookup_tag(""), "latest");
}

/// `do_pull` must refuse to start when the build's row is in
/// `state='deleting'`. Without this guard, an
/// on-demand pull racing eviction would write artifacts the
/// eviction is about to delete (and its upsert would either
/// no-op or resurrect the doomed row depending on which fix
/// is in place).
#[tokio::test]
async fn do_pull_aborts_early_when_build_is_marked_deleting() {
    use std::sync::Mutex;

    let conn = crate::db::test_conn();
    let hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: hash,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "aarch64",
            artifact_size_bytes: 100,
            state: "ready",
        },
    )
    .unwrap();
    // Eviction Phase 1: flip to deleting.
    conn.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE name = ?1 AND content_hash = ?2",
        rusqlite::params!["seed", hash],
    )
    .unwrap();
    let db = Arc::new(Mutex::new(conn));

    // `storage_uris` is empty; the early-abort check fires
    // before the storage-iteration loop so an empty list is
    // fine here. If the guard wasn't in place, do_pull would
    // proceed to the "no usable storage" failure with a
    // different reason.
    let outcome = do_pull(db, "seed", hash, &[], &[], None).await;
    match outcome {
        Err(PullError::Failure(failure)) => {
            assert_eq!(failure, PullFailure::EvictionInProgress);
        }
        other => panic!("expected eviction-in-progress failure, got {other:?}"),
    }
}

#[tokio::test]
async fn do_pull_treats_empty_storage_locations_as_local_only_uncached() {
    use std::sync::Mutex;

    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));
    let hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let outcome = do_pull(db, "seed", hash, &[], &[], None).await;

    match outcome {
        Err(PullError::Failure(failure)) => {
            assert_eq!(failure, PullFailure::NoStorageLocations);
        }
        other => panic!("expected no-storage-locations failure, got {other:?}"),
    }
}

#[tokio::test]
async fn pull_flight_publishes_result_to_waiters_and_cleans_map() {
    let puller = VolumePuller::new();
    let key = PullKey::new("app-runtime", "sha256:test");
    let flight = Arc::new(InFlightPull::new());
    puller
        .in_flight
        .lock()
        .unwrap()
        .insert(key.clone(), Arc::clone(&flight));
    let guard = PullFlightGuard::new(key, Arc::clone(&flight), Arc::clone(&puller.in_flight));

    let waiter_flight = Arc::clone(&flight);
    let waiter = tokio::spawn(async move { waiter_flight.wait_for_result().await });
    tokio::task::yield_now().await;

    guard.finish(PullTaskResult {
        outcome: PullOutcomeSnapshot::Materialized,
        path: PullPath::FreshPull,
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("waiter must be notified")
        .expect("waiter task must finish")
        .expect("owner must publish a result");
    assert!(matches!(result.outcome, PullOutcomeSnapshot::Materialized));
    assert_eq!(result.path, PullPath::FreshPull);
    assert!(
        puller.in_flight.lock().unwrap().is_empty(),
        "finished flight must be removed from the in-flight map"
    );
}

#[tokio::test]
async fn pull_flight_drop_notifies_waiters_without_result() {
    let puller = VolumePuller::new();
    let key = PullKey::new("app-runtime", "sha256:test");
    let flight = Arc::new(InFlightPull::new());
    puller
        .in_flight
        .lock()
        .unwrap()
        .insert(key.clone(), Arc::clone(&flight));
    let guard = PullFlightGuard::new(
        key.clone(),
        Arc::clone(&flight),
        Arc::clone(&puller.in_flight),
    );

    let waiter_flight = Arc::clone(&flight);
    let waiter = tokio::spawn(async move { waiter_flight.wait_for_result().await });
    tokio::task::yield_now().await;

    drop(guard);

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("waiter must be notified")
        .expect("waiter task must finish");
    assert!(
        result.is_none(),
        "dropped owner must not strand waiters behind an absent result"
    );
    assert!(
        puller.in_flight.lock().unwrap().is_empty(),
        "dropped owner must remove the stale flight"
    );
    let (_flight, started) = puller.start_or_join_flight(key);
    assert!(started, "next caller must be able to become the owner");
}

#[test]
fn coalesced_materialized_result_is_tagged_and_counted() {
    let mut result = PullTaskResult {
        outcome: PullOutcomeSnapshot::Materialized,
        path: PullPath::FreshPull,
    };

    assert!(result.mark_coalesced_if_materialized());
    assert_eq!(result.path, PullPath::Coalesced);
    assert_eq!(
        outcome_label(&result.outcome.clone().into_outcome(), result.path),
        "coalesced"
    );
}

#[test]
fn coalesced_non_materialized_result_keeps_owner_path() {
    for outcome in [
        PullOutcomeSnapshot::ClassifiedFailure(PullFailure::ClusterMetadataMissing),
        PullOutcomeSnapshot::HashMismatch(Status::internal("boom")),
    ] {
        let mut result = PullTaskResult {
            outcome,
            path: PullPath::FreshPull,
        };

        assert!(!result.mark_coalesced_if_materialized());
        assert_eq!(result.path, PullPath::FreshPull);
    }
}

#[test]
fn start_or_join_flight_coalesces_same_key() {
    let puller = VolumePuller::new();
    let key = PullKey::new("app-runtime", "sha256:test");

    let (first, first_started) = puller.start_or_join_flight(key.clone());
    let (second, second_started) = puller.start_or_join_flight(key);

    assert!(first_started);
    assert!(!second_started);
    assert!(
        Arc::ptr_eq(&first, &second),
        "same key must join the existing flight"
    );
}

fn storage(bucket: &str, key_prefix: &str, region: &str) -> StorageUri {
    StorageUri {
        bucket: bucket.to_string(),
        key_prefix: key_prefix.to_string(),
        region: region.to_string(),
        endpoint: None,
        path_style: false,
    }
}

#[test]
fn build_object_uri_handles_prefix_with_and_without_slashes() {
    // Plain prefix, no trailing slash.
    assert_eq!(
        build_object_uri(&storage("b", "p", "us-east-2"), "metadata.json"),
        "s3://b/p/metadata.json?region=us-east-2",
    );
    // Surrounding slashes on the prefix are trimmed so we don't emit
    // `s3://b//p//file`.
    assert_eq!(
        build_object_uri(&storage("b", "/p/", "us-east-2"), "manifest.json"),
        "s3://b/p/manifest.json?region=us-east-2",
    );
    // Empty prefix puts the file at the bucket root with no leading "/".
    assert_eq!(
        build_object_uri(&storage("b", "", "us-east-2"), "rootfs.ext4.zst"),
        "s3://b/rootfs.ext4.zst?region=us-east-2",
    );
}

#[test]
fn build_object_uri_percent_encodes_reserved_chars() {
    // psi-review-bot flagged: a future cluster registrar that emits
    // an endpoint with `&`, `?`, `#`, or whitespace would otherwise
    // produce a URI that vol-mgr's StorageUri::parse rejects (split
    // on the wrong `&`, or premature query/fragment boundary). The
    // function must percent-encode those characters so the
    // registration round-trips cleanly. The `:` and `/` in normal
    // endpoints pass through.
    let s = StorageUri::parse(
        "s3://b/p?region=us east 2&endpoint=https://minio:9000?foo=bar%26baz=qux&path_style=true",
    )
    .expect("storage URI must parse");
    let uri = build_object_uri(&s, "rootfs.ext4.zst");
    // Region: space encoded.
    assert!(
        uri.contains("region=us%20east%202"),
        "region whitespace must be percent-encoded: {uri}"
    );
    // Endpoint: `?` and the percent escape for `&` are protected from the
    // outer query parser. We don't escape `=` because the parser only
    // splits on the first `=` of each pair, so embedded `=` in values
    // survives.
    assert!(
        uri.contains("%3F"),
        "endpoint '?' must be percent-encoded: {uri}"
    );
    assert!(
        uri.contains("%2526"),
        "endpoint '&' escape must be protected: {uri}"
    );
    // The `:` and `/` in the endpoint pass through untouched.
    assert!(
        uri.contains("https://minio:9000"),
        "endpoint scheme/host should pass through: {uri}"
    );
    // And the resulting URI must still round-trip through
    // StorageUri::parse — that's the real contract. `=` is not
    // encoded (the parser uses split_once which only consumes the
    // first `=`), so embedded `=` survives untouched.
    let parsed = StorageUri::parse(&uri).expect("round-trip parse must succeed");
    assert_eq!(
        parsed.endpoint.as_deref(),
        Some("https://minio:9000%3Ffoo=bar%2526baz=qux")
    );
    // (Vol-mgr at the receiving end is responsible for decoding the
    // value back if it needs the literal endpoint URL; for our
    // purposes the round-trip preserving the encoded form is enough.)
    assert_eq!(parsed.region, "us%20east%202");
}

#[test]
fn build_object_uri_includes_endpoint_and_path_style() {
    let s =
        StorageUri::parse("s3://b/p?region=us-east-2&endpoint=https://minio:9000&path_style=true")
            .expect("storage URI must parse");
    assert_eq!(
        build_object_uri(&s, "metadata.json"),
        "s3://b/p/metadata.json?region=us-east-2&endpoint=https://minio:9000&path_style=true",
    );
}

/// Build the file shape `try_pull_from` would leave behind right before
/// the DB upsert: all four artifacts present in `digest_dir`, with an
/// unnormalized manifest (the worst case — a cancellation between
/// rootfs.ext4 rename and the atomic manifest re-write).
async fn populate_orphaned_digest_dir(digest_dir: &Path) {
    tokio::fs::write(digest_dir.join("metadata.json"), br#"{"arch":"aarch64"}"#)
        .await
        .unwrap();
    // Raw, pre-normalize manifest — missing the fields
    // normalize_template_manifest injects.
    tokio::fs::write(
        digest_dir.join("manifest.json"),
        br#"{"runtime":"fc","build_date":"2026-05-19T00:00:00Z"}"#,
    )
    .await
    .unwrap();
    tokio::fs::write(
        digest_dir.join(TEMPLATE_ROOTFS_FILE),
        b"decompressed rootfs bytes (test fixture)",
    )
    .await
    .unwrap();
    tokio::fs::write(
        digest_dir.join(TEMPLATE_ROOTFS_ZSTD_FILE),
        b"zstd bytes (test fixture)",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_request_leaves_recoverable_state_for_next_caller() {
    // The user-facing contract of the recovery design: if a previous
    // GetTemplate caller's future was dropped right before the DB
    // upsert, the disk artifacts they wrote are sufficient for the
    // NEXT GetTemplate to register the build without re-downloading
    // or re-decompressing anything. This test stands in for the
    // hard-to-fixture "vol-mgr finished the pull, our spawn_blocking
    // finished the decompress, caller's future was then dropped"
    // sequence by constructing the resulting on-disk shape directly
    // and calling the recovery helper.

    // Fresh in-memory DB with the production schema.
    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    // Build the post-cancel disk state.
    let tmp = tempfile::tempdir().unwrap();
    let digest_dir = tmp.path();
    populate_orphaned_digest_dir(digest_dir).await;

    let name = "seed";
    let content_hash = "sha256:cancellation-residue";

    // Sanity: nothing in the DB yet.
    assert!(
        crate::db::get_build(&db.lock().unwrap(), name, content_hash)
            .unwrap()
            .is_none(),
        "precondition: cancelled caller never wrote the row"
    );

    // The "next GetTemplate" calls recovery.
    let rootfs_size = recover_from_digest_dir(&db, name, content_hash, digest_dir)
        .await
        .expect("orphaned digest dir must recover");
    assert_eq!(
        rootfs_size,
        b"decompressed rootfs bytes (test fixture)".len() as u64
    );

    // The DB row is now present and the manifest is normalized — even
    // though the on-disk manifest the cancelled caller left behind was
    // the raw vol-mgr-pulled object.
    let row = crate::db::get_build(&db.lock().unwrap(), name, content_hash)
        .unwrap()
        .expect("recovery must commit the row");
    assert_eq!(row.state, "ready");
    assert_eq!(row.artifact_size_bytes, rootfs_size);
    let stored: serde_json::Value = serde_json::from_str(&row.manifest_json).unwrap();
    assert_eq!(stored["name"], name);
    assert_eq!(stored["content_hash"], content_hash);
    assert_eq!(stored["bootable_artifact"], TEMPLATE_ROOTFS_FILE);
    assert_eq!(
        stored["files"][TEMPLATE_ROOTFS_FILE]["digest"], content_hash,
        "normalize_template_manifest must have run on the recovered manifest"
    );

    // The on-disk manifest was also rewritten atomically so a future
    // recovery (or any other reader of the digest dir) sees the
    // normalized form.
    let on_disk = tokio::fs::read_to_string(digest_dir.join("manifest.json"))
        .await
        .unwrap();
    let on_disk: serde_json::Value = serde_json::from_str(&on_disk).unwrap();
    assert_eq!(on_disk["content_hash"], content_hash);
    assert_eq!(on_disk["bootable_artifact"], TEMPLATE_ROOTFS_FILE);
}

#[tokio::test]
async fn partial_disk_state_does_not_recover() {
    // Mid-pull cancellation that interrupts BEFORE the decompress
    // completes leaves a digest dir without rootfs.ext4 — recovery
    // MUST decline so the caller falls through to the normal pull
    // path, which uses vol-mgr's skip-if-present to elide re-downloads
    // and runs the missing decompress.

    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let tmp = tempfile::tempdir().unwrap();
    let digest_dir = tmp.path();
    // Three source files present, but NO rootfs.ext4 — the residue of
    // a pull that was cancelled after the downloads finished but
    // before decompress ran.
    tokio::fs::write(digest_dir.join("metadata.json"), b"{}")
        .await
        .unwrap();
    tokio::fs::write(digest_dir.join("manifest.json"), b"{}")
        .await
        .unwrap();
    tokio::fs::write(digest_dir.join(TEMPLATE_ROOTFS_ZSTD_FILE), b"zstd")
        .await
        .unwrap();

    let outcome = recover_from_digest_dir(&db, "seed", "sha256:partial", digest_dir).await;
    assert!(
        outcome.is_err(),
        "recovery must decline when rootfs.ext4 is absent"
    );
    assert!(
        crate::db::get_build(&db.lock().unwrap(), "seed", "sha256:partial")
            .unwrap()
            .is_none(),
        "DB row must not be created on partial-state recovery"
    );
}

#[tokio::test]
async fn recovery_accepts_legacy_manifest_without_zst_on_disk() {
    // Legacy templates published before the zstd format only carry
    // `rootfs.ext4` in `manifest.files`. After a successful pull the
    // digest dir likewise has no `rootfs.ext4.zst`. Manifest-driven
    // recovery must honor that and accept the directory even though
    // the compressed sibling is absent on disk.

    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let tmp = tempfile::tempdir().unwrap();
    let digest_dir = tmp.path();
    let content_hash = "sha256:legacy-recovery";

    // Legacy on-disk shape: metadata + manifest listing only
    // rootfs.ext4 + the rootfs itself. No .zst sibling.
    tokio::fs::write(digest_dir.join("metadata.json"), br#"{"arch":"aarch64"}"#)
        .await
        .unwrap();
    let legacy_manifest = format!(
        r#"{{
            "name": "seed",
            "content_hash": "{content_hash}",
            "build_date": "2026-05-12T19:10:53+00:00",
            "runtime": "fc",
            "arch": "x86_64",
            "config": {{}},
            "bootable_artifact": "rootfs.ext4",
            "files": {{
                "rootfs.ext4": {{ "digest": "{content_hash}", "size": 470163456 }}
            }}
        }}"#
    );
    tokio::fs::write(digest_dir.join("manifest.json"), legacy_manifest.as_bytes())
        .await
        .unwrap();
    tokio::fs::write(
        digest_dir.join(TEMPLATE_ROOTFS_FILE),
        b"legacy rootfs bytes (test fixture)",
    )
    .await
    .unwrap();
    // Deliberately no rootfs.ext4.zst.

    let rootfs_size = recover_from_digest_dir(&db, "seed", content_hash, digest_dir)
        .await
        .expect("legacy on-disk shape must recover without a .zst sibling");
    assert_eq!(
        rootfs_size,
        b"legacy rootfs bytes (test fixture)".len() as u64
    );

    let row = crate::db::get_build(&db.lock().unwrap(), "seed", content_hash)
        .unwrap()
        .expect("legacy recovery must commit the row");
    assert_eq!(row.state, "ready");
    let stored: serde_json::Value = serde_json::from_str(&row.manifest_json).unwrap();
    assert!(
        stored["files"].get(TEMPLATE_ROOTFS_ZSTD_FILE).is_none(),
        "normalized manifest must not invent a .zst entry the legacy template never had"
    );
}

#[tokio::test]
async fn recovery_declines_when_manifest_requires_zst_but_disk_lacks_it() {
    // Inverse of the legacy case: when the manifest declares both
    // rootfs.ext4 AND rootfs.ext4.zst (new format), recovery must
    // require both on disk. A residue with the .zst sibling missing
    // means the pull was interrupted before vol-mgr finished
    // writing it; recovery must decline so the next caller's pull
    // path re-fetches whatever's missing.

    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let tmp = tempfile::tempdir().unwrap();
    let digest_dir = tmp.path();
    let content_hash = "sha256:new-format-partial";

    tokio::fs::write(digest_dir.join("metadata.json"), br#"{"arch":"x86_64"}"#)
        .await
        .unwrap();
    let new_format_manifest = format!(
        r#"{{
            "name": "seed",
            "content_hash": "{content_hash}",
            "build_date": "2026-05-19T19:28:29+00:00",
            "runtime": "fc",
            "arch": "x86_64",
            "config": {{}},
            "bootable_artifact": "rootfs.ext4",
            "files": {{
                "rootfs.ext4":     {{ "digest": "{content_hash}", "size": 474472448 }},
                "rootfs.ext4.zst": {{
                    "digest": "sha256:abcdef",
                    "size": 118171754,
                    "compression": {{ "codec": "zstd", "level": 3 }}
                }}
            }}
        }}"#
    );
    tokio::fs::write(
        digest_dir.join("manifest.json"),
        new_format_manifest.as_bytes(),
    )
    .await
    .unwrap();
    tokio::fs::write(digest_dir.join(TEMPLATE_ROOTFS_FILE), b"rootfs bytes")
        .await
        .unwrap();
    // No rootfs.ext4.zst — the manifest demands it, recovery must decline.

    let outcome = recover_from_digest_dir(&db, "seed", content_hash, digest_dir).await;
    assert!(
        outcome.is_err(),
        "manifest-listed file missing on disk must abort recovery"
    );
    assert!(
        crate::db::get_build(&db.lock().unwrap(), "seed", content_hash)
            .unwrap()
            .is_none(),
        "DB row must not be created when a manifest-listed file is missing"
    );
}

#[test]
fn build_manifest_pull_plan_dedups_zst_pair() {
    use pplx_space_common_core::types::{
        TemplateManifest, TemplateManifestCompression, TemplateManifestFile,
    };

    let storage =
        StorageUri::parse("s3://b/templates/seed/sha256:abc?region=us-east-2&path_style=false")
            .expect("storage URI must parse");

    // New-format manifest: both rootfs.ext4 and rootfs.ext4.zst listed.
    let mut new_format = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
    };
    new_format.files.insert(
        "rootfs.ext4".to_string(),
        TemplateManifestFile {
            digest: "sha256:abc".to_string(),
            size: Some(1024),
            compression: None,
        },
    );
    new_format.files.insert(
        "rootfs.ext4.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:def".to_string(),
            size: Some(256),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );
    let plan = build_manifest_pull_plan(&new_format, &storage, "/space/cache/templates/seed/h");
    assert_eq!(
        plan.len(),
        1,
        "dedup must drop rootfs.ext4 when the .zst sibling is present"
    );
    assert_eq!(plan[0].manifest_key, "rootfs.ext4.zst");
    assert!(
        plan[0].raw,
        "compressed entries must pull with raw=true so vol-mgr doesn't transparently decompress"
    );

    // Legacy manifest: only rootfs.ext4.
    let mut legacy = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
    };
    legacy.files.insert(
        "rootfs.ext4".to_string(),
        TemplateManifestFile {
            digest: "sha256:abc".to_string(),
            size: Some(1024),
            compression: None,
        },
    );
    let plan = build_manifest_pull_plan(&legacy, &storage, "/space/cache/templates/seed/h");
    assert_eq!(
        plan.len(),
        1,
        "legacy manifest yields a single uncompressed entry"
    );
    assert_eq!(plan[0].manifest_key, "rootfs.ext4");
    assert!(
        !plan[0].raw,
        "uncompressed entries must pull with raw=false"
    );
}

#[test]
fn build_manifest_pull_plan_handles_multiple_files_and_pairs() {
    // Forward-compat: when manifests start listing files beyond the
    // rootfs, the pull plan must (a) include every standalone file
    // and (b) apply the dedup rule to every `(X, X.zst)` pair, not
    // just the rootfs pair.
    use pplx_space_common_core::types::{
        TemplateManifest, TemplateManifestCompression, TemplateManifestFile,
    };

    let storage =
        StorageUri::parse("s3://b/templates/seed/sha256:abc?region=us-east-2&path_style=false")
            .expect("storage URI must parse");

    let mut manifest = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
    };
    // Rootfs + its zstd sibling.
    manifest.files.insert(
        "rootfs.ext4".to_string(),
        TemplateManifestFile {
            digest: "sha256:rootfs".to_string(),
            size: Some(1024),
            compression: None,
        },
    );
    manifest.files.insert(
        "rootfs.ext4.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:rootfs-zst".to_string(),
            size: Some(256),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );
    // A second compressed pair, e.g. an extra config blob.
    manifest.files.insert(
        "extra-config.json".to_string(),
        TemplateManifestFile {
            digest: "sha256:cfg".to_string(),
            size: Some(512),
            compression: None,
        },
    );
    manifest.files.insert(
        "extra-config.json.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:cfg-zst".to_string(),
            size: Some(128),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );
    // A standalone file with no compressed counterpart.
    manifest.files.insert(
        "kernel".to_string(),
        TemplateManifestFile {
            digest: "sha256:kern".to_string(),
            size: Some(2048),
            compression: None,
        },
    );

    let plan = build_manifest_pull_plan(&manifest, &storage, "/dir");
    let keys: std::collections::HashSet<String> =
        plan.iter().map(|e| e.manifest_key.clone()).collect();
    assert_eq!(
        plan.len(),
        3,
        "expected 3 entries after dedup, got {plan:?}"
    );
    assert!(
        keys.contains("rootfs.ext4.zst"),
        "rootfs.ext4 must dedup to its .zst sibling"
    );
    assert!(
        keys.contains("extra-config.json.zst"),
        "extra-config.json must dedup to its .zst sibling"
    );
    assert!(
        keys.contains("kernel"),
        "standalone files with no .zst sibling must pull as-is"
    );
    assert!(
        !keys.contains("rootfs.ext4"),
        "uncompressed rootfs entry must not appear when its .zst sibling is listed"
    );
    assert!(
        !keys.contains("extra-config.json"),
        "uncompressed config entry must not appear when its .zst sibling is listed"
    );
}

#[test]
fn build_decompress_targets_pairs_every_zst_with_its_sibling() {
    // Symmetric to `build_manifest_pull_plan_handles_multiple_files_and_pairs`:
    // every `.zst` whose sibling is in the manifest becomes one
    // decompress target, regardless of whether it's the rootfs.
    use pplx_space_common_core::types::{
        TemplateManifest, TemplateManifestCompression, TemplateManifestFile,
    };

    let mut manifest = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
    };
    // Rootfs pair.
    manifest.files.insert(
        "rootfs.ext4".to_string(),
        TemplateManifestFile {
            digest: "sha256:rootfs".to_string(),
            size: Some(1024),
            compression: None,
        },
    );
    manifest.files.insert(
        "rootfs.ext4.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:rootfs-zst".to_string(),
            size: Some(256),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );
    // Extra config pair.
    manifest.files.insert(
        "extra-config.json".to_string(),
        TemplateManifestFile {
            digest: "sha256:cfg".to_string(),
            size: Some(512),
            compression: None,
        },
    );
    manifest.files.insert(
        "extra-config.json.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:cfg-zst".to_string(),
            size: Some(128),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );
    // A .zst with NO uncompressed sibling — should be skipped
    // (nothing to materialize, the manifest didn't ask).
    manifest.files.insert(
        "orphan.bin.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:orphan".to_string(),
            size: Some(64),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );

    let targets = build_decompress_targets(&manifest, "/dir");
    let names: std::collections::HashSet<String> = targets
        .iter()
        .map(|t| t.uncompressed_name.clone())
        .collect();
    assert_eq!(
        targets.len(),
        2,
        "expected 2 decompress pairs, got {targets:?}"
    );
    assert!(names.contains("rootfs.ext4"));
    assert!(names.contains("extra-config.json"));
    // Each target must carry the expected digest for both sides: the
    // compressed bytes and the rehashed expanded bytes are each
    // verified against them during materialization.
    for t in &targets {
        assert!(
            !t.expected_uncompressed_digest.is_empty(),
            "expected_uncompressed_digest must propagate from manifest.files"
        );
        assert!(
            !t.expected_compressed_digest.is_empty(),
            "expected_compressed_digest must propagate from manifest.files"
        );
    }
}

#[test]
fn manifest_digest_check_requires_both_digests_and_rootfs_release_match() {
    let rootfs_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let compressed_digest =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let rootfs_target = DecompressTarget {
        uncompressed_name: TEMPLATE_ROOTFS_FILE.to_string(),
        compressed_path: "/dir/rootfs.ext4.zst".to_string(),
        uncompressed_path: "/dir/rootfs.ext4".to_string(),
        expected_uncompressed_digest: rootfs_digest.to_string(),
        expected_compressed_digest: compressed_digest.to_string(),
    };
    check_manifest_digests_for_target(&rootfs_target, TEMPLATE_ROOTFS_FILE, rootfs_digest)
        .expect("matching rootfs manifest digest passes the manifest check");

    let mismatch = check_manifest_digests_for_target(
        &rootfs_target,
        TEMPLATE_ROOTFS_FILE,
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    )
    .expect_err("rootfs manifest digest must match resolved release hash");
    assert!(matches!(
        mismatch,
        PullError::HashMismatch {
            expected,
            actual
        } if expected == "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            && actual == rootfs_digest
    ));

    let missing_uncompressed = DecompressTarget {
        expected_uncompressed_digest: String::new(),
        ..rootfs_target.clone()
    };
    assert!(matches!(
        check_manifest_digests_for_target(
            &missing_uncompressed,
            TEMPLATE_ROOTFS_FILE,
            rootfs_digest
        ),
        Err(PullError::Failure(PullFailure::ManifestDigestMissing))
    ));

    let missing_compressed = DecompressTarget {
        expected_compressed_digest: String::new(),
        ..rootfs_target.clone()
    };
    assert!(matches!(
        check_manifest_digests_for_target(&missing_compressed, TEMPLATE_ROOTFS_FILE, rootfs_digest),
        Err(PullError::Failure(
            PullFailure::ManifestCompressedDigestMissing
        ))
    ));

    let non_rootfs_target = DecompressTarget {
        uncompressed_name: "extra-config.json".to_string(),
        ..rootfs_target
    };
    check_manifest_digests_for_target(&non_rootfs_target, TEMPLATE_ROOTFS_FILE, rootfs_digest)
        .expect("a non-rootfs pair is not compared with the release hash");
}

#[test]
fn recovery_normalizes_manifest_before_db_commit() {
    // Disk-recovery's old behavior stored whatever manifest.json was on
    // disk straight into the DB. If a cancellation lands between the
    // rootfs.ext4 rename and the atomic manifest re-write, the on-disk
    // manifest is the raw object vol-mgr wrote — missing the fields
    // normalize_template_manifest injects (`content_hash`,
    // `bootable_artifact`, `files["rootfs.ext4"]`). The fix re-runs
    // normalization on recovery so the DB row carries the same shape
    // a fresh pull would have committed.
    let raw_manifest = pplx_space_common_core::types::TemplateManifest {
        runtime: "fc".to_string(),
        ..Default::default()
    };
    let metadata = pplx_space_common_core::types::TemplateMetadata {
        arch: Some("aarch64".to_string()),
        ..Default::default()
    };

    let normalized = crate::service::normalize_template_manifest(
        raw_manifest,
        "seed",
        "sha256:abc123",
        &metadata,
        4096,
        None,
    )
    .unwrap();

    assert_eq!(
        normalized.name, "seed",
        "name must be injected on recovery normalization"
    );
    assert_eq!(
        normalized.content_hash, "sha256:abc123",
        "content_hash must be injected on recovery normalization"
    );
    assert_eq!(
        normalized.bootable_artifact, TEMPLATE_ROOTFS_FILE,
        "bootable_artifact must be injected on recovery normalization"
    );
    let rootfs_entry = normalized
        .files
        .get(TEMPLATE_ROOTFS_FILE)
        .expect("files[rootfs.ext4] must be present on recovery normalization");
    assert_eq!(
        rootfs_entry.digest, "sha256:abc123",
        "files[rootfs.ext4].digest must be injected on recovery normalization"
    );
    assert_eq!(
        rootfs_entry.size,
        Some(4096),
        "files[rootfs.ext4].size must reflect on-disk rootfs.ext4 length"
    );
    // We don't have decompress stats on recovery, so the .zst entry is
    // intentionally absent.
    assert!(
        !normalized.files.contains_key(TEMPLATE_ROOTFS_ZSTD_FILE),
        "rootfs.ext4.zst entry must be absent without decompress stats"
    );
    // `TemplateManifest`'s derived `Default` leaves `config: Null`
    // (serde_json::Value's Default is Null). The pre-typing helper
    // patched this to `{}` so the on-disk manifest stayed consistent
    // across writers; pin that behavior so the typed normalize keeps
    // matching what `serde_json::to_string_pretty` would have emitted.
    assert!(
        matches!(&normalized.config, serde_json::Value::Object(o) if o.is_empty()),
        "null/default config must normalize to {{}} on recovery, got {:?}",
        normalized.config
    );
}

#[test]
fn normalize_patches_explicit_null_config_to_empty_object() {
    // A wire manifest carrying `"config": null` deserializes to
    // `Value::Null`; without the null→{} patch the round-trip would
    // re-emit `"config": null` and the seed-register on-disk shape
    // would drift from the pre-typing one.
    let raw_manifest = pplx_space_common_core::types::TemplateManifest {
        config: serde_json::Value::Null,
        ..Default::default()
    };
    let metadata = pplx_space_common_core::types::TemplateMetadata::default();
    let normalized = crate::service::normalize_template_manifest(
        raw_manifest,
        "seed",
        "sha256:abc",
        &metadata,
        4096,
        None,
    )
    .unwrap();
    let json = serde_json::to_string(&normalized).unwrap();
    assert!(
        json.contains("\"config\":{}"),
        "null config must normalize to empty object on the wire: {json}"
    );
}

#[test]
fn normalize_rejects_conflicting_execution_metadata() {
    use pplx_space_common_core::types::{
        Arch, ExecutionMetadata, GuestOperatingSystem, GuestPlatform, RuntimeIdentity, RuntimeKind,
        RuntimeVersion,
    };

    let metadata_execution = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]);
    let manifest_execution = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: RuntimeVersion::parse("1.16.0").unwrap(),
        },
        GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: Arch::Aarch64,
        },
        metadata_execution.state_format().cloned(),
        vec![],
    )
    .unwrap();
    let mut raw_manifest = pplx_space_common_core::types::TemplateManifest::default();
    raw_manifest.set_execution_metadata(manifest_execution);
    let metadata = pplx_space_common_core::types::TemplateMetadata {
        schema_version: Some(pplx_space_common_core::types::TEMPLATE_METADATA_SCHEMA_VERSION),
        execution: Some(metadata_execution),
        arch: Some("aarch64".to_string()),
        ..Default::default()
    };

    let error = crate::service::normalize_template_manifest(
        raw_manifest,
        "seed",
        "sha256:abc",
        &metadata,
        4096,
        None,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        crate::service::NormalizeTemplateManifestError::ExecutionMismatch
    ));
}

#[tokio::test]
async fn hash_mismatch_cleanup_removes_digest_dir_so_recovery_declines() {
    // Hash mismatch is the cluster-metadata-vs-S3 inconsistency signal.
    // The bad bytes have already been atomically renamed onto
    // rootfs.ext4 by `decompress_rootfs` before we hash-verify, so if
    // we leave the digest dir intact, the NEXT GetTemplate for this
    // `(name, content_hash)` will hit `try_recover_from_disk`, find
    // all four artifacts, and silently upsert a `state=ready` row
    // pointing at unverified bytes. The cleanup MUST clear the dir
    // before returning `HashMismatch`.

    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let tmp = tempfile::tempdir().unwrap();
    let digest_dir = tmp.path();
    populate_orphaned_digest_dir(digest_dir).await;
    assert!(digest_dir.join(TEMPLATE_ROOTFS_FILE).exists());

    // Simulate the cleanup that runs on hash mismatch in
    // try_pull_from. In production this goes through
    // node-manager's BtrfsDeleteTemplateSubvolume RPC; the local
    // sync helper is what nodemgr's handler effectively runs
    // on the privileged side and is what we exercise here in
    // tests (no nodemgr running in this unit-test context).
    cleanup_digest_dir_sync(digest_dir).unwrap();

    // Recovery MUST decline now — the bad rootfs.ext4 is gone.
    let outcome = recover_from_digest_dir(&db, "seed", "sha256:mismatched", digest_dir).await;
    assert!(
        outcome.is_err(),
        "post-mismatch recovery must decline; otherwise bad bytes get committed to DB"
    );
    assert!(
        crate::db::get_build(&db.lock().unwrap(), "seed", "sha256:mismatched")
            .unwrap()
            .is_none(),
        "no DB row may be created from a hash-mismatched pull"
    );
}

// PullCleanupGuard unit tests were removed when the cleanup
// path moved to node-manager: Drop now spawns an async
// `BtrfsDeleteTemplateSubvolume` RPC, which can't usefully be
// tested without a running nodemgr. The on-disk-cleanup
// behavior the old tests covered is exercised by the typed
// `local.template_cache_lru_eviction_reclaims` smoke on kind
// against a real nodemgr.

/// A credential that can write the artifact prefix can replace the compressed
/// rootfs and keep the manifest's declared expanded digest equal to the
/// release hash. The bytes that would boot must be rehashed and refused, and
/// they must never reach the rootfs path: disk recovery adopts whatever sits
/// there, so a refusal that published first would still be recoverable.
#[tokio::test(start_paused = true)]
async fn materialize_rootfs_rejects_decoded_bytes_that_disagree_with_the_release_hash() {
    use pplx_space_common_core::types::{
        TemplateManifest, TemplateManifestCompression, TemplateManifestFile,
    };

    let temp = tempfile::tempdir().unwrap();
    let digest_dir = temp.path().join("digest");
    std::fs::create_dir_all(&digest_dir).unwrap();
    let rootfs_path = digest_dir.join(TEMPLATE_ROOTFS_FILE);
    let compressed_name = format!("{TEMPLATE_ROOTFS_FILE}{ZSTD_FILENAME_SUFFIX}");
    let compressed_path = digest_dir.join(&compressed_name);
    let replaced_bytes = b"rootfs bytes the release never hashed";
    std::fs::write(&rootfs_path, replaced_bytes).unwrap();
    let (replaced_hash, _) = crate::rootfs_zstd::hash_rootfs_file(&rootfs_path).unwrap();
    let compress_stats =
        crate::rootfs_zstd::compress_rootfs(&rootfs_path, &compressed_path).unwrap();
    std::fs::remove_file(&rootfs_path).unwrap();
    let release_hash = format!("sha256:{}", "a".repeat(64));
    assert_ne!(replaced_hash, release_hash);

    let mut manifest = TemplateManifest {
        bootable_artifact: TEMPLATE_ROOTFS_FILE.to_string(),
        ..Default::default()
    };
    manifest.files.insert(
        TEMPLATE_ROOTFS_FILE.to_string(),
        TemplateManifestFile {
            digest: release_hash.clone(),
            size: Some(replaced_bytes.len() as u64),
            compression: None,
        },
    );
    manifest.files.insert(
        compressed_name,
        TemplateManifestFile {
            digest: compress_stats.compressed_hash,
            size: Some(compress_stats.compressed_size),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );

    let result = materialize_rootfs(
        &manifest,
        digest_dir.to_str().unwrap(),
        rootfs_path.to_str().unwrap(),
        TEMPLATE_ROOTFS_FILE,
        &[],
        "seed",
        &release_hash,
    )
    .await;

    match result {
        Err(PullError::HashMismatch { expected, actual }) => {
            assert_eq!(expected, release_hash);
            assert_eq!(actual, replaced_hash);
        }
        Ok(outcome) => panic!(
            "replaced rootfs bytes were committed as ready with digest {}",
            outcome.rootfs_hash
        ),
        Err(other) => panic!("expected a hash mismatch, got {other}"),
    }
    assert!(
        !rootfs_path.exists(),
        "replaced bytes must not be published at the rootfs path, or recovery could adopt them"
    );
}
