use super::*;
use crate::proto::TemplateChunkRepresentation;

fn representation(size: u64, index: char) -> TemplateChunkRepresentation {
    TemplateChunkRepresentation {
        format_version: 1,
        manifest_bytes: 4096,
        chunk_size_bytes: size,
        index_digest: format!("blake3:{}", index.to_string().repeat(64)),
        manifest_digest: format!("blake3:{}", "b".repeat(64)),
        image_blake3: format!("blake3:{}", "c".repeat(64)),
        storage_prefix: "s3://chunk-fixture/objects?region=us-east-1".into(),
    }
}

fn registration(
    representations: Vec<TemplateChunkRepresentation>,
) -> RegisterTemplateReleaseRequest {
    RegisterTemplateReleaseRequest {
        template_name: "chunk-fixture".into(),
        runtime: "fc".into(),
        hash: format!("sha256:{}", "a".repeat(64)),
        tags: vec!["latest".into()],
        chunk_representations: representations,
        ..Default::default()
    }
}

#[tokio::test]
async fn chunk_representations_round_trip_and_conflicts_rollback_tags() -> TestResult {
    let fixture = cluster_fixture().await?.expect("Postgres required");
    register_template(&fixture.service, "chunk-fixture").await?;
    let initial = representation(512 << 10, 'd');
    let request = registration(vec![initial.clone()]);
    let first = fixture
        .service
        .register_template_release(publisher_request(request.clone()))
        .await?
        .into_inner()
        .release
        .unwrap();
    let repeated = fixture
        .service
        .register_template_release(publisher_request(request))
        .await?
        .into_inner()
        .release
        .unwrap();
    assert_eq!(first, repeated);
    let second = representation(4 << 20, 'e');
    let added = fixture
        .service
        .register_template_release(publisher_request(registration(vec![second.clone()])))
        .await?
        .into_inner()
        .release
        .unwrap();
    assert_eq!(added.id, first.id);
    assert_eq!(added.chunk_representations, vec![initial.clone(), second]);
    let by_tag = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            template_name: "chunk-fixture".into(),
            runtime: "fc".into(),
            tag: "latest".into(),
            ..Default::default()
        }))
        .await?
        .into_inner()
        .release
        .unwrap();
    let by_hash = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            template_name: "chunk-fixture".into(),
            hash: first.hash.clone(),
            ..Default::default()
        }))
        .await?
        .into_inner()
        .release
        .unwrap();
    let latest = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "chunk-fixture".into(),
            runtime: "fc".into(),
            limit: 1,
        }))
        .await?
        .into_inner();
    assert_eq!(by_tag, added);
    assert_eq!(by_hash, added);
    assert_eq!(latest.entries[0].release.as_ref(), Some(&added));

    let mut conflicting = registration(vec![representation(512 << 10, 'f')]);
    conflicting.tags = vec!["must-not-commit".into()];
    let error = fixture
        .service
        .register_template_release(publisher_request(conflicting))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);
    let absent = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            template_name: "chunk-fixture".into(),
            runtime: "fc".into(),
            tag: "must-not-commit".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(absent.code(), Code::NotFound);

    // Legacy callers may still retag; they cannot erase the new commitments.
    let legacy = fixture
        .service
        .register_template_release(publisher_request(registration(vec![])))
        .await?
        .into_inner()
        .release
        .unwrap();
    assert_eq!(legacy, added);
    Ok(())
}

#[test]
fn chunk_representations_reject_invalid_commitments_and_untrusted_origins() {
    let policy =
        crate::trusted_storage::TrustedTemplateStorageBuckets::from_buckets(["chunk-fixture"]);
    let rootfs = format!("sha256:{}", "a".repeat(64));
    let good = representation(512 << 10, 'd');
    crate::template_chunks::validate(vec![good.clone()], &rootfs, &policy).unwrap();
    let mut cases = Vec::new();
    let mut bad = good.clone();
    bad.format_version = 2;
    cases.push(vec![bad]);
    let mut bad = good.clone();
    bad.chunk_size_bytes = 0;
    cases.push(vec![bad]);
    let mut bad = good.clone();
    bad.index_digest = "blake3:abcd".into();
    cases.push(vec![bad]);
    let mut bad = good.clone();
    bad.manifest_digest = rootfs.clone();
    cases.push(vec![bad]);
    let mut bad = good.clone();
    bad.storage_prefix = "s3://untrusted/objects?region=us-east-1".into();
    cases.push(vec![bad]);
    let mut bad = good.clone();
    bad.storage_prefix
        .push_str("&endpoint=https://example.invalid");
    cases.push(vec![bad]);
    cases.push(vec![good.clone(), good.clone()]);
    for case in cases {
        crate::template_chunks::validate(case, &rootfs, &policy).unwrap_err();
    }
    crate::template_chunks::validate(vec![good], &format!("blake3:{}", "f".repeat(64)), &policy)
        .unwrap_err();
}
