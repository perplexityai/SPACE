//! A digest stored under both spellings, `sha256:<hex>` and bare `<hex>`:
//! every internal API path that resolves it without an environment answers
//! the release `pplx_space_template_release_original::digest_original` picks,
//! which the shared spelling-pair fixture names. The API and warm-pool tests
//! assert against the same fixture.

use super::*;
use pplx_space_content_digest::Digest;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_spec::{EnvironmentReference, ExecutionMetadata};
use pplx_space_template_release_store::spelling_pairs::{
    OLD_INDEX, SpellingPair, seed_spelling_pairs,
};
use pplx_space_template_store::test_support::tag_release;

const TEMPLATE: &str = "spelling-pairs";

/// Seed the fixture's four digests of `TEMPLATE`, with a twin of each
/// spelling original when `twins` (after dropping the old index).
async fn seed(service: &ClusterApiService, twins: bool) -> TestResult<Vec<SpellingPair>> {
    register_template(service, TEMPLATE).await?;
    let twin_metadata = if twins {
        sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
            "DROP INDEX IF EXISTS {OLD_INDEX}"
        ))))
        .execute(&service.pool)
        .await?;
        let twin = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![])
            .with_environment(EnvironmentReference::new(Digest::sha256([7; 32])));
        Some(serde_json::to_string(&twin)?)
    } else {
        ensure_old_index(&service.pool).await?;
        None
    };
    Ok(seed_spelling_pairs(&service.pool, TEMPLATE, twin_metadata.as_deref()).await?)
}

fn release_id(release: Option<TemplateRelease>) -> TestResult<Uuid> {
    Ok(release.ok_or("no release")?.id.parse()?)
}

/// `FindTemplateReleaseByHash` with no environment answers the digest's
/// original under either spelling.
#[rstest::rstest]
#[case::without_twins(false)]
#[case::with_twins(true)]
#[tokio::test(flavor = "current_thread")]
async fn a_hash_lookup_answers_the_digest_original(#[case] twins: bool) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    for pair in seed(service, twins).await? {
        for hash in [&pair.canonical_hash, &pair.bare_hash] {
            let found = service
                .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
                    template_name: TEMPLATE.into(),
                    hash: hash.clone(),
                    caller_org_id: None,
                    generic_only: false,
                    environment: None,
                }))
                .await?
                .into_inner();
            assert_eq!(release_id(found.release)?, pair.original, "{hash}");
        }
    }
    Ok(())
}

/// `ListLatestTemplateReleases` with no environment lists each digest once,
/// as its original, under the `sha256:` spelling template-manager keys its
/// builds and release pins on, even when the original is stored bare.
#[rstest::rstest]
#[case::without_twins(false)]
#[case::with_twins(true)]
#[tokio::test(flavor = "current_thread")]
async fn the_latest_listing_lists_each_digest_original_once(#[case] twins: bool) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    let pairs = seed(service, twins).await?;
    let listed = service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            limit: 20,
            environment: None,
        }))
        .await?
        .into_inner()
        .entries;
    let mut listed = listed
        .into_iter()
        .map(|entry| {
            let hash = entry.release.as_ref().map(|release| release.hash.clone());
            Ok((release_id(entry.release)?, hash))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut expected = pairs
        .iter()
        .map(|pair| (pair.original, Some(pair.canonical_hash.clone())))
        .collect::<Vec<_>>();
    listed.sort();
    expected.sort();
    assert_eq!(listed, expected);
    // The fixture's layouts include originals stored bare.
    assert!(pairs.iter().any(|pair| pair.original == pair.bare));
    Ok(())
}

/// `ListWarmPoolTemplateReleases` names a pool's release under the
/// `sha256:` spelling template-manager's GC compares against, even when the
/// release is stored bare, so the build it warms stays protected.
#[tokio::test(flavor = "current_thread")]
async fn the_warm_pool_set_names_each_release_under_its_cached_spelling() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    let pairs = seed(service, false).await?;
    for pair in &pairs {
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_configs (
                 template_release_id, vcpu, memory_mib, target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled, managed_by
             )
             VALUES ($1, 2, 4096, 1, 1, 1, 'space', TRUE, 'autoscaler')"
        ))
        .bind(pair.original)
        .execute(&service.pool)
        .await?;
    }
    let protected = service
        .list_warm_pool_template_releases(Request::new(ListWarmPoolTemplateReleasesRequest {}))
        .await?
        .into_inner()
        .releases
        .into_iter()
        .map(|release| (release.template_name, release.hash))
        .collect::<std::collections::HashSet<_>>();
    let expected = pairs
        .iter()
        .map(|pair| (TEMPLATE.to_string(), pair.canonical_hash.clone()))
        .collect();
    assert_eq!(protected, expected);
    assert!(pairs.iter().any(|pair| pair.original == pair.bare));
    Ok(())
}

/// Registering a digest already stored under both spellings, under either
/// spelling and without execution metadata, attaches to the digest's
/// original and moves its tag there, and the tag lookup then answers it.
/// A tag left on the other spelling's original, as one written before the
/// shared rule can be, answers the digest original too, scoped or not.
#[rstest::rstest]
#[case::without_twins(false)]
#[case::with_twins(true)]
#[tokio::test(flavor = "current_thread")]
async fn a_registration_and_its_tag_land_on_the_digest_original(#[case] twins: bool) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    let pairs = seed(service, twins).await?;
    let before = release_count(&service.pool).await?;
    for pair in &pairs {
        tag_release(&service.pool, TEMPLATE, pair.other(), "legacy").await?;
        for generic_only in [false, true] {
            let legacy = service
                .find_template_release(Request::new(FindTemplateReleaseRequest {
                    template_name: TEMPLATE.into(),
                    runtime: "fc".into(),
                    tag: "legacy".into(),
                    caller_org_id: None,
                    generic_only,
                    environment: None,
                }))
                .await?
                .into_inner();
            assert_eq!(release_id(legacy.release)?, pair.original, "{generic_only}");
        }
        for hash in [&pair.canonical_hash, &pair.bare_hash] {
            let registered = service
                .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
                    system_publication_grant_id: None,
                    tenant_publication_grant_id: None,
                    template_name: TEMPLATE.into(),
                    runtime: "fc".into(),
                    hash: hash.clone(),
                    tags: vec!["latest".into()],
                    storage_connection_strings: vec![format!(
                        "s3://fixture-bucket/templates/{TEMPLATE}/{hash}?region=us-east-1"
                    )],
                    org_id: None,
                    execution: None,
                }))
                .await?
                .into_inner();
            assert_eq!(release_id(registered.release)?, pair.original, "{hash}");

            let tagged = service
                .find_template_release(Request::new(FindTemplateReleaseRequest {
                    template_name: TEMPLATE.into(),
                    runtime: "fc".into(),
                    tag: "latest".into(),
                    caller_org_id: None,
                    generic_only: false,
                    environment: None,
                }))
                .await?
                .into_inner();
            assert_eq!(release_id(tagged.release)?, pair.original, "{hash}");
            // The lookup reads a tag on either spelling as the digest's, so
            // check the stored row too: registration writes it on the
            // original.
            let stored: Uuid = sqlx::query_scalar(sql!(
                "SELECT template_release_id FROM template_release_tags
                 WHERE template_name = $1 AND tag = 'latest'"
            ))
            .bind(TEMPLATE)
            .fetch_one(&service.pool)
            .await?;
            assert_eq!(stored, pair.original, "{hash}");
        }
    }
    // Attaching a location to the original never changes which one it is,
    // and no registration added a release.
    assert_eq!(release_count(&service.pool).await?, before);
    Ok(())
}

async fn release_count(pool: &sqlx::PgPool) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT count(*) FROM template_releases WHERE template_name = $1"
    ))
    .bind(TEMPLATE)
    .fetch_one(pool)
    .await?)
}
