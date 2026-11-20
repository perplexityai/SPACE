//! Release twins: a second `template_releases` row with the same template name
//! and hash, bound to another execution environment.
//!
//! The expand migration (`20261118041620_template_releases_environment_unique`) adds the
//! per-environment unique index beside `idx_template_releases_template_name_hash_unique`,
//! which still admits one row per `(template_name, hash)`. A later contract
//! migration drops the old index. These tests run the registration writer and
//! the lookups against all three schemas: both indexes (after the expand migration), the
//! old index alone (before it, or with it rolled back) and the new index alone
//! (after the contract migration, the only schema that admits twins). They
//! also run the statement the previous writer runs against each, which is the
//! evidence for the order the two migrations deploy in.

use super::*;
use pplx_space_content_digest::Digest;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_spec::{EnvironmentReference, ExecutionMetadata};

const TEMPLATE: &str = "code-runtime";
const HASH: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const STORAGE: &str = "s3://bucket/templates/code-runtime/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?region=us-east-1";
const OLD_INDEX: &str = "idx_template_releases_template_name_hash_unique";
const NEW_INDEX: &str = "template_releases_template_name_hash_environment_key";

/// The statement the writer before twins ran, verbatim but for the `sql!`
/// locator: an old pod still runs it during the rollout and after a rollback.
const PREVIOUS_WRITER_INSERT: &str =
    "INSERT INTO template_releases (template_name, type, hash, org_id, execution_metadata)
     SELECT $1, $2, $3, COALESCE($4, org_id), $5
     FROM templates
     WHERE name = $1
     ON CONFLICT (template_name, hash) DO NOTHING
     RETURNING id";

fn environment(seed: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([seed; 32]))
}

fn bound(seed: u8) -> ExecutionMetadata {
    ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]).with_environment(environment(seed))
}

async fn drop_index(pool: &sqlx::PgPool, index: &str) -> TestResult {
    sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
        "DROP INDEX IF EXISTS {index}"
    ))))
    .execute(pool)
    .await?;
    Ok(())
}

async fn register(
    service: &ClusterApiService,
    execution: Option<&ExecutionMetadata>,
    tags: &[&str],
) -> Result<RegisterTemplateReleaseResponse, Status> {
    register_hash(service, HASH, execution, tags).await
}

async fn register_hash(
    service: &ClusterApiService,
    hash: &str,
    execution: Option<&ExecutionMetadata>,
    tags: &[&str],
) -> Result<RegisterTemplateReleaseResponse, Status> {
    Ok(service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            hash: hash.into(),
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
            storage_connection_strings: vec![STORAGE.replace(HASH, hash)],
            org_id: None,
            execution: execution.map(Into::into),
        }))
        .await?
        .into_inner())
}

async fn release_rows(pool: &sqlx::PgPool) -> TestResult<Vec<(Uuid, Option<String>)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT id, execution_metadata->>'environment'
         FROM template_releases
         WHERE template_name = 'code-runtime'
         ORDER BY created_at, id"
    ))
    .fetch_all(pool)
    .await?)
}

/// Who asks: each find RPC has an unscoped statement and a scoped one, and
/// the scoped one serves an asserted organization, a generic-only prefetch
/// and a node-scoped caller.
#[derive(Clone, Copy, Debug)]
enum Scope {
    Unscoped,
    CallerOrg,
    GenericOnly,
    Node,
}

const SCOPES: [Scope; 4] = [
    Scope::Unscoped,
    Scope::CallerOrg,
    Scope::GenericOnly,
    Scope::Node,
];

impl Scope {
    fn caller_org_id(self) -> Option<String> {
        matches!(self, Self::CallerOrg).then(|| "00000000-0000-0000-0000-000000000003".into())
    }

    fn generic_only(self) -> bool {
        matches!(self, Self::GenericOnly)
    }

    fn request<T>(self, message: T) -> Request<T> {
        match self {
            Self::Node => node_scoped_request(message, std::net::Ipv4Addr::new(10, 0, 0, 99)),
            Self::Unscoped | Self::CallerOrg | Self::GenericOnly => Request::new(message),
        }
    }
}

async fn by_tag(
    service: &ClusterApiService,
    environment: Option<EnvironmentReference>,
) -> Result<FindTemplateReleaseResponse, Status> {
    by_tag_as(service, Scope::Unscoped, environment).await
}

async fn by_tag_as(
    service: &ClusterApiService,
    scope: Scope,
    environment: Option<EnvironmentReference>,
) -> Result<FindTemplateReleaseResponse, Status> {
    Ok(service
        .find_template_release(scope.request(FindTemplateReleaseRequest {
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            tag: "latest".into(),
            caller_org_id: scope.caller_org_id(),
            generic_only: scope.generic_only(),
            environment: environment.map(|environment| environment.to_string()),
        }))
        .await?
        .into_inner())
}

async fn by_hash(
    service: &ClusterApiService,
    environment: Option<EnvironmentReference>,
) -> Result<FindTemplateReleaseResponse, Status> {
    by_hash_as(service, Scope::Unscoped, environment).await
}

async fn by_hash_as(
    service: &ClusterApiService,
    scope: Scope,
    environment: Option<EnvironmentReference>,
) -> Result<FindTemplateReleaseResponse, Status> {
    Ok(service
        .find_template_release_by_hash(scope.request(FindTemplateReleaseByHashRequest {
            template_name: TEMPLATE.into(),
            hash: HASH.into(),
            caller_org_id: scope.caller_org_id(),
            generic_only: scope.generic_only(),
            environment: environment.map(|environment| environment.to_string()),
        }))
        .await?
        .into_inner())
}

async fn latest(
    service: &ClusterApiService,
    environment: Option<EnvironmentReference>,
) -> Result<Vec<TemplateRelease>, Status> {
    latest_as(service, Scope::Unscoped, environment).await
}

/// `ListLatestTemplateReleases` has no organization or generic-only field,
/// so only a node-scoped caller takes its scoped statement.
async fn latest_as(
    service: &ClusterApiService,
    scope: Scope,
    environment: Option<EnvironmentReference>,
) -> Result<Vec<TemplateRelease>, Status> {
    Ok(service
        .list_latest_template_releases(scope.request(ListLatestTemplateReleasesRequest {
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            limit: 10,
            environment: environment.map(|environment| environment.to_string()),
        }))
        .await?
        .into_inner()
        .entries
        .into_iter()
        .filter_map(|entry| entry.release)
        .collect())
}

/// With both indexes standing, a registration bound to another environment
/// is refused exactly as before twins, and nothing is written: no row, no tag.
#[tokio::test(flavor = "current_thread")]
async fn twin_registration_is_refused_while_the_old_index_stands() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    register_template(service, TEMPLATE).await?;
    let original = register(service, Some(&bound(1)), &["latest"]).await?;
    let original = original.release.expect("release");
    assert_eq!(original.environment, Some(environment(1).to_string()));

    let error = register(service, Some(&bound(2)), &["twin"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);
    assert_eq!(
        error.message(),
        "template release already has different execution metadata"
    );
    assert_eq!(
        release_rows(&service.pool).await?,
        vec![(original.id.parse()?, Some(environment(1).to_string()))]
    );
    let twin_tags: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM template_release_tags WHERE tag = 'twin'"
    ))
    .fetch_one(&service.pool)
    .await?;
    assert_eq!(twin_tags, 0);
    Ok(())
}

/// The registration writer works against each schema of the rollout: the old
/// index alone (the expand migration not applied or rolled back), both, and the new index
/// alone. Without twins, every schema stores the same single row, fills a row
/// recorded without evidence and refuses contradicting evidence.
#[rstest::rstest]
#[case::old_index_only(Some(NEW_INDEX))]
#[case::both_indexes(None)]
#[case::new_index_only(Some(OLD_INDEX))]
#[tokio::test(flavor = "current_thread")]
async fn writer_without_twins_is_unchanged_on_every_schema(
    #[case] dropped: Option<&str>,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    if let Some(index) = dropped {
        drop_index(&service.pool, index).await?;
    }
    register_template(service, TEMPLATE).await?;

    let first = register(service, None, &["latest"])
        .await?
        .release
        .expect("release");
    assert_eq!(first.environment, None);
    let filled = register(service, Some(&initial_bound()), &["call-1"])
        .await?
        .release
        .expect("release");
    assert_eq!(filled.id, first.id, "evidence fills the existing row");
    assert_eq!(filled.environment, Some(initial().to_string()));
    let retried = register(service, Some(&initial_bound()), &["stable"])
        .await?
        .release
        .expect("release");
    assert_eq!(retried.id, first.id);
    let unselected = register(service, None, &["again"])
        .await?
        .release
        .expect("release");
    assert_eq!(unselected.id, first.id);
    assert_eq!(unselected.environment, Some(initial().to_string()));

    let changed =
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]).with_environment(initial());
    let error = register(service, Some(&changed), &["call-2"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);

    let rows = release_rows(&service.pool).await?;
    assert_eq!(rows, vec![(first.id.parse()?, Some(initial().to_string()))]);
    Ok(())
}

/// Once the old index is gone, a registration bound to another environment
/// inserts the release's twin, sharing its storage; a retry of either is
/// idempotent, and evidence that contradicts a twin is refused like any
/// other. Lookups without an environment still answer with the original
/// release, and never list the twin; with one, they answer with the release
/// bound to it, or NotFound.
#[tokio::test(flavor = "current_thread")]
async fn twins_resolve_only_by_environment_once_the_old_index_drops() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;

    let original = register(service, Some(&bound(1)), &["latest"])
        .await?
        .release
        .expect("release");
    let twin = register(service, Some(&bound(2)), &["latest"])
        .await?
        .release
        .expect("release");
    assert_ne!(twin.id, original.id);
    assert_eq!(twin.hash, original.hash);
    assert_eq!(twin.environment, Some(environment(2).to_string()));
    let retried = register(service, Some(&bound(2)), &["latest"])
        .await?
        .release
        .expect("release");
    assert_eq!(retried.id, twin.id);
    let changed = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![])
        .with_environment(environment(2));
    assert_eq!(
        register(service, Some(&changed), &["call-5"])
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let unbound = register(service, None, &["call-6"])
        .await?
        .release
        .expect("release");
    assert_eq!(
        unbound.id, original.id,
        "a registration without evidence is the original's"
    );
    assert_eq!(release_rows(&service.pool).await?.len(), 2);

    for scope in SCOPES {
        for found in [
            by_tag_as(service, scope, None).await?,
            by_hash_as(service, scope, None).await?,
        ] {
            assert_eq!(found.release.expect("release"), original, "{scope:?}");
            assert_eq!(found.storage_locations.len(), 1, "{scope:?}");
        }
        assert_eq!(
            latest_as(service, scope, None).await?,
            vec![original.clone()],
            "{scope:?}"
        );

        for environment in [environment(1), environment(2)] {
            let expected = if environment == self::environment(1) {
                &original
            } else {
                &twin
            };
            assert_eq!(
                by_tag_as(service, scope, Some(environment))
                    .await?
                    .release
                    .as_ref(),
                Some(expected),
                "{scope:?}"
            );
            let found = by_hash_as(service, scope, Some(environment)).await?;
            assert_eq!(found.release.as_ref(), Some(expected), "{scope:?}");
            assert_eq!(found.storage_locations.len(), 1, "{scope:?}");
            assert_eq!(
                latest_as(service, scope, Some(environment)).await?,
                vec![expected.clone()],
                "{scope:?}"
            );
        }

        let missing = environment(3);
        assert_eq!(
            by_tag_as(service, scope, Some(missing))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound,
            "{scope:?}"
        );
        assert_eq!(
            by_hash_as(service, scope, Some(missing))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound,
            "{scope:?}"
        );
        assert!(
            latest_as(service, scope, Some(missing)).await?.is_empty(),
            "{scope:?}"
        );
    }

    let invalid = service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            template_name: TEMPLATE.into(),
            hash: HASH.into(),
            caller_org_id: None,
            generic_only: false,
            environment: Some("not-an-environment".into()),
        }))
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
    Ok(())
}

/// A selected list ranks each release by its version, the `created_at` of
/// its hash's original release, not by its own: a twin registered for an
/// older version after a newer version was published still lists below the
/// newer one.
#[tokio::test(flavor = "current_thread")]
async fn latest_ranks_an_older_versions_twin_below_a_newer_version() -> TestResult {
    const NEWER: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let older = register_hash(service, HASH, Some(&bound(1)), &["v1"])
        .await?
        .release
        .expect("release");
    let newer = register_hash(service, NEWER, Some(&bound(2)), &["v2"])
        .await?
        .release
        .expect("release");
    let older_twin = register_hash(service, HASH, Some(&bound(2)), &["v1"])
        .await?
        .release
        .expect("release");
    assert_eq!(older_twin.hash, older.hash);
    let ids: Vec<String> = release_rows(&service.pool)
        .await?
        .into_iter()
        .map(|(id, _)| id.to_string())
        .collect();
    assert_eq!(
        ids,
        [older.id.clone(), newer.id.clone(), older_twin.id.clone()],
        "the older version's twin is registered last"
    );

    for scope in SCOPES {
        assert_eq!(
            latest_as(service, scope, Some(environment(2))).await?,
            vec![newer.clone(), older_twin.clone()],
            "{scope:?}"
        );
        assert_eq!(
            latest_as(service, scope, None).await?,
            vec![newer.clone(), older.clone()],
            "{scope:?}"
        );
    }
    Ok(())
}

/// A lookup without an environment answers with the release a server that
/// predates twins answers with: the tagged one, or for a hash, the one row
/// there is, whatever it's bound to. A selector naming the release's own
/// environment answers with it too, and the initial environment's selector
/// also finds an unbound release.
#[rstest::rstest]
#[case::unbound(None, Some(initial()), environment(1))]
#[case::bound_to_e0(Some(initial_bound()), Some(initial()), environment(1))]
#[case::bound_to_e1(Some(bound(1)), Some(environment(1)), initial())]
#[tokio::test(flavor = "current_thread")]
async fn lookups_without_twins_answer_as_before(
    #[case] execution: Option<ExecutionMetadata>,
    #[case] finds: Option<EnvironmentReference>,
    #[case] misses: EnvironmentReference,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    register_template(service, TEMPLATE).await?;
    let release = register(service, execution.as_ref(), &["latest"])
        .await?
        .release
        .expect("release");
    let expected_environment = execution
        .as_ref()
        .and_then(|execution| execution.environment())
        .map(|environment| environment.to_string());
    assert_eq!(release.environment, expected_environment);

    for found in [by_tag(service, None).await?, by_hash(service, None).await?] {
        assert_eq!(found.release.as_ref(), Some(&release));
        assert_eq!(found.storage_locations.len(), 1);
    }
    assert_eq!(latest(service, None).await?, vec![release.clone()]);
    for found in [
        by_tag(service, finds).await?,
        by_hash(service, finds).await?,
    ] {
        assert_eq!(found.release.as_ref(), Some(&release));
    }
    assert_eq!(
        by_hash(service, Some(misses)).await.unwrap_err().code(),
        Code::NotFound
    );
    Ok(())
}

/// The previous writer's `ON CONFLICT (template_name, hash)` keeps working
/// after the expand migration, which leaves the old index in place, and is refused outright
/// once the old index is gone: the contract migration must wait until no
/// deployed or rollback image runs it.
#[tokio::test(flavor = "current_thread")]
async fn previous_writer_needs_the_old_index() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    ensure_old_index(pool).await?;
    register_template(&fixture.service, TEMPLATE).await?;
    let insert = || {
        sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(sql!(PREVIOUS_WRITER_INSERT)))
            .bind(TEMPLATE)
            .bind("fc")
            .bind(HASH)
            .bind(None::<Uuid>)
            .bind(serde_json::to_value(bound(1)).expect("serialize"))
            .fetch_optional(pool)
    };
    assert!(insert().await?.is_some(), "first insert");
    assert!(insert().await?.is_none(), "conflict does nothing");
    let new_writer = register(&fixture.service, Some(&bound(1)), &["latest"]).await?;
    assert!(
        new_writer.release.is_some(),
        "the new writer accepts the old writer's row"
    );

    drop_index(pool, OLD_INDEX).await?;
    let error = insert().await.unwrap_err();
    let database = error.as_database_error().expect("database error");
    // 42P10: no unique or exclusion constraint matching the ON CONFLICT specification.
    assert_eq!(database.code().as_deref(), Some("42P10"));
    Ok(())
}

fn initial() -> EnvironmentReference {
    pplx_space_execution_catalog::catalog::initial_environment_reference()
}

fn unbound() -> ExecutionMetadata {
    ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![])
}

fn initial_bound() -> ExecutionMetadata {
    unbound()
        .with_environment(pplx_space_execution_catalog::catalog::initial_environment_reference())
}

/// Nodes cache an unbound build and one bound to the initial environment in
/// the same legacy partition, so once the old index is gone a writer still
/// never adds one beside the other: the registration attaches to the existing
/// release and its metadata check refuses the difference.
#[rstest::rstest]
#[case::initial_bound_beside_unbound(unbound(), initial_bound())]
#[case::unbound_beside_initial_bound(initial_bound(), unbound())]
#[tokio::test(flavor = "current_thread")]
async fn unbound_and_initial_bound_releases_share_a_partition(
    #[case] first: ExecutionMetadata,
    #[case] second: ExecutionMetadata,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let original = register(service, Some(&first), &["latest"])
        .await?
        .release
        .expect("release");

    let error = register(service, Some(&second), &["second"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);
    assert_eq!(release_rows(&service.pool).await?.len(), 1);
    assert_eq!(
        register(service, Some(&first), &["again"])
            .await?
            .release
            .expect("release")
            .id,
        original.id
    );

    // A twin bound to another environment is still a twin.
    let twin = register(service, Some(&bound(2)), &["latest"])
        .await?
        .release
        .expect("release");
    assert_ne!(twin.id, original.id);
    assert_eq!(release_rows(&service.pool).await?.len(), 2);
    Ok(())
}

/// Once the old index is gone, a release recorded without evidence can sit
/// beside a release that already carries the registration's binding. The
/// registration attaches to the bound release, and the evidence-less one is
/// left as it was. Where the attach path is handed the evidence-less release
/// anyway (a lookup that ran before the bound release committed), filling it
/// would duplicate that binding (23505): the fill's savepoint rolls back, the
/// attach returns the bound release, and the transaction goes on and commits.
#[tokio::test(flavor = "current_thread")]
async fn a_fill_that_collides_with_a_twin_attaches_to_the_twin() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    // The evidence-less release owns the storage location, so a legacy
    // registration's lookup picks it first. Beside it sits a release already
    // bound to the initial environment, which no writer of this version adds
    // next to an evidence-less one, but which manual SQL can leave once the
    // old index is gone; the test inserts it directly.
    let unevidenced = register(service, None, &["latest"])
        .await?
        .release
        .expect("release");
    let twin: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('code-runtime', 'fc', $1, $2)
         RETURNING id"
    ))
    .bind(HASH)
    .bind(serde_json::to_value(initial_bound())?)
    .fetch_one(&service.pool)
    .await?;

    let attached = service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            hash: HASH.into(),
            tags: vec!["latest".into()],
            storage_connection_strings: vec![],
            org_id: None,
            execution: Some((&initial_bound()).into()),
        }))
        .await?
        .into_inner();
    assert_eq!(attached.release.expect("release").id, twin.to_string());
    assert_eq!(
        attached.tags.len(),
        1,
        "the transaction went on after the savepoint"
    );

    let rows = release_rows(&service.pool).await?;
    assert_eq!(
        rows,
        vec![
            (unevidenced.id.parse()?, None),
            (twin, Some(initial().to_string())),
        ]
    );
    let unevidenced_metadata: Option<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT execution_metadata FROM template_releases WHERE id = $1"
    ))
    .bind(unevidenced.id.parse::<Uuid>()?)
    .fetch_one(&service.pool)
    .await?;
    assert_eq!(unevidenced_metadata, None);

    let execution = crate::release_registration::validated_execution_metadata(Some(
        &(&initial_bound()).into(),
    ))?
    .expect("execution metadata");
    let mut tx = service.pool.begin().await?;
    let row: crate::metadata::TemplateReleaseRow = sqlx::query_as(sql!(
        "SELECT id, template_name, type, hash, org_id, created_at,
                execution_metadata->>'environment' AS environment
         FROM template_releases WHERE id = $1"
    ))
    .bind(unevidenced.id.parse::<Uuid>()?)
    .fetch_one(&mut *tx)
    .await?;
    let accepted = crate::release_registration::accept_existing_release(
        &mut tx,
        row,
        "fc",
        None,
        Some(&execution),
    )
    .await?;
    assert_eq!(accepted.id, twin);
    let still_usable: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut *tx).await?;
    assert_eq!(
        still_usable, 1,
        "the transaction went on after the savepoint"
    );
    tx.commit().await?;
    assert_eq!(release_rows(&service.pool).await?, rows);
    Ok(())
}

async fn tag_target(pool: &sqlx::PgPool, tag: &str) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT template_release_id FROM template_release_tags
         WHERE template_name = 'code-runtime' AND tag = $1"
    ))
    .bind(tag)
    .fetch_one(pool)
    .await?)
}

/// A twin shares its original's template and hash. The tags a registration
/// carries attach to the original, so a lookup by tag with no environment
/// keeps resolving the original, and one with an environment reaches the
/// twin through the hash. A registration with no execution metadata attaches
/// to the original even when only the twin has a storage location.
#[tokio::test(flavor = "current_thread")]
async fn tags_and_unbound_registrations_stay_on_the_original() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let original = register(service, Some(&bound(1)), &["latest"])
        .await?
        .release
        .expect("release");
    let original_id: Uuid = original.id.parse()?;

    let twin = register(service, Some(&bound(2)), &["latest"])
        .await?
        .release
        .expect("release");
    assert_ne!(twin.id, original.id);
    assert_eq!(tag_target(&service.pool, "latest").await?, original_id);
    assert_eq!(by_tag(service, None).await?.release, Some(original.clone()));
    assert_eq!(
        by_tag(service, Some(environment(2))).await?.release,
        Some(twin.clone())
    );

    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations WHERE template_release_id = $1"
    ))
    .bind(original_id)
    .execute(&service.pool)
    .await?;
    let unbound = service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            hash: HASH.into(),
            tags: vec!["stable".into()],
            storage_connection_strings: vec![],
            org_id: None,
            execution: None,
        }))
        .await?
        .into_inner();
    assert_eq!(unbound.release.expect("release").id, original.id);
    assert_eq!(tag_target(&service.pool, "stable").await?, original_id);
    Ok(())
}

/// Selecting the initial environment selects an unbound release too: nodes
/// cache both in the legacy partition, and the registration writer treats
/// them as one.
#[tokio::test(flavor = "current_thread")]
async fn selecting_the_initial_environment_finds_an_unbound_release() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    register_template(service, TEMPLATE).await?;
    let release = register(service, Some(&unbound()), &["latest"])
        .await?
        .release
        .expect("release");
    assert_eq!(release.environment, None);
    let initial = pplx_space_execution_catalog::catalog::initial_environment_reference();

    assert_eq!(
        by_tag(service, Some(initial)).await?.release,
        Some(release.clone())
    );
    assert_eq!(
        by_hash(service, Some(initial)).await?.release,
        Some(release.clone())
    );
    assert_eq!(latest(service, Some(initial)).await?, vec![release]);
    assert_eq!(
        by_hash(service, Some(environment(2)))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    Ok(())
}

/// A release stored under the digest's bare spelling and a registration of
/// its `sha256:` spelling for another environment: the insert uses the
/// original's stored spelling, so the old index refuses it as it does for one
/// spelling, and no twin appears while that index stands.
#[tokio::test(flavor = "current_thread")]
async fn the_other_spelling_of_a_digest_is_no_way_around_the_old_index() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('code-runtime', 'fc', $1, $2)"
    ))
    .bind(bare)
    .bind(serde_json::to_value(bound(1))?)
    .execute(&service.pool)
    .await?;

    let error = register(service, Some(&bound(2)), &["other"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);
    assert_eq!(release_rows(&service.pool).await?.len(), 1);
    Ok(())
}

/// The original release of a hash is the one inserted first, even when the
/// transaction that inserts a twin began before the original's registration:
/// `created_at` records the insert, made under the template's publication
/// lock, not the transaction's start.
#[tokio::test(flavor = "current_thread")]
async fn the_original_is_the_first_inserted_not_the_first_begun() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(3).await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;

    // Begins first; its transaction start is before the original's insert.
    let mut early = service.pool.begin().await?;
    sqlx::query("SELECT 1").execute(&mut *early).await?;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let original = register(service, Some(&bound(1)), &["latest"])
        .await?
        .release
        .expect("release");
    let twin_metadata = serde_json::to_value(bound(2))?;
    let twin = crate::template_releases_store::insert_release(
        &mut *early,
        TEMPLATE,
        "fc",
        HASH,
        None,
        Some(&twin_metadata),
    )
    .await?
    .expect("twin inserted");
    early.commit().await?;

    assert_ne!(twin.id.to_string(), original.id);
    assert_eq!(
        by_hash(service, None).await?.release,
        Some(original.clone())
    );
    assert_eq!(latest(service, None).await?, vec![original]);
    Ok(())
}

/// A digest with releases under both spellings: the bare one bound to E1 and
/// owning the storage location, the `sha256:` one artifact-less and without
/// evidence. The original is the bare one, so a registration for E2 is checked
/// against it and refused, as it was before twins; the evidence-less alias is
/// never filled with another environment's binding.
#[rstest::rstest]
#[case::both_indexes(None)]
#[case::new_index_only(Some(OLD_INDEX))]
#[tokio::test(flavor = "current_thread")]
async fn an_evidence_less_alias_never_takes_another_binding(
    #[case] dropped: Option<&str>,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    if let Some(index) = dropped {
        drop_index(&service.pool, index).await?;
    }
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let owner: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('code-runtime', 'fc', $1, $2)
         RETURNING id"
    ))
    .bind(bare)
    .bind(serde_json::to_value(bound(1))?)
    .fetch_one(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(owner)
    .bind(STORAGE)
    .execute(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'stable')"
    ))
    .bind(owner)
    .execute(&service.pool)
    .await?;
    let alias: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash)
         VALUES ('code-runtime', 'fc', $1)
         RETURNING id"
    ))
    .bind(HASH)
    .fetch_one(&service.pool)
    .await?;

    let registered = register(service, Some(&bound(2)), &["stable"]).await;
    let alias_metadata: Option<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT execution_metadata FROM template_releases WHERE id = $1"
    ))
    .bind(alias)
    .fetch_one(&service.pool)
    .await?;
    assert_eq!(alias_metadata, None, "the alias is never filled");
    match dropped {
        // The old index refuses a twin under the original's spelling.
        None => assert_eq!(registered.unwrap_err().code(), Code::AlreadyExists),
        // Without it, the twin is inserted under the original's spelling.
        Some(_) => {
            let twin = registered?.release.expect("release");
            assert_eq!(twin.hash, bare);
            assert_eq!(twin.environment, Some(environment(2).to_string()));
        }
    }
    Ok(())
}

/// What's left of the old index on `public.template_releases`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum OldIndex {
    /// Valid and enforcing.
    Standing,
    /// Gone.
    Dropped,
    /// Left by a failed `DROP INDEX CONCURRENTLY` after its first step:
    /// invalid, so no query plans use it, but still ready, so every insert
    /// still checks it and a twin still conflicts.
    InvalidReady,
    /// Left by a `CREATE UNIQUE INDEX CONCURRENTLY` that fails while it
    /// builds, as when it's recreated over twins (one that fails in its
    /// validation scan leaves it ready, as above), or a `DROP INDEX CONCURRENTLY` failed after its
    /// second step: inserts no longer check it.
    InvalidNotReady,
    /// Gone from `public`, but an unrelated index of the same name sits in
    /// the schema named for the current user, which the default
    /// `search_path` puts ahead of `public`.
    DroppedButShadowed,
}

impl OldIndex {
    /// Whether a registration's insert still conflicts on `(template_name, hash)`.
    fn enforced(self) -> bool {
        matches!(self, Self::Standing | Self::InvalidReady)
    }

    async fn apply(self, pool: &sqlx::PgPool) -> TestResult {
        ensure_old_index(pool).await?;
        let flags = match self {
            Self::Standing => return Ok(()),
            Self::Dropped | Self::DroppedButShadowed => {
                drop_index(pool, &format!("public.{OLD_INDEX}")).await?;
                if self == Self::DroppedButShadowed {
                    for statement in [
                        "DO $$ BEGIN EXECUTE format('CREATE SCHEMA %I', current_user); END $$",
                        "DO $$ BEGIN EXECUTE format(
                             'CREATE TABLE %I.decoy (template_name text, hash text)', current_user);
                         END $$",
                        "DO $$ BEGIN EXECUTE format(
                             'CREATE UNIQUE INDEX idx_template_releases_template_name_hash_unique
                              ON %I.decoy (template_name, hash)', current_user);
                         END $$",
                    ] {
                        sqlx::query(sqlx::AssertSqlSafe(sql!(statement.to_owned())))
                            .execute(pool)
                            .await?;
                    }
                }
                return Ok(());
            }
            Self::InvalidReady => "indisvalid = false",
            Self::InvalidNotReady => "indisvalid = false, indisready = false",
        };
        // The states a failed concurrent build or drop leaves behind, set
        // directly: the test database's user is a superuser.
        sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
            "UPDATE pg_index SET {flags}
             WHERE indexrelid = 'public.{OLD_INDEX}'::regclass"
        ))))
        .execute(pool)
        .await?;
        Ok(())
    }
}

/// A registration bound to an environment the other spelling's original is
/// already bound to never attaches to that original, which would give it the
/// build's storage location and with it the preference, so that a later
/// registration moved tags onto it. Once the old index no longer checks
/// inserts, it inserts a twin under the preferred spelling, confirms the
/// tags already there, and a retry returns that twin. While the old index
/// still checks inserts, even invalid, it's refused as different execution
/// metadata, as before twins. Only the index on `public.template_releases`
/// counts.
#[rstest::rstest]
#[case::both_indexes(OldIndex::Standing)]
#[case::new_index_only(OldIndex::Dropped)]
#[case::old_index_invalid_but_ready(OldIndex::InvalidReady)]
#[case::old_index_invalid_and_not_ready(OldIndex::InvalidNotReady)]
#[case::old_index_name_elsewhere_on_the_search_path(OldIndex::DroppedButShadowed)]
#[tokio::test(flavor = "current_thread")]
async fn a_registration_never_attaches_to_the_other_spellings_original(
    #[case] old_index: OldIndex,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    old_index.apply(&service.pool).await?;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let mut originals = Vec::new();
    for (hash, metadata) in [(bare, bound(2)), (HASH, bound(1))] {
        originals.push(
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
                 VALUES ('code-runtime', 'fc', $1, $2)
                 RETURNING id"
            ))
            .bind(hash)
            .bind(serde_json::to_value(metadata)?)
            .fetch_one(&service.pool)
            .await?,
        );
    }
    let add_location = |id: Uuid, storage: &'static str| {
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(id)
        .bind(storage)
        .execute(&service.pool)
    };
    add_location(originals[1], "s3://bucket/prefixed?region=us-east-1").await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'stable')"
    ))
    .bind(originals[1])
    .execute(&service.pool)
    .await?;

    let tag_moved_at = || {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT modified_at::TEXT FROM template_release_tags
             WHERE template_name = 'code-runtime' AND tag = 'stable'"
        ))
        .fetch_one(&service.pool)
    };
    let moved_at = tag_moved_at().await?;
    let first = register(service, Some(&bound(2)), &["stable"]).await;
    let bare_locations = || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT count(*) FROM template_release_storage_locations
             WHERE template_release_id = $1"
        ))
        .bind(originals[0])
        .fetch_one(&service.pool)
    };
    let tag_unmoved = || async {
        assert_eq!(
            tag_target(&service.pool, "stable").await?,
            originals[1],
            "the tag stays on the preferred original"
        );
        assert_eq!(
            tag_moved_at().await?,
            moved_at,
            "the tag's modified_at moved"
        );
        TestResult::Ok(())
    };
    tag_unmoved().await?;
    assert_eq!(
        bare_locations().await?,
        0,
        "the other spelling's original gains no location"
    );
    if old_index.enforced() {
        assert_eq!(first.unwrap_err().code(), Code::AlreadyExists);
        return Ok(());
    }
    let twin = first?.release.expect("release");
    assert!(!originals.iter().any(|id| id.to_string() == twin.id));
    assert_eq!(twin.hash, HASH, "under the preferred spelling");
    assert_eq!(twin.environment, Some(environment(2).to_string()));

    let retry = register(service, Some(&bound(2)), &["stable"])
        .await?
        .release
        .expect("release");
    assert_eq!(retry.id, twin.id);
    assert_eq!(release_rows(&service.pool).await?.len(), 3);
    tag_unmoved().await?;
    assert_eq!(bare_locations().await?, 0);

    // The publisher registers an already-published hash without execution
    // metadata. The preferred original is unchanged, so the tag stays on it
    // rather than move to the other spelling's original.
    let republished = register(service, None, &["stable"])
        .await?
        .release
        .expect("release");
    assert_eq!(republished.id, originals[1].to_string());
    assert_eq!(tag_target(&service.pool, "stable").await?, originals[1]);
    Ok(())
}

/// Where both spellings of a digest already own storage and the preferred
/// original is the other spelling's, a registration that attaches to the
/// preferred original moves a tag to it from the other spelling's original,
/// as before twins, with or without the old index, within a cache partition
/// or across one.
#[rstest::rstest]
#[case::both_indexes_unbound(OldIndex::Standing, None, None, false)]
#[case::new_index_only_unbound(OldIndex::Dropped, None, None, false)]
#[case::both_indexes_across_partitions(OldIndex::Standing, Some(2), Some(3), true)]
#[case::new_index_only_across_partitions_without_metadata(
    OldIndex::Dropped,
    Some(2),
    Some(3),
    false
)]
#[case::new_index_only_across_partitions(OldIndex::Dropped, Some(2), Some(3), true)]
#[tokio::test(flavor = "current_thread")]
async fn a_tag_moves_to_the_preferred_original_from_the_other_spelling(
    #[case] old_index: OldIndex,
    #[case] preferred: Option<u8>,
    #[case] other: Option<u8>,
    #[case] with_metadata: bool,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    old_index.apply(&service.pool).await?;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let mut originals = Vec::new();
    for (hash, binding, storage) in [
        (bare, preferred, "s3://bucket/bare?region=us-east-1"),
        (HASH, other, "s3://bucket/prefixed?region=us-east-1"),
    ] {
        let metadata = binding
            .map(|n| serde_json::to_value(bound(n)))
            .transpose()?;
        let id: Uuid = sqlx::query_scalar(sql!(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
             VALUES ('code-runtime', 'fc', $1, $2)
             RETURNING id"
        ))
        .bind(hash)
        .bind(metadata)
        .fetch_one(&service.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(id)
        .bind(storage)
        .execute(&service.pool)
        .await?;
        originals.push(id);
    }
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'stable')"
    ))
    .bind(originals[1])
    .execute(&service.pool)
    .await?;

    let execution = preferred.filter(|_| with_metadata).map(bound);
    let release = register(service, execution.as_ref(), &["stable"])
        .await?
        .release
        .expect("release");
    assert_eq!(release.id, originals[0].to_string());
    assert_eq!(tag_target(&service.pool, "stable").await?, originals[0]);
    Ok(())
}

/// Where each spelling of a digest has its own original and no twins, a
/// registration fills the preferred original recorded without evidence and
/// writes its tags, as before twins, rather than attach to the other
/// spelling's original, a release of its own, whose tags it couldn't write.
#[rstest::rstest]
#[case::both_indexes(None)]
#[case::new_index_only(Some(OLD_INDEX))]
#[tokio::test(flavor = "current_thread")]
async fn an_evidence_less_preferred_original_is_filled_before_the_other_spelling_s(
    #[case] dropped: Option<&str>,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    if let Some(index) = dropped {
        drop_index(&service.pool, index).await?;
    }
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let preferred: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash)
         VALUES ('code-runtime', 'fc', $1)
         RETURNING id"
    ))
    .bind(bare)
    .fetch_one(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
         VALUES ($1, 's3://bucket/bare?region=us-east-1')"
    ))
    .bind(preferred)
    .execute(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('code-runtime', 'fc', $1, $2)"
    ))
    .bind(HASH)
    .bind(serde_json::to_value(unbound())?)
    .execute(&service.pool)
    .await?;

    let registered = register(service, Some(&unbound()), &["latest"])
        .await?
        .release
        .expect("release");

    assert_eq!(registered.id, preferred.to_string());
    assert_eq!(tag_target(&service.pool, "latest").await?, preferred);
    Ok(())
}

/// Where the preferred original has no execution metadata and the other
/// spelling's original is bound to the registration's environment, the
/// registration fills the preferred original, which holds its tag, while the
/// old index stands, as before twins. Once it's gone, the preferred original
/// stays the legacy partition's, and the registration inserts a twin under
/// the preferred spelling rather than attach to the other spelling's
/// original.
#[rstest::rstest]
#[case::both_indexes(None)]
#[case::new_index_only(Some(OLD_INDEX))]
#[tokio::test(flavor = "current_thread")]
async fn the_old_index_keeps_filling_an_evidence_less_preferred_original(
    #[case] dropped: Option<&str>,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    if let Some(index) = dropped {
        drop_index(&service.pool, index).await?;
    }
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let preferred: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash)
         VALUES ('code-runtime', 'fc', $1)
         RETURNING id"
    ))
    .bind(bare)
    .fetch_one(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
         VALUES ($1, 's3://bucket/bare?region=us-east-1')"
    ))
    .bind(preferred)
    .execute(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'stable')"
    ))
    .bind(preferred)
    .execute(&service.pool)
    .await?;
    let other: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('code-runtime', 'fc', $1, $2)
         RETURNING id"
    ))
    .bind(HASH)
    .bind(serde_json::to_value(bound(2))?)
    .fetch_one(&service.pool)
    .await?;

    let registered = register(service, Some(&bound(2)), &["stable"])
        .await?
        .release
        .expect("release");
    let preferred_metadata: Option<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT execution_metadata FROM template_releases WHERE id = $1"
    ))
    .bind(preferred)
    .fetch_one(&service.pool)
    .await?;
    match dropped {
        None => {
            assert_eq!(registered.id, preferred.to_string(), "filled, as before");
            assert_eq!(preferred_metadata, Some(serde_json::to_value(bound(2))?));
            assert_eq!(tag_target(&service.pool, "stable").await?, preferred);
        }
        Some(_) => {
            assert_ne!(registered.id, other.to_string(), "not the other original");
            assert_ne!(registered.id, preferred.to_string());
            assert_eq!(registered.hash, bare, "under the preferred spelling");
            assert_eq!(registered.environment, Some(environment(2).to_string()));
            assert_eq!(
                preferred_metadata, None,
                "the legacy original stays unfilled"
            );
            assert_eq!(release_rows(&service.pool).await?.len(), 3);
            assert_eq!(tag_target(&service.pool, "stable").await?, preferred);
        }
    }
    Ok(())
}

/// A retry finds the twin its first registration inserted even after the
/// digest's other spelling gains a storage location and its original becomes
/// the preferred one, instead of inserting a second twin for the environment.
/// The prefixed original is bound to an environment, so a twin can be bound
/// to the initial one (the legacy partition) too, and a bare original
/// recorded without evidence, which a legacy registration would otherwise
/// fill, is left as it is.
#[rstest::rstest]
#[case::another_environment(bound(2), Some(bound(1)))]
#[case::the_initial_environment(initial_bound(), Some(bound(1)))]
#[case::beside_an_evidence_less_original(initial_bound(), None)]
#[tokio::test(flavor = "current_thread")]
async fn a_retry_finds_its_twin_under_the_other_spelling(
    #[case] twin: ExecutionMetadata,
    #[case] bare_original: Option<ExecutionMetadata>,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let mut originals = Vec::new();
    for (hash, metadata) in [(bare, bare_original.as_ref()), (HASH, Some(&bound(1)))] {
        originals.push(
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
                 VALUES ('code-runtime', 'fc', $1, $2)
                 RETURNING id"
            ))
            .bind(hash)
            .bind(metadata.map(serde_json::to_value).transpose()?)
            .fetch_one(&service.pool)
            .await?,
        );
    }
    let add_location = |id: Uuid, storage: &'static str| {
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(id)
        .bind(storage)
        .execute(&service.pool)
    };
    add_location(originals[1], "s3://bucket/prefixed?region=us-east-1").await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'stable')"
    ))
    .bind(originals[1])
    .execute(&service.pool)
    .await?;

    let first = register(service, Some(&twin), &["stable"])
        .await?
        .release
        .expect("release");
    assert_eq!(
        first.hash, HASH,
        "the twin lives under the preferred spelling"
    );
    add_location(originals[0], "s3://bucket/bare?region=us-east-1").await?;
    let retry = register(service, Some(&twin), &["stable"])
        .await?
        .release
        .expect("release");

    assert_eq!(retry.id, first.id);
    let bare_metadata: Option<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT execution_metadata FROM template_releases WHERE id = $1"
    ))
    .bind(originals[0])
    .fetch_one(&service.pool)
    .await?;
    assert_eq!(
        bare_metadata,
        bare_original.map(serde_json::to_value).transpose()?,
        "the bare original is never filled"
    );
    assert_eq!(release_rows(&service.pool).await?.len(), 3);
    Ok(())
}

/// A release recorded without execution metadata predates environments and
/// counts as the legacy partition's. While the old index stands, a
/// registration bound to another environment fills it, as before twins; once
/// the index is gone, it inserts a twin and the original keeps standing for
/// the legacy partition.
#[rstest::rstest]
#[case::both_indexes(None)]
#[case::new_index_only(Some(OLD_INDEX))]
#[tokio::test(flavor = "current_thread")]
async fn an_evidence_less_original_is_filled_only_while_the_old_index_stands(
    #[case] dropped: Option<&str>,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    ensure_old_index(&service.pool).await?;
    if let Some(index) = dropped {
        drop_index(&service.pool, index).await?;
    }
    register_template(service, TEMPLATE).await?;
    let original = register(service, None, &["latest"])
        .await?
        .release
        .expect("release");

    let registered = register(service, Some(&bound(2)), &["latest"])
        .await?
        .release
        .expect("release");
    match dropped {
        None => {
            assert_eq!(registered.id, original.id, "filled, as before twins");
            assert_eq!(registered.environment, Some(environment(2).to_string()));
        }
        Some(_) => {
            assert_ne!(registered.id, original.id, "a twin");
            assert_eq!(
                by_hash(service, None).await?.release,
                Some(original.clone())
            );
            let legacy = register(service, Some(&initial_bound()), &["e0"])
                .await?
                .release
                .expect("release");
            assert_eq!(
                legacy.id, original.id,
                "the legacy registration fills the original"
            );
        }
    }
    Ok(())
}

/// A twin's registration confirms tags already on its hash and writes none:
/// it can't move a tag off another hash or create one.
#[tokio::test(flavor = "current_thread")]
async fn a_twin_registration_never_moves_a_tag_off_another_hash() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let older = register(service, Some(&bound(1)), &["stable"])
        .await?
        .release
        .expect("release");
    // A newer release of another hash holds `latest`.
    let newer_hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let newer = service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: TEMPLATE.into(),
            runtime: "fc".into(),
            hash: newer_hash.into(),
            tags: vec!["latest".into()],
            storage_connection_strings: vec![],
            org_id: None,
            execution: Some((&bound(1)).into()),
        }))
        .await?
        .into_inner()
        .release
        .expect("release");

    let before = release_rows(&service.pool).await?;
    let error = register(service, Some(&bound(2)), &["latest"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(
        release_rows(&service.pool).await?,
        before,
        "the refused registration leaves no twin behind"
    );
    assert_eq!(
        tag_target(&service.pool, "latest").await?,
        newer.id.parse::<Uuid>()?
    );

    // A tag already on its hash is confirmed; a new one is refused, since a
    // twin's registration writes no tag.
    register(service, Some(&bound(2)), &["stable"]).await?;
    assert_eq!(
        tag_target(&service.pool, "stable").await?,
        older.id.parse::<Uuid>()?
    );
    let before = release_rows(&service.pool).await?;
    let error = register(service, Some(&bound(2)), &["stable", "twin-new"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(release_rows(&service.pool).await?, before);
    let created: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM template_release_tags WHERE tag = 'twin-new'"
    ))
    .fetch_one(&service.pool)
    .await?;
    assert_eq!(created, 0);
    Ok(())
}

/// Confirming a tag that already names the original leaves it untouched, so
/// warm-pool retention, which ranks releases by their latest tag move, doesn't
/// promote the original because a twin of it was registered.
#[tokio::test(flavor = "current_thread")]
async fn a_twin_registration_leaves_an_existing_tag_untouched() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    register(service, Some(&bound(1)), &["stable"]).await?;
    sqlx::query(sql!(
        "UPDATE template_release_tags SET modified_at = '2026-01-01 00:00:00+00'
         WHERE template_name = 'code-runtime' AND tag = 'stable'"
    ))
    .execute(&service.pool)
    .await?;

    register(service, Some(&bound(2)), &["stable"]).await?;
    let untouched: bool = sqlx::query_scalar(sql!(
        "SELECT modified_at = TIMESTAMPTZ '2026-01-01 00:00:00+00'
         FROM template_release_tags
         WHERE template_name = 'code-runtime' AND tag = 'stable'"
    ))
    .fetch_one(&service.pool)
    .await?;
    assert!(untouched, "the tag's modified_at moved");
    Ok(())
}

/// A twin's registration keeps the tags it confirms until it commits, so an
/// admin tag delete that commits first, which takes no template lock, leaves
/// the tag missing and the registration refused, rather than returning a
/// tag that's gone.
#[tokio::test(flavor = "current_thread")]
async fn a_twin_registration_never_returns_a_tag_deleted_under_it() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(5).await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    register(service, Some(&bound(1)), &["stable"]).await?;

    let twin = bound(2);
    let mut delete = service.pool.begin().await?;
    sqlx::query(sql!(
        "DELETE FROM template_release_tags
         WHERE template_name = 'code-runtime' AND tag = 'stable'"
    ))
    .execute(&mut *delete)
    .await?;
    let (registration, committed) =
        tokio::join!(register(service, Some(&twin), &["stable"]), async {
            wait_until_a_session_waits_on_a_lock(&service.pool).await?;
            delete.commit().await?;
            TestResult::Ok(())
        },);
    committed?;
    let error = registration.expect_err("the tag was deleted before the registration committed");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    Ok(())
}

/// Wait until a session waits on a lock.
async fn wait_until_a_session_waits_on_a_lock(pool: &sqlx::PgPool) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM pg_stat_activity
              WHERE datname = current_database()
                AND wait_event_type = 'Lock'"
        ))
        .fetch_one(pool)
        .await?;
        if waiting > 0 {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err("no session waited on a lock".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// A selected lookup answers with one release. Where the digest has a
/// release under each spelling recording the same binding, it carries both
/// releases' storage locations, so a pull that finds one copy gone tries the
/// other. Where the other spelling's release records another binding, as an
/// unbound release that the initial environment's selector also matches, its
/// locations stay out.
#[rstest::rstest]
#[case::same_binding(Some(bound(2)), bound(2), environment(2))]
#[case::unbound_beside_initial_bound(None, initial_bound(), initial())]
#[tokio::test(flavor = "current_thread")]
async fn a_selected_lookup_carries_only_its_bindings_locations(
    #[case] bare_metadata: Option<ExecutionMetadata>,
    #[case] prefixed_metadata: ExecutionMetadata,
    #[case] selected: EnvironmentReference,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    const BARE: &str = "s3://bucket/bare?region=us-east-1";
    const PREFIXED: &str = "s3://bucket/prefixed?region=us-east-1";
    let same_binding = bare_metadata.as_ref() == Some(&prefixed_metadata);
    for (hash, metadata, storage) in [
        (bare, bare_metadata, BARE),
        (HASH, Some(prefixed_metadata), PREFIXED),
    ] {
        let id: Uuid = sqlx::query_scalar(sql!(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
             VALUES ('code-runtime', 'fc', $1, $2)
             RETURNING id"
        ))
        .bind(hash)
        .bind(metadata.map(serde_json::to_value).transpose()?)
        .fetch_one(&service.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(id)
        .bind(storage)
        .execute(&service.pool)
        .await?;
    }

    let found = by_hash(service, Some(selected)).await?;
    let release = found.release.expect("release");
    let mut locations = found
        .storage_locations
        .iter()
        .map(|location| location.connection_string.as_str())
        .collect::<Vec<_>>();
    locations.sort_unstable();
    let expected = match (same_binding, release.hash == bare) {
        (true, _) => vec![BARE, PREFIXED],
        (false, true) => vec![BARE],
        (false, false) => vec![PREFIXED],
    };
    assert_eq!(locations, expected);
    Ok(())
}

/// A selected listing counts a digest once toward its limit, whichever
/// spellings it's stored under, and answers with the spelling that has a
/// storage location, so a prefetch window of `limit` builds holds `limit`
/// distinct builds.
#[tokio::test(flavor = "current_thread")]
async fn a_selected_listing_counts_each_digest_once() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    const OLDER: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let mut ids = Vec::new();
    for (hash, storage) in [
        (OLDER, Some("s3://bucket/older?region=us-east-1")),
        (bare, None),
        (HASH, Some("s3://bucket/prefixed?region=us-east-1")),
    ] {
        let id: Uuid = sqlx::query_scalar(sql!(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
             VALUES ('code-runtime', 'fc', $1, $2)
             RETURNING id"
        ))
        .bind(hash)
        .bind(serde_json::to_value(bound(2))?)
        .fetch_one(&service.pool)
        .await?;
        if let Some(storage) = storage {
            sqlx::query(sql!(
                "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
                 VALUES ($1, $2)"
            ))
            .bind(id)
            .bind(storage)
            .execute(&service.pool)
            .await?;
        }
        ids.push(id.to_string());
    }

    for scope in [Scope::Unscoped, Scope::Node] {
        let entries = service
            .list_latest_template_releases(scope.request(ListLatestTemplateReleasesRequest {
                template_name: TEMPLATE.into(),
                runtime: "fc".into(),
                limit: 2,
                environment: Some(environment(2).to_string()),
            }))
            .await?
            .into_inner()
            .entries;
        let listed = entries
            .iter()
            .map(|entry| {
                (
                    entry.release.as_ref().expect("release").id.as_str(),
                    entry.storage_locations.len(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            listed,
            vec![(ids[2].as_str(), 1), (ids[0].as_str(), 1)],
            "{scope:?}"
        );
    }
    Ok(())
}

/// A selected listing orders a digest by when its first spelling was
/// registered, as an unselected one orders it by its original, so a digest
/// that later gained a release under its other spelling doesn't jump ahead
/// of builds registered after it.
#[tokio::test(flavor = "current_thread")]
async fn a_selected_listing_orders_a_digest_by_its_first_spelling() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    const LATER: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let mut ids = Vec::new();
    for (hash, created_at, storage) in [
        (bare, "2026-01-01 00:00:00+00", None),
        (
            LATER,
            "2026-01-02 00:00:00+00",
            Some("s3://bucket/later?region=us-east-1"),
        ),
        (
            HASH,
            "2026-01-03 00:00:00+00",
            Some("s3://bucket/prefixed?region=us-east-1"),
        ),
    ] {
        let id: Uuid = sqlx::query_scalar(sql!(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
             VALUES ('code-runtime', 'fc', $1, $2, $3::TEXT::TIMESTAMPTZ)
             RETURNING id"
        ))
        .bind(hash)
        .bind(serde_json::to_value(bound(2))?)
        .bind(created_at)
        .fetch_one(&service.pool)
        .await?;
        if let Some(storage) = storage {
            sqlx::query(sql!(
                "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
                 VALUES ($1, $2)"
            ))
            .bind(id)
            .bind(storage)
            .execute(&service.pool)
            .await?;
        }
        ids.push(id.to_string());
    }

    for scope in [Scope::Unscoped, Scope::Node] {
        let entries = service
            .list_latest_template_releases(scope.request(ListLatestTemplateReleasesRequest {
                template_name: TEMPLATE.into(),
                runtime: "fc".into(),
                limit: 2,
                environment: Some(environment(2).to_string()),
            }))
            .await?
            .into_inner()
            .entries;
        let listed = entries
            .iter()
            .map(|entry| entry.release.as_ref().expect("release").id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(listed, vec![ids[1].as_str(), ids[2].as_str()], "{scope:?}");
    }
    Ok(())
}

/// An unselected lookup by hash merges the storage locations of the digest's
/// releases under both spellings, as it always has, but only within the
/// answering release's cache partition: a location of a release bound to
/// another environment would hand a pull that partition's build.
#[tokio::test(flavor = "current_thread")]
async fn an_unselected_lookup_merges_locations_only_within_a_partition() -> TestResult {
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    const BARE: &str = "s3://bucket/bare?region=us-east-1";
    const PREFIXED: &str = "s3://bucket/prefixed?region=us-east-1";
    for bare_bound in [true, false] {
        let Some(fixture) = cluster_fixture().await? else {
            return Ok(());
        };
        let service = &fixture.service;
        register_template(service, TEMPLATE).await?;
        let bare_metadata = bare_bound
            .then(|| serde_json::to_value(bound(2)))
            .transpose()?;
        for (hash, metadata, storage) in [(bare, bare_metadata, BARE), (HASH, None, PREFIXED)] {
            let id: Uuid = sqlx::query_scalar(sql!(
                "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
                 VALUES ('code-runtime', 'fc', $1, $2)
                 RETURNING id"
            ))
            .bind(hash)
            .bind(metadata)
            .fetch_one(&service.pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
                 VALUES ($1, $2)"
            ))
            .bind(id)
            .bind(storage)
            .execute(&service.pool)
            .await?;
            if hash == bare {
                sqlx::query(sql!(
                    "INSERT INTO template_release_tags (template_release_id, template_name, tag)
                     VALUES ($1, 'code-runtime', 'latest')"
                ))
                .bind(id)
                .execute(&service.pool)
                .await?;
            }
        }

        for (lookup, found) in [
            ("by hash", by_hash(service, None).await?),
            ("by tag", by_tag(service, None).await?),
        ] {
            let release = found.release.expect("release");
            let mut locations = found
                .storage_locations
                .iter()
                .map(|location| location.connection_string.as_str())
                .collect::<Vec<_>>();
            locations.sort_unstable();
            let expected = match (bare_bound, release.hash == bare) {
                // The tag names the bare release, and an unselected lookup by
                // tag answers with the tagged release alone.
                _ if lookup == "by tag" => vec![BARE],
                (false, _) => vec![BARE, PREFIXED],
                (true, true) => vec![BARE],
                (true, false) => vec![PREFIXED],
            };
            assert_eq!(
                locations, expected,
                "{lookup}, bare release bound: {bare_bound}"
            );
        }
    }
    Ok(())
}

/// A response says whether the server applied the environment selector, so a
/// caller can tell a selected answer from one a server that predates the
/// selector gave.
#[tokio::test(flavor = "current_thread")]
async fn responses_say_whether_the_selector_was_applied() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    register_template(service, TEMPLATE).await?;
    register(service, Some(&bound(1)), &["latest"]).await?;
    for scope in SCOPES {
        for (environment, applied) in [(None, false), (Some(environment(1)), true)] {
            assert_eq!(
                by_hash_as(service, scope, environment)
                    .await?
                    .environment_applied,
                applied,
                "{scope:?}"
            );
            assert_eq!(
                by_tag_as(service, scope, environment)
                    .await?
                    .environment_applied,
                applied,
                "{scope:?}"
            );
            let response = service
                .list_latest_template_releases(scope.request(ListLatestTemplateReleasesRequest {
                    template_name: TEMPLATE.into(),
                    runtime: "fc".into(),
                    limit: 10,
                    environment: environment.map(|environment| environment.to_string()),
                }))
                .await?
                .into_inner();
            assert_eq!(response.environment_applied, applied, "{scope:?}");
        }
    }
    Ok(())
}

/// A twin has its original's owner. An ownerless template that was later
/// given an owner keeps releases recorded without one (the `(template_name,
/// org_id)` foreign key doesn't check them), so a registration under the new
/// owner is refused rather than inserted as a differently owned twin.
#[tokio::test(flavor = "current_thread")]
async fn a_twin_takes_its_original_s_owner() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    let name = format!("owned-twin-{}", Uuid::now_v7());
    register_template(service, &name).await?;
    let request =
        |org_id: Option<Uuid>, execution: &ExecutionMetadata| RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: name.clone(),
            runtime: "fc".into(),
            hash: HASH.into(),
            tags: vec!["latest".into()],
            storage_connection_strings: vec![],
            org_id: org_id.map(|org_id| org_id.to_string()),
            execution: Some(execution.into()),
        };
    service
        .register_template_release(publisher_request(request(None, &bound(1))))
        .await?;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000002")?;
    sqlx::query(sql!("UPDATE templates SET org_id = $2 WHERE name = $1"))
        .bind(&name)
        .bind(owner)
        .execute(&service.pool)
        .await?;

    let error = service
        .register_template_release(publisher_request(request(Some(owner), &bound(2))))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(
        error.message(),
        "release ownership does not match template ownership"
    );
    let rows: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM template_releases WHERE template_name = $1"
    ))
    .bind(&name)
    .fetch_one(&service.pool)
    .await?;
    assert_eq!(rows, 1);
    Ok(())
}

/// A tag can sit on a legacy bare-spelled release while its twin lives under
/// the `sha256:` spelling of the storage-owning original; a lookup by that tag
/// with the twin's environment still reaches the twin.
#[tokio::test(flavor = "current_thread")]
async fn a_selected_tag_lookup_reaches_a_twin_under_the_other_spelling() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    // The older, artifact-less bare release holds `latest`.
    let tagged: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
         VALUES ('code-runtime', 'fc', $1, $2, NOW() - INTERVAL '1 hour')
         RETURNING id"
    ))
    .bind(bare)
    .bind(serde_json::to_value(bound(1))?)
    .fetch_one(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'latest')"
    ))
    .bind(tagged)
    .execute(&service.pool)
    .await?;
    // The `sha256:` release owns the storage location, so it's the original.
    let original: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('code-runtime', 'fc', $1, $2)
         RETURNING id"
    ))
    .bind(HASH)
    .bind(serde_json::to_value(bound(1))?)
    .fetch_one(&service.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(original)
    .bind(STORAGE)
    .execute(&service.pool)
    .await?;

    let twin = register(service, Some(&bound(2)), &["latest"])
        .await?
        .release
        .expect("release");
    assert_eq!(
        twin.hash, HASH,
        "the twin lives under the original's spelling"
    );
    let found = by_tag(service, Some(environment(2))).await?;
    assert_eq!(found.release, Some(twin));
    // The artifact-less tagged release shares the original's environment;
    // the selected lookup answers with the release it can pull.
    let found = by_tag(service, Some(environment(1))).await?;
    assert_eq!(found.release.expect("release").id, original.to_string());
    assert_eq!(
        found
            .storage_locations
            .iter()
            .map(|location| location.connection_string.as_str())
            .collect::<Vec<_>>(),
        vec![STORAGE]
    );
    Ok(())
}

/// Where both spellings of the tagged digest have a storage-owning release
/// bound to the selected environment, the tagged release answers, even when
/// the other one's storage location is older, under every read scope. With no
/// selector the digest original answers, the older of the two, as every
/// lookup without an environment does.
#[tokio::test(flavor = "current_thread")]
async fn a_selected_tag_lookup_prefers_the_tagged_release() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    register_template(service, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("canonical hash");
    let mut ids = Vec::new();
    for (hash, age, storage) in [
        (bare, "2 hours", "s3://bucket/bare?region=us-east-1"),
        (HASH, "1 hour", "s3://bucket/prefixed?region=us-east-1"),
    ] {
        let id: Uuid = sqlx::query_scalar(sql!(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
             VALUES ('code-runtime', 'fc', $1, $2, NOW() - $3::INTERVAL)
             RETURNING id"
        ))
        .bind(hash)
        .bind(serde_json::to_value(bound(1))?)
        .bind(age)
        .fetch_one(&service.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string, created_at)
             VALUES ($1, $2, NOW() - $3::INTERVAL)"
        ))
        .bind(id)
        .bind(storage)
        .bind(age)
        .execute(&service.pool)
        .await?;
        ids.push(id);
    }
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'code-runtime', 'latest')"
    ))
    .bind(ids[1])
    .execute(&service.pool)
    .await?;

    for scope in SCOPES {
        for environment in [None, Some(environment(1))] {
            let (expected, storage) = if environment.is_some() {
                (ids[1], "s3://bucket/prefixed?region=us-east-1")
            } else {
                (ids[0], "s3://bucket/bare?region=us-east-1")
            };
            let found = by_tag_as(service, scope, environment).await?;
            let release = found.release.expect("release");
            assert_eq!(
                release.id,
                expected.to_string(),
                "{scope:?} {environment:?}"
            );
            assert_eq!(
                found
                    .storage_locations
                    .iter()
                    .map(|location| location.connection_string.as_str())
                    .collect::<Vec<_>>(),
                vec![storage],
                "{scope:?} {environment:?}"
            );
        }
    }
    Ok(())
}

/// An unselected lookup by hash answers with the original even when a twin
/// owns the only storage location, under every read scope, and never lists
/// the twin.
#[tokio::test(flavor = "current_thread")]
async fn an_unselected_lookup_never_answers_with_a_twin() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let service = &fixture.service;
    drop_index(&service.pool, OLD_INDEX).await?;
    register_template(service, TEMPLATE).await?;
    let mut ids = Vec::new();
    for (seed, age) in [(1, "2 hours"), (2, "1 hour")] {
        ids.push(
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
                 VALUES ('code-runtime', 'fc', $1, $2, NOW() - $3::INTERVAL)
                 RETURNING id"
            ))
            .bind(HASH)
            .bind(serde_json::to_value(bound(seed))?)
            .bind(age)
            .fetch_one(&service.pool)
            .await?,
        );
    }
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(ids[1])
    .bind(STORAGE)
    .execute(&service.pool)
    .await?;

    for scope in SCOPES {
        let found = by_hash_as(service, scope, None).await?;
        assert_eq!(
            found.release.expect("release").id,
            ids[0].to_string(),
            "{scope:?}"
        );
        assert!(found.storage_locations.is_empty(), "{scope:?}");
        let listed = latest_as(service, scope, None).await?;
        assert_eq!(
            listed
                .iter()
                .map(|release| release.id.as_str())
                .collect::<Vec<_>>(),
            vec![ids[0].to_string().as_str()],
            "{scope:?}"
        );
    }
    Ok(())
}

/// A node resolves a twin of an org-specific template only once a sandbox of
/// the template is placed on it: the selector reaches the twin through the
/// placement predicate, and a node without a placement finds nothing, with or
/// without a selector.
#[tokio::test(flavor = "current_thread")]
async fn a_private_twin_resolves_only_on_its_placed_node() -> TestResult {
    const PRIVATE: &str = "private-base";
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let service = &fixture.service;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    drop_index(pool, OLD_INDEX).await?;
    register_template_with_org(service, PRIVATE, Some(OrgId::from_uuid(owner))).await?;
    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'org_specific' WHERE name = $1"
    ))
    .bind(PRIVATE)
    .execute(pool)
    .await?;
    let mut ids = Vec::new();
    for (seed, age) in [(1, "2 hours"), (2, "1 hour")] {
        let id = sqlx::query_scalar::<_, Uuid>(sql!(
            "INSERT INTO template_releases (template_name, type, hash, org_id, execution_metadata, created_at)
             VALUES ('private-base', 'fc', $1, $2, $3, NOW() - $4::INTERVAL)
             RETURNING id"
        ))
        .bind(HASH)
        .bind(owner)
        .bind(serde_json::to_value(bound(seed))?)
        .bind(age)
        .fetch_one(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string, org_id)
             VALUES ($1, $2, $3)"
        ))
        .bind(id)
        .bind(STORAGE)
        .bind(owner)
        .execute(pool)
        .await?;
        ids.push(id);
    }
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'private-base', 'latest')"
    ))
    .bind(ids[0])
    .execute(pool)
    .await?;
    let placed_ip = std::net::Ipv4Addr::new(10, 0, 0, 30);
    let other_ip = std::net::Ipv4Addr::new(10, 0, 0, 31);
    let placed = insert_k8s_node(pool, placed_ip, "worker").await?;
    insert_k8s_node(pool, other_ip, "worker").await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, command_ready_pending,
             network_policy_generation, template_id
         )
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node', $4, $4, TRUE, 1, 'private-base')"
    ))
    .bind(Uuid::now_v7())
    .bind(SEEDED_USER_ID)
    .bind(owner)
    .bind(placed)
    .execute(pool)
    .await?;

    // Every node-facing read from `node_ip`, as the ids it answers with:
    // by tag, by hash, then the listing.
    let lookups = |node_ip: std::net::Ipv4Addr, environment: Option<EnvironmentReference>| async move {
        let environment = environment.map(|environment| environment.to_string());
        let answered =
            |result: Result<tonic::Response<FindTemplateReleaseResponse>, Status>| match result {
                Ok(response) => Ok(response.into_inner().release.map(|release| release.id)),
                Err(status) if status.code() == Code::NotFound => Ok(None),
                Err(status) => Err(status),
            };
        let by_tag = answered(
            service
                .find_template_release(node_scoped_request(
                    FindTemplateReleaseRequest {
                        template_name: PRIVATE.into(),
                        runtime: "fc".into(),
                        tag: "latest".into(),
                        caller_org_id: None,
                        generic_only: false,
                        environment: environment.clone(),
                    },
                    node_ip,
                ))
                .await,
        )?;
        let by_hash = answered(
            service
                .find_template_release_by_hash(node_scoped_request(
                    FindTemplateReleaseByHashRequest {
                        template_name: PRIVATE.into(),
                        hash: HASH.into(),
                        caller_org_id: None,
                        generic_only: false,
                        environment: environment.clone(),
                    },
                    node_ip,
                ))
                .await,
        )?;
        let listed = service
            .list_latest_template_releases(node_scoped_request(
                ListLatestTemplateReleasesRequest {
                    template_name: PRIVATE.into(),
                    runtime: "fc".into(),
                    limit: 10,
                    environment,
                },
                node_ip,
            ))
            .await?
            .into_inner()
            .entries
            .into_iter()
            .filter_map(|entry| entry.release.map(|release| release.id))
            .collect::<Vec<_>>();
        Ok::<_, Status>((by_tag, by_hash, listed))
    };

    let original = ids[0].to_string();
    let twin = ids[1].to_string();
    assert_eq!(
        lookups(placed_ip, Some(environment(2))).await?,
        (Some(twin.clone()), Some(twin.clone()), vec![twin.clone()]),
        "the placed node reaches the twin"
    );
    assert_eq!(
        lookups(placed_ip, None).await?,
        (
            Some(original.clone()),
            Some(original.clone()),
            vec![original]
        ),
        "the placed node resolves the original without a selector"
    );
    for environment in [None, Some(environment(1)), Some(environment(2))] {
        assert_eq!(
            lookups(other_ip, environment).await?,
            (None, None, vec![]),
            "a node without a placement finds nothing ({environment:?})"
        );
    }
    Ok(())
}
