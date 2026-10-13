use pplx_space_common_core::types::OrgId;
use uuid::Uuid;

use std::collections::VecDeque;

use super::*;
use crate::controllers::test_fixtures::insert_org_specific_template;
use build_proto::recipe::{
    Origin,
    origin::{FromTemplate, Kind},
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db};
use rstest::{fixture, rstest};
const TEST_ORG: OrgId = OrgId::from_uuid(uuid::Uuid::from_u128(1));

struct BaseReleaseFixture {
    db: TestDb,
    owner: Uuid,
    hash_a: String,
    hash_b: String,
}

#[fixture]
async fn base_release_fixture() -> TestDbResult<BaseReleaseFixture> {
    let db = pg_test_db().await?;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let hash_a = format!("sha256:{}", "a".repeat(64));
    let hash_b = format!("sha256:{}", "b".repeat(64));
    insert_org_specific_template(&db.pool, "private-base", owner).await?;
    for hash in [&hash_a, &hash_b] {
        sqlx::query(sql!("INSERT INTO template_releases (template_name, type, hash) VALUES ('private-base', 'fc', $1)"))
            .bind(hash).execute(&db.pool).await?;
    }
    sqlx::query(sql!("INSERT INTO template_release_tags (template_name, template_release_id, tag) SELECT template_name, id, 'stable' FROM template_releases WHERE hash = $1"))
        .bind(&hash_a).execute(&db.pool).await?;
    Ok(BaseReleaseFixture {
        db,
        owner,
        hash_a,
        hash_b,
    })
}

fn base_submission(base: String, org_id: OrgId) -> TemplateBuildSubmission {
    TemplateBuildSubmission {
        target: TemplateBuildTarget::Organization(TemplateName::parse("child").unwrap()),
        client_build_id: None,
        recipe: build_proto::Recipe {
            origin: Some(Origin {
                kind: Some(Kind::FromTemplate(FromTemplate { base })),
            }),
            ..Default::default()
        },
        force: false,
        publish_tags: vec![],
        client: ClientContext::default(),
        org_id,
    }
}

#[rstest]
#[case::canonical(false)]
#[case::legacy_unprefixed(true)]
#[tokio::test]
async fn build_admission_accepts_owner_digest(
    #[future] base_release_fixture: TestDbResult<BaseReleaseFixture>,
    #[case] unprefixed: bool,
) -> TestDbResult<()> {
    let BaseReleaseFixture {
        db, owner, hash_a, ..
    } = base_release_fixture.await?;
    if unprefixed {
        sqlx::query(sql!("UPDATE template_releases SET hash = $1 WHERE template_name = 'private-base' AND hash = $2"))
            .bind(hash_a.strip_prefix("sha256:").unwrap())
            .bind(&hash_a)
            .execute(&db.pool).await?;
    }
    let pinned = format!("private-base@{hash_a}");
    let mut submission = base_submission(pinned.clone(), OrgId::from_uuid(owner));

    submission.authorize_base(&db.pool).await?;

    let Some(Kind::FromTemplate(base)) = submission.recipe.origin.and_then(|origin| origin.kind)
    else {
        panic!("template origin")
    };
    assert_eq!(base.base, pinned);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn build_admission_reports_database_contention_as_retryable(
    #[future] base_release_fixture: TestDbResult<BaseReleaseFixture>,
) -> TestDbResult<()> {
    let BaseReleaseFixture { db, owner, .. } = base_release_fixture.await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query(sql!("SET lock_timeout = '100ms'"))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with((*db.pool.connect_options()).clone())
        .await?;
    let mut lock = db.pool.begin().await?;
    sqlx::query(sql!(
        "LOCK TABLE template_releases IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *lock)
    .await?;
    let mut submission = base_submission("private-base:stable".into(), OrgId::from_uuid(owner));

    let error = submission.authorize_base(&pool).await.unwrap_err();
    lock.rollback().await?;
    pool.close().await;

    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(error.is_lock_contention());
    Ok(())
}

#[rstest]
#[tokio::test]
async fn build_admission_authorizes_and_pins_the_base_release(
    #[future] base_release_fixture: TestDbResult<BaseReleaseFixture>,
) -> TestDbResult<()> {
    let BaseReleaseFixture {
        db,
        owner,
        hash_a,
        hash_b,
    } = base_release_fixture.await?;
    let stranger = pplx_space_db_test_fixtures::SEED_ORG_ID;

    for base in [
        "private-base:stable".into(),
        format!("private-base@{hash_a}"),
        "missing:stable".into(),
        "private-base:missing".into(),
    ] {
        let error = base_submission(base, OrgId::from_uuid(stranger))
            .authorize_base(&db.pool)
            .await
            .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(error.message, "base template not found");
    }
    let mut accepted = base_submission("private-base:stable".into(), OrgId::from_uuid(owner));
    accepted.authorize_base(&db.pool).await?;
    sqlx::query(sql!("UPDATE template_release_tags SET template_release_id = (SELECT id FROM template_releases WHERE hash = $1) WHERE template_name = 'private-base' AND tag = 'stable'"))
        .bind(&hash_b).execute(&db.pool).await?;
    let request = accepted.into_start_build_request(None);
    let Some(recipe) = request.recipe else {
        panic!("inline recipe")
    };
    let Some(Kind::FromTemplate(base)) = recipe.origin.and_then(|origin| origin.kind) else {
        panic!("template origin")
    };
    assert_eq!(base.base, format!("private-base@{hash_a}"));
    assert_eq!(request.org_id, Some(owner.to_string()));

    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'generic' WHERE name = 'private-base'"
    ))
    .execute(&db.pool)
    .await?;
    let mut shared = base_submission("private-base:stable".into(), OrgId::from_uuid(stranger));
    shared.authorize_base(&db.pool).await?;
    let Some(Kind::FromTemplate(base)) = shared.recipe.origin.and_then(|origin| origin.kind) else {
        panic!("template origin")
    };
    assert_eq!(base.base, format!("private-base@{hash_b}"));
    Ok(())
}

/// The base grant is recorded before `StartBuild` is dispatched. The builder
/// pulls the base as soon as it accepts the job, and ClusterApi only lets a
/// builder node resolve an org-specific base that a fresh grant names, so a
/// grant written after the response would race the pull.
#[rstest]
#[tokio::test]
async fn build_submission_grants_the_base_before_dispatch(
    #[future] base_release_fixture: TestDbResult<BaseReleaseFixture>,
) -> TestDbResult<()> {
    let BaseReleaseFixture { db, owner, .. } = base_release_fixture.await?;
    let substrate = Substrate::Direct(Box::new(
        crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
    ));
    let controller = TemplateController::new(substrate, db.pool.clone());
    let submission = base_submission("private-base:stable".into(), OrgId::from_uuid(owner));

    // No builder is registered, so dispatch fails; the grant must already
    // be durable by then.
    controller
        .submit_template_build(submission)
        .await
        .expect_err("no builder node is registered");

    let grant: Option<(Uuid,)> = sqlx::query_as(sql!(
        "SELECT org_id FROM template_build_base_grants
         WHERE template_id = 'child' AND base_template_name = 'private-base'"
    ))
    .fetch_optional(&db.pool)
    .await?;
    assert_eq!(grant, Some((owner,)));

    // A resubmission adds a fresh, unpinned grant of its own and leaves the
    // earlier submission's grant alone; grants past the longest validity are
    // swept on the way.
    let store = TemplateBuildJobsStore::new(db.pool.clone());
    let child = TemplateName::parse("child").unwrap();
    let base = TemplateName::parse("private-base").unwrap();
    sqlx::query(sql!(
        "UPDATE template_build_base_grants
         SET granted_at = NOW() - INTERVAL '2 hours'
         WHERE template_id = 'child'"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants
             (id, template_id, base_template_name, org_id, granted_at)
         VALUES ($1, 'stale-child', 'private-base', $2, NOW() - INTERVAL '8 days')"
    ))
    .bind(Uuid::now_v7())
    .bind(owner)
    .execute(&db.pool)
    .await?;
    let second = store
        .record_base_grant(OrgId::from_uuid(owner), &child, &base)
        .await?;
    let grants: Vec<(Uuid, bool, Option<Uuid>)> = sqlx::query_as(sql!(
        "SELECT id, granted_at > NOW() - INTERVAL '1 minute', builder_node_id
         FROM template_build_base_grants
         WHERE template_id = 'child'
         ORDER BY granted_at"
    ))
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(grants.len(), 2, "each submission keeps its own grant");
    assert!(!grants[0].1, "the earlier submission's grant keeps its age");
    assert_eq!(grants[1], (second.as_uuid(), true, None));
    let stale: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM template_build_base_grants WHERE template_id = 'stale-child'"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(stale, 0, "grants past the longest validity are swept");
    Ok(())
}

/// Once the `StartBuild` response names the accepting builder, that
/// submission's grant, and only that one, is pinned to it, so the job keeps
/// its base authorization however long it waits in that builder's queue. A
/// response without builder identity leaves the grant unpinned, a second
/// accepted build of the same target on another builder keeps its own pin
/// without disturbing the first, and a build from scratch pins nothing.
#[rstest]
#[tokio::test]
async fn accepted_build_pins_only_its_own_base_grant(
    #[future] template_controller_fixture: TestDbResult<TemplateControllerFixture>,
) -> TestDbResult<()> {
    use crate::template_build_jobs_store::BuildBaseGrantId;

    let fixture = template_controller_fixture.await?;
    let pool = &fixture._db.pool;
    insert_org_specific_template(pool, "private-base", TEST_ORG.as_uuid()).await?;
    let other_builder = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.2'::INET, 'builder', 'k8s', 'ready')"
    ))
    .bind(other_builder.as_uuid())
    .execute(pool)
    .await?;
    let child = TemplateName::parse("code-runtime").unwrap();
    let base = TemplateName::parse("private-base").unwrap();
    async fn pinned_builder(
        pool: &sqlx::PgPool,
        grant: BuildBaseGrantId,
    ) -> TestDbResult<Option<Uuid>> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT builder_node_id FROM template_build_base_grants WHERE id = $1"
        ))
        .bind(grant.as_uuid())
        .fetch_one(pool)
        .await?)
    }

    let first = fixture
        .store
        .record_base_grant(TEST_ORG, &child, &base)
        .await?;
    let (_, without_identity) = accepted_build(None);
    fixture
        .controller
        .record_template_build_response(
            TEST_ORG,
            child.clone(),
            None,
            without_identity,
            Some(first),
        )
        .await
        .unwrap();
    assert_eq!(pinned_builder(pool, first).await?, None);

    let (_, on_builder) = accepted_build(Some(fixture.builder_node_id));
    fixture
        .controller
        .record_template_build_response(TEST_ORG, child.clone(), None, on_builder, Some(first))
        .await
        .unwrap();
    assert_eq!(
        pinned_builder(pool, first).await?,
        Some(fixture.builder_node_id.as_uuid())
    );

    // A second build of the same target accepted elsewhere pins its own
    // grant and leaves the first build's pin in place.
    let second = fixture
        .store
        .record_base_grant(TEST_ORG, &child, &base)
        .await?;
    assert_eq!(pinned_builder(pool, second).await?, None);
    let (_, on_other_builder) = accepted_build(Some(other_builder));
    fixture
        .controller
        .record_template_build_response(
            TEST_ORG,
            child.clone(),
            None,
            on_other_builder,
            Some(second),
        )
        .await
        .unwrap();
    assert_eq!(
        pinned_builder(pool, second).await?,
        Some(other_builder.as_uuid())
    );
    assert_eq!(
        pinned_builder(pool, first).await?,
        Some(fixture.builder_node_id.as_uuid())
    );

    // A build from scratch carries no grant and touches none.
    let (_, from_scratch) = accepted_build(Some(other_builder));
    fixture
        .controller
        .record_template_build_response(TEST_ORG, child.clone(), None, from_scratch, None)
        .await
        .unwrap();
    assert_eq!(
        pinned_builder(pool, first).await?,
        Some(fixture.builder_node_id.as_uuid())
    );
    assert_eq!(
        pinned_builder(pool, second).await?,
        Some(other_builder.as_uuid())
    );
    Ok(())
}

struct TemplateControllerFixture {
    controller: TemplateController,
    store: TemplateBuildJobsStore,
    builder_node_id: NodeId,
    _db: TestDb,
}

#[fixture]
async fn template_controller_fixture() -> TestDbResult<TemplateControllerFixture> {
    let db = pg_test_db().await?;
    let builder_node_id = NodeId::new();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'builder', 'k8s', 'ready')"
    ))
    .bind(builder_node_id.as_uuid())
    .execute(&db.pool)
    .await?;
    let store = TemplateBuildJobsStore::new(db.pool.clone());
    // Direct is only a placeholder: the tests using this fixture never reach the substrate.
    let substrate = Substrate::Direct(Box::new(
        crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
    ));
    let controller = TemplateController::new(substrate, db.pool.clone());
    Ok(TemplateControllerFixture {
        controller,
        store,
        builder_node_id,
        _db: db,
    })
}

fn accepted_build(builder_node_id: Option<NodeId>) -> (BuildRunId, BuildTemplateResponse) {
    let job_id = BuildRunId::new();
    (
        job_id,
        BuildTemplateResponse {
            job_id: job_id.to_string(),
            builder_node_id: builder_node_id.map_or_else(String::new, |id| id.to_string()),
        },
    )
}

#[tokio::test(start_paused = true)]
async fn template_build_submission_retries_a_draining_builder() {
    let request = StartBuildRequest {
        template_id: "code-runtime".to_string(),
        ..Default::default()
    };
    let (_, accepted) = accepted_build(Some(NodeId::new()));
    let mut responses = VecDeque::from([
        Err(TemplateBuildAttemptError::Rpc(tonic::Status::unavailable(
            format!(
                "build-manager StartBuild: {}",
                pplx_space_build_mgr_client::BUILD_MANAGER_DRAINING_MESSAGE
            ),
        ))),
        Ok(accepted.clone()),
    ]);
    let mut attempted_requests = Vec::new();

    let actual = start_template_build_with_retry(request.clone(), |attempted_request| {
        attempted_requests.push(attempted_request);
        std::future::ready(responses.pop_front().unwrap())
    })
    .await
    .expect("a fresh builder should accept the build");

    assert_eq!(actual, accepted);
    assert_eq!(attempted_requests, vec![request.clone(), request]);
}

#[tokio::test]
async fn template_build_submission_does_not_retry_other_unavailable_errors() {
    let request = StartBuildRequest {
        template_id: "code-runtime".to_string(),
        ..Default::default()
    };
    let mut attempts = 0;

    let error = start_template_build_with_retry(request, |_| {
        attempts += 1;
        std::future::ready(Err(TemplateBuildAttemptError::Rpc(
            tonic::Status::unavailable("builder connection closed"),
        )))
    })
    .await
    .expect_err("an ambiguous unavailable response must not be retried");

    assert_eq!(attempts, 1);
    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
}

#[rstest]
#[tokio::test]
async fn accepted_build_without_builder_identity_records_unpinned_routing(
    #[future] template_controller_fixture: TestDbResult<TemplateControllerFixture>,
) -> TestDbResult<()> {
    let fixture = template_controller_fixture.await?;
    let template_name = TemplateName::parse("code-runtime").unwrap();
    let (job_id, response) = accepted_build(None);

    let actual = fixture
        .controller
        .record_template_build_response(TEST_ORG, template_name.clone(), None, response, None)
        .await
        .expect("an old Node API response should preserve the accepted build");
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &job_id.to_string())
        .await?;
    let builder_node = fixture.store.find_builder_node(job_id).await?;

    assert_eq!(actual.job_id, job_id.to_string());
    assert_eq!(job.map(|job| job.daemon_job_id), Some(job_id));
    assert_eq!(builder_node, None);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn accepted_build_with_builder_identity_records_routing(
    #[future] template_controller_fixture: TestDbResult<TemplateControllerFixture>,
) -> TestDbResult<()> {
    let fixture = template_controller_fixture.await?;
    let template_name = TemplateName::parse("code-runtime").unwrap();
    let (job_id, response) = accepted_build(Some(fixture.builder_node_id));

    let actual = fixture
        .controller
        .record_template_build_response(TEST_ORG, template_name.clone(), None, response, None)
        .await
        .unwrap();
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &job_id.to_string())
        .await?;
    let builder_node = fixture.store.find_builder_node(job_id).await?;

    assert_eq!(actual.job_id, job_id.to_string());
    assert_eq!(job.map(|job| job.daemon_job_id), Some(job_id));
    assert_eq!(builder_node, Some(fixture.builder_node_id));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn accepted_e2b_build_records_client_build_mapping(
    #[future] template_controller_fixture: TestDbResult<TemplateControllerFixture>,
) -> TestDbResult<()> {
    let fixture = template_controller_fixture.await?;
    let template_name = TemplateName::parse("code-runtime").unwrap();
    let client_build_id = BuildRunId::new().to_string();
    let (job_id, response) = accepted_build(Some(fixture.builder_node_id));

    fixture
        .controller
        .record_template_build_response(
            TEST_ORG,
            template_name.clone(),
            Some(&client_build_id),
            response,
            None,
        )
        .await?;
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &client_build_id)
        .await?;

    assert_eq!(job.map(|job| job.daemon_job_id), Some(job_id));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn accepted_build_with_invalid_builder_identity_is_rejected(
    #[future] template_controller_fixture: TestDbResult<TemplateControllerFixture>,
) -> TestDbResult<()> {
    let fixture = template_controller_fixture.await?;
    let template_name = TemplateName::parse("code-runtime").unwrap();
    let (job_id, mut response) = accepted_build(None);
    response.builder_node_id = "not-a-node-id".to_string();

    let actual = fixture
        .controller
        .record_template_build_response(TEST_ORG, template_name.clone(), None, response, None)
        .await
        .unwrap_err();
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &job_id.to_string())
        .await?;

    assert!(actual.message.contains("invalid node ID"));
    assert_eq!(job, None);
    Ok(())
}

#[rstest]
#[case::organization(None)]
#[case::seed(Some(SystemTemplate::Seed))]
#[case::code_runtime(Some(SystemTemplate::CodeRuntime))]
fn template_build_submission_maps_to_builder_request(#[case] system: Option<SystemTemplate>) {
    let org_id = Uuid::parse_str("018f4a92-5c2a-7a10-bf19-5ec77e1d8f9f").unwrap();
    let request = TemplateBuildSubmission {
        target: system.map_or_else(
            || TemplateBuildTarget::Organization(TemplateName::parse("custom").unwrap()),
            TemplateBuildTarget::System,
        ),
        client_build_id: Some("client-build-id".to_string()),
        recipe: build_proto::Recipe::default(),
        force: true,
        publish_tags: vec!["latest".to_string(), "v1".to_string()],
        client: ClientContext {
            source_path: "/workspace".to_string(),
            cli_version: "0.1.2".to_string(),
            hostname: "api-0".to_string(),
        },
        org_id: OrgId::from_uuid(org_id),
    }
    .into_start_build_request(None);

    assert_eq!(
        request.template_id,
        system.map_or("custom", SystemTemplate::as_str)
    );
    assert_eq!(request.org_id, system.is_none().then(|| org_id.to_string()));
    assert!(request.recipe.is_some());
    let options = request.options.unwrap();
    assert!(options.force);
    assert_eq!(options.publish_tags, vec!["latest", "v1"]);
    let client = request.client.unwrap();
    assert_eq!(client.source_path, "/workspace");
    assert_eq!(client.cli_version, "0.1.2");
    assert_eq!(client.hostname, "api-0");
}

#[test]
fn template_build_status_timeout_is_transient_rpc() {
    assert_eq!(
        template_build_status_transient_reason(&ControllerError::new(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            "deadline exceeded",
        )),
        Some(TemplateBuildStatusTransientReason::Rpc)
    );
}

#[test]
fn template_build_status_transport_error_is_transient_transport() {
    assert_eq!(
        template_build_status_transient_reason(&ControllerError::new(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "h2 protocol error: broken pipe",
        )),
        Some(TemplateBuildStatusTransientReason::Transport)
    );
}

/// A contended poll is transient, and reported as contention rather than as
/// the builder's RPC failing.
///
/// The distinction is not cosmetic here: the message a contention 503 carries
/// matches none of the transport fragments, so without its own arm it lands on
/// the `Rpc` catch-all and the log blames the builder node-api for this
/// service's own lock race — hiding an outcome the `lock_timeout` rollout
/// watches.
#[test]
fn template_build_status_contention_is_transient_contention() {
    assert_eq!(
        template_build_status_transient_reason(&ControllerError::lock_contention()),
        Some(TemplateBuildStatusTransientReason::Contention)
    );
    assert_eq!(
        TemplateBuildStatusTransientReason::Contention.as_str(),
        "lock_contention"
    );
}

#[test]
fn template_build_status_not_found_is_not_transient() {
    assert_eq!(
        template_build_status_transient_reason(&ControllerError::new(
            axum::http::StatusCode::NOT_FOUND,
            "build job not found",
        )),
        None
    );
}

/// A system build records its publication grant before dispatch and withdraws
/// it when dispatch fails, so a submission that never reached a builder
/// authorizes no publication; an organization build records none. Recording
/// sweeps stale grants, which is how this test observes that it happened.
#[tokio::test]
async fn system_build_withdraws_its_publication_grant_when_dispatch_fails() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let substrate = Substrate::Direct(Box::new(
        crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
    ));
    let controller = TemplateController::new(substrate, db.pool.clone());
    let insert_stale_grant = || {
        sqlx::query(sql!(
            "INSERT INTO template_system_publication_grants (id, template_name, granted_at)
             VALUES ($1, 'seed', NOW() - INTERVAL '8 days')"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
    };
    let grants = || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*) FROM template_system_publication_grants"
        ))
        .fetch_one(&db.pool)
    };
    let submission = |target| TemplateBuildSubmission {
        target,
        client_build_id: None,
        recipe: build_proto::Recipe::default(),
        force: false,
        publish_tags: vec![],
        client: ClientContext::default(),
        org_id: TEST_ORG,
    };

    insert_stale_grant().await?;
    controller
        .submit_template_build(submission(TemplateBuildTarget::Organization(
            TemplateName::parse("tenant-output").unwrap(),
        )))
        .await
        .expect_err("no builder node is registered");
    assert_eq!(grants().await?, 1, "an organization build records no grant");

    controller
        .submit_template_build(submission(TemplateBuildTarget::System(
            SystemTemplate::Seed,
        )))
        .await
        .expect_err("no builder node is registered");
    assert_eq!(
        grants().await?,
        0,
        "the system build swept the stale grant and withdrew its own"
    );
    Ok(())
}
