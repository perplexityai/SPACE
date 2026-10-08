use pplx_space_common_core::types::{NodeId, OrgId};

use std::convert::Infallible;

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{HeaderMap, Request, header},
    routing::post,
};
use http_body_util::StreamBody;
use hyper::body::Frame;
use pplx_space_db_test_fixtures::TestDb;
use pplx_space_node_api_client::proto::BuildTemplateResponse;
use prost::Message;
use rstest::{fixture, rstest};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::ServiceExt;

use super::*;
use crate::{routes::session_auth, user_role::UserRole};

struct NativeBuildFixture {
    db: TestDb,
    app: Router,
    org_id: OrgId,
    job_id: Uuid,
    builds: mpsc::Receiver<build_proto::StartBuildRequest>,
    shutdown: CancellationToken,
    tasks: TaskTracker,
}

impl Drop for NativeBuildFixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[fixture]
async fn native_build_fixture() -> TestResult<NativeBuildFixture> {
    let db = pg_test_db().await?;
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Build Owner', $2)"
    ))
    .bind(org_id)
    .bind(format!("build-owner-{org_id}"))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE clusters SET managing_org_id = $1 WHERE singleton"
    ))
    .bind(org_id)
    .execute(&db.pool)
    .await?;

    let node_id = NodeId::new();
    let job_id = Uuid::now_v7();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1'::INET, 'builder', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint)
         VALUES ($1, 'native-build-test', $2)"
    ))
    .bind(node_id)
    .bind(listener.local_addr()?.to_string())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
         (node_id, observed_at, stale_after, reachable, role, scheduling_status,
          health_healthy, services, updated_at)
         VALUES ($1, NOW(), NOW() + INTERVAL '1 hour', TRUE, 'builder', 'ready',
                 TRUE, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    // Only the builder RPC is simulated; HTTP auth, routing and job persistence are real.
    let (sender, builds) = mpsc::channel(1);
    let grant_pool = db.pool.clone();
    let builder = Router::new().route(
        "/space.node.api.v1.BuilderApi/BuildTemplate",
        post(move |body: Bytes| {
            let sender = sender.clone();
            let grant_pool = grant_pool.clone();
            async move {
                assert_eq!(body[0], 0, "the build RPC must be uncompressed");
                let request = build_proto::StartBuildRequest::decode(&body[5..]).unwrap();
                // A fast builder can publish before returning StartBuild. Observe
                // the actual grant here, while dispatch is still in progress.
                if request.org_id.is_none() {
                    let grant = request
                        .system_publication_grant_id
                        .as_deref()
                        .expect("system build carries its pre-dispatch grant")
                        .parse::<Uuid>()
                        .unwrap();
                    let target: String = sqlx::query_scalar(sql!(
                        "SELECT template_name FROM template_system_publication_grants WHERE id = $1"
                    ))
                    .bind(grant)
                    .fetch_one(&grant_pool)
                    .await
                    .unwrap();
                    assert_eq!(target, request.template_id);
                } else {
                    assert!(request.system_publication_grant_id.is_none());
                }
                sender.send(request).await.unwrap();
                let response = BuildTemplateResponse {
                    job_id: job_id.to_string(),
                    builder_node_id: node_id.to_string(),
                }
                .encode_to_vec();
                let mut frame = vec![0];
                frame.extend_from_slice(&(u32::try_from(response.len()).unwrap()).to_be_bytes());
                frame.extend_from_slice(&response);
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                (
                    [(header::CONTENT_TYPE, "application/grpc")],
                    Body::new(StreamBody::new(tokio_stream::iter([
                        Ok::<_, Infallible>(Frame::data(Bytes::from(frame))),
                        Ok(Frame::trailers(trailers)),
                    ]))),
                )
            }
        }),
    );
    let shutdown = CancellationToken::new();
    let tasks = TaskTracker::new();
    let server_shutdown = shutdown.clone();
    tasks.spawn(async move {
        axum::serve(listener, builder)
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    let state = AppState::from_pools_with_lifecycle(
        db.pool.clone(),
        pplx_space_common_svc::sql::CredentialsPool::shared(db.pool.clone()),
        pplx_space_common_svc::DrainSignal::new(),
        shutdown.clone(),
        tasks.clone(),
        None,
    )
    .await?;
    let app = session_auth::router(state.clone())
        .merge(crate::routes::admin::shared_host_router(state.clone()))
        .with_state(state);
    Ok(NativeBuildFixture {
        db,
        app,
        org_id,
        job_id,
        builds,
        shutdown,
        tasks,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateOwnership {
    Missing,
    Unknown,
    Caller,
    OtherOrg,
}

#[rstest]
#[case::managing_org_admin(
    UserRole::OrganizationAdmin,
    true,
    false,
    TemplateOwnership::Missing,
    StatusCode::ACCEPTED
)]
#[case::managing_org_member(
    UserRole::Member,
    true,
    false,
    TemplateOwnership::Missing,
    StatusCode::FORBIDDEN
)]
#[case::other_org_admin(
    UserRole::OrganizationAdmin,
    false,
    false,
    TemplateOwnership::Missing,
    StatusCode::FORBIDDEN
)]
#[case::expired_session(
    UserRole::OrganizationAdmin,
    true,
    true,
    TemplateOwnership::Missing,
    StatusCode::UNAUTHORIZED
)]
#[case::owned_template(
    UserRole::OrganizationAdmin,
    true,
    false,
    TemplateOwnership::Caller,
    StatusCode::ACCEPTED
)]
#[case::unowned_template(
    UserRole::OrganizationAdmin,
    true,
    false,
    TemplateOwnership::Unknown,
    StatusCode::ACCEPTED
)]
#[case::foreign_template(
    UserRole::OrganizationAdmin,
    true,
    false,
    TemplateOwnership::OtherOrg,
    StatusCode::CONFLICT
)]
#[tokio::test]
async fn native_build_session_ownership(
    #[future] native_build_fixture: TestResult<NativeBuildFixture>,
    #[values(None, Some(SystemTemplate::Seed), Some(SystemTemplate::CodeRuntime))] system: Option<
        SystemTemplate,
    >,
    #[case] role: UserRole,
    #[case] managing_org: bool,
    #[case] expired: bool,
    #[case] template_ownership: TemplateOwnership,
    #[case] expected_status: StatusCode,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut fixture = native_build_fixture.await?;
    let template = system.map_or("native-session-build", SystemTemplate::as_str);
    let seed_hash = format!("sha256:{}", "a".repeat(64));
    if system == Some(SystemTemplate::CodeRuntime) {
        insert_template(&fixture.db.pool, &TemplateName::parse("seed")?).await?;
        sqlx::query(sql!(
            "INSERT INTO template_releases (template_name, type, hash)
             VALUES ('seed', 'fc', $1)"
        ))
        .bind(&seed_hash)
        .execute(&fixture.db.pool)
        .await?;
    }
    let user_org = if managing_org {
        fixture.org_id
    } else {
        OrgId::from_uuid(pplx_space_db_test_fixtures::SEED_ORG_ID)
    };
    let template_owner = match template_ownership {
        TemplateOwnership::Missing => None,
        TemplateOwnership::Unknown => Some(None),
        TemplateOwnership::Caller => Some(Some(user_org)),
        TemplateOwnership::OtherOrg => Some(Some(OrgId::from_uuid(
            pplx_space_db_test_fixtures::SEED_ORG_ID,
        ))),
    };
    if let Some(owner) = template_owner {
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id)
             VALUES ($1, $2, $3)"
        ))
        .bind(Uuid::now_v7())
        .bind(template)
        .bind(owner)
        .execute(&fixture.db.pool)
        .await?;
    }
    let password_hash =
        tokio::task::spawn_blocking(|| crate::auth::hash_password("build-password")).await??;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, 'native-builder@example.com', $2, 'Native Builder', $3, $4)"
    ))
    .bind(Uuid::now_v7())
    .bind(password_hash)
    .bind(user_org)
    .bind(role)
    .execute(&fixture.db.pool)
    .await?;
    let login = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/auth/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "email": "native-builder@example.com", "password": "build-password"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(login.status(), StatusCode::OK);
    let login: serde_json::Value =
        serde_json::from_slice(&to_bytes(login.into_body(), usize::MAX).await?)?;
    let token = login["token"]
        .as_str()
        .expect("login returns a session token");
    if expired {
        sqlx::query(sql!(
            "UPDATE sessions SET expires_at = NOW() - INTERVAL '1 second' WHERE token_hash = $1"
        ))
        .bind(crate::auth::hash_token(token))
        .execute(&fixture.db.pool)
        .await?;
    }

    if let Some(template) = system
        && expected_status == StatusCode::ACCEPTED
    {
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::post(format!("/admin/api/v1/templates/{template}/builds"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        scratch_template_build_request_json(KERNEL_VERSION, KERNEL_DIGEST)
                            .to_string(),
                    ))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            fixture.builds.try_recv().is_err(),
            "reserved names must not reach the builder through ordinary builds"
        );
    }

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(match system {
                Some(template) => format!("/admin/api/v1/system-templates/{template}/builds"),
                None => "/admin/api/v1/templates/native-session-build/builds".to_string(),
            })
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(match system {
                Some(SystemTemplate::Seed) => "{}".to_string(),
                Some(SystemTemplate::CodeRuntime) => {
                    serde_json::json!({"base": format!("seed@{seed_hash}")}).to_string()
                }
                None => {
                    scratch_template_build_request_json(KERNEL_VERSION, KERNEL_DIGEST).to_string()
                }
            }))?,
        )
        .await?;

    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let build = fixture.builds.try_recv().ok();
    if expected_status != StatusCode::ACCEPTED {
        assert!(
            build.is_none(),
            "rejected builds must not reach the builder"
        );
    }
    assert_eq!(
        status,
        expected_status,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let jobs: Vec<(Option<OrgId>, String)> = sqlx::query_as(sql!(
        "SELECT org_id, build_id FROM template_build_jobs WHERE template_id = $1"
    ))
    .bind(template)
    .fetch_all(&fixture.db.pool)
    .await?;
    if expected_status == StatusCode::ACCEPTED {
        let request = build.expect("accepted build must reach the builder");
        assert_eq!(
            request.org_id,
            system.is_none().then(|| user_org.to_string())
        );
        if let Some(template) = system {
            let grant = request
                .system_publication_grant_id
                .as_deref()
                .expect("system build grant")
                .parse::<Uuid>()?;
            let retained: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (SELECT 1 FROM template_system_publication_grants WHERE id = $1)"
            ))
            .bind(grant)
            .fetch_one(&fixture.db.pool)
            .await?;
            assert!(
                retained,
                "successful dispatch retains its publication grant"
            );
            assert_eq!(request.template_id, template.as_str());
            assert!(request.options.as_ref().unwrap().publish_tags.is_empty());
            let Some(recipe) = request.recipe else {
                panic!("inline embedded recipe")
            };
            let origin = recipe.origin.and_then(|origin| origin.kind);
            match template {
                SystemTemplate::Seed => assert!(matches!(
                    origin,
                    Some(build_proto::recipe::origin::Kind::PinnedFromScratch(_))
                )),
                SystemTemplate::CodeRuntime => {
                    let Some(build_proto::recipe::origin::Kind::FromTemplate(base)) = origin else {
                        panic!("code-runtime must build from seed")
                    };
                    assert_eq!(base.base, format!("seed@{seed_hash}"));
                }
            }
        }
        assert_eq!(jobs, vec![(Some(user_org), fixture.job_id.to_string())]);
        let response: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(response["job_id"], fixture.job_id.to_string());
    } else {
        assert!(jobs.is_empty(), "rejected builds must not create job rows");
    }
    fixture.shutdown.cancel();
    fixture.tasks.close();
    fixture.tasks.wait().await;
    fixture.db.pool.close().await;
    Ok(())
}
