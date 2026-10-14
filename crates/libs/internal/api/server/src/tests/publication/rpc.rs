use super::*;
use crate::proto::{cluster_api_client::ClusterApiClient, cluster_api_server::ClusterApiServer};
use pplx_space_common_svc::auth::{
    self,
    build::BuildAuthLayer,
    test_support::{
        TokenReviewStatus, kubeconfig_for_endpoint, projected_token, serve_token_reviews,
        service_account_status,
    },
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

fn token(account: &str) -> String {
    static TOKENS: std::sync::OnceLock<std::collections::BTreeMap<&'static str, String>> =
        std::sync::OnceLock::new();
    TOKENS
        .get_or_init(|| {
            [
                "workers",
                "space-api",
                "space-build-manager",
                "space-template-publish",
            ]
            .map(|name| (name, projected_token(name)))
            .into()
        })
        .get(account)
        .unwrap()
        .clone()
}

fn request<T>(body: T, account: &str) -> Request<T> {
    let mut req = Request::new(body);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token(account)).parse().unwrap(),
    );
    req
}

fn registration(name: &str, org: Option<Uuid>) -> RegisterTemplateRequest {
    RegisterTemplateRequest {
        system_publication_grant_id: None,
        tenant_publication_grant_id: None,
        name: name.into(),
        description: None,
        org_id: org.map(|v| v.to_string()),
    }
}

fn release(name: &str, org: Option<Uuid>) -> RegisterTemplateReleaseRequest {
    RegisterTemplateReleaseRequest {
        system_publication_grant_id: None,
        tenant_publication_grant_id: None,
        template_name: name.into(),
        org_id: org.map(|v| v.to_string()),
        runtime: "fc".into(),
        hash: format!("sha256:{}", "a".repeat(64)),
        tags: vec!["latest".into()],
        storage_connection_strings: vec![],
        execution: None,
    }
}

/// What the API records before it dispatches a system template build.
async fn grant_system_publication(pool: &sqlx::PgPool, template: &str) -> TestResult<Uuid> {
    let grant = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name) VALUES ($1, $2)"
    ))
    .bind(grant)
    .bind(template)
    .execute(pool)
    .await?;
    Ok(grant)
}

async fn system_publication_grants(pool: &sqlx::PgPool, template: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT count(*) FROM template_system_publication_grants WHERE template_name = $1"
    ))
    .bind(template)
    .fetch_one(pool)
    .await?)
}

/// Run `scenario` in a child process whose ClusterApi authenticates through a
/// local TokenReview server.
fn in_publication_process<F>(scenario: impl FnOnce() -> F) -> TestResult
where
    F: std::future::Future<Output = TestResult>,
{
    let env = [
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true"),
        // This table exercises authorization, not the separately tested rate limiter.
        ("SPACE_K8S_TOKEN_AUTH_REVIEW_RATE_PER_SEC", "1000"),
        ("SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED", "false"),
        ("KUBERNETES_SERVICE_HOST", "127.0.0.1"),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space"),
        ("SPACE_CLUSTER_API_AUTH_AUDIENCE", "space-cluster-api"),
        (
            "SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS",
            "workers,space-api,space-build-manager,space-template-publish",
        ),
        (
            "SPACE_TEMPLATE_PUBLISH_SYSTEM_SERVICE_ACCOUNTS",
            "space-build-manager,space-template-publish",
        ),
        (
            "SPACE_TEMPLATE_PUBLISH_TENANT_SERVICE_ACCOUNTS",
            "space-build-manager",
        ),
        ("SPACE_TEMPLATE_PUBLISH_RESERVED_NAMES", "operator-custom"),
    ];
    let directory = tempfile::tempdir()?;
    let kubeconfig = directory.path().join("kubeconfig");
    let mut vars: Vec<_> = env
        .iter()
        .map(|(key, value)| (*key, Some(std::ffi::OsStr::new(value))))
        .collect();
    vars.push(("KUBECONFIG", Some(kubeconfig.as_os_str())));
    if !pplx_space_test_env::enter_test_process(&vars) {
        return Ok(());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(scenario())
}

/// A ClusterApi behind the production auth layers, reached from 127.0.0.1.
struct PublicationServer {
    db: pplx_space_db_test_fixtures::TestDb,
    client: ClusterApiClient<tonic::transport::Channel>,
    shutdown: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    reviewer: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl PublicationServer {
    async fn start() -> TestResult<Self> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let config = std::path::PathBuf::from(
            std::env::var_os("KUBECONFIG").expect("test child kubeconfig path"),
        );
        std::fs::write(&config, kubeconfig_for_endpoint(listener.local_addr()?))?;
        let shutdown = CancellationToken::new();
        // The review response, not the unverified bearer claims, establishes identity.
        let tokens = [
            "workers",
            "space-api",
            "space-build-manager",
            "space-template-publish",
        ]
        .map(|name| (token(name), name));
        let reviewer = tokio::spawn(serve_token_reviews(
            listener,
            move |review| match tokens
                .iter()
                .find(|(token, _)| review.token.as_deref() == Some(token.as_str()))
            {
                Some((_, name)) => reviewed_publisher(name),
                None => TokenReviewStatus {
                    authenticated: Some(false),
                    ..Default::default()
                },
            },
            shutdown.clone(),
        ));
        let service_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(
                (*db.pool.connect_options())
                    .clone()
                    .options([("lock_timeout", "100ms")]),
            )
            .await?;
        let mut service = ClusterApiService::new(service_pool);
        service.reserved_template_names =
            crate::template_publication::ReservedTemplateNames::from_env()?;
        let outer = auth::NodeApiAuthLayer::from_config(auth::NodeApiAuthConfig::from_env(
            auth::NodeRole::Cluster,
            "shared-token".into(),
            None,
        )?)
        .await?;
        let publisher = BuildAuthLayer::for_template_publication("shared-token".into()).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(outer)
                .layer(publisher)
                .add_service(ClusterApiServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_shutdown.cancelled_owned(),
                )
                .await
        });
        let client = ClusterApiClient::connect(endpoint).await?;
        Ok(Self {
            db,
            client,
            shutdown,
            server,
            reviewer,
        })
    }

    async fn finish(self) -> TestResult {
        drop(self.client);
        self.shutdown.cancel();
        self.server.await??;
        self.reviewer.await??;
        drop(self.db);
        Ok(())
    }
}

#[test]
fn authenticated_publication_enforces_identity_owner_and_reserved_outputs() -> TestResult {
    in_publication_process(run)
}

async fn run() -> TestResult {
    let mut fixture = PublicationServer::start().await?;
    let db = &fixture.db;
    let client = &mut fixture.client;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000002")?;
    let foreign = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    for account in ["workers", "space-api"] {
        for org in [None, Some(owner)] {
            assert_eq!(
                client
                    .register_template(request(registration("tenant-output", org), account))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
            assert_eq!(
                client
                    .register_template_release(request(release("tenant-output", org), account))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
        }
    }
    client
        .list_nodes(request(
            ListNodesRequest {
                role: None,
                include_deleted: false,
            },
            "workers",
        ))
        .await?;
    for account in ["workers", "space-api", "space-build-manager"] {
        assert_eq!(
            client
                .create_system_template_publication_grant(request(
                    CreateSystemTemplatePublicationGrantRequest {
                        template_name: "direct-custom".into()
                    },
                    account,
                ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    let operator_grant = client
        .create_system_template_publication_grant(request(
            CreateSystemTemplatePublicationGrantRequest {
                template_name: "direct-custom".into(),
            },
            "space-template-publish",
        ))
        .await?
        .into_inner()
        .grant_id;
    let mut direct_template = registration("direct-custom", None);
    direct_template.system_publication_grant_id = Some(operator_grant.clone());
    client
        .register_template(request(direct_template, "space-build-manager"))
        .await?;
    let mut direct_release = release("direct-custom", None);
    direct_release.system_publication_grant_id = Some(operator_grant);
    client
        .register_template_release(request(direct_release, "space-build-manager"))
        .await?;
    assert_eq!(
        system_publication_grants(&db.pool, "direct-custom").await?,
        0
    );
    // Build-manager also publishes tenant templates, so it publishes a system
    // template only for an independently authorized build. The deploy-time
    // publisher needs no grant.
    assert_eq!(
        client
            .register_template(request(
                registration("code-runtime", None),
                "space-build-manager"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .register_template_release(request(
                release("code-runtime", None),
                "space-build-manager"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let grant = grant_system_publication(&db.pool, "code-runtime").await?;
    for (name, publisher) in [
        ("seed", "space-template-publish"),
        ("code-runtime", "space-build-manager"),
        ("operator-custom", "space-template-publish"),
    ] {
        assert_eq!(
            client
                .register_template(request(
                    registration(name, Some(owner)),
                    "space-build-manager"
                ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let mut template = registration(name, None);
        let mut publication = release(name, None);
        if publisher == "space-build-manager" {
            template.system_publication_grant_id = Some(grant.to_string());
            publication.system_publication_grant_id = Some(grant.to_string());
        }
        client
            .register_template(request(template, publisher))
            .await?;
        client
            .register_template_release(request(publication, publisher))
            .await?;
    }
    // One grant covers one release: a second release, an expired grant, or a
    // grant for another template authorizes nothing.
    assert_eq!(
        system_publication_grants(&db.pool, "code-runtime").await?,
        0
    );
    let expired = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name, granted_at)
         VALUES ($1, 'code-runtime', NOW() - INTERVAL '8 days')"
    ))
    .bind(expired)
    .execute(&db.pool)
    .await?;
    let other_template = grant_system_publication(&db.pool, "seed").await?;
    for (proof, expected) in [
        (None, Code::PermissionDenied),
        (Some(grant.to_string()), Code::PermissionDenied),
        (Some(expired.to_string()), Code::PermissionDenied),
        (Some(other_template.to_string()), Code::PermissionDenied),
        (Some(Uuid::now_v7().to_string()), Code::PermissionDenied),
        (Some("invalid-grant".to_string()), Code::InvalidArgument),
    ] {
        let mut template = registration("code-runtime", None);
        template.system_publication_grant_id = proof.clone();
        let mut second = release("code-runtime", None);
        second.hash = format!("sha256:{}", "c".repeat(64));
        second.system_publication_grant_id = proof;
        assert_eq!(
            client
                .register_template(request(template, "space-build-manager"))
                .await
                .unwrap_err()
                .code(),
            expected
        );
        assert_eq!(
            client
                .register_template_release(request(second, "space-build-manager"))
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    sqlx::query(sql!("DELETE FROM template_system_publication_grants"))
        .execute(&db.pool)
        .await?;
    // B publishes while A is still dispatching. Withdrawing A afterward must
    // leave no unused publication authority from B.
    let grant_a = Uuid::now_v7();
    let grant_b = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name, granted_at)
         VALUES ($1, 'code-runtime', NOW() - INTERVAL '2 seconds'),
                ($2, 'code-runtime', NOW() - INTERVAL '1 second')"
    ))
    .bind(grant_a)
    .bind(grant_b)
    .execute(&db.pool)
    .await?;
    let mut publication_b = release("code-runtime", None);
    publication_b.system_publication_grant_id = Some(grant_b.to_string());
    client
        .register_template_release(request(publication_b.clone(), "space-build-manager"))
        .await?;
    let remaining: Vec<Uuid> = sqlx::query_scalar(sql!(
        "SELECT id FROM template_system_publication_grants WHERE template_name = 'code-runtime'"
    ))
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(remaining, vec![grant_a], "B must consume its own grant");
    // A's still-valid grant cannot authorize a replay of B.
    assert_eq!(
        client
            .register_template_release(request(publication_b.clone(), "space-build-manager"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    sqlx::query(sql!(
        "DELETE FROM template_system_publication_grants WHERE id = $1"
    ))
    .bind(grant_a)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        system_publication_grants(&db.pool, "code-runtime").await?,
        0
    );
    assert_eq!(
        client
            .register_template_release(request(publication_b, "space-build-manager"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let rollback_grant = grant_system_publication(&db.pool, "rollback-output").await?;
    let mut rollback_release = release("rollback-output", None);
    rollback_release.system_publication_grant_id = Some(rollback_grant.to_string());
    assert_eq!(
        client
            .register_template_release(request(rollback_release.clone(), "space-build-manager"))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        system_publication_grants(&db.pool, "rollback-output").await?,
        1
    );
    let mut rollback_template = registration("rollback-output", None);
    rollback_template.system_publication_grant_id = Some(rollback_grant.to_string());
    client
        .register_template(request(rollback_template, "space-build-manager"))
        .await?;
    let mut concurrent_client = client.clone();
    let (first, replay) = tokio::join!(
        client.register_template_release(request(rollback_release.clone(), "space-build-manager")),
        concurrent_client
            .register_template_release(request(rollback_release, "space-build-manager")),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(replay.is_ok()), 1);
    assert_eq!(
        first.err().or_else(|| replay.err()).unwrap().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        system_publication_grants(&db.pool, "rollback-output").await?,
        0
    );
    // Tenant publication runs under the grant for the build the API
    // dispatched, from the builder that took it.
    insert_publishing_builder(&db.pool).await?;
    let tenant_grant =
        grant_tenant_publication(&db.pool, "tenant-output", owner, None, "0 seconds").await?;
    client
        .register_template(request(
            tenant_registration("tenant-output", owner, Some(tenant_grant.to_string())),
            "space-build-manager",
        ))
        .await?;
    client
        .register_template_release(request(
            RegisterTemplateReleaseRequest {
                tenant_publication_grant_id: Some(tenant_grant.to_string()),
                ..release("tenant-output", Some(owner))
            },
            "space-build-manager",
        ))
        .await?;
    // Registration derives launch scope from the owner: a tenant build must
    // not be usable by other orgs, while a system publication stays generic.
    let visibilities: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, visibility FROM templates WHERE name IN ('seed', 'tenant-output') ORDER BY name",
    )
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        visibilities,
        vec![
            ("seed".to_string(), "generic".to_string()),
            ("tenant-output".to_string(), "org_specific".to_string()),
        ]
    );
    for org in [None, Some(foreign)] {
        assert_eq!(
            client
                .register_template(request(
                    registration("tenant-output", org),
                    "space-build-manager"
                ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let mut mutation = release("tenant-output", org);
        mutation.hash = format!("sha256:{}", "b".repeat(64));
        assert_eq!(
            client
                .register_template_release(request(mutation, "space-build-manager"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    assert_eq!(
        client
            .register_template(request(
                registration("deploy-tenant", Some(owner)),
                "space-template-publish"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    // A legacy release with mismatched ownership cannot be adopted by
    // publishing, even under a build's grant.
    sqlx::query(sql!(
        "UPDATE template_releases SET org_id=NULL WHERE template_name='tenant-output'"
    ))
    .execute(&db.pool)
    .await?;
    let adoption_grant =
        grant_tenant_publication(&db.pool, "tenant-output", owner, None, "0 seconds").await?;
    assert_eq!(
        client
            .register_template_release(request(
                RegisterTemplateReleaseRequest {
                    tenant_publication_grant_id: Some(adoption_grant.to_string()),
                    ..release("tenant-output", Some(owner))
                },
                "space-build-manager"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let release_owner: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT org_id FROM template_releases WHERE template_name='tenant-output'"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(release_owner, None);
    let managing_org: Uuid =
        sqlx::query_scalar(sql!("SELECT managing_org_id FROM clusters WHERE singleton"))
            .fetch_one(&db.pool)
            .await?;
    sqlx::query(sql!("UPDATE templates SET org_id=$1 WHERE name='seed'"))
        .bind(managing_org)
        .execute(&db.pool)
        .await?;
    // Managing-org membership is not operator authority; authenticated operator
    // re-registration preserves this legacy association rather than adopting it.
    assert_eq!(
        client
            .register_template(request(
                registration("seed", Some(managing_org)),
                "space-build-manager"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let grant = grant_system_publication(&db.pool, "seed").await?;
    let mut template = registration("seed", None);
    template.system_publication_grant_id = Some(grant.to_string());
    client
        .register_template(request(template, "space-build-manager"))
        .await?;
    // Registering the template leaves the grant for the build's release.
    assert_eq!(system_publication_grants(&db.pool, "seed").await?, 1);
    sqlx::query(sql!("UPDATE templates SET org_id=$1 WHERE name='seed'"))
        .bind(foreign)
        .execute(&db.pool)
        .await?;
    assert_eq!(
        client
            .register_template(request(
                registration("seed", None),
                "space-template-publish"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let (stored_owner, hash): (Uuid,String) = sqlx::query_as(sql!("SELECT t.org_id, r.hash FROM templates t JOIN template_release_tags tag ON tag.template_name=t.name JOIN template_releases r ON r.id=tag.template_release_id WHERE t.name='tenant-output' AND tag.tag='latest'")).fetch_one(&db.pool).await?;
    assert_eq!(stored_owner, owner);
    assert_eq!(hash, release("tenant-output", Some(owner)).hash);
    fixture.finish().await
}

/// What the API records before it dispatches an organization's build, and
/// the pin it records from the `StartBuild` response.
async fn grant_tenant_publication(
    pool: &sqlx::PgPool,
    template: &str,
    org: Uuid,
    builder: Option<Uuid>,
    age: &str,
) -> TestResult<Uuid> {
    let grant = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_tenant_publication_grants
             (id, template_name, org_id, builder_node_id, granted_at)
         VALUES ($1, $2, $3, $4, NOW() - $5::INTERVAL)"
    ))
    .bind(grant)
    .bind(template)
    .bind(org)
    .bind(builder)
    .bind(age)
    .execute(pool)
    .await?;
    Ok(grant)
}

async fn tenant_publication_grant_exists(pool: &sqlx::PgPool, grant: Uuid) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM template_tenant_publication_grants WHERE id = $1)"
    ))
    .bind(grant)
    .fetch_one(pool)
    .await?)
}

fn tenant_registration(name: &str, org: Uuid, grant: Option<String>) -> RegisterTemplateRequest {
    RegisterTemplateRequest {
        tenant_publication_grant_id: grant,
        ..registration(name, Some(org))
    }
}

fn tenant_release(
    name: &str,
    org: Uuid,
    hash: char,
    grant: Option<String>,
) -> RegisterTemplateReleaseRequest {
    RegisterTemplateReleaseRequest {
        hash: format!("sha256:{}", hash.to_string().repeat(64)),
        tenant_publication_grant_id: grant,
        ..release(name, Some(org))
    }
}

/// The codes build-manager's identity gets for registering the template and
/// then its release with this grant.
async fn publish_as_build_manager(
    client: &mut ClusterApiClient<tonic::transport::Channel>,
    name: &str,
    org: Uuid,
    hash: char,
    grant: Option<String>,
) -> (Code, Code) {
    let template = client
        .register_template(request(
            tenant_registration(name, org, grant.clone()),
            "space-build-manager",
        ))
        .await;
    let release = client
        .register_template_release(request(
            tenant_release(name, org, hash, grant),
            "space-build-manager",
        ))
        .await;
    (status_code(template), status_code(release))
}

fn status_code<T>(result: Result<T, tonic::Status>) -> Code {
    result.map_or_else(|status| status.code(), |_| Code::Ok)
}

async fn latest_hash(pool: &sqlx::PgPool, template: &str) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT r.hash FROM template_release_tags t
         JOIN template_releases r ON r.id = t.template_release_id
         WHERE t.template_name = $1 AND t.tag = 'latest'"
    ))
    .bind(template)
    .fetch_optional(pool)
    .await?)
}

#[test]
fn tenant_publication_requires_a_registered_builder() -> TestResult {
    in_publication_process(|| async {
        let mut fixture = PublicationServer::start().await?;
        let pool = fixture.db.pool.clone();
        let client = &mut fixture.client;
        let tenant = Uuid::from_u128(3);
        let denied = (Code::PermissionDenied, Code::PermissionDenied);
        // No build exists for this target or organization.
        assert_eq!(
            publish_as_build_manager(client, "undispatched", tenant, 'a', None).await,
            denied
        );
        let created: i64 = sqlx::query_scalar(sql!(
            "SELECT count(*) FROM templates WHERE name = 'undispatched'"
        ))
        .fetch_one(&pool)
        .await?;
        assert_eq!(created, 0);

        // The organization's existing template keeps its `latest` release.
        let service = ClusterApiService::new(pool.clone());
        service
            .register_template(publisher_request(registration("owned", Some(tenant))))
            .await?;
        service
            .register_template_release(publisher_request(release("owned", Some(tenant))))
            .await?;
        assert_eq!(
            client
                .register_template_release(request(
                    tenant_release("owned", tenant, 'd', None),
                    "space-build-manager",
                ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            latest_hash(&pool, "owned").await?,
            Some(release("owned", Some(tenant)).hash)
        );

        // A fresh, unpinned grant serves registered builders only. The caller
        // connects from 127.0.0.1: first unregistered, then a worker.
        let grant =
            grant_tenant_publication(&pool, "tenant-out", tenant, None, "0 seconds").await?;
        assert_eq!(
            publish_as_build_manager(client, "tenant-out", tenant, 'a', Some(grant.to_string()))
                .await,
            denied
        );
        insert_k8s_node(&pool, std::net::Ipv4Addr::LOCALHOST, "worker").await?;
        assert_eq!(
            publish_as_build_manager(client, "tenant-out", tenant, 'a', Some(grant.to_string()))
                .await,
            denied
        );
        fixture.finish().await
    })
}

#[derive(Clone, Copy, Debug)]
enum InvalidGrant {
    WrongOrg,
    WrongTarget,
    ExpiredUnpinned,
    ExpiredPinned,
    WrongBuilder,
    Forged,
    Malformed,
    DeletedBuilder,
    StaleNodeUid,
    WrongNodeName,
    DeletedNode,
}

#[rstest::rstest]
#[case::wrong_org(InvalidGrant::WrongOrg, Code::PermissionDenied)]
#[case::wrong_target(InvalidGrant::WrongTarget, Code::PermissionDenied)]
#[case::expired_unpinned(InvalidGrant::ExpiredUnpinned, Code::PermissionDenied)]
#[case::expired_pinned(InvalidGrant::ExpiredPinned, Code::PermissionDenied)]
#[case::wrong_builder(InvalidGrant::WrongBuilder, Code::PermissionDenied)]
#[case::forged(InvalidGrant::Forged, Code::PermissionDenied)]
#[case::malformed(InvalidGrant::Malformed, Code::InvalidArgument)]
#[case::deleted_builder(InvalidGrant::DeletedBuilder, Code::PermissionDenied)]
#[case::stale_node_uid(InvalidGrant::StaleNodeUid, Code::PermissionDenied)]
#[case::wrong_node_name(InvalidGrant::WrongNodeName, Code::PermissionDenied)]
#[case::deleted_node(InvalidGrant::DeletedNode, Code::PermissionDenied)]
fn tenant_publication_rejects_invalid_grants(
    #[case] invalid: InvalidGrant,
    #[case] code: Code,
) -> TestResult {
    in_publication_process(|| async move {
        let mut fixture = PublicationServer::start().await?;
        let pool = &fixture.db.pool;
        let tenant = Uuid::from_u128(3);
        let builder = insert_publishing_builder(pool).await?;
        let (name, owner, pinned, age) = match invalid {
            InvalidGrant::WrongOrg => ("tenant-out", Uuid::from_u128(2), None, "0 seconds"),
            InvalidGrant::WrongTarget => ("other-target", tenant, None, "0 seconds"),
            InvalidGrant::ExpiredUnpinned => ("tenant-out", tenant, None, "2 hours"),
            InvalidGrant::ExpiredPinned => ("tenant-out", tenant, Some(builder), "8 days"),
            InvalidGrant::WrongBuilder | InvalidGrant::DeletedBuilder => {
                let other =
                    insert_k8s_node(pool, std::net::Ipv4Addr::new(10, 0, 0, 41), "builder").await?;
                ("tenant-out", tenant, Some(other), "0 seconds")
            }
            InvalidGrant::Forged
            | InvalidGrant::Malformed
            | InvalidGrant::StaleNodeUid
            | InvalidGrant::WrongNodeName
            | InvalidGrant::DeletedNode => ("tenant-out", tenant, None, "0 seconds"),
        };
        let grant = grant_tenant_publication(pool, name, owner, pinned, age).await?;
        match invalid {
            InvalidGrant::StaleNodeUid => {
                sqlx::query(sql!(
                    "UPDATE node_k8s SET k8s_node_uid = 'replacement-node-uid' WHERE node_id = $1"
                ))
                .bind(builder)
                .execute(pool)
                .await?;
                // A matching transport address must not rescue stale reviewed identity.
                sqlx::query(sql!(
                    "UPDATE nodes SET address = '127.0.0.1'::INET WHERE id = $1"
                ))
                .bind(builder)
                .execute(pool)
                .await?;
            }
            InvalidGrant::WrongNodeName => {
                sqlx::query(sql!(
                    "UPDATE node_k8s SET k8s_node_name = 'other-builder' WHERE node_id = $1"
                ))
                .bind(builder)
                .execute(pool)
                .await?;
            }
            InvalidGrant::DeletedNode => {
                sqlx::query(sql!("UPDATE nodes SET state = 'deleted' WHERE id = $1"))
                    .bind(builder)
                    .execute(pool)
                    .await?;
            }
            _ => {}
        }

        let control = if matches!(invalid, InvalidGrant::DeletedBuilder) {
            Some(
                grant_tenant_publication(pool, "control-out", tenant, Some(builder), "0 seconds")
                    .await?,
            )
        } else {
            None
        };
        let proof = match invalid {
            InvalidGrant::Forged => Uuid::now_v7().to_string(),
            InvalidGrant::Malformed => "invalid".into(),
            InvalidGrant::DeletedBuilder => {
                sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
                    .bind(pinned)
                    .execute(pool)
                    .await?;
                assert!(!tenant_publication_grant_exists(pool, grant).await?);
                grant.to_string()
            }
            _ => grant.to_string(),
        };

        let responses =
            publish_as_build_manager(&mut fixture.client, "tenant-out", tenant, 'a', Some(proof))
                .await;

        assert_eq!(responses, (code, code));
        assert_eq!(latest_hash(pool, "tenant-out").await?, None);
        let created: i64 = sqlx::query_scalar(sql!(
            "SELECT count(*) FROM templates WHERE name = 'tenant-out'"
        ))
        .fetch_one(pool)
        .await?;
        assert_eq!(created, 0);
        if let Some(control) = control {
            assert!(tenant_publication_grant_exists(pool, control).await?);
            assert_eq!(
                publish_as_build_manager(
                    &mut fixture.client,
                    "control-out",
                    tenant,
                    'b',
                    Some(control.to_string())
                )
                .await,
                (Code::Ok, Code::Ok)
            );
        }
        fixture.finish().await
    })
}

#[test]
fn tenant_grant_cannot_authorize_a_system_template() -> TestResult {
    in_publication_process(|| async {
        let mut fixture = PublicationServer::start().await?;
        let grant = grant_tenant_publication(
            &fixture.db.pool,
            "tenant-out",
            Uuid::from_u128(3),
            None,
            "0 seconds",
        )
        .await?;
        let mut template = registration("code-runtime", None);
        template.tenant_publication_grant_id = Some(grant.to_string());

        let result = fixture
            .client
            .register_template(request(template, "space-build-manager"))
            .await;

        assert_eq!(status_code(result), Code::InvalidArgument);
        fixture.finish().await
    })
}

#[rstest::rstest]
#[case::fresh_unpinned(false, "0 seconds")]
#[case::queued_pinned(true, "2 days")]
fn tenant_publication_consumes_the_grant_once(
    #[case] pinned: bool,
    #[case] age: &'static str,
) -> TestResult {
    in_publication_process(|| async move {
        let mut fixture = PublicationServer::start().await?;
        let pool = &fixture.db.pool;
        let tenant = Uuid::from_u128(3);
        let builder = insert_publishing_builder(pool).await?;
        let grant =
            grant_tenant_publication(pool, "tenant-out", tenant, pinned.then_some(builder), age)
                .await?;
        let client = &mut fixture.client;

        client
            .register_template(request(
                tenant_registration("tenant-out", tenant, Some(grant.to_string())),
                "space-build-manager",
            ))
            .await?;
        assert!(tenant_publication_grant_exists(pool, grant).await?);
        client
            .register_template_release(request(
                tenant_release("tenant-out", tenant, 'a', Some(grant.to_string())),
                "space-build-manager",
            ))
            .await?;

        assert!(!tenant_publication_grant_exists(pool, grant).await?);
        let owner: Option<Uuid> = sqlx::query_scalar(sql!(
            "SELECT org_id FROM templates WHERE name = 'tenant-out'"
        ))
        .fetch_one(pool)
        .await?;
        assert_eq!(owner, Some(tenant));
        assert_eq!(
            publish_as_build_manager(client, "tenant-out", tenant, 'b', Some(grant.to_string()))
                .await,
            (Code::PermissionDenied, Code::PermissionDenied)
        );
        assert_eq!(
            latest_hash(pool, "tenant-out").await?,
            Some(format!("sha256:{}", "a".repeat(64)))
        );
        fixture.finish().await
    })
}

#[test]
fn tenant_publication_rollback_preserves_grant_and_concurrent_release_consumes_once() -> TestResult
{
    in_publication_process(|| async {
        let mut fixture = PublicationServer::start().await?;
        let pool = fixture.db.pool.clone();
        let client = &mut fixture.client;
        let tenant = Uuid::from_u128(3);
        insert_publishing_builder(&pool).await?;
        let rollback =
            grant_tenant_publication(&pool, "rollback-tenant", tenant, None, "0 seconds").await?;
        assert_eq!(
            client
                .register_template_release(request(
                    tenant_release("rollback-tenant", tenant, 'a', Some(rollback.to_string())),
                    "space-build-manager",
                ))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        assert!(tenant_publication_grant_exists(&pool, rollback).await?);
        client
            .register_template(request(
                tenant_registration("rollback-tenant", tenant, Some(rollback.to_string())),
                "space-build-manager",
            ))
            .await?;
        let mut concurrent = client.clone();
        let publication =
            tenant_release("rollback-tenant", tenant, 'a', Some(rollback.to_string()));
        let (first, replay) = tokio::join!(
            client.register_template_release(request(publication.clone(), "space-build-manager")),
            concurrent.register_template_release(request(publication, "space-build-manager")),
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(replay.is_ok()), 1);
        assert_eq!(
            first.err().or_else(|| replay.err()).unwrap().code(),
            Code::PermissionDenied
        );
        assert!(!tenant_publication_grant_exists(&pool, rollback).await?);
        fixture.finish().await
    })
}

fn reviewed_publisher(account: &str) -> TokenReviewStatus {
    let mut status = service_account_status(
        "space-cluster-api",
        &format!("system:serviceaccount:space:{account}"),
    );
    status.user.as_mut().unwrap().extra = Some(
        [
            (
                "authentication.kubernetes.io/pod-name".into(),
                vec!["builder-pod".into()],
            ),
            (
                "authentication.kubernetes.io/pod-uid".into(),
                vec!["builder-pod-uid".into()],
            ),
            (
                "authentication.kubernetes.io/node-name".into(),
                vec!["publication-builder".into()],
            ),
            (
                "authentication.kubernetes.io/node-uid".into(),
                vec!["publication-node-uid".into()],
            ),
        ]
        .into(),
    );
    status
}

async fn insert_publishing_builder(pool: &sqlx::PgPool) -> TestResult<Uuid> {
    // Pod traffic reaches this server from loopback; inventory records a different node IP.
    let id = insert_k8s_node(pool, std::net::Ipv4Addr::new(192, 0, 2, 40), "builder").await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'publication-builder', 'publication-node-uid')"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(id)
}

#[rstest::rstest]
#[case::publisher_lookup("LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE")]
#[case::grant_consumption("LOCK TABLE template_tenant_publication_grants IN ACCESS EXCLUSIVE MODE")]
#[case::owner_lock("LOCK TABLE templates IN ACCESS EXCLUSIVE MODE")]
#[case::tag_write("LOCK TABLE template_release_tags IN ACCESS EXCLUSIVE MODE")]
fn publication_contention_is_retryable(#[case] lock: &'static str) -> TestResult {
    in_publication_process(|| async move {
        let mut fixture = PublicationServer::start().await?;
        let pool = &fixture.db.pool;
        let tenant = Uuid::from_u128(3);
        let builder = insert_publishing_builder(pool).await?;
        let grant =
            grant_tenant_publication(pool, "contended-output", tenant, Some(builder), "0 seconds")
                .await?;
        fixture
            .client
            .register_template(request(
                tenant_registration("contended-output", tenant, Some(grant.to_string())),
                "space-build-manager",
            ))
            .await?;
        let mut blocker = pool.begin().await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(lock)))
            .execute(&mut *blocker)
            .await?;
        let publication = tenant_release("contended-output", tenant, 'a', Some(grant.to_string()));

        let result = fixture
            .client
            .register_template_release(request(publication.clone(), "space-build-manager"))
            .await;

        let status = result.expect_err("the held lock must abort publication");
        assert!(
            pplx_space_node_api_client::cluster::PublicationNotCommitted::from_status(&status)
                .is_some()
        );
        blocker.rollback().await?;
        assert!(tenant_publication_grant_exists(pool, grant).await?);
        assert_eq!(latest_hash(pool, "contended-output").await?, None);
        fixture
            .client
            .register_template_release(request(publication, "space-build-manager"))
            .await?;
        assert!(!tenant_publication_grant_exists(pool, grant).await?);
        fixture.finish().await
    })
}

#[test]
fn bootstrapped_publisher_waits_for_verified_inventory_uid() -> TestResult {
    in_publication_process(|| async move {
        let mut fixture = PublicationServer::start().await?;
        let pool = &fixture.db.pool;
        let tenant = Uuid::from_u128(3);
        let service = ClusterApiService::new(pool.clone());
        service
            .bootstrap_k8s_origin_node(
                "publication-builder",
                SpaceNodeRole::Builder,
                "192.0.2.40".parse()?,
            )
            .await?;
        let builder = space_k8s_node_id("publication-builder", "builder");
        let grant = grant_tenant_publication(
            pool,
            "bootstrap-output",
            tenant,
            Some(builder.as_uuid()),
            "0 seconds",
        )
        .await?;
        let publication = tenant_registration("bootstrap-output", tenant, Some(grant.to_string()));

        let status = fixture
            .client
            .register_template(request(publication.clone(), "space-build-manager"))
            .await
            .expect_err("a NULL UID cannot authorize publication");
        assert!(
            pplx_space_node_api_client::cluster::PublicationNotCommitted::from_status(&status)
                .is_some()
        );
        assert!(tenant_publication_grant_exists(pool, grant).await?);
        let exists: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (SELECT 1 FROM templates WHERE name = 'bootstrap-output')"
        ))
        .fetch_one(pool)
        .await?;
        assert!(!exists);
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
            VALUES ($1, 'publication-builder', 'publication-node-uid')
            ON CONFLICT (node_id) DO UPDATE SET k8s_node_uid = EXCLUDED.k8s_node_uid"
        ))
        .bind(builder)
        .execute(pool)
        .await?;

        fixture
            .client
            .register_template(request(publication, "space-build-manager"))
            .await?;
        fixture
            .client
            .register_template_release(request(
                tenant_release("bootstrap-output", tenant, 'a', Some(grant.to_string())),
                "space-build-manager",
            ))
            .await?;
        assert!(!tenant_publication_grant_exists(pool, grant).await?);
        fixture.finish().await
    })
}
