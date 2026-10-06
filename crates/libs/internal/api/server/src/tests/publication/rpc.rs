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
        name: name.into(),
        description: None,
        org_id: org.map(|v| v.to_string()),
    }
}

fn release(name: &str, org: Option<Uuid>) -> RegisterTemplateReleaseRequest {
    RegisterTemplateReleaseRequest {
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
async fn grant_system_publication(pool: &sqlx::PgPool, template: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name) VALUES ($1, $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(template)
    .execute(pool)
    .await?;
    Ok(())
}

async fn system_publication_grants(pool: &sqlx::PgPool, template: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT count(*) FROM template_system_publication_grants WHERE template_name = $1"
    ))
    .bind(template)
    .fetch_one(pool)
    .await?)
}

#[test]
fn authenticated_publication_enforces_identity_owner_and_reserved_outputs() -> TestResult {
    let env = [
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true"),
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
        .block_on(run())
}

async fn run() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let config = std::path::PathBuf::from(
        std::env::var_os("KUBECONFIG").expect("test child kubeconfig path"),
    );
    std::fs::write(&config, kubeconfig_for_endpoint(listener.local_addr()?))?;
    let shutdown = CancellationToken::new();
    let _guard = shutdown.clone().drop_guard();
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
            Some((_, name)) => service_account_status(
                "space-cluster-api",
                &format!("system:serviceaccount:space:{name}"),
            ),
            None => TokenReviewStatus {
                authenticated: Some(false),
                ..Default::default()
            },
        },
        shutdown.clone(),
    ));
    let mut service = ClusterApiService::new(db.pool.clone());
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
    let mut client = ClusterApiClient::connect(endpoint).await?;
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
    // Build-manager also publishes tenant templates, so it publishes a system
    // template only for a system build the API dispatched. The deploy-time
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
    grant_system_publication(&db.pool, "code-runtime").await?;
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
        client
            .register_template(request(registration(name, None), publisher))
            .await?;
        client
            .register_template_release(request(release(name, None), publisher))
            .await?;
    }
    // One grant covers one release: a second release, an expired grant, or a
    // grant for another template authorizes nothing.
    assert_eq!(
        system_publication_grants(&db.pool, "code-runtime").await?,
        0
    );
    let mut second = release("code-runtime", None);
    second.hash = format!("sha256:{}", "c".repeat(64));
    sqlx::query(sql!(
        "INSERT INTO template_system_publication_grants (id, template_name, granted_at)
         VALUES ($1, 'code-runtime', NOW() - INTERVAL '8 days')"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    grant_system_publication(&db.pool, "seed").await?;
    assert_eq!(
        client
            .register_template_release(request(second, "space-build-manager"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    sqlx::query(sql!("DELETE FROM template_system_publication_grants"))
        .execute(&db.pool)
        .await?;
    client
        .register_template(request(
            registration("tenant-output", Some(owner)),
            "space-build-manager",
        ))
        .await?;
    client
        .register_template_release(request(
            release("tenant-output", Some(owner)),
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
    // A legacy release with mismatched ownership cannot be adopted by publishing.
    sqlx::query(sql!(
        "UPDATE template_releases SET org_id=NULL WHERE template_name='tenant-output'"
    ))
    .execute(&db.pool)
    .await?;
    assert_eq!(
        client
            .register_template_release(request(
                release("tenant-output", Some(owner)),
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
    grant_system_publication(&db.pool, "seed").await?;
    client
        .register_template(request(registration("seed", None), "space-build-manager"))
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
    drop(client);
    shutdown.cancel();
    server.await??;
    reviewer.await??;
    Ok(())
}
