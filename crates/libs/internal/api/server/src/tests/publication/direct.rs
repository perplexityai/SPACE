use super::*;
use crate::proto::{cluster_api_client::ClusterApiClient, cluster_api_server::ClusterApiServer};
use pplx_space_common_core::types::SubstrateKind;
use pplx_space_common_svc::auth::{self, build::BuildAuthLayer};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum Grant {
    Missing,
    WrongTarget,
    WrongBuilder,
    Valid,
}

#[rstest::rstest]
#[case::missing(Grant::Missing, Code::PermissionDenied)]
#[case::wrong_target(Grant::WrongTarget, Code::PermissionDenied)]
#[case::wrong_builder(Grant::WrongBuilder, Code::PermissionDenied)]
#[case::valid(Grant::Valid, Code::Ok)]
fn direct_publisher_requires_its_tenant_build_grant(
    #[case] grant: Grant,
    #[case] expected: Code,
) -> TestResult {
    let directory = tempfile::tempdir()?;
    let token = directory.path().join("publisher.token");
    std::fs::write(&token, "synthetic-dedicated-publication-credential")?;
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_K8S_TOKEN_AUTH_ENABLED",
            Some(std::ffi::OsStr::new("false")),
        ),
        (
            "SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH",
            Some(token.as_os_str()),
        ),
    ]) {
        return Ok(());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(grant, expected))
}

async fn run(grant: Grant, expected: Code) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let owner = pplx_space_db_test_fixtures::SEED_ORG_ID;
    let builder = Uuid::now_v7();
    let other_builder = Uuid::now_v7();
    for (id, address) in [(builder, "127.0.0.1"), (other_builder, "192.0.2.40")] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'builder', $3, 'ready')"
        ))
        .bind(id)
        .bind(address)
        .bind(SubstrateKind::Space)
        .execute(&db.pool)
        .await?;
    }
    let grant_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_tenant_publication_grants (id, template_name, org_id, builder_node_id)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(grant_id)
    .bind(if matches!(grant, Grant::WrongTarget) {
        "other-output"
    } else {
        "direct-output"
    })
    .bind(owner)
    .bind(if matches!(grant, Grant::WrongBuilder) {
        other_builder
    } else {
        builder
    })
    .execute(&db.pool)
    .await?;
    let outer = auth::NodeApiAuthLayer::from_config(auth::NodeApiAuthConfig::from_env(
        auth::NodeRole::Cluster,
        "synthetic-node-token".into(),
        None,
    )?)
    .await?;
    let publisher = BuildAuthLayer::for_template_publication("synthetic-node-token".into()).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let shutdown = CancellationToken::new();
    let _cleanup = shutdown.clone().drop_guard();
    let server_shutdown = shutdown.clone();
    let pool = db.pool.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(outer)
            .layer(publisher)
            .add_service(ClusterApiServer::new(ClusterApiService::new(pool)))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            )
            .await
    });
    let mut client = ClusterApiClient::connect(endpoint).await?;
    let proof = (!matches!(grant, Grant::Missing)).then(|| grant_id.to_string());
    let template = RegisterTemplateRequest {
        name: "direct-output".into(),
        description: None,
        org_id: Some(owner.to_string()),
        system_publication_grant_id: None,
        tenant_publication_grant_id: proof.clone(),
    };
    let publication = RegisterTemplateReleaseRequest {
        template_name: "direct-output".into(),
        runtime: "fc".into(),
        hash: format!("sha256:{}", "a".repeat(64)),
        tags: vec!["latest".into()],
        storage_connection_strings: vec![],
        org_id: Some(owner.to_string()),
        execution: None,
        system_publication_grant_id: None,
        tenant_publication_grant_id: proof,
    };

    let registered = client.register_template(request(template)?).await;
    let published = client
        .register_template_release(request(publication.clone())?)
        .await;

    assert_eq!(
        registered.map_or_else(|status| status.code(), |_| Code::Ok),
        expected
    );
    assert_eq!(
        published.map_or_else(|status| status.code(), |_| Code::Ok),
        expected
    );
    let (templates, releases, grants): (i64, i64, i64) = sqlx::query_as(sql!(
        "SELECT (SELECT count(*) FROM templates WHERE name = 'direct-output'),
                (SELECT count(*) FROM template_releases WHERE template_name = 'direct-output'),
                (SELECT count(*) FROM template_tenant_publication_grants WHERE id = $1)"
    ))
    .bind(grant_id)
    .fetch_one(&db.pool)
    .await?;
    if matches!(grant, Grant::Valid) {
        assert_eq!((templates, releases, grants), (1, 1, 0));
        let replay = client
            .register_template_release(request(publication)?)
            .await
            .unwrap_err();
        assert_eq!(replay.code(), Code::PermissionDenied);
    } else {
        assert_eq!((templates, releases, grants), (0, 0, 1));
    }
    shutdown.cancel();
    server.await??;
    Ok(())
}

fn request<T>(body: T) -> TestResult<Request<T>> {
    let mut request = auth::build::publication_request(body)?;
    request
        .metadata_mut()
        .insert("authorization", "Bearer synthetic-node-token".parse()?);
    Ok(request)
}
