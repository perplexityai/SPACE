use super::*;
use pplx_space_common_svc::auth::{NodeApiAuthConfig, NodeApiAuthLayer, NodeRole};
use pplx_space_node_api_client::proto::cluster_api_client::ClusterApiClient;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Clone, Copy)]
enum Lookup {
    Tag,
    Hash,
}

async fn resolve(
    client: &mut ClusterApiClient<tonic::transport::Channel>,
    lookup: Lookup,
    hash: &str,
    generic_only: bool,
    org: Option<OrgId>,
) -> Result<FindTemplateReleaseResponse, Status> {
    fn authenticated<T>(body: T) -> Request<T> {
        let mut request = Request::new(body);
        request.metadata_mut().insert(
            "authorization",
            "Bearer synthetic-prefetch-token".parse().unwrap(),
        );
        request
    }
    match lookup {
        Lookup::Tag => {
            client
                .find_template_release(authenticated(FindTemplateReleaseRequest {
                    template_name: "prefetch-transition".into(),
                    runtime: "fc".into(),
                    tag: "latest".into(),
                    caller_org_id: org.map(|id| id.to_string()),
                    generic_only,
                }))
                .await
        }
        Lookup::Hash => {
            client
                .find_template_release_by_hash(authenticated(FindTemplateReleaseByHashRequest {
                    template_name: "prefetch-transition".into(),
                    hash: hash.into(),
                    caller_org_id: org.map(|id| id.to_string()),
                    generic_only,
                }))
                .await
        }
    }
    .map(Response::into_inner)
}

#[rstest::rstest]
#[case::tag(Lookup::Tag)]
#[case::hash(Lookup::Hash)]
#[tokio::test]
async fn prefetch_resolution_rechecks_cached_generic_after_privacy_transition(
    #[case] lookup: Lookup,
) -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, visibility_cache(600, 600));
    register_template(&fixture.service, "prefetch-transition").await?;
    let generic_hash = format!("sha256:{}", "a".repeat(64));
    let private_hash = format!("sha256:{}", "b".repeat(64));
    fixture
        .service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            template_name: "prefetch-transition".into(),
            runtime: "fc".into(),
            hash: generic_hash.clone(),
            tags: vec!["latest".into()],
            storage_connection_strings: vec![VISIBILITY_TEST_STORAGE.into()],
            ..Default::default()
        }))
        .await?;
    let config = NodeApiAuthConfig::from_environment(
        &pplx_space_test_env::TestEnv::default(),
        NodeRole::Cluster,
        "synthetic-prefetch-token".into(),
        None,
    )?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let service = fixture.service.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(NodeApiAuthLayer::from_config(config).await.unwrap())
            .add_service(crate::proto::cluster_api_server::ClusterApiServer::new(
                service,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let server = tokio_util::task::AbortOnDropHandle::new(server);
    let mut client = ClusterApiClient::connect(endpoint).await?;

    let generic = resolve(&mut client, lookup, &generic_hash, true, None).await?;
    assert_eq!(generic.template_visibility(), TemplateVisibility::Generic);
    let owner: OrgId = OrgId::from_uuid(SEEDED_ORG_ID);
    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'org_specific', org_id = $2 WHERE name = $1"
    ))
    .bind("prefetch-transition")
    .bind(owner.as_uuid())
    .execute(&fixture.service.pool)
    .await?;
    fixture
        .service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            template_name: "prefetch-transition".into(),
            runtime: "fc".into(),
            hash: private_hash.clone(),
            tags: vec!["latest".into()],
            storage_connection_strings: vec![VISIBILITY_TEST_STORAGE.into()],
            org_id: Some(owner.to_string()),
            ..Default::default()
        }))
        .await?;

    let cached = resolve(&mut client, lookup, &private_hash, false, None).await?;
    assert_eq!(cached.template_visibility(), TemplateVisibility::Generic);
    assert_eq!(cached.release.unwrap().hash, private_hash);
    assert_eq!(cached.storage_locations.len(), 1);
    let denied = resolve(&mut client, lookup, &private_hash, true, None)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::NotFound);
    assert!(generic.generic_authorized);
    assert!(
        resolve(&mut client, lookup, &private_hash, false, Some(owner))
            .await?
            .release
            .is_some()
    );
    assert_eq!(
        resolve(
            &mut client,
            lookup,
            &private_hash,
            false,
            Some(OrgId::new())
        )
        .await
        .unwrap_err()
        .code(),
        Code::NotFound
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    Ok(())
}
