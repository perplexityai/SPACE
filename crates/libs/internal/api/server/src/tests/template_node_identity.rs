use super::*;
use crate::proto::{cluster_api_client::ClusterApiClient, cluster_api_server::ClusterApiServer};
use pplx_space_common_svc::auth::{
    NodeApiAuthConfig, NodeApiAuthLayer, NodeRole,
    test_support::{
        pod_bound_service_account_status, projected_token, serve_token_reviews,
        service_account_status,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_stream::wrappers::TcpListenerStream;

/// A pod's reviewed node binding, rather than its transport address, determines
/// which placed private templates it can pull. Cached reviews retain the binding.
#[tokio::test]
async fn pod_template_reads_follow_reviewed_node_identity() -> TestResult {
    let fixture = cluster_fixture().await?.expect("Postgres required");
    let pool = &fixture.service.pool;
    let owner = SEEDED_ORG_ID;
    let (_, hash) = seed_private_release(&fixture, "placed-template", owner).await?;
    let (_, peer_hash) = seed_private_release(&fixture, "peer-template", owner).await?;
    let node_uid = Uuid::now_v7().to_string();
    for (name, address, template) in [
        ("catalog-worker", "192.0.2.10", "placed-template"),
        ("transport-worker", "127.0.0.1", "peer-template"),
    ] {
        let node = insert_k8s_node(pool, address.parse()?, "worker").await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
        ))
        .bind(node)
        .bind(name)
        .bind(&node_uid)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle,
                placement, target_node_id, network_policy_generation, template_id)
             VALUES ($1, $2, $3, 'k8s', 'started', 'on_node', $4, 1, $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(SEEDED_USER_ID)
        .bind(owner)
        .bind(node)
        .bind(template)
        .execute(pool)
        .await?;
    }

    let tokens = ["bound", "stale", "unbound"].map(projected_token);
    let reviewed_tokens = tokens.clone();
    let reviews = Arc::new(AtomicUsize::new(0));
    let review_count = reviews.clone();
    let review_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let kube_config =
        kube::Config::new(format!("http://{}", review_listener.local_addr()?).parse()?);
    let reviewer = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(serve_token_reviews(
        review_listener,
        move |spec| {
            review_count.fetch_add(1, Ordering::SeqCst);
            let index = reviewed_tokens
                .iter()
                .position(|token| Some(token) == spec.token.as_ref())
                .unwrap();
            let username = "system:serviceaccount:space:workers";
            if index == 2 {
                service_account_status("space-cluster-api", username)
            } else {
                let uid = if index == 0 {
                    node_uid.as_str()
                } else {
                    "stale-node-uid"
                };
                pod_bound_service_account_status(
                    "space-cluster-api",
                    username,
                    "catalog-worker",
                    uid,
                )
            }
        },
        tokio_util::sync::CancellationToken::new(),
    )));
    let mut env = pplx_space_test_env::TestEnv::default();
    for (key, value) in [
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true"),
        ("SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED", "true"),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space"),
        ("KUBERNETES_SERVICE_HOST", "127.0.0.1"),
        ("SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS", "workers"),
    ] {
        env.set(key, value);
    }
    let config = NodeApiAuthConfig::from_environment(
        &env,
        NodeRole::Cluster,
        "synthetic-static-token".into(),
        None,
    )?;
    let auth =
        NodeApiAuthLayer::with_k8s_client_for_test(config, kube::Client::try_from(kube_config)?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(auth)
            .add_service(ClusterApiServer::new(fixture.service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    }));
    let mut client = ClusterApiClient::connect(endpoint).await?;
    for (token, expected) in [
        (tokens[0].as_str(), [true, false]),
        (tokens[1].as_str(), [false, false]),
        (tokens[2].as_str(), [false, false]),
        ("synthetic-static-token", [false, true]),
    ] {
        for _ in 0..2 {
            for ((template, hash), expected) in
                [("placed-template", &hash), ("peer-template", &peer_hash)]
                    .into_iter()
                    .zip(expected)
            {
                let mut request = Request::new(FindTemplateReleaseByHashRequest {
                    template_name: template.into(),
                    hash: hash.clone(),
                    ..Default::default()
                });
                request
                    .metadata_mut()
                    .insert("authorization", format!("Bearer {token}").parse()?);
                let actual = client
                    .find_template_release_by_hash(request)
                    .await
                    .map(|_| Code::Ok)
                    .unwrap_or_else(|status| status.code());
                let expected = if expected { Code::Ok } else { Code::NotFound };
                assert_eq!(actual, expected, "{template}");
            }
        }
    }
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        3,
        "each projected token is reviewed once"
    );
    drop(server);
    drop(reviewer);
    Ok(())
}
