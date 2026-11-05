//! End-to-end check that `NodeApiAuthLayer` rejections surface to gRPC
//! clients as proper statuses (UNAUTHENTICATED) rather than transport-level
//! stream errors.

use pplx_space_rpc_auth::{NodeApiAuthConfig, NodeApiAuthLayer, NodeRole};

#[tokio::test]
async fn auth_layer_rejection_is_a_grpc_status_not_a_transport_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = NodeApiAuthConfig::from_env(
        NodeRole::Worker,
        "expected-token".to_string(),
        Some("control-token".to_string()),
    )
    .unwrap();
    let auth_layer = NodeApiAuthLayer::from_config(config).await.unwrap();

    let (_, health_service) = tonic_health::server::health_reporter();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(auth_layer)
            .add_service(health_service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    // Health/Check is on the auth bypass list and must succeed without a token.
    let mut health = tonic_health::pb::health_client::HealthClient::new(channel.clone());
    health
        .check(tonic_health::pb::HealthCheckRequest::default())
        .await
        .expect("health check must bypass auth");

    // A non-bypassed path with a wrong token must surface UNAUTHENTICATED.
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.unwrap();
    let codec = tonic_prost::ProstCodec::<
        tonic_health::pb::HealthCheckRequest,
        tonic_health::pb::HealthCheckResponse,
    >::default();
    let mut request = tonic::Request::new(tonic_health::pb::HealthCheckRequest::default());
    request
        .metadata_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    let err = grpc
        .unary(
            request,
            http::uri::PathAndQuery::from_static("/space.test.v1.Anything/Do"),
            codec,
        )
        .await
        .expect_err("wrong token must be rejected");

    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "auth layer rejection must map to UNAUTHENTICATED, got: {err:?}"
    );

    server.abort();
}
