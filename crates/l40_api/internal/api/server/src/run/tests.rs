use super::*;
use pplx_space_node_api_client::proto::{
    PublishNodeStateRequest, PublishedSandboxState, cluster_api_client::ClusterApiClient,
};
use prost::Message;
use tonic::Code;

#[test]
fn missing_node_id_file_generates_uuid_v7() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");

    let (node_id, source) = resolve_node_id_with_env(&path, None).unwrap();
    let persisted = std::fs::read_to_string(&path).unwrap();

    assert_eq!(persisted.trim(), node_id);
    assert_eq!(source, path.display().to_string());
    assert_eq!(node_id.chars().nth(14), Some('7'));
}

#[test]
fn env_node_id_takes_precedence_over_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-id");
    std::fs::write(&path, "not-a-uuid\n").unwrap();

    let (node_id, source) =
        resolve_node_id_with_env(&path, Some("  cluster-api-0\n".to_string())).unwrap();

    assert_eq!(node_id, "cluster-api-0");
    assert_eq!(source, SPACE_NODE_ID_ENV);
}

#[tokio::test]
async fn cluster_api_decodes_a_large_full_sandbox_census() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgresql://localhost/space")
        .unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(cluster_api_server(ClusterApiService::new(pool)))
            .serve_with_incoming(TcpIncoming::from(listener))
            .await
    });

    let request = PublishNodeStateRequest {
        node_id: "00000000-0000-7000-8000-000000000001".to_string(),
        sandbox_inventory_complete: true,
        sandboxes: (0..45_000)
            .map(|index| PublishedSandboxState {
                sandbox_id: format!("00000000-0000-7000-8000-{index:012x}"),
                present: true,
                status: Some("running".to_string()),
                content_hash: Some(format!("sha256:{:064x}", index)),
                ..PublishedSandboxState::default()
            })
            .collect(),
        // The handler validates this before any database access. Its error
        // confirms that the complete request reached application code.
        role: Some(i32::MAX),
        ..PublishNodeStateRequest::default()
    };
    assert!(request.encoded_len() > 4 * 1024 * 1024);

    let mut client = ClusterApiClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let status = client.publish_node_state(request).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), format!("unknown node role {}", i32::MAX));
    server.abort();
}
