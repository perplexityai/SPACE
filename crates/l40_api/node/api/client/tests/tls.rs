use std::ffi::OsStr;
use std::time::Duration;

use pplx_space_common_grpc::internal_transport::{CA_ENV, INSECURE_DEV_ENV};
use pplx_space_node_api_client::NodeApiClient;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Identity, Server, ServerTlsConfig};

#[tokio::test]
async fn reconnect_retains_client_trust_but_new_clients_reload_it() {
    // Allocate the path in the parent so its lifetime covers the isolated child.
    let temp = tempfile::tempdir().unwrap();
    let ca_path = temp.path().join("ca.pem");
    if !pplx_space_test_env::enter_test_process(&[
        (CA_ENV, Some(ca_path.as_os_str())),
        (INSECURE_DEV_ENV, None),
        (
            "SPACE_NODE_API_TLS_SERVER_NAME",
            Some(OsStr::new("node.example")),
        ),
        (
            "SPACE_INTERNAL_CONTROL_TOKEN",
            Some(OsStr::new("tls-test-token")),
        ),
        ("SPACE_NODE_API_INTERNAL_TOKEN_PATH", None),
    ]) {
        return;
    }
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        rustls::crypto::CryptoProvider::get_default().expect("provider already installed");
    }
    let ca_path = std::path::PathBuf::from(std::env::var_os(CA_ENV).unwrap());
    let certificate = rcgen::generate_simple_self_signed(vec!["node.example".into()]).unwrap();
    tokio::fs::write(&ca_path, certificate.cert.pem())
        .await
        .unwrap();
    let identity = Identity::from_pem(
        certificate.cert.pem(),
        certificate.signing_key.serialize_pem(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint =
        Endpoint::from_shared(format!("https://{}", listener.local_addr().unwrap())).unwrap();
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))
            .unwrap()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                stopped.await.expect("test signals shutdown");
            }),
    );
    let policy = pplx_space_common_grpc::GrpcRetryPolicy {
        max_retries: 0,
        ..pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY
    };
    let mut client = NodeApiClient::new(endpoint.clone()).with_retry_policy(policy);
    assert!(
        client
            .health_service_serving("", Duration::from_secs(2))
            .await
            .unwrap()
    );
    tokio::fs::remove_file(&ca_path).await.unwrap();
    client.clear();
    assert!(
        client
            .health_service_serving("", Duration::from_secs(2))
            .await
            .unwrap()
    );
    let mut replacement = NodeApiClient::new(endpoint).with_retry_policy(policy);
    replacement
        .health_service_serving("", Duration::from_secs(2))
        .await
        .unwrap_err();
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
