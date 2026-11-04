use super::{BlobUploadSender, SPACE_INTERNAL_TOKEN_ENV, parse_env_file_value};

#[tokio::test]
async fn lazy_client_rejects_plaintext_without_development_opt_in() {
    use std::ffi::OsStr;
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", None),
        ("SPACE_INTERNAL_RPC_TLS_CA", None),
        (
            "SPACE_BUILD_MANAGER_ENDPOINT",
            Some(OsStr::new("http://127.0.0.1:9900")),
        ),
        (
            SPACE_INTERNAL_TOKEN_ENV,
            Some(OsStr::new("synthetic-token")),
        ),
    ]) {
        return;
    }
    assert!(
        super::connect_lazy().is_err(),
        "a bearer client must reject plaintext before opening a connection"
    );
}

#[test]
fn parses_internal_token_from_space_env_file_content() {
    let content = "\n# comment\nOTHER=value\nSPACE_INTERNAL_TOKEN= local-token \n";

    assert_eq!(
        parse_env_file_value(content, SPACE_INTERNAL_TOKEN_ENV).as_deref(),
        Some("local-token")
    );
}

#[test]
fn ignores_missing_or_empty_env_file_values() {
    assert_eq!(
        parse_env_file_value("SPACE_INTERNAL_TOKEN=   ", SPACE_INTERNAL_TOKEN_ENV),
        None
    );
    assert_eq!(
        parse_env_file_value("OTHER=value", SPACE_INTERNAL_TOKEN_ENV),
        None
    );
}

#[tokio::test]
async fn blob_upload_sender_frames_a_committed_stream() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
    let upload = BlobUploadSender::begin(sender).await.unwrap();

    upload.send_data(b"first".to_vec()).await.unwrap();
    upload.send_data(b"second".to_vec()).await.unwrap();
    upload.commit().await.unwrap();

    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push((
            super::proto::UploadBlobPhase::try_from(chunk.phase).unwrap(),
            chunk.data,
        ));
    }
    assert_eq!(
        chunks,
        vec![
            (super::proto::UploadBlobPhase::Data, Vec::new()),
            (super::proto::UploadBlobPhase::Data, b"first".to_vec()),
            (super::proto::UploadBlobPhase::Data, b"second".to_vec()),
            (super::proto::UploadBlobPhase::Commit, Vec::new()),
        ]
    );
}

#[test]
fn projected_client_auth_rotates_and_fails_closed() {
    use pplx_space_bearer_token::SPACE_BUILD_MANAGER_TOKEN_PATH_ENV;
    use tonic::service::Interceptor;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("build-manager.token");
    std::fs::write(&path, "first").unwrap();
    if !pplx_space_test_env::enter_test_process(&[
        (SPACE_BUILD_MANAGER_TOKEN_PATH_ENV, Some(path.as_os_str())),
        (
            SPACE_INTERNAL_TOKEN_ENV,
            Some(std::ffi::OsStr::new("shared-token")),
        ),
    ]) {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(SPACE_BUILD_MANAGER_TOKEN_PATH_ENV).unwrap());
    let mut interceptor = super::BearerAuthInterceptor::from_env().unwrap();
    let request = interceptor.call(tonic::Request::new(())).unwrap();
    assert_eq!(
        request.metadata().get("authorization").unwrap(),
        "Bearer first"
    );

    std::fs::write(&path, "rotated").unwrap();
    std::fs::File::open(&path)
        .unwrap()
        .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(60))
        .unwrap();
    let request = interceptor.call(tonic::Request::new(())).unwrap();
    assert_eq!(
        request.metadata().get("authorization").unwrap(),
        "Bearer rotated"
    );

    std::fs::remove_file(&path).unwrap();
    let error = interceptor.call(tonic::Request::new(())).unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn eager_and_lazy_channels_authenticate_the_configured_service_identity() {
    use pplx_space_common_grpc::internal_transport::{CA_ENV, CERT_ENV, INSECURE_DEV_ENV, KEY_ENV};
    use std::ffi::OsStr;
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let directory = tempfile::tempdir().unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["builder.example".into()]).unwrap();
    let cert = directory.path().join("server.pem");
    let key = directory.path().join("server.key");
    std::fs::write(&cert, identity.cert.pem()).unwrap();
    std::fs::write(&key, identity.signing_key.serialize_pem()).unwrap();
    if !pplx_space_test_env::enter_test_process(&[
        (INSECURE_DEV_ENV, None),
        (CA_ENV, Some(cert.as_os_str())),
        (CERT_ENV, Some(cert.as_os_str())),
        (KEY_ENV, Some(key.as_os_str())),
        (
            "SPACE_BUILD_MANAGER_TLS_SERVER_NAME",
            Some(OsStr::new("builder.example")),
        ),
    ]) {
        return;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    // The dial IP has no certificate SAN; only the configured identity authenticates it.
    let endpoint = listener.local_addr().unwrap().to_string();
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let health = tonic::service::interceptor::InterceptedService::new(
        health,
        |request: tonic::Request<()>| {
            assert_eq!(
                request.metadata().get("authorization").unwrap(),
                "Bearer synthetic-token"
            );
            Ok(request)
        },
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        pplx_space_common_grpc::internal_transport::server_builder()
            .unwrap()
            .add_service(health)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    for channel in [
        super::connect_channel(endpoint.clone()).await.unwrap(),
        super::lazy_channel(endpoint).unwrap(),
    ] {
        let bearer = super::BearerAuthInterceptor::new("Bearer synthetic-token".parse().unwrap());
        HealthClient::with_interceptor(channel, bearer)
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .unwrap();
    }
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
