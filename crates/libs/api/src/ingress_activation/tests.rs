use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tonic::Code;

struct Backend(AtomicUsize);
#[tonic::async_trait]
impl IngressActivation for Backend {
    async fn prepare(&self, _: SandboxId, _: Duration) -> Result<(), Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Status::failed_precondition(
            "automatic access wake is disabled",
        ))
    }
}

#[tokio::test]
async fn delegation_preserves_policy_refusal_and_rejects_invalid_or_draining_requests() {
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let shutdown = CancellationToken::new();
    let service = ActivationService {
        activation: backend.clone(),
        shutdown: shutdown.clone(),
    };
    let request = || {
        Request::new(PrepareIngressRequest {
            sandbox_id: SandboxId::new().to_string(),
        })
    };
    let invalid = Request::new(PrepareIngressRequest {
        sandbox_id: "invalid".into(),
    });
    assert_eq!(
        service.prepare_ingress(invalid).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut expired = request();
    expired.set_timeout(Duration::ZERO);
    assert_eq!(
        service.prepare_ingress(expired).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert_eq!(backend.0.load(Ordering::SeqCst), 0);
    let refused = service.prepare_ingress(request()).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "automatic access wake is disabled");
    shutdown.cancel();
    assert_eq!(
        service.prepare_ingress(request()).await.unwrap_err().code(),
        Code::Unavailable
    );
    assert_eq!(backend.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn remote_activation_authenticates_and_preserves_terminal_status() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("synthetic-activation-token")),
        ),
        (
            "SPACE_K8S_TOKEN_AUTH_ENABLED",
            Some(std::ffi::OsStr::new("false")),
        ),
        (
            "SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED",
            Some(std::ffi::OsStr::new("true")),
        ),
        ("SPACE_CLUSTER_API_TOKEN_PATH", None),
    ]) {
        return;
    }
    let backend = Arc::new(Backend(AtomicUsize::new(0)));
    let shutdown = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let config = auth::NodeApiAuthConfig::from_env(
        auth::NodeRole::Cluster,
        "synthetic-activation-token".into(),
        None,
    )
    .unwrap();
    let layer = auth::NodeApiAuthLayer::from_config(config).await.unwrap();
    let task = tokio::spawn(
        serve(
            ActivationService {
                activation: backend.clone(),
                shutdown: shutdown.clone(),
            },
            listener,
            layer,
            shutdown.clone(),
        )
        .unwrap(),
    );
    let mut client = pplx_space_node_api_client::proto::ingress_activation_api_client::IngressActivationApiClient::connect(endpoint).await.unwrap();
    let request = || {
        Request::new(PrepareIngressRequest {
            sandbox_id: SandboxId::new().to_string(),
        })
    };
    for token in [
        None,
        Some("Bearer wrong"),
        Some("Bearer synthetic-activation-token"),
    ] {
        let mut req = request();
        req.set_timeout(Duration::from_millis(200));
        if let Some(token) = token {
            req.metadata_mut()
                .insert("authorization", token.parse().unwrap());
        }
        let error = client.prepare_ingress(req).await.unwrap_err();
        assert_eq!(
            error.code(),
            if token == Some("Bearer synthetic-activation-token") {
                Code::FailedPrecondition
            } else {
                Code::Unauthenticated
            }
        );
    }
    assert_eq!(backend.0.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await.unwrap().unwrap();
}
