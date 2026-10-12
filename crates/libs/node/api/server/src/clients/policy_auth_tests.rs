use pplx_space_common_core::types::SandboxId;

use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    builder::BuilderApiService,
    proto::{
        builder_api_client::BuilderApiClient, worker_api_client::WorkerApiClient,
        worker_api_internal_client::WorkerApiInternalClient,
        worker_api_internal_server::WorkerApiInternalServer, worker_api_server::WorkerApiServer,
    },
    worker::WorkerApiService,
};
use axum::{Router, body::Body, routing::any};
use pplx_space_common_core::types::NodeId;
use pplx_space_common_svc::{
    auth::{NodeApiAuthConfig, NodeApiAuthLayer, NodeRole},
    bootstrap::wrap_grpc_client_channel,
};
use pplx_space_net_common::{NetworkPolicyV1, proto::PolicyAuthority};
use pplx_space_sbx_mgr_client::proto as sbx;
use pplx_space_template_mgr_client::proto as tmpl;

use tonic::{Code, Request};
use tower::Layer;

pub(super) const PROJECTED_TOKEN: &str = "e30.e30.signature";
pub(super) const STATIC_TOKEN: &str = "shared-token";

pub(super) fn auth_layer(
    role: NodeRole,
    k8s: bool,
    username: &'static str,
    reviews: Arc<AtomicUsize>,
) -> NodeApiAuthLayer {
    let vars = [
        ("KUBERNETES_SERVICE_HOST", "unused.invalid"),
        (
            "SPACE_K8S_TOKEN_AUTH_ENABLED",
            if k8s { "true" } else { "false" },
        ),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space"),
        ("SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED", "true"),
        (
            "SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS",
            "space-api,workers,observer,space-data-plane",
        ),
        (
            "SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS",
            "space-api,workers,observer,space-data-plane",
        ),
        (
            "SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS",
            "space-api",
        ),
        (
            "SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS",
            "space-api",
        ),
        (
            "SPACE_NODE_API_AUTH_GUEST_BRIDGE_SERVICE_ACCOUNTS",
            "space-data-plane",
        ),
    ];
    let config = {
        let mut env = pplx_space_test_env::TestEnv::default();
        for (name, value) in vars {
            env.set(name, value);
        }
        NodeApiAuthConfig::from_environment(
            &env,
            role,
            STATIC_TOKEN.into(),
            Some(STATIC_TOKEN.into()),
        )
        .expect("valid test auth configuration")
    };
    // Only the apiserver is substituted: the layer still parses the bearer,
    // performs TokenReview, classifies the principal, and attaches its authority.
    let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
        let reviews = reviews.clone();
        async move {
            let bytes = axum::body::to_bytes(Body::new(request.into_body()), 8192)
                .await
                .expect("read TokenReview request");
            let review: serde_json::Value =
                serde_json::from_slice(&bytes).expect("TokenReview request JSON");
            assert_eq!(
                review
                    .pointer("/spec/token")
                    .and_then(serde_json::Value::as_str),
                Some(PROJECTED_TOKEN)
            );
            reviews.fetch_add(1, Ordering::SeqCst);
            let response = serde_json::json!({
                "apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
                "status": { "authenticated": true, "audiences": review.pointer("/spec/audiences").expect("requested audiences"),
                    "user": { "username": format!("system:serviceaccount:space:{username}") } }
            });
            Ok::<_, std::convert::Infallible>(http::Response::new(kube::client::Body::from(
                serde_json::to_vec(&response).expect("TokenReview response JSON"),
            )))
        }
    });
    NodeApiAuthLayer::with_k8s_client_for_test(
        config,
        kube::client::ClientBuilder::new(service, "space").build(),
    )
}

pub(super) fn bearer<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("valid test bearer"),
    );
    request
}

#[derive(Clone, Copy, Debug)]
enum Rpc {
    Create,
    Adopt,
    WorkerTemplate,
    BuilderTemplate,
    Network,
    Lifecycle,
}

#[tokio::test]
async fn policy_rpc_authentication_reaches_handlers() {
    let (clients, calls, shutdown, mut servers) = backend().await;
    for (k8s, username, token, trusted) in [
        (true, "space-api", PROJECTED_TOKEN, true),
        (true, "workers", PROJECTED_TOKEN, false),
        (true, "space-api", STATIC_TOKEN, false),
        (false, "space-api", STATIC_TOKEN, true),
    ] {
        for rpc in [
            Rpc::Create,
            Rpc::Adopt,
            Rpc::WorkerTemplate,
            Rpc::BuilderTemplate,
            Rpc::Network,
            Rpc::Lifecycle,
        ] {
            let role = match rpc {
                Rpc::BuilderTemplate => NodeRole::Builder,
                _ => NodeRole::Worker,
            };
            let reviews = Arc::new(AtomicUsize::new(0));
            let auth = auth_layer(role, k8s, username, reviews.clone());
            let routes = tonic::service::Routes::new(WorkerApiServer::new(WorkerApiService::new(
                clients.clone(),
            )))
            .add_service(WorkerApiInternalServer::new(WorkerApiService::new(
                clients.clone(),
            )))
            .add_service(
                BuilderApiService::new(clients.clone(), NodeId::new())
                    .into_grpc_service(Arc::new(tokio::sync::Semaphore::new(32))),
            );
            let service = auth.layer(routes);
            for (network, lifecycle, generation) in [
                (false, false, 0),
                (false, false, 1_000_000),
                (true, false, 0),
                (true, false, 1_000_000),
                (false, true, 0),
                (false, true, 1_000_000),
            ] {
                let network_policy = network.then(|| (&NetworkPolicyV1::allow_all()).into());
                let auto_lifecycle = lifecycle.then(sbx::AutoLifecyclePolicy::default);
                let authority = PolicyAuthority::Cluster as i32;
                let before = calls.load(Ordering::SeqCst);
                let result = match rpc {
                    Rpc::Create => WorkerApiClient::new(service.clone())
                        .create_sandbox(bearer(
                            sbx::CreateRequest {
                                network_policy,
                                auto_lifecycle,
                                network_policy_generation: generation,
                                network_policy_authority: authority,
                                auto_lifecycle_authority: authority,
                                ..Default::default()
                            },
                            token,
                        ))
                        .await
                        .map(|_| ()),
                    Rpc::Adopt => WorkerApiInternalClient::new(service.clone())
                        .adopt_pool_sandbox(bearer(
                            sbx::AdoptPoolSandboxRequest {
                                network_policy,
                                auto_lifecycle,
                                network_policy_generation: generation,
                                network_policy_authority: authority,
                                auto_lifecycle_authority: authority,
                                ..Default::default()
                            },
                            token,
                        ))
                        .await
                        .map(|_| ()),
                    Rpc::WorkerTemplate | Rpc::BuilderTemplate => {
                        let request = bearer(
                            tmpl::UpdateTemplateSettingsRequest {
                                network_policy,
                                default_auto_lifecycle: auto_lifecycle,
                                network_policy_generation: generation,
                                network_policy_authority: authority,
                                auto_lifecycle_authority: authority,
                                ..Default::default()
                            },
                            token,
                        );
                        match rpc {
                            Rpc::WorkerTemplate => WorkerApiClient::new(service.clone())
                                .update_template_settings(request)
                                .await
                                .map(|_| ()),
                            _ => BuilderApiClient::new(service.clone())
                                .update_template_settings(request)
                                .await
                                .map(|_| ()),
                        }
                    }
                    Rpc::Network => WorkerApiClient::new(service.clone())
                        .set_sandbox_network_policy(bearer(
                            sbx::SetNetworkPolicyRequest {
                                sandbox_id: SandboxId::new().to_string(),
                                policy: network_policy,
                                inherit_template: !network,
                                policy_generation: generation,
                                authority,
                                ..Default::default()
                            },
                            token,
                        ))
                        .await
                        .map(|_| ()),
                    Rpc::Lifecycle => WorkerApiClient::new(service.clone())
                        .set_sandbox_auto_lifecycle_policy(bearer(
                            sbx::SetAutoLifecyclePolicyRequest {
                                sandbox_id: SandboxId::new().to_string(),
                                policy: auto_lifecycle,
                                authority,
                            },
                            token,
                        ))
                        .await
                        .map(|_| ()),
                };
                let requires_authority = match rpc {
                    Rpc::Network | Rpc::Lifecycle => true,
                    Rpc::Create | Rpc::Adopt => network || lifecycle || generation != 0,
                    Rpc::WorkerTemplate | Rpc::BuilderTemplate => network || lifecycle,
                };
                let allowed = trusted || !requires_authority;
                assert_eq!(
                    result.map_err(|error| error.code()),
                    if allowed {
                        Ok(())
                    } else {
                        Err(Code::PermissionDenied)
                    },
                    "{rpc:?} k8s={k8s} {username} network={network} lifecycle={lifecycle} generation={generation}"
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst) - before,
                    usize::from(allowed),
                    "{rpc:?} forwarded only if authorized"
                );
            }
            assert_eq!(
                reviews.load(Ordering::SeqCst),
                usize::from(k8s && token == PROJECTED_TOKEN)
            );
        }
    }
    shutdown.cancel();
    while let Some(result) = servers.join_next().await {
        result.unwrap();
    }
}

pub(super) async fn backend() -> (
    BackendClients,
    Arc<AtomicUsize>,
    tokio_util::sync::CancellationToken,
    tokio::task::JoinSet<()>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded_calls = calls.clone();
    // Acknowledging backend: success and the count prove a real handler forwarded
    // the request; denial must leave it untouched. No manager/database is needed.
    let backend = Router::new().fallback(any(move || {
        let calls = recorded_calls.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(Body::from(vec![0u8; 5]))
                .expect("valid gRPC acknowledgement")
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let address = listener.local_addr().expect("backend address");
    let shutdown = tokio_util::sync::CancellationToken::new();
    let stopped = shutdown.clone();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move {
        axum::serve(listener, backend)
            .with_graceful_shutdown(stopped.cancelled_owned())
            .await
            .expect("backend server");
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .expect("backend endpoint")
        .connect()
        .await
        .expect("connect backend");
    let clients = BackendClients {
        sbx: SandboxManagerGrpcClient::new(wrap_grpc_client_channel(channel.clone())),
        node: NodeManagerGrpcClient::new(wrap_grpc_client_channel(channel.clone())),
        tmpl: TemplateManagerGrpcClient::new(wrap_grpc_client_channel(channel.clone())),
        build_mgr: pplx_space_build_mgr_client::client_with_token(channel, STATIC_TOKEN)
            .expect("backend bearer"),
    };
    (clients, calls, shutdown, servers)
}
