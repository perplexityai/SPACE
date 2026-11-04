use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use pplx_space_common_core::types::NodeId;
use pplx_space_common_svc::auth::NodeRole;
use pplx_space_template_mgr_client::proto as tmpl;
use rstest::rstest;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, transport::Server};

use super::policy_auth_tests::{PROJECTED_TOKEN, STATIC_TOKEN, auth_layer, backend, bearer};
use crate::{
    builder::BuilderApiService,
    proto::{
        PullTemplateRequest, builder_api_client::BuilderApiClient,
        worker_api_client::WorkerApiClient, worker_api_server::WorkerApiServer,
    },
    worker::WorkerApiService,
};

#[derive(Clone, Copy, Debug)]
enum TemplateRpc {
    Export,
    Pull,
}

#[rstest]
#[case::controller(true, "space-api", PROJECTED_TOKEN, true)]
#[case::transfer_only(true, "workers", PROJECTED_TOKEN, true)]
#[case::policy_only(true, "policy-only", PROJECTED_TOKEN, false)]
#[case::observer(true, "observer", PROJECTED_TOKEN, false)]
#[case::guest_bridge(true, "space-data-plane", PROJECTED_TOKEN, false)]
#[case::static_fallback(true, "space-api", STATIC_TOKEN, false)]
#[case::static_only(false, "space-api", STATIC_TOKEN, true)]
#[tokio::test]
async fn template_transfer_authority_survives_the_wire(
    #[case] k8s: bool,
    #[case] username: &'static str,
    #[case] token: &'static str,
    #[case] transfer: bool,
    #[values(NodeRole::Worker, NodeRole::Builder)] role: NodeRole,
    #[values(TemplateRpc::Export, TemplateRpc::Pull)] rpc: TemplateRpc,
) {
    let (clients, calls, shutdown, mut servers) = backend().await;
    let _cancel_on_drop = shutdown.clone().drop_guard();
    let auth = auth_layer(role, k8s, username, Arc::new(AtomicUsize::new(0)));
    let routes =
        tonic::service::Routes::new(WorkerApiServer::new(WorkerApiService::new(clients.clone())))
            .add_service(
                BuilderApiService::new(clients, NodeId::new())
                    .into_grpc_service(Arc::new(tokio::sync::Semaphore::new(32))),
            );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stopped = shutdown.clone();
    servers.spawn(async move {
        Server::builder()
            .layer(auth)
            .add_routes(routes)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                stopped.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .timeout(std::time::Duration::from_secs(5))
        .connect()
        .await
        .unwrap();
    let mut worker = WorkerApiClient::new(channel.clone());
    let mut builder = BuilderApiClient::new(channel);

    macro_rules! call {
        ($method:ident, $body:expr) => {{
            let mut request = bearer($body, token);
            // Caller metadata must not substitute for authenticated authority.
            request.metadata_mut().insert(
                "x-space-template-transfer-authority",
                "granted".parse().unwrap(),
            );
            match role {
                NodeRole::Worker => worker.$method(request).await.map(|_| ()),
                NodeRole::Builder => builder.$method(request).await.map(|_| ()),
                NodeRole::Cluster => unreachable!(),
            }
        }};
    }
    let result = match rpc {
        TemplateRpc::Export => call!(node_export_template, tmpl::ExportTemplateRequest::default()),
        TemplateRpc::Pull => call!(
            pull_template,
            PullTemplateRequest {
                template_id: "example-template".into(),
                content_hash: Some(format!("sha256:{}", "a".repeat(64))),
                skip_if_exists: true,
                sources: vec![crate::proto::TransferSource {
                    source: Some(crate::proto::transfer_source::Source::Node(
                        crate::proto::NodeSource {
                            address: "http://127.0.0.1:1".into(),
                        }
                    )),
                }],
                ..Default::default()
            }
        ),
    };

    assert_eq!(
        result.map_err(|status| status.code()),
        if transfer {
            Ok(())
        } else {
            Err(Code::PermissionDenied)
        },
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        usize::from(transfer),
        "unauthorized requests must not reach the backend",
    );

    drop(worker);
    drop(builder);
    shutdown.cancel();
    while let Some(result) = servers.join_next().await {
        result.unwrap();
    }
}
