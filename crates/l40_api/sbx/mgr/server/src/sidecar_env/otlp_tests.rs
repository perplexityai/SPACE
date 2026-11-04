//! Exercise the cleared child environment against a real OTLP receiver.
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use std::process::Command;
use std::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;

const PHASE: &str = "SPACE_SIDECAR_IDENTITY_TEST_PHASE";
const TEST: &str =
    "sidecar_env::otlp_tests::cleared_sidecar_environment_preserves_otlp_producer_identity";

struct Receiver(tokio::sync::mpsc::Sender<ExportMetricsServiceRequest>);

#[tonic::async_trait]
impl MetricsService for Receiver {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.0
            .try_send(request.into_inner())
            .expect("bounded fixture receiver");
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[test]
fn cleared_sidecar_environment_preserves_otlp_producer_identity() {
    if std::env::var(PHASE).as_deref() == Ok("manager") {
        // Match the launcher's env_clear boundary, using the production map.
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env_clear()
            .envs(super::telemetry_env_for_service("space-spaced-host"))
            .env(PHASE, "sidecar")
            .output()
            .expect("launch sidecar fixture");
        assert!(output.status.success(), "{output:?}");
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if std::env::var(PHASE).as_deref() == Ok("sidecar") {
                assert_eq!(std::env::var("OTEL_SERVICE_NAME").unwrap(), "manager-canary");
                assert_eq!(std::env::var("OTEL_RESOURCE_ATTRIBUTES").unwrap(),
                    "team=infra,deployment.environment.name=testing,service.version=revision,service.instance.id=shared");
                let guard = pplx_space_observability::init("space-spaced-host").unwrap();
                pplx_space_metrics::grpc::server_active_requests(1);
                tokio::task::spawn_blocking(move || drop(guard))
                    .await
                    .unwrap();
                return;
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (tx, mut rx) = tokio::sync::mpsc::channel(8);
            let shutdown = tokio_util::sync::CancellationToken::new();
            let token = shutdown.clone();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(MetricsServiceServer::new(Receiver(tx)))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        token.cancelled(),
                    )
                    .await
                    .unwrap();
            });
            for node in ["node-a", "node-b"] {
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .args(["--exact", TEST, "--nocapture"])
                    .env_clear()
                    .env(PHASE, "manager")
                    .env("SPACE_NODE_ID", node)
                    .env("OTEL_SERVICE_NAME", "manager-canary")
                    .env("OTEL_RESOURCE_ATTRIBUTES", "team=infra,deployment.environment.name=testing,service.version=revision,service.instance.id=shared")
                    .env("SPACE_METRICS_SINK", "otlp")
                    .env("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", &endpoint);
                let output = tokio::task::spawn_blocking(move || command.output().unwrap())
                    .await
                    .unwrap();
                assert!(output.status.success(), "{output:?}");
                let request = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                    .await
                    .expect("sidecar exported")
                    .expect("receiver open");
                let resource = request
                    .resource_metrics
                    .first()
                    .unwrap()
                    .resource
                    .as_ref()
                    .unwrap();
                let string_attribute = |key: &str| {
                    let attribute = resource.attributes.iter().find(|a| a.key == key).unwrap();
                    let Some(Value::StringValue(value)) = &attribute.value.as_ref().unwrap().value
                    else {
                        panic!("string resource attribute {key}");
                    };
                    value.as_str()
                };
                assert_eq!(string_attribute("service.name"), "space-spaced-host-canary");
                assert_eq!(string_attribute("team"), "infra");
                assert_eq!(string_attribute("deployment.environment.name"), "testing");
                assert_eq!(string_attribute("service.version"), "revision");
                let (actual_node, pid) = string_attribute("service.instance.id")
                    .rsplit_once(':')
                    .unwrap();
                assert_eq!(
                    actual_node, node,
                    "node identity must survive env_clear independently of PID"
                );
                assert!(pid.parse::<u32>().unwrap() > 0);
            }
            shutdown.cancel();
            server.await.unwrap();
        });
}
