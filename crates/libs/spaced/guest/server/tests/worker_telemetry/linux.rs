use std::os::unix::fs::MetadataExt;
use std::panic::AssertUnwindSafe;
use std::process::Stdio;
use std::time::Duration;

use futures_util::FutureExt as _;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use pplx_space_common_core::types::{CLUSTER_ID_ENV, GuestUserName};
use pplx_space_spaced_common::proto::bridge_guest::{
    WriteBatchEntry, WriteBatchRequest, filesystem_client::FilesystemClient,
};
use pplx_space_spaced_guest_server::{
    FilesystemExecutionMode, HostConnectorType, SpacedGuestRunConfig, SpacedGuestService,
    SpacedGuestServiceConfig, filesystem_worker_requested,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

/// The worker's own environment is cleared, so this value only reaches its
/// exports when the launcher forwards the canonical cluster-id variable.
const FIXTURE_CLUSTER_ID: &str = "worker-fixture-cluster";
/// Export cadence asked of the worker, well under the production default so
/// the negative modes can be judged over several intervals rather than one.
const EXPORT_INTERVAL: Duration = Duration::from_millis(250);
/// How long a mode that must not export is watched. Twelve export intervals:
/// a worker that exports at all has had many chances to.
const SILENCE_WINDOW: Duration = Duration::from_secs(3);

pub(super) fn main() {
    if filesystem_worker_requested() {
        pplx_space_spaced_guest_server::run(SpacedGuestRunConfig::default()).expect("worker entry");
        return;
    }
    let args: Vec<_> = std::env::args().skip(1).collect();
    let has_arg = |name| args.iter().any(|arg| arg == name);
    if has_arg("--list") {
        // This sole test belongs to both the complete and ignored inventories.
        println!("worker_telemetry: test");
        return;
    }
    let is_case = has_arg("--case");
    // CI includes ignored tests. Nextest may invoke a selected test by exact
    // name; internal case children must also bypass the default ignore gate.
    if !is_case
        && !has_arg("--ignored")
        && !has_arg("--include-ignored")
        && !(has_arg("--exact") && has_arg("worker_telemetry"))
    {
        println!("test worker_telemetry ... ignored, requires root for the real credential switch");
        println!("\ntest result: ok. 0 passed; 0 failed; 1 ignored");
        return;
    }
    assert!(
        nix::unistd::Uid::effective().is_root(),
        "worker_telemetry needs root for the real credential switch; run with \
         --config 'target.x86_64-unknown-linux-gnu.runner=[\"sudo\",\"-n\"]' -- --ignored"
    );
    tokio::runtime::Runtime::new()
        .expect("fixture runtime")
        .block_on(async {
            if is_case {
                run_case().await;
            } else {
                run_suite().await;
            }
        });
}

struct Receiver {
    exports: tokio::sync::mpsc::Sender<ExportMetricsServiceRequest>,
    stall: bool,
}

#[tonic::async_trait]
impl MetricsService for Receiver {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.exports
            .try_send(request.into_inner())
            .expect("bounded fixture exports");
        if self.stall {
            std::future::pending::<()>().await;
        }
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

async fn run_suite() {
    // Every mode binds its own port-0 UDP socket and OTLP listener and drives
    // its own child, and each child holds a private temporary directory and
    // reaches its identity worker over an inherited socketpair. Nothing is
    // shared, so the modes run together and the suite waits out one real
    // reader tick rather than five.
    let outcomes =
        futures_util::future::join_all(["otlp", "stalled", "auto", "off", "invalid"].map(
            |mode| async move { (mode, AssertUnwindSafe(run_mode(mode)).catch_unwind().await) },
        ))
        .await;
    // Concurrent output no longer identifies the mode that panicked, and that
    // is the first thing a reader needs, so name every failing mode here.
    let failed: Vec<_> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.is_err())
        .map(|(mode, _)| *mode)
        .collect();
    assert!(
        failed.is_empty(),
        "worker telemetry modes failed: {failed:?}"
    );
}

async fn run_mode(mode: &'static str) {
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("UDP receiver");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("OTLP listener");
    let endpoint = format!("http://{}", listener.local_addr().expect("OTLP address"));
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(Receiver {
                exports: tx,
                stall: mode == "stalled",
            }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), token.cancelled())
            .await
            .expect("serve OTLP receiver");
    });
    let mut command =
        tokio::process::Command::new(std::env::current_exe().expect("fixture executable"));
    command
        .arg("--case")
        .env_clear()
        .env(
            "SPACE_METRICS_SINK",
            if matches!(mode, "stalled" | "invalid") {
                "otlp"
            } else {
                mode
            },
        )
        .env("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
        .env("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
        .env(
            "OTEL_METRIC_EXPORT_INTERVAL",
            EXPORT_INTERVAL.as_millis().to_string(),
        )
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "unsupported")
        .env("DD_AGENT_HOST", "127.0.0.1")
        .env(
            "DD_DOGSTATSD_PORT",
            udp.local_addr().expect("UDP address").port().to_string(),
        )
        .env("HOSTNAME", "worker-fixture-guest")
        .env(CLUSTER_ID_ENV, FIXTURE_CLUSTER_ID)
        .env("SPACE_LOG_FORMAT", "json")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if mode == "invalid" {
        // This must reach the child validator, not silently disappear.
        command.env(
            "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
            "authorization=fixture-only",
        );
    }
    let mut child = command.spawn().expect("launch guest fixture");
    let mut lines = BufReader::new(child.stdout.take().expect("fixture stdout")).lines();
    let ready = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(line) = lines.next_line().await.expect("read fixture readiness") {
            if line == "WORKER_READY" {
                return true;
            }
        }
        false
    })
    .await;
    if mode == "invalid" {
        // An explicitly selected sink whose configuration can never work now
        // fails the worker's startup instead of leaving it serving with
        // telemetry silently disabled.
        assert!(
            !ready.expect("fixture readiness deadline"),
            "{mode}: worker started with an unusable metrics configuration"
        );
        let status = tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .expect("fixture shutdown deadline")
            .expect("reap guest fixture");
        assert!(!status.success(), "{mode}: startup must reject the setting");
        let mut datagram = [0; 2048];
        assert_eq!(
            udp.try_recv(&mut datagram)
                .expect_err("unselected native sink")
                .kind(),
            std::io::ErrorKind::WouldBlock,
            "{mode}: no DogStatsD fallback"
        );
        shutdown.cancel();
        server.abort();
        if let Err(error) = server.await {
            assert!(error.is_cancelled());
        }
        return;
    }
    // Real reader ticks are part of this integration contract: the child is
    // alive and serving throughout, rather than only exporting at exit. The
    // request-depth sampler runs on its own one-second cadence inside the
    // worker, so an early export can legitimately carry no depth sample;
    // every export is checked for the worker's identity and the first one
    // carrying the sampled distribution is the one the assertions below read.
    // Without Datadog compiled in, auto resolves OTLP and exports like an
    // explicit otlp selection; with Datadog it emits over DogStatsD instead.
    let expects_otlp_export =
        matches!(mode, "otlp" | "stalled") || (mode == "auto" && !cfg!(feature = "datadog"));
    let exported = if expects_otlp_export {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let request = rx.recv().await?;
                assert_worker_identity(&request);
                if request_depth_metric(&request).is_some() {
                    return Some(request);
                }
            }
        })
        .await
        .ok()
        .flatten()
    } else {
        tokio::time::timeout(SILENCE_WINDOW, rx.recv())
            .await
            .ok()
            .flatten()
    };
    // Always ask the fixture to reap its worker before asserting outcomes.
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(b"stop\n")
            .await
            .expect("request fixture cleanup");
    }
    let status = tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .expect("fixture shutdown deadline")
        .expect("reap guest fixture");
    shutdown.cancel();
    server.abort();
    if let Err(error) = server.await {
        assert!(error.is_cancelled());
    }
    assert!(
        ready.expect("fixture readiness deadline"),
        "{mode}: file operation completed"
    );
    assert!(
        status.success(),
        "{mode}: file operation during export and worker cleanup"
    );
    let mut datagram = [0; 2048];
    let native = udp.try_recv(&mut datagram);
    // auto resolves Datadog when it is compiled in, and OTLP otherwise, so the
    // native socket only sees traffic in a Datadog build.
    if mode == "auto" && cfg!(feature = "datadog") {
        assert!(native.is_ok(), "default Datadog emission preserved");
    } else {
        assert_eq!(
            native.expect_err("unselected native sink").kind(),
            std::io::ErrorKind::WouldBlock,
            "{mode}: no DogStatsD fallback"
        );
    }
    if expects_otlp_export {
        let request = exported.expect("worker must export while serving");
        let metric = request_depth_metric(&request).expect("worker request distribution");
        // A depth is not dimensionless; declaring UCUM `1` published it with a
        // `_ratio` suffix on a Prometheus-style backend.
        assert_eq!(metric.unit, "");
        let Some(Data::ExponentialHistogram(histogram)) = &metric.data else {
            panic!("request depth remains a histogram");
        };
        // This is a one-second concurrency sample, not one observation per
        // RPC. The completed write leaves the worker idle at depth zero.
        let sample = histogram
            .data_points
            .iter()
            .find(|p| p.count > 0 && p.min == Some(0.0) && p.max.is_some_and(|max| max <= 1.0))
            .expect("idle concurrency sample");
        assert_eq!(
            string_attribute(&sample.attributes, "cluster_id"),
            FIXTURE_CLUSTER_ID
        );
    } else {
        assert!(exported.is_none(), "{mode}: no OTLP emission");
    }
    println!("worker telemetry {mode}: passed");
}

/// The exported resource must be the re-exec'd worker, never the fixture
/// parent that launched it: the parent has its own process identity and its
/// own DogStatsD configuration, so an export carrying the parent's resource
/// would prove nothing about the worker.
fn assert_worker_identity(request: &ExportMetricsServiceRequest) {
    let resource_metrics = request
        .resource_metrics
        .first()
        .expect("exported resource metrics");
    let attributes = &resource_metrics
        .resource
        .as_ref()
        .expect("resource identity")
        .attributes;
    let string_value = |key: &str| string_attribute(attributes, key);
    assert_eq!(string_value("service.name"), "space-spaced-guest-worker");
    assert!(string_value("service.instance.id").starts_with("worker-fixture-guest:"));
    // Carry cluster identity on the resource as well as metric points, even
    // when deployments share an environment label.
    assert_eq!(string_value("space.cluster.id"), FIXTURE_CLUSTER_ID);
    assert!(attributes.iter().all(|a| a.key != "k8s.cluster.name"));
}

fn request_depth_metric(
    request: &ExportMetricsServiceRequest,
) -> Option<&opentelemetry_proto::tonic::metrics::v1::Metric> {
    request
        .resource_metrics
        .iter()
        .flat_map(|r| &r.scope_metrics)
        .flat_map(|s| &s.metrics)
        .find(|m| m.name == "space.grpc.server.active_requests")
}

fn string_attribute<'a>(attributes: &'a [KeyValue], key: &str) -> &'a str {
    let value = attributes
        .iter()
        .find(|a| a.key == key)
        .expect("required identity attribute")
        .value
        .as_ref()
        .expect("identity attribute value");
    let Some(Value::StringValue(value)) = &value.value else {
        panic!("string {key}");
    };
    value.as_str()
}

async fn run_case() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let user = nix::unistd::User::from_name("nobody")
        .expect("look up worker account")
        .expect("unprivileged fixture account");
    nix::unistd::chown(directory.path(), Some(user.uid), Some(user.gid))
        .expect("give the worker its private fixture directory");
    let socket = directory.path().join("guest.sock");
    let shutdown = CancellationToken::new();
    let service = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            default_operation_user: GuestUserName::parse(user.name)
                .expect("valid worker account name"),
            filesystem_execution: FilesystemExecutionMode::IdentityWorkers,
            host_connector_type: HostConnectorType::Uds(directory.path().join("unused.sock")),
            local_socket_path: None,
            bridge_socket_path: Some(socket.clone()),
            ..Default::default()
        },
    )
    .expect("guest service");
    service.ready();
    let serving = tokio::spawn(service.clone().run());
    assert!(service.wait_until_listening().await);
    let channel = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let path = socket.clone();
            let result = tonic::transport::Endpoint::from_static("http://[::]:50051")
                .connect_with_connector(tower::service_fn(move |_| {
                    let path = path.clone();
                    async move {
                        tokio::net::UnixStream::connect(path)
                            .await
                            .map(hyper_util::rt::TokioIo::new)
                    }
                }))
                .await;
            if let Ok(channel) = result {
                break channel;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("guest socket ready");
    let mut client = FilesystemClient::new(channel);
    for index in 0..2 {
        let path = directory.path().join(format!("owned-{index}"));
        tokio::time::timeout(
            Duration::from_secs(5),
            client.write_batch(WriteBatchRequest {
                entries: vec![WriteBatchEntry {
                    path: path.display().to_string(),
                    data: b"worker data".to_vec(),
                    append: false,
                }],
                idempotency_key: None,
                user: None,
            }),
        )
        .await
        .expect("file operation is independent of metric receiver")
        .expect("worker write succeeded");
        assert_eq!(
            tokio::fs::read(&path).await.expect("read worker file"),
            b"worker data"
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("worker file metadata")
                .uid(),
            user.uid.as_raw(),
            "credential switch preserved"
        );
        if index == 0 {
            println!("WORKER_READY");
            tokio::task::spawn_blocking(|| {
                let mut line = String::new();
                std::io::stdin()
                    .read_line(&mut line)
                    .expect("fixture shutdown request");
            })
            .await
            .expect("join shutdown listener");
        }
    }
    drop(client);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(12), serving)
        .await
        .expect("guest shutdown deadline")
        .expect("guest service joined")
        .expect("guest service stopped");
}
