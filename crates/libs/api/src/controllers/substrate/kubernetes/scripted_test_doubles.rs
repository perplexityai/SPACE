//! Scripted Kubernetes apiserver and node-api doubles for substrate tests.
//!
//! The apiserver double answers each request from a test-supplied closure and
//! records it. The node-api double serves `WorkerApi`, `WorkerApiInternal` and
//! gRPC health on an ephemeral loopback port: methods a test scripts are
//! answered from the script, every other method is forwarded to the node-api
//! simulator (or to [`SequencedWakeWorker`] for `WorkerApiInternal`), and
//! every call is recorded in one ordered log so tests can assert which node
//! steps ran and in what order.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use axum::http::{self, HeaderMap, HeaderValue, Method, StatusCode};
use futures_util::future::BoxFuture;
use http_body_util::BodyExt;
use hyper::body::{Bytes, Frame};
use kube::client::{Body as KubeBody, ClientBuilder};
use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternalServer;
use pplx_space_node_api_client::proto::worker_api_server::WorkerApiServer;
use pplx_space_node_api_sim::{SimState, SimWorkerApiService};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, server::TcpIncoming};

use super::tests::SequencedWakeWorker;
use super::*;

pub(super) type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The Kubernetes node the scripted node-api reports as its own.
pub(super) const WORKER_NODE: &str = "node-a";

// ── Scripted Kubernetes apiserver ───────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct KubeRequest {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) body: Option<serde_json::Value>,
}

pub(crate) enum KubeReply {
    Json(StatusCode, serde_json::Value),
    /// Never answer, for callers that bound the read with their own budget.
    Hang,
}

pub(crate) type KubeLog = Arc<Mutex<Vec<KubeRequest>>>;

/// A kube client whose every request is recorded and answered by `reply`.
pub(crate) fn scripted_kube_client(
    reply: impl Fn(&KubeRequest) -> KubeReply + Send + Sync + 'static,
) -> (Client, KubeLog) {
    scripted_kube_client_async(move |request| std::future::ready(reply(&request)))
}

/// [`scripted_kube_client`] for replies that must first observe or change
/// other test state, such as the database, at the moment a request arrives.
pub(super) fn scripted_kube_client_async<F, Fut>(reply: F) -> (Client, KubeLog)
where
    F: Fn(KubeRequest) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = KubeReply> + Send + 'static,
{
    let log: KubeLog = Arc::default();
    let reply = Arc::new(reply);
    let service_log = Arc::clone(&log);
    let service = tower::service_fn(move |req: http::Request<KubeBody>| {
        let log = Arc::clone(&service_log);
        let reply = Arc::clone(&reply);
        async move {
            let method = req.method().clone();
            let path = req.uri().path().to_string();
            let bytes = req.into_body().collect().await?.to_bytes();
            let body = (!bytes.is_empty())
                .then(|| serde_json::from_slice(&bytes))
                .transpose()?;
            let request = KubeRequest { method, path, body };
            log.lock().unwrap().push(request.clone());
            match reply(request).await {
                KubeReply::Json(status, body) => Ok::<_, BoxError>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(KubeBody::from(serde_json::to_vec(&body)?))?,
                ),
                KubeReply::Hang => std::future::pending().await,
            }
        }
    });
    (ClientBuilder::new(service, "space").build(), log)
}

pub(crate) fn kube_status(code: StatusCode, reason: &str) -> KubeReply {
    KubeReply::Json(
        code,
        serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": if code.is_success() { "Success" } else { "Failure" },
            "message": reason,
            "reason": reason,
            "code": code.as_u16(),
        }),
    )
}

pub(crate) fn pod_reply(pod: &Pod) -> KubeReply {
    KubeReply::Json(
        StatusCode::OK,
        serde_json::to_value(pod).expect("pod serializes"),
    )
}

pub(super) fn pod_path(pod_name: &str) -> String {
    format!("/api/v1/namespaces/space/pods/{pod_name}")
}

pub(super) fn requests_matching(log: &KubeLog, method: &Method) -> Vec<KubeRequest> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|request| request.method == *method)
        .cloned()
        .collect()
}

// ── Scripted node-api ───────────────────────────────────────────────────────

/// One node-api call, named `Service/Method` without the proto package (for
/// example `WorkerApi/UpdateSandbox` or `Health/Check`), with its encoded
/// request message.
#[derive(Debug, Clone)]
pub(super) struct GrpcCall {
    pub(super) method: String,
    pub(super) message: Bytes,
}

type CallLog = Arc<Mutex<Vec<GrpcCall>>>;

#[derive(Clone)]
pub(super) enum ScriptedReply {
    Status(tonic::Status),
    Message(Bytes),
    /// Never answer, for callers that bound the call with their own budget.
    Hang,
    /// Answers the n-th call to its method with the n-th reply and every
    /// later call with the last one. An empty sequence scripts nothing.
    Sequence(Arc<[ScriptedReply]>),
}

impl ScriptedReply {
    pub(super) fn message(message: &impl prost::Message) -> Self {
        Self::Message(Bytes::from(message.encode_to_vec()))
    }

    /// The reply to the call numbered `attempt`, counting from zero.
    fn for_attempt(&self, attempt: usize) -> &Self {
        match self {
            Self::Sequence(replies) => replies
                .get(attempt.min(replies.len().saturating_sub(1)))
                .map_or(self, |reply| reply.for_attempt(attempt)),
            reply => reply,
        }
    }
}

/// Runs when a call to its method arrives, before the call is answered, so a
/// test can observe other state, such as the database, at that moment.
pub(super) type CallObserver = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Records every call to `inner`, answers the scripted ones itself and
/// forwards the rest.
#[derive(Clone)]
struct RecordingGrpcFront<S> {
    inner: S,
    replies: Arc<HashMap<&'static str, ScriptedReply>>,
    observers: Arc<HashMap<&'static str, CallObserver>>,
    calls: CallLog,
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for RecordingGrpcFront<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> tower::Service<http::Request<tonic::body::Body>> for RecordingGrpcFront<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let mut segments = request.uri().path().trim_start_matches('/').split('/');
        let service = segments.next().unwrap_or_default();
        let service = service.rsplit('.').next().unwrap_or(service);
        let method = format!("{service}/{}", segments.next().unwrap_or_default());
        let reply = self.replies.get(method.as_str()).cloned();
        let observer = self.observers.get(method.as_str()).cloned();
        let calls = Arc::clone(&self.calls);
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let framed = body
                .collect()
                .await
                .map(|body| body.to_bytes())
                .unwrap_or_default();
            let attempt = {
                let mut calls = calls.lock().unwrap();
                let attempt = calls.iter().filter(|call| call.method == method).count();
                // Strip the 5-byte gRPC length-prefix frame header.
                calls.push(GrpcCall {
                    method,
                    message: framed.slice(5.min(framed.len())..),
                });
                attempt
            };
            if let Some(observer) = observer {
                observer().await;
            }
            match reply.as_ref().map(|reply| reply.for_attempt(attempt)) {
                Some(ScriptedReply::Status(status)) => Ok(status.clone().into_http()),
                Some(ScriptedReply::Message(message)) => Ok(grpc_unary_ok(message.clone())),
                Some(ScriptedReply::Hang) => std::future::pending().await,
                Some(ScriptedReply::Sequence(_)) | None => {
                    let request = http::Request::from_parts(
                        parts,
                        tonic::body::Body::new(http_body_util::Full::new(framed)),
                    );
                    tower::Service::call(&mut inner, request).await
                }
            }
        })
    }
}

fn grpc_unary_ok(message: Bytes) -> http::Response<tonic::body::Body> {
    let mut framed = Vec::with_capacity(message.len() + 5);
    framed.push(0);
    framed.extend_from_slice(
        &u32::try_from(message.len())
            .expect("scripted message fits a gRPC frame")
            .to_be_bytes(),
    );
    framed.extend_from_slice(&message);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let frames = futures_util::stream::iter([
        Ok::<_, Infallible>(Frame::data(Bytes::from(framed))),
        Ok(Frame::trailers(trailers)),
    ]);
    http::Response::builder()
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::new(http_body_util::StreamBody::new(
            frames,
        )))
        .expect("static gRPC response builds")
}

pub(super) struct ScriptedWorkerConfig {
    /// Replies keyed by `Service/Method`.
    pub(super) replies: HashMap<&'static str, ScriptedReply>,
    /// Observers keyed by `Service/Method`, for scripted and forwarded calls
    /// alike.
    pub(super) observers: HashMap<&'static str, CallObserver>,
    pub(super) advertise_operation_receipts: bool,
    /// Unscripted `WorkerApiInternal/WakeSandbox` calls go to a
    /// [`SequencedWakeWorker`] that echoes the requested route identity with
    /// the worker's node id from this attempt on, and reports the runtime not
    /// ready before it.
    pub(super) wake_succeeds_from_attempt: usize,
}

impl ScriptedWorkerConfig {
    pub(super) fn new(replies: impl IntoIterator<Item = (&'static str, ScriptedReply)>) -> Self {
        Self {
            replies: replies.into_iter().collect(),
            observers: HashMap::new(),
            advertise_operation_receipts: true,
            wake_succeeds_from_attempt: 1,
        }
    }
}

pub(super) struct ScriptedWorker {
    pub(super) port: u16,
    /// State behind every forwarded `WorkerApi` method.
    pub(super) sim: SimState,
    calls: CallLog,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for ScriptedWorker {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl ScriptedWorker {
    /// Encoded request messages of every call to `method`, in arrival order.
    pub(super) fn received(&self, method: &str) -> Vec<Bytes> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == method)
            .map(|call| call.message.clone())
            .collect()
    }

    /// Arrival index of the first call to `method`.
    pub(super) fn first_call(&self, method: &str) -> Option<usize> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .position(|call| call.method == method)
    }
}

pub(super) async fn start_scripted_worker(
    config: ScriptedWorkerConfig,
) -> Result<ScriptedWorker, BoxError> {
    let dir = tempfile::tempdir()?;
    let sim = SimState::new(
        dir.path().join("snapshots"),
        dir.path().join("sandboxes"),
        WORKER_NODE.to_string(),
    );
    let calls: CallLog = Arc::default();
    let replies = Arc::new(config.replies);
    let observers = Arc::new(config.observers);
    let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
    health_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE,
            tonic_health::ServingStatus::Serving,
        )
        .await;
    health_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
            if config.advertise_operation_receipts {
                tonic_health::ServingStatus::Serving
            } else {
                tonic_health::ServingStatus::NotServing
            },
        )
        .await;
    let health = RecordingGrpcFront {
        inner: health_service,
        replies: Arc::clone(&replies),
        observers: Arc::clone(&observers),
        calls: Arc::clone(&calls),
    };
    let worker_api = RecordingGrpcFront {
        inner: WorkerApiServer::new(SimWorkerApiService::new(sim.clone())),
        replies: Arc::clone(&replies),
        observers: Arc::clone(&observers),
        calls: Arc::clone(&calls),
    };
    let internal_api = RecordingGrpcFront {
        inner: WorkerApiInternalServer::new(SequencedWakeWorker {
            wake_attempts: Arc::new(AtomicUsize::new(0)),
            succeed_after_attempt: Some(config.wake_succeeds_from_attempt),
            echo_route_identity: true,
            received: Arc::default(),
        }),
        replies: Arc::clone(&replies),
        observers: Arc::clone(&observers),
        calls: Arc::clone(&calls),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        let result = Server::builder()
            .add_service(health)
            .add_service(worker_api)
            .add_service(internal_api)
            .serve_with_incoming_shutdown(
                TcpIncoming::from(listener).with_nodelay(Some(true)),
                server_shutdown.cancelled_owned(),
            )
            .await;
        if let Err(error) = result {
            tracing::warn!(%error, "scripted worker server exited with error");
        }
    });
    Ok(ScriptedWorker {
        port,
        sim,
        calls,
        shutdown,
        task,
        _dir: dir,
    })
}
