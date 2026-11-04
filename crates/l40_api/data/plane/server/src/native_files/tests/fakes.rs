//! A scripted lifecycle API and sandbox runtime for the files routes.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::Stream;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_common_svc::space_api_route_ed25519::SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY as ROUTE_TOKEN_METADATA;
use pplx_space_node_api_client::proto::{
    AuthorizeNativeFileRequestRequest, AuthorizeNativeFileRequestResponse,
    DataPlaneBackendEndpoint, NativeFileAuthorization, NativeFileRequest, NativeFileRouteGrant,
    NativeHttpResponse, PrepareNativeFileCallRequest, PrepareNativeFileCallResponse,
    SpaceApiRouteToken, authorize_native_file_request_response,
    native_file_access_api_server::{NativeFileAccessApi, NativeFileAccessApiServer},
    prepare_native_file_call_response,
};
use pplx_space_sbx_mgr_client::operations::{FileChunk, FileInfo, StatResponse};
use pplx_space_sbx_mgr_client::proto::{SpacedListRequest, SpacedReadRequest, SpacedStatRequest};
use tonic::{Request, Response, Status};

pub(super) const ROUTE_TOKEN: &str = "ed25519.test.cGF5bG9hZA.c2lnbmF0dXJl";

async fn serve<S>(service: S) -> SocketAddr
where
    S: tower::Service<
            axum::http::Request<tonic::body::Body>,
            Response = axum::http::Response<tonic::body::Body>,
            Error = Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    addr
}

/// One RPC the fake API answered.
#[derive(Clone, Debug)]
pub(super) struct AccessCall {
    pub(super) rpc: &'static str,
    pub(super) request: NativeFileRequest,
    pub(super) fingerprint: Option<String>,
    pub(super) bearer: Option<String>,
}

impl AccessCall {
    pub(super) fn header(&self, name: &str) -> Option<&[u8]> {
        self.request
            .headers
            .iter()
            .find(|header| header.name == name)
            .map(|header| header.value.as_slice())
    }
}

/// What a fake API RPC answers.
pub(super) enum Answer<T> {
    Ok(T),
    Refuse(NativeHttpResponse),
    Fail(Status),
}

#[derive(Default)]
struct ApiScript {
    authorize: VecDeque<Answer<NativeFileAuthorization>>,
    prepare: VecDeque<Answer<NativeFileRouteGrant>>,
    calls: Vec<AccessCall>,
}

/// The lifecycle API's native file access RPCs. Unscripted calls authorize
/// and grant the route to [`FakeApi::worker`].
#[derive(Clone)]
pub(super) struct FakeApi {
    script: Arc<Mutex<ApiScript>>,
    pub(super) org_id: OrgId,
    pub(super) sandbox_id: SandboxId,
    pub(super) worker: SocketAddr,
}

impl FakeApi {
    pub(super) async fn start(worker: SocketAddr) -> (Self, SocketAddr) {
        let api = Self {
            script: Arc::default(),
            org_id: OrgId::new(),
            sandbox_id: SandboxId::new(),
            worker,
        };
        let addr = serve(NativeFileAccessApiServer::new(api.clone())).await;
        (api, addr)
    }

    pub(super) fn authorize(&self, answer: Answer<NativeFileAuthorization>) {
        self.script.lock().unwrap().authorize.push_back(answer);
    }

    pub(super) fn prepare(&self, answer: Answer<NativeFileRouteGrant>) {
        self.script.lock().unwrap().prepare.push_back(answer);
    }

    pub(super) fn calls(&self) -> Vec<AccessCall> {
        self.script.lock().unwrap().calls.clone()
    }

    pub(super) fn grant(&self) -> NativeFileRouteGrant {
        NativeFileRouteGrant {
            authorization: Some(self.authorization()),
            sandbox_id: self.sandbox_id.to_string(),
            backend: Some(DataPlaneBackendEndpoint {
                node_id: String::new(),
                endpoint: format!("http://{}", self.worker),
                transport: Some("grpc".to_owned()),
            }),
            space_api_route_token: Some(SpaceApiRouteToken {
                token: ROUTE_TOKEN.to_owned(),
            }),
            guest_idempotency_key: None,
        }
    }

    fn authorization(&self) -> NativeFileAuthorization {
        NativeFileAuthorization {
            org_id: self.org_id.to_string(),
            owner_user_id: None,
        }
    }

    fn record<T>(
        &self,
        rpc: &'static str,
        request: &Request<T>,
        file: Option<NativeFileRequest>,
        fingerprint: Option<String>,
    ) {
        let bearer = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        self.script.lock().unwrap().calls.push(AccessCall {
            rpc,
            request: file.unwrap_or_default(),
            fingerprint,
            bearer,
        });
    }
}

#[tonic::async_trait]
impl NativeFileAccessApi for FakeApi {
    async fn authorize_native_file_request(
        &self,
        request: Request<AuthorizeNativeFileRequestRequest>,
    ) -> Result<Response<AuthorizeNativeFileRequestResponse>, Status> {
        use authorize_native_file_request_response::Outcome;
        self.record(
            "authorize",
            &request,
            request.get_ref().request.clone(),
            None,
        );
        let answer = self.script.lock().unwrap().authorize.pop_front();
        let outcome = match answer.unwrap_or_else(|| Answer::Ok(self.authorization())) {
            Answer::Ok(authorization) => Outcome::Authorized(authorization),
            Answer::Refuse(response) => Outcome::Refused(response),
            Answer::Fail(status) => return Err(status),
        };
        Ok(Response::new(AuthorizeNativeFileRequestResponse {
            outcome: Some(outcome),
        }))
    }

    async fn prepare_native_file_call(
        &self,
        request: Request<PrepareNativeFileCallRequest>,
    ) -> Result<Response<PrepareNativeFileCallResponse>, Status> {
        use prepare_native_file_call_response::Outcome;
        self.record(
            "prepare",
            &request,
            request.get_ref().request.clone(),
            request.get_ref().request_fingerprint.clone(),
        );
        let answer = self.script.lock().unwrap().prepare.pop_front();
        let outcome = match answer.unwrap_or_else(|| Answer::Ok(self.grant())) {
            Answer::Ok(grant) => Outcome::Granted(grant),
            Answer::Refuse(response) => Outcome::Refused(response),
            Answer::Fail(status) => return Err(status),
        };
        Ok(Response::new(PrepareNativeFileCallResponse {
            outcome: Some(outcome),
        }))
    }
}

/// One worker call the fake runtime received.
#[derive(Clone, Debug)]
pub(super) struct WorkerCall {
    pub(super) route_token: Option<String>,
    pub(super) request: WorkerRequest,
}

#[derive(Clone, Debug)]
pub(super) enum WorkerRequest {
    Stat(SpacedStatRequest),
    List(SpacedListRequest),
    Read(SpacedReadRequest),
}

/// What a fake runtime call answers: a message or a stream of them.
pub(super) enum Reply {
    Stat(Result<StatResponse, Status>),
    List(Result<Vec<Result<FileInfo, Status>>, Status>),
    Read(Result<Vec<Result<FileChunk, Status>>, Status>),
}

#[derive(Default)]
struct WorkerScript {
    replies: VecDeque<Reply>,
    calls: Vec<WorkerCall>,
}

/// A sandbox runtime that answers the `WorkerApi` calls the files routes
/// make, from a script, in order. A call with no reply left, or whose next
/// reply is for another call, fails `UNIMPLEMENTED`.
#[derive(Clone, Default)]
pub(super) struct FakeWorker {
    script: Arc<Mutex<WorkerScript>>,
}

impl FakeWorker {
    pub(super) async fn start() -> (Self, SocketAddr) {
        let worker = Self::default();
        let addr = serve(worker.clone()).await;
        (worker, addr)
    }

    pub(super) fn reply(&self, reply: Reply) {
        self.script.lock().unwrap().replies.push_back(reply);
    }

    pub(super) fn calls(&self) -> Vec<WorkerCall> {
        self.script.lock().unwrap().calls.clone()
    }

    /// Replies scripted and not yet sent.
    pub(super) fn unanswered(&self) -> usize {
        self.script.lock().unwrap().replies.len()
    }

    fn take<Req>(&self, request: &Request<Req>, seen: WorkerRequest) -> Option<Reply> {
        let route_token = request
            .metadata()
            .get(ROUTE_TOKEN_METADATA)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut script = self.script.lock().unwrap();
        script.calls.push(WorkerCall {
            route_token,
            request: seen,
        });
        script.replies.pop_front()
    }
}

impl tonic::server::NamedService for FakeWorker {
    const NAME: &'static str = "space.node.api.v1.WorkerApi";
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

impl tower::Service<axum::http::Request<tonic::body::Body>> for FakeWorker {
    type Response = axum::http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: axum::http::Request<tonic::body::Body>) -> Self::Future {
        let worker = self.clone();
        Box::pin(async move {
            Ok(match request.uri().path() {
                "/space.node.api.v1.WorkerApi/SpacedStat" => {
                    grpc()
                        .unary(
                            Unary(move |request: Request<SpacedStatRequest>| {
                                match worker
                                    .take(&request, WorkerRequest::Stat(request.get_ref().clone()))
                                {
                                    Some(Reply::Stat(reply)) => reply,
                                    _ => Err(Status::unimplemented("unscripted stat")),
                                }
                            }),
                            request,
                        )
                        .await
                }
                "/space.node.api.v1.WorkerApi/SpacedList" => {
                    grpc()
                        .server_streaming(
                            Streaming(move |request: Request<SpacedListRequest>| {
                                match worker
                                    .take(&request, WorkerRequest::List(request.get_ref().clone()))
                                {
                                    Some(Reply::List(reply)) => reply,
                                    _ => Err(Status::unimplemented("unscripted list")),
                                }
                            }),
                            request,
                        )
                        .await
                }
                "/space.node.api.v1.WorkerApi/SpacedRead" => {
                    grpc()
                        .server_streaming(
                            Streaming(move |request: Request<SpacedReadRequest>| {
                                match worker
                                    .take(&request, WorkerRequest::Read(request.get_ref().clone()))
                                {
                                    Some(Reply::Read(reply)) => reply,
                                    _ => Err(Status::unimplemented("unscripted read")),
                                }
                            }),
                            request,
                        )
                        .await
                }
                _ => Status::unimplemented("not faked").into_http(),
            })
        })
    }
}

/// A codec for one method's request and response types.
fn grpc<Encode, Decode>() -> tonic::server::Grpc<tonic_prost::ProstCodec<Encode, Decode>>
where
    Encode: prost::Message + Send + 'static,
    Decode: prost::Message + Default + Send + 'static,
{
    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
}

struct Unary<F>(F);

impl<Req, Resp, F> tonic::server::UnaryService<Req> for Unary<F>
where
    F: FnMut(Request<Req>) -> Result<Resp, Status>,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Future = std::future::Ready<Result<Response<Resp>, Status>>;

    fn call(&mut self, request: Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request).map(Response::new))
    }
}

struct Streaming<F>(F);

impl<Req, Resp, F> tonic::server::ServerStreamingService<Req> for Streaming<F>
where
    F: FnMut(Request<Req>) -> Result<Vec<Result<Resp, Status>>, Status>,
    Resp: Send + 'static,
{
    type Response = Resp;
    type ResponseStream = BoxStream<Resp>;
    type Future = std::future::Ready<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request).map(|items| {
            Response::new(Box::pin(futures_util::stream::iter(items)) as BoxStream<Resp>)
        }))
    }
}
