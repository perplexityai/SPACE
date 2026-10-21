//! A cluster-api stub served over gRPC to the code under test.

use super::*;

/// A cluster-api stub. Its `BeginArtifactBackup` refuses every
/// generation-bound registration as superseded and admits the rest, or, when
/// `unavailable`, answers every registration `UNAVAILABLE`. Its
/// placement-filtered snapshot-storage lookup finds no sandbox, as it answers
/// a node the sandbox has moved off, unless `placed`, when it finds one with
/// no storage override. It has a ready backup of every local artifact exactly
/// when `artifact_backed`, and it shows the sandbox has a ready snapshot to
/// restore from exactly when `backed_copy`. When `refuses_only_verdicts`, it
/// refuses a generation-bound registration only when it's verdict-only.
#[derive(Clone, Copy)]
pub(super) struct RetiringClusterApi {
    pub(super) unavailable: bool,
    pub(super) placed: bool,
    pub(super) backed_copy: bool,
    pub(super) artifact_backed: bool,
    pub(super) refuses_only_verdicts: bool,
}

pub(super) const RETIRING: RetiringClusterApi = RetiringClusterApi {
    unavailable: false,
    placed: false,
    backed_copy: true,
    artifact_backed: false,
    refuses_only_verdicts: false,
};

/// The stub's answer to a backup readiness query: whether the sandbox has a
/// ready snapshot to restore from, and whether each local artifact has a ready
/// backup.
#[derive(Clone, Copy)]
struct BackedCopyLookup {
    backed_copy: bool,
    artifact_backed: bool,
}

impl
    tonic::server::UnaryService<pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest>
    for BackedCopyLookup
{
    type Response = pplx_space_node_api_client::proto::GetArtifactBackupLocationResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        request: tonic::Request<
            pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest,
        >,
    ) -> Self::Future {
        use pplx_space_node_api_client::proto::backup_readiness_query::Artifact;
        let readiness = request
            .into_inner()
            .readiness
            .and_then(|query| query.artifact);
        std::future::ready(match readiness {
            Some(Artifact::DurablyRestorable(true)) => Ok(tonic::Response::new(Self::Response {
                ready: Some(self.backed_copy),
                ..Default::default()
            })),
            Some(Artifact::LocalArtifact(_)) => Ok(tonic::Response::new(Self::Response {
                ready: Some(self.artifact_backed),
                ..Default::default()
            })),
            Some(_) => Ok(tonic::Response::new(Self::Response {
                ready: Some(false),
                ..Default::default()
            })),
            None => Err(tonic::Status::not_found("no ready backup of this artifact")),
        })
    }
}

/// The snapshot-storage lookup of a sandbox placed on the calling node, whose
/// organization has no storage override.
#[derive(Clone, Copy)]
struct PlacedStorageLookup;

impl
    tonic::server::UnaryService<
        pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest,
    > for PlacedStorageLookup
{
    type Response = pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        _: tonic::Request<pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest>,
    ) -> Self::Future {
        std::future::ready(Ok(tonic::Response::new(Self::Response {
            storage_uri: None,
        })))
    }
}

impl tonic::server::NamedService for RetiringClusterApi {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl tonic::server::UnaryService<pplx_space_node_api_client::proto::BeginArtifactBackupRequest>
    for RetiringClusterApi
{
    type Response = pplx_space_node_api_client::proto::BeginArtifactBackupResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        request: tonic::Request<pplx_space_node_api_client::proto::BeginArtifactBackupRequest>,
    ) -> Self::Future {
        let request = request.into_inner();
        if self.unavailable {
            return std::future::ready(Err(tonic::Status::unavailable(
                "cluster-api is restarting",
            )));
        }
        let refusable = request.fence_verdict_only || !self.refuses_only_verdicts;
        std::future::ready(match request.execution_generation.filter(|_| refusable) {
            Some(generation) => Err(
                pplx_space_node_api_client::cluster::backup_execution_superseded_status(
                    "retired",
                    ExecutionGeneration::new(generation).unwrap(),
                ),
            ),
            None => Ok(tonic::Response::new(Self::Response {
                local_artifact_id: request.local_artifact_id,
                backup_attempt_id: request.backup_attempt_id,
                fence_verdict_only: request.fence_verdict_only,
            })),
        })
    }
}

impl<B> tower::Service<http::Request<B>> for RetiringClusterApi
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let stub = *self;
        Box::pin(async move {
            if path.ends_with("/BeginArtifactBackup") {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(stub, request).await)
            } else if path.ends_with("/GetSandboxSnapshotStorageUri") && stub.placed {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(PlacedStorageLookup, request).await)
            } else if path.ends_with("/GetSandboxSnapshotStorageUri") {
                Ok(tonic::Status::not_found("sandbox is not placed on this node").into_http())
            } else if path.ends_with("/GetArtifactBackupLocation") {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                let lookup = BackedCopyLookup {
                    backed_copy: stub.backed_copy,
                    artifact_backed: stub.artifact_backed,
                };
                Ok(grpc.unary(lookup, request).await)
            } else {
                Ok(tonic::Status::unimplemented(path).into_http())
            }
        })
    }
}

/// Serves `stub` and runs `test` in an isolated child process whose
/// cluster-api client wiring points at it, with a synthetic snapshot upload
/// destination configured.
pub(in crate::service::grpc_lifecycle) async fn with_cluster_api<S>(
    stub: S,
    test: impl AsyncFnOnce(),
) where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    use std::ffi::OsStr;

    // The parent serves while the isolated child exercises environment-based
    // client wiring. A second runtime thread keeps the server making progress.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let stop = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(stub)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
    );
    let token = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(token.path(), "synthetic-backup-test-token").unwrap();
    let child = pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("1"))),
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV,
            Some(OsStr::new(&endpoint)),
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_CLUSTER_API_TOKEN_PATH_ENV,
            Some(token.path().as_os_str()),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(OsStr::new("synthetic-backup-test-token")),
        ),
        ("SPACE_SNAPSHOT_BUCKET", Some(OsStr::new("test-bucket"))),
        ("SPACE_SNAPSHOT_REGION", Some(OsStr::new("us-east-1"))),
        // Service construction reads its feature-flag provider from the
        // environment; keep the child off any provider the host configures.
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]);
    if child {
        test().await;
    }
    stop.cancel();
    server.await.unwrap().unwrap();
}

/// A generation-bound suspend upload payload for `artifact`.
pub(super) fn bound_upload_payload(
    artifact: LocalArtifactId,
) -> crate::snapshot_upload::SnapshotUploadJobs {
    let mut payload = crate::snapshot_upload_jobs::tests::reclaim_fixture_payload(
        &artifact.to_string(),
        std::path::PathBuf::from("/var/lib/space/sandboxes/s/suspend"),
    );
    payload.meta.execution = Some(
        pplx_space_common_core::types::SnapshotExecutionMetadata::new(
            pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                std::env::consts::ARCH.parse().unwrap(),
                vec![],
            ),
        )
        .unwrap(),
    );
    payload.execution_generation = Some(generation(GENERATION));
    payload
}
