//! A cluster-api stub served over gRPC to the code under test.

use super::*;

/// Lines a stub serving in the parent process appends to a file, which the
/// child that runs the test reads back through the environment variable
/// `env` names.
struct CrossProcessLog {
    env: &'static str,
    path: std::sync::Mutex<Option<std::path::PathBuf>>,
}

impl CrossProcessLog {
    const fn new(env: &'static str) -> Self {
        Self {
            env,
            path: std::sync::Mutex::new(None),
        }
    }

    /// Point the parent's appends at `file`, and return the variable that
    /// hands it to the child.
    fn serve_into<'a>(
        &self,
        file: &'a tempfile::NamedTempFile,
    ) -> (&'static str, Option<&'a std::ffi::OsStr>) {
        *self.path.lock().unwrap() = Some(file.path().to_path_buf());
        (self.env, Some(file.path().as_os_str()))
    }

    fn append(&self, line: &str) {
        use std::io::Write as _;
        let path = self.path.lock().unwrap().clone();
        if let Some(path) = path {
            // One write per line: concurrent requests append to the same
            // file, and an append this small lands whole.
            let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
            file.write_all(format!("{line}\n").as_bytes()).unwrap();
        }
    }

    fn lines(&self) -> Vec<String> {
        let path = std::env::var_os(self.env)
            .unwrap_or_else(|| panic!("{} is set only under with_cluster_api", self.env));
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

/// The local artifact id of each registration the stub admitted.
static ADMITTED_REGISTRATIONS: CrossProcessLog =
    CrossProcessLog::new("SPACE_TEST_ADMITTED_ARTIFACT_BACKUPS");

/// Each `FinalizeArtifactBackup` a stub answered.
static FINALIZED_BACKUPS: CrossProcessLog =
    CrossProcessLog::new("SPACE_TEST_FINALIZED_ARTIFACT_BACKUPS");

/// The local artifact ids of every registration the stub admitted, a probe
/// that registers nothing aside. Admitting one is what lets a backup proceed.
pub(super) fn admitted_registrations() -> Vec<String> {
    ADMITTED_REGISTRATIONS.lines()
}

fn record_admitted_registration(local_artifact_id: &str) {
    ADMITTED_REGISTRATIONS.append(local_artifact_id);
}

/// A `FinalizeArtifactBackup` a stub answered, as the node sent it.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::service::grpc_lifecycle) struct FinalizedBackup {
    pub(in crate::service::grpc_lifecycle) backup_attempt_id: String,
    pub(in crate::service::grpc_lifecycle) suspend_uncommitted: bool,
}

/// Every `FinalizeArtifactBackup` a stub recorded with
/// [`record_finalized_backup`], in arrival order.
pub(in crate::service::grpc_lifecycle) fn finalized_backups() -> Vec<FinalizedBackup> {
    FINALIZED_BACKUPS
        .lines()
        .iter()
        .map(|line| {
            let (backup_attempt_id, suspend_uncommitted) = line.split_once(' ').unwrap();
            FinalizedBackup {
                backup_attempt_id: backup_attempt_id.to_owned(),
                suspend_uncommitted: suspend_uncommitted.parse().unwrap(),
            }
        })
        .collect()
}

/// Record `request` for the child's [`finalized_backups`].
pub(in crate::service::grpc_lifecycle) fn record_finalized_backup(
    request: &pplx_space_node_api_client::proto::FinalizeArtifactBackupRequest,
) {
    FINALIZED_BACKUPS.append(&format!(
        "{} {}",
        request.backup_attempt_id, request.suspend_uncommitted
    ));
}

/// A cluster-api stub. Its `BeginArtifactBackup` refuses every
/// generation-bound registration as superseded and admits the rest, or, when
/// `unavailable`, answers every registration `UNAVAILABLE`. Its
/// placement-filtered snapshot-storage lookup finds no sandbox, as it answers
/// a node the sandbox has moved off, unless `placed`, when it finds one with
/// no storage override. It has a ready backup of every local artifact exactly
/// when `artifact_backed`, and it shows the sandbox has a ready snapshot to
/// restore from exactly when `backed_copy`. When `refuses_only_verdicts`, it
/// refuses a generation-bound registration only when it's verdict-only, and
/// when `refuses_only_registrations`, only when it isn't. When
/// `supersession_lapsed`, it admits every registration, as the cluster does
/// once every newer execution has ended without running. It says its fence
/// admitted a generation-bound registration it admits, unless
/// `admits_unfenced`, as for a replay or for bytes another attempt
/// registered. When `predates_verdicts`, it answers as a server that predates
/// verdict-only requests and the fence's admission does: it echoes neither.
#[derive(Clone, Copy)]
pub(super) struct RetiringClusterApi {
    pub(super) unavailable: bool,
    pub(super) placed: bool,
    pub(super) backed_copy: bool,
    pub(super) artifact_backed: bool,
    pub(super) refuses_only_verdicts: bool,
    pub(super) refuses_only_registrations: bool,
    pub(super) supersession_lapsed: bool,
    pub(super) admits_unfenced: bool,
    pub(super) predates_verdicts: bool,
}

pub(super) const RETIRING: RetiringClusterApi = RetiringClusterApi {
    unavailable: false,
    placed: false,
    backed_copy: true,
    artifact_backed: false,
    refuses_only_verdicts: false,
    refuses_only_registrations: false,
    supersession_lapsed: false,
    admits_unfenced: false,
    predates_verdicts: false,
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
        let refusable = !self.supersession_lapsed
            && if request.fence_verdict_only {
                !self.refuses_only_registrations
            } else {
                !self.refuses_only_verdicts
            };
        std::future::ready(match request.execution_generation.filter(|_| refusable) {
            Some(generation) => Err(
                pplx_space_node_api_client::cluster::backup_execution_superseded_status(
                    "retired",
                    ExecutionGeneration::new(generation).unwrap(),
                ),
            ),
            None => {
                if !request.fence_verdict_only {
                    record_admitted_registration(&request.local_artifact_id);
                }
                Ok(tonic::Response::new(Self::Response {
                    local_artifact_id: request.local_artifact_id,
                    backup_attempt_id: request.backup_attempt_id,
                    fence_verdict_only: request.fence_verdict_only && !self.predates_verdicts,
                    generation_fence_admitted: request.execution_generation.is_some()
                        && !self.admits_unfenced
                        && !self.predates_verdicts,
                }))
            }
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
    let admitted = tempfile::NamedTempFile::new().unwrap();
    let finalized = tempfile::NamedTempFile::new().unwrap();
    let child = pplx_space_test_env::enter_test_process(&[
        ADMITTED_REGISTRATIONS.serve_into(&admitted),
        FINALIZED_BACKUPS.serve_into(&finalized),
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
