//! The SPACE-published source against a local S3 endpoint, through the
//! runtime manager's own install and listing RPCs.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_content_digest::Digest;
use pplx_space_execution_spec::{ArtifactComponentKind, ComponentIdentity, RuntimeVersion};
use pplx_space_node_runtime_core::catalog::PinnedKernel;
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::ScanRoots;
use pplx_space_node_runtime_manager_client::proto::runtime_manager_server::RuntimeManager as _;
use pplx_space_node_runtime_manager_client::proto::{
    ArtifactManagement, ExecutionArtifactStatus, GetOperationRequest, InstallArtifactRequest,
    ListExecutionArtifactsRequest,
};
use pplx_space_uds_peer_auth::PeerIdentity;
use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::SpaceKernelSource;
use crate::installs::source::KernelSource as _;
use crate::installs::test_support::{
    FakeReleaseSource, FakeSource, LocalPublisher, TestPins, banner_kernel, fc_line, kernel_sources,
};
use crate::installs::{ExecutorParts, InstallLimits, Installer, ReleaseContext, ReleaseSources};
use crate::service::RuntimeManagerService;

const BUCKET: &str = "space-kernels";
const PREFIX: &str = "guest-kernels";

/// A path-style S3 endpoint serving `HEAD` and `GET` of fixed objects, which
/// records each request's method, path and whether it was signed.
#[derive(Clone, Default)]
struct Bucket {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    requests: Arc<Mutex<Vec<(String, String, bool)>>>,
    forbidden: Arc<AtomicBool>,
}

impl Bucket {
    async fn serve(&self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let bucket = self.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(bucket.clone().answer(stream));
            }
        });
        format!("http://{address}")
    }

    async fn answer(self, mut stream: tokio::net::TcpStream) {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8];
            if stream.read_exact(&mut byte).await.is_err() {
                return;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).to_string();
        let mut words = head.split_whitespace();
        let (method, path) = (words.next().unwrap().to_lowercase(), words.next().unwrap());
        let path = path.split('?').next().unwrap().replace("%3A", ":");
        let head = head.to_ascii_lowercase();
        let signed = head.contains("\r\nauthorization: aws4-hmac-sha256");
        self.requests
            .lock()
            .unwrap()
            .push((method.clone(), path.clone(), signed));
        let object = self.objects.lock().unwrap().get(&path).cloned();
        let response = match object {
            _ if self.forbidden.load(Ordering::SeqCst) => {
                b"HTTP/1.1 403 Forbidden\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec()
            }
            None => {
                b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec()
            }
            Some(bytes) => {
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    bytes.len()
                )
                .into_bytes();
                if method == "get" {
                    response.extend(bytes);
                }
                response
            }
        };
        let _ = stream.write_all(&response).await;
    }

    fn publish(&self, pin: &PinnedKernel, bytes: &[u8]) {
        self.objects.lock().unwrap().insert(
            format!(
                "/{BUCKET}/{PREFIX}/vmlinux-{}/{}/vmlinux.bin",
                pin.version, pin.sha256
            ),
            bytes.to_vec(),
        );
    }

    fn requests(&self) -> Vec<(String, String, bool)> {
        self.requests.lock().unwrap().clone()
    }
}

/// A SPACE-published pin, unlike any upstream name, for these bytes.
fn space_pin(bytes: &[u8]) -> PinnedKernel {
    PinnedKernel {
        version: "6.1.158-nft".parse().unwrap(),
        runtime: pplx_space_execution_spec::RuntimeKind::Firecracker,
        runtime_line: RuntimeVersion::parse("1.16").unwrap(),
        architecture: crate::installs::host_arch().unwrap(),
        size: bytes.len() as u64,
        sha256: Digest::sha256(sha2::Sha256::digest(bytes).into()),
    }
}

/// The SPACE-published source at `endpoint`, signing with test keys.
fn space_source(
    endpoint: &str,
    pins: Vec<PinnedKernel>,
    upstream: Arc<FakeSource>,
) -> SpaceKernelSource {
    SpaceKernelSource::new(
        format!("s3://{BUCKET}/{PREFIX}?region=us-east-1&endpoint={endpoint}&path_style=true")
            .parse()
            .unwrap(),
        pins,
        upstream,
    )
    .unwrap()
    .with_config(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test-key",
                "test-secret",
                None,
                None,
                "test",
            )),
    )
}

struct Node {
    _dir: tempfile::TempDir,
    service: RuntimeManagerService,
    installer: Installer,
    upstream: Arc<FakeSource>,
}

impl Node {
    /// A node whose Firecracker source is `upstream`, wrapped by the
    /// SPACE-published source at `endpoint` when there is one, with `pins`
    /// in its catalog beside every upstream offer.
    async fn new(
        upstream: Arc<FakeSource>,
        endpoint: Option<&str>,
        pins: Vec<PinnedKernel>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        for name in ["node/kernels", "node/firecracker", "state"] {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
        }
        let catalog = TestPins::new(upstream.clone());
        for pin in &pins {
            catalog.pin_kernel(pin.clone());
        }
        let source: Arc<dyn crate::installs::KernelSource> = match endpoint {
            None => upstream.clone(),
            Some(endpoint) => Arc::new(space_source(endpoint, pins, upstream.clone())),
        };
        let base = dir.path();
        let installer = Installer::open(
            ExecutorParts {
                boot_source: None,
                kernels_root: base.join("node/kernels"),
                staging: base.join("staging"),
                limits: InstallLimits {
                    max_bytes: 1 << 20,
                    download_idle_timeout: Duration::from_secs(5),
                    max_attempts: 1,
                },
                host_arch: crate::installs::host_arch().unwrap(),
                source: kernel_sources(source.clone()),
                releases: ReleaseSources::new(vec![ReleaseContext {
                    root: base.join("node/firecracker"),
                    source: Arc::new(FakeReleaseSource::default()),
                }])
                .unwrap(),
                publisher: LocalPublisher::new(
                    &base.join("node/firecracker"),
                    &base.join("node/kernels"),
                    &base.join("staging"),
                    catalog.clone(),
                ),
                pins: catalog,
            },
            &base.join("state"),
        )
        .await
        .unwrap();
        let service = RuntimeManagerService::new(
            roots(base),
            base.join("state"),
            crate::DEFAULT_SCAN_LIMITS,
            Duration::from_secs(5),
        )
        .with_kernel_source(kernel_sources(source))
        .with_installer(installer.clone());
        Self {
            _dir: dir,
            service,
            installer,
            upstream,
        }
    }

    /// Install `pin` through the RPC and wait for its operation to finish.
    async fn install(
        &self,
        pin: &PinnedKernel,
    ) -> Result<crate::installs::Operation, tonic::Status> {
        let identity = crate::convert::component_identity(ComponentIdentity {
            kind: ArtifactComponentKind::Kernel,
            version: pin.version.clone(),
            digest: pin.sha256,
        });
        let operation = self
            .service
            .install_artifact(as_root(InstallArtifactRequest {
                identity: Some(identity),
            }))
            .await?
            .into_inner()
            .operation
            .unwrap();
        let job_id = operation.operation_id.parse().unwrap();
        loop {
            self.service
                .get_operation(as_root(GetOperationRequest {
                    operation_id: operation.operation_id.clone(),
                }))
                .await?;
            let operation = self.installer.get(job_id).await.unwrap().unwrap();
            if operation.state.is_finished() {
                return Ok(operation);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The node's listing of `pin`: whether it is SPACE-managed and valid.
    async fn listed(&self, pin: &PinnedKernel) -> Option<(i32, i32)> {
        self.service
            .list_execution_artifacts(as_root(ListExecutionArtifactsRequest::default()))
            .await
            .unwrap()
            .into_inner()
            .artifacts
            .into_iter()
            .find(|artifact| artifact.digest.as_deref() == Some(pin.sha256.to_string().as_str()))
            .map(|artifact| (artifact.management, artifact.status))
    }
}

fn roots(base: &Path) -> ScanRoots {
    ScanRoots {
        runtime_releases: Vec::new(),
        vendor_kernels: base.join("vendor/kernels"),
        managed_kernels: Some(base.join("node/kernels")),
        managed_releases: Vec::new(),
        cache_kernels: base.join("cache/firecracker/kernels"),
        cache_legacy_assets: base.join("cache/firecracker/assets"),
        runtime_aliases: Vec::new(),
    }
}

fn as_root<T>(body: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(body);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, None));
    request
}

#[tokio::test]
async fn an_offered_pin_installs_signed_and_converges_to_a_managed_valid_kernel() {
    let kernel = banner_kernel(4096, "6.1.158");
    let pin = space_pin(&kernel);
    let bucket = Bucket::default();
    bucket.publish(&pin, &kernel);
    let endpoint = bucket.serve().await;
    let upstream = Arc::new(FakeSource::default());
    let node = Node::new(upstream, Some(&endpoint), vec![pin.clone()]).await;

    let operation = node.install(&pin).await.unwrap();
    assert!(
        matches!(
            operation.state,
            crate::installs::OperationState::Succeeded(_)
        ),
        "{operation:?}"
    );
    assert_eq!(
        node.listed(&pin).await,
        Some((
            ArtifactManagement::SpaceManaged as i32,
            ExecutionArtifactStatus::Valid as i32
        ))
    );
    let object = format!(
        "/{BUCKET}/{PREFIX}/vmlinux-6.1.158-nft/{}/vmlinux.bin",
        pin.sha256
    );
    assert_eq!(
        bucket.requests(),
        vec![
            ("head".to_string(), object.clone(), true),
            ("get".to_string(), object, true),
        ],
        "one signed HEAD of the pin's own key, never a listing"
    );
}

#[tokio::test]
async fn bytes_other_than_the_pin_are_refused() {
    let kernel = banner_kernel(4096, "6.1.158");
    let pin = space_pin(&kernel);
    let bucket = Bucket::default();
    // The pinned size, other bytes: the download fails verification.
    let mut other = kernel.clone();
    *other.last_mut().unwrap() ^= 1;
    bucket.publish(&pin, &other);
    let endpoint = bucket.serve().await;
    let node = Node::new(
        Arc::new(FakeSource::default()),
        Some(&endpoint),
        vec![pin.clone()],
    )
    .await;
    let operation = node.install(&pin).await.unwrap();
    assert_eq!(operation.state, crate::installs::OperationState::Failed);
    assert_eq!(node.listed(&pin).await, None);

    // Another size is not offered at all.
    bucket.publish(&pin, &kernel[..kernel.len() - 1]);
    let error = node.install(&pin).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound, "{error}");

    // A digest the catalog does not pin is refused before any request.
    let requests = bucket.requests().len();
    let mut unpinned = pin.clone();
    unpinned.sha256 = Digest::sha256([9; 32]);
    let error = node.install(&unpinned).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert_eq!(bucket.requests().len(), requests);
}

#[tokio::test]
async fn unset_the_pin_is_not_offered_and_upstream_kernels_stay_upstream() {
    let kernel = banner_kernel(4096, "6.1.158");
    let pin = space_pin(&kernel);
    let upstream_kernel = banner_kernel(2048, "6.1.158");
    let bucket = Bucket::default();
    bucket.publish(&pin, &kernel);
    let endpoint = bucket.serve().await;

    // Without the source, the pinned SPACE kernel is not offered, as today.
    let upstream = Arc::new(FakeSource::default());
    upstream.offer("1.16", "6.1.158", &upstream_kernel);
    let node = Node::new(upstream.clone(), None, vec![pin.clone()]).await;
    let error = node.install(&pin).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound, "{error}");

    // With it, an upstream kernel is still listed and served by upstream.
    let node = Node::new(upstream.clone(), Some(&endpoint), vec![pin.clone()]).await;
    let upstream_pin = upstream.pins().remove(0);
    let operation = node.install(&upstream_pin).await.unwrap();
    assert!(matches!(
        operation.state,
        crate::installs::OperationState::Succeeded(_)
    ));
    assert_eq!(node.upstream.opens(), 1);
    assert!(
        bucket
            .requests()
            .iter()
            .all(|(method, _, _)| method == "head"),
        "{:?}",
        bucket.requests()
    );
    let listed = space_source(&endpoint, vec![pin.clone()], upstream.clone())
        .list(fc_line("1.17"), crate::installs::host_arch().unwrap())
        .await
        .unwrap();
    assert!(listed.is_empty(), "pins are offered only on their own line");

    // With no pin for its runtime, the flag leaves the upstream source as is.
    let upstream: Arc<dyn crate::installs::KernelSource> = upstream;
    let root = format!("s3://{BUCKET}/{PREFIX}?region=us-east-1")
        .parse()
        .unwrap();
    let source = SpaceKernelSource::wrap(root, Vec::new(), upstream.clone()).unwrap();
    assert!(Arc::ptr_eq(&source, &upstream));
}

#[tokio::test]
async fn a_forbidden_space_source_is_retryable_and_leaves_upstream_kernels_alone() {
    let kernel = banner_kernel(4096, "6.1.158");
    let pin = space_pin(&kernel);
    let bucket = Bucket::default();
    bucket.publish(&pin, &kernel);
    bucket.forbidden.store(true, Ordering::SeqCst);
    let endpoint = bucket.serve().await;
    let upstream = Arc::new(FakeSource::default());
    upstream.offer("1.16", "6.1.158", &banner_kernel(2048, "6.1.158"));
    let node = Node::new(upstream.clone(), Some(&endpoint), vec![pin.clone()]).await;

    let error = node.install(&pin).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert!(error.message().contains("403"), "{error}");
    let listed = node
        .service
        .list_execution_artifacts(as_root(ListExecutionArtifactsRequest::default()))
        .await;
    assert!(listed.is_ok(), "{listed:?}");

    let upstream_pin = upstream.pins().remove(0);
    let operation = node.install(&upstream_pin).await.unwrap();
    assert!(matches!(
        operation.state,
        crate::installs::OperationState::Succeeded(_)
    ));
}

#[tokio::test]
async fn a_pin_installs_through_an_upstream_outage() {
    let kernel = banner_kernel(4096, "6.1.158");
    let pin = space_pin(&kernel);
    let bucket = Bucket::default();
    bucket.publish(&pin, &kernel);
    let endpoint = bucket.serve().await;
    let upstream = Arc::new(FakeSource::default());
    upstream.go_down();
    let node = Node::new(upstream, Some(&endpoint), vec![pin.clone()]).await;

    let operation = node.install(&pin).await.unwrap();
    assert!(
        matches!(
            operation.state,
            crate::installs::OperationState::Succeeded(_)
        ),
        "{operation:?}"
    );
}

#[tokio::test]
async fn an_upstream_kernel_in_the_same_bucket_is_read_by_upstream() {
    let kernel = banner_kernel(4096, "6.1.158");
    let pin = space_pin(&kernel);
    let bucket = Bucket::default();
    bucket.publish(&pin, &kernel);
    let endpoint = bucket.serve().await;
    let upstream = Arc::new(FakeSource::at(
        format!(
            "s3://{BUCKET}/firecracker-ci?region=us-east-1&endpoint={endpoint}&path_style=true"
        )
        .parse()
        .unwrap(),
    ));
    upstream.offer("1.16", "6.1.158", &banner_kernel(2048, "6.1.158"));
    let node = Node::new(upstream.clone(), Some(&endpoint), vec![pin]).await;

    let operation = node.install(&upstream.pins().remove(0)).await.unwrap();
    assert!(matches!(
        operation.state,
        crate::installs::OperationState::Succeeded(_)
    ));
    assert_eq!(upstream.opens(), 1);
    assert_eq!(bucket.requests(), Vec::new(), "no SPACE request for it");
}
