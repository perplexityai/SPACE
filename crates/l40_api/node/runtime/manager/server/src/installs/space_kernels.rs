//! Guest kernels SPACE builds and publishes itself, beside a runtime's
//! upstream kernel source.
//!
//! The source offers exactly the compiled SPACE-published pins of the
//! catalog and never lists its bucket: each pin is one `HEAD` of
//! `<prefix>/vmlinux-<version>/sha256:<hex>/vmlinux.bin`, offered only when
//! the object is there at the pinned size. Requests are signed with the
//! default AWS credential chain, loaded on first use. Every other kernel of
//! the runtime is found, listed and downloaded by the upstream source this
//! one wraps, unchanged and without a request here.
//!
//! The two fail apart. Installing a pin asks only this source, so an
//! upstream outage cannot block it, and its errors surface as errors rather
//! than as "not offered". Listing adds the pins best-effort: a pin whose
//! `HEAD` fails is logged and left out, and the upstream listing stands.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use aws_sdk_s3::error::SdkError;
use pplx_space_artifact_model::StorageUri;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_spec::RuntimeKind;
use pplx_space_node_runtime_core::catalog::PinnedKernel;
use pplx_space_s3_http_client::{DEFAULT_POOL_IDLE_TIMEOUT, S3ClientSpans, s3_http_client};
use tokio::sync::OnceCell;

use super::source::{
    ByteChunks, KernelLocation, KernelSource, KernelVersion, KernelVersionParseError, RuntimeLine,
    SourceError, SourceKernel, VerifiableKernel,
};

/// Bound on a pin's `HEAD` when installing it.
const FIND_TIMEOUT: Duration = Duration::from_secs(20);
/// Bound on every pin's `HEAD` together when listing what is available,
/// well inside a node reconcile pass.
const LIST_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct SpaceKernelSource {
    root: StorageUri,
    client: OnceCell<aws_sdk_s3::Client>,
    pins: Vec<SpacePin>,
    upstream: Arc<dyn KernelSource>,
}

/// A compiled pin, with its version parsed and its object located once.
struct SpacePin {
    pin: PinnedKernel,
    version: KernelVersion,
    object: StorageUri,
}

impl SpaceKernelSource {
    /// `upstream`, plus the pins of its runtime among `pins` offered from
    /// `root`. Without such pins this is `upstream` itself.
    pub(crate) fn wrap(
        root: StorageUri,
        pins: Vec<PinnedKernel>,
        upstream: Arc<dyn KernelSource>,
    ) -> Result<Arc<dyn KernelSource>, KernelVersionParseError> {
        let source = Self::new(root, pins, upstream.clone())?;
        Ok(if source.pins.is_empty() {
            upstream
        } else {
            Arc::new(source)
        })
    }

    /// Offer the pins of `upstream`'s runtime among `pins` from `root`,
    /// and every other kernel of that runtime from `upstream`.
    pub(crate) fn new(
        root: StorageUri,
        pins: Vec<PinnedKernel>,
        upstream: Arc<dyn KernelSource>,
    ) -> Result<Self, KernelVersionParseError> {
        let pins = pins
            .into_iter()
            .filter(|pin| pin.runtime == upstream.runtime())
            .map(|pin| {
                let object = format!("vmlinux-{}/{}/vmlinux.bin", pin.version, pin.sha256);
                let key_prefix = if root.key_prefix.is_empty() {
                    object
                } else {
                    format!("{}/{object}", root.key_prefix)
                };
                Ok(SpacePin {
                    version: pin.version.as_str().parse()?,
                    object: StorageUri {
                        key_prefix,
                        ..root.clone()
                    },
                    pin,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            root,
            client: OnceCell::new(),
            pins,
            upstream,
        })
    }

    /// Sign with `builder`'s configuration instead of the default chain.
    #[cfg(test)]
    pub(crate) fn with_config(mut self, builder: aws_sdk_s3::config::Builder) -> Self {
        self.client = OnceCell::new_with(Some(client(&self.root, builder)));
        self
    }

    async fn client(&self) -> &aws_sdk_s3::Client {
        self.client
            .get_or_init(|| async {
                let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_sdk_s3::config::Region::new(self.root.region.clone()))
                    .http_client(s3_http_client(DEFAULT_POOL_IDLE_TIMEOUT))
                    .load()
                    .await;
                client(&self.root, aws_sdk_s3::config::Builder::from(&shared))
            })
            .await
    }

    fn pins_on(&self, line: RuntimeLine, architecture: Arch) -> impl Iterator<Item = &SpacePin> {
        self.pins.iter().filter(move |SpacePin { pin, .. }| {
            pin.architecture == architecture
                && pin.runtime_line.as_str() == line.release.to_string()
        })
    }

    /// The pin as published, or `None` when its object is absent or is not
    /// the pinned size.
    async fn published(
        &self,
        SpacePin {
            pin,
            version,
            object,
        }: &SpacePin,
        line: RuntimeLine,
        deadline: Instant,
    ) -> Result<Option<SourceKernel>, SourceError> {
        let location = || Box::new(KernelLocation::from(object.clone()));
        let request = self
            .client()
            .await
            .head_object()
            .bucket(&object.bucket)
            .key(&object.key_prefix)
            .send();
        let started = Instant::now();
        let head = match tokio::time::timeout_at(deadline, request).await {
            Err(_) => {
                return Err(SourceError::TimedOut {
                    location: location(),
                    after: started.elapsed(),
                });
            }
            Ok(Err(SdkError::ServiceError(error))) if error.err().is_not_found() => {
                return Ok(None);
            }
            Ok(Err(SdkError::ServiceError(error))) if error.raw().status().as_u16() == 403 => {
                return Err(SourceError::Forbidden {
                    location: location(),
                });
            }
            Ok(Err(error)) => {
                return Err(SourceError::Head {
                    location: location(),
                    source: Box::new(error),
                });
            }
            Ok(Ok(head)) => head,
        };
        let size = head
            .content_length()
            .and_then(|size| u64::try_from(size).ok());
        if size != Some(pin.size) {
            tracing::warn!(
                location = %object,
                ?size,
                pinned = pin.size,
                "SPACE-published kernel object is not the pinned size; not offering the pin"
            );
            return Ok(None);
        }
        Ok(Some(SourceKernel {
            version: version.clone(),
            line,
            architecture: pin.architecture,
            location: object.clone().into(),
            size: pin.size,
            // An entity tag is not the content MD5 for every encryption or
            // upload; the pinned size and SHA-256 verify the bytes.
            md5: None,
        }))
    }
}

fn client(root: &StorageUri, builder: aws_sdk_s3::config::Builder) -> aws_sdk_s3::Client {
    let mut builder = builder
        .region(aws_sdk_s3::config::Region::new(root.region.clone()))
        .http_client(s3_http_client(DEFAULT_POOL_IDLE_TIMEOUT))
        .interceptor(S3ClientSpans)
        .force_path_style(root.path_style)
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build(),
        );
    if let Some(endpoint) = &root.endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    aws_sdk_s3::Client::from_conf(builder.build())
}

#[async_trait::async_trait]
impl KernelSource for SpaceKernelSource {
    fn root(&self) -> KernelLocation {
        self.upstream.root()
    }

    fn runtime(&self) -> RuntimeKind {
        self.upstream.runtime()
    }

    async fn list(
        &self,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError> {
        let mut kernels = self.upstream.list(line, architecture).await?;
        let deadline = Instant::now() + LIST_TIMEOUT;
        for pin in self.pins_on(line, architecture) {
            match self.published(pin, line, deadline).await {
                Ok(kernel) => kernels.extend(kernel),
                Err(error) => tracing::warn!(
                    location = %pin.object,
                    ?error,
                    "SPACE-published kernel source failed; not listing the pin"
                ),
            }
        }
        Ok(kernels)
    }

    async fn find(
        &self,
        line: RuntimeLine,
        architecture: Arch,
        version: &KernelVersion,
    ) -> Result<Option<SourceKernel>, SourceError> {
        match self
            .pins_on(line, architecture)
            .find(|pin| pin.version == *version)
        {
            Some(pin) => {
                self.published(pin, line, Instant::now() + FIND_TIMEOUT)
                    .await
            }
            None => self.upstream.find(line, architecture, version).await,
        }
    }

    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError> {
        match &kernel.location {
            KernelLocation::S3(location) if self.pins.iter().any(|pin| pin.object == *location) => {
                super::s3::download(self.client().await, location, &kernel.location).await
            }
            _ => self.upstream.open(kernel).await,
        }
    }
}

#[cfg(test)]
mod tests;
