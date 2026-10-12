//! Published kernels read anonymously from S3. The public Firecracker source
//! maps newer release lines to reviewed, dated artifact snapshots; custom
//! sources retain `<prefix>/v<line>/<arch>/vmlinux-<version>`.
//!
//! The shared object-store crate signs every request and has no listing
//! call, so this talks to the SDK directly; a signed source for SPACE's own
//! bucket belongs in that crate.

use std::time::Duration;

use pplx_space_common_core::types::{Arch, RuntimeKind, StorageUri};
use pplx_space_s3_http_client::{DEFAULT_POOL_IDLE_TIMEOUT, s3_http_client};

use super::source::{
    ByteChunks, ContentMd5, KernelSource, KernelVersion, ReleaseLine, RuntimeLine, SourceError,
    SourceKernel, VerifiableKernel,
};

const KERNEL_KEY_PREFIX: &str = "vmlinux-";
/// Bound on one listing, every page included, and on a download's first
/// response. The body that follows is bounded per chunk by the caller.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Objects one line's listing may hold: Firecracker's CI bucket keeps a few
/// dozen per line and architecture.
const MAX_LISTED_OBJECTS: usize = 2000;

pub(crate) struct S3KernelSource {
    root: StorageUri,
    client: aws_sdk_s3::Client,
}

impl S3KernelSource {
    /// Read the release lines under `root`, whose key prefix is the
    /// directory above them, such as `firecracker-ci`.
    pub(crate) fn new(root: StorageUri) -> Self {
        let mut builder = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(root.region.clone()))
            .http_client(s3_http_client(DEFAULT_POOL_IDLE_TIMEOUT))
            .force_path_style(root.path_style)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(CONNECT_TIMEOUT)
                    .build(),
            )
            .allow_no_auth();
        if let Some(endpoint) = &root.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        Self {
            client: aws_sdk_s3::Client::from_conf(builder.build()),
            root,
        }
    }

    fn line_prefix(&self, line: RuntimeLine, architecture: Arch) -> String {
        // Upstream stopped publishing v<line> directories after 1.15. Keep
        // a reviewed snapshot per line, never a listing of whichever is newest.
        // Mirrors with their own layout continue using v<line>.
        let snapshot = if self.root.bucket == "spec.ccfc.min"
            && self.root.key_prefix == "firecracker-ci"
            && self.root.endpoint.is_none()
        {
            match line.release {
                release if release == ReleaseLine::new(1, 16) => Some("20260603-e656faf58d6c-0"),
                release if release == ReleaseLine::new(1, 17) => Some("20260909-a8e1c3830545-0"),
                _ => None,
            }
        } else {
            None
        };
        let release = snapshot.map_or_else(|| format!("v{}", line.release), str::to_owned);
        let directory = format!("{release}/{}/{KERNEL_KEY_PREFIX}", architecture.as_str());
        if self.root.key_prefix.is_empty() {
            directory
        } else {
            format!("{}/{directory}", self.root.key_prefix)
        }
    }

    /// The URI of the object at `key` in this source's bucket.
    fn object(&self, key: &str) -> StorageUri {
        StorageUri {
            key_prefix: key.to_string(),
            ..self.root.clone()
        }
    }

    /// Whether `location` is an object in this source's bucket.
    fn holds(&self, location: &StorageUri) -> bool {
        location.bucket == self.root.bucket
            && location.region == self.root.region
            && location.endpoint == self.root.endpoint
    }

    async fn list_pages(
        &self,
        prefix: &str,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError> {
        let mut kernels = Vec::new();
        let mut listed = 0usize;
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.root.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| SourceError::List {
                location: Box::new(self.object(prefix)),
                source: Box::new(error),
            })?;
            // A release line holds dozens of kernels; a listing far past that
            // is not one, and is not followed further.
            listed = listed.saturating_add(page.contents().len());
            if listed > MAX_LISTED_OBJECTS {
                return Err(SourceError::ListingTooLarge {
                    location: Box::new(self.object(prefix)),
                    limit: MAX_LISTED_OBJECTS,
                });
            }
            for object in page.contents() {
                let (Some(key), Some(size)) = (object.key(), object.size()) else {
                    continue;
                };
                // Kernel configs (`.config`) and anything else that is not
                // a kernel image do not parse.
                let Some(version) = key
                    .strip_prefix(prefix)
                    .and_then(|name| name.parse::<KernelVersion>().ok())
                else {
                    continue;
                };
                let Ok(size) = u64::try_from(size) else {
                    continue;
                };
                kernels.push(SourceKernel {
                    version,
                    line,
                    architecture,
                    location: self.object(key),
                    size,
                    md5: object.e_tag().and_then(ContentMd5::from_etag),
                });
            }
        }
        kernels.sort_by(|left, right| left.version.cmp(&right.version));
        Ok(kernels)
    }
}

#[async_trait::async_trait]
impl KernelSource for S3KernelSource {
    fn root(&self) -> &StorageUri {
        &self.root
    }

    /// The layout is Firecracker's CI bucket's.
    fn runtime(&self) -> RuntimeKind {
        RuntimeKind::Firecracker
    }

    async fn list(
        &self,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError> {
        let prefix = self.line_prefix(line, architecture);
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.list_pages(&prefix, line, architecture),
        )
        .await
        .map_err(|_| SourceError::TimedOut {
            location: Box::new(self.object(&prefix)),
            after: REQUEST_TIMEOUT,
        })?
    }

    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError> {
        if !self.holds(&kernel.location) {
            return Err(SourceError::Foreign {
                location: Box::new(kernel.location.clone()),
            });
        }
        let request = self
            .client
            .get_object()
            .bucket(&kernel.location.bucket)
            .key(&kernel.location.key_prefix)
            .send();
        let object = tokio::time::timeout(REQUEST_TIMEOUT, request)
            .await
            .map_err(|_| SourceError::TimedOut {
                location: Box::new(kernel.location.clone()),
                after: REQUEST_TIMEOUT,
            })?
            .map_err(|error| SourceError::Download {
                location: Box::new(kernel.location.clone()),
                source: Box::new(error),
            })?;
        Ok(Box::pin(futures_util::stream::unfold(
            (object.body, kernel.location.clone()),
            |(mut body, location)| async move {
                match body.next().await {
                    None => None,
                    Some(Ok(chunk)) => Some((Ok(chunk), (body, location))),
                    Some(Err(error)) => Some((
                        Err(SourceError::Download {
                            location: Box::new(location.clone()),
                            source: Box::new(error),
                        }),
                        (body, location),
                    )),
                }
            },
        )))
    }
}

#[cfg(test)]
mod tests;
