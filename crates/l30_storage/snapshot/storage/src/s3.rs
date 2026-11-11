//! S3-backed implementation of [`super::StorageBackend`].
//!
//! Each storage-location URI points at a **directory prefix** (one suspend
//! snapshot writes `vmstate`, `memory`, `rootfs`, and `metadata.json` under
//! `s3://<bucket>/<key_prefix>/`). To reap the snapshot we list every
//! object under the prefix and batch-delete them.
//!
//! A thin trait wraps the SDK calls so unit tests substitute a recording fake
//! without exercising the real AWS SDK.
//!
//! ## Region / endpoint dispatch
//!
//! Every URI carries its own `region` (and optional `endpoint` /
//! `path_style` for MinIO and similar S3-compatible stores). The
//! production [`S3StorageBackend`] holds a shared base
//! `aws_sdk_s3::Client` and applies per-request `config_override` to
//! retarget that client at the URI's region/endpoint. Same shape as
//! `pplx_space_object_store::S3ObjectStore` but scoped down to the
//! `List + Delete` pair we need.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_types::SdkConfig;
use pplx_space_common_core::types::StorageUri;
use pplx_space_s3_http_client::{DEFAULT_POOL_IDLE_TIMEOUT, S3ClientSpans, s3_http_client};

use super::{DeletePrefixError, DeletePrefixOutcome, StorageBackend};

/// Maximum number of keys per `DeleteObjects` request. S3's hard limit
/// is 1000; we match it to minimize round-trips on big snapshot prefixes
/// (which sit at a handful of objects today but cost the same one call
/// either way).
const DELETE_OBJECTS_BATCH: usize = 1000;

/// Thin seam over the two `aws_sdk_s3::Client` operations we need.
/// Production: forwards to the real SDK. Tests: substitute a recording
/// fake to assert request shape without touching AWS.
pub trait S3Client: Send + Sync + 'static {
    /// List up to `max_keys` objects under `(bucket, key_prefix)`,
    /// resuming from `continuation_token`. Returns the object keys
    /// for this page plus the next continuation token (if any).
    fn list_objects(
        &self,
        request: ListObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ListObjectsResponse, String>> + Send + '_>>;

    /// Batch-delete `keys` from `bucket`. Implementations should
    /// surface partial-failure responses as `Err` so the reconciler
    /// retries instead of marking the row deletable.
    fn delete_objects(
        &self,
        request: DeleteObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

#[derive(Debug, Clone)]
pub struct ListObjectsRequest<'a> {
    pub bucket: &'a str,
    pub key_prefix: &'a str,
    pub region: &'a str,
    pub endpoint: Option<&'a str>,
    pub path_style: bool,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListObjectsResponse {
    pub keys: Vec<String>,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteObjectsRequest<'a> {
    pub bucket: &'a str,
    pub region: &'a str,
    pub endpoint: Option<&'a str>,
    pub path_style: bool,
    pub keys: Vec<String>,
}

/// Production [`StorageBackend`] backed by `aws_sdk_s3`.
pub struct S3StorageBackend {
    client: Arc<dyn S3Client>,
}

impl S3StorageBackend {
    pub fn new(client: Arc<dyn S3Client>) -> Self {
        Self { client }
    }

    /// Backend on an SDK client built from `sdk_config` with the shared S3
    /// HTTP client, whose pool drops idle connections before S3 closes them.
    /// Per-request overrides keep the base client's HTTP client.
    pub fn from_sdk_config(sdk_config: &SdkConfig) -> Self {
        let config = aws_sdk_s3::config::Builder::from(sdk_config)
            .http_client(s3_http_client(DEFAULT_POOL_IDLE_TIMEOUT))
            .interceptor(S3ClientSpans)
            .build();
        Self::new(Arc::new(Client::from_conf(config)))
    }
}

impl StorageBackend for S3StorageBackend {
    async fn delete_prefix(
        &self,
        uri: &StorageUri,
    ) -> Result<DeletePrefixOutcome, DeletePrefixError> {
        // Always normalize the prefix with a trailing `/` so a key like
        // `clusters/test-cluster/suspend_snapshots/aa/sandbox-1/snap-X`
        // doesn't match the unrelated sibling
        // `clusters/test-cluster/suspend_snapshots/aa/sandbox-1/snap-XYZ`.
        // `StorageUri::parse` already strips trailing `/` so the
        // round-trip is bounded.
        let key_prefix = if uri.key_prefix.is_empty() {
            // Empty prefix means the URI pointed at the bucket root.
            // We refuse — sweeping every object under a bucket is
            // surely never intended and could destroy unrelated data.
            return Err(DeletePrefixError::Backend(
                "refusing to delete bucket root: empty key_prefix".to_string(),
            ));
        } else {
            format!("{}/", uri.key_prefix)
        };

        let mut total_deleted: u64 = 0;
        let mut continuation: Option<String> = None;
        loop {
            let resp = self
                .client
                .list_objects(ListObjectsRequest {
                    bucket: &uri.bucket,
                    key_prefix: &key_prefix,
                    region: &uri.region,
                    endpoint: uri.endpoint.as_deref(),
                    path_style: uri.path_style,
                    continuation_token: continuation.clone(),
                })
                .await
                .map_err(DeletePrefixError::Backend)?;

            // An empty `keys` page is NOT a terminal signal on its own:
            // S3 may legitimately return `IsTruncated=true` with empty
            // `Contents` and a fresh `NextContinuationToken` (delete-
            // marker race on versioned buckets, restricted-IAM prefix
            // filters that discard the whole page). Breaking here on an
            // intermediate empty page would drop the continuation token
            // on the floor and leak the next page's objects after the
            // DB row gets deleted. Only stop when the continuation
            // token chain itself terminates.
            for chunk in resp.keys.chunks(DELETE_OBJECTS_BATCH) {
                let count = chunk.len() as u64;
                self.client
                    .delete_objects(DeleteObjectsRequest {
                        bucket: &uri.bucket,
                        region: &uri.region,
                        endpoint: uri.endpoint.as_deref(),
                        path_style: uri.path_style,
                        keys: chunk.to_vec(),
                    })
                    .await
                    .map_err(DeletePrefixError::Backend)?;
                total_deleted = total_deleted.saturating_add(count);
            }

            continuation = resp.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        }

        if total_deleted == 0 {
            Ok(DeletePrefixOutcome::AlreadyGone)
        } else {
            Ok(DeletePrefixOutcome::Deleted {
                count: total_deleted,
            })
        }
    }
}

impl S3Client for Client {
    fn list_objects(
        &self,
        request: ListObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ListObjectsResponse, String>> + Send + '_>> {
        let bucket = request.bucket.to_string();
        let key_prefix = request.key_prefix.to_string();
        let cfg = per_request_config(request.region, request.endpoint, request.path_style);
        let continuation = request.continuation_token;
        Box::pin(async move {
            let mut op = self
                .list_objects_v2()
                .bucket(bucket)
                .prefix(key_prefix)
                .max_keys(DELETE_OBJECTS_BATCH as i32);
            if let Some(token) = continuation {
                op = op.continuation_token(token);
            }
            let resp = op
                .customize()
                .config_override(cfg)
                .send()
                .await
                .map_err(|err| DisplayErrorContext(&err).to_string())?;
            let keys = resp
                .contents()
                .iter()
                .filter_map(|obj| obj.key().map(str::to_string))
                .collect();
            let next_continuation_token = resp.next_continuation_token().map(str::to_string);
            Ok(ListObjectsResponse {
                keys,
                next_continuation_token,
            })
        })
    }

    fn delete_objects(
        &self,
        request: DeleteObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        let bucket = request.bucket.to_string();
        let cfg = per_request_config(request.region, request.endpoint, request.path_style);
        let keys = request.keys;
        Box::pin(async move {
            let mut delete = Delete::builder();
            for key in keys {
                let object = ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .map_err(|err| DisplayErrorContext(&err).to_string())?;
                delete = delete.objects(object);
            }
            let delete = delete
                .build()
                .map_err(|err| DisplayErrorContext(&err).to_string())?;
            let resp = self
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .customize()
                .config_override(cfg)
                .send()
                .await
                .map_err(|err| DisplayErrorContext(&err).to_string())?;
            // S3 `DeleteObjects` returns a 200 even when individual
            // keys fail — those land in `errors`. Treat any per-key
            // error as a backend failure so the reconciler retries on
            // the next tick rather than DELETEing the DB row.
            let errors = resp.errors();
            if !errors.is_empty() {
                let summary = errors
                    .iter()
                    .map(|e| {
                        format!(
                            "key={} code={} msg={}",
                            e.key().unwrap_or("<unknown>"),
                            e.code().unwrap_or("<unknown>"),
                            e.message().unwrap_or("<unknown>"),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(format!("DeleteObjects returned per-key errors: {summary}"));
            }
            Ok(())
        })
    }
}

fn per_request_config(
    region: &str,
    endpoint: Option<&str>,
    path_style: bool,
) -> aws_sdk_s3::config::Builder {
    let mut builder = aws_sdk_s3::config::Builder::new()
        .region(Region::new(region.to_string()))
        .force_path_style(path_style);
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    builder
}

#[cfg(test)]
mod tests;
