use std::sync::Mutex;

use super::*;

/// Recording fake S3 client. Captures every list/delete call shape
/// so tests can assert on (bucket, key_prefix, key set, region,
/// endpoint, path_style). Configurable to return canned list
/// responses or per-call failures.
pub(crate) struct FakeS3Client {
    list_pages: Mutex<Vec<Result<ListObjectsResponse, String>>>,
    list_calls: Mutex<Vec<RecordedList>>,
    delete_result: Mutex<Result<(), String>>,
    delete_calls: Mutex<Vec<RecordedDelete>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordedList {
    pub bucket: String,
    pub key_prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub continuation_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordedDelete {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub keys: Vec<String>,
}

impl FakeS3Client {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            list_pages: Mutex::new(Vec::new()),
            list_calls: Mutex::new(Vec::new()),
            delete_result: Mutex::new(Ok(())),
            delete_calls: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn enqueue_list_page(&self, response: ListObjectsResponse) {
        self.list_pages.lock().unwrap().push(Ok(response));
    }

    pub(crate) fn enqueue_list_error(&self, error: &str) {
        self.list_pages.lock().unwrap().push(Err(error.to_string()));
    }

    pub(crate) fn set_delete_error(&self, error: &str) {
        *self.delete_result.lock().unwrap() = Err(error.to_string());
    }

    pub(crate) fn list_calls(&self) -> Vec<RecordedList> {
        self.list_calls.lock().unwrap().clone()
    }

    pub(crate) fn delete_calls(&self) -> Vec<RecordedDelete> {
        self.delete_calls.lock().unwrap().clone()
    }
}

impl S3Client for FakeS3Client {
    fn list_objects(
        &self,
        request: ListObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ListObjectsResponse, String>> + Send + '_>> {
        self.list_calls.lock().unwrap().push(RecordedList {
            bucket: request.bucket.to_string(),
            key_prefix: request.key_prefix.to_string(),
            region: request.region.to_string(),
            endpoint: request.endpoint.map(str::to_string),
            path_style: request.path_style,
            continuation_token: request.continuation_token.clone(),
        });
        let mut pages = self.list_pages.lock().unwrap();
        let page = if pages.is_empty() {
            Ok(ListObjectsResponse {
                keys: Vec::new(),
                next_continuation_token: None,
            })
        } else {
            pages.remove(0)
        };
        Box::pin(async move { page })
    }

    fn delete_objects(
        &self,
        request: DeleteObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        self.delete_calls.lock().unwrap().push(RecordedDelete {
            bucket: request.bucket.to_string(),
            region: request.region.to_string(),
            endpoint: request.endpoint.map(str::to_string),
            path_style: request.path_style,
            keys: request.keys.clone(),
        });
        let result = self.delete_result.lock().unwrap().clone();
        Box::pin(async move { result })
    }
}

fn fixture_uri() -> StorageUri {
    StorageUri::parse(
        "s3://example-snapshots\
         /clusters/test-cluster/suspend_snapshots/aa/sandbox-1/snap-1\
         ?region=us-east-1",
    )
    .expect("fixture URI parses")
}

fn make_keys(prefix: &str, names: &[&str]) -> Vec<String> {
    names.iter().map(|n| format!("{prefix}{n}")).collect()
}

#[tokio::test]
async fn empty_listing_returns_already_gone() {
    let uri = fixture_uri();
    let client = FakeS3Client::new();
    client.enqueue_list_page(ListObjectsResponse {
        keys: Vec::new(),
        next_continuation_token: None,
    });
    let backend = S3StorageBackend::new(client.clone());

    let outcome = backend.delete_prefix(&uri).await.unwrap();
    assert_eq!(outcome, DeletePrefixOutcome::AlreadyGone);

    let calls = client.list_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].bucket, uri.bucket);
    assert_eq!(
        calls[0].key_prefix,
        format!("{}/", uri.key_prefix),
        "list must use the URI's key_prefix with a trailing slash"
    );
    assert_eq!(calls[0].region, uri.region);
    assert!(client.delete_calls().is_empty());
}

#[tokio::test]
async fn single_page_listing_deletes_all_keys() {
    let uri = fixture_uri();
    let key_prefix = format!("{}/", uri.key_prefix);
    let client = FakeS3Client::new();
    let keys = make_keys(
        &key_prefix,
        &["vmstate", "memory", "rootfs", "metadata.json"],
    );
    client.enqueue_list_page(ListObjectsResponse {
        keys: keys.clone(),
        next_continuation_token: None,
    });
    let backend = S3StorageBackend::new(client.clone());

    let outcome = backend.delete_prefix(&uri).await.unwrap();
    assert_eq!(outcome, DeletePrefixOutcome::Deleted { count: 4 });

    let deletes = client.delete_calls();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].keys, keys);
    assert_eq!(deletes[0].bucket, uri.bucket);
    assert_eq!(deletes[0].region, uri.region);
}

#[tokio::test]
async fn truncated_listing_follows_continuation_tokens() {
    let uri = fixture_uri();
    let key_prefix = format!("{}/", uri.key_prefix);
    let client = FakeS3Client::new();
    let page_a = make_keys(&key_prefix, &["a", "b"]);
    let page_b = make_keys(&key_prefix, &["c", "d"]);
    client.enqueue_list_page(ListObjectsResponse {
        keys: page_a.clone(),
        next_continuation_token: Some("token-1".to_string()),
    });
    client.enqueue_list_page(ListObjectsResponse {
        keys: page_b.clone(),
        next_continuation_token: None,
    });
    let backend = S3StorageBackend::new(client.clone());

    let outcome = backend.delete_prefix(&uri).await.unwrap();
    assert_eq!(outcome, DeletePrefixOutcome::Deleted { count: 4 });

    let lists = client.list_calls();
    assert_eq!(lists.len(), 2);
    assert_eq!(lists[0].continuation_token, None);
    assert_eq!(
        lists[1].continuation_token,
        Some("token-1".to_string()),
        "second list must replay the continuation token from page one"
    );

    let deletes = client.delete_calls();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0].keys, page_a);
    assert_eq!(deletes[1].keys, page_b);
}

/// Regression: an intermediate page with empty `keys` but a
/// fresh `next_continuation_token` (legal on versioned buckets
/// with delete-marker filtering, or under IAM prefix selectivity)
/// must NOT stop pagination. Breaking early would drop subsequent
/// pages of objects, the DB row would then be DELETEd, and the
/// surviving S3 objects would become unreferenced orphans.
#[tokio::test]
async fn empty_intermediate_page_follows_continuation_token() {
    let uri = fixture_uri();
    let key_prefix = format!("{}/", uri.key_prefix);
    let client = FakeS3Client::new();
    let page_a = make_keys(&key_prefix, &["vmstate"]);
    let page_c = make_keys(&key_prefix, &["memory", "rootfs"]);
    client.enqueue_list_page(ListObjectsResponse {
        keys: page_a.clone(),
        next_continuation_token: Some("token-1".to_string()),
    });
    client.enqueue_list_page(ListObjectsResponse {
        // Empty intermediate page — S3 filtered every key on this
        // page but more remain (token-2 != None).
        keys: Vec::new(),
        next_continuation_token: Some("token-2".to_string()),
    });
    client.enqueue_list_page(ListObjectsResponse {
        keys: page_c.clone(),
        next_continuation_token: None,
    });
    let backend = S3StorageBackend::new(client.clone());

    let outcome = backend.delete_prefix(&uri).await.unwrap();
    assert_eq!(
        outcome,
        DeletePrefixOutcome::Deleted { count: 3 },
        "must paginate past the empty middle page and reap page 3"
    );

    let lists = client.list_calls();
    assert_eq!(lists.len(), 3, "must make all three list calls");
    assert_eq!(lists[0].continuation_token, None);
    assert_eq!(lists[1].continuation_token, Some("token-1".to_string()));
    assert_eq!(
        lists[2].continuation_token,
        Some("token-2".to_string()),
        "must follow the continuation token from the empty page"
    );

    let deletes = client.delete_calls();
    assert_eq!(
        deletes.len(),
        2,
        "only the two non-empty pages produce deletes"
    );
    assert_eq!(deletes[0].keys, page_a);
    assert_eq!(deletes[1].keys, page_c);
}

#[tokio::test]
async fn list_failure_surfaces_as_backend_error() {
    let uri = fixture_uri();
    let client = FakeS3Client::new();
    client.enqueue_list_error("throttled");
    let backend = S3StorageBackend::new(client.clone());

    let err = backend.delete_prefix(&uri).await.unwrap_err();
    assert!(
        matches!(err, DeletePrefixError::Backend(ref msg) if msg.contains("throttled")),
        "expected Backend(throttled), got {err:?}"
    );
    assert!(client.delete_calls().is_empty());
}

#[tokio::test]
async fn delete_failure_surfaces_as_backend_error() {
    let uri = fixture_uri();
    let key_prefix = format!("{}/", uri.key_prefix);
    let client = FakeS3Client::new();
    client.enqueue_list_page(ListObjectsResponse {
        keys: make_keys(&key_prefix, &["vmstate"]),
        next_continuation_token: None,
    });
    client.set_delete_error("access denied");
    let backend = S3StorageBackend::new(client.clone());

    let err = backend.delete_prefix(&uri).await.unwrap_err();
    assert!(
        matches!(err, DeletePrefixError::Backend(ref msg) if msg.contains("access denied")),
        "expected Backend(access denied), got {err:?}"
    );
}

#[tokio::test]
async fn refuses_to_sweep_bucket_root() {
    let uri = StorageUri::parse("s3://my-bucket?region=us-east-1").unwrap();
    let client = FakeS3Client::new();
    let backend = S3StorageBackend::new(client.clone());

    let err = backend.delete_prefix(&uri).await.unwrap_err();
    assert!(
        matches!(err, DeletePrefixError::Backend(ref msg) if msg.contains("bucket root")),
        "expected refusal for bucket-root URI, got {err:?}"
    );
    assert!(client.list_calls().is_empty());
    assert!(client.delete_calls().is_empty());
}

#[tokio::test]
async fn list_uses_trailing_slash_so_prefix_is_unambiguous() {
    // Without the trailing slash, S3 would happily return objects
    // belonging to sibling snapshots whose key starts with our
    // prefix (e.g. `snap-1` and `snap-10`). Regression guard.
    let uri = StorageUri::parse(
        "s3://my-bucket/clusters/c/suspend_snapshots/aa/sbx-1/snap-1?region=us-east-1",
    )
    .unwrap();
    let client = FakeS3Client::new();
    let backend = S3StorageBackend::new(client.clone());

    let _ = backend.delete_prefix(&uri).await;
    let calls = client.list_calls();
    assert_eq!(calls.len(), 1);
    assert!(
        calls[0].key_prefix.ends_with('/'),
        "list prefix must end with `/` to avoid sibling-key matches; got {:?}",
        calls[0].key_prefix,
    );
    assert_eq!(
        calls[0].key_prefix,
        "clusters/c/suspend_snapshots/aa/sbx-1/snap-1/"
    );
}
