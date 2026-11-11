use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(feature = "s3")]
use pplx_space_snapshot_storage::s3::{
    DeleteObjectsRequest, ListObjectsRequest, ListObjectsResponse, S3Client, S3StorageBackend,
};

use super::*;

// A registry-routing fake: records the connection strings it was
// asked to reap and returns a canned outcome.
#[derive(Default)]
struct RecordingBackend {
    scheme: &'static str,
    calls: std::sync::Mutex<Vec<String>>,
}

impl StorageBackend for RecordingBackend {
    fn scheme(&self) -> &'static str {
        self.scheme
    }
    fn validate_route(&self, _connection_string: &str) -> Result<(), DeletePrefixError> {
        Ok(())
    }
    fn delete_prefix<'a>(
        &'a self,
        connection_string: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send + 'a>>
    {
        self.calls
            .lock()
            .unwrap()
            .push(connection_string.to_string());
        Box::pin(async { Ok(DeletePrefixOutcome::AlreadyGone) })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn registry_routes_by_scheme() {
    let s3 = Arc::new(RecordingBackend {
        scheme: "s3",
        ..Default::default()
    });
    let gs = Arc::new(RecordingBackend {
        scheme: "gs",
        ..Default::default()
    });
    let mut registry = StorageBackendRegistry::new();
    registry.register(s3.clone());
    registry.register(gs.clone());
    assert_eq!(registry.schemes(), vec!["gs", "s3"]);

    registry.delete_prefix("s3://b/p?region=x").await.unwrap();
    registry.delete_prefix("gs://b/p").await.unwrap();

    assert_eq!(s3.calls.lock().unwrap().len(), 1);
    assert_eq!(gs.calls.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn registry_unregistered_scheme_is_unsupported() {
    let registry = StorageBackendRegistry::new();
    let err = registry.delete_prefix("gs://bucket/p").await.unwrap_err();
    assert!(matches!(err, DeletePrefixError::UnsupportedScheme(s) if s == "gs"));
    // A connection string with no scheme also maps to unsupported.
    let err = registry.delete_prefix("not-a-uri").await.unwrap_err();
    assert!(matches!(err, DeletePrefixError::UnsupportedScheme(s) if s.is_empty()));
}

// S3Client fake: panics if any S3 call is made, so the parse /
// scope rejections (which must short-circuit before any S3 I/O) can
// assert "the SDK was never touched".
#[cfg(feature = "s3")]
struct PanicS3Client;
#[cfg(feature = "s3")]
impl S3Client for PanicS3Client {
    fn list_objects(
        &self,
        _request: ListObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ListObjectsResponse, String>> + Send + '_>> {
        panic!("S3 list_objects must not be called when the request is rejected pre-flight");
    }
    fn delete_objects(
        &self,
        _request: DeleteObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        panic!("S3 delete_objects must not be called when the request is rejected pre-flight");
    }
}

#[cfg(feature = "s3")]
fn s3_backend_with_panic_client(scoped_bucket: Option<&str>) -> S3Backend {
    S3Backend::new(
        S3StorageBackend::new(Arc::new(PanicS3Client)),
        scoped_bucket.map(str::to_string),
    )
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "s3")]
async fn s3_backend_reports_its_scheme() {
    assert_eq!(s3_backend_with_panic_client(None).scheme(), "s3");
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "s3")]
async fn s3_backend_rejects_invalid_uri_without_s3_io() {
    let backend = s3_backend_with_panic_client(Some("b"));
    let err = backend
        .delete_prefix("gs://wrong-scheme/p")
        .await
        .unwrap_err();
    assert!(matches!(err, DeletePrefixError::InvalidUri));
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "s3")]
async fn s3_backend_rejects_out_of_scope_bucket_without_s3_io() {
    let backend = s3_backend_with_panic_client(Some("authorized-bucket"));
    let err = backend
        .delete_prefix("s3://other-bucket/p?region=us-east-1")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        DeletePrefixError::OutOfScope { backend: "s3", container } if container == "other-bucket"
    ));
}

#[test]
#[cfg(feature = "s3")]
fn s3_backend_rejects_bucket_root_during_preflight() {
    let backend = s3_backend_with_panic_client(Some("authorized-bucket"));
    let err = backend
        .validate_route("s3://authorized-bucket?region=us-east-1")
        .unwrap_err();
    assert!(matches!(err, DeletePrefixError::InvalidUri));
}

// S3Client fake that returns a fixed set of keys then an empty
// listing, so the success-mapping path can be exercised end to end.
#[cfg(feature = "s3")]
struct CannedS3Client {
    keys: Vec<String>,
}
#[cfg(feature = "s3")]
impl S3Client for CannedS3Client {
    fn list_objects(
        &self,
        _request: ListObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ListObjectsResponse, String>> + Send + '_>> {
        let keys = self.keys.clone();
        Box::pin(async move {
            Ok(ListObjectsResponse {
                keys,
                next_continuation_token: None,
            })
        })
    }
    fn delete_objects(
        &self,
        _request: DeleteObjectsRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "s3")]
async fn s3_backend_maps_deleted_and_already_gone() {
    let deleted = S3Backend::new(
        S3StorageBackend::new(Arc::new(CannedS3Client {
            keys: vec!["a".into(), "b".into()],
        })),
        Some("b".to_string()),
    );
    let outcome = deleted
        .delete_prefix("s3://b/p?region=us-east-1")
        .await
        .unwrap();
    assert_eq!(outcome, DeletePrefixOutcome::Deleted { count: 2 });

    let empty = S3Backend::new(
        S3StorageBackend::new(Arc::new(CannedS3Client { keys: vec![] })),
        Some("b".to_string()),
    );
    let outcome = empty
        .delete_prefix("s3://b/p?region=us-east-1")
        .await
        .unwrap();
    assert_eq!(outcome, DeletePrefixOutcome::AlreadyGone);
}
