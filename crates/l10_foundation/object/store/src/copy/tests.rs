use super::*;
use crate::ObjectStore;
use crate::test_support::{Recorder, credentials, every_class, serve, store, target};
use axum::Router;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[test]
fn copy_source_keeps_path_separators_and_encodes_digest_colons() {
    assert_eq!(
        copy_source_value(
            "example-template-artifacts",
            "templates/seed/sha256:3c36fb1e/rootfs.ext4.zst",
        ),
        "example-template-artifacts/templates/seed/sha256%3A3c36fb1e/rootfs.ext4.zst",
    );
}

#[test]
fn copy_source_encodes_characters_that_would_break_the_header() {
    // Not shapes a validated TemplateName/ContentHash can produce; the
    // encoder is the boundary that keeps that true even if a caller ever
    // hands over a raw key.
    assert_eq!(
        copy_source_value("bucket", "a b/c?d#e%f"),
        "bucket/a%20b/c%3Fd%23e%25f",
    );
}

/// The whole reason the threshold exists: every size above it must plan
/// into parts that are each themselves copyable.
#[test]
fn every_multipart_copy_part_stays_within_the_single_copy_limit() {
    let part_size = usize::try_from(MAX_SINGLE_COPY_BYTES).unwrap();
    for size in [
        MAX_SINGLE_COPY_BYTES + 1,
        6 * 1024 * 1024 * 1024,
        21 * 1024 * 1024 * 1024,
    ] {
        let ranges = multipart_part_ranges(size, part_size);
        assert!(
            ranges.len() > 1,
            "size={size} planned to {} part(s)",
            ranges.len()
        );
        let mut offset = 0;
        for range in &ranges {
            assert_eq!(range.offset, offset, "size={size} gap or overlap");
            assert!(
                range.length <= MAX_SINGLE_COPY_BYTES,
                "size={size} part {} is {} bytes",
                range.part_number,
                range.length,
            );
            offset += range.length;
        }
        assert_eq!(offset, size, "size={size} parts do not cover the object");
    }
}

#[tokio::test]
async fn explicit_copy_class_is_sent_and_verified_for_both_copy_paths() {
    for size in [MAX_SINGLE_COPY_BYTES, MAX_SINGLE_COPY_BYTES + 1] {
        for (class, reported_class, verified) in [
            (CopyStorageClass::Standard, None, true),
            (CopyStorageClass::Standard, Some("FUTURE_CLASS"), false),
            (
                CopyStorageClass::GlacierInstantRetrieval,
                Some("GLACIER_IR"),
                true,
            ),
            (CopyStorageClass::GlacierInstantRetrieval, None, false),
        ] {
            let endpoint = serve(Router::new().fallback(
                move |method: Method, uri: Uri, headers: HeaderMap| async move {
                    let query = uri.query().unwrap_or_default();
                    if method == Method::HEAD {
                        let mut response = StatusCode::OK.into_response();
                        let headers = response.headers_mut();
                        headers.insert("content-length", size.to_string().parse().unwrap());
                        if let Some(value) = reported_class {
                            headers.insert("x-amz-storage-class", value.parse().unwrap());
                        }
                        return response;
                    }
                    let class_header = headers.get("x-amz-storage-class");
                    let body = match method {
                        Method::PUT if query.contains("partNumber=") => {
                            assert!(class_header.is_none());
                            "<CopyPartResult><ETag>synthetic-etag</ETag></CopyPartResult>"
                        }
                        Method::PUT => {
                            assert_eq!(size, MAX_SINGLE_COPY_BYTES);
                            assert_eq!(class_header.unwrap(), class.as_str());
                            "<CopyObjectResult><ETag>synthetic-etag</ETag></CopyObjectResult>"
                        }
                        Method::POST if query.contains("uploads") => {
                            assert!(size > MAX_SINGLE_COPY_BYTES);
                            assert_eq!(class_header.unwrap(), class.as_str());
                            "<InitiateMultipartUploadResult><UploadId>synthetic-upload</UploadId></InitiateMultipartUploadResult>"
                        }
                        Method::POST if query.contains("uploadId=") => {
                            assert!(class_header.is_none());
                            "<CompleteMultipartUploadResult><ETag>synthetic-etag</ETag></CompleteMultipartUploadResult>"
                        }
                        _ => panic!("unexpected copy request: {method} {uri}"),
                    };
                    (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
                },
            ))
            .await;
            let mut store = store(
                every_class(Duration::from_secs(2), Duration::from_secs(10)),
                Duration::from_secs(1),
                &Arc::new(Recorder::default()),
            );
            store.multipart_part_size = usize::try_from(MAX_SINGLE_COPY_BYTES).unwrap();
            let source = ObjectTarget {
                key: "source",
                ..target(&endpoint)
            };
            let dest = ObjectTarget {
                key: "destination",
                ..target(&endpoint)
            };
            let result = store
                .copy_object_with_storage_class(&credentials(), source, dest, size, class)
                .await;
            if verified {
                assert_eq!(result.unwrap().size, size);
            } else {
                assert!(matches!(
                    result.unwrap_err(),
                    ObjectStoreError::CopyStorageClassMismatch { expected, actual }
                        if expected == class && actual == reported_class.unwrap_or("STANDARD")
                ));
            }
        }
    }
}

#[tokio::test]
async fn copy_errors_retain_their_classification_across_the_multipart_boundary() {
    for failing in [
        S3Operation::CopyObject,
        S3Operation::UploadPartCopy,
        S3Operation::CreateMultipartUpload,
        S3Operation::CompleteMultipartUpload,
    ] {
        let missing_code = match failing {
            S3Operation::CopyObject | S3Operation::UploadPartCopy => "NoSuchKey",
            _ => "NoSuchBucket",
        };
        for (status, code) in [
            (StatusCode::FORBIDDEN, "AccessDenied"),
            (StatusCode::NOT_FOUND, missing_code),
            (StatusCode::SERVICE_UNAVAILABLE, "SlowDown"),
        ] {
            let failures = Arc::new(AtomicUsize::new(0));
            let aborted = Arc::new(Notify::new());
            let endpoint = serve(copy_error_server(
                failing, status, code, &failures, &aborted,
            ))
            .await;
            let recorder = Arc::new(Recorder::default());
            let mut store = store(
                every_class(Duration::from_secs(2), Duration::from_secs(10)),
                Duration::from_secs(1),
                &recorder,
            );
            store.multipart_part_size = usize::try_from(MAX_SINGLE_COPY_BYTES).unwrap();
            store.multipart_concurrency = 1;
            let source = ObjectTarget {
                key: "source",
                ..target(&endpoint)
            };
            let dest = ObjectTarget {
                key: "destination",
                ..target(&endpoint)
            };
            let size = if failing == S3Operation::CopyObject {
                MAX_SINGLE_COPY_BYTES
            } else {
                MAX_SINGLE_COPY_BYTES + 1
            };
            let error = store
                .copy_object(&credentials(), source, dest, size)
                .await
                .unwrap_err();
            match status {
                StatusCode::FORBIDDEN => assert!(
                    matches!(error, ObjectStoreError::PermissionDenied { .. }),
                    "{failing:?} {code}: {error:?}"
                ),
                StatusCode::NOT_FOUND => {
                    let expected_key = match failing {
                        S3Operation::CopyObject | S3Operation::UploadPartCopy => {
                            "synthetic-bucket/source"
                        }
                        _ => "destination",
                    };
                    assert!(
                        matches!(&error, ObjectStoreError::NotFound { key } if key == expected_key),
                        "{failing:?} {code}: {error:?}"
                    );
                }
                _ => assert!(
                    matches!(
                        error,
                        ObjectStoreError::Copy(_) | ObjectStoreError::Multipart(_)
                    ),
                    "{failing:?} {code}: {error:?}"
                ),
            }
            assert_eq!(
                failures.load(Ordering::SeqCst),
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    3
                } else {
                    1
                }
            );
            if matches!(
                failing,
                S3Operation::UploadPartCopy | S3Operation::CompleteMultipartUpload
            ) {
                tokio::time::timeout(Duration::from_secs(5), aborted.notified())
                    .await
                    .expect("failed multipart copy must abort its upload");
            }
        }
    }
}

fn copy_error_server(
    failing: S3Operation,
    status: StatusCode,
    code: &'static str,
    failures: &Arc<AtomicUsize>,
    aborted: &Arc<Notify>,
) -> Router {
    let failures = failures.clone();
    let aborted = aborted.clone();
    Router::new().fallback(move |method: Method, uri: Uri, headers: HeaderMap| {
        let failures = failures.clone();
        let aborted = aborted.clone();
        async move {
            assert!(headers.get("x-amz-storage-class").is_none());
            let operation = match method {
                Method::PUT if uri.query().is_some_and(|query| query.contains("partNumber=")) => {
                    S3Operation::UploadPartCopy
                }
                Method::PUT => S3Operation::CopyObject,
                Method::POST if uri.query().is_some_and(|query| query.contains("uploads")) => {
                    S3Operation::CreateMultipartUpload
                }
                Method::POST => S3Operation::CompleteMultipartUpload,
                Method::DELETE => {
                    aborted.notify_one();
                    return StatusCode::NO_CONTENT.into_response();
                }
                _ => panic!("unexpected copy request: {method} {uri}"),
            };
            if operation == failing {
                failures.fetch_add(1, Ordering::SeqCst);
                return (
                    status,
                    [("content-type", "application/xml")],
                    format!("<Error><Code>{code}</Code><Message>synthetic copy rejection</Message></Error>"),
                ).into_response();
            }
            let body = match operation {
                S3Operation::CreateMultipartUpload => "<InitiateMultipartUploadResult><UploadId>synthetic-upload</UploadId></InitiateMultipartUploadResult>",
                S3Operation::UploadPartCopy => "<CopyPartResult><ETag>synthetic-etag</ETag></CopyPartResult>",
                _ => panic!("unexpected successful copy request: {operation:?}"),
            };
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
    })
}
