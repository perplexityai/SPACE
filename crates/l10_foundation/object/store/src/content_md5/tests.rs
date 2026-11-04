use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use md5::{Digest as _, Md5};

use crate::{ObjectCredentials, ObjectStore, ObjectStoreError, ObjectTarget, S3ObjectStore};

const UPLOAD_ID: &str = "test-upload";

/// One PutObject or UploadPart body as the S3 stand-in received it.
#[derive(Clone, Debug)]
struct ReceivedUpload {
    part_number: Option<i32>,
    content_md5: Option<String>,
    body: Bytes,
}

/// In-process S3 stand-in that records every upload body off the wire and,
/// like a store that enforces the digest, answers `400 BadDigest` to an upload
/// whose `Content-MD5` is missing or does not match the received body.
/// `reject_every_upload` answers `BadDigest` to every upload regardless, to
/// exercise how the client surfaces the rejection.
#[derive(Clone, Default)]
struct FakeS3 {
    uploads: Arc<Mutex<Vec<ReceivedUpload>>>,
    reject_every_upload: bool,
}

impl FakeS3 {
    async fn serve(self) -> String {
        let app = Router::new()
            .fallback(handle)
            .layer(DefaultBodyLimit::disable())
            .with_state(self);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind S3 stand-in");
        let addr = listener.local_addr().expect("S3 stand-in address");
        tokio::spawn(async move { axum::serve(listener, app).await });
        format!("http://{addr}")
    }

    fn uploads(&self) -> Vec<ReceivedUpload> {
        self.uploads.lock().expect("uploads lock").clone()
    }
}

fn query_param<'a>(uri: &'a Uri, name: &str) -> Option<&'a str> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then_some(value)
    })
}

async fn handle(
    State(s3): State<FakeS3>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let xml = |status: StatusCode, body: String| {
        (status, [("content-type", "application/xml")], body).into_response()
    };
    match method {
        Method::POST if query_param(&uri, "uploads").is_some() => xml(
            StatusCode::OK,
            format!(
                "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
                 <UploadId>{UPLOAD_ID}</UploadId></InitiateMultipartUploadResult>"
            ),
        ),
        Method::POST => xml(
            StatusCode::OK,
            "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
             <ETag>\"multipart\"</ETag></CompleteMultipartUploadResult>"
                .to_string(),
        ),
        Method::PUT => {
            let part_number = query_param(&uri, "partNumber")
                .map(|part| part.parse().expect("numeric partNumber"));
            let content_md5 = headers
                .get("content-md5")
                .map(|value| value.to_str().expect("ASCII Content-MD5").to_string());
            let digest_matches =
                content_md5.as_deref() == Some(BASE64_STANDARD.encode(Md5::digest(&body)).as_str());
            s3.uploads
                .lock()
                .expect("uploads lock")
                .push(ReceivedUpload {
                    part_number,
                    content_md5,
                    body,
                });
            if s3.reject_every_upload || !digest_matches {
                return xml(
                    StatusCode::BAD_REQUEST,
                    "<Error><Code>BadDigest</Code><Message>The Content-MD5 you \
                     specified did not match what we received.</Message></Error>"
                        .to_string(),
                );
            }
            let etag = format!("\"etag-{}\"", part_number.unwrap_or(0));
            (StatusCode::OK, [("etag", etag)]).into_response()
        }
        Method::DELETE => StatusCode::NO_CONTENT.into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn creds() -> ObjectCredentials {
    ObjectCredentials {
        access_key_id: "test-access-key".to_string(),
        secret_access_key: "test-secret-key".to_string(),
        session_token: None,
    }
}

fn target(endpoint: &str) -> ObjectTarget<'_> {
    ObjectTarget {
        bucket: "b",
        key: "k",
        region: "us-east-1",
        endpoint: Some(endpoint),
        path_style: true,
    }
}

/// Pseudo-random bytes, so a digest over the wrong range cannot match by
/// accident.
fn test_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn write_source(dir: &tempfile::TempDir, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.path().join("src");
    std::fs::write(&path, bytes).expect("write upload source");
    path
}

#[track_caller]
fn assert_digest_matches_body(upload: &ReceivedUpload) {
    assert_eq!(
        upload.content_md5.as_deref(),
        Some(BASE64_STANDARD.encode(Md5::digest(&upload.body)).as_str()),
        "part {:?} ({} bytes) must carry the Content-MD5 of its body",
        upload.part_number,
        upload.body.len(),
    );
}

#[tokio::test]
async fn single_put_file_sends_content_md5_of_the_file() {
    let s3 = FakeS3::default();
    let endpoint = s3.clone().serve().await;
    let dir = tempfile::tempdir().expect("tempdir");
    // Spans several hashing chunks with a ragged tail.
    let source = test_bytes(3 * 1024 * 1024 + 7);
    let path = write_source(&dir, &source);

    let uploaded = S3ObjectStore::new()
        .put_file(&creds(), target(&endpoint), &path)
        .await
        .expect("single put");

    assert_eq!(uploaded, source.len() as u64);
    let uploads = s3.uploads();
    assert_eq!(uploads.len(), 1, "one PutObject");
    assert_eq!(uploads[0].part_number, None);
    assert_eq!(uploads[0].body.as_ref(), source.as_slice());
    assert_digest_matches_body(&uploads[0]);
}

#[tokio::test]
async fn put_bytes_sends_content_md5_of_the_body() {
    let s3 = FakeS3::default();
    let endpoint = s3.clone().serve().await;
    let source = Bytes::from(test_bytes(64 * 1024 + 3));

    S3ObjectStore::new()
        .put_bytes(&creds(), target(&endpoint), source.clone())
        .await
        .expect("put_bytes");

    let uploads = s3.uploads();
    assert_eq!(uploads.len(), 1, "one PutObject");
    assert_eq!(uploads[0].body, source);
    assert_digest_matches_body(&uploads[0]);
}

#[tokio::test]
async fn every_multipart_part_sends_content_md5_of_its_range() {
    let s3 = FakeS3::default();
    let endpoint = s3.clone().serve().await;
    let dir = tempfile::tempdir().expect("tempdir");
    // Three parts of 8_738_134, 8_738_134 and a shorter final 8_738_133 bytes.
    let source = test_bytes(25 * 1024 * 1024 + 1);
    let path = write_source(&dir, &source);
    let store = S3ObjectStore {
        multipart_part_size: 10 * 1024 * 1024,
        ..S3ObjectStore::new()
    };

    let uploaded = store
        .put_file(&creds(), target(&endpoint), &path)
        .await
        .expect("multipart upload");

    assert_eq!(uploaded, source.len() as u64);
    let mut uploads = s3.uploads();
    uploads.sort_by_key(|upload| upload.part_number);
    let part_lengths: Vec<_> = uploads
        .iter()
        .map(|upload| (upload.part_number, upload.body.len()))
        .collect();
    assert_eq!(
        part_lengths,
        [
            (Some(1), 8_738_134),
            (Some(2), 8_738_134),
            (Some(3), 8_738_133),
        ],
    );
    for upload in &uploads {
        assert_digest_matches_body(upload);
    }
    let reassembled: Vec<u8> = uploads
        .iter()
        .flat_map(|upload| upload.body.iter().copied())
        .collect();
    assert!(reassembled == source, "parts must reassemble the source");
}

#[tokio::test]
async fn bad_digest_rejection_surfaces_as_put_and_multipart_errors() {
    let s3 = FakeS3 {
        reject_every_upload: true,
        ..FakeS3::default()
    };
    let endpoint = s3.clone().serve().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let small = write_source(&dir, &test_bytes(1024));
    let store = S3ObjectStore {
        multipart_part_size: 10 * 1024 * 1024,
        ..S3ObjectStore::new()
    };

    let single = store.put_file(&creds(), target(&endpoint), &small).await;
    assert!(
        matches!(&single, Err(ObjectStoreError::Put(message)) if message.contains("BadDigest")),
        "single put must fail with BadDigest, got {single:?}",
    );

    let bytes = store
        .put_bytes(&creds(), target(&endpoint), Bytes::from(test_bytes(1024)))
        .await;
    assert!(
        matches!(&bytes, Err(ObjectStoreError::Put(message)) if message.contains("BadDigest")),
        "put_bytes must fail with BadDigest, got {bytes:?}",
    );

    let large_dir = tempfile::tempdir().expect("tempdir");
    let large = write_source(&large_dir, &test_bytes(25 * 1024 * 1024 + 1));
    let multipart = store.put_file(&creds(), target(&endpoint), &large).await;
    assert!(
        matches!(&multipart, Err(ObjectStoreError::Multipart(message)) if message.contains("BadDigest")),
        "multipart upload must fail with BadDigest, got {multipart:?}",
    );
}

#[test]
fn file_range_past_end_of_file_is_unexpected_eof() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_source(&dir, &test_bytes(1024));

    let err = super::of_file_range(&path, 512, 1024, &AtomicBool::new(false))
        .expect_err("a range past the end of the file must not digest");

    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof, "{err}");
}

/// Reader of `remaining` zero bytes that counts what it hands out and sets
/// `cancel` after its first read, as a dropped part future would mid-digest.
struct CancelAfterFirstRead<'a> {
    remaining: u64,
    read: &'a AtomicU64,
    cancel: &'a AtomicBool,
}

impl Read for CancelAfterFirstRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        buf[..len].fill(0);
        self.remaining -= len as u64;
        self.read.fetch_add(len as u64, Ordering::Relaxed);
        self.cancel.store(true, Ordering::Relaxed);
        Ok(len)
    }
}

#[test]
fn cancelled_digest_stops_at_the_next_chunk_without_a_digest() {
    let range_bytes = 64 * super::READ_CHUNK_BYTES as u64;
    let read = AtomicU64::new(0);
    let cancel = AtomicBool::new(false);
    let reader = CancelAfterFirstRead {
        remaining: range_bytes,
        read: &read,
        cancel: &cancel,
    };

    let result = super::digest_chunks(reader, &cancel);

    assert!(
        result.is_err(),
        "a cancelled digest must not produce a digest, got {result:?}"
    );
    assert_eq!(
        read.load(Ordering::Relaxed),
        super::READ_CHUNK_BYTES as u64,
        "cancellation must stop the read after one chunk of the {range_bytes}-byte range",
    );
}

#[test]
fn dropping_the_guard_cancels_the_file_range_digest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_source(&dir, &test_bytes(1024));
    let cancel = super::CancelOnDrop::default();
    let cancelled = cancel.flag();

    drop(cancel);
    let result = super::of_file_range(&path, 0, 1024, &cancelled);

    assert!(
        result.is_err(),
        "a cancelled digest must not produce a digest, got {result:?}"
    );
}
