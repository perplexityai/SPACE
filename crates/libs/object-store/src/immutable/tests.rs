use super::*;
use crate::test_support::{Recorder, credentials, every_class, serve, store, target};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode},
    response::Response,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Remote {
    bytes: Mutex<Option<Vec<u8>>>,
    gets: AtomicUsize,
}

async fn object(State(remote): State<Arc<Remote>>, request: Request) -> Response {
    if request.method() == Method::PUT {
        assert_eq!(request.headers()["if-none-match"], "*");
        let md5 = request.headers()["content-md5"]
            .to_str()
            .unwrap()
            .to_string();
        let body = to_bytes(request.into_body(), 1 << 20)
            .await
            .unwrap()
            .to_vec();
        assert_eq!(md5, content_md5::of_bytes(&body));
        let mut stored = remote.bytes.lock().unwrap();
        if stored.is_some() {
            return Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .body(Body::from("<Error><Code>PreconditionFailed</Code></Error>"))
                .unwrap();
        }
        *stored = Some(body);
        return Response::builder()
            .status(StatusCode::OK)
            .header("x-amz-version-id", "version-one")
            .body(Body::empty())
            .unwrap();
    }
    assert_eq!(request.method(), Method::GET);
    remote.gets.fetch_add(1, Ordering::SeqCst);
    let bytes = remote.bytes.lock().unwrap().clone().unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-length", bytes.len())
        .header("x-amz-version-id", "version-one")
        .body(Body::from(bytes))
        .unwrap()
}

#[tokio::test]
async fn immutable_put_requires_condition_and_verifies_existing_bytes_after_lost_receipt() {
    let remote = Arc::new(Remote::default());
    let endpoint = serve(
        Router::new()
            .fallback(object)
            .with_state(Arc::clone(&remote)),
    )
    .await;
    let backend = store(
        every_class(Duration::from_secs(2), Duration::from_secs(5)),
        Duration::from_secs(1),
        &Arc::new(Recorder::default()),
    );
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("pack");
    let bytes = b"independent frame fixture";
    std::fs::write(&source, bytes).unwrap();
    let digest = Digest::blake3(*blake3::hash(bytes).as_bytes());
    for _ in 0..2 {
        let receipt = backend
            .put_immutable_file(&credentials(), target(&endpoint), &source, digest)
            .await
            .unwrap();
        assert_eq!(receipt.bytes, bytes.len() as u64);
        assert_eq!(receipt.version_id.as_deref(), Some("version-one"));
    }
    assert_eq!(remote.gets.load(Ordering::SeqCst), 1);
    let corrupt = vec![b'x'; bytes.len()];
    *remote.bytes.lock().unwrap() = Some(corrupt.clone());
    backend
        .put_immutable_file(&credentials(), target(&endpoint), &source, digest)
        .await
        .unwrap_err();
    assert_eq!(remote.bytes.lock().unwrap().as_ref().unwrap(), &corrupt);
}

#[tokio::test]
async fn immutable_put_accepts_a_full_pack_and_rejects_size_overflow() {
    use futures_util::StreamExt;

    const PACK_BYTES: u64 = 256 << 20;
    let buffer = vec![0; 1 << 20];
    let mut expected = blake3::Hasher::new();
    for _ in 0..256 {
        expected.update(&buffer);
    }
    let expected = Digest::blake3(*expected.finalize().as_bytes());
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&requests);
    let endpoint = serve(Router::new().fallback(move |request: Request| {
        let observed = Arc::clone(&observed);
        async move {
            assert_eq!(request.method(), Method::PUT);
            assert_eq!(request.headers()["if-none-match"], "*");
            assert_eq!(request.headers()["content-length"], PACK_BYTES.to_string());
            observed.fetch_add(1, Ordering::SeqCst);
            let mut body = request.into_body().into_data_stream();
            let mut bytes = 0;
            let mut digest = blake3::Hasher::new();
            while let Some(frame) = body.next().await {
                let frame = frame.unwrap();
                bytes += frame.len() as u64;
                digest.update(&frame);
            }
            assert_eq!(bytes, PACK_BYTES);
            assert_eq!(Digest::blake3(*digest.finalize().as_bytes()), expected);
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .unwrap()
        }
    }))
    .await;
    let backend = store(
        every_class(Duration::from_secs(15), Duration::from_secs(30)),
        Duration::from_secs(5),
        &Arc::new(Recorder::default()),
    );
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("pack");
    let file = std::fs::File::create(&source).unwrap();
    for invalid_length in [0, PACK_BYTES + 1] {
        file.set_len(invalid_length).unwrap();
        backend
            .put_immutable_file(&credentials(), target(&endpoint), &source, expected)
            .await
            .unwrap_err();
    }
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    file.set_len(PACK_BYTES).unwrap();
    drop(file);
    let receipt = backend
        .put_immutable_file(&credentials(), target(&endpoint), &source, expected)
        .await
        .unwrap();
    assert_eq!(receipt.bytes, PACK_BYTES);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}
