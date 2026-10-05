use super::*;
use pplx_space_common_core::types::SandboxId;
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const MULTIPART_BOUNDARY: &str = "space-test-boundary";

fn multipart_body(parts: &[(&str, Option<&str>, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, filename, data) in parts {
        body.extend_from_slice(format!("--{MULTIPART_BOUNDARY}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"").as_bytes(),
        );
        if let Some(filename) = filename {
            body.extend_from_slice(format!("; filename=\"{filename}\"").as_bytes());
        }
        body.extend_from_slice(b"\r\n\r\n");
        body.extend_from_slice(data);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{MULTIPART_BOUNDARY}--\r\n").as_bytes());
    body
}

fn chunked_body(chunks: Vec<Vec<u8>>) -> Body {
    Body::from_stream(stream::iter(chunks.into_iter().map(Ok::<_, Infallible>)))
}

async fn collect_spooled_write(
    mut upload: SpooledUpload,
    path: &str,
) -> (SpacedWriteOpen, Vec<u8>) {
    upload.rewind().await.unwrap();
    let file = tokio::fs::File::open(upload._path.to_path_buf())
        .await
        .unwrap();
    let requests = spooled_write_requests(SandboxId::new(), path.to_string(), file, None)
        .collect::<Vec<_>>()
        .await;
    let mut requests = requests.into_iter();
    let open = requests.next().unwrap().operation.unwrap().message.unwrap();
    let spaced_write_request::Message::Open(open) = open else {
        panic!("first request must open the file");
    };
    let mut data = Vec::new();
    for request in requests {
        let spaced_write_request::Message::Data(chunk) =
            request.operation.unwrap().message.unwrap()
        else {
            panic!("requests after open must carry file data");
        };
        data.extend_from_slice(&chunk);
    }
    (open, data)
}

#[test]
fn e2b_paths_pass_valid_wire_paths_through() {
    assert_eq!(e2b_request_root("src/lib.rs", None).unwrap(), "src/lib.rs");
    assert_eq!(e2b_request_root("/tmp/file", None).unwrap(), "/tmp/file");
    let root = GuestUserName::parse("root").unwrap();
    assert_eq!(
        e2b_request_root("project/main.py", Some(&root)).unwrap(),
        "project/main.py"
    );
}

#[test]
fn e2b_paths_reject_control_and_windows_paths() {
    e2b_request_root("/tmp/\nfile", None).unwrap_err();
    e2b_request_root("C:\\Users\\user\\file.txt", None).unwrap_err();
    e2b_request_root("C:/Users/user/file.txt", None).unwrap_err();
    e2b_request_root("\\\\server\\share\\file.txt", None).unwrap_err();
    e2b_request_root("//server/share/file.txt", None).unwrap_err();
}

#[tokio::test]
async fn multipart_single_file_is_spooled_with_filename_and_content() {
    let body = multipart_body(&[("file", Some("nested/a.txt"), b"hello")]);
    let split = body.len() / 2;
    let uploads = spool_multipart_uploads(
        chunked_body(vec![body[..split].to_vec(), body[split..].to_vec()]),
        MULTIPART_BOUNDARY,
    )
    .await
    .unwrap();
    let mut writes = resolve_multipart_uploads(uploads, None, None).unwrap();

    assert_eq!(writes.len(), 1);
    let (path, upload) = writes.pop().unwrap();
    let (open, data) = collect_spooled_write(upload, &path).await;
    assert_eq!(path, "nested/a.txt");
    assert_eq!(open.path, "nested/a.txt");
    assert_eq!(data, b"hello");
}

#[tokio::test]
async fn multipart_multiple_files_are_spooled_independently() {
    let body = multipart_body(&[
        ("metadata", None, b"ignored"),
        ("file", Some("a.txt"), b"alpha"),
        ("file", Some("b.txt"), b"beta"),
    ]);
    let uploads = spool_multipart_uploads(
        chunked_body(body.chunks(17).map(<[u8]>::to_vec).collect()),
        MULTIPART_BOUNDARY,
    )
    .await
    .unwrap();
    let writes = resolve_multipart_uploads(uploads, None, None).unwrap();

    assert_eq!(writes.len(), 2);
    let mut actual = Vec::new();
    for (path, upload) in writes {
        let (open, data) = collect_spooled_write(upload, &path).await;
        actual.push((open.path, data));
    }
    assert_eq!(
        actual,
        vec![
            ("a.txt".to_string(), b"alpha".to_vec()),
            ("b.txt".to_string(), b"beta".to_vec()),
        ]
    );
}

#[tokio::test]
async fn multipart_path_query_overrides_filename_and_rejects_multiple_files() {
    let single = multipart_body(&[("file", Some("ignored.txt"), b"content")]);
    let uploads = spool_multipart_uploads(Body::from(single), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    let mut writes =
        resolve_multipart_uploads(uploads, Some("override.txt".to_string()), None).unwrap();
    let (path, upload) = writes.pop().unwrap();
    let (open, data) = collect_spooled_write(upload, &path).await;
    assert_eq!(path, "override.txt");
    assert_eq!(open.path, "override.txt");
    assert_eq!(data, b"content");

    let multiple = multipart_body(&[
        ("file", Some("a.txt"), b"alpha"),
        ("file", Some("b.txt"), b"beta"),
    ]);
    let uploads = spool_multipart_uploads(Body::from(multiple), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    let error =
        resolve_multipart_uploads(uploads, Some("override.txt".to_string()), None).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "path query parameter only supports a single multipart upload"
    );
}

#[tokio::test]
async fn multipart_requires_filename_or_path() {
    let body = multipart_body(&[("file", None, b"content")]);
    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    let error = resolve_multipart_uploads(uploads, None, None).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "multipart uploads require a filename or path query"
    );
}

#[tokio::test]
async fn malformed_multipart_and_missing_boundary_are_rejected() {
    assert_eq!(multipart_boundary("multipart/form-data"), None);
    let error = spool_multipart_uploads(
        Body::from(multipart_body(&[("file", Some("a.txt"), b"content")])),
        "different-boundary",
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.message, "Invalid multipart body");

    let missing_disposition = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Type: application/octet-stream\r\n\r\ncontent\r\n--{MULTIPART_BOUNDARY}--\r\n"
    );
    let error = spool_multipart_uploads(Body::from(missing_disposition), MULTIPART_BOUNDARY)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.message, "Missing multipart Content-Disposition");
}

#[tokio::test]
async fn multipart_file_limit_stops_consuming_the_body_mid_stream() {
    let polls = Arc::new(AtomicUsize::new(0));
    let counted_polls = Arc::clone(&polls);
    let first = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n\r\n{}",
        "a".repeat(128),
    )
    .into_bytes();
    let stream = stream::iter([
        first,
        vec![b'b'; 128],
        format!("\r\n--{MULTIPART_BOUNDARY}--\r\n").into_bytes(),
    ])
    .inspect(move |_| {
        counted_polls.fetch_add(1, Ordering::SeqCst);
    })
    .map(Ok::<_, Infallible>);

    let error =
        spool_multipart_uploads_with_limit(Body::from_stream(stream), MULTIPART_BOUNDARY, 8)
            .await
            .unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(polls.load(Ordering::SeqCst) < 3);
}

#[tokio::test]
async fn oversized_json_file_write_body_is_rejected() {
    let body = Body::from(vec![b'x'; E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES + 1]);
    let error = buffer_json_file_write_body(body).await.unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(error.message.contains("use multipart/form-data"));
}

#[tokio::test]
async fn multipart_limit_caps_the_whole_body_across_file_parts() {
    let body = multipart_body(&[
        ("file", Some("a.txt"), &[b'a'; 6]),
        ("file", Some("b.txt"), &[b'b'; 6]),
    ]);
    let limit = body.len() - 1;
    let error = spool_multipart_uploads_with_limit(Body::from(body), MULTIPART_BOUNDARY, limit)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn multipart_part_without_header_terminator_is_rejected_before_buffering_the_body() {
    let frame_len = 64 * 1024;
    let frames = 4 * E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES / frame_len;
    let polls = Arc::new(AtomicUsize::new(0));
    let counted_polls = Arc::clone(&polls);
    let header = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\nX-Padding: "
    )
    .into_bytes();
    let stream = stream::iter(
        std::iter::once(header).chain((0..frames).map(move |_| vec![b'a'; frame_len])),
    )
    .inspect(move |_| {
        counted_polls.fetch_add(1, Ordering::SeqCst);
    })
    .map(Ok::<_, Infallible>);

    let error = spool_multipart_uploads(Body::from_stream(stream), MULTIPART_BOUNDARY)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(polls.load(Ordering::SeqCst) < frames / 2);
}

#[tokio::test]
async fn multipart_file_larger_than_the_parse_buffer_streams_in_frames() {
    let frame_len = 64 * 1024;
    let content = (0..3 * E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let body = multipart_body(&[
        (
            "note",
            None,
            &vec![b'n'; 2 * E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES],
        ),
        ("file", Some("large.bin"), &content),
    ]);
    let frames = body.chunks(frame_len).map(<[u8]>::to_vec).collect();

    let uploads = spool_multipart_uploads(chunked_body(frames), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    let upload = uploads.into_iter().next().unwrap();
    assert_eq!(upload.filename.as_deref(), Some("large.bin"));
    let (_, data) = collect_spooled_write(upload.upload, "/large.bin").await;
    assert_eq!(data, content);
}

#[tokio::test]
async fn oversized_json_file_write_returns_413_to_a_client_still_sending() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn handler(headers: HeaderMap, body: Body) -> Result<StatusCode, EcwProxyError> {
        read_json_file_write_request(&headers, body).await?;
        Ok(StatusCode::OK)
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = axum::Router::new().route("/files", axum::routing::post(handler));
        axum::serve(listener, app).await.unwrap();
    });

    // The client keeps sending far past the cap, with and without a declared
    // length, and must still read the 413 before the server closes the socket.
    let body_len = 16 * E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES;
    for chunked in [false, true] {
        let framing = if chunked {
            "transfer-encoding: chunked".to_string()
        } else {
            format!("content-length: {body_len}")
        };
        let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        socket
            .write_all(
                format!(
                    "POST /files HTTP/1.1\r\nhost: data-plane\r\ncontent-type: application/json\r\n{framing}\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let (mut reader, mut writer) = socket.into_split();
        let sender = tokio::spawn(async move {
            let chunk = vec![b'x'; 64 * 1024];
            let mut framed = format!("{:x}\r\n", chunk.len()).into_bytes();
            framed.extend_from_slice(&chunk);
            framed.extend_from_slice(b"\r\n");
            for _ in 0..body_len / chunk.len() {
                let frame = if chunked { &framed } else { &chunk };
                if writer.write_all(frame).await.is_err() {
                    return;
                }
            }
        });
        let mut response = Vec::new();
        let mut buf = [0u8; 4096];
        while let Ok(read) = reader.read(&mut buf).await {
            if read == 0 {
                break;
            }
            response.extend_from_slice(&buf[..read]);
        }
        sender.await.unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.starts_with("HTTP/1.1 413 "),
            "chunked={chunked}: {response}"
        );
        assert!(response.contains("use multipart/form-data"), "{response}");
    }
}

#[test]
fn watcher_id_round_trips_root() {
    let watcher_id = encode_e2b_watcher_id("watch-1", "/workspace/src");
    assert_eq!(
        decode_e2b_watcher_id(&watcher_id).unwrap(),
        ("watch-1".to_string(), "/workspace/src".to_string()),
    );
}

#[test]
fn e2b_event_names_are_relative_to_watch_root() {
    assert_eq!(
        e2b_event_name("/workspace/src", "/workspace/src/lib.rs").as_deref(),
        Some("lib.rs")
    );
    assert_eq!(
        e2b_event_name("/workspace/src/", "/workspace/src/nested/mod.rs").as_deref(),
        Some("nested/mod.rs")
    );
    assert_eq!(
        e2b_event_name("/workspace/src", "/workspace/src").as_deref(),
        Some(".")
    );
    assert_eq!(
        e2b_event_name("/workspace/src", "already-relative.rs").as_deref(),
        Some("already-relative.rs")
    );
    assert_eq!(
        e2b_event_name("/workspace/src", "./relative.rs").as_deref(),
        Some("relative.rs")
    );
    assert!(e2b_event_name("/workspace/src", "/workspace/src2/lib.rs").is_none());
}

#[test]
fn e2b_event_from_spaced_filters_outside_root() {
    let event = SpacedFileEvent {
        path: "/workspace/src/lib.rs".to_string(),
        kind: SpacedFileEventKind::Modified as i32,
        observed_at: None,
    };
    let event = e2b_event_from_spaced("/workspace/src", event).unwrap();
    assert_eq!(event.name, "lib.rs");
    assert_eq!(
        E2bEventType::try_from(event.event_type).unwrap(),
        E2bEventType::Write
    );

    let outside = SpacedFileEvent {
        path: "/workspace/src2/lib.rs".to_string(),
        kind: SpacedFileEventKind::Modified as i32,
        observed_at: None,
    };
    assert!(e2b_event_from_spaced("/workspace/src", outside).is_none());
}

#[test]
fn content_length_precheck_rejects_oversized_uploads() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_LENGTH,
        (E2B_FILE_CONTENT_LIMIT_BYTES + 1)
            .to_string()
            .parse()
            .unwrap(),
    );
    let error = reject_oversized_content_length(&headers).unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn octet_stream_upload_is_spooled_and_written_in_chunks() {
    let sandbox_id = SandboxId::new();
    let upload = spool_body_upload(chunked_body(vec![b"hello ".to_vec(), b"world".to_vec()]))
        .await
        .unwrap();
    let file = tokio::fs::File::open(upload._path.to_path_buf())
        .await
        .unwrap();

    let requests = spooled_write_requests(
        sandbox_id,
        "/tmp/hello.txt".to_string(),
        file,
        Some(GuestUserName::parse("user").unwrap()),
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(requests.len(), 2);
    let open = requests[0]
        .operation
        .as_ref()
        .unwrap()
        .message
        .as_ref()
        .unwrap();
    let spaced_write_request::Message::Open(open) = open else {
        panic!("first request must open the file");
    };
    assert_eq!(open.path, "/tmp/hello.txt");
    assert_eq!(open.user.as_deref(), Some("user"));

    let data = requests[1]
        .operation
        .as_ref()
        .unwrap()
        .message
        .as_ref()
        .unwrap();
    let spaced_write_request::Message::Data(data) = data else {
        panic!("second request must carry upload data");
    };
    assert_eq!(data, b"hello world");
}

#[tokio::test]
async fn in_memory_write_requests_are_replayable() {
    let requests = ReplayableSpacedWrite::new(
        SandboxId::new(),
        "/tmp/replayed.txt".to_string(),
        b"replayed body".to_vec(),
        Some(GuestUserName::parse("user").unwrap()),
    );

    let first = requests
        .stream()
        .map(|request| request.encode_to_vec())
        .collect::<Vec<_>>()
        .await;
    let second = requests
        .stream()
        .map(|request| request.encode_to_vec())
        .collect::<Vec<_>>()
        .await;

    assert_eq!(first, second);
    assert_eq!(first.len(), 2);
}
