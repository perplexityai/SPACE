use super::multipart::*;
use super::*;
use pplx_space_common_core::types::{MAX_GUEST_PATH_BYTES, SandboxId};
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
    upload: tempfile::TempPath,
    path: &str,
) -> (SpacedWriteOpen, Vec<u8>) {
    let file = tokio::fs::File::open(upload.to_path_buf()).await.unwrap();
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
async fn multipart_whole_stream_limit_rejects_without_reading_the_rest() {
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
    let frame_len = E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES / 16;
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
async fn multipart_framing_lower_bound_never_exceeds_the_encoded_part() {
    let part = multipart_body(&[("file", Some("a.txt"), b"")]);
    let closing = format!("--{MULTIPART_BOUNDARY}--\r\n").len();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_DISPOSITION,
        "form-data; name=\"file\"; filename=\"a.txt\""
            .parse()
            .unwrap(),
    );
    let estimate = multipart_part_framing_lower_bound(MULTIPART_BOUNDARY, &headers)
        + MULTIPART_PART_DATA_TERMINATOR_BYTES;
    assert!(
        estimate <= part.len() - closing,
        "{estimate} vs {}",
        part.len() - closing
    );

    // httparse accepts bare-LF header lines inside the CRLF-terminated block,
    // so a non-final header can cost just `name:value\n`. Multer must accept
    // this shortest encoding, and the estimate must still fit inside it.
    let shortest = format!(
        "--{MULTIPART_BOUNDARY}\r\ncontent-type:a\ncontent-disposition:form-data; name=\"file\"; filename=\"a.txt\"\r\n\r\n\r\n"
    );
    let uploads = spool_multipart_uploads(
        Body::from(format!("{shortest}--{MULTIPART_BOUNDARY}--\r\n")),
        MULTIPART_BOUNDARY,
    )
    .await
    .unwrap();
    assert_eq!(uploads.len(), 1);
    headers.insert(header::CONTENT_TYPE, "a".parse().unwrap());
    let estimate = multipart_part_framing_lower_bound(MULTIPART_BOUNDARY, &headers)
        + MULTIPART_PART_DATA_TERMINATOR_BYTES;
    assert!(
        estimate <= shortest.len(),
        "{estimate} vs {}",
        shortest.len()
    );
}

#[tokio::test]
async fn multipart_feed_pauses_once_unconsumed_bytes_pass_the_parse_buffer_cap() {
    use futures_util::FutureExt as _;

    let frame_len = E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES / 16;
    let frames =
        vec![vec![b'a'; frame_len]; 4 * E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES / frame_len];
    let consumed = Arc::new(AtomicUsize::new(0));
    let mut stream = Box::pin(throttled_multipart_body(
        chunked_body(frames),
        Arc::clone(&consumed),
    ));
    let mut fed = 0;
    while let Some(Some(frame)) = stream.next().now_or_never() {
        fed += frame.unwrap().len();
    }
    // Feeding pauses after the first frame that takes the buffer past the cap.
    assert_eq!(fed, E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES + frame_len);

    // Progress by the parser resumes feeding.
    consumed.store(fed, Ordering::Relaxed);
    assert!(matches!(stream.next().now_or_never(), Some(Some(Ok(_)))));

    // A pause the parser cannot answer with progress fails the stream.
    while let Some(Some(frame)) = stream.next().now_or_never() {
        frame.unwrap();
    }
    assert!(matches!(stream.next().now_or_never(), Some(Some(Err(_)))));
}

#[tokio::test]
async fn multipart_file_part_with_filename_before_name_is_spooled() {
    // Multer's Content-Disposition parser loses `name` after a quoted filename
    // containing " name"; the part must still be recognized as the file.
    let body = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; filename=\"my name.txt\"; name=\"file\"\r\n\r\ncontent\r\n--{MULTIPART_BOUNDARY}--\r\n"
    );
    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    let upload = uploads.into_iter().next().unwrap();
    assert_eq!(upload.filename.as_deref(), Some("my name.txt"));
    let (_, data) = collect_spooled_write(upload.upload, "/my name.txt").await;
    assert_eq!(data, b"content");
}

#[test]
fn content_disposition_parse_ignores_text_inside_quoted_values() {
    let parse = |value: &[u8]| {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_DISPOSITION,
            axum::http::HeaderValue::from_bytes(value).unwrap(),
        );
        ContentDispositionE2b::parse(&headers)
    };
    let parsed = |name: Option<&str>, filename: Option<&str>| ContentDispositionE2b {
        name: name.map(str::to_string),
        filename: filename.map(str::to_string),
    };
    let cases: [(&[u8], ContentDispositionE2b); 14] = [
        (
            br#"form-data; name="file"; filename="a.txt""#,
            parsed(Some("file"), Some("a.txt")),
        ),
        (
            br#"form-data; filename="a name=b.txt"; name="file""#,
            parsed(Some("file"), Some("a name=b.txt")),
        ),
        (
            br#"form-data; filename="x;name=file;"; name="meta""#,
            parsed(Some("meta"), Some("x;name=file;")),
        ),
        (
            br#"form-data; name="a filename=x"; filename="real.txt""#,
            parsed(Some("a filename=x"), Some("real.txt")),
        ),
        (
            br#"form-data; filename="q\" name=x"; NAME=file"#,
            parsed(Some("file"), Some(r#"q" name=x"#)),
        ),
        // Browsers send `"` as `%22` and a backslash as itself.
        (
            br#"form-data; name="file"; filename="dir\report%22.txt""#,
            parsed(Some("file"), Some(r"dir\report%22.txt")),
        ),
        (
            "form-data; filename=\"\u{fc} name.txt\"; name=\"file\"".as_bytes(),
            parsed(Some("file"), Some("\u{fc} name.txt")),
        ),
        (
            br#"form-data; name=file; filename="a.txt"; filename="b.txt""#,
            parsed(Some("file"), Some("a.txt")),
        ),
        (
            br#"form-data; filename="a.txt""#,
            parsed(None, Some("a.txt")),
        ),
        // A browser filename ending in a backslash, alone and followed by
        // another parameter.
        (
            br#"form-data; name="file"; filename="dir\""#,
            parsed(Some("file"), Some(r"dir\")),
        ),
        (
            br#"form-data; filename="dir\" ; name="file""#,
            parsed(Some("file"), Some(r"dir\")),
        ),
        // httpx and Go write quoted-pairs for backslashes and quotes.
        (
            br#"form-data; name="file"; filename="dir\\""#,
            parsed(Some("file"), Some(r"dir\")),
        ),
        (
            br#"form-data; name="file"; filename="dir\\f""#,
            parsed(Some("file"), Some(r"dir\f")),
        ),
        (
            br#"form-data; name="file"; filename="a\\\"b""#,
            parsed(Some("file"), Some(r#"a\"b"#)),
        ),
    ];
    for (value, expected) in cases {
        let shown = String::from_utf8_lossy(value).into_owned();
        assert_eq!(parse(value).unwrap(), expected, "{shown}");
    }
    for malformed in [
        &br#"form-data; name="file"; filename="a.txt"#[..],
        br#"form-data; name="file"; filename="a.txt"x"#,
        br#"form-data; name="file"; filename=a.txt""#,
        br#"form-data; filename=a"b; name="file""#,
        b"form-data; name=\"file\"; filename=\"\xff.txt\"",
    ] {
        let shown = String::from_utf8_lossy(malformed).into_owned();
        let error = parse(malformed).unwrap_err();
        assert_eq!(
            error.message, "Invalid multipart Content-Disposition",
            "{shown}"
        );
    }
}

#[tokio::test]
async fn multipart_file_part_with_an_impossible_filename_is_rejected_before_spooling() {
    let filename = "f".repeat(MAX_GUEST_PATH_BYTES + 1);
    let body = multipart_body(&[("file", Some(filename.as_str()), b"content")]);
    let error = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        format!("multipart filename exceeds {MAX_GUEST_PATH_BYTES} bytes")
    );
}

#[tokio::test]
async fn multipart_parts_are_classified_by_their_real_name_parameter() {
    // Multer reads `name` from inside these quoted filenames; the file part
    // must still be spooled and the `meta` part, which only mentions
    // `name=file` inside its filename, must not be.
    let mut body = Vec::new();
    for disposition in [
        r#"form-data; filename="a name=b.txt"; name="file""#,
        r#"form-data; filename="x;name=file;"; name="meta""#,
    ] {
        body.extend_from_slice(
            format!(
                "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: {disposition}\r\n\r\ncontent\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(format!("--{MULTIPART_BOUNDARY}--\r\n").as_bytes());

    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].filename.as_deref(), Some("a name=b.txt"));
}

#[test]
fn multipart_boundary_longer_than_rfc_2046_allows_is_rejected() {
    let boundary = "b".repeat(70);
    assert_eq!(
        multipart_boundary(&format!("multipart/form-data; boundary={boundary}")),
        Some(boundary.as_str())
    );
    assert_eq!(
        multipart_boundary(&format!("multipart/form-data; boundary={boundary}b")),
        None
    );
}

#[tokio::test]
async fn multipart_part_count_is_capped() {
    // Tiny parts cost the parser almost nothing each, so the part cap, not the
    // stream cap, is what stops them.
    let parts = vec![("file", Some("a.txt"), b"".as_slice()); E2B_MULTIPART_PART_LIMIT + 1];
    let error = spool_multipart_uploads(Body::from(multipart_body(&parts)), MULTIPART_BOUNDARY)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        error.message,
        "multipart uploads are limited to 1000 parts per request"
    );
}

#[tokio::test]
async fn multipart_framing_overhead_beyond_the_parse_buffer_is_accepted() {
    // Each ignored part carries ~4 KiB of headers, so the request's total
    // framing is several times the parse-buffer cap while no single part comes
    // close, and feeding must pause and resume across the body.
    let long_name = format!("note-{}", "x".repeat(4096));
    let mut parts = vec![(long_name.as_str(), None, b"".as_slice()); E2B_MULTIPART_PART_LIMIT - 1];
    parts.push(("file", Some("last.txt"), b"content".as_slice()));
    let body = multipart_body(&parts);
    let frames = body.chunks(64 * 1024).map(<[u8]>::to_vec).collect();

    let uploads = spool_multipart_uploads(chunked_body(frames), MULTIPART_BOUNDARY)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    let upload = uploads.into_iter().next().unwrap();
    assert_eq!(upload.filename.as_deref(), Some("last.txt"));
    let (_, data) = collect_spooled_write(upload.upload, "/last.txt").await;
    assert_eq!(data, b"content");
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
        let read_response = async {
            while let Ok(read) = reader.read(&mut buf).await {
                if read == 0 {
                    break;
                }
                response.extend_from_slice(&buf[..read]);
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), read_response)
            .await
            .expect("server must close the connection after the 413");
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
    let error = reject_oversized_content_length(
        &headers,
        E2B_FILE_CONTENT_LIMIT_BYTES,
        file_too_large_error,
    )
    .unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn octet_stream_upload_is_spooled_and_written_in_chunks() {
    let sandbox_id = SandboxId::new();
    let upload = spool_body_upload(chunked_body(vec![b"hello ".to_vec(), b"world".to_vec()]))
        .await
        .unwrap();
    let file = tokio::fs::File::open(upload.to_path_buf()).await.unwrap();

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
