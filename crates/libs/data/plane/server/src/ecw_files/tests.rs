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

async fn collect_spooled_write(upload: StagedUpload, path: &str) -> (SpacedWriteOpen, Vec<u8>) {
    let file = tokio::fs::File::open(upload.path).await.unwrap();
    let (read_failed, _read_failure) = tokio::sync::oneshot::channel();
    let requests =
        spooled_write_requests(SandboxId::new(), path.to_string(), file, None, read_failed)
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
    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(
        chunked_body(vec![body[..split].to_vec(), body[split..].to_vec()]),
        MULTIPART_BOUNDARY,
        true,
        &mut spool,
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
    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(
        chunked_body(body.chunks(17).map(<[u8]>::to_vec).collect()),
        MULTIPART_BOUNDARY,
        true,
        &mut spool,
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
    let mut spool = SpoolFiles::default();
    let uploads =
        spool_multipart_uploads(Body::from(single), MULTIPART_BOUNDARY, false, &mut spool)
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
    let mut spool = SpoolFiles::default();
    let error =
        spool_multipart_uploads(Body::from(multiple), MULTIPART_BOUNDARY, false, &mut spool)
            .await
            .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "path query parameter only supports a single multipart upload"
    );
}

#[tokio::test]
async fn multipart_requires_filename_or_path() {
    let body = multipart_body(&[("file", None, b"content")]);
    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "multipart uploads require a filename or path query"
    );
}

#[tokio::test]
async fn malformed_multipart_and_missing_boundary_are_rejected() {
    assert_eq!(multipart_boundary("multipart/form-data"), None);
    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads(
        Body::from(multipart_body(&[("file", Some("a.txt"), b"content")])),
        "different-boundary",
        true,
        &mut spool,
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.message, "Invalid multipart body");

    let missing_disposition = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Type: application/octet-stream\r\n\r\ncontent\r\n--{MULTIPART_BOUNDARY}--\r\n"
    );
    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads(
        Body::from(missing_disposition),
        MULTIPART_BOUNDARY,
        true,
        &mut spool,
    )
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

    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads_with_limit(
        Body::from_stream(stream),
        MULTIPART_BOUNDARY,
        8,
        true,
        &mut spool,
    )
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
    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads_with_limit(
        Body::from(body),
        MULTIPART_BOUNDARY,
        limit,
        true,
        &mut spool,
    )
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

    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads(
        Body::from_stream(stream),
        MULTIPART_BOUNDARY,
        true,
        &mut spool,
    )
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
    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(
        Body::from(format!("{shortest}--{MULTIPART_BOUNDARY}--\r\n")),
        MULTIPART_BOUNDARY,
        true,
        &mut spool,
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
    let fed_total = Arc::new(AtomicUsize::new(0));
    let mut body = Box::pin(chunked_body(frames).into_data_stream());
    let mut stream = Box::pin(throttled_multipart_body(
        &mut body,
        Arc::clone(&consumed),
        Arc::clone(&fed_total),
    ));
    let mut fed = 0;
    while let Some(Some(frame)) = stream.next().now_or_never() {
        fed += frame.unwrap().len();
    }
    assert_eq!(fed_total.load(Ordering::Relaxed), fed);
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
    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
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
        ..ContentDispositionE2b::default()
    };
    let cases: [(&[u8], ContentDispositionE2b); 14] = [
        // Under one quoted-pair rule, `name=file` inside a filename stays in
        // the filename in every parameter order.
        (
            br#"form-data; filename="a\"; name=file; x=\"rest"; name="meta""#,
            parsed(Some("meta"), Some(r#"a"; name=file; x="rest"#)),
        ),
        (
            br#"form-data; name="meta"; filename="a\"; name=file; x=\"rest""#,
            parsed(Some("meta"), Some(r#"a"; name=file; x="rest"#)),
        ),
        (
            br#"form-data; filename="a\"; name=\"file\"; x=y"; name=meta"#,
            parsed(Some("meta"), Some(r#"a"; name="file"; x=y"#)),
        ),
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
            br#"form-data; filename="a.txt""#,
            parsed(None, Some("a.txt")),
        ),
        // httpx doubles backslashes; Go also writes `\"`.
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
        let got = parse(value).unwrap();
        assert_eq!(
            (got.name, got.filename),
            (expected.name, expected.filename),
            "{shown}"
        );
    }
    // Only an escaped backslash in the filename is flagged: it reads as one
    // backslash from httpx or Go but two from raw-backslash clients.
    for (value, flagged) in [
        (&br#"form-data; name="file"; filename="dir\\f""#[..], true),
        (br#"form-data; name="file"; filename="a\\\"b""#, true),
        (br#"form-data; name="a\\b"; filename="f.txt""#, false),
        (
            br#"form-data; name="file"; filename="dir\report.txt""#,
            false,
        ),
        (br#"form-data; name="file"; filename="a\"b""#, false),
    ] {
        let shown = String::from_utf8_lossy(value).into_owned();
        assert_eq!(
            parse(value).unwrap().filename_has_escaped_backslash,
            flagged,
            "{shown}"
        );
    }
    for malformed in [
        &br#"form-data; name="file"; filename="a.txt"#[..],
        br#"form-data; name="file"; filename="a.txt"x"#,
        br#"form-data; name="file"; filename=a.txt""#,
        br#"form-data; filename=a"b; name="file""#,
        b"form-data; name=\"file\"; filename=\"\xff.txt\"",
        // A raw filename ending in a backslash leaves its quote escaped.
        br#"form-data; name="file"; filename="dir\""#,
        br#"form-data; filename="dir\" ; name="file""#,
        // A repeated parameter is ambiguous about which one applies.
        br#"form-data; name=file; filename="a.txt"; filename="b.txt""#,
        br#"form-data; name="meta"; filename="a.txt"; name="file""#,
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
    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        format!("multipart filename exceeds {MAX_GUEST_PATH_BYTES} bytes")
    );
}

#[tokio::test]
async fn multipart_filename_is_ignored_when_a_path_query_names_the_file() {
    let filename = "f".repeat(MAX_GUEST_PATH_BYTES + 1);
    let body = multipart_body(&[("file", Some(filename.as_str()), b"content")]);
    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, false, &mut spool)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].filename, None);
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

    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
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
    let mut spool = SpoolFiles::default();
    let error = spool_multipart_uploads(
        Body::from(multipart_body(&parts)),
        MULTIPART_BOUNDARY,
        true,
        &mut spool,
    )
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

    let mut spool = SpoolFiles::default();
    let uploads =
        spool_multipart_uploads(chunked_body(frames), MULTIPART_BOUNDARY, true, &mut spool)
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

    let mut spool = SpoolFiles::default();
    let uploads =
        spool_multipart_uploads(chunked_body(frames), MULTIPART_BOUNDARY, true, &mut spool)
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
    let mut spool = SpoolFiles::default();
    let upload = spool_body_upload(
        chunked_body(vec![b"hello ".to_vec(), b"world".to_vec()]),
        &mut spool,
    )
    .await
    .unwrap();
    assert_eq!(upload.len, b"hello world".len());
    let file = tokio::fs::File::open(&upload.path).await.unwrap();

    let (read_failed, _read_failure) = tokio::sync::oneshot::channel();
    let requests = spooled_write_requests(
        sandbox_id,
        "/tmp/hello.txt".to_string(),
        file,
        Some(GuestUserName::parse("user").unwrap()),
        read_failed,
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

/// Yields `chunks`, then fails every later read.
struct FailingAfter {
    chunks: std::collections::VecDeque<Vec<u8>>,
}

impl tokio::io::AsyncRead for FailingAfter {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(match self.chunks.pop_front() {
            Some(chunk) => {
                buf.put_slice(&chunk);
                Ok(())
            }
            None => Err(std::io::Error::other("injected spool read failure")),
        })
    }
}

#[tokio::test]
async fn spool_read_failure_signals_the_transfer_instead_of_ending_the_stream() {
    use futures_util::FutureExt as _;

    let (read_failed, mut read_failure) = tokio::sync::oneshot::channel();
    let mut stream = Box::pin(spooled_write_requests(
        SandboxId::new(),
        "/workspace/a".to_string(),
        FailingAfter {
            chunks: [b"first chunk".to_vec()].into(),
        },
        None,
        read_failed,
    ));
    let open = stream.next().await.unwrap();
    assert!(matches!(
        open.operation.unwrap().message.unwrap(),
        spaced_write_request::Message::Open(_)
    ));
    let data = stream.next().await.unwrap();
    let spaced_write_request::Message::Data(data) = data.operation.unwrap().message.unwrap() else {
        panic!("second request must carry data");
    };
    assert_eq!(data, b"first chunk");

    // The failed read neither yields nor ends the stream, and tells the
    // transfer, which drops the call.
    assert!(stream.next().now_or_never().is_none());
    assert_eq!(read_failure.try_recv(), Ok(()));
}

#[test]
fn spooled_write_must_acknowledge_every_staged_byte() {
    check_spooled_write_len(11, 11).unwrap();
    let error = check_spooled_write_len(10, 11).unwrap_err();
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    check_spooled_write_len(12, 11).unwrap_err();
}

async fn spool_dir_entries(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

/// Spool removal runs on a blocking task after the guard drops.
async fn wait_for_empty_spool_dir(dir: &std::path::Path) {
    for _ in 0..500 {
        if spool_dir_entries(dir).await == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("spool files were not removed: {dir:?}");
}

#[tokio::test]
async fn staged_spool_files_are_removed_when_a_later_part_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut body = multipart_body(&[
        ("file", Some("a.txt"), b"a"),
        ("file", Some("b.txt"), b"b"),
        ("file", Some("c.txt"), b"c"),
    ]);
    // Replace the closing delimiter with a part whose header block never ends.
    body.truncate(body.len() - format!("--{MULTIPART_BOUNDARY}--\r\n").len());
    body.extend_from_slice(
        format!("--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"")
            .as_bytes(),
    );
    let mut spool = SpoolFiles::in_dir(dir.path().to_path_buf());
    spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
        .await
        .unwrap_err();
    assert_eq!(spool_dir_entries(dir.path()).await, 3);

    drop(spool);
    wait_for_empty_spool_dir(dir.path()).await;
}

#[tokio::test]
async fn staged_spool_files_are_removed_when_the_request_is_cancelled() {
    let dir = tempfile::tempdir().unwrap();
    let staged = multipart_body(&[("file", Some("a.txt"), b"a"), ("file", Some("b.txt"), b"b")]);
    // Both parts' headers arrive and the body stalls inside the second
    // part's data, so both spool files exist but the request never finishes.
    let prefix = staged[..staged.len() - format!("--{MULTIPART_BOUNDARY}--\r\n").len()].to_vec();
    let body =
        Body::from_stream(stream::iter([Ok::<_, Infallible>(prefix)]).chain(stream::pending()));
    let spool_dir = dir.path().to_path_buf();
    let request = tokio::spawn(async move {
        let mut spool = SpoolFiles::in_dir(spool_dir);
        spool_multipart_uploads(body, MULTIPART_BOUNDARY, true, &mut spool).await
    });
    for _ in 0..500 {
        if spool_dir_entries(dir.path()).await == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(spool_dir_entries(dir.path()).await, 2);

    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    wait_for_empty_spool_dir(dir.path()).await;
}

#[tokio::test]
async fn embedded_name_file_in_a_filename_does_not_make_a_part_a_file() {
    let body = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; filename=\"a\\\"; name=file; x=\\\"rest\"; name=\"meta\"\r\n\r\ncontent\r\n--{MULTIPART_BOUNDARY}--\r\n"
    );
    let mut spool = SpoolFiles::default();
    let uploads = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
        .await
        .unwrap();
    assert!(uploads.is_empty());
}

#[tokio::test]
async fn spool_read_failure_fails_a_write_that_has_not_finished() {
    let (read_failed, read_failure) = tokio::sync::oneshot::channel();
    read_failed.send(()).unwrap();
    // The call never finishes on its own; only the read failure ends it.
    let write =
        std::future::pending::<Result<tonic::Response<spaced_ops::WriteResponse>, tonic::Status>>();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fail_on_spool_read_failure(write, read_failure),
    )
    .await
    .expect("a spool read failure must end the write without waiting for the call");
    let Err(EcwRouteAttemptError::Proxy(error)) = outcome else {
        panic!("a spool read failure must fail the write");
    };
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn write_completes_when_the_spool_was_read_to_the_end() {
    let (read_failed, read_failure) = tokio::sync::oneshot::channel::<()>();
    // The stream finished, so its sender is gone.
    drop(read_failed);
    // Pending on its first poll, so the read-failure arm is always polled
    // while the call is still running, whichever order `select!` picks.
    let write = async {
        tokio::task::yield_now().await;
        Ok(tonic::Response::new(spaced_ops::WriteResponse {
            bytes_written: 11,
        }))
    };
    let response = fail_on_spool_read_failure(write, read_failure)
        .await
        .unwrap_or_else(|_| panic!("a complete read must not fail the write"));
    assert_eq!(response.bytes_written, 11);
}

#[tokio::test]
async fn invalid_multipart_filename_is_rejected_before_spooling() {
    let dir = tempfile::tempdir().unwrap();
    // Both pass multipart header parsing and fail only guest-path validation:
    // a tab is a control character httparse allows in header values.
    for (filename, message) in [
        (
            "C:\\Users\\a.txt",
            "windows-style guest paths are not supported for linux sandboxes",
        ),
        ("a\tb.txt", "guest path must be bounded and control-free"),
    ] {
        let body = multipart_body(&[("file", Some(filename), b"content")]);
        let mut spool = SpoolFiles::in_dir(dir.path().to_path_buf());
        let error = spool_multipart_uploads(Body::from(body), MULTIPART_BOUNDARY, true, &mut spool)
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST, "{filename:?}");
        assert_eq!(error.message, message, "{filename:?}");
        assert_eq!(spool_dir_entries(dir.path()).await, 0, "{filename:?}");
    }
}

#[tokio::test]
async fn filename_with_an_escaped_backslash_is_rejected_only_when_it_names_the_destination() {
    let header = r#"form-data; name="file"; filename="dir\\f.txt""#;
    let body = || {
        format!(
            "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: {header}\r\n\r\ncontent\r\n--{MULTIPART_BOUNDARY}--\r\n"
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let mut spool = SpoolFiles::in_dir(dir.path().to_path_buf());
    let error = spool_multipart_uploads(Body::from(body()), MULTIPART_BOUNDARY, true, &mut spool)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(
        error.message.contains("one backslash or two"),
        "{}",
        error.message
    );
    assert_eq!(spool_dir_entries(dir.path()).await, 0);

    // With a path query the filename is ignored, so the part is written.
    let mut spool = SpoolFiles::default();
    let uploads =
        spool_multipart_uploads(Body::from(body()), MULTIPART_BOUNDARY, false, &mut spool)
            .await
            .unwrap();
    assert_eq!(uploads.len(), 1);
}

/// Spools a complete multipart body, then sends `epilogue_len` more bytes
/// once the request has parked with nothing left to do.
///
/// Callers pause Tokio's clock. A paused current-thread runtime advances it
/// only when every task is parked and no blocking spool I/O is in flight, so
/// the sleep below returns after the parser has consumed the buffered body
/// through its closing delimiter. The request is then either finished or
/// waiting for body bytes the parser no longer reads.
async fn spool_with_late_epilogue(
    epilogue_len: usize,
    limit_over_multipart: usize,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    let multipart = multipart_body(&[("file", Some("a.txt"), b"content")]);
    let limit = multipart.len() + limit_over_multipart;
    let (epilogue, epilogue_received) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let frames = stream::once(async { multipart })
        .chain(stream::once(epilogue_received).filter_map(|epilogue| async { epilogue.ok() }))
        .map(Ok::<_, Infallible>);
    let request = tokio::spawn(async move {
        let mut spool = SpoolFiles::default();
        spool_multipart_uploads_with_limit(
            Body::from_stream(frames),
            MULTIPART_BOUNDARY,
            limit,
            true,
            &mut spool,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // A request that stopped reading at the closing delimiter has already
    // dropped the body, so this send can fail; its result is then `Ok`.
    if epilogue.send(vec![b'e'; epilogue_len]).is_err() {
        assert!(request.is_finished());
    }
    request.await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn multipart_epilogue_counts_toward_the_whole_body_limit() {
    let error = spool_with_late_epilogue(10, 9).await.unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test(start_paused = true)]
async fn multipart_epilogue_within_the_whole_body_limit_is_accepted() {
    let uploads = spool_with_late_epilogue(10, 10).await.unwrap();
    assert_eq!(uploads.len(), 1);
}
