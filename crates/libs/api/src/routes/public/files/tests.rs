use pplx_space_common_core::types::SandboxId;

use super::*;

#[test]
fn public_file_info_maps_native_shape() {
    for value in [0, 42, -1] {
        assert_eq!(public_file_type(value), FileType::Other);
        assert_eq!(public_file_event_kind(value), FileEventKind::Unknown);
    }
    let info = SpacedFileInfo {
        path: "/workspace/main.rs".to_string(),
        name: None,
        file_type: SpacedFileType::File as i32,
        size_bytes: 42,
        mode: 0o644,
        modified_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        owner_uid: 1000,
        owner_gid: 1000,
        symlink_target: None,
        link_count: Some(2),
    };

    let public = public_file_info(info);
    assert_eq!(public.path, "/workspace/main.rs");
    assert_eq!(public.name, "main.rs");
    assert_eq!(public.file_type, FileType::File);
    assert_eq!(public.size_bytes, 42);
    assert_eq!(public.owner_uid, 1000);
    // Carried rather than defaulted: a client that refuses hard-linked
    // files needs the real count, and an assumed 1 would pass that check.
    assert_eq!(public.link_count, Some(2));
    assert!(public.modified_at.is_some());
}

#[test]
fn guest_path_and_user_are_validated_at_boundary() {
    assert_eq!(
        parse_guest_path("/workspace".to_string()).unwrap(),
        "/workspace"
    );
    assert_eq!(
        parse_guest_path("workspace".to_string()).unwrap(),
        "workspace"
    );
    parse_guest_path("bad\npath".to_string()).unwrap_err();
    parse_guest_path("C:\\Users\\user\\file.txt".to_string()).unwrap_err();
    parse_guest_user(Some(String::new())).unwrap_err();
}

#[test]
fn write_payload_decodes_and_enforces_size() {
    let encoded = BASE64_STANDARD.encode("hello");
    assert_eq!(decode_write_payload(&encoded).unwrap(), b"hello");
    decode_write_payload("not base64").unwrap_err();

    let too_large = BASE64_STANDARD.encode(vec![0u8; MAX_FILE_WRITE_BYTES + 1]);
    decode_write_payload(&too_large).unwrap_err();
}

/// Chunked decoding yields exactly what decoding the whole payload would, in
/// message-sized pieces, and rejects what whole decoding rejects.
#[test]
fn write_chunks_decode_like_the_whole_payload() {
    for len in [
        0,
        1,
        FILE_WRITE_CHUNK_BYTES - 1,
        FILE_WRITE_CHUNK_BYTES,
        FILE_WRITE_CHUNK_BYTES + 1,
        3 * FILE_WRITE_CHUNK_BYTES + 2,
        MAX_FILE_WRITE_BYTES,
    ] {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let chunks = decode_write_chunks(&BASE64_STANDARD.encode(&data)).unwrap();
        assert_eq!(chunks.len(), len.div_ceil(FILE_WRITE_CHUNK_BYTES), "{len}");
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= FILE_WRITE_CHUNK_BYTES)
        );
        assert_eq!(chunks.concat(), data, "{len}");
    }

    // Padding in the middle of a payload, at a segment boundary.
    let padded = format!(
        "{}{}",
        BASE64_STANDARD.encode(vec![1u8; FILE_WRITE_CHUNK_BYTES - 1]),
        BASE64_STANDARD.encode(b"ab")
    );
    BASE64_STANDARD.decode(&padded).unwrap_err();
    decode_write_chunks(&padded).unwrap_err();
    decode_write_chunks("not base64").unwrap_err();
    decode_write_chunks(&BASE64_STANDARD.encode(vec![0u8; MAX_FILE_WRITE_BYTES + 1])).unwrap_err();
}

#[test]
fn read_length_is_bounded() {
    validate_read_length(1).unwrap();
    validate_read_length(MAX_FILE_READ_BYTES).unwrap();
    validate_read_length(0).unwrap_err();
    validate_read_length(MAX_FILE_READ_BYTES + 1).unwrap_err();
}

#[test]
fn spaced_write_requests_include_idempotency_on_open() {
    let sandbox_id = SandboxId::new();
    let key = IdempotencyKey::parse("native-file-write").unwrap();
    let requests = spaced_write_requests(
        sandbox_id,
        "/workspace/main.rs".to_string(),
        false,
        key,
        vec![b"abc".to_vec()],
        None,
    );

    assert_eq!(requests.len(), 2);
    let open = requests[0].operation.as_ref().unwrap();
    let Some(spaced_write_request::Message::Open(open)) = open.message.as_ref() else {
        panic!("first write request should open the file");
    };
    assert_eq!(open.path, "/workspace/main.rs");
    assert_eq!(open.idempotency_key.as_deref(), Some("native-file-write"));
}

#[test]
fn modified_filters_are_mutually_exclusive() {
    validate_modified_filters(&FileModifiedRequest {
        window_seconds: Some(60),
        since: None,
        cursor: None,
        user: None,
    })
    .unwrap();
    validate_modified_filters(&FileModifiedRequest {
        window_seconds: Some(60),
        since: Some("2026-06-20T00:00:00Z".to_string()),
        cursor: None,
        user: None,
    })
    .unwrap_err();
}

/// A full sandbox disk reaches clients as a non-retryable precondition
/// failure with a readable message, not as a retryable rate limit: the guest
/// reports ENOSPC with the same gRPC code its admission limits use. Real
/// admission rejections, and host-side failures that mention running out of
/// space, keep their mapping.
#[tokio::test]
async fn a_full_sandbox_disk_is_not_a_retryable_rate_limit() {
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use pplx_space_public_api::{PublicErrorCode, PublicErrorEnvelope};

    async fn envelope(status: tonic::Status) -> (StatusCode, PublicErrorEnvelope) {
        let response = controller_error(ControllerError::from(status)).into_response();
        let code = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (code, serde_json::from_slice(&body).unwrap())
    }

    let mut storage_full =
        tonic::Status::resource_exhausted("No space left on device (os error 28)");
    storage_full.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::GUEST_STORAGE_FULL,
        ),
    );
    let (status, full) = envelope(storage_full).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(full.code, PublicErrorCode::FailedPrecondition);
    assert!(!full.retryable);
    assert_eq!(
        full.message,
        "sandbox storage is full: No space left on device (os error 28)"
    );

    for other in [
        "payload request capacity exhausted",
        "No space left on device (os error 28)",
        "btrfs subvolume snapshot failed: /snapshots/a: no space left on device",
    ] {
        let (status, envelope) = envelope(tonic::Status::resource_exhausted(other)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{other}");
        assert_eq!(envelope.code, PublicErrorCode::RateLimited, "{other}");
    }
}

/// `files:write` accepts a body carrying its largest documented payload,
/// which the framework's 2 MiB default rejected with 413 above about 1.5 MiB.
#[tokio::test]
async fn the_write_body_limit_admits_the_largest_documented_payload() {
    use axum::{Router, body::Body, extract::DefaultBodyLimit, http::Request, routing::post};
    use tower::ServiceExt;

    async fn accept(Json(request): Json<FileWriteRequest>) -> String {
        request.data_base64.len().to_string()
    }
    let body = |raw_bytes: usize| {
        serde_json::to_vec(&serde_json::json!({
            "path": format!("/workspace/{}", "p".repeat(4000)),
            "data_base64": BASE64_STANDARD.encode(vec![7u8; raw_bytes]),
            "append": false,
        }))
        .unwrap()
    };
    let send = |limit: Option<usize>, body: Vec<u8>| async move {
        let route = post(accept);
        let route = match limit {
            Some(limit) => route.layer(DefaultBodyLimit::max(limit)),
            None => route,
        };
        Router::new()
            .route("/write", route)
            .oneshot(
                Request::post("/write")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    };

    assert_eq!(
        send(None, body(3 * 1024 * 1024)).await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "the framework default is what rejected larger writes"
    );
    let limit = Some(WRITE_FILE_BODY_LIMIT_BYTES);
    assert_eq!(
        send(limit, body(MAX_FILE_WRITE_BYTES)).await,
        StatusCode::OK
    );
    assert_eq!(
        send(limit, body(MAX_FILE_WRITE_BYTES + 1024 * 1024)).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    // An encoder that escapes every `/` as `\/` still fits for data whose
    // base64 is roughly uniform; the limit documents the worst case it does not
    // cover.
    let varied: Vec<u8> = (0..MAX_FILE_WRITE_BYTES)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    let encoded = BASE64_STANDARD.encode(varied).replace('/', "\\/");
    assert!(encoded.len() > MAX_FILE_WRITE_BYTES.div_ceil(3) * 4);
    let escaped = format!(
        r#"{{"path":"/workspace/{}","data_base64":"{encoded}","append":false}}"#,
        "p".repeat(4000)
    );
    assert_eq!(send(limit, escaped.into_bytes()).await, StatusCode::OK);
}
