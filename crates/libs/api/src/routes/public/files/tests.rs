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

/// A refusal the guest made before starting anything reaches the caller as a
/// retryable 429 or 503 with the guest's hint as `Retry-After` and its own
/// message, not tonic's rendering; a sandbox that does not answer in time is
/// a retryable 503, not a 500.
#[tokio::test]
async fn guest_refusals_and_timeouts_are_retryable_with_their_hint() {
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use pplx_space_public_api::{PublicErrorCode, PublicErrorEnvelope};

    async fn envelope(status: tonic::Status) -> (StatusCode, Option<String>, PublicErrorEnvelope) {
        let response = controller_error(ControllerError::from(status)).into_response();
        let code = response.status();
        let retry_after = response
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .map(|value| value.to_str().unwrap().to_owned());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (code, retry_after, serde_json::from_slice(&body).unwrap())
    }

    for (code, expected_status, expected_code) in [
        (
            tonic::Code::ResourceExhausted,
            StatusCode::TOO_MANY_REQUESTS,
            PublicErrorCode::RateLimited,
        ),
        (
            tonic::Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            PublicErrorCode::ServiceUnavailable,
        ),
    ] {
        let refused = pplx_space_common_grpc::refused_before_start(
            tonic::Status::new(code, "too many concurrent filesystem file operations"),
            std::time::Duration::from_millis(1500),
        );
        let (status, retry_after, envelope) = envelope(refused).await;
        assert_eq!(status, expected_status, "{code:?}");
        assert_eq!(retry_after.as_deref(), Some("2"), "{code:?}");
        assert_eq!(envelope.code, expected_code, "{code:?}");
        assert!(envelope.retryable, "{code:?}");
        assert_eq!(
            envelope.message,
            "too many concurrent filesystem file operations"
        );
    }

    let (status, _, timeout) = envelope(tonic::Status::deadline_exceeded(
        "request deadline exceeded",
    ))
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(timeout.code, PublicErrorCode::ServiceUnavailable);
    assert!(timeout.retryable);
}

/// mkdir, remove and file-change tracking require an `Idempotency-Key` that
/// the sandbox never sees, so a failure that may have applied the change is
/// not retryable there; a refusal made before anything started still is.
#[tokio::test]
async fn unprotected_mutations_do_not_invite_a_blind_retry() {
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use pplx_space_public_api::PublicErrorEnvelope;

    async fn retry(status: tonic::Status) -> (bool, bool) {
        let response = unprotected_mutation_error(ControllerError::from(status)).into_response();
        let has_retry_after = response
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
        (envelope.retryable, has_retry_after)
    }
    let refused = |code| {
        pplx_space_common_grpc::refused_before_start(
            tonic::Status::new(code, "synthetic refusal"),
            std::time::Duration::from_secs(1),
        )
    };

    // The change may have been applied: no retry. A retry hint without the
    // refusal marker is not evidence that nothing happened.
    for ambiguous in [
        tonic::Status::deadline_exceeded("request deadline exceeded"),
        tonic::Status::unavailable("transport closed"),
        tonic::Status::internal("guest failed"),
        pplx_space_common_grpc::with_retry_after(
            tonic::Status::unavailable("hinted but unmarked"),
            std::time::Duration::from_secs(1),
        ),
    ] {
        let code = ambiguous.code();
        assert_eq!(retry(ambiguous).await, (false, false), "{code:?}");
    }
    // This request did nothing: retry after the hint.
    assert_eq!(
        retry(refused(tonic::Code::ResourceExhausted)).await,
        (true, true)
    );
    assert_eq!(retry(refused(tonic::Code::Unavailable)).await, (true, true));
    // Mapped as before otherwise.
    assert_eq!(
        retry(tonic::Status::not_found("no such file")).await,
        (false, false)
    );
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

mod batch_body_limits {
    use super::*;
    use axum::{
        Router,
        body::Body,
        http::{Method, Request},
    };
    use pplx_space_common_core::sql;
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db,
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use uuid::Uuid;

    const KEY: &str = "native-file-write-limit-key";

    struct Fixture {
        _db: TestDb,
        app: Router,
        sandbox_id: PublicSandboxId,
    }

    /// A native key and a stopped sandbox it owns, so requests pass auth and
    /// owner scope and reach the handler's body extraction and validation.
    async fn fixture() -> TestDbResult<Fixture> {
        let db = pg_test_db().await?;
        sqlx::query(sql!(
            "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
             VALUES ($1, $2, 'native-file-write-limit', $3, ARRAY['native'])"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(crate::auth::hash_token(KEY))
        .execute(&db.pool)
        .await?;
        let sandbox_id = PublicSandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
             VALUES ($1, $2, $3, 'stopped', 1)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .execute(&db.pool)
        .await?;
        let state = AppState::from_pool(db.pool.clone()).await?;
        let app = Router::new()
            .nest("/v1", crate::routes::public::v1_router(state.clone()))
            .with_state(state);
        Ok(Fixture {
            _db: db,
            app,
            sandbox_id,
        })
    }

    /// Serialize `body`, then pad it with trailing JSON whitespace to exactly
    /// `len` bytes so a test can sit precisely on a body limit.
    fn padded_body(body: &Value, len: usize) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(body).expect("test body serializes");
        assert!(
            bytes.len() <= len,
            "body is already {} > {len} bytes",
            bytes.len()
        );
        bytes.resize(len, b' ');
        bytes
    }

    async fn post(
        fixture: &Fixture,
        action: &str,
        body: Vec<u8>,
    ) -> TestDbResult<(StatusCode, Value)> {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/v1/sandboxes/{}/files:{action}",
                fixture.sandbox_id
            ))
            .header("authorization", format!("Bearer {KEY}"))
            .header("content-type", "application/json")
            .header("idempotency-key", "file-write-limit")
            .body(Body::from(body))?;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            fixture.app.clone().oneshot(request),
        )
        .await??;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
        Ok((status, serde_json::from_slice(&body)?))
    }

    fn batch_body(decoded_lens: &[usize]) -> Value {
        let entries = decoded_lens
            .iter()
            .enumerate()
            .map(|(index, len)| {
                json!({
                    "path": format!("/workspace/f{index}"),
                    "data_base64": BASE64_STANDARD.encode(vec![0u8; *len]),
                })
            })
            .collect::<Vec<_>>();
        json!({ "entries": entries })
    }

    fn assert_payload_too_large(status: StatusCode, body: &Value) {
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert_eq!(body["code"], "invalid_request", "{body}");
        assert_eq!(body["retryable"], false, "{body}");
    }

    fn assert_accepted_past_size_checks(status: StatusCode, body: &Value) {
        eprintln!("UNDER-LIMIT status={status} body={body}");
        assert_ne!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(
            !body["message"]
                .as_str()
                .unwrap_or_default()
                .contains("too large"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn file_write_batch_limits_bound_the_total_payload() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let path_bytes = "/workspace/f0".len() + "/workspace/f1".len();
        let half = (MAX_FILE_WRITE_BATCH_BYTES - path_bytes) / 2;
        let at_cap = [half, MAX_FILE_WRITE_BATCH_BYTES - path_bytes - half];

        let (status, body) = post(
            &fixture,
            "write_batch",
            padded_body(&batch_body(&at_cap), WRITE_FILE_BATCH_BODY_LIMIT_BYTES),
        )
        .await?;
        assert_accepted_past_size_checks(status, &body);

        let (status, body) = post(
            &fixture,
            "write_batch",
            padded_body(&batch_body(&at_cap), WRITE_FILE_BATCH_BODY_LIMIT_BYTES + 1),
        )
        .await?;
        assert_payload_too_large(status, &body);

        let over_cap = [at_cap[0], at_cap[1] + 1];
        let (status, body) = post(
            &fixture,
            "write_batch",
            serde_json::to_vec(&batch_body(&over_cap))?,
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or_default()
                .starts_with("file write batch is too large"),
            "{body}"
        );
        Ok(())
    }

    /// The mounted `files:write` route carries its own limit: a body past the
    /// framework's 2 MiB default reaches the handler, and one byte over the
    /// route's limit gets 413.
    #[tokio::test]
    async fn file_write_route_applies_its_body_limit() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let write = |decoded_len: usize| {
            json!({
                "path": "/workspace/f",
                "data_base64": BASE64_STANDARD.encode(vec![0u8; decoded_len]),
                "append": false,
            })
        };

        let (status, body) = post(
            &fixture,
            "write",
            padded_body(&write(3 * 1024 * 1024), WRITE_FILE_BODY_LIMIT_BYTES),
        )
        .await?;
        assert_accepted_past_size_checks(status, &body);

        let (status, body) = post(
            &fixture,
            "write",
            padded_body(&write(3 * 1024 * 1024), WRITE_FILE_BODY_LIMIT_BYTES + 1),
        )
        .await?;
        assert_payload_too_large(status, &body);
        Ok(())
    }

    #[tokio::test]
    async fn other_file_routes_keep_the_default_body_limit() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let body = padded_body(&json!({ "path": "/workspace" }), 2 * 1024 * 1024 + 1);
        let (status, body) = post(&fixture, "stat", body).await?;
        assert_payload_too_large(status, &body);
        Ok(())
    }
}

/// Only the guest's own missing-path answer carries the `resource: path`
/// detail; a not-found from a hop that could not find the sandbox stays bare.
/// The code, status, and `retryable` are the same either way.
#[tokio::test]
async fn only_a_guest_missing_path_carries_the_path_detail() {
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use pplx_space_public_api::{PublicErrorCode, PublicErrorDetail, PublicErrorEnvelope};

    async fn envelope(error: PublicApiError) -> (StatusCode, PublicErrorEnvelope) {
        let response = error.into_response();
        let code = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (code, serde_json::from_slice(&body).unwrap())
    }
    let missing_path = || {
        pplx_space_common_grpc::mark_guest_path_not_found(tonic::Status::not_found(
            "No such file or directory (os error 2)",
        ))
    };

    for public in [
        controller_error(ControllerError::from(missing_path())),
        unprotected_mutation_error(ControllerError::from(missing_path())),
    ] {
        let (status, path) = envelope(public).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(path.code, PublicErrorCode::NotFound);
        assert!(!path.retryable);
        assert_eq!(
            path.details,
            vec![PublicErrorDetail {
                key: "resource".to_string(),
                value: "path".to_string(),
            }]
        );
    }

    for bare in [
        ControllerError::from(tonic::Status::not_found("sandbox not found")),
        ControllerError::new(StatusCode::NOT_FOUND, "sandbox not found"),
    ] {
        let (status, envelope) = envelope(controller_error(bare)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(envelope.code, PublicErrorCode::NotFound);
        assert!(!envelope.retryable);
        assert!(envelope.details.is_empty(), "{:?}", envelope.details);
    }
}
