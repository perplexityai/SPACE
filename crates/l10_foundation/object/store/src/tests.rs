use super::*;
use pplx_space_metrics::object_store::S3AttemptFailureKind;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

fn _assert_object() {
    fn assert<T: ObjectStore>() {}
    assert::<S3ObjectStore>();
}

/// Builds an env reader that responds with `value` for `expected_name`
/// and `None` for everything else. Lets us exercise the resolver
/// without mutating the process-wide environment, which would race
/// against other parallel tests.
fn fake_env(
    expected_name: &'static str,
    value: Option<&'static str>,
) -> impl FnOnce(&str) -> Option<String> {
    move |name: &str| {
        assert_eq!(name, expected_name, "unexpected env var lookup");
        value.map(str::to_string)
    }
}

fn unset_env() -> impl FnOnce(&str) -> Option<String> {
    |_name: &str| None
}

#[test]
fn base_config_only_calculates_request_checksums_when_required() {
    let config = s3_base_config(
        S3Timeouts::resolve(|_| None),
        DEFAULT_POOL_IDLE_TIMEOUT,
        AttemptObserver::logging(),
    )
    .build();

    assert_eq!(
        config.request_checksum_calculation(),
        Some(&RequestChecksumCalculation::WhenRequired),
    );
}

#[test]
fn s3_operation_metric_labels_are_stable() {
    let cases = [
        (S3Operation::AbortMultipartUpload, "AbortMultipartUpload"),
        (
            S3Operation::CompleteMultipartUpload,
            "CompleteMultipartUpload",
        ),
        (S3Operation::CreateMultipartUpload, "CreateMultipartUpload"),
        (S3Operation::GetObject, "GetObject"),
        (S3Operation::HeadObject, "HeadObject"),
        (S3Operation::PutObject, "PutObject"),
        (S3Operation::UploadPart, "UploadPart"),
    ];

    for (operation, expected) in cases {
        assert_eq!(operation.as_str(), expected);
        assert_eq!(operation.to_string(), expected);
    }
}

#[test]
fn s3_rate_limit_classifier_covers_aws_codes_and_statuses() {
    for code in [
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "RequestThrottled",
        "RequestThrottledException",
        "RequestLimitExceeded",
        "TooManyRequests",
        "TooManyRequestsException",
        "TooManyRequestsError",
    ] {
        assert!(
            is_s3_rate_limited(Some(code), None),
            "{code} should be classified as rate limited"
        );
    }

    assert!(is_s3_rate_limited(None, Some(429)));
    assert!(is_s3_rate_limited(None, Some(503)));
    assert!(!is_s3_rate_limited(Some("AccessDenied"), Some(403)));
    assert!(!is_s3_rate_limited(None, Some(500)));
}

fn assert_contiguous_cover(ranges: &[MultipartPartRange], size: u64) {
    let mut offset = 0;
    for (idx, range) in ranges.iter().enumerate() {
        assert_eq!(range.part_number, (idx + 1) as i32);
        assert_eq!(range.offset, offset);
        assert!(range.length > 0, "range {idx} is empty");
        offset += range.length;
    }
    assert_eq!(offset, size);
}

fn assert_s3_safe_download_ranges(
    ranges: &[MultipartPartRange],
    size: u64,
    configured_part_size: u64,
) {
    assert_contiguous_cover(ranges, size);
    for range in ranges {
        let end = range.offset + range.length - 1;
        assert!(
            end < size,
            "range {} extends past object size {size}",
            byte_range_header(*range)
        );
        assert_eq!(
            byte_range_header(*range),
            format!("bytes={}-{}", range.offset, end)
        );
    }
    if ranges.len() > 1 {
        assert!(
            ranges.iter().all(|range| {
                (MULTIPART_PART_SIZE_MIN as u64..=configured_part_size).contains(&range.length)
            }),
            "multi-range transfer emitted a part outside the S3-safe size bounds: {ranges:?}"
        );
        let min_len = ranges.iter().map(|range| range.length).min().unwrap();
        let max_len = ranges.iter().map(|range| range.length).max().unwrap();
        assert!(
            max_len - min_len <= 1,
            "range planner should spread tiny tails instead of leaving a small final range: {ranges:?}"
        );
    }
}

fn test_file_path(name: &str) -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "pplx-space-object-store-{name}-{}-{unique}",
        std::process::id()
    ))
}

#[test]
fn part_size_uses_const_default_when_unset() {
    assert_eq!(
        resolve_multipart_part_size(unset_env()),
        MULTIPART_PART_SIZE
    );
}

#[test]
fn ranged_download_uses_single_get_when_object_fits_one_chunk() {
    for size in [1, 1024, DEFAULT_DOWNLOAD_CHUNK_SIZE as u64] {
        assert_eq!(
            single_writer_download_ranges(
                size,
                DEFAULT_DOWNLOAD_CHUNK_SIZE,
                DEFAULT_DOWNLOAD_READER_CONCURRENCY
            ),
            None,
            "size {size} should keep the already-open streaming GET"
        );
    }
}

#[test]
fn ranged_download_is_disabled_when_reader_concurrency_is_one() {
    let size = 4 * DEFAULT_DOWNLOAD_CHUNK_SIZE as u64;

    assert_eq!(
        single_writer_download_ranges(size, DEFAULT_DOWNLOAD_CHUNK_SIZE, 1),
        None,
        "one reader should keep the already-open streaming GET"
    );

    let ranges = single_writer_download_ranges(size, DEFAULT_DOWNLOAD_CHUNK_SIZE, 2)
        .expect("multiple readers should allow ranged fanout");
    assert_eq!(ranges.len(), 4);
}

#[test]
fn download_chunks_are_fixed_size_with_short_tail() {
    let chunk = DEFAULT_DOWNLOAD_CHUNK_SIZE as u64;
    let size = 3 * chunk + 5;
    let ranges = download_chunk_ranges(size, DEFAULT_DOWNLOAD_CHUNK_SIZE);

    assert_eq!(ranges.len(), 4);
    assert_contiguous_cover(&ranges, size);
    for range in &ranges[..3] {
        assert_eq!(
            range.length, chunk,
            "non-final chunks are exactly chunk_size"
        );
    }
    assert_eq!(ranges[3].length, 5, "tail keeps its natural short length");
    for range in &ranges {
        let end = range.offset + range.length - 1;
        assert_eq!(
            byte_range_header(*range),
            format!("bytes={}-{}", range.offset, end)
        );
        assert!(end < size);
    }
}

#[test]
fn download_reader_concurrency_default_and_bounds() {
    assert_eq!(
        resolve_download_reader_concurrency(unset_env()),
        DEFAULT_DOWNLOAD_READER_CONCURRENCY,
    );
    assert_eq!(
        resolve_download_reader_concurrency(fake_env(DOWNLOAD_READER_CONCURRENCY_ENV, Some("16"))),
        16,
    );
    assert_eq!(
        resolve_download_reader_concurrency(fake_env(DOWNLOAD_READER_CONCURRENCY_ENV, Some("0"))),
        DEFAULT_DOWNLOAD_READER_CONCURRENCY,
    );
}

#[test]
fn pool_idle_timeout_overrides_and_falls_back_out_of_range() {
    assert_eq!(
        resolve_pool_idle_timeout(unset_env()),
        DEFAULT_POOL_IDLE_TIMEOUT
    );
    assert_eq!(
        resolve_pool_idle_timeout(fake_env(POOL_IDLE_TIMEOUT_ENV, Some("5"))),
        Duration::from_secs(5),
    );
    let below = (POOL_IDLE_TIMEOUT_SECS_MIN - 1).to_string();
    let above = (POOL_IDLE_TIMEOUT_SECS_MAX + 1).to_string();
    for value in [below.as_str(), above.as_str(), "3.5", "soon"] {
        assert_eq!(
            resolve_pool_idle_timeout(|_: &str| Some(value.to_string())),
            DEFAULT_POOL_IDLE_TIMEOUT,
            "{value}"
        );
    }
}

#[test]
fn endpoint_span_label_strips_sensitive_url_components() {
    assert_eq!(endpoint_span_label(None), "aws-default");
    assert_eq!(
        endpoint_span_label(Some("http://minio.internal:9000")),
        "minio.internal:9000"
    );
    assert_eq!(
        endpoint_span_label(Some("https://s3.us-east-1.amazonaws.com")),
        "s3.us-east-1.amazonaws.com"
    );
    // Userinfo must never reach telemetry.
    assert_eq!(
        endpoint_span_label(Some("http://user:secret@minio.internal:9000")),
        "minio.internal:9000"
    );
    assert_eq!(endpoint_span_label(Some("not a uri")), "invalid-endpoint");
}

#[test]
fn download_buffer_budget_default_and_bounds() {
    assert_eq!(
        resolve_download_buffer_budget(unset_env()),
        DEFAULT_DOWNLOAD_BUFFER_BUDGET,
    );
    // 1 GiB: valid.
    assert_eq!(
        resolve_download_buffer_budget(fake_env(DOWNLOAD_BUFFER_BUDGET_ENV, Some("1073741824"))),
        1024 * 1024 * 1024,
    );
    // Below the floor.
    assert_eq!(
        resolve_download_buffer_budget(fake_env(DOWNLOAD_BUFFER_BUDGET_ENV, Some("1048576"))),
        DEFAULT_DOWNLOAD_BUFFER_BUDGET,
    );
}

#[test]
fn download_buffer_permit_math_floors_at_one() {
    assert_eq!(
        download_buffer_chunk_permits(DEFAULT_DOWNLOAD_BUFFER_BUDGET, DEFAULT_DOWNLOAD_CHUNK_SIZE),
        64,
    );
    // A budget below one chunk still yields one permit so a lone
    // download always makes progress.
    assert_eq!(
        download_buffer_chunk_permits(DOWNLOAD_BUFFER_BUDGET_MIN, DOWNLOAD_CHUNK_SIZE_MAX),
        1,
    );
}

#[test]
fn download_chunk_size_default_and_bounds() {
    assert_eq!(
        resolve_download_chunk_size(unset_env()),
        DEFAULT_DOWNLOAD_CHUNK_SIZE,
    );
    // 64 MiB: valid and aligned.
    assert_eq!(
        resolve_download_chunk_size(fake_env(DOWNLOAD_CHUNK_SIZE_ENV, Some("67108864"))),
        64 * 1024 * 1024,
    );
    // Any in-range byte count is accepted; no alignment constraint.
    assert_eq!(
        resolve_download_chunk_size(fake_env(DOWNLOAD_CHUNK_SIZE_ENV, Some("33554433"))),
        33554433,
    );
    // Below the floor.
    assert_eq!(
        resolve_download_chunk_size(fake_env(DOWNLOAD_CHUNK_SIZE_ENV, Some("4096"))),
        DEFAULT_DOWNLOAD_CHUNK_SIZE,
    );
}

#[test]
fn multipart_ranges_balance_sub_minimum_tail_across_parts() {
    let part_size = MULTIPART_PART_SIZE as u64;
    let tiny_tail = 126_976;
    let size = 28 * part_size + tiny_tail;
    let ranges = multipart_part_ranges(size, MULTIPART_PART_SIZE);

    assert_eq!(ranges.len(), 29);
    assert_contiguous_cover(&ranges, size);
    assert!(
        ranges
            .iter()
            .all(|range| { (MULTIPART_PART_SIZE_MIN as u64..=part_size).contains(&range.length) })
    );
    assert_s3_safe_download_ranges(&ranges, size, part_size);
    assert!(ranges[0].length - ranges.last().unwrap().length <= 1);
}

#[test]
fn multipart_ranges_fall_back_to_single_put_when_no_valid_split_exists() {
    let part_size = 6 * 1024 * 1024;
    let size = part_size as u64 + 1024 * 1024;
    let ranges = multipart_part_ranges(size, part_size);

    assert_eq!(
        ranges,
        vec![MultipartPartRange {
            part_number: 1,
            offset: 0,
            length: size,
        }]
    );
}

#[test]
fn part_size_overrides_to_valid_value() {
    let raw = (64 * 1024 * 1024).to_string();
    let leaked: &'static str = Box::leak(raw.into_boxed_str());
    let value = resolve_multipart_part_size(fake_env(MULTIPART_PART_SIZE_ENV, Some(leaked)));
    assert_eq!(value, 64 * 1024 * 1024);
}

#[test]
fn part_size_falls_back_on_parse_failure() {
    let value =
        resolve_multipart_part_size(fake_env(MULTIPART_PART_SIZE_ENV, Some("not-a-number")));
    assert_eq!(value, MULTIPART_PART_SIZE);
}

#[test]
fn part_size_falls_back_when_below_min() {
    // 4 MiB is below S3's 5 MiB minimum.
    let value = resolve_multipart_part_size(fake_env(MULTIPART_PART_SIZE_ENV, Some("4194304")));
    assert_eq!(value, MULTIPART_PART_SIZE);
}

#[test]
fn part_size_falls_back_when_above_max() {
    // 6 GiB is above S3's 5 GiB maximum.
    let value = resolve_multipart_part_size(fake_env(MULTIPART_PART_SIZE_ENV, Some("6442450944")));
    assert_eq!(value, MULTIPART_PART_SIZE);
}

#[test]
fn part_size_accepts_min_and_max_inclusive() {
    let min = MULTIPART_PART_SIZE_MIN.to_string();
    let min_leaked: &'static str = Box::leak(min.into_boxed_str());
    assert_eq!(
        resolve_multipart_part_size(fake_env(MULTIPART_PART_SIZE_ENV, Some(min_leaked))),
        MULTIPART_PART_SIZE_MIN,
    );
    let max = MULTIPART_PART_SIZE_MAX.to_string();
    let max_leaked: &'static str = Box::leak(max.into_boxed_str());
    assert_eq!(
        resolve_multipart_part_size(fake_env(MULTIPART_PART_SIZE_ENV, Some(max_leaked))),
        MULTIPART_PART_SIZE_MAX,
    );
}

#[test]
fn concurrency_uses_const_default_when_unset() {
    assert_eq!(
        resolve_multipart_concurrency(unset_env()),
        MULTIPART_CONCURRENCY,
    );
}

#[test]
fn concurrency_overrides_to_valid_value() {
    let value = resolve_multipart_concurrency(fake_env(MULTIPART_CONCURRENCY_ENV, Some("16")));
    assert_eq!(value, 16);
}

#[test]
fn concurrency_falls_back_on_parse_failure() {
    let value = resolve_multipart_concurrency(fake_env(MULTIPART_CONCURRENCY_ENV, Some("sixteen")));
    assert_eq!(value, MULTIPART_CONCURRENCY);
}

#[test]
fn concurrency_falls_back_when_zero() {
    let value = resolve_multipart_concurrency(fake_env(MULTIPART_CONCURRENCY_ENV, Some("0")));
    assert_eq!(value, MULTIPART_CONCURRENCY);
}

#[test]
fn concurrency_falls_back_when_above_max() {
    let value = resolve_multipart_concurrency(fake_env(MULTIPART_CONCURRENCY_ENV, Some("65")));
    assert_eq!(value, MULTIPART_CONCURRENCY);
}

#[test]
fn concurrency_accepts_min_and_max_inclusive() {
    assert_eq!(
        resolve_multipart_concurrency(fake_env(MULTIPART_CONCURRENCY_ENV, Some("1"))),
        1,
    );
    assert_eq!(
        resolve_multipart_concurrency(fake_env(MULTIPART_CONCURRENCY_ENV, Some("64"))),
        64,
    );
}

#[test]
fn store_new_populates_tuning_fields() {
    // `S3ObjectStore::new()` reads from the real process env. We don't
    // assume it's clean, but we do assert the fields are within the
    // valid bounds (either the env-set value or the const fallback).
    let store = S3ObjectStore::new();
    assert!(
        (MULTIPART_PART_SIZE_MIN..=MULTIPART_PART_SIZE_MAX).contains(&store.multipart_part_size),
        "part size out of range: {}",
        store.multipart_part_size,
    );
    assert!(
        (MULTIPART_CONCURRENCY_MIN..=MULTIPART_CONCURRENCY_MAX)
            .contains(&store.multipart_concurrency),
        "concurrency out of range: {}",
        store.multipart_concurrency,
    );
    assert!(
        (MULTIPART_CONCURRENCY_MIN..=MULTIPART_CONCURRENCY_MAX)
            .contains(&store.download_reader_concurrency),
        "download reader concurrency out of range: {}",
        store.download_reader_concurrency,
    );
    assert!(
        (DOWNLOAD_CHUNK_SIZE_MIN..=DOWNLOAD_CHUNK_SIZE_MAX).contains(&store.download_chunk_size),
        "download chunk size out of range: {}",
        store.download_chunk_size,
    );
}

#[test]
fn download_buffer_budget_is_shared_across_store_instances() {
    // The budget is a process-wide invariant: two stores constructed
    // independently (e.g. by different subsystems) must draw from the
    // same permit pool, not mint one budget each.
    let a = S3ObjectStore::new();
    let b = S3ObjectStore::new();
    assert!(
        Arc::ptr_eq(&a.download_buffer_permits, &b.download_buffer_permits),
        "every S3ObjectStore must share the process-global download buffer budget"
    );
}

#[test]
fn multipart_ranges_balance_minimum_tail_across_parts() {
    let part_size = MULTIPART_PART_SIZE as u64;
    let size = 2 * part_size + MULTIPART_PART_SIZE_MIN as u64;
    let ranges = multipart_part_ranges(size, MULTIPART_PART_SIZE);

    assert_eq!(ranges.len(), 3);
    assert_contiguous_cover(&ranges, size);
    assert!(
        ranges
            .iter()
            .all(|range| { (MULTIPART_PART_SIZE_MIN as u64..=part_size).contains(&range.length) })
    );
    assert_s3_safe_download_ranges(&ranges, size, part_size);
    assert!(ranges[0].length - ranges.last().unwrap().length <= 1);
}

#[test]
fn multipart_ranges_balance_seed_sized_upload() {
    let part_size = 64 * 1024 * 1024;
    let size = 474_439_680;
    let ranges = multipart_part_ranges(size, part_size);

    assert_eq!(ranges.len(), 8);
    assert_contiguous_cover(&ranges, size);
    assert!(ranges.iter().all(|range| {
        (MULTIPART_PART_SIZE_MIN as u64..=part_size as u64).contains(&range.length)
    }));
    assert_s3_safe_download_ranges(&ranges, size, part_size as u64);
    assert_eq!(ranges[0].length, 59_304_960);
    assert_eq!(ranges[7].length, 59_304_960);
}

#[test]
fn multipart_ranges_keep_exact_part_boundaries() {
    let part_size = MULTIPART_PART_SIZE as u64;
    let ranges = multipart_part_ranges(2 * part_size, MULTIPART_PART_SIZE);

    assert_eq!(
        ranges,
        vec![
            MultipartPartRange {
                part_number: 1,
                offset: 0,
                length: part_size,
            },
            MultipartPartRange {
                part_number: 2,
                offset: part_size,
                length: part_size,
            },
        ]
    );
}

#[test]
fn byte_range_header_uses_inclusive_end_offset() {
    let header = byte_range_header(MultipartPartRange {
        part_number: 7,
        offset: 1024,
        length: 4096,
    });

    assert_eq!(header, "bytes=1024-5119");
}

#[test]
fn get_object_content_length_rejects_missing_or_negative_lengths() {
    assert_eq!(
        get_object_content_length("rootfs.ext4", Some(4096)).unwrap(),
        4096
    );

    assert!(matches!(
        get_object_content_length("rootfs.ext4", None),
        Err(ObjectStoreError::MissingField("content_length"))
    ));

    let err = get_object_content_length("rootfs.ext4", Some(-1)).unwrap_err();
    assert!(
        err.to_string()
            .contains("object rootfs.ext4 returned negative content-length -1")
    );
}

#[tokio::test]
async fn write_body_accepts_stream_chunks_smaller_than_expected_range() {
    let src = test_file_path("small-chunks-src");
    let dst = test_file_path("small-chunks-dst");
    let body_bytes = b"abcdefghijklmnopqrstuvwxyz";
    tokio::fs::write(&src, body_bytes).await.unwrap();
    tokio::fs::write(&dst, vec![0u8; 64]).await.unwrap();

    let mut body = ByteStream::read_from()
        .path(&src)
        .length(Length::Exact(body_bytes.len() as u64))
        .buffer_size(3)
        .build()
        .await
        .unwrap();
    let mut writer = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&dst)
        .await
        .unwrap();
    writer.seek(SeekFrom::Start(11)).await.unwrap();

    let written = write_body_to_file(
        &mut writer,
        &mut body,
        body_bytes.len() as u64,
        "range bytes=11-36",
        |_| {},
    )
    .await
    .unwrap();

    assert_eq!(written, body_bytes.len() as u64);
    let dst_bytes = tokio::fs::read(&dst).await.unwrap();
    assert_eq!(&dst_bytes[11..11 + body_bytes.len()], body_bytes);

    let _ = tokio::fs::remove_file(src).await;
    let _ = tokio::fs::remove_file(dst).await;
}

#[tokio::test]
async fn write_body_rejects_short_or_overlong_s3_bodies() {
    let short_dst = test_file_path("short-body-dst");
    tokio::fs::write(&short_dst, Vec::<u8>::new())
        .await
        .unwrap();
    let mut short_writer = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&short_dst)
        .await
        .unwrap();
    let mut short_body = ByteStream::from(vec![1, 2, 3]);

    let short_err = write_body_to_file(
        &mut short_writer,
        &mut short_body,
        4,
        "range bytes=0-3",
        |_| {},
    )
    .await
    .unwrap_err();
    assert!(
        short_err
            .to_string()
            .contains("range bytes=0-3 returned 3 bytes, expected 4")
    );

    let long_dst = test_file_path("long-body-dst");
    tokio::fs::write(&long_dst, Vec::<u8>::new()).await.unwrap();
    let mut long_writer = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&long_dst)
        .await
        .unwrap();
    let mut long_body = ByteStream::from(vec![1, 2, 3, 4, 5]);

    let long_err = write_body_to_file(
        &mut long_writer,
        &mut long_body,
        4,
        "range bytes=0-3",
        |_| {},
    )
    .await
    .unwrap_err();
    assert!(
        long_err
            .to_string()
            .contains("range bytes=0-3 returned more bytes than expected")
    );

    let _ = tokio::fs::remove_file(short_dst).await;
    let _ = tokio::fs::remove_file(long_dst).await;
}

/// How the ranged-download stand-in breaks one chunk's response body.
#[derive(Clone, Copy, Debug)]
enum BodyFault {
    /// Send half the body, then drop the connection short of its
    /// `Content-Length`.
    Truncate,
    /// Send half the body, then nothing more.
    Stall,
    /// Refuse the request with 412, as S3 does once the pinned object has
    /// been overwritten.
    PreconditionFailed,
}

const CHUNK_TEST_ETAG: &str = "\"synthetic-etag\"";

/// Serves `object` with ranged GETs pinned to [`CHUNK_TEST_ETAG`]. The
/// first `faults` requests for `faulty_range` fail with `fault`; every
/// request for that range is counted.
fn ranged_stand_in(
    object: Bytes,
    faulty_range: String,
    fault: BodyFault,
    faults: usize,
    requests: Arc<std::sync::atomic::AtomicUsize>,
) -> axum::Router {
    use axum::http::{HeaderMap, StatusCode, header};
    use axum::response::IntoResponse;
    use futures_util::StreamExt;
    use std::sync::atomic::Ordering;

    axum::Router::new().fallback(move |headers: HeaderMap| {
        let (object, faulty_range, requests) =
            (object.clone(), faulty_range.clone(), requests.clone());
        async move {
            let etag = [(header::ETAG, CHUNK_TEST_ETAG)];
            let Some(range) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) else {
                return (StatusCode::OK, etag, object).into_response();
            };
            assert_eq!(
                headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()),
                Some(CHUNK_TEST_ETAG),
                "every ranged GET must be pinned",
            );
            let (start, end) = range
                .strip_prefix("bytes=")
                .and_then(|span| span.split_once('-'))
                .map(|(start, end)| {
                    (
                        start.parse::<usize>().unwrap(),
                        end.parse::<usize>().unwrap(),
                    )
                })
                .expect("range header");
            let chunk = object.slice(start..=end);
            let head = [
                (header::ETAG, CHUNK_TEST_ETAG.to_string()),
                (header::CONTENT_LENGTH, chunk.len().to_string()),
                (
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", object.len()),
                ),
            ];
            if range != faulty_range || requests.fetch_add(1, Ordering::SeqCst) >= faults {
                return (StatusCode::PARTIAL_CONTENT, head, chunk).into_response();
            }
            let half = chunk.slice(..chunk.len() / 2);
            let first = futures_util::stream::iter([Ok::<_, std::io::Error>(half)]);
            let body = match fault {
                BodyFault::PreconditionFailed => {
                    return (
                        StatusCode::PRECONDITION_FAILED,
                        "<Error><Code>PreconditionFailed</Code>\
                         <Message>synthetic</Message></Error>",
                    )
                        .into_response();
                }
                // The pause lets the server flush the headers and the first
                // half, so the cut lands mid-body rather than before the
                // response.
                BodyFault::Truncate => {
                    axum::body::Body::from_stream(first.chain(futures_util::stream::once(async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Err(std::io::Error::other("synthetic cut"))
                    })))
                }
                BodyFault::Stall => {
                    axum::body::Body::from_stream(first.chain(futures_util::stream::pending()))
                }
            };
            (StatusCode::PARTIAL_CONTENT, head, body).into_response()
        }
    })
}

#[tokio::test]
async fn cancelled_download_keeps_its_chunk_charged_until_the_writer_exits() {
    use crate::test_support::{Recorder, credentials, serve, store, target};
    use std::io::Write;

    struct Admission;
    #[async_trait]
    impl DecodeAdmission for Admission {
        async fn admit(&self, _: u64) -> Result<Option<DecodeGuard>, DecodeAdmissionError> {
            Ok(None)
        }
    }

    struct BlockingSink {
        file: std::fs::File,
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
        exited: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl Write for BlockingSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
                self.release
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(std::io::Error::other)?;
            }
            self.file.write(bytes)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    impl FinishingWrite for BlockingSink {
        fn finish(mut self: Box<Self>) -> std::io::Result<bool> {
            self.flush()?;
            Ok(false)
        }
    }
    impl Drop for BlockingSink {
        fn drop(&mut self) {
            if let Some(exited) = self.exited.take() {
                let _ = exited.send(());
            }
        }
    }

    const CHUNK: usize = 256 * 1024;
    let endpoint = serve(ranged_stand_in(
        Bytes::from(vec![42; 2 * CHUNK]),
        String::new(),
        BodyFault::Truncate,
        0,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    ))
    .await;
    let mut store = store(
        S3Timeouts::resolve(|_| None),
        Duration::from_secs(1),
        &Arc::new(Recorder::default()),
    );
    store.download_chunk_size = CHUNK;
    let budget = Arc::new(Semaphore::new(1));
    store.download_reader_concurrency = 2;
    store.download_buffer_permits = budget.clone();
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("download");
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
    let channels = std::sync::Mutex::new(Some((entered_tx, release_rx, exited_tx)));
    let download = tokio::spawn(async move {
        let wrap = move |file| -> Box<dyn FinishingWrite> {
            let (entered, release, exited) = channels.lock().unwrap().take().unwrap();
            Box::new(BlockingSink {
                file,
                entered: Some(entered),
                release,
                exited: Some(exited),
            })
        };
        store
            .get_file_streamed(
                &credentials(),
                target(&endpoint),
                &destination,
                StreamDecode {
                    admission: &Admission,
                    wrap_writer: &wrap,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    download.abort();
    assert!(matches!(download.await, Err(error) if error.is_cancelled()));
    let released_early = budget.available_permits() != 0;
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), exited_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !released_early,
        "detached writer still owns the buffered input bytes"
    );
    assert_eq!(budget.available_permits(), 1);
    assert!(!directory.path().join("download").exists());
}

struct RangedDownload {
    result: Result<u64, ObjectStoreError>,
    expected: Bytes,
    written: Option<Vec<u8>>,
    faulty_requests: usize,
    recorder: Arc<crate::test_support::Recorder>,
}

/// Downloads a four-chunk object through [`ranged_stand_in`] with the
/// second chunk faulty.
async fn ranged_download(fault: BodyFault, faults: usize) -> RangedDownload {
    use crate::test_support::{Recorder, credentials, serve, store};

    const CHUNK: usize = 256 * 1024;
    let object: Bytes = (0..4 * CHUNK).map(|i| (i % 251) as u8).collect();
    let faulty_range = format!("bytes={}-{}", CHUNK, 2 * CHUNK - 1);
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let endpoint = serve(ranged_stand_in(
        object.clone(),
        faulty_range,
        fault,
        faults,
        requests.clone(),
    ))
    .await;
    let recorder = Arc::new(Recorder::default());
    let mut store = store(
        S3Timeouts::resolve(|_| None),
        Duration::from_millis(300),
        &recorder,
    );
    store.download_chunk_size = CHUNK;

    let dir = tempfile::tempdir().expect("download dir");
    let dst = dir.path().join("memory.bin");
    let result = store
        .get_file(&credentials(), crate::test_support::target(&endpoint), &dst)
        .await;
    RangedDownload {
        result,
        expected: object,
        written: std::fs::read(&dst).ok(),
        faulty_requests: requests.load(std::sync::atomic::Ordering::SeqCst),
        recorder,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranged_download_retries_a_chunk_whose_body_fails_once() {
    for (fault, kind) in [
        (BodyFault::Truncate, S3AttemptFailureKind::Io),
        (BodyFault::Stall, S3AttemptFailureKind::StalledStream),
    ] {
        let download = ranged_download(fault, 1).await;

        assert_eq!(
            download.result.as_ref().ok(),
            Some(&(download.expected.len() as u64)),
            "{fault:?}: {:?}",
            download.result,
        );
        assert!(
            download.written.as_deref() == Some(&download.expected[..]),
            "{fault:?}: downloaded bytes differ from the object"
        );
        assert_eq!(download.faulty_requests, 2, "{fault:?}");
        let failures = download.recorder.failed_targets();
        assert_eq!(failures.len(), 1, "{fault:?}: {failures:?}");
        let (target, failure) = &failures[0];
        assert_eq!(
            (failure.attempt, failure.kind, failure.will_retry),
            (1, kind, true),
            "{fault:?}",
        );
        assert_eq!(target.chunk_index, Some(1), "{fault:?}");
        let recoveries = download.recorder.recoveries();
        assert_eq!(recoveries.len(), 1, "{fault:?}");
        assert_eq!(recoveries[0].1.attempts, 2, "{fault:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranged_download_fails_after_three_chunk_body_failures() {
    let download = ranged_download(BodyFault::Truncate, usize::MAX).await;

    assert!(
        matches!(download.result, Err(ObjectStoreError::RangeBodyRead { .. })),
        "{:?}",
        download.result,
    );
    assert_eq!(download.faulty_requests, 3);
    let failures = download.recorder.failures();
    assert_eq!(
        failures
            .iter()
            .map(|failure| (failure.attempt, failure.will_retry))
            .collect::<Vec<_>>(),
        [(1, true), (2, true), (3, false)],
    );
    assert!(download.recorder.recoveries().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranged_download_does_not_retry_a_changed_object() {
    let download = ranged_download(BodyFault::PreconditionFailed, usize::MAX).await;

    assert!(
        matches!(download.result, Err(ObjectStoreError::Get(_))),
        "{:?}",
        download.result,
    );
    assert_eq!(download.faulty_requests, 1);
}
