use super::*;
use std::path::Path;

#[test]
fn snapshot_request_bodies_match_firecracker_contract_on_x86_64() {
    let create_body =
        create_snapshot_request_body(Path::new("/vmstate"), Path::new("/memory")).unwrap();
    assert_eq!(create_body["snapshot_path"], "/vmstate");
    assert_eq!(create_body["mem_file_path"], "/memory");
    assert_eq!(create_body["snapshot_type"], "Diff");

    let load_body =
        load_snapshot_request_body_for_arch(Path::new("/vmstate"), Path::new("/memory"), "x86_64")
            .unwrap();

    assert_eq!(load_body["snapshot_path"], "/vmstate");
    assert_eq!(load_body["mem_backend"]["backend_type"], "File");
    assert_eq!(load_body["mem_backend"]["backend_path"], "/memory");
    assert_eq!(load_body["resume_vm"], false);
    assert_eq!(load_body["track_dirty_pages"], true);
    assert_eq!(load_body["clock_realtime"], true);
}

#[test]
fn load_snapshot_request_omits_realtime_clock_restore_on_aarch64() {
    let body =
        load_snapshot_request_body_for_arch(Path::new("/vmstate"), Path::new("/memory"), "aarch64")
            .unwrap();

    assert_eq!(body["snapshot_path"], "/vmstate");
    assert_eq!(body["mem_backend"]["backend_type"], "File");
    assert_eq!(body["mem_backend"]["backend_path"], "/memory");
    assert_eq!(body["resume_vm"], false);
    assert_eq!(body["track_dirty_pages"], true);
    assert!(body.get("clock_realtime").is_none());
}

#[cfg(unix)]
#[test]
fn create_snapshot_request_rejects_non_utf8_paths() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let err = create_snapshot_request_body(
        Path::new("/vmstate"),
        Path::new(OsStr::from_bytes(b"/tmp/\xFF")),
    )
    .expect_err("non-UTF-8 memory path should fail before calling Firecracker");

    assert_non_utf8_path_error(err, "mem_file_path");
}

#[cfg(unix)]
#[test]
fn load_snapshot_request_rejects_non_utf8_paths() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let err = load_snapshot_request_body_for_arch(
        Path::new("/vmstate"),
        Path::new(OsStr::from_bytes(b"/tmp/\xFF")),
        "x86_64",
    )
    .expect_err("non-UTF-8 memory backend path should fail before calling Firecracker");

    assert_non_utf8_path_error(err, "mem_backend.backend_path");
}

#[cfg(unix)]
fn assert_non_utf8_path_error(error: FcApiError, expected_field: &'static str) {
    match error {
        FcApiError::NonUtf8Path { field, .. } => assert_eq!(field, expected_field),
        other => panic!("expected NonUtf8Path error, got {other:?}"),
    }
}

fn rootfs_limiter() -> RateLimiter {
    RateLimiter {
        bandwidth: TokenBucket {
            size: 104_857_600,
            refill_time_ms: 1000,
        },
        operations: TokenBucket {
            size: 5_000,
            refill_time_ms: 5_000,
        },
    }
}

#[test]
fn drive_rate_limiter_request_matches_firecracker_contract() {
    assert_eq!(
        drive_rate_limiter_request_body("rootfs", &rootfs_limiter()),
        serde_json::json!({
            "drive_id": "rootfs",
            "rate_limiter": {
                "bandwidth": { "size": 104_857_600, "refill_time": 1000 },
                "ops": { "size": 5_000, "refill_time": 5_000 }
            }
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_drive_rate_limiter_update_patches_the_drive() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("firecracker.socket");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let firecracker = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        let (head, body) = loop {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before sending a whole request");
            request.extend_from_slice(&chunk[..read]);
            let text = String::from_utf8(request.clone()).unwrap();
            if let Some((head, body)) = text.split_once("\r\n\r\n") {
                let length = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                if body.len() == length {
                    break (head.to_string(), body.to_string());
                }
            }
        };
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .await
            .unwrap();
        (head, body)
    });

    FcApiClient::new(&socket)
        .update_drive_rate_limiter("rootfs", &rootfs_limiter())
        .await
        .unwrap();
    let (head, body) = firecracker.await.unwrap();
    assert_eq!(head.lines().next(), Some("PATCH /drives/rootfs HTTP/1.1"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        drive_rate_limiter_request_body("rootfs", &rootfs_limiter())
    );
}

#[test]
fn balloon_device_json_matches_firecracker_contract_for_each_mode() {
    use pplx_space_common_core::types::{BalloonConfig, BalloonDevice};
    use std::num::NonZeroU32;

    let cases = [
        (
            BalloonConfig::new(
                BalloonDevice::Inflate {
                    amount_mib: NonZeroU32::new(1024).unwrap(),
                    deflate_on_oom: true,
                },
                0,
            ),
            serde_json::json!({
                "amount_mib": 1024,
                "deflate_on_oom": true,
                "stats_polling_interval_s": 0,
                "free_page_hinting": false,
                "free_page_reporting": false
            }),
        ),
        (
            BalloonConfig::new(BalloonDevice::FreePageReporting, 5),
            serde_json::json!({
                "amount_mib": 0,
                "deflate_on_oom": false,
                "stats_polling_interval_s": 5,
                "free_page_hinting": false,
                "free_page_reporting": true
            }),
        ),
        (
            BalloonConfig::new(BalloonDevice::FreePageHinting, 0),
            serde_json::json!({
                "amount_mib": 0,
                "deflate_on_oom": false,
                "stats_polling_interval_s": 0,
                "free_page_hinting": true,
                "free_page_reporting": false
            }),
        ),
    ];
    for (config, expected) in cases {
        let rendered = balloon_json(config);
        assert_eq!(rendered, expected, "{config:?}");
        // What Firecracker reports back for the device maps to the same mode.
        assert_eq!(
            FirecrackerBalloon::from_json(&rendered).unwrap().mode(),
            config.mode()
        );
    }
}

/// One canned Firecracker API exchange: the request line it answers and the
/// raw HTTP response.
type Exchange = (&'static str, String);

fn http_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

const HINT_START: &str = "PATCH /balloon/hinting/start HTTP/1.1";
const HINT_STATUS: &str = "GET /balloon/hinting/status HTTP/1.1";
const HINT_STOP: &str = "PATCH /balloon/hinting/stop HTTP/1.1";

fn hint_started() -> Exchange {
    (HINT_START, http_response("200 OK", ""))
}

fn hint_running() -> Exchange {
    (
        HINT_STATUS,
        http_response("200 OK", r#"{"host_cmd":2,"guest_cmd":2}"#),
    )
}

fn hint_done() -> Exchange {
    (
        HINT_STATUS,
        http_response("200 OK", r#"{"host_cmd":1,"guest_cmd":0}"#),
    )
}

/// Serve `script` in order, then answer further status polls as still
/// running and a stop as accepted. Ends after a completed status or a stop
/// and returns the request lines it answered. A client that abandons a
/// request mid-exchange is tolerated, as a timed-out poll does.
fn fake_firecracker(socket: &Path, script: Vec<Exchange>) -> tokio::task::JoinHandle<Vec<String>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    tokio::spawn(async move {
        let mut script = script.into_iter();
        let mut seen = Vec::new();
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let head = loop {
                let Ok(read) = stream.read(&mut chunk).await else {
                    break None;
                };
                if read == 0 {
                    break None;
                }
                request.extend_from_slice(&chunk[..read]);
                let text = String::from_utf8_lossy(&request).into_owned();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .map_or(0, |value| value.parse::<usize>().unwrap());
                    if body.len() >= length {
                        break Some(head.to_string());
                    }
                }
            };
            let Some(head) = head else { continue };
            let line = head.lines().next().unwrap().to_string();
            let (expected, response) = script.next().unwrap_or_else(|| {
                if line == HINT_STOP {
                    (HINT_STOP, http_response("200 OK", ""))
                } else {
                    hint_running()
                }
            });
            assert_eq!(line, expected);
            let finished = line == HINT_STOP || response.contains(r#""host_cmd":1"#);
            seen.push(line);
            // The client may already have given up on this request.
            let _ = stream.write_all(response.as_bytes()).await;
            if finished {
                return seen;
            }
        }
    })
}

#[tokio::test(flavor = "current_thread")]
async fn free_page_hinting_waits_for_the_guest_to_finish() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("firecracker.socket");
    let firecracker = fake_firecracker(&socket, vec![hint_started(), hint_running(), hint_done()]);

    let outcome = FcApiClient::new(&socket)
        .hint_free_pages(Duration::from_secs(30), Duration::from_millis(1))
        .await
        .unwrap();

    assert!(matches!(outcome, FreePageHintOutcome::Completed { .. }));
    assert_eq!(
        firecracker.await.unwrap(),
        [HINT_START, HINT_STATUS, HINT_STATUS]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unfinished_free_page_hinting_is_stopped_at_the_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("firecracker.socket");
    let firecracker = fake_firecracker(&socket, vec![hint_started()]);

    let timeout = Duration::from_millis(50);
    let outcome = FcApiClient::new(&socket)
        .hint_free_pages(timeout, Duration::from_millis(5))
        .await
        .unwrap();

    let FreePageHintOutcome::TimedOut { elapsed } = outcome else {
        panic!("expected a timeout, got {outcome:?}");
    };
    assert!(elapsed >= timeout, "{elapsed:?}");
    let seen = firecracker.await.unwrap();
    assert_eq!(seen.first().map(String::as_str), Some(HINT_START));
    assert_eq!(seen.last().map(String::as_str), Some(HINT_STOP));
}

#[tokio::test(flavor = "current_thread")]
async fn a_vm_without_a_balloon_reports_none() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("firecracker.socket");
    let _firecracker = fake_firecracker(
        &socket,
        vec![(
            "GET /balloon HTTP/1.1",
            http_response(
                "400 Bad Request",
                r#"{"fault_message":"The balloon device was not configured."}"#,
            ),
        )],
    );

    assert_eq!(FcApiClient::new(&socket).balloon().await.unwrap(), None);
}
