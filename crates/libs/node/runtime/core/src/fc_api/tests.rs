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
