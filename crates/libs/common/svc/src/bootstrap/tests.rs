use super::*;
use http::HeaderValue;
use tower_http::classify::{ClassifiedResponse, ClassifyResponse};

#[test]
fn staging_socket_recognition_follows_the_binder_and_rejects_other_names() {
    for socket in [
        "s",
        "handoff.sock",
        "volume-manager.sock",
        "a-very-long-service-name.sock",
    ] {
        let path = Path::new(socket);
        for attempt in 0..64 {
            let staged =
                unix_socket_staging_path_for_attempt(path, Uuid::now_v7(), attempt).unwrap();
            assert!(is_unix_socket_staging_name(
                path,
                staged.file_name().unwrap()
            ));
        }
    }
    let path = Path::new("volume-manager.sock");
    for name in [
        "volume-manager.sock",
        ".handoff-",
        ".handoff-012345678!",
        ".handoff-0123456789A",
        ".handoff-012345678",
        "xhandoff-0123456789",
    ] {
        assert!(
            !is_unix_socket_staging_name(path, std::ffi::OsStr::new(name)),
            "{name}"
        );
    }
    assert!(!is_unix_socket_staging_name(
        Path::new("/"),
        std::ffi::OsStr::new("s")
    ));
}

#[test]
fn unix_socket_staging_path_preserves_the_published_path_budget() {
    let socket_path = Path::new(
        "/tmp/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.sock",
    );

    let staging_path =
        unix_socket_staging_path_for_attempt(socket_path, Uuid::now_v7(), 0).unwrap();

    assert!(
        staging_path.as_os_str().as_bytes().len() <= socket_path.as_os_str().as_bytes().len(),
        "staging path {} exceeds published path budget {}",
        staging_path.display(),
        socket_path.display()
    );
}

#[test]
fn concurrent_unix_socket_replacements_use_distinct_staging_paths() {
    let socket_path = Path::new("/tmp/handoff.sock");

    let first = unix_socket_staging_path_for_attempt(socket_path, Uuid::from_u128(0), 0).unwrap();
    let second = unix_socket_staging_path_for_attempt(socket_path, Uuid::from_u128(1), 0).unwrap();

    assert_ne!(
        first, second,
        "concurrent binders must not share staging socket ownership"
    );
}

#[test]
fn compact_unix_socket_staging_keeps_a_minimum_nonce_budget() {
    let socket_path = Path::new("/tmp/1234567890");

    let staging_path =
        unix_socket_staging_path_for_attempt(socket_path, Uuid::from_u128(0), 0).unwrap();
    let staging_file_name = staging_path.file_name().unwrap().as_bytes();

    assert_eq!(staging_file_name.len(), 10);
    assert!(
        !staging_file_name.starts_with(b".handoff-"),
        "readable prefix must not consume the compact nonce budget"
    );
}

#[test]
fn compact_unix_socket_staging_attempts_do_not_repeat() {
    let socket_path = Path::new("/tmp/s");
    let seed = Uuid::from_u128(0);
    let paths = (0..64)
        .map(|attempt| unix_socket_staging_path_for_attempt(socket_path, seed, attempt).unwrap())
        .collect::<std::collections::HashSet<_>>();

    assert_eq!(paths.len(), 64);
}

#[test]
fn unix_socket_replacement_does_not_unlink_another_binders_staging_path() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let _runtime_guard = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let socket_path = directory.path().join("handoff.sock");
    let staging_path = directory.path().join(".handoff.sock.handoff-collision");
    let _published = bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600)).unwrap();
    let competing_listener = std::os::unix::net::UnixListener::bind(&staging_path).unwrap();

    let error =
        bind_unix_socket_replacement_at(&socket_path, UnixSocketOptions::new(0o600), &staging_path)
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("failed to bind unix socket staging path"),
        "unexpected replacement error: {error:#}"
    );
    let connection = std::os::unix::net::UnixStream::connect(&staging_path).unwrap();
    let (accepted, _) = competing_listener.accept().unwrap();
    drop((connection, accepted));
}

#[test]
fn unix_socket_rebind_never_removes_published_path() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let _runtime_guard = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let socket_path = directory.path().join("handoff.sock");
    let listener = bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600)).unwrap();
    let start_acceptor = |listener: tokio::net::UnixListener| {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let thread_running = running.clone();
        let listener = listener.into_std().unwrap();
        listener.set_nonblocking(true).unwrap();
        let thread = std::thread::spawn(move || {
            while thread_running.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => drop(stream),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(_) => break,
                }
            }
        });
        (running, thread)
    };
    let (mut accept_running, mut accept_thread) = start_acceptor(listener);

    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let successes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let not_found = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let other = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let connector = {
        let path = socket_path.clone();
        let running = running.clone();
        let successes = successes.clone();
        let not_found = not_found.clone();
        let other = other.clone();
        std::thread::spawn(move || {
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                match std::os::unix::net::UnixStream::connect(&path) {
                    Ok(stream) => {
                        successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        drop(stream);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        not_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // The synthetic connection storm can fill a listener backlog.
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {}
                    Err(_) => {
                        other.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        })
    };

    for _ in 0..1_000 {
        let listener = bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600)).unwrap();
        let (next_accept_running, next_accept_thread) = start_acceptor(listener);
        accept_running.store(false, std::sync::atomic::Ordering::Relaxed);
        accept_thread.join().unwrap();
        accept_running = next_accept_running;
        accept_thread = next_accept_thread;
    }

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    connector.join().unwrap();
    accept_running.store(false, std::sync::atomic::Ordering::Relaxed);
    accept_thread.join().unwrap();
    let successes = successes.load(std::sync::atomic::Ordering::Relaxed);
    let not_found = not_found.load(std::sync::atomic::Ordering::Relaxed);
    let other = other.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        successes > 0,
        "connector must exercise the published socket"
    );
    assert_eq!(
        (not_found, other),
        (0, 0),
        "the published socket path must never disappear while it is rebound",
    );
}

#[test]
fn parse_grpc_path_splits_tonic_style_path() {
    assert_eq!(
        parse_grpc_path("/pplx.space.node.NodeManager/CreateSandbox"),
        ("pplx.space.node.NodeManager", "CreateSandbox"),
    );
}

#[test]
fn parse_grpc_path_handles_missing_leading_slash() {
    assert_eq!(
        parse_grpc_path("pplx.space.node.NodeManager/CreateSandbox"),
        ("pplx.space.node.NodeManager", "CreateSandbox"),
    );
}

#[test]
fn parse_grpc_path_handles_unexpected_shape() {
    assert_eq!(parse_grpc_path("/health"), ("", "health"));
}

#[test]
fn request_id_prefers_x_request_id() {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", HeaderValue::from_static("req-123"));
    headers.insert("x-correlation-id", HeaderValue::from_static("corr-456"));
    assert_eq!(
        request_id_from_headers(&headers).as_deref(),
        Some("req-123"),
    );
}

#[test]
fn request_id_falls_back_to_correlation_id() {
    let mut headers = HeaderMap::new();
    headers.insert("x-correlation-id", HeaderValue::from_static("corr-456"));
    assert_eq!(
        request_id_from_headers(&headers).as_deref(),
        Some("corr-456"),
    );
}

#[test]
fn request_id_ignores_blank_header() {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", HeaderValue::from_static("   "));
    assert_eq!(request_id_from_headers(&headers), None);
}

#[test]
fn grpc_status_from_headers_parses_numeric_value() {
    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", HeaderValue::from_static("8"));
    assert_eq!(grpc_status_from_headers(&headers), Some(8));
}

#[test]
fn grpc_status_from_headers_skips_missing() {
    let headers = HeaderMap::new();
    assert_eq!(grpc_status_from_headers(&headers), None);
}

#[test]
fn grpc_server_application_statuses_are_not_failures() {
    for code in [
        tonic::Code::Ok,
        tonic::Code::Cancelled,
        tonic::Code::InvalidArgument,
        tonic::Code::NotFound,
        tonic::Code::AlreadyExists,
        tonic::Code::PermissionDenied,
        tonic::Code::ResourceExhausted,
        tonic::Code::FailedPrecondition,
        tonic::Code::Aborted,
        tonic::Code::OutOfRange,
        tonic::Code::Unauthenticated,
    ] {
        let response = HttpResponse::builder()
            .header("grpc-status", (code as i32).to_string())
            .body(())
            .unwrap();

        let classified = grpc_server_status_classifier().classify_response(&response);
        assert!(
            matches!(classified, ClassifiedResponse::Ready(Ok(()))),
            "{code:?} should be a regular server response"
        );
        assert!(!grpc_status_is_error(code));
    }

    for code in [
        tonic::Code::Unknown,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Unimplemented,
        tonic::Code::Internal,
        tonic::Code::Unavailable,
        tonic::Code::DataLoss,
    ] {
        let response = HttpResponse::builder()
            .header("grpc-status", (code as i32).to_string())
            .body(())
            .unwrap();
        let classified = grpc_server_status_classifier().classify_response(&response);
        assert!(
            matches!(classified, ClassifiedResponse::Ready(Err(_))),
            "{code:?} should remain a server failure"
        );
        assert!(grpc_status_is_error(code));
    }
}
#[test]
fn format_grpc_request_params_escapes_special_characters() {
    let formatted = format_grpc_request_params([
        ("sandbox_id", "sbx,with,commas".to_string()),
        ("note", r#"has "quotes" and \backslash"#.to_string()),
        ("port", "8080".to_string()),
    ]);
    // Round-trips as a parseable JSON object — no separator collisions.
    let parsed: serde_json::Value = serde_json::from_str(&formatted).expect("output is valid JSON");
    assert_eq!(parsed["sandbox_id"], "sbx,with,commas");
    assert_eq!(parsed["note"], r#"has "quotes" and \backslash"#);
    assert_eq!(parsed["port"], "8080");
}

#[test]
fn failed_grpc_request_logging_is_enabled_only_for_testing_env() {
    assert!(failed_grpc_request_logging_enabled_for_env(Some("testing")));
    assert!(failed_grpc_request_logging_enabled_for_env(Some(
        " TESTING "
    )));

    assert!(!failed_grpc_request_logging_enabled_for_env(Some("prod")));
    assert!(!failed_grpc_request_logging_enabled_for_env(Some("")));
    assert!(!failed_grpc_request_logging_enabled_for_env(None));
}

#[test]
fn grpc_request_params_log_severity_keeps_application_statuses_debug() {
    assert_eq!(
        grpc_request_params_log_severity(tonic::Code::PermissionDenied),
        GrpcRequestParamsLogSeverity::Debug,
    );
    assert_eq!(
        grpc_request_params_log_severity(tonic::Code::NotFound),
        GrpcRequestParamsLogSeverity::Debug,
    );
    assert_eq!(
        grpc_request_params_log_severity(tonic::Code::Unavailable),
        GrpcRequestParamsLogSeverity::Warn,
    );
}

#[test]
fn grpc_request_params_for_log_preserves_small_params() {
    let params = r#"{"port":"8080","sandbox_id":"sbx"}"#;
    let log = grpc_request_params_for_log(params);

    assert_eq!(log.value, params);
    assert!(!log.truncated);
}

#[test]
fn grpc_request_params_for_log_truncates_large_params() {
    let params = "a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES + 1);
    let log = grpc_request_params_for_log(&params);

    assert!(log.truncated);
    assert!(
        log.value
            .starts_with(&"a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES))
    );
    assert!(log.value.ends_with(&format!(
        "... [truncated, original_bytes={}]",
        MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES + 1
    )));
}

#[test]
fn truncate_utf8_for_log_truncates_on_utf8_boundary() {
    let value = format!(
        "{}é-suffix",
        "a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES - 1)
    );
    let log = truncate_utf8_for_log(&value, MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES);

    assert!(log.truncated);
    assert!(!log.value.contains('\u{fffd}'));
    assert!(log.value.starts_with(&format!(
        "{}...",
        "a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES - 1)
    )));
}

// Signal a re-executed child rather than the test runner: installing stop
// handlers in the runner would change how unrelated tests respond to them.
#[test]
fn stop_signals_are_handled_once_shutdown_token_returns() {
    use nix::sys::signal::Signal;

    const CHILD: &str = "SPACE_COMMON_SVC_SHUTDOWN_SIGNAL_TEST_CHILD";
    if let Some(signal) = std::env::var_os(CHILD) {
        let signal: Signal = signal
            .to_str()
            .expect("signal name")
            .parse()
            .expect("stop signal");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let shutdown = shutdown_token();
                // A current-thread runtime cannot run the spawned signal task
                // before this block yields, so the signal is observed only if
                // `shutdown_token` installed the handlers before returning.
                nix::sys::signal::raise(signal).expect("raise stop signal");
                tokio::time::timeout(Duration::from_secs(5), shutdown.cancelled())
                    .await
                    .expect("stop signal cancels the shutdown token");
            });
        return;
    }

    let test_executable = std::env::current_exe().expect("test executable");
    for signal in [Signal::SIGINT, Signal::SIGTERM] {
        let status = std::process::Command::new(&test_executable)
            .args([
                "--exact",
                "bootstrap::tests::stop_signals_are_handled_once_shutdown_token_returns",
                "--nocapture",
            ])
            .env(CHILD, signal.as_str())
            .stdout(std::process::Stdio::null())
            .status()
            .expect("child test process");
        assert!(
            status.success(),
            "{signal} raised after shutdown_token returned was not handled: {status}"
        );
    }
}
