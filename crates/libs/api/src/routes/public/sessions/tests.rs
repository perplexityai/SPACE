use super::*;
use pplx_space_common_core::types::MAX_GUEST_USER_NAME_BYTES;

const LINUX_CWD: &str = "/workspace";
const WINDOWS_DRIVE_CWD: &str = "C:\\Users\\me";
const WINDOWS_UNC_CWD: &str = "\\\\server\\share";

fn process_request_with_cwd(cwd: &str) -> CreateProcessRequest {
    process_request(Some(cwd.to_string()), None)
}

fn process_request(cwd: Option<String>, user: Option<String>) -> CreateProcessRequest {
    CreateProcessRequest {
        command: vec!["echo".to_string()],
        cwd,
        env: Default::default(),
        pty: None,
        stdin: None,
        user,
        timeout: None,
    }
}

#[test]
fn process_request_user_is_optional_and_bounded() {
    validate_process_request(&process_request(None, None)).expect("absent user accepted");
    validate_process_request(&process_request(None, Some("root".to_string())))
        .expect("named user accepted");

    for rejected in [
        String::new(),
        "u".repeat(MAX_GUEST_USER_NAME_BYTES + 1),
        "ro\0ot".to_string(),
    ] {
        let error = validate_process_request(&process_request(None, Some(rejected)))
            .expect_err("invalid user rejected");
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }
}

#[test]
fn terminal_size_validation_accepts_absent_or_nonzero_size() {
    validate_terminal_size(None).expect("absent size accepted");
    validate_terminal_size(Some(&TerminalSize { rows: 24, cols: 80 }))
        .expect("nonzero size accepted");
}

#[test]
fn terminal_size_validation_rejects_zero_dimensions() {
    let rows = validate_terminal_size(Some(&TerminalSize { rows: 0, cols: 80 }))
        .expect_err("zero rows rejected");
    assert_eq!(rows.into_response().status(), StatusCode::BAD_REQUEST);

    let cols = validate_terminal_size(Some(&TerminalSize { rows: 24, cols: 0 }))
        .expect_err("zero cols rejected");
    assert_eq!(cols.into_response().status(), StatusCode::BAD_REQUEST);
}

#[test]
fn process_request_cwd_requires_a_nonempty_linux_guest_path() {
    validate_process_request(&process_request_with_cwd(LINUX_CWD))
        .expect("Linux guest path accepted");

    for cwd in ["", WINDOWS_DRIVE_CWD, WINDOWS_UNC_CWD] {
        let error = validate_process_request(&process_request_with_cwd(cwd))
            .expect_err("non-Linux guest path rejected");
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }
}

#[test]
fn session_list_limit_defaults_and_caps() {
    assert_eq!(
        ListSessionsQuery {
            cursor: None,
            limit: None,
            kind: None,
        }
        .limit(),
        DEFAULT_SESSION_LIST_LIMIT
    );
    assert_eq!(
        ListSessionsQuery {
            cursor: None,
            limit: Some(MAX_SESSION_LIST_LIMIT + 1),
            kind: None,
        }
        .limit(),
        MAX_SESSION_LIST_LIMIT
    );
}

#[test]
fn session_cursor_round_trips() {
    let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let cursor = session_cursor(created_at, session_id).unwrap();
    let payload = decode_cursor(CursorKind::Session, cursor.as_str()).unwrap();
    let parsed = parse_session_cursor(&payload).unwrap();

    assert_eq!(parsed.created_at, created_at);
    assert_eq!(parsed.session_id, session_id);
}
