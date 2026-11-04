use super::*;
use axum::response::IntoResponse as _;

#[test]
fn public_process_omits_environment() {
    let process = public_process(spaced_ops::ProcessInfo {
        process_id: "process-1".to_string(),
        pid: 42,
        command: vec![
            "python".to_string(),
            "-m".to_string(),
            "http.server".to_string(),
        ],
        cwd: Some("/workspace".to_string()),
        env: [("SECRET".to_string(), "value".to_string())].into(),
        user: Some("user".to_string()),
        tag: Some("server".to_string()),
        pty: None,
        exit: None,
        ended_at: None,
    })
    .expect("valid process");

    assert_eq!(
        process.process_id,
        ProcessId::parse("process-1").expect("process id parses")
    );
    assert_eq!(process.pid, 42);
    assert_eq!(
        process.command,
        vec![
            "python".to_string(),
            "-m".to_string(),
            "http.server".to_string()
        ]
    );
    assert_eq!(process.cwd.as_deref(), Some("/workspace"));
    assert_eq!(process.user.as_deref(), Some("user"));
    assert_eq!(process.tag.as_deref(), Some("server"));
    // A running process keeps the wire shape it had before exits were
    // reported, so strict clients see no new field.
    assert_eq!(
        serde_json::to_value(&process).unwrap(),
        serde_json::json!({
            "process_id": "process-1",
            "pid": 42,
            "command": ["python", "-m", "http.server"],
            "cwd": "/workspace",
            "user": "user",
            "tag": "server",
        })
    );
}

// An exited process reports its exit as the stream's `process_exited` does.
#[test]
fn public_process_reports_an_exit_like_process_exited() {
    use spaced_ops::process_exit::Outcome;
    let ended_at = prost_types::Timestamp {
        seconds: 1_790_596_800,
        nanos: 500_000_000,
    };
    for (outcome, exit_code, exit_status, signal) in [
        (
            Outcome::Exited(spaced_ops::ProcessExited { exit_code: 7 }),
            7,
            "exited",
            None,
        ),
        (
            Outcome::Signaled(spaced_ops::ProcessSignaled { signal: Some(9) }),
            137,
            "signaled",
            Some(9),
        ),
        (
            Outcome::TimedOut(spaced_ops::ProcessTimedOut { signal: Some(9) }),
            124,
            "timed_out",
            Some(9),
        ),
        (
            Outcome::Unknown(spaced_ops::ProcessUnknown {}),
            1,
            "unknown",
            None,
        ),
    ] {
        let process = public_process(spaced_ops::ProcessInfo {
            process_id: "process-1".to_string(),
            pid: 42,
            command: vec!["true".to_string()],
            exit: Some(spaced_ops::ProcessExit {
                outcome: Some(outcome),
            }),
            ended_at: Some(ended_at),
            ..Default::default()
        })
        .expect("valid process");
        let json = serde_json::to_value(&process).unwrap();
        assert_eq!(json["state"], "exited", "{json}");
        assert_eq!(json["exit_code"], exit_code, "{json}");
        assert_eq!(json["exit_status"], exit_status, "{json}");
        assert_eq!(
            json.get("signal").and_then(|signal| signal.as_i64()),
            signal.map(i64::from),
            "{json}"
        );
        assert_eq!(json["ended_at"], "2026-09-28T12:00:00.5Z", "{json}");
    }
}

#[test]
fn public_process_rejects_an_invalid_runtime_process_id() {
    let error = public_process(spaced_ops::ProcessInfo {
        process_id: "x".repeat(pplx_space_common_core::types::MAX_PROCESS_ID_BYTES + 1),
        pid: 42,
        command: vec!["true".to_string()],
        cwd: None,
        env: Default::default(),
        user: None,
        tag: None,
        pty: None,
        exit: None,
        ended_at: None,
    })
    .expect_err("invalid process id");

    assert_eq!(
        error.into_response().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn process_action_dispatch_matches_the_published_paths() {
    for (path, expected) in [
        (
            pplx_space_public_api::routes::SIGNAL_PROCESS.path(),
            ProcessAction::Signal,
        ),
        (
            pplx_space_public_api::routes::KILL_PROCESS.path(),
            ProcessAction::Kill,
        ),
    ] {
        let segment = path
            .rsplit('/')
            .next()
            .expect("published process action path has a final segment")
            .replace("{process_id}", "process-1");
        assert_eq!(
            parse_process_action_segment(&segment).expect("published action should dispatch"),
            (
                ProcessId::parse("process-1").expect("process id parses"),
                expected
            )
        );
    }
    for invalid in ["process-1", "process-1:restart", ":signal"] {
        parse_process_action_segment(invalid).expect_err("unknown action should be rejected");
    }
}

#[test]
fn process_page_orders_processes_and_resumes_after_cursor() {
    let processes = || {
        ["process-3", "process-1", "process-2"]
            .into_iter()
            .enumerate()
            .map(|(pid, process_id)| PublicProcess {
                process_id: ProcessId::parse(process_id).expect("process id parses"),
                pid: pid as u32,
                command: vec!["true".to_string()],
                cwd: None,
                user: None,
                tag: None,
                state: None,
                exit_code: None,
                exit_status: None,
                signal: None,
                ended_at: None,
            })
            .collect::<Vec<_>>()
    };

    let first = process_page(processes(), None, 2).unwrap();
    assert_eq!(
        first
            .data
            .iter()
            .map(|process| process.process_id.as_str())
            .collect::<Vec<_>>(),
        ["process-1", "process-2"]
    );
    let cursor = first.next_cursor.expect("first page has another result");
    let cursor = decode_cursor(CursorKind::Process, cursor.as_str()).unwrap();
    let cursor = ProcessId::parse(cursor).unwrap();

    let second = process_page(processes(), Some(&cursor), 2).unwrap();
    assert_eq!(
        second
            .data
            .iter()
            .map(|process| process.process_id.as_str())
            .collect::<Vec<_>>(),
        ["process-3"]
    );
    assert_eq!(second.next_cursor, None);
}
