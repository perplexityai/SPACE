use axum::http::Method;
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;

use super::sandbox_route_metric;

#[test]
fn sandbox_route_metric_matches_native_lifecycle_routes() {
    let cases = [
        (
            Method::POST,
            "/v1/sandboxes",
            Some(SandboxApiMetricIdentity::native("create")),
        ),
        (
            Method::GET,
            "/v1/sandboxes",
            Some(SandboxApiMetricIdentity::native("list")),
        ),
        (
            Method::GET,
            "/api/v1/sandboxes",
            Some(SandboxApiMetricIdentity::native("list")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc:pause",
            Some(SandboxApiMetricIdentity::native("pause")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc:not_an_action",
            Some(SandboxApiMetricIdentity::native("action")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc/process-sessions",
            Some(SandboxApiMetricIdentity::native("process_session_create")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc/commands",
            Some(SandboxApiMetricIdentity::native("command_create")),
        ),
        (
            Method::GET,
            "/v1/sandboxes/abc/stats",
            Some(SandboxApiMetricIdentity::native("stats")),
        ),
        (
            Method::GET,
            "/v1/sandboxes/abc/processes",
            Some(SandboxApiMetricIdentity::native("process_list")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc/processes/proc-1:signal",
            Some(SandboxApiMetricIdentity::native("process_signal")),
        ),
        (
            Method::GET,
            "/v1/sandboxes/abc/ports",
            Some(SandboxApiMetricIdentity::native("port_list")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc/ports/8080/open",
            Some(SandboxApiMetricIdentity::native("port_open")),
        ),
        (
            Method::GET,
            "/v1/sandboxes/abc/sessions/proc_019786caa9bc7ffeadc617b34456c126",
            Some(SandboxApiMetricIdentity::native("session_get")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc/files:write",
            Some(SandboxApiMetricIdentity::native("files_write")),
        ),
        (
            Method::POST,
            "/v1/sandboxes/abc/files:modified",
            Some(SandboxApiMetricIdentity::native("files_modified")),
        ),
    ];

    for (method, path, expected) in cases {
        let actual = sandbox_route_metric(&method, path);

        assert_eq!(actual, expected);
    }
}

#[test]
fn sandbox_route_metric_matches_ecw_lifecycle_routes() {
    let cases = [
        (
            Method::POST,
            "/sandboxes",
            Some(SandboxApiMetricIdentity::ecw("create")),
        ),
        (
            Method::POST,
            "/e2b/api/sandboxes",
            Some(SandboxApiMetricIdentity::ecw("create")),
        ),
        (
            Method::GET,
            "/v2/sandboxes",
            Some(SandboxApiMetricIdentity::ecw("list")),
        ),
        (
            Method::GET,
            "/e2b/api/v2/sandboxes",
            Some(SandboxApiMetricIdentity::ecw("list")),
        ),
        (
            Method::POST,
            "/sandboxes/abc/pause",
            Some(SandboxApiMetricIdentity::ecw("pause")),
        ),
        (
            Method::POST,
            "/sandboxes/abc/resume",
            Some(SandboxApiMetricIdentity::ecw("resume")),
        ),
        (
            Method::POST,
            "/sandboxes/abc/refreshes",
            Some(SandboxApiMetricIdentity::ecw("refresh")),
        ),
        (Method::GET, "/files", None),
    ];

    for (method, path, expected) in cases {
        let actual = sandbox_route_metric(&method, path);

        assert_eq!(actual, expected);
    }
}
