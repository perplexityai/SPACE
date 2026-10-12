use std::time::Instant;

use axum::{extract::Request, http::Method, middleware::Next, response::Response};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_public_api::SandboxLifecycleAction;

/// Carries the requested sandbox template name from a create handler back to
/// `record_sandbox_route_metrics` via response extensions, so the route-level
/// latency timer can be broken down per template.
#[derive(Clone)]
pub(crate) struct RouteTemplateName(pub(crate) String);

pub async fn record_sandbox_route_metrics(request: Request, next: Next) -> Response {
    let identity = sandbox_route_metric(request.method(), request.uri().path());
    let started_at = Instant::now();
    let response = next.run(request).await;

    if let Some(identity) = identity {
        let template_name = response
            .extensions()
            .get::<RouteTemplateName>()
            .map(|name| name.0.clone());
        pplx_space_metrics::sandbox::api::route_outcome(
            identity,
            response.status().as_u16(),
            started_at,
            template_name.as_deref(),
        );
    }

    response
}

fn sandbox_route_metric(method: &Method, path: &str) -> Option<SandboxApiMetricIdentity> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let segments = normalized_route_segments(&segments);

    native_route_metric(method, segments).or_else(|| ecw_route_metric(method, segments))
}

fn normalized_route_segments<'a>(segments: &'a [&'a str]) -> &'a [&'a str] {
    match segments {
        ["api", "v1", ..] => segments.get(1..).unwrap_or(segments),
        ["e2b", "api", ..] => segments.get(2..).unwrap_or(segments),
        _ => segments,
    }
}

fn native_route_metric(method: &Method, segments: &[&str]) -> Option<SandboxApiMetricIdentity> {
    let operation = match (method, segments) {
        (&Method::GET, ["v1", "sandboxes"]) => "list",
        (&Method::POST, ["v1", "sandboxes"]) => "create",
        (&Method::GET, ["v1", "sandboxes", _]) => "get",
        (&Method::DELETE, ["v1", "sandboxes", _]) => "delete",
        (&Method::GET, ["v1", "sandboxes", _, "stats"]) => "stats",
        (&Method::POST, ["v1", "sandboxes", _, "terminal-sessions"]) => "terminal_session_create",
        (&Method::POST, ["v1", "sandboxes", _, "process-sessions"]) => "process_session_create",
        (&Method::POST, ["v1", "sandboxes", _, "commands"]) => "command_create",
        (&Method::POST, ["v1", "sandboxes", _, "file-watch-sessions"]) => {
            "file_watch_session_create"
        }
        (&Method::POST, ["v1", "sandboxes", _, "port-sessions"]) => "port_session_create",
        (&Method::POST, ["v1", "sandboxes", _, "protocol-sessions"]) => "protocol_session_create",
        (&Method::GET, ["v1", "sandboxes", _, "processes"]) => "process_list",
        (&Method::GET, ["v1", "sandboxes", _, "processes", _]) => "process_get",
        (&Method::POST, ["v1", "sandboxes", _, "processes", action_segment]) => {
            native_process_action_metric(action_segment)
        }
        (&Method::GET, ["v1", "sandboxes", _, "ports"]) => "port_list",
        (&Method::POST, ["v1", "sandboxes", _, "ports", _, action]) => {
            native_port_action_metric(action)
        }
        (&Method::POST, ["v1", "sandboxes", _, file_segment])
            if file_segment.starts_with("files:") =>
        {
            native_file_metric(file_segment)
        }
        (&Method::GET, ["v1", "sandboxes", _, "sessions", _]) => "session_get",
        (&Method::POST, ["v1", "sandboxes", action_segment]) => {
            native_action_metric(action_segment)
        }
        _ => return None,
    };

    Some(SandboxApiMetricIdentity::native(operation))
}

fn native_file_metric(file_segment: &str) -> &'static str {
    match file_segment.rsplit_once(':').map(|(_, action)| action) {
        Some("stat") => "files_stat",
        Some("list") => "files_list",
        Some("read") => "files_read",
        Some("write") => "files_write",
        Some("write_batch") => "files_write_batch",
        Some("changes_enable") => "files_changes_enable",
        Some("changes_disable") => "files_changes_disable",
        Some("modified") => "files_modified",
        Some("mkdir") => "files_mkdir",
        Some("remove") => "files_remove",
        Some("rename") => "files_rename",
        _ => "files_action",
    }
}

fn native_process_action_metric(action_segment: &str) -> &'static str {
    match action_segment.rsplit_once(':').map(|(_, action)| action) {
        Some("signal") => "process_signal",
        Some("kill") => "process_kill",
        _ => "process_action",
    }
}

fn native_port_action_metric(action_segment: &str) -> &'static str {
    match action_segment {
        "open" => "port_open",
        "close" => "port_close",
        _ => "port_action",
    }
}

fn native_action_metric(action_segment: &str) -> &'static str {
    action_segment
        .rsplit_once(':')
        .and_then(|(_, action)| SandboxLifecycleAction::parse_known(action).ok())
        .filter(|action| *action != SandboxLifecycleAction::Delete)
        .map_or("action", |action| action.as_str())
}

fn ecw_route_metric(method: &Method, segments: &[&str]) -> Option<SandboxApiMetricIdentity> {
    let operation = match (method, segments) {
        (&Method::GET, ["sandboxes"]) | (&Method::GET, ["v2", "sandboxes"]) => "list",
        (&Method::POST, ["sandboxes"]) => "create",
        (&Method::GET, ["sandboxes", _]) => "get",
        (&Method::DELETE, ["sandboxes", _]) => "delete",
        (&Method::POST, ["sandboxes", _, "pause"]) => "pause",
        (&Method::POST, ["sandboxes", _, "resume"]) => "resume",
        (&Method::POST, ["sandboxes", _, "connect"]) => "connect",
        (&Method::POST, ["sandboxes", _, "timeout"]) => "timeout",
        (&Method::POST, ["sandboxes", _, "refreshes"]) => "refresh",
        (&Method::POST, ["sandboxes", _, "fork"]) => "fork",
        _ => return None,
    };

    Some(SandboxApiMetricIdentity::ecw(operation))
}

#[cfg(test)]
mod tests;
