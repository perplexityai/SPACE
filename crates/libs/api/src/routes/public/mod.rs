use axum::{
    Router,
    extract::DefaultBodyLimit,
    handler::Handler,
    middleware,
    routing::{MethodFilter, MethodRouter, on},
};
use pplx_space_public_api::routes::{self as native, NativeHttpMethod, NativeOperation};
use std::collections::BTreeSet;

use super::AppState;
use crate::middleware::auth::require_native_auth;

mod credential_references;
mod cursors;
pub(crate) mod error;
mod files;
pub(crate) use files::{
    MAX_FILE_WRITE_BATCH_BYTES, WRITE_FILE_BATCH_BODY_LIMIT_BYTES, WRITE_FILE_BODY_LIMIT_BYTES,
};
pub(crate) mod network_policy;
mod operations;
mod owner_scope;
mod ports;
mod processes;
pub(crate) mod request_context;
pub(crate) mod sandboxes;
pub(crate) mod sessions;
mod snapshots;
mod templates;
mod timeout;

fn method_filter(method: NativeHttpMethod) -> MethodFilter {
    match method {
        NativeHttpMethod::Delete => MethodFilter::DELETE,
        NativeHttpMethod::Get => MethodFilter::GET.or(MethodFilter::HEAD),
        NativeHttpMethod::Patch => MethodFilter::PATCH,
        NativeHttpMethod::Post => MethodFilter::POST,
        NativeHttpMethod::Put => MethodFilter::PUT,
    }
}

pub(crate) fn validate_public_cursor_secret() -> Result<(), cursors::PublicCursorConfigError> {
    cursors::validate_public_cursor_secret()
}

fn mount<H, T>(operation: NativeOperation, handler: H) -> (NativeOperation, MethodRouter<AppState>)
where
    H: Handler<T, AppState>,
    T: 'static,
{
    (operation, on(method_filter(operation.method()), handler))
}

/// [`mount`] with a request body limit other than the framework's 2 MiB
/// default, for an operation whose documented payload is larger.
fn mount_with_body_limit<H, T>(
    operation: NativeOperation,
    handler: H,
    limit_bytes: usize,
) -> (NativeOperation, MethodRouter<AppState>)
where
    H: Handler<T, AppState>,
    T: 'static,
{
    let (operation, router) = mount(operation, handler);
    (operation, router.layer(DefaultBodyLimit::max(limit_bytes)))
}

/// Every native control-plane operation and the handler that serves it.
///
/// [`v1_router`] mounts exactly this table, so `every_control_plane_operation_is_mounted`
/// can assert manifest-to-router parity against it. Custom `:action` operations
/// share one mount with their siblings: they declare a common runtime path, the
/// first entry for a `(path, method)` pair installs the handler, and the rest are
/// listed so that dropping the entry that happens to be mounted first cannot
/// silently unmount the whole group.
fn mounted_operations() -> Vec<(NativeOperation, MethodRouter<AppState>)> {
    vec![
        mount(
            native::REGISTER_CREDENTIAL_REFERENCE,
            credential_references::register,
        ),
        mount(
            native::DELETE_CREDENTIAL_REFERENCE,
            credential_references::retire,
        ),
        mount(native::LIST_TEMPLATES, templates::list_templates),
        mount(native::GET_TEMPLATE, templates::get_template),
        mount(native::CREATE_SANDBOX, sandboxes::create_sandbox),
        mount(native::LIST_SANDBOXES, sandboxes::list_sandboxes),
        mount(native::GET_SANDBOX, sandboxes::get_sandbox),
        mount(native::UPDATE_SANDBOX, sandboxes::update_sandbox),
        mount(native::DELETE_SANDBOX, sandboxes::delete_sandbox),
        mount(
            native::START_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(native::STOP_SANDBOX, sandboxes::apply_custom_sandbox_action),
        mount(
            native::PAUSE_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(
            native::RESUME_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(
            native::SUSPEND_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(
            native::RESTORE_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(native::WAKE_SANDBOX, sandboxes::apply_custom_sandbox_action),
        mount(
            native::SET_SANDBOX_TIMEOUT,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(native::KILL_SANDBOX, sandboxes::apply_custom_sandbox_action),
        mount(
            native::CHECKPOINT_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(
            native::ENABLE_SANDBOX_SNAPSHOTS,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(
            native::DISABLE_SANDBOX_SNAPSHOTS,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(
            native::ROLLBACK_SANDBOX,
            sandboxes::apply_custom_sandbox_action,
        ),
        mount(native::GET_SANDBOX_STATS, sandboxes::get_sandbox_stats),
        mount(
            native::STREAM_SANDBOX_METRICS,
            sandboxes::stream_sandbox_metrics,
        ),
        mount(
            native::GET_SANDBOX_NETWORK_POLICY,
            network_policy::get_sandbox_network_policy,
        ),
        mount(
            native::CREATE_TERMINAL_SESSION,
            sessions::create_terminal_session,
        ),
        mount(
            native::CREATE_PROCESS_SESSION,
            sessions::create_process_session,
        ),
        mount(native::CREATE_COMMAND, sessions::create_command),
        mount(
            native::CREATE_FILE_WATCH_SESSION,
            sessions::create_file_watch_session,
        ),
        mount(native::CREATE_PORT_SESSION, sessions::create_port_session),
        mount(
            native::CREATE_PROTOCOL_SESSION,
            sessions::create_protocol_session,
        ),
        mount(native::LIST_PROCESSES, processes::list_processes),
        mount(native::GET_PROCESS, processes::get_process),
        mount(native::SIGNAL_PROCESS, processes::apply_process_action),
        mount(native::KILL_PROCESS, processes::apply_process_action),
        mount(native::LIST_PORTS, ports::list_ports),
        mount(native::OPEN_PORT, ports::open_port),
        mount(native::CLOSE_PORT, ports::close_port),
        mount(native::LIST_SNAPSHOTS, snapshots::list_snapshots),
        mount(native::GET_SNAPSHOT, snapshots::get_snapshot),
        mount(native::STAT_FILE, files::stat_file),
        mount(native::LIST_FILES, files::list_files),
        mount(native::READ_FILE, files::read_file),
        mount_with_body_limit(
            native::WRITE_FILE,
            files::write_file,
            files::WRITE_FILE_BODY_LIMIT_BYTES,
        ),
        mount_with_body_limit(
            native::WRITE_FILE_BATCH,
            files::write_file_batch,
            files::WRITE_FILE_BATCH_BODY_LIMIT_BYTES,
        ),
        mount(
            native::ENABLE_FILE_CHANGES,
            files::enable_file_change_tracking,
        ),
        mount(
            native::DISABLE_FILE_CHANGES,
            files::disable_file_change_tracking,
        ),
        mount(native::LIST_MODIFIED_FILES, files::modified_files),
        mount(native::CREATE_DIRECTORY, files::mkdir_file),
        mount(native::REMOVE_FILE, files::remove_file),
        mount(native::RENAME_FILE, files::rename_file),
        mount(native::GET_SESSION, sessions::get_session),
        mount(native::REVOKE_SESSION, sessions::revoke_session),
        mount(native::LIST_SESSIONS, sessions::list_sessions),
        mount(
            native::STREAM_SANDBOX_LIFECYCLE_EVENTS,
            operations::stream_sandbox_lifecycle_events,
        ),
        mount(native::GET_OPERATION, operations::get_operation),
        mount(
            native::LIST_OPERATION_EVENTS,
            operations::list_operation_events,
        ),
    ]
}

pub fn v1_router(state: AppState) -> Router<AppState> {
    let mut mounted = BTreeSet::new();
    let mut router = Router::new();
    for (operation, method_router) in mounted_operations() {
        let path = operation.relative_runtime_path();
        if mounted.insert((path, operation.method())) {
            router = router.route(path, method_router);
        }
    }
    router
        .fallback(error::public_route_not_found)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            owner_scope::authorize,
        ))
        .layer(middleware::from_fn_with_state(state, require_native_auth))
        .layer(middleware::from_fn(error::normalize_public_error_response))
        .layer(middleware::from_fn(request_context::with_public_request_id))
}

#[cfg(test)]
mod tests;
