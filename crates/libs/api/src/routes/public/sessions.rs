use pplx_space_common_core::types::{OrgId, UserId};

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, GuestUserName, IdempotencyKey, ProcessCommand, SandboxLifecycle,
    validate_linux_guest_path_input,
};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_public_api::{
    CreateCommandRequest, CreateFileWatchSessionRequest, CreatePortSessionRequest,
    CreateProcessRequest, CreateProtocolSessionRequest, CreateTerminalSessionRequest,
    DataPlaneSessionId, PageCursor, PublicCommand, PublicPage, PublicSandboxId, PublicSession,
    SandboxLifecycleAction, SessionKind, SessionTargetProtocol, TerminalSize,
    routes as native_routes,
};
use serde::Deserialize;
use time::OffsetDateTime;

use crate::{
    controllers::{ControllerError, substrate::ExecutionActivityKind},
    middleware::auth::{AuthOrg, AuthUser},
    models::Sandbox,
    public_operations::{HttpOperationMethod, OperationPathTemplate, PublicOperationStore},
    public_sandbox_operations::{SandboxLifecycleOperationPayload, implicit_wake_activity_enabled},
    public_sandbox_projection::public_lifecycle_state,
    public_sessions::{
        CreatePublicSession, CreatePublicSessionIdempotency, ListPublicSessions,
        PublicSessionCursor, PublicSessionStore, RevokePublicSession, session_fingerprint_payload,
    },
    routes::AppState,
};

use super::{
    cursors::{CursorKind, decode_cursor, encode_cursor},
    error::{PublicApiError, with_retry_after_hint},
    operations::accepts_event_stream,
    owner_scope::{AuthorizedSandbox, authorized_sandbox},
    sandboxes::parse_public_sandbox_id,
};

pub(crate) use super::sandboxes::idempotency_key;

const TERMINAL_SESSION_PATH_TEMPLATE: &str = native_routes::CREATE_TERMINAL_SESSION.path();
const PROCESS_SESSION_PATH_TEMPLATE: &str = native_routes::CREATE_PROCESS_SESSION.path();
const COMMAND_PATH_TEMPLATE: &str = native_routes::CREATE_COMMAND.path();
const FILE_WATCH_SESSION_PATH_TEMPLATE: &str = native_routes::CREATE_FILE_WATCH_SESSION.path();
const PORT_SESSION_PATH_TEMPLATE: &str = native_routes::CREATE_PORT_SESSION.path();
const PROTOCOL_SESSION_PATH_TEMPLATE: &str = native_routes::CREATE_PROTOCOL_SESSION.path();
const DEFAULT_SESSION_LIST_LIMIT: u16 = 100;
const MAX_SESSION_LIST_LIMIT: u16 = 1000;

pub async fn create_terminal_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<CreateTerminalSessionRequest>,
) -> Result<(StatusCode, Json<PublicSession>), PublicApiError> {
    validate_terminal_size(request.size.as_ref())?;
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let _sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let request_json = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize terminal session request");
        PublicApiError::internal()
    })?;
    let fingerprint_payload = session_fingerprint_payload(
        sandbox_id,
        SessionKind::Terminal,
        None,
        None,
        None,
        &request_json,
    );
    let session = create_session(
        &state,
        RouteSessionCreate {
            org_id: org.id,
            actor_user_id: actor.id,
            sandbox_id,
            kind: SessionKind::Terminal,
            target_port: None,
            target_protocol: None,
            target_path: None,
            request_json,
            fingerprint_payload,
            idempotency_key: idempotency_key(&headers)?,
            path_template: OperationPathTemplate::new(TERMINAL_SESSION_PATH_TEMPLATE),
        },
        || Ok(()),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(session)))
}

pub async fn create_process_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<CreateProcessRequest>,
) -> Result<(StatusCode, Json<PublicSession>), PublicApiError> {
    validate_process_request(&request)?;
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let lifecycle = sandbox_lifecycle(&sandbox)?;
    let admission = command_admission(lifecycle, &sandbox.auto_lifecycle.0);
    let request_json = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize process session request");
        PublicApiError::internal()
    })?;
    let fingerprint_payload = session_fingerprint_payload(
        sandbox_id,
        SessionKind::Process,
        None,
        None,
        None,
        &request_json,
    );
    let session = create_session(
        &state,
        RouteSessionCreate {
            org_id: org.id,
            actor_user_id: actor.id,
            sandbox_id,
            kind: SessionKind::Process,
            target_port: None,
            target_protocol: None,
            target_path: None,
            request_json,
            fingerprint_payload,
            idempotency_key: idempotency_key(&headers)?,
            path_template: OperationPathTemplate::new(PROCESS_SESSION_PATH_TEMPLATE),
        },
        || admit(admission, sandbox_id, lifecycle),
    )
    .await?;
    if admission == CommandAdmission::AdmitAndWake {
        enqueue_implicit_wake(
            &state.pool,
            org.id,
            sandbox_id,
            ExecutionActivityKind::Command,
            implicit_wake_activity_enabled(),
        )
        .await?;
    }

    Ok((StatusCode::CREATED, Json(session)))
}

pub async fn create_command(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<CreateCommandRequest>,
) -> Result<Response, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let idempotency_key = idempotency_key(&headers)?;
    let event_stream = accepts_event_stream(&headers);
    if event_stream {
        reject_streamed_pty(&request)?;
    }
    let lifecycle = sandbox_lifecycle(&sandbox)?;
    let admission = command_admission(lifecycle, &sandbox.auto_lifecycle.0);
    let command = create_command_session(
        &state.public_session_store(),
        org.id,
        actor.id,
        sandbox_id,
        request,
        idempotency_key,
        || admit(admission, sandbox_id, lifecycle),
    )
    .await?;
    if admission == CommandAdmission::AdmitAndWake {
        enqueue_implicit_wake(
            &state.pool,
            org.id,
            sandbox_id,
            ExecutionActivityKind::Command,
            implicit_wake_activity_enabled(),
        )
        .await?;
    }

    // Streaming is negotiated after the create, so validation, owner scope,
    // idempotency replay, and their errors are identical in both modes, apart
    // from the pty check above.
    if event_stream
        && let Some(proxy) = state.command_stream.as_ref()
        && let Some(response) = proxy.stream(&command).await
    {
        return Ok(response);
    }
    Ok((StatusCode::CREATED, Json(command)).into_response())
}

/// What `createCommand` and `createProcessSession` do for the sandbox's
/// current lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandAdmission {
    /// Running, or heading to running, which the attach waits for.
    Admit,
    /// At rest or heading there, and its wake policy lets a command wake it.
    AdmitAndWake,
    /// The command could not run until the caller wakes or starts the
    /// sandbox, so repeating the request cannot help.
    Refuse,
}

/// A command wakes a sandbox at rest, or one heading there, only as its
/// `auto_lifecycle.wake.on` allows (`command` or `access`), the rule the node
/// applies when the attach starts the process. Answering here keeps a refused
/// command from turning into an attach that reports a retryable error
/// indefinitely. Nothing starts a stopped sandbox for a command: the wake
/// action refuses `stopped`, and the node refuses guest access to it.
fn command_admission(
    lifecycle: SandboxLifecycle,
    policy: &AutoLifecyclePolicy,
) -> CommandAdmission {
    activity_admission(ExecutionActivityKind::Command, lifecycle, policy)
}

/// [`command_admission`] for a session that will do `activity` once the
/// sandbox runs: a reattach to a process that already started, or a file
/// watch, is access, which wakes only on `access`.
fn activity_admission(
    activity: ExecutionActivityKind,
    lifecycle: SandboxLifecycle,
    policy: &AutoLifecyclePolicy,
) -> CommandAdmission {
    let wakes = activity.allows_implicit_wake(policy);
    match lifecycle {
        // A checkpoint returns the sandbox to the state it left, and the
        // node's command wake waits it out; a paused result is refused there.
        SandboxLifecycle::Started
        | SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Checkpointing => CommandAdmission::Admit,
        SandboxLifecycle::Created
        | SandboxLifecycle::Paused
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Suspending => {
            if wakes {
                CommandAdmission::AdmitAndWake
            } else {
                CommandAdmission::Refuse
            }
        }
        SandboxLifecycle::Stopped
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Failed
        | SandboxLifecycle::Quarantined => CommandAdmission::Refuse,
    }
}

/// The refusal for a new command or process session the sandbox cannot run.
/// The session store calls it only for a request it has not seen, so a retry
/// of one that was created replays it whatever state the sandbox is in now.
fn admit(
    admission: CommandAdmission,
    sandbox_id: PublicSandboxId,
    lifecycle: SandboxLifecycle,
) -> Result<(), PublicApiError> {
    match admission {
        CommandAdmission::Admit | CommandAdmission::AdmitAndWake => Ok(()),
        CommandAdmission::Refuse => Err(PublicApiError::sandbox_not_running(
            sandbox_id,
            &public_lifecycle_state(lifecycle),
        )),
    }
}

fn sandbox_lifecycle(sandbox: &Sandbox) -> Result<SandboxLifecycle, PublicApiError> {
    sandbox.lifecycle.parse().map_err(|error| {
        tracing::error!(sandbox_id = %sandbox.id, %error, "sandbox has an unknown lifecycle");
        PublicApiError::internal()
    })
}

/// Queue the wake a new command or process session waits for, or join the
/// one already queued or running, so a sandbox at rest is placed again even
/// when no attach can reach it. The operation runs through the dispatcher
/// like a `:wake`, after any lifecycle operation queued before it, as the
/// system (no actor) rather than the caller, and only if the sandbox's wake
/// policy still allows `activity` to wake it when it runs. A sandbox still
/// pausing or suspending refuses the activation for now; the operation
/// retries until it settles.
///
/// With `record_activity` ([`implicit_wake_activity_enabled`]), a new wake
/// records `activity`, and the executor judges the policy by it; without,
/// the wake goes in as it did before the record, and runs by the command
/// rule. A join keeps the activity of the wake it joins, whatever this
/// caller's: each caller's activity was allowed when it asked, so the wake's
/// rule can refuse the joiner's only if the policy narrowed in between, as
/// from `access` to `command` under a wake for access that a new command
/// joins. That wake is refused when it runs; the joiner's next attach finds
/// nothing coming and queues a fresh wake for its own activity.
///
/// Called for a replayed session too, so a retry heals a wake that failed to
/// queue after its session was created.
async fn enqueue_implicit_wake(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
    activity: ExecutionActivityKind,
    record_activity: bool,
) -> Result<(), PublicApiError> {
    let payload = SandboxLifecycleOperationPayload {
        sandbox_id,
        action: SandboxLifecycleAction::Wake,
        rollback: None,
        wake_activity: record_activity.then_some(activity.into()),
    }
    .into_json();
    PublicOperationStore::new(pool.clone())
        .create_or_join_active_sandbox_wake(
            org_id,
            &sandbox_id.to_string(),
            payload,
            crate::public_operations::request_traceparent(),
        )
        .await?;
    Ok(())
}

/// Queue, or join, the wake an existing session on `sandbox` waits for, when
/// the sandbox is at rest and its wake policy lets the session's `activity`
/// wake it, as the node will judge that activity. For the session route
/// resolver, whose attach found the sandbox with no node: answers whether
/// the attach should keep waiting. A sandbox that is running or heading there
/// again since the resolver looked needs no wake and keeps waiting too, as
/// does one a start, resume, restore or wake queued since then is about to
/// place; only one nothing can wake for `activity` answers no. A new wake
/// records `activity` as [`enqueue_implicit_wake`] says for
/// `record_activity`.
pub(crate) async fn queue_implicit_wake(
    pool: &sqlx::PgPool,
    sandbox: &Sandbox,
    activity: ExecutionActivityKind,
    record_activity: bool,
) -> Result<bool, PublicApiError> {
    let sandbox_id = PublicSandboxId::from(sandbox.id);
    let mut admission = activity_admission(
        activity,
        sandbox_lifecycle(sandbox)?,
        &sandbox.auto_lifecycle.0,
    );
    if admission == CommandAdmission::Refuse {
        // Read the sandbox again before answering no, with its pending
        // activations in the same statement. A start, resume, restore or
        // wake queued since the first read is about to place it. One that
        // completed, or got under way, since then already has: the lifecycle
        // read now is one the attach is admitted in. A policy updated since
        // then is judged as it is now, so one that has come to allow
        // `activity` wakes the sandbox.
        let observed = pplx_space_sandbox_store::activation_observation(
            pool,
            sandbox.org_id,
            sandbox.id,
            &sandbox_id.to_string(),
        )
        .await
        .map_err(|error| {
            tracing::warn!(sandbox_id = %sandbox.id, %error, "could not read pending activations");
            PublicApiError::internal()
        })?;
        let Some(observed) = observed else {
            return Ok(false);
        };
        if observed.activation_pending {
            return Ok(true);
        }
        let Ok(lifecycle) = observed.lifecycle.parse::<SandboxLifecycle>() else {
            return Ok(false);
        };
        admission = activity_admission(activity, lifecycle, &observed.auto_lifecycle.0);
    }
    match admission {
        CommandAdmission::Admit => Ok(true),
        CommandAdmission::Refuse => Ok(false),
        CommandAdmission::AdmitAndWake => {
            enqueue_implicit_wake(pool, sandbox.org_id, sandbox_id, activity, record_activity)
                .await?;
            Ok(true)
        }
    }
}

/// Create a command (one-shot process) data-plane session for `sandbox_id`,
/// scoped to `org_id` and attributed to `actor_user_id`.
///
/// The org scope is a parameter rather than the caller's authenticated org so
/// the operator (admin) surface can run a command against a sandbox owned by
/// any org by deriving `org_id` from the sandbox row — the same way the admin
/// lifecycle dispatch derives its operation org from the sandbox. The public
/// surface passes its own authenticated org after an ownership check, so this
/// helper never weakens the public org boundary. The returned attach token is
/// session-scoped (independent of `org_id`), so the data-plane stream that the
/// caller connects to authenticates on the token alone.
pub(crate) async fn create_command_session(
    store: &PublicSessionStore,
    org_id: OrgId,
    actor_user_id: UserId,
    sandbox_id: PublicSandboxId,
    request: CreateCommandRequest,
    idempotency_key: IdempotencyKey,
    admit: impl FnOnce() -> Result<(), PublicApiError> + Send,
) -> Result<PublicCommand, PublicApiError> {
    let request = CreateProcessRequest::from(request);
    validate_process_request(&request)?;
    let request_json = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize command request");
        PublicApiError::internal()
    })?;
    let fingerprint_payload = session_fingerprint_payload(
        sandbox_id,
        SessionKind::Process,
        None,
        None,
        None,
        &request_json,
    );
    let session = store
        .create_session(
            CreatePublicSession {
                org_id,
                actor_user_id: Some(actor_user_id),
                sandbox_id,
                kind: SessionKind::Process,
                target_port: None,
                target_protocol: None,
                target_path: None,
                adapter_state: request_json,
                fingerprint_payload: &fingerprint_payload,
                idempotency: CreatePublicSessionIdempotency {
                    method: HttpOperationMethod::Post,
                    path_template: OperationPathTemplate::new(COMMAND_PATH_TEMPLATE),
                    key: &idempotency_key,
                },
            },
            admit,
        )
        .await
        .map_err(PublicApiError::from)??;

    Ok(PublicCommand::from(session))
}

pub async fn create_file_watch_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<CreateFileWatchSessionRequest>,
) -> Result<(StatusCode, Json<PublicSession>), PublicApiError> {
    if request.path.is_empty() {
        return Err(PublicApiError::bad_request("path must not be empty"));
    }
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let _sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let request_json = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize file watch session request");
        PublicApiError::internal()
    })?;
    let fingerprint_payload = session_fingerprint_payload(
        sandbox_id,
        SessionKind::FileWatch,
        None,
        None,
        Some(&request.path),
        &request_json,
    );
    let session = create_session(
        &state,
        RouteSessionCreate {
            org_id: org.id,
            actor_user_id: actor.id,
            sandbox_id,
            kind: SessionKind::FileWatch,
            target_port: None,
            target_protocol: None,
            target_path: Some(&request.path),
            request_json,
            fingerprint_payload,
            idempotency_key: idempotency_key(&headers)?,
            path_template: OperationPathTemplate::new(FILE_WATCH_SESSION_PATH_TEMPLATE),
        },
        || Ok(()),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(session)))
}

pub async fn create_port_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<CreatePortSessionRequest>,
) -> Result<(StatusCode, Json<PublicSession>), PublicApiError> {
    if request.port == 0 {
        return Err(PublicApiError::bad_request(
            "port must be between 1 and 65535",
        ));
    }
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    state
        .sandbox_controller
        .ensure_sandbox_port_exposure_applied(&state.pool, &sandbox, request.port, "native")
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                port = request.port,
                %error,
                "failed to apply public port session exposure"
            );
            controller_error(error)
        })?;
    let protocol = SessionTargetProtocol::from(request.protocol);
    let request_json = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize port session request");
        PublicApiError::internal()
    })?;
    let fingerprint_payload = session_fingerprint_payload(
        sandbox_id,
        SessionKind::Port,
        Some(request.port),
        Some(protocol),
        None,
        &request_json,
    );
    let session = create_session(
        &state,
        RouteSessionCreate {
            org_id: org.id,
            actor_user_id: actor.id,
            sandbox_id,
            kind: SessionKind::Port,
            target_port: Some(request.port),
            target_protocol: Some(protocol),
            target_path: None,
            request_json,
            fingerprint_payload,
            idempotency_key: idempotency_key(&headers)?,
            path_template: OperationPathTemplate::new(PORT_SESSION_PATH_TEMPLATE),
        },
        || Ok(()),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(session)))
}

pub async fn create_protocol_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<CreateProtocolSessionRequest>,
) -> Result<(StatusCode, Json<PublicSession>), PublicApiError> {
    if request.port == 0 {
        return Err(PublicApiError::bad_request(
            "port must be between 1 and 65535",
        ));
    }
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    state
        .sandbox_controller
        .ensure_sandbox_port_exposure_applied(&state.pool, &sandbox, request.port, "native")
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                port = request.port,
                protocol = request.protocol.as_str(),
                %error,
                "failed to apply public protocol session exposure"
            );
            controller_error(error)
        })?;
    let protocol = SessionTargetProtocol::from(request.protocol);
    let request_json = serde_json::to_value(&request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize protocol session request");
        PublicApiError::internal()
    })?;
    let fingerprint_payload = session_fingerprint_payload(
        sandbox_id,
        SessionKind::Protocol,
        Some(request.port),
        Some(protocol),
        None,
        &request_json,
    );
    let session = create_session(
        &state,
        RouteSessionCreate {
            org_id: org.id,
            actor_user_id: actor.id,
            sandbox_id,
            kind: SessionKind::Protocol,
            target_port: Some(request.port),
            target_protocol: Some(protocol),
            target_path: None,
            request_json,
            fingerprint_payload,
            idempotency_key: idempotency_key(&headers)?,
            path_template: OperationPathTemplate::new(PROTOCOL_SESSION_PATH_TEMPLATE),
        },
        || Ok(()),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(session)))
}

pub async fn get_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path((sandbox_id, session_id)): Path<(String, String)>,
) -> Result<Json<PublicSession>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let _sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let session_id = parse_session_id(&session_id)?;
    let store = state.public_session_store();
    let Some(session) = store.fetch_session(org.id, sandbox_id, session_id).await? else {
        return Err(PublicApiError::not_found("session not found"));
    };
    Ok(Json(session))
}

pub async fn list_sessions(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
    Query(query): Query<ListSessionsQuery>,
) -> Result<Json<PublicPage<PublicSession>>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let _sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let limit = query.limit();
    let store = state.public_session_store();
    let mut sessions = store
        .list_sessions(ListPublicSessions {
            org_id: org.id,
            sandbox_id,
            kind: query.kind()?,
            cursor: query.cursor()?,
            limit,
        })
        .await?;

    let has_more = sessions.len() > usize::from(limit);
    if has_more {
        sessions.truncate(usize::from(limit));
    }
    let next_cursor = if has_more {
        sessions
            .last()
            .map(|session| session_cursor(session.created_at, session.id))
            .transpose()?
    } else {
        None
    };

    Ok(Json(PublicPage {
        data: sessions,
        next_cursor,
    }))
}

pub async fn revoke_session(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path((sandbox_id, session_id)): Path<(String, String)>,
) -> Result<Json<PublicSession>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let _sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, sandbox_id).await?;
    let session_id = parse_session_id(&session_id)?;
    let store = state.public_session_store();
    let Some(session) = store
        .revoke_session(RevokePublicSession {
            org_id: org.id,
            sandbox_id,
            session_id,
            reason: "user_requested",
        })
        .await?
    else {
        return Err(PublicApiError::not_found("session not found"));
    };
    Ok(Json(session))
}

#[derive(Debug, Deserialize)]
pub struct ListSessionsQuery {
    cursor: Option<String>,
    limit: Option<u16>,
    kind: Option<String>,
}

impl ListSessionsQuery {
    fn cursor(&self) -> Result<Option<PublicSessionCursor>, PublicApiError> {
        let Some(cursor) = self.cursor.as_deref() else {
            return Ok(None);
        };
        let cursor = decode_cursor(CursorKind::Session, cursor)?;
        parse_session_cursor(&cursor).map(Some)
    }

    fn kind(&self) -> Result<Option<SessionKind>, PublicApiError> {
        self.kind
            .as_deref()
            .map(|kind| {
                SessionKind::parse_known(kind)
                    .map_err(|_| PublicApiError::bad_request("invalid session kind"))
            })
            .transpose()
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_SESSION_LIST_LIMIT,
            Some(limit) if limit > MAX_SESSION_LIST_LIMIT => MAX_SESSION_LIST_LIMIT,
            Some(limit) => limit,
        }
    }
}

async fn create_session(
    state: &AppState,
    input: RouteSessionCreate<'_>,
    admit: impl FnOnce() -> Result<(), PublicApiError> + Send,
) -> Result<PublicSession, PublicApiError> {
    let store = state.public_session_store();
    store
        .create_session(
            CreatePublicSession {
                org_id: input.org_id,
                actor_user_id: Some(input.actor_user_id),
                sandbox_id: input.sandbox_id,
                kind: input.kind,
                target_port: input.target_port,
                target_protocol: input.target_protocol,
                target_path: input.target_path,
                adapter_state: input.request_json,
                fingerprint_payload: &input.fingerprint_payload,
                idempotency: CreatePublicSessionIdempotency {
                    method: HttpOperationMethod::Post,
                    path_template: input.path_template,
                    key: &input.idempotency_key,
                },
            },
            admit,
        )
        .await
        .map_err(PublicApiError::from)?
}

struct RouteSessionCreate<'a> {
    org_id: OrgId,
    actor_user_id: UserId,
    sandbox_id: PublicSandboxId,
    kind: SessionKind,
    target_port: Option<u16>,
    target_protocol: Option<SessionTargetProtocol>,
    target_path: Option<&'a str>,
    request_json: serde_json::Value,
    fingerprint_payload: serde_json::Value,
    idempotency_key: IdempotencyKey,
    path_template: OperationPathTemplate,
}

async fn ensure_sandbox_owner(
    authorized: Option<Extension<AuthorizedSandbox>>,
    pool: &sqlx::PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
) -> Result<Sandbox, PublicApiError> {
    authorized_sandbox(authorized, pool, org_id, sandbox_id).await
}

fn validate_terminal_size(size: Option<&TerminalSize>) -> Result<(), PublicApiError> {
    let Some(size) = size else {
        return Ok(());
    };
    if size.rows == 0 || size.cols == 0 {
        return Err(PublicApiError::bad_request(
            "terminal size rows and cols must be non-zero",
        ));
    }
    Ok(())
}

/// Server-sent events carry no input, so a streamed command with a pty would
/// wait on terminal input that never arrives. Unlike stdin, which the stream
/// starts at EOF, a pty cannot be closed. Checked before the create,
/// whether or not the stream then opens, so nothing is persisted.
fn reject_streamed_pty(request: &CreateCommandRequest) -> Result<(), PublicApiError> {
    if request.pty.is_none() {
        return Ok(());
    }
    Err(PublicApiError::invalid_pty_field(
        "an event-stream command cannot use pty; create it without one, \
         or attach over WebSocket",
    ))
}

fn validate_process_request(request: &CreateProcessRequest) -> Result<(), PublicApiError> {
    ProcessCommand::new(request.command.clone())
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    if let Some(cwd) = request.cwd.as_deref() {
        if cwd.is_empty() {
            return Err(PublicApiError::bad_request("guest path must be non-empty"));
        }
        validate_linux_guest_path_input(cwd)
            .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    }
    if let Some(user) = request.user.clone() {
        GuestUserName::parse(user)
            .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    }
    validate_terminal_size(request.pty.as_ref())
}

fn parse_session_id(value: &str) -> Result<DataPlaneSessionId, PublicApiError> {
    value
        .parse()
        .map_err(|_| PublicApiError::bad_request("invalid session id"))
}

pub(in crate::routes::public) fn controller_error(error: ControllerError) -> PublicApiError {
    // A lock race is this service's own database, not the sandbox fleet. The
    // status match below cannot tell it from an out-of-capacity 503, so it
    // would attribute it to the cluster or the node.
    if error.is_lock_contention() {
        return PublicApiError::lock_contention();
    }
    let retry_after = error.retry_after_header();
    match error.status() {
        StatusCode::BAD_REQUEST => PublicApiError::bad_request(error.to_string()),
        StatusCode::FORBIDDEN => PublicApiError::forbidden(error.to_string()),
        StatusCode::NOT_FOUND => PublicApiError::not_found(error.to_string()),
        StatusCode::CONFLICT => PublicApiError::conflict(error.to_string()),
        StatusCode::PRECONDITION_FAILED => PublicApiError::failed_precondition(error.to_string()),
        StatusCode::TOO_MANY_REQUESTS => with_retry_after_hint(
            PublicApiError::rate_limited(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::Admission),
            retry_after,
        ),
        StatusCode::SERVICE_UNAVAILABLE => with_retry_after_hint(
            PublicApiError::service_unavailable(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus),
            retry_after,
        ),
        status if status.is_server_error() => {
            tracing::warn!(%error, "native session operation failed");
            PublicApiError::internal()
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus)
        }
        _ => PublicApiError::internal(),
    }
}

fn session_cursor(
    created_at: OffsetDateTime,
    session_id: DataPlaneSessionId,
) -> Result<PageCursor, PublicApiError> {
    encode_cursor(
        CursorKind::Session,
        format!("{}:{session_id}", created_at.unix_timestamp_nanos()),
    )
}

fn parse_session_cursor(value: &str) -> Result<PublicSessionCursor, PublicApiError> {
    let Some((created_at, session_id)) = value.split_once(':') else {
        return Err(PublicApiError::bad_request("invalid cursor"));
    };
    let created_at = created_at
        .parse::<i128>()
        .ok()
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .ok_or_else(|| PublicApiError::bad_request("invalid cursor"))?;
    let session_id = session_id
        .parse::<DataPlaneSessionId>()
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    Ok(PublicSessionCursor {
        created_at,
        session_id,
    })
}

#[cfg(test)]
mod tests;
