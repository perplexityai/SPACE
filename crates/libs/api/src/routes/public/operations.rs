use pplx_space_common_core::types::OrgId;

use std::{collections::VecDeque, convert::Infallible, str::FromStr, time::Duration};

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use pplx_space_public_api::{
    OperationId, OperationState, PublicErrorCode, PublicOperationEvent, PublicOperationEventType,
    PublicOperationRecord, PublicPage, PublicSandboxId,
};
use serde::Deserialize;

use crate::{
    controllers::sandbox::fetch_sandbox,
    middleware::auth::AuthOrg,
    public_operations::{
        PublicOperationStore, PublicOperationStoreError, SandboxOperationEventCursor,
    },
    routes::AppState,
};

use super::{
    cursors::{CursorKind, decode_cursor, encode_cursor},
    error::PublicApiError,
};

const DEFAULT_OPERATION_EVENT_LIMIT: u16 = 100;
const MAX_OPERATION_EVENT_LIMIT: u16 = 1000;
const OPERATION_EVENT_STREAM_BATCH_LIMIT: i64 = 100;
const OPERATION_EVENT_STREAM_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub async fn get_operation(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(operation_id): Path<String>,
) -> Result<Json<PublicOperationRecord>, PublicApiError> {
    let operation_id = parse_operation_id(&operation_id)?;
    let store = PublicOperationStore::new(state.pool.clone());
    let Some(operation) = store.fetch_operation(org.id, operation_id).await? else {
        return Err(PublicApiError::not_found("operation not found"));
    };

    Ok(Json(operation))
}

pub async fn list_operation_events(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(operation_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ListOperationEventsQuery>,
) -> Result<Response, PublicApiError> {
    let operation_id = parse_operation_id(&operation_id)?;
    let after_sequence = query.after_sequence(&headers)?;

    let store = PublicOperationStore::new(state.pool.clone());
    if accepts_event_stream(&headers) {
        let Some(operation) = store.fetch_operation(org.id, operation_id).await? else {
            return Err(PublicApiError::not_found("operation not found"));
        };
        let initial_events = store
            .fetch_operation_events(
                org.id,
                operation_id,
                after_sequence,
                OPERATION_EVENT_STREAM_BATCH_LIMIT,
            )
            .await?
            .unwrap_or_default();
        let stream = operation_event_stream(
            store,
            org.id,
            operation_id,
            operation_state_is_terminal(operation.state),
            initial_events,
            after_sequence,
        );
        return Ok(Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response());
    }

    let limit = query.limit();
    let Some(mut events) = store
        .fetch_operation_events(org.id, operation_id, after_sequence, i64::from(limit) + 1)
        .await?
    else {
        return Err(PublicApiError::not_found("operation not found"));
    };

    Ok(Json(operation_event_page(std::mem::take(&mut events), limit)?).into_response())
}

pub async fn stream_sandbox_lifecycle_events(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(sandbox_id): Path<String>,
) -> Result<Response, PublicApiError> {
    let sandbox_id = sandbox_id
        .parse::<PublicSandboxId>()
        .map_err(|_| PublicApiError::bad_request("invalid sandbox id"))?;
    let Some(sandbox) = fetch_sandbox(&state.pool, sandbox_id.into()).await? else {
        return Err(PublicApiError::not_found("sandbox not found"));
    };
    if sandbox.org_id != org.id {
        return Err(PublicApiError::not_found("sandbox not found"));
    }

    let store = PublicOperationStore::new(state.pool.clone());
    let initial_events = store
        .fetch_sandbox_operation_events(
            org.id,
            &sandbox_id.to_string(),
            None,
            OPERATION_EVENT_STREAM_BATCH_LIMIT,
        )
        .await?;
    let stream =
        sandbox_lifecycle_event_stream(store, org.id, sandbox_id.to_string(), initial_events);

    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

#[derive(Debug, Deserialize)]
pub struct ListOperationEventsQuery {
    cursor: Option<String>,
    limit: Option<u16>,
}

impl ListOperationEventsQuery {
    fn after_sequence(&self, headers: &HeaderMap) -> Result<Option<i64>, PublicApiError> {
        if let Some(cursor) = self.cursor.as_deref() {
            return parse_sequence_cursor(cursor).map(Some);
        }
        headers
            .get("last-event-id")
            .map(parse_last_event_id)
            .transpose()
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_OPERATION_EVENT_LIMIT,
            Some(limit) if limit > MAX_OPERATION_EVENT_LIMIT => MAX_OPERATION_EVENT_LIMIT,
            Some(limit) => limit,
        }
    }
}

fn parse_sequence_cursor(value: &str) -> Result<i64, PublicApiError> {
    let cursor = decode_cursor(CursorKind::OperationEvent, value)
        .map_err(|_| PublicApiError::bad_request("invalid operation event cursor"))?;
    let sequence = cursor
        .parse::<i64>()
        .map_err(|_| PublicApiError::bad_request("invalid operation event cursor"))?;
    if sequence < 0 {
        return Err(PublicApiError::bad_request(
            "operation event cursor must not be negative",
        ));
    }
    Ok(sequence)
}

fn parse_last_event_id(value: &axum::http::HeaderValue) -> Result<i64, PublicApiError> {
    let value = value
        .to_str()
        .map_err(|_| PublicApiError::bad_request("invalid Last-Event-ID"))?;
    let sequence = value
        .parse::<i64>()
        .map_err(|_| PublicApiError::bad_request("invalid Last-Event-ID"))?;
    if sequence < 0 {
        return Err(PublicApiError::bad_request(
            "Last-Event-ID must not be negative",
        ));
    }
    Ok(sequence)
}

fn accepts_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|part| {
                part.trim()
                    .split(';')
                    .next()
                    .is_some_and(|media_type| media_type.trim() == "text/event-stream")
            })
        })
}

struct OperationEventStreamState {
    store: PublicOperationStore,
    org_id: OrgId,
    operation_id: OperationId,
    after_sequence: Option<i64>,
    pending: VecDeque<PublicOperationEvent>,
    terminal: bool,
    sleep_before_fetch: bool,
}

fn operation_event_stream(
    store: PublicOperationStore,
    org_id: OrgId,
    operation_id: OperationId,
    operation_terminal: bool,
    initial_events: Vec<PublicOperationEvent>,
    after_sequence: Option<i64>,
) -> impl futures_core::Stream<Item = Result<Event, Infallible>> {
    let initial_len = initial_events.len();
    let state = OperationEventStreamState {
        store,
        org_id,
        operation_id,
        after_sequence,
        pending: VecDeque::from(initial_events),
        terminal: operation_terminal && initial_len == 0,
        sleep_before_fetch: initial_len < OPERATION_EVENT_STREAM_BATCH_LIMIT as usize,
    };

    futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                state.after_sequence = Some(event.sequence);
                if operation_event_is_terminal(&event) {
                    state.terminal = true;
                }
                return Some((Ok(operation_event_sse(&event)), state));
            }
            if state.terminal {
                return None;
            }
            if state.sleep_before_fetch {
                tokio::time::sleep(OPERATION_EVENT_STREAM_POLL_INTERVAL).await;
            }
            match state
                .store
                .fetch_operation_events(
                    state.org_id,
                    state.operation_id,
                    state.after_sequence,
                    OPERATION_EVENT_STREAM_BATCH_LIMIT,
                )
                .await
            {
                Ok(Some(events)) => {
                    if events.is_empty() {
                        match operation_has_terminal_state(
                            &state.store,
                            state.org_id,
                            state.operation_id,
                        )
                        .await
                        {
                            Ok(true) => state.terminal = true,
                            Ok(false) => {}
                            Err(error) => {
                                tracing::error!(
                                    error = %error,
                                    "failed to fetch public operation state while streaming events"
                                );
                                state.terminal = true;
                                return Some((
                                    Ok(operation_error_sse(
                                        PublicErrorCode::ServiceUnavailable,
                                        "operation event stream failed",
                                        true,
                                    )),
                                    state,
                                ));
                            }
                        }
                    }
                    state.sleep_before_fetch =
                        events.len() < OPERATION_EVENT_STREAM_BATCH_LIMIT as usize;
                    state.pending = VecDeque::from(events);
                }
                Ok(None) => {
                    state.terminal = true;
                    return Some((
                        Ok(operation_error_sse(
                            PublicErrorCode::NotFound,
                            "operation not found",
                            false,
                        )),
                        state,
                    ));
                }
                Err(error) => {
                    tracing::error!(error = %error, "failed to stream public operation events");
                    state.terminal = true;
                    return Some((
                        Ok(operation_error_sse(
                            PublicErrorCode::ServiceUnavailable,
                            "operation event stream failed",
                            true,
                        )),
                        state,
                    ));
                }
            }
        }
    })
}

async fn operation_has_terminal_state(
    store: &PublicOperationStore,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<bool, PublicOperationStoreError> {
    Ok(store
        .fetch_operation(org_id, operation_id)
        .await?
        .is_none_or(|operation| operation_state_is_terminal(operation.state)))
}

struct SandboxLifecycleEventStreamState {
    store: PublicOperationStore,
    org_id: OrgId,
    sandbox_id: String,
    cursor: Option<SandboxOperationEventCursor>,
    pending: VecDeque<PublicOperationEvent>,
    sleep_before_fetch: bool,
}

fn sandbox_lifecycle_event_stream(
    store: PublicOperationStore,
    org_id: OrgId,
    sandbox_id: String,
    initial_events: Vec<PublicOperationEvent>,
) -> impl futures_core::Stream<Item = Result<Event, Infallible>> {
    let initial_len = initial_events.len();
    let state = SandboxLifecycleEventStreamState {
        store,
        org_id,
        sandbox_id,
        cursor: None,
        pending: VecDeque::from(initial_events),
        sleep_before_fetch: initial_len < OPERATION_EVENT_STREAM_BATCH_LIMIT as usize,
    };

    futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                state.cursor = Some(SandboxOperationEventCursor {
                    created_at: event.created_at,
                    operation_id: event.operation_id,
                    sequence: event.sequence,
                });
                return Some((Ok(operation_event_sse(&event)), state));
            }
            if state.sleep_before_fetch {
                tokio::time::sleep(OPERATION_EVENT_STREAM_POLL_INTERVAL).await;
            }
            match state
                .store
                .fetch_sandbox_operation_events(
                    state.org_id,
                    &state.sandbox_id,
                    state.cursor.as_ref(),
                    OPERATION_EVENT_STREAM_BATCH_LIMIT,
                )
                .await
            {
                Ok(events) => {
                    state.sleep_before_fetch =
                        events.len() < OPERATION_EVENT_STREAM_BATCH_LIMIT as usize;
                    state.pending = VecDeque::from(events);
                }
                Err(error) => {
                    tracing::error!(error = %error, "failed to stream sandbox lifecycle events");
                    return Some((
                        Ok(operation_error_sse(
                            PublicErrorCode::ServiceUnavailable,
                            "lifecycle event stream failed",
                            true,
                        )),
                        state,
                    ));
                }
            }
        }
    })
}

fn operation_event_sse(event: &PublicOperationEvent) -> Event {
    Event::default()
        .id(event.sequence.to_string())
        .event(event.event_type.as_str())
        .data(serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string()))
}

fn operation_error_sse(code: PublicErrorCode, message: &str, retryable: bool) -> Event {
    Event::default()
        .event(PublicOperationEventType::Error.as_str())
        .data(
            serde_json::json!({
                "code": code.as_str(),
                "message": message,
                "retryable": retryable,
            })
            .to_string(),
        )
}

fn operation_state_is_terminal(state: OperationState) -> bool {
    matches!(
        state,
        OperationState::Succeeded
            | OperationState::Failed
            | OperationState::Canceled
            | OperationState::Expired
    )
}

fn operation_event_is_terminal(event: &PublicOperationEvent) -> bool {
    event.event_type.is_terminal()
}

fn operation_event_page(
    mut events: Vec<PublicOperationEvent>,
    limit: u16,
) -> Result<PublicPage<PublicOperationEvent>, PublicApiError> {
    let has_more = events.len() > usize::from(limit);
    if has_more {
        events.truncate(usize::from(limit));
    }
    let next_cursor = if has_more {
        events
            .last()
            .map(|event| encode_cursor(CursorKind::OperationEvent, event.sequence.to_string()))
            .transpose()?
    } else {
        None
    };

    Ok(PublicPage {
        data: events,
        next_cursor,
    })
}

pub(super) fn parse_operation_id(value: &str) -> Result<OperationId, PublicApiError> {
    OperationId::from_str(value).map_err(|_| PublicApiError::bad_request("invalid operation id"))
}

#[cfg(test)]
mod tests;
