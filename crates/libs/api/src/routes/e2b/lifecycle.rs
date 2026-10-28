use pplx_space_common_core::types::OrgId;

use super::*;
use pplx_space_common_core::types::{AutoLifecyclePolicy, SandboxLifecycle};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicSandboxId,
    SandboxLifecycleAction as PublicSandboxLifecycleAction,
};

use crate::public_operations::{
    CreatePublicOperationResult, PublicOperationStore, PublicOperationStoreError,
};
use crate::public_sandbox_operations::SandboxLifecycleOperationPayload;

const CONNECT_UNREACHABLE_TOUCH_RETRY_ATTEMPTS: usize = 2;
const CONNECT_UNREACHABLE_TOUCH_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(50);
const CONNECT_V2_DEFAULT_TIMEOUT_SECS: u32 = 300;
const DELETE_OPERATION_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
const DELETE_OPERATION_POLL: std::time::Duration = std::time::Duration::from_millis(100);
const DELETE_NOT_ACCEPTED_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(1);
fn connect_activation_request() -> Option<crate::models::SandboxActivationRequest> {
    None
}

/// Client identifier used for concurrency admission, reconnect classification,
/// and request diagnostics. HTTP header names are case-insensitive.
const CLIENT_ID_HEADER: &str = "x-client-id";

/// Extract the trimmed `X-Client-ID` value, if the header is present and
/// non-empty. An absent, non-UTF-8, or blank header yields `None`. Used both to
/// log the connecting client and to key the per-client concurrency limiter.
pub(super) fn request_client_id(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(CLIENT_ID_HEADER)
        .and_then(|value| value.to_str().ok())?
        .trim();
    (!value.is_empty()).then(|| value.to_string())
}

/// Reserve a per-client concurrency slot for a control-plane-touching
/// operation, mapping an enforced rejection to the ECW 429. The returned guard
/// is the *outer* gate: hold it for the whole handler so a rejection here never
/// consults the downstream cluster-activation limiter, and a downstream
/// rejection releases the slot when the guard drops on return (see
/// `crate::client_concurrency`).
pub(super) async fn acquire_client_slot(
    state: &AppState,
    operation: pplx_space_metrics::sandbox::api::client_limit::Operation,
    client_id: Option<&str>,
    minimum_hold: Option<std::time::Duration>,
) -> Result<crate::client_concurrency::ClientConcurrencyGuard, E2bApiError> {
    use crate::client_concurrency::ClientConcurrencyDecision;
    let decision = match minimum_hold {
        Some(minimum_hold) => {
            state
                .client_concurrency
                .try_acquire_for(operation, client_id, minimum_hold)
                .await
        }
        None => {
            state
                .client_concurrency
                .try_acquire(operation, client_id)
                .await
        }
    };
    match decision {
        ClientConcurrencyDecision::Admitted(guard) => Ok(guard),
        ClientConcurrencyDecision::Rejected { retry_after } => {
            Err(E2bApiError::too_many_client_operations().with_retry_after(retry_after))
        }
    }
}

pub(super) fn e2b_native_pause_action() -> SandboxAction {
    SandboxAction::Suspend
}

pub(super) fn e2b_native_resume_action() -> SandboxAction {
    SandboxAction::Restore
}

pub(super) fn e2b_policy_for_resume_update(
    current_policy: &AutoLifecyclePolicy,
    timeout_secs: Option<u32>,
    auto_pause: Option<bool>,
) -> Option<AutoLifecyclePolicy> {
    if timeout_secs.is_none() && auto_pause.is_none() {
        return None;
    }
    let current_config = e2b_lifecycle_config_from_policy(current_policy);
    let updated_policy = e2b_auto_lifecycle_policy(
        timeout_secs
            .or(current_config.map(|config| config.timeout_secs))
            .unwrap_or(ECW_RESPONSE_DEFAULT_TIMEOUT_SECS),
        auto_pause
            .or(current_config.map(|config| config.auto_pause))
            .unwrap_or(false),
        current_config
            .map(|config| config.auto_resume)
            .unwrap_or(false),
    );
    (updated_policy != *current_policy).then_some(updated_policy)
}

pub(super) fn e2b_policy_for_refresh(
    current_policy: &AutoLifecyclePolicy,
) -> Option<AutoLifecyclePolicy> {
    let config = e2b_lifecycle_config_from_policy(current_policy)?;
    Some(e2b_auto_lifecycle_policy(
        config.timeout_secs,
        config.auto_pause,
        config.auto_resume,
    ))
}

fn touch_observed_at_now() -> Result<prost_types::Timestamp, E2bApiError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            E2bApiError::internal(format!("failed to compute touch timestamp: {error}"))
        })?;
    Ok(prost_types::Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

async fn touch_running_e2b_sandbox(
    state: &AppState,
    sandbox: &Sandbox,
    native: &SandboxResponse,
    observed_at: prost_types::Timestamp,
) -> Result<Option<AutoLifecycleTouchOutcome>, E2bApiError> {
    if translate_sandbox_state(native) != Some(SandboxStateE2b::Running) {
        return Ok(None);
    }
    let outcome = state
        .sandbox_controller
        .touch_auto_lifecycle_access_if_started(sandbox, observed_at)
        .await
        .map_err(controller_error)?;
    Ok(Some(outcome))
}

pub(super) fn federated_owner_ref(
    federated: &Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
) -> Option<&crate::federated_identity::FederatedOwner> {
    federated
        .as_ref()
        .map(|Extension(crate::middleware::auth::AuthFederatedOwner(owner))| owner)
}

/// Grants bind to a federated owner row; a legacy local sandbox reached through
/// the bridge has none, so nothing is persisted for it.
pub(super) fn grant_to_persist<'a>(
    sandbox_owner_source: pplx_space_common_core::types::SandboxOwnerSource,
    federated_owner: Option<&'a crate::federated_identity::FederatedOwner>,
    grant: Option<&'a crate::federated_identity::FederatedServiceGrant>,
) -> Option<(
    &'a crate::federated_identity::FederatedOwner,
    &'a crate::federated_identity::FederatedServiceGrant,
)> {
    match sandbox_owner_source {
        pplx_space_common_core::types::SandboxOwnerSource::FederatedIdentity => {
            federated_owner.zip(grant)
        }
        pplx_space_common_core::types::SandboxOwnerSource::Local => None,
    }
}

pub(super) fn request_federated_service_grant(
    headers: &HeaderMap,
    federated_owner: Option<&crate::federated_identity::FederatedOwner>,
) -> Result<Option<crate::federated_identity::FederatedServiceGrant>, E2bApiError> {
    if federated_owner.is_none() {
        return Ok(None);
    }

    crate::federated_identity::federated_service_grant_from_headers(headers)
        .map(Some)
        .map_err(|err| {
            tracing::warn!(
                error = %err,
                "rejected federated ECW request with invalid federated service grant"
            );
            E2bApiError::unauthorized()
        })
}

/// Connect-side variant of [`request_federated_service_grant`], the
/// ramp-phase contract from the federated-identity RFC: a fully absent grant
/// pair is a no-op (`Ok(None)`) while the ECW grant leg ramps — connect has
/// never required these headers, and renewal here is opportunistic. A partial
/// or malformed grant is rejected exactly like refresh, unconditionally: an
/// invalid grant is a bug or an attack, and silently proceeding would hide
/// it. The RFC's end-state strict contract (absent pair on a federated
/// connect = 401) is not enforced yet.
pub(super) fn optional_request_federated_service_grant(
    headers: &HeaderMap,
    federated_owner: Option<&crate::federated_identity::FederatedOwner>,
    grant_required: bool,
) -> Result<Option<crate::federated_identity::FederatedServiceGrant>, E2bApiError> {
    let grant_headers_present = headers
        .contains_key(crate::federated_identity::FEDERATED_SERVICE_GRANT_HEADER)
        || headers
            .contains_key(crate::federated_identity::FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER)
        || headers
            .contains_key(crate::federated_identity::FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER);
    if !grant_headers_present && !grant_required {
        return Ok(None);
    }
    request_federated_service_grant(headers, federated_owner)
}

/// End-state switch for the federated grant contract on connect: absent grant
/// headers on a federated reconnect become 401 (matching refresh) once ECW
/// sends real grants on every reconnect. Default off during the ramp.
pub(super) fn federated_grant_required() -> bool {
    false
}

/// The owner-gated grant persistence shared by refresh and connect, so the
/// two paths cannot drift. Error policy stays at the call sites: refresh
/// propagates (it is the enforced renewal path), connect warns and continues
/// (its renewal is opportunistic). A stale renewal is skipped inside the
/// store and reported as success to both.
pub(super) async fn persist_request_grant(
    state: &AppState,
    sandbox: &crate::models::Sandbox,
    federated_owner: Option<&crate::federated_identity::FederatedOwner>,
    federated_service_grant: Option<&crate::federated_identity::FederatedServiceGrant>,
) -> Result<(), crate::controllers::ControllerError> {
    if let Some((owner, grant)) = grant_to_persist(
        sandbox.owner_source,
        federated_owner,
        federated_service_grant,
    ) {
        state
            .sandbox_controller
            .upsert_federated_service_grant(&state.pool, sandbox.id, owner, grant)
            .await?;
    }
    Ok(())
}

/// Outcome of a best-effort ECW keepalive activity touch on a sandbox the API believed was
/// Running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveKeepaliveTouchOutcome {
    /// The runtime was reached and either accepted the touch, did not need to mutate because the
    /// touch was already current, or did not need auto-lifecycle re-arming.
    Delivered,
    /// The API saw the sandbox as Running, reached the runtime, and sbxmgr reported the local
    /// sandbox is no longer Started.
    NotStarted,
    /// The API saw the sandbox as Running but could not obtain a reachable runtime client.
    RuntimeUnreachable,
    /// A concrete runtime returned a temporary transport error. Unlike a
    /// stale/unreachable route, this is evidence of an upstream outage; do
    /// not enqueue a wake that will join and park behind the same outage.
    TransportUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectTouchRecovery {
    Continue,
    Wake,
    Retry,
    Timeout,
}

pub(super) fn classify_touch(
    touch: Option<AutoLifecycleTouchOutcome>,
) -> LiveKeepaliveTouchOutcome {
    match touch {
        Some(AutoLifecycleTouchOutcome::RuntimeUnreachable) => {
            LiveKeepaliveTouchOutcome::RuntimeUnreachable
        }
        Some(AutoLifecycleTouchOutcome::TransportUnavailable) => {
            LiveKeepaliveTouchOutcome::TransportUnavailable
        }
        Some(AutoLifecycleTouchOutcome::NotStarted) => LiveKeepaliveTouchOutcome::NotStarted,
        Some(
            AutoLifecycleTouchOutcome::Applied
            | AutoLifecycleTouchOutcome::AlreadyCurrent
            | AutoLifecycleTouchOutcome::Disabled,
        )
        | None => LiveKeepaliveTouchOutcome::Delivered,
    }
}

/// Deliver a best-effort keepalive activity touch without failing the caller when it is unavailable.
///
/// Runtime unreachability is a transient/racy condition, not a client error. This helper only
/// classifies the touch result. Callers that need immediate reconnect recovery decide whether to
/// wake from freshly-refetched state or retry the narrowed unreachable case.
async fn deliver_keepalive_touch(
    state: &AppState,
    sandbox: &Sandbox,
    native: &SandboxResponse,
    observed_at: prost_types::Timestamp,
    operation: &'static str,
) -> Result<LiveKeepaliveTouchOutcome, E2bApiError> {
    let touch = touch_running_e2b_sandbox(state, sandbox, native, observed_at).await?;
    let outcome = classify_touch(touch);
    if outcome != LiveKeepaliveTouchOutcome::Delivered {
        tracing::debug!(
            sandbox_id = %sandbox.id,
            operation,
            outcome = ?outcome,
            "live auto-lifecycle keepalive touch did not re-arm immediately"
        );
    }
    Ok(outcome)
}

pub(super) async fn create_sandbox(
    State(state): State<AppState>,
    MatchedEcwHost(ecw_host_pair): MatchedEcwHost,
    Extension(crate::middleware::auth::AuthEcwActor(actor)): Extension<
        crate::middleware::auth::AuthEcwActor,
    >,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    headers: HeaderMap,
    crate::e2b::E2bJson(req): crate::e2b::E2bJson<CreateSandboxRequestE2b>,
) -> Result<impl IntoResponse, E2bApiError> {
    let actor = actor.ok_or_else(E2bApiError::unauthorized)?;
    // Reserve a per-client concurrency slot before doing any control-plane work.
    // Held for the whole handler; released on return.
    let client_id = request_client_id(&headers);
    let _client_slot = acquire_client_slot(
        &state,
        pplx_space_metrics::sandbox::api::client_limit::Operation::Create,
        client_id.as_deref(),
        None,
    )
    .await?;
    let federated_owner = federated_owner_ref(&federated);
    let egress_profiles = state
        .sandbox_controller
        .select_egress_profiles(&headers, federated_owner.is_some())
        .map_err(|error| E2bApiError::bad_request(error.to_string()))?;
    let federated_service_grant = request_federated_service_grant(&headers, federated_owner)?;
    let translated = translate_create_request(req)?;
    let federated_create =
        FederatedSandboxCreateContext::new(federated_owner, federated_service_grant.as_ref())
            .with_egress_profiles(Some(&egress_profiles));
    let native = match state
        .sandbox_controller
        .create(
            &state.pool,
            &actor,
            &translated.native,
            SandboxApiMetricIdentity::ecw("create"),
            federated_create,
        )
        .await
    {
        Ok(native) => native,
        Err(err) => return Err(controller_error(err)),
    };
    record_ecw_sandbox_span_tag(native.id);

    let template_name = translated
        .native
        .template
        .as_ref()
        .map(|template| template.name().as_str().to_owned())
        .unwrap_or_else(|| translated.template_id.clone());

    Ok((
        StatusCode::CREATED,
        Extension(EcwRequestLogContext::sandbox(native.id)),
        Extension(crate::routes::metrics::RouteTemplateName(template_name)),
        Json(build_sandbox_response(
            &state.ecw_credential_keyring,
            &native,
            translated.template_id,
            Some(ecw_host_pair.host),
            pplx_space_common_core::types::EcwCredentialEpoch::new(1)
                .expect("initial ECW credential epoch is valid"),
        )),
    ))
}

pub(super) async fn list_sandboxes(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    crate::e2b::E2bQuery(query): crate::e2b::E2bQuery<SandboxListQueryE2b>,
) -> Result<impl IntoResponse, E2bApiError> {
    validate_list_query(&query)?;
    let metadata_filter = parse_metadata_query(query.metadata.as_deref())?;
    let state_filter = parse_state_filter(query.state.as_deref())?;
    let cursor = query
        .next_token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(decode_list_cursor)
        .transpose()?;
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(ECW_LIST_DEFAULT_LIMIT)
        .min(ECW_LIST_MAX_LIMIT);

    // The controller pushes the metadata filter into the list query and stops
    // the page at `limit` rows or the list byte budget. When more rows follow,
    // the cursor is the LAST kept row's keyset: pagination is strict `<`, so
    // the next request starts at the first row this page left out.
    let scope = ecw_access_scope(&org, &federated);
    let page = fetch_scoped_sandbox_page(&state, &scope, cursor, limit, &metadata_filter).await?;
    let next_cursor_row = if page.has_more {
        page.rows.last().cloned()
    } else {
        None
    };
    let rows = page.rows;

    let mut items = Vec::with_capacity(rows.len());
    for sandbox in rows {
        let native = state.sandbox_controller.cached_sandbox_response(&sandbox);

        let Some(item) = build_listed_sandbox(&state, &sandbox, &native).await? else {
            continue;
        };
        // State filter runs against cached DB state so ECW list stays an inventory read.
        if !state_filter.is_empty() && !state_filter.contains(&item.state) {
            continue;
        }
        items.push(item);
    }

    // Only set the header when another page exists. The SDK paginator reads
    // `bool(x-next-token)` so an absent or empty header both mean "no more
    // pages"; the explicit absence is cleaner on the wire.
    let mut headers = HeaderMap::new();
    if let Some(row) = next_cursor_row.as_ref() {
        let value = header::HeaderValue::from_str(&encode_list_cursor(row))
            .map_err(|_| E2bApiError::internal("Internal server error"))?;
        headers.insert(NEXT_TOKEN_HEADER, value);
    }
    Ok((headers, Json(ListedSandboxesResponseE2b(items))))
}

pub(super) fn validate_list_query(query: &SandboxListQueryE2b) -> Result<(), E2bApiError> {
    let mut unsupported = Vec::new();
    if query.order.is_some() {
        unsupported.push("order");
    }
    if query.started_after.is_some() {
        unsupported.push("startedAfter");
    }
    if query.template.is_some() {
        unsupported.push("template");
    }
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(E2bApiError::bad_request(format!(
            "unsupported sandbox list parameters: {}",
            unsupported.join(", ")
        )))
    }
}

// Header used to carry the opaque keyset cursor for `Sandbox.list` pagination.
// The E2B SDK paginator reads this header to drive `has_next` and to forward
// the cursor on the subsequent request.
pub(super) const NEXT_TOKEN_HEADER: header::HeaderName =
    header::HeaderName::from_static("x-next-token");

pub(super) async fn get_sandbox(
    State(state): State<AppState>,
    MatchedEcwHost(ecw_host_pair): MatchedEcwHost,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    Path(sandbox_id): Path<String>,
) -> Result<Json<SandboxDetailE2b>, E2bApiError> {
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
    let native = match state
        .sandbox_controller
        .tracked_response(&state.pool, sandbox.clone())
        .await
    {
        Ok(native) => native,
        Err(err) => return Err(controller_error(err)),
    };
    let detail = build_detail_sandbox(
        &state.ecw_credential_keyring,
        &sandbox,
        &native,
        Some(ecw_host_pair.host),
    )?;

    Ok(Json(detail))
}

pub(super) async fn delete_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    Path(sandbox_id): Path<String>,
) -> Result<StatusCode, E2bApiError> {
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;

    // Persist one durable delete intent before external teardown. The cluster
    // controller owns execution; this request is only a bounded observer, so
    // disconnecting it cannot cancel teardown or strand a live claim.
    let public_sandbox_id = PublicSandboxId::from(sandbox.id);
    let payload = SandboxLifecycleOperationPayload {
        sandbox_id: public_sandbox_id,
        action: PublicSandboxLifecycleAction::Delete,
        rollback: None,
        wake_activity: None,
    }
    .into_json();
    let target_id = public_sandbox_id.to_string();
    let store = PublicOperationStore::new(state.pool.clone());
    let created = store
        // The scope check above authorizes the caller; the store anchors and
        // coalesces the operation under the sandbox's owning organization.
        .create_or_join_active_sandbox_delete(
            sandbox.org_id,
            &target_id,
            payload,
            crate::public_operations::request_traceparent(),
        )
        .await
        .map_err(|error| {
            tracing::error!(
                sandbox_id = %sandbox.id,
                error = %error,
                "failed to persist durable sandbox delete operation"
            );
            delete_admission_error(&error)
        })?;
    let operation_id = match created {
        CreatePublicOperationResult::Created(operation_id) => operation_id,
        CreatePublicOperationResult::Existing(operation_id) => {
            // The joined delete's attempts link to the request that created
            // it, not to this one; this request finds them by its id.
            pplx_space_observability::trace_propagation::set_span_attribute(
                &tracing::Span::current(),
                "operation_id",
                &operation_id.to_string(),
            );
            operation_id
        }
        CreatePublicOperationResult::IdempotencyConflict { .. }
        | CreatePublicOperationResult::AdmissionDenied => {
            return Err(E2bApiError::internal("Internal server error"));
        }
    };
    wait_for_delete_operation(
        &store,
        sandbox.org_id,
        operation_id,
        public_sandbox_id,
        DELETE_OPERATION_WAIT,
    )
    .await
}

/// Admission joins any active delete for the target, so a retried DELETE
/// never queues a second one. A database failure leaves acceptance unproven
/// and is retryable; only a malformed stored row is a server fault.
fn delete_admission_error(error: &PublicOperationStoreError) -> E2bApiError {
    match error {
        PublicOperationStoreError::Sql(error)
            if pplx_space_common_svc::sql::contention::is_lock_contention(error) =>
        {
            E2bApiError::lock_contention()
        }
        PublicOperationStoreError::Sql(_) => delete_not_accepted(),
        _ => E2bApiError::internal("Internal server error"),
    }
}

fn delete_not_accepted() -> E2bApiError {
    E2bApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "Sandbox delete was not accepted; retry the request",
    )
    .with_classification(ResponseOrigin::Api, ResponseKind::Internal)
    .with_retry_after(DELETE_NOT_ACCEPTED_RETRY_AFTER)
}

struct DeleteOperationObserver {
    operation_id: pplx_space_public_api::OperationId,
    sandbox_id: PublicSandboxId,
    completed: bool,
}

impl Drop for DeleteOperationObserver {
    fn drop(&mut self) {
        if !self.completed {
            tracing::warn!(
                operation_id = %self.operation_id,
                sandbox_id = %self.sandbox_id,
                phase = "http_observer",
                result = "cancelled",
                "sandbox delete observer detached before durable finality"
            );
        }
    }
}

async fn wait_for_delete_operation(
    store: &PublicOperationStore,
    org_id: OrgId,
    operation_id: pplx_space_public_api::OperationId,
    sandbox_id: PublicSandboxId,
    wait: std::time::Duration,
) -> Result<StatusCode, E2bApiError> {
    let mut observer = DeleteOperationObserver {
        operation_id,
        sandbox_id,
        completed: false,
    };
    let result = tokio::time::timeout(wait, async {
        loop {
            let operation = match store.fetch_operation(org_id, operation_id).await {
                Ok(operation) => operation,
                Err(error) => {
                    // The delete is already durable; only this observation failed.
                    tracing::warn!(%operation_id, %sandbox_id, %error, "failed to observe sandbox delete operation");
                    return Ok(StatusCode::ACCEPTED);
                }
            }
            .ok_or_else(|| E2bApiError::internal("Internal server error"))?;
            match operation.state {
                OperationState::Succeeded => return Ok(StatusCode::NO_CONTENT),
                OperationState::Failed | OperationState::Canceled | OperationState::Expired => {
                    return Err(e2b_operation_failure(
                        operation.terminal_error_code,
                        operation.terminal_error.as_ref(),
                        &operation.progress_message,
                    ));
                }
                OperationState::Queued | OperationState::Running => {
                    tokio::time::sleep(DELETE_OPERATION_POLL).await;
                }
                OperationState::Other(_) => {
                    return Err(E2bApiError::internal("Internal server error"));
                }
            }
        }
    })
    .await;
    observer.completed = true;
    match result {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!(
                %operation_id,
                %sandbox_id,
                phase = "http_observer",
                result = "timeout",
                "sandbox delete remains in progress after HTTP wait budget"
            );
            // A worker that is draining or restarting can hold the delete
            // past this budget. A data-plane failure shape would send
            // clients back to reconnect and delete again, but the queued
            // operation finishes on its own, so report it as accepted.
            Ok(StatusCode::ACCEPTED)
        }
    }
}

fn e2b_operation_failure(
    terminal_error_code: Option<PublicErrorCode>,
    terminal_error_json: Option<&serde_json::Value>,
    progress_message: &str,
) -> E2bApiError {
    let message = terminal_error_json
        .as_ref()
        .and_then(|json| json.get("message"))
        .and_then(|message| message.as_str())
        .unwrap_or(progress_message)
        .to_string();
    let status = match terminal_error_code {
        Some(PublicErrorCode::InvalidRequest) => StatusCode::BAD_REQUEST,
        Some(PublicErrorCode::Unauthorized) => StatusCode::UNAUTHORIZED,
        Some(PublicErrorCode::Forbidden) => StatusCode::FORBIDDEN,
        Some(PublicErrorCode::NotFound) => StatusCode::NOT_FOUND,
        Some(PublicErrorCode::Conflict | PublicErrorCode::IdempotencyConflict) => {
            StatusCode::CONFLICT
        }
        Some(PublicErrorCode::FailedPrecondition) => StatusCode::PRECONDITION_FAILED,
        Some(PublicErrorCode::CapacityUnavailable | PublicErrorCode::ServiceUnavailable) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        Some(PublicErrorCode::RateLimited | PublicErrorCode::QuotaExceeded) => {
            StatusCode::TOO_MANY_REQUESTS
        }
        Some(
            PublicErrorCode::OperationFailed
            | PublicErrorCode::Internal
            | PublicErrorCode::Other(_),
        )
        | None => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let error = E2bApiError::new(status, message);
    // The failed operation is no longer active, so a new DELETE queues fresh work.
    if status == StatusCode::SERVICE_UNAVAILABLE {
        error.with_retry_after(DELETE_NOT_ACCEPTED_RETRY_AFTER)
    } else {
        error
    }
}

pub(super) async fn connect_sandbox_v2(
    state: State<AppState>,
    host: MatchedEcwHost,
    org: Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    sandbox_id: Path<String>,
    headers: HeaderMap,
    body: Option<crate::e2b::E2bJson<ConnectSandboxRequestE2b>>,
) -> Result<Json<SandboxResponseE2b>, E2bApiError> {
    // V2 supplies a five-minute minimum; legacy omission remains an activity-only touch.
    let mut request = body
        .map(|crate::e2b::E2bJson(request)| request)
        .unwrap_or_default();
    let timeout = request.timeout.unwrap_or(CONNECT_V2_DEFAULT_TIMEOUT_SECS);
    if timeout == 0 {
        return Err(E2bApiError::bad_request("Timeout must be greater than 0"));
    }
    request.timeout = Some(timeout);
    connect_sandbox(
        state,
        host,
        org,
        federated,
        sandbox_id,
        headers,
        Some(crate::e2b::E2bJson(request)),
    )
    .await
}

pub(super) async fn connect_sandbox(
    state: State<AppState>,
    host: MatchedEcwHost,
    org: Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    sandbox_id: Path<String>,
    headers: HeaderMap,
    body: Option<crate::e2b::E2bJson<ConnectSandboxRequestE2b>>,
) -> Result<Json<SandboxResponseE2b>, E2bApiError> {
    tracing::Span::current().record(
        "client_class",
        state
            .client_classification
            .classify(request_client_id(&headers).as_deref())
            .as_str(),
    );
    validate_connect_request(body.as_ref().map(|crate::e2b::E2bJson(request)| *request))?;
    let observed_at = touch_observed_at_now()?;
    let wake_trace = crate::wake_trace::WakeTrace::new();
    crate::wake_trace::scope(
        Some(wake_trace),
        // Keep the large reconnect state machine out of the task-local wrapper's
        // stack frame (including on small integration-test thread stacks).
        Box::pin(connect_sandbox_inner(
            state,
            host,
            org,
            federated,
            sandbox_id,
            headers,
            body,
            observed_at,
        )),
    )
    .await
}

pub(super) fn validate_connect_request(
    request: Option<ConnectSandboxRequestE2b>,
) -> Result<(), E2bApiError> {
    if matches!(request, Some(request) if request.memory == Some(false)) {
        return Err(E2bApiError::bad_request(
            "unsupported sandbox connect parameter: memory=false",
        ));
    }
    Ok(())
}

// Mirrors the route extractors, retaining one acceptance timestamp across wake.
#[allow(clippy::too_many_arguments)]
pub(super) async fn connect_sandbox_inner(
    State(state): State<AppState>,
    MatchedEcwHost(ecw_host_pair): MatchedEcwHost,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
    body: Option<crate::e2b::E2bJson<ConnectSandboxRequestE2b>>,
    observed_at: prost_types::Timestamp,
) -> Result<Json<SandboxResponseE2b>, E2bApiError> {
    let wake_trace = crate::wake_trace::current().unwrap_or_else(crate::wake_trace::WakeTrace::new);
    let reconnect_started_at = std::time::Instant::now();
    let client_id = request_client_id(&headers);
    // Stamp the coarse client class onto the metric identity once. It flows
    // through every reconnect emission this handler drives — the route-level
    // rejections below and the controller wake path — so the retry-storm alert
    // can split interactive from cron traffic using the configured client classes.
    let reconnect_identity = SandboxApiMetricIdentity::ecw("connect")
        .with_client_class(state.client_classification.classify(client_id.as_deref()));
    // Reserve a per-client concurrency slot before touching the control plane.
    // Held for the whole handler; released on return.
    let _client_slot = match acquire_client_slot(
        &state,
        pplx_space_metrics::sandbox::api::client_limit::Operation::Reconnect,
        client_id.as_deref(),
        None,
    )
    .await
    {
        Ok(slot) => slot,
        Err(err) => {
            // In enforce mode the per-client limiter rejects here, before any
            // control-plane work — and that is exactly the retry-storm traffic.
            // Record it on the authoritative reconnect series (carrying the
            // client class) so the retry-storm alert isn't blind to the rejected
            // fraction once enforcement is on. The lifecycle isn't known yet
            // (the sandbox hasn't been fetched), so it tags as `unknown`.
            crate::controllers::sandbox::record_reconnect_attempt(
                reconnect_identity,
                "",
                pplx_space_metrics::sandbox::api::ReconnectOutcome::Rejected,
                pplx_space_metrics::sandbox::api::ReconnectTerminalPhase::Api,
                pplx_space_metrics::sandbox::api::ReconnectReason::RateLimited,
                reconnect_started_at,
            );
            return Err(err);
        }
    };
    let federated_owner = federated_owner_ref(&federated);
    // Reconnects renew the persisted federated grant when the request
    // carries one, so long-lived sessions that reconnect without ever calling
    // refresh keep a fresh grant. Parsed before the fetch (matching refresh):
    // an invalid grant is unauthorized regardless of the sandbox.
    let federated_service_grant = optional_request_federated_service_grant(
        &headers,
        federated_owner,
        federated_grant_required(),
    )?;
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
    // Warn-and-continue, unlike refresh: renewal on connect is opportunistic
    // (the headers are optional here during the ramp) and a transient store
    // failure must not kill a reconnect that would otherwise succeed. Refresh
    // remains the enforced renewal path and still propagates.
    if let Err(error) = persist_request_grant(
        &state,
        &sandbox,
        federated_owner,
        federated_service_grant.as_ref(),
    )
    .await
    {
        tracing::warn!(
            sandbox_id = %sandbox.id,
            %error,
            "failed to renew the federated service grant on connect; refresh remains the enforced path"
        );
    }

    // Informational only: log the connecting client so it shows in Datadog.
    // Never gates control-plane behavior. Metadata persistence is intentionally
    // deferred for now.
    if let Some(client_id) = client_id.as_deref() {
        tracing::info!(
            sandbox_id = %sandbox.id,
            client_id = %client_id,
            "ecw sandbox connect client id",
        );
    }
    let native = match state
        .sandbox_controller
        .tracked_response(&state.pool, sandbox.clone())
        .await
    {
        Ok(native) => native,
        Err(err) => return Err(controller_error(err)),
    };
    if translate_sandbox_state(&native).is_none() {
        crate::controllers::sandbox::record_reconnect_attempt(
            reconnect_identity,
            &sandbox.lifecycle,
            pplx_space_metrics::sandbox::api::ReconnectOutcome::Rejected,
            pplx_space_metrics::sandbox::api::ReconnectTerminalPhase::Api,
            pplx_space_metrics::sandbox::api::ReconnectReason::TerminalLifecycle,
            reconnect_started_at,
        );
        return Err(terminal_sandbox_error(&native));
    }
    if let Some(err) = teardown_lifecycle_connect_error(&sandbox) {
        return Err(err);
    }

    let mut touched_by_wake = false;
    let mut wake_attempted = false;
    if should_wake_on_connect(&sandbox, &native) {
        wake_attempted = true;
        wake_trace.wake_required();
        match state
            .sandbox_controller
            .action(
                &state.pool,
                sandbox.id,
                Some(&sandbox),
                reconnect_identity,
                SandboxAction::Wake,
                connect_activation_request(),
            )
            .await
        {
            Ok(_) => {
                wake_trace.wake_completed();
                touched_by_wake = true;
            }
            Err(err) => {
                wake_trace.failed();
                return Err(controller_error(err));
            }
        }
    }

    let current_policy = native
        .auto_lifecycle
        .as_ref()
        .unwrap_or(&sandbox.auto_lifecycle.0);
    let requested_timeout_secs = body.and_then(|crate::e2b::E2bJson(request)| request.timeout);
    let connect_should_touch =
        should_touch_on_connect(current_policy, requested_timeout_secs, &native);
    let mut refreshed_response = None;
    if connect_should_touch && !touched_by_wake {
        let mut retries_remaining = CONNECT_UNREACHABLE_TOUCH_RETRY_ATTEMPTS;
        // The rows a retry re-read, which the next touch and the response use.
        let mut retried: Option<(Sandbox, SandboxResponse)> = None;
        loop {
            let (touch_sandbox, touch_native) = retried
                .as_ref()
                .map_or((&sandbox, &native), |(sandbox, native)| (sandbox, native));
            let touch_outcome = deliver_keepalive_touch(
                &state,
                touch_sandbox,
                touch_native,
                observed_at,
                "connect",
            )
            .await?;
            if touch_outcome == LiveKeepaliveTouchOutcome::Delivered {
                refreshed_response = retried;
                break;
            }
            let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
            if let Some(err) = teardown_lifecycle_connect_error(&sandbox) {
                return Err(err);
            }
            let native = match state
                .sandbox_controller
                .tracked_response(&state.pool, sandbox.clone())
                .await
            {
                Ok(native) => native,
                Err(err) => return Err(controller_error(err)),
            };
            match connect_touch_recovery(touch_outcome, &sandbox, &native, retries_remaining) {
                ConnectTouchRecovery::Wake => {
                    wake_attempted = true;
                    wake_trace.wake_required();
                    state
                        .sandbox_controller
                        .action(
                            &state.pool,
                            sandbox.id,
                            Some(&sandbox),
                            reconnect_identity,
                            SandboxAction::Wake,
                            None,
                        )
                        .await
                        .map_err(|err| {
                            wake_trace.failed();
                            controller_error(err)
                        })?;
                    wake_trace.wake_completed();
                    break;
                }
                ConnectTouchRecovery::Timeout => {
                    crate::controllers::sandbox::record_reconnect_attempt(
                        reconnect_identity,
                        &sandbox.lifecycle,
                        pplx_space_metrics::sandbox::api::ReconnectOutcome::Rejected,
                        pplx_space_metrics::sandbox::api::ReconnectTerminalPhase::Api,
                        pplx_space_metrics::sandbox::api::ReconnectReason::Unavailable,
                        reconnect_started_at,
                    );
                    return Err(E2bApiError::sandbox_timeout().with_classification(
                        ResponseOrigin::Cluster,
                        ResponseKind::RouteNotReady,
                    ));
                }
                ConnectTouchRecovery::Continue => {
                    refreshed_response = Some((sandbox, native));
                    break;
                }
                ConnectTouchRecovery::Retry => {
                    retried = Some((sandbox, native));
                    retries_remaining -= 1;
                    tokio::time::sleep(CONNECT_UNREACHABLE_TOUCH_RETRY_DELAY).await;
                }
            }
        }
    }

    let (sandbox, native) = match refreshed_response {
        Some(refreshed) => refreshed,
        // Only a wake changes the rows last read; a delivered keepalive touch
        // updates the runtime alone. Reconnects to running sandboxes are the
        // common case, so skip reading the same rows again.
        None if !wake_attempted => (sandbox, native),
        None => {
            let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
            let native = match state
                .sandbox_controller
                .tracked_response(&state.pool, sandbox.clone())
                .await
            {
                Ok(native) => native,
                Err(err) => return Err(controller_error(err)),
            };
            (sandbox, native)
        }
    };
    if let Some(timeout) = requested_timeout_secs {
        let until = prost_types::Timestamp {
            seconds: observed_at.seconds.saturating_add(i64::from(timeout)),
            nanos: observed_at.nanos,
        };
        state
            .sandbox_controller
            .update_access_idle_deadline(
                &sandbox,
                observed_at,
                tokio::time::Instant::from_std(reconnect_started_at)
                    + crate::controllers::substrate::NODE_RPC_TIMEOUT,
                pplx_space_sbx_mgr_client::proto::access_idle_update::Operation::ExtendUntil(until),
            )
            .await
            .map_err(controller_error)?;
    }
    let template_id = native
        .template
        .clone()
        .or_else(|| sandbox.template_id.clone())
        .unwrap_or_else(|| ECW_DEFAULT_TEMPLATE_ID.to_string());

    if !wake_attempted {
        crate::controllers::sandbox::record_reconnect_attempt(
            reconnect_identity,
            &sandbox.lifecycle,
            pplx_space_metrics::sandbox::api::ReconnectOutcome::Connected,
            pplx_space_metrics::sandbox::api::ReconnectTerminalPhase::NoWake,
            pplx_space_metrics::sandbox::api::ReconnectReason::NoWakeNeeded,
            reconnect_started_at,
        );
    }

    wake_trace.completed(sandbox.active_node_name.as_deref(), wake_attempted);
    Ok(Json(build_sandbox_response(
        &state.ecw_credential_keyring,
        &native,
        template_id,
        Some(ecw_host_pair.host),
        sandbox.ecw_credential_epoch,
    )))
}

pub(super) fn should_wake_on_connect(sandbox: &Sandbox, native: &SandboxResponse) -> bool {
    // ECW presents an in-flight restore as running, but its pod may still be
    // unschedulable. Enter the owned restore path before probing the runtime
    // so retries can join the lease or recover an interrupted placement.
    matches!(sandbox.lifecycle.parse(), Ok(SandboxLifecycle::Restoring))
        || translate_sandbox_state(native) == Some(SandboxStateE2b::Paused)
        || translate_lifecycle_state(&sandbox.lifecycle) == Some(SandboxStateE2b::Paused)
}

pub(super) fn can_retry_unreachable_connect_touch(
    native: &SandboxResponse,
    retries_remaining: usize,
) -> bool {
    retries_remaining > 0 && translate_sandbox_state(native) == Some(SandboxStateE2b::Running)
}

pub(super) fn should_ensure_usable_pod_on_connect(native: &SandboxResponse) -> bool {
    translate_sandbox_state(native) == Some(SandboxStateE2b::Running)
}

pub(super) fn should_touch_on_connect(
    current_policy: &AutoLifecyclePolicy,
    requested_timeout_secs: Option<u32>,
    native: &SandboxResponse,
) -> bool {
    legacy_connect_should_touch(current_policy, requested_timeout_secs)
        || should_ensure_usable_pod_on_connect(native)
}

fn legacy_connect_should_touch(
    current_policy: &AutoLifecyclePolicy,
    requested_timeout_secs: Option<u32>,
) -> bool {
    requested_timeout_secs.is_some() || e2b_policy_for_refresh(current_policy).is_some()
}

fn teardown_lifecycle_connect_error(sandbox: &Sandbox) -> Option<E2bApiError> {
    match sandbox
        .lifecycle
        .trim()
        .to_ascii_lowercase()
        .parse::<SandboxLifecycle>()
    {
        Ok(SandboxLifecycle::Failed | SandboxLifecycle::Quarantined) => {
            Some(E2bApiError::failed_to_place_sandbox())
        }
        Ok(SandboxLifecycle::Stopped) => Some(E2bApiError::not_found("Sandbox")),
        Ok(SandboxLifecycle::Stopping | SandboxLifecycle::Deleting) => {
            Some(E2bApiError::sandbox_not_ready())
        }
        Ok(_) => None,
        Err(_) => match sandbox.lifecycle.trim().to_ascii_lowercase().as_str() {
            "deleted" => Some(E2bApiError::not_found("Sandbox")),
            "cleanup_required" => Some(E2bApiError::sandbox_not_ready()),
            _ => None,
        },
    }
}

fn connect_recovery_lifecycle_allows_wake(sandbox: &Sandbox) -> bool {
    teardown_lifecycle_connect_error(sandbox).is_none()
}

fn should_wake_after_missed_connect_touch(sandbox: &Sandbox, native: &SandboxResponse) -> bool {
    connect_recovery_lifecycle_allows_wake(sandbox)
        && (should_wake_on_connect(sandbox, native)
            || translate_sandbox_state(native) == Some(SandboxStateE2b::Running)
            || matches!(
                sandbox.lifecycle.parse::<SandboxLifecycle>(),
                Ok(SandboxLifecycle::Started | SandboxLifecycle::Restoring)
            ))
}

pub(super) fn connect_touch_recovery(
    touch_outcome: LiveKeepaliveTouchOutcome,
    sandbox: &Sandbox,
    native: &SandboxResponse,
    retries_remaining: usize,
) -> ConnectTouchRecovery {
    match touch_outcome {
        LiveKeepaliveTouchOutcome::Delivered => ConnectTouchRecovery::Continue,
        LiveKeepaliveTouchOutcome::TransportUnavailable => ConnectTouchRecovery::Timeout,
        LiveKeepaliveTouchOutcome::RuntimeUnreachable
            if connect_recovery_lifecycle_allows_wake(sandbox)
                && should_wake_on_connect(sandbox, native) =>
        {
            ConnectTouchRecovery::Wake
        }
        LiveKeepaliveTouchOutcome::RuntimeUnreachable
            if can_retry_unreachable_connect_touch(native, retries_remaining) =>
        {
            ConnectTouchRecovery::Retry
        }
        LiveKeepaliveTouchOutcome::NotStarted | LiveKeepaliveTouchOutcome::RuntimeUnreachable
            if should_wake_after_missed_connect_touch(sandbox, native) =>
        {
            ConnectTouchRecovery::Wake
        }
        LiveKeepaliveTouchOutcome::NotStarted | LiveKeepaliveTouchOutcome::RuntimeUnreachable => {
            ConnectTouchRecovery::Timeout
        }
    }
}

pub(super) async fn pause_sandbox_e2b(
    State(state): State<AppState>,
    MatchedEcwHost(ecw_host_pair): MatchedEcwHost,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    Path(sandbox_id): Path<String>,
    body: Option<crate::e2b::E2bJson<SandboxPauseRequestE2b>>,
) -> Result<Json<SandboxResponseE2b>, E2bApiError> {
    validate_pause_request(body.map(|crate::e2b::E2bJson(request)| request))?;
    action_sandbox_e2b(
        state,
        ecw_access_scope(&org, &federated),
        sandbox_id,
        e2b_native_pause_action(),
        SandboxApiMetricIdentity::ecw("pause"),
        Some(ecw_host_pair.host),
    )
    .await
}

pub(super) fn validate_pause_request(
    request: Option<SandboxPauseRequestE2b>,
) -> Result<(), E2bApiError> {
    if matches!(request, Some(request) if request.memory == Some(false)) {
        return Err(E2bApiError::bad_request(
            "unsupported sandbox pause parameter: memory=false",
        ));
    }
    Ok(())
}

pub(super) async fn resume_sandbox_e2b(
    State(state): State<AppState>,
    MatchedEcwHost(ecw_host_pair): MatchedEcwHost,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    Path(sandbox_id): Path<String>,
    body: Option<crate::e2b::E2bJson<SandboxResumeRequestE2b>>,
) -> Result<Json<SandboxResponseE2b>, E2bApiError> {
    let request = body.map(|crate::e2b::E2bJson(request)| request);
    validate_resume_request(request)?;
    let scope = ecw_access_scope(&org, &federated);
    if let Some(request) = request
        && (request.timeout.is_some() || request.auto_pause.is_some())
    {
        update_resuming_sandbox_policy(&state, &scope, &sandbox_id, request).await?;
    }

    action_sandbox_e2b(
        state,
        scope,
        sandbox_id,
        e2b_native_resume_action(),
        SandboxApiMetricIdentity::ecw("resume"),
        Some(ecw_host_pair.host),
    )
    .await
}

async fn update_resuming_sandbox_policy(
    state: &AppState,
    scope: &SandboxAccessScope,
    sandbox_id: &str,
    request: SandboxResumeRequestE2b,
) -> Result<(), E2bApiError> {
    let sandbox = fetch_scoped_sandbox(state, scope, sandbox_id).await?;
    if sandbox.lifecycle.eq_ignore_ascii_case("failed") {
        return Err(E2bApiError::failed_to_place_sandbox());
    }
    if let Some(policy) = e2b_policy_for_resume_update(
        &sandbox.auto_lifecycle.0,
        request.timeout,
        request.auto_pause,
    ) {
        state
            .sandbox_controller
            .set_auto_lifecycle_policy(&state.pool, &sandbox, policy)
            .await
            .map_err(controller_error)?;
    }
    Ok(())
}

pub(super) fn validate_resume_request(
    request: Option<SandboxResumeRequestE2b>,
) -> Result<(), E2bApiError> {
    if matches!(request, Some(request) if request.memory == Some(false)) {
        return Err(E2bApiError::bad_request(
            "unsupported sandbox resume parameter: memory=false",
        ));
    }
    Ok(())
}

pub(super) async fn action_sandbox_e2b(
    state: AppState,
    scope: SandboxAccessScope,
    sandbox_id: String,
    action: SandboxAction,
    metric_identity: SandboxApiMetricIdentity,
    domain: Option<String>,
) -> Result<Json<SandboxResponseE2b>, E2bApiError> {
    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;

    if action == SandboxAction::Restore && sandbox.lifecycle.eq_ignore_ascii_case("failed") {
        return Err(E2bApiError::failed_to_place_sandbox());
    }

    match state
        .sandbox_controller
        .action(
            &state.pool,
            sandbox.id,
            Some(&sandbox),
            metric_identity,
            action,
            None,
        )
        .await
    {
        Ok(_) => {}
        Err(err) if action == SandboxAction::Restore => return Err(restore_controller_error(err)),
        Err(err) => return Err(controller_error(err)),
    }

    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
    let native = match state
        .sandbox_controller
        .tracked_response(&state.pool, sandbox.clone())
        .await
    {
        Ok(native) => native,
        Err(err) => return Err(controller_error(err)),
    };
    let template_id = native
        .template
        .clone()
        .or_else(|| sandbox.template_id.clone())
        .unwrap_or_else(|| ECW_DEFAULT_TEMPLATE_ID.to_string());

    Ok(Json(build_sandbox_response(
        &state.ecw_credential_keyring,
        &native,
        template_id,
        domain,
        sandbox.ecw_credential_epoch,
    )))
}

pub(super) async fn timeout_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    Path(sandbox_id): Path<String>,
    crate::e2b::E2bJson(request): crate::e2b::E2bJson<SandboxTimeoutRequestE2b>,
) -> Result<StatusCode, E2bApiError> {
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
    let native = state
        .sandbox_controller
        .tracked_response(&state.pool, sandbox.clone())
        .await
        .map_err(controller_error)?;
    if translate_sandbox_state(&native).is_none() {
        return Err(terminal_sandbox_error(&native));
    }
    let observed_at = touch_observed_at_now()?;
    let deadline = tokio::time::Instant::now() + crate::controllers::substrate::NODE_RPC_TIMEOUT;
    state
        .sandbox_controller
        .set_access_idle_timeout(
            &state.pool,
            &sandbox,
            request.timeout,
            observed_at,
            deadline,
            None,
        )
        .await
        .map_err(controller_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn refresh_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    body: Option<crate::e2b::E2bJson<SandboxRefreshRequestE2b>>,
) -> Result<StatusCode, E2bApiError> {
    validate_refresh_request(body.map(|crate::e2b::E2bJson(request)| request))?;
    let federated_owner = federated_owner_ref(&federated);
    let federated_service_grant = request_federated_service_grant(&headers, federated_owner)?;
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &sandbox_id).await?;
    persist_request_grant(
        &state,
        &sandbox,
        federated_owner,
        federated_service_grant.as_ref(),
    )
    .await
    .map_err(controller_error)?;
    let native = state
        .sandbox_controller
        .tracked_response(&state.pool, sandbox.clone())
        .await
        .map_err(controller_error)?;
    if translate_sandbox_state(&native).is_none() {
        return Err(terminal_sandbox_error(&native));
    }
    let current_policy = native
        .auto_lifecycle
        .as_ref()
        .unwrap_or(&sandbox.auto_lifecycle.0);
    if e2b_policy_for_refresh(current_policy).is_none() {
        tracing::debug!(
            sandbox_id = %sandbox.id,
            "skipping E2B refresh for non-E2B auto lifecycle policy",
        );
        return Ok(StatusCode::NO_CONTENT);
    }
    let observed_at = touch_observed_at_now()?;
    deliver_keepalive_touch(&state, &sandbox, &native, observed_at, "refresh").await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) fn validate_refresh_request(
    request: Option<SandboxRefreshRequestE2b>,
) -> Result<(), E2bApiError> {
    if matches!(request, Some(request) if request.duration.is_some()) {
        return Err(E2bApiError::bad_request(
            "unsupported sandbox refresh parameter: duration",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
