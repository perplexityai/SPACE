use pplx_space_common_core::types::{OrgId, UserId};

use std::time::Duration;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use pplx_space_common_core::types::{IdempotencyKey, SandboxId};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_public_api::PublicErrorCode;

use super::{AppState, E2bApiError, MatchedEcwHost, acquire_client_slot, request_client_id};
use crate::e2b::schema::SandboxForkRequestE2b;
use crate::fork_operations::{
    AdmitForkOperation, AdmitForkOperationError, ForkAvailabilityConfig, ForkRequest,
    ForkTargetOutcome, admit_locked_fork_operation, begin_fork_operation_admission,
    observe_fork_operation_idempotency,
};
use crate::middleware::auth::{AuthEcwActor, AuthOrg};
use crate::public_operations::{
    CreatePublicOperationResult, ForkOperationOutcome, ObservePublicOperationIdempotency,
    PublicOperationStore, PublicOperationStoreError,
};

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const INITIAL_FORK_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_FORK_POLL_INTERVAL: Duration = Duration::from_secs(1);
const FORK_HTTP_WAIT_SLACK: Duration = Duration::from_secs(5);

fn fork_http_wait_timeout(hard_deadline: Duration) -> Duration {
    hard_deadline.saturating_add(FORK_HTTP_WAIT_SLACK)
}

#[cfg(test)]
const fn fork_admission_needs_client_slot(created: bool) -> bool {
    created
}

pub(super) async fn fork_sandbox(
    State(state): State<AppState>,
    MatchedEcwHost(ecw_host_pair): MatchedEcwHost,
    (Extension(AuthOrg(org)), Extension(AuthEcwActor(actor)), federated): (
        Extension<AuthOrg>,
        Extension<AuthEcwActor>,
        Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
    ),
    Path(source_sandbox_id): Path<String>,
    headers: HeaderMap,
    crate::e2b::E2bJson(request): crate::e2b::E2bJson<SandboxForkRequestE2b>,
) -> Result<(StatusCode, Json<serde_json::Value>), E2bApiError> {
    let actor = actor.ok_or_else(E2bApiError::unauthorized)?;
    let source_sandbox_id = parse_fork_source_sandbox_id(&source_sandbox_id)?;
    let request = ForkRequest::from_wire(request.timeout, request.count)
        .map_err(|error| E2bApiError::bad_request(error.to_string()))?;
    let idempotency_key = parse_idempotency_key(&headers)?;
    let store = PublicOperationStore::new(state.pool.clone());
    let federated_owner = super::lifecycle::federated_owner_ref(&federated);
    // Malformed federated material is rejected up front even when the source
    // turns out to be a legacy local row that persists no grant.
    super::lifecycle::request_federated_service_grant(&headers, federated_owner)?;

    // Replay before reading the source row. A completed fork remains valid for
    // the operation TTL even if its source sandbox has since been deleted.
    // Federated operations live in the upstream owner's Space organization,
    // not in the trusted integration API key's organization; a fork of a legacy
    // local source is recorded under the key's organization instead. Records
    // are scoped per organization, so one key can exist in both: an exact match
    // anywhere is the replay, and a mismatch only counts in the namespace this
    // request is recorded under, which is known once the source's owner is
    // resolved below.
    let replay_org_id = resolve_fork_replay_org_id(&state.pool, org.id, federated_owner).await?;
    let mut replay_namespaces = Vec::new();
    if let Some(org_id) = replay_org_id {
        replay_namespaces.push((org_id, federated_owner.map(|owner| &owner.subject_id)));
    }
    if federated_owner.is_some() {
        replay_namespaces.push((org.id, None));
    }
    let mut probes = Vec::with_capacity(replay_namespaces.len());
    for (replay_org_id, replay_subject) in replay_namespaces {
        let observation = observe_fork_operation_idempotency(
            &store,
            replay_org_id,
            source_sandbox_id,
            request,
            replay_subject,
            &idempotency_key,
        )
        .await
        // Through the admission mapper, not `fork_internal_error`: this probe
        // runs *before* admission, so a 500 here makes the contention-aware
        // path below unreachable for exactly the callers that have an
        // idempotency key — the ones whose retry would have found the committed
        // record and returned the deduped answer.
        .map_err(|error| fork_admission_error("observe fork replay", error))?;
        probes.push((replay_org_id, observation));
    }
    let conflicting_namespaces = match resolve_replay_probes(probes) {
        ReplayProbe::Existing {
            org_id: replay_org_id,
            operation_id,
        } => {
            let wait_timeout =
                persisted_fork_wait_timeout(&store, replay_org_id, operation_id).await?;
            let outcome =
                wait_for_fork_outcome(store, replay_org_id, operation_id, wait_timeout).await?;
            return render_fork_outcome(&state.ecw_credential_keyring, outcome);
        }
        ReplayProbe::Conflicts(namespaces) => namespaces,
    };

    let scope = super::ecw_access_scope(&org, &federated);
    let source =
        super::fetch_scoped_sandbox(&state, &scope, &source_sandbox_id.to_string()).await?;
    let operation_actor_user_id =
        fork_execution_actor_id(actor.id, actor.org_id, source.user_id, source.org_id);
    // A legacy local source keeps local semantics through the bridge: the fork
    // stays in the key org with no federated owner, grant, or subject.
    let federated_owner = fork_owner_for_source(federated_owner, source.owner_source);
    let (request_owner_org_id, federated_service_grant) = match federated_owner {
        Some(_) => {
            prepare_fork_owner(
                &state.pool,
                org.id,
                replay_org_id,
                &headers,
                federated_owner,
            )
            .await?
        }
        None => (org.id, None),
    };
    if conflicting_namespaces.contains(&request_owner_org_id) {
        return Err(fork_idempotency_conflict());
    }
    let admission = begin_fork_operation_admission(
        &store,
        request_owner_org_id,
        source_sandbox_id,
        request,
        federated_owner.map(|owner| &owner.subject_id),
        &idempotency_key,
    )
    .await
    .map_err(|error| fork_admission_error("begin fork admission", error))?;
    let observation = admission.observation().clone();
    let (operation_id, client_slot, wait_timeout) =
        match observation {
            ObservePublicOperationIdempotency::Existing(id) => {
                admission
                    .finish()
                    .await
                    .map_err(|error| fork_store_error("finish existing fork admission", error))?;
                (
                    id,
                    None,
                    persisted_fork_wait_timeout(&store, request_owner_org_id, id).await?,
                )
            }
            ObservePublicOperationIdempotency::Conflict { .. } => {
                admission.finish().await.map_err(|error| {
                    fork_store_error("finish conflicting fork admission", error)
                })?;
                return Err(fork_idempotency_conflict());
            }
            ObservePublicOperationIdempotency::Missing => {
                admission
                    .finish()
                    .await
                    .map_err(|error| fork_store_error("finish missing fork admission", error))?;

                let availability = ForkAvailabilityConfig::from_env().map_err(|error| {
                    tracing::warn!(%error, "sandbox fork route is unavailable");
                    E2bApiError::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Sandbox fork is unavailable",
                    )
                })?;
                let wait_timeout = fork_http_wait_timeout(availability.hard_deadline());
                let client_id = request_client_id(&headers);
                let client_slot = acquire_client_slot(
                    &state,
                    pplx_space_metrics::sandbox::api::client_limit::Operation::Fork,
                    client_id.as_deref(),
                    Some(wait_timeout),
                )
                .await?;
                let admission = begin_fork_operation_admission(
                    &store,
                    request_owner_org_id,
                    source_sandbox_id,
                    request,
                    federated_owner.map(|owner| &owner.subject_id),
                    &idempotency_key,
                )
                .await
                .map_err(|error| fork_admission_error("begin final fork admission", error))?;
                match admission.observation().clone() {
                    ObservePublicOperationIdempotency::Existing(id) => {
                        admission.finish().await.map_err(|error| {
                            fork_store_error("finish raced fork admission", error)
                        })?;
                        drop(client_slot);
                        (
                            id,
                            None,
                            persisted_fork_wait_timeout(&store, request_owner_org_id, id).await?,
                        )
                    }
                    ObservePublicOperationIdempotency::Conflict { .. } => {
                        admission.finish().await.map_err(|error| {
                            fork_store_error("finish conflicting final fork admission", error)
                        })?;
                        return Err(fork_idempotency_conflict());
                    }
                    ObservePublicOperationIdempotency::Missing => {
                        match admit_locked_fork_operation(
                            admission,
                            AdmitForkOperation {
                                org_id: request_owner_org_id,
                                actor_user_id: Some(operation_actor_user_id),
                                source_sandbox_id,
                                request,
                                ecw_host_domain: &ecw_host_pair.host,
                                hard_deadline: availability.hard_deadline(),
                                federated_owner,
                                federated_service_grant: federated_service_grant.as_ref(),
                                idempotency_key: &idempotency_key,
                            },
                        )
                        .await
                        .map_err(|error| fork_admission_error("admit fork operation", error))?
                        {
                            CreatePublicOperationResult::Created(id) => {
                                (id, Some(client_slot), wait_timeout)
                            }
                            CreatePublicOperationResult::Existing(id) => {
                                drop(client_slot);
                                (id, None, wait_timeout)
                            }
                            CreatePublicOperationResult::IdempotencyConflict { .. } => {
                                return Err(fork_idempotency_conflict());
                            }
                            CreatePublicOperationResult::AdmissionDenied => {
                                return Err(E2bApiError::new(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "Sandbox fork operation admission is unavailable",
                                ));
                            }
                        }
                    }
                }
            }
        };
    if client_slot.is_some() {
        emit_sandbox_event(
            &source_sandbox_id.to_string(),
            SandboxEventType::ForkRequested,
            SandboxEventSource::Api,
            SandboxEventDetails::default().operation(operation_id.to_string()),
        );
    }
    let outcome =
        wait_for_fork_outcome(store, request_owner_org_id, operation_id, wait_timeout).await?;
    drop(client_slot);

    render_fork_outcome(&state.ecw_credential_keyring, outcome)
}

async fn prepare_fork_owner(
    pool: &sqlx::PgPool,
    local_org_id: OrgId,
    replay_org_id: Option<OrgId>,
    headers: &HeaderMap,
    federated_owner: Option<&crate::federated_identity::FederatedOwner>,
) -> Result<
    (
        OrgId,
        Option<crate::federated_identity::FederatedServiceGrant>,
    ),
    E2bApiError,
> {
    let service_grant =
        super::lifecycle::request_federated_service_grant(headers, federated_owner)?;
    let owner_org_id = match (replay_org_id, federated_owner) {
        (Some(org_id), _) => org_id,
        (None, Some(owner)) => {
            crate::federated_identity::materialize_owner(pool, owner)
                .await
                .map_err(fork_owner_error)?
                .org_id
        }
        (None, None) => local_org_id,
    };
    Ok((owner_org_id, service_grant))
}

fn parse_fork_source_sandbox_id(value: &str) -> Result<SandboxId, E2bApiError> {
    value
        .parse::<SandboxId>()
        .map_err(|_| E2bApiError::not_found("Sandbox"))
}

/// Identity storage failures belong to the API, not the sandbox fleet.
fn fork_owner_error(error: crate::federated_identity::MaterializeOwnerError) -> E2bApiError {
    if error.is_lock_contention() {
        return super::translate::controller_error(error.into());
    }
    fork_internal_error("materialize federated fork owner", error)
}

/// Test-only handle on [`fork_owner_error`].
#[cfg(test)]
pub(super) fn fork_owner_error_for_test(
    error: crate::federated_identity::MaterializeOwnerError,
) -> E2bApiError {
    fork_owner_error(error)
}

fn fork_internal_error(context: &'static str, error: impl std::fmt::Display) -> E2bApiError {
    tracing::error!(context, error = %error, "fork request failed");
    E2bApiError::internal("Internal server error")
}

/// [`fork_internal_error`] for the admission steps, which can lose a race on
/// the fork-source advisory lock or the idempotency record.
///
/// Concurrent ECW fork requests sharing an idempotency key serialize on that
/// advisory lock; under a role-level `lock_timeout` the loser aborts with
/// `55P03` instead of waiting, and its retry finds the committed record and
/// gets the deduped answer. Reporting it as a 500 tells the SDK not to bother.
fn fork_admission_error(context: &'static str, error: AdmitForkOperationError) -> E2bApiError {
    match error {
        AdmitForkOperationError::Store(store) => fork_store_error(context, store),
        other => fork_internal_error(context, other),
    }
}

/// Test-only handle on [`fork_store_error`], so the ECW contention mapping can
/// be asserted without standing up a fork request.
#[cfg(test)]
pub(super) fn fork_store_error_for_test(
    context: &'static str,
    error: PublicOperationStoreError,
) -> E2bApiError {
    fork_store_error(context, error)
}

/// Test-only handle on [`fork_sql_error`], which the replay-owner and deadline
/// reads take.
#[cfg(test)]
pub(super) fn fork_sql_error_for_test(context: &'static str, error: &sqlx::Error) -> E2bApiError {
    fork_sql_error(context, error)
}

/// Test-only handle on [`fork_admission_error`], which the replay probe takes.
#[cfg(test)]
pub(super) fn fork_admission_error_for_test(
    context: &'static str,
    error: AdmitForkOperationError,
) -> E2bApiError {
    fork_admission_error(context, error)
}

/// [`fork_internal_error`] for a bare `sqlx` error on the fork path.
///
/// The statements here take no lock of their own, which is why they were left
/// on the 500 — nothing in the SQL says they can block. They still can: a plain
/// `SELECT` waits on `ACCESS EXCLUSIVE` from DDL on the table it reads, and
/// under the role-level bound that wait aborts with `55P03`.
fn fork_sql_error(context: &'static str, error: &sqlx::Error) -> E2bApiError {
    if pplx_space_common_svc::sql::contention::is_lock_contention(error) {
        tracing::warn!(context, error = %error, "fork statement lost a lock race; retryable");
        return E2bApiError::lock_contention();
    }
    fork_internal_error(context, error)
}

/// As [`fork_admission_error`], for the admission steps that surface the store
/// error directly.
fn fork_store_error(context: &'static str, error: PublicOperationStoreError) -> E2bApiError {
    if let PublicOperationStoreError::Sql(sql) = &error
        && pplx_space_common_svc::sql::contention::is_lock_contention(sql)
    {
        tracing::warn!(context, error = %error, "fork admission lost a lock race; retryable");
        return E2bApiError::lock_contention();
    }
    fork_internal_error(context, error)
}

fn render_fork_outcome(
    keyring: &pplx_space_common_svc::ecw_credential::EcwCredentialKeyring,
    outcome: ForkOperationOutcome,
) -> Result<(StatusCode, Json<serde_json::Value>), E2bApiError> {
    match outcome {
        ForkOperationOutcome::Succeeded(result) => {
            let stored = result
                .as_array()
                .ok_or_else(|| E2bApiError::internal("Stored sandbox fork result is invalid"))?;
            let results = stored
                .iter()
                .cloned()
                .map(serde_json::from_value::<ForkTargetOutcome>)
                .map(|outcome| match outcome {
                    Ok(ForkTargetOutcome::Succeeded {
                        sandbox_id,
                        template_id,
                        ecw_host_domain,
                        credential_epoch,
                    }) => Ok(crate::e2b::build_fork_sandbox_result(
                        keyring,
                        sandbox_id,
                        template_id,
                        ecw_host_domain.as_deref(),
                        credential_epoch,
                    )),
                    Ok(ForkTargetOutcome::Failed { code, message }) => {
                        Ok(crate::e2b::build_fork_error_result(
                            code,
                            public_fork_target_failure_message(code, &message),
                        ))
                    }
                    Err(error) => Err(fork_internal_error("decode stored fork result", error)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((StatusCode::CREATED, Json(serde_json::Value::Array(results))))
        }
        ForkOperationOutcome::Failed { code, error } => {
            let stored_message = error
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Sandbox fork failed");
            Err(E2bApiError::new(
                code.as_ref().map_or(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    fork_terminal_failure_status,
                ),
                public_fork_failure_message(code.as_ref(), stored_message),
            ))
        }
        ForkOperationOutcome::Pending => unreachable!("wait loop returns only terminal outcomes"),
    }
}

fn public_fork_failure_message<'a>(
    code: Option<&PublicErrorCode>,
    stored_message: &'a str,
) -> &'a str {
    // A code this build does not know (`Other`) was persisted by a newer or
    // older peer, so nothing is known about what its message may contain;
    // redact it like an internal failure.
    match code {
        None
        | Some(
            PublicErrorCode::Internal
            | PublicErrorCode::OperationFailed
            | PublicErrorCode::Other(_),
        ) => "Sandbox fork failed",
        Some(_) => stored_message,
    }
}

fn public_fork_target_failure_message(code: i32, stored_message: &str) -> &str {
    if (500..=599).contains(&code) {
        "Sandbox fork failed"
    } else {
        stored_message
    }
}

fn fork_terminal_failure_status(code: &PublicErrorCode) -> StatusCode {
    match code {
        PublicErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
        PublicErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        PublicErrorCode::Forbidden => StatusCode::FORBIDDEN,
        PublicErrorCode::NotFound => StatusCode::NOT_FOUND,
        PublicErrorCode::Conflict | PublicErrorCode::IdempotencyConflict => StatusCode::CONFLICT,
        PublicErrorCode::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        PublicErrorCode::CapacityUnavailable | PublicErrorCode::ServiceUnavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        PublicErrorCode::RateLimited | PublicErrorCode::QuotaExceeded => {
            StatusCode::TOO_MANY_REQUESTS
        }
        // A code this build does not know was stored by a newer server; the
        // ECW wrapper cannot classify it more precisely than a server error.
        PublicErrorCode::OperationFailed
        | PublicErrorCode::Internal
        | PublicErrorCode::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn wait_for_fork_outcome(
    store: PublicOperationStore,
    org_id: OrgId,
    operation_id: pplx_space_public_api::OperationId,
    max_wait: Duration,
) -> Result<ForkOperationOutcome, E2bApiError> {
    poll_fork_outcome(max_wait, || store.fetch_fork_outcome(org_id, operation_id)).await
}

async fn poll_fork_outcome<F>(
    max_wait: Duration,
    mut fetch: impl FnMut() -> F,
) -> Result<ForkOperationOutcome, E2bApiError>
where
    F: std::future::Future<
            Output = Result<Option<ForkOperationOutcome>, PublicOperationStoreError>,
        >,
{
    tokio::time::timeout(max_wait, async {
        let mut poll_interval = INITIAL_FORK_POLL_INTERVAL;
        loop {
            if let Some(outcome) = fetch()
                .await
                // The poll re-reads the operation row on every tick, so it is
                // the longest-lived reader in the handler and the most likely
                // to meet a concurrent writer or DDL. A 500 out of the poll
                // discards a fork that is still converging.
                .map_err(|error| fork_store_error("fetch fork outcome", error))?
            {
                match outcome {
                    ForkOperationOutcome::Pending => {}
                    terminal => return Ok::<_, E2bApiError>(terminal),
                }
            }
            tokio::time::sleep(poll_interval).await;
            poll_interval = next_fork_poll_interval(poll_interval);
        }
    })
    .await
    .map_err(|_| E2bApiError::new(StatusCode::GATEWAY_TIMEOUT, "Sandbox fork timed out"))?
}

fn next_fork_poll_interval(current: Duration) -> Duration {
    current.saturating_mul(2).min(MAX_FORK_POLL_INTERVAL)
}

fn fork_idempotency_conflict() -> E2bApiError {
    E2bApiError::new(
        StatusCode::CONFLICT,
        "Idempotency-Key was already used with a different fork request",
    )
}

/// Result of probing every organization namespace a replay could live in.
#[derive(Debug, PartialEq, Eq)]
enum ReplayProbe {
    /// An exact match is the replay, whichever namespace recorded it.
    Existing {
        org_id: OrgId,
        operation_id: pplx_space_public_api::OperationId,
    },
    /// Namespaces holding this key with a different fingerprint. Only the one
    /// the request is recorded under makes that a conflict.
    Conflicts(Vec<OrgId>),
}

fn resolve_replay_probes(probes: Vec<(OrgId, ObservePublicOperationIdempotency)>) -> ReplayProbe {
    let mut conflicts = Vec::new();
    for (org_id, observation) in probes {
        match observation {
            ObservePublicOperationIdempotency::Existing(operation_id) => {
                return ReplayProbe::Existing {
                    org_id,
                    operation_id,
                };
            }
            ObservePublicOperationIdempotency::Conflict { .. } => conflicts.push(org_id),
            ObservePublicOperationIdempotency::Missing => {}
        }
    }
    ReplayProbe::Conflicts(conflicts)
}

fn fork_owner_for_source(
    federated_owner: Option<&crate::federated_identity::FederatedOwner>,
    source_owner_source: pplx_space_common_core::types::SandboxOwnerSource,
) -> Option<&crate::federated_identity::FederatedOwner> {
    federated_owner.filter(|_| {
        matches!(
            source_owner_source,
            pplx_space_common_core::types::SandboxOwnerSource::FederatedIdentity
        )
    })
}

fn fork_execution_actor_id(
    authenticated_actor_id: UserId,
    authenticated_org_id: OrgId,
    source_owner_user_id: UserId,
    source_org_id: OrgId,
) -> UserId {
    if authenticated_org_id == source_org_id {
        authenticated_actor_id
    } else {
        source_owner_user_id
    }
}

async fn resolve_fork_replay_org_id(
    pool: &sqlx::PgPool,
    api_key_org_id: OrgId,
    federated_owner: Option<&crate::federated_identity::FederatedOwner>,
) -> Result<Option<OrgId>, E2bApiError> {
    let Some(owner) = federated_owner else {
        return Ok(Some(api_key_org_id));
    };

    sqlx::query_scalar(pplx_space_common_core::sql!(
        "SELECT id
         FROM organizations
         WHERE owner_source = 'federated_identity'
           AND upstream_issuer = $1
           AND upstream_org_id = $2"
    ))
    .bind(owner.issuer.as_str())
    .bind(owner.org_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| fork_sql_error("resolve fork replay owner", &error))
}

async fn persisted_fork_wait_timeout(
    store: &PublicOperationStore,
    org_id: OrgId,
    operation_id: pplx_space_public_api::OperationId,
) -> Result<Duration, E2bApiError> {
    store
        .fetch_fork_hard_deadline(org_id, operation_id)
        .await
        .map_err(|error| fork_store_error("read fork deadline", error))?
        .map(fork_http_wait_timeout)
        .ok_or_else(|| {
            fork_internal_error(
                "read fork deadline",
                "fork operation disappeared during replay",
            )
        })
}

fn parse_idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, E2bApiError> {
    let value = headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .ok_or_else(|| E2bApiError::bad_request("Idempotency-Key header is required"))?
        .to_str()
        .map_err(|_| E2bApiError::bad_request("Idempotency-Key must be valid ASCII"))?;
    IdempotencyKey::parse(value)
        .map_err(|error| E2bApiError::bad_request(format!("invalid Idempotency-Key: {error}")))
}

#[cfg(test)]
mod tests;
