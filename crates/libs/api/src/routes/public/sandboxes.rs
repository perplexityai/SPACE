use pplx_space_common_core::types::{NodeId, OrgId, SandboxId, UserId};

use pplx_space_common_core::sql;
use std::{convert::Infallible, str::FromStr, time::Duration};

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, IdempotencyKey, SandboxLifecycle, SubstrateKind,
};
use pplx_space_common_svc::types::{
    FACET_SANDBOX_DESIRED_STATE, SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY,
    SANDBOX_FAILURE_EPISODE_KEY, SANDBOX_FAILURE_EXECUTION_GENERATION_KEY,
    SANDBOX_FAILURE_POD_UID_KEY,
};
use pplx_space_metrics::warm_pool::AdmissionClaimOutcome;
use pplx_space_public_api::{
    ConditionStatus, CreateSandboxRequest, OperationId, OperationKind, OperationState,
    OperationTargetType, PageCursor, PublicErrorCode, PublicOperationRecord, PublicPage,
    PublicSandbox, PublicSandboxId, PublicSandboxMetadata, PublicSandboxMetadataValue,
    RequestFingerprint, RollbackSandboxRequest, SandboxCondition, SandboxConditionType,
    SandboxFailureCode, SandboxIdleAction, SandboxLifecycleAction, SandboxLifecycleState,
    SandboxOperationResponse, SandboxResources, SandboxStatsResponse, UpdateSandboxRequest,
    routes as native_routes,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, types::Json as SqlJson};
use time::OffsetDateTime;

use crate::{
    controllers::{
        ControllerError,
        sandbox::{
            AdmissionPoolPlan, SandboxController, fetch_sandbox,
            list_page::{
                ListPage, MAX_SANDBOX_LIST_PAGE_BYTES, PageWalkParams,
                SANDBOX_LIST_ROW_OVERHEAD_BYTES, page_rows_cte,
            },
        },
        substrate::AdmissionPoolClaim,
    },
    middleware::auth::{AuthFederatedOwner, AuthOrg, AuthUser},
    models::{Sandbox, SandboxMetadata as ControllerSandboxMetadata, SandboxMetadataValue},
    public_operations::{
        ApiProduct, CreateClaimedPublicOperationResult, CreatePublicOperation,
        CreatePublicOperationIdempotency, CreatePublicOperationResult, HttpOperationMethod,
        ObservePublicOperationIdempotency, OperationPathTemplate, PublicOperationAdmission,
        PublicOperationStore, SandboxQuota, SandboxQuotaOutcome, SandboxQuotaRefusal,
    },
    public_sandbox_operations::{
        CreateSandboxOperationPayload, SandboxLifecycleOperationPayload,
        UpdateSandboxOperationPayload, controller_create_request,
    },
    public_sandbox_projection::public_lifecycle_state as project_public_lifecycle_state,
    routes::{AppState, metrics::RouteTemplateName},
};

use super::{
    cursors::{CursorKind, decode_cursor, encode_cursor},
    error::{PublicApiError, with_retry_after_hint},
};

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const CREATE_SANDBOX_PATH_TEMPLATE: &str = native_routes::CREATE_SANDBOX.path();
const UPDATE_SANDBOX_PATH_TEMPLATE: &str = native_routes::UPDATE_SANDBOX.path();
const DEFAULT_SANDBOX_LIST_LIMIT: u16 = 100;
const MAX_SANDBOX_LIST_LIMIT: u16 = 1000;
const SANDBOX_METRICS_STREAM_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_NATIVE_IDLE_TIMEOUT_SECS: u64 = 6 * 60 * 60;
/// Env master for claiming a warm-pool slot at native-create admission.
const NATIVE_CREATE_POOL_CLAIM_ENV: &str = "SPACE_NATIVE_CREATE_POOL_CLAIM_ENABLED";
const DEFAULT_NATIVE_CREATE_POOL_CLAIM_ENABLED: bool = false;
/// The constant cluster subject the admission-claim flag is evaluated on.
const NATIVE_CREATE_POOL_CLAIM_FLAG_SUBJECT: &str = "space-cluster";

pub async fn create_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    federated: Option<Extension<AuthFederatedOwner>>,
    headers: HeaderMap,
    Json(request): Json<CreateSandboxRequest>,
) -> Result<impl IntoResponse, PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    let template_name = request.template.name().as_str().to_owned();
    let owner = federated
        .as_ref()
        .map(|Extension(AuthFederatedOwner(owner))| owner);
    let grant = super::owner_scope::required_grant(&headers, owner)?;
    let selected = state
        .sandbox_controller
        .select_egress_profiles(&headers, owner.is_some())
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    let federated_create = owner.zip(grant.as_ref()).map(|(owner, grant)| {
        let mut context =
            crate::fork_operations::ForkFederatedCreateContext::from_request(owner, grant);
        context.egress_profiles = Some(selected);
        context
    });
    let pool_claims =
        native_create_pool_claim_enabled(&state.feature_flags).then(|| NativeCreatePoolClaims {
            controller: &state.sandbox_controller,
            inline: inline_create::InlineCreate::from_env(&state.pool, &state.sandbox_controller),
        });
    let response = create_native_sandbox_operation(
        &state.pool,
        org.id,
        actor.id,
        &idempotency_key,
        request,
        federated_create,
        pool_claims,
        state
            .native_sandbox_limits
            .for_create(org.id, owner.is_some().then_some(actor.id)),
    )
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Extension(RouteTemplateName(template_name)),
        Json(response),
    ))
}

/// Whether native creates claim a warm-pool slot at admission: the
/// `SPACE_NATIVE_CREATE_POOL_CLAIM_ENABLED` env master (off by default) under
/// the `space-native-create-pool-claim` flag (falls back off), by the
/// env-master-first gate policy.
fn native_create_pool_claim_enabled(
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
) -> bool {
    native_create_pool_claim_gate(
        pplx_space_common_core::env::env_flag_var(
            NATIVE_CREATE_POOL_CLAIM_ENV,
            DEFAULT_NATIVE_CREATE_POOL_CLAIM_ENABLED,
        ),
        feature_flags,
    )
}

fn native_create_pool_claim_gate(
    env_enabled: bool,
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
) -> bool {
    feature_flags.gate_enabled(
        env_enabled,
        pplx_space_feature_flags::SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG,
        NATIVE_CREATE_POOL_CLAIM_FLAG_SUBJECT,
    )
}

/// Admission-time warm-pool claiming for native creates.
#[derive(Clone)]
pub(crate) struct NativeCreatePoolClaims<'a> {
    pub(crate) controller: &'a SandboxController,
    /// Run a create that won a claim in this process instead of leaving it
    /// queued for the dispatcher. `None` when the inline kill switch is off.
    pub(crate) inline: Option<inline_create::InlineCreate>,
}

/// Record a native create.
///
/// `pool_claims` is `None` when admission-time pool claiming is off, which
/// is exactly the path that predates it: a fresh sandbox ID and no pool
/// calls. With a controller, an eligible new create claims a ready warm-pool
/// slot in the transaction that records it and takes the slot's reserved ID
/// as its public ID; the claim travels in the payload for the executor to
/// adopt. Anything else (ineligible, a pool or template miss, a claim
/// error) records the create with a fresh ID and no claim, as the disabled
/// path does.
///
/// With inline execution on, a create that won a claim is also claimed for
/// execution in that transaction, and runs here (see [`inline_create`])
/// before the response, up to a short deadline. Every other create is
/// recorded queued for the dispatcher, as before.
///
/// `sandbox_quota` is the caller's live-sandbox limits, `None` when it has
/// none. A new create that would exceed one is refused with a non-retryable
/// 429 naming it and records nothing; a replay of an admitted create still
/// answers with it.
#[expect(
    clippy::too_many_arguments,
    reason = "the native create's request-scoped inputs; bundling them only moves the list"
)]
pub(crate) async fn create_native_sandbox_operation(
    pool: &PgPool,
    org_id: OrgId,
    actor_user_id: UserId,
    idempotency_key: &IdempotencyKey,
    request: CreateSandboxRequest,
    federated_create: Option<crate::fork_operations::ForkFederatedCreateContext>,
    pool_claims: Option<NativeCreatePoolClaims<'_>>,
    sandbox_quota: Option<SandboxQuota>,
) -> Result<SandboxOperationResponse, PublicApiError> {
    let (request, mut fingerprint) = prepare_native_create_request(request)?;
    if let Some(profiles) = federated_create
        .as_ref()
        .and_then(|context| context.egress_profiles.as_ref())
        // Preserve pre-profile fingerprints for empty selections without changing the payload.
        .filter(|profiles| !profiles.is_empty())
    {
        fingerprint = request_fingerprint(&serde_json::json!({
            "request": fingerprint, "egress_profiles": profiles,
        }))?;
    }
    let inline = pool_claims
        .as_ref()
        .and_then(|claims| claims.inline.clone());
    let plan = match pool_claims.as_ref().map(|claims| claims.controller) {
        None => NativeCreatePoolPlan::Unclaimed(AdmissionClaimOutcome::Disabled),
        Some(controller) => {
            let controller_request = controller_create_request(request.clone(), None);
            match controller
                .plan_admission_pool_claim(pool, org_id, &controller_request)
                .await
            {
                AdmissionPoolPlan::Eligible {
                    template_release_id,
                } => NativeCreatePoolPlan::Claim {
                    controller,
                    template_release_id,
                },
                AdmissionPoolPlan::TemplateMiss => {
                    NativeCreatePoolPlan::Unclaimed(AdmissionClaimOutcome::Miss)
                }
                AdmissionPoolPlan::Ineligible => {
                    NativeCreatePoolPlan::Unclaimed(AdmissionClaimOutcome::Ineligible)
                }
            }
        }
    };

    let store = PublicOperationStore::new(pool.clone());
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new(CREATE_SANDBOX_PATH_TEMPLATE),
        key: idempotency_key,
        request_fingerprint: &fingerprint,
    };
    let fresh_sandbox_id = PublicSandboxId::new();
    let create =
        |sandbox_id: PublicSandboxId, sandbox_id_string, pool_claim: Option<AdmissionPoolClaim>| {
            CreatePublicOperation {
                org_id,
                actor_user_id: Some(actor_user_id),
                api_product: ApiProduct::Native,
                kind: OperationKind::CreateSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(sandbox_id_string),
                payload_json: CreateSandboxOperationPayload {
                    sandbox_id,
                    request: request.clone(),
                    // Placement is operator-only (admin surface); the public API never pins.
                    node_pool: None,
                    federated_create: federated_create.clone(),
                    pool_claim: pool_claim.map(|claim| claim.claim.into()),
                }
                .into_json(),
                idempotency: Some(idempotency.clone()),
                traceparent: crate::public_operations::request_traceparent(),
            }
        };

    // The claim on a create recorded for inline execution.
    let mut inline_claim = None;
    let (sandbox_id, outcome, result) = match plan {
        NativeCreatePoolPlan::Unclaimed(outcome) => {
            let sandbox_id_string = fresh_sandbox_id.to_string();
            let result = create_unclaimed_operation(
                &store,
                &idempotency,
                sandbox_quota,
                create(fresh_sandbox_id, &sandbox_id_string, None),
            )
            .await?;
            (fresh_sandbox_id, outcome, result)
        }
        NativeCreatePoolPlan::Claim {
            controller,
            template_release_id,
        } => {
            // Take the idempotency lock before touching the pool: a retry or
            // a conflicting reuse of the key answers without claiming, and a
            // concurrent same-key request waits here until this one commits.
            let mut admission = store
                .begin_operation_admission(org_id, ApiProduct::Native, &idempotency)
                .await?;
            // A retry or a conflict records no create, so its outcome is
            // never counted.
            match admission.observation().clone() {
                ObservePublicOperationIdempotency::Existing(operation_id) => {
                    admission.finish().await?;
                    (
                        fresh_sandbox_id,
                        AdmissionClaimOutcome::Hit,
                        CreatePublicOperationResult::Existing(operation_id),
                    )
                }
                ObservePublicOperationIdempotency::Conflict {
                    operation_id,
                    expected_fingerprint,
                } => {
                    drop(admission);
                    (
                        fresh_sandbox_id,
                        AdmissionClaimOutcome::Hit,
                        CreatePublicOperationResult::IdempotencyConflict {
                            operation_id,
                            expected_fingerprint,
                        },
                    )
                }
                ObservePublicOperationIdempotency::Missing => {
                    // Before the claim, so a refused create never takes a
                    // slot and one waiting for its quota lock holds none.
                    if let Some(quota) = sandbox_quota {
                        admit_within_sandbox_quota(&mut admission, quota).await?;
                    }
                    match controller
                        .claim_admission_pool_slot(admission.transaction(), template_release_id)
                        .await
                    {
                        // The create's public ID is the slot's reserved ID:
                        // the pod the slot booted already runs under it. Only
                        // a newly recorded create keeps the claim; any other
                        // result rolls it back with the transaction.
                        Ok(Some(claim)) => {
                            let sandbox_id = PublicSandboxId::from(claim.sandbox_id);
                            let sandbox_id_string = sandbox_id.to_string();
                            let input = create(sandbox_id, &sandbox_id_string, Some(claim));
                            let result = if inline.is_some() {
                                match admission.create_claimed_or_roll_back(input).await? {
                                    CreateClaimedPublicOperationResult::Claimed {
                                        claim_token,
                                        operation,
                                    } => {
                                        let operation_id = operation.id;
                                        inline_claim = Some((claim_token, operation));
                                        CreatePublicOperationResult::Created(operation_id)
                                    }
                                    CreateClaimedPublicOperationResult::Existing(operation_id) => {
                                        CreatePublicOperationResult::Existing(operation_id)
                                    }
                                    CreateClaimedPublicOperationResult::IdempotencyConflict {
                                        operation_id,
                                        expected_fingerprint,
                                    } => CreatePublicOperationResult::IdempotencyConflict {
                                        operation_id,
                                        expected_fingerprint,
                                    },
                                    CreateClaimedPublicOperationResult::AdmissionDenied => {
                                        CreatePublicOperationResult::AdmissionDenied
                                    }
                                }
                            } else {
                                admission.create_or_roll_back(input).await?
                            };
                            (sandbox_id, AdmissionClaimOutcome::Hit, result)
                        }
                        Ok(None) => {
                            let sandbox_id_string = fresh_sandbox_id.to_string();
                            let result = admission
                                .create_or_roll_back(create(
                                    fresh_sandbox_id,
                                    &sandbox_id_string,
                                    None,
                                ))
                                .await?;
                            (fresh_sandbox_id, AdmissionClaimOutcome::Miss, result)
                        }
                        Err(error) => {
                            // A failed statement aborts the transaction, so
                            // give it up and record the create cold.
                            tracing::warn!(
                                %error,
                                "warm-pool claim at admission failed; admitting the create cold"
                            );
                            drop(admission);
                            let sandbox_id_string = fresh_sandbox_id.to_string();
                            let result = create_unclaimed_operation(
                                &store,
                                &idempotency,
                                sandbox_quota,
                                create(fresh_sandbox_id, &sandbox_id_string, None),
                            )
                            .await?;
                            (fresh_sandbox_id, AdmissionClaimOutcome::Miss, result)
                        }
                    }
                }
            }
        }
    };

    let mut response = match result {
        CreatePublicOperationResult::Created(operation_id) => {
            pplx_space_metrics::warm_pool::admission_claim(outcome);
            // The path names no sandbox yet; the request span learns it here.
            pplx_space_common_svc::record_sandbox_id(
                &tracing::Span::current(),
                &sandbox_id.to_string(),
            );
            SandboxOperationResponse {
                operation_id,
                sandbox_id,
                operation: None,
            }
        }
        CreatePublicOperationResult::Existing(operation_id) => {
            if let Some(context) = &federated_create {
                store
                    .renew_queued_native_create_grant(org_id, actor_user_id, operation_id, context)
                    .await?;
            }
            sandbox_operation_response(&store, org_id, operation_id).await?
        }
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key was already used with a different request body",
            ));
        }
        CreatePublicOperationResult::AdmissionDenied => return Err(PublicApiError::internal()),
    };

    if let Some((inline, (claim_token, operation))) = inline.zip(inline_claim)
        && inline.run(store.clone(), claim_token, operation).await
    {
        response.operation = finished_operation(&store, org_id, response.operation_id).await?;
    }

    Ok(response)
}

/// The record of an operation that already reached a terminal state, so the
/// create can answer with its outcome instead of asking the client to poll.
/// A retryable outcome released back to the dispatcher is still queued and
/// answers as before.
async fn finished_operation(
    store: &PublicOperationStore,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<Option<PublicOperationRecord>, PublicApiError> {
    Ok(store
        .fetch_operation(org_id, operation_id)
        .await?
        .filter(|operation| {
            matches!(
                operation.state,
                OperationState::Succeeded
                    | OperationState::Failed
                    | OperationState::Canceled
                    | OperationState::Expired
            )
        }))
}

/// Record a native create that takes no warm-pool slot, holding a new one to
/// the caller's quota when it has one. Without a quota this is the plain
/// create that predates it.
async fn create_unclaimed_operation(
    store: &PublicOperationStore,
    idempotency: &CreatePublicOperationIdempotency<'_>,
    sandbox_quota: Option<SandboxQuota>,
    input: CreatePublicOperation<'_>,
) -> Result<CreatePublicOperationResult, PublicApiError> {
    let Some(quota) = sandbox_quota else {
        return Ok(store.create_operation(input).await?);
    };
    let mut admission = store
        .begin_operation_admission(input.org_id, ApiProduct::Native, idempotency)
        .await?;
    admit_within_sandbox_quota(&mut admission, quota).await?;
    Ok(admission.create(input).await?)
}

/// Hold a genuinely new create to `quota` in its admission transaction,
/// which then holds the quota locks until it commits; a replay or key
/// conflict passes. Record the create through `admission` next; an error
/// drops it, rolling the transaction back.
async fn admit_within_sandbox_quota(
    admission: &mut PublicOperationAdmission,
    quota: SandboxQuota,
) -> Result<(), PublicApiError> {
    match admission.admit_new_create(quota).await? {
        SandboxQuotaOutcome::Admit => Ok(()),
        SandboxQuotaOutcome::Refuse(refusal) => Err(sandbox_quota_refused(refusal)),
        SandboxQuotaOutcome::Contended => Err(PublicApiError::lock_contention()),
    }
}

fn sandbox_quota_refused(refusal: SandboxQuotaRefusal) -> PublicApiError {
    const LIFTS: &str = "it lifts once a delete completes or an in-flight create ends";
    PublicApiError::sandbox_quota_exceeded(match refusal {
        SandboxQuotaRefusal::Org { limit } => {
            format!("organization sandbox limit of {limit} reached; {LIFTS}")
        }
        SandboxQuotaRefusal::Subject { limit } => {
            format!("sandbox limit of {limit} per user reached; {LIFTS}")
        }
    })
}

/// What admission does about a warm-pool slot for one native create.
enum NativeCreatePoolPlan<'a> {
    /// Record it with a fresh ID and no slot, for this reason.
    Unclaimed(AdmissionClaimOutcome),
    /// Claim a ready slot of this release if the create is new.
    Claim {
        controller: &'a SandboxController,
        template_release_id: uuid::Uuid,
    },
}

fn prepare_native_create_request(
    mut request: CreateSandboxRequest,
) -> Result<(CreateSandboxRequest, RequestFingerprint), PublicApiError> {
    let idle_timeout_secs = request
        .idle_timeout_secs()
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    pplx_space_public_api::limits::validate_create_request(&request)
        .map_err(PublicApiError::request_limit)?;
    // Hash the parsed client request before applying server defaults. This keeps
    // idempotent retries stable across rolling deploys where older API replicas
    // still hash the omitted policy as null.
    let fingerprint = request_fingerprint(&request)?;
    if let Some(timeout_secs) = idle_timeout_secs {
        let auto_pause = request.idle_action != Some(SandboxIdleAction::Delete);
        request.auto_lifecycle = Some(crate::e2b::access_idle_policy(
            timeout_secs,
            auto_pause,
            auto_pause,
        ));
        request.idle_timeout = None;
        request.idle_action = None;
    }
    Ok((with_native_create_defaults(request), fingerprint))
}

fn with_native_create_defaults(mut request: CreateSandboxRequest) -> CreateSandboxRequest {
    request.auto_lifecycle.get_or_insert_with(|| {
        crate::e2b::access_idle_policy(DEFAULT_NATIVE_IDLE_TIMEOUT_SECS, true, false)
    });
    request
}

pub async fn list_sandboxes(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    actor: Option<Extension<AuthUser>>,
    federated: Option<Extension<AuthFederatedOwner>>,
    Query(query): Query<ListSandboxesQuery>,
) -> Result<Json<PublicPage<PublicSandbox>>, PublicApiError> {
    let limit = query.limit();
    let cursor = query.cursor_parts()?;
    let subject_id = federated
        .map(|_| {
            actor
                .map(|Extension(AuthUser(actor))| actor.id)
                .ok_or_else(PublicApiError::internal)
        })
        .transpose()?;
    let rows = fetch_sandboxes_page(&state.pool, org.id, subject_id, cursor, limit).await?;
    Ok(Json(sandboxes_page(rows, limit)?))
}

pub async fn get_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(sandbox_id): Path<String>,
) -> Result<Json<PublicSandbox>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    Ok(Json(
        read_public_sandbox(&state.pool, org.id, sandbox_id).await?,
    ))
}

pub(crate) async fn read_public_sandbox(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
) -> Result<PublicSandbox, PublicApiError> {
    let Some(sandbox) = fetch_public_sandbox(pool, org_id, sandbox_id).await? else {
        return Err(PublicApiError::not_found("sandbox not found"));
    };
    public_sandbox_from_row(sandbox)
}

pub async fn get_sandbox_stats(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(sandbox_id): Path<String>,
) -> Result<Json<SandboxStatsResponse>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let Some(sandbox) = fetch_sandbox(&state.pool, sandbox_id.into()).await? else {
        return Err(PublicApiError::not_found("sandbox not found"));
    };
    if sandbox.org_id != org.id {
        return Err(PublicApiError::not_found("sandbox not found"));
    }
    let stats = state
        .sandbox_controller
        .stats(&state.pool, &sandbox, sandbox_id.into())
        .await
        .map_err(controller_error)?;
    Ok(Json(public_stats_response(sandbox_id, stats)))
}

pub async fn stream_sandbox_metrics(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(sandbox_id): Path<String>,
) -> Result<Response, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let Some(sandbox) = fetch_sandbox(&state.pool, sandbox_id.into()).await? else {
        return Err(PublicApiError::not_found("sandbox not found"));
    };
    if sandbox.org_id != org.id {
        return Err(PublicApiError::not_found("sandbox not found"));
    }

    let stream = sandbox_metrics_stream(state, sandbox, sandbox_id);
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

struct SandboxMetricsStreamState {
    state: AppState,
    sandbox: Sandbox,
    sandbox_id: PublicSandboxId,
    sleep_before_fetch: bool,
    finished: bool,
}

fn sandbox_metrics_stream(
    state: AppState,
    sandbox: Sandbox,
    sandbox_id: PublicSandboxId,
) -> impl futures_core::Stream<Item = Result<Event, Infallible>> {
    futures_util::stream::unfold(
        SandboxMetricsStreamState {
            state,
            sandbox,
            sandbox_id,
            sleep_before_fetch: false,
            finished: false,
        },
        |mut stream| async move {
            if stream.finished {
                return None;
            }
            if stream.sleep_before_fetch {
                tokio::time::sleep(SANDBOX_METRICS_STREAM_INTERVAL).await;
            }
            stream.sleep_before_fetch = true;

            let event = match stream
                .state
                .sandbox_controller
                .stats(
                    &stream.state.pool,
                    &stream.sandbox,
                    stream.sandbox_id.into(),
                )
                .await
                .map_err(controller_error)
                .map(|stats| public_stats_response(stream.sandbox_id, stats))
            {
                Ok(stats) => Event::default()
                    .event("metrics")
                    .data(serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string())),
                Err(_error) => {
                    stream.finished = true;
                    Event::default().event("error").data(
                        serde_json::json!({
                            "code": PublicErrorCode::ServiceUnavailable.as_str(),
                            "message": "sandbox metrics stream failed",
                            "retryable": true,
                        })
                        .to_string(),
                    )
                }
            };

            Some((Ok(event), stream))
        },
    )
}

/// Builds a page from the rows `fetch_sandboxes_page` returned: at most
/// `limit` sandboxes, with a cursor when more follow them.
fn sandboxes_page(
    rows: Vec<PublicSandboxRow>,
    limit: u16,
) -> Result<PublicPage<PublicSandbox>, PublicApiError> {
    let ListPage { rows, has_more } =
        ListPage::from_rows(rows, usize::from(limit), |row| row.has_next);
    let next_cursor = if has_more {
        rows.last()
            .map(|sandbox| sandbox_cursor(sandbox.created_at, sandbox.id))
            .transpose()?
    } else {
        None
    };
    let data = rows
        .into_iter()
        .map(public_sandbox_from_row)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PublicPage { data, next_cursor })
}

pub async fn update_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
    Json(request): Json<UpdateSandboxRequest>,
) -> Result<(StatusCode, Json<SandboxOperationResponse>), PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    validate_update_request(&request)?;
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    ensure_sandbox_owner(&state.pool, org.id, sandbox_id).await?;
    let fingerprint = update_request_fingerprint(sandbox_id, &request)?;
    let sandbox_id_string = sandbox_id.to_string();
    let payload_json = UpdateSandboxOperationPayload {
        sandbox_id,
        request,
    }
    .into_json();

    let store = PublicOperationStore::new(state.pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id: org.id,
            actor_user_id: Some(actor.id),
            api_product: ApiProduct::Native,
            kind: OperationKind::UpdateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Patch,
                path_template: OperationPathTemplate::new(UPDATE_SANDBOX_PATH_TEMPLATE),
                key: &idempotency_key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: crate::public_operations::request_traceparent(),
        })
        .await?;

    let response = match result {
        CreatePublicOperationResult::Created(operation_id) => SandboxOperationResponse {
            operation_id,
            sandbox_id,
            operation: None,
        },
        CreatePublicOperationResult::Existing(operation_id) => {
            sandbox_operation_response(&store, org.id, operation_id).await?
        }
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key was already used with a different request",
            ));
        }
        CreatePublicOperationResult::AdmissionDenied => return Err(PublicApiError::internal()),
    };

    Ok((StatusCode::ACCEPTED, Json(response)))
}

pub async fn apply_custom_sandbox_action(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id_and_action): Path<String>,
    body: Bytes,
) -> Result<Response, PublicApiError> {
    if let Some(sandbox_id) = sandbox_id_and_action.strip_suffix(":set_timeout") {
        return super::timeout::set_timeout(&state, org.id, sandbox_id, &headers, &body)
            .await
            .map(IntoResponse::into_response);
    }
    let (sandbox_id, action) = parse_action_segment(&sandbox_id_and_action, |operation| {
        super::hidden_operations::visible(&state, org.id, operation)
    })?;
    let rollback = rollback_request(action, &body)?;
    enqueue_lifecycle_operation(
        &state.pool,
        org.id,
        actor.id,
        headers,
        sandbox_id,
        action,
        rollback,
    )
    .await
    .map(IntoResponse::into_response)
}

pub async fn delete_sandbox(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
) -> Result<(StatusCode, Json<SandboxOperationResponse>), PublicApiError> {
    enqueue_lifecycle_operation(
        &state.pool,
        org.id,
        actor.id,
        headers,
        &sandbox_id,
        SandboxLifecycleAction::Delete,
        None,
    )
    .await
}

#[derive(Debug, Deserialize)]
pub struct ListSandboxesQuery {
    cursor: Option<String>,
    limit: Option<u16>,
}

impl ListSandboxesQuery {
    fn cursor_parts(&self) -> Result<Option<(OffsetDateTime, SandboxId)>, PublicApiError> {
        let Some(cursor) = self.cursor.as_deref() else {
            return Ok(None);
        };
        let cursor = decode_cursor(CursorKind::Sandbox, cursor)?;
        parse_sandbox_cursor(&cursor).map(Some)
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_SANDBOX_LIST_LIMIT,
            Some(limit) if limit > MAX_SANDBOX_LIST_LIMIT => MAX_SANDBOX_LIST_LIMIT,
            Some(limit) => limit,
        }
    }
}

pub(super) async fn delete_replay_in_owner_scope(
    pool: &PgPool,
    org_id: OrgId,
    subject_id: Option<UserId>,
    headers: &HeaderMap,
    sandbox_id: PublicSandboxId,
) -> Result<bool, PublicApiError> {
    if !headers.contains_key("idempotency-key") {
        return Ok(false);
    }
    let key = idempotency_key(headers)?;
    let action = SandboxLifecycleAction::Delete;
    let fingerprint = lifecycle_request_fingerprint(sandbox_id, action, None)?;
    let store = PublicOperationStore::new(pool.clone());
    let observation = store
        .observe_operation_idempotency(
            org_id,
            ApiProduct::Native,
            &CreatePublicOperationIdempotency {
                method: lifecycle_method(action),
                path_template: lifecycle_path_template(action),
                key: &key,
                request_fingerprint: &fingerprint,
            },
        )
        .await?;
    match observation {
        ObservePublicOperationIdempotency::Existing(operation_id) => Ok(store
            .operation_in_owner_scope(org_id, subject_id, operation_id)
            .await?),
        ObservePublicOperationIdempotency::Missing
        | ObservePublicOperationIdempotency::Conflict { .. } => Ok(false),
    }
}

async fn enqueue_lifecycle_operation(
    pool: &PgPool,
    org_id: OrgId,
    actor_user_id: UserId,
    headers: HeaderMap,
    sandbox_id: &str,
    action: SandboxLifecycleAction,
    rollback: Option<RollbackSandboxRequest>,
) -> Result<(StatusCode, Json<SandboxOperationResponse>), PublicApiError> {
    let idempotency_key = idempotency_key(&headers)?;
    let sandbox_id = parse_public_sandbox_id(sandbox_id)?;
    let fingerprint = lifecycle_request_fingerprint(sandbox_id, action, rollback.as_ref())?;
    let store = PublicOperationStore::new(pool.clone());
    let idempotency = CreatePublicOperationIdempotency {
        method: lifecycle_method(action),
        path_template: lifecycle_path_template(action),
        key: &idempotency_key,
        request_fingerprint: &fingerprint,
    };
    let mut admission = store
        .begin_operation_admission(org_id, ApiProduct::Native, &idempotency)
        .await?;
    // A completed DELETE outlives its sandbox. The tenant-scoped operation and
    // request fingerprint authorize replay; only new intent needs a live target.
    match admission.observation() {
        ObservePublicOperationIdempotency::Existing(operation_id) => {
            let operation_id = *operation_id;
            admission.finish().await?;
            let response = sandbox_operation_response(&store, org_id, operation_id).await?;
            return Ok((StatusCode::ACCEPTED, Json(response)));
        }
        ObservePublicOperationIdempotency::Conflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key was already used with a different request",
            ));
        }
        ObservePublicOperationIdempotency::Missing => {}
    }
    // A create can answer before its sandbox has a row. A delete in between
    // cancels or waits for that create, so it must not miss it.
    let found = if action == SandboxLifecycleAction::Delete {
        admission
            .delete_target_belongs_to_org(sandbox_id, org_id)
            .await?
    } else {
        admission
            .sandbox_belongs_to_org(SandboxId::from_uuid(sandbox_id.as_uuid()), org_id)
            .await?
    };
    if !found {
        return Err(PublicApiError::not_found("sandbox not found"));
    }
    let sandbox_id_string = sandbox_id.to_string();
    let payload_json = SandboxLifecycleOperationPayload {
        sandbox_id,
        action,
        rollback,
        wake_activity: None,
    }
    .into_json();
    let input = CreatePublicOperation {
        org_id,
        actor_user_id: Some(actor_user_id),
        api_product: ApiProduct::Native,
        kind: action.operation_kind(),
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(&sandbox_id_string),
        payload_json,
        idempotency: Some(idempotency),
        traceparent: crate::public_operations::request_traceparent(),
    };
    let result = if action == SandboxLifecycleAction::Wake {
        admission.create_or_join_active_wake(input).await?
    } else {
        admission.create(input).await?
    };

    let response = match result {
        CreatePublicOperationResult::Created(operation_id) => SandboxOperationResponse {
            operation_id,
            sandbox_id,
            operation: None,
        },
        CreatePublicOperationResult::Existing(operation_id) => {
            sandbox_operation_response(&store, org_id, operation_id).await?
        }
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key was already used with a different request",
            ));
        }
        CreatePublicOperationResult::AdmissionDenied => {
            return Err(PublicApiError::internal());
        }
    };

    Ok((StatusCode::ACCEPTED, Json(response)))
}

/// `visible` decides whether the caller may use an action's operation; a
/// hidden one answers exactly as an unknown action does.
fn parse_action_segment(
    value: &str,
    visible: impl FnOnce(native_routes::NativeOperation) -> bool,
) -> Result<(&str, SandboxLifecycleAction), PublicApiError> {
    let Some((sandbox_id, action)) = value.rsplit_once(':') else {
        return Err(PublicApiError::bad_request(
            "sandbox action path must use /v1/sandboxes/{sandbox_id}:action",
        ));
    };
    if sandbox_id.is_empty() || action.is_empty() {
        return Err(PublicApiError::bad_request("invalid sandbox action path"));
    }
    // Request-side input: unknown actions must stay rejected even though the
    // response-side vocabulary is open.
    let action = SandboxLifecycleAction::parse_known(action)
        .map_err(|_| PublicApiError::bad_request("invalid sandbox action"))?;
    if action == SandboxLifecycleAction::Delete {
        return Err(PublicApiError::bad_request(
            "delete uses DELETE /v1/sandboxes/{sandbox_id}",
        ));
    }
    if !visible(lifecycle_operation(action)) {
        return Err(PublicApiError::bad_request("invalid sandbox action"));
    }
    Ok((sandbox_id, action))
}

fn rollback_request(
    action: SandboxLifecycleAction,
    body: &[u8],
) -> Result<Option<RollbackSandboxRequest>, PublicApiError> {
    if action != SandboxLifecycleAction::Rollback {
        return Ok(None);
    }
    if body.is_empty() {
        return Err(PublicApiError::bad_request(
            "rollback requires snapshot_name",
        ));
    }
    serde_json::from_slice::<RollbackSandboxRequest>(body)
        .map(Some)
        .map_err(|error| PublicApiError::bad_request(format!("invalid rollback request: {error}")))
}

async fn ensure_sandbox_owner(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
) -> Result<(), PublicApiError> {
    let found = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(
             SELECT 1
             FROM sandboxes
             WHERE id = $1 AND org_id = $2
         )"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(org_id)
    .fetch_one(pool)
    .await?;
    if !found {
        return Err(PublicApiError::not_found("sandbox not found"));
    }
    Ok(())
}

#[derive(Debug, Clone, FromRow)]
struct PublicSandboxRow {
    id: SandboxId,
    name: Option<String>,
    lifecycle: String,
    has_current_failure: bool,
    placement: String,
    active_node_id: Option<NodeId>,
    template_id: Option<String>,
    content_hash: Option<String>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    disk_mib: Option<i32>,
    metadata: SqlJson<ControllerSandboxMetadata>,
    next_auto_event_at: Option<i64>,
    auto_lifecycle: SqlJson<AutoLifecyclePolicy>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    /// A page row is followed by another sandbox in list order, whether or
    /// not the page's byte budget let it in.
    has_next: bool,
}

const PUBLIC_SANDBOX_SELECT: &str = "SELECT s.id,
            s.name,
            s.lifecycle,
            COALESCE(
                s.lifecycle = 'failed'
                AND LOWER(TRIM(sos.status)) = 'failed'
                AND (
                    (
                        s.managed_by = $6
                        AND EXISTS (
                            SELECT 1
                            FROM sandbox_k8s_executions execution
                            WHERE execution.sandbox_id = s.id
                              AND execution.active
                              AND failure_observation.observed_payload
                                  -> $2::TEXT
                                  @> jsonb_build_object(
                                      $3::TEXT, execution.generation,
                                      $4::TEXT, execution.pod_uid,
                                      $5::TEXT, execution.route_activation_id::TEXT
                                  )
                              AND execution.pod_uid IS NOT NULL
                              AND execution.route_activation_id IS NOT NULL
                        )
                    )
                    OR (
                        s.managed_by = $7
                        AND direct_route.sandbox_id IS NOT NULL
                        AND failure_observation.observed_payload
                            -> $2::TEXT
                            @> jsonb_build_object(
                                $3::TEXT, direct_route.execution_generation,
                                $5::TEXT, direct_route.activation_attempt_id::TEXT
                            )
                    )
                ),
                FALSE
            ) AS has_current_failure,
            s.placement,
            s.active_node_id,
            s.template_id,
            s.content_hash,
            s.vcpu,
            s.memory_mib,
            s.disk_mib,
            s.metadata,
            s.next_auto_event_at,
            s.auto_lifecycle_json AS auto_lifecycle,
            s.created_at,
            s.updated_at";

/// The joins of a public sandbox row onto `s`.
const PUBLIC_SANDBOX_JOINS: &str = "LEFT JOIN sandbox_direct_route_identities direct_route
       ON direct_route.sandbox_id = s.id
     LEFT JOIN sandbox_observed_states sos
       ON sos.sandbox_id = s.id
      AND sos.node_id = COALESCE(direct_route.current_node_id, s.active_node_id)
     LEFT JOIN node_facet_observed_status failure_observation
       ON failure_observation.node_id = COALESCE(direct_route.current_node_id, s.active_node_id)
      AND failure_observation.facet = $1
      AND failure_observation.element = s.id::TEXT";

/// The sandboxes the list shows after the cursor: the org's live sandboxes,
/// owned by subject $12 or, without one, by a local user.
const PAGE_CANDIDATE_ROWS: &str = "SELECT s.id, s.created_at
    FROM sandboxes s
    WHERE s.org_id = $8
      AND s.lifecycle <> 'deleted'
      AND (($12::UUID IS NOT NULL AND s.user_id = $12)
        OR ($12::UUID IS NULL AND EXISTS (
            SELECT 1 FROM users owner WHERE owner.id = s.user_id
              AND owner.owner_source = 'local'
        )))
      AND {after_cursor}
    ORDER BY s.created_at DESC, s.id DESC
    LIMIT {max_rows}";

const PAGE_WALK_PARAMS: PageWalkParams = PageWalkParams {
    cursor_created_at: "$9",
    cursor_id: "$10",
    max_rows: "$11",
    max_bytes: "$13",
    row_overhead_bytes: "$14",
    // The public sandbox has no env.
    lists_env: false,
};

#[derive(Clone, Copy)]
enum PublicSandboxQuery {
    ById,
    Page,
}

fn public_sandbox_query(query: PublicSandboxQuery) -> sqlx::AssertSqlSafe<String> {
    // Control-plane retention keeps deleted tombstones, but deleted public
    // resources are NotFound and must not consume a list page's lookahead.
    let sql = match query {
        PublicSandboxQuery::ById => format!(
            "{PUBLIC_SANDBOX_SELECT}, FALSE AS has_next
             FROM sandboxes s
             {PUBLIC_SANDBOX_JOINS}
             WHERE s.id = $8 AND s.org_id = $9 AND s.lifecycle <> 'deleted'"
        ),
        PublicSandboxQuery::Page => format!(
            "{page_rows}
             {PUBLIC_SANDBOX_SELECT}, page.has_next
             FROM page_rows page
             JOIN sandboxes s ON s.id = page.id
             {PUBLIC_SANDBOX_JOINS}
             ORDER BY page.position",
            page_rows = page_rows_cte(PAGE_CANDIDATE_ROWS, &PAGE_WALK_PARAMS),
        ),
    };
    sqlx::AssertSqlSafe(sql!(sql))
}

async fn fetch_public_sandbox(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
) -> Result<Option<PublicSandboxRow>, sqlx::Error> {
    sqlx::query_as::<_, PublicSandboxRow>(public_sandbox_query(PublicSandboxQuery::ById))
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(SANDBOX_FAILURE_EPISODE_KEY)
        .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
        .bind(SANDBOX_FAILURE_POD_UID_KEY)
        .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
        .bind(SubstrateKind::Kubernetes)
        .bind(SubstrateKind::Space)
        .bind(sandbox_id.as_uuid())
        .bind(org_id)
        .fetch_optional(pool)
        .await
}

async fn fetch_sandboxes_page(
    pool: &PgPool,
    org_id: OrgId,
    subject_id: Option<UserId>,
    cursor: Option<(OffsetDateTime, SandboxId)>,
    limit: u16,
) -> Result<Vec<PublicSandboxRow>, sqlx::Error> {
    let (cursor_created_at, cursor_id) = cursor
        .map(|(created_at, id)| (Some(created_at), Some(id)))
        .unwrap_or((None, None));
    sqlx::query_as::<_, PublicSandboxRow>(public_sandbox_query(PublicSandboxQuery::Page))
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(SANDBOX_FAILURE_EPISODE_KEY)
        .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
        .bind(SANDBOX_FAILURE_POD_UID_KEY)
        .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
        .bind(SubstrateKind::Kubernetes)
        .bind(SubstrateKind::Space)
        .bind(org_id)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit) + 1)
        .bind(subject_id)
        .bind(MAX_SANDBOX_LIST_PAGE_BYTES)
        .bind(SANDBOX_LIST_ROW_OVERHEAD_BYTES)
        .fetch_all(pool)
        .await
}

async fn sandbox_operation_response(
    store: &PublicOperationStore,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<SandboxOperationResponse, PublicApiError> {
    let operation = store
        .fetch_operation(org_id, operation_id)
        .await?
        .ok_or_else(PublicApiError::internal)?;
    let target_id = operation
        .target_id
        .as_deref()
        .ok_or_else(PublicApiError::internal)?;
    let sandbox_id = parse_public_sandbox_id(target_id)?;
    Ok(SandboxOperationResponse {
        operation_id,
        sandbox_id,
        operation: None,
    })
}

/// The native `Idempotency-Key` header, shared by every `/v1` route that
/// requires one so all of them match the published parameter.
pub(crate) fn idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, PublicApiError> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Err(PublicApiError::bad_request(
            "Idempotency-Key header is required",
        ));
    };
    IdempotencyKey::parse_header(value.as_bytes())
        .map_err(|error| PublicApiError::bad_request(error.to_string()))
}

fn validate_update_request(request: &UpdateSandboxRequest) -> Result<(), PublicApiError> {
    if request
        .resources
        .as_ref()
        .and_then(|resources| resources.disk_mib)
        .is_some()
    {
        return Err(PublicApiError::bad_request(
            "disk_mib is not mutable in place; recreate or restore the sandbox with the new disk size",
        ));
    }
    pplx_space_public_api::limits::validate_update_request(request)
        .map_err(PublicApiError::request_limit)
}

fn request_fingerprint(
    request: &impl serde::Serialize,
) -> Result<RequestFingerprint, PublicApiError> {
    let bytes = serde_json::to_vec(request).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize public create sandbox request");
        PublicApiError::internal()
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(error = %error, "failed to build public request fingerprint");
        PublicApiError::internal()
    })
}

fn update_request_fingerprint(
    sandbox_id: PublicSandboxId,
    request: &UpdateSandboxRequest,
) -> Result<RequestFingerprint, PublicApiError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "sandbox_id": sandbox_id.to_string(),
        "request": request,
    }))
    .map_err(|error| {
        tracing::error!(error = %error, "failed to serialize public update sandbox request fingerprint");
        PublicApiError::internal()
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(error = %error, "failed to build public update sandbox request fingerprint");
        PublicApiError::internal()
    })
}

pub(super) fn lifecycle_request_fingerprint(
    sandbox_id: PublicSandboxId,
    action: SandboxLifecycleAction,
    rollback: Option<&RollbackSandboxRequest>,
) -> Result<RequestFingerprint, PublicApiError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "sandbox_id": sandbox_id.to_string(),
        "action": action.as_str(),
        "rollback": rollback,
    }))
    .map_err(|error| {
        tracing::error!(error = %error, "failed to serialize public lifecycle request fingerprint");
        PublicApiError::internal()
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(error = %error, "failed to build public lifecycle request fingerprint");
        PublicApiError::internal()
    })
}

pub(super) fn lifecycle_method(action: SandboxLifecycleAction) -> HttpOperationMethod {
    match action {
        SandboxLifecycleAction::Delete => HttpOperationMethod::Delete,
        _ => HttpOperationMethod::Post,
    }
}

pub(super) fn lifecycle_path_template(action: SandboxLifecycleAction) -> OperationPathTemplate {
    OperationPathTemplate::new(lifecycle_operation(action).path())
}

pub(super) fn lifecycle_operation(
    action: SandboxLifecycleAction,
) -> native_routes::NativeOperation {
    match action {
        SandboxLifecycleAction::Start => native_routes::START_SANDBOX,
        SandboxLifecycleAction::Stop => native_routes::STOP_SANDBOX,
        SandboxLifecycleAction::Pause => native_routes::PAUSE_SANDBOX,
        SandboxLifecycleAction::Resume => native_routes::RESUME_SANDBOX,
        SandboxLifecycleAction::Suspend => native_routes::SUSPEND_SANDBOX,
        SandboxLifecycleAction::Restore => native_routes::RESTORE_SANDBOX,
        SandboxLifecycleAction::Wake => native_routes::WAKE_SANDBOX,
        SandboxLifecycleAction::Kill => native_routes::KILL_SANDBOX,
        SandboxLifecycleAction::Checkpoint => native_routes::CHECKPOINT_SANDBOX,
        SandboxLifecycleAction::EnableSnapshots => native_routes::ENABLE_SANDBOX_SNAPSHOTS,
        SandboxLifecycleAction::DisableSnapshots => native_routes::DISABLE_SANDBOX_SNAPSHOTS,
        SandboxLifecycleAction::Rollback => native_routes::ROLLBACK_SANDBOX,
        SandboxLifecycleAction::Delete => native_routes::DELETE_SANDBOX,
    }
}

/// The one `{sandbox_id}` parser for the native API: `owner_scope` and every
/// handler call it, so a malformed id gets the same 400 whichever rejects it.
/// `sandbox_operation_response` also parses a stored operation's `target_id`
/// with it, so a corrupt stored id answers this 400 too.
pub(super) fn parse_public_sandbox_id(value: &str) -> Result<PublicSandboxId, PublicApiError> {
    PublicSandboxId::from_str(value).map_err(|_| PublicApiError::bad_request("invalid sandbox id"))
}

pub(crate) fn controller_error(error: ControllerError) -> PublicApiError {
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
        StatusCode::TOO_MANY_REQUESTS => {
            with_retry_after_hint(PublicApiError::rate_limited(error.to_string()), retry_after)
        }
        StatusCode::SERVICE_UNAVAILABLE => with_retry_after_hint(
            PublicApiError::capacity_unavailable(error.to_string()),
            retry_after,
        ),
        status if status.is_server_error() => {
            tracing::warn!(%error, "native sandbox operation failed");
            PublicApiError::internal()
        }
        _ => PublicApiError::internal(),
    }
}

fn public_sandbox_from_row(sandbox: PublicSandboxRow) -> Result<PublicSandbox, PublicApiError> {
    let state = public_lifecycle_state(&sandbox.lifecycle)?;
    let conditions = sandbox_conditions(&state, &sandbox);
    let failure_code = sandbox
        .has_current_failure
        .then_some(SandboxFailureCode::RuntimeFailure);
    let expires_at = sandbox
        .next_auto_event_at
        .map(OffsetDateTime::from_unix_timestamp)
        .transpose()
        .map_err(|error| {
            tracing::error!(%error, "invalid stored sandbox idle deadline");
            PublicApiError::internal()
        })?;
    let (idle_timeout, idle_action) = match sandbox.auto_lifecycle.0.stages.as_slice() {
        [stage] => match crate::e2b::lifecycle_config_from_policy(&sandbox.auto_lifecycle.0) {
            Some(config) => (
                Some(format!("{}s", stage.at)),
                Some(if config.auto_pause {
                    SandboxIdleAction::Suspend
                } else {
                    SandboxIdleAction::Delete
                }),
            ),
            None => (None, None),
        },
        _ => (None, None),
    };
    Ok(PublicSandbox {
        id: PublicSandboxId::from(sandbox.id),
        name: sandbox.name,
        state,
        failure_code,
        conditions,
        template: sandbox.template_id,
        template_digest: sandbox.content_hash,
        resources: SandboxResources {
            vcpu: sandbox.vcpu.and_then(|value| u32::try_from(value).ok()),
            memory_mib: sandbox
                .memory_mib
                .and_then(|value| u32::try_from(value).ok()),
            disk_mib: sandbox.disk_mib.and_then(|value| u32::try_from(value).ok()),
        },
        metadata: public_metadata(sandbox.metadata.0),
        expires_at,
        idle_timeout,
        idle_action,
        created_at: sandbox.created_at,
        updated_at: sandbox.updated_at,
    })
}

/// The public stats shape. Every sandbox-manager field is named, so a field
/// added to the proto must be placed here before this compiles.
fn public_stats_response(
    sandbox_id: PublicSandboxId,
    stats: pplx_space_sbx_mgr_client::proto::StatsResponse,
) -> SandboxStatsResponse {
    let pplx_space_sbx_mgr_client::proto::StatsResponse {
        sandbox_id: _,
        cpu_max,
        memory_max,
        memory_current,
        pids_max,
        pids_current,
        cpu_usage_usec,
        network_tx_bytes,
        network_rx_bytes,
        net_gateway_reachable,
        tcp_active_connections,
        tcp_total_connections,
        udp_active_mappings,
        bytes_forwarded,
        tcp_denied_connections,
    } = stats;
    SandboxStatsResponse {
        sandbox_id,
        cpu_max,
        memory_max,
        memory_current,
        pids_max,
        pids_current,
        cpu_usage_usec,
        network_tx_bytes,
        network_rx_bytes,
        net_gateway_reachable,
        tcp_active_connections,
        tcp_total_connections,
        tcp_denied_connections,
        udp_active_mappings,
        bytes_forwarded,
    }
}

fn public_lifecycle_state(lifecycle: &str) -> Result<SandboxLifecycleState, PublicApiError> {
    let lifecycle = lifecycle.parse::<SandboxLifecycle>().map_err(|error| {
        tracing::error!(%error, "unknown sandbox lifecycle in public sandbox projection");
        PublicApiError::internal()
    })?;
    Ok(project_public_lifecycle_state(lifecycle))
}

fn sandbox_conditions(
    state: &SandboxLifecycleState,
    sandbox: &PublicSandboxRow,
) -> Vec<SandboxCondition> {
    vec![
        SandboxCondition {
            condition_type: SandboxConditionType::Ready,
            status: if state == &SandboxLifecycleState::Running {
                ConditionStatus::True
            } else {
                ConditionStatus::False
            },
            reason: state.to_string(),
            message: if state == &SandboxLifecycleState::Running {
                "sandbox is running".to_string()
            } else {
                "sandbox is not running".to_string()
            },
        },
        SandboxCondition {
            condition_type: SandboxConditionType::NetworkReady,
            status: if sandbox.active_node_id.is_some() {
                ConditionStatus::True
            } else {
                ConditionStatus::Unknown
            },
            reason: sandbox.placement.clone(),
            message: "network readiness follows sandbox placement".to_string(),
        },
    ]
}

fn public_metadata(metadata: ControllerSandboxMetadata) -> PublicSandboxMetadata {
    metadata
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                SandboxMetadataValue::String(value) => PublicSandboxMetadataValue::String(value),
                SandboxMetadataValue::Number(value) => PublicSandboxMetadataValue::Number(value),
                SandboxMetadataValue::Bool(value) => PublicSandboxMetadataValue::Bool(value),
            };
            (key, value)
        })
        .collect()
}

fn sandbox_cursor(created_at: OffsetDateTime, id: SandboxId) -> Result<PageCursor, PublicApiError> {
    encode_cursor(
        CursorKind::Sandbox,
        format!("{}:{id}", created_at.unix_timestamp_nanos()),
    )
}

fn parse_sandbox_cursor(value: &str) -> Result<(OffsetDateTime, SandboxId), PublicApiError> {
    let Some((created_at, id)) = value.split_once(':') else {
        return Err(PublicApiError::bad_request("invalid cursor"));
    };
    let created_at = created_at
        .parse::<i128>()
        .ok()
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .ok_or_else(|| PublicApiError::bad_request("invalid cursor"))?;
    let id = id
        .parse::<SandboxId>()
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    Ok((created_at, id))
}

#[cfg(all(test, feature = "kubernetes"))]
pub(crate) mod admission_test_fixture;
mod inline_create;
#[cfg(test)]
mod tests;
