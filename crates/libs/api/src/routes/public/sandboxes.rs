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
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecyclePolicy, AutoLifecycleStage, IdempotencyKey, SandboxLifecycle,
};
use pplx_space_common_svc::types::{
    FACET_SANDBOX_DESIRED_STATE, SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY,
    SANDBOX_FAILURE_EPISODE_KEY, SANDBOX_FAILURE_EXECUTION_GENERATION_KEY,
    SANDBOX_FAILURE_POD_UID_KEY,
};
use pplx_space_public_api::{
    ConditionStatus, CreateSandboxRequest, OperationId, OperationKind, OperationTargetType,
    PageCursor, PublicErrorCode, PublicPage, PublicSandbox, PublicSandboxId, PublicSandboxMetadata,
    PublicSandboxMetadataValue, RequestFingerprint, RollbackSandboxRequest, SandboxCondition,
    SandboxConditionType, SandboxFailureCode, SandboxLifecycleAction, SandboxLifecycleState,
    SandboxOperationResponse, SandboxResources, SandboxStatsResponse, UpdateSandboxRequest,
    routes as native_routes,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, types::Json as SqlJson};
use time::OffsetDateTime;

use crate::{
    controllers::{ControllerError, sandbox::fetch_sandbox},
    middleware::auth::{AuthFederatedOwner, AuthOrg, AuthUser},
    models::{Sandbox, SandboxMetadata as ControllerSandboxMetadata, SandboxMetadataValue},
    public_operations::{
        ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
        CreatePublicOperationResult, HttpOperationMethod, OperationPathTemplate,
        PublicOperationStore,
    },
    public_sandbox_operations::{
        CreateSandboxOperationPayload, SandboxLifecycleOperationPayload,
        UpdateSandboxOperationPayload,
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
    let federated_create = owner.zip(grant.as_ref()).map(|(owner, grant)| {
        crate::fork_operations::ForkFederatedCreateContext::from_request(owner, grant)
    });
    let response = create_native_sandbox_operation(
        &state.pool,
        org.id,
        actor.id,
        &idempotency_key,
        request,
        federated_create,
    )
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Extension(RouteTemplateName(template_name)),
        Json(response),
    ))
}

async fn create_native_sandbox_operation(
    pool: &PgPool,
    org_id: OrgId,
    actor_user_id: UserId,
    idempotency_key: &IdempotencyKey,
    request: CreateSandboxRequest,
    federated_create: Option<crate::fork_operations::ForkFederatedCreateContext>,
) -> Result<SandboxOperationResponse, PublicApiError> {
    let (request, fingerprint) = prepare_native_create_request(request)?;
    let sandbox_id = PublicSandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let payload_json = CreateSandboxOperationPayload {
        sandbox_id,
        request,
        // Placement is operator-only (admin surface); the public API never pins.
        node_pool: None,
        federated_create: federated_create.clone(),
    }
    .into_json();

    let store = PublicOperationStore::new(pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor_user_id),
            api_product: ApiProduct::Native,
            kind: OperationKind::CreateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(CREATE_SANDBOX_PATH_TEMPLATE),
                key: idempotency_key,
                request_fingerprint: &fingerprint,
            }),
        })
        .await?;

    let response = match result {
        CreatePublicOperationResult::Created(operation_id) => SandboxOperationResponse {
            operation_id,
            sandbox_id,
        },
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

    Ok(response)
}

fn prepare_native_create_request(
    request: CreateSandboxRequest,
) -> Result<(CreateSandboxRequest, RequestFingerprint), PublicApiError> {
    if let Some(size_mib) = request
        .resources
        .as_ref()
        .and_then(|resources| resources.disk_mib)
    {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(size_mib)
            .map_err(|error| PublicApiError::bad_request(format!("disk_mib: {error}")))?;
    }
    // Hash the parsed client request before applying server defaults. This keeps
    // idempotent retries stable across rolling deploys where older API replicas
    // still hash the omitted policy as null.
    let fingerprint = request_fingerprint(&request)?;
    Ok((with_native_create_defaults(request), fingerprint))
}

fn with_native_create_defaults(mut request: CreateSandboxRequest) -> CreateSandboxRequest {
    request
        .auto_lifecycle
        .get_or_insert_with(|| AutoLifecyclePolicy {
            wake: Default::default(),
            // Command activity advances both the command and access clocks in
            // sbx-mgr. One access-idle condition therefore means "no access or
            // command activity"; separate conditions would fire when either
            // individual clock expired.
            condition_sets: std::collections::BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: DEFAULT_NATIVE_IDLE_TIMEOUT_SECS,
                action: AutoLifecycleIdleAction::Suspend,
            }],
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
    let Some(sandbox) = fetch_public_sandbox(&state.pool, org.id, sandbox_id).await? else {
        return Err(PublicApiError::not_found("sandbox not found"));
    };

    Ok(Json(public_sandbox_from_row(sandbox)?))
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

fn sandboxes_page(
    mut rows: Vec<PublicSandboxRow>,
    limit: u16,
) -> Result<PublicPage<PublicSandbox>, PublicApiError> {
    let has_more = rows.len() > usize::from(limit);
    if has_more {
        rows.truncate(usize::from(limit));
    }
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
        })
        .await?;

    let response = match result {
        CreatePublicOperationResult::Created(operation_id) => SandboxOperationResponse {
            operation_id,
            sandbox_id,
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
) -> Result<(StatusCode, Json<SandboxOperationResponse>), PublicApiError> {
    let (sandbox_id, action) = parse_action_segment(&sandbox_id_and_action)?;
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
    ensure_sandbox_owner(pool, org_id, sandbox_id).await?;
    let fingerprint = lifecycle_request_fingerprint(sandbox_id, action, rollback.as_ref())?;
    let sandbox_id_string = sandbox_id.to_string();
    let payload_json = SandboxLifecycleOperationPayload {
        sandbox_id,
        action,
        rollback,
    }
    .into_json();
    let store = PublicOperationStore::new(pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor_user_id),
            api_product: ApiProduct::Native,
            kind: action.operation_kind(),
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: lifecycle_method(action),
                path_template: lifecycle_path_template(action),
                key: &idempotency_key,
                request_fingerprint: &fingerprint,
            }),
        })
        .await?;

    let response = match result {
        CreatePublicOperationResult::Created(operation_id) => SandboxOperationResponse {
            operation_id,
            sandbox_id,
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

fn parse_action_segment(value: &str) -> Result<(&str, SandboxLifecycleAction), PublicApiError> {
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
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

const PUBLIC_SANDBOX_SELECT: &str = "SELECT s.id,
            s.name,
            s.lifecycle,
            COALESCE(
                s.lifecycle = 'failed'
                AND LOWER(TRIM(sos.status)) = 'failed'
                AND (
                    (
                        s.managed_by = 'k8s'
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
                        s.managed_by = 'space'
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
            s.created_at,
            s.updated_at
     FROM sandboxes s
     LEFT JOIN sandbox_direct_route_identities direct_route
       ON direct_route.sandbox_id = s.id
     LEFT JOIN sandbox_observed_states sos
       ON sos.sandbox_id = s.id
      AND sos.node_id = COALESCE(direct_route.current_node_id, s.active_node_id)
     LEFT JOIN node_facet_observed_status failure_observation
       ON failure_observation.node_id = COALESCE(direct_route.current_node_id, s.active_node_id)
      AND failure_observation.facet = $1
      AND failure_observation.element = s.id::TEXT";

#[derive(Clone, Copy)]
enum PublicSandboxQuery {
    ById,
    Page,
}

fn public_sandbox_query(query: PublicSandboxQuery) -> sqlx::AssertSqlSafe<String> {
    // Control-plane retention keeps deleted tombstones, but deleted public
    // resources are NotFound and must not consume a list page's lookahead.
    let suffix = match query {
        PublicSandboxQuery::ById => {
            "WHERE s.id = $6 AND s.org_id = $7 AND s.lifecycle <> 'deleted'"
        }
        PublicSandboxQuery::Page => {
            "WHERE s.org_id = $6
               AND s.lifecycle <> 'deleted'
               AND (($10::UUID IS NOT NULL AND s.user_id = $10)
                 OR ($10::UUID IS NULL AND EXISTS (
                     SELECT 1 FROM users owner WHERE owner.id = s.user_id
                       AND owner.owner_source = 'local'
                 )))
               AND (
                   $7::TIMESTAMPTZ IS NULL
                   OR (s.created_at, s.id) < ($7::TIMESTAMPTZ, $8::UUID)
               )
             ORDER BY s.created_at DESC, s.id DESC
             LIMIT $9"
        }
    };
    sqlx::AssertSqlSafe(sql!(format!("{PUBLIC_SANDBOX_SELECT}\n{suffix}")))
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
        .bind(org_id)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit) + 1)
        .bind(subject_id)
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
    })
}

fn idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, PublicApiError> {
    let Some(value) = headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(PublicApiError::bad_request(
            "Idempotency-Key header is required",
        ));
    };
    IdempotencyKey::parse(value)
        .map_err(|error| PublicApiError::bad_request(format!("invalid Idempotency-Key: {error}")))
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
    Ok(())
}

fn request_fingerprint(
    request: &CreateSandboxRequest,
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

fn lifecycle_request_fingerprint(
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

fn lifecycle_method(action: SandboxLifecycleAction) -> HttpOperationMethod {
    match action {
        SandboxLifecycleAction::Delete => HttpOperationMethod::Delete,
        _ => HttpOperationMethod::Post,
    }
}

fn lifecycle_path_template(action: SandboxLifecycleAction) -> OperationPathTemplate {
    OperationPathTemplate::new(match action {
        SandboxLifecycleAction::Start => native_routes::START_SANDBOX.path(),
        SandboxLifecycleAction::Stop => native_routes::STOP_SANDBOX.path(),
        SandboxLifecycleAction::Pause => native_routes::PAUSE_SANDBOX.path(),
        SandboxLifecycleAction::Resume => native_routes::RESUME_SANDBOX.path(),
        SandboxLifecycleAction::Suspend => native_routes::SUSPEND_SANDBOX.path(),
        SandboxLifecycleAction::Restore => native_routes::RESTORE_SANDBOX.path(),
        SandboxLifecycleAction::Wake => native_routes::WAKE_SANDBOX.path(),
        SandboxLifecycleAction::Kill => native_routes::KILL_SANDBOX.path(),
        SandboxLifecycleAction::Checkpoint => native_routes::CHECKPOINT_SANDBOX.path(),
        SandboxLifecycleAction::EnableSnapshots => native_routes::ENABLE_SANDBOX_SNAPSHOTS.path(),
        SandboxLifecycleAction::DisableSnapshots => native_routes::DISABLE_SANDBOX_SNAPSHOTS.path(),
        SandboxLifecycleAction::Rollback => native_routes::ROLLBACK_SANDBOX.path(),
        SandboxLifecycleAction::Delete => native_routes::DELETE_SANDBOX.path(),
    })
}

fn parse_public_sandbox_id(value: &str) -> Result<PublicSandboxId, PublicApiError> {
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

#[cfg(test)]
mod tests;
