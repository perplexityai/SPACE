use pplx_space_accounts::{NodeId, OrgId};

use std::collections::HashMap;
use std::time::Duration;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt as _;
use pplx_space_build_mgr_client::proto as build_proto;
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_cluster_trigger_queue::enqueue_triggers_tx;
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_node_model::SubstrateKind;
use pplx_space_node_registration_store::direct::registry as direct_registry;
use pplx_space_public_api::{
    OperationKind, OperationTargetType, PublicErrorCode, PublicErrorDetail, PublicErrorEnvelope,
    RequestFingerprint,
};
use pplx_space_template_model::{
    BuildRunId, SystemTemplate, TemplateDesiredState, TemplateId, TemplateName, TemplateTag,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

use tracing::Instrument as _;

use crate::{
    controllers::{
        ControllerError,
        substrate::BuilderClient,
        template::{
            TEMPLATE_BUILD_SUBMIT_BUDGET, TemplateBuildSubmission, TemplateBuildSubmitError,
            TemplateBuildTarget, template_build_status_with,
        },
    },
    middleware::auth::AuthUser,
    models::{
        CreatePoolApiRequest, CreateTemplateRequest, NetworkPolicyStatus, NullablePatch,
        SystemTemplateBuildRequest, TemplateBaseSnapshotPolicyRequest, TemplateBuildClientContext,
        TemplateBuildConfig, TemplateBuildCopyStep, TemplateBuildListEntry, TemplateBuildOrigin,
        TemplateBuildPackageManager, TemplateBuildRecipe, TemplateBuildRequest,
        TemplateBuildSource, TemplateBuildStartResponse, TemplateBuildStateResponse,
        TemplateBuildStatusResponse, TemplateBuildStep, TemplateOperationResponse,
        TemplateReleaseResponse, TemplateReleaseTagRequest, TemplateReleaseWarmPoolResponse,
        TemplateResponse, UpdateTemplateRequest, User,
    },
    public_operations::{
        ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
        CreatePublicOperationResult, HttpOperationMethod, ObservePublicOperationIdempotency,
        OperationPathTemplate, PublicOperationStore, PublicOperationStoreError,
        ResponseReservation,
    },
    public_template_operations::{DeleteTemplateOperationPayload, TemplateInUse},
    reconcile_queue::{
        ReconcileRef, enqueue_node_reconcile_refs_for_nodes_tx,
        requeue_node_reconcile_refs_for_nodes_tx,
    },
    routes::{
        AppState,
        admin::{
            audit,
            support::{self, ContentionScope, IDEMPOTENCY_KEY_HEADER, parse_if_match_generation},
        },
        authz,
    },
    template_build_jobs_store::RecordedBuild,
};

type ApiErrorResponse = (StatusCode, Json<PublicErrorEnvelope>);

type TemplateRow =
    pplx_space_template_store::templates::AdminTemplate<Vec<crate::models::SnapshotInfoResponse>>;

#[derive(Debug)]
struct TemplateReleaseRow {
    id: Uuid,
    template_name: String,
    runtime: String,
    content_hash: String,
    created_at: OffsetDateTime,
}

impl From<pplx_space_template_release_store::lookup::VisibleRelease> for TemplateReleaseRow {
    fn from(release: pplx_space_template_release_store::lookup::VisibleRelease) -> Self {
        Self {
            id: release.id,
            template_name: release.template_name,
            runtime: release.runtime,
            content_hash: release.content_hash,
            created_at: release.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
struct TemplateBaseSnapshotPolicyResponse {
    template: String,
    configured: bool,
    ensure_base_snapshot: bool,
    snapshot_vcpu: u32,
    snapshot_memory_mib: u32,
    updated_at: Option<String>,
}

/// This module's copy of the operator gate, kept only because it answers in
/// `ApiErrorResponse` rather than `support`'s tuple. See
/// `support::require_managing_org_admin` for why the contention arm classifies
/// and why it is `ReadOnly`.
async fn require_template_admin(pool: &sqlx::PgPool, actor: &User) -> Result<(), ApiErrorResponse> {
    match authz::is_managing_org_admin(pool, actor).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(api_error(StatusCode::FORBIDDEN, "Insufficient permissions")),
        Err(error) => {
            if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                return Err(response);
            }
            tracing::error!(%error, actor_id = %actor.id, "failed to authorize template admin");
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
            ))
        }
    }
}

fn api_error(status: StatusCode, message: impl Into<String>) -> ApiErrorResponse {
    api_error_with_details(status, message, Vec::new())
}

/// The response for a lost lock race, or `None` when `error` is a real failure
/// the caller should log and answer 500 itself.
///
/// Wraps [`support::contention_disposition`] into this module's envelope so
/// every `sqlx` error arm below shares one disposition instead of restating the
/// split. Restating it is how this surface came to answer the same `55P03` two
/// ways: `support::require_managing_org_admin` classified it and this module's
/// `require_template_admin` did not, so the operator saw a 503 or a 500 for the
/// identical lock timeout on the identical preflight `SELECT` depending on
/// which path they hit.
///
/// `scope` is a property of the statement, not of the handler — both
/// `require_template_admin` and the [`managing_org_id`] lookup a write handler
/// runs before opening its transaction are [`ContentionScope::ReadOnly`],
/// because at that point the request has written nothing for a serialization
/// abort to have raced.
fn contention_response(scope: ContentionScope, error: &sqlx::Error) -> Option<ApiErrorResponse> {
    let (status, message) = support::contention_disposition(scope, error)?;
    Some(contention_envelope(status, message))
}

/// [`contention_response`] for a [`PublicOperationStore`] failure, so a
/// store-backed arm answers exactly as this module's bare `sqlx` arms do.
fn store_contention_response(
    scope: ContentionScope,
    error: &PublicOperationStoreError,
) -> Option<ApiErrorResponse> {
    let (status, message) = support::store_contention_disposition(scope, error)?;
    Some(contention_envelope(status, message))
}

/// Open a transaction, or the response for having failed to.
///
/// Eight handlers below opened one the same way, each with its own three-line
/// arm that answered a flat 500. The disposition is new at all eight — it is
/// not a dedup of eight existing checks — and lives here once rather than
/// being written out per handler.
///
/// `ReadOnly`, and in practice dead: a `BEGIN` takes no locks, so this fails
/// only on pool acquisition or transport, neither of which carries a SQLSTATE.
/// It goes through the classifier anyway because exempting a statement rests on
/// a property no reader of the call site can see, and because a transaction
/// start that later grows a `SET LOCAL` or a `begin_with` gets the disposition
/// without anyone remembering to add it.
async fn begin_tx(
    pool: &sqlx::PgPool,
    template: Option<&TemplateName>,
    log: &'static str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, ApiErrorResponse> {
    pool.begin().await.map_err(|error| {
        contention_response(ContentionScope::ReadOnly, &error).unwrap_or_else(|| {
            // `template` is `None` on two of the eight callers, each for its
            // own reason: the release-tag handler is keyed on a release id and
            // does not learn the name until it reads the row inside the
            // transaction, and the create handler takes its name from the
            // request body rather than the path and did not log it here before
            // these arms were collapsed. No caller passes a name to a
            // begin-failure log that did not already carry one.
            //
            // The event message is new for all eight, though: each arm used to
            // carry its own string as the message, and that string is now the
            // `log` field. A saved query matching on the old messages needs
            // repointing at this one.
            tracing::error!(
                %error,
                template = template.map(TemplateName::as_str),
                log,
                "failed to begin admin template transaction"
            );
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        })
    })
}

/// The cluster's managing org, or the response for having failed to read it.
///
/// `ReadOnly`: this runs before the write handlers that need it open their
/// transaction, so the request has written nothing a serialization abort could
/// have raced.
async fn managing_org_id(
    pool: &sqlx::PgPool,
    log: &'static str,
) -> Result<OrgId, ApiErrorResponse> {
    pplx_space_accounts_store::clusters::cluster_managing_org_id(pool)
        .await
        .map_err(sqlx::Error::from)
        .map_err(|error| {
            contention_response(ContentionScope::ReadOnly, &error).unwrap_or_else(|| {
                tracing::error!(%error, log, "failed to load managing org for admin template action");
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            })
        })
}

/// `api_error`'s envelope with `retryable` forced on.
///
/// `api_error` derives that flag from the status, and 409 is absent from its
/// set — so the seven other 409s this module returns all render
/// `retryable: false`: an idempotency key reused with a different body, a
/// template that already exists, a deleted or still-deleting template on a
/// build, a release missing or still tagged `latest`, a release still
/// referenced, a template delete while warm pools still use the template. Some of those do clear without the caller doing anything, so
/// the flag is imprecise for them; what matters here is that a contention
/// 409's own message tells the caller to retry, so its envelope must not say
/// otherwise. Built from `api_error` rather than re-listing the envelope's
/// fields so a new field cannot go unset on this path alone.
fn contention_envelope(status: StatusCode, message: &'static str) -> ApiErrorResponse {
    let (status, Json(mut envelope)) = api_error(status, message);
    envelope.retryable = true;
    (status, Json(envelope))
}

/// The 500 for a statement that ran *after* `tx.commit()` returned.
///
/// `api_error` marks every 500 `retryable: true`, which on this path invites
/// precisely the replay the post-commit arms exist to prevent: the write has
/// already landed, and on the `If-Match` path a replay answers 412 because the
/// generation it matched on has moved. Withholding the contention disposition
/// is not enough on its own — the envelope has to stop advertising the retry
/// too, or an SDK that reads the flag replays anyway.
fn post_commit_error(message: &'static str) -> ApiErrorResponse {
    let (status, Json(mut envelope)) = api_error(StatusCode::INTERNAL_SERVER_ERROR, message);
    envelope.retryable = false;
    (status, Json(envelope))
}

fn api_error_with_details(
    status: StatusCode,
    message: impl Into<String>,
    details: Vec<PublicErrorDetail>,
) -> ApiErrorResponse {
    (
        status,
        Json(PublicErrorEnvelope {
            code: public_error_code(status),
            message: message.into(),
            request_id: pplx_space_public_api_http::current_request_id(),
            retryable: matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::INTERNAL_SERVER_ERROR
            ),
            details,
        }),
    )
}

fn public_error_code(status: StatusCode) -> PublicErrorCode {
    match status {
        StatusCode::UNAUTHORIZED => PublicErrorCode::Unauthorized,
        StatusCode::FORBIDDEN => PublicErrorCode::Forbidden,
        StatusCode::NOT_FOUND => PublicErrorCode::NotFound,
        StatusCode::CONFLICT => PublicErrorCode::Conflict,
        StatusCode::PRECONDITION_FAILED => PublicErrorCode::FailedPrecondition,
        StatusCode::TOO_MANY_REQUESTS => PublicErrorCode::RateLimited,
        StatusCode::SERVICE_UNAVAILABLE => PublicErrorCode::ServiceUnavailable,
        StatusCode::INTERNAL_SERVER_ERROR => PublicErrorCode::Internal,
        _ => PublicErrorCode::InvalidRequest,
    }
}

fn format_time(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn nonnegative_i32_to_u32(value: i32) -> u32 {
    u32::try_from(value).unwrap_or_default()
}

fn optional_count_to_u32(value: Option<i64>) -> u32 {
    value
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default()
}

fn template_response(row: TemplateRow) -> TemplateResponse {
    let name = row.name;
    TemplateResponse {
        name: name.clone(),
        description: row.description,
        org_id: row.org_id,
        desired_state: row.desired_state,
        created_at: Some(format_time(row.created_at)),
        built_at: row.built_at,
        content_hash: row.content_hash,
        runtime: row.runtime,
        runtime_version: row.runtime_version,
        arch: row.arch,
        has_base_snapshot: row.has_base_snapshot,
        pool_target: row.pool_target.and_then(|value| u32::try_from(value).ok()),
        pool_current: row.pool_current.and_then(|value| u32::try_from(value).ok()),
        artifact_size_bytes: row
            .artifact_size_bytes
            .and_then(|value| u64::try_from(value).ok()),
        cache_size_bytes: row
            .cache_size_bytes
            .and_then(|value| u64::try_from(value).ok()),
        base_snapshot_size_bytes: row
            .base_snapshot_size_bytes
            .and_then(|value| u64::try_from(value).ok()),
        snapshots: row
            .snapshots
            .map(|snapshots| snapshots.0)
            .unwrap_or_default(),
        vcpu: row.vcpu.and_then(|value| u32::try_from(value).ok()),
        memory_mib: row.memory_mib.and_then(|value| u32::try_from(value).ok()),
        disk_size_mb: row.disk_size_mb.and_then(|value| u32::try_from(value).ok()),
        default_auto_lifecycle: row.default_auto_lifecycle.map(|value| value.0),
        network_policy: Some(row.network_policy.0),
        network_policy_generation: u64::try_from(row.network_policy_generation).ok(),
        network_policy_status: row
            .observed_network_policy_generation
            .and_then(|generation| u64::try_from(generation).ok())
            .map(|observed_generation| NetworkPolicyStatus {
                observed_network_policy: None,
                observed_generation: Some(observed_generation),
                observed_source: Some("template".to_string()),
                observed_authority: row.observed_network_policy_authority,
                observed_at: None,
                inherited_from_template: None,
                source_template: Some(name),
                conditions: Vec::new(),
            }),
    }
}

async fn fetch_template(
    pool: &sqlx::PgPool,
    template_name: &TemplateName,
) -> Result<Option<TemplateRow>, sqlx::Error> {
    Ok(
        pplx_space_template_store::templates::list_admin(pool, Some(template_name))
            .await?
            .into_iter()
            .next(),
    )
}

/// The node-discovery read this module's reconcile enqueues begin with.
///
/// `pplx_space_cluster_trigger_queue::fetch_trigger_targets` carries its own near-copy
/// of the predicate for a different call shape; this is not the only place it appears.
///
/// Separate from the enqueue writes because the two do not classify the same
/// way. It reads `nodes` and `node_direct` and writes nothing, so on a request
/// that has not written yet it is a [`ContentionScope::ReadOnly`] statement —
/// and it can still abort: it names two relations, whose `ACCESS SHARE` locks
/// are taken one at a time as each is opened, so DDL that locks the same two
/// in the opposite order can make this read a deadlock victim. Holding no
/// locks when the statement *starts* is not the same as holding none while it
/// acquires them. Folded into the write
/// path it would answer that `40P01` with the 409's "conflicted with a
/// concurrent admin write", which is the wording `contention_disposition`
/// reserves for a statement that actually wrote.
async fn reconcile_target_node_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<NodeId>, sqlx::Error> {
    direct_registry::live_worker_and_builder_ids(&mut **tx, SubstrateKind::Space)
        .await
        .map_err(sqlx::Error::from)
}

/// The enqueue writes, for a caller that already holds the node ids.
///
/// Always [`ContentionScope::Writing`]: the only statements that can fail here
/// are the two enqueue writes, so an interleave abort is the caller's to retry.
/// The empty-`node_ids` path returns before either runs and cannot error.
async fn enqueue_template_refs_for_nodes_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_ids: Vec<NodeId>,
    refs: &[ReconcileRef],
    reason: pplx_space_state_reconciliation::ReconcileReason,
) -> Result<(), sqlx::Error> {
    if node_ids.is_empty() {
        return Ok(());
    }
    enqueue_node_reconcile_refs_for_nodes_tx(tx, node_ids.clone(), refs).await?;
    enqueue_triggers_tx(&mut **tx, node_ids, reason).await?;
    Ok(())
}

/// Discovery read plus enqueue writes, for the callers that have already
/// *issued* a write by the time they get here — `delete_template_pool`'s
/// `DELETE` may have matched no row — and so classify both the same way.
///
/// The two handlers that can reach the discovery read having written nothing —
/// `update_template` on a request that carries no field, and
/// `reconcile_base_snapshot_policy`, which never writes before its fanout —
/// call the two halves separately instead.
async fn enqueue_template_refs_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    refs: &[ReconcileRef],
    reason: pplx_space_state_reconciliation::ReconcileReason,
) -> Result<(), sqlx::Error> {
    let node_ids = reconcile_target_node_ids(tx).await?;
    enqueue_template_refs_for_nodes_tx(tx, node_ids, refs, reason).await
}

/// The fan-out for a runtime environment policy write (`runtime_environments`):
/// `refs` and a trigger on every node [`reconcile_target_node_ids`] picks, in
/// the caller's transaction, so the policy writes read Direct's tables through
/// an existing query instead of adding one. Classified as
/// [`enqueue_template_refs_tx`] is: the caller has already written.
///
/// Unlike the template writes, a ref already queued is queued again as a new
/// request ([`requeue_node_reconcile_refs_for_nodes_tx`]), due at once with
/// its attempts reset; one a node holds a claim on runs again once the claim
/// is released. A changed policy is new desired state, so it shouldn't
/// wait out the backoff of a ref that was failing to apply the old one.
pub(super) async fn requeue_reconcile_targets_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    refs: &[ReconcileRef],
    reason: pplx_space_state_reconciliation::ReconcileReason,
) -> Result<(), sqlx::Error> {
    let node_ids = reconcile_target_node_ids(tx).await?;
    if node_ids.is_empty() {
        return Ok(());
    }
    requeue_node_reconcile_refs_for_nodes_tx(tx, node_ids.clone(), refs).await?;
    enqueue_triggers_tx(&mut **tx, node_ids, reason).await?;
    Ok(())
}

fn template_refs(template_name: TemplateName) -> [ReconcileRef; 3] {
    [
        ReconcileRef::network_policy(
            pplx_space_state_reconciliation::NetworkPolicyTarget::template(template_name.clone()),
        ),
        ReconcileRef::template_desired_state(template_name.clone()),
        ReconcileRef::template_base_snapshot_policy(template_name),
    ]
}

pub async fn list_templates(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    match pplx_space_template_store::templates::list_admin(&state.pool, None)
        .await
        .map_err(sqlx::Error::from)
    {
        Ok(rows) => {
            Json(rows.into_iter().map(template_response).collect::<Vec<_>>()).into_response()
        }
        Err(error) => contention_response(ContentionScope::ReadOnly, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, "failed to list admin templates");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to list templates",
                )
            })
            .into_response(),
    }
}

pub async fn create_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(request): Json<CreateTemplateRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let template_name = match TemplateName::parse(&request.name) {
        Ok(template_name) => template_name,
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("invalid template name: {error}"),
            )
            .into_response();
        }
    };
    let managing_org_id = match managing_org_id(
        &state.pool,
        "failed to load managing org for template create",
    )
    .await
    {
        Ok(org_id) => org_id,
        Err(response) => return response.into_response(),
    };

    // `None`, so this arm carries the same *fields* it did before the eight
    // begin arms were collapsed into `begin_tx` — the event message did change,
    // for all eight of them; see `begin_tx`. The six that name a
    // path-parameter template pass `Some`; this one's name comes from the
    // request body, and adding it here would be a logging change this PR has
    // no reason to make.
    let mut tx = match begin_tx(&state.pool, None, "failed to begin template create").await {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    let create_result: Result<Option<TemplateRow>, sqlx::Error> = async {
        let row = pplx_space_template_store::templates::create_or_undelete(
            &mut *tx,
            pplx_space_template_store::templates::AdminCreate {
                id: TemplateId::new(),
                name: &template_name,
                description: request.description.as_deref(),
                org_id: managing_org_id,
                network_policy: &request.network_policy,
            },
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        pplx_space_template_store::default_auto_lifecycle_policies::upsert(
            &mut *tx,
            &template_name,
            &request.default_auto_lifecycle,
        )
        .await?;
        let refs = template_refs(template_name.clone());
        enqueue_template_refs_tx(
            &mut tx,
            &refs,
            pplx_space_state_reconciliation::ReconcileReason::TemplateDesiredStateUpdated,
        )
        .await?;
        tx.commit().await?;
        Ok(Some(row))
    }
    .await;

    match create_result {
        Ok(Some(row)) => (StatusCode::CREATED, Json(template_response(row))).into_response(),
        Ok(None) => api_error(
            StatusCode::CONFLICT,
            "Template already registered or deletion is still reconciling",
        )
        .into_response(),
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to create template");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to create template",
                )
            })
            .into_response(),
    }
}

pub async fn get_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    match fetch_template(&state.pool, &template_name).await {
        Ok(Some(row)) => Json(template_response(row)).into_response(),
        Ok(None) => api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
        Err(error) => contention_response(ContentionScope::ReadOnly, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to get template");
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get template")
            })
            .into_response(),
    }
}

pub async fn update_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
    headers: HeaderMap,
    Json(request): Json<UpdateTemplateRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let expected_generation = if request.network_policy.is_some() {
        match parse_if_match_generation(&headers) {
            Ok(value) => value,
            Err(message) => return api_error(StatusCode::BAD_REQUEST, message).into_response(),
        }
    } else {
        None
    };

    let mut tx = match begin_tx(
        &state.pool,
        Some(&template_name),
        "failed to begin template update",
    )
    .await
    {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    // The scope travels with the error instead of being fixed at the match
    // arm, because all three writes below are conditional on request fields
    // that are `Option` with a serde default: `PATCH` with `{}` deserializes
    // with none of them set, runs the preflight `SELECT` and the fanout's
    // discovery read, and writes nothing. A single `Writing` for the block
    // would answer a contended read on that path with the 409's "conflicted
    // with a concurrent admin write". `scope` is what the request has done so
    // far; a write reports `Writing` for itself.
    let update_result: Result<bool, (ContentionScope, sqlx::Error)> = async {
        let mut scope = ContentionScope::ReadOnly;
        let desired_state =
            pplx_space_template_store::templates::desired_state(&mut *tx, &template_name)
                .await
                .map_err(|error| (scope, sqlx::Error::from(error)))?;
        let Some(desired_state) = desired_state else {
            return Ok(false);
        };
        if desired_state != TemplateDesiredState::Present.as_str() {
            return Ok(false);
        }

        if let Some(policy) = &request.default_auto_lifecycle {
            pplx_space_template_store::default_auto_lifecycle_policies::upsert_if_present(
                &mut *tx,
                &template_name,
                policy,
            )
            .await
            .map_err(|error| (ContentionScope::Writing, sqlx::Error::from(error)))?;
            scope = ContentionScope::Writing;
        }

        if let Some(policy) = &request.network_policy {
            let updated = pplx_space_template_store::templates::replace_network_policy_if_present(
                &mut *tx,
                &template_name,
                policy,
                expected_generation.map(|generation| generation as i64),
            )
            .await
            .map_err(|error| (ContentionScope::Writing, sqlx::Error::from(error)))?;
            scope = ContentionScope::Writing;
            if updated.is_none() {
                return Ok(false);
            }
        }

        if let Some(description) = &request.description {
            pplx_space_template_store::templates::set_description_if_present(
                &mut *tx,
                &template_name,
                description,
            )
            .await
            .map_err(|error| (ContentionScope::Writing, sqlx::Error::from(error)))?;
            scope = ContentionScope::Writing;
        }

        let refs = template_refs(template_name.clone());
        // Split from the enqueue below so this read carries `scope` — on a
        // no-field request it is still `ReadOnly` here.
        let node_ids = reconcile_target_node_ids(&mut tx)
            .await
            .map_err(|error| (scope, error))?;
        // The enqueue writes only when the discovery read found a node, so the
        // commit's scope is derived rather than assumed: on an empty cluster a
        // fieldless update commits a transaction that wrote nothing at all.
        let fanout_wrote = !node_ids.is_empty();
        enqueue_template_refs_for_nodes_tx(
            &mut tx,
            node_ids,
            &refs,
            pplx_space_state_reconciliation::ReconcileReason::TemplateDesiredStateUpdated,
        )
        .await
        .map_err(|error| (ContentionScope::Writing, error))?;
        if fanout_wrote {
            scope = ContentionScope::Writing;
        }
        tx.commit().await.map_err(|error| (scope, error))?;
        Ok(true)
    }
    .await;

    match update_result {
        Ok(true) => match fetch_template(&state.pool, &template_name).await {
            Ok(Some(row)) => Json(template_response(row)).into_response(),
            Ok(None) => api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
            // Deliberately unclassified, unlike every other arm in this
            // module, and answered through `post_commit_error` so the envelope
            // does not advertise a retry either. `tx.commit()` has already
            // returned by the time this reload runs, so the update landed. On
            // the `If-Match` path a replay is worse than useless whenever the
            // policy actually changed — which is the case `If-Match` exists
            // for: the generation the caller matched on has been incremented,
            // so the retry answers 412 and reads as "your update did not apply"
            // for an update that did.
            Err(error) => {
                tracing::error!(%error, template = template_name.as_str(), "failed to reload template");
                post_commit_error("failed to update template").into_response()
            }
        },
        Ok(false) => api_error(
            if expected_generation.is_some() {
                StatusCode::PRECONDITION_FAILED
            } else {
                StatusCode::NOT_FOUND
            },
            if expected_generation.is_some() {
                "template network_policy_generation does not match If-Match"
            } else {
                "Template not found"
            },
        )
        .into_response(),
        Err((scope, error)) => contention_response(scope, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to update template");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to update template",
                )
            })
            .into_response(),
    }
}

fn template_delete_idempotency_key(
    headers: &HeaderMap,
) -> Result<IdempotencyKey, ApiErrorResponse> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header is required",
        ));
    };
    IdempotencyKey::parse_header(value.as_bytes())
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))
}

fn template_delete_fingerprint(
    template_name: &TemplateName,
) -> Result<RequestFingerprint, ApiErrorResponse> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "template": template_name.as_str(),
        "action": "delete",
        "admin": true,
    }))
    .map_err(|error| {
        tracing::error!(%error, "failed to serialize template delete request fingerprint");
        api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(%error, "failed to build template delete request fingerprint");
        api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })
}

pub async fn delete_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }

    let idempotency_key = match template_delete_idempotency_key(&headers) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let managing_org_id = match managing_org_id(
        &state.pool,
        "failed to load managing org for template delete",
    )
    .await
    {
        Ok(org_id) => org_id,
        Err(response) => return response.into_response(),
    };

    // The third field is false when the recorded delete operation reached a
    // terminal state or was garbage-collected. Such a delete can never finish
    // on its own, so a new request replaces it instead of returning it.
    let existing_state = match pplx_space_template_store::templates::delete_state(
        &state.pool,
        &template_name,
    )
    .await
    .map_err(sqlx::Error::from)
    {
        Ok(value) => value.map(|row| {
            (
                row.desired_state,
                row.delete_operation_id,
                row.delete_operation_live,
            )
        }),
        Err(error) => {
            if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                return response.into_response();
            }
            tracing::error!(%error, template = template_name.as_str(), "failed to load template delete state");
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                .into_response();
        }
    };

    let mut stale_delete_operation_id = None;
    match existing_state {
        Some((state, Some(operation_id), true))
            if state == TemplateDesiredState::Deleting.as_str() =>
        {
            return (
                StatusCode::ACCEPTED,
                Json(TemplateOperationResponse {
                    operation_id: pplx_space_public_api::OperationId::from_uuid(operation_id),
                    template: template_name.to_string(),
                }),
            )
                .into_response();
        }
        Some((state, Some(operation_id), false))
            if state == TemplateDesiredState::Deleting.as_str() =>
        {
            tracing::info!(
                template = template_name.as_str(),
                stale_operation_id = %operation_id,
                "template delete operation is no longer active; starting a replacement"
            );
            stale_delete_operation_id = Some(operation_id);
        }
        Some((state, _, _)) if state == TemplateDesiredState::Deleted.as_str() => {
            return api_error(StatusCode::NOT_FOUND, "Template not found").into_response();
        }
        Some(_) => {}
        None => return api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
    }

    let fingerprint = match template_delete_fingerprint(&template_name) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let template_target = template_name.to_string();
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Delete,
        path_template: OperationPathTemplate::new("/admin/v1/templates/{template}"),
        key: &idempotency_key,
        request_fingerprint: &fingerprint,
    };
    let store = PublicOperationStore::new(state.pool.clone());
    let mut admission = match store
        .begin_operation_admission(managing_org_id, ApiProduct::Admin, &idempotency)
        .await
    {
        Ok(admission) => admission,
        Err(error) => {
            if let Some(response) = store_contention_response(ContentionScope::Writing, &error) {
                return response.into_response();
            }
            tracing::error!(%error, template = template_name.as_str(), "failed to look up template delete idempotency key");
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                .into_response();
        }
    };

    // Refuse a new request before creating an operation or marking the
    // template `deleting`: the operation cannot delete releases that warm
    // pools still hold. It checks again for a use that appears after this
    // read. A replayed or conflicting key answers as it did the first time,
    // whatever uses the template now. The check reads on the admission's
    // transaction, so a request holds one connection, not two.
    if matches!(
        admission.observation(),
        ObservePublicOperationIdempotency::Missing
    ) {
        match TemplateInUse::check(admission.transaction(), &template_name).await {
            Ok(None) => {}
            Ok(Some(in_use)) => {
                return api_error_with_details(
                    StatusCode::CONFLICT,
                    in_use.message(),
                    in_use.details(),
                )
                .into_response();
            }
            Err(error) => {
                if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                    return response.into_response();
                }
                tracing::error!(%error, template = template_name.as_str(), "failed to check template use before delete");
                return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                    .into_response();
            }
        }
    }

    let operation = match admission
        .create(CreatePublicOperation {
            org_id: managing_org_id,
            actor_user_id: Some(actor.id),
            api_product: ApiProduct::Admin,
            kind: OperationKind::DeleteTemplate,
            target_type: Some(OperationTargetType::Template),
            target_id: Some(&template_target),
            payload_json: DeleteTemplateOperationPayload::new(template_name.clone()).into_json(),
            idempotency: Some(idempotency),
            traceparent: crate::public_operations::request_traceparent(),
        })
        .await
    {
        Ok(CreatePublicOperationResult::Created(operation_id)) => operation_id,
        Ok(CreatePublicOperationResult::Existing(operation_id)) => {
            // A replayed key answers with its original operation. Once that
            // operation is terminal it must not become the template's
            // current delete: it would never run, and installing it could
            // displace an active replacement. The caller retries with a new
            // key.
            match pplx_space_public_operation_store::operations::is_active(
                &state.pool,
                operation_id,
            )
            .await
            {
                Ok(true) => operation_id,
                Ok(false) => {
                    return (
                        StatusCode::ACCEPTED,
                        Json(TemplateOperationResponse {
                            operation_id,
                            template: template_name.to_string(),
                        }),
                    )
                        .into_response();
                }
                Err(error) => {
                    let error = sqlx::Error::from(error);
                    if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                        return response.into_response();
                    }
                    tracing::error!(%error, template = template_name.as_str(), "failed to load replayed template delete operation");
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                        .into_response();
                }
            }
        }
        Ok(CreatePublicOperationResult::IdempotencyConflict { operation_id, .. }) => {
            let details = operation_id
                .map(|id| PublicErrorDetail {
                    key: "operation_id".to_string(),
                    value: id.to_string(),
                })
                .into_iter()
                .collect();
            return api_error_with_details(
                StatusCode::CONFLICT,
                "idempotency key was already used with a different request",
                details,
            )
            .into_response();
        }
        Ok(CreatePublicOperationResult::AdmissionDenied) => {
            tracing::error!(
                template = template_name.as_str(),
                "template delete operation unexpectedly refused admission"
            );
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                .into_response();
        }
        Err(error) => {
            // Retryable despite being a write: the operation is keyed on the
            // caller's required `Idempotency-Key`, so a replay of the same
            // request returns that operation's id rather than opening a second
            // delete.
            if let Some(response) = store_contention_response(ContentionScope::Writing, &error) {
                return response.into_response();
            }
            tracing::error!(%error, template = template_name.as_str(), "failed to create template delete operation");
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                .into_response();
        }
    };

    let mut tx = match begin_tx(
        &state.pool,
        Some(&template_name),
        "failed to begin template delete mark",
    )
    .await
    {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    let mark_result: Result<u64, sqlx::Error> = async {
        // The operation id is replaced only when it still names the stale
        // operation observed above, so a concurrent replacement keeps its
        // own id instead of being overwritten.
        let updated = pplx_space_template_store::templates::request_delete(
            &mut *tx,
            &template_name,
            actor.id,
            operation.as_uuid(),
            stale_delete_operation_id,
        )
        .await?;
        if updated > 0 {
            enqueue_template_refs_tx(
                &mut tx,
                &[ReconcileRef::template_desired_state(template_name.clone())],
                pplx_space_state_reconciliation::ReconcileReason::TemplateDesiredStateUpdated,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(updated)
    }
    .await;

    match mark_result {
        Ok(0) => api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(TemplateOperationResponse {
                operation_id: operation,
                template: template_name.to_string(),
            }),
        )
            .into_response(),
        // Retryable even though the delete operation row is already committed
        // above: nothing outside the database has run yet, and a replay of the
        // same request carries the same `Idempotency-Key`, so it adopts that
        // operation rather than opening a second one. The `UPDATE` itself is
        // `COALESCE`-guarded and re-applies to the same values.
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to mark template deleting");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete template",
                )
            })
            .into_response(),
    }
}

#[derive(Debug, thiserror::Error)]
enum TemplateBuildRequestError {
    #[error("recipe.origin must set exactly one of from_template or from_scratch")]
    OriginOneof,
    #[error("recipe.origin.from_scratch.source must set ubuntu_minimal_cloud_squashfs")]
    ScratchSourceMissing,
    #[error("recipe.steps[{index}] must set exactly one step body")]
    StepBodyOneof { index: usize },
    #[error(
        "recipe.steps[{index}].copy must set exactly one of inline_bytes_base64 or blob_sha256"
    )]
    CopyPayloadOneof { index: usize },
    #[error("recipe.steps[{index}].copy.inline_bytes_base64 is invalid: {source}")]
    InlineBytesBase64 {
        index: usize,
        #[source]
        source: base64::DecodeError,
    },
    #[error("invalid publish tag {tag:?}: {source}")]
    PublishTag {
        tag: String,
        #[source]
        source: pplx_space_template_model::TemplateTagParseError,
    },
}

fn template_build_state_response(state: build_proto::BuildState) -> TemplateBuildStateResponse {
    match state {
        build_proto::BuildState::Pending => TemplateBuildStateResponse::Pending,
        build_proto::BuildState::Running => TemplateBuildStateResponse::Running,
        build_proto::BuildState::Succeeded => TemplateBuildStateResponse::Succeeded,
        build_proto::BuildState::Failed => TemplateBuildStateResponse::Failed,
        build_proto::BuildState::PublishFailed => TemplateBuildStateResponse::PublishFailed,
        build_proto::BuildState::Unspecified => TemplateBuildStateResponse::Unspecified,
    }
}

fn optional_nonempty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn normalize_publish_tags(tags: Vec<String>) -> Result<Vec<String>, TemplateBuildRequestError> {
    tags.into_iter()
        .map(|tag| {
            TemplateTag::parse(tag.trim())
                .map(|tag| tag.as_str().to_string())
                .map_err(|source| TemplateBuildRequestError::PublishTag { tag, source })
        })
        .collect()
}

fn template_build_recipe_proto(
    recipe: TemplateBuildRecipe,
) -> Result<build_proto::Recipe, TemplateBuildRequestError> {
    Ok(build_proto::Recipe {
        metadata: Some(build_proto::recipe::Metadata {
            description: recipe.metadata.description,
        }),
        build_config: recipe.build_config.map(template_build_config_proto),
        origin: Some(template_build_origin_proto(recipe.origin)?),
        steps: recipe
            .steps
            .into_iter()
            .enumerate()
            .map(|(index, step)| template_build_step_proto(index + 1, step))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn template_build_config_proto(config: TemplateBuildConfig) -> build_proto::recipe::BuildConfig {
    build_proto::recipe::BuildConfig {
        vcpu: config.vcpu,
        memory_bytes: config.memory_bytes,
        disk_bytes: config.disk_bytes,
        balloon: config
            .balloon
            .map(pplx_space_build_mgr::recipe::balloon_proto),
    }
}

fn template_build_origin_proto(
    origin: TemplateBuildOrigin,
) -> Result<build_proto::recipe::Origin, TemplateBuildRequestError> {
    let kind = match (origin.from_template, origin.from_scratch) {
        (Some(from_template), None) => build_proto::recipe::origin::Kind::FromTemplate(
            build_proto::recipe::origin::FromTemplate {
                base: from_template.base,
            },
        ),
        (None, Some(from_scratch)) => {
            let source = Some(template_build_source_proto(from_scratch.source)?);
            match from_scratch.guest_kernel {
                Some(guest_kernel) => {
                    let guest_kernel = build_proto::recipe::origin::GuestKernel {
                        version: guest_kernel.version.to_string(),
                        digest: guest_kernel.digest.to_string(),
                    };
                    build_proto::recipe::origin::Kind::PinnedFromScratch(
                        build_proto::recipe::origin::PinnedFromScratch {
                            source,
                            guest_kernel: Some(guest_kernel),
                        },
                    )
                }
                None => build_proto::recipe::origin::Kind::FromScratch(
                    build_proto::recipe::origin::FromScratch { source },
                ),
            }
        }
        _ => return Err(TemplateBuildRequestError::OriginOneof),
    };
    Ok(build_proto::recipe::Origin { kind: Some(kind) })
}

fn template_build_source_proto(
    source: TemplateBuildSource,
) -> Result<build_proto::recipe::Source, TemplateBuildRequestError> {
    let Some(source) = source.ubuntu_minimal_cloud_squashfs else {
        return Err(TemplateBuildRequestError::ScratchSourceMissing);
    };
    Ok(build_proto::recipe::Source {
        kind: Some(
            build_proto::recipe::source::Kind::UbuntuMinimalCloudSquashfs(
                build_proto::recipe::source::UbuntuMinimalCloudSquashfs {
                    release: source.release,
                    version: source.version,
                    date: source.date,
                },
            ),
        ),
    })
}

fn template_build_step_proto(
    index: usize,
    step: TemplateBuildStep,
) -> Result<build_proto::recipe::Step, TemplateBuildRequestError> {
    let bodies = [
        step.set_runtime.is_some(),
        step.clear_runtime.is_some(),
        step.package_install.is_some(),
        step.run.is_some(),
        step.env.is_some(),
        step.copy.is_some(),
        step.playwright_install.is_some(),
    ];
    if bodies.into_iter().filter(|present| *present).count() != 1 {
        return Err(TemplateBuildRequestError::StepBodyOneof { index });
    }
    let body = if let Some(runtime) = step.set_runtime {
        build_proto::recipe::step::Body::SetRuntime(pplx_space_build_mgr_client::runtime::encode(
            &runtime,
        ))
    } else if step.clear_runtime.is_some() {
        build_proto::recipe::step::Body::ClearRuntime(build_proto::recipe::ClearRuntime {})
    } else if let Some(step) = step.package_install {
        build_proto::recipe::step::Body::PackageInstall(build_proto::recipe::PackageInstall {
            manager: template_build_package_manager_proto(step.manager) as i32,
            packages: step.packages,
            extra_args: step.extra_args,
        })
    } else if let Some(step) = step.run {
        build_proto::recipe::step::Body::Run(build_proto::recipe::Run {
            command: step.command,
            user: step.user,
        })
    } else if let Some(step) = step.env {
        build_proto::recipe::step::Body::Env(build_proto::recipe::Env {
            vars: step.vars.into_iter().collect(),
        })
    } else if let Some(step) = step.copy {
        build_proto::recipe::step::Body::Copy(template_build_copy_proto(index, step)?)
    } else if let Some(step) = step.playwright_install {
        build_proto::recipe::step::Body::PlaywrightInstall(build_proto::recipe::PlaywrightInstall {
            browsers: step.browsers,
        })
    } else {
        return Err(TemplateBuildRequestError::StepBodyOneof { index });
    };
    Ok(build_proto::recipe::Step {
        display_name: step.display_name,
        // Keep guest names intact; only the builder can authorize and resolve exports.
        env_refs: step.env_refs,
        body: Some(body),
    })
}

fn template_build_package_manager_proto(
    manager: TemplateBuildPackageManager,
) -> build_proto::recipe::PackageManager {
    match manager {
        TemplateBuildPackageManager::Apt => build_proto::recipe::PackageManager::Apt,
        TemplateBuildPackageManager::Pip => build_proto::recipe::PackageManager::Pip,
        TemplateBuildPackageManager::Npm => build_proto::recipe::PackageManager::Npm,
        TemplateBuildPackageManager::NpmGlobal => build_proto::recipe::PackageManager::NpmGlobal,
    }
}

fn template_build_copy_proto(
    index: usize,
    step: TemplateBuildCopyStep,
) -> Result<build_proto::recipe::Copy, TemplateBuildRequestError> {
    let payload = match (step.inline_bytes_base64, step.blob_sha256) {
        (Some(bytes), None) => {
            let bytes = BASE64_STANDARD
                .decode(bytes)
                .map_err(|source| TemplateBuildRequestError::InlineBytesBase64 { index, source })?;
            build_proto::recipe::copy::Payload::InlineBytes(bytes)
        }
        (None, Some(sha)) => build_proto::recipe::copy::Payload::BlobSha256(sha),
        _ => return Err(TemplateBuildRequestError::CopyPayloadOneof { index }),
    };
    Ok(build_proto::recipe::Copy {
        payload: Some(payload),
        dest: step.dest,
        source_path: step.source_path,
    })
}

async fn reject_deleted_template_build(
    pool: &sqlx::PgPool,
    template_name: &TemplateName,
) -> Result<(), ApiErrorResponse> {
    let state = pplx_space_template_store::templates::desired_state(pool, template_name)
        .await
        .map_err(sqlx::Error::from)
        .map_err(|error| {
            contention_response(ContentionScope::ReadOnly, &error).unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to load template build state");
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            })
        })?;
    match state.as_deref() {
        Some(state) if state == TemplateDesiredState::Deleting.as_str() => Err(api_error(
            StatusCode::CONFLICT,
            "Template deletion is still reconciling",
        )),
        Some(state) if state == TemplateDesiredState::Deleted.as_str() => Err(api_error(
            StatusCode::CONFLICT,
            "Template is deleted; recreate it before building",
        )),
        _ => Ok(()),
    }
}

pub async fn start_template_build(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
    Json(request): Json<TemplateBuildRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    if template_name.as_str().parse::<SystemTemplate>().is_ok() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "Reserved system template; use /v1/system-templates/{template}/builds",
        )
        .into_response();
    }
    if let Err(error) = reject_deleted_template_build(&state.pool, &template_name).await {
        return error.into_response();
    }
    let recipe = match template_build_recipe_proto(request.recipe) {
        Ok(recipe) => recipe,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let publish_tags = match normalize_publish_tags(request.publish_tags) {
        Ok(tags) => tags,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    submit_admin_template_build(
        state,
        TemplateBuildTarget::Organization(template_name),
        recipe,
        request.force,
        publish_tags,
        request.client,
        None,
    )
    .await
}

/// The idempotency namespace for system template builds. Its records hold the
/// build's response, not an operation, so this path must never meet
/// `PublicOperationStore::create_operation`.
const SYSTEM_TEMPLATE_BUILD_PATH_TEMPLATE: &str = "/admin/v1/system-templates/{template}/builds";

/// Qualification only: installation and execution policy are checked on the builder.
pub async fn list_system_template_environments(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    use pplx_space_cpu_arch::Arch;
    use pplx_space_execution_catalog::catalog::CompiledEnvironment;
    let mut environments = Vec::new();
    for architecture in [Arch::X86_64, Arch::Aarch64] {
        let catalog = match CompiledEnvironment::cached_for_architecture(architecture) {
            Ok(catalog) => catalog,
            Err(error) => {
                tracing::error!(%error, "failed to read qualified build environments");
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Cannot read qualified runtime/kernel pairs",
                )
                .into_response();
            }
        };
        for entry in catalog {
            // System recipes currently implement direct Linux kernel boot only.
            let Some(kernel) = entry.kernel() else {
                continue;
            };
            environments.push(serde_json::json!({
                "environment": entry.definition().reference(),
                "name": entry.name(),
                "architecture": architecture,
                "runtime": entry.release().release.kind,
                "runtime_version": entry.release().release.version,
                "kernel_version": kernel.version,
            }));
        }
    }
    Json(serde_json::json!({ "environments": environments })).into_response()
}

pub async fn start_system_template_build(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template): Path<SystemTemplate>,
    headers: HeaderMap,
    Json(request): Json<SystemTemplateBuildRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let idempotency_key = match optional_idempotency_key(&headers) {
        Ok(key) => key,
        Err(error) => return error.into_response(),
    };
    if let Some(base) = &request.base
        && (template != SystemTemplate::CodeRuntime || base.name().as_str() != "seed")
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "base is only supported for code-runtime and must reference seed",
        )
        .into_response();
    }
    let publish_tags = match normalize_publish_tags(request.publish_tags) {
        Ok(tags) => tags,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let base = request.base.map(|base| base.to_string());
    let managing_org_id = match managing_org_id(
        &state.pool,
        "failed to load managing org for template build",
    )
    .await
    {
        Ok(org_id) => org_id,
        Err(response) => return response.into_response(),
    };
    let build = SystemTemplateBuild {
        managing_org_id,
        template,
        base,
        force: request.force,
        publish_tags,
        environment: request.environment,
    };
    let Some(key) = idempotency_key else {
        return match build.dispatch(&state).await {
            Ok(started) => (StatusCode::ACCEPTED, Json(started)).into_response(),
            Err(failure) => *failure.response,
        };
    };
    let fingerprint = match system_template_build_fingerprint(
        template,
        build.base.as_deref(),
        build.force,
        &build.publish_tags,
        build.environment,
    ) {
        Ok(fingerprint) => fingerprint,
        Err(error) => return error.into_response(),
    };
    // Spawned so that a client disconnect, which drops this handler, cannot
    // cancel the reservation or the dispatch it stands for halfway: the task
    // reserves, dispatches and records the outcome whether or not anyone is
    // still waiting.
    match tokio::spawn(build.submit_once(state, key, fingerprint).in_current_span()).await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(%error, %template, "system template build submission task failed");
            post_commit_error("Internal server error").into_response()
        }
    }
}

/// How long a system build's idempotency reservation counts as in flight
/// before a replay reports its outcome unknown. It outlasts the builder RPC
/// attempts, the one part of the submission with a known bound; the
/// database work and builder discovery around them have none, so a
/// submission can still be running past it.
const SYSTEM_TEMPLATE_BUILD_RESERVATION: Duration = Duration::from_secs(15 * 60);
const _: () =
    assert!(SYSTEM_TEMPLATE_BUILD_RESERVATION.as_secs() > TEMPLATE_BUILD_SUBMIT_BUDGET.as_secs());

/// A validated system template build request.
struct SystemTemplateBuild {
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
    managing_org_id: OrgId,
    template: SystemTemplate,
    base: Option<String>,
    force: bool,
    publish_tags: Vec<String>,
}

impl SystemTemplateBuild {
    /// Submit this build at most once under `key`, answering a replay from
    /// the key's record.
    ///
    /// The builder starts a new job per submission and takes no caller
    /// identity to deduplicate on, so the key is reserved, committed, before
    /// dispatch, and a replay meets the reservation instead of the builder.
    /// The reservation is released only when the builder certainly did not
    /// take the build; on any other failure it stays pending, so the key
    /// never starts a second job while its record lives.
    async fn submit_once(
        self,
        state: AppState,
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
    ) -> axum::response::Response {
        let idempotency = CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new(SYSTEM_TEMPLATE_BUILD_PATH_TEMPLATE),
            key: &key,
            request_fingerprint: &fingerprint,
        };
        let store = PublicOperationStore::new(state.pool.clone());
        let reservation = match store
            .reserve_response(
                self.managing_org_id,
                ApiProduct::Admin,
                &idempotency,
                SYSTEM_TEMPLATE_BUILD_RESERVATION,
            )
            .await
        {
            Ok(ResponseReservation::Reserved(reservation)) => reservation,
            Ok(ResponseReservation::Completed(response)) => {
                return (StatusCode::ACCEPTED, Json(response)).into_response();
            }
            Ok(ResponseReservation::InProgress { reserved_at }) => {
                return build_reservation_conflict(
                    "a build with this Idempotency-Key may still be being submitted; retry shortly"
                        .to_string(),
                    reserved_at,
                    None,
                    true,
                )
                .into_response();
            }
            Ok(ResponseReservation::OutcomeUnknown { reserved_at, known }) => {
                return build_reservation_conflict(
                    format!(
                        "the build submitted with this Idempotency-Key at {} has an unknown \
                         outcome: it failed without proof that the builder did not take it, or \
                         recorded nothing within 15 minutes. It may have started and may still \
                         be running. Check Template Releases (/template-releases), or the \
                         build's status when job_id is given, before submitting again with a \
                         new key",
                        format_time(reserved_at)
                    ),
                    reserved_at,
                    known.as_ref(),
                    false,
                )
                .into_response();
            }
            Err(error) => return response_admission_error(&error).into_response(),
        };
        let template = self.template;
        match self.dispatch(&state).await {
            // The job is running, so a failure to record it is logged, not
            // answered: an error would invite the retry that starts a second
            // job. The reservation, still pending, answers that retry instead.
            Ok(started) => {
                let response =
                    serde_json::to_value(&started).expect("build start response serializes");
                match store
                    .complete_response_reservation(reservation, response)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => tracing::error!(
                        %template,
                        job_id = %started.job_id,
                        "the system template build's idempotency reservation was no longer pending"
                    ),
                    Err(error) => tracing::error!(
                        %error,
                        %template,
                        job_id = %started.job_id,
                        "failed to record the system template build's idempotency key; \
                         replays report it in progress, then of unknown outcome"
                    ),
                }
                (StatusCode::ACCEPTED, Json(started)).into_response()
            }
            Err(failure) => {
                match failure.outcome {
                    BuildDispatchOutcome::NotAccepted => {
                        if let Err(error) = store.release_response_reservation(reservation).await {
                            tracing::warn!(
                                %error,
                                %template,
                                "failed to release an undispatched system template build's \
                                 idempotency reservation; replays report it in progress, then \
                                 of unknown outcome"
                            );
                        }
                    }
                    // Nothing will complete the record now, so it is noted
                    // unresolved and replays say so at once instead of
                    // asking for a retry that cannot succeed.
                    BuildDispatchOutcome::MaybeAccepted { job_id } => {
                        let known = match job_id {
                            Some(job_id) => serde_json::json!({ "job_id": job_id.to_string() }),
                            None => serde_json::json!({}),
                        };
                        if let Err(error) = store.note_unresolved_response(reservation, known).await
                        {
                            tracing::warn!(
                                %error,
                                %template,
                                ?job_id,
                                "failed to note a system template build the builder may have \
                                 taken; replays report it in progress, then of unknown outcome"
                            );
                        }
                    }
                }
                *failure.response
            }
        }
    }

    async fn dispatch(
        &self,
        state: &AppState,
    ) -> Result<TemplateBuildStartResponse, BuildDispatchFailure> {
        let template_name = self.template.into();
        let not_accepted = |response: axum::response::Response| BuildDispatchFailure {
            response: Box::new(response),
            outcome: BuildDispatchOutcome::NotAccepted,
        };
        reject_deleted_template_build(&state.pool, &template_name)
            .await
            .map_err(|error| not_accepted(error.into_response()))?;
        let mut recipe = system_template_recipe(self.template, self.base.as_deref()).map_err(
            |error| {
                tracing::error!(%error, template = %self.template, "invalid bundled system template recipe");
                not_accepted(
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Invalid bundled system template recipe",
                    )
                    .into_response(),
                )
            },
        )?;
        if self.environment.is_some() {
            // An explicit pair supplies the kernel, rather than the stock recipe's pin.
            // The builder resolves and verifies that pair before any VM is created.
            use build_proto::recipe::origin::{FromScratch, Kind};
            if let Some(origin) = recipe.origin.as_mut()
                && let Some(Kind::PinnedFromScratch(pinned)) = origin.kind.as_ref()
            {
                origin.kind = Some(Kind::FromScratch(FromScratch {
                    source: pinned.source.clone(),
                }));
            }
        }
        dispatch_admin_template_build(
            state,
            TemplateBuildSubmission {
                environment: self.environment,
                org_id: self.managing_org_id,
                target: TemplateBuildTarget::System(self.template),
                client_build_id: None,
                recipe,
                force: self.force,
                publish_tags: self.publish_tags.clone(),
                client: admin_build_client(None),
            },
        )
        .await
    }
}

/// A failed admin build dispatch: the response for the caller, and whether
/// the builder may have taken the build.
struct BuildDispatchFailure {
    response: Box<axum::response::Response>,
    outcome: BuildDispatchOutcome,
}

enum BuildDispatchOutcome {
    /// Nothing was dispatched, or the builder provably refused it.
    NotAccepted,
    /// The builder may have taken it; `job_id` is the job it named, if any.
    MaybeAccepted { job_id: Option<BuildRunId> },
}

/// A replay that met another request's still-pending reservation, with the
/// job that request learned of, if any.
fn build_reservation_conflict(
    message: String,
    reserved_at: OffsetDateTime,
    known: Option<&serde_json::Value>,
    retryable: bool,
) -> ApiErrorResponse {
    let mut details = vec![PublicErrorDetail {
        key: "reserved_at".to_string(),
        value: format_time(reserved_at),
    }];
    if let Some(job_id) = known
        .and_then(|known| known.get("job_id"))
        .and_then(serde_json::Value::as_str)
    {
        details.push(PublicErrorDetail {
            key: "job_id".to_string(),
            value: job_id.to_string(),
        });
    }
    let (status, Json(mut envelope)) =
        api_error_with_details(StatusCode::CONFLICT, message, details);
    envelope.retryable = retryable;
    (status, Json(envelope))
}

/// The `Idempotency-Key` header when the caller sent one.
fn optional_idempotency_key(
    headers: &HeaderMap,
) -> Result<Option<IdempotencyKey>, ApiErrorResponse> {
    headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .map(|value| {
            IdempotencyKey::parse_header(value.as_bytes())
                .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))
        })
        .transpose()
}

/// What makes two system template builds the same request: the normalized
/// body, field for field, so a replay that changes any field is refused
/// rather than silently answered with a job built from different input.
fn system_template_build_fingerprint(
    template: SystemTemplate,
    base: Option<&str>,
    force: bool,
    publish_tags: &[String],
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
) -> Result<RequestFingerprint, ApiErrorResponse> {
    let mut body = serde_json::Map::from_iter([
        (
            "template".to_string(),
            serde_json::json!(template.to_string()),
        ),
        ("base".to_string(), serde_json::json!(base)),
        ("force".to_string(), serde_json::json!(force)),
        ("publish_tags".to_string(), serde_json::json!(publish_tags)),
    ]);
    // Preserve fingerprints for legacy requests whose keys predate selection.
    if let Some(environment) = environment {
        body.insert("environment".to_string(), serde_json::json!(environment));
    }
    let bytes = serde_json::to_vec(&body).map_err(|error| {
        tracing::error!(%error, "failed to serialize system template build fingerprint");
        api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    RequestFingerprint::parse(hex::encode(Sha256::digest(bytes))).map_err(|error| {
        tracing::error!(%error, "failed to build system template build fingerprint");
        api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })
}

fn response_admission_error(
    error: &crate::public_operations::ResponseAdmissionError,
) -> ApiErrorResponse {
    use crate::public_operations::ResponseAdmissionError;
    let sql = match error {
        ResponseAdmissionError::Conflict => {
            return api_error(
                StatusCode::CONFLICT,
                "idempotency key was already used with a different request",
            );
        }
        ResponseAdmissionError::Sql(error)
        | ResponseAdmissionError::Store(PublicOperationStoreError::Sql(error)) => Some(error),
        ResponseAdmissionError::Store(_) => None,
    };
    sql.and_then(|error| contention_response(ContentionScope::Writing, error))
        .unwrap_or_else(|| {
            tracing::error!(%error, "failed to admit system template build idempotency key");
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        })
}

fn system_template_recipe(
    template: SystemTemplate,
    base: Option<&str>,
) -> Result<build_proto::Recipe, pplx_space_build_mgr::recipe::RecipeError> {
    use pplx_space_build_mgr::recipe::{RecipeAdapterError, RecipeError, TomlRecipe};
    let content = match template {
        SystemTemplate::Seed => {
            include_str!("../../../../../../templates/firecracker/recipes/seed.toml")
        }
        SystemTemplate::CodeRuntime => {
            include_str!("../../../../../../templates/firecracker/recipes/code-runtime.toml")
        }
    };
    let path = format!("templates/firecracker/recipes/{template}.toml");
    let recipe: TomlRecipe = toml::from_str(content).map_err(|source| RecipeError::Parse {
        path: path.clone(),
        source,
    })?;
    // The recipes are compiled in, so the files they copy must be too.
    recipe.into_proto(&path, base, |index, source| {
        let bytes: &[u8] = match source {
            "code-runtime/pyproject.toml" => include_bytes!(
                "../../../../../../templates/firecracker/recipes/code-runtime/pyproject.toml"
            ),
            "code-runtime/uv.lock" => include_bytes!(
                "../../../../../../templates/firecracker/recipes/code-runtime/uv.lock"
            ),
            _ => {
                return Err(RecipeAdapterError::MissingCopyPayload {
                    index,
                    source_path: source.to_string(),
                });
            }
        };
        Ok(build_proto::recipe::copy::Payload::InlineBytes(
            bytes.to_vec(),
        ))
    })
}

async fn submit_admin_template_build(
    state: AppState,
    target: TemplateBuildTarget,
    recipe: build_proto::Recipe,
    force: bool,
    publish_tags: Vec<String>,
    client: Option<TemplateBuildClientContext>,
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
) -> axum::response::Response {
    let managing_org_id = match managing_org_id(
        &state.pool,
        "failed to load managing org for template build",
    )
    .await
    {
        Ok(org_id) => org_id,
        Err(response) => return response.into_response(),
    };
    match dispatch_admin_template_build(
        &state,
        TemplateBuildSubmission {
            environment,
            org_id: managing_org_id,
            target,
            client_build_id: None,
            recipe,
            force,
            publish_tags,
            client: admin_build_client(client),
        },
    )
    .await
    {
        Ok(started) => (StatusCode::ACCEPTED, Json(started)).into_response(),
        Err(failure) => *failure.response,
    }
}

fn admin_build_client(client: Option<TemplateBuildClientContext>) -> build_proto::ClientContext {
    let client = client.unwrap_or_default();
    build_proto::ClientContext {
        source_path: client.source_path.unwrap_or_default(),
        cli_version: client
            .cli_version
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
        hostname: client
            .hostname
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_default(),
    }
}

async fn dispatch_admin_template_build(
    state: &AppState,
    submission: TemplateBuildSubmission,
) -> Result<TemplateBuildStartResponse, BuildDispatchFailure> {
    let template_name = submission.target.template_name();
    let response = state
        .template_controller
        .submit_template_build(submission)
        .await;
    match response {
        Ok(response) => Ok(TemplateBuildStartResponse {
            job_id: response.job_id,
            template_name: template_name.to_string(),
        }),
        Err(error) => {
            let outcome = match &error {
                TemplateBuildSubmitError::NotAccepted(_) => BuildDispatchOutcome::NotAccepted,
                TemplateBuildSubmitError::MaybeAccepted { job_id, .. } => {
                    BuildDispatchOutcome::MaybeAccepted { job_id: *job_id }
                }
            };
            Err(BuildDispatchFailure {
                response: Box::new(ControllerError::from(error).into_response()),
                outcome,
            })
        }
    }
}

pub async fn get_template_build_status(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path((template_name, job_id)): Path<(TemplateName, String)>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let response = state
        .template_controller
        .get_template_build_status_by_job_id(job_id.clone())
        .await;
    match response {
        Ok(response) if response.template_id == template_name.as_str() => {
            let state = build_proto::BuildState::try_from(response.state)
                .unwrap_or(build_proto::BuildState::Unspecified);
            Json(TemplateBuildStatusResponse {
                job_id,
                template_name: template_name.to_string(),
                state: template_build_state_response(state),
                content_hash: optional_nonempty(response.content_hash),
                error: optional_nonempty(response.error),
            })
            .into_response()
        }
        Ok(_) => api_error(StatusCode::NOT_FOUND, "Build job not found").into_response(),
        Err(error) => error.into_response(),
    }
}

/// Builds listed when the caller omits `limit`.
const DEFAULT_TEMPLATE_BUILDS_LIMIT: u32 = 20;
/// Largest `limit`. Each listed job costs one builder status call, so this and
/// the two bounds below keep the route within its latency budget.
const MAX_TEMPLATE_BUILDS_LIMIT: u32 = 50;
const TEMPLATE_BUILD_STATUS_CONCURRENCY: usize = 16;
const TEMPLATE_BUILD_STATUS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListTemplateBuildsQuery {
    limit: Option<u32>,
}

/// `GET /templates/{template_name}/builds`: the template's recorded build
/// jobs, newest first, each with the status its builder reports now.
///
/// Postgres records which builder accepted each job; state, content hash and
/// failure message live only on that builder. The page resolves one client
/// per distinct builder, then makes one bounded status call per job. Jobs with
/// no recorded builder (routed without one, or whose builder's node row has
/// since been deleted) are asked through unpinned builder discovery, as the
/// single-build status route asks them. A job its builder cannot report still
/// lists, with `state` unset and `status_error` saying why; a builder that
/// fails to resolve or connect stamps the same error on each of its jobs.
pub async fn list_template_builds(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
    axum::extract::Query(query): axum::extract::Query<ListTemplateBuildsQuery>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let limit = query.limit.unwrap_or(DEFAULT_TEMPLATE_BUILDS_LIMIT);
    if !(1..=MAX_TEMPLATE_BUILDS_LIMIT).contains(&limit) {
        return api_error(
            StatusCode::BAD_REQUEST,
            format!("limit must be between 1 and {MAX_TEMPLATE_BUILDS_LIMIT}"),
        )
        .into_response();
    }
    match pplx_space_template_store::templates::exists_not_deleted(&state.pool, &template_name)
        .await
        .map_err(sqlx::Error::from)
    {
        Ok(true) => {}
        Ok(false) => return api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
        Err(error) => {
            return contention_response(ContentionScope::ReadOnly, &error)
                .unwrap_or_else(|| {
                    tracing::error!(%error, template = template_name.as_str(), "failed to load template for build listing");
                    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                })
                .into_response();
        }
    }
    let builds = match state
        .template_controller
        .recent_template_builds(&template_name, limit)
        .await
    {
        Ok(builds) => builds,
        Err(error) => return error.into_response(),
    };
    // One client per distinct builder on the page: resolving it reads the
    // builder's node row and opens a connection, so doing that per job would
    // multiply both by the page size. Only the status RPCs fan out per job.
    let builders: std::collections::BTreeSet<Option<NodeId>> =
        builds.iter().map(|build| build.builder_node_id).collect();
    let controller = &state.template_controller;
    let clients: HashMap<Option<NodeId>, Result<BuilderClient, String>> =
        futures_util::stream::iter(builders)
            .map(|builder| async move {
                let client = tokio::time::timeout(
                    TEMPLATE_BUILD_STATUS_TIMEOUT,
                    controller.builder_client_for_job(builder),
                )
                .await;
                let client = match client {
                    Ok(Ok(client)) => Ok(client),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err(status_timeout_message("builder connection")),
                };
                (builder, client)
            })
            .buffer_unordered(TEMPLATE_BUILD_STATUS_CONCURRENCY)
            .collect()
            .await;
    let entries: Vec<TemplateBuildListEntry> = futures_util::stream::iter(builds)
        .map(|build| {
            let client = clients
                .get(&build.builder_node_id)
                .cloned()
                .unwrap_or_else(|| Err("builder was not resolved".to_string()));
            template_build_list_entry(&template_name, build, client)
        })
        .buffered(TEMPLATE_BUILD_STATUS_CONCURRENCY)
        .collect()
        .await;
    Json(entries).into_response()
}

fn status_timeout_message(what: &str) -> String {
    format!(
        "{what} timed out after {}s",
        TEMPLATE_BUILD_STATUS_TIMEOUT.as_secs()
    )
}

/// One listed job: its recorded routing, and the status its builder's
/// client reports. A builder that failed to resolve stamps its error on each
/// of its jobs without a call.
async fn template_build_list_entry(
    template_name: &TemplateName,
    build: RecordedBuild,
    client: Result<BuilderClient, String>,
) -> TemplateBuildListEntry {
    let job_id = build.daemon_job_id.to_string();
    let mut entry = TemplateBuildListEntry {
        job_id: job_id.clone(),
        template_name: template_name.to_string(),
        created_at: format_time(build.created_at),
        builder_node_id: build.builder_node_id,
        state: None,
        content_hash: None,
        error: None,
        status_error: None,
    };
    let mut client = match client {
        Ok(client) => client,
        Err(error) => {
            entry.status_error = Some(error);
            return entry;
        }
    };
    let status = tokio::time::timeout(
        TEMPLATE_BUILD_STATUS_TIMEOUT,
        template_build_status_with(&mut client, job_id),
    )
    .await;
    match status {
        Ok(Ok(response)) if response.template_id == template_name.as_str() => {
            let build_state = build_proto::BuildState::try_from(response.state)
                .unwrap_or(build_proto::BuildState::Unspecified);
            entry.state = Some(template_build_state_response(build_state));
            entry.content_hash = optional_nonempty(response.content_hash);
            entry.error = optional_nonempty(response.error);
        }
        Ok(Ok(_)) => {
            entry.status_error = Some("builder reported the job for another template".to_string());
        }
        Ok(Err(error)) => entry.status_error = Some(error.to_string()),
        Err(_) => entry.status_error = Some(status_timeout_message("builder status")),
    }
    entry
}

pub async fn list_template_releases(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    match query_template_releases(&state.pool, &actor).await {
        Ok(releases) => Json(releases).into_response(),
        Err(error) => contention_response(ContentionScope::ReadOnly, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, "failed to query template releases");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to query template releases",
                )
            })
            .into_response(),
    }
}

async fn query_template_releases(
    pool: &sqlx::PgPool,
    actor: &User,
) -> Result<Vec<TemplateReleaseResponse>, sqlx::Error> {
    let org_ids = authz::visible_org_ids(pool, actor).await?;
    let rows =
        pplx_space_template_release_store::lookup::releases_unowned_or_owned_by(pool, &org_ids)
            .await?
            .into_iter()
            .map(TemplateReleaseRow::from)
            .collect();
    hydrate_template_releases(pool, rows).await
}

async fn hydrate_template_releases(
    pool: &sqlx::PgPool,
    rows: Vec<TemplateReleaseRow>,
) -> Result<Vec<TemplateReleaseResponse>, sqlx::Error> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let release_ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    let tags = fetch_release_tags(pool, &release_ids).await?;
    let pools = fetch_release_warm_pools(pool, &release_ids).await?;
    Ok(rows
        .into_iter()
        .map(|row| release_response(row, &tags, &pools))
        .collect())
}

async fn fetch_release_tags(
    pool: &sqlx::PgPool,
    release_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<String>>, sqlx::Error> {
    let rows =
        pplx_space_template_store::release_tags::list_for_releases(pool, release_ids).await?;
    let mut tags = HashMap::<Uuid, Vec<String>>::new();
    for row in rows {
        tags.entry(row.template_release_id)
            .or_default()
            .push(row.tag);
    }
    Ok(tags)
}

async fn fetch_release_warm_pools(
    pool: &sqlx::PgPool,
    release_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<TemplateReleaseWarmPoolResponse>>, sqlx::Error> {
    let rows = WarmPoolStore::new(pool.clone())
        .release_configs_with_counts(release_ids)
        .await
        .map_err(|err| match err {
            // This chain is sqlx::Error-typed end to end; a raw read model
            // can only fail in sqlx, so unwrap that case and box the rest.
            pplx_space_cluster_pool::PoolError::Sql(err) => err,
            other => sqlx::Error::Decode(Box::new(other)),
        })?;
    let mut pools = HashMap::<Uuid, Vec<TemplateReleaseWarmPoolResponse>>::new();
    for row in rows {
        pools
            .entry(row.template_release_id)
            .or_default()
            .push(TemplateReleaseWarmPoolResponse {
                id: row.id,
                vcpu: nonnegative_i32_to_u32(row.vcpu),
                memory_mib: nonnegative_i32_to_u32(row.memory_mib),
                disk_mib: row.disk_mib.and_then(|value| u32::try_from(value).ok()),
                target_ready_slots: nonnegative_i32_to_u32(row.target_ready_slots),
                refill_per_tick: nonnegative_i32_to_u32(row.refill_per_tick),
                max_warming_slots: nonnegative_i32_to_u32(row.max_warming_slots),
                namespace: row.namespace,
                enabled: row.enabled,
                encryptable: row.encryptable,
                managed_by: row.managed_by,
                warming_slots: optional_count_to_u32(row.warming_slots),
                ready_slots: optional_count_to_u32(row.ready_slots),
                claimed_slots: optional_count_to_u32(row.claimed_slots),
            });
    }
    Ok(pools)
}

fn release_response(
    row: TemplateReleaseRow,
    tags: &HashMap<Uuid, Vec<String>>,
    pools: &HashMap<Uuid, Vec<TemplateReleaseWarmPoolResponse>>,
) -> TemplateReleaseResponse {
    TemplateReleaseResponse {
        id: row.id,
        template_name: row.template_name,
        runtime: row.runtime,
        content_hash: row.content_hash,
        tags: tags.get(&row.id).cloned().unwrap_or_default(),
        warm_pools: pools.get(&row.id).cloned().unwrap_or_default(),
        created_at: format_time(row.created_at),
    }
}

pub async fn tag_template_release(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(release_id): Path<Uuid>,
    Json(request): Json<TemplateReleaseTagRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let tag = match TemplateTag::parse(request.tag.trim()) {
        Ok(tag) => tag,
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("invalid template tag: {error}"),
            )
            .into_response();
        }
    };
    let mut tx = match begin_tx(&state.pool, None, "failed to begin template release tag").await {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    let expected = request
        .expected_current_release_id
        .map(|expected| match expected {
            NullablePatch::Set(release_id) => Some(release_id),
            NullablePatch::Clear => None,
        });
    let result: Result<TagMove, sqlx::Error> = async {
        // The template row first, then the release, as `delete_template_release`
        // takes them, so a tag and a delete of one release queue rather than
        // deadlock.
        pplx_space_template_store::templates::lock_no_key_update_of_release(&mut *tx, release_id)
            .await?;
        let row = pplx_space_template_release_store::lookup::lock_release_of_present_template(
            &mut *tx, release_id,
        )
        .await?
        .map(TemplateReleaseRow::from);
        let Some(row) = row else {
            return Ok(TagMove::ReleaseMissing);
        };
        // A tag names a template and hash, and resolves to the original
        // release of it; a lookup with an environment reaches a twin through
        // the hash. Tagging a twin, or the other spelling's original of a
        // digest stored under both, would make tag and digest lookups
        // disagree, so it's refused in favor of the original.
        if let Some(original) = original_release_of(&mut tx, &row).await?
            && original != row.id
        {
            return Ok(TagMove::Twin { original });
        }
        // Not every tag writer locks the template row, so the tag row's own
        // lock is what makes the precondition and the move one step. A tag
        // created between this read and the insert below has committed by the
        // time `DO NOTHING` reports it, so one more read finds and locks it.
        let mut previous = pplx_space_template_store::release_tags::current_release_locked(
            &mut *tx,
            &row.template_name,
            tag.as_str(),
        )
        .await?;
        for _ in 0..2 {
            if let Some(expected) = expected
                && expected != previous
            {
                return Ok(TagMove::PreconditionFailed {
                    template_name: row.template_name,
                    current: previous,
                });
            }
            let moved = match previous {
                Some(_) => {
                    pplx_space_template_store::release_tags::retarget(
                        &mut *tx,
                        row.id,
                        &row.template_name,
                        &tag,
                    )
                    .await?
                }
                None => {
                    pplx_space_template_store::release_tags::insert_if_absent(
                        &mut *tx,
                        row.id,
                        &row.template_name,
                        &tag,
                    )
                    .await?
                }
            };
            if moved {
                let template_name = TemplateName::parse(&row.template_name)
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
                enqueue_template_refs_tx(
                    &mut tx,
                    &[ReconcileRef::template_desired_state(template_name)],
                    pplx_space_state_reconciliation::ReconcileReason::TemplateDesiredStateUpdated,
                )
                .await?;
                tx.commit().await?;
                return Ok(TagMove::Moved { row, previous });
            }
            previous = pplx_space_template_store::release_tags::current_release_locked(
                &mut *tx,
                &row.template_name,
                tag.as_str(),
            )
            .await?;
        }
        Ok(TagMove::Raced)
    }
    .await;
    match result {
        Ok(TagMove::Moved { row, previous }) => {
            audit::record_template_tag(audit::AdminTemplateTagAuditEntry {
                actor: &actor,
                template_name: &row.template_name,
                tag: tag.as_str(),
                release_id,
                previous_release_id: previous,
                success: true,
                error: None,
            });
            let releases = match hydrate_template_releases(&state.pool, vec![row]).await {
                Ok(releases) => releases,
                // Unclassified, and non-retryable, for the reason spelled
                // out on `update_template`'s reload: the tag has already
                // committed, so neither the status nor the envelope may
                // advertise a request that succeeded as one to send again.
                Err(error) => {
                    tracing::error!(%error, %release_id, "failed to hydrate tagged release");
                    return post_commit_error("failed to tag release").into_response();
                }
            };
            Json(releases.into_iter().next().expect("one release")).into_response()
        }
        Ok(TagMove::PreconditionFailed {
            template_name,
            current,
        }) => {
            let message = "tag does not point at expected_current_release_id";
            audit::record_template_tag(audit::AdminTemplateTagAuditEntry {
                actor: &actor,
                template_name: &template_name,
                tag: tag.as_str(),
                release_id,
                previous_release_id: current,
                success: false,
                error: Some(message),
            });
            let details = current
                .map(|id| PublicErrorDetail {
                    key: "current_release_id".to_string(),
                    value: id.to_string(),
                })
                .into_iter()
                .collect();
            api_error_with_details(StatusCode::PRECONDITION_FAILED, message, details)
                .into_response()
        }
        Ok(TagMove::Raced) => contention_envelope(
            StatusCode::CONFLICT,
            "tag changed while it was being moved; read it again and retry",
        )
        .into_response(),
        Ok(TagMove::ReleaseMissing) => {
            api_error(StatusCode::NOT_FOUND, "Template release not found").into_response()
        }
        Ok(TagMove::Twin { original }) => api_error(
            StatusCode::CONFLICT,
            format!("template release isn't its digest's original (it's a twin bound to another environment, or the digest's other spelling); tag its original release {original}"),
        )
        .into_response(),
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, %release_id, "failed to tag template release");
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "failed to tag release")
            })
            .into_response(),
    }
}

/// What `tag_template_release`'s transaction did.
enum TagMove {
    ReleaseMissing,
    /// The tag now points at the release; `previous` is where it pointed
    /// before, `None` when this created it.
    Moved {
        row: TemplateReleaseRow,
        previous: Option<Uuid>,
    },
    /// `expected_current_release_id` did not match `current`.
    PreconditionFailed {
        template_name: String,
        current: Option<Uuid>,
    },
    /// The tag was created and removed again by other writers between this
    /// request's reads.
    Raced,
    /// The release isn't its digest's original, `original`, which tags
    /// attach to: it's a twin, or the other spelling's original.
    Twin {
        original: Uuid,
    },
}

/// The original release of `release`'s template and digest under either
/// spelling (`pplx_space_template_release_original::digest_original`), the
/// release the internal API's tag writer and every lookup without an
/// environment resolve the digest to. A twin bound to another environment is
/// registered after its original and never is one; neither is the other
/// spelling's original where the digest is stored under both.
async fn original_release_of(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release: &TemplateReleaseRow,
) -> Result<Option<Uuid>, sqlx::Error> {
    pplx_space_template_release_store::lookup::original_release_id(
        &mut **tx,
        &release.template_name,
        &release.content_hash,
    )
    .await
    .map_err(sqlx::Error::from)
}

pub async fn delete_template_release_tag(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path((release_id, tag)): Path<(Uuid, String)>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let tag = match TemplateTag::parse(tag.trim()) {
        Ok(tag) => tag,
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("invalid template tag: {error}"),
            )
            .into_response();
        }
    };
    if tag == TemplateTag::latest() {
        return api_error(StatusCode::BAD_REQUEST, "latest tag cannot be deleted").into_response();
    }
    match pplx_space_template_store::release_tags::delete(&state.pool, release_id, &tag)
        .await
        .map_err(sqlx::Error::from)
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, %release_id, tag = tag.as_str(), "failed to delete release tag");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete release tag",
                )
            })
            .into_response(),
    }
}

/// What `delete_template_release`'s transaction did.
enum DeleteRelease {
    Deleted,
    Refused(&'static str),
}

pub async fn delete_template_release(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(release_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let result: Result<DeleteRelease, sqlx::Error> = async {
        let mut tx = state.pool.begin().await?;
        // Release registration holds the parent template's row lock
        // (`FOR UPDATE`) while it inserts, so locking it here keeps the twin
        // check below and the delete from interleaving with a registration
        // that adds the release's first twin. `NO KEY UPDATE` still conflicts
        // with that lock, but not with the key-share locks node publications'
        // foreign-key checks take on the template row.
        pplx_space_template_store::templates::lock_no_key_update_of_release(&mut *tx, release_id)
            .await?;
        // A writer that predates the shared rule can tag the other
        // spelling's release without the template lock; its insert only
        // key-share locks the release it names. Locking every release of the
        // digest makes that insert wait, so the tag check below sees it. A
        // release of the digest inserted after this lock can't make a tag
        // resolve to the deleted release: once the delete commits, the
        // digest original is one of the surviving releases.
        pplx_space_template_release_store::removal::lock_releases_of_digest(&mut *tx, release_id)
            .await?;
        // A tag resolves to the digest original of the release it names, and
        // a tag written before the shared rule can name the other spelling's
        // original. The release's own tags go with it, except `latest`, which
        // keeps the template's default. Any tag on another release of the
        // digest outlives the delete and would resolve to a different
        // release, so it guards the digest original too.
        let serves_a_tag =
            pplx_space_template_release_store::lookup::release_serves_a_tag(&mut *tx, release_id)
                .await?;
        if serves_a_tag {
            return Ok(DeleteRelease::Refused(
                "template release is missing, still tagged latest, or serving a tag on its other spelling",
            ));
        }
        // Deleting an original that has twins would make the oldest twin,
        // bound to another environment, the release every lookup without an
        // environment resolves. Its twins go first; a twin itself may go.
        let has_twins =
            pplx_space_template_release_store::lookup::release_has_twins(&mut *tx, release_id)
                .await?;
        if has_twins {
            return Ok(DeleteRelease::Refused(
                "template release has twins bound to other environments; delete them first",
            ));
        }
        pplx_space_template_store::release_tags::delete_for_release(&mut *tx, release_id).await?;
        pplx_space_template_release_store::removal::delete_storage_locations_of_release(
            &mut *tx, release_id,
        )
        .await?;
        let deleted =
            pplx_space_template_release_store::removal::delete_release(&mut *tx, release_id)
                .await?;
        tx.commit().await?;
        Ok(if deleted {
            DeleteRelease::Deleted
        } else {
            DeleteRelease::Refused("template release is missing or still tagged latest")
        })
    }
    .await;
    match result {
        Ok(DeleteRelease::Deleted) => StatusCode::NO_CONTENT.into_response(),
        Ok(DeleteRelease::Refused(message)) => {
            api_error(StatusCode::CONFLICT, message).into_response()
        }
        Err(error) => {
            if error
                .as_database_error()
                .is_some_and(|db| db.is_foreign_key_violation())
            {
                return api_error(StatusCode::CONFLICT, "template release is still referenced")
                    .into_response();
            }
            contention_response(ContentionScope::Writing, &error)
                .unwrap_or_else(|| {
                    tracing::error!(%error, %release_id, "failed to delete template release");
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to delete template release",
                    )
                })
                .into_response()
        }
    }
}

async fn fetch_template_snapshot_defaults(
    pool: &sqlx::PgPool,
    template_name: &TemplateName,
) -> Result<(u32, u32), sqlx::Error> {
    let row =
        pplx_space_node_template_inventory_store::template_snapshot_shape(pool, template_name)
            .await?;
    Ok((
        row.as_ref()
            .and_then(|row| row.vcpu)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(pplx_space_snapshot_model::MIN_BASE_SNAPSHOT_VCPU),
        row.as_ref()
            .and_then(|row| row.memory_mib)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(pplx_space_snapshot_model::MIN_BASE_SNAPSHOT_MEMORY_MIB),
    ))
}

fn base_snapshot_policy_response(
    template: String,
    configured: bool,
    ensure_base_snapshot: bool,
    snapshot_vcpu: u32,
    snapshot_memory_mib: u32,
    updated_at: Option<OffsetDateTime>,
) -> TemplateBaseSnapshotPolicyResponse {
    TemplateBaseSnapshotPolicyResponse {
        template,
        configured,
        ensure_base_snapshot,
        snapshot_vcpu,
        snapshot_memory_mib,
        updated_at: updated_at.map(format_time),
    }
}

pub async fn get_base_snapshot_policy(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    match pplx_space_template_store::base_snapshot_policies::get_present(
        &state.pool,
        &template_name,
    )
    .await
    .map_err(sqlx::Error::from)
    {
        Ok(Some(row)) => Json(base_snapshot_policy_response(
            row.template,
            true,
            row.ensure_base_snapshot,
            nonnegative_i32_to_u32(row.snapshot_vcpu),
            nonnegative_i32_to_u32(row.snapshot_memory_mib),
            Some(row.updated_at),
        ))
        .into_response(),
        Ok(None) => match fetch_template_snapshot_defaults(&state.pool, &template_name).await {
            Ok((vcpu, memory_mib)) => Json(base_snapshot_policy_response(
                template_name.to_string(),
                false,
                false,
                vcpu,
                memory_mib,
                None,
            ))
            .into_response(),
            Err(error) => contention_response(ContentionScope::ReadOnly, &error)
                .unwrap_or_else(|| {
                    tracing::error!(%error, template = template_name.as_str(), "failed to load template defaults");
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to load template defaults",
                    )
                })
                .into_response(),
        },
        Err(error) => contention_response(ContentionScope::ReadOnly, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to load base snapshot policy");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to load base snapshot policy",
                )
            })
            .into_response(),
    }
}

pub async fn update_base_snapshot_policy(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
    Json(request): Json<TemplateBaseSnapshotPolicyRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let defaults = match fetch_template_snapshot_defaults(&state.pool, &template_name).await {
        Ok(defaults) => defaults,
        Err(error) => {
            if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                return response.into_response();
            }
            tracing::error!(%error, template = template_name.as_str(), "failed to load template defaults");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load template defaults",
            )
            .into_response();
        }
    };
    let snapshot_vcpu = request.snapshot_vcpu.unwrap_or(defaults.0);
    let snapshot_memory_mib = request.snapshot_memory_mib.unwrap_or(defaults.1);
    let mut tx = match begin_tx(
        &state.pool,
        Some(&template_name),
        "failed to begin base snapshot update",
    )
    .await
    {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    let result: Result<
        Option<pplx_space_template_store::base_snapshot_policies::BaseSnapshotPolicy>,
        sqlx::Error,
    > = async {
        let row = pplx_space_template_store::base_snapshot_policies::upsert_if_present(
            &mut *tx,
            &template_name,
            request.ensure_base_snapshot,
            snapshot_vcpu as i32,
            snapshot_memory_mib as i32,
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        enqueue_template_refs_tx(
            &mut tx,
            &[
                ReconcileRef::template_base_snapshot_policy(template_name.clone()),
                ReconcileRef::template_desired_state(template_name.clone()),
            ],
            pplx_space_state_reconciliation::ReconcileReason::TemplateBaseSnapshotPolicyUpdated,
        )
        .await?;
        tx.commit().await?;
        Ok(Some(row))
    }
    .await;
    match result {
        Ok(Some(row)) => Json(base_snapshot_policy_response(
            row.template,
            true,
            row.ensure_base_snapshot,
            nonnegative_i32_to_u32(row.snapshot_vcpu),
            nonnegative_i32_to_u32(row.snapshot_memory_mib),
            Some(row.updated_at),
        ))
        .into_response(),
        Ok(None) => api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to update base snapshot policy");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to update base snapshot policy",
                )
            })
            .into_response(),
    }
}

pub async fn reconcile_base_snapshot_policy(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let mut tx = match begin_tx(
        &state.pool,
        Some(&template_name),
        "failed to begin base snapshot reconcile",
    )
    .await
    {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    // Nothing in this handler writes before the fanout, so the discovery read
    // is `ReadOnly` with no branch to qualify it, and is classified apart from
    // the enqueue writes it feeds. The 500 fallback below is a new event
    // string, so a saved query matching this handler's old failure message
    // will not see it — same caveat as `begin_tx`.
    let node_ids = match reconcile_target_node_ids(&mut tx).await {
        Ok(node_ids) => node_ids,
        Err(error) => {
            return contention_response(ContentionScope::ReadOnly, &error)
                .unwrap_or_else(|| {
                    tracing::error!(%error, template = template_name.as_str(), "failed to read base snapshot reconcile targets");
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to enqueue reconcile",
                    )
                })
                .into_response();
        }
    };
    let result = async {
        enqueue_template_refs_for_nodes_tx(
            &mut tx,
            node_ids,
            &[
                ReconcileRef::template_base_snapshot_policy(template_name.clone()),
                ReconcileRef::template_desired_state(template_name.clone()),
            ],
            pplx_space_state_reconciliation::ReconcileReason::TemplateBaseSnapshotPolicyUpdated,
        )
        .await?;
        tx.commit().await
    }
    .await;
    match result {
        Ok(()) => {
            Json(serde_json::json!({"template": template_name.to_string(), "fanout_queued": true}))
                .into_response()
        }
        // The discovery read was classified above, so what remains is the
        // enqueue and the commit. The enqueue can only fail once it is
        // writing, since it returns before either statement when the read
        // found no node. The commit is where `Writing` overstates: on an empty
        // cluster it commits a transaction that wrote nothing. A write-free
        // `COMMIT` raises neither code the scope arbitrates, so that case has
        // no answer left to get wrong.
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to enqueue base snapshot reconcile");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to enqueue reconcile",
                )
            })
            .into_response(),
    }
}

pub async fn upsert_template_pool(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
    Json(request): Json<CreatePoolApiRequest>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let boot_mode = request.boot_mode.unwrap_or_else(|| "cold".to_string());
    if boot_mode != "cold" && boot_mode != "snapshot" {
        return api_error(
            StatusCode::BAD_REQUEST,
            "boot_mode must be 'cold' or 'snapshot'",
        )
        .into_response();
    }
    let target_count = request.target_count.unwrap_or(3);
    if target_count == 0 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "target_count must be greater than 0",
        )
        .into_response();
    }
    let mut tx = match begin_tx(
        &state.pool,
        Some(&template_name),
        "failed to begin template pool upsert",
    )
    .await
    {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    let result = async {
        let upserted = pplx_space_template_store::pool_policies::upsert_if_present(
            &mut *tx,
            &template_name,
            target_count as i32,
            &boot_mode,
        )
        .await?;
        if !upserted {
            return Ok(false);
        }
        let mut refs = vec![ReconcileRef::template_desired_state(template_name.clone())];
        if boot_mode == "snapshot" {
            refs.push(ReconcileRef::template_base_snapshot_policy(
                template_name.clone(),
            ));
        }
        enqueue_template_refs_tx(
            &mut tx,
            &refs,
            pplx_space_state_reconciliation::ReconcileReason::TemplatePoolPolicyUpdated,
        )
        .await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(true)
    }
    .await;
    match result {
        Ok(true) => Json(serde_json::json!({
            "ok": true,
            "template": template_name.to_string(),
            "target_count": target_count,
            "boot_mode": boot_mode,
        }))
        .into_response(),
        Ok(false) => api_error(StatusCode::NOT_FOUND, "Template not found").into_response(),
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to upsert template pool");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to upsert template pool",
                )
            })
            .into_response(),
    }
}

pub async fn delete_template_pool(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let mut tx = match begin_tx(
        &state.pool,
        Some(&template_name),
        "failed to begin template pool delete",
    )
    .await
    {
        Ok(tx) => tx,
        Err(response) => return response.into_response(),
    };
    let result = async {
        pplx_space_template_store::pool_policies::delete(&mut *tx, &template_name).await?;
        enqueue_template_refs_tx(
            &mut tx,
            &[ReconcileRef::template_desired_state(template_name.clone())],
            pplx_space_state_reconciliation::ReconcileReason::TemplatePoolPolicyDeleted,
        )
        .await?;
        tx.commit().await
    }
    .await;
    match result {
        Ok(()) => Json(serde_json::json!({"ok": true, "template": template_name.to_string()}))
            .into_response(),
        Err(error) => contention_response(ContentionScope::Writing, &error)
            .unwrap_or_else(|| {
                tracing::error!(%error, template = template_name.as_str(), "failed to delete template pool");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete template pool",
                )
            })
            .into_response(),
    }
}

pub async fn template_pool_status(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let policy = match pplx_space_template_store::pool_policies::get(&state.pool, &template_name)
        .await
        .map_err(sqlx::Error::from)
    {
        Ok(policy) => policy,
        Err(error) => {
            if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                return response.into_response();
            }
            tracing::error!(%error, template = template_name.as_str(), "failed to load template pool policy");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load template pool status",
            )
            .into_response();
        }
    };
    let observed = match pplx_space_node_template_inventory_store::template_pool_observed(
        &state.pool,
        &template_name,
    )
    .await
    {
        Ok(observed) => observed,
        Err(error) => {
            let error = sqlx::Error::from(error);
            if let Some(response) = contention_response(ContentionScope::ReadOnly, &error) {
                return response.into_response();
            }
            tracing::error!(%error, template = template_name.as_str(), "failed to load observed template pool status");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load template pool status",
            )
            .into_response();
        }
    };
    Json(serde_json::json!({
        "template": template_name.to_string(),
        "policy": policy.map(|policy| serde_json::json!({
            "target_count": policy.target_count,
            "boot_mode": policy.boot_mode,
        })),
        "observed": observed.map(|observed| serde_json::json!({
            "pool_target": observed.pool_target,
            "pool_current": observed.pool_current,
            "pool_pending": observed.pool_pending,
            "sandbox_ids": observed.pool_sandbox_ids.0,
        })),
    }))
    .into_response()
}

#[cfg(test)]
mod tests;
