use pplx_space_common_core::types::{NodeId, OrgId};

use pplx_space_common_core::sql;
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
use pplx_space_common_core::types::{
    BuildRunId, IdempotencyKey, SubstrateKind, SystemTemplate, TemplateDesiredState, TemplateId,
    TemplateName, TemplateTag,
};
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_public_api::{
    OperationKind, OperationTargetType, PublicErrorCode, PublicErrorDetail, PublicErrorEnvelope,
    RequestFingerprint,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, types::Json as SqlxJson};
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
    reconcile_queue::{ReconcileFacet, ReconcileRef, enqueue_node_reconcile_refs_for_nodes_tx},
    reconcile_trigger::enqueue_triggers_tx,
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

#[derive(Debug, FromRow)]
struct TemplateRow {
    name: String,
    description: Option<String>,
    org_id: Option<OrgId>,
    desired_state: String,
    network_policy_generation: i64,
    created_at: OffsetDateTime,
    built_at: Option<String>,
    content_hash: Option<String>,
    runtime: Option<String>,
    arch: Option<String>,
    has_base_snapshot: Option<bool>,
    pool_target: Option<i32>,
    pool_current: Option<i32>,
    artifact_size_bytes: Option<i64>,
    cache_size_bytes: Option<i64>,
    base_snapshot_size_bytes: Option<i64>,
    snapshots: Option<SqlxJson<Vec<crate::models::SnapshotInfoResponse>>>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    disk_size_mb: Option<i32>,
    default_auto_lifecycle: Option<SqlxJson<pplx_space_common_core::types::AutoLifecyclePolicy>>,
    network_policy: SqlxJson<NetworkPolicyV1>,
    observed_network_policy_generation: Option<i64>,
    observed_network_policy_authority: Option<String>,
}

#[derive(Debug, FromRow)]
struct TemplateReleaseRow {
    id: Uuid,
    template_name: String,
    runtime: String,
    content_hash: String,
    created_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct TemplateReleaseTagRow {
    template_release_id: Uuid,
    tag: String,
}

#[derive(Debug, FromRow)]
struct TemplatePoolObservedRow {
    pool_target: Option<i32>,
    pool_current: i32,
    pool_pending: i32,
    pool_sandbox_ids: SqlxJson<Vec<String>>,
}

#[derive(Debug, FromRow)]
struct TemplatePoolPolicyRow {
    target_count: i32,
    boot_mode: String,
}

#[derive(Debug, FromRow)]
struct BaseSnapshotPolicyRow {
    template: String,
    ensure_base_snapshot: bool,
    snapshot_vcpu: i32,
    snapshot_memory_mib: i32,
    updated_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct TemplateSnapshotDefaultRow {
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
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
    authz::cluster_managing_org_id(pool).await.map_err(|error| {
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
    Ok(fetch_template_rows(pool, Some(template_name))
        .await?
        .into_iter()
        .next())
}

async fn fetch_template_rows(
    pool: &sqlx::PgPool,
    template_name: Option<&TemplateName>,
) -> Result<Vec<TemplateRow>, sqlx::Error> {
    sqlx::query_as::<_, TemplateRow>(
         sql!("WITH template_names AS (
             SELECT name
             FROM templates
             WHERE desired_state <> $4
               AND ($3::TEXT IS NULL OR name = $3)
         ),
         builder_metadata AS (
             SELECT DISTINCT ON (nti.template_name)
                    nti.*
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE n.role = 'builder'
             ORDER BY nti.template_name, nti.observed_at DESC, nti.built_at DESC NULLS LAST, nti.content_hash ASC
         ),
         availability AS (
             SELECT DISTINCT ON (nti.template_name)
                    nti.*
             FROM node_template_inventory nti
             LEFT JOIN builder_metadata bm ON bm.template_name = nti.template_name
             WHERE NULLIF(bm.content_hash, '') IS NULL
                OR nti.content_hash = bm.content_hash
             ORDER BY nti.template_name,
                      nti.has_base_snapshot DESC,
                      nti.observed_at DESC,
                      nti.content_hash ASC
         ),
         pools AS (
             SELECT nti.template_name,
                    SUM(nti.pool_target) FILTER (WHERE nti.pool_target IS NOT NULL)::INTEGER AS pool_target,
                    SUM(nti.pool_current)::INTEGER AS pool_current
             FROM node_template_inventory nti
             LEFT JOIN builder_metadata bm ON bm.template_name = nti.template_name
             WHERE NULLIF(bm.content_hash, '') IS NULL
                OR nti.content_hash = bm.content_hash
             GROUP BY nti.template_name
         ),
         network_policy_status AS (
             SELECT substring(element from 10) AS template_name,
                    MAX(observed_generation) AS observed_network_policy_generation,
                    CASE
                        WHEN BOOL_OR(applied_authority = 'local') THEN 'local'
                        WHEN BOOL_OR(applied_authority = 'cluster') THEN 'cluster'
                        ELSE NULL
                    END AS observed_network_policy_authority
             FROM node_facet_observed_status
             WHERE facet = $2
               AND element LIKE 'template:%'
             GROUP BY substring(element from 10)
         )
         SELECT names.name,
                t.description,
                t.org_id,
                t.desired_state,
                t.network_policy_generation,
                COALESCE(t.created_at, builder_metadata.observed_at, availability.observed_at) AS created_at,
                builder_metadata.built_at,
                NULLIF(builder_metadata.content_hash, '') AS content_hash,
                builder_metadata.runtime,
                builder_metadata.arch,
                availability.has_base_snapshot,
                COALESCE(pool_policy.target_count, pools.pool_target) AS pool_target,
                pools.pool_current,
                builder_metadata.artifact_size_bytes,
                availability.cache_size_bytes,
                availability.base_snapshot_size_bytes,
                availability.snapshots,
                builder_metadata.vcpu,
                builder_metadata.memory_mib,
                builder_metadata.disk_size_mb,
                COALESCE(
                    auto_lifecycle_policy.default_auto_lifecycle_json,
                    builder_metadata.default_auto_lifecycle_json
                ) AS default_auto_lifecycle,
                COALESCE(t.network_policy_json, $1) AS network_policy,
                network_policy_status.observed_network_policy_generation,
                network_policy_status.observed_network_policy_authority
         FROM template_names names
         JOIN templates t ON t.name = names.name
         LEFT JOIN builder_metadata ON builder_metadata.template_name = names.name
         LEFT JOIN availability ON availability.template_name = names.name
         LEFT JOIN pools ON pools.template_name = names.name
         LEFT JOIN template_default_auto_lifecycle_policies auto_lifecycle_policy
                ON auto_lifecycle_policy.template = names.name
         LEFT JOIN template_pool_policies pool_policy
                ON pool_policy.template = names.name
         LEFT JOIN network_policy_status ON network_policy_status.template_name = names.name
         ORDER BY names.name"),
    )
    .bind(SqlxJson(NetworkPolicyV1::deny_all()))
    .bind(ReconcileFacet::NetworkPolicy.as_str())
    .bind(template_name.map(TemplateName::as_str))
    .bind(TemplateDesiredState::Deleted.as_str())
    .fetch_all(pool)
    .await
}

/// The node-discovery read this module's reconcile enqueues begin with.
///
/// `reconcile_trigger` carries its own near-copy of the predicate for a
/// different call shape; this is not the only place it appears.
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
    sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT DISTINCT n.id
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         WHERE n.role IN ('worker', 'builder')
           AND (
             n.managed_by != $1
             OR (nd.node_id IS NOT NULL AND nd.deleted_at IS NULL)
           )"
    ))
    .bind(SubstrateKind::Space)
    .fetch_all(&mut **tx)
    .await
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
    reason: pplx_space_common_svc::types::ReconcileReason,
) -> Result<(), sqlx::Error> {
    if node_ids.is_empty() {
        return Ok(());
    }
    enqueue_node_reconcile_refs_for_nodes_tx(tx, node_ids.clone(), refs).await?;
    enqueue_triggers_tx(tx, node_ids, reason).await?;
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
    reason: pplx_space_common_svc::types::ReconcileReason,
) -> Result<(), sqlx::Error> {
    let node_ids = reconcile_target_node_ids(tx).await?;
    enqueue_template_refs_for_nodes_tx(tx, node_ids, refs, reason).await
}

fn template_refs(template_name: TemplateName) -> [ReconcileRef; 3] {
    [
        ReconcileRef::network_policy(pplx_space_common_svc::types::NetworkPolicyTarget::template(
            template_name.clone(),
        )),
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
    match fetch_template_rows(&state.pool, None).await {
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
        let row = sqlx::query_as::<_, TemplateRow>(
            sql!("INSERT INTO templates
                 (id, name, description, org_id, desired_state, network_policy_json, network_policy_generation)
             VALUES ($1, $2, $3, $4, $6, $5, 1)
             ON CONFLICT (name) DO UPDATE
             SET description = EXCLUDED.description,
                 org_id = COALESCE(templates.org_id, EXCLUDED.org_id),
                 desired_state = EXCLUDED.desired_state,
                 delete_requested_at = NULL,
                 delete_requested_by = NULL,
                 delete_operation_id = NULL,
                 network_policy_json = EXCLUDED.network_policy_json,
                 network_policy_generation =
                     templates.network_policy_generation
                     + CASE WHEN templates.network_policy_json IS DISTINCT FROM EXCLUDED.network_policy_json
                            THEN 1 ELSE 0 END,
                 updated_at = NOW()
             WHERE (templates.org_id IS NULL OR templates.org_id = EXCLUDED.org_id)
               AND (
                   templates.desired_state = $6
                   OR (
                       templates.desired_state = $7
                       AND NOT EXISTS (
                           SELECT 1
                           FROM node_template_inventory nti
                           WHERE nti.template_name = EXCLUDED.name
                       )
                   )
               )
             RETURNING name, description, org_id, desired_state, network_policy_generation, created_at,
                       NULL::TEXT AS built_at,
                       NULL::TEXT AS content_hash,
                       NULL::TEXT AS runtime,
                       NULL::TEXT AS arch,
                       NULL::BOOLEAN AS has_base_snapshot,
                       NULL::INTEGER AS pool_target,
                       NULL::INTEGER AS pool_current,
                       NULL::BIGINT AS artifact_size_bytes,
                       NULL::BIGINT AS cache_size_bytes,
                       NULL::BIGINT AS base_snapshot_size_bytes,
                       NULL::JSONB AS snapshots,
                       NULL::INTEGER AS vcpu,
                       NULL::INTEGER AS memory_mib,
                       NULL::INTEGER AS disk_size_mb,
                       NULL::JSONB AS default_auto_lifecycle,
                       network_policy_json AS network_policy,
                       NULL::BIGINT AS observed_network_policy_generation,
                       NULL::TEXT AS observed_network_policy_authority"),
        )
        .bind(TemplateId::new().as_uuid())
        .bind(template_name.as_str())
        .bind(&request.description)
        .bind(managing_org_id)
        .bind(SqlxJson(&request.network_policy))
        .bind(TemplateDesiredState::Present.as_str())
        .bind(TemplateDesiredState::Deleted.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        sqlx::query(
            sql!("INSERT INTO template_default_auto_lifecycle_policies
                 (template, default_auto_lifecycle_json, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (template) DO UPDATE
             SET default_auto_lifecycle_json = EXCLUDED.default_auto_lifecycle_json,
                 updated_at = NOW()"),
        )
        .bind(template_name.as_str())
        .bind(SqlxJson(&request.default_auto_lifecycle))
        .execute(&mut *tx)
        .await?;
        let refs = template_refs(template_name.clone());
        enqueue_template_refs_tx(
            &mut tx,
            &refs,
            pplx_space_common_svc::types::ReconcileReason::TemplateDesiredStateUpdated,
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
        let desired_state = sqlx::query_scalar::<_, String>(sql!(
            "SELECT desired_state FROM templates WHERE name = $1"
        ))
        .bind(template_name.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| (scope, error))?;
        let Some(desired_state) = desired_state else {
            return Ok(false);
        };
        if desired_state != TemplateDesiredState::Present.as_str() {
            return Ok(false);
        }

        if let Some(policy) = &request.default_auto_lifecycle {
            sqlx::query(sql!(
                "INSERT INTO template_default_auto_lifecycle_policies
                     (template, default_auto_lifecycle_json, updated_at)
                 SELECT name, $2, NOW()
                 FROM templates
                 WHERE name = $1
                   AND desired_state = $3
                 ON CONFLICT (template) DO UPDATE
                 SET default_auto_lifecycle_json = EXCLUDED.default_auto_lifecycle_json,
                     updated_at = NOW()"
            ))
            .bind(template_name.as_str())
            .bind(SqlxJson(policy))
            .bind(TemplateDesiredState::Present.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|error| (ContentionScope::Writing, error))?;
            scope = ContentionScope::Writing;
        }

        if let Some(policy) = &request.network_policy {
            let updated = sqlx::query_scalar::<_, i64>(sql!(
                "UPDATE templates
                 SET network_policy_json = $2,
                     network_policy_generation =
                       CASE WHEN network_policy_json IS DISTINCT FROM $2
                            THEN network_policy_generation + 1
                            ELSE network_policy_generation END,
                     updated_at = NOW()
                 WHERE name = $1
                   AND desired_state = $4
                   AND ($3::BIGINT IS NULL OR network_policy_generation = $3)
                 RETURNING network_policy_generation"
            ))
            .bind(template_name.as_str())
            .bind(SqlxJson(policy))
            .bind(expected_generation.map(|generation| generation as i64))
            .bind(TemplateDesiredState::Present.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| (ContentionScope::Writing, error))?;
            scope = ContentionScope::Writing;
            if updated.is_none() {
                return Ok(false);
            }
        }

        if let Some(description) = &request.description {
            sqlx::query(sql!(
                "UPDATE templates
                 SET description = $2, updated_at = NOW()
                 WHERE name = $1 AND desired_state = $3"
            ))
            .bind(template_name.as_str())
            .bind(description)
            .bind(TemplateDesiredState::Present.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|error| (ContentionScope::Writing, error))?;
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
            pplx_space_common_svc::types::ReconcileReason::TemplateDesiredStateUpdated,
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

/// Whether a public operation can still run: it exists and is queued or
/// running.
async fn delete_operation_is_active(
    pool: &sqlx::PgPool,
    operation_id: pplx_space_public_api::OperationId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM public_operations
             WHERE operation_id = $1
               AND state IN ('queued', 'running')
         )"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(pool)
    .await
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

    // The third column is false when the recorded delete operation reached a
    // terminal state or was garbage-collected. Such a delete can never finish
    // on its own, so a new request replaces it instead of returning it.
    let existing_state = match sqlx::query_as::<_, (String, Option<Uuid>, bool)>(sql!(
        "SELECT t.desired_state,
                t.delete_operation_id,
                COALESCE(po.state IN ('queued', 'running'), FALSE)
         FROM templates t
         LEFT JOIN public_operations po ON po.operation_id = t.delete_operation_id
         WHERE t.name = $1"
    ))
    .bind(template_name.as_str())
    .fetch_optional(&state.pool)
    .await
    {
        Ok(value) => value,
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
        match TemplateInUse::check(&mut **admission.transaction(), &template_name).await {
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
            match delete_operation_is_active(&state.pool, operation_id).await {
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
        let updated = sqlx::query(sql!(
            "UPDATE templates
             SET desired_state = $2,
                 delete_requested_at = COALESCE(delete_requested_at, NOW()),
                 delete_requested_by = COALESCE(delete_requested_by, $3),
                 delete_operation_id = CASE
                     WHEN delete_operation_id IS NULL
                       OR delete_operation_id = $6 THEN $4
                     ELSE delete_operation_id
                 END,
                 updated_at = NOW()
             WHERE name = $1
               AND desired_state <> $5"
        ))
        .bind(template_name.as_str())
        .bind(TemplateDesiredState::Deleting.as_str())
        .bind(actor.id)
        .bind(operation.as_uuid())
        .bind(TemplateDesiredState::Deleted.as_str())
        .bind(stale_delete_operation_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated > 0 {
            enqueue_template_refs_tx(
                &mut tx,
                &[ReconcileRef::template_desired_state(template_name.clone())],
                pplx_space_common_svc::types::ReconcileReason::TemplateDesiredStateUpdated,
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
        source: pplx_space_common_core::types::TemplateTagParseError,
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
        // The REST build API does not expose the balloon setting yet.
        balloon: None,
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
    let state = sqlx::query_scalar::<_, String>(sql!("SELECT desired_state FROM templates WHERE name = $1"))
        .bind(template_name.as_str())
        .fetch_optional(pool)
        .await
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
    )
    .await
}

/// The idempotency namespace for system template builds. Its records hold the
/// build's response, not an operation, so this path must never meet
/// `PublicOperationStore::create_operation`.
const SYSTEM_TEMPLATE_BUILD_PATH_TEMPLATE: &str = "/admin/v1/system-templates/{template}/builds";

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
        let recipe = system_template_recipe(self.template, self.base.as_deref()).map_err(
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
        dispatch_admin_template_build(
            state,
            self.managing_org_id,
            TemplateBuildTarget::System(self.template),
            recipe,
            self.force,
            self.publish_tags.clone(),
            None,
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
) -> Result<RequestFingerprint, ApiErrorResponse> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "template": template.to_string(),
        "base": base,
        "force": force,
        "publish_tags": publish_tags,
    }))
    .map_err(|error| {
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
        managing_org_id,
        target,
        recipe,
        force,
        publish_tags,
        client,
    )
    .await
    {
        Ok(started) => (StatusCode::ACCEPTED, Json(started)).into_response(),
        Err(failure) => *failure.response,
    }
}

async fn dispatch_admin_template_build(
    state: &AppState,
    managing_org_id: OrgId,
    target: TemplateBuildTarget,
    recipe: build_proto::Recipe,
    force: bool,
    publish_tags: Vec<String>,
    client: Option<TemplateBuildClientContext>,
) -> Result<TemplateBuildStartResponse, BuildDispatchFailure> {
    let template_name = target.template_name();
    let client = client.unwrap_or_default();
    let response = state
        .template_controller
        .submit_template_build(TemplateBuildSubmission {
            target,
            client_build_id: None,
            recipe,
            force,
            publish_tags,
            client: build_proto::ClientContext {
                source_path: client.source_path.unwrap_or_default(),
                cli_version: client
                    .cli_version
                    .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
                hostname: client
                    .hostname
                    .or_else(|| std::env::var("HOSTNAME").ok())
                    .unwrap_or_default(),
            },
            org_id: managing_org_id,
        })
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
    match super::template_store::template_exists(&state.pool, &template_name).await {
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
    let rows = sqlx::query_as::<_, TemplateReleaseRow>(sql!(
        "SELECT r.id, r.template_name, r.type AS runtime, r.hash AS content_hash, r.created_at
         FROM template_releases r
         JOIN templates t ON t.name = r.template_name
         WHERE t.desired_state = $2
           AND (t.org_id IS NULL OR t.org_id = ANY($1))
         ORDER BY r.template_name ASC, r.created_at DESC"
    ))
    .bind(&org_ids)
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_all(pool)
    .await?;
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
    let rows = sqlx::query_as::<_, TemplateReleaseTagRow>(sql!(
        "SELECT template_release_id, tag
         FROM template_release_tags
         WHERE template_release_id = ANY($1)
         ORDER BY template_release_id, tag"
    ))
    .bind(release_ids)
    .fetch_all(pool)
    .await?;
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
        let row = sqlx::query_as::<_, TemplateReleaseRow>(sql!(
            "SELECT r.id, r.template_name, r.type AS runtime, r.hash AS content_hash, r.created_at
             FROM template_releases r
             JOIN templates t ON t.name = r.template_name
             WHERE r.id = $1
               AND t.desired_state = $2
             FOR UPDATE"
        ))
        .bind(release_id)
        .bind(TemplateDesiredState::Present.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Ok(TagMove::ReleaseMissing);
        };
        // Not every tag writer locks the template row, so the tag row's own
        // lock is what makes the precondition and the move one step. A tag
        // created between this read and the insert below has committed by the
        // time `DO NOTHING` reports it, so one more read finds and locks it.
        let mut previous =
            current_tag_release_for_update(&mut tx, &row.template_name, tag.as_str()).await?;
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
                Some(_) => sqlx::query(sql!(
                    "UPDATE template_release_tags
                     SET template_release_id = $1,
                         modified_at = NOW()
                     WHERE template_name = $2 AND tag = $3"
                )),
                None => sqlx::query(sql!(
                    "INSERT INTO template_release_tags (template_release_id, template_name, tag)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (template_name, tag) DO NOTHING"
                )),
            }
            .bind(row.id)
            .bind(&row.template_name)
            .bind(tag.as_str())
            .execute(&mut *tx)
            .await?
            .rows_affected()
                == 1;
            if moved {
                let template_name = TemplateName::parse(&row.template_name)
                    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
                enqueue_template_refs_tx(
                    &mut tx,
                    &[ReconcileRef::template_desired_state(template_name)],
                    pplx_space_common_svc::types::ReconcileReason::TemplateDesiredStateUpdated,
                )
                .await?;
                tx.commit().await?;
                return Ok(TagMove::Moved { row, previous });
            }
            previous =
                current_tag_release_for_update(&mut tx, &row.template_name, tag.as_str()).await?;
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
}

/// The release a tag points at, locking its row for the rest of the
/// transaction, or `None` when the tag does not exist.
async fn current_tag_release_for_update(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    template_name: &str,
    tag: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT template_release_id
         FROM template_release_tags
         WHERE template_name = $1 AND tag = $2
         FOR UPDATE"
    ))
    .bind(template_name)
    .bind(tag)
    .fetch_optional(&mut **tx)
    .await
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
    match sqlx::query(sql!(
        "DELETE FROM template_release_tags
         WHERE template_release_id = $1 AND tag = $2"
    ))
    .bind(release_id)
    .bind(tag.as_str())
    .execute(&state.pool)
    .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
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

pub async fn delete_template_release(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(release_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let result: Result<bool, sqlx::Error> = async {
        let mut tx = state.pool.begin().await?;
        let latest_count: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM template_release_tags
             WHERE template_release_id = $1 AND tag = 'latest'"
        ))
        .bind(release_id)
        .fetch_one(&mut *tx)
        .await?;
        if latest_count > 0 {
            return Ok(false);
        }
        sqlx::query(sql!(
            "DELETE FROM template_release_tags WHERE template_release_id = $1"
        ))
        .bind(release_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(sql!(
            "DELETE FROM template_release_storage_locations WHERE template_release_id = $1"
        ))
        .bind(release_id)
        .execute(&mut *tx)
        .await?;
        let deleted = sqlx::query(sql!("DELETE FROM template_releases WHERE id = $1"))
            .bind(release_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(deleted > 0)
    }
    .await;
    match result {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => api_error(
            StatusCode::CONFLICT,
            "template release is missing or still tagged latest",
        )
        .into_response(),
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
    let row = sqlx::query_as::<_, TemplateSnapshotDefaultRow>(sql!(
        "SELECT nti.vcpu, nti.memory_mib
         FROM node_template_inventory nti
         JOIN nodes n ON n.id = nti.node_id
         JOIN templates t ON t.name = nti.template_name
         WHERE nti.template_name = $1
           AND t.desired_state = $2
           AND (nti.vcpu IS NOT NULL OR nti.memory_mib IS NOT NULL)
         ORDER BY CASE n.role WHEN 'worker' THEN 0 WHEN 'builder' THEN 1 ELSE 2 END,
                  nti.observed_at DESC,
                  nti.built_at DESC NULLS LAST,
                  nti.content_hash ASC
         LIMIT 1"
    ))
    .bind(template_name.as_str())
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(pool)
    .await?;
    Ok((
        row.as_ref()
            .and_then(|row| row.vcpu)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(pplx_space_common_core::types::MIN_BASE_SNAPSHOT_VCPU),
        row.as_ref()
            .and_then(|row| row.memory_mib)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(pplx_space_common_core::types::MIN_BASE_SNAPSHOT_MEMORY_MIB),
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
    match sqlx::query_as::<_, BaseSnapshotPolicyRow>(
        sql!("SELECT p.template, p.ensure_base_snapshot, p.snapshot_vcpu, p.snapshot_memory_mib, p.updated_at
         FROM template_base_snapshot_policies p
         JOIN templates t ON t.name = p.template
         WHERE p.template = $1
           AND t.desired_state = $2"),
    )
    .bind(template_name.as_str())
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(&state.pool)
    .await
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
    let result: Result<Option<BaseSnapshotPolicyRow>, sqlx::Error> = async {
        let row = sqlx::query_as::<_, BaseSnapshotPolicyRow>(
            sql!("INSERT INTO template_base_snapshot_policies
                 (template, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib, updated_at)
             SELECT name, $2, $3, $4, NOW()
             FROM templates
             WHERE name = $1
               AND desired_state = $5
             ON CONFLICT (template) DO UPDATE
             SET ensure_base_snapshot = EXCLUDED.ensure_base_snapshot,
                 snapshot_vcpu = EXCLUDED.snapshot_vcpu,
                 snapshot_memory_mib = EXCLUDED.snapshot_memory_mib,
                 updated_at = NOW()
             RETURNING template, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib, updated_at"),
        )
        .bind(template_name.as_str())
        .bind(request.ensure_base_snapshot)
        .bind(snapshot_vcpu as i32)
        .bind(snapshot_memory_mib as i32)
        .bind(TemplateDesiredState::Present.as_str())
        .fetch_optional(&mut *tx)
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
            pplx_space_common_svc::types::ReconcileReason::TemplateBaseSnapshotPolicyUpdated,
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
            pplx_space_common_svc::types::ReconcileReason::TemplateBaseSnapshotPolicyUpdated,
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
        let result = sqlx::query(sql!(
            "INSERT INTO template_pool_policies (template, target_count, boot_mode, updated_at)
             SELECT name, $2, $3, NOW()
             FROM templates
             WHERE name = $1
               AND desired_state = $4
             ON CONFLICT (template) DO UPDATE
             SET target_count = EXCLUDED.target_count,
                 boot_mode = EXCLUDED.boot_mode,
                 updated_at = NOW()"
        ))
        .bind(template_name.as_str())
        .bind(target_count as i32)
        .bind(&boot_mode)
        .bind(TemplateDesiredState::Present.as_str())
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
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
            pplx_space_common_svc::types::ReconcileReason::TemplatePoolPolicyUpdated,
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
        sqlx::query(sql!(
            "DELETE FROM template_pool_policies WHERE template = $1"
        ))
        .bind(template_name.as_str())
        .execute(&mut *tx)
        .await?;
        enqueue_template_refs_tx(
            &mut tx,
            &[ReconcileRef::template_desired_state(template_name.clone())],
            pplx_space_common_svc::types::ReconcileReason::TemplatePoolPolicyDeleted,
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

async fn fetch_template_pool_observed(
    pool: &sqlx::PgPool,
    template_name: &TemplateName,
) -> Result<Option<TemplatePoolObservedRow>, sqlx::Error> {
    sqlx::query_as::<_, TemplatePoolObservedRow>(
        sql!("WITH builder_metadata AS (
             SELECT nti.content_hash
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE nti.template_name = $1
               AND n.role = 'builder'
             ORDER BY nti.observed_at DESC, nti.built_at DESC NULLS LAST, nti.content_hash ASC
             LIMIT 1
         ),
         observed AS (
             SELECT SUM(nti.pool_target)::INTEGER AS pool_target,
                    COALESCE(SUM(nti.pool_current), 0)::INTEGER AS pool_current,
                    COALESCE(SUM(nti.pool_pending), 0)::INTEGER AS pool_pending
             FROM node_template_inventory nti
             LEFT JOIN builder_metadata bm ON TRUE
             WHERE nti.template_name = $1
               AND nti.pool_target IS NOT NULL
               AND (NULLIF(bm.content_hash, '') IS NULL OR nti.content_hash = bm.content_hash)
         )
         SELECT observed.pool_target,
                observed.pool_current,
                observed.pool_pending,
                COALESCE(
                    (
                        SELECT JSONB_AGG(DISTINCT sandbox_id)
                        FROM node_template_inventory nti2
                        LEFT JOIN builder_metadata bm ON TRUE
                        LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(nti2.pool_sandbox_ids) AS sandbox_id ON TRUE
                        WHERE nti2.template_name = $1
                          AND (NULLIF(bm.content_hash, '') IS NULL OR nti2.content_hash = bm.content_hash)
                          AND sandbox_id IS NOT NULL
                    ),
                    '[]'::JSONB
                ) AS pool_sandbox_ids
         FROM observed
         WHERE observed.pool_target IS NOT NULL"),
    )
    .bind(template_name.as_str())
    .fetch_optional(pool)
    .await
}

pub async fn template_pool_status(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(template_name): Path<TemplateName>,
) -> impl IntoResponse {
    if let Err(error) = require_template_admin(&state.pool, &actor).await {
        return error.into_response();
    }
    let policy = match sqlx::query_as::<_, TemplatePoolPolicyRow>(sql!(
        "SELECT target_count, boot_mode
         FROM template_pool_policies
         WHERE template = $1"
    ))
    .bind(template_name.as_str())
    .fetch_optional(&state.pool)
    .await
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
    let observed = match fetch_template_pool_observed(&state.pool, &template_name).await {
        Ok(observed) => observed,
        Err(error) => {
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
