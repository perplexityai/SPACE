use pplx_space_common_core::sql;
use std::{collections::HashSet, time::Duration};

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::{NodeManagedBy, NodeRole, SubstrateKind, TemplateName};
use pplx_space_common_svc::{
    bearer_token::SpaceBearerToken,
    types::{FACET_NETWORK_POLICY, ReconcileFacet, ReconcileReason},
};
use pplx_space_node_api_client::proto::{
    GetNodeInfoRequest, NodeRole as ProtoNodeRole, NodeSource, PullTemplateRequest, TransferSource,
    node_info_client::NodeInfoClient, transfer_source::Source,
};
use pplx_space_node_api_client::{BuilderApiClient, NodeApiClient, WorkerApiClient};
use pplx_space_template_mgr_client::proto::{
    DeleteTemplateBuildRequest, DeleteTemplateRequest, TemplateRef,
};
use serde::{Deserialize, Deserializer, Serialize, de};
use sqlx::FromRow;
use time::OffsetDateTime;
use tonic::transport::Endpoint;

use crate::{
    middleware::auth::AuthUser,
    reconcile_queue::{
        ReconcileRef, enqueue_node_reconcile_refs_and_trigger,
        enqueue_node_reconcile_refs_for_nodes,
    },
    reconcile_trigger::enqueue_triggers,
    routes::{AppState, admin::support::require_managing_org_admin},
};

mod node_api;
mod node_status;
mod node_templates;

use node_api::*;
pub use node_status::*;
pub use node_templates::*;

#[derive(Debug, Clone, FromRow)]
struct NodeRow {
    id: NodeId,
    name: Option<String>,
    address: Option<String>,
    node_api_endpoint: Option<String>,
    role: NodeRole,
    managed_by: NodeManagedBy,
    state: Option<String>,
    metadata: Option<sqlx::types::Json<serde_json::Value>>,
    last_heartbeat: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    deleted_at: Option<OffsetDateTime>,
    node_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct NodeResponse {
    id: NodeId,
    name: Option<String>,
    address: Option<String>,
    node_api_endpoint: Option<String>,
    role: String,
    managed_by: String,
    state: Option<String>,
    metadata: serde_json::Value,
    last_heartbeat: Option<String>,
    created_at: String,
    updated_at: Option<String>,
    deleted_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateNodeRequest {
    id: Option<NodeId>,
    name: Option<String>,
    address: Option<String>,
    node_api_endpoint: String,
    role: NodeRole,
    state: Option<String>,
    #[serde(default = "empty_metadata")]
    metadata: serde_json::Value,
    #[serde(default)]
    verify: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateNodeRequest {
    name: Option<String>,
    address: Option<String>,
    node_api_endpoint: Option<String>,
    role: Option<NodeRole>,
    state: Option<String>,
    metadata: Option<serde_json::Value>,
    #[serde(default)]
    verify: bool,
}

fn empty_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn format_time(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn format_optional_time(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(format_time)
}

fn api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": message.into()})))
}

/// This surface's constraint-specific shapes, falling through to the shared
/// admin classifier.
///
/// The special cases below are more specific than anything generic — a unique
/// violation on `nodes` means the node already exists, and each check
/// constraint names the field the operator got wrong — so they stay ahead of
/// it. Everything else, contention included, is the same question the rest of
/// `/admin/v1` answers, and answering it twice is how the two answers diverge.
pub(super) fn db_error(err: sqlx::Error) -> (StatusCode, Json<serde_json::Value>) {
    if let Some(db) = err.as_database_error() {
        if db.is_unique_violation() {
            return api_error(StatusCode::CONFLICT, "Node already exists");
        }
        if db.code().as_deref() == Some("23514") {
            return match db.constraint() {
                Some("nodes_role_check") => api_error(
                    StatusCode::BAD_REQUEST,
                    "role must be one of: worker, builder, cluster",
                ),
                Some("node_direct_metadata_is_object") => {
                    api_error(StatusCode::BAD_REQUEST, "metadata must be an object")
                }
                _ => {
                    tracing::warn!(
                        error = %err,
                        constraint = db.constraint().unwrap_or("unknown"),
                        "admin node maintenance check constraint failed"
                    );
                    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                }
            };
        }
        if db.code().as_deref() == Some("22P02") {
            return api_error(
                StatusCode::BAD_REQUEST,
                "address must be a valid IP address",
            );
        }
    }
    crate::routes::admin::support::db_error(err)
}

fn node_response(row: NodeRow) -> NodeResponse {
    NodeResponse {
        id: row.id,
        name: row.name.clone().or(row.node_name),
        address: row.address,
        node_api_endpoint: row.node_api_endpoint,
        role: row.role.to_string(),
        managed_by: row.managed_by.to_string(),
        state: row.state,
        metadata: row
            .metadata
            .map(|m| m.0)
            .unwrap_or_else(|| serde_json::json!({})),
        last_heartbeat: format_optional_time(row.last_heartbeat),
        created_at: format_time(row.created_at),
        updated_at: format_optional_time(row.updated_at),
        deleted_at: format_optional_time(row.deleted_at),
    }
}

/// The substrate whose nodes this registry owns. Every registry statement
/// scopes to it, and an update's `verify` reads only a node of it, so the
/// registry never touches a node another substrate manages.
const REGISTRY_SUBSTRATE: SubstrateKind = SubstrateKind::Space;

fn require_admin_node_registry(
    state: &AppState,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if !state
        .sandbox_controller
        .substrate_capabilities()
        .registers_nodes_via_admin_api
    {
        return Err(api_error(
            StatusCode::NOT_IMPLEMENTED,
            "direct node registry is only available for the direct/self-managed substrate",
        ));
    }
    Ok(())
}

fn inventory_entry_to_row(
    entry: crate::controllers::substrate::NodeInventoryEntry,
) -> Option<NodeRow> {
    Some(NodeRow {
        id: entry.id,
        name: entry.name,
        address: entry.address.map(Into::into),
        node_api_endpoint: None,
        role: entry.role,
        managed_by: entry.managed_by,
        state: entry.state,
        metadata: None,
        last_heartbeat: entry.last_heartbeat,
        created_at: entry.created_at,
        updated_at: None,
        deleted_at: None,
        node_name: entry.substrate_node_id,
    })
}

async fn fetch_node(pool: &sqlx::PgPool, id: NodeId) -> Result<Option<NodeRow>, sqlx::Error> {
    sqlx::query_as::<_, NodeRow>(sql!(
        "SELECT n.id,
                nd.name,
                host(n.address) AS address,
                nd.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                nd.metadata,
                n.last_heartbeat,
                n.created_at,
                nd.updated_at,
                nd.deleted_at,
                nk.k8s_node_name AS node_name
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE n.id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
}

async fn fetch_nodes(
    pool: &sqlx::PgPool,
    role: Option<NodeRole>,
    include_deleted: bool,
) -> Result<Vec<NodeRow>, sqlx::Error> {
    sqlx::query_as::<_, NodeRow>(sql!(
        "SELECT n.id,
                nd.name,
                host(n.address) AS address,
                nd.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                nd.metadata,
                n.last_heartbeat,
                n.created_at,
                nd.updated_at,
                nd.deleted_at,
                nk.k8s_node_name AS node_name
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE ($1::TEXT IS NULL OR n.role = $1)
           AND ($2::BOOL OR nd.deleted_at IS NULL)
         ORDER BY n.role ASC,
                  COALESCE(nd.name, nk.k8s_node_name) ASC NULLS LAST,
                  nd.node_api_endpoint ASC NULLS LAST"
    ))
    .bind(role.map(NodeRole::as_str))
    .bind(include_deleted)
    .fetch_all(pool)
    .await
}

async fn fetch_inventory_nodes(
    state: &AppState,
    role_filter: Option<NodeRole>,
) -> Result<Vec<NodeRow>, (StatusCode, Json<serde_json::Value>)> {
    if let Some(entries) = state
        .sandbox_controller
        .list_inventory_nodes(role_filter.map(NodeRole::as_str))
        .await
        .map_err(|err| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("substrate inventory error: {err}"),
            )
        })?
    {
        return Ok(entries
            .into_iter()
            .filter_map(inventory_entry_to_row)
            .collect());
    }
    fetch_nodes(&state.pool, role_filter, false)
        .await
        .map_err(db_error)
}

async fn enqueue_default_node_publish(
    pool: &sqlx::PgPool,
    node: &NodeRow,
    reason: ReconcileReason,
) {
    if !node_supports_state_reconciliation(node.role) {
        return;
    }
    if let Err(error) =
        enqueue_node_reconcile_refs_and_trigger(pool, node.id, &default_node_publish_refs(), reason)
            .await
    {
        tracing::warn!(
            node_id = %node.id,
            %error,
            "failed to enqueue node state reconciliation"
        );
    }
}

pub async fn create_node(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(req): Json<CreateNodeRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Err(err) = require_admin_node_registry(&state) {
        return err.into_response();
    }
    let role = req.role;
    if !req.metadata.is_object() {
        return api_error(StatusCode::BAD_REQUEST, "metadata must be an object").into_response();
    }
    let endpoint = match normalize_node_api_endpoint(&req.node_api_endpoint) {
        Ok(endpoint) => endpoint,
        Err(err) => return err.into_response(),
    };

    let mut node_id = req.id.unwrap_or_else(NodeId::new);
    if req.verify {
        let verified_id = match verify_node_endpoint(&endpoint, role).await {
            Ok(id) => id,
            Err(err) => return err.into_response(),
        };
        if let Some(requested_id) = req.id
            && requested_id != verified_id
        {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("node id mismatch: expected {requested_id}, got {verified_id}"),
            )
            .into_response();
        }
        node_id = verified_id;
    }

    let reuse_deleted_by_endpoint = !req.verify && req.id.is_none();
    let reactivated = sqlx::query_as::<_, NodeRow>(sql!(
        "WITH candidate AS (
           SELECT nd.node_id
           FROM node_direct nd
           JOIN nodes n ON n.id = nd.node_id
           WHERE nd.deleted_at IS NOT NULL
             AND n.managed_by = $10
             AND (
               n.id = $1
               OR ($8::BOOL AND nd.node_api_endpoint = $6)
             )
           ORDER BY (n.id = $1) DESC,
                    nd.updated_at DESC,
                    n.created_at DESC,
                    n.id DESC
           LIMIT 1
         ),
         updated_node AS (
           UPDATE nodes n
           SET address = NULLIF($3, '')::INET,
             role = $4,
             managed_by = $10,
             state = COALESCE($5, 'registered'),
             last_heartbeat = CASE
                 WHEN $9::BOOL THEN NOW()
                 ELSE n.last_heartbeat
             END
           WHERE n.id = (SELECT node_id FROM candidate)
           RETURNING n.id,
                     host(n.address) AS address,
                     n.role,
                     n.managed_by,
                     n.state,
                     n.last_heartbeat,
                     n.created_at
         ),
         updated_direct AS (
           UPDATE node_direct nd
           SET name = $2,
             node_api_endpoint = $6,
             metadata = $7,
             deleted_at = NULL,
             updated_at = NOW()
           WHERE nd.node_id = (SELECT id FROM updated_node)
           RETURNING nd.node_id,
                     nd.name,
                     nd.node_api_endpoint,
                     nd.metadata,
                     nd.updated_at,
                     nd.deleted_at
         )
         SELECT n.id,
                d.name,
                n.address,
                d.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                d.metadata,
                n.last_heartbeat,
                n.created_at,
                d.updated_at,
                d.deleted_at,
                NULL::TEXT AS node_name
         FROM updated_node n
         JOIN updated_direct d ON d.node_id = n.id"
    ))
    .bind(node_id)
    .bind(&req.name)
    .bind(req.address.as_deref().unwrap_or(""))
    .bind(role.as_str())
    .bind(&req.state)
    .bind(&endpoint)
    .bind(sqlx::types::Json(&req.metadata))
    .bind(reuse_deleted_by_endpoint)
    .bind(req.verify)
    .bind(REGISTRY_SUBSTRATE)
    .fetch_optional(&state.pool)
    .await;

    match reactivated {
        Ok(Some(row)) => {
            enqueue_default_node_publish(&state.pool, &row, ReconcileReason::NodeRegistered).await;
            return (StatusCode::CREATED, Json(node_response(row))).into_response();
        }
        Ok(None) => {}
        Err(err) => return db_error(err).into_response(),
    }

    let result = sqlx::query_as::<_, NodeRow>(sql!(
        "WITH inserted_node AS (
           INSERT INTO nodes (id, address, role, managed_by, state, last_heartbeat)
           VALUES (
               $1,
               NULLIF($3, '')::INET,
               $4,
               $9,
               COALESCE($5, 'registered'),
               CASE WHEN $8::BOOL THEN NOW() ELSE NULL END
           )
           RETURNING id,
                     host(address) AS address,
                     role,
                     managed_by,
                     state,
                     last_heartbeat,
                     created_at
         ),
         inserted_direct AS (
           INSERT INTO node_direct (node_id, name, node_api_endpoint, metadata)
           SELECT id, $2, $6, $7
           FROM inserted_node
           RETURNING node_id,
                     name,
                     node_api_endpoint,
                     metadata,
                     updated_at,
                     deleted_at
         )
         SELECT n.id,
                d.name,
                n.address,
                d.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                d.metadata,
                n.last_heartbeat,
                n.created_at,
                d.updated_at,
                d.deleted_at,
                NULL::TEXT AS node_name
         FROM inserted_node n
         JOIN inserted_direct d ON d.node_id = n.id"
    ))
    .bind(node_id)
    .bind(&req.name)
    .bind(req.address.as_deref().unwrap_or(""))
    .bind(role.as_str())
    .bind(&req.state)
    .bind(&endpoint)
    .bind(sqlx::types::Json(&req.metadata))
    .bind(req.verify)
    .bind(REGISTRY_SUBSTRATE)
    .fetch_one(&state.pool)
    .await;

    match result {
        Ok(row) => {
            enqueue_default_node_publish(&state.pool, &row, ReconcileReason::NodeRegistered).await;
            (StatusCode::CREATED, Json(node_response(row))).into_response()
        }
        Err(err) => db_error(err).into_response(),
    }
}

pub async fn update_node(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(id): Path<NodeId>,
    Json(req): Json<UpdateNodeRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Err(err) = require_admin_node_registry(&state) {
        return err.into_response();
    }
    if let Some(metadata) = req.metadata.as_ref()
        && !metadata.is_object()
    {
        return api_error(StatusCode::BAD_REQUEST, "metadata must be an object").into_response();
    }
    let endpoint = match req.node_api_endpoint.as_deref() {
        Some(endpoint) => match normalize_node_api_endpoint(endpoint) {
            Ok(endpoint) => Some(endpoint),
            Err(err) => return err.into_response(),
        },
        None => None,
    };
    let existing = if req.verify {
        match fetch_node(&state.pool, id).await {
            Ok(Some(node))
                if node.deleted_at.is_none() && node.managed_by == REGISTRY_SUBSTRATE =>
            {
                Some(node)
            }
            Ok(_) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
            Err(err) => return db_error(err).into_response(),
        }
    } else {
        None
    };
    if req.verify {
        let existing = existing.as_ref().expect("existing node loaded for verify");
        let desired_endpoint = match endpoint
            .as_deref()
            .or(existing.node_api_endpoint.as_deref())
        {
            Some(endpoint) => endpoint,
            None => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "node has no node_api_endpoint",
                )
                .into_response();
            }
        };
        let desired_role = req.role.unwrap_or(existing.role);
        match verify_node_endpoint(desired_endpoint, desired_role).await {
            Ok(verified_id) if verified_id == id => {}
            Ok(verified_id) => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    format!("node id mismatch: expected {id}, got {verified_id}"),
                )
                .into_response();
            }
            Err(err) => return err.into_response(),
        }
    }
    let result = sqlx::query_as::<_, NodeRow>(sql!(
        "WITH updated_node AS (
           UPDATE nodes n
           SET address = COALESCE(NULLIF($2, '')::INET, address),
               role = COALESCE($4, role),
               state = COALESCE($5, state),
               last_heartbeat = CASE
                   WHEN $8::BOOL THEN NOW()
                   ELSE n.last_heartbeat
               END
           WHERE n.id = $7
             AND n.managed_by = $9
           RETURNING n.id,
                     host(n.address) AS address,
                     n.role,
                     n.managed_by,
                     n.state,
                     n.last_heartbeat,
                     n.created_at
         ),
         updated_direct AS (
           UPDATE node_direct nd
           SET name = COALESCE($1, name),
               node_api_endpoint = COALESCE($3, node_api_endpoint),
               metadata = COALESCE($6::JSONB, metadata),
               updated_at = NOW()
           FROM updated_node n
           WHERE nd.node_id = n.id
             AND nd.deleted_at IS NULL
           RETURNING nd.node_id,
                     nd.name,
                     nd.node_api_endpoint,
                     nd.metadata,
                     nd.updated_at,
                     nd.deleted_at
         )
         SELECT n.id,
                d.name,
                n.address,
                d.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                d.metadata,
                n.last_heartbeat,
                n.created_at,
                d.updated_at,
                d.deleted_at,
                NULL::TEXT AS node_name
         FROM updated_node n
         JOIN updated_direct d ON d.node_id = n.id"
    ))
    .bind(&req.name)
    .bind(req.address.as_deref().unwrap_or(""))
    .bind(&endpoint)
    .bind(req.role.map(NodeRole::as_str))
    .bind(&req.state)
    .bind(req.metadata.as_ref().map(sqlx::types::Json))
    .bind(id)
    .bind(req.verify)
    .bind(REGISTRY_SUBSTRATE)
    .fetch_optional(&state.pool)
    .await;

    match result {
        Ok(Some(row)) => {
            enqueue_default_node_publish(&state.pool, &row, ReconcileReason::NodeUpdated).await;
            Json(node_response(row)).into_response()
        }
        Ok(None) => api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(err) => db_error(err).into_response(),
    }
}

pub async fn delete_node(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(id): Path<NodeId>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Err(err) = require_admin_node_registry(&state) {
        return err.into_response();
    }

    let result = hard_delete_direct_node(&state.pool, id).await;

    match result {
        Ok(count) if count > 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(err) => db_error(err).into_response(),
    }
}

async fn hard_delete_direct_node(pool: &sqlx::PgPool, id: NodeId) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM nodes n
         WHERE n.id = $1
           AND n.managed_by = $2
           AND EXISTS (
               SELECT 1
               FROM node_direct nd
               WHERE nd.node_id = n.id
           )"
    ))
    .bind(id)
    .bind(REGISTRY_SUBSTRATE)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests;
