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
use pplx_space_common_core::types::{NodeManagedBy, NodeRole, TemplateName};
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

const NODE_API_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const NODE_API_FAST_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const NODE_API_TEMPLATE_RPC_TIMEOUT: Duration = Duration::from_secs(600);
const NODE_TEMPLATE_INVENTORY_CONCURRENCY: usize = 16;
const DEFAULT_NODE_STATUS_CACHE_TTL_SECS: u64 = 30;

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

#[derive(Debug, Clone, FromRow)]
struct NodeStatusRow {
    node_id: NodeId,
    stale_after: OffsetDateTime,
    reachable: bool,
    error: Option<String>,
    health_healthy: Option<bool>,
    services: sqlx::types::Json<Vec<ServiceHealthResponse>>,
    node_ctl: Option<sqlx::types::Json<NodeCtlStatusResponse>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServiceHealthResponse {
    name: String,
    reachable: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReconciliationStatusResponse {
    name: String,
    poll_interval_secs: u64,
    last_started_at_unix_secs: u64,
    last_completed_at_unix_secs: u64,
    last_success_at_unix_secs: u64,
    next_due_at_unix_secs: u64,
    running: bool,
    templates_seen: u64,
    sandboxes_seen: u64,
    templates_applied: u64,
    sandboxes_applied: u64,
    last_error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeCtlStatusResponse {
    reachable: bool,
    error: Option<String>,
    reconciliations: Vec<ReconciliationStatusResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct NodeHealthStatusResponse {
    reachable: bool,
    healthy: bool,
    error: Option<String>,
    services: Vec<ServiceHealthResponse>,
    node_ctl: Option<NodeCtlStatusResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeTemplateSnapshotResponse {
    vcpu: u32,
    memory_mib: u32,
    snapshot_hash: String,
}

#[derive(Debug, FromRow)]
struct NodeTemplateInventoryRow {
    name: String,
    content_hash: Option<String>,
    build_date: Option<String>,
    artifact_size_bytes: Option<i64>,
    cache_size_bytes: Option<i64>,
    disk_size_mb: Option<i32>,
    runtime: Option<String>,
    arch: Option<String>,
    snapshots: sqlx::types::Json<Vec<NodeTemplateSnapshotResponse>>,
}

#[derive(Debug, Serialize)]
struct NodeTemplateBuildResponse {
    name: String,
    content_hash: Option<String>,
    build_date: Option<String>,
    artifact_size_bytes: Option<u64>,
    cache_size_bytes: Option<u64>,
    disk_size_mb: Option<u32>,
    runtime: Option<String>,
    arch: Option<String>,
    snapshots: Vec<NodeTemplateSnapshotResponse>,
}

#[derive(Debug, Serialize)]
struct NodeTemplateInventoryResponse {
    node: NodeResponse,
    reachable: bool,
    error: Option<String>,
    builds: Vec<NodeTemplateBuildResponse>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshNodeStatusRequest {
    role: Option<NodeRole>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileFacetRequest {
    #[serde(deserialize_with = "deserialize_reconcile_facet")]
    facet: ReconcileFacet,
    element: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileNodesRequest {
    role: Option<NodeRole>,
    #[serde(default)]
    node_ids: Vec<NodeId>,
    #[serde(default, deserialize_with = "deserialize_reconcile_facets")]
    facets: Vec<ReconcileFacet>,
    #[serde(default)]
    refs: Vec<ReconcileFacetRequest>,
}

#[derive(Debug, Deserialize)]
pub struct NodeFilters {
    role: Option<NodeRole>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullNodeTemplateRequest {
    template_id: String,
    content_hash: Option<String>,
    source_node_id: NodeId,
    #[serde(default)]
    include_snapshot: bool,
    snapshot_vcpu: Option<u32>,
    snapshot_memory_mib: Option<u32>,
    #[serde(default = "default_true")]
    skip_if_exists: bool,
    local_tag: Option<String>,
    block_on_snapshot_policy_achieved: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteNodeTemplateQuery {
    content_hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct PullNodeTemplateResponse {
    content_hash: String,
    already_existed: bool,
    bytes_transferred: u64,
}

#[derive(Debug, Serialize)]
struct RefreshNodeStatusResponse {
    refreshed: bool,
    nodes_seen: usize,
    nodes_updated: usize,
    nodes_failed: usize,
    cache_ttl_secs: u64,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReconcileNodeResponse {
    node_id: NodeId,
    node_name: Option<String>,
    role: String,
    queued: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReconcileNodesResponse {
    nodes_seen: usize,
    nodes_queued: usize,
    nodes_failed: usize,
    nodes: Vec<ReconcileNodeResponse>,
}

#[derive(Debug, Serialize)]
struct ReconcileNodeNetworkPolicyResponse {
    queued: bool,
}

fn deserialize_reconcile_facet<'de, D>(deserializer: D) -> Result<ReconcileFacet, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    ReconcileFacet::parse(value.trim())
        .ok_or_else(|| de::Error::custom(format!("unsupported reconcile facet: {value}")))
}

fn deserialize_reconcile_facets<'de, D>(deserializer: D) -> Result<Vec<ReconcileFacet>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            ReconcileFacet::parse(value.trim())
                .ok_or_else(|| de::Error::custom(format!("unsupported reconcile facet: {value}")))
        })
        .collect()
}

fn empty_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn default_true() -> bool {
    true
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

fn node_supports_state_reconciliation(role: NodeRole) -> bool {
    matches!(role, NodeRole::Worker | NodeRole::Builder)
}

fn template_capable_node_role(
    node: &NodeRow,
    label: &str,
) -> Result<NodeRole, (StatusCode, Json<serde_json::Value>)> {
    match node.role {
        role @ (NodeRole::Worker | NodeRole::Builder) => Ok(role),
        NodeRole::Cluster => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("{label} node must be a worker or builder"),
        )),
    }
}

fn ensure_template_capable_node(
    node: &NodeRow,
    label: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if node.deleted_at.is_some() {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("{label} node not found"),
        ));
    }
    template_capable_node_role(node, label)?;
    Ok(())
}

fn require_direct_substrate(state: &AppState) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if state.sandbox_controller.managed_by() != "space" {
        return Err(api_error(
            StatusCode::NOT_IMPLEMENTED,
            "direct node registry is only available for the direct/self-managed substrate",
        ));
    }
    Ok(())
}

fn normalize_node_api_endpoint(raw: &str) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    let mut endpoint = raw.trim().trim_end_matches('/').to_string();
    if let Some(stripped) = endpoint.strip_prefix("http://") {
        endpoint = stripped.to_string();
    }
    if endpoint.starts_with("https://") {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "node_api_endpoint must use plaintext node-api host:port",
        ));
    }
    if endpoint.is_empty() || endpoint.contains('/') {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "node_api_endpoint must be a host:port endpoint",
        ));
    }
    let uri: axum::http::Uri = endpoint_uri(&endpoint).parse().map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid node_api_endpoint: {err}"),
        )
    })?;
    if uri
        .authority()
        .and_then(axum::http::uri::Authority::port_u16)
        .is_none()
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "node_api_endpoint must include an explicit port",
        ));
    }
    Ok(endpoint)
}

fn endpoint_uri(endpoint: &str) -> String {
    pplx_space_common_grpc::internal_transport::qualify_endpoint(endpoint)
}

fn node_endpoint(
    endpoint: &str,
    request_timeout: Option<Duration>,
) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    // `node_api_endpoint` is scheme-aware: it supplies the internal RPC scheme for bare
    // `host:port` and passes `http://`/`https://`-qualified inputs through
    // untouched. Trim whitespace only — stripping `http://` here would mangle
    // an `https://` input into `http://https://...`.
    let endpoint = endpoint.trim();
    Ok(crate::controllers::substrate::node_api_endpoint(
        endpoint,
        NODE_API_CONNECT_TIMEOUT,
        request_timeout.unwrap_or(crate::controllers::substrate::NODE_RPC_TIMEOUT),
    )?)
}

fn proto_role_name(role: i32) -> Option<&'static str> {
    if role == ProtoNodeRole::Worker as i32 {
        Some(NodeRole::Worker.as_str())
    } else if role == ProtoNodeRole::Builder as i32 {
        Some(NodeRole::Builder.as_str())
    } else if role == ProtoNodeRole::Cluster as i32 {
        Some(NodeRole::Cluster.as_str())
    } else {
        None
    }
}

fn bearer_kind_for_role(role: NodeRole) -> SpaceBearerToken {
    match role {
        NodeRole::Cluster => SpaceBearerToken::ClusterApi,
        NodeRole::Worker | NodeRole::Builder => SpaceBearerToken::NodeApi,
    }
}

async fn verify_node_endpoint(
    endpoint: &str,
    expected_role: NodeRole,
) -> Result<NodeId, (StatusCode, Json<serde_json::Value>)> {
    let endpoint = node_endpoint(endpoint, Some(NODE_API_FAST_RPC_TIMEOUT)).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid node_api_endpoint: {err}"),
        )
    })?;
    let service = match expected_role {
        NodeRole::Cluster => pplx_space_common_grpc::internal_transport::Service::ClusterApi,
        NodeRole::Worker | NodeRole::Builder => {
            pplx_space_common_grpc::internal_transport::Service::NodeApi
        }
    };
    let endpoint =
        pplx_space_common_grpc::internal_transport::client_connect_endpoint(service, endpoint)
            .await
            .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;
    let channel = endpoint.connect().await.map_err(|err| {
        api_error(
            StatusCode::BAD_GATEWAY,
            format!("node-api endpoint is not reachable: {err}"),
        )
    })?;
    let mut client = NodeInfoClient::new(crate::controllers::substrate::node_api_channel(
        channel,
        bearer_kind_for_role(expected_role),
    ));
    let response = client
        .get_node_info(GetNodeInfoRequest {})
        .await
        .map_err(|err| {
            api_error(
                StatusCode::BAD_GATEWAY,
                format!("node-api NodeInfo probe failed: {err}"),
            )
        })?
        .into_inner();

    let actual_role = proto_role_name(response.role).ok_or_else(|| {
        api_error(
            StatusCode::BAD_GATEWAY,
            format!("node-api returned unknown role {}", response.role),
        )
    })?;
    if actual_role != expected_role.as_str() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("node-api role mismatch: expected {expected_role}, got {actual_role}"),
        ));
    }

    response.node_id.parse::<NodeId>().map_err(|err| {
        api_error(
            StatusCode::BAD_GATEWAY,
            format!("node-api returned invalid node_id: {err}"),
        )
    })
}

fn node_api_client(
    node: &NodeRow,
    label: &str,
    request_timeout: Duration,
) -> Result<NodeApiClient, (StatusCode, Json<serde_json::Value>)> {
    let raw_endpoint = node.node_api_endpoint.as_deref().ok_or_else(|| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{label} node has no node_api_endpoint"),
        )
    })?;
    let endpoint = node_endpoint(raw_endpoint, Some(request_timeout)).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid {label} node_api_endpoint: {err}"),
        )
    })?;
    Ok(NodeApiClient::new(endpoint))
}

fn node_rpc_error(operation: &str, err: tonic::Status) -> (StatusCode, Json<serde_json::Value>) {
    let status = match err.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::FailedPrecondition => StatusCode::CONFLICT,
        _ => StatusCode::BAD_GATEWAY,
    };
    api_error(status, format!("{operation} failed: {err}"))
}

fn node_api_rpc_error_response(
    node: &NodeRow,
    command: &'static str,
    status: tonic::Status,
) -> (StatusCode, Json<serde_json::Value>) {
    tracing::warn!(
        node_id = %node.id,
        endpoint = %node.node_api_endpoint.as_deref().unwrap_or("-"),
        command,
        grpc_code = ?status.code(),
        "node-api RPC failed"
    );
    node_rpc_error(command, status)
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

async fn fetch_inventory_node(
    state: &AppState,
    id: NodeId,
) -> Result<NodeRow, (StatusCode, Json<serde_json::Value>)> {
    let nodes = fetch_inventory_nodes(state, None).await?;
    nodes
        .into_iter()
        .find(|node| node.id == id && node.deleted_at.is_none())
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Node not found"))
}

async fn fetch_node_status_map(
    pool: &sqlx::PgPool,
    node_ids: &[NodeId],
) -> Result<Vec<NodeStatusRow>, sqlx::Error> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as::<_, NodeStatusRow>(sql!(
        "SELECT node_id,
                stale_after,
                reachable,
                error,
                health_healthy,
                services,
                node_ctl
         FROM node_status_snapshots
         WHERE node_id = ANY($1)"
    ))
    .bind(node_ids)
    .fetch_all(pool)
    .await
}

async fn node_status_refresh_needed(
    pool: &sqlx::PgPool,
    nodes: &[NodeRow],
) -> Result<bool, sqlx::Error> {
    let node_ids = nodes
        .iter()
        .filter(|node| node_supports_state_reconciliation(node.role))
        .map(|node| node.id)
        .collect::<Vec<_>>();
    if node_ids.is_empty() {
        return Ok(false);
    }
    let statuses = fetch_node_status_map(pool, &node_ids).await?;
    let now = OffsetDateTime::now_utc();
    Ok(node_ids.iter().any(|node_id| {
        statuses
            .iter()
            .find(|status| status.node_id == *node_id)
            .map(|status| status.stale_after <= now)
            .unwrap_or(true)
    }))
}

async fn fetch_cached_node_health(pool: &sqlx::PgPool, node: &NodeRow) -> NodeHealthStatusResponse {
    if !node_supports_state_reconciliation(node.role) {
        return NodeHealthStatusResponse {
            reachable: false,
            healthy: false,
            error: Some("node health is only available for worker and builder nodes".to_string()),
            services: Vec::new(),
            node_ctl: None,
        };
    }

    match fetch_node_status_map(pool, &[node.id]).await {
        Ok(statuses) => {
            let Some(status) = statuses
                .into_iter()
                .find(|status| status.node_id == node.id)
            else {
                return NodeHealthStatusResponse {
                    reachable: false,
                    healthy: false,
                    error: Some("cached node health is unavailable".to_string()),
                    services: Vec::new(),
                    node_ctl: None,
                };
            };
            NodeHealthStatusResponse {
                reachable: status.reachable,
                healthy: status.health_healthy.unwrap_or(false),
                error: status.error,
                services: status.services.0,
                node_ctl: status.node_ctl.map(|node_ctl| node_ctl.0),
            }
        }
        Err(err) => NodeHealthStatusResponse {
            reachable: false,
            healthy: false,
            error: Some(format!("cached node health query failed: {err}")),
            services: Vec::new(),
            node_ctl: None,
        },
    }
}

fn default_node_publish_refs() -> Vec<ReconcileRef> {
    vec![
        ReconcileRef::full(ReconcileFacet::NodeStatus),
        ReconcileRef::full(ReconcileFacet::SandboxInventory),
        ReconcileRef::full(ReconcileFacet::TemplateInventory),
    ]
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

fn reconcile_refs_from_request(
    req: &ReconcileNodesRequest,
) -> Result<Vec<ReconcileRef>, (StatusCode, Json<serde_json::Value>)> {
    if !req.refs.is_empty() {
        return req
            .refs
            .iter()
            .map(|reference| {
                ReconcileRef::from_storage(reference.facet, reference.element.as_deref()).map_err(
                    |error| {
                        api_error(
                            StatusCode::BAD_REQUEST,
                            format!("invalid reconcile ref element: {error}"),
                        )
                    },
                )
            })
            .collect();
    }

    let facets = if req.facets.is_empty() {
        vec![
            ReconcileFacet::SandboxInventory,
            ReconcileFacet::TemplateInventory,
            ReconcileFacet::NodeStatus,
        ]
    } else {
        req.facets.clone()
    };
    facets
        .into_iter()
        .map(|facet| Ok(ReconcileRef::full(facet)))
        .collect()
}

fn non_empty_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() { None } else { Some(value) }
    })
}

async fn delete_cached_node_template_inventory(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    template: &str,
    content_hash: Option<&str>,
) -> Result<(), sqlx::Error> {
    if let Some(content_hash) = content_hash {
        sqlx::query(sql!(
            "DELETE FROM node_template_inventory
             WHERE node_id = $1
               AND template_name = $2
               AND content_hash = $3"
        ))
        .bind(node_id)
        .bind(template)
        .bind(content_hash)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(sql!(
            "DELETE FROM node_template_inventory
             WHERE node_id = $1
               AND template_name = $2"
        ))
        .bind(node_id)
        .bind(template)
        .execute(pool)
        .await?;
    }

    let remaining: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT
         FROM node_template_inventory
         WHERE node_id = $1
           AND template_name = $2"
    ))
    .bind(node_id)
    .bind(template)
    .fetch_one(pool)
    .await?;

    if remaining == 0 {
        sqlx::query(sql!(
            "DELETE FROM node_facet_observed_status
             WHERE node_id = $1
               AND facet = $2
               AND element = $3"
        ))
        .bind(node_id)
        .bind(FACET_NETWORK_POLICY)
        .bind(format!("template:{template}"))
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn note_node_template_deleted(
    pool: &sqlx::PgPool,
    node: &NodeRow,
    template: &str,
    content_hash: Option<&str>,
) {
    if let Err(error) =
        delete_cached_node_template_inventory(pool, node.id, template, content_hash).await
    {
        tracing::warn!(
            node_id = %node.id,
            template,
            content_hash = content_hash.unwrap_or(""),
            %error,
            "failed to delete cached node template inventory after node delete"
        );
    }

    if let Err(error) = enqueue_node_reconcile_refs_and_trigger(
        pool,
        node.id,
        &[ReconcileRef::full(ReconcileFacet::TemplateInventory)],
        ReconcileReason::ManualNodeReconcile,
    )
    .await
    {
        tracing::warn!(
            node_id = %node.id,
            template,
            content_hash = content_hash.unwrap_or(""),
            %error,
            "failed to enqueue template inventory refresh after node template delete"
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
    if let Err(err) = require_direct_substrate(&state) {
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
             AND n.managed_by = 'space'
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
             managed_by = 'space',
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
               'space',
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
    if let Err(err) = require_direct_substrate(&state) {
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
                if node.deleted_at.is_none() && node.managed_by == NodeManagedBy::Space =>
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
             AND n.managed_by = 'space'
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
    if let Err(err) = require_direct_substrate(&state) {
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
           AND n.managed_by = 'space'
           AND EXISTS (
               SELECT 1
               FROM node_direct nd
               WHERE nd.node_id = n.id
           )"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn refresh_node_status(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(req): Json<RefreshNodeStatusRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let cache_ttl_secs = DEFAULT_NODE_STATUS_CACHE_TTL_SECS;
    let nodes = match fetch_inventory_nodes(&state, req.role).await {
        Ok(nodes) => nodes
            .into_iter()
            .filter(|node| node_supports_state_reconciliation(node.role))
            .collect::<Vec<_>>(),
        Err(err) => return err.into_response(),
    };

    if !req.force {
        match node_status_refresh_needed(&state.pool, &nodes).await {
            Ok(true) => {}
            Ok(false) => {
                return Json(RefreshNodeStatusResponse {
                    refreshed: false,
                    nodes_seen: nodes.len(),
                    nodes_updated: 0,
                    nodes_failed: 0,
                    cache_ttl_secs,
                    error: None,
                })
                .into_response();
            }
            Err(err) => return db_error(err).into_response(),
        }
    }

    let node_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    let enqueued = match enqueue_node_reconcile_refs_for_nodes(
        &state.pool,
        node_ids,
        &default_node_publish_refs(),
    )
    .await
    {
        Ok(enqueued) => enqueued,
        Err(error) => {
            tracing::warn!(%error, "failed to enqueue node status refresh reconciliation");
            Vec::new()
        }
    };
    let trigger_nodes = enqueued
        .iter()
        .map(|enqueued| enqueued.node_id)
        .collect::<Vec<_>>();
    if let Err(error) = enqueue_triggers(
        &state.pool,
        trigger_nodes,
        ReconcileReason::ManualNodeStatusRefresh,
    )
    .await
    {
        tracing::warn!(%error, "failed to enqueue manual node status refresh triggers");
    }
    let nodes_seen = nodes.len();
    let nodes_updated = enqueued.len();
    Json(RefreshNodeStatusResponse {
        refreshed: true,
        nodes_seen,
        nodes_updated,
        nodes_failed: nodes_seen.saturating_sub(nodes_updated),
        cache_ttl_secs,
        error: None,
    })
    .into_response()
}

pub async fn reconcile_nodes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(req): Json<ReconcileNodesRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let refs = match reconcile_refs_from_request(&req) {
        Ok(refs) => refs,
        Err(err) => return err.into_response(),
    };

    let requested_node_ids = req.node_ids.iter().copied().collect::<HashSet<_>>();
    let mut nodes = match fetch_inventory_nodes(&state, req.role).await {
        Ok(nodes) => nodes
            .into_iter()
            .filter(|node| node_supports_state_reconciliation(node.role))
            .filter(|node| requested_node_ids.is_empty() || requested_node_ids.contains(&node.id))
            .collect::<Vec<_>>(),
        Err(err) => return err.into_response(),
    };
    nodes.sort_by(|a, b| a.role.cmp(&b.role).then_with(|| a.name.cmp(&b.name)));

    let node_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    let queued_node_ids =
        match enqueue_node_reconcile_refs_for_nodes(&state.pool, node_ids, &refs).await {
            Ok(enqueued) => enqueued
                .into_iter()
                .map(|enqueued| enqueued.node_id)
                .collect::<HashSet<_>>(),
            Err(error) => {
                tracing::warn!(%error, "failed to enqueue manual node reconciliation");
                HashSet::new()
            }
        };
    let enqueue_error = if queued_node_ids.is_empty() && !nodes.is_empty() {
        Some("enqueue reconcile refs failed".to_string())
    } else {
        None
    };
    let nodes = nodes
        .into_iter()
        .map(|node| {
            let queued = queued_node_ids.contains(&node.id);
            ReconcileNodeResponse {
                node_id: node.id,
                node_name: node.name,
                role: node.role.to_string(),
                queued,
                error: if queued { None } else { enqueue_error.clone() },
            }
        })
        .collect::<Vec<_>>();
    let nodes_queued = nodes.iter().filter(|node| node.queued).count();
    let trigger_nodes = nodes
        .iter()
        .filter(|node| node.queued)
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    if let Err(error) = enqueue_triggers(
        &state.pool,
        trigger_nodes,
        ReconcileReason::ManualNodeReconcile,
    )
    .await
    {
        tracing::warn!(%error, "failed to enqueue manual node reconcile triggers");
    }
    let nodes_seen = nodes.len();
    Json(ReconcileNodesResponse {
        nodes_seen,
        nodes_queued,
        nodes_failed: nodes_seen.saturating_sub(nodes_queued),
        nodes,
    })
    .into_response()
}

pub async fn node_health(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(id): Path<NodeId>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    let node = match fetch_inventory_node(&state, id).await {
        Ok(node) => node,
        Err(err) => return err.into_response(),
    };
    if !node_supports_state_reconciliation(node.role) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "node health is only available for worker and builder nodes",
        )
        .into_response();
    }

    Json(fetch_cached_node_health(&state.pool, &node).await).into_response()
}

pub async fn reconcile_node_network_policy(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(id): Path<NodeId>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    let node = match fetch_inventory_node(&state, id).await {
        Ok(node) => node,
        Err(err) => return err.into_response(),
    };
    if !node_supports_state_reconciliation(node.role) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "network policy reconciliation is only available for worker and builder nodes",
        )
        .into_response();
    }

    let refs = [ReconcileRef::full(ReconcileFacet::NetworkPolicy)];
    match enqueue_node_reconcile_refs_and_trigger(
        &state.pool,
        node.id,
        &refs,
        ReconcileReason::ManualNodeNetworkPolicyReconcile,
    )
    .await
    {
        Ok(_) => Json(ReconcileNodeNetworkPolicyResponse { queued: true }).into_response(),
        Err(err) => db_error(err).into_response(),
    }
}

pub async fn pull_node_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(target_id): Path<NodeId>,
    Json(req): Json<PullNodeTemplateRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Err(err) = require_direct_substrate(&state) {
        return err.into_response();
    }

    let template_id = req.template_id.trim().to_string();
    if template_id.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "template_id is required").into_response();
    }
    let template_name = match TemplateName::parse(&template_id) {
        Ok(template_name) => template_name,
        Err(err) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                format!("invalid template_id: {err}"),
            )
            .into_response();
        }
    };

    if req.include_snapshot && (req.snapshot_vcpu.is_none() || req.snapshot_memory_mib.is_none()) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "snapshot_vcpu and snapshot_memory_mib are required when include_snapshot is true",
        )
        .into_response();
    }

    let target = match fetch_node(&state.pool, target_id).await {
        Ok(Some(node)) => node,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "target node not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    let target_role = match template_capable_node_role(&target, "target") {
        Ok(role) => role,
        Err(err) => return err.into_response(),
    };

    let source = match fetch_node(&state.pool, req.source_node_id).await {
        Ok(Some(node)) => node,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "source node not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    if let Err(err) = ensure_template_capable_node(&source, "source") {
        return err.into_response();
    }

    let source_address = match source.node_api_endpoint.clone() {
        Some(value) => value,
        None => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "source node has no node_api_endpoint",
            )
            .into_response();
        }
    };
    let request = PullTemplateRequest {
        template_id: template_id.clone(),
        content_hash: non_empty_optional(req.content_hash),
        tag: None,
        sources: vec![TransferSource {
            source: Some(Source::Node(NodeSource {
                address: source_address,
            })),
        }],
        include_snapshot: req.include_snapshot,
        snapshot_vcpu: req.snapshot_vcpu,
        snapshot_memory_mib: req.snapshot_memory_mib,
        skip_if_exists: req.skip_if_exists,
        local_tag: non_empty_optional(req.local_tag),
        block_on_snapshot_policy_achieved: req.block_on_snapshot_policy_achieved,
    };

    let mut node_api = match node_api_client(&target, "target", NODE_API_TEMPLATE_RPC_TIMEOUT) {
        Ok(client) => client,
        Err(err) => return err.into_response(),
    };
    let response = match target_role {
        NodeRole::Worker => {
            let mut client = WorkerApiClient::new(&mut node_api);
            client
                .pull_template(request, NODE_API_TEMPLATE_RPC_TIMEOUT)
                .await
        }
        NodeRole::Builder => {
            let mut client = BuilderApiClient::new(&mut node_api);
            client
                .pull_template(request, NODE_API_TEMPLATE_RPC_TIMEOUT)
                .await
        }
        NodeRole::Cluster => unreachable!("cluster nodes are rejected above"),
    }
    .map_err(|status| node_api_rpc_error_response(&target, "PullTemplate", status));

    match response {
        Ok(response) => {
            if let Err(err) = enqueue_node_reconcile_refs_and_trigger(
                &state.pool,
                target.id,
                &[ReconcileRef::template_inventory(template_name)],
                ReconcileReason::NodeTemplatePulled,
            )
            .await
            {
                tracing::warn!(error = %err, "failed to enqueue pulled template inventory reconcile");
            }
            let response = response.into_inner();
            Json(PullNodeTemplateResponse {
                content_hash: response.content_hash,
                already_existed: response.already_existed,
                bytes_transferred: response.bytes_transferred,
            })
            .into_response()
        }
        Err(err) => err.into_response(),
    }
}

pub async fn delete_node_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path((node_id, name)): Path<(NodeId, String)>,
    Query(query): Query<DeleteNodeTemplateQuery>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Err(err) = require_direct_substrate(&state) {
        return err.into_response();
    }

    let name = name.trim().to_string();
    if name.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "template name is required").into_response();
    }

    let node = match fetch_node(&state.pool, node_id).await {
        Ok(Some(node)) => node,
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(err) => return db_error(err).into_response(),
    };
    let target_role = match template_capable_node_role(&node, "target") {
        Ok(role) => role,
        Err(err) => return err.into_response(),
    };
    let mut node_api = match node_api_client(&node, "target", NODE_API_FAST_RPC_TIMEOUT) {
        Ok(client) => client,
        Err(err) => return err.into_response(),
    };

    if let Some(content_hash) = non_empty_optional(query.content_hash) {
        let template_name = name.clone();
        let content_hash_for_cache = content_hash.clone();
        let request = DeleteTemplateBuildRequest {
            template: Some(TemplateRef {
                name,
                content_hash: Some(content_hash),
                tag: None,
            }),
        };
        let response = match target_role {
            NodeRole::Worker => {
                let mut client = WorkerApiClient::new(&mut node_api);
                client
                    .delete_template_build(request, NODE_API_FAST_RPC_TIMEOUT)
                    .await
            }
            NodeRole::Builder => {
                let mut client = BuilderApiClient::new(&mut node_api);
                client
                    .delete_template_build(request, NODE_API_FAST_RPC_TIMEOUT)
                    .await
            }
            NodeRole::Cluster => unreachable!("cluster nodes are rejected above"),
        }
        .map_err(|status| node_api_rpc_error_response(&node, "DeleteTemplateBuild", status));
        return match response {
            Ok(_) => {
                note_node_template_deleted(
                    &state.pool,
                    &node,
                    &template_name,
                    Some(&content_hash_for_cache),
                )
                .await;
                StatusCode::NO_CONTENT.into_response()
            }
            Err(err) => err.into_response(),
        };
    }

    let template_name = name.clone();
    let request = DeleteTemplateRequest { template: name };
    let response = match target_role {
        NodeRole::Worker => {
            let mut client = WorkerApiClient::new(&mut node_api);
            client
                .delete_template(request, NODE_API_FAST_RPC_TIMEOUT)
                .await
        }
        NodeRole::Builder => {
            let mut client = BuilderApiClient::new(&mut node_api);
            client
                .delete_template(request, NODE_API_FAST_RPC_TIMEOUT)
                .await
        }
        NodeRole::Cluster => unreachable!("cluster nodes are rejected above"),
    }
    .map_err(|status| node_api_rpc_error_response(&node, "DeleteTemplate", status));

    match response {
        Ok(_) => {
            note_node_template_deleted(&state.pool, &node, &template_name, None).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => err.into_response(),
    }
}

async fn list_templates_for_node(
    pool: &sqlx::PgPool,
    node: NodeRow,
) -> NodeTemplateInventoryResponse {
    let node_response = node_response(node.clone());
    if matches!(node.role, NodeRole::Cluster) {
        return NodeTemplateInventoryResponse {
            node: node_response,
            reachable: true,
            error: Some("cluster nodes do not host templates".to_string()),
            builds: Vec::new(),
        };
    }

    match sqlx::query_as::<_, NodeTemplateInventoryRow>(sql!(
        "SELECT template_name AS name,
                NULLIF(content_hash, '') AS content_hash,
                built_at AS build_date,
                artifact_size_bytes,
                cache_size_bytes,
                disk_size_mb,
                runtime,
                arch,
                snapshots
         FROM node_template_inventory
         WHERE node_id = $1
         ORDER BY template_name ASC, content_hash ASC"
    ))
    .bind(node.id)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => NodeTemplateInventoryResponse {
            node: node_response,
            reachable: true,
            error: None,
            builds: rows
                .into_iter()
                .map(|build| NodeTemplateBuildResponse {
                    name: build.name,
                    content_hash: build.content_hash,
                    build_date: build.build_date,
                    artifact_size_bytes: build
                        .artifact_size_bytes
                        .and_then(|value| u64::try_from(value).ok()),
                    cache_size_bytes: build
                        .cache_size_bytes
                        .and_then(|value| u64::try_from(value).ok()),
                    disk_size_mb: build
                        .disk_size_mb
                        .and_then(|value| u32::try_from(value).ok()),
                    runtime: build.runtime,
                    arch: build.arch,
                    snapshots: build.snapshots.0,
                })
                .collect(),
        },
        Err(err) => NodeTemplateInventoryResponse {
            node: node_response,
            reachable: false,
            error: Some(format!(
                "query cached node template inventory failed: {err}"
            )),
            builds: Vec::new(),
        },
    }
}

pub async fn list_node_templates(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(filters): Query<NodeFilters>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let role_filter = filters.role;
    let nodes = match fetch_inventory_nodes(&state, role_filter).await {
        Ok(nodes) => nodes
            .into_iter()
            .filter(|node| role_filter.is_some() || node_supports_state_reconciliation(node.role))
            .collect::<Vec<_>>(),
        Err(err) => return err.into_response(),
    };

    let pool = state.pool.clone();
    let mut inventories = stream::iter(nodes.into_iter().enumerate())
        .map(|(index, node)| {
            let pool = pool.clone();
            async move { (index, list_templates_for_node(&pool, node).await) }
        })
        .buffer_unordered(NODE_TEMPLATE_INVENTORY_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    inventories.sort_by_key(|(index, _)| *index);
    Json(
        inventories
            .into_iter()
            .map(|(_, inventory)| inventory)
            .collect::<Vec<_>>(),
    )
    .into_response()
}

#[cfg(test)]
mod tests;
