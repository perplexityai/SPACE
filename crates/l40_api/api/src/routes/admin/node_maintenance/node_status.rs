//! Admin node status, health and reconcile routes, and the cached node status they read.

use super::*;

#[derive(Debug, Clone, FromRow)]
pub(super) struct NodeStatusRow {
    pub(super) node_id: NodeId,
    pub(super) stale_after: OffsetDateTime,
    pub(super) reachable: bool,
    pub(super) error: Option<String>,
    pub(super) health_healthy: Option<bool>,
    pub(super) services: sqlx::types::Json<Vec<ServiceHealthResponse>>,
    pub(super) node_ctl: Option<sqlx::types::Json<NodeCtlStatusResponse>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ServiceHealthResponse {
    pub(super) name: String,
    pub(super) reachable: bool,
    pub(super) error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ReconciliationStatusResponse {
    pub(super) name: String,
    pub(super) poll_interval_secs: u64,
    pub(super) last_started_at_unix_secs: u64,
    pub(super) last_completed_at_unix_secs: u64,
    pub(super) last_success_at_unix_secs: u64,
    pub(super) next_due_at_unix_secs: u64,
    pub(super) running: bool,
    pub(super) templates_seen: u64,
    pub(super) sandboxes_seen: u64,
    pub(super) templates_applied: u64,
    pub(super) sandboxes_applied: u64,
    pub(super) last_error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct NodeCtlStatusResponse {
    pub(super) reachable: bool,
    pub(super) error: Option<String>,
    pub(super) reconciliations: Vec<ReconciliationStatusResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct NodeHealthStatusResponse {
    pub(super) reachable: bool,
    pub(super) healthy: bool,
    pub(super) error: Option<String>,
    pub(super) services: Vec<ServiceHealthResponse>,
    pub(super) node_ctl: Option<NodeCtlStatusResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshNodeStatusRequest {
    pub(super) role: Option<NodeRole>,
    #[serde(default)]
    pub(super) force: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileFacetRequest {
    #[serde(deserialize_with = "deserialize_reconcile_facet")]
    pub(super) facet: ReconcileFacet,
    pub(super) element: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileNodesRequest {
    pub(super) role: Option<NodeRole>,
    #[serde(default)]
    pub(super) node_ids: Vec<NodeId>,
    #[serde(default, deserialize_with = "deserialize_reconcile_facets")]
    pub(super) facets: Vec<ReconcileFacet>,
    #[serde(default)]
    pub(super) refs: Vec<ReconcileFacetRequest>,
}

#[derive(Debug, Serialize)]
pub(super) struct RefreshNodeStatusResponse {
    pub(super) refreshed: bool,
    pub(super) nodes_seen: usize,
    pub(super) nodes_updated: usize,
    pub(super) nodes_failed: usize,
    pub(super) cache_ttl_secs: u64,
    pub(super) error: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ReconcileNodeResponse {
    pub(super) node_id: NodeId,
    pub(super) node_name: Option<String>,
    pub(super) role: String,
    pub(super) queued: bool,
    pub(super) error: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ReconcileNodesResponse {
    pub(super) nodes_seen: usize,
    pub(super) nodes_queued: usize,
    pub(super) nodes_failed: usize,
    pub(super) nodes: Vec<ReconcileNodeResponse>,
}

#[derive(Debug, Serialize)]
pub(super) struct ReconcileNodeNetworkPolicyResponse {
    pub(super) queued: bool,
}

pub(super) fn deserialize_reconcile_facet<'de, D>(
    deserializer: D,
) -> Result<ReconcileFacet, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    ReconcileFacet::parse(value.trim())
        .ok_or_else(|| de::Error::custom(format!("unsupported reconcile facet: {value}")))
}

pub(super) fn deserialize_reconcile_facets<'de, D>(
    deserializer: D,
) -> Result<Vec<ReconcileFacet>, D::Error>
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

pub(super) fn node_supports_state_reconciliation(role: NodeRole) -> bool {
    matches!(role, NodeRole::Worker | NodeRole::Builder)
}

pub(super) async fn fetch_inventory_node(
    state: &AppState,
    id: NodeId,
) -> Result<NodeRow, (StatusCode, Json<serde_json::Value>)> {
    let nodes = fetch_inventory_nodes(state, None).await?;
    nodes
        .into_iter()
        .find(|node| node.id == id && node.deleted_at.is_none())
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Node not found"))
}

pub(super) async fn fetch_node_status_map(
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

pub(super) async fn node_status_refresh_needed(
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

pub(super) async fn fetch_cached_node_health(
    pool: &sqlx::PgPool,
    node: &NodeRow,
) -> NodeHealthStatusResponse {
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

pub(super) fn default_node_publish_refs() -> Vec<ReconcileRef> {
    vec![
        ReconcileRef::full(ReconcileFacet::NodeStatus),
        ReconcileRef::full(ReconcileFacet::SandboxInventory),
        ReconcileRef::full(ReconcileFacet::TemplateInventory),
    ]
}

pub(super) fn reconcile_refs_from_request(
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

pub async fn refresh_node_status(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(req): Json<RefreshNodeStatusRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    // A node's status window at node-ctl's default poll interval. A refresh is a
    // no-op while every node's status is still fresh.
    let cache_ttl_secs =
        pplx_space_node_api_client::node_status_freshness::DEFAULT_NODE_STATUS_FRESHNESS.as_secs();
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
