//! Admin node template routes: pull and delete a template on a node through its node-api, and list
//! a node's templates from the cached template inventory.

use super::*;

pub(super) const NODE_API_TEMPLATE_RPC_TIMEOUT: Duration = Duration::from_secs(600);

pub(super) const NODE_TEMPLATE_INVENTORY_CONCURRENCY: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct NodeTemplateSnapshotResponse {
    pub(super) vcpu: u32,
    pub(super) memory_mib: u32,
    pub(super) snapshot_hash: String,
}

#[derive(Debug, FromRow)]
pub(super) struct NodeTemplateInventoryRow {
    pub(super) name: String,
    pub(super) content_hash: Option<String>,
    pub(super) build_date: Option<String>,
    pub(super) artifact_size_bytes: Option<i64>,
    pub(super) cache_size_bytes: Option<i64>,
    pub(super) disk_size_mb: Option<i32>,
    pub(super) runtime: Option<String>,
    pub(super) arch: Option<String>,
    pub(super) snapshots: sqlx::types::Json<Vec<NodeTemplateSnapshotResponse>>,
}

#[derive(Debug, Serialize)]
pub(super) struct NodeTemplateBuildResponse {
    pub(super) name: String,
    pub(super) content_hash: Option<String>,
    pub(super) build_date: Option<String>,
    pub(super) artifact_size_bytes: Option<u64>,
    pub(super) cache_size_bytes: Option<u64>,
    pub(super) disk_size_mb: Option<u32>,
    pub(super) runtime: Option<String>,
    pub(super) arch: Option<String>,
    pub(super) snapshots: Vec<NodeTemplateSnapshotResponse>,
}

#[derive(Debug, Serialize)]
pub(super) struct NodeTemplateInventoryResponse {
    pub(super) node: NodeResponse,
    pub(super) reachable: bool,
    pub(super) error: Option<String>,
    pub(super) builds: Vec<NodeTemplateBuildResponse>,
}

#[derive(Debug, Deserialize)]
pub struct NodeFilters {
    pub(super) role: Option<NodeRole>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullNodeTemplateRequest {
    pub(super) template_id: String,
    pub(super) content_hash: Option<String>,
    pub(super) source_node_id: NodeId,
    #[serde(default)]
    pub(super) include_snapshot: bool,
    pub(super) snapshot_vcpu: Option<u32>,
    pub(super) snapshot_memory_mib: Option<u32>,
    #[serde(default = "default_true")]
    pub(super) skip_if_exists: bool,
    pub(super) local_tag: Option<String>,
    pub(super) block_on_snapshot_policy_achieved: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteNodeTemplateQuery {
    pub(super) content_hash: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct PullNodeTemplateResponse {
    pub(super) content_hash: String,
    pub(super) already_existed: bool,
    pub(super) bytes_transferred: u64,
}

pub(super) fn default_true() -> bool {
    true
}

pub(super) fn template_capable_node_role(
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

pub(super) fn ensure_template_capable_node(
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

pub(super) fn non_empty_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() { None } else { Some(value) }
    })
}

pub(super) async fn delete_cached_node_template_inventory(
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

pub(super) async fn note_node_template_deleted(
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

pub async fn pull_node_template(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(target_id): Path<NodeId>,
    Json(req): Json<PullNodeTemplateRequest>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Err(err) = require_admin_node_registry(&state) {
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
    if let Err(err) = require_admin_node_registry(&state) {
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
                environment: None,
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

pub(super) async fn list_templates_for_node(
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
