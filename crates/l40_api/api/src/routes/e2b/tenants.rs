//! `RegisterTenant`: a client's API key registers its own tenants.
//!
//! A tenant is a child org of the API key's org, keyed by the client's
//! `external_id`. Sandbox routes take `x-tenant-id` (resolved in
//! `middleware::auth::require_e2b_api_key`) and then run as the tenant org.
//!
//! A tenant may ask for dedicated capacity. That is a `node_groups` row owned
//! by the tenant org, drawn from a generic pool (`SPACE_DEDICATED_CAPACITY_POOL`,
//! an EKS managed node group on AWS). A tenant that brings a cache key gets a
//! pool of its own instead: `pool` is the group's name, and the cluster
//! controller creates an EKS managed node group of that name whose launch
//! template encrypts the root volume and the instance-store cache under the
//! tenant's key (`SPACE_TENANT_NODE_GROUPS_ENABLED`). The controller's
//! `dedicated_capacity` reconciler grows the pool and assigns nodes to it;
//! this module only records intent.

use std::sync::OnceLock;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use pplx_space_accounts::OrgId;
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AppState;
use crate::{e2b::errors::E2bApiError, middleware::auth::AuthApiKey};

/// Pool that dedicated groups draw from. Unset means this cluster offers no
/// dedicated capacity, and a request for it is refused.
pub(crate) const DEDICATED_CAPACITY_POOL_ENV: &str = "SPACE_DEDICATED_CAPACITY_POOL";

/// Whether the cluster controller creates a node group per encrypted tenant.
/// Unset means this cluster offers no encrypted dedicated capacity.
pub(crate) const TENANT_NODE_GROUPS_ENV: &str = "SPACE_TENANT_NODE_GROUPS_ENABLED";

fn pool_from_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn dedicated_capacity_pool() -> Option<&'static str> {
    static POOL: OnceLock<Option<String>> = OnceLock::new();
    POOL.get_or_init(|| pool_from_env(DEDICATED_CAPACITY_POOL_ENV))
        .as_deref()
}

fn tenant_node_groups_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        pool_from_env(TENANT_NODE_GROUPS_ENV).is_some_and(|value| {
            matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
        })
    })
}

/// Upper bound on nodes per tenant for the PoC.
const MAX_DEDICATED_NODES: i32 = 4;

#[derive(Debug, Deserialize)]
pub(crate) struct RegisterTenantRequest {
    external_id: String,
    #[serde(default)]
    dedicated_node_group: Option<DedicatedNodeGroupRequest>,
    /// Run the tenant's template builds on its dedicated nodes too. Not yet
    /// supported: builds run on the shared builders. (Templates a tenant
    /// builds are already `org_specific`, so they launch and are cached only
    /// on its dedicated group.)
    #[serde(default)]
    dedicated_templates: bool,
    /// The tenant's KMS key (BYOK). Requires `dedicated_node_group`: the
    /// tenant's node group is created with its root volume and instance-store
    /// cache encrypted under this key. Fixed at registration: a later call
    /// can't change or drop it.
    #[serde(default)]
    byok_key_arn: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DedicatedNodeGroupRequest {
    nodes: i32,
}

#[derive(Debug, Serialize)]
pub(crate) struct TenantResponse {
    tenant_id: OrgId,
    external_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dedicated_node_group: Option<DedicatedNodeGroupStatus>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DedicatedNodeGroupStatus {
    name: String,
    desired_nodes: i32,
    ready_nodes: i64,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    byok_key_arn: Option<String>,
}

#[derive(sqlx::FromRow)]
struct TenantRow {
    id: OrgId,
    external_id: String,
    group_name: Option<String>,
    desired_nodes: Option<i32>,
    ready_nodes: i64,
    cache_kms_key_arn: Option<String>,
}

impl TenantRow {
    fn into_response(self) -> TenantResponse {
        let dedicated_node_group = match (self.group_name, self.desired_nodes) {
            (Some(name), Some(desired_nodes)) => Some(DedicatedNodeGroupStatus {
                status: match (desired_nodes, self.ready_nodes) {
                    (0, 0) => "released",
                    (0, _) => "releasing",
                    (desired, ready) if ready >= i64::from(desired) => "ready",
                    _ => "provisioning",
                },
                name,
                desired_nodes,
                ready_nodes: self.ready_nodes,
                byok_key_arn: self.cache_kms_key_arn,
            }),
            _ => None,
        };
        TenantResponse {
            tenant_id: self.id,
            external_id: self.external_id,
            dedicated_node_group,
        }
    }
}

/// `arn:<partition>:kms:<region>:<12-digit account>:key/<key id>`. Aliases are
/// refused: an alias can be repointed after the nodes are keyed.
fn is_kms_key_arn(arn: &str) -> bool {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    matches!(
        parts.as_slice(),
        ["arn", partition, "kms", region, account, resource]
            if partition.starts_with("aws")
                && !region.is_empty()
                && account.len() == 12
                && account.bytes().all(|b| b.is_ascii_digit())
                && resource.strip_prefix("key/").is_some_and(|id| {
                    !id.is_empty()
                        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
    )
}

/// The node group name for a tenant: stable, unique, and a valid label value.
fn group_name(tenant: OrgId) -> String {
    format!("t-{tenant}")
}

const SELECT_TENANT: &str = sql!(
    "SELECT o.id, o.external_id, g.name AS group_name, g.desired_nodes, g.cache_kms_key_arn,
            (SELECT count(*) FROM nodes n
              WHERE g.id IS NOT NULL AND n.node_group_id = g.id
                AND n.state = 'ready' AND n.drain_phase IS NULL) AS ready_nodes
       FROM organizations o
       LEFT JOIN node_groups g ON g.org_id = o.id AND g.pool IS NOT NULL
      WHERE o.id = $1 AND o.parent_id = $2"
);

async fn load_tenant(
    pool: &sqlx::PgPool,
    tenant: OrgId,
    client: OrgId,
) -> Result<Option<TenantRow>, sqlx::Error> {
    sqlx::query_as::<_, TenantRow>(SELECT_TENANT)
        .bind(tenant)
        .bind(client)
        .fetch_optional(pool)
        .await
}

fn db_error(error: sqlx::Error) -> E2bApiError {
    tracing::error!(%error, "tenant store query failed");
    E2bApiError::internal("Internal server error")
}

/// `POST /v1/tenants`. Idempotent on `(client org, external_id)`: a repeat
/// returns the same tenant and updates the requested node count.
pub(crate) async fn register_tenant(
    State(state): State<AppState>,
    Extension(AuthApiKey(key)): Extension<AuthApiKey>,
    Json(request): Json<RegisterTenantRequest>,
) -> Result<(StatusCode, Json<TenantResponse>), E2bApiError> {
    let external_id = request.external_id.trim();
    if external_id.is_empty() || external_id.len() > 128 {
        return Err(E2bApiError::bad_request(
            "external_id must be 1 to 128 characters",
        ));
    }
    if request.dedicated_templates {
        return Err(E2bApiError::not_implemented(
            "dedicated_templates is not supported yet: template builds run on shared builders",
        ));
    }
    let cache_kms_key_arn = request
        .byok_key_arn
        .map(|arn| arn.trim().to_string())
        .filter(|arn| !arn.is_empty());
    if cache_kms_key_arn.is_some() && request.dedicated_node_group.is_none() {
        return Err(E2bApiError::bad_request(
            "byok_key_arn requires dedicated_node_group",
        ));
    }
    let dedicated = match request.dedicated_node_group {
        None => None,
        Some(DedicatedNodeGroupRequest { nodes }) => {
            if !(1..=MAX_DEDICATED_NODES).contains(&nodes) {
                return Err(E2bApiError::bad_request(format!(
                    "dedicated_node_group.nodes must be 1 to {MAX_DEDICATED_NODES}"
                )));
            }
            if let Some(arn) = &cache_kms_key_arn
                && !is_kms_key_arn(arn)
            {
                return Err(E2bApiError::bad_request(
                    "byok_key_arn must be a KMS key ARN \
                     (arn:aws:kms:<region>:<account>:key/<id>)",
                ));
            }
            // An encrypted group never draws from a shared pool: it is its
            // own pool, a node group born keyed to the tenant. The name is
            // filled in once the tenant id is known.
            let pool = if cache_kms_key_arn.is_some() {
                if !tenant_node_groups_enabled() {
                    return Err(E2bApiError::bad_request(
                        "this cluster does not offer encrypted dedicated node groups",
                    ));
                }
                None
            } else {
                Some(dedicated_capacity_pool().ok_or_else(|| {
                    E2bApiError::bad_request("this cluster does not offer dedicated node groups")
                })?)
            };
            Some((pool, nodes, cache_kms_key_arn))
        }
    };

    let client = key.org_id;
    let mut tx = state.pool.begin().await.map_err(db_error)?;
    let new_id = OrgId::from_uuid(Uuid::now_v7());
    // The slug is unique cluster-wide and means nothing to the client.
    let slug = format!("tenant-{new_id}");
    let tenant: OrgId = sqlx::query_scalar(sql!(
        "INSERT INTO organizations (id, name, slug, parent_id, external_id)
         VALUES ($1, $2, $3, $4, $2)
         ON CONFLICT (parent_id, external_id) WHERE external_id IS NOT NULL
         DO UPDATE SET updated_at = organizations.updated_at
         RETURNING id"
    ))
    .bind(new_id)
    .bind(external_id)
    .bind(slug)
    .bind(client)
    .fetch_one(&mut *tx)
    .await
    .map_err(db_error)?;

    if let Some((pool, nodes, cache_kms_key_arn)) = dedicated {
        let name = group_name(tenant);
        let pool = pool.map_or_else(|| name.clone(), str::to_string);
        // Re-registering resizes the group. It can't change the key or the
        // pool: the nodes are already built around them.
        let updated = sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, pool, desired_nodes, cache_kms_key_arn)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (name) DO UPDATE SET desired_nodes = EXCLUDED.desired_nodes
              WHERE node_groups.pool IS NOT DISTINCT FROM EXCLUDED.pool
                AND node_groups.cache_kms_key_arn IS NOT DISTINCT FROM EXCLUDED.cache_kms_key_arn"
        ))
        .bind(Uuid::now_v7())
        .bind(&name)
        .bind(tenant)
        .bind(pool)
        .bind(nodes)
        .bind(cache_kms_key_arn)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        if updated.rows_affected() == 0 {
            return Err(E2bApiError::new(
                StatusCode::CONFLICT,
                "byok_key_arn is fixed when a tenant's dedicated node group is first \
                 registered; register a new tenant to use a different key",
            ));
        }
    }
    tx.commit().await.map_err(db_error)?;

    let row = load_tenant(&state.pool, tenant, client)
        .await
        .map_err(db_error)?
        .ok_or_else(|| E2bApiError::internal("tenant vanished after registration"))?;
    let status = if tenant == new_id {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(row.into_response())))
}

/// `GET /v1/tenants/{tenantID}`.
pub(crate) async fn get_tenant(
    State(state): State<AppState>,
    Extension(AuthApiKey(key)): Extension<AuthApiKey>,
    Path(tenant): Path<OrgId>,
) -> Result<Json<TenantResponse>, E2bApiError> {
    load_tenant(&state.pool, tenant, key.org_id)
        .await
        .map_err(db_error)?
        .map(|row| Json(row.into_response()))
        .ok_or_else(|| E2bApiError::not_found("tenant"))
}

/// `DELETE /v1/tenants/{tenantID}`. Releases the tenant's dedicated nodes:
/// the reconciler drains and terminates them. The tenant org stays, so its
/// sandboxes and the `external_id` mapping are unaffected.
pub(crate) async fn delete_tenant(
    State(state): State<AppState>,
    Extension(AuthApiKey(key)): Extension<AuthApiKey>,
    Path(tenant): Path<OrgId>,
) -> Result<(StatusCode, Json<TenantResponse>), E2bApiError> {
    let pool = &state.pool;
    if load_tenant(pool, tenant, key.org_id)
        .await
        .map_err(db_error)?
        .is_none()
    {
        return Err(E2bApiError::not_found("tenant"));
    }
    sqlx::query(sql!(
        "UPDATE node_groups SET desired_nodes = 0 WHERE org_id = $1 AND pool IS NOT NULL"
    ))
    .bind(tenant)
    .execute(pool)
    .await
    .map_err(db_error)?;
    let row = load_tenant(pool, tenant, key.org_id)
        .await
        .map_err(db_error)?
        .ok_or_else(|| E2bApiError::not_found("tenant"))?;
    Ok((StatusCode::ACCEPTED, Json(row.into_response())))
}

/// Resolve `x-tenant-id` to the tenant org, which must be a child of the API
/// key's org. `Ok(None)` means no such tenant for this client.
pub(crate) async fn resolve_tenant_org(
    pool: &sqlx::PgPool,
    tenant: OrgId,
    client: OrgId,
) -> Result<Option<crate::models::Organization>, sqlx::Error> {
    sqlx::query_as::<_, crate::models::Organization>(sql!(
        "SELECT * FROM organizations WHERE id = $1 AND parent_id = $2"
    ))
    .bind(tenant)
    .bind(client)
    .fetch_optional(pool)
    .await
}
