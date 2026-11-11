//! `/admin/v1/node-groups` operator surface.
//!
//! Node groups are the control-plane rows behind org-scoped placement.
//! Users never name a node group; these endpoints are how operators stand
//! groups up, dedicate them to orgs, and curate the `restricted` template
//! allowlist. Every write enforces the RFC invariants and emits an audit
//! event — node groups are never hand-edited SQL.
//!
//! The row is only half of a node group: the matching EKS managed node group
//! carries the `space-node-group` label (and, for org-owned groups, the
//! dedicated taint + DaemonSet toleration rollout). Those are provisioned in
//! lockstep via Terraform; this API deliberately performs no substrate
//! writes. `nodes.node_group_id` is likewise never set here — the
//! cluster-controller mirror reconciler copies it from the live node label.

use pplx_space_common_core::types::NodeGroupId;
use pplx_space_common_core::types::OrgId;

use std::str::FromStr;

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_common_core::types::admin::{
    AdminNodeGroupResponse, AssignOrgRequest, CreateNodeGroupRequest, SetCompatRequest,
    SetTemplatePolicyRequest,
};
use pplx_space_common_core::types::{NodeGroupTemplatePolicy, TemplateVisibility};
use uuid::Uuid;

use super::audit::{self, AdminNodeGroupAuditEntry};
use super::node_group_store::{self, AdminNodeGroupStore, NodeGroupRow, violated_constraint};
use super::support::{
    api_error, db_error, format_time, parse_json_body, require_managing_org_admin,
};
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeGroupAction {
    AssignOrg,
    SetDefault,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown node group action")]
struct NodeGroupActionParseError;

impl FromStr for NodeGroupAction {
    type Err = NodeGroupActionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "assign-org" => Ok(Self::AssignOrg),
            "set-default" => Ok(Self::SetDefault),
            _ => Err(NodeGroupActionParseError),
        }
    }
}

/// Parse a `{spec}` segment that carries either a bare UUID (GET/DELETE and
/// the subresource PUTs) or a `UUID:action` pair (POST form). Mirrors the
/// `/admin/v1/nodes/{spec}` workaround for matchit's lack of dynamic
/// suffixes.
fn parse_group_spec(
    spec: &str,
) -> Result<(NodeGroupId, Option<NodeGroupAction>), (StatusCode, Json<serde_json::Value>)> {
    let (id_part, action_part) = match spec.split_once(':') {
        Some((id, action)) => (id, Some(action)),
        None => (spec, None),
    };
    let node_group_id = id_part.parse::<NodeGroupId>().map_err(|_| {
        api_error(
            StatusCode::BAD_REQUEST,
            "node group id must be a valid UUID",
        )
    })?;
    let action = match action_part {
        None => None,
        Some(label) => Some(label.parse::<NodeGroupAction>().map_err(|_| {
            api_error(
                StatusCode::NOT_FOUND,
                format!("unknown admin action {label:?}; expected one of: assign-org, set-default"),
            )
        })?),
    };
    Ok((node_group_id, action))
}

/// Parse a `{spec}` that must be a bare UUID (no `:action` suffix).
fn parse_bare_group_id(spec: &str) -> Result<NodeGroupId, (StatusCode, Json<serde_json::Value>)> {
    let (node_group_id, action) = parse_group_spec(spec)?;
    if action.is_some() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "this endpoint takes a bare node group id, not an :action suffix",
        ));
    }
    Ok(node_group_id)
}

fn node_group_response(row: NodeGroupRow, compat_templates: Vec<String>) -> AdminNodeGroupResponse {
    AdminNodeGroupResponse {
        id: row.id,
        name: row.name,
        org_id: row.org_id,
        template_policy: row.template_policy,
        is_default: row.is_default,
        created_at: format_time(row.created_at),
        nodes_count: row.nodes_count,
        compat_templates,
    }
}

/// Pre-rendered `org=… policy=… default=…` summary for audit entries.
///
/// `pub` because the node-group seed lives out of crate (the cluster
/// controller's node-group labeler) and emits its own `space.admin.audit`
/// entry. It has to render the *identical* summary or the trail forks: a
/// consumer reading `to_state` positionally would see one term count for
/// `node-group-create` and another for `node-group-seed-default`. Sharing the
/// formatter makes that agreement structural instead of two literals that
/// happen to match today.
pub fn group_state_summary(
    org_id: Option<OrgId>,
    policy: NodeGroupTemplatePolicy,
    default: bool,
) -> String {
    let org = org_id.map_or_else(|| "generic".to_string(), |id| id.to_string());
    format!("org={org} policy={policy} default={default}")
}

fn row_state_summary(row: &NodeGroupRow) -> String {
    group_state_summary(row.org_id, row.template_policy, row.is_default)
}

/// Fetch the post-write state of a group (row + allowlist) for the response
/// body, so callers can read the result without an extra round trip.
async fn fetch_group_response(
    store: &AdminNodeGroupStore,
    node_group_id: NodeGroupId,
) -> Result<Option<AdminNodeGroupResponse>, sqlx::Error> {
    let Some(row) = store.fetch(node_group_id).await? else {
        return Ok(None);
    };
    let compat = store
        .compat_templates_for(std::slice::from_ref(&node_group_id))
        .await?
        .remove(&node_group_id)
        .unwrap_or_default();
    Ok(Some(node_group_response(row, compat)))
}

/// [`fetch_group_response`] with the miss/error cases already mapped onto
/// ready-to-return responses, for the read-back that every handler performs.
async fn fetch_group_or_error(
    pool: &sqlx::PgPool,
    node_group_id: NodeGroupId,
) -> Result<AdminNodeGroupResponse, Box<axum::response::Response>> {
    let store = AdminNodeGroupStore::new(pool.clone());
    match fetch_group_response(&store, node_group_id).await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(Box::new(
            api_error(StatusCode::NOT_FOUND, "Node group not found").into_response(),
        )),
        Err(err) => Err(Box::new(db_error(err).into_response())),
    }
}

pub async fn list_node_groups(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let store = AdminNodeGroupStore::new(state.pool.clone());
    let rows = match store.list().await {
        Ok(rows) => rows,
        Err(err) => return db_error(err).into_response(),
    };
    let ids: Vec<NodeGroupId> = rows.iter().map(|row| row.id).collect();
    let mut compat_by_id = match store.compat_templates_for(&ids).await {
        Ok(map) => map,
        Err(err) => return db_error(err).into_response(),
    };
    let responses: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let compat = compat_by_id.remove(&row.id).unwrap_or_default();
            node_group_response(row, compat)
        })
        .collect();
    Json(responses).into_response()
}

pub async fn get_node_group(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let (node_group_id, action) = match parse_group_spec(&spec) {
        Ok(parsed) => parsed,
        Err(err) => return err.into_response(),
    };
    if action.is_some() {
        return api_error(StatusCode::METHOD_NOT_ALLOWED, "admin actions require POST")
            .into_response();
    }
    match fetch_group_or_error(&state.pool, node_group_id).await {
        Ok(response) => Json(response).into_response(),
        Err(response) => *response,
    }
}

/// `POST /admin/v1/node-groups` — create a group row.
///
/// Cross-field invariants are validated here (friendly 400s); the matching
/// CHECK constraints in the schema are the backstop. Name and singleton-
/// default uniqueness can only be enforced by the database, so those two
/// surface as mapped 409s.
pub async fn create_node_group(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let request: CreateNodeGroupRequest = match parse_json_body(&body, "create node group") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let template_policy = request
        .template_policy
        .unwrap_or(NodeGroupTemplatePolicy::Default);
    if request.org_id.is_none() && matches!(template_policy, NodeGroupTemplatePolicy::Restricted) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "a generic node group must use the 'default' template policy; \
             'restricted' is only meaningful for org-owned groups",
        )
        .into_response();
    }
    if request.is_default && request.org_id.is_some() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "only a generic node group can be the cluster default",
        )
        .into_response();
    }
    // A dedicated group must not be the cluster's first node group.
    //
    // Two things go wrong otherwise, and the second is why a doc note is not
    // enough. The labeler's `ensure_default_group` seeds the default generic
    // group *only* while no non-default group exists, so an org-owned row
    // created first blocks that seeding permanently — and no admin route can
    // clear it, since the default can be neither deleted nor un-defaulted. The
    // cluster is then latched with dedicated capacity and no generic group.
    //
    // In that state a generic template resolves to an empty candidate set for
    // every org without dedicated capacity, which reports `unprovisioned` and
    // renders no boundary at all — so those sandboxes are held off the
    // dedicated nodes only by the taint, and the warm-pool claim has no
    // candidate set to check an already-running pod against. Refusing the
    // create is cheaper than defending that state everywhere downstream.
    if request.org_id.is_some() {
        match node_group_store::generic_node_group_exists(&state.pool).await {
            Ok(true) => {}
            Ok(false) => {
                return api_error(
                    StatusCode::CONFLICT,
                    "this cluster has no generic node group, so a dedicated group cannot be \
                     created yet: it would block the default generic group from ever being \
                     seeded. Create the default generic group first.",
                )
                .into_response();
            }
            Err(err) => {
                tracing::error!(%err, "failed to check for an existing generic node group");
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to validate node-group provisioning order",
                )
                .into_response();
            }
        }
    }

    let insert = node_group_store::insert_node_group(
        &state.pool,
        &request.name,
        request.org_id,
        template_policy,
        request.is_default,
    )
    .await;
    let node_group_id = match insert {
        Ok(id) => id,
        Err(err) => {
            let (status, message) = match violated_constraint(&err).as_deref() {
                Some("node_groups_name_key") => (
                    StatusCode::CONFLICT,
                    format!(
                        "a node group named {:?} already exists",
                        request.name.as_str()
                    ),
                ),
                Some("idx_node_groups_single_default") => (
                    StatusCode::CONFLICT,
                    "another node group is already the cluster default; \
                     use :set-default to move the flag"
                        .to_string(),
                ),
                Some("node_groups_org_id_fkey") => {
                    (StatusCode::BAD_REQUEST, "unknown org".to_string())
                }
                _ => {
                    return db_error(err).into_response();
                }
            };
            audit::record_node_group(AdminNodeGroupAuditEntry {
                actor: &actor,
                action: "node-group-create",
                node_group_id: NodeGroupId::from_uuid(Uuid::nil()),
                node_group_name: Some(request.name.as_str()),
                from_state: None,
                to_state: Some(&group_state_summary(
                    request.org_id,
                    template_policy,
                    request.is_default,
                )),
                success: false,
                error: Some(&message),
            });
            return api_error(status, message).into_response();
        }
    };

    audit::record_node_group(AdminNodeGroupAuditEntry {
        actor: &actor,
        action: "node-group-create",
        node_group_id,
        node_group_name: Some(request.name.as_str()),
        from_state: None,
        to_state: Some(&group_state_summary(
            request.org_id,
            template_policy,
            request.is_default,
        )),
        success: true,
        error: None,
    });

    match fetch_group_or_error(&state.pool, node_group_id).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(response) => *response,
    }
}

/// Dispatch a `POST /admin/v1/node-groups/{spec}` where `spec` must be a
/// `UUID:action` pair (`:assign-org` or `:set-default`).
pub async fn dispatch_node_group_action(
    state: State<AppState>,
    actor: Extension<AuthUser>,
    Path(spec): Path<String>,
    body: Bytes,
) -> axum::response::Response {
    let (node_group_id, action) = match parse_group_spec(&spec) {
        Ok(parsed) => parsed,
        Err(err) => return err.into_response(),
    };
    let Some(action) = action else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "admin POST requires an :action suffix (assign-org, set-default)",
        )
        .into_response();
    };
    match action {
        NodeGroupAction::AssignOrg => apply_assign_org(state, actor, node_group_id, &body).await,
        NodeGroupAction::SetDefault => apply_set_default(state, actor, node_group_id).await,
    }
}

/// `:assign-org` — set or clear a group's owning org.
///
/// Residue-guarded: the group must hold no sandboxes (live placements or
/// local-only suspended snapshots on its nodes) and no cached org-specific
/// rootfs the new assignment would strand. Releasing (or re-assigning) a
/// `restricted` group resets the policy to `default` and clears the
/// allowlist in the same transaction. This endpoint only moves the
/// control-plane row: the paired cluster-side changes are the dedicated taint
/// (Terraform) and the boundary label (an admin-credentialed Node patch — the
/// EKS managed-node-group path could also write it and this repo deliberately
/// does not use it, so the label is not a Terraform change either way).
async fn apply_assign_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    node_group_id: NodeGroupId,
    body: &[u8],
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let request: AssignOrgRequest = match parse_json_body(body, "assign-org") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };
    let group = match node_group_store::lock_group_for_update(&mut tx, node_group_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node group not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    let from_state = row_state_summary(&group);
    let audit_failure = |error: &str| {
        audit::record_node_group(AdminNodeGroupAuditEntry {
            actor: &actor,
            action: "node-group-assign-org",
            node_group_id,
            node_group_name: Some(group.name.as_str()),
            from_state: Some(&from_state),
            to_state: None,
            success: false,
            error: Some(error),
        });
    };

    if request.org_id == group.org_id {
        // No-op: already in the requested state. Don't audit a transition
        // that didn't happen; just return the current state.
        drop(tx);
        return match fetch_group_or_error(&state.pool, node_group_id).await {
            Ok(response) => Json(response).into_response(),
            Err(response) => *response,
        };
    }
    if group.is_default && request.org_id.is_some() {
        audit_failure("conflict: default group must stay generic");
        return api_error(
            StatusCode::CONFLICT,
            "the default node group must stay generic; move the default to another \
             group (:set-default) before dedicating this one",
        )
        .into_response();
    }
    if let Some(org_id) = request.org_id {
        match node_group_store::org_exists_in_tx(&mut tx, org_id).await {
            Ok(true) => {}
            Ok(false) => {
                audit_failure("unknown org");
                return api_error(StatusCode::BAD_REQUEST, "unknown org").into_response();
            }
            Err(err) => return db_error(err).into_response(),
        }
    }

    // Residue guard, under the same lock as the write: sandboxes still on
    // the group's nodes, then cached org-specific rootfs the change would
    // strand. Both must be zero before the group can change hands.
    let occupancy = match node_group_store::group_occupancy_in_tx(&mut tx, node_group_id).await {
        Ok(occupancy) => occupancy,
        Err(err) => return db_error(err).into_response(),
    };
    if occupancy.active > 0 || occupancy.local_snapshots > 0 {
        audit_failure("conflict: group still holds sandboxes");
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "node group still holds sandboxes; drain its nodes before \
                          changing the org assignment",
                "sandboxes_active": occupancy.active,
                "sandboxes_local_snapshots": occupancy.local_snapshots,
            })),
        )
            .into_response();
    }
    let stranded = match node_group_store::stranded_cached_templates_in_tx(
        &mut tx,
        node_group_id,
        request.org_id,
    )
    .await
    {
        Ok(count) => count,
        Err(err) => return db_error(err).into_response(),
    };
    if stranded > 0 {
        audit_failure("conflict: cached org-specific rootfs would be stranded");
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "org-specific template rootfs is still cached on the group's \
                          nodes and would be stranded by this assignment; wait for \
                          cache eviction (or clear the caches) first",
                "stranded_cached_templates": stranded,
            })),
        )
            .into_response();
    }

    let reset_policy = matches!(group.template_policy, NodeGroupTemplatePolicy::Restricted);
    if let Err(err) = node_group_store::update_group_org_in_tx(
        &mut tx,
        node_group_id,
        request.org_id,
        reset_policy,
    )
    .await
    {
        return db_error(err).into_response();
    }
    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }

    let response = match fetch_group_or_error(&state.pool, node_group_id).await {
        Ok(response) => response,
        Err(response) => return *response,
    };
    audit::record_node_group(AdminNodeGroupAuditEntry {
        actor: &actor,
        action: "node-group-assign-org",
        node_group_id,
        node_group_name: Some(group.name.as_str()),
        from_state: Some(&from_state),
        to_state: Some(&group_state_summary(
            response.org_id,
            response.template_policy,
            response.is_default,
        )),
        success: true,
        error: None,
    });
    Json(response).into_response()
}

/// `:set-default` — transactionally move the singleton default flag onto
/// this (generic) group. The incumbent's row is locked first so concurrent
/// moves serialize in a consistent order instead of racing the single-default
/// unique index.
async fn apply_set_default(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    node_group_id: NodeGroupId,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };
    let incumbent = match node_group_store::lock_current_default_for_update(&mut tx).await {
        Ok(incumbent) => incumbent,
        Err(err) => return db_error(err).into_response(),
    };
    let group = match node_group_store::lock_group_for_update(&mut tx, node_group_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node group not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    let from_state = row_state_summary(&group);
    if group.org_id.is_some() {
        audit::record_node_group(AdminNodeGroupAuditEntry {
            actor: &actor,
            action: "node-group-set-default",
            node_group_id,
            node_group_name: Some(group.name.as_str()),
            from_state: Some(&from_state),
            to_state: None,
            success: false,
            error: Some("conflict: only a generic group can be the default"),
        });
        return api_error(
            StatusCode::CONFLICT,
            "only a generic node group can be the cluster default",
        )
        .into_response();
    }
    if incumbent == Some(node_group_id) {
        // Already the default — idempotent no-op.
        drop(tx);
        return match fetch_group_or_error(&state.pool, node_group_id).await {
            Ok(response) => Json(response).into_response(),
            Err(response) => *response,
        };
    }
    if let Err(err) = node_group_store::move_default_in_tx(&mut tx, node_group_id).await {
        // With no incumbent default there is no row for the lock above to
        // serialize on, so two concurrent moves race the single-default unique
        // index; the loser's violation is a normal retryable conflict, not
        // an internal error.
        if violated_constraint(&err).as_deref() == Some("idx_node_groups_single_default") {
            audit::record_node_group(AdminNodeGroupAuditEntry {
                actor: &actor,
                action: "node-group-set-default",
                node_group_id,
                node_group_name: Some(group.name.as_str()),
                from_state: Some(&from_state),
                to_state: None,
                success: false,
                error: Some("conflict: lost a concurrent set-default"),
            });
            return api_error(
                StatusCode::CONFLICT,
                "another node group became the default concurrently; re-inspect and retry",
            )
            .into_response();
        }
        return db_error(err).into_response();
    }
    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }
    let response = match fetch_group_or_error(&state.pool, node_group_id).await {
        Ok(response) => response,
        Err(response) => return *response,
    };
    audit::record_node_group(AdminNodeGroupAuditEntry {
        actor: &actor,
        action: "node-group-set-default",
        node_group_id,
        node_group_name: Some(group.name.as_str()),
        from_state: Some(&from_state),
        to_state: Some(&group_state_summary(
            response.org_id,
            response.template_policy,
            response.is_default,
        )),
        success: true,
        error: None,
    });
    Json(response).into_response()
}

/// `DELETE /admin/v1/node-groups/{id}` — remove an empty, non-default group.
pub async fn delete_node_group(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let node_group_id = match parse_bare_group_id(&spec) {
        Ok(id) => id,
        Err(err) => return err.into_response(),
    };
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };
    let group = match node_group_store::lock_group_for_update(&mut tx, node_group_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node group not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    let from_state = row_state_summary(&group);
    let audit_failure = |error: &str| {
        audit::record_node_group(AdminNodeGroupAuditEntry {
            actor: &actor,
            action: "node-group-delete",
            node_group_id,
            node_group_name: Some(group.name.as_str()),
            from_state: Some(&from_state),
            to_state: None,
            success: false,
            error: Some(error),
        });
    };
    if group.is_default {
        audit_failure("conflict: cannot delete the default group");
        return api_error(
            StatusCode::CONFLICT,
            "cannot delete the default node group; promote another generic group \
             with :set-default first",
        )
        .into_response();
    }
    if group.nodes_count > 0 {
        audit_failure("conflict: group still has nodes");
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "node group still has nodes; relabel or retire them first",
                "nodes_count": group.nodes_count,
            })),
        )
            .into_response();
    }
    if let Err(err) = node_group_store::delete_group_in_tx(&mut tx, node_group_id).await {
        // The empty-group guard above can race a concurrent label mirror;
        // the nodes FK is the backstop.
        if violated_constraint(&err).as_deref() == Some("nodes_node_group_id_fkey") {
            audit_failure("conflict: group gained nodes during delete");
            return api_error(
                StatusCode::CONFLICT,
                "node group gained nodes while the delete was in flight; retry once \
                 the nodes are relabeled",
            )
            .into_response();
        }
        return db_error(err).into_response();
    }
    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }
    audit::record_node_group(AdminNodeGroupAuditEntry {
        actor: &actor,
        action: "node-group-delete",
        node_group_id,
        node_group_name: Some(group.name.as_str()),
        from_state: Some(&from_state),
        to_state: Some("deleted"),
        success: true,
        error: None,
    });
    Json(node_group_response(group, Vec::new())).into_response()
}

/// `PUT /admin/v1/node-groups/{id}/template-policy` — switch between the
/// `default` and `restricted` admission policies. Switching to `default`
/// clears the allowlist in the same transaction (the rows would otherwise be
/// dead configuration that silently resurrects on a later flip back).
pub async fn set_template_policy(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let node_group_id = match parse_bare_group_id(&spec) {
        Ok(id) => id,
        Err(err) => return err.into_response(),
    };
    let request: SetTemplatePolicyRequest = match parse_json_body(&body, "template-policy") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };
    let group = match node_group_store::lock_group_for_update(&mut tx, node_group_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node group not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    let from_state = row_state_summary(&group);
    if group.org_id.is_none()
        && matches!(request.template_policy, NodeGroupTemplatePolicy::Restricted)
    {
        audit::record_node_group(AdminNodeGroupAuditEntry {
            actor: &actor,
            action: "node-group-set-template-policy",
            node_group_id,
            node_group_name: Some(group.name.as_str()),
            from_state: Some(&from_state),
            to_state: None,
            success: false,
            error: Some("conflict: generic group cannot be restricted"),
        });
        return api_error(
            StatusCode::CONFLICT,
            "a generic node group must use the 'default' template policy; assign an \
             org first to restrict it",
        )
        .into_response();
    }
    if let Err(err) =
        node_group_store::set_template_policy_in_tx(&mut tx, node_group_id, request.template_policy)
            .await
    {
        return db_error(err).into_response();
    }
    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }
    let response = match fetch_group_or_error(&state.pool, node_group_id).await {
        Ok(response) => response,
        Err(response) => return *response,
    };
    audit::record_node_group(AdminNodeGroupAuditEntry {
        actor: &actor,
        action: "node-group-set-template-policy",
        node_group_id,
        node_group_name: Some(group.name.as_str()),
        from_state: Some(&from_state),
        to_state: Some(&group_state_summary(
            response.org_id,
            response.template_policy,
            response.is_default,
        )),
        success: true,
        error: None,
    });
    Json(response).into_response()
}

/// `PUT /admin/v1/node-groups/{id}/compat` — replace the `restricted`
/// allowlist. Server-side enforcement of the hard cross-org rule: another
/// org's org-specific template can never be listed, no matter what the
/// operator sends.
pub async fn set_compat(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let node_group_id = match parse_bare_group_id(&spec) {
        Ok(id) => id,
        Err(err) => return err.into_response(),
    };
    let request: SetCompatRequest = match parse_json_body(&body, "compat") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    // Dedupe (stable order) so a repeated name can't trip the primary key.
    let mut names: Vec<String> = request
        .templates
        .iter()
        .map(|name| name.as_str().to_string())
        .collect();
    names.sort();
    names.dedup();

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };
    let group = match node_group_store::lock_group_for_update(&mut tx, node_group_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node group not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    let from_state = row_state_summary(&group);
    let audit_failure = |error: &str| {
        audit::record_node_group(AdminNodeGroupAuditEntry {
            actor: &actor,
            action: "node-group-set-compat",
            node_group_id,
            node_group_name: Some(group.name.as_str()),
            from_state: Some(&from_state),
            to_state: None,
            success: false,
            error: Some(error),
        });
    };
    let Some(group_org_id) = group.org_id else {
        audit_failure("conflict: generic group has no allowlist");
        return api_error(
            StatusCode::CONFLICT,
            "a generic node group has no allowlist; the compatibility predicate \
             never consults it",
        )
        .into_response();
    };
    if !matches!(group.template_policy, NodeGroupTemplatePolicy::Restricted) {
        audit_failure("conflict: group is not restricted");
        return api_error(
            StatusCode::CONFLICT,
            "the allowlist only applies to a 'restricted' group; set \
             template-policy to 'restricted' first",
        )
        .into_response();
    }

    let known = match node_group_store::templates_by_names_in_tx(&mut tx, &names).await {
        Ok(rows) => rows,
        Err(err) => return db_error(err).into_response(),
    };
    let missing: Vec<&str> = {
        let known_names: std::collections::HashSet<&str> =
            known.iter().map(|row| row.name.as_str()).collect();
        names
            .iter()
            .map(String::as_str)
            .filter(|name| !known_names.contains(name))
            .collect()
    };
    if !missing.is_empty() {
        audit_failure("unknown templates in allowlist");
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "allowlist names templates that do not exist",
                "missing_templates": missing,
            })),
        )
            .into_response();
    }
    let cross_org: Vec<&str> = known
        .iter()
        .filter(|row| {
            matches!(row.visibility, TemplateVisibility::OrgSpecific)
                && row.org_id != Some(group_org_id)
        })
        .map(|row| row.name.as_str())
        .collect();
    if !cross_org.is_empty() {
        audit_failure("cross-org org-specific templates in allowlist");
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "allowlist names another org's org-specific templates; the \
                          cross-org rule cannot be opted into via the allowlist",
                "cross_org_templates": cross_org,
            })),
        )
            .into_response();
    }

    if let Err(err) = node_group_store::replace_compat_in_tx(&mut tx, node_group_id, &names).await {
        return db_error(err).into_response();
    }
    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }
    let response = match fetch_group_or_error(&state.pool, node_group_id).await {
        Ok(response) => response,
        Err(response) => return *response,
    };
    audit::record_node_group(AdminNodeGroupAuditEntry {
        actor: &actor,
        action: "node-group-set-compat",
        node_group_id,
        node_group_name: Some(group.name.as_str()),
        from_state: Some(&from_state),
        to_state: Some(&format!("allowlist_size={}", names.len())),
        success: true,
        error: None,
    });
    Json(response).into_response()
}

#[cfg(test)]
mod tests;
