//! `GET /runtime-environments/nodes`: per-node published execution policy and
//! conformance against the cluster desired policy.

use std::collections::BTreeSet;

use axum::{
    Extension, Json,
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use pplx_space_accounts::NodeId;
use pplx_space_admin_protocol::ListAdminNodesFilters;
use pplx_space_execution_spec::{EnvironmentReference, ExecutionPolicyObservation};
use pplx_space_node_model::NodeRole;
use pplx_space_runtime_environment_policy_store::{self as policy_store, RuntimeEnvironmentPolicy};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::policy_read_error;
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;
use crate::routes::admin::node_store::{self, AdminNodeRow, AdminNodeStore};
use crate::routes::admin::support::{format_time, read_db_error, require_managing_org_admin};

/// Rows per `GET /runtime-environments/nodes` page, matching
/// `GET /nodes/{node_id}/sandboxes`.
pub(super) const NODE_PAGE_SIZE: usize = 100;

/// How a node's published execution policy compares with the cluster
/// policy. This compares content, not generations: a node that conforms
/// enforces what the current policy asks for, whichever write it applied, and
/// a node on its startup policy, which publishes no generation, is judged the
/// same way.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "conformance", rename_all = "snake_case")]
pub(super) enum NodePolicyConformance {
    /// The node hasn't published a readable policy, or reports it couldn't
    /// read the cluster policy it persisted (and so refuses everything).
    Unknown,
    Conforms,
    Diverges {
        /// The node resolves unbound work to a different default.
        default_differs: bool,
        /// The node executes environments the cluster policy doesn't, for
        /// example one that was blocked.
        executes_outside_policy: Vec<EnvironmentReference>,
        /// The node allows unbound legacy work the cluster revoked.
        allows_revoked_legacy_unbound: bool,
    },
}

/// A node narrows to the cluster's executing set, never widens past what its
/// build certifies, so its allowlist may be a strict subset. Until a default
/// is set the cluster asks for nothing, and every published policy conforms
/// on the default and allowlist. The rules nodes apply are in
/// `docs/execution-environments.md`, "Cluster desired policy".
pub(super) fn conformance(
    policy: &RuntimeEnvironmentPolicy,
    observed: Option<&ExecutionPolicyObservation>,
) -> NodePolicyConformance {
    // A node that couldn't read the cluster policy it persisted refuses
    // everything; that's no evidence of which policy it would enforce.
    let Some(observed) = observed.filter(|observed| !observed.cluster_policy_unreadable()) else {
        return NodePolicyConformance::Unknown;
    };
    let (default_differs, executes_outside_policy) = match policy.default_environment {
        None => (false, Vec::new()),
        Some(default) => {
            let executing: BTreeSet<_> = policy.executing().collect();
            (
                observed.default_environment_reference() != Some(default),
                observed
                    .allowed_environment_references()
                    .iter()
                    .copied()
                    .filter(|environment| !executing.contains(environment))
                    .collect(),
            )
        }
    };
    // The cluster can revoke the legacy permission, never grant it, so a
    // node that refuses legacy work the cluster permits is only stricter.
    let allows_revoked_legacy_unbound =
        policy.legacy_unbound_allowed == Some(false) && observed.legacy_unbound_allowed();
    if !default_differs && executes_outside_policy.is_empty() && !allows_revoked_legacy_unbound {
        return NodePolicyConformance::Conforms;
    }
    NodePolicyConformance::Diverges {
        default_differs,
        executes_outside_policy,
        allows_revoked_legacy_unbound,
    }
}

#[derive(Debug, Serialize)]
pub(super) struct NodeRuntimePolicyStatus {
    pub(super) node_id: NodeId,
    pub(super) name: Option<String>,
    pub(super) role: NodeRole,
    pub(super) substrate_node_id: Option<String>,
    /// Whether the node's status snapshot is within its `stale_after`.
    /// `null` when the node has never reported status. A stale node's
    /// conformance describes what it last published, not what it runs now.
    pub(super) status_fresh: Option<bool>,
    /// Control-plane receipt time of the published policy.
    pub(super) policy_received_at: Option<String>,
    /// The policy the node reports enforcing.
    pub(super) enforced: Option<ExecutionPolicyObservation>,
    /// The cluster policy generation the node reports applying. `null` for a
    /// node enforcing its startup policy only (an older build, or the cluster
    /// serving no policy), or one that couldn't read the policy it persisted.
    pub(super) applied_generation: Option<u64>,
    /// Whether `applied_generation` is the generation compared with. A node
    /// behind can still conform, when nothing it would change has changed.
    pub(super) applied_current: Option<bool>,
    #[serde(flatten)]
    pub(super) conformance: NodePolicyConformance,
}

#[derive(Debug, Serialize)]
struct NodeRuntimePolicyPage {
    /// The cluster policy each node was compared with.
    generation: u64,
    default_environment_ref: Option<EnvironmentReference>,
    nodes: Vec<NodeRuntimePolicyStatus>,
    /// Pass as `after` for the next page; `null` on the last page.
    next_after: Option<NodeId>,
}

#[derive(Debug, Deserialize)]
pub(in crate::routes::admin) struct NodeRuntimePolicyQuery {
    pub(super) after: Option<NodeId>,
}

/// A snapshot is stale from `stale_after` on, as the node listing's `stale`
/// flag counts it.
pub(super) fn status_fresh(stale_after: OffsetDateTime, now: OffsetDateTime) -> bool {
    stale_after > now
}

pub(super) fn node_status(
    policy: &RuntimeEnvironmentPolicy,
    row: AdminNodeRow,
    now: OffsetDateTime,
) -> NodeRuntimePolicyStatus {
    let enforced = node_store::execution_policy(&row);
    let applied_generation = enforced
        .as_ref()
        .and_then(|(policy, _)| policy.applied_generation());
    NodeRuntimePolicyStatus {
        node_id: row.id,
        name: row.name,
        role: row.role,
        substrate_node_id: row.substrate_node_id,
        status_fresh: row
            .status_stale_after
            .map(|stale_after| status_fresh(stale_after, now)),
        policy_received_at: enforced
            .as_ref()
            .and_then(|(_, received_at)| *received_at)
            .map(format_time),
        conformance: conformance(policy, enforced.as_ref().map(|(policy, _)| policy)),
        enforced: enforced.map(|(policy, _)| policy),
        applied_generation,
        applied_current: applied_generation.map(|applied| applied == policy.generation),
    }
}

/// The page of `rows` after `after`, in node id order, and the cursor for
/// the next page when one follows.
pub(super) fn node_page(
    mut rows: Vec<AdminNodeRow>,
    after: Option<NodeId>,
) -> (Vec<AdminNodeRow>, Option<NodeId>) {
    rows.sort_unstable_by_key(|row| row.id);
    rows.retain(|row| after.is_none_or(|after| row.id > after));
    let next_after = if rows.len() > NODE_PAGE_SIZE {
        rows.truncate(NODE_PAGE_SIZE);
        rows.last().map(|row| row.id)
    } else {
        None
    };
    (rows, next_after)
}

/// `GET /runtime-environments/nodes`: each node's published execution
/// policy and how it compares with the cluster policy, a page at a time in
/// node id order.
///
/// Reads the admin node listing, which already joins the Direct-only node
/// table for names and soft deletion, and pages it here: shared code reads
/// that table only through an existing query (`controllers/substrate/AGENTS.md`).
/// That costs one full node listing per page, the same read `list-nodes`
/// makes without occupancy counts.
pub(in crate::routes::admin) async fn list_runtime_environment_nodes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<NodeRuntimePolicyQuery>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let policy = match policy_store::get(&state.pool).await {
        Ok(policy) => policy,
        Err(error) => return policy_read_error(error),
    };
    let rows = match AdminNodeStore::new(state.pool.clone())
        .list(&ListAdminNodesFilters::default())
        .await
    {
        Ok(rows) => rows,
        Err(err) => return read_db_error(err),
    };
    let (rows, next_after) = node_page(rows, query.after);
    let now = OffsetDateTime::now_utc();
    Json(NodeRuntimePolicyPage {
        generation: policy.generation,
        default_environment_ref: policy.default_environment,
        nodes: rows
            .into_iter()
            .map(|row| node_status(&policy, row, now))
            .collect(),
        next_after,
    })
    .into_response()
}
