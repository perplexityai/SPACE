//! `GET /nodes/{node_id}/holds`: the sandboxes the node-release gates count
//! against one node, why each holds it, and which gate counts it. The rows,
//! terms and reasons are `pplx_space_sandbox_store::node_holds`'s; this module
//! picks the cluster's settings and shapes the response.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_sandbox_store::node_holds::{
    self, NodeHold, NodeHoldPlacement, NodeHoldReason, NodeHoldTermCounts, NodeHoldTerms,
    NodeHolds, STUCK_TRANSIENT_AFTER,
};
use serde::Serialize;

use super::super::node_store::AdminNodeStore;
use super::super::support::{
    api_error, db_error, format_optional_time, format_time, require_managing_org_admin,
};
use super::cluster_residency_evidence;
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;

/// The most rows one response lists. The counts cover every row.
const NODE_HOLDS_LIMIT: NonZeroUsize = NonZeroUsize::new(500).expect("non-zero");

#[derive(Debug, Serialize)]
struct NodeHoldsResponse {
    node_id: NodeId,
    residency_evidence: &'static str,
    stuck_transient_after_secs: u64,
    total: u64,
    truncated: bool,
    reason_counts: BTreeMap<&'static str, u64>,
    term_counts: NodeHoldTermCounts,
    holds: Vec<NodeHoldEntry>,
}

#[derive(Debug, Serialize)]
struct NodeHoldEntry {
    sandbox_id: SandboxId,
    lifecycle: String,
    lifecycle_changed_at: String,
    placement: NodeHoldPlacement,
    reason: NodeHoldReason,
    counted_by: CountedBy,
    terms: NodeHoldTerms,
    facts: NodeHoldFactsEntry,
}

/// Which gates count the row, from its terms alone. The API does not know
/// the cluster-controller's termination-blocking gate setting, so the drain
/// is reported for both settings.
#[derive(Debug, Serialize)]
struct CountedBy {
    terminate: bool,
    drain: bool,
    drain_with_termination_blocking_gate: bool,
}

#[derive(Debug, Serialize)]
struct NodeHoldFactsEntry {
    census_absent: bool,
    durable_snapshot: bool,
    upload_in_flight: bool,
    upload_started_at: Option<String>,
    evacuation_restore_target: Option<String>,
    restore_attempt_count: Option<u32>,
    next_restore_attempt_at: Option<String>,
    past_stuck_threshold: bool,
}

fn hold_entry(hold: NodeHold) -> NodeHoldEntry {
    NodeHoldEntry {
        sandbox_id: hold.sandbox_id,
        lifecycle: hold.lifecycle,
        lifecycle_changed_at: format_time(hold.lifecycle_changed_at),
        placement: hold.placement,
        reason: hold.reason,
        counted_by: CountedBy {
            terminate: hold.terms.terminate(),
            drain: hold.terms.drain(),
            drain_with_termination_blocking_gate: hold.terms.drain_with_termination_blocking_gate(),
        },
        terms: hold.terms,
        facts: NodeHoldFactsEntry {
            census_absent: hold.facts.census_absent,
            durable_snapshot: hold.facts.durable_snapshot,
            upload_in_flight: hold.facts.upload_in_flight,
            upload_started_at: format_optional_time(hold.facts.upload_started_at),
            evacuation_restore_target: hold.facts.evacuation_restore_target,
            restore_attempt_count: hold.facts.restore_attempt_count,
            next_restore_attempt_at: format_optional_time(hold.facts.next_restore_attempt_at),
            past_stuck_threshold: hold.facts.past_stuck_threshold,
        },
    }
}

fn holds_response(
    node_id: NodeId,
    residency: ResidencyEvidence,
    holds: NodeHolds,
) -> NodeHoldsResponse {
    NodeHoldsResponse {
        node_id,
        residency_evidence: residency.as_str(),
        stuck_transient_after_secs: STUCK_TRANSIENT_AFTER.as_secs(),
        total: holds.total,
        truncated: holds.truncated,
        reason_counts: holds
            .reason_counts
            .into_iter()
            .map(|(reason, count)| (reason.as_str(), count))
            .collect(),
        term_counts: holds.term_counts,
        holds: holds.rows.into_iter().map(hold_entry).collect(),
    }
}

pub(in crate::routes::admin) async fn list_node_holds(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(node_id): Path<NodeId>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    match AdminNodeStore::new(state.pool.clone()).fetch(node_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(err) => return db_error(err).into_response(),
    }
    let residency = cluster_residency_evidence();
    match node_holds::list(&state.pool, node_id, residency, NODE_HOLDS_LIMIT).await {
        Ok(holds) => Json(holds_response(node_id, residency, holds)).into_response(),
        Err(err) => db_error(err).into_response(),
    }
}
