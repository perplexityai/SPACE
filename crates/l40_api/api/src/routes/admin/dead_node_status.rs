//! `GET /dead-node-status`: the cluster controller's dead-node verb's last
//! result for each node it judged, read from `nodes.dead_node_status`.
//!
//! The controller runs the verb and writes the values; this surface only reads
//! them, so the response is empty until `SPACE_DEAD_NODE_MODE` is `dry-run` or
//! `live` and the verb has judged a node. An entry stays until its node is
//! deleted, whether or not the controller still judges the node.

use pplx_space_common_core::types::{NodeId, SandboxId};

use std::collections::BTreeMap;

use axum::{Extension, Json, extract::State, http::StatusCode, response::IntoResponse};
use pplx_space_sandbox_store::dead_node::{self, DeadNodeStatus};
use serde::Serialize;
use time::OffsetDateTime;

use crate::middleware::auth::AuthUser;
use crate::routes::AppState;
use crate::routes::admin::support::{self, ContentionScope, require_managing_org_admin};

/// More nodes than any cluster judges at once; one value per judged node.
const MAX_STATUSES: u32 = 1000;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeadNodeStatusResponse {
    pub nodes: Vec<DeadNodeStatusEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeadNodeStatusEntry {
    pub node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    /// `dry-run` or `live`.
    pub mode: String,
    /// `lost` or `unknown`.
    pub verdict: String,
    /// The condition the verdict rests on; for `unknown`, the first that
    /// failed.
    pub reason: String,
    /// The controller's per-node row cap cut the read short, so `counts` is
    /// a lower bound.
    pub rows_truncated: bool,
    /// Each instance asked about: `dead`, `live`, or `unknown:<reason>`.
    pub instances: BTreeMap<String, String>,
    /// Rows per outcome. An outcome with no rows is absent.
    pub counts: BTreeMap<String, u64>,
    /// Rows per outcome whose write committed; empty in a dry run.
    pub applied: BTreeMap<String, u64>,
    /// The first rows of each outcome.
    pub samples: BTreeMap<String, Vec<DeadNodeStatusRow>>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeadNodeStatusRow {
    pub sandbox_id: SandboxId,
    pub lifecycle: String,
}

impl From<DeadNodeStatus> for DeadNodeStatusEntry {
    fn from(status: DeadNodeStatus) -> Self {
        Self {
            node_id: status.node_id,
            observed_at: status.observed_at,
            mode: status.mode,
            verdict: status.verdict,
            reason: status.reason,
            rows_truncated: status.rows_truncated,
            instances: status.instances,
            counts: status.counts,
            applied: status.applied,
            samples: status
                .samples
                .into_iter()
                .map(|(outcome, rows)| {
                    let rows = rows
                        .into_iter()
                        .map(|row| DeadNodeStatusRow {
                            sandbox_id: row.sandbox_id,
                            lifecycle: row.lifecycle,
                        })
                        .collect();
                    (outcome, rows)
                })
                .collect(),
        }
    }
}

pub async fn list_dead_node_status(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    match dead_node::statuses(&state.pool, MAX_STATUSES).await {
        Ok(statuses) => Json(DeadNodeStatusResponse {
            nodes: statuses
                .into_iter()
                .map(DeadNodeStatusEntry::from)
                .collect(),
        })
        .into_response(),
        Err(error) => match support::contention_disposition(ContentionScope::ReadOnly, &error) {
            Some((status, message)) => api_error(status, message).into_response(),
            None => {
                tracing::warn!(%error, "failed to load dead-node status");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to load dead-node status",
                )
                .into_response()
            }
        },
    }
}

fn api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": message.into()})))
}

#[cfg(test)]
mod tests;
