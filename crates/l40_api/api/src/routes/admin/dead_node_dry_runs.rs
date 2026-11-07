//! `GET /dead-node-dry-runs`: the cluster controller's last dead-node dry run
//! for each node it judged, read from `dead_node_dry_runs`.
//!
//! The controller runs the verb and writes the rows; this surface only reads
//! them, so the response is empty until `SPACE_DEAD_NODE_MODE=dry-run` has
//! judged a node. An entry stays until its node is deleted, whether or not the
//! controller still judges the node.

use pplx_space_common_core::types::{NodeId, SandboxId};

use std::collections::BTreeMap;

use axum::{Extension, Json, extract::State, http::StatusCode, response::IntoResponse};
use pplx_space_sandbox_store::dead_node::{self, DeadNodeReport};
use serde::Serialize;
use time::OffsetDateTime;

use crate::middleware::auth::AuthUser;
use crate::routes::AppState;
use crate::routes::admin::support::{self, ContentionScope, require_managing_org_admin};

/// More nodes than any cluster judges at once; the table holds one row per
/// judged node.
const MAX_REPORTS: u32 = 1000;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeadNodeDryRunsResponse {
    pub dry_runs: Vec<DeadNodeDryRun>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeadNodeDryRun {
    pub node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    /// `lost` or `unknown`.
    pub verdict: String,
    /// The condition the verdict rests on; for `unknown`, the first that
    /// failed.
    pub reason: String,
    /// The controller's per-node row cap cut the read short, so `counts` is
    /// a lower bound.
    pub rows_truncated: bool,
    /// Rows per outcome. An outcome with no rows is absent.
    pub counts: BTreeMap<String, u64>,
    /// The first rows of each outcome.
    pub samples: BTreeMap<String, Vec<DeadNodeDryRunRow>>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeadNodeDryRunRow {
    pub sandbox_id: SandboxId,
    pub lifecycle: String,
}

impl From<DeadNodeReport> for DeadNodeDryRun {
    fn from(report: DeadNodeReport) -> Self {
        Self {
            node_id: report.node_id,
            observed_at: report.observed_at,
            verdict: report.verdict,
            reason: report.reason,
            rows_truncated: report.rows_truncated,
            counts: report.counts,
            samples: report
                .samples
                .into_iter()
                .map(|(outcome, rows)| {
                    let rows = rows
                        .into_iter()
                        .map(|row| DeadNodeDryRunRow {
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

pub async fn list_dead_node_dry_runs(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    match dead_node::reports(&state.pool, MAX_REPORTS).await {
        Ok(reports) => Json(DeadNodeDryRunsResponse {
            dry_runs: reports.into_iter().map(DeadNodeDryRun::from).collect(),
        })
        .into_response(),
        Err(error) => match support::contention_disposition(ContentionScope::ReadOnly, &error) {
            Some((status, message)) => api_error(status, message).into_response(),
            None => {
                tracing::warn!(%error, "failed to load dead-node dry runs");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to load dead-node dry runs",
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
