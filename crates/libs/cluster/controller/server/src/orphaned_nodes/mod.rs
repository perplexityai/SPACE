//! Periodic reconciler that hard-deletes `nodes` rows whose backing EC2
//! instance has been force-terminated outside the drain path.
//!
//! ## Why this exists
//!
//! `node_drain` hard-deletes a `nodes` row on `:terminate` success. Anything
//! that bypasses that path — manual `aws ec2 terminate-instances`,
//! `terminate-instance-in-auto-scaling-group` from a rollout, ASG-driven AMI
//! rollouts pre-`SPACE_NODE_DRAIN_ENABLED=true` — leaves the row behind with
//! `deleted_at IS NULL` forever. The orphans mislead operators doing pre-run
//! sanity checks, get reused via `k8s_node_name` lookup when IPs are recycled
//! (`nodes.id` is `Uuid::new_v5(k8s_node_name, role)`), and eventually slow
//! down `/admin/v1/nodes` queries.
//!
//! ## Design
//!
//! Sibling to `orphaned_sandboxes` / `node_drain` / `drained_node_terminator`,
//! shaped after `drained_node_terminator`:
//!
//! - One env-driven [`ReconcilerConfig`] with a master enable gate. Off by
//!   default everywhere; flip on per-environment once durable suspend / drain
//!   is live so we never delete a row that the drain pipeline still owns.
//! - Two pluggable seams:
//!   - [`OrphanedNodesSink`] — Postgres queries (candidate scan + conditional
//!     DELETE). Production impl in [`store`]; in-process fakes in tests.
//!   - [`InstanceLookup`] — EC2 `DescribeInstances`. Production impl in
//!     [`aws`]; in-process fakes in tests.
//! - The reconciler walks candidates (`managed_by='k8s'`, `drain_phase IS NULL`,
//!   no live sandbox placement) and for each one:
//!   1. Calls `InstanceLookup::lookup_by_private_dns` against
//!      `node_k8s.k8s_node_name`.
//!   2. **Fails closed**: anything other than an explicit `terminated` EC2
//!      state — including unknown / newly-introduced state names — counts as
//!      live. A live result skips this row.
//!   3. Calls `OrphanedNodesSink::delete_node_if_orphan` with the
//!      `(node_id, expected_k8s_node_uid)` we observed. The DELETE re-checks
//!      the UID under SQL, so a same-private-DNS ASG replacement that landed
//!      on the same `nodes.id` between candidate-scan and DELETE is detected
//!      (`node_k8s.k8s_node_uid` flips on the upsert) and the row is left alone.
//!
//! ## Lifecycle scope
//!
//! Only rows with `drain_phase IS NULL` are touched. Rows in `draining` /
//! `drained` / `terminating` belong to `node_drain` / `drained_node_terminator`
//! and have their own (live) instance to manage; deleting one out from under
//! those reconcilers would strand the drain pipeline.
//!
//! Like `node_drain`, the reconciler is gated on `SPACE_NODE_DRAIN_ENABLED` at
//! the call site so the AWS SDK init is skipped entirely on environments that
//! don't want to use this path. The master enable gate
//! `SPACE_ORPHANED_NODES_ENABLED` is the per-environment opt-in.

use pplx_space_common_core::types::NodeId;

use std::sync::Arc;
use std::time::Duration;

use pplx_space_metrics::placement_fence as placement_fence_metrics;

use tokio_util::sync::CancellationToken;

use crate::env_config;

#[cfg(feature = "kubernetes")]
pub mod aws;
pub mod store;

#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use aws::Ec2InstanceLookup;
pub use store::NodeDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_MAX_PER_TICK: u32 = 25;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default so the reconciler is inert until
    /// the operator explicitly opts in via
    /// `SPACE_ORPHANED_NODES_ENABLED=true`. Same shape as
    /// `drained_node_terminator` / `node_drain`.
    pub enabled: bool,
    pub tick: Duration,
    /// Hard cap on how many DELETEs a single tick may issue. Bounds blast
    /// radius if an EC2 lookup outage suddenly returns "no live instance"
    /// for everything and the fail-closed contract somehow regressed.
    pub max_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            max_per_tick: DEFAULT_MAX_PER_TICK,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.orphanedNodes.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config.truthy_default("SPACE_ORPHANED_NODES_ENABLED", default.enabled),
            tick: default.tick,
            max_per_tick: default.max_per_tick,
        }
    }
}

/// Candidate orphan row surfaced by the sink scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanCandidate {
    pub node_id: NodeId,
    /// `node_k8s.k8s_node_name`, which is the EC2 `PrivateDnsName` on EKS.
    /// `None` here means the row has no kube registration and we can't
    /// verify with EC2 — the sink filters those out, so production code
    /// always sees `Some(_)`.
    pub k8s_node_name: String,
    /// `node_k8s.k8s_node_uid` at scan time. Passed into the DELETE so the
    /// SQL guard refuses to delete if a replacement kube Node bumped the UID
    /// between scan and delete.
    pub k8s_node_uid: Option<String>,
}

/// EC2 verdict for a single private DNS name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceLookupOutcome {
    /// No matching instance, or every match is in EC2 state `terminated`.
    /// Safe to delete the row.
    Gone,
    /// At least one matching instance is in a non-`terminated` state
    /// (including unknown / newly-introduced state names — we fail closed).
    Live { instance_id: String, state: String },
}

#[derive(Debug, thiserror::Error)]
pub enum InstanceLookupError {
    #[error("ec2 describe-instances failed: {0}")]
    Backend(String),
}

/// Postgres-side surface. Production impl: [`NodeDbStore`].
pub trait OrphanedNodesSink: Send + Sync + 'static {
    /// Return up to `limit` rows that look like orphan candidates:
    /// `managed_by='k8s'`, `drain_phase IS NULL`, joined `node_k8s` row
    /// with a non-NULL `k8s_node_name`, and no sandbox in a non-terminal
    /// lifecycle pointing at this node via `active_node_id` or
    /// `target_node_id`. Filtered to rows with `nodes.id > after` so the
    /// reconciler can cursor through the full candidate set across ticks
    /// without re-checking the same prefix each tick (which would
    /// permanently starve later candidates whenever the prefix held more
    /// `Live` rows than `max_per_tick`).  Ordered by `nodes.id` ASC.
    fn list_candidates(
        &self,
        limit: i64,
        after: Option<NodeId>,
    ) -> impl std::future::Future<Output = Result<Vec<OrphanCandidate>, sqlx::Error>> + Send;

    /// Conditional DELETE. Returns `Ok(true)` if the row was deleted.
    /// Returns `Ok(false)` (without error) if any of these atomic guards
    /// failed:
    /// - the row no longer exists,
    /// - `nodes.drain_phase` is no longer NULL (someone started a drain),
    /// - `node_k8s.k8s_node_uid` differs from `expected_uid` (same-DNS
    ///   ASG replacement landed on the same `nodes.id`),
    /// - a sandbox in a non-terminal lifecycle has been pinned to the row
    ///   since the scan.
    fn delete_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;
}

/// EC2 lookup surface. Production impl: [`Ec2InstanceLookup`].
pub trait InstanceLookup: Send + Sync + 'static {
    fn lookup_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> impl std::future::Future<Output = Result<InstanceLookupOutcome, InstanceLookupError>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: usize,
    pub deleted: usize,
    /// Candidates whose EC2 lookup returned `Live` (any non-`terminated`
    /// state, including unknown / newly introduced ones).
    pub skipped_live: usize,
    /// Candidates whose DELETE returned `rows_affected = 0` (UID changed,
    /// drain phase flipped, sandbox got pinned to the row, etc.). The next
    /// tick will re-scan; the row may or may not still be a candidate.
    pub skipped_drift: usize,
    /// Candidates that the per-tick cap rate-limited; the next tick picks
    /// them up.
    pub rate_limited: usize,
    /// EC2 lookup errors. Surface in metrics but don't bail the whole
    /// tick — one row's failure shouldn't block the rest.
    pub lookup_errors: usize,
    /// DB DELETE errors. Same shape as `lookup_errors`.
    pub delete_errors: usize,
    /// Candidates whose DELETE gave up waiting for the `nodes` row lock and
    /// deferred to the next tick. Separate from [`Self::delete_errors`] because
    /// nothing failed: a conflicting nodes-row writer held the row. Placement
    /// fences, registration/heartbeat, and inventory upserts can all be that
    /// holder. A sustained nonzero value means the reap is contending with one
    /// of those writers, not that either operation is broken.
    pub contention_defers: usize,
}

pub struct Reconciler<S: OrphanedNodesSink, L: InstanceLookup> {
    config: ReconcilerConfig,
    sink: Arc<S>,
    lookup: Arc<L>,
    /// Cursor for the rolling sweep. Advances to the last-scanned `nodes.id`
    /// at the end of every tick; resets to `None` when a page returns fewer
    /// than `limit` rows (signaling we've reached the end of the candidate
    /// set). Without this cursor, a `max_per_tick` smaller than the number
    /// of live K8s nodes at the front of the ascending-`id` order would
    /// permanently starve later orphan candidates — every tick would scan
    /// the same prefix and classify them all as `Live`, never reaching the
    /// actual orphans.
    scan_after: Option<NodeId>,
}

impl<S: OrphanedNodesSink, L: InstanceLookup> Reconciler<S, L> {
    pub fn new(config: ReconcilerConfig, sink: S, lookup: L) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
            lookup: Arc::new(lookup),
            scan_after: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let limit = i64::from(self.config.max_per_tick.saturating_mul(2));
        let candidates = match self.sink.list_candidates(limit, self.scan_after).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "orphaned_nodes: list_candidates failed");
                return report;
            }
        };
        report.scanned = candidates.len();
        let cap = usize::try_from(self.config.max_per_tick).unwrap_or(usize::MAX);

        // Advance the cursor *before* processing so even a tick that
        // panics or rate-limits its way out makes forward progress. The
        // sweep wraps to the beginning when `list_candidates` returns
        // fewer than `limit` rows.
        let next_cursor = if (candidates.len() as i64) < limit {
            None
        } else {
            candidates.last().map(|c| c.node_id)
        };
        self.scan_after = next_cursor;

        for candidate in candidates {
            if report.deleted >= cap {
                report.rate_limited += 1;
                continue;
            }

            let verdict = match self
                .lookup
                .lookup_by_private_dns(candidate.k8s_node_name.clone())
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        error = %err,
                        "orphaned_nodes: ec2 lookup failed; will retry next tick"
                    );
                    report.lookup_errors += 1;
                    continue;
                }
            };
            if let InstanceLookupOutcome::Live { instance_id, state } = verdict {
                tracing::info!(
                    node_id = %candidate.node_id,
                    k8s_node_name = %candidate.k8s_node_name,
                    instance_id,
                    state,
                    "orphaned_nodes: ec2 lookup returned live instance; skipping"
                );
                report.skipped_live += 1;
                continue;
            }

            match self
                .sink
                .delete_node_if_orphan(candidate.node_id, candidate.k8s_node_uid.clone())
                .await
            {
                Ok(true) => {
                    tracing::info!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        "orphaned_nodes: deleted orphan row"
                    );
                    report.deleted += 1;
                }
                Ok(false) => {
                    tracing::info!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        "orphaned_nodes: DELETE refused (uid changed, drain started, or sandbox pinned)"
                    );
                    report.skipped_drift += 1;
                }
                Err(err)
                    if pplx_space_common_svc::sql::node_placement_fence::is_lock_timeout(&err) =>
                {
                    // The DELETE's bounded `nodes`-row lock wait fired, not a
                    // database problem: a conflicting nodes-row writer holds the
                    // row (placement fence, registration/heartbeat, inventory,
                    // etc.). Debug rather than warn, and *not* counted as a delete
                    // error — nothing failed to delete, and a node whose row is
                    // held by a long writer would otherwise emit one WARN per tick
                    // indefinitely, reading identically to an outage. The next
                    // tick re-lists the candidate.
                    tracing::debug!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        "orphaned_nodes: DELETE deferred; nodes row contended"
                    );
                    placement_fence_metrics::termination_contention_deferral();
                    report.contention_defers += 1;
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        error = %err,
                        "orphaned_nodes: DELETE failed; will retry next tick"
                    );
                    report.delete_errors += 1;
                }
            }
        }

        report
    }
}

/// Drive the reconciler loop. Returns when `shutdown_token` fires.
///
/// When `config.enabled = false` (the default), the function logs the inert
/// state once and then waits on the shutdown token without ever running a
/// tick — same shape as `drained_node_terminator::run`.
pub async fn run<S: OrphanedNodesSink, L: InstanceLookup>(
    config: ReconcilerConfig,
    sink: S,
    lookup: L,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "orphaned-nodes reconciler is disabled \
             (SPACE_ORPHANED_NODES_ENABLED unset); \
             waiting on shutdown without running any ticks"
        );
        shutdown_token.cancelled().await;
        tracing::info!("orphaned-nodes reconciler stopping");
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        max_per_tick = config.max_per_tick,
        "starting orphaned-nodes reconciler"
    );
    let mut reconciler = Reconciler::new(config.clone(), sink, lookup);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick without processing. Gives a fresh
    // deploy / env-var flip a full window to let kube node-sync settle so
    // we don't race the inventory reflector during startup.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let _ = reconciler.tick().await;
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    tracing::info!("orphaned-nodes reconciler stopping");
}
