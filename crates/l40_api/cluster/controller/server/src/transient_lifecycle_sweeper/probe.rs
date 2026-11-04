//! Per-node liveness probing for the verify-before-destroy sweep.
//!
//! Candidates are grouped by the node their active execution row points at
//! and each node is asked once (`WorkerApi/SandboxState`, batched ids) what
//! it knows about them. The fan-out is sequential per node on purpose: this
//! is a 60s-tick background loop with a small wedged-tail population, and
//! sequential probing bounds concurrency without a knob. Duration is bounded
//! separately by the caller-supplied `budget`: each unreachable node costs up
//! to ~2x the probe timeout, and the sweeper's target scenario is a
//! cluster-wide incident with many unreachable node-apis — exactly when the
//! candidate set is large — so without a budget one tick could stretch to
//! tens of minutes and act on verdicts gathered long before. Nodes not
//! probed within the budget have their candidates downgraded to
//! [`ProbeVerdict::Inconclusive`] (defer) and roll to the next tick; the
//! candidate select's `ORDER BY random()` keeps that rollover fair.

use pplx_space_common_core::types::SandboxId;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::{RetireFence, SubstrateError};
use pplx_space_common_core::types::SandboxLifecycle;

use super::SweepSink;
use super::classify::{ProbeObservation, ProbeVerdict};

/// One aged transient row listed by the candidate select — *without* any
/// lifecycle flip having happened yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepCandidate {
    pub sandbox_id: SandboxId,
    /// The transient lifecycle the row is stuck in (re-checked by every
    /// action CAS).
    pub lifecycle: SandboxLifecycle,
    /// How long the row has sat in `lifecycle` (from `lifecycle_changed_at`).
    pub age: Duration,
    /// Node-drain's persisted wake target, when set. Candidates carry it only
    /// via the `suspending` exemption path — marked `restoring` rows are
    /// exempt from the sweep entirely.
    pub evacuation_restore_target: Option<SandboxLifecycle>,
    /// Whether a `state='ready'` durable snapshot exists for the row, under
    /// per-lifecycle semantics (`store::ready_snapshot_predicate`):
    /// pre-restore source-window for `restoring` (where the snapshot is
    /// recoverable and may drive a guarded revert), placement-freshness for
    /// everything else (diagnostic only — the decision table never reverts
    /// those, because their matching backup is a stale prior-cycle copy).
    /// Evaluated in the candidate select; the revert CAS re-checks a stricter
    /// suspended-revert-safe predicate.
    pub ready_snapshot: bool,
    /// Whether the restore lease belongs to a sandbox-fork operation. Fork
    /// targets restore another sandbox's artifact and therefore cannot have a
    /// target-owned ready snapshot while restoring.
    pub fork_restore: bool,
    /// `k8s_node_name` from the row's active `sandbox_k8s_executions` row.
    /// `None` when the row has no active execution or its pod never reached a
    /// node — there is no node to probe, which classifies as
    /// [`ProbeVerdict::MissingOnNode`].
    pub node_name: Option<String>,
    /// Whether the row's active execution is routable per the DB — an active
    /// `sandbox_k8s_executions` row with `state = 'running'` and a non-NULL
    /// `pod_ip` — or the row's substrate has no routability contract
    /// (`managed_by <> 'k8s'`). In the Kubernetes substrate, `started` is a
    /// promise that such an execution exists; the converge arm must not
    /// assert `started` without it (the wake path's fresh-pod refusal wedges
    /// a `started` gen≥2 row that lacks restore context).
    pub routable_execution: bool,
    /// Whether the active execution's activation-attempt lease ended a full
    /// age window ago, whether it expired or its owner gave it up: the request
    /// that claimed the execution is gone and nothing else will finish its
    /// activation. The candidate select never lists an activation-owned row
    /// ([`super::is_activation_owned`]) whose lease ended more recently than
    /// that, and `classify::decide` reads this only for an alive `creating`
    /// row, where it tells a gone owner from an execution with no lease
    /// left (one that never took one, or whose activation finished). The
    /// fail, converge and revert CASes re-check the owner.
    pub activation_abandoned: bool,
    /// Whether this is a `restoring` row the restoring-episode deadline
    /// ([`super::ReconcilerConfig::restoring_episode_deadline`]) let through:
    /// its episode is overdue with no pod IP on its active execution, and its
    /// restore lease or activation attempt would otherwise have spared it.
    /// Only the revert honors that; the converge and fail CASes keep their
    /// owner guards. It labels the revert's outcome and reads nothing else.
    pub episode_overdue: bool,
    /// Whether the active execution holds an activation attempt. Only a held
    /// attempt can be completed by the node's activation-completion receipt,
    /// which matches on the attempt id; an owner that gave its activation up
    /// clears the attempt and keeps only the expiry, so an abandoned row
    /// without one has nothing left that could finish it.
    pub activation_attempt_held: bool,
    /// What the candidate select observed of the active execution, passed back
    /// to the retire so it acts only on what this sweep saw — including when
    /// what it saw was *no* execution, which the retire must re-prove rather
    /// than read as "nothing to protect".
    pub pod_fence: RetireFence,
}

/// Why one per-node `SandboxState` probe produced no answer. Any of these
/// downgrade every candidate on that node to
/// [`ProbeVerdict::Inconclusive`] — unreachable means defer, never destroy.
#[derive(Debug, thiserror::Error)]
pub enum ProbeNodeError {
    #[error("node-api probe failed: {0}")]
    NodeApi(#[from] SubstrateError),
    #[error("node-api probe timed out after {}s", .0.as_secs())]
    Timeout(Duration),
}

/// Group `targets` by node, probe each node once, and return a per-sandbox
/// verdict map covering **every** input id:
///
/// - no node to probe -> [`ProbeVerdict::MissingOnNode`]
/// - probe error/timeout -> [`ProbeVerdict::Inconclusive`] for that node's ids
/// - id not covered by the node's answer -> [`ProbeVerdict::Inconclusive`]
/// - node not probed because `budget` ran out -> [`ProbeVerdict::Inconclusive`]
///
/// The `budget` bounds the whole fan-out: once it elapses, no further node
/// is probed and the remaining candidates defer to the next tick. This also
/// bounds how stale a verdict can be by the time the caller acts on it.
pub(super) async fn probe_targets<S: SweepSink>(
    sink: &S,
    targets: &[(SandboxId, Option<String>)],
    probe_timeout: Duration,
    budget: Duration,
) -> HashMap<SandboxId, ProbeObservation> {
    let mut observations = HashMap::with_capacity(targets.len());
    let mut by_node: HashMap<&str, Vec<SandboxId>> = HashMap::new();
    for (sandbox_id, node_name) in targets {
        match node_name.as_deref() {
            Some(node_name) => by_node.entry(node_name).or_default().push(*sandbox_id),
            None => {
                observations.insert(
                    *sandbox_id,
                    ProbeObservation::from_verdict(ProbeVerdict::MissingOnNode),
                );
            }
        }
    }

    let started = Instant::now();
    let total_nodes = by_node.len();
    let mut budget_exhausted = false;
    for (probed_nodes, (node_name, sandbox_ids)) in by_node.into_iter().enumerate() {
        if !budget_exhausted && started.elapsed() >= budget {
            budget_exhausted = true;
            tracing::warn!(
                probed_nodes,
                deferred_nodes = total_nodes - probed_nodes,
                budget_secs = budget.as_secs(),
                "transient_lifecycle_sweeper: probe fan-out hit its tick budget; \
                 candidates on unprobed nodes defer to the next tick"
            );
        }
        if budget_exhausted {
            for sandbox_id in sandbox_ids {
                observations.insert(sandbox_id, ProbeObservation::inconclusive());
            }
            continue;
        }
        match sink
            .probe_node(node_name, &sandbox_ids, probe_timeout)
            .await
        {
            Ok(observed) => {
                for sandbox_id in sandbox_ids {
                    observations.insert(
                        sandbox_id,
                        observed
                            .get(&sandbox_id)
                            .copied()
                            .unwrap_or_else(ProbeObservation::inconclusive),
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    node_name,
                    sandboxes = sandbox_ids.len(),
                    error = %err,
                    "transient_lifecycle_sweeper: node probe failed; treating its \
                     candidates as inconclusive (deferred to next tick)"
                );
                for sandbox_id in sandbox_ids {
                    observations.insert(sandbox_id, ProbeObservation::inconclusive());
                }
            }
        }
    }

    observations
}
