//! Metric helpers for the cluster-controller's node-drain reconciler.
//!
//! Names match the `node_drain.*` namespace.

use std::time::Instant;

use pplx_space_common_core::types::NodeDrainMode;

use crate::emit::{counter, elapsed_ms, gauge, tag, timing_ms};

/// Transition between two `nodes.drain_phase` values (driven by the reconciler).
pub fn transition(from: &str, to: &str) {
    counter(
        "node_drain.transitions",
        1,
        &[tag("from", from), tag("to", to)],
    );
}

/// A draining node finished draining: [`transition`] from `draining` to
/// `drained`, tagged with the drain's `mode` (`forced` evacuates residents,
/// `gentle` waits for them to leave), which only this transition knows.
pub fn drained(from: &str, to: &str, mode: NodeDrainMode) {
    counter(
        "node_drain.transitions",
        1,
        &[tag("from", from), tag("to", to), tag("mode", mode.as_str())],
    );
}

/// Per-state node count. Emit one series per known state per tick so a
/// `state` tag never stops being reported just because it momentarily has
/// zero nodes.
pub fn nodes_by_state(state: &str, count: u64) {
    gauge(
        "node_drain.nodes_by_state",
        count as f64,
        &[tag("state", state)],
    );
}

/// Nodes currently draining in gentle mode — cordoned, residents left alone,
/// waiting for them to leave. Emitted every tick including zero, so an alert can
/// evaluate it directly without the series going stale.
///
/// This is the steady-state signal for a gentle drain. There is deliberately no
/// per-tick log line for a waiting node: a gentle drain is expected to last
/// hours or days, and one info line per node per 5s tick would be ~17k
/// lines/node/day of "still waiting" that buries the reconciler's real warnings.
/// Pair this with the per-node `node_drain.sandboxes_remaining` to see which
/// nodes are parked and how far from converging they are.
pub fn gentle_nodes(count: u64) {
    gauge("node_drain.gentle_nodes", count as f64, &[]);
}

/// A gentle drain that passed its escalation deadline and was rewritten to
/// `forced`. One per node per escalation. Once-per-drain, but not because the
/// UPDATE matches on mode: it carries no `drain_mode` predicate. The
/// gentle/NULL/unrecognized classification lives only in the Rust caller
/// (`from_column_str`), which stops offering a node as a candidate once the
/// escalation writes `forced`, so it is never escalated a second time.
///
/// Every increment means resident sandboxes on that node are about to be
/// evacuated and their user calls about to 409, i.e. the disruption the
/// operator declined by taking the default. Worth alerting on during a shrink
/// campaign.
pub fn gentle_escalation(node_id: &str) {
    counter("node_drain.gentle_escalations", 1, &[tag("node", node_id)]);
}

/// Sandboxes still resident on a draining node (work queue length).
pub fn sandboxes_remaining(node_id: &str, count: u64) {
    gauge(
        "node_drain.sandboxes_remaining",
        count as f64,
        &[tag("node", node_id)],
    );
}

/// Rows pinned to the node by `active_node_id` that the node's own published
/// census proves it no longer holds, and which the drain therefore does not
/// count. Emitted on every resident scan, zero included, so the series can be
/// read alongside `sandboxes_remaining`: a drop here with no matching rise
/// there means the node stopped answering and every row is counting again.
pub fn residents_discounted(node_id: &str, count: u64) {
    gauge(
        "node_drain.residents_discounted",
        count as f64,
        &[tag("node", node_id)],
    );
}

/// Rows the `draining -> drained` gate counts against this node.
///
/// The gate's third input, alongside the resident count and the in-flight count.
/// It is published because it does not decompose into the other per-node gauges:
/// it counts rows that are neither in a resident lifecycle nor in the pinned
/// local-only list, so a node can sit held with `sandboxes_remaining` and
/// `pinned_suspended` both at zero. The canonical case is a suspended
/// rollout-skew row with a NULL `current_local_artifact_id`, which a gentle
/// drain excludes from the pinned list to keep it off the legacy reupload route.
///
/// Emitted on every resident scan, zero included, so an alert can read it
/// directly. A node whose `node_drain.gentle_nodes` membership persists while
/// every other per-node gauge reads zero is explained by this one.
pub fn drain_blockers(node_id: &str, count: u64) {
    gauge(
        "node_drain.drain_blockers",
        count as f64,
        &[tag("node", node_id)],
    );
}
/// Sandboxes that the drained-node terminator's resident guard would refuse a
/// draining node for, holding it in `draining` instead of letting it advance to
/// `drained`.
///
/// Emitted once per scan **that reaches the gate**, as the count only when
/// nothing else is holding the node — zero otherwise, because the reconciler
/// skips the scan when a cheaper term already blocks the drain. So nonzero means
/// "this guard is the sole remaining blocker".
///
/// Three gaps to know before building a monitor on it:
///
/// * A tick that fails an earlier gate query returns before this is emitted, so
///   a DB blip stops the series rather than zeroing it.
/// * On the tick where the atomic residency guard in `set_drain_phase` actually
///   fires, this has **already** published zero — the pre-check saw nothing,
///   which is exactly why that guard exists — so the one outcome it catches reads
///   here as "nothing blocking" until the next tick corrects it.
/// * [`clear_node_gauges`] only runs on terminate/delete success, so an admin
///   undrain of a held node stops emission with the last point nonzero.
///
/// Same shape as `sandboxes_remaining` and `residents_discounted` on the first
/// and third. Prefer a staleness window over a bare last-value threshold, and
/// expect a flapping series on a node that moves in and out of otherwise-ready
/// across ticks.
///
/// Not a work queue that drains down like `sandboxes_remaining`: the evacuation
/// pipeline never services these rows (`created`, `stopped`, `deleting`,
/// `quarantined`), so it falls only when the sandbox is deleted, its `deleting`
/// completes, or quarantine resolution releases it.
pub fn termination_blocking_residents(node_id: &str, count: u64) {
    gauge(
        "node_drain.termination_blocking_residents",
        count as f64,
        &[tag("node", node_id)],
    );
}

/// Sandboxes mid-migration on the node — bounded by `max_per_node`.
pub fn sandboxes_in_flight(node_id: &str, count: u64) {
    gauge(
        "node_drain.sandboxes_in_flight",
        count as f64,
        &[tag("node", node_id)],
    );
}

/// Zero the per-node gauges for a node that is leaving the cluster, so the
/// series are eligible for retention-based pruning instead of accumulating
/// forever as nodes churn. Call once when the `nodes` row is deleted
/// (i.e. after a successful `terminating → DELETE FROM nodes`).
pub fn clear_node_gauges(node_id: &str) {
    sandboxes_remaining(node_id, 0);
    sandboxes_in_flight(node_id, 0);
    drain_blockers(node_id, 0);
    pinned_suspended(node_id, 0);
    residents_discounted(node_id, 0);
    termination_blocking_residents(node_id, 0);
}

/// Per-sandbox suspend latency. `pre_lifecycle` is the user-visible
/// lifecycle the sandbox was in at evacuation start (`started` or
/// `paused`), so the lifecycle-preserving paused path can be charted
/// separately from the normal started path.
///
/// `node` is the node being evacuated. Only forced drains evacuate, so the
/// series needs no `mode`.
pub fn sandbox_suspend_duration(started_at: Instant, node: &str, pre_lifecycle: &str) {
    timing_ms(
        "node_drain.sandbox_suspend_duration_ms",
        elapsed_ms(started_at),
        &[tag("node", node), tag("pre_lifecycle", pre_lifecycle)],
    );
}

/// Per-sandbox restore latency. See [`sandbox_suspend_duration`] for the
/// `pre_lifecycle` tag — the `paused` series covers restores issued with
/// `target_state = Paused`, which skip the post-load resume so the VM
/// stays paused (no extra pause RPC at the end of restore).
pub fn sandbox_restore_duration(started_at: Instant, pre_lifecycle: &str) {
    timing_ms(
        "node_drain.sandbox_restore_duration_ms",
        elapsed_ms(started_at),
        &[tag("pre_lifecycle", pre_lifecycle)],
    );
}

pub fn node_duration(started_at: Instant) {
    timing_ms("node_drain.node_duration_ms", elapsed_ms(started_at), &[]);
}

/// One per-node store query's wall-clock, tagged by `query` and by `outcome`
/// (`ok` / `error`). Neither of these scans costs in proportion to the node's
/// *remaining* work: both pay a per-row probe for every row the node's
/// `target_node_id` still pins, which suspend never clears, so the cost grows
/// monotonically with the node's lifetime and does not fall as the drain
/// converges. A long-lived node can therefore grow into the control-plane
/// `statement_timeout` with no other signal than the reconciler silently
/// making no progress. The `outcome` tag carries the failure rate, so
/// the latency trend and the cancellations read off one metric.
pub fn store_query_duration(node_id: &str, query: &str, outcome: &str, started_at: Instant) {
    timing_ms(
        "node_drain.store_query_duration_ms",
        elapsed_ms(started_at),
        &[
            tag("node", node_id),
            tag("query", query),
            tag("outcome", outcome),
        ],
    );
}

/// Per-sandbox migration failure. `phase` identifies which step in the
/// migration pipeline failed (`suspend`, `restore`, `lookup`, ...).
/// `pre_lifecycle` is the user-visible lifecycle the sandbox was in
/// at evacuation start (`started` or `paused`). A cohort-restore wake
/// that timed out while the node was still restoring is NOT a failure —
/// it counts in [`restore_converging`], which carries the readiness step
/// that stalled in its own `phase` tag.
pub fn failure(node_id: &str, phase: &str, pre_lifecycle: &str) {
    counter(
        "node_drain.failures",
        1,
        &[
            tag("node", node_id),
            tag("phase", phase),
            tag("pre_lifecycle", pre_lifecycle),
        ],
    );
}

/// A cohort-restore attempt whose blocking wake wait exhausted the API-side
/// node-RPC budget while the node was still restoring. Not a failure (see
/// [`failure`]): the next sweep re-drives the same sandbox until it
/// converges. `phase` names the readiness step the wait stalled in —
/// `pod_never_ready` / `proxy_unreachable` / `guest_not_ready` — or `budget`
/// when the outer wake budget fired without entering a specific readiness
/// wait. `pre_lifecycle` matches the [`failure`] tag of the same name.
/// A sustained high rate with no matching
/// `node_drain.sandbox_restore_duration_ms` completions means restores are
/// wedged rather than slow — the `phase` tag says where.
pub fn restore_converging(phase: &str, pre_lifecycle: &str) {
    counter(
        "node_drain.restore_converging",
        1,
        &[tag("phase", phase), tag("pre_lifecycle", pre_lifecycle)],
    );
}

/// A sandbox the drain reconciler flipped to `lifecycle = 'failed'` because
/// its evacuation suspend was rejected with "sandbox is failed": the cluster
/// SSOT read it `started`/`paused` (the `pre_lifecycle` tag) but the node
/// reported it `failed`, a divergence that never self-heals and would
/// otherwise wedge the node's drain forever. A sustained rate points at guests
/// dying on a node without the node-local `failed` verdict propagating back to
/// Postgres.
pub fn sandbox_failed_on_node(node_id: &str, pre_lifecycle: &str) {
    counter(
        "node_drain.sandbox_failed_on_node",
        1,
        &[tag("node", node_id), tag("pre_lifecycle", pre_lifecycle)],
    );
}

/// ASG `TerminateInstanceInAutoScalingGroup` failure.
pub fn terminate_failure(node_id: &str) {
    counter("node_drain.terminate_failures", 1, &[tag("node", node_id)]);
}

/// `drained_node_terminator` skipped a `drained -> terminating` flip
/// because the k8s Node carries the `space.pplx.net/drain-keep`
/// operator override annotation.
pub fn drain_keep_skips(node_id: &str) {
    counter("node_drain.drain_keep_skips", 1, &[tag("node", node_id)]);
}

/// Draining/drained rows whose backing k8s Node is gone and which could
/// not be safely auto-recovered this tick: either no EC2 cross-check is
/// configured, the EC2 instance is still live-but-deregistered, or the
/// EC2 lookup errored (plus, for `phase = "drained"`, the guarded
/// hard-delete being refused). `phase` is the `nodes.drain_phase` the row
/// was parked in: `"drained"` is emitted by the `drained_node_terminator`,
/// `"draining"` by the node-drain reconciler's KubeNodeGone recovery.
/// Each emitter reports its phase every tick — including zero — so an
/// alert can fire on a sustained nonzero value rather than only on a
/// transient spike. A row that stays parked across many ticks needs
/// manual review (`POST /admin/v1/nodes/{id}:terminate`).
pub fn kube_node_gone_parked(phase: &str, count: u64) {
    gauge(
        "node_drain.kube_node_gone_parked",
        count as f64,
        &[tag("phase", phase)],
    );
}

/// Resident sandboxes the node-drain KubeNodeGone recovery flipped to
/// `lifecycle = 'failed'` on a draining node whose kube Node is gone and
/// whose EC2 instance is confirmed terminated: their VMs (and any
/// in-flight transitions) died with the instance, so no suspend/restore
/// pipeline can ever service them. Incremented by the per-node count once
/// per tick that flipped any row (zero-count ticks are not emitted). The
/// matching log line carries `reason = "drain_kube_node_gone"`.
pub fn kube_node_gone_failed_sandboxes(node_id: &str, count: u64) {
    counter(
        "node_drain.kube_node_gone_failed_sandboxes",
        count,
        &[tag("node", node_id)],
    );
}

/// One refusing guard observed after the KubeNodeGone recovery's guarded
/// hard-delete matched no row. `reason` is one of `phase_drift`, `uid_drift`,
/// `non_portable_suspended`, `active_resident`, or `transient_race` (the
/// post-refusal diagnosis found every guard clear — the blocker resolved
/// between the DELETE and the diagnostic read). A single refused tick can
/// increment several reasons when more than one guard refuses at once.
pub fn kube_node_gone_delete_refused(node_id: &str, reason: &str) {
    counter(
        "node_drain.kube_node_gone_delete_refused",
        1,
        &[tag("node", node_id), tag("reason", reason)],
    );
}

/// Suspended sandboxes the KubeNodeGone recovery marked `lifecycle = 'failed'`
/// because the node pinning their only copy has a confirmed-terminated EC2
/// instance and no durable snapshot (nor any upload that could still produce
/// one) exists — the give-up that lets the guarded reap converge on the next
/// tick instead of parking forever. Incremented by the per-node count once per
/// tick that marked any (zero-count ticks are not emitted).
pub fn kube_node_gone_sandboxes_marked_lost(node_id: &str, count: u64) {
    counter(
        "node_drain.kube_node_gone_sandboxes_marked_lost",
        count,
        &[tag("node", node_id)],
    );
}

pub fn loop_latency(started_at: Instant) {
    timing_ms("node_drain.loop_latency_ms", elapsed_ms(started_at), &[]);
}

/// Sandboxes flipped to `lifecycle = 'failed'` by the drain reconciler
/// because they sat wedged in a non-evacuable resident transient
/// lifecycle (creating/starting/pausing/resuming/stopping, or a
/// non-evacuation suspending/restoring) past the
/// stale-abandon deadline on a draining node.
/// Incremented by the per-node count once per tick that did any
/// abandoning (zero-count ticks are not emitted). The matching log line
/// carries `reason = "drain_abandon_stale"` for grepability.
pub fn stale_abandoned(node_id: &str, count: u64) {
    counter("node_drain.stale_abandoned", count, &[tag("node", node_id)]);
}

/// Crash-stranded `suspending` evacuations the node-drain re-drive advanced to
/// `suspended` because their durable snapshot finalized `ready` after a
/// mid-suspend controller crash. Incremented by the count once per restore
/// sweep that advanced any row (zero-count sweeps are not emitted). A non-zero
/// value means the controller crashed in the narrow window between the suspend
/// RPC returning and the `suspended` commit, and self-healed once the upload
/// finalized — rare, so any sustained rate is worth a look.
pub fn redrive_stranded_suspended(count: u64) {
    counter("node_drain.redrive_stranded_suspended", count, &[]);
}

/// Reconciler-driven promotion of a local-only suspended snapshot to
/// S3, so the source node can finish draining. `outcome` is one of:
///
/// - `"backup_enqueued"` — the reconciler enqueued a backup of the current
///   full local artifact on the on-node sbx-mgr's durable worker. The sandbox drops out of the
///   local-only list on the next tick once the durable worker
///   registers a matching `state='ready'` backup row.
/// - `"failed"` — the on-node RPC errored or timed out. The reconciler
///   retries on the next tick. See [`failure`] for the matching
///   per-phase error counter.
/// - `"failed_artifact_missing"` — terminal: the on-node RPC reported
///   that the local snapshot files have been pruned and no S3 copy
///   exists. The reconciler auto-marks the row `lifecycle = 'failed'`
///   so drain can proceed; the data was already lost before this
///   tick.
/// - `"node_gone_gave_up_remote_snapshot"` — the node's kube Node is gone
///   and its EC2 instance is confirmed terminated, so the backup enqueue
///   can never reach it; the reconciler adopted the newest ready remote
///   snapshot instead (the sandbox stays `suspended` and recoverable).
/// - `"node_gone_lost"` — same node-gone trigger, but no ready remote
///   snapshot exists; the local artifact died with the instance, so the
///   row was marked `lifecycle = 'failed'` (terminal, data already lost).
/// - `"node_gone_retry"` — same node-gone trigger, but neither the adopt
///   nor the mark-lost guard fired this tick (in-flight upload row, stale
///   ready snapshot, or drift); re-evaluated next tick.
pub fn local_snapshot_promotion(node_id: &str, outcome: &str) {
    counter(
        "node_drain.local_snapshot_promotion",
        1,
        &[tag("node", node_id), tag("outcome", outcome)],
    );
}

/// Suspended sandboxes whose `target_node_id` still pins them to this
/// draining node *and* whose snapshot has not yet been registered to
/// S3 — the work queue the promotion step has left. Drops to zero once
/// every pinned sandbox has a `state='ready'` snapshots row
/// (`target_node_id` is preserved as a placement diagnostic); only
/// then does the reconciler advance `draining → drained`.
pub fn pinned_suspended(node_id: &str, count: u64) {
    gauge(
        "node_drain.pinned_suspended",
        count as f64,
        &[tag("node", node_id)],
    );
}
