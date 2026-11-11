//! Rollout state of the org-scoped node-group boundary label on the shared
//! worker pool, as published by the cluster-singleton labeler.
//!
//! The distinction that shapes this module: the labeler's interesting
//! conditions are **standing**, not momentary. "N workers will never be
//! labeled" persists until an operator acts, so it has to be a gauge —
//! a counter or a log line makes it an event, and a monitor over an event
//! fires once and then recovers while the condition is still true and the
//! capacity still dark. Counters here are reserved for things that genuinely
//! happen once per occurrence.

use pplx_space_metrics_emit::{counter, gauge, tag};

/// A tick that ran to completion, with a real fleet measurement behind it.
///
/// Every gauge in this module is published only alongside this counter, so
/// "no increments recently" is the staleness signal: it means the reconciler
/// is bailing early (cold reflector, failing census, failing seed) and the
/// gauges are holding their last good values rather than reading zero. Alert
/// on absence, not on value. What the split guarantees is only the negative: a
/// bailed tick cannot publish a zero that silences a `sustained > 0` monitor.
/// Whether the monitor then sees a gap or the prior point is a `notify_no_data`
/// and evaluation-window question, not something this module decides.
pub fn tick_completed() {
    counter("node_group_labeler.tick_completed", 1, &[]);
}

/// Worker Nodes the labeler evaluated this tick — every Node matching the
/// complete sandbox placement selector. The denominator for every gauge
/// below except [`other_groups`], which counts `node_groups` rows rather
/// than nodes. A drop to zero means the selector stopped
/// matching the worker pool, the pool is genuinely empty, or the reflector's
/// writer died *after* initial sync (the frozen-store case nothing detects). It
/// does **not** mean a cold reflector: that is an `Err` from the scan, so the
/// tick early-returns and this gauge is withheld rather than published as zero
/// — look for `tick_error{kind:list}` instead.
pub fn workers(count: usize) {
    gauge("node_group_labeler.workers", count as f64, &[]);
}

/// Worker Nodes carrying the default group's label, **excluding
/// dedicated-tainted ones**. The classification skips a tainted Node before it
/// looks at labels, so one carrying the taint *and* the default label is counted
/// in [`dedicated_skipped_nodes`] and [`dedicated_mislabeled_nodes`] and never
/// here — it holds the label this gauge is named for and is absent from it. The
/// identity below is consistent with that, because it subtracts
/// `dedicated_skipped_nodes` wholesale. Together with [`workers`] this is the
/// rollout's progress bar.
///
/// The gauges close into an identity only once no write is outstanding, because
/// `workers` counts a node the moment it is seen while `labeled_nodes` counts it
/// only after the apiserver acks. Fully:
///
/// `labeled_nodes == workers - dedicated_skipped_nodes - foreign_group_nodes
/// - suppressed_nodes - deferred_nodes -` (this tick's
/// `label_write{outcome:error}` + `label_write{outcome:indeterminate}`)
///
/// The last three terms are unresolved writes, and the final two are *counters*,
/// not gauges — so a dashboard built from gauges alone will show a transient
/// deficit during back-pressure or an apiserver failure, and that is the metrics
/// being right rather than drift. On a quiescent tick they are all zero and it
/// reduces to `workers - dedicated_skipped_nodes - foreign_group_nodes`.
///
/// `labeled_nodes == workers` is true only when nothing is dedicated-tainted
/// *and* nothing carries a foreign label — both states this module reports — so
/// don't dashboard that as "rollout complete".
///
/// A non-zero [`suppressed_nodes`] is the thing to act on, not a term to
/// reconcile: the rollout can no longer complete on its own.
pub fn labeled_nodes(count: usize) {
    gauge("node_group_labeler.labeled_nodes", count as f64, &[]);
}

/// Unlabeled worker Nodes the labeler **refused** to touch because a
/// non-default node group exists, so "unlabeled" no longer implies "default
/// pool".
///
/// A gauge because it *can* be a standing condition with real cost: a Node that
/// stays here serves no sandboxes at all once required affinity ships — present,
/// healthy, and unused. Alert on `> 0` sustained rather than on a transition,
/// because the transient case is routine: another group's node is
/// selector-matched, untainted and unlabeled until its control-plane-applied
/// dedicated taint lands, and counts here until it does. Sustained non-zero is
/// the actionable shape.
pub fn suppressed_nodes(count: usize) {
    gauge("node_group_labeler.suppressed_nodes", count as f64, &[]);
}

/// Unlabeled worker Nodes whose patch the tick never issued — the label phase's
/// budget ran out first, the shutdown token fired mid-phase, or the pre-write
/// re-read found the node gone, no longer a worker-selector match (relabeled out
/// of the sandbox pool mid-tick), already grouped, or newly dedicated-tainted.
///
/// Only the first cause can sustain, and only the first one is actionable: it is
/// ordinary back-pressure on a large first rollout, and a *sustained* non-zero
/// value is the rollout plateauing. The other two clear themselves — a shutdown
/// deferral is set by the last tick the process runs, so the series goes stale
/// with the pod, and a superseded node is re-classified from the next tick's
/// fresh snapshot. The three are indistinguishable in this gauge; the tick's
/// summary log carries `label_deferred_shutdown` and `label_superseded` for
/// telling them apart at a single point.
///
/// So the reading an operator needs: a spike that coincides with a
/// cluster-controller deploy is an orderly wind-down and needs no action, while a
/// sustained spike with no deploy behind it is real back-pressure — raise
/// `write_concurrency` or wait more ticks.
pub fn deferred_nodes(count: usize) {
    gauge("node_group_labeler.deferred_nodes", count as f64, &[]);
}

/// Worker Nodes labeled with some *other* group's name.
///
/// Expected once dedicated groups exist. Non-zero while
/// `node_group_labeler.other_groups` is zero means the label names a group
/// that does not exist in `node_groups` — the Node is permanently
/// unlabelable (the labeler never overwrites) and will serve nothing once
/// required affinity ships.
///
/// **This counts; it does not name — and that gap compounds with the
/// `patch nodes` grant that makes the boundary label forgeable.** The two
/// forgery directions get asymmetric detection. A *dedicated* node relabeled
/// into the default group raises [`dedicated_mislabeled_nodes`] **plus** a
/// per-node warn naming up to five nodes a tick. A *shared* worker relabeled
/// into a dedicated org's group raises only this gauge: the reconciler's naming
/// warn is gated on `other_groups == 0`, and that gate is shut from the moment
/// any dedicated group exists — which is exactly when the second direction
/// becomes worth doing. Concretely: dedicated group `acme` exists, a guest
/// escape on a shared worker reaches `space-node-ctl`'s projected token and
/// patches the label to `acme`. This gauge goes 0→1, `suppressed_nodes` is
/// unchanged, `tick_completed` looks normal, no line names the node — and
/// `acme`'s sandboxes may then schedule onto an attacker-controlled node that
/// the dedicated taint does not repel.
///
/// Ungating the warn is not the fix: it would emit a line per legitimately
/// foreign dedicated node every tick, which is what the gate exists to prevent,
/// and a forged `acme` label is indistinguishable from a legitimate one without
/// out-of-band pool intent. The mitigation is the `patch nodes` holder audit
/// that `charts/space/values.yaml` already requires as a precondition of the
/// required-affinity flip.
pub fn foreign_group_nodes(count: usize) {
    gauge("node_group_labeler.foreign_group_nodes", count as f64, &[]);
}

/// Worker Nodes carrying the `space.pplx.net/dedicated-org` taint — the
/// org-dedicated marker, **not** `space.pplx.net/dedicated`, which is the
/// separate cred-authority fence — which the labeler leaves entirely alone.
pub fn dedicated_skipped_nodes(count: usize) {
    gauge(
        "node_group_labeler.dedicated_skipped_nodes",
        count as f64,
        &[],
    );
}

/// Worker Nodes that carry the dedicated taint **and** the default group's
/// label — a **subset** of [`dedicated_skipped_nodes`], never a replacement
/// for it.
///
/// The one state the labeler's own warn calls "needs operator attention", and
/// the only one it cannot fix: the label is written only when absent, so this
/// reconciler never removes or corrects one. It clears when an operator strips
/// the stray label, or when the Node goes away.
///
/// Why it earns its own series: such a Node is counted as shared-pool capacity
/// nowhere (the dedicated taint keeps generic sandboxes off it) and as reserved
/// capacity nowhere (its label names the default group, not its org's), so it
/// is simply dark. And if the taint is ever removed — a launch-template or MNG
/// change, a manual `kubectl taint` — the label is already there, so it joins
/// the shared pool silently, with no tick writing anything and nothing to
/// review. The per-node warn is rationed to a handful of names per tick, so on
/// a fleet with dozens of them the log names a few and this gauge is the only
/// signal for the rest. Alert on sustained `> 0`.
pub fn dedicated_mislabeled_nodes(count: usize) {
    gauge(
        "node_group_labeler.dedicated_mislabeled_nodes",
        count as f64,
        &[],
    );
}

/// Node groups other than the singleton default. Zero is the only value in
/// which the labeler will write a label at all, so this is the gate's state
/// expressed directly.
pub fn other_groups(count: i64) {
    gauge("node_group_labeler.other_groups", count as f64, &[]);
}

/// `count` label patches that shared an outcome. Emitted as one packet per
/// outcome rather than one per node: a full-fleet rollout would otherwise be
/// thousands of per-node statsd packets for a handful of series.
pub fn label_writes(outcome: LabelWriteOutcome, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "node_group_labeler.label_write",
        count,
        &[tag("outcome", outcome.as_str())],
    );
}

/// Terminal result of one Node's boundary-label patch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelWriteOutcome {
    /// The apiserver acknowledged the patch.
    Applied,
    /// The request was issued and returned an error. Not proof the label was
    /// not written — the seam cannot tell a 409 from a transport reset or a
    /// 5xx that followed a commit.
    Error,
    /// The request was issued and the client-side bound elapsed first, so the
    /// outcome is unknown.
    Indeterminate,
}

impl LabelWriteOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Error => "error",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// A tick that ended early, or degraded, without completing its work.
pub fn tick_errors(kind: TickErrorKind, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "node_group_labeler.tick_error",
        count,
        &[tag("kind", kind.as_str())],
    );
}

/// Which stage of the tick failed. `seed` is the transient-Postgres arm — on
/// first enablement it is what a missing node-groups migration looks like,
/// which is why it is not folded in with the others, and why
/// [`Self::SeedBlocked`] is not folded into *it*: a climbing `seed` says look
/// at the database (starting with the migration), and a climbing
/// `seed_blocked` says the migration is fine and an operator has to decide
/// something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickErrorKind {
    /// Resolving or seeding the default group failed **transiently** — the
    /// Postgres error arm. Three causes land here and this tag cannot separate
    /// them: the node-groups migration is not applied in this environment (the
    /// first-enablement case, and the only one that clears on its own once the
    /// schema is there), the seeding path's `lock_timeout` or a server
    /// `statement_timeout` expired waiting on `node_groups`, or an ordinary
    /// connection/pool failure. The tick's warn names all three; the error
    /// string on it is what tells them apart. Splitting them would need a
    /// SQLSTATE match and a new tag value, and the operator action — look, then
    /// decide — is the same for all three.
    Seed,
    /// Resolving or seeding the default group hit a condition that retrying
    /// will never clear: no default exists but other groups do, the stored
    /// default's name is not a valid node-group slug, or the seed INSERT
    /// conflicted. Each needs an operator decision, so this is the arm to page
    /// on — and checking the migration in response to it is the misdiagnosis
    /// the split exists to prevent.
    SeedBlocked,
    /// Counting non-default groups failed on a tick that wrote nothing, so
    /// the gate's precondition is unknown but no label is in doubt.
    Census,
    /// The post-label census failed on a tick that *did* write labels, so the
    /// gate cannot be confirmed to have held. Distinct from `Census` because
    /// that one promises nothing was written, and from
    /// [`gate_race_detected`] because no group was actually observed to
    /// appear. Owes the same operator label audit as a race.
    CensusAfterWrite,
    /// Listing worker Nodes failed, including a cold reflector cache.
    List,
}

impl TickErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::SeedBlocked => "seed_blocked",
            Self::Census => "census",
            Self::CensusAfterWrite => "census_after_write",
            Self::List => "list",
        }
    }
}

/// A tick that labeled Nodes and then found a non-default group had appeared
/// while it was writing, so the labels it applied cannot be assumed to be
/// default-pool-only.
///
/// Needs a human label audit, so alert on any occurrence. A counter rather
/// than a gauge because the audit is owed for the *event*, not for a state
/// the next tick can re-observe.
///
/// **A tick that confirmed no label at all can also trip this.** The predicate is
/// fail-closed on purpose — neither [`LabelWriteOutcome::Error`] nor
/// [`LabelWriteOutcome::Indeterminate`] is proof no label was written — so
/// either one keeps the "wrote something" term non-zero, and the next group
/// create landing inside a tick window reports a race. Two sources, and the
/// second is routine rather than a fault: a node whose patch fails on every tick
/// (a 409 from a competing writer, a partial RBAC denial) holds `errored` above
/// zero indefinitely; and on any fleet larger than one tick's write budget, the
/// writes the phase deadline truncates are classified `indeterminate` by design,
/// every tick, at the tail of the phase. So the signature is an empty
/// `confirmed_nodes` list in the accompanying error log with a non-zero
/// `errored` **or** `indeterminate` count — mid-rollout the systematic shape is
/// `errored = 0` with `indeterminate` near the write concurrency, so matching on
/// `errored` alone finds the guidance doesn't apply. Either way no label was
/// confirmed, so the audit is about whether a rejected or unacknowledged patch
/// nonetheless landed, not about labels this tick applied.
pub fn gate_race_detected() {
    counter("node_group_labeler.gate_race_detected", 1, &[]);
}
