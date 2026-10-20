//! Liveness and outcome of the cluster-singleton reconciler that mirrors each
//! kube Node's node-group label into `nodes.node_group_id`.
//!
//! The distinction that shapes this module: the mirror's *steady state is
//! silence*. Once the column has converged it changes nothing, and before any
//! label exists in the cluster it changes nothing either — so without a series
//! that ticks on a no-op there is no difference between a healthy mirror and
//! one that has stopped ticking. [`tick_completed`] is that series, and it is
//! the reason this module exists; everything else here is context for reading
//! it. (A LIST or statement that hangs books [`tick_error`] once the
//! reconciler's bound on it elapses; a loop that isn't running books nothing,
//! which is why absence is the signal.)

use crate::emit::{counter, gauge, tag};

/// A tick that ran to completion: the kube LIST returned, the `node_groups`
/// read returned, and the bulk UPDATE committed — the last skipped when the
/// tick resolved no named Node to write, which is itself a completed tick.
///
/// Emitted on **every** successful tick, including the ones that write
/// nothing, so absence of increments is the staleness signal. That is the whole
/// point of the series: alert on absence, not on value.
///
/// [`observed`] and [`unknown_group_nodes`] are published only alongside this
/// counter. What that guarantees is the negative: a failed tick cannot publish
/// a zero that a monitor reads as "converged" or "nothing to worry about". How
/// long the last good value then stands, versus going no-data, is a statsd
/// context-expiry and evaluation-window question rather than something this
/// module decides.
pub fn tick_completed() {
    counter("node_group_mirror.tick_completed", 1, &[]);
}

/// Selector-matching kube Nodes the tick read, labeled or not — the denominator
/// for [`unknown_group_nodes`].
///
/// This is the mirror's own view of the worker fleet, taken from a live LIST
/// against the apiserver rather than from a reflector cache, so it does not go
/// stale the way a frozen store would. A drop to zero means the sandbox node
/// selector stopped matching the worker pool or the pool is genuinely empty;
/// it does not mean the LIST failed, because that path books
/// `tick_error{kind:list}` and withholds this gauge instead of publishing a
/// zero.
///
/// Related to `node_group_labeler.workers` as `observed >= workers` **on the
/// populations** — greater-or-equal, not reliably greater: the labeler matches the
/// complete sandbox placement selector while this LIST passes only the primary
/// node-pool key/value pair, so a Node missing one of the
/// `sandboxPod.nodeSelector.extraLabels` entries is counted here and not there
/// — but on a fleet where every node-pool member carries the extras the two
/// coincide, and equality is the normal reading rather than evidence of a
/// mis-scoped LIST.
///
/// The containment is about sets, not about any two samples. The labeler scans
/// a shared Node reflector store on a 30-second tick; this gauge comes from a
/// live apiserver LIST on a 60-second one. On a shrinking fleet a reflector
/// that has not yet processed a deletion reads *higher* than a LIST that has,
/// so `workers > observed` for a window. Compare them over a settled period,
/// not point-to-point.
pub fn observed(count: usize) {
    gauge("node_group_mirror.observed", count as f64, &[]);
}

/// Nodes whose node-group label names a group with no `node_groups` row, which
/// the mirror writes as NULL until the row exists or the label is fixed.
///
/// A gauge because it is a **standing** condition: the mirror cannot resolve
/// itself out of it, so the value persists across every tick until an operator
/// inserts the named group or strips the label, or the Nodes go away. Alert on
/// sustained `> 0`. A counter would fire once and recover while the nodes
/// stayed mis-attributed.
///
/// What the condition costs while it stands, **once the singleton default group
/// exists**: `nodes_count`'s `is_default` arm resolves a NULL-membership worker
/// to that group, so these Nodes report as shared-pool capacity while their
/// label claims a group with no row to report anything at all. Before stage 0
/// seeds the default there is no row for the arm to attribute to, so they are
/// simply counted nowhere. The shape that produces it in practice:
/// a managed node group whose declared labels include the boundary label, on a
/// cluster where no `node_groups` row carries that group's name.
///
/// The reconciler also warns per node per tick, which names them; this gauge is
/// what makes the condition monitorable without reading logs.
pub fn unknown_group_nodes(count: usize) {
    gauge("node_group_mirror.unknown_groups", count as f64, &[]);
}

/// `nodes` rows whose `node_group_id` this tick actually changed.
///
/// A **counter**, matching `node_group_labeler.label_write` rather than the
/// gauges above, because it is a per-tick delta and not a state. A statsd gauge
/// re-reports its last value on every flush until the value changes or the
/// context expires, and the flush interval is shorter than this reconciler's
/// tick — so as a gauge one tick that moved three rows would keep reading three
/// across the flushes that follow it, and a window sum would count those rows
/// once per flush. As a counter the same tick books three, once. (The exact
/// multiplier depends on the agent's flush interval, which nothing in this repo
/// pins; the direction of the error does not.)
///
/// Zero is not emitted — the steady state is a no-op, and a per-minute zero on
/// every cluster is noise. [`tick_completed`] is what proves the tick ran; this
/// only says whether it had anything to do.
///
/// Reading it: a burst is the mirror converging on a change — the labeler's
/// rollout, an MNG label edit, a stage-4 rollback relabeling Nodes out of a
/// dedicated group, or the missing `node_groups` row being inserted so a
/// previously unresolvable label starts resolving, which shows up as a burst
/// here and a matching drop in [`unknown_group_nodes`]. A low steady rate on a
/// fleet where no label is moving is ordinary node churn: a replaced worker
/// registers a fresh `nodes` row with a NULL column and the next tick stamps
/// it — though only where that node's kube Node carries a *resolvable* label:
/// before any labeling has happened, a fresh row is already NULL, the mirror
/// resolves `None`, and `IS DISTINCT FROM` makes it a no-op that books nothing
/// here. There is no "fighting another writer" reading available, because this
/// loop is the only writer of the column.
pub fn rows_updated(count: u64) {
    if count == 0 {
        return;
    }
    counter("node_group_mirror.rows_updated", count, &[]);
}

/// A tick that failed before completing, so no mirrored value was confirmed
/// written.
///
/// Singular, unlike the labeler's `tick_errors(kind, count)`: that reconciler
/// accumulates independently-failing sub-phases within one tick, while this
/// one runs its steps in sequence behind a single `Result`, stopping at the
/// first failure, so a tick fails once or not at all.
pub fn tick_error(kind: TickErrorKind) {
    counter(
        "node_group_mirror.tick_error",
        1,
        &[tag("kind", kind.as_str())],
    );
}

/// Which half of the tick failed. Neither can leave the column *partially*
/// applied — the write is one bulk UPDATE — and the next successful tick
/// recomputes the whole fleet from scratch, so an isolated occurrence of either
/// self-heals. They are split because they point at different systems to look
/// at.
///
/// **All-or-none is not the same as "nothing was written", and [`Query`] is the
/// arm where that bites.** A connection lost after Postgres commits but before
/// the client sees `ReadyForQuery` surfaces as an error whose rows landed
/// anyway, so this counter means *unconfirmed*, not *untouched*. The
/// distinction is cosmetic for the mirror itself, which recomputes from the
/// labels next tick either way, but it matters to an operator reading the
/// column during an incident: a `kind:query` burst does not license the
/// conclusion that `node_group_id` still holds the prior tick's values.
///
/// [`Query`]: Self::Query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickErrorKind {
    /// The kube LIST of selector-matching Nodes failed or outran the
    /// reconciler's client-side bound: apiserver unavailability, a throttled
    /// client, an expired or under-scoped service account token.
    List,
    /// Postgres rejected or could not serve the tick's `node_groups` read or its
    /// write — the write's setup (pool acquire included), the bulk UPDATE, or its
    /// COMMIT — including a step that outran one of the reconciler's bounds. A
    /// burst during pool exhaustion may involve no slow statement at all. Not
    /// split further because the first move is the same for each: go look at
    /// the database. What the split would not capture either: a failed read or
    /// setup wrote nothing, and a failed UPDATE is rolled back with its
    /// transaction, so only a failed COMMIT, or a write the reconciler stopped
    /// waiting for during its COMMIT, is merely *unconfirmed*, per the note
    /// above. The reconciler can't tell which part of a write it stopped
    /// waiting on, so a timed-out write reports as unconfirmed either way.
    Query,
}

impl TickErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Query => "query",
        }
    }
}
