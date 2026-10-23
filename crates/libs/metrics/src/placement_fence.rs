//! Metric helpers for the termination fence that orders placement writers
//! against the provider-terminate path.
//!
//! Emitted from **two** services, which matters for any service-scoped query:
//! `space-api` (and `space-admin-api`) for the create/wake/restore writers, and
//! `space-cluster-controller`, because node-drain's cohort restore drives the
//! same Kubernetes substrate and so runs the pod-placement writer.

use crate::emit::{counter, tag};

#[cfg(test)]
mod tests;

/// A placement writer abandoned its `active_node_id` write because the fence
/// refused the node.
///
/// `writer` names the call site so the series says *which* path raced, and
/// `verdict` says what the fence saw: `terminating`, `drained`,
/// `unrecognized_phase`, or `missing_node` — the `nodes` row was already deleted,
/// which the drain loop does as soon as the provider accepts the termination and
/// therefore while the machine may still be running. That is the whole set. The
/// one phase that is never a verdict is `draining`, in **either** drain mode:
/// refusing there would remove the sandbox from the evacuation queue that keys on
/// `active_node_id`, so it alone never emits.
///
/// **Every increment is a sandbox that would otherwise have been lost or
/// stranded**, so this is an alertable series rather than a dashboard curiosity:
/// a placement reaching a `drained` or `terminating` node means the window
/// between choosing a node and persisting the placement outlived a whole drain,
/// which is expected only under fleet-shrink pressure (a node-group rollout
/// draining and terminating its old nodes). A sustained rate outside a rollout
/// means placement selection is handing out nodes that are already leaving.
///
/// No `node` tag on purpose. The node id is in the matching WARN, and it churns
/// once per instance, so tagging it here would trade an alertable low-cardinality
/// series for a per-instance one nobody can threshold.
pub fn refusal(writer: &str, verdict: &str) {
    counter(
        "placement_fence.refusals",
        1,
        &[tag("writer", writer), tag("verdict", verdict)],
    );
}

/// A termination-side writer deferred after its bounded wait for the shared
/// `nodes` row lock expired. Placement fences, registration/heartbeat, and
/// inventory upserts can all be the conflicting holder; this counter does not
/// attribute one. The deferred operation and node id stay in the matching debug
/// log rather than becoming metric tags, so this signal's meaning does not
/// depend on a backend keeping a metric-specific tag.
pub fn termination_contention_deferral() {
    counter("placement_fence.termination_contention_deferrals", 1, &[]);
}
