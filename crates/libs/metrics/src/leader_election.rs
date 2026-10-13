//! Metric helpers for the cluster-controller leader election.
//!
//! Names match the `cluster_controller.leader_election.*` namespace. The `is_leader` gauge is the operational signal:
//! summed across pods it should equal exactly `1` while the controller is
//! healthy (one holder), and `0` only briefly during a lease handoff.

use crate::emit::{counter, gauge, tag};

/// Per-pod leadership state. `identity` is the pod name (or the configured
/// election identity). Sum across identities should be `1`.
///
/// Emitted on every acquisition attempt, holder and standby alike: a standby
/// told that a peer holds emits `0`, and so does a contender whose attempt
/// errors or times out, beside `acquire_failed{reason}`, because a replica
/// still campaigning is not the leader whatever the cause. On renewal a holder
/// emits `1`; a renewal that errors, times out, or is answered with a peer
/// holding the lease emits no sample but increments a counter, and the last
/// starts a resignation rather than reporting `0`. So this gauge is not a
/// liveness heartbeat: a wedged process emits nothing. Monitor absence
/// separately from a reported zero.
pub fn is_leader(identity: &str, leader: bool) {
    gauge(
        "cluster_controller.leader_election.is_leader",
        if leader { 1.0 } else { 0.0 },
        &[tag("identity", identity)],
    );
}

/// A replica won the lease (a standby became the active reconciler-runner).
pub fn acquired(identity: &str) {
    counter(
        "cluster_controller.leader_election.acquired",
        1,
        &[tag("identity", identity)],
    );
}

/// A lease renewal query failed while this replica held the lease. Transient
/// failures retry; only a failure sustained past the renew deadline forces a
/// resignation, whatever the reason — a holder that cannot confirm its lease
/// must stop acting even when the cause is one a standby would shrug off.
///
/// `reason` is the failure category (see `LeaseFailure` in the
/// cluster-controller's leader-election store): a bounded set, safe as a tag.
pub fn renew_failed(reason: &'static str) {
    counter(
        "cluster_controller.leader_election.renew_failed",
        1,
        &[tag("reason", reason)],
    );
}

/// A lease acquisition query failed for a replica that does *not* currently hold
/// the lease (a standby). Kept separate from [`renew_failed`] so an apiserver
/// outage seen by many standbys doesn't masquerade as the active holder failing
/// to renew.
///
/// `reason` is the failure category, and it is what makes a brownout
/// distinguishable from a misconfiguration without reading pod logs:
/// `unreachable` spends the liveness budget and ends in a restart, `overloaded`
/// deliberately doesn't (a restart re-LISTs against an already-struggling
/// control plane), `refused` means a human has to change something, and `clock`
/// means this replica cannot reason about expiry at all. A bounded set, safe as
/// a tag.
pub fn acquire_failed(reason: &'static str) {
    counter(
        "cluster_controller.leader_election.acquire_failed",
        1,
        &[tag("reason", reason)],
    );
}

/// The holder lost the lease — either another replica stole it (renewal
/// observed a different holder) or the holder could not renew within the TTL.
/// The process resigns and exits so Kubernetes restarts it as a standby.
pub fn lease_lost(identity: &str) {
    counter(
        "cluster_controller.leader_election.lease_lost",
        1,
        &[tag("identity", identity)],
    );
}

/// A replica could not reach the `Lease` for the whole configured grace window
/// and is exiting non-zero so Kubernetes restarts it. This is the
/// cluster-controller's liveness signal for the lease path: it deliberately
/// fires on a much longer window than readiness, so an apiserver brownout that
/// flips the pod NotReady for a few seconds doesn't restart it. Expected `0`;
/// a non-zero value means a replica was wedged out of the election long enough
/// that a restart was the only way back (a half-open apiserver connection, a
/// stale projected token, or a sustained outage).
///
/// `reason` is the category of the failure that exhausted the budget. Only the
/// restart-fixable categories can appear here — `unreachable` and `clock` —
/// because the others deliberately don't spend the budget; the tag is present so
/// the terminal metric says *why* without a log lookup.
pub fn lease_unreachable(identity: &str, reason: &'static str) {
    counter(
        "cluster_controller.leader_election.lease_unreachable",
        1,
        &[tag("identity", identity), tag("reason", reason)],
    );
}
