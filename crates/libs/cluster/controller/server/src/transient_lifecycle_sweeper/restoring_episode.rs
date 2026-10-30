//! The restoring-episode deadline: when the sweep stops waiting for the owner
//! of a `restoring` row that client retries keep alive (see
//! [`super::ReconcilerConfig::restoring_episode_deadline`]).

use pplx_space_sandbox_portability::{
    restore_retry_source_snapshot_exists, restore_retry_suspended_revert_snapshot_exists,
};

use super::probe::SweepCandidate;

/// The binds [`restoring_episode_overdue`] reads.
#[derive(Debug, Clone, Copy)]
pub(super) struct EpisodeDeadlineBinds {
    /// A nullable `BIGINT` bind of the deadline in seconds; NULL turns the
    /// deadline off.
    pub(super) deadline: &'static str,
    /// A bind of `SubstrateKind::Kubernetes`.
    pub(super) kubernetes: &'static str,
}

/// The candidate select's binds (`store::list_aged_transient_query`).
pub(super) const LIST_EPISODE_DEADLINE: EpisodeDeadlineBinds = EpisodeDeadlineBinds {
    deadline: "$6",
    kubernetes: "$5",
};

/// The revert CASes' binds (`store::revert_to_suspended_query`).
pub(super) const REVERT_EPISODE_DEADLINE: EpisodeDeadlineBinds = EpisodeDeadlineBinds {
    deadline: "$5",
    kubernetes: "$6",
};

/// Whether a `restoring` row's episode outlived the restoring-episode
/// deadline while its active execution has no pod IP, so that no retry can
/// keep it alive.
///
/// A client retry re-claims an expired restore lease, renews it, and gives
/// the wake a fresh activation attempt on the same pending pod, so a client
/// that retries faster than the lease and owner guards' windows keeps a
/// failed restore's pod, and the node slot it holds, forever. The deadline
/// keys on `lifecycle_changed_at`, which a retry can't move: the user claim
/// leaves it alone on a `restoring -> restoring` re-drive, and the lifecycle
/// trigger stamps it only when the lifecycle changes.
///
/// It stops short of a restore whose pod is up. A restore pod gets its IP
/// only once its node has loaded the snapshot and resumed the VM, so an active
/// execution with a pod IP means that load finished; what is left is the
/// wake's own completion, and only the ordinary guards may let that row go.
/// The check reads the IP the execution holds now: one whose pod was replaced
/// has none again, and its new pod starts the load over. It also requires the
/// revert's own snapshot proof
/// ([`restore_retry_suspended_revert_snapshot_exists`]) and the restore-source
/// proof the sweep's decision reverts on
/// ([`restore_retry_source_snapshot_exists`]): the deadline exists to revert,
/// and a row that can't be reverted keeps the owner guards rather than being
/// listed for a fail its owner guards then refuse every tick.
///
/// Only a Kubernetes row has a pod IP to wait for. Any other substrate's
/// `restoring` row has no Kubernetes execution at all, which would read as
/// one without a pod IP and override a live restore lease.
///
/// Reverting under a live owner is safe for the same reason as reverting a
/// gone one: the wake's own writes are `restoring`-guarded, and the revert's
/// pod fence refuses a pod that was replaced, or whose object changed, after
/// the candidate was observed. A pod that comes up between that re-check and
/// the CAS is still deleted, which wastes its load but loses nothing: the row
/// never reached `started`, so no request reached its VM, and the snapshot
/// the revert proved is the one the next restore selects.
///
/// The revert CAS also takes the active execution's row lock, which the
/// pod-IP writers take, so the no-IP read here is current when it commits,
/// and it defers rather than waits when a writer holds it
/// (`SubstrateSweepSink::revert_to_suspended`).
pub(super) fn restoring_episode_overdue(binds: EpisodeDeadlineBinds) -> String {
    let EpisodeDeadlineBinds {
        deadline,
        kubernetes,
    } = binds;
    let revert_safe_snapshot = restore_retry_suspended_revert_snapshot_exists("sandboxes");
    let source_snapshot = restore_retry_source_snapshot_exists("sandboxes");
    format!(
        "({deadline}::BIGINT IS NOT NULL
                     AND sandboxes.managed_by = {kubernetes}
                     AND sandboxes.lifecycle_changed_at <
                        NOW() - make_interval(secs => {deadline}::BIGINT)
                     AND NOT EXISTS (
                         SELECT 1
                           FROM sandbox_k8s_executions ip
                          WHERE ip.sandbox_id = sandboxes.id
                            AND ip.active
                            AND ip.pod_ip IS NOT NULL
                     )
                     AND {revert_safe_snapshot}
                     AND {source_snapshot})"
    )
}

/// The reason a claimed revert reports. A revert the deadline let through is
/// told apart, since it may have overridden a live retry.
pub(super) fn revert_reason(candidate: &SweepCandidate) -> &'static str {
    if candidate.episode_overdue {
        "restoring_episode_deadline"
    } else {
        "recoverable_snapshot"
    }
}
