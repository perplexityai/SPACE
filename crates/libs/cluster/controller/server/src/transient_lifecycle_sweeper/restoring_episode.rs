//! The restoring-episode deadline: when the sweep stops waiting for the owner
//! of a `restoring` row that client retries keep alive (see
//! [`super::ReconcilerConfig::restoring_episode_deadline`]).

use pplx_space_sandbox_store::sweeper::EpisodeDeadlineBinds;

use super::probe::SweepCandidate;

/// The candidate select's binds (`store::list_aged_transient_query`) for
/// `pplx_space_sandbox_store::sweeper::restoring_episode_overdue`, which owns
/// the deadline's predicate alongside the revert CASes that also apply it.
pub(super) const LIST_EPISODE_DEADLINE: EpisodeDeadlineBinds = EpisodeDeadlineBinds {
    deadline: "$6",
    kubernetes: "$5",
};

/// The reason a claimed revert reports. A revert the deadline let through is
/// told apart, since it may have overridden a live retry.
pub(super) fn revert_reason(candidate: &SweepCandidate) -> &'static str {
    if candidate.episode_overdue {
        "restoring_episode_deadline"
    } else {
        "recoverable_snapshot"
    }
}
