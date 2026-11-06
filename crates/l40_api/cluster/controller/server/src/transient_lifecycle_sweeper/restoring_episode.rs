//! The restoring-episode deadline: when the sweep stops waiting for the owner
//! of a `restoring` row that client retries keep alive (see
//! [`super::ReconcilerConfig::restoring_episode_deadline`]).

use super::probe::SweepCandidate;

/// The reason a claimed revert reports. A revert the deadline let through is
/// told apart, since it may have overridden a live retry.
pub(super) fn revert_reason(candidate: &SweepCandidate) -> &'static str {
    if candidate.episode_overdue {
        "restoring_episode_deadline"
    } else {
        "recoverable_snapshot"
    }
}
