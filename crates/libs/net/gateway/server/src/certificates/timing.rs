//! Local CA renewal and leaf-admission budgets; not an issuer wire contract.

use std::time::Duration;

pub(super) const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(45);
pub(super) const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);
/// Both ten-second TLS handshakes plus acquisition/scheduling margin.
pub(super) const ADMISSION_RESERVE: Duration = Duration::from_secs(30);
/// How long after the first of a run of sandbox-manager's designed not-ready
/// answers the owner keeps logging them at debug; a marked answer that arrives
/// later warns as before. The owner checks only when an answer arrives, and
/// retries back off (up to [`MAX_RETRY_BACKOFF`]), so the first answer past
/// this window can come seconds after it ends. An activation that publishes
/// the execution identity before the retry that would bring that answer logs
/// no warning for the wait.
pub(super) const QUIET_NOT_READY: Duration = Duration::from_secs(5);
/// Two acquisition attempts, one maximum backoff and scheduling margin.
pub(super) const MIN_REMAINING: Duration = ADMISSION_RESERVE
    .saturating_add(ACQUIRE_TIMEOUT.saturating_mul(2))
    .saturating_add(MAX_RETRY_BACKOFF)
    .saturating_add(Duration::from_secs(30));

/// Polling cadence of the CA owner loop.
///
/// These are scheduling intervals, not protocol deadlines: how often the owner
/// re-checks a renewal date it is waiting on, how often it re-checks an epoch
/// it has stopped issuing for, and how fast it backs off between failed
/// acquisitions. [`Default`] is the production cadence and is what every
/// production constructor uses. Carrying them in a value lets a test that
/// drives a real HostControl server — where a paused clock is unavailable —
/// observe the same ordering over a shorter span.
#[derive(Clone, Copy, Debug)]
pub(super) struct AuthorityTiming {
    /// Re-check interval while a still-valid CA has not reached `renew_at`.
    pub(super) renew_poll: Duration,
    /// Re-check interval while the owner holds no usable identity, or has
    /// stopped issuing for the current one.
    pub(super) blocked_poll: Duration,
    /// First delay after a failed acquisition; doubles up to `max_backoff`.
    pub(super) base_backoff: Duration,
    /// Ceiling for the acquisition backoff.
    pub(super) max_backoff: Duration,
    /// Delay after local candidate-key generation fails. Longer than
    /// `base_backoff` because the failure is local and not request-driven.
    pub(super) candidate_backoff: Duration,
}

impl Default for AuthorityTiming {
    fn default() -> Self {
        Self {
            renew_poll: Duration::from_secs(1),
            blocked_poll: Duration::from_millis(250),
            base_backoff: Duration::from_millis(250),
            max_backoff: MAX_RETRY_BACKOFF,
            candidate_backoff: Duration::from_secs(5),
        }
    }
}
