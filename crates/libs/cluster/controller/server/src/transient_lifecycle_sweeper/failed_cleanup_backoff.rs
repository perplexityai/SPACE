//! Per-sandbox retry spacing for the failed-cleanup pass.
//!
//! The pass lists the same `failed` rows every tick until their retire
//! succeeds, and a retire that fails for a reason a retry will not soon fix
//! used to be attempted, and warned about, on every one of them. This spaces a
//! sandbox's attempts out after consecutive failures and, once the spacing
//! reaches its cap, reports the sandbox once and keeps retrying quietly.
//!
//! A waiting sandbox is neither probed nor retired, so it is not repaired to
//! `started` either until its wait ends, even if its VM came back.
//! A quiet sandbox's attempts emit no `cleanup_started` event, so the
//! `cleanup_completed` that ends one has no `cleanup_started` before it.
//!
//! The state lives in memory: a controller restart or a change of leader
//! forgets it, so each stuck sandbox starts again from its first failure, with
//! up to four more `cleanup_failed` warnings and one more `persistent_failure`
//! report before it is back at the capped interval, and the per-tick summary
//! warns on each of those attempts too. It never skips a cleanup. Time is
//! counted in ticks of the pass, since the pass can only act on a tick.

use std::collections::HashMap;

use pplx_space_common_core::types::SandboxId;

/// Longest wait, in ticks of the failed-cleanup pass, between two attempts to
/// retire one sandbox.
pub(super) const MAX_BACKOFF_TICKS: u64 = 16;

/// Consecutive failures after which a sandbox's retire is reported as
/// persistently failing, once, and later failures stop warning. The wait has
/// reached [`MAX_BACKOFF_TICKS`] by then.
pub(super) const QUIET_AFTER_FAILURES: u32 = 5;

/// Ticks without a failed attempt after which a sandbox's entry is dropped.
/// A sandbox still failing fails again within this, once the listing reaches
/// it, as long as the failed backlog is fewer than 128 listing batches, so the
/// rotation comes back to each sandbox within that many ticks; past that, a
/// stuck sandbox starts again from its first failure. One that left the
/// listing by another path (repaired, deleted, cleaned up elsewhere) would
/// otherwise be kept forever.
const FORGET_AFTER_TICKS: u64 = 8 * MAX_BACKOFF_TICKS;

/// Ticks to wait after the `consecutive_failures`-th failure in a row: the
/// next tick after the first, then twice as long after each further one, up
/// to [`MAX_BACKOFF_TICKS`].
fn backoff_ticks(consecutive_failures: u32) -> u64 {
    let doublings = consecutive_failures.saturating_sub(1).min(u64::BITS - 1);
    (1_u64 << doublings).min(MAX_BACKOFF_TICKS)
}

#[derive(Debug, Clone, Copy)]
struct FailingRetire {
    consecutive_failures: u32,
    last_failed_tick: u64,
}

/// What a sandbox's failed retire should be reported as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FailureReport {
    /// Warn, as for any failure.
    Warn,
    /// This failure reached [`QUIET_AFTER_FAILURES`]: report it once as
    /// persistent.
    Persistent,
    /// Already reported as persistent: no warning.
    Quiet,
}

#[derive(Debug, Default)]
pub(super) struct FailedCleanupBackoff {
    /// Ticks of the failed-cleanup pass so far.
    tick: u64,
    failing: HashMap<SandboxId, FailingRetire>,
}

impl FailedCleanupBackoff {
    /// Count one tick of the pass, and drop the entries it has outlived.
    pub(super) fn start_tick(&mut self) {
        self.tick += 1;
        let tick = self.tick;
        self.failing
            .retain(|_, failing| tick - failing.last_failed_tick <= FORGET_AFTER_TICKS);
    }

    /// Whether the sandbox's last retire failed too recently to try again
    /// this tick.
    pub(super) fn waiting(&self, sandbox_id: SandboxId) -> bool {
        self.failing.get(&sandbox_id).is_some_and(|failing| {
            self.tick < failing.last_failed_tick + backoff_ticks(failing.consecutive_failures)
        })
    }

    /// Whether the sandbox has already been reported as persistently failing.
    pub(super) fn quiet(&self, sandbox_id: SandboxId) -> bool {
        self.failing
            .get(&sandbox_id)
            .is_some_and(|failing| failing.consecutive_failures >= QUIET_AFTER_FAILURES)
    }

    /// Record a failed retire, and say how to report it.
    pub(super) fn record_failure(&mut self, sandbox_id: SandboxId) -> FailureReport {
        let failing = self.failing.entry(sandbox_id).or_insert(FailingRetire {
            consecutive_failures: 0,
            last_failed_tick: self.tick,
        });
        failing.consecutive_failures = failing.consecutive_failures.saturating_add(1);
        failing.last_failed_tick = self.tick;
        match failing.consecutive_failures.cmp(&QUIET_AFTER_FAILURES) {
            std::cmp::Ordering::Less => FailureReport::Warn,
            std::cmp::Ordering::Equal => FailureReport::Persistent,
            std::cmp::Ordering::Greater => FailureReport::Quiet,
        }
    }

    /// Forget a sandbox whose retire ended without failing, so a later failure
    /// starts over.
    pub(super) fn forget(&mut self, sandbox_id: SandboxId) {
        self.failing.remove(&sandbox_id);
    }
}
