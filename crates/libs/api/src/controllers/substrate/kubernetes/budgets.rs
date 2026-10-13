//! Wall-clock budgets a [`KubernetesSubstrate`](super::KubernetesSubstrate)
//! carries as values rather than reading from its constants, so a test can
//! shorten them while Postgres, the scripted apiserver and the scripted worker
//! stay real.

use std::time::Duration;

use tokio::time::Instant;

/// The budgets of `create_execution`'s readiness wait and preserve decision,
/// `suspend_execution`'s durability wait, started warm-pool adoption's
/// split-receipt wait and teardown's worker connect.
///
/// Production uses [`Default`], which is exactly the constants each field
/// names, so production deadlines, error messages and logs are unchanged.
/// Other paths that share a constant (wake, start and access readiness, and
/// the other warm-pool setup RPCs) still read it directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SubstrateBudgets {
    /// [`super::CREATE_READY_BUDGET`]: the outer bound on a create's
    /// readiness wait. Its phase deadline also bounds the readiness phases of
    /// a create that joins an in-flight activation; that joined wait has no
    /// outer bound.
    pub(super) create_ready: Duration,
    /// [`super::READY_WAIT_PHASE_MARGIN`], as reserved inside
    /// [`Self::create_ready`].
    pub(super) create_ready_phase_margin: Duration,
    /// [`super::PRESERVE_DECISION_BUDGET`].
    pub(super) preserve_decision: Duration,
    /// [`super::adapter::TEARDOWN_SUSPEND_DURABILITY_BUDGET`].
    pub(super) suspend_durability: Duration,
    /// [`super::adapter::WARM_POOL_SETUP_RPC_TIMEOUT`], as the deadline for a
    /// started adoption's worker to advertise split operation receipts.
    pub(super) pooled_split_receipt_wait: Duration,
    /// [`super::POD_WORKER_CONNECT_TIMEOUT`], as teardown's connect bound for
    /// the pod's worker.
    pub(super) teardown_worker_connect: Duration,
}

impl SubstrateBudgets {
    pub(super) const DEFAULT: Self = Self {
        create_ready: super::CREATE_READY_BUDGET,
        create_ready_phase_margin: super::READY_WAIT_PHASE_MARGIN,
        preserve_decision: super::PRESERVE_DECISION_BUDGET,
        suspend_durability: super::adapter::TEARDOWN_SUSPEND_DURABILITY_BUDGET,
        pooled_split_receipt_wait: super::adapter::WARM_POOL_SETUP_RPC_TIMEOUT,
        teardown_worker_connect: super::POD_WORKER_CONNECT_TIMEOUT,
    };

    /// [`super::support::ready_wait_bounds`] within these budgets: the outer
    /// backstop and the phase-aware inner deadline for a readiness wait of
    /// `requested`. The inner wait is clamped to
    /// `create_ready − create_ready_phase_margin`, and the outer backstop is
    /// one margin later, so it never exceeds `create_ready` while the margin
    /// fits inside it.
    pub(super) fn ready_wait_bounds(&self, requested: Duration) -> (Duration, Instant) {
        // The defaults are statically asserted to leave room. A test-shortened
        // pair must too, or the backstop would outlast `create_ready`.
        debug_assert!(
            self.create_ready_phase_margin <= self.create_ready,
            "the phase margin must fit inside the create budget"
        );
        let inner = requested.min(
            self.create_ready
                .saturating_sub(self.create_ready_phase_margin),
        );
        let outer = inner + self.create_ready_phase_margin;
        (outer, Instant::now() + inner)
    }
}

impl Default for SubstrateBudgets {
    fn default() -> Self {
        Self::DEFAULT
    }
}
