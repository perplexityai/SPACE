//! Retry of a failed node-state publication ahead of the next tick.
//!
//! Every tick publishes the facets that go out in full each tick (sandbox
//! inventory, template inventory and node status). When ClusterApi is
//! unreachable for one of those publishes, waiting a whole tick leaves the
//! cluster without the node's state for up to a poll interval, and two lost
//! publications plus a long tick outlast the window the cluster trusts a
//! node's status for (`node_status_freshness`). So a pass whose full publish
//! of one of them fails with [`OutboundError::ClusterUnavailable`] schedules
//! one retry of the facets that failed, with jittered exponential backoff.
//! The first pass that leaves none of them to retry ends the episode and
//! returns the node to the tick cadence. Any other failure (a rejection, a
//! deadline, a node-local collection failure) is not retried: the facet
//! waits for the next tick, as it always did, and a facet the episode was
//! retrying that fails that way leaves the retry set. The episode's end logs
//! `outbound publish recovered` when every facet it retried has since
//! settled: published in full, or skipped because its outbound sync is
//! disabled or no cluster is configured. It logs `outbound publish failed in
//! a way a retry cannot fix`, naming them, when some dropped out that way and
//! have not settled since.
//! A pass cut short by shutdown is not recorded.
//!
//! The worker runs one item at a time and a retry is just another pass on
//! it, so a retry never overlaps another publish. There is only ever one
//! pending retry. Any pass that publishes a failing facet in full replaces
//! it: a tick that fires during backoff is that backoff's attempt, and a
//! dirty sync of a whole facet (node-manager marks node status dirty when
//! its slot pool changes) feeds the schedule the same way. Each publication
//! is rebuilt from the node's current state and applies as an upsert or a
//! full-snapshot replace on the cluster, so repeating one cannot double-apply
//! anything.
//!
//! Item-scoped dirty refs, and the facets only dirty refs publish (network
//! policy), are not retried here: they keep their persisted per-ref backoff
//! (`dirty_state`) and go out with whichever pass runs once they are due, a
//! retry pass included.
//!
//! Attempt budget: the backoff step doubles from [`FIRST_RETRY_STEP`] to
//! [`MAX_RETRY_STEP`], and each delay, counted from the end of the pass
//! before it, whatever started that pass, is drawn uniformly from the upper
//! half of its step. With instant failures the shortest delays put retry
//! passes at 0.5, 1.5, 3.5, 7.5, 15.5, 23.5 s and every 8 s after, so retries
//! add up to 10 retry passes in a node's first minute of failure and at most
//! 8 a minute once the step reaches its 16 s cap (7.5-15 s in when failures
//! are instant; later when passes take time), about 5 a minute on average.
//! Each retry pass sends one `PublishNodeState` per failing facet, up to
//! three, so a fleet of N nodes adds at most 30N calls in the first minute
//! and 24N a minute after it. Ticks and dirty-driven publishes run on top of
//! that; a failing dirty sync reschedules the retry from its own end, which
//! can bring it earlier but keeps every retry at least half a step after the
//! pass before it.
//!
//! Recovery: the scheduled delay is capped at 16 s after a pass ends, so
//! while a facet is being retried, its next attempt is due within 16 s of
//! the last pass that failed it with ClusterApi unavailable. Worker
//! occupancy (queued work runs first), collection and RPC latency can
//! extend recovery past that. Across a fleet the republishes are a denser
//! burst than its ticks, which the nodes' start times spread across the
//! whole interval.

use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::Instant;

use crate::dirty_state::FacetRef;
use crate::facets::{FacetKind, FacetTickMode, OutboundError};

/// The backoff step after the first failed pass.
const FIRST_RETRY_STEP: Duration = Duration::from_secs(1);

/// The largest backoff step: well below the 60 s default poll interval, so a
/// retried facet's next attempt is due within 16 s of the pass that last
/// failed it.
const MAX_RETRY_STEP: Duration = Duration::from_secs(16);

/// Why an outbound pass runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboundPass {
    /// A tick, a manual refresh or a dirty sync.
    Scheduled,
    /// The retry of facets the previous pass failed to publish.
    Retry,
}

impl OutboundPass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Retry => "retry",
        }
    }

    /// The `trigger` tag on node-ctl's `reconcile.run` metrics, so a retry
    /// pass counts apart from the scheduled ones.
    pub(crate) fn metric_trigger(self) -> &'static str {
        match self {
            Self::Scheduled => "node_ctl",
            Self::Retry => "node_ctl_retry",
        }
    }
}

/// What one outbound pass did with the full publish of each every-tick facet
/// it handled. A facet is in at most one set.
#[derive(Debug, Default)]
pub(crate) struct OutboundOutcome {
    /// Published, or settled without a publish (sync disabled, or no
    /// cluster configured).
    settled: BTreeSet<FacetKind>,
    /// Failed with ClusterApi unavailable: retried soon.
    retry: BTreeSet<FacetKind>,
    /// Failed some other way: left to the next tick.
    unretried: BTreeSet<FacetKind>,
}

impl OutboundOutcome {
    /// A pass that could not read its dirty state, and so published none of
    /// `refs`. Nothing reached the cluster: the next tick tries again.
    pub(crate) fn none_published(refs: &[FacetRef]) -> Self {
        let mut outcome = Self::default();
        for reference in refs {
            if reference.element.is_none()
                && let Some(facet) = FacetKind::parse(&reference.facet)
            {
                outcome.insert(facet, Slot::Unretried);
            }
        }
        outcome
    }

    /// A facet's full publish succeeded or needed no publish.
    pub(crate) fn settled(&mut self, facet: FacetKind) {
        self.insert(facet, Slot::Settled);
    }

    /// A facet's full publish failed with `error`.
    pub(crate) fn failed(&mut self, facet: FacetKind, error: &OutboundError) {
        let slot = if error.retry_soon() {
            Slot::Retry
        } else {
            Slot::Unretried
        };
        self.insert(facet, slot);
    }

    /// Only the facets every tick publishes count, and callers pass only
    /// full-scope results: an item-scoped success does not stand in for a
    /// full publish that failed.
    fn insert(&mut self, facet: FacetKind, slot: Slot) {
        if facet.plugin().outbound_tick() != FacetTickMode::EveryTickFull {
            return;
        }
        self.settled.remove(&facet);
        self.retry.remove(&facet);
        self.unretried.remove(&facet);
        match slot {
            Slot::Settled => self.settled.insert(facet),
            Slot::Retry => self.retry.insert(facet),
            Slot::Unretried => self.unretried.insert(facet),
        };
    }
}

#[derive(Clone, Copy)]
enum Slot {
    Settled,
    Retry,
    Unretried,
}

/// The pending retry, if any, of the facets the last pass failed to publish.
#[derive(Debug, Default)]
pub(crate) struct OutboundRetry {
    /// Facets whose last full publish failed with ClusterApi unavailable.
    failing: BTreeSet<FacetKind>,
    /// Facets this episode retried that then failed in a way a retry cannot
    /// fix, and have not settled since.
    abandoned: BTreeSet<FacetKind>,
    /// Consecutive passes that failed a facet that way: the backoff exponent.
    failed_passes: u32,
    /// When the pending retry runs.
    due: Option<Instant>,
}

/// How a retry episode, from the first failed pass to the pass that leaves
/// nothing to retry, ended.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct EpisodeEnd {
    /// The passes that failed a facet with ClusterApi unavailable.
    pub(crate) failed_passes: u32,
    /// Retried facets that then failed in a way a retry cannot fix and have
    /// not settled since; empty when every retried facet settled.
    pub(crate) abandoned: BTreeSet<FacetKind>,
}

impl EpisodeEnd {
    pub(crate) fn log(&self, pass: OutboundPass) {
        if self.abandoned.is_empty() {
            tracing::info!(
                pass = pass.as_str(),
                failed_passes = self.failed_passes,
                "outbound publish recovered; back to the tick cadence"
            );
        } else {
            let facets = self
                .abandoned
                .iter()
                .map(|facet| facet.name())
                .collect::<Vec<_>>();
            tracing::info!(
                pass = pass.as_str(),
                failed_passes = self.failed_passes,
                facets = ?facets,
                "outbound publish failed in a way a retry cannot fix; back to the tick cadence"
            );
        }
    }
}

impl OutboundRetry {
    /// Resolves when the pending retry is due; never, when none is.
    pub(crate) async fn until_due(&self) {
        match self.due {
            Some(due) => tokio::time::sleep_until(due).await,
            None => std::future::pending().await,
        }
    }

    /// Take the pending retry: the full refs of the failing facets.
    pub(crate) fn take(&mut self) -> Vec<FacetRef> {
        self.due = None;
        self.failing
            .iter()
            .map(|facet| FacetRef::new(facet.name(), None::<String>))
            .collect()
    }

    /// Update the schedule from a finished pass. A pass that failed a facet
    /// with ClusterApi unavailable replaces any pending retry with the next
    /// backoff step. A pass that touched no failing facet leaves the pending
    /// retry as it was. A pass that leaves nothing to retry ends the episode
    /// and returns the node to the tick cadence; the end says which retried
    /// facets, if any, were dropped for failing in a way a retry cannot fix.
    pub(crate) fn record(
        &mut self,
        pass: OutboundPass,
        outcome: OutboundOutcome,
        now: Instant,
    ) -> Option<EpisodeEnd> {
        for facet in &outcome.settled {
            self.failing.remove(facet);
            self.abandoned.remove(facet);
        }
        for facet in &outcome.unretried {
            if self.failing.remove(facet) {
                self.abandoned.insert(*facet);
            }
        }
        if !outcome.retry.is_empty() {
            for facet in &outcome.retry {
                self.abandoned.remove(facet);
            }
            self.failing.extend(outcome.retry);
            self.failed_passes = self.failed_passes.saturating_add(1);
            let delay = jittered(retry_step(self.failed_passes));
            self.due = Some(now + delay);
            let facets = self
                .failing
                .iter()
                .map(|facet| facet.name())
                .collect::<Vec<_>>();
            tracing::info!(
                pass = pass.as_str(),
                failed_passes = self.failed_passes,
                retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                facets = ?facets,
                "outbound publish failed; retry scheduled"
            );
            return None;
        }
        if !self.failing.is_empty() || self.failed_passes == 0 {
            return None;
        }
        self.due = None;
        Some(EpisodeEnd {
            failed_passes: std::mem::take(&mut self.failed_passes),
            abandoned: std::mem::take(&mut self.abandoned),
        })
    }
}

/// The backoff step after `failed_passes` consecutive failed passes:
/// 1, 2, 4, 8, then 16 s.
fn retry_step(failed_passes: u32) -> Duration {
    let doublings = failed_passes.saturating_sub(1).min(31);
    FIRST_RETRY_STEP
        .saturating_mul(1 << doublings)
        .min(MAX_RETRY_STEP)
}

/// A random delay in the upper half of `step`. The half-step floor bounds how
/// often a node retries; the spread keeps a fleet whose cluster recovers from
/// retrying in lockstep.
fn jittered(step: Duration) -> Duration {
    step.mul_f64(0.5 + 0.5 * rand::random::<f64>())
}

#[cfg(test)]
mod tests;
