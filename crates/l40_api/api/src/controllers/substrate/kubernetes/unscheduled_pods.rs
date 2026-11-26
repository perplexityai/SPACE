//! Reflector-maintained index of unscheduled sandbox pods waiting on
//! `kube-scheduler`, read in O(1) by cold-create cluster admission.
//!
//! A sandbox pod counts while it's neither terminal (`Succeeded` / `Failed`)
//! nor deleting (`deletionTimestamp` set), its `spec.nodeName` is unset or
//! empty, and `kube-scheduler` hasn't already evaluated it and marked
//! `PodScheduled=False, reason=Unschedulable`. Every `space-api`-managed pod in
//! the sandbox namespace (`OnDemand`, `Restore`, `Pooled`) shares one
//! `kube-scheduler` active queue, so unassessed pods across all three roles
//! reflect the true scheduling backlog cold creates contend with; once
//! `kube-scheduler` parks a pod in `unschedulablePods` (for instance a
//! warm-pool refill waiting on autoscaler scale-up in a constrained pool, or a
//! pinned restore waiting on its source node), that pod is no longer an
//! unevaluated queue backlog and doesn't trip the circuit breaker across
//! healthy pools.
//!
//! Keyed by pod name (the reflector watch is scoped to one namespace), updated
//! idempotently on `Apply` / `Delete`, and swapped in whole on `InitDone` so an
//! in-progress re-list never exposes a partial count.
//!
//! Freshness tracks when the watch first stalled (`watch_failing_since`):
//!
//! - `mark_watch_error` and `Event::Init` set `watch_failing_since` via
//!   `get_or_insert`, preserving the original stall timestamp across repeated
//!   failed `LIST` retries;
//! - `Event::Apply`, `Event::Delete`, and `Event::InitDone` clear it.
//!
//! Cold-create admission uses `(last_count, stale_for)` to price both failure
//! modes: hold an active backlog rejection across brief apiserver brownouts,
//! and fail open once `stale_for` exceeds the configured stale-hold window or
//! when the last snapshot was healthy.

use std::{
    collections::HashSet,
    sync::{Arc, PoisonError, RwLock},
    time::Instant,
};

use kube::runtime::watcher;

use super::sandbox_pod_view::SandboxPodView;
use crate::controllers::substrate::{UnscheduledPodBacklog, UnscheduledPodBacklogUnknown};

#[derive(Clone, Default)]
pub(super) struct UnscheduledPods {
    state: Arc<RwLock<UnscheduledPodsState>>,
}

#[derive(Default)]
struct UnscheduledPodsState {
    live: UnscheduledPodIndex,
    rebuilding: Option<UnscheduledPodIndex>,
    /// Timestamp of the earliest unresolved watch error or re-list start;
    /// cleared on the next live event or completed re-list swap.
    watch_failing_since: Option<Instant>,
}

#[derive(Default)]
struct UnscheduledPodIndex {
    /// Names of qualifying unscheduled pods in the sandbox namespace.
    by_pod: HashSet<Box<str>>,
}

fn is_unscheduled_backlog_pod(pod: &SandboxPodView) -> bool {
    !pod.is_terminal()
        && !pod.is_deleting()
        && pod.node_name().is_none_or(str::is_empty)
        && pod.unschedulable_message().is_none()
}

impl UnscheduledPodIndex {
    fn upsert(&mut self, pod: &SandboxPodView) {
        let qualifies = is_unscheduled_backlog_pod(pod);
        if qualifies {
            if !self.by_pod.contains(pod.name()) {
                self.by_pod.insert(pod.name().into());
            }
        } else {
            self.by_pod.remove(pod.name());
        }
    }

    fn remove(&mut self, pod_name: &str) {
        self.by_pod.remove(pod_name);
    }

    fn len(&self) -> u64 {
        u64::try_from(self.by_pod.len()).unwrap_or(u64::MAX)
    }
}

impl UnscheduledPods {
    pub(super) fn apply(&self, event: &watcher::Event<SandboxPodView>) {
        self.apply_at(event, Instant::now());
    }

    pub(super) fn apply_at(&self, event: &watcher::Event<SandboxPodView>, now: Instant) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let delivering = match event {
            watcher::Event::Apply(pod) => {
                state.live.upsert(pod);
                true
            }
            watcher::Event::Delete(pod) => {
                state.live.remove(pod.name());
                true
            }
            watcher::Event::Init => {
                state.rebuilding = Some(UnscheduledPodIndex::default());
                state.watch_failing_since.get_or_insert(now);
                false
            }
            watcher::Event::InitApply(pod) => {
                state.rebuilding.get_or_insert_default().upsert(pod);
                false
            }
            watcher::Event::InitDone => {
                state.live = state.rebuilding.take().unwrap_or_default();
                true
            }
        };
        if delivering {
            state.watch_failing_since = None;
        }
    }

    /// Record that the sandbox-pod watch errored or ended. Preserves an
    /// existing timestamp so repeated retry failures advance `stale_for`
    /// monotonically until a live event or `InitDone` arrives.
    pub(super) fn mark_watch_error(&self) {
        self.mark_watch_error_at(Instant::now());
    }

    pub(super) fn mark_watch_error_at(&self, now: Instant) {
        self.state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .watch_failing_since
            .get_or_insert(now);
    }

    /// Read the unscheduled-pod backlog. `ready` is the pod reflector's
    /// initial-list readiness.
    pub(super) fn count(&self, ready: bool) -> UnscheduledPodBacklog {
        self.count_at(ready, Instant::now())
    }

    pub(super) fn count_at(&self, ready: bool, now: Instant) -> UnscheduledPodBacklog {
        if !ready {
            return UnscheduledPodBacklog::Unknown(UnscheduledPodBacklogUnknown::PodCacheCold);
        }
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let count = state.live.len();
        if let Some(since) = state.watch_failing_since {
            return UnscheduledPodBacklog::Stale {
                last_count: count,
                stale_for: now.saturating_duration_since(since),
            };
        }
        UnscheduledPodBacklog::Fresh(count)
    }
}

#[cfg(test)]
#[path = "unscheduled_pods/tests.rs"]
mod tests;
