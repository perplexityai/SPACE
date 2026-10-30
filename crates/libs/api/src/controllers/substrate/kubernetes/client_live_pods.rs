//! Per-client index of live sandbox pods, maintained from the sandbox-pod
//! reflector's watch events, so the live cap counts a client's sandboxes
//! without scanning the whole cache on every check.
//!
//! The index maps a `space.pplx.net/client-id` label value to the sandboxes
//! that have at least one qualifying pod carrying it: a sandbox or restore pod
//! that isn't terminal. Pending and terminating pods count, because they hold
//! scheduler capacity until they're gone. A sandbox leaves a value's set only
//! when its last qualifying pod does. Keyed by pod name, so repeated and
//! reordered watch events land idempotently, and rebuilt whole on a re-list
//! (`Init`, `InitApply`, `InitDone`), like [`super::sandbox_pod_view`]'s
//! network-slot index.
//!
//! The index also records whether it can be trusted. A watch that breaks
//! after its initial list keeps serving its old index while readiness stays
//! green, so a count answers [`LiveSandboxCountUnknown::PodCacheStale`]:
//!
//! - from a watch error, or the end of the watch stream, until the next live
//!   event or completed re-list;
//! - during a re-list, until its rebuilt index is swapped in.
//!
//! A healthy watch that's merely quiet stays trusted however long it's quiet.
//! The watcher reconnects on its own when a watch delivers nothing for its
//! idle timeout, so a dead connection surfaces as a reconnect, which replays
//! missed events from the last resource version, or as an error or re-list.
//!
//! A stale count is never used to refuse: the cap then checks the pending set
//! alone (see `crate::client_live_cap`), so a frozen index can't keep a client
//! refused for sandboxes it has already deleted.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, PoisonError, RwLock},
};

use kube::runtime::watcher;
use pplx_space_common_core::types::{ClientId, SandboxId};

use super::sandbox_pod_view::{SandboxPodKey, SandboxPodView};
use crate::controllers::substrate::{
    CountedIds, LiveCountExclusions, LiveSandboxCount, LiveSandboxCountUnknown,
};

#[derive(Clone, Default)]
pub(super) struct ClientLivePods {
    index: Arc<RwLock<ClientLivePodsState>>,
}

/// The index and its freshness sit under one lock, so a count never reads
/// a fresh flag and then an index a re-list has since started replacing.
#[derive(Default)]
struct ClientLivePodsState {
    live: ClientLivePodIndex,
    rebuilding: Option<ClientLivePodIndex>,
    /// Set by a watch error or a re-list's start; cleared by the next live
    /// event or the re-list's swap.
    watch_failing: bool,
}

#[derive(Default)]
struct ClientLivePodIndex {
    /// Pod name -> the label value and sandbox it counts for. Holds only
    /// qualifying pods. Keyed by name alone because the sandbox-pod watch is
    /// scoped to one namespace.
    by_pod: HashMap<Box<str>, (ClientId, SandboxId)>,
    /// Label value -> sandbox -> its qualifying pod count.
    by_client: HashMap<ClientId, HashMap<SandboxId, u32>>,
}

/// The label value and sandbox a pod counts for, when it qualifies.
fn qualifying(pod: &SandboxPodView) -> Option<(ClientId, SandboxId)> {
    if pod.is_terminal() {
        return None;
    }
    let client = pod.client_id()?;
    let sandbox = pod
        .annotation_or_label(SandboxPodKey::SandboxId)
        .and_then(|value| value.parse::<SandboxId>().ok())?;
    Some((client, sandbox))
}

impl ClientLivePodIndex {
    fn upsert(&mut self, pod: &SandboxPodView) {
        let next = qualifying(pod);
        if self.by_pod.get(pod.name()) == next.as_ref() {
            return;
        }
        self.remove(pod.name());
        let Some((client, sandbox)) = next else {
            return;
        };
        *self
            .by_client
            .entry(client.clone())
            .or_default()
            .entry(sandbox)
            .or_default() += 1;
        self.by_pod.insert(pod.name().into(), (client, sandbox));
    }

    fn remove(&mut self, pod_name: &str) {
        let Some((client, sandbox)) = self.by_pod.remove(pod_name) else {
            return;
        };
        let Some(sandboxes) = self.by_client.get_mut(&client) else {
            return;
        };
        if let Some(pods) = sandboxes.get_mut(&sandbox) {
            *pods = pods.saturating_sub(1);
            if *pods == 0 {
                sandboxes.remove(&sandbox);
            }
        }
        if sandboxes.is_empty() {
            self.by_client.remove(&client);
        }
    }

    fn count(&self, ids: &CountedIds, exclusions: &LiveCountExclusions<'_>) -> u64 {
        let mut counted: HashSet<SandboxId> = HashSet::new();
        let mut add = |sandboxes: &HashMap<SandboxId, u32>| {
            counted.extend(
                sandboxes
                    .keys()
                    .copied()
                    .filter(|sandbox| !exclusions.excludes(*sandbox)),
            );
        };
        match ids {
            CountedIds::Exactly(ids) => {
                for id in ids {
                    if let Some(sandboxes) = self.by_client.get(id) {
                        add(sandboxes);
                    }
                }
            }
            CountedIds::AllExcept(_) => {
                for (id, sandboxes) in &self.by_client {
                    if ids.counts(id) {
                        add(sandboxes);
                    }
                }
            }
        }
        u64::try_from(counted.len()).unwrap_or(u64::MAX)
    }
}

impl ClientLivePods {
    /// Apply one watch event. A re-list (`Init` through `InitDone`) keeps the
    /// count untrusted until the rebuilt index is swapped in: the old index
    /// it replaces may be missing pods created during the watch gap. A live
    /// `Apply` or `Delete`, or the swap, shows the watch is delivering again.
    pub(super) fn apply(&self, event: &watcher::Event<SandboxPodView>) {
        let mut state = self.index.write().unwrap_or_else(PoisonError::into_inner);
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
                state.rebuilding = Some(ClientLivePodIndex::default());
                state.watch_failing = true;
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
            state.watch_failing = false;
        }
    }

    /// The watch returned an error, or its stream ended: stop trusting the
    /// index until the next live event or completed re-list.
    pub(super) fn mark_watch_error(&self) {
        self.index
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .watch_failing = true;
    }

    /// Count the distinct sandboxes, other than `activating` and those in
    /// `already_pending`, with a qualifying pod labeled with an id `ids`
    /// selects. `ready` is the reflector's initial-list readiness.
    pub(super) fn count(
        &self,
        ids: &CountedIds,
        exclusions: &LiveCountExclusions<'_>,
        ready: bool,
    ) -> LiveSandboxCount {
        if !ready {
            return LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheCold);
        }
        let state = self.index.read().unwrap_or_else(PoisonError::into_inner);
        if state.watch_failing {
            return LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheStale);
        }
        LiveSandboxCount::Known(state.live.count(ids, exclusions))
    }
}

#[cfg(test)]
#[path = "client_live_pods/tests.rs"]
mod tests;
