use std::time::Duration;

use futures_util::{Stream, StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    runtime::{
        WatchStreamExt,
        reflector::{ObjectRef, Store},
        watcher::{self, Config as WatcherConfig},
    },
};
use tokio::{
    sync::{broadcast, watch},
    time::{Instant, sleep_until, timeout},
};

use super::sandbox_pod_view::{self, NodeNetworkSlots, SandboxPodView, SandboxPodWriter};

const SANDBOX_POD_LABEL_SELECTOR: &str = "space.pplx.net/managed-by=space-api";
/// Buffer depth for the per-name event broadcast. Sized well above the steady-
/// state event rate so a slow `wait_for` consumer can absorb a burst before
/// being marked lagged.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Tracks whether a reflector has completed its initial `LIST` and the store is
/// safe to read from cache. Cheap to clone (shared `Arc`).
///
/// The sandbox pod reflector exposes it through HTTP `/readyz`; the Node
/// reflector uses it to distinguish a cold cache from a confidently empty
/// cluster without creating a second waiter on the kube store's one-shot
/// readiness notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReflectorReadiness {
    Pending,
    Ready,
    WriterDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReflectorReadyError {
    #[error("reflector writer dropped before initial sync")]
    WriterDropped,
}

#[derive(Clone)]
pub struct ReflectorReady {
    state: watch::Sender<ReflectorReadiness>,
}

impl Default for ReflectorReady {
    fn default() -> Self {
        let (state, _) = watch::channel(ReflectorReadiness::Pending);
        Self { state }
    }
}

impl ReflectorReady {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a handle that reports ready immediately. Use this for the
    /// non-Kubernetes substrate (local dev) where there is no reflector to
    /// gate on.
    pub fn always_ready() -> Self {
        let handle = Self::new();
        handle.mark_ready();
        handle
    }

    pub fn mark_ready(&self) {
        self.transition_to(ReflectorReadiness::Ready);
    }

    pub(crate) fn mark_writer_dropped(&self) {
        self.transition_to(ReflectorReadiness::WriterDropped);
    }

    pub fn is_ready(&self) -> bool {
        *self.state.borrow() == ReflectorReadiness::Ready
    }

    pub(crate) async fn wait_until_ready(&self) -> Result<(), ReflectorReadyError> {
        let mut state = self.state.subscribe();
        loop {
            match *state.borrow_and_update() {
                ReflectorReadiness::Pending => {}
                ReflectorReadiness::Ready => return Ok(()),
                ReflectorReadiness::WriterDropped => {
                    return Err(ReflectorReadyError::WriterDropped);
                }
            }
            if state.changed().await.is_err() {
                return Err(ReflectorReadyError::WriterDropped);
            }
        }
    }

    fn transition_to(&self, next: ReflectorReadiness) {
        self.state.send_if_modified(|current| {
            if *current != ReflectorReadiness::Pending {
                return false;
            }
            *current = next;
            true
        });
    }
}

/// Per-pod-name event signaller backed by a `broadcast` channel populated by
/// the sandbox-pod reflector watch loop. Allows callers of `read_sandbox_pod`
/// to wait for the next reflector update for a specific pod with a bounded
/// budget, instead of falling straight through to a kube-apiserver `GET`
/// during the brief window between pod creation/scheduling and the reflector
/// observing the bind event.
#[derive(Clone)]
pub struct PodEvents {
    tx: broadcast::Sender<String>,
}

impl PodEvents {
    fn new() -> (Self, broadcast::Sender<String>) {
        let (tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        (Self { tx: tx.clone() }, tx)
    }

    #[cfg(test)]
    pub(super) fn for_test() -> Self {
        Self::new().0
    }

    /// Subscribe to the raw per-name event stream. Used by waiters that must
    /// re-check their own store condition on *every* wakeup — including
    /// `Lagged` — rather than waiting for a specific pod name. An absence
    /// waiter can't use [`PodEvents::wait_for`] because that swallows `Lagged`
    /// and keeps blocking: if the target pod's delete signal was one of the
    /// dropped messages there may be no later event for it, stranding the wait
    /// even though the store already removed the pod.
    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }

    /// Wait up to `budget` for a reflector event referencing `pod_name`.
    /// The broadcast carries every touched pod name, so the signal fires on pod
    /// add/modify *and* delete events; a delete signal lets a waiter observe
    /// pod absence (by re-checking the reflector `Store<SandboxPodView>`)
    /// without polling the apiserver.
    ///
    /// Returns `true` only if such an event was definitively observed within
    /// the budget. `Lagged` (consumer fell behind, broadcast dropped messages)
    /// is *not* treated as a hit: under burst the broadcast carries every
    /// sandbox-pod event and lag on unrelated pods would otherwise collapse
    /// the wait and force a `GET` fallback, recreating the APF spike this
    /// path is meant to absorb. On `Lagged` we simply keep waiting; the
    /// reflector store is updated by the same task that broadcasts, so
    /// callers should still re-check the cache after this returns regardless
    /// of the boolean result, in case the store caught up while the broadcast
    /// receiver was lagging.
    pub async fn wait_for(&self, pod_name: &str, budget: Duration) -> bool {
        let mut rx = self.tx.subscribe();
        let deadline = Instant::now() + budget;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            match timeout(remaining, rx.recv()).await {
                Ok(Ok(name)) if name == pod_name => return true,
                Ok(Ok(_)) => continue,
                Ok(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                    tracing::warn!(
                        pod_name,
                        skipped,
                        "sandbox pod reflector event broadcast lagged; continuing wait"
                    );
                    continue;
                }
                Ok(Err(broadcast::error::RecvError::Closed)) => return false,
                Err(_) => return false,
            }
        }
    }
}

/// Outcome of waiting for a sandbox pod to disappear from the reflector store.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum ReflectorAbsence {
    /// The reflector store shows the pod gone — its delete was observed through
    /// the shared watch, with no apiserver `GET`.
    Confirmed,
    /// The wait budget elapsed with the pod still present in the store. The
    /// caller should fall back to a single direct apiserver `GET` to decide
    /// (reflector lag or watch failure).
    Pending,
}

/// Wait up to `deadline` for `pod_name` to disappear from the reflector
/// `Store<SandboxPodView>`, driven by the `PodEvents` delete signal rather than a
/// fixed-interval apiserver poll. This opens no new watch; it rides the shared
/// reflector.
///
/// Only a **present-then-absent transition** in the store counts as proof the
/// reflector observed the delete: kube's reflector drops a pod from the store
/// solely when it processes a `Delete` event, or when a re-list no longer
/// lists it (which is itself a delete the watch observed). A pod that is merely
/// *uncached* — never seen present during the wait — is ambiguous: it may be a
/// delete observed in a gap, but it may equally be a cold-cache miss (watch
/// restart / reflector lag) for a pod that still exists as `Terminating`. A
/// bare cache miss is therefore never confirmed; instead the wait runs out the
/// budget (in case the reflector catches up and observes a real transition) and
/// otherwise returns [`ReflectorAbsence::Pending`] for the caller's `GET`. A
/// name-matching event with the store absent is *not* a shortcut either: the
/// broadcast carries no event type, and a reflector re-list broadcasts buffered
/// `InitApply` names for pods that still exist before swapping them into the
/// readable store, so a matching name + absent store can be a live pod, not a
/// delete. Only the `seen_present`-then-absent transition is reliable.
///
/// `present_before_delete` seeds the seen-present state: pass `true` when the
/// caller observed the pod in the store *before* issuing the `DELETE` (the
/// normal suspend case — the pod is a running execution). This closes the
/// fast-delete race where the reflector removes the pod between the accepted
/// `DELETE` and this call: the store is already absent on entry, but because we
/// know it was genuinely present, that absence is a real present-then-absent
/// transition and confirms immediately instead of waiting out the budget. It is
/// safe because the live store retains its prior contents through a re-list
/// (kube swaps the rebuilt buffer in only at `InitDone`), so a previously-cached
/// pod never reads as transiently absent — only a real delete removes it.
///
/// The wait consumes the full budget up to `deadline` in the unconfirmed case —
/// it does not give up early — so a pod that is still `Terminating` mid-budget
/// but deleted before the deadline is still confirmed, and a cold-cache miss
/// does not collapse to an immediate timeout right after the DELETE.
///
/// Returns [`ReflectorAbsence::Confirmed`] once a pod that was (or was seeded as)
/// present disappears from the store, or [`ReflectorAbsence::Pending`] if the
/// budget elapses without such a transition.
pub(super) async fn wait_for_pod_absence(
    pod_store: &Store<SandboxPodView>,
    namespace: &str,
    pod_events: &PodEvents,
    pod_name: &str,
    deadline: Instant,
    present_before_delete: bool,
) -> ReflectorAbsence {
    let key = ObjectRef::<SandboxPodView>::new(pod_name).within(namespace);
    // Subscribe directly rather than via `PodEvents::wait_for`: we re-check the
    // store on *every* wakeup, including `Lagged`, so a delete signal dropped
    // under burst can't strand us for the whole budget while the store has
    // already removed the pod. Any wakeup is a cheap O(1) store re-check.
    let mut rx = pod_events.subscribe();
    let mut seen_present = present_before_delete;
    loop {
        match pod_store.get(&key) {
            Some(_) => seen_present = true,
            // Present-then-absent: the reflector processed the pod's delete.
            None if seen_present => return ReflectorAbsence::Confirmed,
            // Never seen present yet — a bare cache miss is not proof; keep
            // waiting in case the reflector catches up and observes a real one.
            None => {}
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ReflectorAbsence::Pending;
        }
        match timeout(remaining, rx.recv()).await {
            // Any event (matching pod or not), a lagged burst, or the budget
            // elapsing: loop back and let the store re-check at the top resolve
            // the outcome. We deliberately do NOT shortcut on a name-matching
            // event with the store absent: the broadcast carries no event type,
            // and during a reflector re-list kube-runtime buffers `InitApply`
            // events for pods that still exist, broadcasting their names before
            // `InitDone` swaps the buffer into the readable store — so a matching
            // name + absent store is not proof of a delete. Only the reliable
            // `seen_present`-then-absent transition (checked at the loop top)
            // confirms; everything else falls through to the caller's GET.
            Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) | Err(_) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // Reflector task ended; no further events will arrive. Don't
                // busy-loop on the closed receiver and don't give up the budget
                // early — wait it out so a delete completing at the apiserver
                // can still be confirmed by the caller's GET, then stop.
                sleep_until(deadline).await;
                return if seen_present && pod_store.get(&key).is_none() {
                    ReflectorAbsence::Confirmed
                } else {
                    ReflectorAbsence::Pending
                };
            }
        }
    }
}

/// Starts a kube reflector that watches sandbox pods in `namespace` and
/// populates a `Store<SandboxPodView>`. Each watcher event is converted to the
/// slim [`SandboxPodView`] before the store sees it, so the cache never holds
/// a full `Pod`. The watcher runs in a detached background task for the
/// lifetime of the process. The returned store is cheap to clone (internally
/// an `Arc`) and is safe to call from any sync context.
///
/// The returned [`NodeNetworkSlots`] is the per-node network-slot index,
/// written from the same events just before the store.
///
/// The returned `PodEvents` is fed by the same watch stream and lets callers
/// wait for the next add/modify/delete event for a specific pod name.
///
/// The returned `ReflectorReady` is flipped to `true` once the reflector's
/// initial `LIST` populates the store. The HTTP `/readyz` probe gates the
/// Service endpoint on this flag, so kube-apiserver does not see traffic on
/// a freshly-spawned API replica before its cache is warm. Returns
/// immediately — the boot budget is enforced by the Kubernetes startup probe
/// (which restarts the pod after `failureThreshold * periodSeconds`), not by
/// an inline timeout here.
pub fn start(
    client: Client,
    namespace: String,
) -> (
    Store<SandboxPodView>,
    NodeNetworkSlots,
    PodEvents,
    ReflectorReady,
) {
    let pods: Api<Pod> = Api::namespaced(client, &namespace);
    let cfg = WatcherConfig::default().labels(SANDBOX_POD_LABEL_SELECTOR);
    let (store, network_slots, writer) = sandbox_pod_view::sandbox_pod_cache();
    let stream = watcher::watcher(pods, cfg)
        .map_ok(sandbox_pod_view::sandbox_pod_view_event)
        .default_backoff();
    let (events, events_tx) = PodEvents::new();
    let ready = ReflectorReady::new();

    let log_namespace = namespace.clone();
    tokio::spawn(async move {
        reflect_sandbox_pods(stream.boxed(), writer, events_tx, &log_namespace).await;
    });

    let ready_for_task = ready.clone();
    let store_for_ready = store.clone();
    let ready_namespace = namespace.clone();
    tokio::spawn(async move {
        match store_for_ready.wait_until_ready().await {
            Ok(()) => {
                ready_for_task.mark_ready();
                tracing::info!(
                    namespace = %ready_namespace,
                    pod_count = store_for_ready.len(),
                    "sandbox pod reflector initial sync complete; marked ready"
                );
            }
            Err(err) => {
                ready_for_task.mark_writer_dropped();
                tracing::warn!(
                    namespace = %ready_namespace,
                    error = %err,
                    "sandbox pod reflector writer dropped before initial sync; readiness will stay false until the pod is restarted"
                );
            }
        }
    });

    (store, network_slots, events, ready)
}

/// Drains the sandbox-pod watcher stream: applies each event to the slot index
/// and store, then broadcasts the touched pod's name on `events_tx`. Applied
/// (add/modify, including re-list `InitApply`) and deleted pods are both
/// broadcast, matching `touched_objects()`. A re-list `InitApply` only fills the
/// store's private buffer, so its broadcast can precede the pod becoming
/// readable at `InitDone`. Waiters in `PodEvents::wait_for` re-check the store
/// after a signal, so a delete signal lets them observe pod absence without an
/// apiserver poll.
async fn reflect_sandbox_pods<S>(
    mut stream: S,
    mut writer: SandboxPodWriter,
    events_tx: broadcast::Sender<String>,
    namespace: &str,
) where
    S: Stream<Item = Result<watcher::Event<SandboxPodView>, watcher::Error>> + Unpin,
{
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                writer.apply_watcher_event(&event);
                if let watcher::Event::Apply(pod)
                | watcher::Event::InitApply(pod)
                | watcher::Event::Delete(pod) = &event
                {
                    let _ = events_tx.send(pod.name().to_string());
                }
            }
            Err(err) => {
                tracing::warn!(
                    namespace = %namespace,
                    error = %err,
                    "sandbox pod reflector watch error"
                );
            }
        }
    }
    tracing::warn!(
        namespace = %namespace,
        "sandbox pod reflector stream ended"
    );
}

#[cfg(test)]
mod tests;
