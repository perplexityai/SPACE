//! Node refusals of sandbox pods, observed by one watch per process.
//!
//! A pod whose sandbox the node refuses never gets an IP: kubelet records a
//! `FailedCreatePodSandBox` Event and retries the pod until the create's
//! readiness deadline. A create waiting for its pod's IP reads the refusal
//! from those Events to fail fast instead.
//!
//! kube-apiserver keeps no watch cache for Events by default (its default
//! watch cache sizes set `events` to 0), so every LIST of Events, and every
//! watch from `"0"`, reads all of the namespace's Events from etcd whatever
//! its field selector. One watch per process, filtered to the reason, carries
//! every refusal; each is routed by its pod's UID to the waits registered for
//! that pod, and kept for [`REFUSAL_RETENTION`] so a refusal recorded before
//! its create registers still reaches it.

use std::collections::{HashMap, VecDeque};
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::{
    Api, Client,
    runtime::{
        WatchStreamExt,
        watcher::{self, Config as WatcherConfig},
    },
};
use pplx_space_node_mgr_client::MEMORY_ADMISSION_REFUSAL_PREFIX;
use tokio::{sync::Notify, time::Instant};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::controllers::substrate::SubstrateError;

/// The kubelet event reason for a pod sandbox the runtime failed to start.
const FAILED_CREATE_POD_SANDBOX_REASON: &str = "FailedCreatePodSandBox";

/// The marker `annotateStatus` in `shim/space.go` writes for node-manager's
/// `InvalidArgument`: the request fails identically on every node. sbx-mgr's
/// `failure_status` decides which node-manager codes reach the shim intact.
const INVALID_ARGUMENT_MARKER: &str = "[space-err=InvalidArgument]";
const RESOURCE_EXHAUSTED_MARKER: &str = "[space-err=ResourceExhausted]";

/// How long a refusal is kept for a wait that has not registered yet. A
/// create registers as soon as its pod create returns, so this only needs to
/// cover that gap and the watch's own lag, with a wide margin.
pub(super) const REFUSAL_RETENTION: Duration = Duration::from_secs(300);

/// The most pods whose refusals are kept at once; the oldest go first.
const MAX_RETAINED_PODS: usize = 4096;

/// The most distinct refusal messages kept for one pod. Kubelet folds a
/// repeated message into one Event, so a pod rarely has more than one.
const MAX_REFUSALS_PER_POD: usize = 4;

/// The refusal `event_message` reports, or `None` when it reports none that
/// ends the wait. Only explicit node-manager verdicts end it; other failures
/// remain eligible for kubelet's retry of the same pod. A memory refusal ends
/// it only with `fail_fast_on_memory_refusal`.
pub(super) fn pod_sandbox_rejection_error(
    event_message: &str,
    fail_fast_on_memory_refusal: bool,
) -> Option<SubstrateError> {
    if let Some((_, reason)) = event_message.split_once(INVALID_ARGUMENT_MARKER) {
        return Some(SubstrateError::InvalidRequest {
            message: format!(
                "the node refused to start the sandbox: {}",
                reason.trim_start_matches(':').trim()
            ),
        });
    }
    if !fail_fast_on_memory_refusal {
        return None;
    }
    let (_, reason) = event_message.split_once(RESOURCE_EXHAUSTED_MARKER)?;
    // ResourceExhausted also covers transient queue/slot pressure. The
    // allocation path's explicit prefix distinguishes memory admission.
    let (_, reason) = reason.split_once(MEMORY_ADMISSION_REFUSAL_PREFIX)?;
    Some(SubstrateError::Unavailable {
        message: format!("{MEMORY_ADMISSION_REFUSAL_PREFIX} {}", reason.trim()),
    })
}

/// The refusals the process's Events watch has seen, and the waits for them.
/// Cheap to clone. A default value has no watch: its waits are never refused.
#[derive(Clone, Default)]
pub(super) struct PodSandboxRejections {
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Refusal messages by pod UID. Only messages that end some wait are kept.
    refusals: HashMap<Box<str>, Retained>,
    /// The UIDs in `refusals`, oldest first, for eviction.
    order: VecDeque<(Instant, Box<str>)>,
    waiters: HashMap<Box<str>, Waiters>,
}

struct Retained {
    at: Instant,
    messages: Vec<Box<str>>,
}

struct Waiters {
    notify: Arc<Notify>,
    count: usize,
}

impl State {
    fn lock(shared: &Shared) -> std::sync::MutexGuard<'_, Self> {
        shared.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keep `message` for `pod_uid`. `true` when it was not kept already.
    fn retain(&mut self, pod_uid: &str, message: &str, now: Instant) -> bool {
        self.evict(now);
        if let Some(retained) = self.refusals.get_mut(pod_uid) {
            if retained.messages.len() >= MAX_REFUSALS_PER_POD
                || retained.messages.iter().any(|kept| &**kept == message)
            {
                return false;
            }
            retained.messages.push(message.into());
            return true;
        }
        if self.refusals.len() >= MAX_RETAINED_PODS
            && let Some((_, oldest)) = self.order.pop_front()
        {
            self.refusals.remove(&oldest);
        }
        self.refusals.insert(
            pod_uid.into(),
            Retained {
                at: now,
                messages: vec![message.into()],
            },
        );
        self.order.push_back((now, pod_uid.into()));
        true
    }

    fn evict(&mut self, now: Instant) {
        while let Some((at, _)) = self.order.front()
            && now.saturating_duration_since(*at) >= REFUSAL_RETENTION
        {
            let (_, pod_uid) = self.order.pop_front().expect("front exists");
            self.refusals.remove(&pod_uid);
        }
    }

    fn refusal(
        &self,
        pod_uid: &str,
        fail_fast_on_memory_refusal: bool,
        now: Instant,
    ) -> Option<SubstrateError> {
        let retained = self.refusals.get(pod_uid)?;
        if now.saturating_duration_since(retained.at) >= REFUSAL_RETENTION {
            return None;
        }
        retained
            .messages
            .iter()
            .find_map(|message| pod_sandbox_rejection_error(message, fail_fast_on_memory_refusal))
    }
}

impl PodSandboxRejections {
    /// Start the process's watch of `namespace`'s sandbox refusals on
    /// `task_tracker`, until `shutdown_token` is cancelled.
    pub(super) fn start(
        client: Client,
        namespace: &str,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) -> Self {
        let rejections = Self::default();
        let events: Api<Event> = Api::namespaced(client, namespace);
        task_tracker.spawn(watch_refusals(
            events,
            rejections.clone(),
            namespace.to_owned(),
            shutdown_token,
        ));
        rejections
    }

    /// Register a wait for `pod_uid`'s refusal. The wait sees every refusal
    /// kept for the pod, including those recorded before it registered.
    pub(super) fn register(&self, pod_uid: &str) -> RejectionWait {
        let notify = {
            let mut state = State::lock(&self.shared);
            let waiters = state
                .waiters
                .entry(pod_uid.into())
                .or_insert_with(|| Waiters {
                    notify: Arc::default(),
                    count: 0,
                });
            waiters.count += 1;
            Arc::clone(&waiters.notify)
        };
        RejectionWait {
            shared: Arc::clone(&self.shared),
            pod_uid: pod_uid.into(),
            notify,
        }
    }

    /// Keep `event`'s refusal, if it reports one, and wake its pod's waits.
    fn observe(&self, event: &Event, now: Instant) {
        if event.reason.as_deref() != Some(FAILED_CREATE_POD_SANDBOX_REASON) {
            return;
        }
        let (Some(pod_uid), Some(message)) = (
            event.involved_object.uid.as_deref(),
            event.message.as_deref(),
        ) else {
            return;
        };
        // Every message that ends a wait without the memory flag ends one
        // with it, so this keeps every refusal any wait acts on.
        if pod_sandbox_rejection_error(message, true).is_none() || recorded_long_ago(event) {
            return;
        }
        let notify = {
            let mut state = State::lock(&self.shared);
            if !state.retain(pod_uid, message, now) {
                return;
            }
            state
                .waiters
                .get(pod_uid)
                .map(|waiters| Arc::clone(&waiters.notify))
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

/// Whether kubelet last recorded `event` longer ago than a refusal is kept:
/// a re-list replays every Event still stored, refusals of long-gone pods
/// among them.
fn recorded_long_ago(event: &Event) -> bool {
    let recorded = event
        .last_timestamp
        .as_ref()
        .map(|time| time.0)
        .or_else(|| event.event_time.as_ref().map(|time| time.0))
        .or_else(|| {
            event
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|time| time.0)
        });
    let Some(recorded) = recorded else {
        return false;
    };
    let Ok(retention) = jiff::SignedDuration::try_from(REFUSAL_RETENTION) else {
        return false;
    };
    jiff::Timestamp::now().duration_since(recorded) > retention
}

/// A create's registration for its pod's refusal. Dropping it unregisters.
pub(super) struct RejectionWait {
    shared: Arc<Shared>,
    pod_uid: Box<str>,
    notify: Arc<Notify>,
}

impl RejectionWait {
    /// Resolve with the pod's refusal as [`pod_sandbox_rejection_error`]
    /// classifies it; otherwise never. While the process cannot watch Events
    /// this never resolves, and the create waits for its pod's IP as before.
    pub(super) async fn refused(&self, fail_fast_on_memory_refusal: bool) -> SubstrateError {
        loop {
            // Enabled before the check, so a refusal kept after it still
            // wakes this wait.
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            if let Some(error) = State::lock(&self.shared).refusal(
                &self.pod_uid,
                fail_fast_on_memory_refusal,
                Instant::now(),
            ) {
                return error;
            }
            notified.await;
        }
    }
}

impl Drop for RejectionWait {
    fn drop(&mut self) {
        let mut state = State::lock(&self.shared);
        if let Some(waiters) = state.waiters.get_mut(&self.pod_uid) {
            waiters.count -= 1;
            if waiters.count == 0 {
                state.waiters.remove(&self.pod_uid);
            }
        }
    }
}

/// The apiserver's HTTP status for a watcher failure, when it answered with one.
fn failure_code(error: &watcher::Error) -> Option<u16> {
    match error {
        watcher::Error::InitialListFailed(kube::Error::Api(response))
        | watcher::Error::WatchStartFailed(kube::Error::Api(response))
        | watcher::Error::WatchFailed(kube::Error::Api(response)) => Some(response.code),
        watcher::Error::WatchError(status) => Some(status.code),
        _ => None,
    }
}

/// A client error that another attempt would get again, such as a missing
/// `list` or `watch` grant: anything in 4xx but a request timeout, an expired
/// resource version or throttling.
fn is_permanent(code: Option<u16>) -> bool {
    code.is_some_and(|code| (400..500).contains(&code) && !matches!(code, 408 | 410 | 429))
}

/// Feed `rejections` from one watch of `events`' sandbox refusals. The
/// watcher lists once, then watches; it lists again when the watch expires
/// (410 Gone) and retries any failure with backoff, so no wait ever opens a
/// read of its own.
async fn watch_refusals(
    events: Api<Event>,
    rejections: PodSandboxRejections,
    namespace: String,
    shutdown_token: CancellationToken,
) {
    let config = WatcherConfig::default()
        .fields(&format!("reason={FAILED_CREATE_POD_SANDBOX_REASON}"))
        .any_semantic();
    let mut stream = pin!(watcher::watcher(events, config).default_backoff());
    let mut failing = false;
    loop {
        let next = tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return,
            next = stream.next() => next,
        };
        match next {
            Some(Ok(event)) => {
                if failing {
                    failing = false;
                    tracing::info!(
                        namespace = %namespace,
                        "watching sandbox pod refusals again"
                    );
                }
                if let watcher::Event::Apply(event) | watcher::Event::InitApply(event) = &event {
                    rejections.observe(event, Instant::now());
                }
            }
            Some(Err(error)) => {
                if !failing {
                    failing = true;
                    if is_permanent(failure_code(&error)) {
                        tracing::warn!(
                            namespace = %namespace,
                            error = %error,
                            "sandbox pod events cannot be watched; creates wait for their pod \
                             IP without failing fast on a node refusal"
                        );
                    } else {
                        tracing::warn!(
                            namespace = %namespace,
                            error = %error,
                            "failed to watch sandbox pod events; watching them again"
                        );
                    }
                }
            }
            None => {
                tracing::warn!(namespace = %namespace, "sandbox pod refusal watch ended");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests;
