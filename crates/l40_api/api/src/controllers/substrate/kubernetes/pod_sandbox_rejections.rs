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

use std::collections::{BTreeSet, HashMap};
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum ParsedRefusal {
    InvalidRequest(Box<str>),
    MemoryAdmission(Box<str>),
}

fn parse_refusal(event_message: &str) -> Option<ParsedRefusal> {
    if let Some((_, reason)) = event_message.split_once(INVALID_ARGUMENT_MARKER) {
        return Some(ParsedRefusal::InvalidRequest(
            format!(
                "the node refused to start the sandbox: {}",
                reason.trim_start_matches(':').trim()
            )
            .into_boxed_str(),
        ));
    }
    let (_, reason) = event_message.split_once(RESOURCE_EXHAUSTED_MARKER)?;
    // ResourceExhausted also covers transient queue/slot pressure. The
    // allocation path's explicit prefix distinguishes memory admission.
    let (_, reason) = reason.split_once(MEMORY_ADMISSION_REFUSAL_PREFIX)?;
    Some(ParsedRefusal::MemoryAdmission(
        format!("{MEMORY_ADMISSION_REFUSAL_PREFIX} {}", reason.trim()).into_boxed_str(),
    ))
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
    /// Parsed refusals by pod UID. Keeping one slot per verdict class bounds
    /// per-pod memory without letting fluctuating memory-admission messages
    /// crowd out a subsequent permanent `InvalidArgument` refusal.
    refusals: HashMap<Box<str>, Retained>,
    /// `(retained.at, retained.seq, pod_uid)` entries, oldest first, for
    /// bounded eviction (`<= MAX_RETAINED_PODS` entries; refreshed in place).
    order: BTreeSet<(Instant, u64, Box<str>)>,
    next_seq: u64,
    waiters: HashMap<Box<str>, Waiters>,
}

#[derive(Clone, Default)]
struct VerdictSlots {
    invalid_request: Option<Box<str>>,
    memory_admission: Option<Box<str>>,
}

impl VerdictSlots {
    fn record(&mut self, parsed: &ParsedRefusal) -> bool {
        let slot = match parsed {
            ParsedRefusal::InvalidRequest(_) => &mut self.invalid_request,
            ParsedRefusal::MemoryAdmission(_) => &mut self.memory_admission,
        };
        if slot.is_some() {
            return false;
        }
        *slot = Some(match parsed {
            ParsedRefusal::InvalidRequest(msg) | ParsedRefusal::MemoryAdmission(msg) => msg.clone(),
        });
        true
    }

    fn error(&self, fail_fast_on_memory_refusal: bool) -> Option<SubstrateError> {
        if let Some(message) = &self.invalid_request {
            return Some(SubstrateError::InvalidRequest {
                message: (**message).to_owned(),
            });
        }
        if fail_fast_on_memory_refusal && let Some(message) = &self.memory_admission {
            return Some(SubstrateError::Unavailable {
                message: (**message).to_owned(),
            });
        }
        None
    }
}

struct Retained {
    at: Instant,
    seq: u64,
    verdicts: VerdictSlots,
}

struct Waiters {
    notify: Arc<Notify>,
    count: usize,
    verdicts: VerdictSlots,
}

impl State {
    fn lock(shared: &Shared) -> std::sync::MutexGuard<'_, Self> {
        shared.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn alloc_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        seq
    }

    /// Keep `parsed` for `pod_uid`. `true` when that verdict class was not kept
    /// already for either the bounded retention map or an active waiter.
    fn retain(&mut self, pod_uid: &str, parsed: ParsedRefusal, now: Instant) -> bool {
        self.evict(now);
        let woke_waiter = self
            .waiters
            .get_mut(pod_uid)
            .is_some_and(|waiters| waiters.verdicts.record(&parsed));
        if self.refusals.contains_key(pod_uid) {
            let seq = self.alloc_seq();
            let retained = self.refusals.get_mut(pod_uid).expect("checked key");
            let recorded_new = retained.verdicts.record(&parsed);
            self.order
                .remove(&(retained.at, retained.seq, pod_uid.into()));
            retained.at = now;
            retained.seq = seq;
            self.order.insert((now, seq, pod_uid.into()));
            return recorded_new || woke_waiter;
        }
        while self.refusals.len() >= MAX_RETAINED_PODS {
            let Some((_, _, oldest)) = self.order.pop_first() else {
                break;
            };
            self.refusals.remove(&oldest);
        }
        let seq = self.alloc_seq();
        let mut verdicts = VerdictSlots::default();
        verdicts.record(&parsed);
        self.refusals.insert(
            pod_uid.into(),
            Retained {
                at: now,
                seq,
                verdicts,
            },
        );
        self.order.insert((now, seq, pod_uid.into()));
        true
    }

    fn evict(&mut self, now: Instant) {
        while self.order.first().is_some_and(|(queued_at, _, _)| {
            now.saturating_duration_since(*queued_at) >= REFUSAL_RETENTION
        }) {
            let Some((_, _, pod_uid)) = self.order.pop_first() else {
                break;
            };
            self.refusals.remove(&pod_uid);
        }
    }

    fn refusal(
        &self,
        pod_uid: &str,
        fail_fast_on_memory_refusal: bool,
        now: Instant,
    ) -> Option<SubstrateError> {
        if let Some(waiters) = self.waiters.get(pod_uid)
            && let Some(error) = waiters.verdicts.error(fail_fast_on_memory_refusal)
        {
            return Some(error);
        }
        let retained = self.refusals.get(pod_uid)?;
        if now.saturating_duration_since(retained.at) >= REFUSAL_RETENTION {
            return None;
        }
        retained.verdicts.error(fail_fast_on_memory_refusal)
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
    /// kept for the pod, including those recorded before it registered, and
    /// preserves them until all registered waits for `pod_uid` drop.
    pub(super) fn register(&self, pod_uid: &str) -> RejectionWait {
        let now = Instant::now();
        let notify = {
            let mut state = State::lock(&self.shared);
            state.evict(now);
            let seeded = state
                .refusals
                .get(pod_uid)
                .map(|retained| retained.verdicts.clone())
                .unwrap_or_default();
            let waiters = state
                .waiters
                .entry(pod_uid.into())
                .or_insert_with(|| Waiters {
                    notify: Arc::default(),
                    count: 0,
                    verdicts: seeded,
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
        let Some(parsed) = parse_refusal(message) else {
            return;
        };
        if recorded_long_ago(event) {
            return;
        }
        let notify = {
            let mut state = State::lock(&self.shared);
            if !state.retain(pod_uid, parsed, now) {
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
        .or_else(|| {
            event
                .series
                .as_ref()
                .and_then(|series| series.last_observed_time.as_ref())
                .map(|time| time.0)
        })
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
    /// Resolve with the pod's terminal node-manager refusal once recorded
    /// (`InvalidArgument` always, `ResourceExhausted` when
    /// `fail_fast_on_memory_refusal` is enabled); otherwise never. While the
    /// process cannot watch Events this never resolves, and the create waits
    /// for its pod's IP as before.
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
    // `kube_runtime::watcher` in `ListWatch` mode emits `Ok(Event::Init)` from
    // `State::Empty` before calling `api.list`. Filter `Event::Init` out before
    // `.default_backoff()` so `StreamBackoff` only resets its exponential
    // backoff after an HTTP 200 list/watch response from `kube-apiserver`.
    let mut stream = pin!(
        watcher::watcher(events, config)
            .try_filter(|event| std::future::ready(!matches!(event, watcher::Event::Init)))
            .default_backoff()
    );
    let mut failing = false;
    loop {
        let next = tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return,
            next = stream.next() => next,
        };
        match next {
            Some(Ok(event)) => {
                if failing
                    && matches!(
                        &event,
                        watcher::Event::InitDone
                            | watcher::Event::Apply(_)
                            | watcher::Event::Delete(_)
                    )
                {
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
