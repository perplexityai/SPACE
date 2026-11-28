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
//!
//! Because uncached Events watches don't emit watch-cache bookmarks, a quiet
//! watch whose `resourceVersion` falls behind etcd compaction receives `410
//! Gone` on reconnect and re-lists the namespace's filtered Events in 500-item
//! pages — bounding steady-state cost to one paginated LIST per `space-api`
//! replica per compaction window rather than one full Events scan per create.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::{
    Api, Client,
    api::{ListParams, WatchEvent, WatchParams},
    runtime::{utils::Backoff, watcher::DefaultBackoff},
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

/// How long a refusal is kept for a wait that hasn't registered yet. A create
/// registers as soon as its pod create returns, so this only needs to cover
/// that gap and the watch's own lag, with a wide margin.
pub(super) const REFUSAL_RETENTION: Duration = Duration::from_secs(300);

/// The most pods whose refusals are kept at once; the oldest go first.
const MAX_RETAINED_PODS: usize = 4096;

/// Page size for initial and post-`410` `Events` re-lists, matching client-go
/// and `kube_runtime::watcher`'s default pager chunk size.
const EVENT_LIST_PAGE_LIMIT: u32 = 500;

/// Client-side bound on each `Events` list page and watch-open roundtrip,
/// guarding against a half-open connection that never delivers response headers.
const EVENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Server-side timeout requested on each `Events` watch stream (`< 295s` per
/// `kube::api::WatchParams`), plus the client-side idle margin before treating
/// a silent connection as dead.
const EVENT_WATCH_SERVER_TIMEOUT: Duration = Duration::from_secs(290);
const EVENT_WATCH_IDLE_MARGIN: Duration = Duration::from_secs(5);

/// How long an opened watch stream must stay open without error before a prior
/// failure is considered recovered, matching `DefaultBackoff`'s 120s reset
/// window and distinguishing normal 290s watch renewals from premature closes.
const WATCH_RECOVERY_WINDOW: Duration = Duration::from_secs(120);

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
    /// Parsed refusals for pods without an active waiter, keyed by pod UID.
    /// While a pod has registered [`Waiters`], its [`VerdictSlots`] moves into
    /// `waiters` and returns here on the last [`RejectionWait::drop`] with its
    /// remaining TTL, so a pod's verdicts have one owner at a time. Keeping one
    /// slot per verdict class bounds per-pod memory without letting fluctuating
    /// memory-admission messages crowd out a subsequent permanent
    /// `InvalidArgument` refusal.
    refusals: HashMap<Box<str>, Retained>,
    /// `(retained.expires_at, retained.seq, pod_uid)` entries, earliest expiry
    /// first, for bounded eviction (`<= MAX_RETAINED_PODS` entries; refreshed
    /// in place).
    order: BTreeSet<(Instant, u64, Box<str>)>,
    next_seq: u64,
    waiters: HashMap<Box<str>, Waiters>,
}

struct Verdict {
    message: Box<str>,
    /// When this verdict class ages out of unwaited retention (`None` when the
    /// Event was already older than [`REFUSAL_RETENTION`], so active waits
    /// still see it while registered and `unregister` drops it rather than
    /// retaining it).
    expires_at: Option<Instant>,
}

#[derive(Default)]
struct VerdictSlots {
    invalid_request: Option<Verdict>,
    memory_admission: Option<Verdict>,
}

impl VerdictSlots {
    fn record(&mut self, parsed: &ParsedRefusal, expires_at: Option<Instant>) -> bool {
        let (slot, message) = match parsed {
            ParsedRefusal::InvalidRequest(msg) => (&mut self.invalid_request, msg),
            ParsedRefusal::MemoryAdmission(msg) => (&mut self.memory_admission, msg),
        };
        if let Some(existing) = slot {
            if let Some(next) = expires_at
                && existing.expires_at.is_none_or(|cur| next > cur)
            {
                existing.message = message.clone();
                existing.expires_at = Some(next);
            }
            return false;
        }
        *slot = Some(Verdict {
            message: message.clone(),
            expires_at,
        });
        true
    }

    /// Drop any verdict class whose retention window has elapsed at `now`,
    /// returning the latest remaining expiry when at least one class survives.
    fn prune_expired(&mut self, now: Instant) -> Option<Instant> {
        if self
            .invalid_request
            .as_ref()
            .is_none_or(|verdict| verdict.expires_at.is_none_or(|exp| exp <= now))
        {
            self.invalid_request = None;
        }
        if self
            .memory_admission
            .as_ref()
            .is_none_or(|verdict| verdict.expires_at.is_none_or(|exp| exp <= now))
        {
            self.memory_admission = None;
        }
        self.latest_expiry()
    }

    fn latest_expiry(&self) -> Option<Instant> {
        let invalid = self.invalid_request.as_ref().and_then(|v| v.expires_at);
        let memory = self.memory_admission.as_ref().and_then(|v| v.expires_at);
        match (invalid, memory) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        }
    }

    fn error(&self, fail_fast_on_memory_refusal: bool) -> Option<SubstrateError> {
        if let Some(verdict) = &self.invalid_request {
            return Some(SubstrateError::InvalidRequest {
                message: (*verdict.message).to_owned(),
            });
        }
        if fail_fast_on_memory_refusal && let Some(verdict) = &self.memory_admission {
            return Some(SubstrateError::Unavailable {
                message: (*verdict.message).to_owned(),
            });
        }
        None
    }

    #[cfg(test)]
    fn retained_error(
        &self,
        fail_fast_on_memory_refusal: bool,
        now: Instant,
    ) -> Option<SubstrateError> {
        if let Some(verdict) = &self.invalid_request
            && verdict.expires_at.is_some_and(|exp| exp > now)
        {
            return Some(SubstrateError::InvalidRequest {
                message: (*verdict.message).to_owned(),
            });
        }
        if fail_fast_on_memory_refusal
            && let Some(verdict) = &self.memory_admission
            && verdict.expires_at.is_some_and(|exp| exp > now)
        {
            return Some(SubstrateError::Unavailable {
                message: (*verdict.message).to_owned(),
            });
        }
        None
    }
}

struct Retained {
    expires_at: Instant,
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

    fn take_retained(&mut self, pod_uid: &str, now: Instant) -> VerdictSlots {
        let Some(mut retained) = self.refusals.remove(pod_uid) else {
            return VerdictSlots::default();
        };
        self.order
            .remove(&(retained.expires_at, retained.seq, pod_uid.into()));
        retained.verdicts.prune_expired(now);
        retained.verdicts
    }

    fn insert_retained(
        &mut self,
        pod_uid: &str,
        verdicts: VerdictSlots,
        expires_at: Instant,
    ) -> bool {
        if self.refusals.len() >= MAX_RETAINED_PODS
            && self
                .order
                .first()
                .is_some_and(|(oldest_expires_at, _, _)| *oldest_expires_at > expires_at)
        {
            return false;
        }
        while self.refusals.len() >= MAX_RETAINED_PODS {
            let Some((_, _, oldest)) = self.order.pop_first() else {
                break;
            };
            self.refusals.remove(&oldest);
        }
        let seq = self.alloc_seq();
        self.refusals.insert(
            pod_uid.into(),
            Retained {
                expires_at,
                seq,
                verdicts,
            },
        );
        self.order.insert((expires_at, seq, pod_uid.into()));
        true
    }

    /// Keep `parsed` for `pod_uid`: directly in `waiters` while waits are
    /// registered (regardless of Event age), or in `refusals` until
    /// `expires_at` when no wait is registered. `true` when that verdict class
    /// wasn't already recorded for `pod_uid`.
    fn retain(
        &mut self,
        pod_uid: &str,
        parsed: ParsedRefusal,
        now: Instant,
        expires_at: Option<Instant>,
    ) -> bool {
        self.evict(now);
        let unexpired = expires_at.filter(|expires_at| *expires_at > now);
        if let Some(waiters) = self.waiters.get_mut(pod_uid) {
            return waiters.verdicts.record(&parsed, unexpired);
        }
        let Some(expires_at) = unexpired else {
            return false;
        };
        if let Some(retained) = self.refusals.get_mut(pod_uid) {
            retained.verdicts.prune_expired(now);
            let recorded_new = retained.verdicts.record(&parsed, Some(expires_at));
            let refreshed = retained
                .verdicts
                .latest_expiry()
                .expect("just recorded an unexpired verdict");
            if refreshed != retained.expires_at {
                let old_key = (retained.expires_at, retained.seq, pod_uid.into());
                let seq = self.next_seq;
                self.next_seq = self.next_seq.wrapping_add(1);
                retained.expires_at = refreshed;
                retained.seq = seq;
                self.order.remove(&old_key);
                self.order.insert((refreshed, seq, pod_uid.into()));
            }
            return recorded_new;
        }
        let mut verdicts = VerdictSlots::default();
        verdicts.record(&parsed, Some(expires_at));
        self.insert_retained(pod_uid, verdicts, expires_at)
    }

    fn unregister(&mut self, pod_uid: &str, now: Instant) {
        let Some(waiters) = self.waiters.get_mut(pod_uid) else {
            return;
        };
        waiters.count -= 1;
        if waiters.count > 0 {
            return;
        }
        let Some(mut waiters) = self.waiters.remove(pod_uid) else {
            return;
        };
        self.evict(now);
        if let Some(expires_at) = waiters.verdicts.prune_expired(now) {
            self.insert_retained(pod_uid, waiters.verdicts, expires_at);
        }
    }

    fn evict(&mut self, now: Instant) {
        while self
            .order
            .first()
            .is_some_and(|(expires_at, _, _)| *expires_at <= now)
        {
            let Some((_, _, pod_uid)) = self.order.pop_first() else {
                break;
            };
            self.refusals.remove(&pod_uid);
        }
    }

    fn refusal(&self, pod_uid: &str, fail_fast_on_memory_refusal: bool) -> Option<SubstrateError> {
        self.waiters
            .get(pod_uid)?
            .verdicts
            .error(fail_fast_on_memory_refusal)
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

    /// Register a wait for `pod_uid`'s refusal. Moves any retained verdicts
    /// for `pod_uid` into the active waiter entry and returns them to
    /// retention with their remaining TTL once the last wait for `pod_uid`
    /// drops.
    pub(super) fn register(&self, pod_uid: &str) -> RejectionWait {
        let now = Instant::now();
        let notify = {
            let mut state = State::lock(&self.shared);
            state.evict(now);
            if let Some(waiters) = state.waiters.get_mut(pod_uid) {
                waiters.count += 1;
                Arc::clone(&waiters.notify)
            } else {
                let verdicts = state.take_retained(pod_uid, now);
                let notify = Arc::default();
                state.waiters.insert(
                    pod_uid.into(),
                    Waiters {
                        notify: Arc::clone(&notify),
                        count: 1,
                        verdicts,
                    },
                );
                notify
            }
        };
        RejectionWait {
            shared: Arc::clone(&self.shared),
            pod_uid: pod_uid.into(),
            notify,
        }
    }

    /// Keep `event`'s refusal, if it reports one, and wake its pod's waits.
    pub(super) fn observe(&self, event: &Event, wall_now: jiff::Timestamp, now: Instant) {
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
        let expires_at = remaining_retention(event, wall_now).map(|remaining| now + remaining);
        let notify = {
            let mut state = State::lock(&self.shared);
            if !state.retain(pod_uid, parsed, now, expires_at) {
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

fn last_recorded_timestamp(event: &Event) -> Option<jiff::Timestamp> {
    [
        event.last_timestamp.as_ref().map(|time| time.0),
        event
            .series
            .as_ref()
            .and_then(|series| series.last_observed_time.as_ref())
            .map(|time| time.0),
        event.event_time.as_ref().map(|time| time.0),
        event
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|time| time.0),
    ]
    .into_iter()
    .flatten()
    .max()
}

/// Remaining retention window for `event` from when kubelet last recorded it:
/// a re-list replays every Event still stored in etcd, refusals of long-gone
/// pods among them, in storage-key rather than chronological order.
fn remaining_retention(event: &Event, wall_now: jiff::Timestamp) -> Option<Duration> {
    let Some(recorded) = last_recorded_timestamp(event) else {
        return Some(REFUSAL_RETENTION);
    };
    let age = wall_now.duration_since(recorded);
    if age.is_negative() {
        return Some(REFUSAL_RETENTION);
    }
    let Ok(age) = Duration::try_from(age) else {
        return None;
    };
    REFUSAL_RETENTION
        .checked_sub(age)
        .filter(|remaining| !remaining.is_zero())
}

/// A create's registration for its pod's refusal. Dropping it unregisters.
pub(super) struct RejectionWait {
    shared: Arc<Shared>,
    pod_uid: Box<str>,
    notify: Arc<Notify>,
}

impl RejectionWait {
    /// Return the pod's terminal node-manager refusal if one is already
    /// recorded, without waiting.
    pub(super) fn recorded(&self, fail_fast_on_memory_refusal: bool) -> Option<SubstrateError> {
        State::lock(&self.shared).refusal(&self.pod_uid, fail_fast_on_memory_refusal)
    }

    /// Resolve with the pod's terminal node-manager refusal once recorded
    /// (`InvalidArgument` always, `ResourceExhausted` when
    /// `fail_fast_on_memory_refusal` is enabled); otherwise never. While the
    /// process can't watch Events this never resolves, and the create waits
    /// for its pod's IP as before.
    pub(super) async fn refused(&self, fail_fast_on_memory_refusal: bool) -> SubstrateError {
        loop {
            // Enabled before the check, so a refusal kept after it still
            // wakes this wait.
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            if let Some(error) = self.recorded(fail_fast_on_memory_refusal) {
                return error;
            }
            notified.await;
        }
    }
}

impl Drop for RejectionWait {
    fn drop(&mut self) {
        State::lock(&self.shared).unregister(&self.pod_uid, Instant::now());
    }
}

/// The apiserver's HTTP status for a client failure, when it answered with one.
fn kube_status_code(error: &kube::Error) -> Option<u16> {
    match error {
        kube::Error::Api(response) => Some(response.code),
        _ => None,
    }
}

/// A client error that another attempt would get again, such as a missing
/// `list` or `watch` grant: anything in 4xx but a request timeout, an expired
/// resource version or throttling.
fn is_permanent(code: Option<u16>) -> bool {
    code.is_some_and(|code| (400..500).contains(&code) && !matches!(code, 408 | 410 | 429))
}

struct WatchHealth {
    backoff: DefaultBackoff,
    failing: bool,
    logged_permanent: bool,
    relisted_for_410: bool,
    namespace: String,
}

impl WatchHealth {
    fn new(namespace: String) -> Self {
        Self {
            backoff: DefaultBackoff::default(),
            failing: false,
            logged_permanent: false,
            relisted_for_410: false,
            namespace,
        }
    }

    fn needs_recovery_window(&self) -> bool {
        self.failing || self.relisted_for_410
    }

    fn mark_recovered(&mut self, recovery_deadline: &mut Option<Instant>) {
        *recovery_deadline = None;
        self.backoff.reset();
        self.logged_permanent = false;
        self.relisted_for_410 = false;
        if std::mem::take(&mut self.failing) {
            tracing::info!(
                namespace = %self.namespace,
                "watching sandbox pod refusals again"
            );
        }
    }

    fn record_failure(&mut self, code: Option<u16>, error: &dyn std::fmt::Display) {
        if code == Some(410) && !self.failing && !self.relisted_for_410 {
            self.relisted_for_410 = true;
            tracing::debug!(
                namespace = %self.namespace,
                error = %error,
                "sandbox pod event watch resourceVersion expired; re-listing"
            );
            return;
        }
        if is_permanent(code) {
            self.failing = true;
            if !std::mem::replace(&mut self.logged_permanent, true) {
                tracing::warn!(
                    namespace = %self.namespace,
                    error = %error,
                    "cannot watch sandbox pod events; creates wait for their pod \
                     IP without failing fast on a node refusal until a retry succeeds"
                );
            }
            return;
        }
        if !std::mem::replace(&mut self.failing, true) {
            tracing::warn!(
                namespace = %self.namespace,
                error = %error,
                "failed to watch sandbox pod events; watching them again"
            );
        }
    }

    async fn sleep_backoff(&mut self, shutdown_token: &CancellationToken) -> bool {
        if let Some(delay) = self.backoff.next() {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => return false,
                () = tokio::time::sleep(delay) => {}
            }
        }
        true
    }
}

/// Paginate a consistent (`MostRecent`) list of `events` matching `base_lp`,
/// observing every returned item and returning the snapshot's
/// `resourceVersion`.
async fn list_refusal_events(
    events: &Api<Event>,
    base_lp: &ListParams,
    rejections: &PodSandboxRejections,
    health: &mut WatchHealth,
    shutdown_token: &CancellationToken,
) -> ControlFlow<(), Option<String>> {
    let mut continue_token: Option<String> = None;
    let mut last_rv: Option<String> = None;
    loop {
        let mut lp = base_lp.clone();
        lp.continue_token = continue_token;
        let page = tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return ControlFlow::Break(()),
            res = tokio::time::timeout(EVENT_REQUEST_TIMEOUT, events.list(&lp)) => res,
        };
        match page {
            Ok(Ok(list)) => {
                let wall_now = jiff::Timestamp::now();
                let now = Instant::now();
                for event in &list.items {
                    rejections.observe(event, wall_now, now);
                }
                last_rv = list
                    .metadata
                    .resource_version
                    .filter(|s| !s.is_empty())
                    .or(last_rv);
                continue_token = list.metadata.continue_.filter(|s| !s.is_empty());
                if continue_token.is_none() {
                    if let Some(rv) = last_rv {
                        return ControlFlow::Continue(Some(rv));
                    }
                    health.record_failure(None, &"sandbox pod event list lacked resourceVersion");
                    return if health.sleep_backoff(shutdown_token).await {
                        ControlFlow::Continue(None)
                    } else {
                        ControlFlow::Break(())
                    };
                }
            }
            Ok(Err(error)) => {
                health.record_failure(kube_status_code(&error), &error);
                return if health.sleep_backoff(shutdown_token).await {
                    ControlFlow::Continue(None)
                } else {
                    ControlFlow::Break(())
                };
            }
            Err(_) => {
                health.record_failure(None, &"timed out listing sandbox pod events");
                return if health.sleep_backoff(shutdown_token).await {
                    ControlFlow::Continue(None)
                } else {
                    ControlFlow::Break(())
                };
            }
        }
    }
}

/// Feed `rejections` from one watch of `events`' sandbox refusals. Lists in
/// bounded pages when starting or after `410 Gone`, then watches from the
/// tracked `resourceVersion` and backs off across any list, watch-open, stream,
/// or immediate empty-body failure so no wait ever opens a read of its own.
async fn watch_refusals(
    events: Api<Event>,
    rejections: PodSandboxRejections,
    namespace: String,
    shutdown_token: CancellationToken,
) {
    let field_selector = format!("reason={FAILED_CREATE_POD_SANDBOX_REASON}");
    let base_lp = ListParams::default()
        .fields(&field_selector)
        .limit(EVENT_LIST_PAGE_LIMIT);
    let wp = WatchParams::default().fields(&field_selector).timeout(
        u32::try_from(EVENT_WATCH_SERVER_TIMEOUT.as_secs())
            .expect("watch server timeout fits in u32"),
    );
    let idle_timeout = EVENT_WATCH_SERVER_TIMEOUT + EVENT_WATCH_IDLE_MARGIN;
    let mut health = WatchHealth::new(namespace);
    let mut resource_version: Option<String> = None;

    loop {
        if resource_version.is_none() {
            match list_refusal_events(&events, &base_lp, &rejections, &mut health, &shutdown_token)
                .await
            {
                ControlFlow::Continue(Some(rv)) => resource_version = Some(rv),
                ControlFlow::Continue(None) => continue,
                ControlFlow::Break(()) => return,
            }
        }
        let rv = resource_version
            .as_deref()
            .expect("resource_version set before opening watch");
        let opened = tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return,
            res = tokio::time::timeout(EVENT_REQUEST_TIMEOUT, events.watch(&wp, rv)) => res,
        };
        let stream = match opened {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                let code = kube_status_code(&error);
                if code == Some(410) {
                    resource_version = None;
                }
                health.record_failure(code, &error);
                if !health.sleep_backoff(&shutdown_token).await {
                    return;
                }
                continue;
            }
            Err(_) => {
                health.record_failure(None, &"timed out opening sandbox pod event watch");
                if !health.sleep_backoff(&shutdown_token).await {
                    return;
                }
                continue;
            }
        };

        let opened_at = Instant::now();
        let mut recovery_deadline = health
            .needs_recovery_window()
            .then(|| opened_at + WATCH_RECOVERY_WINDOW);
        let mut stream = pin!(stream);
        loop {
            let item = tokio::select! {
                biased;
                () = shutdown_token.cancelled() => return,
                () = async {
                    match recovery_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    health.mark_recovered(&mut recovery_deadline);
                    continue;
                }
                item = tokio::time::timeout(idle_timeout, stream.next()) => item,
            };
            match item {
                Ok(Some(Ok(WatchEvent::Added(event) | WatchEvent::Modified(event)))) => {
                    let Some(next_rv) = event
                        .metadata
                        .resource_version
                        .as_deref()
                        .filter(|s| !s.is_empty())
                    else {
                        resource_version = None;
                        health.record_failure(
                            None,
                            &"sandbox pod event watch item lacked resourceVersion",
                        );
                        if !health.sleep_backoff(&shutdown_token).await {
                            return;
                        }
                        break;
                    };
                    resource_version = Some(next_rv.to_owned());
                    rejections.observe(&event, jiff::Timestamp::now(), Instant::now());
                }
                Ok(Some(Ok(WatchEvent::Deleted(event)))) => {
                    let Some(next_rv) = event
                        .metadata
                        .resource_version
                        .as_deref()
                        .filter(|s| !s.is_empty())
                    else {
                        resource_version = None;
                        health.record_failure(
                            None,
                            &"deleted sandbox pod event lacked resourceVersion",
                        );
                        if !health.sleep_backoff(&shutdown_token).await {
                            return;
                        }
                        break;
                    };
                    resource_version = Some(next_rv.to_owned());
                }
                Ok(Some(Ok(WatchEvent::Bookmark(bookmark)))) => {
                    if bookmark.metadata.resource_version.is_empty() {
                        resource_version = None;
                        health.record_failure(
                            None,
                            &"sandbox pod event bookmark lacked resourceVersion",
                        );
                        if !health.sleep_backoff(&shutdown_token).await {
                            return;
                        }
                        break;
                    }
                    resource_version = Some(bookmark.metadata.resource_version);
                }
                Ok(Some(Ok(WatchEvent::Error(status)))) => {
                    if status.code == 410 {
                        resource_version = None;
                    }
                    health.record_failure(Some(status.code), &status.message);
                    if !health.sleep_backoff(&shutdown_token).await {
                        return;
                    }
                    break;
                }
                Ok(Some(Err(error))) => {
                    let code = kube_status_code(&error);
                    if code == Some(410) {
                        resource_version = None;
                    }
                    health.record_failure(code, &error);
                    if !health.sleep_backoff(&shutdown_token).await {
                        return;
                    }
                    break;
                }
                Ok(None) | Err(_) => {
                    if opened_at.elapsed() < WATCH_RECOVERY_WINDOW {
                        health.record_failure(
                            None,
                            &"sandbox pod event watch closed before reaching its recovery window",
                        );
                        if !health.sleep_backoff(&shutdown_token).await {
                            return;
                        }
                    }
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
