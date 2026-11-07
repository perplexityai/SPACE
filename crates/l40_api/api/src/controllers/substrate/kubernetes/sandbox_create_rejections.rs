//! Node-side admission rejections of sandbox pods, observed from kubelet
//! `FailedCreatePodSandBox` Events.
//!
//! When node-mgr's memory admission refuses a new sandbox, the shim returns
//! `ResourceExhausted` to containerd and kubelet records a
//! `FailedCreatePodSandBox` Event on the pod. Kubelet does not reflect that
//! failure in the Pod's status: the pod stays `Pending` with no IP and kubelet
//! retries `RunPodSandbox` against the same node. The Pod watch alone sees
//! nothing until the create's data-plane budget runs out (`pod_never_ready`);
//! this index lets the wait end as soon as the node has refused the pod.
//!
//! One namespace-wide Event watch (field-selected to
//! `reason=FailedCreatePodSandBox`) indexes rejections by pod name, so a
//! waiting create answers from memory instead of each create listing Events.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::{
    Api, Client,
    runtime::{WatchStreamExt, watcher},
};

/// Kubelet's Event reason for a failed `RunPodSandbox`.
pub(super) const FAILED_CREATE_POD_SANDBOX_REASON: &str = "FailedCreatePodSandBox";

/// Marker the SPACE shim puts in the CRI error for a gRPC `ResourceExhausted`
/// from node-mgr (see `create_support.rs`, `space-err=<code>`). Transient
/// backpressure (queue limits, rate limits, slot exhaustion) shares the code
/// and usually clears on kubelet's next retry, so only a refusal that also
/// carries node-mgr's memory admission prefix counts.
const RESOURCE_EXHAUSTED_MARKER: &str = "space-err=ResourceExhausted";

/// How long a recorded rejection is kept. Longer than any create's ready
/// budget; pod names carry the execution generation, so a stale entry can only
/// match the same pod instance, and the uid check guards a recreated name.
const REJECTION_TTL: Duration = Duration::from_secs(15 * 60);

/// Prune expired entries once the index grows past this many, at most once
/// per [`PRUNE_INTERVAL`], so a large live index costs no per-insert scan.
const PRUNE_THRESHOLD: usize = 4096;
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// Hard bound on the index. A burst of distinct refusals within the TTL that
/// fills it drops further ones, so those creates fall back to the wait budget.
const MAX_ENTRIES: usize = 16 * 1024;

/// Env flag enabling the fail-fast (default off). Needs list/watch on
/// `events` in the sandbox namespace for the API's service account.
const FAIL_FAST_ENV: &str = "SPACE_CREATE_FAIL_FAST_ON_NODE_REJECTION";

/// A node's admission refusal of one sandbox pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxCreateRejection {
    pub pod_uid: Option<String>,
    pub node_name: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug)]
struct Entry {
    rejection: SandboxCreateRejection,
    recorded_at: Instant,
}

/// Index of node admission rejections keyed by sandbox pod name.
#[derive(Clone, Default)]
pub struct SandboxCreateRejections {
    inner: Arc<Mutex<Inner>>,
    disabled: bool,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    last_pruned: Option<Instant>,
}

impl SandboxCreateRejections {
    /// An index with no watch behind it; lookups always miss unless a test
    /// records into it.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Spawn the namespace-wide Event watch feeding the index, when the
    /// fail-fast is enabled.
    pub fn spawn(client: Client, sandbox_namespace: &str) -> Self {
        let disabled = !pplx_space_common_core::env::env_flag_var(FAIL_FAST_ENV, false);
        let index = Self {
            inner: Arc::default(),
            disabled,
        };
        if disabled {
            tracing::info!(
                env = FAIL_FAST_ENV,
                "create fail-fast on node admission rejection disabled"
            );
            return index;
        }
        let api: Api<Event> = Api::namespaced(client, sandbox_namespace);
        let config = watcher::Config::default().fields(&format!(
            "reason={FAILED_CREATE_POD_SANDBOX_REASON},involvedObject.kind=Pod"
        ));
        let writer = index.clone();
        tokio::spawn(async move {
            let stream = watcher::watcher(api, config)
                .default_backoff()
                .applied_objects();
            let mut stream = std::pin::pin!(stream);
            let mut reported_error = false;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        reported_error = false;
                        writer.observe_event(&event);
                    }
                    // Usually missing list/watch on `events`, which leaves the
                    // fail-fast inert: say so loudly once per outage.
                    Err(error) if !reported_error => {
                        reported_error = true;
                        tracing::error!(
                            %error,
                            env = FAIL_FAST_ENV,
                            "sandbox create rejection event watcher failed; create fail-fast \
                             is inactive until it recovers (needs list/watch on events)"
                        );
                    }
                    Err(error) => tracing::warn!(
                        %error,
                        "sandbox create rejection event watcher error"
                    ),
                }
            }
            tracing::warn!("sandbox create rejection event watcher stream ended");
        });
        index
    }

    /// Record `event` if it is a node admission rejection of a sandbox pod.
    pub(super) fn observe_event(&self, event: &Event) {
        let Some((pod_name, rejection)) = rejection_from_event(event) else {
            return;
        };
        self.record(pod_name, rejection);
    }

    pub(super) fn record(&self, pod_name: String, rejection: SandboxCreateRejection) {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.entries.len() >= PRUNE_THRESHOLD
            && inner
                .last_pruned
                .is_none_or(|at| now.duration_since(at) >= PRUNE_INTERVAL)
        {
            inner
                .entries
                .retain(|_, entry| now.duration_since(entry.recorded_at) < REJECTION_TTL);
            inner.last_pruned = Some(now);
        }
        if inner.entries.len() >= MAX_ENTRIES && !inner.entries.contains_key(&pod_name) {
            tracing::debug!(%pod_name, "sandbox create rejection index full; dropping rejection");
            return;
        }
        inner.entries.insert(
            pod_name,
            Entry {
                rejection,
                recorded_at: now,
            },
        );
    }

    /// The node's admission rejection of `pod_name`, if one was observed. With
    /// `pod_uid`, only a rejection recorded for that uid matches; one for
    /// another (or an unknown) uid may belong to an earlier pod with the same
    /// name.
    pub fn rejection_for(
        &self,
        pod_name: &str,
        pod_uid: Option<&str>,
    ) -> Option<SandboxCreateRejection> {
        if self.disabled {
            return None;
        }
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner.entries.get(pod_name)?;
        if entry.recorded_at.elapsed() >= REJECTION_TTL {
            return None;
        }
        // A known uid must match a recorded one exactly; only a wait that
        // doesn't know its pod's uid yet matches by name (pod names carry the
        // execution generation).
        if let Some(expected) = pod_uid
            && entry.rejection.pod_uid.as_deref() != Some(expected)
        {
            return None;
        }
        Some(entry.rejection.clone())
    }

    /// Resolve once `pod_name` has a recorded rejection; never resolves when
    /// the fail-fast is disabled. Polls the in-memory index, so a waiting
    /// create costs no API server traffic.
    pub async fn wait_for_rejection(
        &self,
        pod_name: &str,
        pod_uid: Option<&str>,
    ) -> SandboxCreateRejection {
        if self.disabled {
            return std::future::pending().await;
        }
        let mut tick = tokio::time::interval(REJECTION_POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Some(rejection) = self.rejection_for(pod_name, pod_uid) {
                return rejection;
            }
        }
    }
}

/// How often a waiting create checks the index.
const REJECTION_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// `(pod name, rejection)` when `event` is kubelet's `FailedCreatePodSandBox`
/// for a pod whose CRI error carries the shim's `ResourceExhausted` marker.
/// Other sandbox-create failures (jailer timeouts, network setup) are left to
/// kubelet's retry and the existing ready budget.
pub(super) fn rejection_from_event(event: &Event) -> Option<(String, SandboxCreateRejection)> {
    if event.reason.as_deref() != Some(FAILED_CREATE_POD_SANDBOX_REASON) {
        return None;
    }
    let involved = &event.involved_object;
    if involved.kind.as_deref() != Some("Pod") {
        return None;
    }
    let message = event.message.as_deref()?;
    if !is_node_admission_rejection(message) {
        return None;
    }
    let pod_name = involved.name.clone()?;
    let node_name = event
        .source
        .as_ref()
        .and_then(|source| source.host.clone())
        .or_else(|| event.reporting_instance.clone());
    Some((
        pod_name,
        SandboxCreateRejection {
            pod_uid: involved.uid.clone(),
            node_name,
            message: summarize_rejection(message),
        },
    ))
}

/// Whether a kubelet sandbox-create error is node-mgr's memory admission
/// refusal, which kubelet's retries on the same node won't get past.
pub(super) fn is_node_admission_rejection(message: &str) -> bool {
    message.contains(RESOURCE_EXHAUSTED_MARKER)
        && message.contains(pplx_space_node_mgr_client::MEMORY_ADMISSION_REFUSED)
}

/// The node-mgr reason from a kubelet error, without the nested CRI/gRPC
/// prefixes, bounded for the API error body and logs.
pub(super) fn summarize_rejection(message: &str) -> String {
    const MAX_LEN: usize = 512;
    // `... sbx-manager Wake [space-err=ResourceExhausted]: network allocate
    // failed: node uses ...`: keep what node-mgr said.
    let detail = message
        .split_once(RESOURCE_EXHAUSTED_MARKER)
        .map(|(_, tail)| tail.trim_start_matches(']').trim_start_matches(':'))
        .or_else(|| message.rsplit_once("desc = ").map(|(_, tail)| tail))
        .unwrap_or(message);
    let detail = detail.trim().trim_end_matches(['"', '\\']);
    let end = detail.floor_char_boundary(MAX_LEN);
    detail.get(..end).unwrap_or(detail).to_string()
}

#[cfg(test)]
mod tests;
