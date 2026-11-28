use super::*;

/// Identity of the execution pod a proxy-reachability wait targets. Groups
/// the execution-row fields so `wait_for_pod_proxy_reachable`'s signature
/// stays under clippy's `too_many_arguments` ceiling as knobs are added.
#[derive(Clone, Copy)]
pub(in crate::controllers::substrate::kubernetes) struct PodProxyWaitTarget<'a> {
    pub pod_name: &'a str,
    /// Becomes the binding's `execution_object_id` and pins readiness to the
    /// materialized pod's UID when known; joiners whose owner may still be
    /// replacing a predecessor pod pass `None`.
    pub pod_uid: Option<&'a str>,
    pub generation: i64,
    /// `sandbox_k8s_executions.updated_at` captured before `create_execution`
    /// created the pod, carried only when initial `persist_pod_metadata`
    /// failed so `creating`-row repair stays fenced against a same-generation
    /// reset that raced before readiness observed the row.
    pub creating_repair_epoch: Option<time::OffsetDateTime>,
    /// True on owned create/wake waits (`create_execution` and
    /// `ensure_active_execution_pod_proxy_reachable` with
    /// `match_recorded_pod_uid`) and UID-pinned joined-create waits
    /// (`join_in_flight_create_execution` when `execution.pod_uid.is_some()`),
    /// where a Postgres-confirmed same-generation UID replacement means this
    /// wait must exit immediately instead of waiting until `phase_deadline`.
    pub abort_on_uid_change: bool,
}

impl<'a> PodProxyWaitTarget<'a> {
    pub(in crate::controllers::substrate::kubernetes) const fn new(
        pod_name: &'a str,
        pod_uid: Option<&'a str>,
        generation: i64,
    ) -> Self {
        Self {
            pod_name,
            pod_uid,
            generation,
            creating_repair_epoch: None,
            abort_on_uid_change: false,
        }
    }

    pub(in crate::controllers::substrate::kubernetes) const fn abort_on_uid_change(
        mut self,
    ) -> Self {
        self.abort_on_uid_change = true;
        self
    }

    pub(in crate::controllers::substrate::kubernetes) const fn with_creating_repair_epoch(
        mut self,
        epoch: Option<time::OffsetDateTime>,
    ) -> Self {
        self.creating_repair_epoch = epoch;
        self
    }

    pub(in crate::controllers::substrate::kubernetes) const fn with_pod_uid(
        mut self,
        pod_uid: Option<&'a str>,
    ) -> Self {
        self.pod_uid = pod_uid;
        self
    }
}

#[cfg(test)]
pub(in crate::controllers::substrate::kubernetes) type AfterIpAssignedHook =
    Box<dyn FnOnce() + Send>;

#[cfg(test)]
#[derive(Clone, Default)]
pub(in crate::controllers::substrate::kubernetes) struct ReadinessTestHooks {
    pub proxy_wait_started_at: Arc<std::sync::Mutex<Option<Instant>>>,
    pub proxy_wait_finished_at: Arc<std::sync::Mutex<Option<Instant>>>,
    pub proxy_wait_entered: Arc<tokio::sync::Notify>,
    pub recheck_armed: Arc<tokio::sync::Notify>,
    pub suppressed_healthy_fallback_error: Arc<tokio::sync::Notify>,
    pub awaiting_in_flight_terminal_fallback: Arc<tokio::sync::Notify>,
    pub awaiting_post_ip_loss_confirmation: Arc<tokio::sync::Notify>,
    pub follow_owner_status_error: Arc<tokio::sync::Notify>,
    pub ip_wait_iterations: Arc<std::sync::atomic::AtomicUsize>,
    pub fallback_jitter: Arc<std::sync::Mutex<Option<f64>>>,
    pub after_ip_assigned: Arc<std::sync::Mutex<Option<AfterIpAssignedHook>>>,
}

#[cfg(test)]
pub(super) struct ProxyWaitHookGuard(Arc<std::sync::Mutex<Option<Instant>>>);

#[cfg(test)]
impl Drop for ProxyWaitHookGuard {
    fn drop(&mut self) {
        let mut slot = self.0.lock().expect("proxy_wait_finished_at hook");
        if slot.is_none() {
            *slot = Some(Instant::now());
        }
    }
}

#[cfg(test)]
impl ReadinessTestHooks {
    pub(super) fn enter_proxy_wait(&self, started_at: Instant) -> ProxyWaitHookGuard {
        let mut slot = self
            .proxy_wait_started_at
            .lock()
            .expect("proxy_wait_started_at hook");
        if slot.is_none() {
            *slot = Some(started_at);
        }
        drop(slot);
        self.proxy_wait_entered.notify_waiters();
        ProxyWaitHookGuard(Arc::clone(&self.proxy_wait_finished_at))
    }

    pub(super) fn run_after_ip_assigned(&self) {
        if let Some(callback) = self
            .after_ip_assigned
            .lock()
            .expect("after_ip_assigned hook")
            .take()
        {
            callback();
        }
    }
}

/// Whether `sandboxes` show `target`'s execution served at `pod_ip`. For a
/// kubelet-published address, a field the node does not report (an older
/// node, or a sandbox manager that has restarted since the allocation) is not
/// compared. A node-reported address is confirmed only by a node that reports
/// the target's generation, Pod UID and that address.
pub(super) fn serves_target(
    sandboxes: Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>,
    sandbox_id: SandboxId,
    target: &PodProxyWaitTarget<'_>,
    pod_ip: PodIp,
    source: PodIpSource,
) -> bool {
    let strict = source == PodIpSource::NodeReported;
    let matches = |compared: Option<bool>| compared.unwrap_or(!strict);
    let sandbox_id = sandbox_id.to_string();
    sandboxes.iter().any(|info| {
        info.sandbox_id == sandbox_id
            && matches(
                info.execution_generation
                    .map(|generation| generation == target.generation),
            )
            && match (target.pod_uid, info.pod_uid.as_deref()) {
                (Some(expected), Some(reported)) => expected == reported,
                _ => !strict,
            }
            && matches(
                info.external_address
                    .as_deref()
                    .map(|reported| PodIp::parse(reported).ok() == Some(pod_ip)),
            )
    })
}

/// `sandbox_id`'s entries as the worker behind `client` reports them, or
/// `None` when it does not answer within `wait`.
pub(super) async fn node_sandboxes(
    client: &mut crate::controllers::substrate::RuntimeClient,
    sandbox_id: SandboxId,
    wait: Duration,
) -> Option<Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>> {
    let state = timeout(
        wait,
        client.sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
            sandbox_ids: vec![sandbox_id.to_string()],
        }),
    )
    .await;
    match state {
        Ok(Ok(response)) => Some(response.into_inner().sandboxes),
        _ => None,
    }
}

/// What a worker's report says about `target`'s runtime there.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum RuntimeAnswer {
    /// The worker reports the target's generation and Pod UID, and `pod_ip`
    /// whenever it reports an address.
    Serves,
    /// The worker does not report the sandbox, or reports a different
    /// generation or Pod UID.
    Elsewhere,
    /// The worker reports the sandbox without its generation or Pod UID (an
    /// older node, or a sandbox manager that has restarted), or reports the
    /// target's execution at another address.
    Unconfirmed,
}

/// [`RuntimeAnswer`] for `sandboxes`. Unlike readiness ([`serves_target`]),
/// acting on a runtime that is there does not need its address reported: the
/// identity fields name the execution.
pub(in crate::controllers::substrate::kubernetes) fn runtime_answer(
    sandboxes: Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>,
    sandbox_id: SandboxId,
    target: &PodProxyWaitTarget<'_>,
    pod_ip: PodIp,
) -> RuntimeAnswer {
    let sandbox_id = sandbox_id.to_string();
    let Some(info) = sandboxes.iter().find(|info| info.sandbox_id == sandbox_id) else {
        return RuntimeAnswer::Elsewhere;
    };
    let identity_differs = info
        .execution_generation
        .is_some_and(|generation| generation != target.generation)
        || info
            .pod_uid
            .as_deref()
            .zip(target.pod_uid)
            .is_some_and(|(reported, expected)| reported != expected);
    let address_differs = info
        .external_address
        .as_deref()
        .is_some_and(|reported| PodIp::parse(reported).ok() != Some(pod_ip));
    let identified =
        info.execution_generation.is_some() && info.pod_uid.is_some() && target.pod_uid.is_some();
    if identity_differs {
        RuntimeAnswer::Elsewhere
    } else if identified && !address_differs {
        RuntimeAnswer::Serves
    } else {
        RuntimeAnswer::Unconfirmed
    }
}

impl PodProxyWaitTarget<'_> {
    /// Whether `pod` is the pod this wait binds: with a target UID, only the
    /// pod that has it.
    pub(in crate::controllers::substrate::kubernetes) fn names(
        &self,
        pod: &SandboxPodView,
    ) -> bool {
        self.pod_uid.is_none_or(|uid| pod.uid() == Some(uid))
    }
}

/// The address `sandboxes` report for `sandbox_id` while its pod is `pod_uid`.
pub(super) fn node_reported_address(
    sandboxes: Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
) -> Option<PodIp> {
    let sandbox_id = sandbox_id.to_string();
    sandboxes
        .into_iter()
        .find(|info| {
            info.sandbox_id == sandbox_id
                && info.execution_generation == Some(generation)
                && info.pod_uid.as_deref() == Some(pod_uid)
        })
        .and_then(|info| PodIp::parse(info.external_address.as_deref()?).ok())
}
