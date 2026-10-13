use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{
    FutureExt,
    future::{BoxFuture, WeakShared},
};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, NetworkPolicy, NetworkPolicyV1,
    network_policy_hash, proto::NetworkPolicyV1 as NetworkPolicyProto,
};
use tonic::Status;

const TEMPLATE_NETWORK_POLICY_CACHE_TTL: Duration = Duration::from_secs(30);
const TEMPLATE_NETWORK_POLICY_FETCH_CAPACITY: usize = 32;
const TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
// GetTemplate can materialize artifacts under the server's request budget.
// Reserve the normal connection allowance around it; lazy() does not invoke
// that retry helper, so this bounds connection establishment and RPC together,
// not the connection phase separately. Foreground waits remain short.
const TEMPLATE_NETWORK_POLICY_FETCH_TIMEOUT: Duration =
    pplx_space_template_mgr_client::GRPC_REQUEST_TIMEOUT
        .saturating_add(pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY.max_elapsed);

pub(crate) fn network_policy_from_proto(
    policy: &NetworkPolicyProto,
) -> Result<NetworkPolicyV1, Status> {
    NetworkPolicyV1::try_from(policy).map_err(|err| Status::invalid_argument(err.to_string()))
}

pub(crate) fn optional_network_policy_from_proto(
    policy: Option<&NetworkPolicyProto>,
) -> Result<Option<NetworkPolicy>, Status> {
    policy.map(network_policy_from_proto).transpose()
}

pub(crate) fn network_policy_to_proto(policy: &NetworkPolicy) -> NetworkPolicyProto {
    NetworkPolicyProto::from(policy)
}

pub(crate) fn template_network_policy_from_proto(
    policy: Option<&NetworkPolicyProto>,
    generation: u64,
    authority: i32,
) -> Result<Option<EffectiveNetworkPolicy>, Status> {
    let Some(policy) = policy else {
        return Ok(None);
    };
    let policy = network_policy_from_proto(policy)?;
    let authority = crate::db::PolicyAuthority::try_from(authority)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(Some(EffectiveNetworkPolicy::new(
        policy, generation, authority, true,
    )))
}

#[derive(Clone, Debug)]
pub(crate) struct EffectiveNetworkPolicy {
    pub policy: NetworkPolicy,
    pub generation: u64,
    pub authority: crate::db::PolicyAuthority,
    pub inherited: bool,
    pub policy_hash: String,
}

impl EffectiveNetworkPolicy {
    pub(crate) fn new(
        policy: NetworkPolicy,
        generation: u64,
        authority: crate::db::PolicyAuthority,
        inherited: bool,
    ) -> Self {
        let policy_hash = network_policy_hash(&policy);
        Self {
            policy,
            generation,
            authority,
            inherited,
            policy_hash,
        }
    }
}

pub(crate) fn effective_network_policy_ref(
    sandbox: &crate::db::SandboxRow,
    policy: &EffectiveNetworkPolicy,
) -> Result<EffectiveNetworkPolicyRef, Status> {
    let scope = if policy.inherited {
        EffectiveNetworkPolicyRefScope::Template
    } else {
        EffectiveNetworkPolicyRefScope::Sandbox
    };
    let owner = if policy.inherited {
        sandbox.template.as_str()
    } else {
        sandbox.id.as_str()
    };
    let policy_hash = policy.policy_hash.as_str();
    match policy.authority {
        crate::db::PolicyAuthority::Cluster => {
            EffectiveNetworkPolicyRef::cluster(scope, owner, policy.generation, policy_hash)
        }
        crate::db::PolicyAuthority::Local => {
            EffectiveNetworkPolicyRef::local(scope, owner, policy_hash)
        }
    }
    .map_err(|error| {
        Status::internal(format!(
            "effective network policy ref construction failed: {error}"
        ))
    })
}

pub(crate) fn network_policy_override_from_set_request(
    inherit_template: bool,
    policy: Option<&NetworkPolicyProto>,
) -> Result<Option<NetworkPolicy>, Status> {
    let policy = optional_network_policy_from_proto(policy)?;
    match (inherit_template, policy) {
        (true, None) => Ok(None),
        (false, Some(policy)) => Ok(Some(policy)),
        (true, Some(_)) => Err(Status::invalid_argument(
            "set either inherit_template or policy, not both",
        )),
        (false, None) => Err(Status::invalid_argument(
            "policy is required unless inherit_template is true",
        )),
    }
}

pub(crate) async fn fetch_template_default_network_policy(
    templates: &dyn crate::template_resolver::TemplateResolver,
    template: &str,
) -> Result<EffectiveNetworkPolicy, Status> {
    let response = match templates
        .get_template_on_shared_channel(tonic::Request::new(
            pplx_space_template_mgr_client::proto::GetTemplateRequest {
                caller_org_id: None,
                template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
                    name: template.to_string(),
                    content_hash: None,
                    tag: None,
                }),
                vcpu: None,
                memory_mib: None,
            },
        ))
        .await
    {
        Ok(response) => response.into_inner(),
        Err(error) => {
            return Err(Status::internal(format!(
                "template-manager GetTemplate({template}) failed: {error}"
            )));
        }
    };
    let policy_proto = response.network_policy.as_ref().ok_or_else(|| {
        Status::internal(format!(
            "template-manager GetTemplate({template}) returned no network policy"
        ))
    })?;
    let policy = network_policy_from_proto(policy_proto)?;
    let authority = crate::db::PolicyAuthority::try_from(response.network_policy_authority)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(EffectiveNetworkPolicy::new(
        policy,
        response.network_policy_generation,
        authority,
        true,
    ))
}

#[derive(Clone)]
pub(crate) struct TemplateNetworkPolicyCache {
    inner: Arc<tokio::sync::Mutex<TemplateNetworkPolicyCacheState>>,
    fetch_slots: Arc<tokio::sync::Semaphore>,
    shutdown_token: tokio_util::sync::CancellationToken,
    task_tracker: tokio_util::task::TaskTracker,
    templates: Arc<dyn crate::template_resolver::TemplateResolver>,
}

#[derive(Default)]
struct TemplateNetworkPolicyCacheState {
    // Only owner reads populate this disposable cache. Hints are preconditions,
    // and notifications invalidate; neither creates a retained authority floor.
    cached: BTreeMap<String, CachedTemplateNetworkPolicy>,
    pending_fetches: BTreeMap<String, PendingTemplateNetworkPolicy>,
}

const TEMPLATE_NETWORK_POLICY_CACHE_CAPACITY: usize = 1024;
type PolicyFetch = BoxFuture<'static, Result<EffectiveNetworkPolicy, Status>>;

struct PendingTemplateNetworkPolicy {
    fetch: WeakShared<PolicyFetch>,
    claim: Arc<PolicyFetchClaim>,
}

#[derive(Default)]
struct PolicyFetchClaim {
    invalidated: tokio_util::sync::CancellationToken,
}

struct CachedTemplateNetworkPolicy {
    policy: EffectiveNetworkPolicy,
    refresh_after: tokio::time::Instant,
}

impl TemplateNetworkPolicyCache {
    pub(crate) fn new(
        shutdown_token: tokio_util::sync::CancellationToken,
        task_tracker: tokio_util::task::TaskTracker,
        templates: Arc<dyn crate::template_resolver::TemplateResolver>,
    ) -> Self {
        Self {
            inner: Arc::default(),
            fetch_slots: Arc::new(tokio::sync::Semaphore::new(
                TEMPLATE_NETWORK_POLICY_FETCH_CAPACITY,
            )),
            shutdown_token,
            task_tracker,
            templates,
        }
    }

    /// Resolve within five seconds, including capacity admission. A timeout or
    /// caller cancellation leaves an admitted, tracked fetch running.
    pub(crate) async fn get_or_fetch(
        &self,
        template: &str,
    ) -> Result<EffectiveNetworkPolicy, Status> {
        let templates = Arc::clone(&self.templates);
        self.get_or_fetch_with(template, |template| async move {
            fetch_template_default_network_policy(templates.as_ref(), &template).await
        })
        .await
    }

    /// Create/Adopt may lead the owner's independently reconciled Cluster policy.
    /// Refuse admission until the owner meets the hint instead of installing it.
    pub(crate) async fn require_hint(
        &self,
        template: &str,
        hint: &EffectiveNetworkPolicy,
        inherits_template: bool,
    ) -> Result<(), Status> {
        if !inherits_template {
            return Ok(());
        }
        let policy = self.get_or_fetch(template).await?;
        let result = require_template_policy_hint(&policy, hint);
        if result.is_err() {
            // A failed precondition must not pin a stale cache entry until TTL.
            self.invalidate(template).await;
        }
        result.map_err(pplx_space_sbx_mgr_client::template_policy_dependency_status)
    }

    pub(super) async fn get_or_fetch_with<F, Fut>(
        &self,
        template: &str,
        fetch: F,
    ) -> Result<EffectiveNetworkPolicy, Status>
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<EffectiveNetworkPolicy, Status>> + Send,
    {
        use pplx_space_metrics::reconcile::NetworkPolicyWaitOutcome;
        let mut observation = pplx_space_metrics::reconcile::network_policy_wait();
        let result = tokio::time::timeout(
            TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT,
            self.resolve_with(template, fetch, &mut observation),
        )
        .await;
        observation.finish(match &result {
            Ok(Ok(_)) => NetworkPolicyWaitOutcome::Success,
            Ok(Err(_)) => NetworkPolicyWaitOutcome::Error,
            Err(_) => NetworkPolicyWaitOutcome::Timeout,
        });
        result
            .unwrap_or_else(|_| {
                tracing::warn!(template, "template policy foreground resolution timed out");
                Err(Status::deadline_exceeded("template policy wait timed out"))
            })
            .map_err(|status| {
                // Manager shutdown is still a worker readiness failure. An owner
                // fetch or admission constraint is not worker-health evidence.
                if self.shutdown_token.is_cancelled() {
                    status
                } else {
                    pplx_space_sbx_mgr_client::template_policy_dependency_status(status)
                }
            })
    }

    async fn resolve_with<F, Fut>(
        &self,
        template: &str,
        fetch: F,
        observation: &mut pplx_space_metrics::reconcile::NetworkPolicyWait,
    ) -> Result<EffectiveNetworkPolicy, Status>
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<EffectiveNetworkPolicy, Status>> + Send,
    {
        use pplx_space_metrics::reconcile::NetworkPolicyWaitPath;
        let mut permit = None;
        loop {
            let mut state = self.inner.lock().await;
            state
                .pending_fetches
                .retain(|_, pending| pending.fetch.upgrade().is_some());
            if self.shutdown_token.is_cancelled() {
                return Err(Status::unavailable(
                    "template policy cache is shutting down",
                ));
            }
            if let Some(cached) = state.cached.get(template)
                && tokio::time::Instant::now() < cached.refresh_after
            {
                observation.set_path(NetworkPolicyWaitPath::Cached);
                return Ok(cached.policy.clone());
            }
            observation.set_path(NetworkPolicyWaitPath::Fetch);
            if let Some(pending) = state.pending_fetches.get(template)
                && let Some(fetch) = pending.fetch.upgrade()
            {
                drop(state);
                drop(permit);
                return fetch.await;
            }
            let Some(permit) = permit.take() else {
                // Never hold the cache lock while waiting for capacity. Recheck
                // both cache and same-key ownership after acquiring a slot.
                drop(state);
                permit = Some(tokio::select! {
                    biased;
                    _ = self.shutdown_token.cancelled() =>
                        return Err(Status::unavailable("template policy cache is shutting down")),
                    permit = Arc::clone(&self.fetch_slots).acquire_owned() =>
                        permit.map_err(|_| Status::unavailable("template policy capacity closed"))?,
                });
                continue;
            };
            let claim = Arc::new(PolicyFetchClaim::default());
            let owner_claim = Arc::clone(&claim);
            let cache = self.clone();
            let shutdown = self.shutdown_token.clone();
            let fetch_template = template.to_string();
            let pending = async move {
                use pplx_space_metrics::reconcile::NetworkPolicyFetchOutcome;
                let started = std::time::Instant::now();
                let (result, outcome) = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => (
                        Err(Status::unavailable("template policy cache is shutting down")),
                        NetworkPolicyFetchOutcome::Shutdown,
                    ),
                    _ = owner_claim.invalidated.cancelled() => (
                        Err(Status::unavailable("template policy changed during fetch; retry")),
                        NetworkPolicyFetchOutcome::Notification,
                    ),
                    result = tokio::time::timeout(TEMPLATE_NETWORK_POLICY_FETCH_TIMEOUT, fetch(fetch_template.clone())) => {
                        match result {
                            Ok(result) => {
                                let outcome = if result.is_ok() {
                                    NetworkPolicyFetchOutcome::Success
                                } else {
                                    NetworkPolicyFetchOutcome::Error
                                };
                                (result, outcome)
                            }
                            Err(_) => (
                                Err(Status::deadline_exceeded("template policy fetch timed out")),
                                NetworkPolicyFetchOutcome::Timeout,
                            ),
                        }
                    }
                };
                pplx_space_metrics::reconcile::network_policy_fetch(outcome, started);
                cache.complete_fetch(fetch_template, &owner_claim, result).await
            }.boxed().shared();
            let Some(weak) = pending.downgrade() else {
                return Err(Status::internal("template policy fetch already completed"));
            };
            state.pending_fetches.insert(
                template.to_string(),
                PendingTemplateNetworkPolicy { fetch: weak, claim },
            );
            let owner = pending.clone();
            self.task_tracker.spawn(async move {
                // Also bound ready Shared owners waiting for their first poll.
                let _permit = permit;
                if let Err(error) = owner.await {
                    tracing::debug!(%error, "template policy background fetch ended without a policy");
                }
            });
            drop(state);
            return pending.await;
        }
    }

    pub(crate) async fn invalidate(&self, template: &str) {
        let mut state = self.inner.lock().await;
        state.cached.remove(template);
        if let Some(pending) = state.pending_fetches.get(template) {
            // Keep identity until completion: eviction cannot mint a parallel
            // owner, and cancelled results cannot reach either cache or callers.
            pending.claim.invalidated.cancel();
        }
    }

    async fn complete_fetch(
        &self,
        template: String,
        claim: &Arc<PolicyFetchClaim>,
        result: Result<EffectiveNetworkPolicy, Status>,
    ) -> Result<EffectiveNetworkPolicy, Status> {
        let mut state = self.inner.lock().await;
        if !state
            .pending_fetches
            .get(&template)
            .is_some_and(|pending| Arc::ptr_eq(&pending.claim, claim))
        {
            return Err(Status::unavailable("template policy fetch superseded"));
        }
        state.pending_fetches.remove(&template);
        if claim.invalidated.is_cancelled() {
            return Err(Status::unavailable(
                "template policy changed during fetch; retry",
            ));
        }
        let policy = result?;
        let now = tokio::time::Instant::now();
        state.cached.retain(|_, cached| cached.refresh_after > now);
        if state.cached.len() >= TEMPLATE_NETWORK_POLICY_CACHE_CAPACITY
            && let Some(oldest) = state
                .cached
                .iter()
                .min_by_key(|(_, cached)| cached.refresh_after)
                .map(|(name, _)| name.clone())
        {
            state.cached.remove(&oldest);
        }
        state.cached.insert(
            template,
            CachedTemplateNetworkPolicy {
                policy: policy.clone(),
                refresh_after: now + TEMPLATE_NETWORK_POLICY_CACHE_TTL,
            },
        );
        Ok(policy)
    }
}

fn require_template_policy_hint(
    source: &EffectiveNetworkPolicy,
    hint: &EffectiveNetworkPolicy,
) -> Result<(), Status> {
    use crate::db::PolicyAuthority::{Cluster, Local};
    match (source.authority, hint.authority) {
        (Local, Cluster) => Ok(()),
        (Local, Local) if source.policy_hash == hint.policy_hash => Ok(()),
        (_, Local) => Err(Status::failed_precondition(
            "local template policy hint differs from template-manager; update the owner first",
        )),
        (Cluster, Cluster) if source.generation < hint.generation => Err(Status::unavailable(
            "template-manager has not reconciled the requested template policy generation; retry",
        )),
        (Cluster, Cluster)
            if source.generation == hint.generation && source.policy_hash != hint.policy_hash =>
        {
            Err(Status::failed_precondition(
                "template policy hint conflicts with template-manager at the same generation",
            ))
        }
        (Cluster, Cluster) => Ok(()),
    }
}

#[derive(Clone, Default)]
pub(crate) struct NetworkPolicyReconcileQueue {
    inner: Arc<Mutex<NetworkPolicyReconcileQueueState>>,
    notify: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct NetworkPolicyReconcileQueueState {
    pending: VecDeque<String>,
    retry_after: BTreeMap<String, NetworkPolicyRetryEntry>,
    attempts: BTreeMap<String, u32>,
    dead: BTreeMap<String, String>,
    active: BTreeMap<String, uuid::Uuid>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NetworkPolicyReconcileAttempt {
    pub sandbox_id: String,
    identity: uuid::Uuid,
}

#[derive(Clone, Copy)]
struct NetworkPolicyRetryEntry {
    deadline: tokio::time::Instant,
}

const NETWORK_POLICY_RECONCILE_MAX_ATTEMPTS: u32 = 16;
const NETWORK_POLICY_RECONCILE_MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
const NETWORK_POLICY_RECONCILE_FACET: &str = "network-policy";

impl NetworkPolicyReconcileQueue {
    #[allow(
        clippy::expect_used,
        reason = "Queue lock protects only owned collections; no caller callbacks run while held"
    )]
    fn state(&self) -> std::sync::MutexGuard<'_, NetworkPolicyReconcileQueueState> {
        self.inner
            .lock()
            .expect("network policy queue invariant mutex poisoned")
    }

    pub(crate) fn retire(&self, sandbox_id: &str) {
        let mut state = self.state();
        state.pending.retain(|pending| pending != sandbox_id);
        state.retry_after.remove(sandbox_id);
        state.attempts.remove(sandbox_id);
        state.dead.remove(sandbox_id);
        state.active.remove(sandbox_id);
        drop(state);
        self.emit_metrics();
    }

    pub(crate) fn enqueue(&self, sandbox_id: impl Into<String>) -> bool {
        let sandbox_id = sandbox_id.into();
        let inserted = {
            let mut state = self.state();
            state.retry_after.remove(&sandbox_id);
            state.attempts.remove(&sandbox_id);
            state.dead.remove(&sandbox_id);
            if state.pending.contains(&sandbox_id) {
                false
            } else {
                state.pending.push_back(sandbox_id);
                true
            }
        };
        self.emit_metrics();
        self.notify.notify_one();
        inserted
    }

    #[cfg(test)]
    fn enqueue_retry(&self, sandbox_id: String, base_delay: Duration, error: &str) {
        let should_notify = {
            let mut state = self.state();
            Self::enqueue_retry_locked(&mut state, sandbox_id, base_delay, error)
        };
        self.emit_metrics();
        if should_notify {
            self.notify.notify_one();
        }
    }

    fn enqueue_retry_locked(
        state: &mut NetworkPolicyReconcileQueueState,
        sandbox_id: String,
        base_delay: Duration,
        error: &str,
    ) -> bool {
        state.pending.retain(|pending| pending != &sandbox_id);
        let attempts = state
            .attempts
            .get(&sandbox_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        if attempts >= NETWORK_POLICY_RECONCILE_MAX_ATTEMPTS {
            state.retry_after.remove(&sandbox_id);
            state.attempts.remove(&sandbox_id);
            state.dead.insert(sandbox_id, error.to_string());
            pplx_space_metrics::reconcile::queue_dead_letter(NETWORK_POLICY_RECONCILE_FACET);
            false
        } else {
            state.attempts.insert(sandbox_id.clone(), attempts);
            let delay = retry_delay(base_delay, attempts);
            let deadline = tokio::time::Instant::now() + delay;
            let next_entry = NetworkPolicyRetryEntry { deadline };
            match state.retry_after.get_mut(&sandbox_id) {
                Some(existing) if deadline < existing.deadline => {
                    *existing = next_entry;
                    true
                }
                Some(_) => false,
                None => {
                    state.retry_after.insert(sandbox_id, next_entry);
                    true
                }
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self.state().pending.is_empty()
    }

    pub(crate) fn take_pending(
        &self,
        in_flight: &std::collections::BTreeSet<&str>,
        limit: usize,
    ) -> Vec<NetworkPolicyReconcileAttempt> {
        let mut state = self.state();
        std::iter::from_fn(|| {
            let index = state
                .pending
                .iter()
                .position(|sandbox_id| !in_flight.contains(sandbox_id.as_str()))?;
            let sandbox_id = state.pending.remove(index)?;
            let identity = uuid::Uuid::now_v7();
            state.active.insert(sandbox_id.clone(), identity);
            Some(NetworkPolicyReconcileAttempt {
                sandbox_id,
                identity,
            })
        })
        .take(limit)
        .collect()
    }

    #[cfg(test)]
    pub(crate) fn drain_pending(&self) -> std::collections::BTreeSet<String> {
        let mut state = self.state();
        state.pending.drain(..).collect()
    }

    pub(crate) fn promote_due_retries(&self, now: tokio::time::Instant) -> usize {
        let mut due = Vec::new();
        {
            let state = self.state();
            for (sandbox_id, entry) in &state.retry_after {
                if entry.deadline <= now {
                    due.push(sandbox_id.clone());
                }
            }
        }
        if due.is_empty() {
            return 0;
        }

        let mut state = self.state();
        let mut promoted = 0;
        for sandbox_id in due {
            if state.retry_after.remove(&sandbox_id).is_some() {
                state.pending.push_back(sandbox_id);
                promoted += 1;
            }
        }
        drop(state);
        self.emit_metrics();
        promoted
    }

    fn next_retry_deadline(&self) -> Option<tokio::time::Instant> {
        self.state()
            .retry_after
            .values()
            .map(|entry| entry.deadline)
            .min()
    }

    pub(crate) fn finish_attempt(
        &self,
        attempt: NetworkPolicyReconcileAttempt,
        failure: Option<&str>,
        base_delay: Duration,
    ) {
        let mut state = self.state();
        if state.active.get(&attempt.sandbox_id) != Some(&attempt.identity) {
            return;
        }
        state.active.remove(&attempt.sandbox_id);
        let notify = if let Some(error) = failure {
            Self::enqueue_retry_locked(&mut state, attempt.sandbox_id, base_delay, error)
        } else {
            state.retry_after.remove(&attempt.sandbox_id);
            state.attempts.remove(&attempt.sandbox_id);
            false
        };
        drop(state);
        self.emit_metrics();
        if notify {
            self.notify.notify_one();
        }
    }

    pub(crate) async fn wait_for_work_or_retry_deadline(&self) {
        if self.has_pending() {
            return;
        }

        if let Some(deadline) = self.next_retry_deadline() {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        } else {
            self.notify.notified().await;
        }
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.state().pending.len()
    }

    #[cfg(test)]
    fn retry_len(&self) -> usize {
        self.state().retry_after.len()
    }

    #[cfg(test)]
    fn dead_len(&self) -> usize {
        self.state().dead.len()
    }

    fn emit_metrics(&self) {
        let state = self.state();
        pplx_space_metrics::reconcile::queue_depth(
            NETWORK_POLICY_RECONCILE_FACET,
            "ready",
            state.pending.len() as u64,
        );
        pplx_space_metrics::reconcile::queue_depth(
            NETWORK_POLICY_RECONCILE_FACET,
            "retry",
            state.retry_after.len() as u64,
        );
        pplx_space_metrics::reconcile::queue_depth(
            NETWORK_POLICY_RECONCILE_FACET,
            "dead",
            state.dead.len() as u64,
        );
    }
}

fn retry_delay(base_delay: Duration, attempts: u32) -> Duration {
    let multiplier = 1_u32 << attempts.saturating_sub(1).min(16);
    base_delay
        .saturating_mul(multiplier)
        .min(NETWORK_POLICY_RECONCILE_MAX_RETRY_DELAY)
}

#[cfg(test)]
mod tests;
