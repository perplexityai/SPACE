//! Production [`SweepSink`] backed by Postgres and the substrate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::cluster_reconciler::{HydrationOutcome, OutcomeEvent};
use pplx_space_api::controllers::substrate::{
    CommandReadyProbe, ExecutionRoutabilityCheck, NodeApiTarget, ObservedPodIdentity,
    PodFenceMismatch, PodIdentityFence, RecordedExecution, RetireExecutionOutcome,
    RetireExecutionRequest, RetireFence, RetireStopRoute, RuntimeClient, Substrate,
    SubstrateAdapter, SubstrateError, connect_node_api_client_for_target,
    sandbox_states_with_client,
};
use pplx_space_durations::PositiveDuration;
use pplx_space_k8s_ids::{PodName, PodUid};
use pplx_space_node_model::{NodeManagedBy, NodeRole, SubstrateKind};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId, SandboxLifecycle};
use pplx_space_sandbox_store::k8s_executions::transient_sweeper;
use pplx_space_sandbox_store::row_locks;
use pplx_space_sandbox_store::sweeper;
use sqlx::PgPool;

use super::classify::{ExecutionRoutability, ProbeObservation};
use super::probe::{ProbeNodeError, SweepCandidate};
use super::{
    CommandReadyPendingCandidate, FailGuard, FailedCleanupCandidate, FirstActivation,
    RevertedToSuspended, SweepSink, is_activation_owned,
};
use pplx_space_api::controllers::substrate::AbandonedRestoreRetirement;

/// Max preserved `command_ready_pending` rows resolved per tick. Each costs one
/// non-recovering guest probe (bounded by the configured probe timeout), so the
/// cap keeps a node-mgr-wide preserve backlog from fanning out an unbounded
/// number of RPCs in a single tick — the remainder rolls to the next tick. The
/// steady-state candidate count is tiny (only slow-create preserves no client
/// ever retried), so this rarely binds.
const COMMAND_READY_PENDING_BATCH: i64 = 50;

/// The `managed_by` the failed-active-execution cleanup binds, or `None` when
/// the running substrate is not Kubernetes. That cleanup's statements scope to
/// Kubernetes-managed rows only, so on any other substrate they match nothing
/// and the caller skips them. An unparseable label is not Kubernetes.
pub(super) fn failed_active_execution_scope(managed_by: &str) -> Option<SubstrateKind> {
    match managed_by.parse::<SubstrateKind>() {
        Ok(SubstrateKind::Kubernetes) => Some(SubstrateKind::Kubernetes),
        Ok(SubstrateKind::Space) | Err(_) => None,
    }
}

/// Max aged candidates listed per lifecycle per tick. Each candidate costs at
/// most a share of one per-node probe RPC, so the cap bounds tick I/O when a
/// cluster-wide incident wedges a large population at once; the remainder
/// rolls to the next tick.
const SWEEP_CANDIDATE_BATCH: i64 = 100;

/// Build the retire fence for a candidate from its active execution row's
/// `(pod_name, pod_uid)`.
///
/// Every candidate gets a fence — this never yields the unfenced teardown,
/// which is the most destructive path there is (it deletes every tracked pod
/// name and releases tracking unconditionally). Three shapes, weakest last:
///
/// * **No execution row** — the row had none when it was selected, and
///   [`RetireFence::NoExecution`] makes the substrate re-prove that before
///   acting. Reading "nothing to protect" out of it would be a fail-open: a
///   row with no execution is exactly the one a concurrent create or wake is
///   most likely to give one.
/// * **A pod name and a UID** — the full fence.
/// * **A pod name alone** — `pod_uid` is nullable because a pod's UID is
///   persisted after the pod is created, so a row wedged before that write has
///   a real pod and no recorded identity. Supersession still applies, and the
///   substrate refuses to touch a live pod it cannot identify.
///
/// A parse failure is schema drift, not a state: it resolves to
/// [`RetireFence::NoExecution`], which fails closed, because an execution row
/// does exist and the substrate will refuse on that basis.
/// The retire fence for a candidate, built from its active execution row and
/// upgraded with what the substrate currently sees.
///
/// The persisted UID cannot tell a pod that *moved*: a Pending pod becoming
/// Running keeps its UID, and the sweep's evidence that the sandbox has no
/// live runtime is a node probe, which a pod coming up after it invalidates.
/// A resourceVersion observed here, before the probe and the CAS, is what the
/// substrate re-checks to notice.
///
/// The revert to `suspended` re-checks it once, before its CAS
/// (`retire_fence_mismatch`). With abandoned-restore retirement disabled (the
/// default), the cleanup then retires the still-active execution under this
/// fence and refuses a pod that came up since, and the revert is undone.
/// Enabled, the revert retires the execution in the CAS's own transaction,
/// and a restore pod that binds after that check is the abandoned
/// restore's leftover: the restore's lease has expired, its row aged past the
/// sweep's window, and the revert's snapshot proof says restore selection
/// picks a durable pre-restore source. So the cleanup deletes it, the row
/// stays `suspended`, and the next wake restores from that source. The row
/// never reached `started`, and no user request reached its VM: the API's
/// access, runtime-client, ingress and data-plane route paths never dial a
/// `restoring` sandbox's pod, so nothing it holds is newer than the source.
/// That holds only once every routing replica runs those paths, which is why
/// retirement stays disabled until then (see `AbandonedRestoreRetirement`).
///
/// The observation may only *confirm* the row's persisted UID, never replace
/// it. The row is read by one query and the pods by a later one, so a row that
/// is re-claimed in between — which clears its UID and keeps its generation,
/// pod name and activation attempt — would otherwise produce a fence built
/// from the old row's facts and the replacement's pod, a combination that
/// matches the new state exactly and authorizes tearing it down.
pub(crate) fn candidate_pod_fence_with_observation(
    sandbox_id: SandboxId,
    pod_name: Option<String>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    generation: Option<i64>,
    recorded_pod_uid: Option<PodUid>,
    route_activation_id: Option<SandboxActivationAttemptId>,
    observed: &HashMap<PodName, ObservedPodIdentity>,
) -> RetireFence {
    let Some(pod_name) = pod_name else {
        return RetireFence::NoExecution;
    };
    let pod_name = match PodName::parse(&pod_name) {
        Ok(pod_name) => pod_name,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %error,
                "transient_lifecycle_sweeper: unparseable pod name on the active execution \
                 row; the retire will fail closed against the execution that does exist"
            );
            return RetireFence::NoExecution;
        }
    };
    let with_identity = |fence: PodIdentityFence| {
        // Every shape of fence carries what the row said about itself at
        // select time. It is the only part of the identity that can notice the
        // row being re-claimed when no pod is left to compare, and the fence
        // is at its weakest exactly there.
        let fence = fence.with_recorded(RecordedExecution::Read {
            pod_uid: recorded_pod_uid.clone(),
            route_activation_id,
        });
        let fence = match activation_attempt_id {
            Some(attempt) => fence.with_activation_attempt(attempt),
            None => fence,
        };
        match generation {
            Some(generation) => fence.with_generation(generation),
            None => fence,
        }
    };
    match (recorded_pod_uid.as_ref(), observed.get(&pod_name)) {
        // The row and the live pod agree on which object this is, so the
        // observation is provably of the execution that was selected.
        (Some(recorded), Some(identity)) if recorded == &identity.pod_uid => {
            let mut fence = PodIdentityFence::new(pod_name).with_pod_uid(identity.pod_uid.clone());
            if let Some(resource_version) = identity.resource_version.clone() {
                fence = fence.with_resource_version(resource_version);
            }
            RetireFence::Pod(with_identity(fence))
        }
        // A pod exists under this name that the selected row does not claim —
        // either the row was re-claimed since the select, or its UID was never
        // written. Either way nothing here can prove the two describe the same
        // object, so fall through to the name-only fence, which refuses to act
        // while any pod is there. A row whose UID is simply late gets the full
        // fence on the tick after it lands.
        _ => {
            // A name-only fence would let a retire proceed against a live pod
            // it cannot prove is still the one the probe judged, because a
            // Pending pod becoming Running keeps its UID. It says exactly
            // that — act only on an absent pod.
            RetireFence::Pod(with_identity(PodIdentityFence::new(pod_name)))
        }
    }
}

/// Snapshot the substrate's current view of every pod named in the candidate
/// rows. Reflector-backed, so this is a map lookup per candidate rather than a
/// round trip; a pod the substrate cannot see is simply absent and its
/// candidate falls back to the persisted identity.
async fn observe_candidate_pods(
    substrate: &Substrate,
    pod_names: impl Iterator<Item = &str>,
) -> HashMap<PodName, ObservedPodIdentity> {
    let names: Vec<PodName> = pod_names
        .filter_map(|name| PodName::parse(name).ok())
        .collect();
    if names.is_empty() {
        return HashMap::new();
    }
    substrate.observe_pod_identities(&names).await
}

/// Begin a transaction holding the sandbox row lock
/// ([`row_locks::lock_id`]), for a CAS whose owner check must see any
/// activation attempt claimed while it waited for the row.
///
/// The fail, converge and revert CASes check the owner with a subquery on the
/// execution (`activation_owner_guard`), and every activation-attempt
/// assignment (`assign_activation_attempt_in`'s callers) locks the sandbox row
/// without changing it before it writes a new lease there. An `UPDATE` that
/// waits on that lock goes ahead on its original snapshot once the lock is
/// released, so it would act on a row a new owner had just claimed. Taking the
/// lock first, in the CAS's own transaction, makes the CAS read a snapshot
/// that includes any such claim.
async fn begin_locked(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    row_locks::lock_id(&mut tx, sandbox_id).await?;
    Ok(tx)
}

/// Retire the reverted restore's execution as `replaced` inside the revert
/// CAS's transaction, matching the identity the candidate select recorded
/// (see [`SweepSink::revert_to_suspended`]). A crash, error or refusal
/// between a revert and a separate retire would leave a `suspended` row with
/// its same-generation execution still active, which nothing revisits.
///
/// Returns `false`, and the caller rolls the revert back, when the active
/// execution is no longer the one the candidate observed.
async fn retire_reverted_restore_execution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    fence: &PodIdentityFence,
) -> Result<bool, sqlx::Error> {
    if fence.recorded == RecordedExecution::Unread {
        return Ok(false);
    }
    let retired = transient_sweeper::retire_reverted_restore_execution(
        &mut **tx,
        sandbox_id,
        fence.pod_name.as_str(),
        fence.generation,
        fence.activation_attempt_id,
        fence.recorded.pod_uid().map(PodUid::as_str),
        fence.recorded.route_activation_id(),
    )
    .await?;
    if retired {
        // The retired execution no longer places the row. The cleanup that
        // follows unplaces it too, but it is best-effort, and a `suspended`
        // row left reading as placed is one nothing revisits.
        sweeper::unplace_reverted_restore(&mut **tx, sandbox_id).await?;
        // The node state the open exposures described died with the runtime.
        pplx_space_sandbox_store::port_exposures::invalidate_open_observations(
            &mut **tx, sandbox_id,
        )
        .await?;
    }
    Ok(retired)
}

pub struct SubstrateSweepSink {
    pool: PgPool,
    substrate: Arc<Substrate>,
    /// Per-node node-api clients for the liveness probe, keyed by
    /// `k8s_node_name`. Mirrors the node-drain promotion client cache:
    /// bounded by cluster size, evicted on transport-shaped errors so a
    /// replaced node-api pod gets a fresh connection. Probing is sequential,
    /// so the `tokio::sync::Mutex` is uncontended.
    probe_clients: tokio::sync::Mutex<HashMap<String, CachedProbeClient>>,
    /// See [`super::ReconcilerConfig::abandoned_restore_retirement`].
    abandoned_restore_retirement: AbandonedRestoreRetirement,
    /// See [`super::ReconcilerConfig::restoring_episode_deadline`].
    restoring_episode_deadline: Option<PositiveDuration>,
}

#[derive(Clone)]
struct CachedProbeClient {
    endpoint_addr: String,
    client: RuntimeClient,
}

/// Whether a probe failure should drop the cached client for its node.
/// Transport-shaped errors mean the connection (or the node-api pod behind
/// it) may be gone; precondition-shaped errors do not.
///
/// `LockContention` is deliberately absent: a lost lock race on a
/// control-plane statement says nothing about the node's connection. It used
/// to arrive here as `Failed` and evict the client, which was never right.
fn should_evict_probe_client(error: &SubstrateError) -> bool {
    matches!(
        error,
        SubstrateError::Unavailable { .. }
            | SubstrateError::Failed { .. }
            | SubstrateError::Timeout { .. }
            | SubstrateError::Cancelled { .. }
    )
}

impl SubstrateSweepSink {
    pub fn new(pool: PgPool, substrate: Arc<Substrate>) -> Self {
        Self {
            pool,
            substrate,
            probe_clients: tokio::sync::Mutex::new(HashMap::new()),
            abandoned_restore_retirement: AbandonedRestoreRetirement::Disabled,
            restoring_episode_deadline: None,
        }
    }

    /// Whether [`SweepSink::revert_to_suspended`] retires the abandoned
    /// restore's execution. Disabled by default.
    pub fn with_abandoned_restore_retirement(
        mut self,
        retirement: AbandonedRestoreRetirement,
    ) -> Self {
        self.abandoned_restore_retirement = retirement;
        self
    }

    /// How long a `restoring` episode may go without a pod IP before the
    /// select and the revert stop waiting for its owner. Off by default.
    pub fn with_restoring_episode_deadline(mut self, deadline: Option<PositiveDuration>) -> Self {
        self.restoring_episode_deadline = deadline;
        self
    }

    async fn probe_client(
        &self,
        node_name: &str,
        probe_timeout: Duration,
    ) -> Result<CachedProbeClient, SubstrateError> {
        let mut clients = self.probe_clients.lock().await;
        if let Some(client) = clients.get(node_name) {
            return Ok(client.clone());
        }
        let managed_by = self
            .substrate
            .managed_by()
            .parse::<NodeManagedBy>()
            .map_err(|err| SubstrateError::Failed {
                message: format!("substrate reported invalid managed_by value: {err}"),
            })?;
        let target = NodeApiTarget {
            managed_by,
            role: NodeRole::Worker,
            direct_endpoint: None,
            substrate_node_id: Some(node_name),
        };
        let (endpoint_addr, client) = connect_node_api_client_for_target(
            self.substrate.as_ref(),
            &target,
            // Per-request channel timeout: the probe deadline is the RPC
            // budget too, so a wedged node-api can't hold a tick longer
            // than the outer bound in `probe_node`.
            probe_timeout,
        )
        .await?;
        let client = CachedProbeClient {
            endpoint_addr,
            client,
        };
        clients.insert(node_name.to_string(), client.clone());
        Ok(client)
    }

    async fn evict_probe_client(&self, node_name: &str) {
        self.probe_clients.lock().await.remove(node_name);
    }
}

impl SweepSink for SubstrateSweepSink {
    /// List aged candidates for `lifecycle` without flipping anything.
    ///
    /// `managed_by` scope: each substrate writes its label
    /// (`SubstrateAdapter::managed_by()`) into `sandboxes.managed_by` at
    /// row creation. The sweeper scopes to its own substrate's label so a
    /// cluster-controller running with the Kubernetes substrate doesn't
    /// claim direct-substrate rows (and vice versa) — the cleanup step
    /// is substrate-specific and would fail or silently no-op against an
    /// executor it doesn't own. Mirrors the `managed_by = 'k8s'` scope on
    /// the drain reconciler's queries.
    ///
    /// `lifecycle_changed_at` is maintained by a BEFORE UPDATE trigger
    /// (`sandboxes_lifecycle_changed_at`) so the column is bumped exactly
    /// when `lifecycle` changes, regardless of which SQL call site issued
    /// the UPDATE.
    ///
    /// Evacuation-marked `restoring` rows (a non-NULL
    /// `evacuation_restore_target`) are exempt — node-drain's restore sweep
    /// owns re-driving them and they legitimately run for the evacuate-timeout
    /// window. A marked `suspending` row is exempt only while its current
    /// suspend snapshot is still recoverable (`creating` or `ready`); once
    /// that snapshot terminalizes `failed` (or never existed) the backstop
    /// lists it, because nothing else recovers a crashed/hung suspend with no
    /// durable copy. See [`sweeper::list_aged_transient_query`] for the full
    /// rationale.
    async fn list_aged_transient(
        &self,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
    ) -> Result<Vec<SweepCandidate>, sqlx::Error> {
        // `as BIGINT` for the SQL bind. `u64::MAX` is ~5.8e11 years and
        // `i64::MAX` is ~2.9e11 years; only an operator who sets
        // `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_<...>_AGE_SECS` past `i64::MAX`
        // can trip the saturation. Saturate (rather than silently wrap or
        // return an error) because any value above i64::MAX is "never age
        // out" in human terms, which matches what an operator typing a
        // huge number was almost certainly trying to express. The
        // `debug_assert!` documents the intentional clamp so a future
        // reader doesn't mistake this for fail-silent error swallowing.
        let secs = age.get().as_secs();
        debug_assert!(
            i64::try_from(secs).is_ok(),
            "age threshold {secs}s exceeds i64::MAX; will saturate to i64::MAX (effectively disabling the sweep for this lifecycle)"
        );
        let managed_by = self.substrate.managed_by();
        let rows = sweeper::list_aged_transient(
            &self.pool,
            managed_by,
            lifecycle,
            age,
            SWEEP_CANDIDATE_BATCH,
            self.restoring_episode_deadline,
        )
        .await?;
        let observed = observe_candidate_pods(
            &self.substrate,
            rows.iter().filter_map(|row| row.pod_name.as_deref()),
        )
        .await;
        Ok(rows
            .into_iter()
            .map(|row| {
                let evacuation_restore_target =
                    row.evacuation_restore_target
                        .and_then(|parsed| match parsed {
                            Ok(target) => Some(target.lifecycle()),
                            Err(err) => {
                                // The column CHECK pins values to
                                // 'started'/'paused'/'stopped'; reaching this
                                // means schema/enum drift. Treat as unset (the
                                // converge target then defaults to started).
                                tracing::warn!(
                                    sandbox_id = %row.id,
                                    error = %err,
                                    "transient_lifecycle_sweeper: unparseable \
                                     evacuation_restore_target; treating as unset"
                                );
                                None
                            }
                        });
                let activation_attempt_held = row.activation_attempt_id.is_some();
                let pod_fence = candidate_pod_fence_with_observation(
                    row.id,
                    row.pod_name,
                    row.activation_attempt_id,
                    row.generation,
                    row.pod_uid,
                    row.route_activation_id,
                    &observed,
                );
                SweepCandidate {
                    sandbox_id: row.id,
                    lifecycle,
                    // Negative only under clock skew; clamp defensively.
                    age: Duration::from_secs(row.age_secs.max(0).cast_unsigned()),
                    evacuation_restore_target,
                    ready_snapshot: row.ready_snapshot,
                    fork_restore: row.fork_restore,
                    node_name: row.k8s_node_name,
                    routable_execution: row.routable_execution,
                    activation_abandoned: row.activation_abandoned,
                    episode_overdue: row.episode_overdue,
                    activation_attempt_held,
                    pod_fence,
                }
            })
            .collect())
    }

    /// Batch-probe `sandbox_ids` on `node_name` via the worker node-api's
    /// `SandboxState` RPC (which answers from the on-node sbx-mgr). The
    /// endpoint resolves **by node** through the substrate's node-api pod
    /// cache — never through the sandbox pod IP, which is unreachable for
    /// exactly the dead-pod population this probe exists to detect.
    ///
    /// Latency contract: connect and RPC are each bounded by
    /// `probe_timeout` (the channel's request timeout equals it), so the
    /// worst case per node is ~2x `probe_timeout`.
    async fn probe_node(
        &self,
        node_name: &str,
        sandbox_ids: &[SandboxId],
        probe_timeout: Duration,
    ) -> Result<HashMap<SandboxId, ProbeObservation>, ProbeNodeError> {
        let cached =
            match tokio::time::timeout(probe_timeout, self.probe_client(node_name, probe_timeout))
                .await
            {
                Ok(Ok(cached)) => cached,
                Ok(Err(err)) => {
                    if should_evict_probe_client(&err) {
                        self.evict_probe_client(node_name).await;
                    }
                    return Err(ProbeNodeError::NodeApi(err));
                }
                Err(_) => return Err(ProbeNodeError::Timeout(probe_timeout)),
            };
        let mut client = cached.client.clone();
        let states = match tokio::time::timeout(
            probe_timeout,
            sandbox_states_with_client(&mut client, &cached.endpoint_addr, sandbox_ids),
        )
        .await
        {
            Ok(Ok(states)) => states,
            Ok(Err(err)) => {
                if should_evict_probe_client(&err) {
                    self.evict_probe_client(node_name).await;
                }
                return Err(ProbeNodeError::NodeApi(err));
            }
            Err(_) => {
                self.evict_probe_client(node_name).await;
                return Err(ProbeNodeError::Timeout(probe_timeout));
            }
        };
        Ok(states
            .into_iter()
            .map(|(sandbox_id, state)| (sandbox_id, ProbeObservation::from(state)))
            .collect())
    }

    /// Routability verify-and-repair for an alive `restoring` candidate the
    /// DB reported unroutable, through the substrate's
    /// [`SubstrateAdapter::ensure_routable_execution`]. The Kubernetes
    /// substrate consults the apiserver and hydrates the execution row from a
    /// live matching pod; the direct substrate has no pod-routability
    /// contract, so its rows are always routable (and in practice never reach
    /// this call — the candidate select's fact is unconditionally true for
    /// `managed_by <> 'k8s'` rows).
    async fn ensure_routable_execution(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> Result<ExecutionRoutability, String> {
        let check = SubstrateAdapter::ensure_routable_execution(
            self.substrate.as_ref(),
            sandbox_id,
            probe_timeout,
        )
        .await
        .map_err(|err| {
            // A broken check (apiserver unreachable, RBAC, DB error) must
            // be distinguishable from "nothing to do": the affected rows
            // sit `restoring` until this path works again, so its failure
            // rate needs its own alertable signal.
            OutcomeEvent::execution_hydration(HydrationOutcome::Error)
                .sandbox_id(sandbox_id)
                .error(&err)
                .warn();
            err.to_string()
        })?;
        match check {
            // Nothing was checked, so there is no hydration outcome to report.
            ExecutionRoutabilityCheck::Unconditional => {}
            ExecutionRoutabilityCheck::Hydrated => {
                OutcomeEvent::execution_hydration(HydrationOutcome::HydratedExecutionFromLivePod)
                    .sandbox_id(sandbox_id)
                    .info()
            }
            ExecutionRoutabilityCheck::AlreadyRoutable => {
                OutcomeEvent::execution_hydration(HydrationOutcome::Skipped)
                    .reason("already_routable")
                    .sandbox_id(sandbox_id)
                    .debug()
            }
            ExecutionRoutabilityCheck::NotRoutable => {
                OutcomeEvent::execution_hydration(HydrationOutcome::Deferred)
                    .reason("no_routable_pod")
                    .sandbox_id(sandbox_id)
                    .debug()
            }
        }
        Ok(if check.is_routable() {
            ExecutionRoutability::Routable
        } else {
            ExecutionRoutability::NotRoutable
        })
    }

    async fn converge_restoring(
        &self,
        sandbox_id: SandboxId,
        target: SandboxLifecycle,
        age: PositiveDuration,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let mut tx = begin_locked(&self.pool, sandbox_id).await?;
        let claimed =
            sweeper::converge_restoring(&mut *tx, sandbox_id, managed_by, target, age).await?;
        tx.commit().await?;
        Ok(claimed)
    }

    async fn undo_revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        reverted: &RevertedToSuspended,
        preserve_local_artifact: bool,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        // Bound to the exact write this sweep made, not merely to the row
        // reading `suspended`. A restore that started and genuinely suspended
        // again in the meantime holds a different token, and undoing *that*
        // would drag a legitimately suspended sandbox back to `restoring`.
        sweeper::undo_revert_to_suspended(
            &self.pool,
            sandbox_id,
            managed_by,
            lifecycle,
            &reverted.write_token,
            preserve_local_artifact,
            reverted.prior_placement_changed_at,
        )
        .await
        .map_err(sqlx::Error::from)
    }

    async fn revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
        fence: &RetireFence,
    ) -> Result<Option<RevertedToSuspended>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let mut tx = self.pool.begin().await?;
        // Take the sandbox row lock that every execution claim also takes, so
        // the execution check below can't race a new claim, and read the
        // placement clock the CAS clears while holding it.
        let prior_placement_changed_at: Option<Option<time::OffsetDateTime>> =
            sweeper::lock_placement_changed_at(&mut *tx, sandbox_id, managed_by).await?;
        let Some(prior_placement_changed_at) = prior_placement_changed_at else {
            tx.rollback().await?;
            return Ok(None);
        };
        // Then the active execution's, which the pod-IP writers take, so the
        // CAS's "no pod IP" check reads a committed IP rather than racing one.
        // If an execution-only writer holds that lock, defer to the next tick
        // rather than waiting while holding the sandbox lock.
        let execution_locked = transient_sweeper::lock_active_nowait(&mut *tx, sandbox_id)
            .await
            .map_err(sqlx::Error::from);
        match execution_locked {
            Ok(_) => {}
            Err(error) if crate::node_drain::store::is_lock_timeout(&error) => {
                tx.rollback().await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        let retire = self.abandoned_restore_retirement.is_enabled();
        let deadline = self.restoring_episode_deadline;
        let claimed = if retire {
            sweeper::revert_to_suspended(&mut *tx, sandbox_id, managed_by, lifecycle, age, deadline)
                .await?
        } else {
            sweeper::revert_to_suspended_lifecycle_only(
                &mut *tx, sandbox_id, managed_by, lifecycle, age, deadline,
            )
            .await?
        };
        let Some(write_token) = claimed else {
            tx.rollback().await?;
            return Ok(None);
        };
        if !retire {
            // The cleanup retires the execution afterwards, under the fence.
            tx.commit().await?;
            return Ok(Some(RevertedToSuspended {
                write_token,
                prior_placement_changed_at,
            }));
        }
        let execution_settled = match fence {
            RetireFence::Pod(pod_fence) => {
                retire_reverted_restore_execution(&mut tx, sandbox_id, pod_fence).await?
            }
            RetireFence::NoExecution => {
                !pplx_space_sandbox_store::k8s_executions::exists(&mut *tx, sandbox_id).await?
            }
            // The candidate select always observes one of the two above; a
            // caller that observed nothing can't name the execution to retire.
            RetireFence::Unobserved => false,
        };
        if !execution_settled {
            tx.rollback().await?;
            return Ok(None);
        }
        tx.commit().await?;
        Ok(Some(RevertedToSuspended {
            write_token,
            prior_placement_changed_at,
        }))
    }

    async fn fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
        guard: FailGuard,
    ) -> Result<Option<String>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        if !is_activation_owned(lifecycle) {
            return sweeper::fail_aged_candidate(
                &self.pool, sandbox_id, managed_by, lifecycle, age, guard,
            )
            .await
            .map_err(sqlx::Error::from);
        }
        let mut tx = begin_locked(&self.pool, sandbox_id).await?;
        let token =
            sweeper::fail_aged_candidate(&mut *tx, sandbox_id, managed_by, lifecycle, age, guard)
                .await?;
        tx.commit().await?;
        Ok(token)
    }

    async fn undo_fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        write_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sweeper::undo_fail_aged_candidate(
            &self.pool,
            sandbox_id,
            managed_by,
            lifecycle,
            write_token,
        )
        .await
        .map_err(sqlx::Error::from)
    }

    async fn list_unrestorable_restoring(
        &self,
        min_placement_age: PositiveDuration,
    ) -> Result<Vec<(SandboxId, Duration)>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let rows = sweeper::list_unrestorable_restoring(
            &self.pool,
            managed_by,
            min_placement_age,
            SWEEP_CANDIDATE_BATCH,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|sweeper::UnrestorableRestoringRow { id, age_secs }| {
                // Negative only under clock skew; clamp defensively.
                (id, Duration::from_secs(age_secs.max(0).cast_unsigned()))
            })
            .collect())
    }

    async fn fail_unrestorable_restoring(
        &self,
        sandbox_id: SandboxId,
        min_placement_age: PositiveDuration,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sweeper::fail_unrestorable_restoring(&self.pool, sandbox_id, managed_by, min_placement_age)
            .await
            .map_err(sqlx::Error::from)
    }

    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, String> {
        self.substrate
            .retire_fence_mismatch(sandbox_id, fence)
            .await
            .map_err(|err| err.to_string())
    }

    async fn cleanup_after_sweep(
        &self,
        sandbox_id: SandboxId,
        fence: RetireFence,
    ) -> Result<RetireExecutionOutcome, String> {
        self.substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence,
                stop_route: RetireStopRoute::Execution,
            })
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_failed_active_executions(
        &self,
        batch_size: i64,
    ) -> Result<Vec<FailedCleanupCandidate>, sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(Vec::new());
        };
        let rows = pplx_space_sandbox_store::k8s_executions::list_failed_active_executions(
            &self.pool, managed_by, batch_size,
        )
        .await?;
        let observed = observe_candidate_pods(
            &self.substrate,
            rows.iter().map(|row| row.pod_name.as_str()),
        )
        .await;
        Ok(rows
            .into_iter()
            .map(|row| FailedCleanupCandidate {
                sandbox_id: row.id,
                node_name: row.k8s_node_name,
                first_activation: FirstActivation::from_columns(
                    row.first_activation_unfinished,
                    row.first_activation_abandoned,
                ),
                pod_fence: candidate_pod_fence_with_observation(
                    row.id,
                    Some(row.pod_name),
                    row.activation_attempt_id,
                    Some(row.generation),
                    row.pod_uid,
                    row.route_activation_id,
                    &observed,
                ),
            })
            .collect())
    }

    async fn first_activation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<FirstActivation>, sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(None);
        };
        let columns = pplx_space_sandbox_store::k8s_executions::first_activation(
            &self.pool, sandbox_id, managed_by,
        )
        .await?;
        Ok(columns.map(|columns| {
            FirstActivation::from_columns(
                columns.first_activation_unfinished,
                columns.first_activation_abandoned,
            )
        }))
    }

    async fn note_failed_cleanup_attempt(&self, sandbox_id: SandboxId) -> Result<(), sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(());
        };
        // This bumps `updated_at`, which is also where the age of a
        // materialization attempt comes from. That overlap is safe only
        // because a `failed` sandbox cannot have a create in flight, so
        // nothing reads that age for these rows; were it read, every rotation
        // would refresh it and an abandoned create would defer forever.
        transient_sweeper::note_failed_cleanup_attempt(&self.pool, sandbox_id, managed_by).await?;
        Ok(())
    }

    async fn cleanup_failed_execution(
        &self,
        sandbox_id: SandboxId,
        fence: RetireFence,
        stop_route: RetireStopRoute,
    ) -> Result<RetireExecutionOutcome, String> {
        self.substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence,
                stop_route,
            })
            .await
            .map_err(|err| err.to_string())
    }

    async fn repair_failed_active_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(false);
        };
        sweeper::repair_failed_active_execution(&self.pool, sandbox_id, managed_by)
            .await
            .map_err(sqlx::Error::from)
    }

    /// List preserved `started` rows whose `command_ready_pending` marker has
    /// stuck longer than `age`. `lifecycle_changed_at` is the right age clock:
    /// the create preserve path sets the marker in the same write that flips
    /// the row to `started`, so `lifecycle_changed_at` is when the row became
    /// this pending zombie. Scoped to this controller's substrate (the reap's
    /// `cleanup_after_sweep` is substrate-specific), and capped at
    /// [`COMMAND_READY_PENDING_BATCH`] so one tick can't fan out an unbounded
    /// number of per-row guest probes.
    async fn list_command_ready_pending(
        &self,
        age: PositiveDuration,
    ) -> Result<Vec<CommandReadyPendingCandidate>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let rows = sweeper::list_command_ready_pending(
            &self.pool,
            managed_by,
            age,
            COMMAND_READY_PENDING_BATCH,
        )
        .await?;
        // `age_secs` is `NOW() - lifecycle_changed_at`, always positive given
        // the WHERE clause; clamp a negative (clock skew) to zero defensively.
        let observed = observe_candidate_pods(
            &self.substrate,
            rows.iter().filter_map(|row| row.pod_name.as_deref()),
        )
        .await;
        Ok(rows
            .into_iter()
            .map(|row| CommandReadyPendingCandidate {
                sandbox_id: row.id,
                pending_age: Duration::from_secs(row.age_secs.max(0).cast_unsigned()),
                pod_fence: candidate_pod_fence_with_observation(
                    row.id,
                    row.pod_name,
                    row.activation_attempt_id,
                    row.generation,
                    row.pod_uid,
                    row.route_activation_id,
                    &observed,
                ),
            })
            .collect())
    }

    async fn probe_command_ready(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> CommandReadyProbe {
        self.substrate
            .probe_command_ready(sandbox_id, probe_timeout)
            .await
    }

    async fn heal_command_ready_pending(&self, sandbox_id: SandboxId) -> Result<(), sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sweeper::heal_command_ready_pending(&self.pool, sandbox_id, managed_by)
            .await
            .map_err(sqlx::Error::from)
    }

    async fn reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sweeper::reap_command_ready_pending(&self.pool, sandbox_id, managed_by)
            .await
            .map_err(sqlx::Error::from)
    }

    async fn undo_reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
        write_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sweeper::undo_reap_command_ready_pending(&self.pool, sandbox_id, managed_by, write_token)
            .await
            .map_err(sqlx::Error::from)
    }
}
