//! Pod read paths: cached and uncached sandbox pod reads, pod IP resolution,
//! and remembering the node a resolved pod landed on.

use super::*;
use crate::controllers::substrate::kubernetes::store::{
    FencedPodMetadataOutcome, ObservedAttemptPodFence, PodMetadataFence,
};
use crate::controllers::substrate::kubernetes::wait_ready::PodProxyWaitTarget;

mod cache_reads;

pub(in crate::controllers::substrate::kubernetes) use cache_reads::{
    PodGet, PooledClaimPodObservation, ScheduledCreatePodRead,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) struct FencedNodePlacement {
    pub node_id: Option<NodeId>,
    pub matched_execution: bool,
    pub execution_updated_at: Option<time::OffsetDateTime>,
}

/// Outcome of [`KubernetesSubstrate::resolve_pod_ip_for_readiness`], carrying
/// both the resolved address (when published or recorded) and metadata the
/// readiness loop needs to pace fallback reads and wake `node_reported_pod_ip`.
#[derive(Debug)]
pub(in crate::controllers::substrate::kubernetes) struct ReadinessPodResolution {
    pub resolved: Option<(PodIp, PodIpSource)>,
    pub resolved_uid: Option<PodUid>,
    /// Scheduled non-terminal pod observed during resolution (including from a
    /// degraded-reflector fallback `GET` before kubelet publishes `podIP`), so
    /// `node_reported_pod_ip` can query the worker node immediately.
    pub scheduled_pod: Option<SandboxPodView>,
    /// True when resolution issued an uncached apiserver `GET`.
    pub used_uncached_get: bool,
    /// True when the reflector store held a pod view authoritative for the
    /// current execution row (matching `row.pod_uid` and not a stale terminal
    /// view refuted by apiserver verification).
    pub cache_matches_execution: bool,
    /// True when resolution already confirmed a missing or terminal pod (or saw
    /// no active execution row) and retired tracking, so a caller awaiting an
    /// in-flight fallback `GET` doesn't issue a duplicate confirmation `GET`.
    pub terminal_or_absent_retired: bool,
    /// True when an owned UID-pinned wait (`target.abort_on_uid_change`) saw the
    /// same generation move on to another `pod_uid` (or reset for replacement),
    /// so the caller aborts with retryable `SubstrateError::Unavailable`.
    pub pod_uid_superseded: bool,
    /// True when an apiserver `GET` was cut short by the readiness deadline.
    pub deadline_elapsed: bool,
    /// True when the active execution row is still `creating` (or unpinned
    /// `recovering`) so resolution returned early without verifying `pod`.
    pub execution_in_flight: bool,
}

#[derive(Debug)]
pub(in crate::controllers::substrate::kubernetes) struct ActiveExecutionPodMatch {
    pub matches_generation: bool,
    pub execution_abandoned: bool,
    pub pod_uid_superseded: bool,
    pub owner_replacing_pod: bool,
    /// Expected `pod_uid` snapshot for a fenced node-placement write (`Some(Some(uid))`
    /// when `row.pod_uid` matched, `Some(None)` on an unmaterialized UID-pinned row
    /// without `creating_repair_epoch`, or `None` when placement must not run yet).
    pub placement_fence_uid: Option<Option<PodUid>>,
    pub recorded_uid: Option<PodUid>,
    pub recorded_ip: Option<(PodIp, PodIpSource)>,
}

#[derive(Default)]
struct PodInstanceResolution {
    instance: Option<ResolvedPodInstance>,
    scheduled_pod: Option<SandboxPodView>,
    generation: Option<i64>,
    used_uncached_get: bool,
    cache_matches_execution: bool,
    terminal_or_absent_retired: bool,
    pod_uid_superseded: bool,
    deadline_elapsed: bool,
    execution_in_flight: bool,
}

enum PodInstanceAttempt {
    Done(Box<PodInstanceResolution>),
    Retry { used_uncached_get: bool },
}

impl PodInstanceAttempt {
    fn done(resolution: PodInstanceResolution) -> Self {
        Self::Done(Box::new(resolution))
    }
}

impl KubernetesSubstrate {
    /// Checks `pod` against `sandbox_id`'s active execution row in a single
    /// Postgres read with zero apiserver `GET`s, returning:
    /// - whether `pod` is compatible with the active execution's `generation`
    ///   and any recorded `pod_uid`,
    /// - whether `pod` matches a concrete recorded `pod_uid` (required before a
    ///   UID-unpinned joiner may persist node placement), and
    /// - any already-confirmed `pod_ip` recorded for this exact `pod_uid` (so
    ///   both joined and redriven waits resolve a confirmed `NodeReported` IP
    ///   even while `pod_store` still holds a pre-`podIP` `Pending` view).
    pub(in crate::controllers::substrate::kubernetes) async fn inspect_active_execution_for_pod(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        pod: &SandboxPodView,
    ) -> Result<ActiveExecutionPodMatch, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(ActiveExecutionPodMatch {
                matches_generation: false,
                execution_abandoned: true,
                pod_uid_superseded: false,
                owner_replacing_pod: false,
                placement_fence_uid: None,
                recorded_uid: None,
                recorded_ip: None,
            });
        };
        if row.generation != target.generation || is_suspended_lifecycle(&row.lifecycle) {
            return Ok(ActiveExecutionPodMatch {
                matches_generation: false,
                execution_abandoned: true,
                pod_uid_superseded: false,
                owner_replacing_pod: false,
                placement_fence_uid: None,
                recorded_uid: None,
                recorded_ip: None,
            });
        }
        if let (Some(recorded_uid), Some(target_uid)) =
            (row.pod_uid.as_ref().map(PodUid::as_str), target.pod_uid)
            && recorded_uid != target_uid
        {
            return Ok(ActiveExecutionPodMatch {
                matches_generation: false,
                execution_abandoned: false,
                pod_uid_superseded: true,
                owner_replacing_pod: false,
                placement_fence_uid: None,
                recorded_uid: None,
                recorded_ip: None,
            });
        }
        match row.pod_uid.as_ref() {
            Some(expected) => {
                let uid_matches = pod.uid() == Some(expected.as_str())
                    || (target.pod_uid.is_some() && pod.uid().is_none());
                if !uid_matches {
                    return Ok(ActiveExecutionPodMatch {
                        matches_generation: false,
                        execution_abandoned: false,
                        pod_uid_superseded: false,
                        owner_replacing_pod: target.pod_uid.is_none()
                            && row.activation_attempt_live,
                        placement_fence_uid: None,
                        recorded_uid: None,
                        recorded_ip: None,
                    });
                }
                if target.pod_uid.is_none()
                    && row.activation_attempt_live
                    && row.lifecycle == SandboxLifecycle::Stopped.as_str()
                    && self
                        .store
                        .execution_predates_current_stop(sandbox_id, row.generation)
                        .await?
                {
                    return Ok(ActiveExecutionPodMatch {
                        matches_generation: false,
                        execution_abandoned: false,
                        pod_uid_superseded: false,
                        owner_replacing_pod: true,
                        placement_fence_uid: None,
                        recorded_uid: None,
                        recorded_ip: None,
                    });
                }
                let in_flight = execution_pod_lookup_in_flight(row.state)
                    && !(row.state == ActiveExecutionState::Recovering || target.pod_uid.is_some());
                let recorded_ip = if !in_flight
                    && !pod.is_deleting()
                    && terminal_pod_phase(pod).is_none()
                    && pod.unschedulable_message().is_none()
                {
                    current_pod_ip_with_source(pod, Some(expected), row.pod_ip.as_deref())?
                } else {
                    None
                };
                Ok(ActiveExecutionPodMatch {
                    matches_generation: true,
                    execution_abandoned: false,
                    pod_uid_superseded: false,
                    owner_replacing_pod: false,
                    placement_fence_uid: Some(Some(expected.clone())),
                    recorded_uid: Some(expected.clone()),
                    recorded_ip,
                })
            }
            None => {
                let replacing_in_flight = matches!(
                    row.state,
                    ActiveExecutionState::Creating | ActiveExecutionState::Recovering
                ) && !(row.state == ActiveExecutionState::Creating
                    && target.creating_repair_epoch == Some(row.updated_at));
                if target.pod_uid.is_some() && (row.pod_materialized_at_set || replacing_in_flight)
                {
                    return Ok(ActiveExecutionPodMatch {
                        matches_generation: false,
                        execution_abandoned: false,
                        pod_uid_superseded: true,
                        owner_replacing_pod: false,
                        placement_fence_uid: None,
                        recorded_uid: None,
                        recorded_ip: None,
                    });
                }
                Ok(ActiveExecutionPodMatch {
                    matches_generation: true,
                    execution_abandoned: false,
                    pod_uid_superseded: false,
                    owner_replacing_pod: false,
                    placement_fence_uid: (target.pod_uid.is_some()
                        && target.creating_repair_epoch.is_none())
                    .then_some(None),
                    recorded_uid: target.pod_uid.and_then(|uid| PodUid::parse(uid).ok()),
                    recorded_ip: None,
                })
            }
        }
    }

    /// `retry` states what the calling operation's retry would do; see
    /// [`PlacementRetryDisposition`]. It is a parameter because this helper
    /// serves both `create` (whose retry mints a new sandbox) and
    /// `start`/`wake`/`access` (whose retry converges).
    ///
    /// Two call sites pass a constant that is not their operation's:
    /// `resolve_pod_ip` and `wait_ready`'s `reflected_pod_ip` are
    /// each reached from create readiness *and* from wake/access/delete, and
    /// both pass `RetryDuplicatesExecution`. That is the safe direction — it
    /// costs a converging caller a 500 where a 503 would have done, rather
    /// than risking a duplicate sandbox — but it does mean contention on those
    /// two paths is not retryable even for operations that converge. Relaxing
    /// either one requires threading the disposition down through it, not
    /// flipping the constant.
    pub(in crate::controllers::substrate::kubernetes) async fn remember_pod_node_placement(
        &self,
        sandbox_id: SandboxId,
        pod: &SandboxPodView,
        retry: PlacementRetryDisposition,
    ) -> Result<Option<NodeId>, SubstrateError> {
        let Some((node_name, node)) = self.lookup_assigned_pod_node(pod).await? else {
            return Ok(None);
        };
        let (node_uid, node_id, role, instance_id, address) =
            Self::extract_pod_node_placement_fields(node_name, &node)?;

        self.store
            .persist_pod_node_placement_after_pod_exists(
                PodNodePlacement {
                    sandbox_id,
                    node_name,
                    node_uid,
                    node_id,
                    role,
                    address: Some(address.as_str()),
                    instance_id: instance_id.as_ref(),
                },
                retry,
            )
            .await?;
        Ok(Some(node_id))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn remember_pod_node_placement_fenced(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_pod_uid: Option<&str>,
        pod: &SandboxPodView,
        retry: PlacementRetryDisposition,
    ) -> Result<FencedNodePlacement, SubstrateError> {
        let Some((node_name, node)) = self.lookup_assigned_pod_node(pod).await? else {
            return Ok(FencedNodePlacement {
                node_id: None,
                matched_execution: true,
                execution_updated_at: None,
            });
        };
        let (node_uid, node_id, role, instance_id, address) =
            Self::extract_pod_node_placement_fields(node_name, &node)?;

        let execution_updated_at = self
            .store
            .persist_pod_node_placement_fenced_after_pod_exists(
                PodNodePlacement {
                    sandbox_id,
                    node_name,
                    node_uid,
                    node_id,
                    role,
                    address: Some(address.as_str()),
                    instance_id: instance_id.as_ref(),
                },
                generation,
                expected_pod_uid,
                retry,
            )
            .await?;
        Ok(FencedNodePlacement {
            node_id: execution_updated_at.map(|_| node_id),
            matched_execution: execution_updated_at.is_some(),
            execution_updated_at,
        })
    }

    async fn lookup_assigned_pod_node<'a>(
        &self,
        pod: &'a SandboxPodView,
    ) -> Result<Option<(&'a str, Arc<Node>)>, SubstrateError> {
        let Some(node_name) = assigned_pod_node_name(pod) else {
            return Ok(None);
        };
        // Watcher-backed cache is the warm path. Fall back to a direct API
        // get when the reflector hasn't populated this Node yet — startup
        // races and slow API-server lists must not break placement that the
        // scheduler has already committed.
        let node = match self.k8s.caches.node_by_name(node_name) {
            Some(node) => node,
            None => {
                let nodes: Api<Node> = Api::all(self.k8s.client.clone());
                let fetched = nodes.get(node_name).await.map_err(|err| match err {
                    kube::Error::Api(resp) if resp.code == 404 => SubstrateError::Unavailable {
                        message: format!(
                            "Kubernetes node {node_name} no longer exists; cannot preserve node-local sandbox placement"
                        ),
                    },
                    other => SubstrateError::Failed {
                        message: format!("failed to query Kubernetes node {node_name}: {other}"),
                    },
                })?;
                std::sync::Arc::new(fetched)
            }
        };
        Ok(Some((node_name, node)))
    }

    fn extract_pod_node_placement_fields<'a>(
        node_name: &str,
        node: &'a Node,
    ) -> Result<
        (
            &'a str,
            NodeId,
            SpaceNodeRole,
            Option<Ec2InstanceId>,
            String,
        ),
        SubstrateError,
    > {
        let node_uid = node
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| SubstrateError::Failed {
                message: format!("Kubernetes node {node_name} has no metadata.uid"),
            })?;
        let role = SpaceNodeRole::Worker;
        let node_id = space_k8s_node_id(node_name, role.as_str());
        let instance_id = k8s_node_instance_id(node);
        let address = k8s_node_internal_ip(node).ok_or_else(|| SubstrateError::Unavailable {
            message: format!(
                "Kubernetes node {node_name} has no InternalIP; cannot authenticate node-local reconciliation"
            ),
        })?;
        Ok((node_uid, node_id, role, instance_id, address))
    }

    /// Read-only counterpart to the repairing resolver. Missing, terminal, or
    /// replaced pods belong to recovery; a reachability probe cannot delete
    /// them or change execution tracking while its request can be cancelled.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_reachable_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        // A restoring pod is not reachable for a request: see
        // `join_restore_before_dial`.
        if is_suspended_lifecycle(&row.lifecycle)
            || is_restoring_lifecycle(&row.lifecycle)
            || execution_pod_lookup_in_flight(row.state)
            || row.pod_ip.is_none()
            || row.pod_uid.is_none()
        {
            return Ok(None);
        }
        // The row's IP for the cached pod it names is current before kubelet
        // publishes `status.podIP` once scheduler binding matches; when the
        // cache misses, holds a predecessor UID, or predates scheduler binding,
        // wait up to `reflector_wait` before falling back to an apiserver `GET`.
        let expected_uid = row.pod_uid.as_ref().map(PodUid::as_str);
        let expected_node = row.k8s_node_name.as_deref();
        let cached = self
            .read_cached_sandbox_pod_matching(
                &row.pod_name,
                self.budgets.reflector_wait,
                |pod, _now| {
                    pod.uid() == expected_uid
                        && (pod_status_is_settled(pod)
                            || assigned_pod_node_name(pod) == expected_node)
                },
            )
            .await;
        let pod = match cached {
            Some(pod) => pod,
            None => match fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                .await
                .map_err(|error| {
                    kube_error_for_substrate("failed to read reachable sandbox pod", error)
                })? {
                Some(pod) => pod,
                None => return Ok(None),
            },
        };
        if pod.is_deleting()
            || terminal_pod_phase(&pod).is_some()
            || pod.unschedulable_message().is_some()
            || row.pod_uid.is_none()
            || row.pod_uid.as_ref().map(PodUid::as_str) != pod.uid()
        {
            return Ok(None);
        }
        let address =
            current_pod_ip_with_source(&pod, row.pod_uid.as_ref(), row.pod_ip.as_deref())?;
        // Let the owned path publish changed routing evidence before returning
        // a client; otherwise ingress would repeatedly resolve stale metadata.
        if row.pod_ip != address.map(|(ip, _)| ip.to_string())
            || row.k8s_node_name.as_deref() != assigned_pod_node_name(&pod)
        {
            return Ok(None);
        }
        let Some(address) = address else {
            return Ok(None);
        };
        // An address kubelet has not published is reachable only while the
        // worker behind it reports this execution.
        let target = PodProxyWaitTarget::new(
            row.pod_name.as_str(),
            row.pod_uid.as_ref().map(PodUid::as_str),
            row.generation,
        );
        let Some(verified) = self
            .verified_runtime_pod_ip(sandbox_id, address, &target)
            .await
            .ok()
            .flatten()
        else {
            return Ok(None);
        };
        let Some(current) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if is_suspended_lifecycle(&current.lifecycle)
            || is_restoring_lifecycle(&current.lifecycle)
            || execution_pod_lookup_in_flight(current.state)
            || current.generation != row.generation
            || current.pod_uid != row.pod_uid
            || current.pod_ip != row.pod_ip
            || current.k8s_node_name != row.k8s_node_name
        {
            return Ok(None);
        }
        Ok(Some(verified))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        Ok(self
            .resolve_pod_instance(sandbox_id)
            .await?
            .map(|pod| pod.ip))
    }

    /// Bound degraded-mode fallback pod `GET`s by `readiness_pod_get` without
    /// putting the resolver's store mutations under that shorter timeout. When
    /// the phase `deadline` expires first, the read returns `Ok(None)` so the
    /// caller maps it to `Timeout`; when `readiness_pod_get` expires before
    /// `deadline` or the apiserver returns `429`/`5xx`, it fails fast with
    /// [`SubstrateError::Unavailable`]. Only an address for the execution and
    /// pod `target` names is returned, with where it comes from.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip_for_readiness(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
    ) -> Result<ReadinessPodResolution, SubstrateError> {
        self.resolve_pod_ip_for_readiness_with_mode(sandbox_id, target, deadline, false)
            .await
    }

    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip_for_readiness_uncached(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
    ) -> Result<ReadinessPodResolution, SubstrateError> {
        self.resolve_pod_ip_for_readiness_with_mode(sandbox_id, target, deadline, true)
            .await
    }

    async fn resolve_pod_ip_for_readiness_with_mode(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        force_uncached: bool,
    ) -> Result<ReadinessPodResolution, SubstrateError> {
        let out = self
            .resolve_pod_instance_inner(sandbox_id, Some((deadline, target, force_uncached)))
            .await?;
        let generation_matches = out.generation == Some(target.generation);
        let (resolved, resolved_uid) = match out.instance.filter(|pod| {
            pod.generation == target.generation
                && target
                    .pod_uid
                    .is_none_or(|uid| pod.pod_uid.as_ref().map(PodUid::as_str) == Some(uid))
        }) {
            Some(pod) => (Some((pod.ip, pod.source)), pod.pod_uid),
            None => (None, None),
        };
        let scheduled_pod = out.scheduled_pod.filter(|pod| {
            generation_matches && target.pod_uid.is_none_or(|uid| pod.uid() == Some(uid))
        });
        Ok(ReadinessPodResolution {
            resolved,
            resolved_uid,
            scheduled_pod,
            used_uncached_get: out.used_uncached_get,
            cache_matches_execution: generation_matches && out.cache_matches_execution,
            terminal_or_absent_retired: out.terminal_or_absent_retired,
            pod_uid_superseded: out.pod_uid_superseded,
            deadline_elapsed: out.deadline_elapsed,
            execution_in_flight: out.execution_in_flight,
        })
    }

    /// [`Self::resolve_pod_ip`], with the identity of the pod the address
    /// belongs to.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_instance(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<ResolvedPodInstance>, SubstrateError> {
        Ok(self
            .resolve_pod_instance_inner(sandbox_id, None)
            .await?
            .instance)
    }

    async fn resolve_pod_instance_inner(
        &self,
        sandbox_id: SandboxId,
        readiness: Option<(Instant, &PodProxyWaitTarget<'_>, bool)>,
    ) -> Result<PodInstanceResolution, SubstrateError> {
        let force_uncached_readiness =
            readiness.is_some_and(|(_, _, force_uncached)| force_uncached);
        let mut prior_uncached_get = false;
        for attempt in 0..3 {
            match self
                .resolve_pod_instance_once(sandbox_id, readiness)
                .await?
            {
                PodInstanceAttempt::Done(mut out) => {
                    out.used_uncached_get |= prior_uncached_get;
                    return Ok(*out);
                }
                PodInstanceAttempt::Retry { used_uncached_get } => {
                    prior_uncached_get |= used_uncached_get;
                    if attempt == 2 || (force_uncached_readiness && prior_uncached_get) {
                        let generation = self
                            .store
                            .active_execution_status(sandbox_id)
                            .await?
                            .map(|row| row.generation);
                        return Ok(PodInstanceResolution {
                            generation,
                            used_uncached_get: prior_uncached_get,
                            ..PodInstanceResolution::default()
                        });
                    }
                }
            }
        }
        unreachable!("bounded retry loop returns within 3 attempts")
    }

    async fn resolve_pod_instance_once(
        &self,
        sandbox_id: SandboxId,
        readiness: Option<(Instant, &PodProxyWaitTarget<'_>, bool)>,
    ) -> Result<PodInstanceAttempt, SubstrateError> {
        let Some(mut row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                terminal_or_absent_retired: true,
                ..PodInstanceResolution::default()
            }));
        };
        if is_suspended_lifecycle(&row.lifecycle) {
            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                generation: Some(row.generation),
                terminal_or_absent_retired: readiness.is_some(),
                ..PodInstanceResolution::default()
            }));
        }
        let readiness_deadline = readiness.map(|(deadline, _, _)| deadline);
        if let Some((_, target, _)) = readiness {
            if row.generation != target.generation {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    terminal_or_absent_retired: true,
                    ..PodInstanceResolution::default()
                }));
            }
            if let (Some(recorded_uid), Some(target_uid)) =
                (row.pod_uid.as_ref().map(PodUid::as_str), target.pod_uid)
                && recorded_uid != target_uid
            {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    pod_uid_superseded: target.abort_on_uid_change,
                    ..PodInstanceResolution::default()
                }));
            }
            let replacing_in_flight = matches!(
                row.state,
                ActiveExecutionState::Creating | ActiveExecutionState::Recovering
            ) && !(row.state == ActiveExecutionState::Creating
                && target.creating_repair_epoch == Some(row.updated_at));
            if target.pod_uid.is_some()
                && row.pod_uid.is_none()
                && (row.pod_materialized_at_set || replacing_in_flight)
            {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    terminal_or_absent_retired: !replacing_in_flight,
                    pod_uid_superseded: replacing_in_flight && target.abort_on_uid_change,
                    ..PodInstanceResolution::default()
                }));
            }
        }

        let readiness_owns_created_pod = row.state == ActiveExecutionState::Creating
            && readiness.is_some_and(|(_, target, _)| {
                target.pod_uid.is_some_and(|target_uid| {
                    row.pod_uid.as_ref().map(PodUid::as_str) == Some(target_uid)
                        || (!row.pod_materialized_at_set
                            && row.pod_uid.is_none()
                            && target.creating_repair_epoch == Some(row.updated_at))
                })
            });
        let readiness_joiner_with_live_owner = readiness
            .is_some_and(|(_, target, _)| target.pod_uid.is_none())
            && row.activation_attempt_live;
        let expected_uid = row.pod_uid.as_ref().map(PodUid::as_str).or_else(|| {
            readiness
                .filter(|_| readiness_owns_created_pod || !row.pod_materialized_at_set)
                .and_then(|(_, target, _)| target.pod_uid)
        });
        // External callers (`readiness_deadline.is_none()`) treat both
        // `creating` and `recovering` as in-flight so route lookups do not
        // touch an unready recovery row. Readiness waits (`Some(_)`) must
        // resolve a `recovering` pod's IP once `pod_uid` is known because
        // `record_pod_metadata` keeps `state = 'recovering'` until
        // started-sandbox recovery completes, and an owning UID-pinned wait
        // (`target.pod_uid.is_some()` on a never-materialized row) must also
        // resolve and repair a `creating` row if the post-create metadata
        // write failed transiently.
        let in_flight = execution_pod_lookup_in_flight(row.state)
            && !(readiness_deadline.is_some()
                && ((row.state == ActiveExecutionState::Recovering && expected_uid.is_some())
                    || readiness_owns_created_pod));
        let (pod, mut used_uncached_get, mut cache_matches_execution) = if in_flight {
            let allow_stale_restore_get = readiness.is_none_or(|(_, _, force_uncached)| {
                !readiness_joiner_with_live_owner && force_uncached
            });
            if !allow_stale_restore_get
                || !stale_restore_execution_create(
                    &row.lifecycle,
                    row.state,
                    row.create_age_expired,
                )
            {
                let cache_matches = self.lookup_sandbox_pod(&row.pod_name).is_some_and(|pod| {
                    self.cached_pod_is_authoritative(&pod, expected_uid, false, Instant::now())
                });
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    cache_matches_execution: cache_matches,
                    execution_in_flight: true,
                    ..PodInstanceResolution::default()
                }));
            }
            (
                self.get_pod_for_resolution(
                    &row.pod_name,
                    readiness_deadline,
                    "failed to verify stale creating restore pod",
                )
                .await?,
                true,
                false,
            )
        } else if let Some((_, _, force_uncached)) = readiness {
            // Readiness callers pass `force_uncached = false` when resolving
            // from the reflector cache, and `force_uncached = true` from paced
            // degraded-reflector fallback reads (`fallback.due(now, self)`).
            // Never fall through to an uncached `GET` when `force_uncached` is
            // `false`, or cache updates arriving while the reflector is silent
            // or degraded would bypass `ReadinessFallback` pacing.
            let now = Instant::now();
            if !force_uncached {
                if let Some(pod) = self.lookup_sandbox_pod(&row.pod_name)
                    && self.cached_pod_is_authoritative(&pod, expected_uid, false, now)
                {
                    crate::metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
                    (PodGet::Observed(Some(pod)), false, true)
                } else {
                    return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                        generation: Some(row.generation),
                        execution_in_flight: readiness_joiner_with_live_owner,
                        ..PodInstanceResolution::default()
                    }));
                }
            } else {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
                (
                    self.get_pod_for_resolution(
                        &row.pod_name,
                        readiness_deadline,
                        "failed to query K8s pod",
                    )
                    .await?,
                    true,
                    false,
                )
            }
        } else if let Some(pod) = self
            .read_cached_sandbox_pod_for_uid(
                &row.pod_name,
                expected_uid,
                false,
                self.budgets.reflector_wait,
            )
            .await
        {
            (PodGet::Observed(Some(pod)), false, true)
        } else {
            (
                self.get_pod_for_resolution(
                    &row.pod_name,
                    readiness_deadline,
                    "failed to query K8s pod",
                )
                .await?,
                true,
                false,
            )
        };

        let verified_uncached = used_uncached_get;
        let PodGet::Observed(pod) = pod else {
            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                generation: Some(row.generation),
                used_uncached_get,
                cache_matches_execution,
                deadline_elapsed: true,
                ..PodInstanceResolution::default()
            }));
        };
        let pod = match pod {
            Some(pod) => pod,
            None => {
                // `pod` can only be `None` here when `get_pod_for_resolution`
                // already returned an authoritative uncached `404` above
                // (`verified_uncached` is always `true`), because a reflector
                // cache miss alone never produces `PodGet::Observed(None)`.
                // Guard explicitly so a future caller that surfaces a cached
                // absence still confirms once via uncached `GET` without
                // issuing duplicate back-to-back `404` `GET`s.
                let confirmed = if verified_uncached {
                    None
                } else {
                    used_uncached_get = true;
                    cache_matches_execution = false;
                    let PodGet::Observed(pod) = self
                        .get_pod_for_resolution(
                            &row.pod_name,
                            readiness_deadline,
                            "failed to verify missing Kubernetes sandbox pod",
                        )
                        .await?
                    else {
                        return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                            generation: Some(row.generation),
                            used_uncached_get,
                            deadline_elapsed: true,
                            ..PodInstanceResolution::default()
                        }));
                    };
                    pod
                };
                match confirmed {
                    Some(pod) => pod,
                    None => {
                        if row.state == ActiveExecutionState::Recovering {
                            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                                generation: Some(row.generation),
                                used_uncached_get,
                                ..PodInstanceResolution::default()
                            }));
                        }
                        if row.state == ActiveExecutionState::RecoveryFailed {
                            match self
                                .store
                                .converge_failed_recovery_after_pod_absence_if_generation(
                                    sandbox_id,
                                    row.generation,
                                    row.pod_uid.as_ref().map(PodUid::as_str),
                                    row.pod_ip.as_deref(),
                                    row.k8s_node_name.as_deref(),
                                )
                                .await?
                            {
                                FailedRecoveryPodAbsenceOutcome::NotMatched => {}
                                FailedRecoveryPodAbsenceOutcome::ClaimPreserved => tracing::debug!(
                                    sandbox_id = %sandbox_id,
                                    generation = row.generation,
                                    pod_name = %row.pod_name,
                                    "failed recovery pod is absent; clearing cached route and preserving execution claim"
                                ),
                                FailedRecoveryPodAbsenceOutcome::ConvergedSuspended => {
                                    tracing::info!(
                                        sandbox_id = %sandbox_id,
                                        generation = row.generation,
                                        pod_name = %row.pod_name,
                                        "failed recovery pod is absent; converged current durable snapshot to suspended"
                                    )
                                }
                            }
                            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                                generation: Some(row.generation),
                                used_uncached_get,
                                ..PodInstanceResolution::default()
                            }));
                        }
                        if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                            && let Some(pod_uid) = row.pod_uid.as_ref()
                            && self
                                .store
                                .finalize_missing_suspending_pod_with_current_snapshot(
                                    sandbox_id,
                                    row.generation,
                                    pod_uid.as_str(),
                                )
                                .await?
                        {
                            tracing::info!(
                                sandbox_id = %sandbox_id,
                                generation = row.generation,
                                pod_name = %row.pod_name,
                                "finalized suspended sandbox after its pod disappeared with a current durable snapshot"
                            );
                            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                                generation: Some(row.generation),
                                used_uncached_get,
                                ..PodInstanceResolution::default()
                            }));
                        }
                        if readiness_joiner_with_live_owner {
                            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                                generation: Some(row.generation),
                                used_uncached_get,
                                cache_matches_execution: false,
                                execution_in_flight: true,
                                ..PodInstanceResolution::default()
                            }));
                        }
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            pod_name = %row.pod_name,
                            cached_pod_ip = ?row.pod_ip,
                            "tracked Kubernetes sandbox pod is authoritatively absent; clearing cached pod IP"
                        );
                        let retired = self
                            .store
                            .forget_pod_tracking_if_generation_and_observed_attempt(
                                sandbox_id,
                                row.generation,
                                row.activation_attempt_id,
                                ObservedAttemptPodFence::observed_row(
                                    row.pod_uid.as_ref().map(PodUid::as_str),
                                    row.pod_materialized_at_set,
                                    row.updated_at,
                                ),
                                PodTrackingCleanupCause::MissingPodObservation,
                            )
                            .await?;
                        let terminal_or_absent_retired = if retired {
                            true
                        } else {
                            self.store
                                .active_execution_status(sandbox_id)
                                .await?
                                .is_none_or(|current| {
                                    current.generation != row.generation
                                        || is_suspended_lifecycle(&current.lifecycle)
                                })
                        };
                        return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                            generation: Some(row.generation),
                            used_uncached_get,
                            terminal_or_absent_retired,
                            ..PodInstanceResolution::default()
                        }));
                    }
                }
            }
        };

        if readiness_deadline.is_some()
            && let Some(expected) = expected_uid
            && let Some(actual) = pod.uid()
            && actual != expected
        {
            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                generation: Some(row.generation),
                used_uncached_get,
                cache_matches_execution: false,
                execution_in_flight: readiness_joiner_with_live_owner,
                ..PodInstanceResolution::default()
            }));
        }

        if readiness_joiner_with_live_owner {
            let pod_is_restore_in_progress = pod
                .has_annotation(SandboxPodKey::SuspendRestoreStorageUri)
                || pod.has_annotation(SandboxPodKey::SuspendRestoreSourceNode);
            let stale_for_owner = pod.is_deleting()
                || terminal_pod_phase(&pod).is_some()
                || (is_suspended_lifecycle(&row.lifecycle) && !pod_is_restore_in_progress)
                || (row.lifecycle == SandboxLifecycle::Stopped.as_str()
                    && self
                        .store
                        .execution_predates_current_stop(sandbox_id, row.generation)
                        .await?);
            if stale_for_owner {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: false,
                    execution_in_flight: true,
                    ..PodInstanceResolution::default()
                }));
            }
        }

        let pod = if let Some(cached_phase) = terminal_pod_phase(&pod) {
            if row.state == ActiveExecutionState::Recovering {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution,
                    ..PodInstanceResolution::default()
                }));
            }
            let authoritative_pod = if verified_uncached {
                Some(Arc::clone(&pod))
            } else {
                used_uncached_get = true;
                let PodGet::Observed(authoritative_pod) = self
                    .get_pod_for_resolution(
                        &row.pod_name,
                        readiness_deadline,
                        "failed to verify terminal Kubernetes sandbox pod",
                    )
                    .await?
                else {
                    return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                        generation: Some(row.generation),
                        used_uncached_get,
                        cache_matches_execution: false,
                        deadline_elapsed: true,
                        ..PodInstanceResolution::default()
                    }));
                };
                authoritative_pod
            };
            if readiness_deadline.is_some()
                && let Some(expected) = expected_uid
                && let Some(actual) = authoritative_pod.as_deref().and_then(SandboxPodView::uid)
                && actual != expected
            {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: false,
                    execution_in_flight: readiness_joiner_with_live_owner,
                    ..PodInstanceResolution::default()
                }));
            }
            if let (Some(row_uid), Some(actual_uid)) = (
                row.pod_uid.as_ref().map(PodUid::as_str),
                authoritative_pod.as_deref().and_then(SandboxPodView::uid),
            ) && actual_uid != row_uid
            {
                let row_advanced = self
                    .store
                    .active_execution_status(sandbox_id)
                    .await?
                    .is_none_or(|current| {
                        current.generation != row.generation
                            || current.pod_uid != row.pod_uid
                            || is_suspended_lifecycle(&current.lifecycle)
                    });
                if row_advanced {
                    return Ok(PodInstanceAttempt::Retry { used_uncached_get });
                }
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: false,
                    ..PodInstanceResolution::default()
                }));
            }
            if let Some(phase) = terminal_cleanup_phase_after_uncached_check(
                cached_phase,
                authoritative_pod.as_deref(),
            ) {
                let publication_grace_remaining = authoritative_pod
                    .as_ref()
                    .filter(|_| phase == TerminalPodPhase::Succeeded)
                    .and_then(|pod| {
                        succeeded_pod_publication_grace_remaining(pod, jiff::Timestamp::now())
                    });
                if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                    && publication_grace_remaining.is_some()
                {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        pod_name = %row.pod_name,
                        generation = row.generation,
                        pod_uid = ?row.pod_uid,
                        lifecycle = %row.lifecycle,
                        grace_remaining_ms = publication_grace_remaining
                            .map(|remaining| remaining.as_millis()),
                        "deferring successful terminal pod cleanup while suspend publication may still arrive"
                    );
                    return Err(SubstrateError::suspend_in_flight(sandbox_id));
                }
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %row.pod_name,
                    cached_phase = %cached_phase,
                    phase = %phase,
                    cached_pod_ip = ?row.pod_ip,
                    "tracked Kubernetes sandbox pod is terminal after uncached verification; deleting it and clearing cached pod IP"
                );
                let delete_uid = authoritative_pod
                    .as_deref()
                    .and_then(SandboxPodView::uid)
                    .or_else(|| pod.uid())
                    .or(expected_uid);
                let retired = self
                    .delete_pod_and_forget_tracking(
                        TerminalPodCleanup {
                            sandbox_id,
                            pod_name: &row.pod_name,
                            generation: row.generation,
                            expected_delete_uid: delete_uid,
                            activation_attempt_id: row.activation_attempt_id,
                            pod_fence: ObservedAttemptPodFence::observed_row(
                                row.pod_uid.as_ref().map(PodUid::as_str),
                                row.pod_materialized_at_set,
                                row.updated_at,
                            ),
                        },
                        PodTrackingCleanupCause::MissingPodObservation,
                    )
                    .await?;
                let terminal_or_absent_retired = retired
                    || self
                        .store
                        .active_execution_status(sandbox_id)
                        .await?
                        .is_none_or(|current| {
                            current.generation != row.generation
                                || is_suspended_lifecycle(&current.lifecycle)
                        });
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: cache_matches_execution && terminal_or_absent_retired,
                    terminal_or_absent_retired,
                    ..PodInstanceResolution::default()
                }));
            }
            // The cached terminal view was stale; mark it non-authoritative so
            // readiness loops don't re-verify the same cached `Arc` at 250ms.
            cache_matches_execution = false;
            tracing::info!(
                sandbox_id = %sandbox_id,
                pod_name = %row.pod_name,
                cached_phase = %cached_phase,
                cached_pod_ip = ?row.pod_ip,
                "cached Kubernetes sandbox pod was terminal but uncached pod is still active; keeping tracking"
            );
            authoritative_pod.expect("non-terminal verification result requires an existing pod")
        } else {
            pod
        };

        if let Some(message) = pod.unschedulable_message() {
            if readiness_joiner_with_live_owner
                && crate::controllers::substrate::kubernetes::is_restoring_lifecycle(&row.lifecycle)
                && pod.has_required_source_pin()
            {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: false,
                    execution_in_flight: true,
                    ..PodInstanceResolution::default()
                }));
            }
            return Err(SubstrateError::Unschedulable {
                message: format!("sandbox pod is unschedulable: {message}"),
            });
        }

        if in_flight && row.pod_materialized_at_set && row.pod_uid.is_none() {
            return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                generation: Some(row.generation),
                used_uncached_get,
                cache_matches_execution: false,
                execution_in_flight: true,
                ..PodInstanceResolution::default()
            }));
        }

        let current_node_name = assigned_pod_node_name(&pod).map(str::to_owned);
        let scheduled_pod = current_node_name.as_ref().map(|_| (*pod).clone());
        let pod_uid = pod.uid().map(str::to_owned);
        let pod_client = pod.client_id();
        let mut repaired_creating_pod_uid = false;

        if readiness_owns_created_pod
            && row.state == ActiveExecutionState::Creating
            && row.pod_uid.is_none()
        {
            let Some(uid) = pod_uid.as_deref() else {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: false,
                    ..PodInstanceResolution::default()
                }));
            };
            let Some(repair_epoch) =
                readiness.and_then(|(_, target, _)| target.creating_repair_epoch)
            else {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(row.generation),
                    used_uncached_get,
                    cache_matches_execution: false,
                    ..PodInstanceResolution::default()
                }));
            };
            match self
                .store
                .repair_creating_pod_uid(
                    sandbox_id,
                    row.generation,
                    uid,
                    repair_epoch,
                    pod_client.as_ref(),
                )
                .await
            {
                Ok(true) => {
                    row.pod_uid = PodUid::parse(uid).ok();
                    row.state = ActiveExecutionState::Created;
                    repaired_creating_pod_uid = true;
                }
                Ok(false) => {
                    let Some(current) = self.store.active_execution_status(sandbox_id).await?
                    else {
                        return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                            generation: Some(row.generation),
                            used_uncached_get,
                            cache_matches_execution: false,
                            ..PodInstanceResolution::default()
                        }));
                    };
                    if current.generation != row.generation
                        || is_suspended_lifecycle(&current.lifecycle)
                        || current.pod_uid.as_ref().map(PodUid::as_str) != Some(uid)
                    {
                        return Ok(PodInstanceAttempt::Retry { used_uncached_get });
                    }
                    repaired_creating_pod_uid = current.state != ActiveExecutionState::Creating;
                    row = current;
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        error = %err,
                        "failed to repair creating Kubernetes pod metadata after pod lookup"
                    );
                    let resolved = current_pod_ip_with_source(&pod, None, row.pod_ip.as_deref())?;
                    return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                        instance: resolved.map(|(ip, source)| ResolvedPodInstance {
                            ip,
                            source,
                            pod_uid: pod_uid.as_deref().and_then(|uid| PodUid::parse(uid).ok()),
                            generation: row.generation,
                            sandbox_restoring: is_restoring_lifecycle(&row.lifecycle),
                        }),
                        scheduled_pod,
                        generation: Some(row.generation),
                        used_uncached_get,
                        cache_matches_execution,
                        ..PodInstanceResolution::default()
                    }));
                }
            }
        }

        if row.k8s_node_name.as_deref() != current_node_name.as_deref() {
            let placement = self
                .remember_pod_node_placement_fenced(
                    sandbox_id,
                    row.generation,
                    row.pod_uid.as_ref().map(PodUid::as_str),
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?;
            if !placement.matched_execution {
                return Ok(PodInstanceAttempt::Retry { used_uncached_get });
            }
        }

        let resolved =
            current_pod_ip_with_source(&pod, row.pod_uid.as_ref(), row.pod_ip.as_deref())?;
        let pod_ip = resolved.map(|(ip, _)| ip);
        let pod_ip_string = pod_ip.map(|ip| ip.to_string());
        if repaired_creating_pod_uid {
            if let (Some(uid), Some(ip)) = (row.pod_uid.as_ref().map(PodUid::as_str), pod_ip)
                && row.pod_ip.as_deref() != pod_ip_string.as_deref()
            {
                match self
                    .store
                    .record_confirmed_pod_ip(sandbox_id, row.generation, uid, ip)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        return Ok(PodInstanceAttempt::Retry { used_uncached_get });
                    }
                    Err(err) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            generation = row.generation,
                            error = %err,
                            "failed to persist confirmed Kubernetes pod IP after creating repair"
                        );
                    }
                }
            }
        } else if row.pod_uid.as_ref().map(PodUid::as_str) != pod_uid.as_deref()
            || row.pod_ip.as_deref() != pod_ip_string.as_deref()
        {
            match self
                .store
                .persist_pod_metadata_if_unchanged(
                    sandbox_id,
                    row.generation,
                    PodMetadataFence {
                        expected_pod_uid: row.pod_uid.as_ref().map(PodUid::as_str),
                        expected_pod_ip: row.pod_ip.as_deref(),
                        expected_pod_materialized_at_set: row.pod_materialized_at_set,
                    },
                    pod_uid.as_deref(),
                    pod_ip_string.as_deref(),
                    pod_client.as_ref(),
                )
                .await
            {
                Ok(FencedPodMetadataOutcome::Applied(_)) => {}
                Ok(FencedPodMetadataOutcome::Retired | FencedPodMetadataOutcome::Superseded) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        "pod metadata write fenced out after pod lookup: the execution was \
                         retired or replaced mid-flight"
                    );
                    return Ok(PodInstanceAttempt::Retry { used_uncached_get });
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        error = %err,
                        "failed to persist Kubernetes pod metadata after pod lookup"
                    );
                }
            }
        } else if self.k8s.activation_client_attribution.is_enabled()
            && let Some(pod_uid) = pod_uid.as_deref()
            && crate::controllers::substrate::kubernetes::store::attribution_update(
                &row.lifecycle,
                Some(pod_uid),
                Some(pod_uid),
                pod_client.as_ref(),
                row.activating_client_id.as_ref(),
            )
            .is_some()
        {
            // The pod is the one the row tracks, and only the record lags its
            // label: repair the record alone, leaving the row's state as it is.
            if let Err(err) = self
                .store
                .repair_runtime_attribution(
                    sandbox_id,
                    row.generation,
                    pod_uid,
                    pod_client.as_ref(),
                )
                .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    error = %err,
                    "failed to repair the sandbox's activating client after pod lookup"
                );
            }
        }

        let mut sandbox_restoring = is_restoring_lifecycle(&row.lifecycle);
        if readiness.is_none() && resolved.is_some() {
            let Some(current) = self.store.active_execution_status(sandbox_id).await? else {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    terminal_or_absent_retired: true,
                    used_uncached_get,
                    ..PodInstanceResolution::default()
                }));
            };
            if is_suspended_lifecycle(&current.lifecycle) {
                return Ok(PodInstanceAttempt::done(PodInstanceResolution {
                    generation: Some(current.generation),
                    used_uncached_get,
                    ..PodInstanceResolution::default()
                }));
            }
            let current_uid = current.pod_uid.as_ref().map(PodUid::as_str);
            let uid_drifted = match (current_uid, pod_uid.as_deref()) {
                (Some(recorded), Some(observed)) => recorded != observed,
                (Some(_), None) => true,
                (None, Some(_)) => current.pod_materialized_at_set,
                (None, None) => false,
            };
            let ip_drifted = current.pod_ip.is_some() && current.pod_ip != pod_ip_string;
            let node_drifted = current.k8s_node_name.is_some()
                && assigned_pod_node_name(&pod)
                    .is_some_and(|node| current.k8s_node_name.as_deref() != Some(node));
            if current.generation != row.generation
                || execution_pod_lookup_in_flight(current.state)
                || uid_drifted
                || ip_drifted
                || node_drifted
            {
                return Ok(PodInstanceAttempt::Retry { used_uncached_get });
            }
            sandbox_restoring = is_restoring_lifecycle(&current.lifecycle);
        }

        Ok(PodInstanceAttempt::done(PodInstanceResolution {
            instance: resolved.map(|(ip, source)| ResolvedPodInstance {
                ip,
                source,
                pod_uid: pod_uid.as_deref().and_then(|uid| PodUid::parse(uid).ok()),
                generation: row.generation,
                sandbox_restoring,
            }),
            scheduled_pod,
            generation: Some(row.generation),
            used_uncached_get,
            cache_matches_execution,
            terminal_or_absent_retired: false,
            pod_uid_superseded: false,
            deadline_elapsed: false,
            execution_in_flight: false,
        }))
    }
}

#[cfg(test)]
mod tests;
