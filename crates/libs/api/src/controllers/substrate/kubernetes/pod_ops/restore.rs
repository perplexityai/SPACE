//! Restore hydration and plans: hydrating a restored execution from its pod and
//! resolving the suspend or stopped restore plan.

use super::*;

pub(in crate::controllers::substrate::kubernetes) struct SuspendRestorePlan {
    pub source_node_name: Option<String>,
    pub storage_uri: Option<String>,
}

/// Outcome of [`KubernetesSubstrate::hydrate_restored_execution`]: the
/// kube-verified repair of an active execution row whose pod metadata a dead
/// wake handler never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum ExecutionHydrationOutcome {
    /// A live Running pod matching the active execution's generation was
    /// found and its `pod_uid`/`pod_ip` were persisted onto the execution
    /// row. The execution is now routable.
    Hydrated,
    /// The execution row was already routable (`running` + `pod_ip`) — a
    /// concurrent hydration (e.g. the real wake path) won the race.
    AlreadyRoutable,
    /// The execution is not routable: the row has no active execution, the
    /// pod is gone from the apiserver, the pod that exists does not match the
    /// active execution (wrong sandbox/generation, not Running, deleting, or
    /// no IP yet), the row is `recovering`, or the execution was retired or
    /// replaced mid-read. At most, the pod identity was recorded on an
    /// execution row that is not running, such as a `recovering` one.
    NotRoutable,
}

/// Whether `pod`'s identity annotations match this sandbox execution — the
/// sandbox-id and execution-generation stamps written at create time. Phase-
/// agnostic (unlike [`restored_pod_routability`]): used to recognize a
/// still-Pending successor a prior interrupted attempt created.
pub(super) fn pod_matches_execution_generation(
    pod: &SandboxPodView,
    sandbox_id: SandboxId,
    generation: i64,
) -> bool {
    pod.annotation(SandboxPodKey::SandboxId) == Some(sandbox_id.to_string().as_str())
        && pod
            .annotation(SandboxPodKey::ExecutionGeneration)
            .and_then(|value| value.parse::<i64>().ok())
            == Some(generation)
}

/// Pure routability check for a pod fetched to back an active execution.
/// Returns the identity to hydrate only when **all** of these hold:
///
/// * the pod's sandbox-id and execution-generation annotations match this
///   execution ([`pod_matches_execution_generation`] — never trust a name
///   collision or a stale prior-generation pod),
/// * the pod is phase `Running` and not being deleted,
/// * the pod has an IP.
pub(super) fn restored_pod_routability(
    pod: &SandboxPodView,
    sandbox_id: SandboxId,
    generation: i64,
) -> Option<RoutablePodIdentity> {
    if !pod_matches_execution_generation(pod, sandbox_id, generation) {
        return None;
    }
    if pod.is_deleting() {
        return None;
    }
    if pod.phase() != Some("Running") {
        return None;
    }
    let pod_ip = pod.pod_ip()?.to_owned();
    Some(RoutablePodIdentity {
        pod_uid: pod.uid().map(str::to_owned),
        pod_ip,
    })
}

impl KubernetesSubstrate {
    /// Kube-verified repair of an active execution row whose pod metadata was
    /// never persisted — the wake handler created the pod and the node-side
    /// restore succeeded, but the handler died before `persist_pod_metadata`
    /// ran, leaving the execution unroutable (`pod_ip` NULL / not `running`)
    /// while a healthy pod serves traffic. It answers this substrate's
    /// [`SubstrateAdapter::ensure_routable_execution`], which the cluster
    /// controller's transient-lifecycle sweeper and the api's cancelled-wake
    /// completion redrive call before converging an alive `restoring` row to
    /// `started`: in this substrate `started` is a contract that a routable
    /// execution exists, and asserting it without one wedges the sandbox
    /// behind the gen≥2 fresh-pod refusal.
    ///
    /// Truth-copying, not truth-inventing: the only write is
    /// [`K8sStore::persist_pod_metadata`] — the exact statement the wake path
    /// uses, CAS-guarded on `sandbox_id + generation + active` — and only
    /// with values read from an **uncached** apiserver GET of a pod whose
    /// sandbox-id and generation annotations match the active execution
    /// ([`restored_pod_routability`]). Placement is never written: an
    /// execution row with a probe-reachable node necessarily had its node
    /// placement persisted transactionally with `k8s_node_name` already.
    ///
    /// `budget` bounds the apiserver GET so a wedged apiserver can't stall
    /// the caller's loop. A timeout or transport error is returned as an
    /// error (the caller defers and retries next tick), never as
    /// `NotRoutable`.
    ///
    /// A hydration write that no-ops because the active execution was
    /// retired or replaced between the apiserver GET and the persist (the
    /// generation-guarded `persist_pod_metadata` reports this) returns
    /// `NotRoutable`, never `Hydrated`. Even so, the returned outcome is
    /// advisory, not a fence — `Hydrated`/`AlreadyRoutable` can go stale the
    /// instant this returns. Each caller's converge CAS (the sweeper's and
    /// the redrive's) re-checks routability inside the `UPDATE` itself; do
    /// not treat this outcome as proof for any other lifecycle write.
    pub(in crate::controllers::substrate::kubernetes) async fn hydrate_restored_execution(
        &self,
        sandbox_id: SandboxId,
        budget: Duration,
    ) -> Result<ExecutionHydrationOutcome, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let Some(execution) = self.store.active_execution_status(sandbox_uuid).await? else {
            return Ok(ExecutionHydrationOutcome::NotRoutable);
        };
        if execution.state == ActiveExecutionState::Running && execution.pod_ip.is_some() {
            return Ok(ExecutionHydrationOutcome::AlreadyRoutable);
        }
        // Authoritative uncached GET: DB state is about to be written from
        // this answer, so reflector lag must not stand in for the apiserver.
        let pod = match timeout(budget, self.read_sandbox_pod_uncached(&execution.pod_name)).await {
            Ok(Ok(pod)) => pod,
            Ok(Err(err)) => {
                return Err(SubstrateError::Failed {
                    message: format!(
                        "failed to fetch Kubernetes pod {} for execution hydration: {err}",
                        execution.pod_name
                    ),
                });
            }
            Err(_) => {
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "timed out fetching Kubernetes pod {} for execution hydration",
                        execution.pod_name
                    ),
                    phase: None,
                });
            }
        };
        let Some(identity) = pod
            .as_ref()
            .and_then(|pod| restored_pod_routability(pod, sandbox_uuid, execution.generation))
        else {
            return Ok(ExecutionHydrationOutcome::NotRoutable);
        };
        match self
            .store
            .persist_pod_metadata(
                sandbox_uuid,
                execution.generation,
                identity.pod_uid.as_deref(),
                Some(identity.pod_ip.as_str()),
            )
            .await?
        {
            // `Hydrated` is claimed only when the resulting row satisfies
            // the exact predicate the converge CAS checks: `running` with a
            // pod IP.
            Some(ActiveExecutionState::Running) => Ok(ExecutionHydrationOutcome::Hydrated),
            // The row exists but is not `running` — e.g. `recovering`, whose
            // promote to `running` is owned by the node-side recovery flow
            // and deliberately not overridden by a metadata write. The pod
            // IP is recorded, but the execution is not routable yet.
            Some(_) => Ok(ExecutionHydrationOutcome::NotRoutable),
            // The active execution was retired or replaced (a concurrent
            // wake claimed a new generation) between the apiserver GET and
            // the persist — nothing was written and the pod we verified no
            // longer backs the active execution.
            None => Ok(ExecutionHydrationOutcome::NotRoutable),
        }
    }

    async fn existing_suspend_restore_annotations(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SuspendRestorePlan>, SubstrateError> {
        let Some(execution) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        let Some(pod) = self
            .read_sandbox_pod(&execution.pod_name)
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("failed to query Kubernetes restore pod: {err}"),
            })?
        else {
            return Ok(None);
        };
        let storage_uri = pod
            .annotation(SandboxPodKey::SuspendRestoreStorageUri)
            .map(str::to_owned);
        if let Some(storage_uri) = &storage_uri {
            StorageUri::parse(storage_uri).map_err(|err| SubstrateError::Failed {
                message: format!("invalid suspend restore storage URI on Kubernetes pod: {err}"),
            })?;
        }
        let source_node_name = pod
            .annotation(SandboxPodKey::SuspendRestoreSourceNode)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        if source_node_name.is_none() && storage_uri.is_none() {
            return Ok(None);
        }
        Ok(Some(SuspendRestorePlan {
            source_node_name,
            storage_uri,
        }))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn suspend_restore_plan(
        &self,
        sandbox: &Sandbox,
        evacuation: bool,
    ) -> Result<SuspendRestorePlan, SubstrateError> {
        let existing = self
            .existing_suspend_restore_annotations(sandbox.id)
            .await?;
        let storage_uri = match existing.as_ref().and_then(|plan| plan.storage_uri.clone()) {
            Some(storage_uri) => Some(storage_uri),
            None => {
                // Evacuation restores and restore retries select the newest
                // full snapshot rather than the placement-fresh one: it waits
                // for the current suspend upload (not a stale prior-cycle
                // snapshot) and survives a failed restore attempt's placement
                // bump (see `selected_suspend_restore_storage_uri`).
                self.store
                    .selected_suspend_restore_storage_uri(sandbox, evacuation)
                    .await?
            }
        };
        if evacuation {
            return storage_uri
                .map(|storage_uri| SuspendRestorePlan {
                    source_node_name: None,
                    storage_uri: Some(storage_uri),
                })
                .ok_or_else(|| SubstrateError::Conflict {
                    message: "cannot evacuate suspended sandbox without a ready remote snapshot"
                        .to_string(),
                });
        }
        let source_node_name = match existing.and_then(|plan| plan.source_node_name) {
            Some(source_node_name) => Some(source_node_name),
            None => self.store.restore_source_node_name(sandbox).await?,
        };
        if source_node_name.is_none() && storage_uri.is_none() {
            return Err(SubstrateError::Timeout {
                message: "cannot restore suspended sandbox before source node or remote snapshot is available"
                    .to_string(),
                phase: None,
            });
        }
        Ok(SuspendRestorePlan {
            source_node_name,
            storage_uri,
        })
    }

    /// Resolve where a `stopped` sandbox is restarted from: its source node,
    /// its evacuated disk artifact in S3, or both.
    ///
    /// **The two are deliberately not mutually exclusive.** The pair is what
    /// selects [`parse_pod_create_context`]'s third placement shape — the
    /// source node as a preference or a required shortlist rather than a
    /// binding, with S3 as the fallback — and that shape is the one worth
    /// having, so neither source is dropped when the other is present.
    /// See that function's docs for the `restore_source_node_hard_pin` case
    /// where the pair still renders a required pin.
    /// Suppressing the node once a URI exists would strip that preference from
    /// every stopped restart, including the same-node retry between the
    /// artifact becoming durable and the local row being deleted, when the
    /// rootfs is still on the node. Suppressing the URI once a node is known
    /// would keep the pre-bound `spec.nodeName` shape, whose failure mode is
    /// exactly what evacuating a stopped sandbox exists to remove: the pod
    /// cannot land anywhere else, and a source node that can no longer run it
    /// ends the restore instead of re-driving it cross-node.
    ///
    /// Both absent stays [`SubstrateError::Timeout`] -> 503 + `Retry-After`. A
    /// `start` racing the evacuation window is asked to retry, not told the
    /// sandbox is broken.
    ///
    /// ## Known asymmetry with [`Self::suspend_restore_plan`]
    ///
    /// That one consults [`Self::existing_suspend_restore_annotations`] first,
    /// reusing whatever plan is already stamped on the live pod; this one
    /// always re-derives from Postgres. The *write* half is shared — the pod
    /// builder stamps both annotations whenever the plan carries them — so on
    /// a retry against a pod a prior attempt already created for this
    /// generation, the two attempts can resolve different URIs if the pin
    /// advanced in between. The stopped evacuation's pin write
    /// (`record_stopped_evacuation_artifact` in the cluster controller's node
    /// drain) lands before its teardown and refuses to replace a pin with a
    /// different id, so it does not advance the pin mid-evacuation; the
    /// read-back is still not wired.
    ///
    /// ## A stopped evacuation in flight is refused
    ///
    /// Once a drain has evacuated the sandbox and released the node holding its
    /// local copy, neither source is usable yet:
    ///
    /// * the node name `restore_source_node_name` would return is not a
    ///   source — its executions fallback still names the drained node,
    ///   because retiring an execution marks the row inactive without deleting
    ///   it, and that node is cordoned, about to disappear, and has no bytes;
    /// * the URI cannot boot it either. sandbox-manager restores a URI-only
    ///   `stopped` start through its cross-node checkpoint path, whose
    ///   finalize accepts only a full artifact
    ///   (`transition_restored_sandbox_to_suspended_parts`), so a disk
    ///   artifact stages and then fails, and the readiness wait re-drives a
    ///   pod that never comes up. That qualifies the S3 fallback described
    ///   above too: until sandbox-manager can boot a same-id disk artifact,
    ///   it cannot complete on a node other than the source.
    ///
    /// So the plan answers [`SubstrateError::Timeout`] — 503 with
    /// `Retry-After` — for as long as the evacuation leaves the sandbox
    /// unplaced. The drain's restore sweep is meant to place it again, after
    /// which the row has a node and this refusal no longer applies; until the
    /// restore claim admits a `stopped` row and sandbox-manager can
    /// materialize one back to `stopped`, that sweep cannot, and the refusal
    /// stands.
    pub(super) async fn stopped_restore_plan(
        &self,
        sandbox: &Sandbox,
    ) -> Result<SuspendRestorePlan, SubstrateError> {
        let source = self.store.restore_source_plan(sandbox.id).await?;
        if source.stopped_evacuation_unplaced {
            return Err(SubstrateError::Timeout {
                message: "cannot restart stopped sandbox while it is being migrated off a \
                          draining node; its disk is durable and re-placement is pending"
                    .to_string(),
                phase: None,
            });
        }
        let source_node_name = source.node_name;
        let storage_uri = self
            .store
            .selected_stopped_restore_storage_uri(sandbox)
            .await?;
        if source_node_name.is_none() && storage_uri.is_none() {
            return Err(SubstrateError::Timeout {
                message: "cannot restart stopped sandbox before its source node or evacuated \
                          disk artifact is available"
                    .to_string(),
                phase: None,
            });
        }
        Ok(SuspendRestorePlan {
            source_node_name,
            storage_uri,
        })
    }
}
