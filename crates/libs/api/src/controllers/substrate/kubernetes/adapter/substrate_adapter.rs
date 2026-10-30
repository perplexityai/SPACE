//! `SubstrateAdapter` implementation for `KubernetesSubstrate`.

use super::*;
use crate::controllers::substrate::runtime_upgrade::UpgradeActivation;
use pplx_space_common_core::types::SubstrateKind;

impl SubstrateAdapter for KubernetesSubstrate {
    fn managed_by(&self) -> &'static str {
        "k8s"
    }

    fn capabilities(&self) -> SubstrateCapabilities {
        SubstrateCapabilities::KUBERNETES
    }

    async fn create_execution<'a>(
        &'a self,
        request: CreateExecutionRequest<'a>,
    ) -> Result<CreateExecutionOutcome, SubstrateError> {
        let sandbox_id = request.sandbox_id;
        let sandbox_uuid = sandbox_id;

        // Cascade-amplifier backpressure: if recent cancel rate
        // exceeds the configured budget, reject new creates with 503 so we
        // don't pile more cleanup load onto a node-mgr that is already
        // saturated. Returning early here also short-circuits the DB write
        // in `ensure_active_execution_row`.
        if self.cancel_tracker.is_over_budget() {
            // Rejection is recorded as a tagged `sandbox.api.create.error`
            // with `reason:rejected_cancel_storm` — alerts can filter on
            // the tag without us also emitting a parallel counter.
            pplx_space_metrics::sandbox::api::create_step_error(
                sandbox_uuid,
                request.metric_identity,
                "fc",
                Some(request.template_ref.template.as_str()),
                "execution_create",
                "rejected_cancel_storm",
                None,
            );
            tracing::warn!(
                sandbox_id = %sandbox_id,
                cancel_count = self.cancel_tracker.current_count(),
                budget = self.cancel_tracker.budget_per_sec(),
                "create_execution rejected: cancel-storm backpressure active"
            );
            return Err(SubstrateError::Unavailable {
                message: "create_execution rejected: cancel-storm backpressure active".to_string(),
            });
        }

        match network_slot_capacity_preflight(
            &self.k8s.caches.nodes.state(),
            &self.pod_network_slots,
            &self.k8s.sandbox_pod_config,
        ) {
            NetworkSlotCapacityPreflight::Available => {}
            NetworkSlotCapacityPreflight::Unknown => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "create_execution proceeding with unknown Kubernetes network-slot capacity"
                );
            }
            NetworkSlotCapacityPreflight::Full => {
                pplx_space_metrics::sandbox::api::create_step_error(
                    sandbox_uuid,
                    request.metric_identity,
                    "fc",
                    Some(request.template_ref.template.as_str()),
                    "execution_create",
                    NETWORK_SLOT_CAPACITY_UNAVAILABLE_REASON,
                    None,
                );
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "create_execution rejected: cached Kubernetes network-slot capacity is full"
                );
                return Err(SubstrateError::Unavailable {
                    message:
                        "create_execution rejected: no Kubernetes network-slot capacity available"
                            .to_string(),
                });
            }
        }

        // Resolved from the `templates` row and the `node_groups` rows, for the
        // requesting org — on create the actor *is* the owner, and the cold pod
        // metadata stamps the same org onto the pod, which the builder
        // cross-checks against this placement. Both pod-request branches below
        // render it; neither may guess it, so a resolution failure fails the
        // create.
        //
        // Ahead of `begin_activation_execution_row`, and that ordering is the
        // point: this is the one create-path step that can fail on catalog
        // state rather than on Kubernetes, and resolving it first means the
        // failure happens while there is still nothing to unwind — no claim for
        // the guard below to give back. It also keeps the database read out of
        // the `execution_create` latency, which measures the Kubernetes create.
        // Of the exits after the claim, only the pod-create failure books an
        // `execution_create` step error; the guard's early-failure arm releases
        // the claim and books none.
        //
        // Everything it needs is in hand at function entry, so the hoist costs
        // nothing.
        let node_group_placement = self
            .node_group_placement
            .resolve(
                NodeGroupResolvePath::Create,
                PlacementSubject::Sandbox(request.actor.org_id),
                request.template_ref.template.as_str(),
            )
            .await?;

        let requested_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let execution = match self
            .store
            .begin_activation_execution_row(
                sandbox_id,
                requested_attempt_id,
                SandboxTransitionTargetState::Started,
            )
            .await
        {
            Ok(execution) => execution,
            Err(SubstrateError::ActivationInFlight {
                same_target: true,
                owner_attempt_id,
                ..
            }) => {
                let fence = self
                    .store
                    .reconcile_activation_fence(
                        sandbox_id,
                        requested_attempt_id,
                        Some(owner_attempt_id),
                        SandboxTransitionTargetState::Started,
                    )
                    .await?;
                let execution = fence.execution;
                let (_, phase_deadline) = self.budgets.ready_wait_bounds(self.budgets.create_ready);
                let (pod_ip, binding) = self
                    .wait_for_pod_proxy_reachable(
                        request.metric_identity,
                        sandbox_id,
                        PodProxyWaitTarget {
                            pod_name: &execution.pod_name,
                            pod_uid: execution.pod_uid.as_ref().map(PodUid::as_str),
                            generation: execution.generation,
                        },
                        "create_join",
                        phase_deadline,
                    )
                    .await?;
                let binding = if request.wait_until_ready {
                    self.poll_until_command_ready(
                        sandbox_id,
                        request.metric_identity,
                        self.k8s
                            .create_command_ready_timeout
                            .min(remaining_ready_window(phase_deadline)),
                        CommandReadyPod::PreResolved(pod_ip),
                        Some(request.template_ref.template.as_str()),
                    )
                    .await?
                } else {
                    binding
                };
                let joined_pod = match self.lookup_sandbox_pod(&execution.pod_name) {
                    Some(pod) => Some(pod),
                    None => fetch_sandbox_pod_uncached(&self.k8s, &execution.pod_name)
                        .await
                        .map_err(|error| SubstrateError::Failed {
                            message: format!("failed to query joined create pod: {error}"),
                        })?,
                }
                .ok_or_else(|| SubstrateError::Unavailable {
                    message: "joined create reached readiness but its pod disappeared".to_string(),
                })?;
                let active_node_id = self
                    .remember_pod_node_placement(
                        sandbox_uuid,
                        &joined_pod,
                        PlacementRetryDisposition::RetryDuplicatesExecution,
                    )
                    .await?
                    .ok_or_else(|| SubstrateError::Unavailable {
                        message: "joined create pod has no registered node placement".to_string(),
                    })?;
                return Ok(CreateExecutionOutcome {
                    lease_completion: None,
                    sandbox_id,
                    binding: Some(binding),
                    initial_lifecycle: "started",
                    initial_placement: "on_node",
                    active_node_id: Some(active_node_id),
                    command_ready: true,
                });
            }
            Err(error) => return Err(error),
        };
        // Unreachable: the claim above always assigns an attempt. Without one
        // there is no attempt to fence a release on, so this exit would leave
        // the claim to reconciliation, like the unguarded window below.
        let activation_attempt_id =
            execution
                .activation_attempt_id
                .ok_or_else(|| SubstrateError::Failed {
                    message: "activation execution has no attempt fence".to_string(),
                })?;
        let pod_name = execution.pod_name.clone();

        let mut cancel_guard = CancelGuard::new(
            sandbox_id,
            pod_name.clone(),
            execution.generation,
            activation_attempt_id,
            Arc::clone(&self.cancel_tracker),
            self.task_tracker.clone(),
        );
        // The execution and its activation attempt are claimed from here on.
        // Until the pod create is sent the guard releases that claim alone. The
        // caller deletes its fresh sandbox row on any error, which drops the
        // claim with it, so a leaked claim bites only when that delete fails or
        // the request is cancelled: the sandbox row then survives with an
        // activation that will never produce a pod, and a later data-plane
        // access, start or wake joins that leaked attempt.
        //
        // One window stays unguarded: `begin_activation_execution_row` commits
        // before it returns, so a cancellation after the server commits and
        // before its reply arrives leaves a committed claim and no guard. It is
        // one round trip wide and ends where every early exit ended before this
        // guard existed: the pod-less `creating` claim waits for the
        // orphaned-sandbox reconciler, which acts once the execution has been
        // unmodified for `CREATE_AGE_THRESHOLD_SECS`, or for the
        // transient-lifecycle sweeper's age-out.
        cancel_guard.arm_claim(self.store.clone());

        // Resolve the per-sandbox setup once through the shared routine, then
        // encode it into the pod spec. Warm-pool adoption resolves the same
        // setup from its claim request and applies it via gRPC; see
        // `per_sandbox_setup` for the compile-time guarantee that both paths
        // stay in sync.
        //
        // An inherited policy is seeded at 0, as adoption also sends it, so the
        // first reconcile at the sandbox's generation replaces the seed; see
        // `cold_pod_network_policy_generation`.
        let cold_network_policy_generation = cold_pod_network_policy_generation(
            request.network_policy_override.is_some(),
            request.network_policy_generation,
        );
        // Every fallible step between the claim and the pod create, so a
        // failure gives the claim back before the error is returned.
        let prepared = async {
            let mut setup = PerSandboxSetup::from_create_request(&request);
            setup.auto_lifecycle = self
                .store
                .auto_lifecycle_policy_after_claim(sandbox_id)
                .await?;
            let cold_metadata = setup.into_cold_pod_metadata(cold_network_policy_generation)?;
            let fork_restore = request
                .restore_from
                .as_ref()
                .map(|restore| {
                    restore
                        .storage_uri
                        .map(|storage_uri| (restore, storage_uri))
                        .ok_or_else(|| SubstrateError::Failed {
                            message: "fork disk restore requires an exact artifact storage URI"
                                .to_string(),
                        })
                })
                .transpose()?;
            Ok::<_, SubstrateError>((cold_metadata, fork_restore))
        }
        .await;
        let (cold_metadata, fork_restore) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                cancel_guard.release_claim().await;
                return Err(err);
            }
        };

        let pod_create_started_at = Instant::now();
        let common = SandboxPodCommon {
            pod_name: &pod_name,
            sandbox_id: sandbox_uuid,
            execution_generation: execution.generation,
            activation_attempt_id: execution.bootstrap_route_activation_id(),
            template: request.template_ref.template.as_str(),
            content_hash: request.template_ref.content_hash,
            env: request.env,
            traceparent: pplx_space_observability::trace_propagation::current_traceparent(),
        };
        let runtime = cold_metadata.runtime_metadata(request.resources);
        let restore_source_sandbox_id: String;
        let pod_request = match fork_restore {
            Some((restore, storage_uri)) => {
                restore_source_sandbox_id = restore.source_sandbox_id.to_string();
                SandboxPodRequest::Restore {
                    artifact_kind: pplx_space_common_core::types::ArtifactKind::Disk,
                    common,
                    runtime,
                    suspend_restore_storage_uri: Some(storage_uri),
                    suspend_restore_source_node: None,
                    fork_disk_restore: Some(Box::new(ForkDiskRestorePodContext {
                        source_sandbox_id: &restore_source_sandbox_id,
                        artifact_snapshot_id: restore.snapshot_id,
                        target_owner: request.actor.sandbox_owner.as_str(),
                    })),
                    direct_node_name: None,
                    preferred_node_name: None,
                    restore_node_shortlist: Vec::new(),
                    extra_tolerations: &[],
                    extra_required_affinity: None,
                    // A fork restore creates a *new* sandbox from a snapshot,
                    // so the set is the target org's for the target template —
                    // resolved above with the rest of the create path. It
                    // carries no source-node pin (`direct_node_name: None`),
                    // so kube-scheduler places it inside the set.
                    node_group_placement: node_group_placement.placement(),
                }
            }
            None => SandboxPodRequest::OnDemand {
                common,
                runtime,
                // Create-time isolation pool pin, straight from the request; the
                // PodSpec gets a nodeSelector + toleration. Not persisted.
                node_pool: request.node_pool,
                node_group_placement: node_group_placement.placement(),
            },
        };
        // From here until the response, the apiserver may accept a pod this
        // call never learns, so the guard keeps the claim that names it.
        cancel_guard.begin_pod_create();
        let pod = match create_sandbox_pod(&self.k8s, pod_request)
            .instrument(tracing::info_span!(
                "substrate.k8s.create_pod",
                sandbox_id = %sandbox_id,
                pod_name = %pod_name,
                namespace = %self.k8s.sandbox_namespace,
                template = %request.template_ref.template,
            ))
            .await
        {
            Ok(pod) => {
                // Pod exists — arm the guard so cancellation triggers cleanup.
                let pod_uid = pod.uid().and_then(|uid| PodUid::parse(uid).ok());
                cancel_guard.arm(self.k8s.clone(), self.store.clone(), pod_uid);
                pod
            }
            Err(err) => {
                pplx_space_metrics::sandbox::api::create_step_error(
                    sandbox_uuid,
                    request.metric_identity,
                    "fc",
                    Some(request.template_ref.template.as_str()),
                    "execution_create",
                    "kubernetes_api",
                    None,
                );
                // Retires the claim on every pod-create error, including an
                // ambiguous one where the apiserver persisted the pod but the
                // response failed — the same unseen-pod class the guard keeps
                // claimed on a cancellation mid-create.
                if let Err(forget_err) = self
                    .store
                    .forget_pod_tracking_if_generation_and_attempt(
                        sandbox_uuid,
                        execution.generation,
                        activation_attempt_id,
                    )
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = execution.generation,
                        error = %forget_err,
                        "failed to forget Kubernetes pod tracking after pod create failure"
                    );
                }
                cancel_guard.defuse();
                return Err(SubstrateError::Failed {
                    message: format!("failed to create sandbox pod: {err}"),
                });
            }
        };

        pplx_space_metrics::sandbox::api::create_step_latency(
            sandbox_uuid,
            pplx_space_metrics::sandbox::api::CreateStep {
                identity: request.metric_identity,
                backend: "fc",
                template: Some(request.template_ref.template.as_str()),
                step: "execution_create",
                result: "success",
                started_at: pod_create_started_at.into(),
                phase: None,
            },
        );

        let mut active_node_id = self
            .remember_pod_node_placement(
                sandbox_uuid,
                &pod,
                PlacementRetryDisposition::RetryDuplicatesExecution,
            )
            .await?;
        let pod_uid = pod.uid().map(str::to_owned);
        let pod_ip = assigned_pod_ip(&pod)?;
        let phase = normalize_pod_phase(pod.phase().map(str::to_owned), "pending");

        let pod_ip_string = pod_ip.map(|ip| ip.to_string());
        match self
            .store
            .persist_pod_metadata(
                sandbox_uuid,
                execution.generation,
                pod_uid.as_deref(),
                pod_ip_string.as_deref(),
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = execution.generation,
                "pod metadata write no-oped after pod create: the execution was \
                 retired or replaced mid-flight"
            ),
            Err(err) => tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = execution.generation,
                error = %err,
                "failed to persist Kubernetes pod metadata after pod create"
            ),
        }

        let wait_until_ready = request.wait_until_ready;
        // Cap the combined readiness wait at the ALB-aware budget. The two
        // sub-waits below run sequentially and their per-phase timeouts can
        // sum past the upstream gateway ceiling; this outer bound guarantees
        // the create returns before the gateway would cut the connection.
        // The inner waits clamp to `phase_deadline` so their phase-tagged
        // timeouts fire first; the outer timeout is the backstop for the
        // stretches between them.
        let (create_budget, phase_deadline) =
            self.budgets.ready_wait_bounds(self.budgets.create_ready);
        let select_outcome = match tokio::time::timeout(create_budget, async {
            let (pod_ip, binding) = self
                .wait_for_pod_proxy_reachable(
                    request.metric_identity,
                    sandbox_id,
                    PodProxyWaitTarget {
                        pod_name: &pod_name,
                        pod_uid: pod_uid.as_deref(),
                        generation: execution.generation,
                    },
                    "create",
                    phase_deadline,
                )
                .instrument(tracing::info_span!(
                    "substrate.k8s.wait_pod_proxy_reachable",
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    namespace = %self.k8s.sandbox_namespace,
                ))
                .await?;
            if wait_until_ready {
                // Command-ready keeps its own (shorter) bound inside the
                // outer ALB budget — intentionally distinct from it. The
                // pod proxy is already reachable here, so the guest agent
                // normally comes up in seconds; the cap stops a single
                // create from pinning on a wedged guest. When it does
                // fire, the outer match preserves the (scheduled,
                // running) pod and returns the sandbox as
                // `started`/`command_ready=false` — the client's first
                // data-plane op then gets the SDK-recoverable
                // `sandbox_timeout` (the guest is briefly unreachable)
                // and converges on retry. So a guest that needs longer
                // than this cap still heals; we don't hold the create
                // call open for the full budget waiting on it.
                self.poll_until_command_ready(
                    sandbox_id,
                    request.metric_identity,
                    self.k8s
                        .create_command_ready_timeout
                        .min(remaining_ready_window(phase_deadline)),
                    CommandReadyPod::PreResolved(pod_ip),
                    Some(request.template_ref.template.as_str()),
                )
                .await
            } else {
                Ok(binding)
            }
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(create_ready_timeout_error(self.budgets.create_ready)),
        };
        let binding = match select_outcome {
            Ok(binding) => binding,
            Err(err) => {
                // Slow-but-healthy create: the pod is still coming up. Only
                // preservable if it's far enough along that a client retry can
                // reach it (execution past `creating`, with a persisted IP) —
                // then leave the pod running (no delete, no `forget_pod_tracking`)
                // and report the sandbox as `started` on its node, NOT
                // `creating` (which the transient-lifecycle sweeper would age
                // out and delete). `command_ready: false` lets the caller emit
                // `create_pending` and skip the binding it never got. If the
                // pod isn't preservable (no IP/state still `creating`, terminal,
                // or unscheduled), fall through to teardown.
                // Bound the preserve decision itself: it runs inside the
                // post-budget response margin and can fall through to an
                // unbounded apiserver GET on a cache miss. A slow decision
                // degrades to teardown (`Err(Elapsed)` / `Ok(None)` both fall
                // through) rather than overrunning the margin and re-tripping
                // the latency cliff.
                if create_wait_should_preserve_node_work(&err)
                    && let Ok(Some(node_id)) = tokio::time::timeout(
                        self.budgets.preserve_decision,
                        self.preservable_create_node(sandbox_uuid, &pod_name),
                    )
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        "sandbox create exhausted the ALB-aware readiness budget but the pod is \
                         scheduled and running; returning it as started-on-node so the node \
                         converges and the client's data-plane retry reaches it"
                    );
                    cancel_guard.defuse();
                    self.store
                        .release_activation_attempt_best_effort(
                            sandbox_id,
                            execution.generation,
                            activation_attempt_id,
                        )
                        .await;
                    return Ok(CreateExecutionOutcome {
                        lease_completion: None,
                        sandbox_id,
                        binding: None,
                        initial_lifecycle: "started",
                        initial_placement: "on_node",
                        active_node_id: Some(node_id),
                        command_ready: false,
                    });
                }
                // Cascade-amplifier short-circuit: when the failure looks
                // like node-mgr is unreachable *and* the pod is
                // authoritatively absent (cache + apiserver GET both say
                // it's gone), defer cleanup to the orphaned-sandbox
                // reconciler. `read_sandbox_pod` falls back to a real GET
                // on cache misses, so a reflector-lag false-miss can't
                // strand the row in `creating` — the reconciler's
                // `OrphanKind::Normal` path skips Present pods, and we
                // only want to defer when there's nothing for it to miss.
                // Otherwise fall through to the synchronous delete and
                // let the downstream cascade controls (sbx-mgr cleanup
                // cap, node-mgr per-method limiter) absorb the load.
                //
                // The outer ALB-budget timeout synthesizes a generic
                // `Timeout` that carries no node-mgr signal, so during a
                // node-mgr brownout `is_nodemgr_unreachable` would miss it and
                // re-enter the synchronous delete that feeds the brownout.
                // Treat a budget timeout with an authoritatively-absent pod the
                // same way: defer to the reconciler — there's nothing to delete
                // anyway, and it keeps the cascade protection intact.
                let err_defers_cleanup =
                    is_nodemgr_unreachable(&err) || matches!(err, SubstrateError::Timeout { .. });
                let pod_authoritatively_missing = err_defers_cleanup
                    && matches!(self.read_sandbox_pod(&pod_name).await, Ok(None));
                if pod_authoritatively_missing {
                    cancel_guard.mark_cause(CancelCause::NodeMgrUnreachable);
                    return Err(err);
                }
                // Cleanup runs after the budget already expired, but it is NOT
                // response-margin-critical: the binding upstream cut is the
                // node-RPC channel timeout (which doesn't bound this kube
                // DELETE), and the ALB idle timeout is far higher (see
                // `ALB_IDLE_TIMEOUT_SECS`). So let the delete complete — an
                // unbounded delete is what actually removes the pod, whereas
                // abandoning it on a timeout would forget tracking + delete the
                // row and leave the pod running with nothing to GC it.
                // `Ok` here means the apiserver *accepted* a graceful delete
                // (`DeleteParams::default()`), not that the pod is gone: it
                // sets `deletionTimestamp` and the VM runs through its grace
                // period, staying Terminating indefinitely if the node is
                // unreachable — which is one of the conditions that reach this
                // arm. So this is "cleanup was handed off", which is the most
                // this call can establish; the orphan reconciler owns the rest.
                // The distinction matters because it is the demotion's input.
                let pod_cleanup_accepted = match delete_sandbox_pod(&self.k8s, &pod_name).await {
                    Ok(()) => true,
                    // `delete_sandbox_pod` propagates the raw apiserver error,
                    // so an already-absent pod arrives here as 404. That is the
                    // most confirmed removal there is — treating it as
                    // unconfirmed would demote the common benign race.
                    Err(delete_err) if is_not_found_error(&delete_err) => true,
                    Err(delete_err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            error = %delete_err,
                            "failed to delete sandbox pod during creation cleanup — pod may be orphaned"
                        );
                        false
                    }
                };
                if let Err(forget_err) = self
                    .store
                    .forget_pod_tracking_if_generation_and_attempt(
                        sandbox_uuid,
                        execution.generation,
                        activation_attempt_id,
                    )
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = execution.generation,
                        error = %forget_err,
                        "failed to forget Kubernetes pod tracking after creation cleanup"
                    );
                }
                cancel_guard.defuse();
                // The pod delete is best-effort and only logs. When it was not
                // even accepted, nothing is removing this pod, so `err` must
                // not tell the caller to retry: `create` mints a fresh
                // `SandboxId`, so the retry buys a second sandbox and leaves
                // this one orphaned with no cleanup pending. An accepted
                // delete puts the pod on a path to removal and the row is
                // deleted below, so a contention abort stays retryable there —
                // which is the common case and the one the bound needs.
                if pod_cleanup_accepted {
                    return Err(err);
                }
                return Err(err.demoted_after_side_effect());
            }
        };

        let initial_placement = "on_node";
        let initial_lifecycle = match phase.as_str() {
            "failed" => "failed",
            _ => "started",
        };

        // The reflector watch and the per-pod watch used by
        // `wait_for_pod_proxy_reachable` are independent streams, so the
        // reflector cache may briefly lag the scheduler-binding MODIFIED
        // event. We are at the post-readiness step, so the pod must have
        // been bound to a node by now; if the cache still shows it as
        // unscheduled, that is definitive evidence of a stale cache and
        // we must fall back to an authoritative GET. Without this check,
        // a stale cache hit would leave `active_node_id` as `None` and the
        // caller's `UPDATE sandboxes SET active_node_id = $3` would clear
        // the placement that the watch already persisted to the DB.
        let post_create_pod = match self.lookup_sandbox_pod(&pod_name) {
            Some(pod) if assigned_pod_node_name(&pod).is_some() => Some(pod),
            _ => fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                .await
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to query created sandbox pod placement: {err}"),
                })?,
        };
        if let Some(pod) = post_create_pod
            && let Some(node_id) = self
                .remember_pod_node_placement(
                    sandbox_uuid,
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?
        {
            active_node_id = Some(node_id);
        }

        cancel_guard.defuse();
        self.store
            .release_activation_attempt_best_effort(
                sandbox_id,
                execution.generation,
                activation_attempt_id,
            )
            .await;
        Ok(CreateExecutionOutcome {
            lease_completion: Some(super::super::super::ActivationLeaseCompletion {
                generation: execution.generation,
                attempt_id: activation_attempt_id,
            }),
            sandbox_id,
            binding: Some(binding),
            initial_lifecycle,
            initial_placement,
            active_node_id,
            command_ready: true,
        })
    }

    async fn complete_activation_lease(
        &self,
        sandbox_id: SandboxId,
        completion: Option<super::super::super::ActivationLeaseCompletion>,
    ) {
        if let Some(completion) = completion {
            self.store
                .release_finished_activation_attempt_best_effort(
                    sandbox_id,
                    completion.generation,
                    completion.attempt_id,
                )
                .await;
        }
    }

    async fn delete_execution(&self, sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        // Unfenced: a delete is the caller asking for the sandbox to be gone
        // whatever pod currently backs it, so there is no observed identity to
        // pin to and the teardown can only report `Retired`.
        self.teardown_execution(
            sandbox_id,
            ExecutionTeardown::Delete,
            &RetireFence::Unobserved,
        )
        .await
        .map(|_| ())
    }

    /// Answered entirely from the sandbox-pod reflector — the same cache every
    /// hot read uses — so a caller can snapshot a whole candidate batch for
    /// the cost of a map lookup each. A pod missing from the cache is omitted
    /// rather than fetched: the caller wanted a cheap observation, and a
    /// per-candidate apiserver GET is exactly what it was avoiding.
    async fn observe_pod_identities(
        &self,
        pod_names: &[PodName],
    ) -> std::collections::HashMap<PodName, ObservedPodIdentity> {
        pod_names
            .iter()
            .filter_map(|pod_name| {
                let pod = self.lookup_sandbox_pod(pod_name.as_str())?;
                let pod_uid = pod.uid().and_then(|uid| PodUid::parse(uid).ok())?;
                let resource_version = pod
                    .resource_version()
                    .and_then(|version| PodResourceVersion::parse(version).ok());
                Some((
                    pod_name.clone(),
                    ObservedPodIdentity {
                        pod_uid,
                        resource_version,
                    },
                ))
            })
            .collect()
    }

    async fn retire_execution(
        &self,
        request: RetireExecutionRequest,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        self.teardown_execution(
            request.sandbox_id,
            ExecutionTeardown::Retire,
            &request.fence,
        )
        .await
    }

    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, SubstrateError> {
        match fence {
            // Nothing was observed, so there is nothing that could have
            // changed underneath the caller.
            RetireFence::Unobserved => Ok(None),
            RetireFence::NoExecution => Ok(self
                .store
                .active_execution_status(sandbox_id)
                .await?
                .map(|_| PodFenceMismatch::ExecutionMissing)),
            RetireFence::Pod(pod_fence) => {
                match self
                    .check_pod_fence(
                        sandbox_id,
                        pod_fence,
                        FenceReadFreshness::MustBeAuthoritative,
                    )
                    .await?
                {
                    // Nothing to refuse either way: the retire will hold, or
                    // it has already happened.
                    PodFenceCheck::Holds(_) | PodFenceCheck::AlreadyRetired => Ok(None),
                    PodFenceCheck::Mismatch(mismatch) => Ok(Some(mismatch)),
                }
            }
        }
    }

    async fn delete_observed_execution(
        &self,
        sandbox_id: SandboxId,
        target: &NodeApiTarget<'_>,
    ) -> Result<(), SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .db_context("failed to start pool slot delete transaction")?;
        let slot = match WarmPoolStore::lock_open_slot_for_delete_by_reserved_sandbox_id(
            &mut tx,
            sandbox_uuid,
        )
        .await
        .db_context("failed to lock observed pool slot for delete")?
        {
            OpenPoolSlotDeleteLock::Locked(slot) => slot,
            OpenPoolSlotDeleteLock::NotFound => {
                return super::super::super::delete_observed_execution_via_node_api(
                    self, sandbox_id, target,
                )
                .await;
            }
            OpenPoolSlotDeleteLock::NotOpen { state } => {
                return Err(SubstrateError::Conflict {
                    message: format!(
                        "observed Kubernetes pool slot is {state}; delete the adopted sandbox instead"
                    ),
                });
            }
            OpenPoolSlotDeleteLock::Busy => {
                return Err(SubstrateError::Conflict {
                    message: "observed Kubernetes pool slot is busy; retry delete".to_string(),
                });
            }
        };

        let pods: Api<Pod> = Api::namespaced(self.k8s.client.clone(), &slot.pod_namespace);
        if let Err(err) = pods.delete(&slot.pod_name, &DeleteParams::default()).await
            && !is_not_found_error(&err)
        {
            return Err(SubstrateError::Failed {
                message: format!("failed to delete observed pool slot pod: {err}"),
            });
        }

        // Classified even though the pod above is already gone: unlike create,
        // this path is idempotent under retry — the re-run's `pods.delete`
        // answers NotFound, which the guard above tolerates, and the row delete
        // converges to the same absent state. The lock and the commit around it
        // are already classified, so leaving this one statement out is the hole
        // that reports an abort here as permanent while its neighbours in the
        // same transaction report it as retryable.
        let deleted = WarmPoolStore::delete_locked_open_slot(&mut tx, slot.id)
            .await
            .db_context("failed to delete observed pool slot row")?;
        if !deleted {
            return Err(SubstrateError::Conflict {
                message: "observed Kubernetes pool slot changed before delete".to_string(),
            });
        }
        tx.commit()
            .await
            .db_context("failed to commit observed pool slot delete")?;

        tracing::info!(
            sandbox_id = %sandbox_id,
            pool_slot_id = %slot.id,
            pod_name = %slot.pod_name,
            pod_namespace = %slot.pod_namespace,
            "deleted observed Kubernetes pool slot"
        );

        Ok(())
    }

    async fn start_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        if !options.blocking {
            return Err(SubstrateError::Conflict {
                message: "kubernetes substrate requires blocking=true for start".to_string(),
            });
        }
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        if !is_recoverable_live_lifecycle(&sandbox.lifecycle) {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox cannot be started while lifecycle is {}",
                    sandbox.lifecycle
                ),
            });
        }
        // `start` on a `suspended`/`restoring` sandbox IS a restore: it loads a
        // snapshot and may leave live node work behind. Route it through the
        // shared wake/restore machinery (`wake_execution_inner` via
        // `wake_execution_with_metric_identity`) so it claims `suspended ->
        // restoring` + a restore lease, bounds the wait below the channel
        // timeout, and reverts/releases under the unifying invariant. Without
        // this, a timed-out start would leave the row `suspended` with a live
        // restore pod — swept by neither the orphaned-sandbox nor the
        // transient-lifecycle sweeper — stranding the pod and making retries see
        // a dormant sandbox. Non-restore starts (e.g. `stopped`) keep the inline
        // path below; they never claim `restoring`, so there's no orphan to
        // guard against.
        if matches!(
            normalize_lifecycle(&sandbox.lifecycle).as_str(),
            "suspended" | "restoring"
        ) {
            return self
                .wake_execution_with_metric_identity(
                    &sandbox,
                    options.ready_timeout,
                    metric_identity,
                    charge,
                )
                .await;
        }
        // A stopped sandbox boots under the environment it last ran under
        // unless the policy moves it. Decided before admission is charged or
        // a pod exists, so a stranded sandbox costs nothing and places nothing.
        let runtime_upgrade = if normalize_lifecycle(&sandbox.lifecycle) == "stopped" {
            self.activation_runtime_upgrade(sandbox_id, UpgradeActivation::Start, None)
                .await?
        } else {
            None
        };
        // The controller may have deferred admission from a stale restore-
        // shaped snapshot. If the refreshed row now takes the cold-start path,
        // spend that deferred charge before creating an execution.
        charge.charge_cold_activation().await?;
        // Only non-restore lifecycles (`stopped`, etc.) reach here — the
        // `suspended`/`restoring` cases delegated to the restore machinery
        // above — so this resolves to `None` (no snapshot to restore from).
        let restore_plan = if matches!(
            normalize_lifecycle(&sandbox.lifecycle).as_str(),
            "suspended" | "restoring"
        ) {
            Some(self.suspend_restore_plan(&sandbox, false).await?)
        } else {
            None
        };
        // Bound the entire blocking start phase — pod (re)creation, the
        // blocking `wake_sandbox` RPC, and command-ready polling — below the
        // node-RPC channel timeout, exactly like `wake_execution_inner`.
        // Without this the blocking wake (governed only by the 120s endpoint
        // timeout) could ride up to the channel cut and surface as the opaque
        // `Cancelled` cliff this change exists to prevent; the clamp makes a
        // slow start return a structured, retryable `Timeout` instead.
        // The command-ready poll clamps to `phase_deadline` so its
        // phase-tagged timeout fires before this budget's generic one; the
        // backstop sits one phase margin above the caller's requested wait
        // (capped at `CREATE_READY_BUDGET`), so the caller's effective
        // readiness wait is never shortened by the margin.
        let (wait_budget, phase_deadline) = ready_wait_bounds(options.ready_timeout);
        let requested_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let activation_assignment = ActivationAttemptAssignment::new(
            requested_attempt_id,
            SandboxTransitionTargetState::Started,
        );
        let wait = async {
            // Boxed: `ensure_pod_exists` is a large future, and inlining it in
            // this async block overflows the test thread's stack.
            let (activation_attempt_id, joined_attempt_id) = match Box::pin(
                self.ensure_pod_exists(
                    &sandbox,
                    None,
                    restore_plan
                        .as_ref()
                        .and_then(|plan| plan.source_node_name.as_deref()),
                    restore_plan
                        .as_ref()
                        .and_then(|plan| plan.storage_uri.as_deref()),
                    PodMaterializationOwnership::new(
                        None,
                        // Non-restore start path holds no restore lease.
                        None,
                        Some(activation_assignment.clone()),
                    ),
                ),
            )
            .await
            {
                Ok(()) => (activation_assignment.assigned_or_requested(), None),
                Err(SubstrateError::ActivationInFlight {
                    same_target: true,
                    owner_attempt_id,
                    ..
                }) => (requested_attempt_id, Some(owner_attempt_id)),
                Err(error) => {
                    // Materialization can acquire ownership before placement
                    // is available. No Wake was dispatched, so a retry must
                    // re-enter recovery instead of joining this abandoned lease.
                    if let Some((attempt_id, generation)) = activation_assignment.assigned() {
                        self.store
                            .give_up_activation_attempt_best_effort(
                                sandbox_id, generation, attempt_id,
                            )
                            .await;
                    }
                    return Err(error);
                }
            };
            let fence = self
                .store
                .reconcile_activation_fence(
                    sandbox_id,
                    activation_attempt_id,
                    joined_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await?;
            let fence_ownership = fence.ownership();
            let owned_attempt = fence
                .owned_attempt_id()
                .map(|attempt_id| (attempt_id, fence.execution.generation));
            let active_execution = fence.execution;
            let wake_action = activation_wake_action(
                &active_execution,
                fence_ownership,
                SandboxLifecycle::Started,
            )?;
            let restore_storage_uri = restore_plan
                .as_ref()
                .and_then(|plan| plan.storage_uri.as_deref());
            SandboxEvent::new(
                sandbox_id,
                SandboxEventType::ExecutionEnsured,
                SandboxEventSource::Substrate,
            )
            .emit_with(sandbox_template_event_details(
                &sandbox,
                restore_storage_uri,
                SandboxEventDetails::default()
                    .operation("start")
                    .lifecycle_state(sandbox.lifecycle.as_str())
                    .bool_field(
                        SandboxEventBoolField::RestoreStorageUriPresent,
                        restore_storage_uri.is_some(),
                    ),
            ));
            let emit_wake_requested = || {
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::WakeRequested,
                    SandboxEventSource::Substrate,
                )
                .emit_with(sandbox_template_event_details(
                    &sandbox,
                    restore_storage_uri,
                    SandboxEventDetails::default()
                        .operation("start")
                        .bool_field(SandboxEventBoolField::HasNetnsPath, false)
                        .bool_field(
                            SandboxEventBoolField::RestoreStorageUriPresent,
                            restore_storage_uri.is_some(),
                        ),
                ));
            };
            let wake_result = self
                .execute_activation_wake_action(
                    sandbox_id,
                    wake_action,
                    phase_deadline,
                    |route_identity, activation_policy| {
                        pplx_space_sbx_mgr_client::proto::WakeRequest {
                            implicit_wake_source: 0,
                            sandbox_id: sandbox_id.to_string(),
                            blocking: true,
                            start_context: Some(
                                pplx_space_sbx_mgr_client::proto::WakeStartContext {
                                    activation_auto_lifecycle: Some(activation_policy),
                                    expected_execution_generation: route_identity
                                        .execution_generation,
                                    operation_activation_attempt_id: active_execution
                                        .activation_attempt_id
                                        .map(|id| id.to_string()),
                                    vcpu: sandbox_vcpu_option(&sandbox),
                                    memory_mib: sandbox_memory_mib_option(&sandbox),
                                    netns_path: None,
                                    suspend_restore_storage_uri: restore_plan
                                        .as_ref()
                                        .and_then(|plan| plan.storage_uri.clone()),
                                    suspend_restore_source_node: restore_plan
                                        .as_ref()
                                        .and_then(|plan| plan.source_node_name.clone()),
                                    expected_local_artifact_id: None,
                                    route_identity: Some(wake_request_route_identity(
                                        route_identity,
                                    )),
                                },
                            ),
                            target_state: 0,
                            runtime_upgrade: runtime_upgrade.clone(),
                        }
                    },
                    emit_wake_requested,
                )
                .await;
            let (wake_outcome, route_identity) = match wake_result {
                Ok(outcome) => outcome,
                Err(failure) => {
                    if failure.operation_release_is_safe()
                        && let Some((attempt_id, generation)) = owned_attempt
                    {
                        self.store
                            .give_up_activation_attempt_best_effort(
                                sandbox_id, generation, attempt_id,
                            )
                            .await;
                    }
                    let dispatched = failure.was_dispatched();
                    let error = failure.into_error();
                    if dispatched {
                        SandboxEvent::new(
                            sandbox_id,
                            SandboxEventType::WakeFailed,
                            SandboxEventSource::Substrate,
                        )
                        .emit_with(sandbox_template_event_details(
                            &sandbox,
                            restore_storage_uri,
                            SandboxEventDetails::default()
                                .operation("start")
                                .error(error.to_string()),
                        ));
                    }
                    return Err(error);
                }
            };
            let emit_wake_completed = |readiness_fallback: bool| {
                let mut details = SandboxEventDetails::default().operation("start");
                if readiness_fallback {
                    details = details.reason("resident_route_unchanged_readiness_fallback");
                }
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::WakeCompleted,
                    SandboxEventSource::Substrate,
                )
                .emit_with(sandbox_template_event_details(
                    &sandbox,
                    restore_storage_uri,
                    details,
                ));
            };
            if wake_outcome.dispatch.was_dispatched() && wake_outcome.landed {
                emit_wake_completed(false);
            }
            let binding = self
                .poll_until_command_ready(
                    sandbox_id,
                    metric_identity,
                    options
                        .ready_timeout
                        .min(remaining_ready_window(phase_deadline)),
                    match route_identity.as_ref() {
                        Some(route_identity) => CommandReadyPod::SelectedRoute {
                            route_identity,
                            pod_ip: wake_outcome.pod_ip,
                        },
                        None => CommandReadyPod::ResolveLegacy,
                    },
                    None,
                )
                .await?;

            if wake_outcome.dispatch.was_dispatched() && !wake_outcome.landed {
                emit_wake_completed(true);
            }

            Ok((binding, owned_attempt))
        };
        let result = match tokio::time::timeout(wait_budget, wait).await {
            Ok(inner) => inner,
            Err(_elapsed) => Err(SubstrateError::Timeout {
                message: format!(
                    "sandbox start did not reach readiness within {}s; the node is still \
                     starting — retry the same sandbox to converge",
                    wait_budget.as_secs()
                ),
                phase: None,
            }),
        };
        if let Ok((_, Some((activation_attempt_id, generation)))) = &result {
            self.store
                .release_activation_attempt_best_effort(
                    sandbox_id,
                    *generation,
                    *activation_attempt_id,
                )
                .await;
        }
        result.map(|(binding, owned_attempt)| {
            let mut activation = ExecutionActivation::owned(binding);
            activation.lease_completion = owned_attempt.map(|(attempt_id, generation)| {
                super::super::super::ActivationLeaseCompletion {
                    generation,
                    attempt_id,
                }
            });
            activation
        })
    }

    async fn restore_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ActivationCompletion, SubstrateError> {
        // User-API restore is always to `Started` — the HTTP wake/restore
        // contract returns a running sandbox. Only the drain pipeline's
        // `restore_evacuated_execution` opts into `Paused`.
        self.restore_execution_inner(
            metric_identity,
            sandbox_id,
            options,
            None,
            pplx_space_common_core::types::SandboxLifecycle::Started,
            charge,
        )
        .await
    }

    async fn ensure_routable_execution(
        &self,
        sandbox_id: SandboxId,
        budget: Duration,
    ) -> Result<ExecutionRoutabilityCheck, SubstrateError> {
        Ok(
            match self.hydrate_restored_execution(sandbox_id, budget).await? {
                ExecutionHydrationOutcome::Hydrated => ExecutionRoutabilityCheck::Hydrated,
                ExecutionHydrationOutcome::AlreadyRoutable => {
                    ExecutionRoutabilityCheck::AlreadyRoutable
                }
                ExecutionHydrationOutcome::NotRoutable => ExecutionRoutabilityCheck::NotRoutable,
            },
        )
    }

    async fn suspend_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        let suspend_deadline = Instant::now() + self.budgets.suspend_durability;
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::SuspendRequested,
            SandboxEventSource::Substrate,
        )
        .emit_with(SandboxEventDetails::default());
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox is not assigned to a pod".to_string(),
            })?;
        let expected_generation =
            ExecutionGeneration::new(execution.generation).map_err(|error| {
                SubstrateError::Failed {
                    message: format!("invalid active Kubernetes execution generation: {error}"),
                }
            })?;
        let expected_pod_uid =
            execution
                .pod_uid
                .clone()
                .ok_or_else(|| SubstrateError::Unavailable {
                    message: "sandbox pod has no Kubernetes UID for suspend retirement".to_string(),
                })?;
        let suspend_claim = self
            .store
            .claim_suspend_execution(sandbox_id, expected_generation, &expected_pod_uid)
            .await?;
        tracing::info!(
            %sandbox_id,
            generation = %expected_generation,
            claim = %suspend_claim,
            "claimed generation-bound Kubernetes suspend"
        );
        let committed_suspend_artifact = match suspend_claim {
            SuspendExecutionClaim::Completed(artifact) => Some(artifact),
            SuspendExecutionClaim::Claimed | SuspendExecutionClaim::Existing => {
                self.store
                    .committed_suspend_artifact(sandbox_id, expected_generation, &expected_pod_uid)
                    .await?
            }
        };
        let node_suspend_artifact = if committed_suspend_artifact.is_some() {
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = %expected_generation,
                "resuming pod retirement for a committed Kubernetes suspend"
            );
            None
        } else {
            let k8s_node_name =
                execution
                    .k8s_node_name
                    .as_deref()
                    .ok_or_else(|| SubstrateError::Unavailable {
                        message: "sandbox pod is not assigned to a Kubernetes node".to_string(),
                    })?;
            let target = NodeApiTarget {
                managed_by: SpaceNodeManagedBy::Kubernetes,
                role: SpaceNodeRole::Worker,
                direct_endpoint: None,
                substrate_node_id: Some(k8s_node_name),
            };
            let suspend_path_started_at = Instant::now();
            let endpoint_addr = match tokio::time::timeout_at(
                suspend_deadline,
                self.resolve_node_api_endpoint(&target),
            )
            .await
            {
                Err(_) => {
                    return Err(SubstrateError::Timeout {
                        message: format!(
                            "timed out resolving node-api for generation-bound suspend of sandbox {sandbox_id}"
                        ),
                        phase: None,
                    });
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        k8s_node_name,
                        elapsed_ms = duration_millis_to_u64(suspend_path_started_at.elapsed()),
                        error = %err,
                        "failed to resolve node-api endpoint for Kubernetes sandbox suspend"
                    );
                    return Err(err);
                }
                Ok(Ok(endpoint_addr)) => endpoint_addr,
            };
            let endpoint = match node_api_endpoint(
                &endpoint_addr,
                POD_WORKER_CONNECT_TIMEOUT,
                NODE_RPC_TIMEOUT,
            ) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        k8s_node_name,
                        node_api_endpoint = %endpoint_addr,
                        elapsed_ms = duration_millis_to_u64(suspend_path_started_at.elapsed()),
                        error = %err,
                        "invalid node-api endpoint for Kubernetes sandbox suspend"
                    );
                    return Err(err);
                }
            };
            let mut client = match tokio::time::timeout_at(
                suspend_deadline,
                connect_worker(endpoint),
            )
            .await
            {
                Err(_) => {
                    return Err(SubstrateError::Timeout {
                        message: format!(
                            "timed out connecting node-api for generation-bound suspend of sandbox {sandbox_id}"
                        ),
                        phase: None,
                    });
                }
                Ok(Ok(client)) => client,
                Ok(Err(err)) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        k8s_node_name,
                        node_api_endpoint = %endpoint_addr,
                        elapsed_ms = duration_millis_to_u64(suspend_path_started_at.elapsed()),
                        error = %err,
                        "failed to connect node-api for Kubernetes sandbox suspend"
                    );
                    return Err(SubstrateError::Unavailable {
                        message: format!("node-api not reachable at {endpoint_addr}: {err}"),
                    });
                }
            };

            let suspend_rpc_started_at = Instant::now();
            let published_artifact = match await_generation_suspend(
                &mut client,
                sandbox_id,
                expected_generation,
                &endpoint_addr,
                suspend_deadline,
                self.budgets.suspend_durability,
            )
            .await
            {
                Ok(GenerationSuspendOutcome::Durable(published_artifact)) => Ok(published_artifact),
                // The node can delete the sandbox after its suspend backup
                // finalized (a failed post-suspend cleanup takes the delete
                // path), and then has no artifact to publish. The ready
                // snapshot still proves the suspend durable; without it the
                // suspend did not complete and the caller may retry.
                Ok(GenerationSuspendOutcome::NodeDeleted) => self
                    .store
                    .durable_suspend_snapshot_artifact(
                        sandbox_id,
                        expected_generation,
                        &expected_pod_uid,
                    )
                    .await?
                    .ok_or_else(|| SubstrateError::Unavailable {
                        message: format!(
                            "sandbox {sandbox_id} was deleted on its node before suspend completed"
                        ),
                    }),
                Err(error) => Err(error),
            };
            let published_artifact = match published_artifact {
                Ok(published_artifact) => published_artifact,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = %expected_generation,
                        k8s_node_name,
                        node_api_endpoint = %endpoint_addr,
                        elapsed_ms = duration_millis_to_u64(suspend_rpc_started_at.elapsed()),
                        error = %error,
                        "generation-bound Kubernetes sandbox suspend did not become durable"
                    );
                    SandboxEvent::new(
                        sandbox_id,
                        SandboxEventType::SuspendFailed,
                        SandboxEventSource::Substrate,
                    )
                    .emit_with(
                        SandboxEventDetails::default()
                            .error(error.to_string())
                            .logged_by_caller(),
                    );
                    return Err(error);
                }
            };
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = %expected_generation,
                k8s_node_name,
                node_api_endpoint = %endpoint_addr,
                local_artifact_id = %published_artifact.id,
                elapsed_ms = duration_millis_to_u64(suspend_rpc_started_at.elapsed()),
                "generation-bound Kubernetes sandbox suspend became durable"
            );
            Some(published_artifact)
        };

        self.finish_suspend_handoff(
            sandbox_id,
            expected_generation,
            expected_pod_uid,
            execution.pod_name,
            node_suspend_artifact,
        )
        .await
        .map(Some)
    }

    async fn wake_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        wake_activity: Option<ExecutionActivityKind>,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        self.wake_execution_with_metric_identity_and_activity(
            sandbox,
            timeout,
            metric_identity,
            charge,
            wake_activity,
        )
        .await
    }

    async fn runtime_client(
        &self,
        sandbox_id: SandboxId,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        let sandbox_before_resolution = self
            .store
            .require_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        if let Some(pod) = self
            .resolve_pod_instance(sandbox_id)
            .await
            .map_err(ControllerError::from)?
        {
            if pod.sandbox_restoring {
                return self
                    .join_restore_before_dial(
                        sandbox_id,
                        pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                            "runtime_client",
                        ),
                        Duration::from_secs(120),
                        charge,
                    )
                    .await;
            }
            if let Ok(client) = self.access_kept_worker_client(sandbox_id, &pod).await {
                return Ok(client);
            }
        }

        let sandbox = self
            .store
            .require_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        if is_restoring_lifecycle(&sandbox.lifecycle) {
            return self
                .join_restore_before_dial(
                    sandbox_id,
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                        "runtime_client",
                    ),
                    Duration::from_secs(120),
                    charge,
                )
                .await;
        }
        if normalize_lifecycle(&sandbox_before_resolution.lifecycle) == "suspending"
            && is_suspended_lifecycle(&sandbox.lifecycle)
        {
            return self
                .wake_and_dial_once_started(
                    &sandbox,
                    Duration::from_secs(120),
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                        "runtime_client",
                    ),
                    charge,
                )
                .await;
        }
        if is_wake_recovery_lifecycle(&sandbox.lifecycle)
            && !is_dormant_lifecycle(&sandbox.lifecycle)
        {
            let binding = self
                .ensure_execution_ready_for_access(
                    &sandbox,
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                        "runtime_client",
                    ),
                    Duration::from_secs(120),
                    charge,
                    None,
                )
                .await?;
            return connect_worker(binding.runtime_endpoint).await;
        }

        Err(ControllerError::service_unavailable(
            "sandbox is not assigned to a node-api",
        ))
    }

    async fn reachable_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        let Some(pod) = self
            .resolve_pod_instance(sandbox_id)
            .await
            .map_err(ControllerError::from)?
        else {
            return Ok(None);
        };
        if pod.sandbox_restoring {
            // Never dial a restoring pod (see `join_restore_before_dial`).
            // This path neither recovers nor waits: its callers are
            // best-effort or turn "unreachable" into a retryable 503.
            return Ok(None);
        }

        match self.access_kept_worker_client(sandbox_id, &pod).await {
            Ok(client) => Ok(Some(client)),
            Err(_) => Ok(None),
        }
    }

    async fn probe_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        let Some(pod_ip) = self
            .resolve_reachable_pod_ip(sandbox_id)
            .await
            .map_err(ControllerError::from)?
        else {
            return Ok(None);
        };

        match self.worker_client_for_pod_ip(pod_ip).await {
            Ok(client) => Ok(Some(client)),
            Err(_) => Ok(None),
        }
    }

    async fn access_runtime_client(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
        timeout: Duration,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        if is_suspended_lifecycle(&sandbox.lifecycle) {
            Self::should_allow_access_recovery(sandbox, activity)?;
            return self
                .wake_and_dial_once_started(
                    sandbox,
                    timeout,
                    metric_identity,
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await;
        }

        if let Some(pod) = self
            .resolve_pod_instance(sandbox.id)
            .await
            .map_err(ControllerError::from)?
        {
            if pod.sandbox_restoring {
                return self
                    .join_restore_before_dial(sandbox.id, metric_identity, timeout, charge)
                    .await;
            }
            let pod_ip = pod.ip;
            // A preserved slow create was reported `started` before its guest
            // command API was confirmed ready. Force a (bounded) command-ready
            // confirmation on first access so a data-plane op can't reach a
            // not-yet-ready guest, then clear the marker so subsequent accesses
            // take the plain fast path. On timeout the `SubstrateError::Timeout`
            // surfaces as the SDK-recoverable `sandbox_timeout`. Normal
            // (non-pending) sandboxes skip this entirely — no added latency.
            if sandbox.command_ready_pending {
                let sandbox_id = sandbox.id;
                self.poll_until_command_ready(
                    sandbox_id,
                    metric_identity,
                    timeout,
                    CommandReadyPod::PreResolved(pod_ip),
                    None,
                )
                .await
                .map_err(ControllerError::from)?;
                // Clearing the marker is the access side of a CAS against the
                // command-ready reaper: if it didn't claim the row, the reaper
                // may have flipped the sandbox to `failed` and started tearing
                // the pod down between our readiness probe and here. Re-read the
                // lifecycle; if it's no longer `started`, don't hand back a
                // client to a doomed pod — surface a retryable error instead. A
                // `false` with the row still `started` is a benign concurrent
                // clear, so we proceed.
                let claimed = self
                    .store
                    .clear_command_ready_pending(sandbox.id)
                    .await
                    .map_err(ControllerError::from)?;
                if !claimed {
                    let current = self
                        .store
                        .require_sandbox(sandbox_id)
                        .await
                        .map_err(ControllerError::from)?;
                    if normalize_lifecycle(&current.lifecycle) != "started" {
                        return Err(ControllerError::from(SubstrateError::Conflict {
                            message: format!(
                                "sandbox was concurrently reaped while never command-ready \
                                 (now {}); create a new sandbox",
                                current.lifecycle
                            ),
                        }));
                    }
                }
            }
            if let Ok(client) = self.access_kept_worker_client(sandbox.id, &pod).await {
                return Ok(client);
            }
        }

        let sandbox = self
            .store
            .require_sandbox(sandbox.id)
            .await
            .map_err(ControllerError::from)?;
        Self::should_allow_access_recovery(&sandbox, activity)?;
        if is_restoring_lifecycle(&sandbox.lifecycle) {
            return self
                .join_restore_before_dial(sandbox.id, metric_identity, timeout, charge)
                .await;
        }
        let binding = self
            .ensure_execution_ready_for_access(&sandbox, metric_identity, timeout, charge, None)
            .await?;
        connect_worker(binding.runtime_endpoint).await
    }

    async fn builder_client(&self) -> Result<BuilderClient, ControllerError> {
        self.builder_node_client().await
    }

    async fn builder_client_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<BuilderClient, ControllerError> {
        let node_name = sqlx::query_scalar::<_, String>(sql!(
            "SELECT nk.k8s_node_name
             FROM nodes n
             JOIN node_k8s nk ON nk.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = $2
               AND n.role = 'builder'
             LIMIT 1"
        ))
        .bind(node_id.as_uuid())
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| {
            db_contention_error(&format!("failed to load builder node {node_id}"), &error)
        })?
        .ok_or_else(|| {
            ControllerError::service_unavailable(format!(
                "builder node {node_id} is not registered"
            ))
        })?;
        let target = NodeApiTarget {
            managed_by: SpaceNodeManagedBy::Kubernetes,
            role: SpaceNodeRole::Builder,
            direct_endpoint: None,
            substrate_node_id: Some(&node_name),
        };
        let address = self
            .resolve_node_api_endpoint(&target)
            .await
            .map_err(ControllerError::from)?;
        self.builder_node_client_at(&address).await
    }

    fn pool_eligible_for_request(
        &self,
        boot_mode: CreateBootMode,
        backend: &str,
        wait_until_ready: bool,
        disk_mib: Option<u32>,
        file_changes_enabled: bool,
        implicit_wake_enabled: bool,
    ) -> bool {
        pool::pool_eligible_for_request(
            boot_mode,
            backend,
            wait_until_ready,
            disk_mib,
            file_changes_enabled,
            implicit_wake_enabled,
        )
    }

    fn pool_supports_request_env(&self) -> bool {
        true
    }

    async fn claim_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        // A pooled pod is built for no org and therefore carries no node-group
        // boundary — and adoption applies the per-sandbox setup to that
        // already-running pod over gRPC, so no second pod build ever renders
        // one. Nothing downstream re-checks placement either: a successful
        // adoption returns before `create_execution`, where the create path's
        // own resolver lives. So the claim itself has to decide, and the
        // decision is about the **node the slot is already on**, because that
        // node is where the sandbox will run.
        //
        // Resolved lazily, inside the loop and memoized: a template with no
        // warm pool at all never reaches a claim, and paying a catalog read on
        // every pool-eligible create to answer a question that cannot arise
        // would tax the common path for the rare one.
        //
        // Every refusal below picks one of two ways to give the slot up, and
        // the choice is a policy knob with a real cost on each side:
        //
        // * `release` returns the slot to the pool with `ready_at` untouched.
        //   The pool keeps its capacity — but the claim orders by `ready_at
        //   ASC`, so a slot released for a reason that will still hold next
        //   time is handed straight back to the next create, forever. That is
        //   a tarpit: the pool's hit rate sits at zero with every pod healthy.
        // * `invalidate` takes the slot out of the pool for the reconciler to
        //   replace. It ends the tarpit — but it destroys a warm pod, and if
        //   the reason was momentary the pool pays a create to rebuild what it
        //   already had.
        //
        // So the rule is: release when the refusal is about *this create* and
        // the slot is still good for the next one; invalidate when the refusal
        // is evidence about *the slot*. Each arm says which side it is on.
        //
        // One arm is neither, and is the deliberate exception: `Unknown` is
        // about our *ability to read* the node, which says nothing about either
        // the create or the slot. It releases, because the common cause is a
        // node cache still filling on a freshly rolled replica — invalidating
        // there would destroy warm pods on every deploy, per replica. The cost
        // is that a permanently unreadable node (a reflector task that panicked
        // and left a readable stale store) re-offers the same slot to every
        // create. That is a pre-existing reflector-lifecycle gap rather than a
        // reason to invert this knob, and `nodes_ready()` cannot bound it
        // because `ReflectorReady::transition_to` is a one-way latch that keeps
        // reporting ready after the writer drops.
        let mut placement: Option<ResolvedPlacement> = None;
        let mut attempted_pool_shape = None;
        for attempt in 1..=MAX_STALE_POOL_CLAIMS_PER_REQUEST {
            let claim_attempt = pool::claim_pooled_execution(
                self.store.pool(),
                request,
                &self.k8s.sandbox_namespace,
            )
            .await?;
            attempted_pool_shape = claim_attempt.pool_shape;
            let Some(claim) = claim_attempt.claim else {
                return Ok(claim_attempt);
            };

            let pod = match self.read_sandbox_pod(&claim.pod_name).await {
                Ok(pod) => pod,
                Err(error) => {
                    pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                    tracing::warn!(
                        claim_id = %claim.claim_id,
                        sandbox_id = %claim.sandbox_id,
                        pod_name = %claim.pod_name,
                        attempt,
                        %error,
                        "failed to validate claimed Kubernetes warm-pool pod; released claim and falling back"
                    );
                    return Ok(PooledExecutionClaimAttempt::missed(
                        claim_attempt.pool_shape,
                    ));
                }
            };
            // Usability first, and this order matters. A pod that is gone,
            // replaced, or finished makes every node question unanswerable —
            // and it is the *stale claim* path, so the slot is invalidated and
            // can never be handed to another caller. Asking the candidate set
            // first would let an unreadable node turn a dead pod's slot into a
            // `release`, which puts it back at the front of the queue for every
            // create that follows.
            if !pooled_claim_pod_is_usable(&claim, pod.as_deref()) {
                tracing::warn!(
                    claim_id = %claim.claim_id,
                    sandbox_id = %claim.sandbox_id,
                    pod_name = %claim.pod_name,
                    attempt,
                    "invalidating stale Kubernetes warm-pool claim"
                );
                pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
                continue;
            }

            let resolved = match placement.as_ref() {
                Some(resolved) => resolved,
                None => {
                    let resolved = self
                        .node_group_placement
                        .resolve(
                            NodeGroupResolvePath::PoolClaim,
                            PlacementSubject::Sandbox(request.actor.org_id),
                            request.template,
                        )
                        .await;
                    match resolved {
                        Ok(resolved) => placement.insert(resolved),
                        // Nothing can be shown to satisfy a boundary that
                        // cannot be computed, so this claim can't be admitted.
                        // It reports a *miss*, not an error: the cold create
                        // resolves the same candidate set on `path:create` and
                        // fails there, which is where the user-facing mapping
                        // and the operator's metric both live. Returning `Err`
                        // here instead would abort the create inside the claim
                        // step, book it as `warm_pool_claim/claim_failed`, and
                        // read to an operator as warm-pool trouble rather than
                        // a placement refusal — and it would turn a momentary
                        // catalog read failure into a failed create that a
                        // cold build would have served.
                        Err(error) => {
                            // Which disposition the rule above picks turns on
                            // the same question the dedicated-only arm asks —
                            // can a pooled pod host this template at all? —
                            // and here the caller's own error already answers
                            // it.
                            //
                            // `NoCompatibleNodeGroup` from a caller that *has*
                            // an org can only mean an org-specific template: a
                            // generic one with no compatible group reports
                            // `Unprovisioned` instead, because the `Generic`
                            // arm of the compatibility relation does not read
                            // the caller. An org-specific template is
                            // unhostable by every org-less pod, so the config
                            // is dead to every caller, its refill is suppressed
                            // for the same reason, and its slots sit on shared
                            // workers holding a private rootfs. Invalidate —
                            // that is evidence about the slot.
                            //
                            // A reaped `templates` row is not that. It is a
                            // control-plane inconsistency an operator repairs
                            // by re-creating the row, after which these slots
                            // are usable again, so release and keep the capacity.
                            // Same for a catalog blip, which is evidence about
                            // nothing.
                            let dead_config = matches!(
                                error,
                                NodeGroupPlacementResolveError::NoCompatibleNodeGroup { .. }
                            );
                            // Each standing condition is reported as a refusal
                            // of the class the *cold path* would have produced
                            // for it, so a strict `boot_mode=pool` create — the
                            // one mode with no cold path to fall through to —
                            // gets the same answer either way instead of a
                            // retry instruction it can never satisfy.
                            //
                            // `NoCompatibleNodeGroup` maps to `Conflict` there,
                            // so it is a 409 here. `TemplateAbsent` and
                            // `Invariant` map to `Failed`, so they are 500s:
                            // a reaped row is a control-plane fault an operator
                            // repairs, and answering it 409 would take the only
                            // signal of it out of 5xx alerting — while
                            // answering 503 would tell the client to retry what
                            // only an operator can fix. Only `Catalog` is
                            // genuinely momentary, and only there is the
                            // ordinary retryable miss the right answer, because
                            // a retry really can clear it.
                            let refusal = match &error {
                                NodeGroupPlacementResolveError::NoCompatibleNodeGroup {
                                    ..
                                } => Some(PooledClaimRefusal::PlacementIncompatible {
                                    message: error.to_string(),
                                }),
                                NodeGroupPlacementResolveError::TemplateAbsent { .. }
                                | NodeGroupPlacementResolveError::Invariant(_) => {
                                    Some(PooledClaimRefusal::PlacementUnresolvable {
                                        message: error.to_string(),
                                    })
                                }
                                NodeGroupPlacementResolveError::Catalog(_) => None,
                            };
                            if dead_config {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::OrgSpecificTemplate,
                                );
                                tracing::info!(
                                    claim_id = %claim.claim_id,
                                    template = request.template,
                                    %error,
                                    "invalidating a warm-pool slot whose template no pooled \
                                     pod may host; this config drains and does not refill"
                                );
                                pool::invalidate_claimed_pool_slot(self.store.pool(), &claim)
                                    .await?;
                            } else {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unresolved,
                                );
                                tracing::info!(
                                    claim_id = %claim.claim_id,
                                    template = request.template,
                                    %error,
                                    "declining the warm pool: this sandbox's node-group \
                                     candidate set could not be resolved; released the claim \
                                     and falling through to a cold create unless the request \
                                     pinned boot_mode=pool, which has no cold path"
                                );
                                pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                            }
                            return Ok(match refusal {
                                Some(refusal) => PooledExecutionClaimAttempt::refused(
                                    claim_attempt.pool_shape,
                                    refusal,
                                ),
                                None => {
                                    PooledExecutionClaimAttempt::missed(claim_attempt.pool_shape)
                                }
                            });
                        }
                    }
                }
            };

            match resolved.placement() {
                // Gate off, or no generic group on this cluster: the pod
                // carries no boundary and neither does the sandbox.
                //
                // The two are booked differently. With the gate off there is no
                // placement decision to record and the series stays silent.
                // Unprovisioned *is* a decision — the gate is on and the answer
                // was "nothing to enforce" — and recording it is what keeps
                // this counter a total on a cluster that reached stage 3 before
                // stage 0, where `admitted` is necessarily zero while adoption
                // works perfectly well.
                None => {
                    if matches!(resolved, ResolvedPlacement::Unprovisioned) {
                        // `Unprovisioned` means no *generic* group admits this
                        // template — not that the cluster has no groups. A
                        // dedicated group can exist without one (nothing orders
                        // `create_node_group`), and in that state this sandbox
                        // renders no boundary while some nodes belong to
                        // another org's hardware.
                        //
                        // Rendering nothing there is pre-existing behavior;
                        // a generic group must be provisioned before dedicated
                        // groups. Adopting a pod already sitting on such a node
                        // would hand this sandbox capacity
                        // reserved for another org with no check at all, since
                        // there is no candidate set here to check against.
                        //
                        // So a labeled node is disqualifying on this arm. With
                        // no generic group provisioned, a node carrying a
                        // group label belongs to a dedicated group by
                        // elimination — the only writer is the labeler and the
                        // only groups are dedicated ones — so the slot is
                        // released and the sandbox builds cold. On a genuinely
                        // unprovisioned cluster no node is labeled and nothing
                        // changes.
                        match pooled_pod_node_group(&self.k8s.caches, pod.as_deref()) {
                            PooledPodNodeGroup::Labeled(group) => {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::UnprovisionedLabeledNode,
                                );
                                tracing::warn!(
                                    claim_id = %claim.claim_id,
                                    sandbox_id = %claim.sandbox_id,
                                    pod_name = %claim.pod_name,
                                    node_group = %group,
                                    "declining a warm-pool slot whose node belongs to a node \
                                     group while no generic group admits this template; a \
                                     dedicated group exists without a default generic one"
                                );
                                pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                                return Ok(PooledExecutionClaimAttempt::missed(
                                    claim_attempt.pool_shape,
                                ));
                            }
                            // Cannot be shown safe, so it is not adopted. This
                            // arm has no candidate set to test membership
                            // against, so an unreadable node is the one case
                            // where "adopt anyway" would rest on nothing at
                            // all — a cold reflector would hand this sandbox a
                            // pod that may be sitting on another org's
                            // dedicated hardware.
                            PooledPodNodeGroup::Unreadable => {
                                pplx_space_metrics::node_group_placement::pool_claim_admission(
                                    pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unknown,
                                );
                                tracing::info!(
                                    claim_id = %claim.claim_id,
                                    pod_name = %claim.pod_name,
                                    "could not read the claimed warm pod's node while no generic \
                                     group admits this template; released the claim and \
                                     building cold rather than adopting unverified"
                                );
                                pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                                return Ok(PooledExecutionClaimAttempt::missed(
                                    claim_attempt.pool_shape,
                                ));
                            }
                            // Read, and in no group: the genuinely
                            // unprovisioned cluster, where there is nothing to
                            // enforce and nothing to be outside of.
                            PooledPodNodeGroup::Unlabeled => {}
                        }
                        pplx_space_metrics::node_group_placement::pool_claim_admission(
                            pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unconstrained,
                        );
                    }
                    return Ok(PooledExecutionClaimAttempt {
                        claim: Some(claim),
                        pool_shape: claim_attempt.pool_shape,
                        refusal: None,
                    });
                }
                Some(placement) => {
                    // Dedicated-only set: every candidate is this caller's own
                    // hardware, and a pooled pod lands on shared capacity by
                    // construction, so no slot can satisfy *this* sandbox.
                    // Answered from the set alone, without reading a node.
                    //
                    // How to give the slot up then depends on a question the
                    // caller's set cannot answer: can the pool host this
                    // template for *anybody*? Two different states reach here.
                    //
                    // * An **org-specific** template. No org-less pod may ever
                    //   host it, so every slot under the config is dead to
                    //   every caller, and `reconcile_config`'s own org-less
                    //   resolve fails and suppresses refill. Invalidate: the
                    //   pool drains to zero, and the private rootfs those pods
                    //   pulled stops sitting on shared workers.
                    // * A **generic** template on a cluster that has a
                    //   dedicated group and no generic group. This caller owns
                    //   dedicated capacity so its set is all its own, but an
                    //   org-less resolve of the same template reports
                    //   `Unprovisioned` — `Ok`, not `Err` — so refill keeps
                    //   running and a caller without dedicated capacity adopts
                    //   these slots unconstrained. Release: invalidating would
                    //   destroy a usable pod once per create, forever, and the
                    //   reconciler would rebuild it just as fast.
                    //
                    // So ask the pool's own question directly rather than infer
                    // it. One extra catalog read, on an arm that returns.
                    if !placement.admits_shared_capacity() {
                        let poolable = self
                            .node_group_placement
                            .resolve(
                                NodeGroupResolvePath::PoolClaim,
                                PlacementSubject::WarmPool,
                                request.template,
                            )
                            .await;
                        // Three outcomes, not a bool. The refusal itself is
                        // the same in all of them — this sandbox's set is its
                        // own dedicated hardware and no pooled pod runs there,
                        // which no retry changes — but what it says about the
                        // *config*, and therefore what the slot and the caller
                        // are told, differs:
                        //
                        // Note the deliberate asymmetry with the arm above,
                        // which withholds a refusal for a `Catalog` blip alone
                        // — a reaped row and a predicate disagreement carry
                        // `PlacementUnresolvable` there. It is not an
                        // inconsistency to harmonize: up there the sandbox's own
                        // set is unknown, so a momentary read failure
                        // establishes nothing about whether a pooled pod could
                        // ever serve it; down here the set resolved and is
                        // dedicated-only, which settles that on its own
                        // whatever this second read says — so every outcome
                        // below refuses, and only the slot's fate varies.
                        //
                        // * `Ok(_)` — the pool can host this template for
                        //   somebody (an `Unprovisioned` cluster, or a generic
                        //   set this caller is simply not in). Release, so
                        //   those callers keep the slot.
                        // * `Err(NoCompatibleNodeGroup)` — org-specific
                        //   template, the one error that proves it: a generic
                        //   template with no groups at all reports
                        //   `Unprovisioned`. Nobody can adopt any slot under
                        //   this config, so invalidate.
                        // * `Err(_)` — a reaped row or a catalog blip on this
                        //   second read. It proves nothing about the config, so
                        //   release; and it must not borrow either of the other
                        //   two messages, both of which state a cluster fact as
                        //   certain. The first read succeeded, so this is the
                        //   only place that failure is newly reachable.
                        // The message is built as a base plus an optional
                        // cause, rather than three independent strings. The
                        // base is derived from the sandbox's own resolved set,
                        // so it is true in every arm; a cause is appended only
                        // where this second read actually established one. A
                        // fourth variant added later therefore falls back to
                        // saying less, not to asserting something false — which
                        // is the failure the previous shape had.
                        let base = format!(
                            "no warm-pool pod runs on capacity that sandbox template '{}' may \
                             use",
                            request.template
                        );
                        let (verdict, cause, invalidate) = match &poolable {
                            // `Unprovisioned` is the only outcome that
                            // establishes the cluster has no shared group for
                            // this template. `Placed` reaching here means a
                            // generic group appeared between the claim's two
                            // reads — the race the `caller_dedicated_only`
                            // catalog entry names — and asserting the cluster
                            // fact there would state something the second read
                            // just disproved. Same verdict, since the slot's
                            // fate is the same; no cause, because none was
                            // established for *this* create.
                            Ok(ResolvedPlacement::Unprovisioned) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::CallerDedicatedOnly,
                                Some(
                                    "this cluster has no shared node group it may use, so only \
                                     capacity dedicated to this organization is eligible",
                                ),
                                false,
                            ),
                            Ok(ResolvedPlacement::Disabled | ResolvedPlacement::Placed(_)) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::CallerDedicatedOnly,
                                None,
                                false,
                            ),
                            Err(NodeGroupPlacementResolveError::NoCompatibleNodeGroup {
                                ..
                            }) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::OrgSpecificTemplate,
                                Some(
                                    "the template may run only on node groups dedicated to its \
                                     organization",
                                ),
                                true,
                            ),
                            Err(_) => (
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unresolved,
                                None,
                                false,
                            ),
                        };
                        let message = match cause {
                            Some(cause) => format!("{base}: {cause}"),
                            None => base,
                        };
                        pplx_space_metrics::node_group_placement::pool_claim_admission(verdict);
                        if invalidate {
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                template = request.template,
                                "invalidating a warm-pool slot whose template no pooled pod \
                                 may host; this config drains and does not refill"
                            );
                            pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
                        } else {
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                template = request.template,
                                %verdict,
                                "declining the warm pool: this sandbox admits only dedicated \
                                 capacity, and nothing proves the slot is unusable by another \
                                 caller, so it goes back"
                            );
                            pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                        }
                        return Ok(PooledExecutionClaimAttempt::refused(
                            claim_attempt.pool_shape,
                            PooledClaimRefusal::PlacementIncompatible { message },
                        ));
                    }
                    match pooled_pod_node_admission(&self.k8s.caches, placement, pod.as_deref()) {
                        PooledPodAdmission::Admitted => {
                            pplx_space_metrics::node_group_placement::pool_claim_admission(pplx_space_metrics::node_group_placement::PoolClaimAdmission::Admitted);
                            return Ok(PooledExecutionClaimAttempt {
                                claim: Some(claim),
                                pool_shape: claim_attempt.pool_shape,
                                refusal: None,
                            });
                        }
                        // The node's own label puts it outside the set — a
                        // group reassigned under resident slots. Take the slot
                        // *out* of the pool rather than releasing it: `release`
                        // leaves `ready_at` untouched and the claim orders by
                        // `ready_at ASC`, so releasing would hand the same slot
                        // to the next create forever and the pool's hit rate
                        // would sit at zero with every pod healthy. Invalidating
                        // lets the reconciler replace it somewhere admissible,
                        // and also stops a warm pod squatting on hardware that
                        // now belongs to one org.
                        PooledPodAdmission::OutsideSet => {
                            pplx_space_metrics::node_group_placement::pool_claim_admission(pplx_space_metrics::node_group_placement::PoolClaimAdmission::OutsideSet);
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                sandbox_id = %claim.sandbox_id,
                                pod_name = %claim.pod_name,
                                attempt,
                                "invalidating a warm-pool slot whose node is outside this \
                                 sandbox's node-group candidate set"
                            );
                            pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
                            continue;
                        }
                        // The node is unlabeled: refuse this adoption, but
                        // leave the pod alone. The label is the labeler's to
                        // write and it repairs on its next tick, so the slot is
                        // usable again shortly — whereas invalidating would
                        // turn a label write into fleet-wide pool loss. The
                        // brief tarpit that a release causes here (`ready_at`
                        // is untouched, so this slot is re-picked first) is
                        // bounded by that tick, and every isolation guarantee
                        // still holds because the sandbox builds cold.
                        PooledPodAdmission::LabelAbsent => {
                            pplx_space_metrics::node_group_placement::pool_claim_admission(
                                pplx_space_metrics::node_group_placement::PoolClaimAdmission::LabelAbsent,
                            );
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                sandbox_id = %claim.sandbox_id,
                                pod_name = %claim.pod_name,
                                attempt,
                                "declining a warm-pool slot whose node carries no node-group \
                                 label; released the claim for the labeler to repair and \
                                 falling through to a cold create"
                            );
                            pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                            return Ok(PooledExecutionClaimAttempt::missed(
                                claim_attempt.pool_shape,
                            ));
                        }
                        // Not evidence against the slot, so leave it in the
                        // pool and build cold.
                        PooledPodAdmission::Unknown => {
                            // Booked before the release, so a release failure
                            // propagates with the verdict already recorded.
                            // Every arm here does the same, which makes this
                            // counter a total of the *placement* decisions —
                            // not of claims: an unusable pod, an unreadable
                            // pod, and a gate-off adoption all pass through
                            // without a verdict, because none of them is a
                            // placement decision.
                            pplx_space_metrics::node_group_placement::pool_claim_admission(pplx_space_metrics::node_group_placement::PoolClaimAdmission::Unknown);
                            pool::release_claimed_pool_slot(self.store.pool(), &claim).await?;
                            tracing::info!(
                                claim_id = %claim.claim_id,
                                pod_name = %claim.pod_name,
                                "could not read the claimed warm pod's node group; released \
                                 the claim and falling through to a cold create unless the \
                                 request pinned boot_mode=pool, which has no cold path"
                            );
                            return Ok(PooledExecutionClaimAttempt::missed(
                                claim_attempt.pool_shape,
                            ));
                        }
                    }
                }
            }
        }

        tracing::warn!(
            limit = MAX_STALE_POOL_CLAIMS_PER_REQUEST,
            "stopped claiming Kubernetes warm-pool slots after discarding unusable candidates"
        );
        Ok(PooledExecutionClaimAttempt::missed(attempted_pool_shape))
    }

    async fn lookup_admitted_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        admitted: AdmittedPoolClaim,
        sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        let lookup = pool::lookup_admitted_pooled_execution(
            self.store.pool(),
            request,
            admitted,
            sandbox_id,
            &self.k8s.sandbox_namespace,
        )
        .await?;
        let AdmittedPoolClaimLookup::Adoptable { claim, pool_shape } = lookup else {
            return Ok(lookup);
        };

        // The same pod and placement checks `claim_pooled_execution` makes,
        // with one difference in how a slot is given up. The ordinary claim
        // may `release` a slot that is fine for another caller; this slot's
        // reserved ID is already this create's public ID, so returning it to
        // the inventory would let another create bind the same ID. Every
        // refusal here invalidates instead: the slot leaves the inventory, the
        // stale-claimed sweep collects its pod, and this create builds cold.
        let usable = match self.read_sandbox_pod(&claim.pod_name).await {
            Ok(pod) => {
                pooled_claim_pod_is_usable(&claim, pod.as_deref())
                    && self
                        .admitted_pool_pod_placement_admits(request, pod.as_deref())
                        .await
            }
            Err(error) => {
                tracing::warn!(
                    claim_id = %claim.claim_id,
                    sandbox_id = %claim.sandbox_id,
                    pod_name = %claim.pod_name,
                    %error,
                    "failed to read the admitted warm-pool pod; not adopting it"
                );
                false
            }
        };
        if !usable {
            tracing::info!(
                claim_id = %claim.claim_id,
                sandbox_id = %claim.sandbox_id,
                pod_name = %claim.pod_name,
                "invalidating an admitted warm-pool slot this create will not adopt"
            );
            pool::invalidate_claimed_pool_slot(self.store.pool(), &claim).await?;
            return Ok(AdmittedPoolClaimLookup::ReleasePending);
        }
        Ok(AdmittedPoolClaimLookup::Adoptable { claim, pool_shape })
    }

    async fn skip_admitted_pool_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), ControllerError> {
        self.store
            .retire_pool_reserved_generation(sandbox_id)
            .await
            .map_err(ControllerError::from)
    }

    async fn admitted_pool_cleanup_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, ControllerError> {
        pool::admitted_pool_cleanup_pending(self.store.pool(), sandbox_id).await
    }

    async fn claim_admission_pool_slot(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
    ) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
        pool::claim_admission_pool_slot(tx, template_release_id, &self.k8s.sandbox_namespace).await
    }

    async fn persist_pooled_execution_claim(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        pool::persist_pooled_execution_claim(tx, request, claim).await
    }

    async fn rollback_pooled_execution_claim(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        pool::invalidate_claimed_pool_slot(self.store.pool(), claim)
            .await
            .map_err(|error| {
                ControllerError::internal(format!(
                    "pool claim leaked for pod {}: failed to invalidate claimed pool slot: {error}",
                    claim.pod_name
                ))
            })
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        pool::mark_claimed_pool_slot_cleanup_pending(self.store.pool(), claim).await?;
        Ok(PooledPrepareFailureCleanup::DeferredToReconciler)
    }

    async fn apply_pooled_execution_auto_lifecycle(
        &self,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        let pod_ip = PodIp::parse(&claim.pod_ip).map_err(ControllerError::from)?;
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        let mut node_api = NodeApiClient::with_service_name("worker-node-api", endpoint);

        let mut client = RetryingWorkerApiClient::new(&mut node_api);
        Self::apply_pooled_execution_auto_lifecycle_with_client(&mut client, claim, policy).await
    }

    async fn prepare_pooled_execution(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
        wait_until_ready: bool,
        timeout: Duration,
    ) -> Result<(), ControllerError> {
        let sandbox_id = claim.sandbox_id;
        let pod_ip = PodIp::parse(&claim.pod_ip).map_err(ControllerError::from)?;
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        let mut node_api = NodeApiClient::with_service_name("worker-node-api", endpoint);
        let require_pooled_adoption_node_id = if wait_until_ready {
            wait_for_pooled_split_receipt_capability(
                &mut node_api,
                Instant::now() + self.budgets.pooled_split_receipt_wait,
            )
            .await?;
            true
        } else {
            // Manager-local Kubernetes node hydration and exact operation
            // receipts shipped behind the same prerequisite. Keep the landed
            // mixed-version contract for non-starting adoption: an older or
            // rolled-back manager may omit only its locally-owned node ID,
            // while any node ID it does return must still match the claim.
            node_api
                .health_service_serving(
                    pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                    WARM_POOL_SETUP_RPC_TIMEOUT,
                )
                .await
                .map_err(|error| {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox route-identity capability check failed: {error}"
                    ))
                })?
        };

        // Resolve the per-sandbox setup through the same shared routine the cold
        // create path uses, then apply every step to the already-booted pool pod
        // via gRPC. `apply_adopted_per_sandbox_setup` consumes the resolved setup
        // by exhaustive destructure, so a new cold-path setup step can't silently
        // skip pooled sandboxes. See `per_sandbox_setup`.
        let mut setup = PerSandboxSetup::from_claim_request(request, claim);
        // Pool ownership precedes this policy capture; the create request may
        // still contain the policy from before the claim.
        setup.auto_lifecycle = policy.clone();
        // Inheritance is sent at 0 and an override at the request generation,
        // as the cold pod seeds it: a later reconcile or user override carries
        // the API's Postgres generation and must not be skipped or collide
        // with a template counter.
        let network_policy_generation = if request.network_policy_override.is_some() {
            request.network_policy_generation
        } else {
            0
        };
        self.apply_adopted_per_sandbox_setup(
            &mut node_api,
            claim,
            setup,
            network_policy_generation,
        )
        .await?;

        if !wait_until_ready {
            let mut internal_client = RetryingWorkerApiInternalClient::new(&mut node_api);
            let mut wake_request = pooled_route_identity_adoption_request(claim)?;
            let expected_route_identity = wake_request
                .start_context
                .as_ref()
                .and_then(|context| context.route_identity.as_ref())
                .expect("pooled route adoption request always has a route identity")
                .clone();
            wake_request
                .start_context
                .as_mut()
                .expect("pooled route adoption request always has start context")
                .route_identity = Some(wake_request_route_identity(&expected_route_identity));
            let response = internal_client
                .wake_sandbox(wake_request, WARM_POOL_SETUP_RPC_TIMEOUT)
                .instrument(tracing::info_span!(
                    "substrate.k8s.pool.adopt_route_identity.client",
                    sandbox_id = %sandbox_id,
                ))
                .await
                .map_err(|err| {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox route identity adoption failed: {err}"
                    ))
                })?;
            validate_wake_route_identity_with_node_requirement(
                response.get_ref(),
                &claim.sandbox_id.to_string(),
                &expected_route_identity,
                require_pooled_adoption_node_id,
            )
            .map_err(ControllerError::from)?;
        } else {
            let requested_attempt_id =
                pplx_space_common_core::types::SandboxActivationAttemptId::new();
            let execution_generation = i64::try_from(claim.execution_generation)
                .map_err(|_| ControllerError::internal("pool execution generation exceeds i64"))?;
            let (activation_attempt_id, joined_attempt_id) = match self
                .store
                .assign_activation_attempt(
                    sandbox_id,
                    execution_generation,
                    requested_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await
            {
                Ok(assigned_attempt_id) => (assigned_attempt_id, None),
                Err(SubstrateError::ActivationInFlight {
                    same_target: true,
                    owner_attempt_id,
                    ..
                }) => (requested_attempt_id, Some(owner_attempt_id)),
                Err(error) => return Err(ControllerError::from(error)),
            };
            let fence = self
                .store
                .reconcile_activation_fence(
                    sandbox_id,
                    activation_attempt_id,
                    joined_attempt_id,
                    SandboxTransitionTargetState::Started,
                )
                .await
                .map_err(ControllerError::from)?;
            let fence_ownership = fence.ownership();
            let owned_attempt_id = fence.owned_attempt_id();
            let mut wake_may_have_landed = false;
            let wake_and_ready = async {
                // Node-fs artifact-dir permissions are ensured eagerly by
                // sbx-mgr's shared resume path (`resume_after_wake_lock`)
                // before the VM resumes — API cannot reach node-mgr directly.
                // Once the pool slot is bound, PostgreSQL is authoritative:
                // during the consumer-first rollout the old producer may
                // still rotate this value away from the original slot ID.
                let wake_deadline = Instant::now() + timeout;
                let wake_action = activation_wake_action(
                    &fence.execution,
                    fence_ownership,
                    SandboxLifecycle::Started,
                )
                .map_err(ControllerError::from)?;
                if let ActivationWakeAction::Execute {
                    route_identity: expected_route_identity,
                    policy,
                    require_split_receipt_capability,
                    publish_legacy_route_after_ack,
                    ..
                } = wake_action
                {
                    // Per-sandbox setup can outlive a manager restart. Recheck
                    // the dynamically bridged capability immediately before
                    // emitting the split A/B request; the pre-setup check
                    // prevents partial setup against an incompatible node,
                    // while this check closes that restart window.
                    if require_split_receipt_capability {
                        wait_for_pooled_split_receipt_capability(
                            &mut node_api,
                            wake_deadline,
                        )
                        .await?;
                    }
                    let mut wake_request = pooled_wake_request(
                        claim,
                        pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
                            pooled_route_activation_id(claim)?,
                        ),
                        pplx_space_sbx_mgr_client::proto::WakeTargetState::Unspecified,
                    )?;
                    wake_request
                        .start_context
                        .as_mut()
                        .expect("pooled Wake request always has start context")
                        .route_identity =
                        Some(wake_request_route_identity(&expected_route_identity));
                    wake_request
                        .start_context
                        .as_mut()
                        .expect("pooled Wake context")
                        .operation_activation_attempt_id = fence
                        .execution
                        .activation_attempt_id
                        .map(|id| id.to_string());

                    let wake_started_at = Instant::now();
                    let mut internal_client = RetryingWorkerApiInternalClient::new(&mut node_api);
                    loop {
                        let Some(remaining) = wake_deadline.checked_duration_since(Instant::now())
                        else {
                            return Err(ControllerError::service_unavailable(
                                "pooled sandbox Wake did not complete within the readiness budget",
                            ));
                        };
                        // Polling the RPC is the point of no return: any
                        // transport error after this may hide a manager-side
                        // lifecycle transition or delayed completion receipt.
                        wake_may_have_landed = true;
                        let response = internal_client
                            .wake_sandbox(wake_request.clone(), remaining.min(NODE_RPC_TIMEOUT))
                            .instrument(tracing::info_span!(
                                "substrate.k8s.pool.wake_sandbox.client",
                                sandbox_id = %sandbox_id,
                            ))
                            .await;
                        match response {
                            Ok(response) => {
                                validate_wake_route_identity_if_present(
                                    response.get_ref(),
                                    &claim.sandbox_id.to_string(),
                                    &expected_route_identity,
                                    policy.requires_route_identity(),
                                )
                                .map_err(ControllerError::from)?;
                                break;
                            }
                            Err(error) => match classify_wake_rpc_error(&error) {
                                WakeRpcErrorDisposition::ReadinessPoll(_) => {
                                    let delay = start_wait_poll_interval(wake_started_at);
                                    if tokio::time::timeout_at(
                                        wake_deadline,
                                        tokio::time::sleep(delay),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        return Err(ControllerError::service_unavailable(format!(
                                            "pooled sandbox Wake did not complete within the readiness budget: {error}"
                                        )));
                                    }
                                }
                                WakeRpcErrorDisposition::Fail => {
                                    return Err(ControllerError::conflict(format!(
                                        "sandbox wake failed: {error}"
                                    )));
                                }
                            },
                        }
                    }
                    if let Some(route_activation_id) = publish_legacy_route_after_ack {
                        let expected_pod_uid = expected_route_identity
                            .pod_uid
                            .as_deref()
                            .ok_or_else(|| {
                                ControllerError::internal(
                                    "acknowledged pooled legacy route has no Pod UID",
                                )
                            })?;
                        self.store
                            .publish_legacy_route_identity_after_ack(
                                sandbox_id,
                                fence.execution.generation,
                                route_activation_id,
                                expected_pod_uid,
                            )
                            .await
                            .map_err(ControllerError::from)?;
                    }
                }
                self.poll_connected_client_until_command_ready(
                    &mut node_api,
                    sandbox_id,
                    metric_identity,
                    wake_deadline
                        .checked_duration_since(Instant::now())
                        .unwrap_or_default(),
                    ExecutionBinding {
                        execution_object_id: claim.pod_name.clone(),
                        runtime_endpoint: Self::endpoint_for_pod_ip(
                            pod_ip,
                            self.k8s.sandbox_proxy_port,
                        )
                        .map_err(ControllerError::from)?,
                        execution_generation: claim.execution_generation,
                    },
                    None,
                )
                .await
                .map_err(ControllerError::from)
            }
            .await;
            if let Some(activation_attempt_id) = owned_attempt_id
                && (wake_and_ready.is_ok() || !wake_may_have_landed)
            {
                self.store
                    .release_activation_attempt_best_effort(
                        sandbox_id,
                        fence.execution.generation,
                        activation_attempt_id,
                    )
                    .await;
            }
            wake_and_ready?;
        }

        Ok(())
    }

    async fn list_pool_configs(&self) -> Result<Vec<ClusterPoolConfigResponse>, ControllerError> {
        pool::list_pool_configs(&self.store).await
    }

    async fn upsert_pool_config(
        &self,
        request: ClusterPoolConfigRequest,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        pool::upsert_pool_config(&self.store, request).await
    }

    async fn disable_pool_config(
        &self,
        id: Uuid,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        pool::disable_pool_config(&self.store, id).await
    }

    async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        let started_at = Instant::now();
        loop {
            match self.try_resolve_node_api_endpoint(target) {
                Ok(endpoint) => return Ok(endpoint),
                Err(err)
                    if matches!(err, SubstrateError::Unavailable { .. })
                        && started_at.elapsed() < NODE_API_ENDPOINT_RESOLVE_TIMEOUT =>
                {
                    sleep(NODE_API_ENDPOINT_RESOLVE_RETRY_INTERVAL).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn select_ready_worker_node_api_target(
        &self,
    ) -> Result<OwnedNodeApiTarget, SubstrateError> {
        let candidates = self
            .list_inventory_nodes(Some(SpaceNodeRole::Worker.as_str()))
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter(|node| {
                node.state
                    .as_deref()
                    .and_then(|state| state.parse::<NodeLifecycleState>().ok())
                    == Some(NodeLifecycleState::Ready)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(SubstrateError::Unavailable {
                message: "no ready Kubernetes worker node-api pods".to_string(),
            });
        }
        let index = rand::rng().random_range(0..candidates.len());
        let candidate = candidates
            .get(index)
            .ok_or_else(|| SubstrateError::Failed {
                message: "ready Kubernetes worker selection produced an invalid index".to_string(),
            })?;
        Ok(OwnedNodeApiTarget {
            managed_by: candidate.managed_by,
            role: candidate.role,
            direct_endpoint: candidate.address.as_ref().map(ToString::to_string),
            substrate_node_id: candidate
                .substrate_node_id
                .clone()
                .or_else(|| candidate.name.clone()),
        })
    }

    async fn list_inventory_nodes(
        &self,
        role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        let pods = self.k8s.caches.node_api_pods.state();
        let mut candidates = Vec::with_capacity(pods.len());
        for pod in pods {
            let Some(app_name) = caches::pod_app_name(&pod) else {
                continue;
            };
            let Some(role) = caches::role_from_app_name(app_name) else {
                continue;
            };
            let role = SpaceNodeRole::parse(role).map_err(|err| SubstrateError::Failed {
                message: format!("node-api pod advertised invalid role {role:?}: {err}"),
            })?;
            if let Some(filter) = role_filter
                && filter != role.as_str()
            {
                continue;
            }
            let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) else {
                continue;
            };
            let Some(k8s_node) = self.k8s.caches.node_by_name(node_name) else {
                // Node hasn't been seen by the Node reflector yet; skip
                // rather than fabricate an ID. Will appear once the watch
                // catches up.
                continue;
            };
            let Some(uid_str) = k8s_node.metadata.uid.as_deref() else {
                continue;
            };
            let id = space_k8s_node_id(node_name, role.as_str());
            let Some(raw_address) = k8s_node_internal_ip(&k8s_node) else {
                tracing::warn!(
                    k8s_node_name = %node_name,
                    role = %role,
                    "skipping Kubernetes node inventory row without InternalIP"
                );
                continue;
            };
            let address = match raw_address.parse::<NodeAddress>() {
                Ok(address) => address,
                Err(err) => {
                    tracing::warn!(
                        k8s_node_name = %node_name,
                        role = %role,
                        raw_address = %raw_address,
                        %err,
                        "skipping Kubernetes node inventory row with invalid InternalIP"
                    );
                    continue;
                }
            };
            // The inventory listing derives state from node-api *pod*
            // readiness, which lags Kubernetes Node readiness during a
            // node-api rollout. It is not the lifecycle owner, so it persists
            // with `InsertOnly` — it seeds the state of a brand-new node row
            // but must never overwrite a live node's `kube_node_sync`-owned
            // state, or an unready pod on a Ready node would oscillate it back
            // to `registered`. The returned entry still reports the observed
            // pod-readiness state.
            let observed_state = NodeLifecycleState::from_ready(caches::pod_is_ready(&pod));
            match self
                .store
                .persist_k8s_node_inventory(
                    node_name,
                    uid_str,
                    id,
                    role,
                    Some(raw_address.as_str()),
                    NodeStateWrite::InsertOnly(observed_state),
                )
                .await
            {
                Ok(_) => {}
                Err(SubstrateError::NodeTombstoned { message }) => {
                    tracing::info!(
                        k8s_node_name = %node_name,
                        role = %role,
                        reason = %message,
                        "skipping tombstoned kube node from inventory listing"
                    );
                    continue;
                }
                Err(err) => return Err(err),
            }
            let created_at = pod
                .metadata
                .creation_timestamp
                .as_ref()
                .and_then(|t| OffsetDateTime::from_unix_timestamp_nanos(t.0.as_nanosecond()).ok())
                .unwrap_or_else(OffsetDateTime::now_utc);
            candidates.push(NodeInventoryEntry {
                id,
                name: Some(node_name.to_string()),
                address: Some(address),
                role,
                managed_by: SpaceNodeManagedBy::Kubernetes,
                state: Some(observed_state.as_str().to_string()),
                last_heartbeat: None,
                created_at,
                substrate_node_id: Some(node_name.to_string()),
            });
        }
        Ok(Some(dedupe_inventory_entries(candidates)))
    }
}

/// `create_execution`'s error when its outer readiness budget expires.
///
/// Neutral message: whether the work is preserved (pod left running, retry the
/// same sandbox) or torn down (pod never scheduled) is decided by the preserve
/// check after it, so don't promise either here. `Duration`'s `Debug` renders
/// a whole-second budget such as the production default as `135s`, and keeps
/// a test's sub-second budget legible.
pub(super) fn create_ready_timeout_error(create_ready: Duration) -> SubstrateError {
    SubstrateError::Timeout {
        message: format!("sandbox create did not reach readiness within {create_ready:?}"),
        phase: None,
    }
}
