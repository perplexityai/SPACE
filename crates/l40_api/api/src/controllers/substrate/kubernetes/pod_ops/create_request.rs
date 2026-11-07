//! Pod create request: node-group placement, the effective network policy,
//! the create context for a claim, and building the sandbox pod request.

use super::*;

impl KubernetesSubstrate {
    pub(super) async fn effective_network_policy_for_sandbox(
        &self,
        sandbox: &Sandbox,
    ) -> Result<pplx_space_net_common::NetworkPolicy, SubstrateError> {
        if let Some(policy) = sandbox.network_policy.as_ref() {
            return Ok(policy.0.clone());
        }
        let Some(template) = sandbox.template_id.as_deref() else {
            return Ok(pplx_space_net_common::NetworkPolicyV1::default());
        };
        Ok(self
            .store
            .template_network_policy(template)
            .await?
            .unwrap_or_default())
    }

    /// Resolve this pod build's node-group candidate set, and pre-check a
    /// local-only restore's source-node pin against it.
    ///
    /// The candidate set is recomputed from the *current* template visibility
    /// and the **sandbox's own** owning org at every build — never replayed
    /// from a snapshot taken at create, and never derived from the actor who
    /// triggered the work: a restore or an evacuation is usually driven by a
    /// controller loop or a managing-org admin, and feeding their org into the
    /// compatibility predicate would reject the owner org's own template as
    /// cross-org.
    /// `pub(super)` for the sibling test module, which drives it end to end —
    /// the pre-flight below is a data-isolation check whose wiring nothing else
    /// would catch if it stopped being called.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_node_group_placement(
        &self,
        sandbox: &Sandbox,
        context: &PodCreateContext<'_>,
    ) -> Result<ResolvedPlacement, SubstrateError> {
        let resolved = self
            .node_group_placement
            .resolve(
                context.node_group_resolve_path(),
                PlacementSubject::Sandbox(sandbox.org_id),
                sandbox_template_name(sandbox),
            )
            .await?;
        // A pre-bound pod skips kube-scheduler, so the boundary is evaluated
        // only by kubelet admission and a rejection there is terminal for the
        // restore. Pre-check the source node's live label so the failure is an
        // actionable "migrate this sandbox" instead of a pod that lands, takes
        // a network slot, and dies. The reflector store is the read, not a
        // `GET /nodes/{name}`: this is a request path, and the reflector holds
        // the apiserver's Node object — the same object whose labels kubelet
        // admission will evaluate, which the mirrored `nodes.node_group_id`
        // column is only a lagging copy of.
        if let (Some(placement), Some(node_name)) =
            (resolved.placement(), context.direct_node_name())
        {
            let node = self.k8s.caches.node_by_name(node_name);
            let source = match node.as_deref() {
                Some(node) => RestoreSourceNode::Resolved(node),
                None if self.k8s.caches.nodes_ready() => RestoreSourceNode::Absent,
                None => RestoreSourceNode::CacheCold,
            };
            check_local_restore_source(placement, sandbox.id, node_name, source)?;
        }
        Ok(resolved)
    }

    /// [`Self::resolve_node_group_placement`] with the claim discipline
    /// `pod_create_context_for_claim` uses: a claim this call is about to
    /// leave unmaterialized is retired before the error propagates, so the
    /// next ensure re-creates instead of stalling behind the create-in-flight
    /// early return until the stale-create age expires.
    pub(super) async fn resolve_node_group_placement_for_claim(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        context: &PodCreateContext<'_>,
    ) -> Result<ResolvedPlacement, SubstrateError> {
        let resolved = self.resolve_node_group_placement(sandbox, context).await;
        self.retire_unmaterialized_claim_on_error(sandbox, claim, resolved, "node-group placement")
            .await
    }

    /// [`Self::resolve_pod_environment`] for a pod build whose execution
    /// claim is already outstanding, which a failure retires as
    /// [`Self::resolve_node_group_placement_for_claim`] does.
    pub(super) async fn resolve_pod_environment_for_claim(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        context: &PodCreateContext<'_>,
    ) -> Result<Option<PodEnvironment>, SubstrateError> {
        let resolved = self.resolve_pod_environment(sandbox, context).await;
        self.retire_unmaterialized_claim_on_error(sandbox, claim, resolved, "the pod's environment")
            .await
    }

    /// Pass `resolved` through, retiring `claim` first when it failed and the
    /// create is still in flight.
    async fn retire_unmaterialized_claim_on_error<T>(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        resolved: Result<T, SubstrateError>,
        what: &'static str,
    ) -> Result<T, SubstrateError> {
        let Err(err) = resolved else {
            return resolved;
        };
        if execution_pod_create_in_flight(claim.row.state) {
            // The retirement's own failure must not replace `err`: the
            // caller needs to know why the *pod build* failed, and a store
            // error here only means the claim outlives this attempt (the
            // stale-create age then clears it). Log both, return the one that
            // explains the outcome.
            match self
                .store
                .abandon_created_unmaterialized_create_claim(sandbox.id, claim)
                .await
            {
                Ok(true) => tracing::warn!(
                    sandbox_id = %sandbox.id,
                    generation = claim.row.generation,
                    error = %err,
                    what,
                    "retired unmaterialized Kubernetes pod create claim after a pod \
                     placement input could not be resolved"
                ),
                Ok(false) => {}
                Err(retire_error) => tracing::warn!(
                    sandbox_id = %sandbox.id,
                    generation = claim.row.generation,
                    error = %err,
                    %retire_error,
                    what,
                    "could not retire the unmaterialized create claim after a pod placement \
                     input failed to resolve; the claim clears at the stale-create age"
                ),
            }
        }
        Err(err)
    }

    /// The environment this pod build's work is bound to, which the pod
    /// records while the environment placement gate evaluates and requires a
    /// node label for while it's on (`pod_environment`); `None` for unbound
    /// work, and while the gate is off.
    ///
    /// The work is what the pod will boot: the template build for an
    /// on-demand pod ([`Self::template_pod_environment`]), the checkpoint a
    /// suspended sandbox restores, and the execution a stopped sandbox last
    /// published for a disk restore, which cold boots it. Like the
    /// node-group placement, it's resolved from the current rows at every
    /// build.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_environment(
        &self,
        sandbox: &Sandbox,
        context: &PodCreateContext<'_>,
    ) -> Result<Option<PodEnvironment>, SubstrateError> {
        let gate = self.k8s.environment_placement;
        if !gate.evaluates() {
            return Ok(None);
        }
        let restore = match context {
            PodCreateContext::OnDemand => {
                return match sandbox.content_hash.as_deref() {
                    Some(content_hash) => {
                        self.template_pod_environment(sandbox_template_name(sandbox), content_hash)
                            .await
                    }
                    None => Ok(None),
                };
            }
            PodCreateContext::Restore(restore) => restore,
        };
        let (storage_uri, pinned_disk) = match restore {
            RestoreContext::UserRestore {
                suspend_restore_storage_uri,
                ..
            } => (*suspend_restore_storage_uri, None),
            RestoreContext::Evacuation {
                suspend_restore_storage_uri,
                stopped_materialization,
                ..
            } => (
                *suspend_restore_storage_uri,
                stopped_materialization.map(|materialization| materialization.artifact_id),
            ),
        };
        let value = match pinned_disk {
            Some(artifact_id) => {
                self.stopped_environment(sandbox.id, Some(artifact_id))
                    .await
            }
            None if sandbox.lifecycle.parse::<SandboxLifecycle>()
                == Ok(SandboxLifecycle::Stopped) =>
            {
                self.stopped_environment(sandbox.id, None).await
            }
            None => self
                .store
                .restore_checkpoint_execution(sandbox.id, storage_uri)
                .await
                .map(|recorded| recorded.and_then(|recorded| recorded.execution.environment)),
        };
        let value = places_as_off_in_shadow(gate, "the restore's", value)?;
        let environment =
            rendered_pod_environment(gate, format_args!("sandbox {}", sandbox.id), value)?;
        // A pod pre-bound to its source node skips kube-scheduler, so only
        // kubelet admission checks the label, and its refusal is terminal for
        // a restore whose bytes are on that node. Pre-check the source node's
        // live labels, as `resolve_node_group_placement` does its node group,
        // so the restore is refused before any pod exists, with the same 409
        // and the same way out: draining the node publishes the sandbox's
        // snapshot to remote storage, after which it restores on any node
        // that publishes the environment. A node the cache doesn't have keeps
        // the pin, with kubelet as the backstop.
        if let (Some(required), Some(node_name)) = (
            environment
                .as_ref()
                .filter(|environment| environment.required),
            context.direct_node_name(),
        ) && let Some(node) = self.k8s.caches.node_by_name(node_name)
            && !node.metadata.labels.as_ref().is_some_and(|labels| {
                pplx_space_sandbox_pod::node_publishes_environment(labels, &required.reference)
            })
        {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox {} can only restore from node {node_name}, which doesn't publish \
                     its environment {} while the environment placement gate is on; drain \
                     that node to publish this sandbox's snapshot to remote storage, after \
                     which it can restore on a node that publishes the environment",
                    sandbox.id, required.reference
                ),
            });
        }
        Ok(environment)
    }

    /// The environment a stopped sandbox's disk boots under: the one its
    /// disk artifact was produced under (a drain's re-placement names the
    /// pinned artifact `pinned`; otherwise it's the sandbox's current one),
    /// else the execution the sandbox last published. The disk's own record
    /// comes first because it outlives the node the sandbox stopped on, whose
    /// deletion removes the published execution with it.
    async fn stopped_environment(
        &self,
        sandbox_id: SandboxId,
        pinned: Option<LocalArtifactId>,
    ) -> Result<Option<String>, SubstrateError> {
        let disk = match pinned {
            Some(artifact_id) => {
                self.store
                    .disk_artifact_environment(sandbox_id, artifact_id)
                    .await?
            }
            None => {
                self.store
                    .current_disk_artifact_environment(sandbox_id)
                    .await?
            }
        };
        if let Some(environment) = disk {
            return Ok(Some(environment));
        }
        Ok(self
            .store
            .last_published_execution(sandbox_id)
            .await?
            .and_then(|recorded| recorded.execution.environment))
    }

    /// The environment a fork's pod renders under the gate: it boots the
    /// disk artifact of source snapshot `snapshot_id`, so it's bound to the
    /// environment that snapshot was produced under, not the template's.
    pub(in crate::controllers::substrate::kubernetes) async fn fork_pod_environment(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<PodEnvironment>, SubstrateError> {
        let gate = self.k8s.environment_placement;
        if !gate.evaluates() {
            return Ok(None);
        }
        let value = places_as_off_in_shadow(
            gate,
            "the fork source's",
            self.store.snapshot_environment(snapshot_id).await,
        )?;
        rendered_pod_environment(gate, format_args!("snapshot {snapshot_id}"), value)
    }

    /// The environment a pod booting `template` at `content_hash` renders
    /// under the gate: the build's binding
    /// (`environment_placement::work_environment`).
    pub(in crate::controllers::substrate::kubernetes) async fn template_pod_environment(
        &self,
        template: &str,
        content_hash: &str,
    ) -> Result<Option<PodEnvironment>, SubstrateError> {
        let gate = self.k8s.environment_placement;
        let value = self
            .store
            .work_environment(gate, template, content_hash)
            .await?;
        rendered_pod_environment(gate, format_args!("template {template}"), value)
    }

    /// `pub(super)` for the sibling test module, which renders each context's
    /// pod to assert the boundary actually reaches the PodSpec.
    pub(in crate::controllers::substrate::kubernetes) fn sandbox_pod_request<'a>(
        &self,
        sandbox: &'a Sandbox,
        execution: &'a SandboxExecutionRow,
        inputs: SandboxPodRequestInputs<'a>,
        pod_create_context: PodCreateContext<'a>,
    ) -> SandboxPodRequest<'a> {
        let SandboxPodRequestInputs {
            sandbox_owner,
            auto_lifecycle_json,
            network_policy_json,
            node_group_placement,
            environment,
            client_id,
        } = inputs;
        let disk_io_limits = u64::try_from(sandbox.disk_io_max_operations_per_sec)
            .ok()
            .zip(u64::try_from(sandbox.disk_io_max_bandwidth_mib_per_sec).ok())
            .and_then(|(operations, bandwidth)| {
                pplx_space_common_core::types::DiskIoLimits::try_new(operations, bandwidth).ok()
            })
            .unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    operations_per_sec = sandbox.disk_io_max_operations_per_sec,
                    bandwidth_mib_per_sec = sandbox.disk_io_max_bandwidth_mib_per_sec,
                    "invalid persisted sandbox disk I/O limits; using platform defaults"
                );
                pplx_space_common_core::types::DiskIoLimits::default()
            });
        let runtime = SandboxRuntimePodMetadata {
            sandbox_owner,
            user_id: sandbox.user_id,
            org_id: sandbox.org_id,
            client_id,
            auto_lifecycle_json,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
            network_policy_json,
            network_policy_generation: cold_pod_network_policy_generation(
                sandbox.network_policy.is_some(),
                u64::try_from(sandbox.network_policy_generation).unwrap_or(0),
            ),
            network_policy_authority: PolicyAuthority::Cluster,
            vcpu: sandbox_vcpu_option(sandbox),
            memory_mib: sandbox_memory_mib_option(sandbox),
            disk_mib: sandbox_disk_mib_option(sandbox),
            disk_io_limits,
            volume_encryption: sandbox.volume_encryption,
        };
        let common = SandboxPodCommon {
            pod_name: &execution.pod_name,
            sandbox_id: sandbox.id,
            execution_generation: execution.generation,
            activation_attempt_id: execution.bootstrap_route_activation_id(),
            template: sandbox_template_name(sandbox),
            content_hash: sandbox.content_hash.as_deref(),
            env: &sandbox.env.0,
            traceparent: pplx_space_observability::trace_propagation::current_traceparent(),
            environment,
        };

        // `node_group_placement` is resolved by the caller — the pod build is
        // synchronous and resolution is a database read — and is `None`
        // whenever the gate is off or the cluster has no generic node group,
        // which renders no node-group constraints at all. It is recomputed from
        // the current template visibility and the sandbox's owning org at every
        // build, never replayed from a snapshot taken at create time.
        match pod_create_context {
            // The reconcile/materialization path has no create request in hand,
            // and the pool pin is create-time-only (not persisted), so a pod
            // rebuilt here carries no pin.
            PodCreateContext::OnDemand => SandboxPodRequest::OnDemand {
                common,
                runtime,
                node_pool: None,
                node_group_placement,
            },
            PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_storage_uri,
                suspend_restore_source_node,
                direct_node_name,
                preferred_node_name,
            }) => {
                // Source-aware shortlist, computed at request-build time so
                // every path that renders a source preference gets it: the
                // source plus schedulable fallbacks, keeping the whole set
                // under kube-scheduler's prefilter floor so the weight-100
                // preferred source term is guaranteed to be scored. Only for
                // portable restores (an S3 fallback must exist for a fallback
                // node to restore from); the re-drive successor passes
                // `preferred_node_name: None` and stays shortlist-free.
                let restore_node_shortlist = match preferred_node_name {
                    Some(source)
                        if self.k8s.sandbox_pod_config.restore_source_node_hard_pin
                            && suspend_restore_storage_uri.is_some() =>
                    {
                        self.restore_node_shortlist(sandbox.id, source, node_group_placement)
                    }
                    _ => Vec::new(),
                };
                SandboxPodRequest::Restore {
                    stopped_materialization: None,
                    artifact_kind: if sandbox.lifecycle.parse::<SandboxLifecycle>()
                        == Ok(SandboxLifecycle::Stopped)
                    {
                        pplx_space_common_core::types::ArtifactKind::Disk
                    } else {
                        pplx_space_common_core::types::ArtifactKind::Full
                    },
                    common,
                    runtime,
                    // Rendered alongside `direct_node_name` when the restore is
                    // local-only. The pin stands: the caller has already
                    // pre-checked the source node's live label against this
                    // set, and dropping the pin is not available — the field is
                    // populated only when there is no portable snapshot, so any
                    // other node in the set has neither the local bytes nor a
                    // storage URI to restore from.
                    node_group_placement,
                    suspend_restore_storage_uri,
                    suspend_restore_source_node,
                    fork_disk_restore: None,
                    direct_node_name,
                    preferred_node_name,
                    restore_node_shortlist,
                    extra_tolerations: &[],
                    extra_required_affinity: None,
                    runtime_upgrade: None,
                }
            }
            PodCreateContext::Restore(RestoreContext::Evacuation {
                suspend_restore_storage_uri,
                extra_tolerations,
                extra_required_affinity,
                stopped_materialization,
            }) => SandboxPodRequest::Restore {
                // A stopped re-placement restores a disk artifact even though
                // its row reads `restoring` by now.
                artifact_kind: if stopped_materialization.is_some()
                    || sandbox.lifecycle.parse::<SandboxLifecycle>()
                        == Ok(SandboxLifecycle::Stopped)
                {
                    pplx_space_common_core::types::ArtifactKind::Disk
                } else {
                    pplx_space_common_core::types::ArtifactKind::Full
                },
                stopped_materialization,
                common,
                runtime,
                node_group_placement,
                suspend_restore_storage_uri,
                suspend_restore_source_node: None,
                fork_disk_restore: None,
                direct_node_name: None,
                preferred_node_name: None,
                restore_node_shortlist: Vec::new(),
                extra_tolerations,
                extra_required_affinity,
                runtime_upgrade: None,
            },
        }
    }

    pub(super) async fn pod_create_context_for_claim<'a>(
        &self,
        sandbox: &'a Sandbox,
        claim: &SandboxExecutionClaim,
        evacuation_context: Option<EvacuationContext<'a>>,
        suspend_restore_source_node: Option<&'a str>,
        suspend_restore_storage_uri: Option<&'a str>,
    ) -> Result<PodCreateContext<'a>, SubstrateError> {
        let sandbox_id = sandbox.id;
        match parse_pod_create_context(
            sandbox_id,
            claim.row.generation,
            &sandbox.lifecycle,
            self.store.prior_state_exists(sandbox_id).await?,
            evacuation_context,
            suspend_restore_source_node,
            suspend_restore_storage_uri,
        ) {
            Ok(context) => Ok(context),
            Err(err) => {
                if execution_pod_create_in_flight(claim.row.state) {
                    let retired = self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, claim)
                        .await?;
                    if retired {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            lifecycle = %sandbox.lifecycle,
                            "retired invalid Kubernetes pod create claim"
                        );
                    }
                }
                Err(err)
            }
        }
    }
}

/// [`crate::controllers::substrate::environment_placement::bound_pod_environment`],
/// with an unparseable binding failing the pod build.
fn rendered_pod_environment(
    gate: crate::controllers::substrate::environment_placement::EnvironmentPlacementGate,
    subject: impl std::fmt::Display,
    value: Option<String>,
) -> Result<Option<PodEnvironment>, SubstrateError> {
    crate::controllers::substrate::environment_placement::bound_pod_environment(
        gate, subject, value,
    )
    .map_err(|error| SubstrateError::Failed {
        message: error.to_string(),
    })
}

/// `read` of the environment `what` work records, as template work's read is
/// (`environment_placement::work_environment`): in shadow a failure is logged
/// and reads as unbound, so shadow places exactly as off; on, it fails the
/// pod build.
fn places_as_off_in_shadow<T>(
    gate: crate::controllers::substrate::environment_placement::EnvironmentPlacementGate,
    what: &str,
    read: Result<Option<T>, SubstrateError>,
) -> Result<Option<T>, SubstrateError> {
    match read {
        Err(error) if !gate.refuses() => {
            tracing::warn!(
                %error,
                "environment placement gate couldn't read {what} environment; shadow places as off"
            );
            Ok(None)
        }
        read => read,
    }
}
