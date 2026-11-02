//! Warm-pool adoption: pooled wake and route-identity requests, split-receipt
//! capability waits, placement re-admission, and per-sandbox setup applied to
//! an adopted pod.

use super::*;

pub(in crate::controllers::substrate::kubernetes) fn pooled_route_identity_adoption_request(
    claim: &PooledExecutionClaim,
) -> Result<pplx_space_sbx_mgr_client::proto::WakeRequest, ControllerError> {
    let pool_slot_id = pooled_route_activation_id(claim)?;
    pooled_wake_request(
        claim,
        pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(pool_slot_id),
        pplx_space_sbx_mgr_client::proto::WakeTargetState::Paused,
    )
}

pub(super) fn pooled_wake_request(
    claim: &PooledExecutionClaim,
    activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    target_state: pplx_space_sbx_mgr_client::proto::WakeTargetState,
) -> Result<pplx_space_sbx_mgr_client::proto::WakeRequest, ControllerError> {
    let pod_uid = claim.pod_uid.as_ref().ok_or_else(|| {
        ControllerError::internal("Kubernetes pooled claim is missing its pod UID route")
    })?;
    Ok(pplx_space_sbx_mgr_client::proto::WakeRequest {
        implicit_wake_source: 0,
        sandbox_id: claim.sandbox_id.to_string(),
        blocking: true,
        start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
            activation_auto_lifecycle: None,
            expected_execution_generation: None,
            operation_activation_attempt_id: None,
            vcpu: claim.vcpu,
            memory_mib: claim.memory_mib,
            netns_path: None,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            expected_local_artifact_id: None,
            route_identity: Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                sandbox_id: None,
                route_generation: Some(claim.execution_generation),
                current_node_id: claim.active_node_id.map(|node_id| node_id.to_string()),
                execution_generation: i64::try_from(claim.execution_generation).ok(),
                pod_uid: Some(pod_uid.to_string()),
                activation_attempt_id: Some(activation_attempt_id.to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes
                    as i32,
            }),
        }),
        // A ready pool member is Paused, so this adopts route identity without
        // resuming it. If the invariant is violated and the VM is already
        // Started, Wake preserves Started rather than pausing a live VM.
        target_state: target_state as i32,
        runtime_upgrade: None,
    })
}

pub(super) async fn wait_for_pooled_split_receipt_capability(
    node_api: &mut NodeApiClient,
    deadline: Instant,
) -> Result<(), ControllerError> {
    let mut last_error = "worker did not advertise split operation receipts".to_string();
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(ControllerError::service_unavailable(format!(
                "pooled sandbox operation-receipt capability was not ready before split activation: {last_error}"
            )));
        };
        match node_api
            .health_service_serving(
                pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                remaining.min(START_WAIT_CONNECT_TIMEOUT),
            )
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                last_error =
                    "worker node-api reported operation receipts as NOT_SERVING".to_string();
            }
            Err(error) => last_error = error.to_string(),
        }
        if tokio::time::timeout_at(deadline, tokio::time::sleep(START_WAIT_FAST_POLL_INTERVAL))
            .await
            .is_err()
        {
            return Err(ControllerError::service_unavailable(format!(
                "pooled sandbox operation-receipt capability was not ready before split activation: {last_error}"
            )));
        }
    }
}

pub(super) fn pooled_metadata_update_error_is_reclaimable(status: &tonic::Status) -> bool {
    status.code() == Code::DeadlineExceeded
        || pplx_space_common_grpc::is_temporary_tcp_failure(status)
}

impl KubernetesSubstrate {
    /// Whether an admitted warm pod's node is somewhere this sandbox may run,
    /// by the same rules `claim_pooled_execution` admits a claim under. Only
    /// the outcomes that claim path *adopts* answer `true`; every outcome it
    /// declines, refuses, or cannot decide answers `false`. Metrics stay with
    /// the ordinary claim path, which owns the placement-admission series.
    pub(super) async fn admitted_pool_pod_placement_admits(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        pod: Option<&SandboxPodView>,
    ) -> bool {
        let resolved = match self
            .node_group_placement
            .resolve(
                NodeGroupResolvePath::PoolClaim,
                PlacementSubject::Sandbox(request.actor.org_id),
                request.template,
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                tracing::info!(
                    template = request.template,
                    %error,
                    "could not resolve node-group placement for an admitted warm-pool slot"
                );
                return false;
            }
        };
        match resolved.placement() {
            None => match resolved {
                ResolvedPlacement::Unprovisioned => matches!(
                    pooled_pod_node_group(&self.k8s.caches, pod),
                    PooledPodNodeGroup::Unlabeled
                ),
                ResolvedPlacement::Disabled | ResolvedPlacement::Placed(_) => true,
            },
            Some(placement) => {
                placement.admits_shared_capacity()
                    && matches!(
                        pooled_pod_node_admission(&self.k8s.caches, placement, pod),
                        PooledPodAdmission::Admitted
                    )
            }
        }
    }
}

impl KubernetesSubstrate {
    pub(super) async fn apply_pooled_execution_auto_lifecycle_with_client(
        client: &mut RetryingWorkerApiClient<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        client
            .set_sandbox_auto_lifecycle_policy(
                pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyRequest {
                    sandbox_id: claim.sandbox_id.to_string(),
                    policy: Some(crate::controllers::auto_lifecycle::policy_to_proto(policy)),
                    authority: PolicyAuthority::Cluster.as_proto_i32(),
                },
                WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .await
            .map_err(|err| {
                ControllerError::service_unavailable(format!(
                    "pooled sandbox data-plane auto lifecycle policy update failed: {err}"
                ))
            })?;

        Ok(())
    }

    /// Apply every per-sandbox setup step to an already-booted warm-pool pod via
    /// gRPC, mirroring what a cold create bakes into the pod spec.
    ///
    /// This is the warm-pool consumer of [`PerSandboxSetup`]; the cold consumer
    /// is [`PerSandboxSetup::into_cold_pod_metadata`]. `setup` is consumed by an
    /// exhaustive destructure so a new setup field fails to compile here until it
    /// is handled — the structural guarantee that adoption can't silently skip a
    /// cold-path setup step.
    pub(super) async fn apply_adopted_per_sandbox_setup(
        &self,
        client: &mut NodeApiClient,
        claim: &PooledExecutionClaim,
        setup: PerSandboxSetup,
        network_policy_generation: u64,
    ) -> Result<(), ControllerError> {
        let mut client = RetryingWorkerApiClient::new(client);
        let sandbox_id = claim.sandbox_id;
        let PerSandboxSetup {
            sandbox_owner,
            user_id,
            org_id,
            process_env,
            auto_lifecycle,
            network_policy,
            node_fs,
            // Always false for adoption (pool members are plaintext by
            // construction and encrypted creates bypass the pool); nothing to
            // apply to the already-booted pod.
            volume_encryption: _,
            disk_io_limits: _,
        } = setup;
        // The pool pod's user_id/org_id annotations carry the pool's identity;
        // adoption conveys the adopter's identity through the owner update below
        // (the owner encodes the user). Those pod annotations are cold-boot
        // metadata and are not rewritten here. If they ever become load-bearing
        // post-adoption, patch them alongside the network-policy annotation.
        let _ = (user_id, org_id);
        // Node-fs permissions can't be applied from the API process (node-mgr is
        // only reachable via sbx-mgr). They are ensured eagerly by sbx-mgr's
        // shared resume path before the VM resumes (`resume_after_wake_lock`),
        // which every adoption wake funnels through. Listed here so a future
        // node-fs requirement is surfaced at this destructure. (Irrefutable
        // today; adding a `NodeFilesystemSetup` variant makes this a compile
        // error until handled.)
        let NodeFilesystemSetup::EnsureArtifactDirPermissions = node_fs;
        let policy_proto =
            super::super::super::override_request_policy(network_policy.override_policy.as_ref());

        // Step: sandbox metadata. Flip the sandbox from the pool's root owner
        // to the adopter and replace process env before wake.
        let env_count = process_env.as_map().len();
        let process_env = std::collections::HashMap::<String, String>::from(process_env);
        client
            .update_sandbox(
                pplx_space_sbx_mgr_client::proto::UpdateRequest {
                    sandbox_id: claim.sandbox_id.to_string(),
                    vcpu: None,
                    memory_mib: None,
                    pids_max: None,
                    expected_current_owner: Some(
                        pplx_space_common_core::types::ROOT_SANDBOX_OWNER
                            .as_str()
                            .to_string(),
                    ),
                    owner: Some(sandbox_owner.as_str().to_string()),
                    process_env,
                    replace_process_env: true,
                },
                WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.pool.update_sandbox_metadata.client",
                sandbox_id = %sandbox_id,
                env_count,
            ))
            .await
            .map_err(|err| {
                if err.code() == Code::FailedPrecondition {
                    ControllerError::conflict(format!(
                        "pooled sandbox metadata update failed: {err}"
                    ))
                } else if pooled_metadata_update_error_is_reclaimable(&err) {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox metadata update failed: {err}"
                    ))
                } else if err.code() == Code::Cancelled {
                    ControllerError::new(
                        axum::http::StatusCode::REQUEST_TIMEOUT,
                        format!("pooled sandbox metadata update failed: {err}"),
                    )
                } else {
                    ControllerError::internal(format!(
                        "pooled sandbox metadata update failed: {err}"
                    ))
                }
            })?;

        // Step: network policy. Apply the effective policy to sbx-mgr's local DB
        // before waking the sandbox.
        let NetworkPolicySetup {
            override_policy,
            effective_policy,
            authority,
        } = network_policy;
        // This closes the window where a sandbox
        // adopted with a non-default policy (e.g. allow_internet_access=false)
        // could wake up enforcing the pool's original default policy and rely on
        // the periodic reconciler to correct it asynchronously.
        //
        // Why sbx-mgr can target this sandbox as a non-pool network identity:
        // K8s pool pods are created by the containerd shim calling sbx-mgr
        // Create without setting pool_template, so pool_template = NULL in
        // sbx-mgr's SQLite from the start (unlike direct pool sandboxes, which
        // use AdoptPoolSandbox to clear a real pool_template marker). The
        // sbx-mgr reconcile-enqueue gate also requires netgtw_pid IS NOT NULL;
        // sync_to_gateway below verifies the live gateway and fails if it cannot
        // confirm the policy.
        //
        // sync_to_gateway: true makes sbx-mgr call refresh_network_policy on a
        // live net-gateway before returning, rather than enqueuing the background
        // reconciler. This closes the window entirely: the gateway confirms the
        // new policy before wake_sandbox is called. If the gateway is not live,
        // sbx-mgr returns Unavailable and the create path rolls back this claimed
        // pool pod instead of waking it with an unconfirmed policy.
        let inherit_template = override_policy.is_none();
        let set_policy_response = client
            .set_sandbox_network_policy(
                pplx_space_sbx_mgr_client::proto::SetNetworkPolicyRequest {
                    sandbox_id: claim.sandbox_id.to_string(),
                    policy: policy_proto,
                    inherit_template,
                    policy_generation: network_policy_generation,
                    authority: authority.as_proto_i32(),
                    sync_to_gateway: true,
                },
                WARM_POOL_SETUP_RPC_TIMEOUT,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.pool.set_network_policy.client",
                sandbox_id = %sandbox_id,
            ))
            .await
            .map_err(|err| {
                if err.code() == Code::FailedPrecondition {
                    ControllerError::conflict(format!(
                        "pooled sandbox network policy update conflicted: {err}"
                    ))
                } else if err.code() == Code::Unavailable {
                    ControllerError::service_unavailable(format!(
                        "pooled sandbox data-plane network policy update failed: {err}"
                    ))
                } else {
                    ControllerError::internal(format!(
                        "pooled sandbox network policy update failed: {err}"
                    ))
                }
            })?;

        let set_policy_response = set_policy_response.into_inner();

        // A successful sync response only proves *a* policy was confirmed on the
        // gateway — verify it's the one the API resolved for this request, not a
        // stale policy sbx-mgr resolved from its own (possibly lagging) template
        // cache or a skipped update. Compare the confirmed effective policy value
        // against the API-resolved effective policy; on mismatch, roll back the
        // claim rather than wake under the wrong policy.
        let confirmed_effective_policy = set_policy_response
            .effective_policy
            .as_ref()
            .map(pplx_space_net_common::NetworkPolicyV1::try_from)
            .transpose()
            .map_err(|err| {
                ControllerError::internal(format!(
                    "pooled sandbox returned an undecodable effective network policy: {err}"
                ))
            })?;
        if confirmed_effective_policy.as_ref() != Some(&effective_policy) {
            return Err(ControllerError::service_unavailable(
                "pooled sandbox gateway confirmed a network policy that differs from the \
                 request-resolved policy (sbx-mgr template cache may be stale); rolling back",
            ));
        }

        // Patch the pod annotation with the explicit override so that if sbx-mgr
        // ever restarts and re-reads the pod spec, the shim passes the correct
        // policy to sbx-mgr's Create.
        //
        // K8s pool pods have no network-policy annotation at creation time (the
        // pool pod builder uses SandboxPodRequest::Pooled which omits the runtime
        // annotation block). Only write the annotation for an explicit override;
        // leave it absent for template inheritance so a future shim Create sees nil
        // NetworkPolicy → sbx-mgr inherits the template default rather than locking
        // in the current template value as a per-sandbox override. This is
        // best-effort and fire-and-forget — sbx-mgr's DB already holds the correct
        // policy from the preceding set_sandbox_network_policy call.
        if let Some(override_policy) = override_policy.as_ref() {
            use kube::api::{Patch, PatchParams};
            match serde_json::to_string(override_policy) {
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %claim.pod_name,
                        %err,
                        "pool adoption: failed to serialize network policy override for pod annotation"
                    );
                }
                Ok(policy_json) if ensure_network_policy_annotation_size(&policy_json).is_err() => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %claim.pod_name,
                        "pool adoption: network policy override too large for pod annotation, skipping"
                    );
                }
                Ok(policy_json) => {
                    let patch = serde_json::json!({
                        "metadata": {
                            "annotations": {
                                SANDBOX_NETWORK_POLICY_ANNOTATION: policy_json,
                                SANDBOX_NETWORK_POLICY_GENERATION_ANNOTATION: network_policy_generation.to_string(),
                                SANDBOX_NETWORK_POLICY_AUTHORITY_ANNOTATION: authority.to_string(),
                            }
                        }
                    });
                    let pods: Api<Pod> =
                        Api::namespaced(self.k8s.client.clone(), &self.k8s.sandbox_namespace);
                    let pod_name = claim.pod_name.clone();
                    let sandbox_id_bg = sandbox_id;
                    tokio::spawn(async move {
                        if let Err(err) = pods
                            .patch(&pod_name, &PatchParams::default(), &Patch::Merge(&patch))
                            .await
                        {
                            tracing::warn!(
                                sandbox_id = %sandbox_id_bg,
                                pod_name = %pod_name,
                                %err,
                                "pool adoption: failed to update pod network policy annotation"
                            );
                        }
                    });
                }
            }
        }

        // Step: auto-lifecycle. Set before any wake so the idle policy is live
        // the moment the VM resumes.
        Self::apply_pooled_execution_auto_lifecycle_with_client(
            &mut client,
            claim,
            &auto_lifecycle,
        )
        .instrument(tracing::info_span!(
            "substrate.k8s.pool.set_auto_lifecycle_policy.client",
            sandbox_id = %sandbox_id,
        ))
        .await?;

        Ok(())
    }
}
