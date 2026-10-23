//! Suspend flow: tracked generation suspends, polling the node for the suspend
//! outcome, the published artifact, pod-deletion confirmation, and the suspend
//! handoff.

use super::*;

pub(super) async fn await_suspend_continuation<T, F>(
    task_tracker: &TaskTracker,
    continuation: F,
) -> Result<T, SubstrateError>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, SubstrateError>> + Send + 'static,
{
    task_tracker
        .spawn(continuation.in_current_span())
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("sandbox suspend task stopped unexpectedly: {error}"),
        })?
}

impl KubernetesSubstrate {
    /// Keep generation-bound suspend finalization alive after its request
    /// waiter disconnects. The tracked task also remains part of graceful
    /// service shutdown, matching the detached restore-leader path.
    pub(in crate::controllers::substrate) async fn suspend_execution_tracked(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        let substrate = self.clone();
        await_suspend_continuation(&self.task_tracker, async move {
            <KubernetesSubstrate as SubstrateAdapter>::suspend_execution(&substrate, sandbox_id)
                .await
        })
        .await
    }
}

/// How the node answered a generation-bound suspend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationSuspendOutcome {
    /// The node published this durable suspend artifact.
    Durable(PublishedLocalArtifact),
    /// A generation-fenced local delete covers the execution, so the node has
    /// no artifact to publish. The suspend may still have become durable
    /// before that delete; only the control plane's snapshot record can say.
    NodeDeleted,
}

/// A failed generation suspend, and whether the node provably never began it.
#[derive(Debug)]
pub(super) struct GenerationSuspendFailure {
    pub(super) error: SubstrateError,
    /// True only for a refusal the node's handler returns before it records
    /// the suspend, and only when no earlier call in this poll could have
    /// reached it. Every other failure may have happened after the node began.
    pub(super) node_not_begun: bool,
}

pub(super) async fn await_generation_suspend(
    client: &mut RuntimeClient,
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
    endpoint_addr: &str,
    deadline: Instant,
    durability_budget: Duration,
) -> Result<GenerationSuspendOutcome, GenerationSuspendFailure> {
    poll_generation_suspend(
        |request| {
            let mut client = client.clone();
            async move { client.prepare_teardown_suspend(request).await }
        },
        sandbox_id,
        execution_generation,
        endpoint_addr,
        deadline,
        durability_budget,
    )
    .await
}

pub(super) async fn poll_generation_suspend<F, Fut>(
    mut prepare: F,
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
    endpoint_addr: &str,
    deadline: Instant,
    // The budget `deadline` was derived from, named in the timeout error.
    durability_budget: Duration,
) -> Result<GenerationSuspendOutcome, GenerationSuspendFailure>
where
    F: FnMut(
        tonic::Request<pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest>,
    ) -> Fut,
    Fut: std::future::Future<
            Output = Result<
                tonic::Response<pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse>,
                tonic::Status,
            >,
        >,
{
    let poll_started_at = Instant::now();
    let poll_budget = deadline.saturating_duration_since(poll_started_at);
    let mut pending_responses = 0_u64;
    let mut reasonless_pending_responses = 0_u64;
    let mut retryable_errors = 0_u64;
    let mut last_pending_reason = None;
    let mut last_reasoned_pending_reason = None;
    let mut last_retryable_code = None;
    let mut pending_reason_logs = 0_u8;
    let mut last_logged_pending_reason = None;
    let mut next_pending_reason_log_at = poll_started_at;
    let mut refused_before_begin = false;
    let result = async {
        loop {
            let mut request = tonic::Request::new(
                pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_generation: execution_generation.get(),
                    action: pplx_space_sbx_mgr_client::proto::TeardownSuspendAction::BeginOrJoin as i32,
                },
            );
            request.metadata_mut().insert(
                SUSPEND_TRIGGER_METADATA,
                tonic::metadata::MetadataValue::from_static("user"),
            );
            let response = match tokio::time::timeout_at(
                deadline,
                prepare(request),
            )
            .await
            .map_err(|_| SubstrateError::Timeout {
                message: format!(
                    "generation-bound suspend for sandbox {sandbox_id} generation {execution_generation} did not become durable within {durability_budget:?}",
                ),
                phase: None,
            })? {
                Ok(response) => response.into_inner(),
                Err(error) if generation_suspend_rpc_should_retry(&error) => {
                    retryable_errors += 1;
                    last_retryable_code = Some(error.code());
                    tracing::debug!(
                        %sandbox_id,
                        %execution_generation,
                        node_api_endpoint = %endpoint_addr,
                        code = %error.code(),
                        error = %error,
                        "retrying generation-bound suspend observation after transient node-api failure"
                    );
                    tokio::time::sleep_until(
                        (Instant::now() + Duration::from_millis(250)).min(deadline),
                    )
                    .await;
                    continue;
                }
                Err(error) => {
                    // Each call in this poll that could have reached the node
                    // is counted above, so with none counted this is the first
                    // reply. The node-api forwards the call once, replaying it
                    // only after a failure that never reached the handler.
                    refused_before_begin = pending_responses == 0
                        && retryable_errors == 0
                        && generation_suspend_rpc_refused_before_begin(&error);
                    return Err(SubstrateError::Unavailable {
                        message: format!(
                            "node-api generation-bound suspend at {endpoint_addr} failed: {error}"
                        ),
                    });
                }
            };
            let outcome =
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::try_from(response.outcome)
                    .unwrap_or(pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Unspecified);
            match outcome {
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Durable => {
                    return published_generation_suspend_artifact(response, endpoint_addr)
                        .map(GenerationSuspendOutcome::Durable);
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Pending => {
                    pending_responses += 1;
                    // Keep bounded operator diagnostics out of client errors and
                    // SuspendFailed events. A reasonless tail describes the latest
                    // observation without erasing earlier reasoned Pending history.
                    last_pending_reason = response.reason.map(|reason| {
                        pplx_space_common_svc::bootstrap::truncate_utf8_for_log(
                            &reason,
                            MAX_LOGGED_SUSPEND_PENDING_REASON_BYTES,
                        ).value
                    });
                    if let Some(reason) = &last_pending_reason {
                        last_reasoned_pending_reason = Some(reason.clone());
                    } else {
                        reasonless_pending_responses += 1;
                    }
                    // Suppress ordinary reasonless waits. Once a reason appears,
                    // expose changes (including recovery to None) with both a
                    // cooldown and a hard cap, even if the node keeps oscillating.
                    let now = Instant::now();
                    if pending_reason_logs < MAX_SUSPEND_PENDING_REASON_LOGS
                        && now >= next_pending_reason_log_at
                        && (pending_reason_logs > 0 || last_pending_reason.is_some())
                        && (pending_reason_logs == 0
                            || last_pending_reason != last_logged_pending_reason)
                    {
                        tracing::info!(
                            %sandbox_id,
                            %execution_generation,
                            node_api_endpoint = endpoint_addr,
                            poll_elapsed_ms = duration_millis_to_u64(poll_started_at.elapsed()),
                            pending_responses,
                            ?last_pending_reason,
                            "generation-bound suspend pending observation changed"
                        );
                        pending_reason_logs += 1;
                        last_logged_pending_reason = last_pending_reason.clone();
                        next_pending_reason_log_at = now + SUSPEND_PENDING_REASON_LOG_INTERVAL;
                    }
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Superseded => {
                    return Err(SubstrateError::Unavailable {
                        message: response.reason.unwrap_or_else(|| {
                            format!(
                                "execution generation {execution_generation} no longer owns sandbox {sandbox_id}"
                            )
                        }),
                    });
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Deleted => {
                    return Ok(GenerationSuspendOutcome::NodeDeleted);
                }
                pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Unspecified => {
                    return Err(SubstrateError::Failed {
                        message: format!(
                            "node-api at {endpoint_addr} returned an invalid generation-bound suspend outcome"
                        ),
                    });
                }
            }
            tokio::time::sleep_until((Instant::now() + Duration::from_millis(250)).min(deadline)).await;
        }
    }
    .await;
    if let Err(error) = &result {
        // One summary for every unsuccessful exit, including terminal RPC and
        // artifact-validation failures; never one event per observation.
        tracing::warn!(
            %sandbox_id,
            %execution_generation,
            node_api_endpoint = endpoint_addr,
            poll_elapsed_ms = duration_millis_to_u64(poll_started_at.elapsed()),
            poll_budget_ms = duration_millis_to_u64(poll_budget),
            pending_responses,
            reasonless_pending_responses,
            retryable_errors,
            ?last_pending_reason,
            ?last_reasoned_pending_reason,
            ?last_retryable_code,
            %error,
            "generation-bound suspend polling failed"
        );
    }
    result.map_err(|error| GenerationSuspendFailure {
        error,
        node_not_begun: refused_before_begin,
    })
}

/// Statuses the node's `PrepareTeardownSuspend` handler returns only before
/// it records the suspend: request validation and peer authorization. The
/// handler's later failures (`Internal`, `NotFound`, `FailedPrecondition`)
/// can follow a suspend that already began, so they are not listed.
pub(in crate::controllers::substrate::kubernetes) fn generation_suspend_rpc_refused_before_begin(
    error: &tonic::Status,
) -> bool {
    matches!(
        error.code(),
        tonic::Code::InvalidArgument | tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
    )
}

pub(in crate::controllers::substrate::kubernetes) fn generation_suspend_rpc_should_retry(
    error: &tonic::Status,
) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    ) || pplx_space_common_grpc::is_temporary_tcp_failure(error)
}

pub(in crate::controllers::substrate::kubernetes) fn published_generation_suspend_artifact(
    response: pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse,
    endpoint_addr: &str,
) -> Result<PublishedLocalArtifact, SubstrateError> {
    let artifact = response
        .published_artifact
        .ok_or_else(|| SubstrateError::Failed {
            message: format!(
                "node-api at {endpoint_addr} reported durable generation suspend without an artifact"
            ),
        })?;
    let origin = pplx_space_sbx_mgr_client::proto::ArtifactOrigin::try_from(artifact.origin)
        .unwrap_or(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Unspecified);
    if origin != pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend {
        return Err(SubstrateError::Failed {
            message: format!(
                "node-api at {endpoint_addr} reported durable generation suspend with {origin:?} artifact origin"
            ),
        });
    }
    PublishedLocalArtifact::from_published(
        Some(artifact),
        "generation-bound suspend",
        ArtifactKind::Full,
    )
}

pub(in crate::controllers::substrate::kubernetes) fn suspend_pod_deletion_confirmation(
    outcome: K8sBlockingDeleteOutcome,
    pod_name: &str,
    timeout_secs: u64,
) -> Result<(), SubstrateError> {
    match outcome {
        K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound => Ok(()),
        K8sBlockingDeleteOutcome::TimedOut => Err(SubstrateError::Timeout {
            message: format!(
                "timed out after {timeout_secs}s waiting for Kubernetes sandbox pod {pod_name} to be absent after suspend"
            ),
            phase: None,
        }),
        K8sBlockingDeleteOutcome::ObserveError => Err(SubstrateError::Unavailable {
            message: format!(
                "could not confirm Kubernetes sandbox pod {pod_name} is absent after suspend"
            ),
        }),
        K8sBlockingDeleteOutcome::PreconditionFailed => Err(SubstrateError::Unavailable {
            message: format!("Kubernetes sandbox pod {pod_name} changed before suspend retirement"),
        }),
    }
}

impl KubernetesSubstrate {
    /// Everything `suspend_execution` does once it holds the suspend claim.
    ///
    /// A claim this call just made is undone when the suspend fails before
    /// the node was asked to begin: nothing node-side exists to finish, and
    /// leaving the sandbox `suspending` would make every retry join the same
    /// refusal. A failure after the node may have begun leaves the claim for
    /// a retry to join or the transient-lifecycle sweeper to settle.
    pub(in crate::controllers::substrate::kubernetes) async fn suspend_claimed_execution(
        &self,
        sandbox_id: SandboxId,
        execution: ActiveExecutionStatusRow,
        expected_generation: ExecutionGeneration,
        expected_pod_uid: PodUid,
        suspend_claim: SuspendExecutionClaim,
        suspend_deadline: Instant,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        let pod_uid = expected_pod_uid.clone();
        let mut node_may_have_begun = false;
        let outcome = self
            .contact_node_and_finish_suspend(
                sandbox_id,
                execution,
                expected_generation,
                expected_pod_uid,
                suspend_claim,
                suspend_deadline,
                &mut node_may_have_begun,
            )
            .await;
        if outcome.is_err()
            && !node_may_have_begun
            && let SuspendExecutionClaim::Claimed(restore) = suspend_claim
        {
            match self
                .store
                .revert_suspend_claim(sandbox_id, expected_generation, &pod_uid, restore)
                .await
            {
                Ok(reverted) => tracing::info!(
                    %sandbox_id,
                    generation = %expected_generation,
                    reverted,
                    "reverted the Kubernetes suspend claim after a refusal before the node began"
                ),
                Err(error) => tracing::warn!(
                    %sandbox_id,
                    generation = %expected_generation,
                    %error,
                    "failed to revert the Kubernetes suspend claim after a refusal before the node began"
                ),
            }
        }
        outcome
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the suspend steps share one claimed execution and the begun flag"
    )]
    async fn contact_node_and_finish_suspend(
        &self,
        sandbox_id: SandboxId,
        execution: ActiveExecutionStatusRow,
        expected_generation: ExecutionGeneration,
        expected_pod_uid: PodUid,
        suspend_claim: SuspendExecutionClaim,
        suspend_deadline: Instant,
        node_may_have_begun: &mut bool,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        let committed_suspend_artifact = match suspend_claim {
            SuspendExecutionClaim::Completed(artifact) => Some(artifact),
            SuspendExecutionClaim::Claimed(_) | SuspendExecutionClaim::Existing => {
                self.store
                    .committed_suspend_artifact(sandbox_id, expected_generation, &expected_pod_uid)
                    .await?
            }
        };
        let node_suspend_artifact = if committed_suspend_artifact.is_some() {
            // A committed artifact means a suspend already reached the node, so
            // a failure while retiring the pod must keep the claim.
            *node_may_have_begun = true;
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
            *node_may_have_begun = true;
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
                Err(failure) => {
                    *node_may_have_begun = !failure.node_not_begun;
                    Err(failure.error)
                }
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
}

impl KubernetesSubstrate {
    pub(in crate::controllers::substrate::kubernetes) async fn finish_suspend_handoff(
        &self,
        sandbox_id: SandboxId,
        expected_generation: ExecutionGeneration,
        expected_pod_uid: PodUid,
        pod_name: String,
        node_suspend_artifact: Option<PublishedLocalArtifact>,
    ) -> Result<PublishedLocalArtifact, SubstrateError> {
        let persisted_suspend_artifact = self
            .store
            .committed_suspend_artifact(sandbox_id, expected_generation, &expected_pod_uid)
            .await?;
        let published_artifact = match (persisted_suspend_artifact, node_suspend_artifact) {
            (Some(persisted), Some(from_node)) if persisted != from_node => {
                return Err(SubstrateError::Unavailable {
                    message: "Kubernetes suspend artifact changed before pod retirement"
                        .to_string(),
                });
            }
            (Some(persisted), _) => persisted,
            (None, Some(from_node)) => {
                self.store
                    .persist_or_accept_suspend_commit(
                        sandbox_id,
                        expected_generation,
                        &expected_pod_uid,
                        from_node,
                    )
                    .await?;
                from_node
            }
            (None, None) => {
                return Err(SubstrateError::Unavailable {
                    message: "Kubernetes suspend commit was not persisted".to_string(),
                });
            }
        };

        match self
            .k8s_blocking_delete_sandbox_pod_if_uid(&pod_name, &expected_pod_uid)
            .await
        {
            Ok(K8sBlockingDeleteOutcome::Deleted) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    "deleted Kubernetes sandbox pod after suspend"
                );
            }
            Ok(K8sBlockingDeleteOutcome::NotFound) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    "Kubernetes sandbox pod was already gone after suspend"
                );
            }
            Ok(outcome) => {
                let error = suspend_pod_deletion_confirmation(
                    outcome,
                    &pod_name,
                    self.k8s.k8s_blocking_delete_timeout.as_secs(),
                )
                .expect_err("unconfirmed pod deletion cannot finalize suspend");
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    delete_outcome = outcome.as_str(),
                    error = %error,
                    "pod deletion is unconfirmed after suspend; keeping the active execution"
                );
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::SuspendFailed,
                    SandboxEventSource::Substrate,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .pod_name(pod_name.clone())
                        .error(error.to_string())
                        .logged_by_caller(),
                );
                return Err(error);
            }
            Err(err) => {
                SandboxEvent::new(
                    sandbox_id,
                    SandboxEventType::SuspendFailed,
                    SandboxEventSource::Substrate,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .pod_name(pod_name.clone())
                        .error(err.to_string()),
                );
                return Err(SubstrateError::Failed {
                    message: format!(
                        "failed to delete suspended sandbox pod before marking suspended: {err}"
                    ),
                });
            }
        }

        self.store
            .finish_suspended_execution(
                sandbox_id,
                expected_generation,
                &expected_pod_uid,
                published_artifact,
            )
            .await?;

        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::SuspendCompleted,
            SandboxEventSource::Substrate,
        )
        .emit_with(SandboxEventDetails::default().pod_name(pod_name));

        Ok(published_artifact)
    }
}
