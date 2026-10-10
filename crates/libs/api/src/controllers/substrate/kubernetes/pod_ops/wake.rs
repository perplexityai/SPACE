//! Selected-execution wake: the wake pod IP and binding, the activation wake
//! action, and its dispatch status.

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) struct SelectedWakeOutcome {
    pub(in crate::controllers::substrate::kubernetes) dispatch: WakeDispatchStatus,
    pub(in crate::controllers::substrate::kubernetes) landed: bool,
    pub(in crate::controllers::substrate::kubernetes) pod_ip: Option<PodIp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum WakeDispatchStatus {
    NotDispatched,
    MayHaveLanded,
}

impl WakeDispatchStatus {
    pub(in crate::controllers::substrate::kubernetes) const fn was_dispatched(self) -> bool {
        matches!(self, Self::MayHaveLanded)
    }

    /// An operation owner may be released after failure only when no node-side
    /// work or delayed completion receipt can exist.
    pub(in crate::controllers::substrate::kubernetes) const fn operation_release_is_safe(
        self,
    ) -> bool {
        matches!(self, Self::NotDispatched)
    }
}

#[derive(Debug)]
pub(in crate::controllers::substrate::kubernetes) struct ActivationWakeFailure {
    pub(in crate::controllers::substrate::kubernetes) error: SubstrateError,
    pub(in crate::controllers::substrate::kubernetes) dispatch: WakeDispatchStatus,
}

#[derive(Clone, Copy, Debug)]
struct SelectedWakeSettings {
    policy: WakeWaitPolicy,
    require_split_receipt_capability: bool,
    phase_deadline: Instant,
}

impl ActivationWakeFailure {
    pub(in crate::controllers::substrate::kubernetes) const fn was_dispatched(&self) -> bool {
        self.dispatch.was_dispatched()
    }

    pub(in crate::controllers::substrate::kubernetes) const fn operation_release_is_safe(
        &self,
    ) -> bool {
        self.dispatch.operation_release_is_safe()
    }

    pub(in crate::controllers::substrate::kubernetes) fn into_error(self) -> SubstrateError {
        self.error
    }
}

impl KubernetesSubstrate {
    /// Resolve the dial address from an authoritative Kubernetes Pod GET, not
    /// from the execution row's cached IP. The complete route and immutable
    /// Pod UID fence both the lookup and the persistence CAS so a stale cached
    /// address or same-name replacement cannot receive node-api credentials.
    pub(in crate::controllers::substrate::kubernetes) async fn selected_wake_pod_ip(
        &self,
        sandbox_id: SandboxId,
        expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected execution disappeared while waiting for Wake".to_string(),
            })?;
        let mut selected = k8s_execution_route_identity(&execution)?;
        // A previously unresolved node may be filled by placement publication.
        // This matches the Wake response's runtime-authoritative node contract.
        if expected.current_node_id.is_none() {
            selected.current_node_id = None;
        }
        if selected != *expected {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed while waiting for Wake; retry against the current execution".to_string(),
            });
        }
        let pod_uid = execution
            .pod_uid
            .as_ref()
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected Kubernetes route lost its Pod UID".to_string(),
            })?;
        let route_activation_id = execution
            .route_activation_id
            .or(execution.activation_attempt_id)
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected Kubernetes route lost its activation identity".to_string(),
            })?;
        let pod = fetch_sandbox_pod_uncached(&self.k8s, &execution.pod_name)
            .await
            .map_err(|error| {
                kube_error_for_substrate("failed to resolve selected Wake pod", error)
            })?;
        let Some(pod) = pod else {
            return Ok(None);
        };
        let live_pod_uid = pod
            .uid()
            .and_then(|uid| PodUid::parse(uid).ok())
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected Wake pod has no valid Kubernetes UID".to_string(),
            })?;
        if live_pod_uid != *pod_uid {
            return Err(SubstrateError::Unavailable {
                message: "selected Wake pod UID changed; refusing to dial a replacement pod"
                    .to_string(),
            });
        }
        if pod.is_deleting() || terminal_pod_phase(&pod).is_some() {
            // Never dial a stopped Pod, including while node-local suspend
            // finishes its backup. Only the exact selected execution may ask
            // its recovery owner to wait for that handoff and reselect.
            let generation = ExecutionGeneration::new(execution.generation).map_err(|error| {
                SubstrateError::Failed {
                    message: format!("invalid selected Wake execution generation: {error}"),
                }
            })?;
            if self
                .store
                .local_suspend_backup_pending(sandbox_id, generation, pod_uid)
                .await?
            {
                return Err(SubstrateError::suspend_in_flight(sandbox_id));
            }
            return Err(SubstrateError::Unavailable {
                message: "selected Wake pod is terminating or terminal; retry execution recovery"
                    .to_string(),
            });
        }
        let Some(pod_ip) = assigned_pod_ip(&pod)? else {
            return Ok(None);
        };
        let live_pod_ip = pod_ip.to_string();
        if execution.pod_ip.as_deref() != Some(live_pod_ip.as_str())
            && !self
                .store
                .persist_selected_route_pod_ip(
                    sandbox_id,
                    execution.generation,
                    &execution.pod_name,
                    route_activation_id,
                    pod_uid,
                    pod_ip,
                )
                .await?
        {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed before its live pod IP could be persisted"
                    .to_string(),
            });
        }

        self.ensure_selected_wake_route_current(sandbox_id, expected, pod_ip)
            .await?;
        Ok(Some(pod_ip))
    }

    /// Re-fence cached Pod evidence before another side-effecting Wake. This
    /// is deliberately store-only: retrying against the same established
    /// connection does not need another uncached Kubernetes GET, but it must
    /// still stop if a concurrent operation superseded the selected route.
    async fn ensure_selected_wake_route_current(
        &self,
        sandbox_id: SandboxId,
        expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        pod_ip: PodIp,
    ) -> Result<(), SubstrateError> {
        let current = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected execution disappeared before Wake".to_string(),
            })?;
        let mut current_route = k8s_execution_route_identity(&current)?;
        if expected.current_node_id.is_none() {
            current_route.current_node_id = None;
        }
        let selected_pod_ip = pod_ip.to_string();
        if current_route != *expected || current.pod_ip.as_deref() != Some(selected_pod_ip.as_str())
        {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed before Wake; refusing the stale pod address"
                    .to_string(),
            });
        }
        Ok(())
    }

    /// Re-fence request-local Pod evidence before publishing readiness. This
    /// is intentionally a store-only check: the authoritative Kubernetes GET
    /// and Pod-UID check happened before the authenticated connection, while
    /// this read prevents a concurrent route replacement from producing a
    /// binding that mixes the predecessor's endpoint with the successor's
    /// generation and Pod name.
    pub(in crate::controllers::substrate::kubernetes) async fn selected_wake_binding(
        &self,
        sandbox_id: SandboxId,
        expected: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        pod_ip: PodIp,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let current = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "selected execution disappeared before readiness publication".to_string(),
            })?;
        let mut current_route = k8s_execution_route_identity(&current)?;
        if expected.current_node_id.is_none() {
            current_route.current_node_id = None;
        }
        let selected_pod_ip = pod_ip.to_string();
        if current_route != *expected || current.pod_ip.as_deref() != Some(selected_pod_ip.as_str())
        {
            return Err(SubstrateError::Unavailable {
                message: "selected route changed before readiness publication; retry against the current execution".to_string(),
            });
        }
        Ok(ExecutionBinding {
            execution_object_id: current.pod_name,
            runtime_endpoint: Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)?,
            execution_generation: execution_generation_to_u64(current.generation)?,
        })
    }

    /// Build a binding for a fence whose lifecycle operation already
    /// completed. A complete route keeps the live Pod/UID/store fences; a
    /// pre-route legacy row uses the same resolver fallback that existed
    /// before route identities were introduced.
    pub(in crate::controllers::substrate::kubernetes) async fn completed_execution_binding(
        &self,
        sandbox_id: SandboxId,
        execution: &ActiveExecutionStatusRow,
        route_identity: Option<&pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        if let Some(route_identity) = route_identity {
            let pod_ip = self
                .selected_wake_pod_ip(sandbox_id, route_identity)
                .await?
                .ok_or_else(|| SubstrateError::Unavailable {
                    message: "completed execution has no live Kubernetes pod IP".to_string(),
                })?;
            return self
                .selected_wake_binding(sandbox_id, route_identity, pod_ip)
                .await;
        }

        let pod_ip = match execution.pod_ip.as_deref() {
            Some(pod_ip) => PodIp::parse(pod_ip)?,
            None => self.resolve_pod_ip(sandbox_id).await?.ok_or_else(|| {
                SubstrateError::Unavailable {
                    message: "completed legacy execution has no pod IP".to_string(),
                }
            })?,
        };
        Ok(ExecutionBinding {
            execution_object_id: execution.pod_name.clone(),
            runtime_endpoint: Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)?,
            execution_generation: execution_generation_to_u64(execution.generation)?,
        })
    }

    /// Execute one already-classified activation action. Keeping this match in
    /// one place prevents individual lifecycle entry points from collapsing a
    /// completed fence into a joined operation or applying the split-receipt
    /// capability to B/B initialization.
    pub(in crate::controllers::substrate::kubernetes) async fn execute_activation_wake_action<
        F,
        D,
    >(
        &self,
        sandbox_id: SandboxId,
        action: ActivationWakeAction,
        phase_deadline: Instant,
        build_request: F,
        on_dispatch: D,
    ) -> Result<
        (
            SelectedWakeOutcome,
            Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
        ),
        ActivationWakeFailure,
    >
    where
        F: FnOnce(
            &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
            pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy,
        ) -> pplx_space_sbx_mgr_client::proto::WakeRequest,
        D: FnOnce(),
    {
        match action {
            ActivationWakeAction::SkipCompleted { route_identity } => Ok((
                SelectedWakeOutcome {
                    dispatch: WakeDispatchStatus::NotDispatched,
                    landed: false,
                    pod_ip: None,
                },
                route_identity,
            )),
            ActivationWakeAction::Execute {
                execution_generation,
                route_identity,
                policy,
                require_split_receipt_capability,
                publish_legacy_route_after_ack,
            } => {
                let activation_policy = self
                    .store
                    .auto_lifecycle_policy_after_claim(sandbox_id)
                    .await
                    .map_err(|error| ActivationWakeFailure {
                        error,
                        dispatch: WakeDispatchStatus::NotDispatched,
                    })?;
                let request = build_request(&route_identity, activation_policy.into());
                let outcome = self
                    .wake_selected_execution(
                        sandbox_id,
                        request,
                        &route_identity,
                        SelectedWakeSettings {
                            policy,
                            require_split_receipt_capability,
                            phase_deadline,
                        },
                        on_dispatch,
                    )
                    .await?;
                if let Some(route_activation_id) = publish_legacy_route_after_ack {
                    let expected_pod_uid =
                        route_identity
                            .pod_uid
                            .as_deref()
                            .ok_or_else(|| ActivationWakeFailure {
                                error: SubstrateError::Failed {
                                    message: "acknowledged legacy route has no Pod UID".to_string(),
                                },
                                dispatch: WakeDispatchStatus::MayHaveLanded,
                            })?;
                    self.store
                        .publish_legacy_route_identity_after_ack(
                            sandbox_id,
                            execution_generation,
                            route_activation_id,
                            expected_pod_uid,
                        )
                        .await
                        .map_err(|error| ActivationWakeFailure {
                            error,
                            dispatch: WakeDispatchStatus::MayHaveLanded,
                        })?;
                }
                Ok((outcome, Some(route_identity)))
            }
        }
    }

    /// Retry connection establishment and recoverable Wake results through
    /// the caller's readiness deadline. A pod can be materialized before its
    /// proxy or sandbox-manager runtime context is ready; neither condition is
    /// evidence that route-identity acknowledgement failed terminally.
    async fn wake_selected_execution<D>(
        &self,
        sandbox_id: SandboxId,
        request: pplx_space_sbx_mgr_client::proto::WakeRequest,
        expected_route_identity: &pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        settings: SelectedWakeSettings,
        on_dispatch: D,
    ) -> Result<SelectedWakeOutcome, ActivationWakeFailure>
    where
        D: FnOnce(),
    {
        let SelectedWakeSettings {
            policy,
            require_split_receipt_capability,
            phase_deadline,
        } = settings;
        let probe = policy == WakeWaitPolicy::ResidentProbe;
        let deadline = if probe {
            phase_deadline.min(Instant::now() + RESIDENT_WAKE_PROBE_TIMEOUT)
        } else {
            phase_deadline
        };
        let mut last_pod_ip = None;
        let mut wake_may_have_landed = false;
        let mut last_pre_dispatch_error = None;
        let mut on_dispatch = Some(on_dispatch);
        let wait = async {
            let started_at = Instant::now();
            let mut selected_pod_ip = None;
            let mut connection: Option<(
                PodIp,
                crate::controllers::substrate::RuntimeInternalClient,
            )> = None;
            loop {
                let reused_selection = selected_pod_ip.is_some();
                let mut waiting_for_pod_ip = false;
                if selected_pod_ip.is_none() {
                    match self
                        .selected_wake_pod_ip(sandbox_id, expected_route_identity)
                        .await
                    {
                        Ok(pod_ip) => {
                            selected_pod_ip = pod_ip;
                            waiting_for_pod_ip = selected_pod_ip.is_none();
                        }
                        Err(SubstrateError::Timeout { message, .. }) => {
                            last_pre_dispatch_error = Some(message);
                            waiting_for_pod_ip = true;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let Some(pod_ip) = selected_pod_ip {
                    if reused_selection {
                        self.ensure_selected_wake_route_current(
                            sandbox_id,
                            expected_route_identity,
                            pod_ip,
                        )
                        .await?;
                    }
                    last_pod_ip = Some(pod_ip);
                    if connection
                        .as_ref()
                        .is_none_or(|(connected_ip, _)| *connected_ip != pod_ip)
                    {
                        connection = match Self::pod_worker_internal_client(
                            pod_ip,
                            self.k8s.sandbox_proxy_port,
                            START_WAIT_CONNECT_TIMEOUT,
                            require_split_receipt_capability,
                        )
                        .await
                        {
                            Ok(Some(client)) => {
                                last_pre_dispatch_error = None;
                                Some((pod_ip, client))
                            }
                            Ok(None) => {
                                last_pre_dispatch_error =
                                    Some("worker node-api TCP connection is not ready".to_string());
                                // A failed connection is the point at which a
                                // once-live address may have become stale.
                                selected_pod_ip = None;
                                waiting_for_pod_ip = true;
                                None
                            }
                            Err(error) if probe => {
                                tracing::warn!(
                                    sandbox_id = %sandbox_id,
                                    %error,
                                    "resident Wake skipped because required worker capabilities could not be verified"
                                );
                                return Ok(SelectedWakeOutcome {
                                    dispatch: WakeDispatchStatus::NotDispatched,
                                    landed: false,
                                    pod_ip: Some(pod_ip),
                                });
                            }
                            Err(error) => {
                                // The capability probe's channel is discarded,
                                // so the next attempt will reconnect. Re-run
                                // the live Pod UID/IP fence before sending
                                // internal credentials to that new channel.
                                last_pre_dispatch_error = Some(error.to_string());
                                selected_pod_ip = None;
                                waiting_for_pod_ip = true;
                                None
                            }
                        };
                    }
                    if let Some((_, client)) = connection.as_mut() {
                        // Once the RPC future is polled, transport failure does
                        // not prove the server skipped the request. Preserve
                        // durable lifecycle/operation evidence on deadline.
                        if let Some(on_dispatch) = on_dispatch.take() {
                            on_dispatch();
                        }
                        wake_may_have_landed = true;
                        match client.wake_sandbox(request.clone()).await {
                            Ok(response) => {
                                if let Err(error) = validate_wake_route_identity_if_present(
                                    response.get_ref(),
                                    &request.sandbox_id,
                                    expected_route_identity,
                                    policy.requires_route_identity(),
                                ) {
                                    // The server returned a successful Wake,
                                    // so lifecycle work may already be live
                                    // even though its route evidence is unusable.
                                    // Preserve the restoring/operation owner for
                                    // reconciliation instead of reporting a
                                    // clean pre-dispatch failure that callers
                                    // are allowed to roll back.
                                    return Err(SubstrateError::Timeout {
                                        message: format!(
                                            "Wake completed without usable route identity evidence: {error}"
                                        ),
                                        phase: None,
                                    });
                                }
                                return Ok(SelectedWakeOutcome {
                                    dispatch: WakeDispatchStatus::MayHaveLanded,
                                    landed: true,
                                    pod_ip: Some(pod_ip),
                                });
                            }
                            Err(error) => match classify_wake_rpc_error(&error) {
                                WakeRpcErrorDisposition::ReadinessPoll(_) if probe => {
                                    return Ok(SelectedWakeOutcome {
                                        dispatch: WakeDispatchStatus::MayHaveLanded,
                                        landed: false,
                                        pod_ip: Some(pod_ip),
                                    });
                                }
                                WakeRpcErrorDisposition::ReadinessPoll(_) => {}
                                WakeRpcErrorDisposition::Fail => {
                                    return Err(SubstrateError::Conflict {
                                        message: format!("sandbox wake failed: {error}"),
                                    });
                                }
                            },
                        }
                    }
                }

                if probe {
                    return Ok(SelectedWakeOutcome {
                        dispatch: WakeDispatchStatus::NotDispatched,
                        landed: false,
                        pod_ip: None,
                    });
                }
                // An authoritative GET is intentionally required before
                // credentials are sent, but an unassigned Pod IP does not
                // justify polling kube-apiserver four times per second.
                let delay = if waiting_for_pod_ip {
                    START_WAIT_SLOW_POLL_INTERVAL
                } else {
                    start_wait_poll_interval(started_at)
                };
                tokio::time::sleep(delay).await;
            }
        };
        // Bound selection, connect, RPC, and retries together. An optional
        // resident probe may time out, but an explicit rejection never falls back.
        let result = match tokio::time::timeout_at(deadline, wait).await {
            Ok(result) => result,
            Err(_) if probe => Ok(SelectedWakeOutcome {
                dispatch: if wake_may_have_landed {
                    WakeDispatchStatus::MayHaveLanded
                } else {
                    WakeDispatchStatus::NotDispatched
                },
                landed: false,
                pod_ip: last_pod_ip,
            }),
            Err(_) if wake_may_have_landed => Err(SubstrateError::Timeout {
                message: "required Wake did not complete within the readiness budget; sandbox-manager may still be completing the same activation".to_string(),
                phase: None,
            }),
            Err(_) => Err(SubstrateError::Unavailable {
                message: last_pre_dispatch_error.unwrap_or_else(|| {
                    "required Wake was not dispatched before the readiness deadline".to_string()
                }),
            }),
        };
        result.map_err(|error| ActivationWakeFailure {
            error,
            dispatch: if wake_may_have_landed {
                WakeDispatchStatus::MayHaveLanded
            } else {
                WakeDispatchStatus::NotDispatched
            },
        })
    }
}
