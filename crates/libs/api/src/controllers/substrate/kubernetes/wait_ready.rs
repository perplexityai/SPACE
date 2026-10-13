use super::store::PlacementRetryDisposition;
use super::support::*;
use super::*;
use pplx_space_common_svc::space_api_route::wait_ready_lifecycle_request;

#[derive(Clone, Copy)]
pub(super) enum CommandReadyPod<'a> {
    PreResolved(PodIp),
    /// Preserve the pre-route legacy behavior: resolve the active execution on
    /// each retry without claiming that guest readiness proves route identity.
    ResolveLegacy,
    SelectedRoute {
        route_identity: &'a pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        pod_ip: Option<PodIp>,
    },
}

impl KubernetesSubstrate {
    pub(super) async fn worker_client_for_pod_ip(
        &self,
        pod_ip: PodIp,
    ) -> Result<RuntimeClient, ControllerError> {
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        connect_worker(endpoint).await
    }

    pub(super) fn should_allow_access_recovery(
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
    ) -> Result<(), ControllerError> {
        if is_dormant_lifecycle(&sandbox.lifecycle)
            && !activity.allows_implicit_wake(&sandbox.auto_lifecycle.0)
        {
            return Err(ControllerError::new(
                axum::http::StatusCode::CONFLICT,
                "sandbox is not running",
            ));
        }

        if !is_wake_recovery_lifecycle(&sandbox.lifecycle) {
            return Err(ControllerError::service_unavailable(format!(
                "sandbox cannot be recovered while lifecycle is {}",
                sandbox.lifecycle
            )));
        }

        Ok(())
    }

    async fn watch_until_pod_ip_assigned(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let pods: Api<Pod> = Api::namespaced(self.k8s.client.clone(), &self.k8s.sandbox_namespace);
        let watch_params = WatchParams::default()
            .fields(&format!("metadata.name={pod_name}"))
            .timeout(POD_IP_WATCH_TIMEOUT_SECS);
        let stream =
            pods.watch(&watch_params, "0")
                .await
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to watch K8s pod: {err}"),
                })?;
        pin_mut!(stream);

        loop {
            let event = match stream.try_next().await {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(None),
                Err(err) => {
                    return Err(SubstrateError::Failed {
                        message: format!("failed to watch K8s pod event: {err}"),
                    });
                }
            };

            let pod = match event {
                WatchEvent::Added(pod) | WatchEvent::Modified(pod) => SandboxPodView::from(pod),
                WatchEvent::Deleted(_) => return Ok(None),
                WatchEvent::Bookmark(_) => continue,
                WatchEvent::Error(status) => {
                    let message = if status.message.is_empty() {
                        "unknown error".to_string()
                    } else {
                        status.message
                    };
                    return Err(SubstrateError::Failed {
                        message: format!("K8s pod watch error: {message}"),
                    });
                }
            };

            if let Some(message) = pod.unschedulable_message() {
                return Err(SubstrateError::Unavailable {
                    message: format!("sandbox pod is unschedulable: {message}"),
                });
            }

            let pod_uid = pod.uid().map(str::to_owned);
            let pod_ip = assigned_pod_ip(&pod)?;
            let _ = self
                .remember_pod_node_placement(
                    sandbox_uuid,
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?;
            if pod_ip.is_none() {
                continue;
            }

            let pod_ip_string = pod_ip.map(|ip| ip.to_string());
            match self
                .store
                .persist_pod_metadata(
                    sandbox_uuid,
                    generation,
                    pod_uid.as_deref(),
                    pod_ip_string.as_deref(),
                )
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation,
                    "pod metadata write no-oped after pod IP assignment: the execution \
                     was retired or replaced mid-flight"
                ),
                Err(err) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation,
                    error = %err,
                    "failed to persist Kubernetes pod metadata after pod IP assignment"
                ),
            }

            return Ok(pod_ip);
        }
    }

    async fn poll_until_pod_ip_assigned(
        &self,
        sandbox_id: SandboxId,
        deadline: Instant,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let started_at = Instant::now();
        loop {
            if let Some(pod_ip) = self.resolve_pod_ip(sandbox_id).await? {
                return Ok(Some(pod_ip));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            sleep(start_wait_poll_interval(started_at)).await;
        }
    }

    async fn wait_for_pod_ip_assignment(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
        deadline: Instant,
        phase: &'static str,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let started_at = Instant::now();
        if let Some(pod_ip) = self.resolve_pod_ip(sandbox_id).await? {
            record_pod_ip_ready(metric_identity, sandbox_id, started_at, "cached");
            return Ok(Some(pod_ip));
        }

        match timeout_at(
            deadline,
            self.watch_until_pod_ip_assigned(sandbox_id, generation, pod_name)
                .instrument(tracing::info_span!(
                    "substrate.k8s.watch_pod_ip",
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    namespace = %self.k8s.sandbox_namespace,
                )),
        )
        .await
        {
            Ok(Ok(Some(pod_ip))) => {
                record_pod_ip_ready(metric_identity, sandbox_id, started_at, "watch");
                Ok(Some(pod_ip))
            }
            Ok(Ok(None)) => {
                let pod_ip = self
                    .poll_until_pod_ip_assigned(sandbox_id, deadline)
                    .instrument(tracing::info_span!(
                        "substrate.k8s.poll_pod_ip",
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        namespace = %self.k8s.sandbox_namespace,
                    ))
                    .await?;
                if pod_ip.is_some() {
                    record_pod_ip_ready(metric_identity, sandbox_id, started_at, "poll");
                }
                Ok(pod_ip)
            }
            Ok(Err(err)) => {
                pplx_space_metrics::sandbox::api::create_step_error(
                    sandbox_id,
                    metric_identity,
                    "fc",
                    None,
                    "execution_watch",
                    "watch_error",
                    Some(phase),
                );
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "pod IP watch failed; falling back to polling"
                );
                let pod_ip = self
                    .poll_until_pod_ip_assigned(sandbox_id, deadline)
                    .instrument(tracing::info_span!(
                        "substrate.k8s.poll_pod_ip",
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        namespace = %self.k8s.sandbox_namespace,
                        fallback_reason = "watch_error",
                    ))
                    .await?;
                if pod_ip.is_some() {
                    record_pod_ip_ready(
                        metric_identity,
                        sandbox_id,
                        started_at,
                        "poll_after_watch_error",
                    );
                }
                Ok(pod_ip)
            }
            Err(_) => Ok(None),
        }
    }

    pub(super) async fn wait_for_pod_proxy_reachable(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        target: PodProxyWaitTarget<'_>,
        phase: &'static str,
        // Phase deadline of the enclosing budget-bounded wait (see
        // `ready_phase_deadline`). The configured data-plane timeout is
        // clamped to it so this wait's phase-tagged timeout fires before
        // the outer budget swallows it into a generic phase-less one.
        phase_deadline: Instant,
    ) -> Result<(PodIp, ExecutionBinding), SubstrateError> {
        let started_at = Instant::now();
        let mut connect_attempts = 0u64;
        let deadline =
            (Instant::now() + self.k8s.create_data_plane_ready_timeout).min(phase_deadline);
        let sandbox_id_string = sandbox_id.to_string();

        let Some(pod_ip) = self
            .wait_for_pod_ip_assignment(
                metric_identity,
                sandbox_id,
                target.generation,
                target.pod_name,
                deadline,
                phase,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.wait_pod_ip",
                sandbox_id = %sandbox_id,
                pod_name = %target.pod_name,
                namespace = %self.k8s.sandbox_namespace,
            ))
            .await?
        else {
            record_pod_proxy_wait_timeout(metric_identity, sandbox_id, started_at);
            // Pod never reached scheduled / IP-assigned in the window. No
            // node-mgr probe ran on this path, so the message names only
            // the phase that stalled; the create path's defer-cleanup
            // decision keys off the `Timeout` variant itself, not a
            // node-mgr label.
            let phase = WaitReadyTimeoutPhase::PodNeverReady;
            return Err(SubstrateError::Timeout {
                message: format!(
                    "timed out waiting for sandbox pod to become ready (phase: {phase})"
                ),
                phase: Some(phase),
            });
        };

        loop {
            connect_attempts += 1;
            let connect_started_at = Instant::now();
            if Self::pod_proxy_accepts_tcp(
                pod_ip,
                self.k8s.sandbox_proxy_port,
                START_WAIT_CONNECT_TIMEOUT,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.connect_pod_proxy",
                sandbox_id = %sandbox_id,
                pod_ip = %pod_ip,
                port = self.k8s.sandbox_proxy_port,
                attempt = connect_attempts,
            ))
            .await
            {
                record_pod_proxy_wait_ready(
                    metric_identity,
                    sandbox_id,
                    started_at,
                    connect_started_at,
                );
                return Ok((
                    pod_ip,
                    ExecutionBinding {
                        execution_object_id: target
                            .pod_uid
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| sandbox_id_string.clone()),
                        runtime_endpoint: Self::endpoint_for_pod_ip(
                            pod_ip,
                            self.k8s.sandbox_proxy_port,
                        )?,
                        execution_generation: execution_generation_to_u64(target.generation)?,
                    },
                ));
            }

            if Instant::now() >= deadline {
                record_pod_proxy_wait_timeout(metric_identity, sandbox_id, started_at);
                // Pod IP was assigned but every TCP connect attempt to the
                // sandbox proxy failed. No node-mgr probe ran here either —
                // name the phase that stalled and let the `Timeout` variant
                // drive the defer-cleanup decision.
                let phase = WaitReadyTimeoutPhase::ProxyUnreachable;
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "timed out waiting for sandbox pod proxy to become reachable \
                         (phase: {phase})"
                    ),
                    phase: Some(phase),
                });
            }

            sleep(start_wait_poll_interval(started_at)).await;
        }
    }

    pub(super) async fn poll_until_command_ready(
        &self,
        sandbox_id: SandboxId,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
        pod: CommandReadyPod<'_>,
        // Sandbox template, threaded through only so the
        // `sandbox.api.create.command_ready_timeout` counter can carry a
        // template tag when the create deadline fires. Ignored for non-create
        // operations.
        template: Option<&str>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let started_at = Instant::now();
        let deadline = started_at + ready_timeout;
        let mut poll_backoff = CommandReadyPollBackoff::new(started_at, deadline);
        let mut node_api_connected_at = None;
        // Tracks whether a pod IP ever resolved, so a deadline that fires
        // with no IP is attributed to `pod_never_ready` rather than to a
        // proxy/guest phase the wait never reached.
        let mut resolved_ip_seen = false;
        // Tracks whether any guest-ready probe returned `Code::Unavailable`.
        // Cascade-amplifier signal: a deadline that fires after a probe
        // genuinely returned gRPC `Unavailable` is the node-mgr-brownout
        // symptom, and the timeout message names node-mgr so callers can
        // classify the failure and route cleanup to the reconciler instead
        // of spawning more cleanup load. TCP connect failures and per-probe
        // deadline misses do NOT set this — they have other dominant causes
        // (e.g. a wedged sbx-mgr) and naming node-mgr for them sends the
        // on-call chasing the wrong service.
        let mut saw_unavailable_probe = false;
        let (
            selected_route_identity,
            mut selected_route_pod_ip,
            pre_resolved_pod_ip,
            resolve_legacy,
        ) = match pod {
            CommandReadyPod::PreResolved(pod_ip) => (None, None, Some(pod_ip), false),
            CommandReadyPod::ResolveLegacy => (None, None, None, true),
            CommandReadyPod::SelectedRoute {
                route_identity,
                pod_ip,
            } => (Some(route_identity), pod_ip, None, false),
        };
        let mut connection: Option<(PodIp, RuntimeClient)> = None;
        let mut selected_pod_resolution_error = None;

        loop {
            if let Some(route_identity) = selected_route_identity
                && selected_route_pod_ip.is_none()
            {
                match self.selected_wake_pod_ip(sandbox_id, route_identity).await {
                    Ok(pod_ip) => {
                        selected_route_pod_ip = pod_ip;
                        selected_pod_resolution_error = None;
                    }
                    Err(SubstrateError::Timeout { message, .. }) => {
                        // A live route remains selected; retry transient
                        // apiserver resolution at the slow cadence through the
                        // caller's readiness deadline. Identity/UID changes
                        // stay terminal and never enter this arm.
                        selected_pod_resolution_error = Some(message);
                    }
                    Err(error) => return Err(error),
                }
            }
            let resolved_pod_ip = if resolve_legacy {
                self.resolve_pod_ip(sandbox_id).await?
            } else {
                selected_route_pod_ip.or(pre_resolved_pod_ip)
            };
            if let Some(pod_ip) = resolved_pod_ip {
                resolved_ip_seen = true;
                if connection
                    .as_ref()
                    .is_none_or(|(connected_ip, _)| *connected_ip != pod_ip)
                {
                    let connect_started_at = Instant::now();
                    let client = Self::pod_worker_client(
                        pod_ip,
                        self.k8s.sandbox_proxy_port,
                        START_WAIT_CONNECT_TIMEOUT,
                    )
                    .await;
                    if client.is_some() {
                        record_api_wait_ready_latency(
                            sandbox_id,
                            metric_identity,
                            "node_api_connect",
                            "success",
                            connect_started_at,
                            None,
                        );
                    }
                    connection = client.map(|client| (pod_ip, client));
                    if connection.is_none() && selected_route_identity.is_some() {
                        // Before retrying a failed authenticated connection,
                        // revalidate the live Pod UID/IP and selected route.
                        selected_route_pod_ip = None;
                    }
                }
                // A failed TCP connect to the sandbox pod proxy is NOT a
                // node-mgr signal: the proxy can be down because the shim's
                // LaunchSandbox is hung on a wedged sbx-mgr just as easily
                // as on a stuck node-mgr. The deadline classification below
                // reports it as `proxy_unreachable` (via
                // `node_api_connected_at` staying `None`) without naming a
                // service, and the create path's defer-cleanup decision
                // keys off the `Timeout` variant itself.
                let mut reset_connection = false;
                if let Some((_, client)) = connection.as_mut() {
                    if node_api_connected_at.is_none() {
                        node_api_connected_at = Some(Instant::now());
                        record_api_wait_ready_latency(
                            sandbox_id,
                            metric_identity,
                            "node_api_first_connect",
                            "success",
                            started_at,
                            None,
                        );
                        record_api_wait_ready_latency(
                            sandbox_id,
                            metric_identity,
                            "placement",
                            "success",
                            started_at,
                            None,
                        );
                    }

                    let probe_started_at = Instant::now();
                    let probe = client.spaced_wait_ready(wait_ready_lifecycle_request(
                        SpacedWaitReadyRequest {
                            sandbox_id: sandbox_id.to_string(),
                            timeout: Some(duration_to_proto(START_WAIT_COMMAND_PROBE_TIMEOUT)),
                        },
                    ));
                    match timeout(START_WAIT_COMMAND_PROBE_TIMEOUT, probe).await {
                        Ok(Ok(_)) => {
                            record_api_wait_ready_latency(
                                sandbox_id,
                                metric_identity,
                                "command_probe",
                                "success",
                                probe_started_at,
                                None,
                            );
                            record_api_wait_ready_latency(
                                sandbox_id,
                                metric_identity,
                                "total",
                                "success",
                                started_at,
                                None,
                            );
                            if let Some(node_api_connected_at) = node_api_connected_at {
                                record_api_wait_ready_latency(
                                    sandbox_id,
                                    metric_identity,
                                    "command_ready_after_placement",
                                    "success",
                                    node_api_connected_at,
                                    Some("placement"),
                                );
                            }
                            if let (Some(route_identity), Some(selected_pod_ip)) =
                                (selected_route_identity, selected_route_pod_ip)
                            {
                                return self
                                    .selected_wake_binding(
                                        sandbox_id,
                                        route_identity,
                                        selected_pod_ip,
                                    )
                                    .await;
                            }
                            let execution = self
                                .store
                                .active_execution_status(sandbox_id)
                                .await?
                                .ok_or_else(|| SubstrateError::Unavailable {
                                message: "sandbox has no active Kubernetes execution".to_string(),
                            })?;
                            return Ok(ExecutionBinding {
                                execution_object_id: execution.pod_name,
                                runtime_endpoint: Self::endpoint_for_pod_ip(
                                    pod_ip,
                                    self.k8s.sandbox_proxy_port,
                                )?,
                                execution_generation: execution_generation_to_u64(
                                    execution.generation,
                                )?,
                            });
                        }
                        Ok(Err(err))
                            if err.code() == Code::FailedPrecondition
                                && (err.message().contains("not started")
                                    || err.message().contains("sandbox is starting")
                                    || err.message().contains("wait for start to complete")) => {}
                        Ok(Err(err)) if err.code() == Code::Unavailable => {
                            saw_unavailable_probe = true;
                            reset_connection = true;
                        }
                        Ok(Err(err)) => {
                            record_api_wait_ready_error(
                                sandbox_id,
                                metric_identity,
                                "command_probe",
                                "grpc_error",
                                grpc_code_tag(err.code()),
                            );
                            return Err(SubstrateError::Failed {
                                message: format!("sandbox failed before start completed: {err}"),
                            });
                        }
                        Err(_) => {
                            // Per-probe deadline elapsed: connected to the
                            // proxy but `spaced_wait_ready` didn't respond
                            // in time. Not a node-mgr signal — the probe
                            // hung rather than returning `Unavailable` — so
                            // the eventual deadline reports `guest_not_ready`
                            // without naming a service.
                            reset_connection = true;
                        }
                    }
                }
                if reset_connection {
                    // Tonic channels reconnect internally. Once a probe fails,
                    // retaining the channel could silently reconnect to a
                    // reassigned Pod IP without rechecking the immutable UID.
                    // Rebuild legacy/pre-resolved transports too, matching the
                    // pre-cache behavior instead of burning the full deadline
                    // on one half-open channel.
                    connection = None;
                    if selected_route_identity.is_some() {
                        selected_route_pod_ip = None;
                    }
                }
            }

            if Instant::now() >= deadline {
                if metric_identity.op == "create" {
                    pplx_space_metrics::sandbox::api::create_command_ready_timeout(
                        sandbox_id, template,
                    );
                }
                let phase = if !resolved_ip_seen {
                    WaitReadyTimeoutPhase::PodNeverReady
                } else if node_api_connected_at.is_none() {
                    WaitReadyTimeoutPhase::ProxyUnreachable
                } else {
                    WaitReadyTimeoutPhase::GuestNotReady
                };
                let mut error = command_ready_timeout_error(phase, saw_unavailable_probe);
                if let (SubstrateError::Timeout { message, .. }, Some(resolution_error)) =
                    (&mut error, selected_pod_resolution_error.as_deref())
                {
                    message.push_str(&format!("; last pod resolution error: {resolution_error}"));
                }
                return Err(error);
            }

            let delay = poll_backoff.next_delay();
            // Recompute after connection/probe handling: either path may have
            // invalidated a previously selected IP in this same iteration.
            // The slow clamp must apply immediately or a brownout produces an
            // uncached Pod GET and several store reads every 250 ms.
            let waiting_for_selected_pod_ip =
                selected_route_identity.is_some() && selected_route_pod_ip.is_none();
            let delay = if waiting_for_selected_pod_ip {
                delay
                    .max(START_WAIT_SLOW_POLL_INTERVAL)
                    .min(deadline.saturating_duration_since(Instant::now()))
            } else {
                delay
            };
            sleep(delay).await;
        }
    }

    pub(super) async fn poll_connected_client_until_command_ready(
        &self,
        client: &mut NodeApiClient,
        sandbox_id: SandboxId,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
        binding: ExecutionBinding,
        template: Option<&str>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let started_at = Instant::now();
        let deadline = started_at + ready_timeout;
        let mut poll_backoff = CommandReadyPollBackoff::new(started_at, deadline);
        let node_api_connected_at = Instant::now();
        // See `poll_until_command_ready`: set only by a probe that genuinely
        // returned gRPC `Unavailable`, so the timeout message names node-mgr
        // only when a probe actually implicated it.
        let mut saw_unavailable_probe = false;

        record_api_wait_ready_latency(
            sandbox_id,
            metric_identity,
            "node_api_first_connect",
            "success",
            started_at,
            None,
        );
        record_api_wait_ready_latency(
            sandbox_id,
            metric_identity,
            "placement",
            "success",
            started_at,
            None,
        );

        loop {
            let probe_started_at = Instant::now();
            let mut worker_client = RetryingWorkerApiClient::new(client);
            let probe = worker_client.spaced_wait_ready_with_metadata(
                wait_ready_lifecycle_request(SpacedWaitReadyRequest {
                    sandbox_id: sandbox_id.to_string(),
                    timeout: Some(duration_to_proto(START_WAIT_COMMAND_PROBE_TIMEOUT)),
                }),
                START_WAIT_COMMAND_PROBE_TIMEOUT,
            );
            match timeout(START_WAIT_COMMAND_PROBE_TIMEOUT, probe).await {
                Ok(Ok(_)) => {
                    record_api_wait_ready_latency(
                        sandbox_id,
                        metric_identity,
                        "command_probe",
                        "success",
                        probe_started_at,
                        None,
                    );
                    record_api_wait_ready_latency(
                        sandbox_id,
                        metric_identity,
                        "total",
                        "success",
                        started_at,
                        None,
                    );
                    record_api_wait_ready_latency(
                        sandbox_id,
                        metric_identity,
                        "command_ready_after_placement",
                        "success",
                        node_api_connected_at,
                        Some("placement"),
                    );
                    return Ok(binding);
                }
                Ok(Err(err))
                    if err.code() == Code::FailedPrecondition
                        && (err.message().contains("not started")
                            || err.message().contains("sandbox is starting")
                            || err.message().contains("wait for start to complete")) => {}
                Ok(Err(err)) if err.code() == Code::Unavailable => {
                    saw_unavailable_probe = true;
                }
                Ok(Err(err)) => {
                    record_api_wait_ready_error(
                        sandbox_id,
                        metric_identity,
                        "command_probe",
                        "grpc_error",
                        grpc_code_tag(err.code()),
                    );
                    return Err(SubstrateError::Failed {
                        message: format!("sandbox failed before start completed: {err}"),
                    });
                }
                Err(_) => {}
            }

            if Instant::now() >= deadline {
                if metric_identity.op == "create" {
                    pplx_space_metrics::sandbox::api::create_command_ready_timeout(
                        sandbox_id, template,
                    );
                }
                // The caller hands us an already-connected client, so the
                // proxy was reachable — the guest is what never reported in.
                return Err(command_ready_timeout_error(
                    WaitReadyTimeoutPhase::GuestNotReady,
                    saw_unavailable_probe,
                ));
            }

            sleep(poll_backoff.next_delay()).await;
        }
    }

    pub(super) async fn ensure_active_execution_pod_proxy_reachable(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        // Forwarded to `wait_for_pod_proxy_reachable`; see its doc.
        phase_deadline: Instant,
    ) -> Result<(), SubstrateError> {
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox has no active Kubernetes execution".to_string(),
            })?;

        if execution.pod_ip.is_some() {
            return Ok(());
        }

        self.wait_for_pod_proxy_reachable(
            metric_identity,
            sandbox_id,
            PodProxyWaitTarget {
                pod_name: &execution.pod_name,
                pod_uid: None,
                generation: execution.generation,
            },
            "wake",
            phase_deadline,
        )
        .instrument(tracing::info_span!(
            "substrate.k8s.wait_wake_pod_proxy_reachable",
            sandbox_id = %sandbox_id,
            pod_name = %execution.pod_name,
            namespace = %self.k8s.sandbox_namespace,
        ))
        .await?;
        Ok(())
    }
}

/// Identity of the execution pod a proxy-reachability wait targets. Groups
/// the execution-row fields so `wait_for_pod_proxy_reachable`'s signature
/// stays under clippy's `too_many_arguments` ceiling as knobs are added.
pub(super) struct PodProxyWaitTarget<'a> {
    pub pod_name: &'a str,
    /// Becomes the binding's `execution_object_id` when present; the wake
    /// path passes `None` (the binding it produces is discarded).
    pub pod_uid: Option<&'a str>,
    pub generation: i64,
}
