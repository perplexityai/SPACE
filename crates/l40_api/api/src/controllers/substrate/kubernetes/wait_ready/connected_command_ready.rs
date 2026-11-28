use super::*;
use pplx_space_api_route::wait_ready_lifecycle_request;

impl KubernetesSubstrate {
    pub(in crate::controllers::substrate::kubernetes) async fn poll_connected_client_until_command_ready(
        &self,
        client: &mut NodeApiClient,
        sandbox_id: SandboxId,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
        binding: ExecutionBinding,
        template: Option<&str>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let probes = match Self::probe_connected_client_until_command_ready(
            client,
            sandbox_id,
            metric_identity,
            ready_timeout,
        )
        .await?
        {
            ConnectedCommandReadiness::Ready => return Ok(binding),
            ConnectedCommandReadiness::TimedOut(probes) => probes,
        };
        if metric_identity.op == "create" {
            crate::metrics::sandbox::api::create_command_ready_timeout(sandbox_id, template);
        }
        // The caller reached the node for the adoption's setup and Wake, so
        // the timeout names the guest's phase. A probe that could not reach
        // the node also ends in `DeadlineExceeded`; the warning carries what
        // the probes returned. Keep it out of the message: a transport
        // signature there would make the timeout reclaimable and spend
        // another readiness budget on the same pod.
        tracing::warn!(
            sandbox_id = %sandbox_id,
            deadline_exceeded_probes = probes.deadline_exceeded,
            last_probe_code = ?probes.last_status.as_ref().map(tonic::Status::code),
            last_probe_message = probes.last_status.as_ref().map(tonic::Status::message),
            "pooled sandbox command readiness wait timed out"
        );
        Err(command_ready_timeout_error(
            WaitReadyTimeoutPhase::GuestNotReady,
            probes.saw_unavailable,
        ))
    }

    /// Probes the connected node until the guest reports command readiness
    /// or `ready_timeout` passes. Starts no probe once the deadline has
    /// passed: one with no budget left would end before it reached the node.
    pub(in crate::controllers::substrate::kubernetes) async fn probe_connected_client_until_command_ready(
        client: &mut NodeApiClient,
        sandbox_id: SandboxId,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
    ) -> Result<ConnectedCommandReadiness, SubstrateError> {
        let started_at = Instant::now();
        let deadline = started_at + ready_timeout;
        let mut poll_backoff = CommandReadyPollBackoff::new(started_at, deadline);
        let node_api_connected_at = Instant::now();
        let mut probes = SwallowedCommandProbes::default();

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
            if probe_started_at >= deadline {
                return Ok(ConnectedCommandReadiness::TimedOut(probes));
            }

            let probe_timeout = (START_WAIT_COMMAND_PROBE_TIMEOUT
                + START_WAIT_COMMAND_PROBE_CLIENT_MARGIN)
                .min(deadline.saturating_duration_since(probe_started_at));
            let mut worker_client = RetryingWorkerApiClient::new(client);
            let probe = worker_client.spaced_wait_ready_with_metadata(
                wait_ready_lifecycle_request(SpacedWaitReadyRequest {
                    sandbox_id: sandbox_id.to_string(),
                    timeout: Some(duration_to_proto(START_WAIT_COMMAND_PROBE_TIMEOUT)),
                }),
                probe_timeout,
            );
            match timeout(probe_timeout, probe).await {
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
                    return Ok(ConnectedCommandReadiness::Ready);
                }
                Ok(Err(err))
                    if err.code() == Code::FailedPrecondition
                        && (err.message().contains("not started")
                            || err.message().contains("sandbox is starting")
                            || err.message().contains("wait for start to complete")) =>
                {
                    probes.last_status = Some(err);
                }
                Ok(Err(err)) if err.code() == Code::Unavailable => {
                    probes.saw_unavailable = true;
                    probes.last_status = Some(err);
                }
                // Not ready yet, like the outer timeout below: the readiness
                // deadline, not one probe, decides when the guest has failed
                // to start. Three sources end a probe this way: the client's
                // per-probe deadline, which ends a probe for a guest that is
                // not ready because the node keeps waiting for it; the
                // retrying client running out of its budget while the node
                // refuses connections; and the node's own expired wait.
                Ok(Err(err)) if err.code() == Code::DeadlineExceeded => {
                    probes.deadline_exceeded += 1;
                    probes.last_status = Some(err);
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
                // The same bound as the client's deadline, which usually
                // fires first.
                Err(_) => probes.deadline_exceeded += 1,
            }

            sleep(poll_backoff.next_delay()).await;
        }
    }
}

/// How [`KubernetesSubstrate::probe_connected_client_until_command_ready`]
/// ended.
pub(in crate::controllers::substrate::kubernetes) enum ConnectedCommandReadiness {
    Ready,
    /// The readiness deadline passed first.
    TimedOut(SwallowedCommandProbes),
}

/// What the readiness probes the poll swallowed returned, for the warning at
/// the deadline. A node that stops answering and a guest that never reports
/// both end every probe at its deadline, which the timeout's phase cannot
/// tell apart.
#[derive(Debug, Default)]
pub(in crate::controllers::substrate::kubernetes) struct SwallowedCommandProbes {
    /// See `poll_until_command_ready`: set only by a probe that genuinely
    /// returned gRPC `Unavailable`, so the timeout message names node-mgr
    /// only when a probe actually implicated it.
    pub(in crate::controllers::substrate::kubernetes) saw_unavailable: bool,
    /// Probes that ended in `DeadlineExceeded`, counting those the outer
    /// timeout ended without a status.
    pub(in crate::controllers::substrate::kubernetes) deadline_exceeded: usize,
    /// The latest status a probe returned.
    pub(in crate::controllers::substrate::kubernetes) last_status: Option<tonic::Status>,
}
