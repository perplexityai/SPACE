//! Guest readiness probes and readiness snapshots.

use super::*;

pub(super) const READY_WAIT_RETRY_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(5);

// One caller drives readiness for all concurrent waiters. Keep each guest
// long-poll finite so a guest that accepts but never responds cannot monopolize
// the shared driver for an arbitrarily large caller-supplied timeout.
pub(super) const MAX_COORDINATED_READY_PROBE_WAIT: std::time::Duration =
    std::time::Duration::from_secs(1);

pub(super) const BRIDGE_GUEST_PROBE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(1);

pub(super) const BRIDGE_GUEST_READINESS_RESPONSE_SLACK: std::time::Duration =
    std::time::Duration::from_millis(100);

pub(super) fn bridge_guest_readiness_response_timeout(
    guest_wait_timeout: std::time::Duration,
) -> std::time::Duration {
    guest_wait_timeout.saturating_add(BRIDGE_GUEST_READINESS_RESPONSE_SLACK)
}

impl SpacedHostService {
    pub(super) fn log_guest_ready_snapshot(
        &self,
        snapshot: &GuestReadySnapshot,
        source: &'static str,
    ) {
        tracing::debug!(target: LOG_TARGET,
            guest_uptime_secs = snapshot
                .guest_uptime
                .as_ref()
                .map(|duration| duration.seconds),
            guest_version = %snapshot.version,
            guest_compatibility_family = %snapshot.compatibility_family,
            source,
            "spaced guest reported ready"
        );
    }

    pub(super) fn record_lifecycle_ready_snapshot(&self, snapshot: GuestReadySnapshot) {
        self.log_guest_ready_snapshot(&snapshot, "wait_ready");
        let previous = self
            .inner
            .guest_readiness
            .snapshot
            .send_replace(Some(snapshot.clone()));
        if previous.is_none() {
            worker_metrics::spaced_guest_ready(&snapshot.version);
        }
    }

    pub(crate) async fn ping_bridge_guest_once(&self) -> Result<(), Status> {
        with_bridge_guest_timeout(
            BRIDGE_GUEST_PROBE_TIMEOUT,
            "bridge guest ping timed out",
            async {
                let (client, _) = self.connect_bridge_guest().await.map_err(|error| {
                    host_relay_status(
                        Status::unavailable(error.to_string()),
                        "bridge_guest_ping",
                        "bridge_guest_meta",
                    )
                })?;
                client
                    .max_decoding_message_size(
                        pplx_space_spaced_common::BRIDGE_GUEST_PING_MAX_DECODE_BYTES,
                    )
                    .ping(BridgeGuestPingRequest::default())
                    .await
                    .map(|_| ())
                    .map_err(|status| {
                        host_relay_status(status, "bridge_guest_ping", "bridge_guest_meta")
                    })
            },
        )
        .await
    }

    pub(crate) async fn bridge_guest_command_plane_connected(&self) -> bool {
        self.ping_bridge_guest_once().await.is_ok()
    }

    pub(crate) fn clear_guest_ready(&self) {
        self.inner.guest_readiness.snapshot.send_replace(None);
    }

    pub(super) fn guest_ready_snapshot_from_wait(
        &self,
        response: BridgeGuestWaitReadyResponse,
    ) -> Result<Option<GuestReadySnapshot>, Status> {
        if !response.ready {
            return Ok(None);
        }
        pplx_space_spaced_common::validate_spaced_guest_compatibility(
            Some(&response.version),
            Some(&response.compatibility_family),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let guest_uptime = validate_optional_duration("guest_uptime", response.guest_uptime)?;
        let process_execution = response.process_execution.map(|mut info| {
            // A newer guest can advertise guarantees this host cannot forward.
            info.capabilities.retain(|capability| {
                capability
                    == pplx_space_common_grpc::GuestCapability::ProcessOutputGapResume.as_str()
            });
            info
        });
        Ok(Some(GuestReadySnapshot {
            process_execution,
            guest_uptime: guest_uptime.map(duration_to_proto).transpose()?,
            version: response.version,
            compatibility_family: response.compatibility_family,
            mem_total_bytes: response.mem_total_bytes,
            vcpu_count: response.vcpu_count,
        }))
    }

    pub(super) async fn wait_bridge_guest_ready_once(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<GuestReadySnapshot>, Status> {
        let (mut client, _) = with_bridge_guest_timeout(
            BRIDGE_GUEST_PROBE_TIMEOUT,
            "bridge guest readiness connection timed out",
            async {
                self.connect_bridge_guest()
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            },
        )
        .await?;

        with_bridge_guest_timeout(
            bridge_guest_readiness_response_timeout(timeout),
            "bridge guest readiness probe timed out",
            async {
                let response = match client
                    .wait_ready(BridgeGuestWaitReadyRequest {
                        timeout: Some(duration_to_proto(timeout)?),
                    })
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(error) => return Err(error),
                };
                self.guest_ready_snapshot_from_wait(response)
            },
        )
        .await
    }

    pub(crate) async fn refresh_guest_ready_status(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<GuestReadySnapshot>, Status> {
        let mut ready_rx = self.inner.guest_readiness.snapshot.subscribe();
        let operation_budget = BRIDGE_GUEST_PROBE_TIMEOUT
            .saturating_add(bridge_guest_readiness_response_timeout(timeout));
        let deadline = tokio::time::Instant::now() + operation_budget;
        self.coordinated_bridge_guest_ready_probe(&mut ready_rx, deadline, timeout)
            .await
    }

    pub(super) async fn coordinated_bridge_guest_ready_probe(
        &self,
        ready_rx: &mut tokio::sync::watch::Receiver<Option<GuestReadySnapshot>>,
        deadline: tokio::time::Instant,
        guest_wait_timeout: std::time::Duration,
    ) -> Result<Option<GuestReadySnapshot>, Status> {
        let driver = loop {
            let acquire_driver = self.inner.guest_readiness.driver.acquire();
            tokio::select! {
                biased;
                _ = self.inner.shutdown.cancelled() => {
                    return Err(Status::cancelled("request cancelled"));
                }
                changed = ready_rx.changed() => {
                    changed.map_err(|_| Status::unavailable("guest readiness channel closed"))?;
                    if let Some(snapshot) = ready_rx.borrow_and_update().clone() {
                        return Ok(Some(snapshot));
                    }
                }
                permit = acquire_driver => {
                    break permit.map_err(|_| Status::unavailable("guest readiness coordinator closed"))?;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(Status::deadline_exceeded(
                        "guest did not report ready before timeout",
                    ));
                }
            }
        };

        // A previous driver can publish and release the permit before this
        // receiver wins the semaphore branch. Observe that generation before
        // starting another guest RPC.
        if ready_rx
            .has_changed()
            .map_err(|_| Status::unavailable("guest readiness channel closed"))?
            && let Some(snapshot) = ready_rx.borrow_and_update().clone()
        {
            return Ok(Some(snapshot));
        }

        let probe = self.wait_bridge_guest_ready_once(guest_wait_timeout);
        let result = tokio::select! {
            biased;
            _ = self.inner.shutdown.cancelled() => {
                Err(Status::cancelled("request cancelled"))
            }
            result = probe => result,
            _ = tokio::time::sleep_until(deadline) => {
                Err(Status::deadline_exceeded(
                    "guest did not report ready before timeout",
                ))
            }
        };
        if let Some(snapshot) = result.as_ref().ok().and_then(Option::as_ref) {
            self.record_lifecycle_ready_snapshot(snapshot.clone());
        } else {
            self.clear_guest_ready();
            // An incompatible identity cannot become supported by retrying.
            // Preserve the terminal verdict even for a very short probe budget.
            if result
                .as_ref()
                .is_err_and(|error| error.code() == tonic::Code::FailedPrecondition)
            {
                return result;
            }
            // The driver owns the retry cadence as well as the RPC. Releasing
            // the permit before this delay would let a herd of waiters turn a
            // fast NotReady/error response into a serialized busy loop.
            tokio::select! {
                biased;
                _ = self.inner.shutdown.cancelled() => {
                    return Err(Status::cancelled("request cancelled"));
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(Status::deadline_exceeded(
                        "guest did not report ready before timeout",
                    ));
                }
                _ = tokio::time::sleep(READY_WAIT_RETRY_INTERVAL) => {}
            }
        }
        drop(driver);
        result
    }

    pub(crate) async fn wait_guest_ready(
        &self,
        timeout: ReadyTimeout,
        context: RetryContext,
    ) -> Result<GuestReadySnapshot, Status> {
        let context = context
            .limited_by_timeout(timeout.as_duration())
            .with_cancellation(self.inner.shutdown.clone());
        let Some(remaining) = context.remaining() else {
            return Err(Status::deadline_exceeded(
                "guest readiness timeout is required",
            ));
        };
        let deadline = tokio::time::Instant::now() + remaining;
        let mut ready_rx = self.inner.guest_readiness.snapshot.subscribe();

        // A cached snapshot proves a previous generation was ready, not that
        // the current guest command plane is live. Revalidate it through the
        // same single-driver slot used by uncached waiters.
        if ready_rx.borrow_and_update().is_some() {
            let probe_timeout = remaining.min(READY_WAIT_RETRY_INTERVAL);
            match self
                .coordinated_bridge_guest_ready_probe(&mut ready_rx, deadline, probe_timeout)
                .await
            {
                Ok(Some(snapshot)) => return Ok(snapshot),
                Ok(None) => {}
                Err(error)
                    if !self.inner.shutdown.is_cancelled()
                        && (pplx_space_spaced_common::status_is_retryable(&error)
                            || error.code() == tonic::Code::Cancelled) =>
                {
                    tracing::trace!(target: LOG_TARGET, %error, "cached guest readiness probe interrupted; continuing wait");
                }
                Err(error) => return Err(error),
            }
        }

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Status::deadline_exceeded(
                    "guest did not report ready before timeout",
                ));
            }
            let probe_timeout = remaining.min(MAX_COORDINATED_READY_PROBE_WAIT);
            match self
                .coordinated_bridge_guest_ready_probe(&mut ready_rx, deadline, probe_timeout)
                .await
            {
                Ok(Some(snapshot)) => return Ok(snapshot),
                Ok(None) => {}
                Err(error) if error.code() == tonic::Code::DeadlineExceeded => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                    tracing::trace!(target: LOG_TARGET, %error, "bridge guest readiness probe timed out");
                }
                Err(error)
                    if self.inner.shutdown.is_cancelled()
                        || error.code() == tonic::Code::FailedPrecondition =>
                {
                    return Err(error);
                }
                Err(error) => {
                    // Tonic also reports a closed transport as Cancelled. A
                    // read-only readiness probe can reconnect within this same
                    // deadline; actual shutdown is checked above, and dropping
                    // the caller still drops its probe and driver permit.
                    tracing::trace!(target: LOG_TARGET, %error, "bridge guest readiness wait failed");
                }
            }
        }
    }

    pub(crate) fn guest_ready_status(&self) -> Option<GuestReadySnapshot> {
        self.inner.guest_readiness.snapshot.borrow().clone()
    }
}
