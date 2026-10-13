use super::*;
use crate::controllers::db_contention_error;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{MAX_SANDBOX_PORT_EXPOSURES, SandboxId};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Authority, insert_space_api_route_ed25519_token,
};
use pplx_space_common_svc::types::ReconcileReason;
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};

impl SandboxController {
    pub async fn list_sandbox_port_exposures(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
    ) -> Result<Vec<SandboxPortExposure>, ControllerError> {
        sqlx::query_as::<_, SandboxPortExposure>(sql!(
            "SELECT sandbox_id,
                    protocol,
                    guest_port,
                    visibility,
                    source,
                    desired_state,
                    apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at,
                    created_at,
                    updated_at
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
             ORDER BY guest_port ASC, protocol ASC"
        ))
        .bind(sandbox.id)
        .fetch_all(pool)
        .await
        .map_err(|err| db_contention_error("failed to list sandbox port exposures", &err))
    }

    pub async fn open_sandbox_port(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        port: u16,
        protocol: Option<SandboxPortProtocol>,
        visibility: Option<SandboxPortVisibility>,
        source: &str,
    ) -> Result<SandboxPortExposure, ControllerError> {
        let port = validate_sandbox_port(port)?;
        let protocol = normalize_sandbox_port_protocol(protocol);
        let visibility = normalize_sandbox_port_visibility(visibility);
        let event_details = || {
            SandboxEventDetails::default()
                .protocol(protocol)
                .guest_port(port.to_string())
                .visibility(visibility)
                .reason(source)
        };
        SandboxEvent::new(
            sandbox.id,
            SandboxEventType::PortExposureOpenRequested,
            SandboxEventSource::Api,
        )
        .emit_with(event_details());
        let mut tx = pool.begin().await.map_err(|err| {
            let error = "failed to begin sandbox port exposure update";
            let mapped = db_contention_error(error, &err);
            // Same rule as the upsert and commit arms below: a lost race
            // wrote nothing and the caller retries, so it must not leave a
            // permanent-looking failure on the sandbox's timeline.
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureOpenFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(event_details().error(error));
            }
            mapped
        })?;
        // Serialize admission per sandbox so concurrent API and internal ECW
        // opens cannot both observe the last free slot. Existing desired-open
        // rows remain idempotent and do not consume another slot.
        let locked_sandbox = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| db_contention_error("failed to lock sandbox for port admission", &err))?;
        if locked_sandbox.is_none() {
            return Err(ControllerError::not_found("Sandbox"));
        }
        let already_open = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM sandbox_port_exposures
                 WHERE sandbox_id = $1
                   AND protocol = $2
                   AND guest_port = $3
                   AND desired_state = 'open'
             )"
        ))
        .bind(sandbox.id)
        .bind(protocol)
        .bind(port as i32)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| {
            db_contention_error("failed to check existing sandbox port exposure", &err)
        })?;
        let other_open_ports = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*)
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND desired_state = 'open'
               AND NOT (protocol = $2 AND guest_port = $3)"
        ))
        .bind(sandbox.id)
        .bind(protocol)
        .bind(port as i32)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| db_contention_error("failed to count sandbox port exposures", &err))?;
        if let Err(error) = enforce_sandbox_port_exposure_capacity(other_open_ports, already_open) {
            SandboxEvent::new(
                sandbox.id,
                SandboxEventType::PortExposureOpenFailed,
                SandboxEventSource::Api,
            )
            .emit_with(event_details().error(error.to_string()));
            return Err(error);
        }
        let exposure = sqlx::query_as::<_, SandboxPortExposure>(sql!(
            "INSERT INTO sandbox_port_exposures (
                 sandbox_id,
                 protocol,
                 guest_port,
                 visibility,
                 source,
                 desired_state,
                 apply_state,
                 last_applied_node_id,
                 last_error
             )
             VALUES ($1, $2, $3, $4, $5, 'open', 'pending', NULL, NULL)
             ON CONFLICT (sandbox_id, protocol, guest_port) DO UPDATE
             SET visibility = EXCLUDED.visibility,
                 source = EXCLUDED.source,
                 desired_state = 'open',
                 apply_state = 'pending',
                 last_error = NULL,
                 updated_at = NOW()
             RETURNING sandbox_id,
                       protocol,
                       guest_port,
                       visibility,
                       source,
                       desired_state,
                       apply_state,
                       last_applied_node_id,
                       last_error,
                       last_bridge_status,
                       last_applied_bridge_generation,
                       last_observed_at,
                       created_at,
                       updated_at"
        ))
        .bind(sandbox.id)
        .bind(protocol)
        .bind(port as i32)
        .bind(visibility)
        .bind(source)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| {
            let error = "failed to persist sandbox port exposure";
            // This upsert blocks on any uncommitted writer of the same
            // exposure row — node publications write it without holding the
            // sandbox row lock this transaction took. A lost race is not a
            // failed open: the caller retries and it succeeds, so it must not
            // leave a permanent-looking failure on the sandbox's timeline.
            let mapped = db_contention_error(error, &err);
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureOpenFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(event_details().error(error));
            }
            mapped
        })?;

        enqueue_sandbox_reconcile_and_trigger_tx(
            &mut tx,
            sandbox.id,
            &[ReconcileRef::sandbox_desired_state(sandbox.id)],
            ReconcileReason::SandboxPortExposureUpdated,
        )
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                %error,
                "failed to enqueue sandbox port exposure reconciliation"
            );
            let mapped = db_contention_error(
                "failed to enqueue sandbox port exposure reconciliation",
                &error,
            );
            // The failure event records a failed exposure change. A lost
            // lock race is not one — nothing was written and the caller
            // retries — so it is skipped. Branching on the mapped error
            // rather than re-running the predicate keeps this decision
            // from drifting if `db_contention_error` ever widens.
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureOpenFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(event_details().error(error.to_string()).logged_by_caller());
            }
            mapped
        })?;
        tx.commit().await.map_err(|error| {
            // Unlike the upsert above, this arm *does* record a failure event,
            // and the difference is not an oversight. `COMMIT` acquires no
            // locks and runs no deferred checks — there is no `DEFERRABLE`
            // constraint in `db/`, and the transaction is READ COMMITTED — so
            // it cannot fail on contention. What reaches here is a transport or
            // backend failure, which is the permanent-looking outcome the
            // timeline should show. The `db_contention_error` below is a
            // fallthrough for that classification, not a live branch.
            SandboxEvent::new(
                sandbox.id,
                SandboxEventType::PortExposureOpenFailed,
                SandboxEventSource::Api,
            )
            .emit_with(event_details().error(error.to_string()));
            db_contention_error("failed to commit sandbox port exposure", &error)
        })?;
        SandboxEvent::new(
            sandbox.id,
            SandboxEventType::PortExposureOpenCompleted,
            SandboxEventSource::Api,
        )
        .emit_with(event_details().result("desired_open"));
        Ok(exposure)
    }

    pub async fn ensure_sandbox_port_exposure(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        port: u16,
        source: &str,
    ) -> Result<SandboxPortExposure, ControllerError> {
        let port = validate_sandbox_port(port)?;
        let existing = sqlx::query_as::<_, SandboxPortExposure>(sql!(
            "SELECT sandbox_id,
                    protocol,
                    guest_port,
                    visibility,
                    source,
                    desired_state,
                    apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at,
                    created_at,
                    updated_at
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND protocol = 'tcp'
               AND guest_port = $2"
        ))
        .bind(sandbox.id)
        .bind(port as i32)
        .fetch_optional(pool)
        .await
        .map_err(|err| db_contention_error("failed to load sandbox port exposure", &err))?;

        if let Some(exposure) = existing
            && exposure.desired_state == SANDBOX_PORT_DESIRED_OPEN
        {
            if sandbox_port_exposure_ready_on_node(&exposure, sandbox.active_node_id) {
                return Ok(exposure);
            }
            return self
                .reconcile_sandbox_port_exposure(pool, sandbox, exposure)
                .await;
        }

        self.open_sandbox_port(
            pool,
            sandbox,
            port,
            Some(SandboxPortProtocol::Tcp),
            Some(SandboxPortVisibility::Private),
            source,
        )
        .await
    }

    pub async fn ensure_sandbox_port_exposure_applied(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        port: u16,
        source: &str,
    ) -> Result<SandboxPortExposure, ControllerError> {
        let port = validate_sandbox_port(port)?;
        let exposure = self
            .ensure_sandbox_port_exposure(pool, sandbox, port, source)
            .await?;

        let sandbox = fetch_sandbox(pool, sandbox.id)
            .await
            .map_err(|err| db_contention_error("failed to refresh sandbox", &err))?
            .ok_or_else(|| ControllerError::not_found("Sandbox"))?;

        if sandbox_port_exposure_ready_on_node(&exposure, sandbox.active_node_id) {
            return Ok(exposure);
        }

        let active_node_id = sandbox.active_node_id.ok_or_else(|| {
            ControllerError::conflict("sandbox has no active node for port exposure")
        })?;

        super::lifecycle::ensure_access_path_not_blocked_by_drain(
            pool,
            &sandbox,
            super::lifecycle::SandboxAction::OpenPort,
        )
        .await?;

        // Exposing a port on a dormant sandbox drives `access_runtime_client`,
        // which restores/recreates it — charge the cluster activation limiter
        // for that implicit wake (no-op for a live sandbox with a ready pod,
        // which returns early above).
        super::lifecycle::charge_implicit_access_activation(
            self.cluster_admission.as_ref(),
            &sandbox,
            ExecutionActivityKind::Access,
        )
        .await?;

        match self.apply_open_sandbox_port(&sandbox, port).await {
            Ok(observation) => {
                let exposure = mark_sandbox_port_exposure_applied(
                    pool,
                    sandbox.id,
                    port,
                    active_node_id,
                    &observation,
                )
                .await?;
                let mut details = SandboxEventDetails::default()
                    .protocol("tcp")
                    .guest_port(port.to_string())
                    .target_node_id(active_node_id.to_string())
                    .result("applied");
                details = details.bridge_status(observation.bridge_status);
                if let Some(service_generation) = observation.service_generation {
                    details = details.service_generation(service_generation);
                }
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureApplyCompleted,
                    SandboxEventSource::Api,
                )
                .emit_with(details);
                Ok(exposure)
            }
            Err(error) => {
                if let Ok(Some(current)) =
                    load_tcp_sandbox_port_exposure(pool, sandbox.id, port).await
                    && sandbox_port_exposure_ready_on_node(&current, Some(active_node_id))
                {
                    return Ok(current);
                }
                mark_sandbox_port_exposure_failed(
                    pool,
                    sandbox.id,
                    port,
                    active_node_id,
                    &error.error,
                    error.observation.as_ref(),
                )
                .await;
                let mut details = SandboxEventDetails::default()
                    .protocol("tcp")
                    .guest_port(port.to_string())
                    .target_node_id(active_node_id.to_string())
                    .error(error.error.to_string());
                if let Some(observation) = error.observation {
                    details = details.bridge_status(observation.bridge_status);
                    if let Some(service_generation) = observation.service_generation {
                        details = details.service_generation(service_generation);
                    }
                }
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureApplyFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(details);
                Err(error.error)
            }
        }
    }

    pub async fn invalidate_sandbox_port_exposure_after_proxy_failure(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        port: u16,
        reason: &str,
    ) -> Result<(), ControllerError> {
        let port = validate_sandbox_port(port)?;
        let mut tx = pool.begin().await.map_err(|err| {
            db_contention_error("failed to begin port exposure invalidation", &err)
        })?;
        let result = sqlx::query(sql!(
            "UPDATE sandbox_port_exposures
             SET apply_state = 'pending',
                 last_applied_node_id = NULL,
                 last_error = $3,
                 last_bridge_status = NULL,
                 last_applied_bridge_generation = NULL,
                 last_observed_at = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND protocol = 'tcp'
               AND guest_port = $2
               AND desired_state = 'open'"
        ))
        .bind(sandbox_id)
        .bind(port as i32)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|err| db_contention_error("failed to invalidate sandbox port exposure", &err))?;

        let invalidated = result.rows_affected() > 0;

        tx.commit().await.map_err(|error| {
            db_contention_error("failed to commit port exposure invalidation", &error)
        })?;

        if invalidated {
            let controller = self.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                controller
                    .recover_loopback_bridge_after_proxy_failure(pool, sandbox_id, port)
                    .await;
            });
        }

        Ok(())
    }

    async fn recover_loopback_bridge_after_proxy_failure(
        &self,
        pool: PgPool,
        sandbox_id: SandboxId,
        port: u16,
    ) {
        match tokio::time::timeout(
            SANDBOX_PORT_PROXY_FAILURE_BRIDGE_CLOSE_TIMEOUT,
            self.close_loopback_bridge_after_proxy_failure(&pool, sandbox_id, port),
        )
        .await
        {
            Ok(()) => {}
            Err(_) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    port,
                    timeout_ms = SANDBOX_PORT_PROXY_FAILURE_BRIDGE_CLOSE_TIMEOUT.as_millis(),
                    "timed out closing loopback bridge after proxy failure"
                );
            }
        }

        if let Err(error) = enqueue_sandbox_reconcile_and_trigger(
            &pool,
            sandbox_id,
            &[ReconcileRef::sandbox_desired_state(sandbox_id)],
            ReconcileReason::SandboxPortExposureUpdated,
        )
        .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                port,
                %error,
                "failed to enqueue sandbox port exposure invalidation"
            );
        }
    }

    async fn close_loopback_bridge_after_proxy_failure(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        port: u16,
    ) {
        let sandbox = match fetch_sandbox(pool, sandbox_id).await {
            Ok(Some(sandbox)) => sandbox,
            Ok(None) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    port,
                    "skipping loopback bridge close after proxy failure: sandbox not found"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    port,
                    %error,
                    "failed to load sandbox for loopback bridge close after proxy failure"
                );
                return;
            }
        };

        if !sandbox_lifecycle_may_have_live_loopback_bridge(&sandbox.lifecycle) {
            tracing::debug!(
                sandbox_id = %sandbox_id,
                port,
                lifecycle = %sandbox.lifecycle,
                "skipping loopback bridge close after proxy failure: sandbox lifecycle is not live"
            );
            return;
        }

        let mut client = match self.substrate.reachable_runtime_client(sandbox_id).await {
            Ok(Some(client)) => client,
            Ok(None) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    port,
                    "skipping loopback bridge close after proxy failure: runtime is not reachable"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    port,
                    %error,
                    "failed to reach runtime client for loopback bridge close after proxy failure"
                );
                return;
            }
        };

        let request = match loopback_bridge_request(
            self.route_ed25519_authority.as_deref(),
            &sandbox,
            pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeRequest {
                sandbox_id: sandbox_id.to_string(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeRequest {
                        port: u32::from(port),
                    },
                ),
            },
        ) {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    port,
                    %error,
                    "failed to prepare loopback bridge close request after proxy failure"
                );
                return;
            }
        };

        let close = client.spaced_close_loopback_bridge(request);
        match close.await {
            Ok(_) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    port,
                    "closed loopback bridge after proxy failure"
                );
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    port,
                    %error,
                    "failed to close loopback bridge after proxy failure"
                );
            }
        }
    }

    async fn apply_open_sandbox_port(
        &self,
        sandbox: &Sandbox,
        port: u16,
    ) -> Result<SandboxPortExposureApplyObservation, SandboxPortExposureApplyError> {
        let mut client = self
            .substrate
            .access_runtime_client(
                SandboxApiMetricIdentity::native("ports"),
                sandbox,
                ExecutionActivityKind::Access,
                DEFAULT_BLOCKING_WAIT_TIMEOUT,
                crate::controllers::substrate::RecoveryCharge::enforce(
                    self.cluster_admission.as_ref(),
                ),
            )
            .await?;
        let sandbox_id = sandbox.id.to_string();
        let port_u32 = u32::from(port);

        client
            .allow_port(AllowPortRequest {
                sandbox_id: sandbox_id.clone(),
                port: port_u32,
                protocol: NetworkProtocol::Tcp as i32,
            })
            .await?;
        client
            .expose_port(ExposePortRequest {
                sandbox_id: sandbox_id.clone(),
                port: port_u32,
                protocol: NetworkProtocol::Tcp as i32,
                guest_port: Some(port_u32),
            })
            .await?;
        // Install the in-VM loopback bridge so guest services bound only to
        // 127.0.0.1:port are reachable via the just-installed host DNAT.
        // Status semantics:
        //   Direct / LoopbackForwarded → success, port is reachable.
        //   WaitingForListener / BindConflict / Failed → return
        //     ServiceUnavailable so the exposure row is marked failed and
        //     the reconciler retries; otherwise the caller would mark the
        //     port `applied` while clients still hit 502.
        // The bridge RPC itself: an outright RPC error is also surfaced as
        // ServiceUnavailable, since we cannot confirm the bridge is in place
        // and marking `applied` while clients hit 502 is the failure mode this
        // path is meant to prevent.
        let request = loopback_bridge_request(
            self.route_ed25519_authority.as_deref(),
            sandbox,
            pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest {
                sandbox_id,
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                        port: port_u32,
                        wait_timeout: Some(prost_types::Duration {
                            seconds: 2,
                            nanos: 0,
                        }),
                    },
                ),
            },
        )
        .map_err(|error| SandboxPortExposureApplyError::new(error, None))?;

        match client.spaced_open_loopback_bridge(request).await {
            Ok(response) => {
                let response = response.into_inner();
                tracing::debug!(
                    sandbox_id = %sandbox.id,
                    port,
                    status = response.status,
                    detail = %response.detail,
                    service_generation = %response.service_generation,
                    "loopback bridge install completed"
                );
                let status =
                    pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus::try_from(
                        response.status,
                    )
                    .unwrap_or(
                        pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus::Unspecified,
                    );
                let observation =
                    sandbox_port_bridge_observation(status, &response.service_generation);
                use pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus as S;
                match status {
                    S::Direct | S::LoopbackForwarded => observation.ok_or_else(|| {
                        SandboxPortExposureApplyError::new(
                            ControllerError::service_unavailable(format!(
                                "loopback bridge returned invalid ready status for port {port}: status={:?}",
                                status
                            )),
                            None,
                        )
                    }),
                    S::WaitingForListener | S::BindConflict | S::Failed | S::Unspecified => Err(
                        SandboxPortExposureApplyError::new(
                            ControllerError::service_unavailable(format!(
                                "loopback bridge not ready for port {port}: status={:?} detail={}",
                                status, response.detail
                            )),
                            observation,
                        ),
                    ),
                }
            }
            Err(error) => {
                // Transport failure (sbx-mgr unreachable, RPC error, etc.) —
                // we don't know the bridge's actual status, so we can't
                // claim `applied`. Surface it as `service_unavailable` so
                // the api caller marks the row `failed`; the reconciler
                // periodic tick or explicit state-reconciliation trigger
                // will re-apply once sbx-mgr is reachable again.
                // Previously we treated this as
                // success ("relying on host DNAT alone"), which silently
                // marked 127.0.0.1-only services as applied while clients
                // saw 502s.
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    port,
                    %error,
                    "loopback bridge install RPC failed"
                );
                Err(SandboxPortExposureApplyError::new(
                    ControllerError::service_unavailable(format!(
                        "loopback bridge install RPC failed for port {port}: {error}"
                    )),
                    None,
                ))
            }
        }
    }

    pub async fn close_sandbox_port(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        port: u16,
        protocol: Option<SandboxPortProtocol>,
    ) -> Result<SandboxPortExposure, ControllerError> {
        let port = validate_sandbox_port(port)?;
        let protocol = normalize_sandbox_port_protocol(protocol);
        let event_details = || {
            SandboxEventDetails::default()
                .protocol(protocol)
                .guest_port(port.to_string())
        };
        SandboxEvent::new(
            sandbox.id,
            SandboxEventType::PortExposureCloseRequested,
            SandboxEventSource::Api,
        )
        .emit_with(event_details());
        let mut tx = pool.begin().await.map_err(|err| {
            let error = "failed to begin sandbox port exposure update";
            let mapped = db_contention_error(error, &err);
            // Same rule as the upsert and commit arms below: a lost race
            // wrote nothing and the caller retries, so it must not leave a
            // permanent-looking failure on the sandbox's timeline.
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureCloseFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(event_details().error(error));
            }
            mapped
        })?;
        // Keep the same sandbox-row-first lock order as open admission. The
        // reconcile enqueue below also locks this row, so updating the port
        // row first would permit an open/close deadlock.
        let locked_sandbox = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| db_contention_error("failed to lock sandbox for port close", &err))?;
        if locked_sandbox.is_none() {
            return Err(ControllerError::not_found("Sandbox"));
        }
        let exposure = sqlx::query_as::<_, SandboxPortExposure>(sql!(
            "INSERT INTO sandbox_port_exposures (
                 sandbox_id,
                 protocol,
                 guest_port,
                 visibility,
                 source,
                 desired_state,
                 apply_state,
                 last_applied_node_id,
                 last_error
             )
             VALUES ($1, $2, $3, 'private', 'api', 'closed', 'pending', NULL, NULL)
             ON CONFLICT (sandbox_id, protocol, guest_port) DO UPDATE
             SET desired_state = 'closed',
                 apply_state = 'pending',
                 last_error = NULL,
                 updated_at = NOW()
             RETURNING sandbox_id,
                       protocol,
                       guest_port,
                       visibility,
                       source,
                       desired_state,
                       apply_state,
                       last_applied_node_id,
                       last_error,
                       last_bridge_status,
                       last_applied_bridge_generation,
                       last_observed_at,
                       created_at,
                       updated_at"
        ))
        .bind(sandbox.id)
        .bind(protocol)
        .bind(port as i32)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| {
            let error = "failed to persist sandbox port exposure";
            // This upsert blocks on any uncommitted writer of the same
            // exposure row — node publications write it without holding the
            // sandbox row lock this transaction took. A lost race is not a
            // failed close: the caller retries and it succeeds, so it must not
            // leave a permanent-looking failure on the sandbox's timeline.
            let mapped = db_contention_error(error, &err);
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureCloseFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(event_details().error(error));
            }
            mapped
        })?;

        enqueue_sandbox_reconcile_and_trigger_tx(
            &mut tx,
            sandbox.id,
            &[ReconcileRef::sandbox_desired_state(sandbox.id)],
            ReconcileReason::SandboxPortExposureUpdated,
        )
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                %error,
                "failed to enqueue sandbox port exposure reconciliation"
            );
            let mapped = db_contention_error(
                "failed to enqueue sandbox port exposure reconciliation",
                &error,
            );
            // The failure event records a failed exposure change. A lost
            // lock race is not one — nothing was written and the caller
            // retries — so it is skipped. Branching on the mapped error
            // rather than re-running the predicate keeps this decision
            // from drifting if `db_contention_error` ever widens.
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::PortExposureCloseFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(event_details().error(error.to_string()).logged_by_caller());
            }
            mapped
        })?;
        tx.commit().await.map_err(|error| {
            // Unlike the upsert above, this arm *does* record a failure event,
            // and the difference is not an oversight. `COMMIT` acquires no
            // locks and runs no deferred checks — there is no `DEFERRABLE`
            // constraint in `db/`, and the transaction is READ COMMITTED — so
            // it cannot fail on contention. What reaches here is a transport or
            // backend failure, which is the permanent-looking outcome the
            // timeline should show. The `db_contention_error` below is a
            // fallthrough for that classification, not a live branch.
            SandboxEvent::new(
                sandbox.id,
                SandboxEventType::PortExposureCloseFailed,
                SandboxEventSource::Api,
            )
            .emit_with(event_details().error(error.to_string()));
            db_contention_error("failed to commit sandbox port exposure", &error)
        })?;
        SandboxEvent::new(
            sandbox.id,
            SandboxEventType::PortExposureCloseCompleted,
            SandboxEventSource::Api,
        )
        .emit_with(event_details().result("desired_closed"));
        Ok(exposure)
    }

    async fn reconcile_sandbox_port_exposure(
        &self,
        pool: &PgPool,
        _sandbox: &Sandbox,
        exposure: SandboxPortExposure,
    ) -> Result<SandboxPortExposure, ControllerError> {
        enqueue_sandbox_reconcile_and_trigger(
            pool,
            exposure.sandbox_id,
            &[ReconcileRef::sandbox_desired_state(exposure.sandbox_id)],
            ReconcileReason::SandboxPortExposureUpdated,
        )
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %exposure.sandbox_id,
                %error,
                "failed to enqueue sandbox port exposure reconciliation"
            );
            db_contention_error(
                "failed to enqueue sandbox port exposure reconciliation",
                &error,
            )
        })?;
        Ok(exposure)
    }

    pub(super) async fn reconcile_open_sandbox_port_exposures_best_effort(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) {
        let Ok(Some(sandbox)) = fetch_sandbox(pool, sandbox_id).await else {
            return;
        };
        let Ok(exposures) = sqlx::query_as::<_, SandboxPortExposure>(sql!(
            "SELECT sandbox_id,
                    protocol,
                    guest_port,
                    visibility,
                    source,
                    desired_state,
                    apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at,
                    created_at,
                    updated_at
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND desired_state = 'open'
             ORDER BY guest_port ASC, protocol ASC"
        ))
        .bind(sandbox_id)
        .fetch_all(pool)
        .await
        else {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                "failed to load sandbox port exposures for reconciliation"
            );
            return;
        };

        for exposure in exposures {
            if let Err(error) = self
                .reconcile_sandbox_port_exposure(pool, &sandbox, exposure)
                .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "failed to reconcile sandbox port exposure"
                );
            }
        }
    }
}

pub(super) fn enforce_sandbox_port_exposure_capacity(
    other_open_ports: i64,
    already_open: bool,
) -> Result<(), ControllerError> {
    if !already_open && other_open_ports >= MAX_SANDBOX_PORT_EXPOSURES as i64 {
        Err(ControllerError::conflict(format!(
            "sandbox port exposure limit reached (maximum {MAX_SANDBOX_PORT_EXPOSURES})"
        )))
    } else {
        Ok(())
    }
}

fn sandbox_port_bridge_observation(
    status: pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus,
    service_generation: &str,
) -> Option<SandboxPortExposureApplyObservation> {
    use pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus as S;

    let bridge_status = match status {
        S::Direct => SANDBOX_PORT_BRIDGE_DIRECT,
        S::LoopbackForwarded => SANDBOX_PORT_BRIDGE_LOOPBACK_FORWARDED,
        S::WaitingForListener => "waiting_for_listener",
        S::BindConflict => "bind_conflict",
        S::Failed => "failed",
        S::Unspecified => return None,
    };
    let service_generation = if service_generation.trim().is_empty() {
        None
    } else {
        Some(service_generation.to_string())
    };

    Some(SandboxPortExposureApplyObservation {
        bridge_status,
        service_generation,
    })
}

pub(super) fn sandbox_lifecycle_may_have_live_loopback_bridge(lifecycle: &str) -> bool {
    !matches!(lifecycle, "paused" | "suspended" | "stopped")
}

fn loopback_bridge_request<T>(
    authority: Option<&SpaceApiRouteEd25519Authority>,
    sandbox: &Sandbox,
    message: T,
) -> Result<tonic::Request<T>, ControllerError> {
    let mut request = tonic::Request::new(message);
    if let Some(route_token) = super::spaced::space_api_bridge_route_token(authority, sandbox)? {
        insert_space_api_route_ed25519_token(request.metadata_mut(), &route_token).map_err(
            |error| {
                ControllerError::service_unavailable(format!(
                    "attach SPACE API route token to loopback bridge request: {error}"
                ))
            },
        )?;
    }
    Ok(request)
}
