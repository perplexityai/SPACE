fn map_activation_operation_error(error: crate::db::ActivationOperationError) -> Status {
    use crate::db::ActivationOperationError;
    match error {
        ActivationOperationError::Database(error) => Status::internal(error.to_string()),
        ActivationOperationError::MissingSandbox => Status::not_found(error.to_string()),
        ActivationOperationError::RouteMismatch
        | ActivationOperationError::Superseded
        | ActivationOperationError::InvalidTarget
        | ActivationOperationError::CleanupPending => Status::aborted(error.to_string()),
    }
}

use super::activation_support::{
    GuestActivityKind, WakeStartConfig, remaining_wake_budget, space_api_route_identity_to_wire,
    wake_ready_timeout_secs, wake_target_state_to_proto, wake_transition_for_state,
};
use super::network_runtime::{
    NetGatewayRouteIdentityPushOutcome, NetGatewayRouteSecurityPushOutcome,
    NetGatewayRouteVerificationKeyPushOutcome, push_route_security_with_retry_policy,
};
use super::restart_admission::wait_for_activation_admission;
use super::tail_support::*;
use super::validation::duration_ceil_secs;
use super::*;

/// How long a waiter on another owner's transition sleeps before re-reading
/// the row: 200 ms, or less when the wake budget is nearly spent.
fn transition_poll_interval(
    deadline: Option<tokio::time::Instant>,
    state: SandboxState,
) -> Result<Duration, Status> {
    Ok(remaining_wake_budget(deadline, state)?
        .map(|remaining| remaining.min(Duration::from_millis(200)))
        .unwrap_or(Duration::from_millis(200)))
}

pub(super) const WAKE_ROUTE_SECURITY_SYNC_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const WAKE_ROUTE_SECURITY_CONNECT_RETRY_POLICY: pplx_space_common_grpc::GrpcRetryPolicy =
    pplx_space_common_grpc::GrpcRetryPolicy {
        max_retries: 8,
        initial_delay: Duration::from_millis(50),
        backoff_multiplier: 2.0,
        max_delay: Duration::from_millis(500),
        max_elapsed: Duration::from_secs(2),
        jitter_factor: 0.2,
        max_server_retry_after: Duration::from_millis(500),
    };

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResidentWakeCompletion {
    AlreadySatisfied,
    Resumed,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum WakeRouteSecuritySyncError {
    #[error("gateway route-security synchronization timed out")]
    Timeout,
    #[error("gateway route-security synchronization failed: {0}")]
    Push(#[source] Status),
    #[error("authoritative wake route identity is absent")]
    IdentityAbsent,
    #[error("gateway route identity conflicts with durable sandbox state")]
    IdentityDiverged,
}

impl WakeRouteSecuritySyncError {
    fn into_status(self, sandbox_id: &str) -> Status {
        match self {
            Self::Timeout => Status::aborted(format!(
                "wake route-security synchronization timed out for sandbox {sandbox_id}; retry wake"
            )),
            Self::Push(error) => Status::aborted(format!(
                "wake route-security synchronization failed for sandbox {sandbox_id} with {}: {}; retry wake",
                error.code(),
                error.message()
            )),
            Self::IdentityAbsent => Status::failed_precondition(format!(
                "authoritative wake route identity is absent for sandbox {sandbox_id}"
            )),
            Self::IdentityDiverged => Status::failed_precondition(format!(
                "net-gateway route identity conflicts with durable state for sandbox {sandbox_id}; an operator must run space-sbxmgr cycle-net-gateway {sandbox_id} --execute"
            )),
        }
    }
}

pub(super) fn require_wake_route_security_sync(
    outcome: NetGatewayRouteSecurityPushOutcome,
) -> Result<(), WakeRouteSecuritySyncError> {
    match outcome.identity {
        NetGatewayRouteIdentityPushOutcome::Applied { .. } => match outcome.verification_keys {
            Some(NetGatewayRouteVerificationKeyPushOutcome::Applied) | None => Ok(()),
        },
        NetGatewayRouteIdentityPushOutcome::Absent => {
            Err(WakeRouteSecuritySyncError::IdentityAbsent)
        }
        NetGatewayRouteIdentityPushOutcome::Diverged => {
            Err(WakeRouteSecuritySyncError::IdentityDiverged)
        }
    }
}

fn wake_start_route_identity(
    start: &WakeStartConfig,
) -> Option<crate::proto::SpaceApiRouteIdentity> {
    space_api_route_identity_to_wire(
        start
            .requested_space_api_route_identity
            .as_ref()
            .unwrap_or(&start.space_api_route_identity),
    )
}

impl WakeStartConfig {
    fn resume_policy(&self) -> Option<(&AutoLifecyclePolicy, Option<ExecutionGeneration>)> {
        self.activation_auto_lifecycle
            .as_ref()
            .map(|policy| (policy, self.expected_execution_generation))
    }

    /// A readiness budget requests blocking admission, including `Some(0)`;
    /// absence preserves the nonblocking request's absent timeout.
    pub(super) fn start_request(
        &self,
        sandbox_id: &str,
        restore_from_suspend: bool,
        blocking_ready_timeout_secs: Option<u32>,
    ) -> Result<StartRequest, Status> {
        Ok(StartRequest {
            activation_auto_lifecycle: self.activation_auto_lifecycle.clone().map(Into::into),
            sandbox_id: sandbox_id.to_string(),
            vcpu: self.vcpu,
            memory_mib: self.memory_mib,
            checkpoint: restore_from_suspend.then(|| "suspend".to_string()),
            blocking: blocking_ready_timeout_secs.is_some(),
            netns_path: self.network_context.netns_path_for_launch()?,
            ready_timeout_secs: blocking_ready_timeout_secs,
            target_state: wake_target_state_to_proto(self.target_state),
            suspend_restore_storage_uri: restore_from_suspend
                .then(|| self.suspend_restore_storage_uri.clone())
                .flatten(),
            suspend_restore_source_node: restore_from_suspend
                .then(|| self.suspend_restore_source_node.clone())
                .flatten(),
            expected_local_artifact_id: restore_from_suspend
                .then(|| self.expected_local_artifact_id.clone())
                .flatten(),
            route_identity: wake_start_route_identity(self),
            operation_activation_attempt_id: self
                .operation_activation_attempt_id
                .map(|id| id.to_string()),
            ..Default::default()
        })
    }
}

impl SandboxManagerService {
    pub(super) fn authenticated_host_control_sandbox<T>(
        &self,
        request: &Request<T>,
    ) -> Result<crate::db::SandboxRow, Status> {
        let connect_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .ok_or_else(|| Status::unauthenticated("host control connection info missing"))?;
        let peer_cred = connect_info
            .peer_cred
            .ok_or_else(|| Status::unauthenticated("host control peer credentials missing"))?;
        let pid = peer_cred
            .pid()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| Status::unauthenticated("host control peer pid unavailable"))?;
        let uid = peer_cred.uid();
        self.lookup_sandbox_by_spaced_host_process(pid, uid)
    }

    pub(super) fn authenticated_net_gateway_host_control_sandbox<T>(
        &self,
        request: &Request<T>,
    ) -> Result<crate::db::SandboxRow, Status> {
        let connect_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .ok_or_else(|| Status::unauthenticated("host control connection info missing"))?;
        let peer_cred = connect_info
            .peer_cred
            .ok_or_else(|| Status::unauthenticated("host control peer credentials missing"))?;
        let pid = peer_cred
            .pid()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| Status::unauthenticated("host control peer pid unavailable"))?;
        let uid = peer_cred.uid();
        self.lookup_sandbox_by_net_gateway_process(pid, uid)
    }

    pub(super) async fn set_guest_activity_state(
        &self,
        sandbox_id: &str,
        idle: bool,
    ) -> Result<bool, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if sandbox.state != SandboxState::Started {
            return Ok(false);
        }
        if !sandbox.auto_lifecycle.has_guest_activity_stages() {
            return Ok(false);
        }

        let now = now_secs();
        let mut runtime_state = sandbox.auto_lifecycle_state.clone();
        let applied = if idle {
            notify_guest_activity_state(&mut runtime_state, &sandbox.auto_lifecycle, true, now)
        } else {
            let Some(cap) = sandbox.auto_lifecycle.guest_activity_hold_cap() else {
                return Ok(false);
            };
            extend_guest_activity_idle_epoch(
                &mut runtime_state,
                &sandbox.auto_lifecycle,
                now,
                duration_ceil_secs(cap.as_duration()),
            )
        };
        recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);

        let db = self.db_conn()?;
        crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state)
            .map_err(|e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")))?;

        Ok(applied)
    }

    pub(super) async fn request_guest_active_for(
        &self,
        sandbox_id: &str,
        hold_for: Duration,
    ) -> Result<HostControlActiveForOutcome, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if sandbox.state != SandboxState::Started {
            return Ok(HostControlActiveForOutcome::Denied(HostControlDenied {
                reason: format!("sandbox is {}", sandbox.state),
            }));
        }

        let guest_activity_enabled = sandbox.auto_lifecycle.has_guest_activity_stages();
        if !guest_activity_enabled {
            return Ok(HostControlActiveForOutcome::Denied(HostControlDenied {
                reason: "guest activity control is not enabled for this sandbox".to_string(),
            }));
        }

        let guest_hold_cap = sandbox
            .auto_lifecycle
            .guest_activity_hold_cap()
            .map(|duration| duration.as_duration());
        let Some(cap) = guest_hold_cap else {
            return Ok(HostControlActiveForOutcome::Denied(HostControlDenied {
                reason: "guest active hold cap is not configured for this sandbox".to_string(),
            }));
        };
        let granted_for = hold_for.min(cap);

        let mut runtime_state = sandbox.auto_lifecycle_state.clone();
        extend_guest_activity_idle_epoch(
            &mut runtime_state,
            &sandbox.auto_lifecycle,
            now_secs(),
            duration_ceil_secs(granted_for),
        );
        recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);

        let db = self.db_conn()?;
        crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state)
            .map_err(|e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")))?;

        Ok(HostControlActiveForOutcome::Accepted(
            HostControlActiveForAccepted {
                granted_for: Some(duration_to_proto(granted_for)),
            },
        ))
    }

    pub(super) async fn ensure_active_for_authorized_guest_activity(
        &self,
        route_auth: &super::auth_lookup::SpaceApiBridgeRouteAuthorization,
        sandbox_id: &str,
        implicit: bool,
        wait_budget: Option<Duration>,
        activity: GuestActivityKind,
    ) -> Result<crate::db::SandboxRow, Status> {
        self.ensure_active_for_guest_activity_with_start(
            Some(route_auth),
            sandbox_id,
            implicit,
            wait_budget,
            activity,
            WakeStartConfig::default(),
        )
        .await
    }

    pub(super) async fn ensure_active_for_read_only_guest_activity(
        &self,
        route_auth: Option<super::auth_lookup::SpaceApiBridgeRouteAuthorization>,
        sandbox_id: &str,
        implicit: bool,
        wait_budget: Option<Duration>,
        activity: GuestActivityKind,
        method: &'static str,
    ) -> Result<crate::db::SandboxRow, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        super::guest_image::require_agent(&sandbox).map_err(|refusal| match &route_auth {
            Some(auth) => auth.refuse_sandbox(&sandbox, refusal),
            None => refusal,
        })?;
        let state_writer = match self.state_writer_lease(method) {
            Ok(state_writer) => state_writer,
            Err(drain_error) => {
                let sandbox = self.lookup_sandbox(sandbox_id)?;
                if sandbox.state != SandboxState::Started {
                    return Err(match &route_auth {
                        Some(route_auth) => route_auth.refuse_sandbox(&sandbox, drain_error),
                        None => drain_error,
                    });
                }
                if let Some(route_auth) = &route_auth {
                    route_auth.authorize_sandbox(&sandbox)?;
                }
                return Ok(sandbox);
            }
        };
        let service = self.clone();
        let sandbox_id = sandbox_id.to_owned();
        self.task_tracker
            .spawn(super::drain::with_state_writer(
                state_writer,
                async move {
                    service
                        .ensure_active_with_start(
                            &sandbox_id,
                            implicit,
                            wait_budget,
                            Some(activity),
                            WakeStartConfig::default(),
                            route_auth.as_ref(),
                        )
                        .await
                }
                // The request normally awaits this wake, so it stays in the
                // request's trace. A request dropped mid-wake leaves its span
                // open until the wake ends, so that span then reports the
                // wake's duration rather than the request's.
                .in_current_span(),
            ))
            .await
            .map_err(|error| Status::internal(format!("guest wake task failed: {error}")))?
    }

    pub(super) async fn ensure_active_for_guest_activity_with_start(
        &self,
        route_auth: Option<&super::auth_lookup::SpaceApiBridgeRouteAuthorization>,
        sandbox_id: &str,
        implicit: bool,
        wait_budget: Option<Duration>,
        activity: GuestActivityKind,
        start: WakeStartConfig,
    ) -> Result<crate::db::SandboxRow, Status> {
        self.ensure_active_with_start(
            sandbox_id,
            implicit,
            wait_budget,
            Some(activity),
            start,
            route_auth,
        )
        .await
    }

    /// Adopt the route identity carried by a control-plane wake whose target
    /// lifecycle is already satisfied. Operator wakes without an identity keep
    /// their historical no-op behavior.
    fn adopt_wake_route_identity_locally(
        &self,
        sandbox_id: &str,
        start: &WakeStartConfig,
    ) -> Result<(), Status> {
        if !start.authoritative_route_identity {
            return Ok(());
        }
        if start.provisional_activation_attempt {
            use pplx_space_metrics::sandbox::worker::{
                RouteIdentityAdoptionOutcome, RouteIdentityAdoptionReason, route_identity_adoption,
            };
            route_identity_adoption(
                RouteIdentityAdoptionOutcome::Inert,
                RouteIdentityAdoptionReason::ProvisionalNotAdopted,
            );
            tracing::warn!(
                %sandbox_id,
                "control-plane wake route identity has no activation attempt; retaining the \
                 current runtime identity"
            );
            return Ok(());
        }
        let sandbox_id = sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::internal(format!("sandbox id is invalid: {error}")))?;
        self.adopt_authoritative_route_identity(sandbox_id, &start.space_api_route_identity)
    }

    async fn finish_resident_wake(
        &self,
        sandbox_id: &str,
        start: &WakeStartConfig,
        activity: Option<GuestActivityKind>,
    ) -> Result<crate::db::SandboxRow, Status> {
        let verification_bundle = self.route_ed25519_verification_bundle.load_full();
        self.finish_resident_wake_with_gateway_push(
            sandbox_id,
            start,
            activity,
            move |sandbox_id, identity| async move {
                push_route_security_with_retry_policy(
                    &sandbox_id,
                    &identity,
                    verification_bundle.as_deref(),
                    WAKE_ROUTE_SECURITY_CONNECT_RETRY_POLICY,
                )
                .await
            },
        )
        .await
    }

    /// Finish a resident Wake while the wake lock remains held.
    ///
    /// After a real resume, recording precedes every fallible completion step
    /// because the active runtime is durable lifecycle truth even if identity
    /// persistence or gateway synchronization subsequently fails. An
    /// already-satisfied wake validates identity adoption before it is recorded.
    pub(super) async fn finish_resident_wake_with_gateway_push<P, PFut>(
        &self,
        sandbox_id: &str,
        start: &WakeStartConfig,
        activity: Option<GuestActivityKind>,
        push_route_security: P,
    ) -> Result<crate::db::SandboxRow, Status>
    where
        P: FnOnce(String, SpaceApiRouteIdentity) -> PFut,
        PFut: std::future::Future<Output = Result<NetGatewayRouteSecurityPushOutcome, Status>>,
    {
        self.finish_resident_wake_completion_with_gateway_push(
            sandbox_id,
            start,
            activity,
            ResidentWakeCompletion::AlreadySatisfied,
            push_route_security,
        )
        .await
    }

    async fn finish_resident_wake_completion_with_gateway_push<P, PFut>(
        &self,
        sandbox_id: &str,
        start: &WakeStartConfig,
        activity: Option<GuestActivityKind>,
        completion: ResidentWakeCompletion,
        push_route_security: P,
    ) -> Result<crate::db::SandboxRow, Status>
    where
        P: FnOnce(String, SpaceApiRouteIdentity) -> PFut,
        PFut: std::future::Future<Output = Result<NetGatewayRouteSecurityPushOutcome, Status>>,
    {
        if completion == ResidentWakeCompletion::Resumed {
            self.auto_lifecycle.record_wake_completed(sandbox_id);
        }
        self.adopt_wake_route_identity_locally(sandbox_id, start)?;
        if completion == ResidentWakeCompletion::AlreadySatisfied {
            self.auto_lifecycle.record_wake_completed(sandbox_id);
        }
        let adopted = self.lookup_sandbox(sandbox_id)?;
        if let Some(activity) = activity {
            self.touch_runtime_state_for_guest_activity(&adopted, activity)?;
        }
        if !start.authoritative_route_identity || start.provisional_activation_attempt {
            return self.lookup_sandbox(sandbox_id);
        }

        // Lifecycle completion is durable even if gateway synchronization or
        // the request later fails. Legacy central publication keeps its lease
        // until route_activation_id is initialized by the post-ack CAS.
        if let Some(operation) = start.operation_activation_attempt_id
            && adopted.state == start.target_state
        {
            // Wake(Paused) on Started deliberately remains Started. Preserve
            // that no-op contract without certifying an unmet Paused target.
            crate::db::complete_resident_wake_operation(
                &*self.db_conn()?,
                sandbox_id,
                &start.space_api_route_identity,
                operation,
                start.target_state,
            )
            .map_err(map_activation_operation_error)?;
        }

        tokio::time::timeout(
            WAKE_ROUTE_SECURITY_SYNC_TIMEOUT,
            push_route_security(adopted.id, adopted.space_api_route_identity),
        )
        .await
        .map_err(|_| WakeRouteSecuritySyncError::Timeout)
        .and_then(|result| result.map_err(WakeRouteSecuritySyncError::Push))
        .and_then(require_wake_route_security_sync)
        .map_err(|error| error.into_status(sandbox_id))?;

        self.lookup_sandbox(sandbox_id)
    }

    async fn finish_resident_wake_after_resume<F>(
        &self,
        sandbox_id: &str,
        start: &WakeStartConfig,
        activity: Option<GuestActivityKind>,
        resume: F,
    ) -> Result<crate::db::SandboxRow, Status>
    where
        F: std::future::Future<Output = Result<(), Status>>,
    {
        let verification_bundle = self.route_ed25519_verification_bundle.load_full();
        self.finish_resident_wake_after_resume_with_gateway_push(
            sandbox_id,
            start,
            activity,
            resume,
            move |sandbox_id, identity| async move {
                push_route_security_with_retry_policy(
                    &sandbox_id,
                    &identity,
                    verification_bundle.as_deref(),
                    WAKE_ROUTE_SECURITY_CONNECT_RETRY_POLICY,
                )
                .await
            },
        )
        .await
    }

    pub(super) async fn finish_resident_wake_after_resume_with_gateway_push<F, P, PFut>(
        &self,
        sandbox_id: &str,
        start: &WakeStartConfig,
        activity: Option<GuestActivityKind>,
        resume: F,
        push_route_security: P,
    ) -> Result<crate::db::SandboxRow, Status>
    where
        F: std::future::Future<Output = Result<(), Status>>,
        P: FnOnce(String, SpaceApiRouteIdentity) -> PFut,
        PFut: std::future::Future<Output = Result<NetGatewayRouteSecurityPushOutcome, Status>>,
    {
        resume.await?;
        self.finish_resident_wake_completion_with_gateway_push(
            sandbox_id,
            start,
            activity,
            ResidentWakeCompletion::Resumed,
            push_route_security,
        )
        .await
    }

    async fn ensure_active_with_start(
        &self,
        sandbox_id: &str,
        implicit: bool,
        wait_budget: Option<Duration>,
        activity: Option<GuestActivityKind>,
        start: WakeStartConfig,
        route_auth: Option<&super::auth_lookup::SpaceApiBridgeRouteAuthorization>,
    ) -> Result<crate::db::SandboxRow, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if activity.is_some() {
            super::guest_image::require_agent(&sandbox).map_err(|refusal| match route_auth {
                Some(auth) => auth.refuse_sandbox(&sandbox, refusal),
                None => refusal,
            })?;
        }

        super::activation_support::validate_route_identity_sandbox(
            &start.space_api_route_identity,
            &sandbox.id,
        )?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        // A missing grpc-timeout must not turn observation of another owner's
        // transition into an immortal detached waiter. Include lock admission
        // in the readiness budget; owned activation work retains its own bounds.
        let wait_deadline = Some(
            tokio::time::Instant::now()
                + wait_budget.unwrap_or(Duration::from_secs(
                    DEFAULT_BLOCKING_READY_TIMEOUT_SECS.into(),
                )),
        );
        let _guard = wait_for_activation_admission(&sandbox.id, wake_lock.lock()).await?;
        // At most once per Wake: a replay of a retired upgrade answers
        // `Upgraded` without booting, and the row it leaves must not send the
        // loop back into the upgrade.
        let mut runtime_upgrade = start.runtime_upgrade.as_ref();
        loop {
            let sandbox = self.lookup_sandbox(&sandbox.id)?;
            let wake_policy =
                if sandbox.auto_lifecycle_authority == crate::db::PolicyAuthority::Local {
                    &sandbox.auto_lifecycle
                } else {
                    start
                        .activation_auto_lifecycle
                        .as_ref()
                        .unwrap_or(&sandbox.auto_lifecycle)
                };
            // Teardown clears the active route binding. Refusing implicit wake
            // needs no complete runtime identity and must not touch activity or
            // bind an operation, but surviving identity mismatches still fence.
            if implicit
                && matches!(
                    sandbox.state,
                    SandboxState::Created
                        | SandboxState::Stopped
                        | SandboxState::Paused
                        | SandboxState::Suspended
                )
                && !activity.is_some_and(|activity| activity.allows_implicit_wake(wake_policy))
            {
                let refusal = pplx_space_common_grpc::mark_sandbox_not_running(
                    Status::failed_precondition(format!(
                        "sandbox is {}, not started",
                        sandbox.state
                    )),
                    sandbox.state.as_str(),
                );
                return Err(match route_auth {
                    Some(route_auth) => route_auth.refuse_sandbox(&sandbox, refusal),
                    None => refusal,
                });
            }
            // The route may have changed while admission was queued. Authorize
            // the current incarnation under wake exclusion before touching
            // activity clocks, binding an operation, or resuming the runtime.
            if let Some(route_auth) = route_auth {
                route_auth.authorize_sandbox(&sandbox)?;
            }
            if let Some(operation) = start.operation_activation_attempt_id {
                crate::db::bind_activation_operation(
                    &*self.db_conn()?,
                    &sandbox.id,
                    &start.space_api_route_identity,
                    operation,
                )
                .map_err(map_activation_operation_error)?;
            }
            match sandbox.state {
                SandboxState::Started => {
                    // Asymmetry note: wake with `target_state = Paused`
                    // against an already-Started sandbox does not pause
                    // it here. The drain pipeline never reaches this
                    // case (it suspends first, so the Suspended arm
                    // below handles target_state correctly), but a
                    // future caller passing this combination should use
                    // the explicit Pause RPC rather than expecting wake
                    // to drive a Started → Paused transition. We log
                    // and continue rather than fail so the existing
                    // wake-on-started no-op contract is preserved.
                    if start.target_state == SandboxState::Paused {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            "wake target_state=Paused requested on already-Started sandbox; \
                             leaving Started — use Pause RPC for Started→Paused transitions"
                        );
                    }
                    return self
                        .finish_resident_wake(&sandbox.id, &start, activity)
                        .await;
                }
                SandboxState::Paused => {
                    // target_state=Paused on a Paused sandbox is a no-op:
                    // the lifecycle is already at the target, no resume
                    // needed. Guest-activity callers still touch their
                    // condition clocks; explicit lifecycle wake leaves
                    // activity timers unchanged.
                    if start.target_state == SandboxState::Paused {
                        return self
                            .finish_resident_wake(&sandbox.id, &start, activity)
                            .await;
                    }
                    let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
                    let _guard =
                        wait_for_activation_admission(&sandbox.id, artifact_lock.lock()).await?;
                    return self
                        .finish_resident_wake_after_resume(
                            &sandbox.id,
                            &start,
                            activity,
                            self.resume_after_wake_lock(
                                &sandbox.id,
                                start.resume_policy(),
                                true,
                                wake_ready_timeout_secs(
                                    wait_deadline,
                                    DEFAULT_RESUME_READY_TIMEOUT_SECS,
                                    sandbox.state,
                                )?,
                            ),
                        )
                        .await;
                }
                SandboxState::Suspended | SandboxState::Stopped
                    if let Some(upgrade) = runtime_upgrade.take() =>
                {
                    // The upgrade boots the disk under this node's checkpoint, so
                    // that checkpoint must be the one the upgrade was decided
                    // from: the row's pinned local artifact must be the one the
                    // Wake names. Staging pins the staged checkpoint, so the
                    // check holds across retries; a stale copy here is refused,
                    // never booted.
                    if sandbox.state == SandboxState::Suspended
                        && (upgrade.checkpoint_local_artifact_id().is_none()
                            || sandbox.current_local_artifact_id
                                != upgrade.checkpoint_local_artifact_id())
                    {
                        return Err(Status::failed_precondition(format!(
                            "sandbox {}'s checkpoint on this node isn't the one its runtime \
                             upgrade was decided from; refusing to boot it",
                            sandbox.id
                        )));
                    }
                    let ready_timeout_secs = wake_ready_timeout_secs(
                        wait_deadline,
                        DEFAULT_BLOCKING_READY_TIMEOUT_SECS,
                        sandbox.state,
                    )?;
                    // The upgrade boots the cold start this Wake would run for
                    // a stopped sandbox, never the checkpoint. Boxed: the
                    // upgrade is a large future, and inlined it would grow every
                    // Wake's state machine, upgrade or not, past the stack of an
                    // unoptimized build.
                    let outcome = Box::pin(self.wake_runtime_upgrade(
                        upgrade,
                        start.start_request(&sandbox.id, false, Some(ready_timeout_secs))?,
                    ))
                    .await?;
                    if matches!(
                        outcome,
                        super::runtime_upgrade::RuntimeUpgradeOutcome::AlreadyOnTarget
                    ) {
                        // Nothing was fenced or booted; the next pass starts or
                        // restores the sandbox as a Wake without the upgrade.
                        tracing::info!(
                            sandbox_id = %sandbox.id,
                            state = %sandbox.state,
                            "wake runtime upgrade found the sandbox already on its target"
                        );
                    }
                }
                SandboxState::Suspended => {
                    self.start_sandbox_with_authority(
                        start.start_request(
                            &sandbox.id,
                            true,
                            Some(wake_ready_timeout_secs(
                                wait_deadline,
                                DEFAULT_BLOCKING_READY_TIMEOUT_SECS,
                                sandbox.state,
                            )?),
                        )?,
                        false,
                        Self::sandbox_owner_local_route_identity_authority(&sandbox),
                    )
                    .await?;
                }
                SandboxState::Created | SandboxState::Stopped => {
                    self.start_sandbox_with_authority(
                        start.start_request(
                            &sandbox.id,
                            false,
                            Some(wake_ready_timeout_secs(
                                wait_deadline,
                                DEFAULT_BLOCKING_READY_TIMEOUT_SECS,
                                sandbox.state,
                            )?),
                        )?,
                        false,
                        Self::sandbox_owner_local_route_identity_authority(&sandbox),
                    )
                    .await?;
                }
                SandboxState::Starting => {
                    // Another owner is starting the sandbox; its commit ends
                    // the wait at once.
                    let poll_interval = transition_poll_interval(wait_deadline, sandbox.state)?;
                    wait_for_activation_admission(
                        &sandbox.id,
                        super::activation_support::wait_while_starting(
                            super::activation_support::start_succeeded_notify(),
                            poll_interval,
                            || {
                                Ok(self.lookup_sandbox(&sandbox.id)?.state
                                    == SandboxState::Starting)
                            },
                        ),
                    )
                    .await??;
                }
                SandboxState::Creating
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
                | SandboxState::Suspending
                | SandboxState::Restoring
                | SandboxState::Stopping
                | SandboxState::CleanupRequired => {
                    let sleep_for = transition_poll_interval(wait_deadline, sandbox.state)?;
                    wait_for_activation_admission(&sandbox.id, tokio::time::sleep(sleep_for))
                        .await?;
                }
                SandboxState::Deleting
                | SandboxState::CleanupFailed
                | SandboxState::Quarantined
                | SandboxState::Failed => {
                    return Err(pplx_space_common_grpc::mark_sandbox_not_running(
                        Status::failed_precondition(format!(
                            "sandbox is {}, cannot wake",
                            sandbox.state
                        )),
                        sandbox.state.as_str(),
                    ));
                }
            }
        }
    }

    pub(super) async fn wake_with_start_config(
        &self,
        sandbox_id: String,
        wait_budget: Option<Duration>,
        start: WakeStartConfig,
        blocking: bool,
        activity: Option<GuestActivityKind>,
    ) -> Result<Response<WakeResponse>, Status> {
        let transition = if blocking {
            let initial_state = self.lookup_sandbox(&sandbox_id)?.state;

            self.ensure_active_with_start(
                &sandbox_id,
                activity.is_some(),
                wait_budget,
                activity,
                start,
                None,
            )
            .await?;

            wake_transition_for_state(initial_state)?
        } else {
            self.start_wake_transition_without_ready_wait(&sandbox_id, start)
                .await?
        };

        let sandbox = self.lookup_sandbox(&sandbox_id)?;
        Ok(Response::new(WakeResponse {
            sandbox_id,
            transition: transition as i32,
            route_identity: space_api_route_identity_to_wire(&sandbox.space_api_route_identity),
        }))
    }

    pub(super) async fn start_wake_transition_without_ready_wait(
        &self,
        sandbox_id: &str,
        start: WakeStartConfig,
    ) -> Result<WakeTransition, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        super::activation_support::validate_route_identity_sandbox(
            &start.space_api_route_identity,
            &sandbox.id,
        )?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wait_for_activation_admission(&sandbox.id, wake_lock.lock()).await?;

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        let transition = wake_transition_for_state(sandbox.state)?;
        if let Some(operation) = start.operation_activation_attempt_id {
            crate::db::bind_activation_operation(
                &*self.db_conn()?,
                &sandbox.id,
                &start.space_api_route_identity,
                operation,
            )
            .map_err(map_activation_operation_error)?;
        }
        match sandbox.state {
            SandboxState::Started => {
                self.finish_resident_wake(&sandbox.id, &start, None).await?;
                return Ok(transition);
            }
            SandboxState::Paused => {
                let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
                let _guard =
                    wait_for_activation_admission(&sandbox.id, artifact_lock.lock()).await?;
                self.finish_resident_wake_after_resume(
                    &sandbox.id,
                    &start,
                    None,
                    self.resume_after_wake_lock(
                        &sandbox.id,
                        start.resume_policy(),
                        false,
                        DEFAULT_RESUME_READY_TIMEOUT_SECS,
                    ),
                )
                .await?;
                return Ok(transition);
            }
            SandboxState::Suspended => {
                self.start_sandbox_with_authority(
                    start.start_request(&sandbox.id, true, None)?,
                    false,
                    Self::sandbox_owner_local_route_identity_authority(&sandbox),
                )
                .await?;
            }
            SandboxState::Created | SandboxState::Stopped => {
                self.start_sandbox_with_authority(
                    start.start_request(&sandbox.id, false, None)?,
                    false,
                    Self::sandbox_owner_local_route_identity_authority(&sandbox),
                )
                .await?;
            }
            SandboxState::Creating
            | SandboxState::Starting
            | SandboxState::Pausing
            | SandboxState::Resuming
            | SandboxState::Checkpointing
            | SandboxState::Suspending
            | SandboxState::Restoring
            | SandboxState::Stopping
            | SandboxState::CleanupRequired => {}
            SandboxState::Deleting
            | SandboxState::CleanupFailed
            | SandboxState::Quarantined
            | SandboxState::Failed => {
                unreachable!("wake_transition_for_state rejects terminal states")
            }
        }

        // Record while the wake lock is still held. A suspend waiting on this
        // lock can acquire it as soon as Wake returns; its supersession check
        // must already observe this activation or it can tear down the runtime
        // Wake just handed off. Resident transitions return above and record
        // through `finish_resident_wake_completion_with_gateway_push`.
        self.auto_lifecycle.record_wake_completed(&sandbox.id);

        Ok(transition)
    }
}
