//! Resolving a data-plane session's route, its transport and route token, and checking ingress and
//! attach authorization.

use super::*;

#[derive(Debug, Clone, FromRow)]
pub(super) struct SessionRouteRow {
    pub(super) session_id: String,
    pub(super) protocol_session_id: Option<String>,
    pub(super) org_id: OrgId,
    pub(super) sandbox_id: SandboxId,
    pub(super) kind: String,
    pub(super) target_port: Option<i32>,
    pub(super) target_protocol: Option<String>,
    pub(super) target_path: Option<String>,
    pub(super) adapter_state_json: String,
    pub(super) route_generation: RouteGeneration,
    pub(super) current_node_id: Option<NodeId>,
    pub(super) backend_endpoint: Option<String>,
    pub(super) backend_transport: Option<String>,
    pub(super) revocation_epoch: i64,
    pub(super) revocation_status: String,
    pub(super) revocation_reason: Option<String>,
    pub(super) revocation_updated_at: OffsetDateTime,
    pub(super) org_revocation_epoch: i64,
    pub(super) scopes: Vec<String>,
    pub(super) token_hash: Option<String>,
    pub(super) token_expires_at: Option<OffsetDateTime>,
    pub(super) sandbox_active_node_id: Option<NodeId>,
    pub(super) sandbox_lifecycle: SandboxLifecycle,
    pub(super) node_address: Option<String>,
    pub(super) node_managed_by: Option<String>,
    pub(super) k8s_node_name: Option<String>,
    pub(super) direct_node_api_endpoint: Option<String>,
    pub(super) sandbox_pod_uid: Option<PodUid>,
    pub(super) execution_generation: Option<ExecutionGeneration>,
    pub(super) activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(super) backend_process_id: Option<String>,
}

pub(crate) async fn resolve_session_route(
    budget: Duration,
    activation: Option<&dyn crate::ingress_activation::IngressActivation>,
    pool: &sqlx::PgPool,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
    refresh_authority: Option<DataPlaneSessionRefreshAuthority>,
    request: ResolveDataPlaneSessionRouteRequest,
) -> Result<ResolveDataPlaneSessionRouteResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let token_sha256 = optional(request.token_sha256.as_deref());
    let required_scope = optional(request.required_scope.as_deref());
    let deadline = tokio::time::Instant::now() + budget;
    let mut activated = false;
    loop {
        let mut row = fetch_route_row(pool, &session_id, protocol_session_id.as_deref()).await?;
        validate_attach_authorization(
            &row,
            refresh_authority,
            token_sha256.as_deref(),
            required_scope.as_deref(),
        )?;
        let _waiter = if matches!(
            row.kind.parse::<SessionKind>(),
            Ok(SessionKind::Port | SessionKind::Protocol)
        ) {
            validate_ingress_session(&row)?;
            Some(
                pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(Some(
                    row.sandbox_id,
                ))?,
            )
        } else {
            None
        };
        let ingress = matches!(
            row.kind.parse::<SessionKind>(),
            Ok(SessionKind::Port | SessionKind::Protocol)
        );
        let sandbox_id = row.sandbox_id;
        let restoring = row.sandbox_lifecycle == SandboxLifecycle::Restoring;
        let unplaced =
            row.sandbox_active_node_id.is_none() || resolve_backend_endpoint(&row).is_none();
        let org_id = row.org_id;
        let activity = waiting_activity(&row.kind, row.backend_process_id.as_deref());
        let result = async {
            refuse_restoring_route(row.sandbox_lifecycle)?;
            if matches!(
                row.kind.parse::<SessionKind>(),
                Ok(SessionKind::Port | SessionKind::Protocol)
            ) {
                require_started_ingress(row.sandbox_lifecycle)?;
            }
            let current_node_id = row.sandbox_active_node_id.ok_or_else(|| {
                Status::failed_precondition("data-plane session sandbox is not assigned to a node")
            })?;
            if !ingress {
                hold_route_for_activation(pool, org_id, sandbox_id, row.sandbox_lifecycle).await?;
            }
            let backend_endpoint = resolve_backend_endpoint(&row).ok_or_else(|| {
                Status::failed_precondition(missing_backend_endpoint_message(
                    "data-plane session",
                    row.sandbox_active_node_id,
                    row.node_managed_by.as_deref(),
                    row.k8s_node_name.as_deref(),
                ))
            })?;
            let backend_transport = row
                .backend_transport
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(BACKEND_TRANSPORT_GRPC)
                .to_string();

            if row.current_node_id != Some(current_node_id)
                || row.backend_endpoint.as_deref() != Some(backend_endpoint.as_str())
                || row.backend_transport.as_deref() != Some(backend_transport.as_str())
            {
                row = update_route_row(
                    pool,
                    &session_id,
                    current_node_id,
                    &backend_endpoint,
                    &backend_transport,
                )
                .await?;
            }

            let route_generation = row.route_generation.get();
            let kind = row.kind.parse::<SessionKind>().map_err(|error| {
                Status::internal(format!("data-plane session kind is invalid: {error}"))
            })?;
            data_plane_session_transport(request.requested_transport, kind)?;
            if matches!(kind, SessionKind::Port | SessionKind::Protocol) {
                let port = row
                    .target_port
                    .and_then(|value| u16::try_from(value).ok())
                    .and_then(|value| GuestPort::try_from_u16(value).ok())
                    .ok_or_else(|| {
                        Status::failed_precondition("service session target port is invalid")
                    })?;
                prepare_current_ingress_worker(
                    deadline,
                    activation.is_some() && !activated,
                    async {
                        ensure_sandbox_service_port_ingress_path(
                            pool,
                            SandboxServicePortRouteIdentity {
                                sandbox_id: row.sandbox_id,
                                active_node_id: current_node_id,
                                route_generation: row
                                    .execution_generation
                                    .and_then(|value| u64::try_from(value.get()).ok())
                                    .and_then(|value| RouteGeneration::new(value).ok())
                                    .ok_or_else(|| {
                                        Status::failed_precondition(
                                            "service session execution generation is missing",
                                        )
                                    })?,
                                execution_generation: row.execution_generation,
                                pod_uid: row.sandbox_pod_uid.clone(),
                                substrate_kind: row
                                    .node_managed_by
                                    .as_deref()
                                    .and_then(|value| value.parse().ok()),
                                activation_attempt_id: row.activation_attempt_id,
                            },
                            port,
                            &backend_endpoint,
                            route_ed25519_authority,
                        )
                        .await?;
                        Ok(())
                    },
                )
                .await?;
            }
            let space_api_route_token = space_api_route_token_for_session(
                &row,
                row.current_node_id.unwrap_or(current_node_id),
                route_ed25519_authority,
            )?;
            Ok(ResolveDataPlaneSessionRouteResponse {
                session_id: row.session_id,
                protocol_session_id: row.protocol_session_id,
                route_generation,
                current_node_id: row.current_node_id.unwrap_or(current_node_id).to_string(),
                backend: Some(DataPlaneBackendEndpoint {
                    node_id: current_node_id.to_string(),
                    endpoint: row.backend_endpoint.unwrap_or(backend_endpoint),
                    transport: Some(backend_transport),
                }),
                revocation: Some(revocation_state(
                    row.revocation_epoch,
                    &row.revocation_status,
                    row.revocation_updated_at,
                    row.revocation_reason,
                    row.org_revocation_epoch,
                )?),
                sandbox_id: row.sandbox_id.to_string(),
                kind: row.kind,
                target_port: row
                    .target_port
                    .and_then(|value| u16::try_from(value).ok())
                    .map(u32::from),
                target_protocol: row.target_protocol,
                target_path: row.target_path,
                adapter_state_json: row.adapter_state_json,
                backend_process_id: row.backend_process_id,
                org_id: row.org_id.to_string(),
                space_api_route_token,
            })
        }
        .await;
        let error: Status = match result {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        let at_rest = if !ingress && error.code() == tonic::Code::FailedPrecondition && unplaced {
            match route_at_rest(pool, org_id, sandbox_id).await {
                Some(lifecycle)
                    if !implicit_wake_queued(activation, sandbox_id, activity, deadline).await =>
                {
                    Some(lifecycle)
                }
                _ => None,
            }
        } else {
            None
        };
        let error = match at_rest {
            Some(lifecycle) => {
                pplx_space_common_grpc::mark_session_route_at_rest(error, sandbox_id, lifecycle)
            }
            None => error,
        };
        let Some(owner) = activation
            .filter(|_| !activated && (restoring || activation_may_repair(ingress, &error)))
        else {
            return Err(error);
        };
        // A restoring row waits on its restore here; if that fails, the
        // caller still gets the retryable refusal rather than the waiter's
        // error.
        if let Err(prepare_error) = owner
            .prepare(
                sandbox_id,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
        {
            return Err(if restoring { error } else { prepare_error });
        }
        activated = true;
        // Re-read identity and authorization after activation; never replay bytes.
    }
}

/// The sandbox's lifecycle when nothing is about to give the session's
/// sandbox a node: it is not heading to running and no start, resume, restore
/// or wake is queued or running for it. The lifecycle is read again with the
/// operations, so one that just finished is not paired with the state it left.
/// A failed read answers `None`, which keeps the attach retryable as it always
/// was.
pub(super) async fn route_at_rest(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    sandbox_id: SandboxId,
) -> Option<SandboxLifecycle> {
    let target_id = pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string();
    let observed = match pplx_space_sandbox_store::activation_observation(
        pool, org_id, sandbox_id, &target_id,
    )
    .await
    {
        Ok(observed) => observed?,
        Err(error) => {
            tracing::warn!(%sandbox_id, %error, "could not read pending wakes for a session route");
            return None;
        }
    };
    let lifecycle = observed.lifecycle.parse::<SandboxLifecycle>().ok()?;
    (!heading_to_running(lifecycle) && !observed.activation_pending).then_some(lifecycle)
}

/// Running, or on its way there with nothing more to ask for.
fn heading_to_running(lifecycle: SandboxLifecycle) -> bool {
    matches!(
        lifecycle,
        SandboxLifecycle::Started
            | SandboxLifecycle::Creating
            | SandboxLifecycle::Starting
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Checkpointing
    )
}

/// Hold the route of a sandbox that still has a node but is not heading to
/// running while a start, resume, restore or wake is queued or running for
/// it. The node refuses work on a stopped sandbox, or on a paused one its
/// policy does not wake, as final, though the activation is about to make it
/// run; held, the attach waits for it as for any placement on its way. A
/// failed read routes as before.
pub(super) async fn hold_route_for_activation(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    sandbox_id: SandboxId,
    lifecycle: SandboxLifecycle,
) -> Result<(), Status> {
    if heading_to_running(lifecycle) {
        return Ok(());
    }
    let target_id = pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string();
    match pplx_space_sandbox_store::activation_observation(pool, org_id, sandbox_id, &target_id)
        .await
    {
        Ok(Some(observed))
            if observed.activation_pending
                && observed
                    .lifecycle
                    .parse::<SandboxLifecycle>()
                    .is_ok_and(|lifecycle| !heading_to_running(lifecycle)) =>
        {
            Err(Status::failed_precondition(
                "data-plane session sandbox is about to be activated",
            ))
        }
        Ok(_) => Ok(()),
        Err(error) => {
            tracing::warn!(%sandbox_id, %error, "could not read pending activations for a session route");
            Ok(())
        }
    }
}

/// What the attach does once the sandbox runs, as the node judges it against
/// the wake policy: the first attach of a terminal or process session starts
/// its process, a command; reconnecting to a process that already started,
/// and every file-watch call, is access.
pub(super) fn waiting_activity(
    kind: &str,
    backend_process_id: Option<&str>,
) -> ImplicitWakeActivity {
    match kind.parse::<SessionKind>() {
        Ok(SessionKind::Terminal | SessionKind::Process) if backend_process_id.is_none() => {
            ImplicitWakeActivity::Command
        }
        _ => ImplicitWakeActivity::Access,
    }
}

/// Ask the lifecycle owner to queue, or join, the wake a session on an
/// at-rest sandbox waits for, judged for the session's `activity`. Only its
/// definite "nothing can wake it" lets the route be answered as at rest; no
/// owner, an owner from before this request, or one that cannot be reached
/// is unknown, and leaves it retryable.
pub(super) async fn implicit_wake_queued(
    activation: Option<&dyn crate::ingress_activation::IngressActivation>,
    sandbox_id: SandboxId,
    activity: ImplicitWakeActivity,
    deadline: tokio::time::Instant,
) -> bool {
    let Some(activation) = activation else {
        return true;
    };
    let budget = deadline.saturating_duration_since(tokio::time::Instant::now());
    match activation
        .queue_implicit_wake(sandbox_id, activity, budget)
        .await
    {
        Ok(keep_waiting) => keep_waiting,
        Err(error) if error.code() == tonic::Code::Unimplemented => {
            tracing::debug!(%sandbox_id, %error, "lifecycle owner cannot queue a session's wake");
            true
        }
        Err(error) => {
            tracing::warn!(%sandbox_id, %error, "could not queue the wake a session waits for");
            true
        }
    }
}

pub(super) fn space_api_route_token_for_session(
    row: &SessionRouteRow,
    current_node_id: NodeId,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Option<ProtoSpaceApiRouteToken>, Status> {
    let kind = row.kind.parse::<SessionKind>().map_err(|error| {
        Status::internal(format!("data-plane session kind is invalid: {error}"))
    })?;
    let (purpose, service_port) = match kind {
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch => {
            (SpaceApiRoutePurpose::SpaceApiBridge, None)
        }
        SessionKind::Port | SessionKind::Protocol => {
            let port = row
                .target_port
                .and_then(|value| u16::try_from(value).ok())
                .and_then(|value| GuestPort::try_from_u16(value).ok())
                .ok_or_else(|| {
                    Status::failed_precondition("service session target port is invalid")
                })?;
            (SpaceApiRoutePurpose::ServicePortIngress, Some(port))
        }
    };
    let execution_generation = row.execution_generation;
    let signed_route_generation =
        required_route_generation(execution_generation, "data-plane session")?;
    space_api_route_token(
        SpaceApiRouteTokenParams {
            purpose,
            sandbox_id: row.sandbox_id,
            route_generation: signed_route_generation,
            current_node_id,
            substrate_kind: row
                .node_managed_by
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|error| {
                    Status::internal(format!(
                        "data-plane session node substrate is invalid: {error}"
                    ))
                })?,
            execution_generation,
            pod_uid: row.sandbox_pod_uid.clone(),
            activation_attempt_id: row.activation_attempt_id,
            service_port,
        },
        route_ed25519_authority,
    )
}

pub(super) fn data_plane_session_transport(
    requested_transport: Option<i32>,
    kind: SessionKind,
) -> Result<SandboxServiceRouteTransport, Status> {
    let transport = requested_transport
        .map(SandboxServiceRouteTransport::try_from)
        .transpose()
        .map_err(|_| Status::invalid_argument("requested_transport is invalid"))?
        .unwrap_or({
            if matches!(kind, SessionKind::Port | SessionKind::Protocol) {
                SandboxServiceRouteTransport::ServicePortIngress
            } else {
                SandboxServiceRouteTransport::Direct
            }
        });
    match (kind, transport) {
        (
            SessionKind::Port | SessionKind::Protocol,
            SandboxServiceRouteTransport::ServicePortIngress,
        ) => Ok(SandboxServiceRouteTransport::ServicePortIngress),
        (
            SessionKind::Port | SessionKind::Protocol,
            SandboxServiceRouteTransport::Direct | SandboxServiceRouteTransport::Unspecified,
        ) => Err(Status::invalid_argument(
            "port and protocol sessions require service port ingress transport",
        )),
        (_, SandboxServiceRouteTransport::Direct) => Ok(transport),
        (_, SandboxServiceRouteTransport::Unspecified) => Ok(SandboxServiceRouteTransport::Direct),
        (_, SandboxServiceRouteTransport::ServicePortIngress) => Err(Status::invalid_argument(
            "service port ingress transport requires a port or protocol session",
        )),
    }
}

pub(super) fn validate_ingress_session(row: &SessionRouteRow) -> Result<(), Status> {
    validate_ingress_session_access(
        &row.revocation_status,
        row.token_expires_at,
        OffsetDateTime::now_utc(),
    )
}

pub(super) fn validate_ingress_session_access(
    status: &str,
    expires: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> Result<(), Status> {
    if revocation_status(status)? != DataPlaneRevocationStatus::Active {
        return Err(Status::permission_denied("ingress session is revoked"));
    }
    if !expires.is_some_and(|expires| expires > now) {
        return Err(Status::unauthenticated("ingress session token is expired"));
    }
    Ok(())
}

pub(super) fn validate_attach_authorization(
    row: &SessionRouteRow,
    refresh_authority: Option<DataPlaneSessionRefreshAuthority>,
    token_sha256: Option<&str>,
    required_scope: Option<&str>,
) -> Result<(), Status> {
    if token_sha256.is_none() && required_scope.is_none() {
        // The data-plane edge refreshes the route of a session it already
        // attached without re-sending the attach secret. Any other caller
        // holding only a session ID must prove the attach, or the ID alone
        // would mint a route token for another tenant's session.
        return match refresh_authority {
            Some(DataPlaneSessionRefreshAuthority::Granted) => Ok(()),
            _ => Err(Status::unauthenticated(
                "data-plane attach token is required",
            )),
        };
    }

    let token_sha256 = token_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("data-plane attach token is required"))?;
    let stored_hash = row
        .token_hash
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("data-plane session has no attach token"))?;
    if stored_hash
        .as_bytes()
        .ct_eq(token_sha256.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(Status::unauthenticated("invalid data-plane attach token"));
    }

    match row.token_expires_at {
        Some(expires_at) if expires_at > OffsetDateTime::now_utc() => {}
        Some(_) => return Err(Status::unauthenticated("data-plane attach token expired")),
        None => {
            return Err(Status::unauthenticated(
                "data-plane attach token has no expiry",
            ));
        }
    }

    if let Some(required_scope) = required_scope
        && !row.scopes.iter().any(|scope| scope == required_scope)
    {
        return Err(Status::permission_denied(
            "data-plane attach token does not grant requested scope",
        ));
    }

    Ok(())
}
