//! The ECW sandbox service route: resolution, its token, and waiting for the sandbox to be
//! command-ready.

use super::*;

pub(super) const SANDBOX_SERVICE_ROUTE_COMMAND_READY_RPC_TIMEOUT: Duration =
    Duration::from_secs(10);

pub(super) const SANDBOX_SERVICE_ROUTE_COMMAND_READY_WAIT_TIMEOUT: prost_types::Duration =
    prost_types::Duration {
        seconds: 8,
        nanos: 0,
    };

#[derive(Debug, Clone, FromRow)]
pub(super) struct SandboxServiceRouteRow {
    pub(super) sandbox_id: SandboxId,
    pub(super) org_id: OrgId,
    pub(super) template_id: Option<String>,
    pub(super) lifecycle: String,
    pub(super) active_node_id: Option<NodeId>,
    pub(super) node_address: Option<String>,
    pub(super) node_managed_by: Option<NodeManagedBy>,
    pub(super) k8s_node_name: Option<String>,
    pub(super) direct_node_api_endpoint: Option<String>,
    pub(super) sandbox_pod_uid: Option<PodUid>,
    pub(super) execution_generation: Option<ExecutionGeneration>,
    pub(super) activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(super) ecw_credential_epoch: EcwCredentialEpoch,
    pub(super) command_ready_pending: bool,
}

pub(crate) async fn unstable_resolve_sandbox_service_route(
    budget: Duration,
    activation: Option<&dyn crate::ingress_activation::IngressActivation>,
    pool: &sqlx::PgPool,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
    ecw_credential_keyring: &EcwCredentialKeyring,
    request: UnstableResolveSandboxServiceRouteRequest,
) -> Result<UnstableResolveSandboxServiceRouteResponse, Status> {
    let sandbox_id = required(&request.sandbox_id, "sandbox_id")?
        .parse::<SandboxId>()
        .map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))?;
    let port = sandbox_service_route_port(request.port)?;
    let route_kind = sandbox_service_route_kind(request.route_kind)?;
    let transport = sandbox_service_route_transport(request.transport, route_kind)?;
    let deadline = tokio::time::Instant::now() + budget;
    let mut activated = false;
    loop {
        let row = fetch_sandbox_service_route_row(pool, sandbox_id).await?;
        validate_unstable_sandbox_service_route_authorization(
            pool,
            &row,
            route_kind,
            ecw_credential_keyring,
            &request,
        )
        .await?;

        let _waiter = if transport == SandboxServiceRouteTransport::ServicePortIngress {
            Some(
                pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(Some(
                    sandbox_id,
                ))?,
            )
        } else {
            None
        };
        let ingress = transport == SandboxServiceRouteTransport::ServicePortIngress;
        let result = async {
            if transport == SandboxServiceRouteTransport::ServicePortIngress {
                require_started_ingress(row.lifecycle.parse().map_err(|error| {
                    Status::internal(format!("invalid sandbox lifecycle: {error}"))
                })?)?;
            }
            let current_node_id = row.active_node_id.ok_or_else(|| {
                sandbox_service_route_not_ready_status(
                    Code::FailedPrecondition,
                    "sandbox service route sandbox is not assigned to a node",
                )
            })?;
            let backend_endpoint =
                resolve_sandbox_service_backend_endpoint(&row).ok_or_else(|| {
                    sandbox_service_route_not_ready_status(
                        Code::FailedPrecondition,
                        missing_backend_endpoint_message(
                            "sandbox service route",
                            row.active_node_id,
                            row.node_managed_by.as_ref().map(|value| value.as_str()),
                            row.k8s_node_name.as_deref(),
                        ),
                    )
                })?;
            let route_generation =
                required_route_generation(row.execution_generation, "sandbox service route")?;
            let (bridge_route_token, service_port_ingress_token) = match transport {
                SandboxServiceRouteTransport::Direct => {
                    let bridge_route_token = space_api_route_token_for_sandbox_service_route(
                        &row,
                        current_node_id,
                        port,
                        SpaceApiRoutePurpose::SpaceApiBridge,
                        route_ed25519_authority,
                    )?;
                    ensure_sandbox_service_route_command_ready(
                        pool,
                        &row,
                        &backend_endpoint,
                        bridge_route_token.as_ref(),
                    )
                    .await?;
                    (bridge_route_token, None)
                }
                SandboxServiceRouteTransport::ServicePortIngress => {
                    let ingress_port = GuestPort::try_from_u16(
                        u16::try_from(port)
                            .map_err(|_| Status::invalid_argument("port is invalid"))?,
                    )
                    .map_err(|_| Status::invalid_argument("port is invalid"))?;
                    prepare_current_ingress_worker(
                        deadline,
                        activation.is_some() && !activated,
                        async {
                            // Activation normally clears this marker. Keep the existing probe
                            // for deployments without an activation adapter or a pending result.
                            if row.command_ready_pending {
                                let readiness_route_token =
                                    space_api_route_token_for_sandbox_service_route(
                                        &row,
                                        current_node_id,
                                        port,
                                        SpaceApiRoutePurpose::SpaceApiBridge,
                                        route_ed25519_authority,
                                    )?;
                                ensure_sandbox_service_route_command_ready(
                                    pool,
                                    &row,
                                    &backend_endpoint,
                                    readiness_route_token.as_ref(),
                                )
                                .await?;
                            }
                            ensure_sandbox_service_port_ingress_path(
                                pool,
                                SandboxServicePortRouteIdentity {
                                    sandbox_id: row.sandbox_id,
                                    active_node_id: current_node_id,
                                    route_generation,
                                    execution_generation: row.execution_generation,
                                    pod_uid: row.sandbox_pod_uid.clone(),
                                    substrate_kind: row.node_managed_by,
                                    activation_attempt_id: row.activation_attempt_id,
                                },
                                ingress_port,
                                &backend_endpoint,
                                route_ed25519_authority,
                            )
                            .await?;
                            Ok(())
                        },
                    )
                    .await?;
                    let service_port_ingress_token =
                        space_api_route_token_for_sandbox_service_route(
                            &row,
                            current_node_id,
                            port,
                            SpaceApiRoutePurpose::ServicePortIngress,
                            route_ed25519_authority,
                        )?;
                    (None, service_port_ingress_token)
                }
                SandboxServiceRouteTransport::Unspecified => {
                    unreachable!("transport is normalized")
                }
            };

            Ok(UnstableResolveSandboxServiceRouteResponse {
                sandbox_id: row.sandbox_id.to_string(),
                org_id: row.org_id.to_string(),
                port,
                lifecycle: row.lifecycle,
                route_generation: route_generation.get(),
                current_node_id: current_node_id.to_string(),
                backend: Some(DataPlaneBackendEndpoint {
                    node_id: current_node_id.to_string(),
                    endpoint: backend_endpoint,
                    transport: Some(BACKEND_TRANSPORT_GRPC.to_string()),
                }),
                template_id: row.template_id,
                space_api_route_token: bridge_route_token,
                service_port_ingress_token,
            })
        }
        .await;
        let error = match result {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        let Some(owner) =
            activation.filter(|_| !activated && activation_may_repair(ingress, &error))
        else {
            return Err(error);
        };
        owner
            .prepare(
                sandbox_id,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await?;
        activated = true;
        // Re-read identity and authorization after activation; never replay bytes.
    }
}

pub(super) fn space_api_route_token_for_sandbox_service_route(
    row: &SandboxServiceRouteRow,
    current_node_id: NodeId,
    port: u32,
    purpose: SpaceApiRoutePurpose,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Option<ProtoSpaceApiRouteToken>, Status> {
    let execution_generation = row.execution_generation;
    let signed_route_generation =
        required_route_generation(execution_generation, "sandbox service route")?;
    let service_port = if purpose == SpaceApiRoutePurpose::ServicePortIngress {
        let port = u16::try_from(port)
            .ok()
            .and_then(|port| GuestPort::try_from_u16(port).ok())
            .ok_or_else(|| Status::invalid_argument("service port must be 1-65535"))?;
        Some(port)
    } else {
        None
    };
    space_api_route_token(
        SpaceApiRouteTokenParams {
            purpose,
            sandbox_id: row.sandbox_id,
            route_generation: signed_route_generation,
            current_node_id,
            substrate_kind: row.node_managed_by,
            execution_generation,
            pod_uid: row.sandbox_pod_uid.clone(),
            activation_attempt_id: row.activation_attempt_id,
            service_port,
        },
        route_ed25519_authority,
    )
}

pub(super) async fn ensure_sandbox_service_route_command_ready(
    pool: &sqlx::PgPool,
    row: &SandboxServiceRouteRow,
    backend_endpoint: &str,
    space_api_route_token: Option<&ProtoSpaceApiRouteToken>,
) -> Result<(), Status> {
    if !row.command_ready_pending {
        return Ok(());
    }

    if row.lifecycle.parse::<SandboxLifecycle>().ok() != Some(SandboxLifecycle::Started) {
        return Err(sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            "sandbox service route command readiness requires a started sandbox",
        ));
    }

    let sandbox_id = row.sandbox_id;
    let mut node_api = worker_node_api(
        backend_endpoint,
        SANDBOX_SERVICE_ROUTE_COMMAND_READY_RPC_TIMEOUT,
    )
    .map_err(|status| {
        Status::unavailable(format!(
            "sandbox service route command readiness node endpoint is invalid: {status}"
        ))
    })?;
    let mut worker = WorkerApiClient::new(&mut node_api);
    let mut request = Request::new(SpacedWaitReadyRequest {
        sandbox_id: sandbox_id.to_string(),
        timeout: Some(SANDBOX_SERVICE_ROUTE_COMMAND_READY_WAIT_TIMEOUT),
    });
    if let Some(token) = space_api_route_token {
        let token = token
            .token
            .parse::<SpaceApiRouteEd25519Token>()
            .map_err(|error| Status::internal(format!("route token is invalid: {error}")))?;
        insert_space_api_route_ed25519_token(request.metadata_mut(), &token).map_err(|error| {
            Status::internal(format!("route token metadata is invalid: {error}"))
        })?;
    }
    worker
        .spaced_wait_ready_with_metadata(request, SANDBOX_SERVICE_ROUTE_COMMAND_READY_RPC_TIMEOUT)
        .await
        .map_err(sandbox_service_route_command_ready_status)?;

    let claimed = clear_sandbox_command_ready_pending(pool, row.sandbox_id).await?;
    if !claimed && !sandbox_lifecycle_is_started(pool, row.sandbox_id).await? {
        return Err(sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            "sandbox service route command readiness lost race with lifecycle transition",
        ));
    }
    Ok(())
}

pub(super) fn sandbox_service_route_command_ready_status(status: Status) -> Status {
    match status.code() {
        Code::DeadlineExceeded | Code::Cancelled => sandbox_service_route_not_ready_status(
            Code::DeadlineExceeded,
            "sandbox service route command readiness timed out",
        ),
        Code::FailedPrecondition | Code::Unavailable => sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            format!("sandbox service route command readiness not ready: {status}"),
        ),
        _ => Status::unavailable(format!(
            "sandbox service route command readiness failed: {status}"
        )),
    }
}

pub(super) async fn clear_sandbox_command_ready_pending(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, Status> {
    // This CAS must match the substrate access-side clear in
    // `KubernetesStore::clear_command_ready_pending`: the `lifecycle =
    // 'started'` guard makes both access paths lose to the command-ready
    // reaper when it has already claimed the sandbox for teardown, instead of
    // clearing a marker on a runtime that is no longer safe to hand back.
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
         SET command_ready_pending = FALSE
         WHERE id = $1 AND lifecycle = 'started' AND command_ready_pending"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(pool)
    .await
    .map_err(|error| {
        Status::internal(format!(
            "clear sandbox service route command readiness marker: {error}"
        ))
    })?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn sandbox_lifecycle_is_started(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, Status> {
    let lifecycle: Option<String> = sandbox_store::get(pool, sandbox_id)
        .await
        .map_err(|error| {
            Status::internal(format!("fetch sandbox service route lifecycle: {error}"))
        })?
        .map(|record| record.lifecycle);

    Ok(lifecycle
        .as_deref()
        .and_then(|value| value.parse::<SandboxLifecycle>().ok())
        == Some(SandboxLifecycle::Started))
}

pub(super) fn sandbox_service_route_port(port: u32) -> Result<u32, Status> {
    if port == 0 || port > u32::from(u16::MAX) {
        return Err(Status::invalid_argument("port must be between 1 and 65535"));
    }
    Ok(port)
}

pub(super) fn sandbox_service_route_kind(
    route_kind: i32,
) -> Result<SandboxServiceRouteKind, Status> {
    let route_kind = SandboxServiceRouteKind::try_from(route_kind)
        .map_err(|_| Status::invalid_argument("route_kind is invalid"))?;
    match route_kind {
        SandboxServiceRouteKind::Unspecified => {
            Err(Status::invalid_argument("route_kind is required"))
        }
        SandboxServiceRouteKind::Agent | SandboxServiceRouteKind::ServicePort => Ok(route_kind),
    }
}

pub(super) fn sandbox_service_route_transport(
    transport: i32,
    route_kind: SandboxServiceRouteKind,
) -> Result<SandboxServiceRouteTransport, Status> {
    let transport = SandboxServiceRouteTransport::try_from(transport)
        .map_err(|_| Status::invalid_argument("transport is invalid"))?;
    match (route_kind, transport) {
        (SandboxServiceRouteKind::Agent, SandboxServiceRouteTransport::Unspecified) => {
            Ok(SandboxServiceRouteTransport::Direct)
        }
        (SandboxServiceRouteKind::ServicePort, SandboxServiceRouteTransport::Unspecified) => {
            Ok(SandboxServiceRouteTransport::ServicePortIngress)
        }
        (SandboxServiceRouteKind::Agent, SandboxServiceRouteTransport::Direct) => {
            Ok(SandboxServiceRouteTransport::Direct)
        }
        (
            SandboxServiceRouteKind::ServicePort,
            SandboxServiceRouteTransport::ServicePortIngress,
        ) => Ok(SandboxServiceRouteTransport::ServicePortIngress),
        (SandboxServiceRouteKind::ServicePort, SandboxServiceRouteTransport::Direct) => Err(
            Status::invalid_argument("service port routes require service port ingress transport"),
        ),
        (SandboxServiceRouteKind::Agent, SandboxServiceRouteTransport::ServicePortIngress) => {
            Err(Status::invalid_argument(
                "service port ingress transport requires service port route kind",
            ))
        }
        (SandboxServiceRouteKind::Unspecified, _) => {
            Err(Status::invalid_argument("route_kind is required"))
        }
    }
}
