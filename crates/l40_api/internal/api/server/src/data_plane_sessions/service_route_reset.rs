//! Resetting an ECW sandbox service route and enqueueing the desired-state reconcile it needs.

use super::*;

pub(super) const RESET_ROUTE_DIAGNOSTIC_MAX_CHARS: usize = 512;

pub(crate) async fn reset_sandbox_service_route(
    pool: &sqlx::PgPool,
    request: UnstableResetSandboxServiceRouteRequest,
) -> Result<UnstableResetSandboxServiceRouteResponse, Status> {
    let route = request
        .route
        .ok_or_else(|| Status::invalid_argument("route is required"))?;
    let sandbox_id = required(&route.sandbox_id, "route.sandbox_id")?
        .parse::<SandboxId>()
        .map_err(|_| Status::invalid_argument("route.sandbox_id must be a UUID"))?;
    let port = sandbox_service_route_port(route.port)?;
    let port_i32 = i32::try_from(port).map_err(|_| Status::invalid_argument("port is invalid"))?;
    let route_generation = route_generation_from_wire(request.route_generation)?;
    let kind = sandbox_service_route_key_kind(route.kind)?;
    if kind != SandboxServiceRouteKind::ServicePort {
        return Ok(UnstableResetSandboxServiceRouteResponse {
            result: UnstableResetSandboxServiceRouteResult::Noop as i32,
            reconcile_enqueued: false,
        });
    }

    let diagnostic = bounded_reset_route_diagnostic(request.diagnostic_message.as_deref());
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Status::internal(format!("begin reset sandbox service route: {error}")))?;

    // The caller's generation is the one the service-route resolver handed
    // out, so it is resolved as the shared route readers resolve it: a Direct
    // sandbox has no execution row, and its generation is its route identity's.
    let reset = sandbox_store::direct::service_route_reset::reset_applied_service_port(
        &mut *tx,
        sandbox_id,
        port_i32,
        route_generation,
        &diagnostic,
    )
    .await
    .map_err(|error| Status::internal(format!("reset sandbox service route: {error}")))?;

    let Some(active_node_id) = reset else {
        tx.commit().await.map_err(|error| {
            Status::internal(format!("commit reset sandbox service route no-op: {error}"))
        })?;
        return Ok(UnstableResetSandboxServiceRouteResponse {
            result: UnstableResetSandboxServiceRouteResult::Noop as i32,
            reconcile_enqueued: false,
        });
    };

    enqueue_sandbox_desired_state_reconcile_tx(&mut tx, sandbox_id, active_node_id).await?;
    tx.commit().await.map_err(|error| {
        Status::internal(format!("commit reset sandbox service route: {error}"))
    })?;

    Ok(UnstableResetSandboxServiceRouteResponse {
        result: UnstableResetSandboxServiceRouteResult::Reset as i32,
        reconcile_enqueued: true,
    })
}

pub(super) async fn enqueue_sandbox_desired_state_reconcile_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    active_node_id: NodeId,
) -> Result<(), Status> {
    let reference = ReconcileRef::sandbox_desired_state(sandbox_id);
    queue_node_reconcile_refs(
        &mut **tx,
        &[active_node_id],
        &[(
            reference.facet_str().to_string(),
            reference.element_string().unwrap_or_default(),
        )],
        QueuedRefConflict::KeepProgress,
    )
    .await
    .map_err(|error| Status::internal(format!("enqueue sandbox route reconcile: {error}")))?;

    enqueue_triggers_tx(
        &mut **tx,
        [active_node_id],
        ReconcileReason::SandboxPortExposureUpdated,
    )
    .await
    .map_err(|error| Status::internal(format!("trigger sandbox route reconcile: {error}")))?;
    Ok(())
}

pub(super) fn sandbox_service_route_key_kind(
    route_kind: i32,
) -> Result<SandboxServiceRouteKind, Status> {
    let route_kind = SandboxServiceRouteKind::try_from(route_kind)
        .map_err(|_| Status::invalid_argument("route.kind is invalid"))?;
    match route_kind {
        SandboxServiceRouteKind::Unspecified => {
            Err(Status::invalid_argument("route.kind is required"))
        }
        SandboxServiceRouteKind::Agent | SandboxServiceRouteKind::ServicePort => Ok(route_kind),
    }
}

pub(super) fn route_generation_from_wire(route_generation: u64) -> Result<RouteGeneration, Status> {
    RouteGeneration::new(route_generation)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

pub(super) fn bounded_reset_route_diagnostic(diagnostic: Option<&str>) -> String {
    let diagnostic = diagnostic
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("sandbox service route reset requested");
    diagnostic
        .chars()
        .take(RESET_ROUTE_DIAGNOSTIC_MAX_CHARS)
        .collect()
}
