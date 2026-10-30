//! Resetting an ECW sandbox service route and enqueueing the desired-state reconcile it needs.

use super::*;

pub(super) const RESET_ROUTE_DIAGNOSTIC_MAX_CHARS: usize = 512;

#[derive(Debug, Clone, FromRow)]
pub(super) struct UnstableResetSandboxServiceRouteRow {
    pub(super) active_node_id: NodeId,
}

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
    let reset = sqlx::query_as::<_, UnstableResetSandboxServiceRouteRow>(sql!(
        "WITH current_route AS (
             SELECT s.id AS sandbox_id, s.active_node_id
             FROM sandboxes s
             LEFT JOIN sandbox_k8s_executions e
               ON e.sandbox_id = s.id
              AND e.active
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             WHERE s.id = $1
               AND s.active_node_id IS NOT NULL
               AND COALESCE(e.generation, direct_route.execution_generation) = $3
         ),
         updated AS (
             UPDATE sandbox_port_exposures spe
             SET apply_state = 'pending',
                 last_applied_node_id = NULL,
                 last_error = $4,
                 last_bridge_status = NULL,
                 last_applied_bridge_generation = NULL,
                 last_observed_at = NULL,
                 updated_at = NOW()
             FROM current_route
             WHERE spe.sandbox_id = current_route.sandbox_id
               AND spe.protocol = 'tcp'
               AND spe.guest_port = $2
               AND spe.desired_state = 'open'
               AND spe.apply_state = 'applied'
               AND spe.last_applied_node_id = current_route.active_node_id
             RETURNING current_route.active_node_id
         )
         SELECT active_node_id FROM updated"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(port_i32)
    .bind(route_generation)
    .bind(diagnostic)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| Status::internal(format!("reset sandbox service route: {error}")))?;

    let Some(reset) = reset else {
        tx.commit().await.map_err(|error| {
            Status::internal(format!("commit reset sandbox service route no-op: {error}"))
        })?;
        return Ok(UnstableResetSandboxServiceRouteResponse {
            result: UnstableResetSandboxServiceRouteResult::Noop as i32,
            reconcile_enqueued: false,
        });
    };

    enqueue_sandbox_desired_state_reconcile_tx(&mut tx, sandbox_id, reset.active_node_id).await?;
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
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id,
             facet,
             element,
             requested_at,
             updated_at
         )
         VALUES ($1, $2, $3, NOW(), NOW())
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET generation = node_reconcile_queue.generation + 1,
             requested_at = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL
                 THEN EXCLUDED.requested_at
                 ELSE node_reconcile_queue.requested_at
             END,
             next_attempt_at = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL
                 THEN NOW()
                 WHEN node_reconcile_queue.claim_token = ''
                      AND node_reconcile_queue.attempts = 0
                 THEN NOW()
                 ELSE node_reconcile_queue.next_attempt_at
             END,
             attempts = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL THEN 0
                 ELSE node_reconcile_queue.attempts
             END,
             last_error = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL
                      OR node_reconcile_queue.attempts = 0
                 THEN ''
                 ELSE node_reconcile_queue.last_error
             END,
             dead_at = NULL,
             dead_reason = '',
             updated_at = NOW()"
    ))
    .bind(active_node_id.as_uuid())
    .bind(reference.facet_str())
    .bind(reference.element_string().unwrap_or_default())
    .execute(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("enqueue sandbox route reconcile: {error}")))?;

    enqueue_triggers_tx(
        tx,
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
