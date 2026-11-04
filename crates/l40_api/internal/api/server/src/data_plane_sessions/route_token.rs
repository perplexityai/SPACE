//! Minting the Space API route token a resolved route carries.

use super::*;

pub(super) fn required_route_generation(
    execution_generation: Option<ExecutionGeneration>,
    route_kind: &str,
) -> Result<RouteGeneration, Status> {
    match execution_generation {
        Some(generation) => RouteGeneration::new(
            u64::try_from(generation.get())
                .map_err(|_| Status::internal("execution generation is invalid"))?,
        )
        .map_err(|error| Status::internal(error.to_string())),
        None => Err(Status::failed_precondition(format!(
            "{route_kind} has no authoritative execution generation"
        ))),
    }
}

pub(super) struct SpaceApiRouteTokenParams {
    pub(super) purpose: SpaceApiRoutePurpose,
    pub(super) sandbox_id: SandboxId,
    pub(super) route_generation: RouteGeneration,
    pub(super) current_node_id: NodeId,
    pub(super) substrate_kind: Option<NodeManagedBy>,
    pub(super) execution_generation: Option<ExecutionGeneration>,
    pub(super) pod_uid: Option<PodUid>,
    pub(super) activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(super) service_port: Option<GuestPort>,
}

pub(super) fn space_api_route_token(
    params: SpaceApiRouteTokenParams,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Option<ProtoSpaceApiRouteToken>, Status> {
    let complete_identity = route_identity_for_signing(&SandboxRouteIdentityParts {
        sandbox_id: params.sandbox_id,
        route_generation: params.route_generation,
        current_node_id: params.current_node_id,
        substrate_kind: params.substrate_kind,
        execution_generation: params.execution_generation,
        activation_attempt_id: params.activation_attempt_id,
        pod_uid: params.pod_uid.clone(),
    })?;
    let authority = route_ed25519_authority.ok_or_else(|| {
        Status::failed_precondition("SPACE API route signing authority is unavailable")
    })?;
    let token = match params.purpose {
        SpaceApiRoutePurpose::SpaceApiBridge => authority.mint_space_api_bridge(complete_identity),
        SpaceApiRoutePurpose::ServicePortIngress => authority.mint_service_port_ingress(
            complete_identity,
            params
                .service_port
                .expect("ServicePort ingress claims require a port"),
            pplx_space_common_svc::space_api_route::SpaceApiRouteProtocol::Tcp,
        ),
    }
    .map_err(|error| Status::internal(format!("mint SPACE API route token: {error}")))?;
    Ok(Some(ProtoSpaceApiRouteToken {
        token: token.secret().to_owned(),
    }))
}
