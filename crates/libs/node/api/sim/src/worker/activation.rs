//! Simulated activation policy and runtime identity.

use super::*;

pub(super) async fn sim_install_activation_policy(
    state: &SimState,
    sandbox_id: &str,
    policy: Option<&sbx::AutoLifecyclePolicy>,
) -> Result<(), Status> {
    use prost::Message;
    if let Some(policy) = policy {
        pplx_space_common_core::types::AutoLifecyclePolicy::try_from(policy)
            .map_err(Status::from)?;
        let encoded = policy.encode_to_vec();
        let record = state.get(sandbox_id).await.map_err(map_state_error)?;
        if record.auto_lifecycle_authority != pplx_space_common_core::types::PolicyAuthority::Local
            && record.access_idle_policy != encoded
        {
            state
                .apply_access_deadline(sandbox_id, None, Some(encoded))
                .await
                .map_err(map_state_error)?;
        }
    }
    Ok(())
}

/// Shared implementation of WakeSandbox used by both `WorkerApi` (public) and
/// `WorkerApiInternal`. Picks the appropriate WakeTransition based on the
/// current in-memory state.
/// `requested_identity` is the reserved identity the control plane passes in
/// `WakeStartContext.route_identity` when a direct wake hydrates a new
/// execution. A start or restore prefers it over the record's last identity
/// and records it, so the receipt carries the reserved generation rather than
/// a stale or default one; resume and no-op wakes ignore it, as the recorded
/// execution stays in place.
pub(super) async fn sim_wake(
    state: &SimState,
    sandbox_id: &str,
    requested_identity: Option<sbx::SpaceApiRouteIdentity>,
    implicit_wake_source: i32,
    activation_policy: Option<sbx::AutoLifecyclePolicy>,
    expected_execution_generation: Option<i64>,
) -> Result<Response<sbx::WakeResponse>, Status> {
    if expected_execution_generation.is_some() && activation_policy.is_none() {
        return Err(Status::invalid_argument(
            "wake execution generation requires a policy",
        ));
    }
    if expected_execution_generation.is_some_and(|generation| generation <= 0) {
        return Err(Status::invalid_argument("invalid execution generation"));
    }
    let rec = state.get(sandbox_id).await;
    // Direct fresh activation requires new control-plane authority. A resident
    // observation is only a resume fence and cannot authorize reusing its ID.
    if let Ok(record) = &rec
        && matches!(record.status.as_str(), status::STOPPED | status::SUSPENDED)
        && record.route_substrate_kind == Some(pplx_space_common_core::types::NodeManagedBy::Space)
        && let Some(retained) = record.execution_generation
    {
        let Some(identity) = &requested_identity else {
            return Err(
                pplx_space_sbx_mgr_client::authoritative_route_identity_required_status(
                    "fresh activation requires authoritative route identity",
                ),
            );
        };
        if identity
            .execution_generation
            .is_none_or(|generation| generation <= retained)
        {
            return Err(Status::aborted("activation generation is already retired"));
        }
    }
    let source = sbx::AutoLifecycleWakeSource::try_from(implicit_wake_source)
        .map_err(|_| Status::invalid_argument("invalid implicit wake source"))?;
    if source != sbx::AutoLifecycleWakeSource::Unspecified {
        let record = rec
            .as_ref()
            .map_err(|_| Status::not_found("sandbox not resident"))?;
        if record.status != status::RUNNING {
            use prost::Message;
            let policy = match activation_policy.as_ref() {
                Some(policy)
                    if record.auto_lifecycle_authority
                        != pplx_space_common_core::types::PolicyAuthority::Local =>
                {
                    policy.clone()
                }
                _ => sbx::AutoLifecyclePolicy::decode(record.access_idle_policy.as_slice())
                    .map_err(|_| Status::internal("invalid simulated lifecycle policy"))?,
            };
            let sources = policy.wake.map(|wake| wake.on).unwrap_or_default();
            let allowed = sources.contains(&(sbx::AutoLifecycleWakeSource::Access as i32))
                || source == sbx::AutoLifecycleWakeSource::Command
                    && sources.contains(&(sbx::AutoLifecycleWakeSource::Command as i32));
            if !allowed {
                return Err(Status::failed_precondition("automatic wake is disabled"));
            }
        }
    }
    let (rec, transition) = match rec {
        Ok(rec) => match rec.status.as_str() {
            status::RUNNING => (rec, sbx::WakeTransition::Noop),
            status::PAUSED => {
                if activation_policy.is_some()
                    && expected_execution_generation != rec.execution_generation
                {
                    return Err(Status::failed_precondition(
                        "resident execution changed before resume",
                    ));
                }
                let r = state
                    .transition(sandbox_id, &[status::PAUSED], status::RUNNING)
                    .await
                    .map_err(map_state_error)?;
                (r, sbx::WakeTransition::Resume)
            }
            status::STOPPED | status::CREATING => {
                let r = state
                    .transition(
                        sandbox_id,
                        &[status::STOPPED, status::CREATING],
                        status::RUNNING,
                    )
                    .await
                    .map_err(map_state_error)?;
                (r, sbx::WakeTransition::Start)
            }
            status::SUSPENDED => {
                let r = state
                    .restore_from_shared_dir(sandbox_id)
                    .await
                    .map_err(map_state_error)?;
                (r, sbx::WakeTransition::Restore)
            }
            _ => (rec, sbx::WakeTransition::Noop),
        },
        Err(SimStateError::NotFound(_)) => {
            // No in-memory record: try restore from the shared dir first; if
            // there's no snapshot either (K8s substrate's "create" path skips
            // CreateSandbox and the sim per-sandbox pod is fresh), fall
            // through to a default-running auto-create so the api-server can
            // proceed.
            match state.restore_from_shared_dir(sandbox_id).await {
                Ok(rec) => (rec, sbx::WakeTransition::Restore),
                Err(SimStateError::SnapshotMissing { .. }) => {
                    let rec = state
                        .ensure_default_running(sandbox_id)
                        .await
                        .map_err(map_state_error)?;
                    (rec, sbx::WakeTransition::Start)
                }
                Err(err) => return Err(map_state_error(err)),
            }
        }
        Err(err) => return Err(map_state_error(err)),
    };
    if transition != sbx::WakeTransition::Noop {
        sim_install_activation_policy(state, sandbox_id, activation_policy.as_ref()).await?;
    }
    // Authoritative Wake adopts and acknowledges the requested identity even
    // when the resident lifecycle needs no transition. Without a request,
    // retain the legacy resident response: the record does not retain enough
    // runtime-owned Kubernetes dimensions to reconstruct a complete receipt.
    let route_identity = match transition {
        sbx::WakeTransition::Noop | sbx::WakeTransition::Resume | sbx::WakeTransition::Wait
            if requested_identity.is_none() =>
        {
            None
        }
        _ => {
            let identity = match requested_identity {
                Some(requested) => sim_direct_route_identity(state.node_id(), Some(requested)),
                None => record_route_identity(state.node_id(), &rec)
                    .unwrap_or_else(|| sim_direct_route_identity(state.node_id(), None)),
            };
            state
                .set_route_identity_provenance(
                    &rec.sandbox_id,
                    identity.route_generation,
                    identity.execution_generation,
                    identity.activation_attempt_id.clone(),
                )
                .await
                .map_err(map_state_error)?;
            Some(identity)
        }
    };
    Ok(Response::new(sbx::WakeResponse {
        sandbox_id: rec.sandbox_id,
        transition: transition.into(),
        route_identity,
    }))
}

/// The identity the simulator last reported for `rec`, if it has one.
pub(super) fn record_route_identity(
    node_id: &str,
    rec: &SandboxRecord,
) -> Option<sbx::SpaceApiRouteIdentity> {
    if rec.route_generation.is_none() && rec.execution_generation.is_none() {
        return None;
    }
    Some(sim_direct_route_identity(
        node_id,
        Some(sbx::SpaceApiRouteIdentity {
            route_generation: rec.route_generation,
            execution_generation: rec.execution_generation,
            activation_attempt_id: rec.activation_attempt_id.clone(),
            ..Default::default()
        }),
    ))
}

pub(super) fn sim_direct_route_identity(
    node_id: &str,
    requested_identity: Option<sbx::SpaceApiRouteIdentity>,
) -> sbx::SpaceApiRouteIdentity {
    let requested_identity = requested_identity.unwrap_or_default();
    let kubernetes_identity =
        requested_identity.substrate_kind == sbx::SandboxSubstrateKind::Kubernetes as i32;
    let route_generation = requested_identity
        .route_generation
        .or_else(|| {
            requested_identity
                .execution_generation
                .and_then(|generation| u64::try_from(generation).ok())
        })
        .unwrap_or(1);
    let execution_generation = requested_identity
        .execution_generation
        .or_else(|| i64::try_from(route_generation).ok())
        .unwrap_or(1);
    sbx::SpaceApiRouteIdentity {
        sandbox_id: requested_identity.sandbox_id,
        route_generation: Some(route_generation),
        current_node_id: if kubernetes_identity {
            requested_identity.current_node_id.or_else(|| {
                Some(
                    pplx_space_common_svc::types::k8s::space_k8s_node_id(node_id, "worker")
                        .to_string(),
                )
            })
        } else {
            Some(node_id.to_string())
        },
        execution_generation: Some(execution_generation),
        pod_uid: kubernetes_identity
            .then_some(requested_identity.pod_uid)
            .flatten(),
        activation_attempt_id: requested_identity
            .activation_attempt_id
            .or_else(|| Some(uuid::Uuid::now_v7().to_string())),
        substrate_kind: if kubernetes_identity {
            sbx::SandboxSubstrateKind::Kubernetes.into()
        } else {
            sbx::SandboxSubstrateKind::Direct.into()
        },
    }
}
