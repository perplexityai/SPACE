//! The sandbox desired-state facet and the local desired-state apply it drives.

use super::*;

pub(super) fn sandbox_desired_reconcile_action(
    has_local_state: bool,
    has_port_exposures: bool,
    has_local_allocation: bool,
) -> SandboxDesiredReconcileAction {
    if has_port_exposures && !has_local_allocation {
        SandboxDesiredReconcileAction::SkipNonLocal
    } else if has_local_state {
        SandboxDesiredReconcileAction::ApplyAll
    } else if has_port_exposures {
        SandboxDesiredReconcileAction::ApplyPortExposuresAndRetry
    } else {
        SandboxDesiredReconcileAction::SkipNonLocal
    }
}

pub(super) async fn refresh_sandbox_desired_state(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    sync_interval: std::time::Duration,
    sandbox_listing: &InboundSandboxListing,
) -> Result<InboundFacetResult, String> {
    let sandbox_ids = resolve_sandbox_scope(scope, sandbox_listing).await?;
    let mut stats = ReconcileStats {
        sandboxes_seen: sandbox_ids.len() as u64,
        ..ReconcileStats::default()
    };
    if sandbox_ids.is_empty() {
        return Ok(InboundFacetResult::from_stats(stats));
    }

    let mut cluster = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = cluster
        .get_node_sandbox_desired_state(GetNodeSandboxDesiredStateRequest {
            node_id: node_id.to_string(),
            sandbox_ids,
            role: proto_node_role(node_role),
        })
        .await
        .map_err(|error| format!("cluster sandbox desired-state refresh failed: {error}"))?
        .into_inner();

    let has_port_exposures = response
        .sandboxes
        .iter()
        .any(|sandbox| !sandbox.port_exposures.is_empty());
    let mut sbx = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-manager: {error}"))?;
    // Deleted ids are included so the eviction step below can gate on the
    // sandbox's current local state without a second State round-trip.
    let local_state_lookup_ids = response
        .sandboxes
        .iter()
        .map(|sandbox| sandbox.sandbox_id.clone())
        .chain(response.deleted_sandbox_ids.iter().cloned())
        .collect::<Vec<_>>();
    let local_sandbox_state_by_id = fetch_local_sandbox_states(&mut sbx, local_state_lookup_ids)
        .await
        .map_err(|error| format!("sandbox desired-state shared state lookup failed: {error}"))?;
    let mut node = has_port_exposures.then(pplx_space_node_mgr_client::lazy);
    let local_sandbox_allocations = if let Some(node) = node.as_mut() {
        local_sandbox_allocation_ids(node).await.map_err(|error| {
            format!("sandbox desired-state shared allocation lookup failed: {error}")
        })?
    } else {
        BTreeSet::new()
    };
    let mut element_errors = BTreeMap::new();
    let mut exposure_apply_states = Vec::new();
    for sandbox in response.sandboxes {
        let sandbox_id = sandbox
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| format!("invalid sandbox desired-state id: {error}"))?;
        let has_sandbox_port_exposures = !sandbox.port_exposures.is_empty();
        let local_sandbox_state = local_sandbox_state_by_id.get(&sandbox.sandbox_id);
        let action = sandbox_desired_reconcile_action(
            local_sandbox_state.is_some(),
            has_sandbox_port_exposures,
            local_sandbox_allocations.contains(&sandbox_id),
        );
        if action == SandboxDesiredReconcileAction::SkipNonLocal {
            tracing::debug!(
                sandbox_id = %sandbox.sandbox_id,
                exposures = sandbox.port_exposures.len(),
                "skipping desired state for sandbox that is no longer local"
            );
            continue;
        }
        if let Some(local_sandbox_state) = local_sandbox_state {
            handle_local_desired_state_apply(
                apply_sandbox_snapshot_desired(
                    &mut sbx,
                    &sandbox.sandbox_id,
                    local_sandbox_state,
                    sandbox.snapshots_enabled_desired,
                )
                .await,
                &sandbox.sandbox_id,
                &mut stats,
                &mut element_errors,
            );
            if let Some(policy) = sandbox.auto_lifecycle {
                handle_local_desired_state_apply(
                    apply_sandbox_auto_lifecycle_desired(
                        &mut sbx,
                        &sandbox.sandbox_id,
                        local_sandbox_state,
                        policy,
                    )
                    .await,
                    &sandbox.sandbox_id,
                    &mut stats,
                    &mut element_errors,
                );
            }
        } else {
            tracing::warn!(
                sandbox_id = %sandbox.sandbox_id,
                exposures = sandbox.port_exposures.len(),
                "local sandbox state is unavailable; applying port exposures and retrying the ref"
            );
        }
        if let Some(node) = node.as_mut() {
            reconcile_sandbox_port_exposures(
                PortExposureReconcileMode { action },
                node,
                &sandbox.sandbox_id,
                sandbox.port_exposures,
                &mut stats,
                &mut element_errors,
                &mut exposure_apply_states,
            )
            .await;
        }
    }
    if !exposure_apply_states.is_empty() {
        match cluster
            .publish_sandbox_port_exposure_states(PublishSandboxPortExposureStatesRequest {
                node_id: node_id.to_string(),
                states: exposure_apply_states,
                role: proto_node_role(node_role),
            })
            .await
        {
            Ok(response) => {
                for error in response.into_inner().errors {
                    tracing::warn!(
                        sandbox_id = %error.sandbox_id,
                        protocol = port_protocol_label(error.protocol),
                        guest_port = error.guest_port,
                        error = %error.error,
                        "failed to publish sandbox port exposure apply state"
                    );
                    record_element_error(
                        &mut element_errors,
                        &error.sandbox_id,
                        &format!("publish port exposure apply state: {}", error.error),
                    );
                }
            }
            Err(error) => {
                return Err(format!(
                    "publish sandbox port exposure apply states: {error}"
                ));
            }
        }
    }
    if !response.deleted_sandbox_ids.is_empty() {
        let evict_cap = CLUSTER_DELETED_EVICTS_PER_SYNC;
        let now = std::time::Instant::now();
        CLUSTER_DELETED_EVICT_BACKOFF_UNTIL.retain(|_, until| *until > now);
        let candidates = select_cluster_deleted_evict_candidates(
            &response.deleted_sandbox_ids,
            &local_sandbox_state_by_id,
            CLUSTER_DELETED_CANDIDATE_WINDOW,
            |sandbox_id| CLUSTER_DELETED_EVICT_BACKOFF_UNTIL.contains_key(sandbox_id),
        );
        let started = std::time::Instant::now();
        let mut evicted = 0_u64;
        let mut deferred = 0_u64;
        let mut budget_exhausted = false;
        for sandbox_id in candidates {
            if evicted >= evict_cap as u64 {
                break;
            }
            if started.elapsed() > CLUSTER_DELETED_EVICT_SYNC_BUDGET {
                budget_exhausted = true;
                break;
            }
            let call = sbx.evict_cluster_deleted_sandbox(local_manager_request(
                EvictClusterDeletedSandboxRequest {
                    sandbox_id: sandbox_id.to_string(),
                },
                true,
            ));
            let result = match tokio::time::timeout(CLUSTER_DELETED_EVICT_RPC_TIMEOUT, call).await {
                Ok(result) => result,
                // The teardown runs detached in sbx-manager, so an
                // elapsed deadline means "still in progress", not
                // failure; the next sync observes the outcome.
                Err(_elapsed) => Err(tonic::Status::deadline_exceeded(
                    "eviction still in progress at the per-RPC deadline",
                )),
            };
            let status = match &result {
                Ok(_) => {
                    evicted = evicted.saturating_add(1);
                    stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Evicted,
                    );
                    continue;
                }
                Err(status) => status,
            };
            match cluster_deleted_evict_refusal_disposition(status, sync_interval) {
                ClusterDeletedEvictDisposition::RetryNextSync => {
                    deferred = deferred.saturating_add(1);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Deferred,
                    );
                    tracing::debug!(
                        sandbox_id,
                        status = %status,
                        "cluster-deleted eviction deferred to the next sync: restore staging in flight"
                    );
                }
                ClusterDeletedEvictDisposition::Deferred(backoff) => {
                    deferred = deferred.saturating_add(1);
                    CLUSTER_DELETED_EVICT_BACKOFF_UNTIL
                        .insert(sandbox_id.to_string(), std::time::Instant::now() + backoff);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Deferred,
                    );
                    tracing::debug!(
                        sandbox_id,
                        status = %status,
                        backoff_secs = backoff.as_secs(),
                        "cluster-deleted eviction deferred"
                    );
                }
                ClusterDeletedEvictDisposition::Failed(backoff) => {
                    CLUSTER_DELETED_EVICT_BACKOFF_UNTIL
                        .insert(sandbox_id.to_string(), std::time::Instant::now() + backoff);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Failed,
                    );
                    tracing::warn!(
                        sandbox_id,
                        status = %status,
                        "cluster-deleted eviction failed"
                    );
                    record_element_error(
                        &mut element_errors,
                        sandbox_id,
                        &format!("cluster-deleted eviction failed: {status}"),
                    );
                }
            }
        }
        if evicted > 0 || deferred > 0 || budget_exhausted {
            tracing::info!(
                deleted_reported = response.deleted_sandbox_ids.len(),
                evicted,
                deferred,
                budget_exhausted,
                per_sync_cap = evict_cap,
                "evicted local presence of cluster-deleted sandboxes"
            );
        }
    }

    Ok(InboundFacetResult {
        stats,
        element_errors,
    })
}

/// sbx-manager RPCs the snapshot / auto-lifecycle desired-state apply sub-ops
/// issue. Kept as an enum so terminal-code classification keys off the exact
/// operation instead of substring-matching a formatted message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LocalDesiredStateRpc {
    EnableSnapshots,
    DisableSnapshots,
    SetAutoLifecyclePolicy,
}

impl LocalDesiredStateRpc {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::EnableSnapshots => "enable snapshots",
            Self::DisableSnapshots => "disable snapshots",
            Self::SetAutoLifecyclePolicy => "set auto-lifecycle policy",
        }
    }
}

impl std::fmt::Display for LocalDesiredStateRpc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Failure applying a snapshot / auto-lifecycle desired-state sub-op against
/// the local sbx-manager. RPC failures preserve the tonic `Status` so a
/// guest-unreachable `FailedPrecondition` (sandbox failed/suspending/
/// suspended/stopped) is distinguishable from transient failures.
#[derive(Debug, thiserror::Error)]
pub(super) enum LocalDesiredStateApplyError {
    /// A sbx-manager RPC failed.
    #[error("{operation}: {status}")]
    Rpc {
        operation: LocalDesiredStateRpc,
        #[source]
        status: tonic::Status,
    },
}

impl LocalDesiredStateApplyError {
    /// See [`sbx_mgr_desired_state_apply_is_terminal`].
    pub(super) fn is_terminal(&self) -> bool {
        match self {
            Self::Rpc { status, .. } => sbx_mgr_desired_state_apply_is_terminal(status),
        }
    }
}

pub(super) async fn apply_sandbox_snapshot_desired(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &str,
    state: &SandboxInfo,
    snapshots_enabled_desired: bool,
) -> Result<(), LocalDesiredStateApplyError> {
    if state.snapshots_enabled == snapshots_enabled_desired {
        return Ok(());
    }

    let (operation, result) = if snapshots_enabled_desired {
        (
            LocalDesiredStateRpc::EnableSnapshots,
            client
                .enable_snapshots(local_manager_request(
                    EnableSnapshotsRequest {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    true,
                ))
                .await
                .map(|_| ()),
        )
    } else {
        (
            LocalDesiredStateRpc::DisableSnapshots,
            client
                .disable_snapshots(local_manager_request(
                    DisableSnapshotsRequest {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    true,
                ))
                .await
                .map(|_| ()),
        )
    };

    result.map_err(|status| LocalDesiredStateApplyError::Rpc { operation, status })
}

pub(super) async fn apply_sandbox_auto_lifecycle_desired(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &str,
    state: &SandboxInfo,
    policy: pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy,
) -> Result<(), LocalDesiredStateApplyError> {
    if state.auto_lifecycle.as_ref() == Some(&policy) {
        return Ok(());
    }

    client
        .set_auto_lifecycle_policy(local_manager_request(
            SetAutoLifecyclePolicyRequest {
                sandbox_id: sandbox_id.to_string(),
                policy: Some(policy),
                authority: PolicyAuthority::Cluster.as_proto_i32(),
            },
            true,
        ))
        .await
        .map(|_| ())
        .map_err(|status| LocalDesiredStateApplyError::Rpc {
            operation: LocalDesiredStateRpc::SetAutoLifecyclePolicy,
            status,
        })
}

/// Fold a snapshot / auto-lifecycle apply outcome into the reconcile stats and
/// element errors. `Ok` (applied or already in desired state) counts as
/// applied. A server-marked terminal state-gate failure publishes
/// the diagnostic and metric but records **no** element error, so the sandbox
/// desired-state ref completes. A separate lifecycle/cluster repair changes
/// the state, and periodic desired-state sync re-applies afterward; retrying
/// this unchanged ref could never converge and would pin the queue-lag gauge.
/// A transient failure records an element error so the queue re-drives it.
pub(super) fn handle_local_desired_state_apply(
    result: Result<(), LocalDesiredStateApplyError>,
    sandbox_id: &str,
    stats: &mut ReconcileStats,
    element_errors: &mut BTreeMap<String, String>,
) {
    let Err(error) = result else {
        stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
        return;
    };
    let result_kind = if error.is_terminal() {
        tracing::info!(
            sandbox_id,
            %error,
            "terminal sandbox desired-state apply; completing ref until state changes"
        );
        pplx_space_metrics::reconcile::DesiredStateApplyResult::Terminal
    } else {
        tracing::warn!(
            sandbox_id,
            %error,
            "retryable sandbox desired-state apply failure"
        );
        record_element_error(element_errors, sandbox_id, &error.to_string());
        pplx_space_metrics::reconcile::DesiredStateApplyResult::Retryable
    };
    pplx_space_metrics::reconcile::desired_state_apply(result_kind);
}
