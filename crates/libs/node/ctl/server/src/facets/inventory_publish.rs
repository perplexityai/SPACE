//! Outbound publishes of sandbox and template inventory, node status, pending backups and
//! auto-lifecycle delete completions.

use super::*;

pub(super) async fn publish_sandbox_inventory(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let complete = matches!(scope, FacetScope::Full);
    let (sandboxes, sandboxes_seen) =
        collect_sandbox_states(scope, SandboxPublishPayload::Inventory).await?;
    let auto_lifecycle_delete_completions =
        collect_auto_lifecycle_delete_completions(node_role).await;
    // The report rides the Full census only. Not because a delta's list
    // would be short — `ListPendingLocalBackups` answers for the whole
    // node either way — but because the contract this field is published
    // under (see `PublishNodeStateRequest` in
    // `proto/node/api/v1/cluster.proto`) lets a consumer cross-check the
    // report against the sandbox rows in the same publication, and a delta
    // carries only a slice of those.
    //
    // The report is also strictly the newest thing in the publication:
    // `collect_sandbox_states` and the delete-receipt collector each made
    // their own round trip before this one. A sandbox that reaches
    // suspended-with-pin in between is still *present* in this
    // publication's `sandboxes` — the Full scope enumerates from sbx-mgr
    // `List`, whose query is `FROM sandboxes ORDER BY created_at` with no
    // state filter, so the row was picked up while it was still running.
    // Present, not missing — but its lifecycle is whatever the `State`
    // call saw, and `State` is only the second of several round trips
    // before the report: disk usage, snapshots and the delete receipts
    // all follow it. A sandbox that suspends after `State` therefore
    // ships a Running row beside a pending entry naming it. Genuine
    // absence is narrower still, needing a sandbox created *and* pinned
    // entirely inside the gap: a create plus a stop or suspend between
    // two consecutive RPCs.
    //
    // The order is still the better one. Reversed, a sandbox that suspends
    // after the report but before `collect_sandbox_states` ships a
    // freshly-suspended row whose pending entry is missing — a publication
    // that contradicts itself in the direction of under-reporting. As
    // written, the contradiction runs the other way: an entry whose row is
    // a moment stale, which is the better shape to hand a reader than an
    // absence. It does put an obligation on that reader, though, and this
    // is the only place the obligation is visible: **a pending entry can
    // name an artifact newer than anything its own sandbox row reflects**,
    // so the row's backup marker must never be read as evidence about the
    // reported artifact. The next census reconciles either way.
    let pending_local_backups = if complete {
        collect_pending_local_backups().await
    } else {
        None
    };
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: complete,
            sandboxes,
            template_inventory_complete: false,
            templates: Vec::new(),
            sandboxes_seen,
            templates_seen: 0,
            node_status,
            execution_inventory: None,
            auto_lifecycle_delete_completions,
            pending_local_backups,
        },
    )
    .await
}

pub(super) async fn publish_template_inventory(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let complete = matches!(scope, FacetScope::Full);
    let (templates, templates_seen) = collect_template_states(scope).await?;
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: false,
            sandboxes: Vec::new(),
            template_inventory_complete: complete,
            templates,
            sandboxes_seen: 0,
            templates_seen,
            node_status,
            execution_inventory: None,
            auto_lifecycle_delete_completions: Vec::new(),
            pending_local_backups: None,
        },
    )
    .await
}

pub(super) async fn publish_node_status(
    node_id: &str,
    node_role: Option<NodeRole>,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let execution_inventory = match pplx_space_node_mgr_client::connect().await {
        Ok(mut node_manager) => match node_manager
            .get_execution_inventory(local_manager_request(
                GetExecutionInventoryRequest {},
                false,
            ))
            .await
        {
            Ok(response) => Some(response.into_inner()),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "skipping execution inventory publish because node-manager inventory is unavailable"
                );
                None
            }
        },
        Err(error) => {
            tracing::warn!(
                %error,
                "skipping execution inventory publish because node-manager is unavailable"
            );
            None
        }
    };
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: false,
            sandboxes: Vec::new(),
            template_inventory_complete: false,
            templates: Vec::new(),
            sandboxes_seen: 0,
            templates_seen: 0,
            node_status,
            execution_inventory,
            auto_lifecycle_delete_completions: Vec::new(),
            pending_local_backups: None,
        },
    )
    .await
}

/// How long one census will spend obtaining its pending-backup report.
///
/// Both legs below are otherwise generous: `connect()` carries the shared
/// 10-second connect-retry budget, and the sandbox-manager channel's own
/// request timeout is 120 seconds. Those are the right numbers for a
/// payload that has to arrive. This one does not — every failure yields
/// `None`, the census publishes anyway, and the next tick regenerates the
/// report (60 s by default, `poll_interval_secs`). Spending ten of the
/// facet watchdog's ninety — its default, `CLUSTER_RPC_TIMEOUT` × 3 —
/// let alone all of them, to produce the same `None` an immediate failure
/// produces would starve the publish the report rides on.
///
/// Two seconds still covers several of `connect()`'s retry attempts, so a
/// momentary socket blip costs nothing; only a genuinely unavailable
/// manager costs the node a cycle of gate freshness.
pub(super) const PENDING_LOCAL_BACKUP_BUDGET: Duration = Duration::from_secs(2);

/// Ask sandbox-manager which of this node's current lifecycle artifacts
/// still have no durable remote copy.
///
/// Every failure — sandbox-manager unreachable, the RPC erroring, the whole
/// exchange outrunning [`PENDING_LOCAL_BACKUP_BUDGET`], an older
/// sandbox-manager answering `Unimplemented` — yields `None`, which is
/// byte-identical on the wire to what a node built before the field existed
/// sends. Deliberately not an empty report: an empty *report* asserts this
/// disk holds nothing that needs rescuing, and a failed enumeration has not
/// established that.
pub(super) async fn collect_pending_local_backups() -> Option<PendingLocalBackupReport> {
    bounded_pending_local_backups(fetch_pending_local_backups()).await
}

/// The budget, split from the fetch so the expiry arm can be exercised
/// without a sandbox-manager: a test supplies its own slow future.
pub(super) async fn bounded_pending_local_backups(
    fetch: impl Future<Output = Option<PendingLocalBackupReport>>,
) -> Option<PendingLocalBackupReport> {
    match tokio::time::timeout(PENDING_LOCAL_BACKUP_BUDGET, fetch).await {
        Ok(report) => report,
        Err(_elapsed) => {
            tracing::warn!(
                budget = ?PENDING_LOCAL_BACKUP_BUDGET,
                "gave up listing pending local backups within the census budget"
            );
            None
        }
    }
}

pub(super) async fn fetch_pending_local_backups() -> Option<PendingLocalBackupReport> {
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(%error, "cannot connect to sbx-manager to list pending local backups");
            return None;
        }
    };
    match client
        .list_pending_local_backups(Request::new(ListPendingLocalBackupsRequest {}))
        .await
    {
        Ok(response) => {
            // `cluster.proto` carries sandbox-manager's own
            // `PendingLocalBackup`, so the artifact identities move up
            // without being re-encoded — there is no field-by-field copy
            // here to fall out of step with the message.
            let pending = response.into_inner().pending;
            // The counterpart to the warn arms above: they say the node
            // could not answer, this says it did and with how many. Without
            // it a node-local log cannot tell an empty report apart from an
            // enumeration that never ran.
            tracing::debug!(
                pending_local_backups = pending.len(),
                "reporting pending local backups in the full census"
            );
            Some(PendingLocalBackupReport { pending })
        }
        Err(error) if error.code() == Code::Unimplemented => {
            // Not a warning — an older sandbox-manager during a rollout is
            // expected. Logged all the same so a census that quietly stops
            // carrying the report has a node-local explanation.
            tracing::debug!(%error, "sbx-manager does not serve the pending-backup inventory");
            None
        }
        Err(error) => {
            tracing::warn!(%error, "cannot list pending local backups");
            None
        }
    }
}

pub(super) async fn collect_auto_lifecycle_delete_completions(
    node_role: Option<NodeRole>,
) -> Vec<AutoLifecycleDeleteCompletion> {
    if node_role != Some(NodeRole::Worker) {
        return Vec::new();
    }
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot connect to sbx-manager to list delete receipts");
            return Vec::new();
        }
    };
    match client
        .list_auto_lifecycle_delete_receipts(Request::new(
            ListAutoLifecycleDeleteReceiptsRequest {},
        ))
        .await
    {
        Ok(response) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Succeeded,
            );
            response
                .into_inner()
                .receipts
                .into_iter()
                .map(|receipt| AutoLifecycleDeleteCompletion {
                    receipt_id: receipt.receipt_id,
                    sandbox_id: receipt.sandbox_id,
                    execution_generation: receipt.execution_generation,
                    route_activation_id: receipt.route_activation_id,
                    auto_lifecycle_policy_fingerprint: receipt.auto_lifecycle_policy_fingerprint,
                })
                .collect()
        }
        Err(error) if error.code() == Code::Unimplemented => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Unimplemented,
            );
            Vec::new()
        }
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot list auto-lifecycle delete receipts");
            Vec::new()
        }
    }
}

pub(super) async fn acknowledge_auto_lifecycle_delete_completions(
    node_role: Option<NodeRole>,
    receipt_ids: &[String],
) {
    if node_role != Some(NodeRole::Worker) || receipt_ids.is_empty() {
        return;
    }
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::Acknowledge,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot connect to sbx-manager to acknowledge delete receipts");
            return;
        }
    };
    match client
        .acknowledge_auto_lifecycle_delete_receipts(Request::new(
            AcknowledgeAutoLifecycleDeleteReceiptsRequest {
                receipt_ids: receipt_ids.to_vec(),
            },
        ))
        .await
    {
        Ok(_) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::Acknowledge,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Succeeded,
            );
        }
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::Acknowledge,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot acknowledge auto-lifecycle delete receipts");
        }
    }
}
