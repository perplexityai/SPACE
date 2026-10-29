use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::internal_api::{LocalBackupQueueOutcome, local_backup_queue_reconcile};
use pplx_space_placement::StoredSandboxLifecycle;

use crate::OriginNode;
use crate::auto_lifecycle_delete_completions::AutoLifecycleDeleteConsumption;
use crate::node_local_backup_pending::PendingLocalBackupReconcile;
use crate::proto::PublishNodeStateResponse;
use crate::published_state::SandboxPublicationOutcome;
use crate::status::SandboxInventoryPrune;

/// Effects of a successfully committed publication. Construct only after
/// COMMIT succeeds; consuming the outcome prevents emitting its events twice.
#[derive(Debug)]
pub(crate) struct PublishOutcome {
    pub(crate) origin: OriginNode,
    pub(crate) sandbox_inventory_complete: bool,
    pub(crate) template_inventory_complete: bool,
    pub(crate) templates_accepted: u64,
    pub(crate) sandbox_publication: SandboxPublicationOutcome,
    pub(crate) inventory_prune: SandboxInventoryPrune,
    pub(crate) delete_consumption: AutoLifecycleDeleteConsumption,
    pub(crate) auto_lifecycle_delete_completions_received: usize,
    pub(crate) reported_count: Option<usize>,
    pub(crate) backup_queue: Result<PendingLocalBackupReconcile, LocalBackupQueueOutcome>,
}

impl PublishOutcome {
    pub(crate) fn emit_post_commit(self) -> PublishNodeStateResponse {
        let Self {
            origin,
            sandbox_inventory_complete,
            template_inventory_complete,
            templates_accepted,
            sandbox_publication,
            inventory_prune,
            delete_consumption,
            auto_lifecycle_delete_completions_received,
            reported_count,
            backup_queue,
        } = self;
        delete_consumption.emit_metrics();
        // One verdict, used for the metric and the log, so the two cannot
        // disagree about what this publication did.
        let backup_queue_outcome = match backup_queue {
            Ok(reconcile) => {
                reconcile.emit_metrics();
                LocalBackupQueueOutcome::Applied
            }
            Err(outcome) => {
                local_backup_queue_reconcile(outcome);
                outcome
            }
        };
        for sample in sandbox_publication.post_commit_state_convergence {
            let duration_ms = (time::OffsetDateTime::now_utc() - sample.changed_at)
                .as_seconds_f64()
                .max(0.0)
                * 1_000.0;
            pplx_space_metrics::sandbox::publication::state_convergence_duration_ms(
                &sample.state,
                duration_ms,
            );
        }
        for event in sandbox_publication.post_commit_events {
            event.emit(origin.id);
        }
        for deleted in &delete_consumption.deleted_sandboxes {
            SandboxEvent::new(
                deleted.sandbox_id,
                SandboxEventType::DeleteCompleted,
                SandboxEventSource::Api,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .reason("auto_lifecycle_delete_completion")
                    .operation("node_state_publish")
                    .trigger(SandboxEventTrigger::AutoLifecycle)
                    .with_from_state(deleted.from_lifecycle.as_str())
                    .with_to_state(StoredSandboxLifecycle::Deleted.as_str())
                    .receipt_id(deleted.receipt_id.to_string())
                    .execution_generation(deleted.execution_generation.to_string())
                    .route_activation_id(deleted.route_activation_id.to_string())
                    .source_node_id(origin.id.to_string()),
            );
        }
        let sandboxes_accepted = sandbox_publication.accepted;
        let mirrored = backup_queue.ok();

        tracing::info!(
            target: "pplx_space_internal_api_server::publish",
            node_id = %origin.display_id,
            resolved_node_id = %origin.id,
            role = %origin.role,
            sandbox_inventory_complete,
            // The node's claim and the server's verdict, both, so a node whose
            // inventory is rejected every tick is visible without a query.
            sandbox_inventory_pruned = inventory_prune.stamps_pruned_at(),
            template_inventory_complete,
            sandboxes_accepted,
            templates_accepted,
            auto_lifecycle_delete_completions_received,
            auto_lifecycle_delete_completions_acknowledged = delete_consumption
                .acknowledged_receipt_ids
                .len(),
            auto_lifecycle_delete_completions_applied = delete_consumption.deleted_sandboxes.len(),
            auto_lifecycle_delete_completions_deferred_by_batch_limit = delete_consumption
                .deferred_by_batch_limit,
            auto_lifecycle_delete_completions_deferred_by_request_limit = delete_consumption
                .deferred_by_request_limit,
            // The node's claim and the server's verdict again. The verdict is
            // the outcome rather than a bool, so a node whose report is
            // declined every tick — and whose gate therefore never advances —
            // says *why* without a second lookup.
            pending_local_backups_reported = reported_count,
            local_backup_queue_outcome = %backup_queue_outcome,
            local_backup_queue_depth = mirrored.map(|queue| queue.depth),
            local_backup_queue_unknown_sandboxes = mirrored.map(|queue| queue.unknown_sandboxes),
            local_backup_queue_deleted_durable = mirrored.map(|queue| queue.deleted_durable),
            local_backup_queue_retained_absent = mirrored.map(|queue| queue.retained_absent),
            "node state snapshot published"
        );

        PublishNodeStateResponse {
            sandboxes_accepted,
            templates_accepted,
            acknowledged_auto_lifecycle_delete_receipt_ids: delete_consumption
                .acknowledged_receipt_ids
                .into_iter()
                .map(|receipt_id| receipt_id.to_string())
                .collect(),
        }
    }
}
