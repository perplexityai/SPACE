use super::metadata::*;
use super::status::*;
use super::*;
use crate::auto_lifecycle_delete_completions::consume_auto_lifecycle_delete_completions;
use crate::node_local_backup_pending::{self, PendingLocalBackupCensus};
use pplx_space_metrics::internal_api::{LocalBackupQueueOutcome, local_backup_queue_reconcile};
use pplx_space_node_api_client::node_status_freshness::reported_freshness;
use pplx_space_placement::StoredSandboxLifecycle;

impl ClusterApiService {
    pub(super) async fn publish_node_state(
        &self,
        request: Request<PublishNodeStateRequest>,
    ) -> Result<Response<PublishNodeStateResponse>, Status> {
        let sandbox_inventory_complete = request.get_ref().sandbox_inventory_complete;
        let template_inventory_complete = request.get_ref().template_inventory_complete;
        let auto_lifecycle_delete_completions_received =
            request.get_ref().auto_lifecycle_delete_completions.len();
        let requested_role = optional_db_role_from_proto(request.get_ref().role)?;
        let publishes_delete_completions = auto_lifecycle_delete_completions_received > 0;
        let publishes_sandboxes = sandbox_inventory_complete
            || !request.get_ref().sandboxes.is_empty()
            || publishes_delete_completions;
        if publishes_sandboxes
            && let Some(role) = requested_role
            && role != SpaceNodeRole::Worker
            && role != SpaceNodeRole::Builder
        {
            return Err(Status::invalid_argument(
                "sandbox inventory state can only be published by worker or builder nodes",
            ));
        }
        let role_hint = if publishes_sandboxes {
            requested_role.or(Some(SpaceNodeRole::Worker))
        } else {
            requested_role
        };
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        // Sandbox inventory is worker-or-builder, but only workers run the
        // cluster-owned auto-lifecycle deletes these receipts prove. Gate on the
        // authenticated role rather than the self-declared one, which is
        // optional on the wire.
        if publishes_delete_completions && origin.role != SpaceNodeRole::Worker {
            return Err(Status::invalid_argument(
                "auto-lifecycle delete completions can only be published by worker nodes",
            ));
        }
        let request = request.into_inner();
        let reported_window = reported_freshness(
            request
                .node_status
                .as_ref()
                .and_then(|status| status.node_ctl.as_ref()),
        );
        let node_status = request
            .node_status
            .map(published_node_status_db)
            .transpose()?;
        // Per-row validation and telemetry live in
        // `publish_sandbox_states_with_retirement`.
        // Pruning reuses only identities that can name a central row.
        let mut sandbox_inventory_prunable = sandbox_inventory_complete;
        let present_sandbox_ids = request
            .sandboxes
            .iter()
            .filter_map(|sandbox| match parse_sandbox_id(&sandbox.sandbox_id) {
                Ok(sandbox_id) => sandbox.present.then_some(sandbox_id),
                Err(error) => {
                    sandbox_inventory_prunable = false;
                    tracing::warn!(
                        node_id = %origin.display_id,
                        sandbox_id = %sandbox.sandbox_id,
                        %error,
                        "preserving existing node associations because a complete inventory contains an invalid sandbox identity"
                    );
                    None
                }
            })
            .collect::<Vec<_>>();
        let present_template_keys = request
            .templates
            .iter()
            .filter(|template| template.present)
            .filter_map(|template| {
                let name = optional_trimmed(Some(&template.template))?;
                let content_hash =
                    optional_trimmed(template.content_hash.as_deref()).unwrap_or_default();
                Some((name, content_hash))
            })
            .collect::<Vec<_>>();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| Status::internal(format!("begin node state publish: {err}")))?;

        let execution_observation = crate::execution_observation::prepare(
            &mut tx,
            origin.id,
            request.execution_observation.as_ref(),
            request.execution_inventory.as_ref(),
        )
        .await?;
        let templates_accepted =
            publish_template_states(&mut tx, origin.id, origin.role, &request.templates).await?;
        let sandbox_publication = crate::published_state::publish_sandbox_states_with_retirement(
            &mut tx,
            origin.id,
            &request.sandboxes,
            self.abandoned_restore_retirement,
        )
        .await?;
        let execution_observation =
            crate::execution_observation::publish(&mut tx, execution_observation).await?;
        // Produce the census-freshness verdict from the branch that prunes,
        // rather than from the wire flag. The timestamp and
        // `sandbox_observed_states` are then written from one decision in one
        // transaction, so they cannot come to disagree — which is what lets the
        // drain gates read the timestamp as "this node's census is current".
        let inventory_prune = if sandbox_inventory_prunable {
            prune_absent_node_sandboxes(&mut tx, origin.id, &present_sandbox_ids).await?;
            SandboxInventoryPrune::Applied
        } else {
            SandboxInventoryPrune::Skipped
        };
        if template_inventory_complete {
            prune_absent_node_templates(&mut tx, origin.id, &present_template_keys).await?;
        }
        // Mirror the node's pending-backup set before the status upsert that
        // stamps its freshness gate, so the gate is only ever written after
        // the set-replace it certifies — and, since both are in this one
        // transaction, never without it.
        // `Ok` carries the reconcile's counts, `Err` the reason the mirror
        // declined: the two outcomes this publication can have, held in one
        // value so the gate stamp below and the metric after the commit cannot
        // come to disagree about which one happened.
        let backup_queue = match node_local_backup_pending::decide(
            &mut tx,
            request.pending_local_backups.as_ref(),
            inventory_prune,
            &sandbox_publication.state_only_sandbox_ids,
            origin.id,
        )
        .await?
        {
            PendingLocalBackupCensus::Report(report) => {
                Ok(node_local_backup_pending::reconcile(&mut tx, origin.id, &report).await?)
            }
            PendingLocalBackupCensus::Declined(outcome) => Err(outcome),
        };
        // Three outcomes, not two. A decline on a publication the node did
        // not claim was a complete inventory is a facet tick that never
        // could have mirrored, and blanking the gate on those would clear it
        // many times a minute.
        // A decline on a Full census means the queue was not refreshed, and
        // leaving a recent timestamp there tells a reader the empty queue is
        // certified when the server knows it is not.
        //
        // The two directions key on deliberately different things, and the
        // asymmetry is the point. *Granting* permission takes the server's
        // own verdict (`inventory_prune`), because a node's claim is not
        // evidence. *Withdrawing* it takes the node's claim
        // (`sandbox_inventory_complete`), because the claim is what says the
        // node meant to give a full answer — and one unparseable sandbox id
        // downgrades the verdict while leaving that intent intact, which
        // would otherwise preserve a gate over a census that plainly failed.
        // Both directions therefore err toward not certifying.
        let local_backup_report = match (&backup_queue, sandbox_inventory_complete) {
            (Ok(_), _) => LocalBackupInventoryReport::Mirrored,
            (Err(_), true) => LocalBackupInventoryReport::Invalidated,
            (Err(_), false) => LocalBackupInventoryReport::Absent,
        };
        upsert_node_status_from_publish(
            &mut tx,
            origin.id,
            reported_window,
            node_status,
            inventory_prune,
            local_backup_report,
            execution_observation,
        )
        .await?;
        // Consume receipts only after this publication's sandbox, execution,
        // pruning, and node-status writes. Receipt fencing must observe the
        // final evidence produced by the same transaction.
        let delete_consumption = consume_auto_lifecycle_delete_completions(
            &mut tx,
            origin.id,
            &request.auto_lifecycle_delete_completions,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("commit node state publish: {err}")))?;
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
            pending_local_backups_reported = request
                .pending_local_backups
                .as_ref()
                .map(|report| report.pending.len()),
            local_backup_queue_outcome = %backup_queue_outcome,
            local_backup_queue_depth = mirrored.map(|queue| queue.depth),
            local_backup_queue_unknown_sandboxes = mirrored.map(|queue| queue.unknown_sandboxes),
            local_backup_queue_deleted_durable = mirrored.map(|queue| queue.deleted_durable),
            local_backup_queue_retained_absent = mirrored.map(|queue| queue.retained_absent),
            "node state snapshot published"
        );

        Ok(Response::new(PublishNodeStateResponse {
            sandboxes_accepted,
            templates_accepted,
            acknowledged_auto_lifecycle_delete_receipt_ids: delete_consumption
                .acknowledged_receipt_ids
                .into_iter()
                .map(|receipt_id| receipt_id.to_string())
                .collect(),
        }))
    }
}
