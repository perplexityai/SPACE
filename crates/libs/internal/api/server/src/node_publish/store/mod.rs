use sqlx::PgPool;
use tonic::Status;

pub(crate) mod absent_sandboxes;
pub(crate) mod activity;
pub(crate) mod degraded_observations;
pub(crate) mod failure_episodes;
pub(crate) mod inventory_reconcile;
pub(crate) mod lifecycle_transitions;
pub(crate) mod network_policy;
pub(crate) mod observed_state;
pub(crate) mod quarantine_claims;
pub(crate) mod quarantine_persistence;
pub(crate) mod sandbox_facet_observations;
pub(crate) mod snapshot_inventory;
pub(crate) mod suspend_finalization;
pub(crate) mod template_components;
pub(crate) mod template_inventory;
pub(crate) mod template_network_policy;
pub(crate) mod template_pruning;

use crate::auto_lifecycle_delete_completions::consume_validated_auto_lifecycle_delete_completions;
use crate::node_local_backup_pending::{self, PendingLocalBackupCensus};
use crate::node_publish::outcome::PublishOutcome;
use crate::node_publish::validate::ValidatedPublish;
use crate::published_state::{
    ApplySandboxClaimsError, prune_absent_node_templates, publish_validated_sandbox_states,
    publish_validated_template_states,
};
use crate::status::{
    LocalBackupInventoryReport, SandboxInventoryPrune, prune_absent_node_sandboxes,
    upsert_node_status_from_publish,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyPublishError {
    #[error("begin node state publish: {0}")]
    Begin(#[source] sqlx::Error),
    #[error("commit node state publish: {0}")]
    Commit(#[source] sqlx::Error),
    #[error(transparent)]
    Sandboxes(#[from] ApplySandboxClaimsError),
    #[error(transparent)]
    PendingBackupDecision(#[from] node_local_backup_pending::DecidePendingBackupError),
    #[error(transparent)]
    PendingBackupReconciliation(#[from] node_local_backup_pending::ReconcilePendingBackupError),
    #[error(transparent)]
    ExecutionPreparation(#[from] crate::execution_observation::PrepareExecutionObservationError),
    #[error(transparent)]
    ExecutionInventory(#[from] crate::execution_observation::PublishExecutionInventoryError),
    #[error(transparent)]
    SandboxCensus(#[from] crate::status::PruneSandboxCensusError),
    #[error(transparent)]
    NodeStatus(#[from] crate::status::UpsertPublishedNodeStatusError),
    #[error(transparent)]
    DeleteCompletions(
        #[from] crate::auto_lifecycle_delete_completions::ConsumeDeleteCompletionsError,
    ),
    #[error(transparent)]
    Legacy(#[from] Status),
}

/// Apply all publication evidence in one transaction and retain row-lock order.
/// Backup certification follows its set-replace; receipt fencing follows every
/// publication write. A successful commit is required to produce an outcome.
pub(crate) async fn apply(
    pool: &PgPool,
    publish: ValidatedPublish,
) -> Result<PublishOutcome, ApplyPublishError> {
    let ValidatedPublish {
        origin,
        reported_window,
        node_status,
        sandbox_census,
        census_diagnostics,
        template_census,
        templates,
        sandboxes,
        execution_observation,
        pending_local_backups,
        delete_completions,
        delete_completions_received: auto_lifecycle_delete_completions_received,
    } = publish;
    let sandbox_inventory_complete = sandbox_census.claimed_complete();
    let template_inventory_complete = template_census.is_some();
    for diagnostic in census_diagnostics {
        diagnostic.emit(&origin);
    }

    let mut tx = pool.begin().await.map_err(ApplyPublishError::Begin)?;

    // Opt-in synthetic timing: exclude pool acquisition and BEGIN, and
    // include the entire open transaction through successful COMMIT.
    let transaction_started =
        tracing::enabled!(target: "space_publish_timing", tracing::Level::TRACE)
            .then(std::time::Instant::now);

    let execution_observation =
        crate::execution_observation::prepare(&mut tx, origin.id, execution_observation).await?;
    let templates_accepted =
        publish_validated_template_states(&mut tx, origin.id, templates).await?;
    let sandbox_publication =
        publish_validated_sandbox_states(&mut tx, origin.id, sandboxes).await?;
    let execution_observation =
        crate::execution_observation::publish(&mut tx, execution_observation).await?;
    // Produce the census-freshness verdict from the branch that prunes,
    // rather than from the wire flag. The timestamp and
    // `sandbox_observed_states` are then written from one decision in one
    // transaction, so they cannot come to disagree — which is what lets the
    // drain gates read the timestamp as "this node's census is current".
    let inventory_prune = if let Some(present_ids) = sandbox_census.pruning_ids() {
        prune_absent_node_sandboxes(&mut tx, origin.id, present_ids).await?;
        SandboxInventoryPrune::Applied
    } else {
        SandboxInventoryPrune::Skipped
    };
    if let Some(census) = template_census {
        prune_absent_node_templates(&mut tx, origin.id, census.legacy_keys()).await?;
    }
    // Mirror the node's pending-backup set before the status upsert that
    // stamps its freshness gate, so the gate is only ever written after
    // the set-replace it certifies — and, since both are in this one
    // transaction, never without it.
    // `Ok` carries the reconcile's counts, `Err` the reason the mirror
    // declined: the two outcomes this publication can have, held in one
    // value so the gate stamp below and the metric after the commit cannot
    // come to disagree about which one happened.
    let reported_count = pending_local_backups.reported_count();
    let backup_queue = match node_local_backup_pending::decide(
        &mut tx,
        pending_local_backups,
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
    let delete_consumption =
        consume_validated_auto_lifecycle_delete_completions(&mut tx, origin.id, delete_completions)
            .await?;
    tx.commit().await.map_err(ApplyPublishError::Commit)?;
    if let Some(started) = transaction_started {
        tracing::trace!(
            target: "space_publish_timing",
            transaction_open_us = started.elapsed().as_secs_f64() * 1_000_000.0,
            "node publish committed"
        );
    }
    Ok(PublishOutcome {
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
    })
}
