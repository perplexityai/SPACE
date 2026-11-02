//! Writes of `sandboxes` that project what a node reports. From a
//! `PublishNodeState` publication: the restoring-to-started convergence,
//! the failed and quarantined projections, activation-completion receipts,
//! pause-failed recovery, and the census prune, which forgets the node's
//! observations of every sandbox its complete census omits and tombstones
//! absent Direct rows. The suspended finalization runs for a publication and
//! for a ready suspend backup (`FinalizeArtifactBackup`).
//!
//! Stored and published lifecycle text is compared either with
//! `LOWER(TRIM(..))` or exactly, as each statement is written; which one
//! decides how a statement treats non-canonical text. Several are single
//! statements whose CTEs also write other tables (`sandbox_k8s_executions`,
//! `sandbox_port_exposures`, `sandbox_observed_states`, the quarantine and
//! node-facet tables, `sandbox_snapshot_inventory`): splitting one would
//! change what it does. The caller owns the transaction, the locking reads
//! that select candidates, the other tables' own statements, and the events
//! it emits.
//!
//! A new lifecycle write here from a node report follows the rule for such
//! writes in `crates/libs/internal/api/server/AGENTS.md`, which also lists
//! the existing exceptions.
//!
//! Recordset inputs are the JSON rows the internal API builds; each function
//! names the record columns its statement reads.

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, LocalArtifactId, NodeId, PodUid, SandboxId,
    SandboxQuarantineHistoryAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
    SandboxTransitionReceiptKind, SandboxTransitionTargetState, SubstrateKind,
};

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    SuspendedTargetNodeAssignment, node_published_execution_identity_matches,
    publish_clear_or_preserve_current_local_artifact_marker_assignments,
    suspended_k8s_finalization_sql,
};
use sqlx::types::{Json, JsonValue};
use uuid::Uuid;

/// The `jsonb_to_recordset` column list of one published sandbox row with
/// its transition receipt, as the publish path serializes it. Two statements
/// still in the internal API splice it as well, and its names must match the
/// fields that path serializes: `jsonb_to_recordset` reads a missing field as
/// NULL.
pub const TRANSITION_RECEIPT_RECORD_COLUMNS: &str = "
    sandbox_id UUID,
    status TEXT,
    transition_receipt_id UUID,
    transition_receipt_kind TEXT,
    transition_receipt_target_state TEXT,
    transition_receipt_execution_generation BIGINT,
    transition_receipt_reason_code TEXT,
    transition_receipt_recovery_policy TEXT,
    transition_receipt_activation_attempt_id UUID,
    failure_reason TEXT,
    current_local_artifact_id UUID,
    current_local_artifact_kind TEXT,
    current_local_artifact_origin TEXT,
    execution_generation BIGINT,
    pod_uid TEXT
";

/// Mark the publisher's claimed rows `quarantined`, returning the ids that
/// flipped. **Observed projection**: the node reported a quarantine hold.
///
/// `claimed` is a recordset of `{sandbox_id, from_lifecycle}`, the lifecycle
/// the caller's claim read for each row. The write re-checks that the row is
/// still on `node_id` and still holds exactly that lifecycle, so a row another
/// writer moved since the claim is left alone. The claim is a plain read, not
/// a row lock: that compare-and-set is the only guard, and it compares the
/// stored text exactly, where the claim compared it with `LOWER(TRIM(..))`.
pub async fn mark_published_quarantined(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    claimed: Json<JsonValue>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "UPDATE sandboxes s
                 SET lifecycle = 'quarantined'
                 FROM jsonb_to_recordset($1::jsonb) AS input(
                     sandbox_id UUID,
                     from_lifecycle TEXT
                 )
                 WHERE s.id = input.sandbox_id
                   AND s.active_node_id = $2
                   AND s.lifecycle = input.from_lifecycle
                 RETURNING s.id"
    ))
    .bind(claimed)
    .bind(node_id)
    .fetch_all(executor)
    .await
}

/// Move the publisher's Kubernetes rows that it published `failed` to
/// `lifecycle = 'failed'`, returning each moved row with the lifecycle it
/// left. **Observed projection**: the node reported the runtime failed.
///
/// `rows` is a recordset of `{sandbox_id, status}`. The statement claims
/// before it writes: its `claimed` CTE takes `FOR NO KEY UPDATE` on rows still
/// on `node_id` and outside the terminal and hold lifecycles, so a row a
/// concurrent writer holds is re-read at its committed version once the lock
/// is granted, and the returned from-state is the lifecycle the update
/// actually replaced. The lock is part of the statement.
pub async fn mark_published_failed(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: Json<JsonValue>,
) -> Result<Vec<(SandboxId, String)>, sqlx::Error> {
    let failed_sandboxes = sqlx::query_as::<_, (Uuid, String)>(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT
             )
         ),
         claimed AS (
             SELECT s.id, s.lifecycle
             FROM sandboxes s
             JOIN input ON input.sandbox_id = s.id
             WHERE s.active_node_id = $1
               AND s.managed_by = $3
               AND LOWER(TRIM(input.status)) = 'failed'
               AND LOWER(TRIM(s.lifecycle)) NOT IN (
                   'failed',
                   'quarantined',
                   'deleted',
                   'cleanup_required',
                   'deleting'
               )
             FOR NO KEY UPDATE OF s
         )
         UPDATE sandboxes s
         SET lifecycle = 'failed'
         FROM claimed
         WHERE s.id = claimed.id
         RETURNING s.id, claimed.lifecycle"
    ))
    .bind(node_id)
    .bind(rows)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor)
    .await?;
    Ok(failed_sandboxes
        .into_iter()
        .map(|(sandbox_id, lifecycle)| (SandboxId::from_uuid(sandbox_id), lifecycle))
        .collect())
}

/// The suspend artifact a node published for a sandbox.
#[derive(Clone, Debug)]
pub struct PublishedSuspendArtifact {
    pub id: LocalArtifactId,
    pub kind: ArtifactKind,
    pub origin: ArtifactOrigin,
}

/// The execution a node published a suspend artifact from.
#[derive(Clone, Debug)]
pub struct PublishedExecutionIdentity {
    pub generation: ExecutionGeneration,
    pub pod_uid: PodUid,
    pub pod_uid_was_missing: bool,
}

/// One sandbox the caller selected for suspended finalization.
#[derive(Debug)]
pub struct SuspendedFinalizationCandidate {
    pub sandbox_id: SandboxId,
    pub clear_marker: bool,
    pub published_artifact: Option<PublishedSuspendArtifact>,
    pub published_execution_identity: Option<PublishedExecutionIdentity>,
}

/// Finalize the selected rows as `suspended`, returning each finalized row
/// with its `clear_marker`. **Observed projection**: a node reported the
/// sandbox suspended, or reported its suspend backup ready.
///
/// One statement retires the active execution, resets open port exposures
/// to pending and writes the suspended assignments
/// ([`suspended_k8s_finalization_sql`]) with the artifact marker published,
/// cleared or kept. A candidate with a published artifact finalizes only
/// when its active execution is the exact one the node published from
/// ([`node_published_execution_identity_matches`]); one without finalizes
/// whatever its execution, or with none, as a Direct row has.
///
/// The statement checks no lifecycle, node or substrate of its own: it
/// relies on the caller's candidate selection, which holds the row locks
/// (`FOR UPDATE OF s`) in the same transaction.
pub async fn finalize_suspended(
    executor: impl sqlx::PgExecutor<'_>,
    candidates: &[SuspendedFinalizationCandidate],
) -> Result<Vec<(SandboxId, bool)>, sqlx::Error> {
    let artifact_marker_assignments =
        publish_clear_or_preserve_current_local_artifact_marker_assignments(
            "s",
            "candidate.published_artifact_id",
            "candidate.published_artifact_kind",
            "candidate.published_artifact_origin",
            "candidate.clear_marker",
        );
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "candidate",
        &artifact_marker_assignments,
        SuspendedTargetNodeAssignment::PreserveOrBackfillActive,
    );
    let candidate_ids = candidates
        .iter()
        .map(|candidate| candidate.sandbox_id)
        .collect::<Vec<_>>();
    let clear_markers = candidates
        .iter()
        .map(|candidate| candidate.clear_marker)
        .collect::<Vec<_>>();
    let published_artifact_ids = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_artifact
                .as_ref()
                .map(|artifact| artifact.id.as_uuid())
        })
        .collect::<Vec<_>>();
    let published_artifact_kinds = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_artifact
                .as_ref()
                .map(|artifact| artifact.kind.as_str())
        })
        .collect::<Vec<_>>();
    let published_artifact_origins = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_artifact
                .as_ref()
                .map(|artifact| artifact.origin.as_str())
        })
        .collect::<Vec<_>>();
    let published_execution_generations = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_execution_identity
                .as_ref()
                .map(|identity| identity.generation.get())
        })
        .collect::<Vec<_>>();
    let published_pod_uids = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_execution_identity
                .as_ref()
                .map(|identity| identity.pod_uid.as_str())
        })
        .collect::<Vec<_>>();
    let published_pod_uids_were_missing = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_execution_identity
                .as_ref()
                .map(|identity| identity.pod_uid_was_missing)
        })
        .collect::<Vec<_>>();
    let candidate_cte = "candidate AS (
             SELECT *
               FROM UNNEST(
                   $1::UUID[], $2::BOOL[], $3::UUID[], $4::TEXT[], $5::TEXT[],
                   $6::BIGINT[], $7::TEXT[], $8::BOOL[]
               ) AS candidate(
                   id, clear_marker, published_artifact_id,
                   published_artifact_kind, published_artifact_origin,
                   published_execution_generation, published_pod_uid,
                   published_pod_uid_was_missing
               )
         )";
    let exact_execution_identity = node_published_execution_identity_matches(
        "e",
        "candidate.published_execution_generation",
        "candidate.published_pod_uid",
        "candidate.published_pod_uid_was_missing",
    );
    let finalization_query = format!(
        "WITH {candidate_cte},
         retired_execution AS (
             UPDATE sandbox_k8s_executions e
                SET {active_execution_assignments}
               FROM candidate
              WHERE e.sandbox_id = candidate.id
                AND e.active
                AND (
                    candidate.published_artifact_id IS NULL
                    OR ({exact_execution_identity})
                )
             RETURNING e.sandbox_id
         ),
         eligible_candidate AS (
             SELECT candidate.*
               FROM candidate
              WHERE candidate.published_artifact_id IS NULL
                 OR EXISTS (
                     SELECT 1
                       FROM retired_execution retired
                      WHERE retired.sandbox_id = candidate.id
                 )
         ),
         reset_port_exposures AS (
             UPDATE sandbox_port_exposures spe
                SET {open_port_exposure_assignments}
               FROM eligible_candidate candidate
              WHERE spe.sandbox_id = candidate.id
                AND spe.desired_state = 'open'
             RETURNING spe.sandbox_id
         )
         UPDATE sandboxes s
            SET {sandbox_assignments}
           FROM eligible_candidate candidate
          WHERE s.id = candidate.id
          RETURNING s.id, candidate.clear_marker",
        active_execution_assignments = finalization.active_execution_assignments("e"),
        open_port_exposure_assignments = finalization.open_port_exposure_assignments(),
        sandbox_assignments = finalization.sandbox_assignments(),
    );
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(finalization_query.as_str())))
        .bind(&candidate_ids)
        .bind(&clear_markers)
        .bind(&published_artifact_ids)
        .bind(&published_artifact_kinds)
        .bind(&published_artifact_origins)
        .bind(&published_execution_generations)
        .bind(&published_pod_uids)
        .bind(&published_pod_uids_were_missing)
        .fetch_all(executor)
        .await
}

/// Converge the publisher's `restoring` rows it published `started` to
/// `lifecycle = 'started'`, returning the ids that converged. **Observed
/// projection**: the node reported the restored runtime running.
///
/// `rows` is a recordset of `{sandbox_id, status, transition_receipt_kind}`.
/// The write re-checks that the row is on `node_id` and still `restoring`, and
/// leaves alone a row whose publication carries an activation-completed
/// receipt (the receipt path owns it), a row marked for the node-drain
/// redrive (`evacuation_restore_target`), and a Kubernetes row with no
/// routable execution
/// ([`pplx_space_sandbox_routability::routable_execution_or_non_k8s`]), which
/// stays `restoring` so the next connect can hydrate the in-flight pod.
pub async fn converge_published_restoring_to_started(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: Json<JsonValue>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    // Publishing `started` proves the node-local runtime finished, but for
    // Kubernetes sandboxes the control plane also needs a routable active
    // execution row. The blocking wake/restore path is what hydrates that row
    // with `running` + pod IP; if it timed out first, keep the row in
    // `restoring` so the next connect can wait on and hydrate the in-flight
    // pod instead of treating it as a started sandbox with a broken execution.
    // Evacuation-marked rows are owned by the node-drain redrive (the same
    // exemption the sweeper makes) and are left alone.
    let routable_execution =
        pplx_space_sandbox_routability::routable_execution_or_non_k8s("s", "$4");
    let restored_convergence_query = format!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT,
                 transition_receipt_kind TEXT
             )
         )
         UPDATE sandboxes s
         SET lifecycle = 'started'
         FROM input
         WHERE s.id = input.sandbox_id
           AND s.active_node_id = $1
           AND s.evacuation_restore_target IS NULL
           AND LOWER(TRIM(input.status)) = 'started'
           AND input.transition_receipt_kind IS DISTINCT FROM $3
           AND LOWER(TRIM(s.lifecycle)) = 'restoring'
           AND {routable_execution}
         RETURNING s.id"
    );
    sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        restored_convergence_query.as_str()
    )))
    .bind(node_id)
    .bind(rows)
    .bind(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor)
    .await
}

/// Release the exact activation attempts that published activation-completed
/// receipts, and project each receipt's target lifecycle onto its sandbox,
/// returning the ids projected. **Operation step**: ends the in-flight
/// activation the receipt names.
///
/// `rows` is a recordset of [`TRANSITION_RECEIPT_RECORD_COLUMNS`]. The
/// execution release requires an activation-completed receipt whose
/// published status equals its target state, naming the active execution's
/// generation, activation attempt and target state; a route activation to be
/// present; and either a `created` lifecycle (for a `created` target) or a
/// running execution with a pod IP (for `started` and `paused`). It checks
/// `active_node_id` only through an unlocked join, as of the statement's
/// snapshot, and the `sandboxes` update does not re-check it, so a change to
/// `active_node_id` committed after that snapshot is not seen. The lifecycle
/// projection re-checks only that the row is outside the suspend, hold and
/// delete lifecycles: if a newer suspend owns the row, the attempt is still
/// released but its stale target is not projected.
pub async fn release_completed_activations(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: Json<JsonValue>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let query = format!(
        "WITH input AS (
             SELECT *
               FROM jsonb_to_recordset($2::jsonb) AS published({TRANSITION_RECEIPT_RECORD_COLUMNS})
         ), released AS (
             UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL,
                    updated_at = NOW()
               FROM sandboxes sandbox, input
              WHERE sandbox.id = execution.sandbox_id
                AND input.sandbox_id = sandbox.id
                AND sandbox.active_node_id = $1
                AND execution.active
                AND execution.generation = input.transition_receipt_execution_generation
                AND execution.activation_attempt_id = input.transition_receipt_activation_attempt_id
                AND execution.route_activation_id IS NOT NULL
                AND execution.activation_attempt_target_state = input.transition_receipt_target_state
                AND input.transition_receipt_kind = $3
                AND LOWER(TRIM(input.status)) = input.transition_receipt_target_state
                AND (
                    (
                        input.transition_receipt_target_state = 'created'
                        AND LOWER(TRIM(sandbox.lifecycle)) = 'created'
                    )
                    OR (
                        input.transition_receipt_target_state IN ('started', 'paused')
                        AND execution.state = 'running'
                        AND execution.pod_ip IS NOT NULL
                    )
                )
              RETURNING execution.sandbox_id, input.transition_receipt_target_state
         )
         UPDATE sandboxes sandbox
            SET lifecycle = released.transition_receipt_target_state,
                placement = CASE
                    WHEN released.transition_receipt_target_state IN ('started', 'paused')
                    THEN 'on_node'
                    ELSE sandbox.placement
                END
           FROM released
          WHERE sandbox.id = released.sandbox_id
            AND LOWER(TRIM(sandbox.lifecycle)) NOT IN (
                'suspending', 'suspended', 'quarantined', 'deleting', 'deleted', 'cleanup_required'
            )
         RETURNING sandbox.id"
    );
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(rows)
        .bind(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
        .fetch_all(executor)
        .await
}

/// The quarantine source of an sbx-manager pause failure, as recorded.
pub const PAUSE_FAILED_QUARANTINE_SOURCE: SandboxQuarantineSource =
    SandboxQuarantineSource::SbxManager;

/// The quarantine reason code of an sbx-manager pause failure, as recorded.
pub const PAUSE_FAILED_QUARANTINE_REASON: SandboxQuarantineReasonCode =
    SandboxQuarantineReasonCode::PauseFailed;

/// Recover a `quarantined` row from an sbx-manager pause failure to the
/// healthy state its node published, clearing the observed failure and
/// retiring the quarantine with a history entry. Returns `true` when the
/// quarantine was retired. `false` may follow a partial write: the
/// `sandboxes` update runs to completion even when the observed-state,
/// history or retirement step matches nothing, so the caller must roll back
/// its transaction on `false`. **Observed projection**: the node reported a
/// definitive healthy runtime state.
///
/// The write re-checks that the row is on `node_id`, still `quarantined`, and
/// still held by `quarantine_id` from an sbx-manager pause failure; the caller
/// holds both the sandbox and the quarantine row `FOR UPDATE` from its
/// selection. The lifecycle check is exact (`lifecycle = 'quarantined'`),
/// where the caller's selection compares `LOWER(TRIM(lifecycle))`: a selected
/// row whose stored text is not canonical, such as ` Quarantined `, returns
/// `false`.
pub async fn recover_published_pause_failed(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    sandbox_id: SandboxId,
    quarantine_id: Uuid,
    target_state: SandboxTransitionTargetState,
    action: SandboxQuarantineHistoryAction,
) -> Result<bool, sqlx::Error> {
    let quarantine_source = PAUSE_FAILED_QUARANTINE_SOURCE.as_str();
    let quarantine_reason = PAUSE_FAILED_QUARANTINE_REASON.as_str();
    let recovered = sqlx::query_scalar::<_, Uuid>(sql!(
        "WITH recovered AS (
                 UPDATE sandboxes
                    SET lifecycle = $1
                  WHERE id = $2
                    AND active_node_id = $3
                    AND lifecycle = 'quarantined'
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_quarantine_state
                         WHERE sandbox_id = $2
                           AND quarantine_id = $4
                           AND source = $5
                           AND reason_code = $6
                    )
                 RETURNING id
             ),
             observed AS (
                 UPDATE sandbox_observed_states observed
                    SET failure_reason = NULL
                   FROM recovered
                  WHERE observed.sandbox_id = recovered.id
                    AND observed.node_id = $3
                 RETURNING observed.sandbox_id
             ),
             recorded AS (
                 INSERT INTO sandbox_quarantine_history (
                     id, quarantine_id, sandbox_id, occurred_at, source, action,
                     reason_code, message, next_action, attempt_count, evidence
                 )
                 SELECT $7, quarantine.quarantine_id, quarantine.sandbox_id,
                        NOW(), $8, $9, quarantine.reason_code,
                        'node inventory reported a definitive healthy runtime state',
                        NULL, quarantine.attempt_count,
                        quarantine.evidence
                            || jsonb_build_object('recovered_by_node_id', $10::TEXT)
                   FROM sandbox_quarantine_state quarantine
                   JOIN observed ON observed.sandbox_id = quarantine.sandbox_id
                  WHERE quarantine.sandbox_id = $2
                    AND quarantine.quarantine_id = $4
                    AND quarantine.source = $5
                    AND quarantine.reason_code = $6
                 RETURNING sandbox_id, quarantine_id
             ),
             retired AS (
                 DELETE FROM sandbox_quarantine_state quarantine
                  USING recorded
                  WHERE quarantine.sandbox_id = recorded.sandbox_id
                    AND quarantine.quarantine_id = recorded.quarantine_id
                    AND quarantine.source = $5
                    AND quarantine.reason_code = $6
                 RETURNING quarantine.sandbox_id
             )
             SELECT sandbox_id FROM retired"
    ))
    .bind(target_state.as_str())
    .bind(sandbox_id)
    .bind(node_id)
    .bind(quarantine_id)
    .bind(quarantine_source)
    .bind(quarantine_reason)
    .bind(Uuid::now_v7())
    .bind(SandboxQuarantineSource::NodeStatePublish.as_str())
    .bind(action.as_str())
    .bind(node_id.to_string())
    .fetch_optional(executor)
    .await?;
    Ok(recovered == Some(sandbox_id.as_uuid()))
}

/// Built once: every input is bound, so the text is the same on every publish.
/// Mirrors the composed statement in [`crate::lost`].
///
/// The interpolated term comes from `pplx_space_sandbox_portability` rather
/// than being spelled here because that crate is the single definition of "are
/// this sandbox's bytes recoverable elsewhere", and a deletion path is exactly
/// where a second spelling does damage.
static PRUNE_ABSENT_NODE_SANDBOXES_QUERY: LazyLock<String> = LazyLock::new(|| {
    let released_with_durable_artifact =
        pplx_space_sandbox_portability::stopped_artifact_durable("s");
    sql!(
        format!(
            "WITH removed AS (
             DELETE FROM sandbox_observed_states sos
             WHERE sos.node_id = $1
               AND NOT (sos.sandbox_id = ANY($2))
             RETURNING sos.sandbox_id
         ),
         removed_elements AS (
             SELECT 'sandbox:' || sandbox_id::text AS element
             FROM removed
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING removed_elements
             WHERE nfos.node_id = $1
               AND nfos.facet = $3
               AND nfos.element = removed_elements.element
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING removed_elements
             WHERE nfc.node_id = $1
               AND nfc.facet = $3
               AND nfc.element = removed_elements.element
         ),
         marked_deleted AS (
             UPDATE sandboxes s
             SET lifecycle = 'deleted',
                 placement = 'unplaced',
                 active_node_id = NULL,
                 target_node_id = NULL,
                 placement_changed_at = NOW(),
                 lifecycle_changed_at = NOW(),
                 updated_at = NOW(),
                 command_ready_pending = FALSE,
                 restore_lease_expires_at = NULL,
                 restore_lease_token = NULL
             WHERE s.active_node_id = $1
               AND s.managed_by = $4
               AND s.lifecycle NOT IN ('deleted', 'deleting')
               AND NOT {released_with_durable_artifact}
               AND NOT (s.id = ANY($2))
             RETURNING s.id AS sandbox_id
         )
         DELETE FROM sandbox_snapshot_inventory
         WHERE sandbox_id IN (SELECT sandbox_id FROM removed)
            OR sandbox_id IN (SELECT sandbox_id FROM marked_deleted)"
        )
        .as_str()
    )
});

/// Reconcile a node's complete sandbox census: forget the observations of
/// sandboxes it no longer reports, and tombstone the Direct rows on it that
/// the census omits. **Inference**: the control plane reads absence from a
/// complete census as proof a Direct sandbox is unrecoverable.
///
/// The tombstone skips rows already `deleted` or `deleting`, rows of other
/// substrates (`managed_by` bound to [`SubstrateKind::Space`]) and stopped
/// rows whose artifact is durable
/// ([`pplx_space_sandbox_portability::stopped_artifact_durable`]). The caller
/// decides the census is complete and documents why the absence is sound.
/// `network_policy_facet` is the facet name of the per-sandbox network-policy
/// rows it forgets.
pub async fn prune_census_absent_sandboxes(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    present_sandbox_ids: &[SandboxId],
    network_policy_facet: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sqlx::AssertSqlSafe(
        PRUNE_ABSENT_NODE_SANDBOXES_QUERY.as_str(),
    ))
    .bind(node_id)
    .bind(present_sandbox_ids)
    .bind(network_policy_facet)
    .bind(SubstrateKind::Space)
    .execute(executor)
    .await?;
    Ok(())
}
