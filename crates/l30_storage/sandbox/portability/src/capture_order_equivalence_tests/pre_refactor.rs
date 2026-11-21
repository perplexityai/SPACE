//! The order fragments as they were before [`crate::capture_order`] computed
//! each snapshot's rank and first registration once per row: every
//! comparison inlined them. Frozen verbatim, for the equivalence test only,
//! except for four later policy changes carried in the inlined form:
//! [`released_unpinned_copy`]'s refusal of an outranked copy,
//! [`restore_order_unordered_where`]'s count of suspend captures only,
//! [`capture_rank_where`]'s retry rank reading only pre-claim generations,
//! and the failed-retry exception's refusal to excuse a newer failed row
//! with no artifact id.

use crate::{
    registered_before_restore_claim, restore_retry_source_snapshot_window,
    snapshot_is_current_local_artifact,
};
use pplx_space_snapshot_model::ArtifactOrigin;

pub(super) fn no_newer_full_snapshot(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_generation(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
         ))"
    )
}

pub(super) fn no_newer_suspend_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_generation(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND COALESCE(newer.artifact_origin, '{suspend}') = '{suspend}'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
         ))"
    )
}

pub(super) fn full_snapshot_outranked_by_live_capture(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let rank = capture_rank(snapshot_alias);
    full_snapshot_outranked_where(sandbox_alias, &rank, "dom.state <> 'failed'")
}

pub(super) fn no_newer_live_suspend_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let outranked = full_snapshot_outranked_by_live_capture(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_live_capture(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND newer.state <> 'failed'\n\
                AND COALESCE(newer.artifact_origin, '{suspend}') = '{suspend}'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
         ))"
    )
}

pub(super) fn capture_registered_at(snapshot_alias: &str) -> String {
    format!(
        "COALESCE((\n\
             SELECT MIN(first_backup.timestamp)\n\
               FROM snapshots first_backup\n\
              WHERE first_backup.sandbox_id = {snapshot_alias}.sandbox_id\n\
                AND first_backup.type = {snapshot_alias}.type\n\
                AND first_backup.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                AND first_backup.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
         ), {snapshot_alias}.timestamp)"
    )
}

pub(super) fn capture_rank(snapshot_alias: &str) -> String {
    capture_rank_where(snapshot_alias, "TRUE", None)
}

pub(super) fn capture_rank_in_retry_window(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let window = restore_retry_source_snapshot_window(sandbox_alias, "rank_later");
    capture_rank_where(snapshot_alias, &window, Some(sandbox_alias))
}

pub(super) fn capture_rank_where(
    snapshot_alias: &str,
    later_condition: &str,
    claimed_by: Option<&str>,
) -> String {
    let later = capture_registered_at("rank_later");
    let before = capture_registered_at("rank_before");
    let this = capture_registered_at(snapshot_alias);
    let claimed = |alias: &str| {
        claimed_by.map_or_else(
            || "TRUE".to_owned(),
            |sandbox| registered_before_restore_claim(sandbox, alias),
        )
    };
    let same_artifact_claimed = claimed("same_artifact");
    let before_claimed = claimed("rank_before");
    format!(
        "COALESCE({snapshot_alias}.generation, (\n\
             SELECT MIN(same_artifact.generation)\n\
               FROM snapshots same_artifact\n\
              WHERE same_artifact.sandbox_id = {snapshot_alias}.sandbox_id\n\
                AND same_artifact.type = {snapshot_alias}.type\n\
                AND same_artifact.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                AND same_artifact.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                AND same_artifact.generation IS NOT NULL\n\
                AND {same_artifact_claimed}\n\
         ), CASE WHEN EXISTS (\n\
             SELECT 1\n\
               FROM snapshots rank_later\n\
              WHERE rank_later.sandbox_id = {snapshot_alias}.sandbox_id\n\
                AND rank_later.type = 'full'\n\
                AND rank_later.generation IS NOT NULL\n\
                AND {later_condition}\n\
                AND CASE WHEN EXISTS (\n\
                         SELECT 1\n\
                           FROM sandbox_k8s_executions rank_execution\n\
                          WHERE rank_execution.sandbox_id = {snapshot_alias}.sandbox_id\n\
                            AND rank_execution.generation = rank_later.generation\n\
                     )\n\
                     THEN NOT EXISTS (\n\
                         SELECT 1\n\
                           FROM sandbox_k8s_executions rank_execution\n\
                          WHERE rank_execution.sandbox_id = {snapshot_alias}.sandbox_id\n\
                            AND rank_execution.generation = rank_later.generation\n\
                            AND rank_execution.created_at <= {this}\n\
                     )\n\
                     ELSE {later} > {this}\n\
                          AND NOT EXISTS (\n\
                              SELECT 1\n\
                                FROM snapshots rank_before\n\
                               WHERE rank_before.sandbox_id = {snapshot_alias}.sandbox_id\n\
                                 AND rank_before.type = 'full'\n\
                                 AND rank_before.generation >= rank_later.generation\n\
                                 AND {before_claimed}\n\
                                 AND {before} <= {this}\n\
                          )\n\
                     END\n\
         ) THEN 0 END)"
    )
}

pub(super) fn restore_order_unordered(sandbox_alias: &str) -> String {
    restore_order_unordered_where(sandbox_alias, &capture_rank("unordered"), "TRUE", "TRUE")
}

pub(super) fn restore_order_unordered_in_retry_window(sandbox_alias: &str) -> String {
    let window = restore_retry_source_snapshot_window(sandbox_alias, "unordered");
    let rank = capture_rank_in_retry_window(sandbox_alias, "unordered");
    let era_window = restore_retry_source_snapshot_window(sandbox_alias, "era");
    restore_order_unordered_where(sandbox_alias, &rank, &window, &era_window)
}

pub(super) fn restore_order_unordered_where(
    sandbox_alias: &str,
    rank: &str,
    condition: &str,
    era_condition: &str,
) -> String {
    // The probe for a capture with a generation goes first: a sandbox with
    // none, nearly every one, never ranks its rows without one.
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "(EXISTS (\n\
             SELECT 1\n\
               FROM snapshots era\n\
              WHERE era.sandbox_id = {sandbox_alias}.id\n\
                AND era.type = 'full'\n\
                AND era.generation IS NOT NULL\n\
                AND {era_condition}\n\
         )\n\
         AND EXISTS (\n\
             SELECT 1\n\
               FROM snapshots unordered\n\
              WHERE unordered.sandbox_id = {sandbox_alias}.id\n\
                AND unordered.type = 'full'\n\
                AND unordered.generation IS NULL\n\
                AND {rank} IS NULL\n\
                AND COALESCE(unordered.artifact_origin, '{suspend}') = '{suspend}'\n\
                AND {condition}\n\
         ))"
    )
}

pub(super) fn full_snapshot_outranked_by_generation(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let rank = capture_rank(snapshot_alias);
    full_snapshot_outranked_where(sandbox_alias, &rank, "TRUE")
}

pub(super) fn full_snapshot_outranked_where(
    sandbox_alias: &str,
    rank: &str,
    condition: &str,
) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots dom\n\
              WHERE dom.sandbox_id = {sandbox_alias}.id\n\
                AND dom.type = 'full'\n\
                -- First, so a row without a generation never computes the rank.\n\
                AND dom.generation IS NOT NULL\n\
                AND dom.generation > {rank}\n\
                AND {condition}\n\
         )"
    )
}

pub(super) fn no_newer_full_snapshot_ignoring_failed_retry(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let suspend = ArtifactOrigin::Suspend.as_str();
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_generation(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
                AND NOT (\n\
                    {snapshot_alias}.local_artifact_id IS NOT NULL\n\
                    AND newer.state = 'failed'\n\
                    AND newer.local_artifact_id IS NOT DISTINCT FROM {snapshot_alias}.local_artifact_id\n\
                    AND newer.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                    AND COALESCE(newer.artifact_origin, '{suspend}') =\n\
                        COALESCE({snapshot_alias}.artifact_origin, '{suspend}')\n\
                )\n\
         ))"
    )
}

pub(super) fn restore_retry_source_snapshot_is_current(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let suspend = ArtifactOrigin::Suspend.as_str();
    let source_window = restore_retry_source_snapshot_window(sandbox_alias, snapshot_alias);
    let newer_source_window = restore_retry_source_snapshot_window(sandbox_alias, "newer");
    let outranked = full_snapshot_outranked_in_retry_window(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_in_retry_window(sandbox_alias, "newer");
    format!(
        "({source_window}\n\
         AND NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND {newer_captured} > {captured}\n\
                AND {newer_source_window}\n\
                AND NOT {newer_outranked}\n\
                AND NOT (\n\
                    {snapshot_alias}.local_artifact_id IS NOT NULL\n\
                    AND newer.state = 'failed'\n\
                    AND newer.local_artifact_id IS NOT DISTINCT FROM {snapshot_alias}.local_artifact_id\n\
                    AND newer.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                    AND COALESCE(newer.artifact_origin, '{suspend}') =\n\
                        COALESCE({snapshot_alias}.artifact_origin, '{suspend}')\n\
                )\n\
         ))"
    )
}

pub(super) fn full_snapshot_outranked_in_retry_window(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let window = restore_retry_source_snapshot_window(sandbox_alias, "dom");
    let rank = capture_rank_in_retry_window(sandbox_alias, snapshot_alias);
    full_snapshot_outranked_where(sandbox_alias, &rank, &window)
}

pub(super) fn snapshot_durable_for_source_release(sandbox_alias: &str) -> String {
    let unordered = restore_order_unordered(sandbox_alias);
    // An unpinned row releases its source only on a copy the restore would
    // load: a suspend capture, by the rules below. A named snapshot's or a
    // stop's backup isn't one, and while it outranks the suspend capture,
    // nothing is.
    let copy =
        durable_copy_exists_where(sandbox_alias, &released_unpinned_copy(sandbox_alias, true));
    // The unordered check is per sandbox, so it's ANDed outside the copy's
    // EXISTS, once.
    format!(
        "(({sandbox_alias}.current_local_artifact_id IS NOT NULL OR NOT {unordered})
         AND {copy})"
    )
}

pub(super) fn snapshot_durable_copy_exists(sandbox_alias: &str) -> String {
    let unordered = restore_order_unordered(sandbox_alias);
    let ordered =
        ordered_unpinned_copy(sandbox_alias, &no_newer_full_snapshot(sandbox_alias, "sn"));
    // Everything the source-release gate would accept counts too.
    let released = released_unpinned_copy(sandbox_alias, false);
    durable_copy_exists_where(
        sandbox_alias,
        &format!("({unordered} OR {ordered} OR {released})"),
    )
}

/// Carries the later policy change the module's fragments made after the
/// rank moved, in the inlined form: with `refuse_outranked`, an unmarked
/// row's copy that any capture outranks isn't released, a failed one
/// included. The give-up passes `false`, as it does there.
pub(super) fn released_unpinned_copy(sandbox_alias: &str, refuse_outranked: bool) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let newest = no_newer_suspend_capture(sandbox_alias, "sn");
    let newest_live = no_newer_live_suspend_capture(sandbox_alias, "sn");
    let captured = capture_registered_at("sn");
    let outranked = if refuse_outranked {
        full_snapshot_outranked_by_generation(sandbox_alias, "sn")
    } else {
        "FALSE".to_owned()
    };
    format!(
        "((sn.artifact_origin IS NULL OR sn.artifact_origin = '{suspend}')
          AND (({sandbox_alias}.evacuation_restore_target IS NOT NULL AND {newest})
               OR ({sandbox_alias}.evacuation_restore_target IS NULL
                   AND {newest_live}
                   AND NOT {outranked}
                   AND ({sandbox_alias}.placement_changed_at IS NULL
                        OR {captured} >= {sandbox_alias}.placement_changed_at))))"
    )
}

pub(super) fn ordered_unpinned_copy(sandbox_alias: &str, no_newer: &str) -> String {
    let captured = capture_registered_at("sn");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, "sn");
    format!(
        "(({sandbox_alias}.evacuation_restore_target IS NOT NULL AND {no_newer})
          OR (
              {sandbox_alias}.evacuation_restore_target IS NULL
              AND (
                  {sandbox_alias}.placement_changed_at IS NULL
                  OR {captured} >= {sandbox_alias}.placement_changed_at
              )
              AND NOT {outranked}
          ))"
    )
}

pub(super) fn durable_copy_exists_where(sandbox_alias: &str, unpinned: &str) -> String {
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
    // Keep the sandbox correlation in a single EXISTS so a fleet-wide caller
    // can use a hash semi/anti join. OR-ing complete EXISTS expressions makes
    // Postgres probe snapshots and storage locations separately for each row.
    format!(
        "EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = {sandbox_alias}.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    {pinned}
                    OR ({sandbox_alias}.current_local_artifact_id IS NULL AND {unpinned})
                )
         )"
    )
}
