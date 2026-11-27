//! Capture order, computed once per row.
//!
//! The order fragments compare snapshots by [`super::capture_rank`] and
//! [`super::capture_registered_at`]. Inlining those expressions at every
//! comparison nests them: a newer-capture check computes the rank of each
//! newer row, each rank computes the first registration of every row it
//! compares, and a gate that combines several checks repeats the lot. The
//! statement grows multiplicatively, and Postgres plans and runs every copy.
//!
//! Here the sandbox's full snapshots become one relation, `capture_order`,
//! that carries each row's first registration and rank as columns, computed
//! once per row. A fragment over it is a single sublink: its text grows with
//! the number of comparisons, not their product. The relation keeps the
//! columns of `snapshots` the order fragments and their callers' conditions
//! read, under the same names.
//!
//! The fragments reserve the aliases `capture_backups`, `capture_order`,
//! `capture_execution`, `f`, `b`, `rank_later`, `rank_before`, `me`, `dom`,
//! `newer`, `unordered`, `pinnable_later`, `pinnable_ssl`, `post_claim` and
//! `pin_exec`. A caller's sandbox and snapshot aliases must be other names, or
//! the fragment's own alias shadows them.

use pplx_space_snapshot_model::ArtifactOrigin;

use super::{registered_before_restore_claim, restore_retry_source_snapshot_window};

/// The ranks a [`with_capture_order`] relation carries.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ranks {
    /// `rank`: [`super::capture_rank`].
    Plain,
    /// `rank` and `retry_rank`: [`super::capture_rank_in_retry_window`].
    WithRetry,
}

/// The `WITH` clause that defines `capture_order` for the sandbox aliased as
/// `sandbox_alias`: its full snapshots, in any state, with
///
/// - `registered_at`: [`super::capture_registered_at`]. The earliest
///   `timestamp` of the row's artifact (`local_artifact_id`, `artifact_kind`,
///   with a NULL kind matching only NULL), or the row's own without an id;
/// - `artifact_generation`: the lowest generation a backup of the row's
///   artifact records, NULL without an id;
/// - `execution_created_at`: when the sandbox's execution of the row's
///   generation started, NULL when it has none (generations are unique per
///   sandbox);
/// - `rank`, and for [`Ranks::WithRetry`] `retry_rank`, with
///   `retry_artifact_generation`: `artifact_generation` among the backups
///   registered before the restore claim.
///
/// Both CTEs are `MATERIALIZED`, so each row's values are computed once per
/// evaluation of the enclosing sublink, however many comparisons read them.
pub(crate) fn with_capture_order(sandbox_alias: &str, ranks: Ranks) -> String {
    with_capture_order_where(sandbox_alias, ranks, None)
}

/// [`with_capture_order`] restricted to full snapshots registered before the
/// current `restoring` claim ([`registered_before_restore_claim`]), so a
/// post-claim snapshot published by a failed restore branch can't hide or
/// re-rank the pre-claim captures an operator could pin.
pub(crate) fn with_pre_claim_capture_order(sandbox_alias: &str) -> String {
    let before_claim = registered_before_restore_claim(sandbox_alias, "f");
    with_capture_order_where(sandbox_alias, Ranks::Plain, Some(&before_claim))
}

fn with_capture_order_where(
    sandbox_alias: &str,
    ranks: Ranks,
    snapshot_filter: Option<&str>,
) -> String {
    let rank = rank_expression("TRUE", "b.artifact_generation", "TRUE");
    let extra_where = snapshot_filter
        .map(|filter| format!("\n                    AND {filter}"))
        .unwrap_or_default();
    let (retry_generation, retry_rank) = match ranks {
        Ranks::Plain => (String::new(), String::new()),
        Ranks::WithRetry => {
            let window = restore_retry_source_snapshot_window(sandbox_alias, "rank_later");
            let claimed = registered_before_restore_claim(sandbox_alias, "f");
            let before_claimed = registered_before_restore_claim(sandbox_alias, "rank_before");
            (
                format!(
                    ",\n                        CASE WHEN f.local_artifact_id IS NOT NULL\n                             THEN MIN(f.generation) FILTER (WHERE {claimed}) OVER capture_artifact\n                        END AS retry_artifact_generation"
                ),
                format!(
                    ",\n                        {} AS retry_rank",
                    rank_expression(&window, "b.retry_artifact_generation", &before_claimed)
                ),
            )
        }
    };
    format!(
        "WITH capture_backups AS MATERIALIZED (
                 SELECT f.id, f.state, f.generation, f.artifact_origin,
                        f.local_artifact_id, f.artifact_kind, f.timestamp,
                        CASE WHEN f.local_artifact_id IS NULL THEN f.timestamp
                             ELSE MIN(f.timestamp) OVER capture_artifact
                        END AS registered_at,
                        CASE WHEN f.local_artifact_id IS NOT NULL
                             THEN MIN(f.generation) OVER capture_artifact
                        END AS artifact_generation{retry_generation},
                        (SELECT capture_execution.created_at
                           FROM sandbox_k8s_executions capture_execution
                          WHERE capture_execution.sandbox_id = {sandbox_alias}.id
                            AND capture_execution.generation = f.generation) AS execution_created_at
                   FROM snapshots f
                  WHERE f.sandbox_id = {sandbox_alias}.id
                    AND f.type = 'full'{extra_where}
                 WINDOW capture_artifact AS (PARTITION BY f.local_artifact_id, f.artifact_kind)
             ),
             capture_order AS MATERIALIZED (
                 SELECT b.*,
                        {rank} AS rank{retry_rank}
                   FROM capture_backups b
             )"
    )
}

/// The rank of the `capture_backups` row aliased `b`, counting as a later
/// execution's capture only rows `rank_later` for which `later_condition`
/// holds, inheriting the generation in the column `artifact_generation`,
/// and letting only rows `rank_before` for which `before_condition` holds
/// keep a later capture from ranking it. [`super::capture_rank`] states the
/// rule.
fn rank_expression(
    later_condition: &str,
    artifact_generation: &str,
    before_condition: &str,
) -> String {
    format!(
        "COALESCE(b.generation, {artifact_generation}, CASE WHEN EXISTS (
                     SELECT 1
                       FROM capture_backups rank_later
                      WHERE rank_later.generation IS NOT NULL
                        AND {later_condition}
                        AND CASE WHEN rank_later.execution_created_at IS NOT NULL
                                 THEN rank_later.execution_created_at > b.registered_at
                                 ELSE rank_later.registered_at > b.registered_at
                                      AND NOT EXISTS (
                                          SELECT 1
                                            FROM capture_backups rank_before
                                           WHERE rank_before.generation >= rank_later.generation
                                             AND {before_condition}
                                             AND rank_before.registered_at <= b.registered_at
                                      )
                            END
                 ) THEN 0 END)"
    )
}

/// An `EXISTS` over `capture_order`: some row `dom` for which `condition`
/// holds records a higher generation than `rank`. Never TRUE for a NULL rank.
pub(crate) fn outranked(rank: &str, condition: &str) -> String {
    format!(
        "EXISTS (
             SELECT 1
               FROM capture_order dom
              WHERE dom.generation IS NOT NULL
                AND dom.generation > {rank}
                AND {condition}
         )"
    )
}

/// What [`newest`] compares.
pub(crate) struct Newest<'a> {
    /// The `capture_order` rank column, `rank` or `retry_rank`.
    pub(crate) rank: &'a str,
    /// Which rows `newer` may make the snapshot stale.
    pub(crate) newer: &'a str,
    /// Which rows `dom` may outrank either.
    pub(crate) outranking: &'a str,
}

/// The `capture_order` row aliased `snapshot` is newest: no row `dom`
/// outranks it, and no row `newer` registered after it that nothing
/// outranks. [`super::no_newer_full_snapshot`] states the rule.
pub(crate) fn newest(snapshot: &str, order: &Newest<'_>) -> String {
    let rank = order.rank;
    let outranked_self = outranked(&format!("{snapshot}.{rank}"), order.outranking);
    let outranked_newer = outranked(&format!("newer.{rank}"), order.outranking);
    format!(
        "(NOT {outranked_self}
          AND NOT EXISTS (
              SELECT 1
                FROM capture_order newer
               WHERE newer.registered_at > {snapshot}.registered_at
                 AND {newer}
                 AND NOT {outranked_newer}
          ))",
        newer = order.newer
    )
}

/// An `EXISTS` over `capture_order`: a suspend capture without a generation,
/// for which `condition` holds, has no rank in the column `rank`. Only a
/// suspend capture counts: an unpinned restore loads nothing else, and a
/// node never registers a named snapshot or a stop's backup with a
/// generation, so one of those would leave every history it joins unordered.
pub(crate) fn unranked_capture(rank: &str, condition: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "EXISTS (
             SELECT 1
               FROM capture_order unordered
              WHERE unordered.generation IS NULL
                AND unordered.{rank} IS NULL
                AND COALESCE(unordered.artifact_origin, '{suspend}') = '{suspend}'
                AND {condition}
         )"
    )
}

/// Evaluates `predicate` over the `capture_order` row aliased `me` that is
/// the snapshot aliased `snapshot_alias`, a full snapshot of the sandbox
/// aliased `sandbox_alias`: one sublink for the whole predicate.
pub(crate) fn for_snapshot(
    sandbox_alias: &str,
    ranks: Ranks,
    snapshot_alias: &str,
    predicate: &str,
) -> String {
    let order = with_capture_order(sandbox_alias, ranks);
    format!(
        "EXISTS (
             {order}
             SELECT 1
               FROM capture_order me
              WHERE me.id = {snapshot_alias}.id
                AND {predicate}
         )"
    )
}
