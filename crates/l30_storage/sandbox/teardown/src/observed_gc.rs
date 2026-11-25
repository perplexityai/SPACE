//! The cluster controller's observed-projection GC sweeps over
//! `sandbox_observed_states`: each deletes a window's cold observed rows and,
//! in the same statement, their sandboxes' `network-policy`
//! (`sandbox:<id>`), `sandbox-inventory` and `sandbox-desired-state`
//! observed-status and condition rows on the row's node.
//!
//! Each verb is one statement: one candidate set (`stale`) binds all three
//! deletes from one snapshot, so a sandbox's observed row and its facet rows
//! go together or not at all. The candidate reads join `sandboxes` without
//! locking it. Both compare against the database clock (`NOW()`), so each
//! runs on the executor its caller passes, the pool today, and binds the ages
//! as seconds. Candidates are picked unordered within the window `(from, to]`
//! and bounded by `limit`. Each returns the rows deleted from
//! `sandbox_observed_states`, `node_facet_observed_status` and
//! `node_facet_conditions`, in that order. The deleted observed rows'
//! `sandbox_snapshot_inventory` rows go with them through that table's
//! foreign-key cascade.

use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Rows deleted from `sandbox_observed_states`, `node_facet_observed_status`
/// and `node_facet_conditions`, in that order.
pub type DeletedCounts = (i64, i64, i64);

/// Delete up to `limit` observed rows in `(from, to]` of sandboxes that have
/// been `suspended` for `stale_suspended_age_secs` (their `sandboxes` row
/// unchanged that long), with their sandbox facet rows, unless the row's node
/// observed a `quarantine_recovery` for the sandbox within
/// `orphan_age_secs`.
pub async fn delete_stale_suspended(
    executor: impl sqlx::PgExecutor<'_>,
    stale_suspended_age_secs: i64,
    limit: i64,
    orphan_age_secs: i64,
    from: Uuid,
    to: Uuid,
) -> Result<DeletedCounts, sqlx::Error> {
    sqlx::query_as(sql!(
        // `stale` keeps MATERIALIZED for its multiple downstream references;
        // its own LIMIT still evaluates lazily.
        "WITH stale AS MATERIALIZED (
             SELECT sos.sandbox_id, sos.node_id
             FROM sandbox_observed_states sos
             JOIN sandboxes s ON s.id = sos.sandbox_id
             WHERE sos.sandbox_id > $4
               AND sos.sandbox_id <= $5
               AND s.lifecycle = 'suspended'
               AND s.updated_at < NOW() - make_interval(secs => $1::BIGINT)
               AND NOT EXISTS (
                   SELECT 1
                   FROM node_facet_observed_status nfos
                   WHERE nfos.node_id = sos.node_id
                     AND nfos.facet = 'sandbox-desired-state'
                     AND nfos.element = sos.sandbox_id::TEXT
                     AND nfos.observed_payload ? 'quarantine_recovery'
                     AND nfos.observed_at
                         >= NOW() - make_interval(secs => $3::BIGINT)
               )
             LIMIT $2
         ),
         stale_refs AS MATERIALIZED (
             SELECT
                 stale.node_id,
                 'network-policy'::TEXT AS facet,
                 'sandbox:' || stale.sandbox_id::TEXT AS element
             FROM stale
             UNION ALL
             SELECT
                 stale.node_id,
                 sandbox_facets.facet,
                 stale.sandbox_id::TEXT AS element
             FROM stale
             CROSS JOIN (
                 VALUES
                     ('sandbox-inventory'::TEXT),
                     ('sandbox-desired-state'::TEXT)
             ) AS sandbox_facets(facet)
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING stale_refs
             WHERE nfc.node_id = stale_refs.node_id
               AND nfc.facet = stale_refs.facet
               AND nfc.element = stale_refs.element
             RETURNING 1
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING stale_refs
             WHERE nfos.node_id = stale_refs.node_id
               AND nfos.facet = stale_refs.facet
               AND nfos.element = stale_refs.element
             RETURNING 1
         ),
         deleted_observed AS (
             DELETE FROM sandbox_observed_states sos
             USING stale
             WHERE sos.sandbox_id = stale.sandbox_id
             RETURNING 1
         )
         SELECT
             (SELECT COUNT(*)::BIGINT FROM deleted_observed),
             (SELECT COUNT(*)::BIGINT FROM deleted_status),
             (SELECT COUNT(*)::BIGINT FROM deleted_conditions)"
    ))
    .bind(stale_suspended_age_secs)
    .bind(limit)
    .bind(orphan_age_secs)
    .bind(from)
    .bind(to)
    .fetch_one(executor)
    .await
}

/// Delete up to `limit` observed rows in `(from, to]` whose sandbox has no
/// `sandboxes` row and that have not been updated for `orphan_age_secs`, with
/// their sandbox facet rows, unless the row's node observed a
/// `quarantine_recovery` for the sandbox within `orphan_age_secs`.
pub async fn delete_orphaned(
    executor: impl sqlx::PgExecutor<'_>,
    orphan_age_secs: i64,
    limit: i64,
    from: Uuid,
    to: Uuid,
) -> Result<DeletedCounts, sqlx::Error> {
    sqlx::query_as(sql!(
        "WITH stale AS MATERIALIZED (
             SELECT sos.sandbox_id, sos.node_id
             FROM sandbox_observed_states sos
             LEFT JOIN sandboxes s ON s.id = sos.sandbox_id
             WHERE sos.sandbox_id > $3
               AND sos.sandbox_id <= $4
               AND s.id IS NULL
               AND sos.updated_at < NOW() - make_interval(secs => $1::BIGINT)
               AND NOT EXISTS (
                   SELECT 1
                   FROM node_facet_observed_status nfos
                   WHERE nfos.node_id = sos.node_id
                     AND nfos.facet = 'sandbox-desired-state'
                     AND nfos.element = sos.sandbox_id::TEXT
                     AND nfos.observed_payload ? 'quarantine_recovery'
                     AND nfos.observed_at
                         >= NOW() - make_interval(secs => $1::BIGINT)
               )
             LIMIT $2
         ),
         stale_refs AS MATERIALIZED (
             SELECT
                 stale.node_id,
                 'network-policy'::TEXT AS facet,
                 'sandbox:' || stale.sandbox_id::TEXT AS element
             FROM stale
             UNION ALL
             SELECT
                 stale.node_id,
                 sandbox_facets.facet,
                 stale.sandbox_id::TEXT AS element
             FROM stale
             CROSS JOIN (
                 VALUES
                     ('sandbox-inventory'::TEXT),
                     ('sandbox-desired-state'::TEXT)
             ) AS sandbox_facets(facet)
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING stale_refs
             WHERE nfc.node_id = stale_refs.node_id
               AND nfc.facet = stale_refs.facet
               AND nfc.element = stale_refs.element
             RETURNING 1
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING stale_refs
             WHERE nfos.node_id = stale_refs.node_id
               AND nfos.facet = stale_refs.facet
               AND nfos.element = stale_refs.element
             RETURNING 1
         ),
         deleted_observed AS (
             DELETE FROM sandbox_observed_states sos
             USING stale
             WHERE sos.sandbox_id = stale.sandbox_id
             RETURNING 1
         )
         SELECT
             (SELECT COUNT(*)::BIGINT FROM deleted_observed),
             (SELECT COUNT(*)::BIGINT FROM deleted_status),
             (SELECT COUNT(*)::BIGINT FROM deleted_conditions)"
    ))
    .bind(orphan_age_secs)
    .bind(limit)
    .bind(from)
    .bind(to)
    .fetch_one(executor)
    .await
}

#[cfg(test)]
mod tests;
