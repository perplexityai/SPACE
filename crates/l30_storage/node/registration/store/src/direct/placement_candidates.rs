//! The Direct substrate's placement reads: the live, registered Direct
//! workers (`nodes` joined to a live `node_direct` row) a create or a pooled
//! claim may be placed on, each with what the environment gate would refuse
//! it for, and how many workers a start could place on.
//!
//! Each candidate statement also joins `node_status_snapshots`
//! (`pplx-space-node-status-store`'s) and the template inventory and runtime
//! tables other stores own. Each interpolates
//! `pplx_space_node_status_store::environment_refusal`, the single definition
//! of why a snapshot doesn't show a node can run an environment, and nothing
//! else; the statement is built once and binds every value. They compare
//! against `NOW()`, the database clock, so the caller keeps the executor.
//!
//! The caller keeps the environment gate's decision: what to do with the
//! refusal each row returns, which statement to fall back to and the
//! environment each one binds.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_node_status_store::environment_refusal;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, PgExecutor};

/// A worker a create may be placed on, with what the environment gate would
/// refuse it for (an `EnvironmentRefusal` wire value); NULL when there's
/// nothing to refuse.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct GatedCandidateRow {
    pub id: NodeId,
    pub node_api_endpoint: String,
    pub environment_refusal: Option<String>,
}

/// A worker whose warm pool a pooled claim may draw on, with the build its
/// inventory row reports and, as [`GatedCandidateRow::environment_refusal`],
/// what the environment gate would refuse it for.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PooledCandidateRow {
    pub id: NodeId,
    pub node_api_endpoint: String,
    pub content_hash: String,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub environment_refusal: Option<String>,
}

/// What [`cached_ready_worker_create_candidates`] and
/// [`last_known_worker_create_candidates`] bind.
#[derive(Debug, Clone, Copy)]
pub struct CreateCandidateBinds<'a> {
    /// `$1`: the template.
    pub template: &'a str,
    /// `$2`: the build's content hash, or `None` for any build.
    pub content_hash: Option<&'a str>,
    /// `$3`: the runtime.
    pub runtime: &'a str,
    /// `$4`: whether the create needs a base snapshot.
    pub require_base_snapshot: bool,
    /// `$5`: the substrate whose nodes are read.
    pub managed_by: SubstrateKind,
    /// `$6`: whether the environment gate refuses.
    pub refuse: bool,
    /// `$7`: the release's environment, for an inventory row that names none.
    pub release_environment: Option<&'a str>,
}

/// The cached-ready create candidates: workers whose fresh, `ready`
/// snapshot and inventory show they hold the build and can run it, in
/// random order.
pub async fn cached_ready_worker_create_candidates<'e>(
    executor: impl PgExecutor<'e>,
    binds: CreateCandidateBinds<'_>,
) -> Result<Vec<GatedCandidateRow>, sqlx::Error> {
    static CACHED_READY_WORKER_CREATE_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        format!(
            "SELECT n.id, nd.node_api_endpoint, environment_gate.refusal AS environment_refusal
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             JOIN node_template_inventory nti ON nti.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE n.managed_by = $5
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (($6::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL)
                    OR $3 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $3
                          AND guest.guest_os = nti.guest_os
                          AND guest.guest_arch = nti.arch
                    )
               ))
               AND nti.template_name = $1
               AND ($2::TEXT IS NULL OR nti.content_hash = $2)
               AND (($3 = 'fc' AND nti.runtime IS NULL) OR nti.runtime = $3)
               AND (
                    ($6::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL)
                    OR ($3 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $3
                    )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    -- The environment gate's match replaces the format
                    -- comparison for bound work on a node with evidence.
                    OR ($6::BOOLEAN IS TRUE
                        AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL)
                    OR ($3 = 'fc' AND (
                        nti.state_format_name IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM node_runtime_installations inventory
                            WHERE inventory.node_id = n.id
                        )
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_state_formats state_format
                        WHERE state_format.node_id = n.id
                          AND state_format.runtime = $3
                          AND state_format.state_format_name = nti.state_format_name
                          AND split_part(state_format.state_format_version, '.', 1)
                              = split_part(nti.state_format_version, '.', 1)
                          AND state_format.can_load IS TRUE
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM node_template_components requested
                    WHERE requested.node_id = nti.node_id
                      -- On, the environment match stands in for the kernel
                      -- check for bound work: a node publishes a managed
                      -- environment's kernel only in its evidence.
                      AND NOT (($6::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL) AND requested.kind = 'kernel')
                      AND requested.template_name = nti.template_name
                      AND requested.content_hash = nti.content_hash
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = requested.role_key
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND ($4::BOOLEAN IS FALSE OR nti.has_base_snapshot IS TRUE)
             ORDER BY random()",
            refusal = environment_refusal("nss", "COALESCE(nti.execution_metadata->>'environment', $7::TEXT)"),
        )
    });
    sqlx::query_as::<_, GatedCandidateRow>(sqlx::AssertSqlSafe(sql!(
        CACHED_READY_WORKER_CREATE_CANDIDATES.as_str()
    )))
    .bind(binds.template)
    .bind(binds.content_hash)
    .bind(binds.runtime)
    .bind(binds.require_base_snapshot)
    .bind(binds.managed_by)
    .bind(binds.refuse)
    .bind(binds.release_environment)
    .fetch_all(executor)
    .await
}

/// The last-known create candidates, one row per worker, in node id order:
/// as [`cached_ready_worker_create_candidates`], but a worker's snapshot may
/// be missing, stale or in any status other than `full`, `draining` or
/// `unavailable`; an unreachable one is excluded. A build for a runtime other
/// than `fc` still needs a reachable, fresh, `ready` snapshot whose complete,
/// current execution inventory lists a matching runtime guest, unless the
/// environment gate refuses and the work is bound to an environment (the
/// row's binding, else the release's). Each worker's row is its most recently
/// observed matching inventory row.
pub async fn last_known_worker_create_candidates<'e>(
    executor: impl PgExecutor<'e>,
    binds: CreateCandidateBinds<'_>,
) -> Result<Vec<GatedCandidateRow>, sqlx::Error> {
    static LAST_KNOWN_WORKER_CREATE_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        format!(
            "SELECT DISTINCT ON (n.id) n.id, nd.node_api_endpoint,
                    environment_gate.refusal AS environment_refusal
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_template_inventory nti ON nti.node_id = n.id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE n.managed_by = $5
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND COALESCE(nss.reachable, TRUE) IS TRUE
               AND COALESCE(nss.scheduling_status, 'ready') NOT IN ('full', 'draining', 'unavailable')
               AND (($6::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL)
                    OR $3 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $3
                          AND guest.guest_os = nti.guest_os
                          AND guest.guest_arch = nti.arch
                    )
               ))
               AND nti.template_name = $1
               AND ($2::TEXT IS NULL OR nti.content_hash = $2)
               AND (($3 = 'fc' AND nti.runtime IS NULL) OR nti.runtime = $3)
               AND (
                    ($6::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL)
                    OR ($3 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $3
                    )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    -- The environment gate's match replaces the format
                    -- comparison for bound work on a node with evidence.
                    OR ($6::BOOLEAN IS TRUE
                        AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL)
                    OR ($3 = 'fc' AND (
                        nti.state_format_name IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM node_runtime_installations inventory
                            WHERE inventory.node_id = n.id
                        )
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_state_formats state_format
                        WHERE state_format.node_id = n.id
                          AND state_format.runtime = $3
                          AND state_format.state_format_name = nti.state_format_name
                          AND split_part(state_format.state_format_version, '.', 1)
                              = split_part(nti.state_format_version, '.', 1)
                          AND state_format.can_load IS TRUE
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM node_template_components requested
                    WHERE requested.node_id = nti.node_id
                      -- On, the environment match stands in for the kernel
                      -- check for bound work: a node publishes a managed
                      -- environment's kernel only in its evidence.
                      AND NOT (($6::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $7::TEXT) IS NOT NULL) AND requested.kind = 'kernel')
                      AND requested.template_name = nti.template_name
                      AND requested.content_hash = nti.content_hash
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = requested.role_key
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND ($4::BOOLEAN IS FALSE OR nti.has_base_snapshot IS TRUE)
             ORDER BY n.id, nti.observed_at DESC, nss.observed_at DESC NULLS LAST",
            refusal = environment_refusal("nss", "COALESCE(nti.execution_metadata->>'environment', $7::TEXT)"),
        )
    });
    sqlx::query_as::<_, GatedCandidateRow>(sqlx::AssertSqlSafe(sql!(
        LAST_KNOWN_WORKER_CREATE_CANDIDATES.as_str()
    )))
    .bind(binds.template)
    .bind(binds.content_hash)
    .bind(binds.runtime)
    .bind(binds.require_base_snapshot)
    .bind(binds.managed_by)
    .bind(binds.refuse)
    .bind(binds.release_environment)
    .fetch_all(executor)
    .await
}

/// What [`pull_on_create_worker_candidates`] binds.
#[derive(Debug, Clone, Copy)]
pub struct PullOnCreateBinds<'a> {
    /// `$1`: the runtime.
    pub runtime: &'a str,
    /// `$2`: the substrate whose nodes are read.
    pub managed_by: SubstrateKind,
    /// `$3`: the build's environment, for a candidate that doesn't hold it.
    pub build_environment: Option<&'a str>,
    /// `$4`: whether the environment gate refuses.
    pub refuse: bool,
    /// `$5`: the template.
    pub template: &'a str,
    /// `$6`: the build's content hash.
    pub content_hash: Option<&'a str>,
    /// `$7`: the release's environment, for a held row that names none.
    pub release_environment: Option<&'a str>,
}

/// The workers with a fresh, `ready` snapshot that can pull the build and
/// create from it, holders included, in random order. With the gate
/// refusing, only those it doesn't refuse.
pub async fn pull_on_create_worker_candidates<'e>(
    executor: impl PgExecutor<'e>,
    binds: PullOnCreateBinds<'_>,
) -> Result<Vec<GatedCandidateRow>, sqlx::Error> {
    static PULL_ON_CREATE_WORKER_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        format!(
            "SELECT n.id, nd.node_api_endpoint, environment_gate.refusal AS environment_refusal
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             -- A candidate that already holds the digest runs the build it
             -- holds, so it's gated as a cached one is: by its own row's
             -- binding, else the release's. A row that names a binding wins
             -- over one that doesn't, then the exact spelling of the digest.
             LEFT JOIN LATERAL (
                 SELECT COALESCE(held.execution_metadata->>'environment', $7::TEXT)
                            AS environment,
                        TRUE AS holds
                 FROM node_template_inventory held
                 WHERE held.node_id = n.id
                   AND held.template_name = $5
                   AND held.content_hash IN ($6, replace($6, 'sha256:', ''))
                 ORDER BY held.execution_metadata->>'environment' IS NULL,
                          held.content_hash = $6 DESC
                 LIMIT 1
             ) held ON TRUE
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (
                    ($4::BOOLEAN IS TRUE AND CASE WHEN held.holds THEN held.environment ELSE $3::TEXT END IS NOT NULL)
                    OR ($1 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $1
                    )
               )
               AND (($4::BOOLEAN IS TRUE AND CASE WHEN held.holds THEN held.environment ELSE $3::TEXT END IS NOT NULL)
                    OR $1 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $1
                    )
               ))
               AND (
                    $4::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             ORDER BY random()",
            refusal = environment_refusal("nss", "CASE WHEN held.holds THEN held.environment ELSE $3::TEXT END"),
        )
    });
    sqlx::query_as::<_, GatedCandidateRow>(sqlx::AssertSqlSafe(sql!(
        PULL_ON_CREATE_WORKER_CANDIDATES.as_str()
    )))
    .bind(binds.runtime)
    .bind(binds.managed_by)
    .bind(binds.build_environment)
    .bind(binds.refuse)
    .bind(binds.template)
    .bind(binds.content_hash)
    .bind(binds.release_environment)
    .fetch_all(executor)
    .await
}

/// What [`pooled_worker_candidates`] binds.
#[derive(Debug, Clone, Copy)]
pub struct PooledCandidateBinds<'a> {
    /// `$1`: the template.
    pub template: &'a str,
    /// `$2`: the build's content hash, or `None` for any build.
    pub content_hash: Option<&'a str>,
    /// `$3`: the runtime.
    pub runtime: &'a str,
    /// `$4`: the substrate whose nodes are read.
    pub managed_by: SubstrateKind,
    /// `$5`: whether the environment gate refuses.
    pub refuse: bool,
    /// `$6`: the release's environment, for an inventory row that names none.
    pub release_environment: Option<&'a str>,
}

/// The workers with a fresh, `ready` snapshot whose warm pool of the build
/// has a current member, fullest pool first. With the gate refusing, only
/// those it doesn't refuse.
pub async fn pooled_worker_candidates<'e>(
    executor: impl PgExecutor<'e>,
    binds: PooledCandidateBinds<'_>,
) -> Result<Vec<PooledCandidateRow>, sqlx::Error> {
    static POOLED_WORKER_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        format!(
            "SELECT n.id, nd.node_api_endpoint, nti.content_hash, nti.vcpu, nti.memory_mib,
                    environment_gate.refusal AS environment_refusal
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             JOIN node_template_inventory nti ON nti.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT {refusal} AS refusal
             ) environment_gate
             WHERE n.managed_by = $4
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (($5::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $6::TEXT) IS NOT NULL)
                    OR $3 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $3
                          AND guest.guest_os = nti.guest_os
                          AND guest.guest_arch = nti.arch
                    )
               ))
               AND nti.template_name = $1
               AND ($2::TEXT IS NULL OR nti.content_hash = $2)
               AND (($3 = 'fc' AND nti.runtime IS NULL) OR nti.runtime = $3)
               AND (
                    ($5::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $6::TEXT) IS NOT NULL)
                    OR ($3 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $3
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM node_template_components requested
                    WHERE requested.node_id = nti.node_id
                      -- On, the environment match stands in for the kernel
                      -- check for bound work: a node publishes a managed
                      -- environment's kernel only in its evidence.
                      AND NOT (($5::BOOLEAN IS TRUE AND COALESCE(nti.execution_metadata->>'environment', $6::TEXT) IS NOT NULL) AND requested.kind = 'kernel')
                      AND requested.template_name = nti.template_name
                      AND requested.content_hash = nti.content_hash
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = requested.role_key
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND nti.pool_current > 0
               AND (
                    $5::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             ORDER BY nti.pool_current DESC, random()",
            refusal = environment_refusal("nss", "COALESCE(nti.execution_metadata->>'environment', $6::TEXT)"),
        )
    });
    sqlx::query_as::<_, PooledCandidateRow>(sqlx::AssertSqlSafe(sql!(
        POOLED_WORKER_CANDIDATES.as_str()
    )))
    .bind(binds.template)
    .bind(binds.content_hash)
    .bind(binds.runtime)
    .bind(binds.managed_by)
    .bind(binds.refuse)
    .bind(binds.release_environment)
    .fetch_all(executor)
    .await
}

/// How many live, registered Direct workers under `managed_by` a start could
/// place on: those with no snapshot, or one that is reachable and not
/// `full`, `draining` or `unavailable`, however stale.
pub async fn count_start_eligible_workers<'e>(
    executor: impl PgExecutor<'e>,
    managed_by: SubstrateKind,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        sql!("SELECT COUNT(*)
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $1
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND COALESCE(nss.reachable, TRUE) IS TRUE
               AND COALESCE(nss.scheduling_status, 'ready') NOT IN ('full', 'draining', 'unavailable')"),
    )
    .bind(managed_by)
    .fetch_one(executor)
    .await
}
