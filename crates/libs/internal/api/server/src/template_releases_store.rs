//! Postgres store for `template_releases` reads used by the
//! [`super::ClusterApiService`] handlers. Mirrors [`WarmPoolStore`] in
//! `pplx-space-cluster-pool`: holds a `PgPool`, exposes async methods
//! per query, returns plain Rust domain types so the store is
//! independent of the tonic-generated proto types.
//!
//! [`WarmPoolStore`]: pplx_space_cluster_pool::WarmPoolStore

use pplx_space_common_core::types::{NodeId, OrgId};

use std::net::IpAddr;

use pplx_space_common_core::sql;
use serde::Deserialize;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use tonic::Status;
use uuid::Uuid;

/// A `template_releases` row plus its zero-or-more
/// `template_release_storage_locations`.
#[derive(Debug, Clone)]
pub(super) struct LatestRelease {
    pub(super) id: Uuid,
    pub(super) template_name: String,
    pub(super) runtime: String,
    pub(super) hash: String,
    pub(super) created_at: OffsetDateTime,
    pub(super) storage_locations: Vec<LatestReleaseStorage>,
}

/// Storage location as it comes back from the per-release
/// `json_agg(json_build_object(...))` aggregate. UUID and timestamp
/// are stringified inside the JSON object so deserialization is
/// trivially driven by `serde::Deserialize` without pulling
/// `time` serde features. The handler emits the strings verbatim onto
/// the proto wire (they're already RFC 3339 / canonical UUID), so
/// there's no parse-and-reformat round trip in Rust.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct LatestReleaseStorage {
    pub(super) id: String,
    pub(super) connection_string: String,
    pub(super) created_at: String,
}

/// Flat row returned by the GROUP BY query: one row per release, with
/// the per-release storage locations folded into a JSONB column built
/// by `json_agg` on the server side. Eliminates the implicit
/// "consecutive rows belong to the same release" ordering contract
/// that the older flat LEFT JOIN shape required between SQL and Rust.
#[derive(Debug, FromRow)]
struct LatestReleaseRow {
    id: Uuid,
    template_name: String,
    runtime: String,
    hash: String,
    created_at: OffsetDateTime,
    storage_locations: Json<Vec<LatestReleaseStorage>>,
}

/// Which part of the catalog a lookup may see. The handler derives it from
/// the authenticated caller, never from request fields.
///
/// `NodeScoped` is every TokenReview identity and the fallback static token.
/// It resolves generic templates that are still present, plus org-specific
/// templates the control plane has tied to one of the caller's registered
/// nodes (`node_ids`, matched by remote address):
///
/// - a sandbox of that template placed on the node, active or target;
/// - for a registered worker, a template an enabled warm pool warms: warm
///   slots are scheduled onto any worker, and the control plane records where
///   one landed only once it is ready, after its template was pulled;
/// - for a registered builder, a base template named by a pre-dispatch build
///   grant (`template_build_base_grants`). The API learns which builder took a
///   job only from the `StartBuild` response, so a grant starts unpinned and
///   any registered builder may use it, for one hour from submission. The API
///   pins it to the accepting builder seconds after dispatch; a pinned grant
///   serves that builder alone, for seven days, so a job that waits in the
///   builder's queue behind long builds keeps its authorization until it runs.
///
/// The placement clauses deliberately ignore `desired_state`: a sandbox
/// placed on the node still needs its template pulled while that template is
/// being retired. A node-scoped caller with no registered node resolves
/// generic templates only. `Unscoped` is the static-only trust boundary and
/// any call that reached the handler without the auth layer (tests, local
/// simulators).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TemplateReadScope {
    Unscoped,
    NodeScoped { node_ids: Vec<NodeId> },
}

impl TemplateReadScope {
    fn node_ids(&self) -> Option<&[NodeId]> {
        match self {
            Self::Unscoped => None,
            Self::NodeScoped { node_ids } => Some(node_ids),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct TemplateReleasesStore {
    pool: PgPool,
}

impl TemplateReleasesStore {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The node scope of a caller that connected from `address`: every live
    /// registered node at that address. An address with no node yields an
    /// empty scope, which resolves generic templates only.
    pub(super) async fn node_scope_for_address(
        &self,
        address: IpAddr,
    ) -> Result<TemplateReadScope, sqlx::Error> {
        let node_ids = sqlx::query_scalar::<_, NodeId>(sql!(
            "SELECT id
             FROM nodes
             WHERE address = $1::INET
               AND managed_by IN ('space', 'k8s')
               AND COALESCE(state, 'registered') != 'deleted'"
        ))
        .bind(address.to_string())
        .fetch_all(&self.pool)
        .await?;
        Ok(TemplateReadScope::NodeScoped { node_ids })
    }

    /// Unscoped lookups with no caller organization never touch `templates`:
    /// release resolution serves on-demand sandbox-create pulls and must keep
    /// working against a schema that predates `086_templates_visibility`
    /// (see `template_visibility_degrades_to_unspecified_without_column`).
    /// Every scoped lookup needs the `templates` row and fails closed when
    /// the table or its `visibility` column is unavailable.
    ///
    /// In the scoped statement `$4` is the caller-asserted organization
    /// (`NULL` leaves the lookup unfiltered by organization) and `$5` the
    /// caller's node ids (`NULL` for an unscoped caller). A node-scoped caller
    /// passes both predicates, so asserting the owner's organization grants
    /// nothing its placements do not already justify.
    pub(super) async fn find_by_tag(
        &self,
        name: &str,
        runtime: &str,
        tag: &str,
        caller_org: Option<OrgId>,
        scope: &TemplateReadScope,
    ) -> Result<Vec<super::metadata::FindTemplateReleaseRow>, sqlx::Error> {
        if caller_org.is_none() && scope.node_ids().is_none() {
            return sqlx::query_as(sql!(
                "SELECT r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_release_tags t
             JOIN template_releases r
               ON r.id = t.template_release_id
              AND r.template_name = t.template_name
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE t.template_name = $1
               AND t.tag = $2
               AND r.type = $3
             ORDER BY s.created_at ASC NULLS LAST"
            ))
            .bind(name)
            .bind(tag)
            .bind(runtime)
            .fetch_all(&self.pool)
            .await;
        }
        sqlx::query_as(sql!(
            "SELECT r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_release_tags t
             JOIN template_releases r
               ON r.id = t.template_release_id
              AND r.template_name = t.template_name
             JOIN templates tpl ON tpl.name = r.template_name
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE t.template_name = $1
               AND t.tag = $2
               AND r.type = $3
               AND ($4::UUID IS NULL OR (
                       tpl.desired_state = 'present'
                       AND (tpl.visibility = 'generic'
                            OR (tpl.visibility = 'org_specific' AND tpl.org_id = $4))))
               AND ($5::UUID[] IS NULL
                    OR (tpl.visibility = 'generic' AND tpl.desired_state = 'present')
                    OR EXISTS (
                        SELECT 1
                        FROM sandboxes sb
                        WHERE sb.template_id = tpl.name
                          AND (sb.active_node_id = ANY($5) OR sb.target_node_id = ANY($5)))
                    OR EXISTS (
                        SELECT 1
                        FROM kubernetes_pool_configs pc
                        JOIN template_releases pr ON pr.id = pc.template_release_id
                        JOIN nodes wn ON wn.id = ANY($5)
                        WHERE pc.enabled
                          AND pr.template_name = tpl.name
                          AND wn.role = 'worker')
                    OR EXISTS (
                        SELECT 1
                        FROM template_build_base_grants g
                        JOIN nodes bn ON bn.id = ANY($5)
                        WHERE g.base_template_name = tpl.name
                          AND g.org_id = tpl.org_id
                          AND bn.role = 'builder'
                          AND ((g.builder_node_id IS NULL
                                AND g.granted_at > NOW() - INTERVAL '1 hour')
                               OR (g.builder_node_id = bn.id
                                   AND g.granted_at > NOW() - INTERVAL '7 days'))))
             ORDER BY s.created_at ASC NULLS LAST"
        ))
        .bind(name)
        .bind(tag)
        .bind(runtime)
        .bind(caller_org.map(|org| org.as_uuid()))
        .bind(scope.node_ids())
        .fetch_all(&self.pool)
        .await
    }

    /// See [`Self::find_by_tag`] for the two statement shapes and the scope
    /// parameters.
    pub(super) async fn find_by_hash(
        &self,
        name: &str,
        hash: &str,
        bare_hash: &str,
        caller_org: Option<OrgId>,
        scope: &TemplateReadScope,
    ) -> Result<Vec<super::metadata::FindTemplateReleaseRow>, sqlx::Error> {
        if caller_org.is_none() && scope.node_ids().is_none() {
            return sqlx::query_as(sql!(
                "SELECT r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_releases r
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE r.template_name = $1
               AND r.hash IN ($2, $3)
             ORDER BY s.created_at ASC NULLS LAST"
            ))
            .bind(name)
            .bind(hash)
            .bind(bare_hash)
            .fetch_all(&self.pool)
            .await;
        }
        sqlx::query_as(sql!(
            "SELECT r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_releases r
             JOIN templates tpl ON tpl.name = r.template_name
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE r.template_name = $1
               AND r.hash IN ($2, $3)
               AND ($4::UUID IS NULL OR (
                       tpl.desired_state = 'present'
                       AND (tpl.visibility = 'generic'
                            OR (tpl.visibility = 'org_specific' AND tpl.org_id = $4))))
               AND ($5::UUID[] IS NULL
                    OR (tpl.visibility = 'generic' AND tpl.desired_state = 'present')
                    OR EXISTS (
                        SELECT 1
                        FROM sandboxes sb
                        WHERE sb.template_id = tpl.name
                          AND (sb.active_node_id = ANY($5) OR sb.target_node_id = ANY($5)))
                    OR EXISTS (
                        SELECT 1
                        FROM kubernetes_pool_configs pc
                        JOIN template_releases pr ON pr.id = pc.template_release_id
                        JOIN nodes wn ON wn.id = ANY($5)
                        WHERE pc.enabled
                          AND pr.template_name = tpl.name
                          AND wn.role = 'worker')
                    OR EXISTS (
                        SELECT 1
                        FROM template_build_base_grants g
                        JOIN nodes bn ON bn.id = ANY($5)
                        WHERE g.base_template_name = tpl.name
                          AND g.org_id = tpl.org_id
                          AND bn.role = 'builder'
                          AND ((g.builder_node_id IS NULL
                                AND g.granted_at > NOW() - INTERVAL '1 hour')
                               OR (g.builder_node_id = bn.id
                                   AND g.granted_at > NOW() - INTERVAL '7 days'))))
             ORDER BY s.created_at ASC NULLS LAST"
        ))
        .bind(name)
        .bind(hash)
        .bind(bare_hash)
        .bind(caller_org.map(|org| org.as_uuid()))
        .bind(scope.node_ids())
        .fetch_all(&self.pool)
        .await
    }

    /// Return the latest `limit` releases for `(template_name,
    /// runtime)` ordered by `created_at` DESC (ties broken by `id`
    /// DESC for determinism). Each entry carries its zero-or-more
    /// storage locations, aggregated server-side via `json_agg`.
    ///
    /// The store does not validate `limit`; bounds-checking lives in
    /// the handler so the error message can name the wire field. A
    /// zero or negative `limit` will silently return an empty
    /// result — Postgres clamps it — but the handler rejects those
    /// up front.
    pub(super) async fn list_latest(
        &self,
        template_name: &str,
        runtime: &str,
        limit: i64,
        scope: &TemplateReadScope,
    ) -> Result<Vec<LatestRelease>, Status> {
        // Server-side aggregation: inner subquery picks the latest
        // `limit` releases; outer `LEFT JOIN` plus `GROUP BY` rolls
        // each release's storage locations into one JSON array.
        //
        // - `FILTER (WHERE s.id IS NOT NULL)` discards the synthetic
        //   NULL storage row that LEFT JOIN produces when a release
        //   has zero storage locations, so the aggregate becomes
        //   `[]` instead of `[null]`.
        // - `COALESCE(..., '[]'::json)` handles the case where the
        //   FILTER drops every row (a release with no storage
        //   locations would otherwise produce `NULL` from json_agg).
        // - `s.id::text` and `to_char(s.created_at, ...)` keep the
        //   JSON payload purely textual, so deserialization in Rust
        //   doesn't need `time` serde features. `to_char` with
        //   `'.US"Z"'` emits microsecond-precision RFC 3339 (Postgres
        //   timestamptz native precision) terminated by literal `Z`.
        //
        // The node-scoped statement joins `templates` inside the inner
        // subquery with the same node predicate as `find_by_tag` (`$4`
        // is the caller's node ids); the unscoped statement stays off
        // that table for the same schema-skew reason.
        let rows = if let Some(node_ids) = scope.node_ids() {
            sqlx::query_as::<_, LatestReleaseRow>(sql!(
                "SELECT r.id,
                    r.template_name,
                    r.type AS runtime,
                    r.hash,
                    r.created_at,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id', s.id::text,
                                'connection_string', s.storage_connection_string,
                                'created_at', to_char(
                                    s.created_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'
                                )
                            )
                            ORDER BY s.created_at ASC
                        ) FILTER (WHERE s.id IS NOT NULL),
                        '[]'::json
                    ) AS storage_locations
             FROM (
                 SELECT rel.id, rel.template_name, rel.type, rel.hash, rel.created_at
                 FROM template_releases rel
                 JOIN templates tpl ON tpl.name = rel.template_name
                 WHERE rel.template_name = $1
                   AND rel.type = $2
                   AND ((tpl.visibility = 'generic' AND tpl.desired_state = 'present')
                        OR EXISTS (
                            SELECT 1
                            FROM sandboxes sb
                            WHERE sb.template_id = tpl.name
                              AND (sb.active_node_id = ANY($4) OR sb.target_node_id = ANY($4)))
                        OR EXISTS (
                            SELECT 1
                            FROM kubernetes_pool_configs pc
                            JOIN template_releases pr ON pr.id = pc.template_release_id
                            JOIN nodes wn ON wn.id = ANY($4)
                            WHERE pc.enabled
                              AND pr.template_name = tpl.name
                              AND wn.role = 'worker')
                        OR EXISTS (
                            SELECT 1
                            FROM template_build_base_grants g
                            JOIN nodes bn ON bn.id = ANY($4)
                            WHERE g.base_template_name = tpl.name
                              AND g.org_id = tpl.org_id
                              AND bn.role = 'builder'
                              AND ((g.builder_node_id IS NULL
                                    AND g.granted_at > NOW() - INTERVAL '1 hour')
                                   OR (g.builder_node_id = bn.id
                                       AND g.granted_at > NOW() - INTERVAL '7 days'))))
                 ORDER BY rel.created_at DESC, rel.id DESC
                 LIMIT $3
             ) r
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             GROUP BY r.id, r.template_name, r.type, r.hash, r.created_at
             ORDER BY r.created_at DESC, r.id DESC"
            ))
            .bind(template_name)
            .bind(runtime)
            .bind(limit)
            .bind(node_ids)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, LatestReleaseRow>(sql!(
                "SELECT r.id,
                    r.template_name,
                    r.type AS runtime,
                    r.hash,
                    r.created_at,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id', s.id::text,
                                'connection_string', s.storage_connection_string,
                                'created_at', to_char(
                                    s.created_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'
                                )
                            )
                            ORDER BY s.created_at ASC
                        ) FILTER (WHERE s.id IS NOT NULL),
                        '[]'::json
                    ) AS storage_locations
             FROM (
                 SELECT id, template_name, type, hash, created_at
                 FROM template_releases
                 WHERE template_name = $1 AND type = $2
                 ORDER BY created_at DESC, id DESC
                 LIMIT $3
             ) r
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             GROUP BY r.id, r.template_name, r.type, r.hash, r.created_at
             ORDER BY r.created_at DESC, r.id DESC"
            ))
            .bind(template_name)
            .bind(runtime)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|err| Status::internal(format!("query latest releases: {err}")))?;

        Ok(rows
            .into_iter()
            .map(|row| LatestRelease {
                id: row.id,
                template_name: row.template_name,
                runtime: row.runtime,
                hash: row.hash,
                created_at: row.created_at,
                storage_locations: row.storage_locations.0,
            })
            .collect())
    }
}
