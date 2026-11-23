//! Postgres store for `template_releases` reads and publication writes used by the
//! [`super::ClusterApiService`] handlers. Mirrors [`WarmPoolStore`] in
//! `pplx-space-cluster-pool`: holds a `PgPool`, exposes async methods
//! per query, returns plain Rust domain types so the store is
//! independent of the tonic-generated proto types.
//!
//! [`WarmPoolStore`]: pplx_space_cluster_pool::WarmPoolStore

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_node_model::NodeRole;
use pplx_space_node_registration_store::lookup as node_lookup;
use pplx_space_node_registration_store::nodes_for_identity;

use std::net::IpAddr;

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sql_locator::sql;
use pplx_space_template_release_original::{digest_original, spelling_original};
use serde::Deserialize;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use std::sync::LazyLock;
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
    pub(super) environment: Option<String>,
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
    environment: Option<String>,
    storage_locations: Json<Vec<LatestReleaseStorage>>,
}

/// Which part of the catalog a lookup may see. The handler derives it from
/// the authenticated caller, never from request fields.
///
/// `NodeScoped` is every TokenReview identity and the fallback static token.
/// It resolves generic templates that are still present, plus org-specific
/// templates the control plane has tied to one of the caller's registered
/// nodes (`node_ids`, matched by reviewed Kubernetes identity or remote address):
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

/// The binds of a lookup's environment selector: the environment matched
/// against a release's `execution_metadata.environment`, and whether an
/// unbound release matches too. Nodes cache an unbound build and one bound to
/// the initial environment in the same legacy partition, so selecting the
/// initial environment also selects an unbound release, as the registration
/// writer treats them.
fn selector_binds(environment: Option<EnvironmentReference>) -> (Option<String>, bool) {
    match environment {
        None => (None, false),
        Some(environment) => (
            Some(environment.to_string()),
            pplx_space_execution_catalog::catalog::template_cache_partition(Some(environment))
                == pplx_space_template_model::TemplateCachePartition::Legacy,
        ),
    }
}

/// What a release lookup asks of the release beyond its name and selector:
/// the caller-asserted organization, the prefetch generic-only requirement and
/// the execution environment it selects (`None`: the tagged or original
/// release). See [`TemplateReleasesStore::find_by_tag`].
#[derive(Debug, Clone, Copy)]
pub(super) struct ReleaseLookupFilter {
    pub(super) caller_org: Option<OrgId>,
    pub(super) generic_only: bool,
    pub(super) environment: Option<EnvironmentReference>,
}

pub(super) enum PublicationNode {
    Registered(NodeId),
    PendingInventory,
    Denied,
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
        let node_ids = node_lookup::live_nodes_at_address(&self.pool, address).await?;
        Ok(TemplateReadScope::NodeScoped { node_ids })
    }

    /// Match the reviewed node name and UID, independent of the caller's pod IP.
    /// Missing or stale inventory yields an empty scope, never an address fallback.
    pub(super) async fn node_scope_for_identity(
        &self,
        node: &pplx_space_rpc_auth::AuthenticatedKubernetesNode,
    ) -> Result<TemplateReadScope, sqlx::Error> {
        let node_ids = nodes_for_identity(
            &self.pool,
            node.name(),
            node.uid(),
            &[NodeRole::Worker, NodeRole::Builder],
        )
        .await?
        .into_iter()
        .filter_map(|(id, uid_matches)| (uid_matches == Some(true)).then_some(id))
        .collect();
        Ok(TemplateReadScope::NodeScoped { node_ids })
    }

    /// A registration without an inventory UID is not authority: wait for
    /// node sync to supply it.
    pub(super) async fn publisher_node_for_identity(
        &self,
        node: &pplx_space_rpc_auth::build::AuthenticatedBuildNode,
    ) -> Result<PublicationNode, sqlx::Error> {
        let rows =
            nodes_for_identity(&self.pool, node.name(), node.uid(), &[NodeRole::Builder]).await?;
        if let Some((id, _)) = rows.iter().find(|(_, matches)| *matches == Some(true)) {
            Ok(PublicationNode::Registered(*id))
        } else if rows.iter().any(|(_, matches)| matches.is_none()) {
            Ok(PublicationNode::PendingInventory)
        } else {
            Ok(PublicationNode::Denied)
        }
    }

    /// Unscoped lookups with no caller organization or generic-only requirement
    /// never touch `templates`:
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
    /// nothing its placements do not already justify. `$6` adds the prefetch
    /// generic-only requirement in the same snapshot as the selected release.
    ///
    /// `environment` (`$7` scoped, `$4` unscoped) selects, among the releases
    /// of the tagged release's digest under either spelling (bare or
    /// `sha256:`), the one bound to that environment: the tagged release
    /// itself or its twin, which lives under its original's spelling. When
    /// several match, a release with a storage location answers before one
    /// without (nothing can pull an artifact-less duplicate), then the tagged
    /// release, then the one whose storage location is oldest, then the
    /// earliest registered. `None` selects the digest original
    /// (`pplx_space_template_release_original::digest_original`) of the
    /// tagged release's digest, which a tag written before the shared rule
    /// may not point at.
    pub(super) async fn find_by_tag(
        &self,
        name: &str,
        runtime: &str,
        tag: &str,
        scope: &TemplateReadScope,
        filter: ReleaseLookupFilter,
    ) -> Result<Vec<super::metadata::FindTemplateReleaseRow>, sqlx::Error> {
        let ReleaseLookupFilter {
            caller_org,
            generic_only,
            environment,
        } = filter;
        let (environment, legacy_selector) = selector_binds(environment);
        if !generic_only && caller_org.is_none() && scope.node_ids().is_none() {
            return {
                /// Built once: every input is a literal or a bind placeholder.
                static UNSCOPED: LazyLock<String> = LazyLock::new(|| {
                    sql!(format!(
                        "SELECT FALSE AS generic_authorized,
                    r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    r.execution_metadata->>'environment' AS release_environment,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_release_tags t
             JOIN template_releases tr
               ON tr.id = t.template_release_id
              AND tr.template_name = t.template_name
             JOIN template_releases r
               ON r.template_name = tr.template_name
              AND (r.hash = tr.hash
                   OR r.hash = 'sha256:' || tr.hash
                   OR 'sha256:' || r.hash = tr.hash)
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE t.template_name = $1
               AND t.tag = $2
               AND r.type = $3
               AND (($4::TEXT IS NULL AND {digest_original})
                    OR r.execution_metadata->>'environment' = $4
                    OR ($5 AND r.execution_metadata->>'environment' IS NULL))
             ORDER BY s.id IS NULL,
                      r.id = tr.id DESC,
                      s.created_at ASC NULLS LAST,
                      r.created_at ASC,
                      r.id ASC,
                      s.id ASC",
                        digest_original = digest_original("r"),
                    ))
                });
                sqlx::query_as(sqlx::AssertSqlSafe(UNSCOPED.as_str()))
                    .bind(name)
                    .bind(tag)
                    .bind(runtime)
                    .bind(environment)
                    .bind(legacy_selector)
                    .fetch_all(&self.pool)
                    .await
            };
        }
        {
            /// Built once: every input is a literal or a bind placeholder.
            static SCOPED: LazyLock<String> = LazyLock::new(|| {
                sql!(format!(
                    "SELECT ($6 AND tpl.visibility = 'generic') AS generic_authorized,
                    r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    r.execution_metadata->>'environment' AS release_environment,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_release_tags t
             JOIN template_releases tr
               ON tr.id = t.template_release_id
              AND tr.template_name = t.template_name
             JOIN template_releases r
               ON r.template_name = tr.template_name
              AND (r.hash = tr.hash
                   OR r.hash = 'sha256:' || tr.hash
                   OR 'sha256:' || r.hash = tr.hash)
             JOIN templates tpl ON tpl.name = r.template_name
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE t.template_name = $1
               AND t.tag = $2
               AND r.type = $3
               AND (($7::TEXT IS NULL AND {digest_original})
                    OR r.execution_metadata->>'environment' = $7
                    OR ($8 AND r.execution_metadata->>'environment' IS NULL))
               AND (NOT $6 OR tpl.visibility = 'generic')
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
             ORDER BY s.id IS NULL,
                      r.id = tr.id DESC,
                      s.created_at ASC NULLS LAST,
                      r.created_at ASC,
                      r.id ASC,
                      s.id ASC",
                    digest_original = digest_original("r"),
                ))
            });
            sqlx::query_as(sqlx::AssertSqlSafe(SCOPED.as_str()))
                .bind(name)
                .bind(tag)
                .bind(runtime)
                .bind(caller_org.map(|org| org.as_uuid()))
                .bind(scope.node_ids())
                .bind(generic_only)
                .bind(environment)
                .bind(legacy_selector)
                .fetch_all(&self.pool)
                .await
        }
    }

    /// See [`Self::find_by_tag`] for the two statement shapes and the scope
    /// parameters. `environment` selects the release of the hash bound to
    /// it; `None` selects each spelling's original release, the first
    /// registered for the hash under that spelling, so twins never answer an
    /// unselected lookup. The digest's original
    /// (`pplx_space_template_release_original::digest_original`, the release
    /// every caller resolves the digest to) comes first, so it's the release
    /// an unselected lookup answers with; the other spelling's original only
    /// contributes storage locations.
    pub(super) async fn find_by_hash(
        &self,
        name: &str,
        hash: &str,
        bare_hash: &str,
        scope: &TemplateReadScope,
        filter: ReleaseLookupFilter,
    ) -> Result<Vec<super::metadata::FindTemplateReleaseRow>, sqlx::Error> {
        let ReleaseLookupFilter {
            caller_org,
            generic_only,
            environment,
        } = filter;
        let (environment, legacy_selector) = selector_binds(environment);
        if !generic_only && caller_org.is_none() && scope.node_ids().is_none() {
            /// Built once: every input is a literal or a bind placeholder.
            static UNSCOPED: LazyLock<String> = LazyLock::new(|| {
                sql!(format!(
                    "SELECT FALSE AS generic_authorized,
                    r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    r.execution_metadata->>'environment' AS release_environment,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_releases r
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE r.template_name = $1
               AND r.hash IN ($2, $3)
               AND (($4::TEXT IS NULL AND {spelling_original})
                    OR r.execution_metadata->>'environment' = $4
                    OR ($5 AND r.execution_metadata->>'environment' IS NULL))
             ORDER BY ($4::TEXT IS NULL AND {digest_original}) DESC,
                      s.created_at ASC NULLS LAST,
                      r.created_at ASC,
                      r.id ASC,
                      s.id ASC",
                    spelling_original = spelling_original("r"),
                    digest_original = digest_original("r"),
                ))
            });
            return sqlx::query_as(sqlx::AssertSqlSafe(UNSCOPED.as_str()))
                .bind(name)
                .bind(hash)
                .bind(bare_hash)
                .bind(environment)
                .bind(legacy_selector)
                .fetch_all(&self.pool)
                .await;
        }
        /// Built once: every input is a literal or a bind placeholder.
        static SCOPED: LazyLock<String> = LazyLock::new(|| {
            sql!(format!(
                "SELECT ($6 AND tpl.visibility = 'generic') AS generic_authorized,
                    r.id              AS release_id,
                    r.template_name   AS release_template_name,
                    r.type            AS release_type,
                    r.hash            AS release_hash,
                    r.created_at      AS release_created_at,
                    r.execution_metadata->>'environment' AS release_environment,
                    s.id              AS storage_id,
                    s.storage_connection_string AS storage_connection_string,
                    s.created_at      AS storage_created_at
             FROM template_releases r
             JOIN templates tpl ON tpl.name = r.template_name
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             WHERE r.template_name = $1
               AND r.hash IN ($2, $3)
               AND (($7::TEXT IS NULL AND {spelling_original})
                    OR r.execution_metadata->>'environment' = $7
                    OR ($8 AND r.execution_metadata->>'environment' IS NULL))
               AND (NOT $6 OR tpl.visibility = 'generic')
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
             ORDER BY ($7::TEXT IS NULL AND {digest_original}) DESC,
                      s.created_at ASC NULLS LAST,
                      r.created_at ASC,
                      r.id ASC,
                      s.id ASC",
                spelling_original = spelling_original("r"),
                digest_original = digest_original("r"),
            ))
        });
        sqlx::query_as(sqlx::AssertSqlSafe(SCOPED.as_str()))
            .bind(name)
            .bind(hash)
            .bind(bare_hash)
            .bind(caller_org.map(|org| org.as_uuid()))
            .bind(scope.node_ids())
            .bind(generic_only)
            .bind(environment)
            .bind(legacy_selector)
            .fetch_all(&self.pool)
            .await
    }

    /// Return the latest `limit` releases for `(template_name,
    /// runtime)`: those bound to `environment`, or with none, each digest's
    /// original release (`pplx_space_template_release_original::digest_original`,
    /// one per digest under either spelling) and never its twins. Releases rank newest version
    /// first, by the `created_at` of their digest's earliest release under
    /// either spelling, so a twin registered late for an older version
    /// doesn't outrank a newer version; ties break on
    /// the release's own `created_at` and then `id` DESC for determinism. Each entry carries its zero-or-more
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
        environment: Option<EnvironmentReference>,
    ) -> Result<Vec<LatestRelease>, Status> {
        let (environment, legacy_selector) = selector_binds(environment);
        // Server-side aggregation: inner subquery picks the latest
        // `limit` releases (one release per digest, whichever spelling it's
        // stored under, preferring one with a storage location, so the two
        // spellings of a digest never take two of the `limit` slots: an
        // unselected listing keeps only the digest's original, and a selected
        // one the first of its bound releases); outer `LEFT JOIN` plus `GROUP BY` rolls
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
            /// Built once: every input is a literal or a bind placeholder.
            static NODE_SCOPED: LazyLock<String> = LazyLock::new(|| {
                sql!(format!(
                "SELECT r.id,
                    r.template_name,
                    r.type AS runtime,
                    r.hash,
                    r.created_at,
                    r.environment,
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
                 SELECT * FROM (
                 SELECT DISTINCT ON (CASE WHEN $5::TEXT IS NULL THEN rel.id::TEXT
                                    ELSE 'sha256:' || replace(rel.hash, 'sha256:', '') END)
                        rel.id, rel.template_name, rel.type, rel.hash, rel.created_at,
                        rel.execution_metadata->>'environment' AS environment,
                        (SELECT MIN(o.created_at) FROM template_releases o
                         WHERE o.template_name = rel.template_name
                           AND o.hash IN (replace(rel.hash, 'sha256:', ''),
                                          'sha256:' || replace(rel.hash, 'sha256:', ''))) AS version_created_at
                 FROM template_releases rel
                 JOIN templates tpl ON tpl.name = rel.template_name
                 WHERE rel.template_name = $1
                   AND rel.type = $2
                   AND (($5::TEXT IS NULL AND {digest_original})
                        OR rel.execution_metadata->>'environment' = $5
                    OR ($6 AND rel.execution_metadata->>'environment' IS NULL))
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
                 ORDER BY CASE WHEN $5::TEXT IS NULL THEN rel.id::TEXT
                                    ELSE 'sha256:' || replace(rel.hash, 'sha256:', '') END,
                          EXISTS (
                              SELECT 1 FROM template_release_storage_locations l
                              WHERE l.template_release_id = rel.id) DESC,
                          rel.created_at ASC,
                          rel.id ASC
                 ) rel
                 ORDER BY rel.version_created_at DESC, rel.created_at DESC, rel.id DESC
                 LIMIT $3
             ) r
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             GROUP BY r.id, r.template_name, r.type, r.hash, r.created_at, r.environment,
                      r.version_created_at
             ORDER BY r.version_created_at DESC, r.created_at DESC, r.id DESC",
                digest_original = digest_original("rel"),
            ))
            });
            sqlx::query_as::<_, LatestReleaseRow>(sqlx::AssertSqlSafe(NODE_SCOPED.as_str()))
            .bind(template_name)
            .bind(runtime)
            .bind(limit)
            .bind(node_ids)
            .bind(&environment)
            .bind(legacy_selector)
            .fetch_all(&self.pool)
            .await
        } else {
            /// Built once: every input is a literal or a bind placeholder.
            static UNSCOPED: LazyLock<String> = LazyLock::new(|| {
                sql!(format!(
                "SELECT r.id,
                    r.template_name,
                    r.type AS runtime,
                    r.hash,
                    r.created_at,
                    r.environment,
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
                 SELECT * FROM (
                 SELECT DISTINCT ON (CASE WHEN $4::TEXT IS NULL THEN rel.id::TEXT
                                    ELSE 'sha256:' || replace(rel.hash, 'sha256:', '') END)
                        rel.id, rel.template_name, rel.type, rel.hash, rel.created_at,
                        rel.execution_metadata->>'environment' AS environment,
                        (SELECT MIN(o.created_at) FROM template_releases o
                         WHERE o.template_name = rel.template_name
                           AND o.hash IN (replace(rel.hash, 'sha256:', ''),
                                          'sha256:' || replace(rel.hash, 'sha256:', ''))) AS version_created_at
                 FROM template_releases rel
                 WHERE rel.template_name = $1
                   AND rel.type = $2
                   AND (($4::TEXT IS NULL AND {digest_original})
                        OR rel.execution_metadata->>'environment' = $4
                    OR ($5 AND rel.execution_metadata->>'environment' IS NULL))
                 ORDER BY CASE WHEN $4::TEXT IS NULL THEN rel.id::TEXT
                                    ELSE 'sha256:' || replace(rel.hash, 'sha256:', '') END,
                          EXISTS (
                              SELECT 1 FROM template_release_storage_locations l
                              WHERE l.template_release_id = rel.id) DESC,
                          rel.created_at ASC,
                          rel.id ASC
                 ) rel
                 ORDER BY rel.version_created_at DESC, rel.created_at DESC, rel.id DESC
                 LIMIT $3
             ) r
             LEFT JOIN template_release_storage_locations s
               ON s.template_release_id = r.id
             GROUP BY r.id, r.template_name, r.type, r.hash, r.created_at, r.environment,
                      r.version_created_at
             ORDER BY r.version_created_at DESC, r.created_at DESC, r.id DESC",
                digest_original = digest_original("rel"),
            ))
            });
            sqlx::query_as::<_, LatestReleaseRow>(sqlx::AssertSqlSafe(UNSCOPED.as_str()))
            .bind(template_name)
            .bind(runtime)
            .bind(limit)
            .bind(&environment)
            .bind(legacy_selector)
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
                environment: row.environment,
                storage_locations: row.storage_locations.0,
            })
            .collect())
    }
}
