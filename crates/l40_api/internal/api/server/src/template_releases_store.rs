//! Postgres reads of the template catalog (`template_releases` and the
//! nodes a caller's scope names) used by the [`super::ClusterApiService`]
//! handlers. The release statements are
//! `pplx_space_template_release_store::catalog`'s and the node lookups
//! `pplx_space_node_registration_store`'s; this holds the pool and turns a
//! caller's scope into their arguments.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_node_model::NodeRole;
use pplx_space_node_registration_store::lookup as node_lookup;
use pplx_space_node_registration_store::nodes_for_identity;

use std::net::IpAddr;

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_template_release_store::PartitionSelector;
use pplx_space_template_release_store::catalog::{
    self, CatalogFilter, CatalogReleaseRow, LatestRelease,
};
use sqlx::PgPool;
use tonic::Status;

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
///   A tenant build's publication grant honours the same two windows; see
///   `pplx_space_template_store::publication_grants::tenant_grant_covers`.
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

/// The partition a lookup's environment selects: the environment matched
/// against a release's `execution_metadata.environment`, and whether an
/// unbound release matches too. Nodes cache an unbound build and one bound to
/// the initial environment in the same legacy partition, so selecting the
/// initial environment also selects an unbound release, as the registration
/// writer treats them.
fn partition_selector(environment: Option<EnvironmentReference>) -> Option<PartitionSelector> {
    environment.map(|environment| PartitionSelector {
        environment,
        includes_unbound: pplx_space_execution_catalog::catalog::template_cache_partition(Some(
            environment,
        )) == pplx_space_template_model::TemplateCachePartition::Legacy,
    })
}

/// What a release lookup asks of the release beyond its name and selector:
/// the caller-asserted organization, the prefetch generic-only requirement and
/// the execution environment it selects (`None`: the tagged or original
/// release). See [`catalog::find_by_tag`].
#[derive(Debug, Clone, Copy)]
pub(super) struct ReleaseLookupFilter {
    pub(super) caller_org: Option<OrgId>,
    pub(super) generic_only: bool,
    pub(super) environment: Option<EnvironmentReference>,
}

impl ReleaseLookupFilter {
    fn catalog_filter(self) -> CatalogFilter {
        CatalogFilter {
            caller_org: self.caller_org,
            generic_only: self.generic_only,
            selector: partition_selector(self.environment),
        }
    }
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

    /// The releases a lookup of `name`'s `runtime` release tagged `tag`
    /// answers with, one row per storage location; see
    /// [`catalog::find_by_tag`] for its two statements, which one an
    /// unscoped caller gets, and which release the environment selects.
    pub(super) async fn find_by_tag(
        &self,
        name: &str,
        runtime: &str,
        tag: &str,
        scope: &TemplateReadScope,
        filter: ReleaseLookupFilter,
    ) -> Result<Vec<CatalogReleaseRow>, sqlx::Error> {
        catalog::find_by_tag(
            &self.pool,
            name,
            runtime,
            tag,
            scope.node_ids(),
            filter.catalog_filter(),
        )
        .await
    }

    /// The releases a lookup of `name` at `hash` (or its `bare_hash`)
    /// answers with, one row per storage location; see
    /// [`catalog::find_by_hash`].
    pub(super) async fn find_by_hash(
        &self,
        name: &str,
        hash: &str,
        bare_hash: &str,
        scope: &TemplateReadScope,
        filter: ReleaseLookupFilter,
    ) -> Result<Vec<CatalogReleaseRow>, sqlx::Error> {
        catalog::find_by_hash(
            &self.pool,
            name,
            hash,
            bare_hash,
            scope.node_ids(),
            filter.catalog_filter(),
        )
        .await
    }

    /// The latest `limit` releases for `(template_name, runtime)`; see
    /// [`catalog::list_latest`].
    ///
    /// The store does not validate `limit`; bounds-checking lives in
    /// the handler so the error message can name the wire field. The
    /// handler rejects a zero `limit` up front, and the wire field is
    /// unsigned, so a negative one never reaches the store.
    pub(super) async fn list_latest(
        &self,
        template_name: &str,
        runtime: &str,
        limit: i64,
        scope: &TemplateReadScope,
        environment: Option<EnvironmentReference>,
    ) -> Result<Vec<LatestRelease>, Status> {
        catalog::list_latest(
            &self.pool,
            template_name,
            runtime,
            limit,
            scope.node_ids(),
            partition_selector(environment),
        )
        .await
        .map_err(|err| Status::internal(format!("query latest releases: {err}")))
    }
}
