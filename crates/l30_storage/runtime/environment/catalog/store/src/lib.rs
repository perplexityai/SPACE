//! Data access for the cluster's catalog of known execution environments
//! (`runtime_environments`) and the per-node certifications it aggregates
//! (`node_environment_certifications`).
//!
//! Each catalog row is an environment some node build has published as
//! certified in its compiled catalog. Rows aren't removed when builds stop
//! publishing them; an operator retires an environment instead, by setting
//! `retired_at`. Besides that, [`record_node_certifications`] writes rows and
//! edges from publications, and [`recompute_all_edges`] rewrites edges from the
//! cluster controller's periodic pass.
//!
//! [`list_unretired_for_node`] is what a node is asked to pre-install: the
//! provisioning recipe that `GetNodeRuntimeDesiredState` serves. Installation
//! is independent of the environment policy, so an environment with no policy
//! state, or a blocked one, still appears here until it's retired.
//! [`list_unretired_entries`] adds each environment's identity and certified
//! edges, which runtime upgrades attribute recorded executions to and follow.

use std::collections::BTreeSet;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{DigestParseError, EnvironmentReference, NodeId};

mod certifications;
pub use certifications::{
    CatalogIdentity, CertificationOutcome, CertifiedEnvironment, RecomputeOutcome,
    recompute_all_edges, record_node_certifications,
};

/// Reading the catalog failed. Every variant fails closed: the caller gets no
/// list rather than a partial one.
#[derive(Debug, thiserror::Error)]
pub enum ListUnretiredError {
    #[error("stored environment reference {value:?} is invalid")]
    InvalidEnvironmentReference {
        value: String,
        #[source]
        source: DigestParseError,
    },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// The unretired catalog environments `node_id` should install: the recipe
/// [`list_unretired_within`] defines for the known set the node last reported
/// in `known_environment_refs`, or for none when it has reported none (NULL,
/// or no status row). NULL also follows an observation that failed to decode. The known set is used whatever its age: it describes the
/// node's build, which changes only when the node publishes again. Both reads
/// share one read-only repeatable-read snapshot, so a publication committing
/// between them can't pair the node's previous known set with the catalog its
/// new build wrote. Takes a pool, since it opens that transaction; takes no
/// lock.
pub async fn list_unretired_for_node(
    pool: &sqlx::PgPool,
    node_id: NodeId,
) -> Result<Vec<EnvironmentReference>, ListUnretiredError> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    ))
    .execute(&mut *tx)
    .await?;
    let known = sqlx::query_scalar::<_, Option<Vec<String>>>(sql!(
        "SELECT known_environment_refs FROM node_status_snapshots WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    let recipe = list_unretired_within(&mut *tx, known.as_deref()).await?;
    tx.commit().await?;
    Ok(recipe)
}

/// The provisioning recipe for a node build that knows `known`: the unretired
/// catalog environments among them, sorted by the text of `environment_ref`,
/// or every unretired one when `known` is `None`. A build installs only
/// environments it certifies, and node-ctl refuses a recipe over its limit
/// whole, so a cluster-wide catalog across builds and architectures would stop
/// every node at once. The publication path samples this recipe's size, so the
/// served recipe and its alert share one rule. Takes no lock.
pub async fn list_unretired_within(
    executor: impl sqlx::PgExecutor<'_>,
    known: Option<&[String]>,
) -> Result<Vec<EnvironmentReference>, ListUnretiredError> {
    let rows = sqlx::query_scalar::<_, String>(sql!(
        "SELECT environment_ref
         FROM runtime_environments
         WHERE retired_at IS NULL
           AND ($1::TEXT[] IS NULL OR environment_ref = ANY($1::TEXT[]))
         ORDER BY environment_ref"
    ))
    .bind(known)
    .fetch_all(executor)
    .await?;
    rows.into_iter().map(parse_reference).collect()
}

/// One unretired catalog environment: its identity and the edges its
/// publishing builds certify.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogEntry {
    pub reference: EnvironmentReference,
    pub runtime: String,
    pub runtime_version: String,
    pub kernel_version: String,
    pub state_format_name: String,
    pub state_format_version: String,
    /// Environments this one can be cold-boot upgraded to.
    pub upgrade_edges: Vec<EnvironmentReference>,
    /// Upgrade edges whose target also loads this one's checkpoints.
    pub memory_edges: Vec<EnvironmentReference>,
}

#[derive(sqlx::FromRow)]
struct CatalogRow {
    environment_ref: String,
    runtime: String,
    runtime_version: String,
    kernel_version: String,
    state_format_name: String,
    state_format_version: String,
    upgrade_edges: Vec<String>,
    memory_edges: Vec<String>,
}

fn parse_reference(value: String) -> Result<EnvironmentReference, ListUnretiredError> {
    value
        .parse()
        .map_err(|source| ListUnretiredError::InvalidEnvironmentReference { value, source })
}

/// Every environment in the catalog that isn't retired, with its identity and
/// edges, sorted by the text of `environment_ref`. A malformed reference in
/// any row, edges included, fails the whole read. Accepts a pool or a
/// transaction connection; takes no lock.
pub async fn list_unretired_entries(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<CatalogEntry>, ListUnretiredError> {
    let rows = sqlx::query_as::<_, CatalogRow>(sql!(
        "SELECT environment_ref, runtime, runtime_version, kernel_version,
                state_format_name, state_format_version, upgrade_edges, memory_edges
         FROM runtime_environments
         WHERE retired_at IS NULL
         ORDER BY environment_ref"
    ))
    .fetch_all(executor)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(CatalogEntry {
                reference: parse_reference(row.environment_ref)?,
                runtime: row.runtime,
                runtime_version: row.runtime_version,
                kernel_version: row.kernel_version,
                state_format_name: row.state_format_name,
                state_format_version: row.state_format_version,
                upgrade_edges: row
                    .upgrade_edges
                    .into_iter()
                    .map(parse_reference)
                    .collect::<Result<_, _>>()?,
                memory_edges: row
                    .memory_edges
                    .into_iter()
                    .map(parse_reference)
                    .collect::<Result<_, _>>()?,
            })
        })
        .collect()
}

/// Which of `environments` the catalog holds as retired. Accepts a pool or a
/// transaction connection; takes no lock.
pub async fn retired_among(
    executor: impl sqlx::PgExecutor<'_>,
    environments: &[String],
) -> Result<BTreeSet<String>, sqlx::Error> {
    let retired = sqlx::query_scalar::<_, String>(sql!(
        "SELECT environment_ref
         FROM runtime_environments
         WHERE environment_ref = ANY($1) AND retired_at IS NOT NULL"
    ))
    .bind(environments)
    .fetch_all(executor)
    .await?;
    Ok(retired.into_iter().collect())
}

#[cfg(test)]
mod tests;
