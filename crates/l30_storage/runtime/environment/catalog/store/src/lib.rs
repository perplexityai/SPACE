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

use std::collections::BTreeSet;

use pplx_space_accounts::NodeId;
use pplx_space_content_digest::DigestParseError;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_postgres_pool::{Db, DbError, Executor};
use pplx_space_sql_locator::sql;

mod certifications;
pub use certifications::{
    CatalogIdentity, CertificationOutcome, CertifiedEnvironment, RecomputeOutcome,
    recompute_all_edges, record_node_certifications,
};

/// A boolean SQL predicate, TRUE iff the node whose id is the SQL
/// expression `node_id` certifies, in its own build's row, an upgrade edge
/// from `source` to `target` (text SQL expressions).
///
/// A catalog edge alone doesn't prove that: the aggregate lets a build that
/// doesn't know an edge's target abstain rather than veto, so
/// `runtime_environments` can carry `A -> B` while some nodes hosting `A` run
/// a build that doesn't know `B`. A caller choosing a node to run an upgrade
/// on has to ask this of that node, alongside
/// `pplx_space_node_status_store::environment_evidence`: these rows are
/// replaced only from complete inventories, so they say what the node's build
/// certifies, not that the node is fresh.
///
/// A NULL `node_id`, `source` or `target` is FALSE, never NULL. The statement
/// reads only `node_environment_certifications`, so a caller builds it once
/// (a `LazyLock<String>`) and binds the rest.
pub fn node_certifies_upgrade(node_id: &str, source: &str, target: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
             FROM node_environment_certifications node_certification\n\
             WHERE node_certification.node_id = {node_id}\n\
               AND node_certification.environment_ref = {source}\n\
               AND {target} = ANY(node_certification.upgrade_edges)\n\
         )"
    )
}

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
    Database(#[from] DbError),
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
    db: &Db,
    node_id: NodeId,
) -> Result<Vec<EnvironmentReference>, ListUnretiredError> {
    let mut tx = db.begin_snapshot().await?;
    let known = sqlx::query_scalar::<_, Option<Vec<String>>>(sql!(
        "SELECT known_environment_refs FROM node_status_snapshots WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional((&mut tx).into_sqlx())
    .await
    .map_err(DbError::from)?
    .flatten();
    let recipe = list_unretired_within(&mut tx, known.as_deref()).await?;
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
    executor: impl Executor<'_>,
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
    .fetch_all(executor.into_sqlx())
    .await
    .map_err(DbError::from)?;
    rows.into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|source| ListUnretiredError::InvalidEnvironmentReference { value, source })
        })
        .collect()
}

/// For each `(source, target)` in `edges`, in order, whether the catalog
/// certifies a cold-boot upgrade edge from `source` to `target`: `source` is
/// unretired and lists `target` in its `upgrade_edges`. `false` when `source`
/// isn't in the catalog. One query however many edges. Accepts a pool or a
/// transaction connection; takes no lock.
pub async fn upgrade_edges_certified(
    executor: impl Executor<'_>,
    edges: &[(EnvironmentReference, EnvironmentReference)],
) -> Result<Vec<bool>, DbError> {
    if edges.is_empty() {
        return Ok(Vec::new());
    }
    let (sources, targets): (Vec<String>, Vec<String>) = edges
        .iter()
        .map(|(source, target)| (source.to_string(), target.to_string()))
        .unzip();
    // The 1-based positions in `edges` of the certified ones.
    let certified = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT edge.position
         FROM unnest($1::text[], $2::text[]) WITH ORDINALITY
             AS edge(source, target, position)
         WHERE EXISTS (
             SELECT 1
             FROM runtime_environments
             WHERE environment_ref = edge.source
               AND retired_at IS NULL
               AND edge.target = ANY(upgrade_edges)
         )"
    ))
    .bind(sources)
    .bind(targets)
    .fetch_all(executor.into_sqlx())
    .await?;
    let mut answers = vec![false; edges.len()];
    for position in certified {
        if let Some(answer) = usize::try_from(position - 1)
            .ok()
            .and_then(|index| answers.get_mut(index))
        {
            *answer = true;
        }
    }
    Ok(answers)
}

/// Which of `environments` the catalog holds as retired. Accepts a pool or a
/// transaction connection; takes no lock.
pub async fn retired_among(
    executor: impl Executor<'_>,
    environments: &[String],
) -> Result<BTreeSet<String>, DbError> {
    let retired = sqlx::query_scalar::<_, String>(sql!(
        "SELECT environment_ref
         FROM runtime_environments
         WHERE environment_ref = ANY($1) AND retired_at IS NOT NULL"
    ))
    .bind(environments)
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(retired.into_iter().collect())
}

/// One catalog environment with its edges, as stored: references are text,
/// for the caller to parse and to report a bad one its own way.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct CatalogEnvironmentRow {
    pub environment_ref: String,
    pub runtime: String,
    pub runtime_version: String,
    pub kernel_version: String,
    pub state_format_name: String,
    pub state_format_version: String,
    pub upgrade_edges: Vec<String>,
    pub memory_edges: Vec<String>,
}

/// Every catalog environment that isn't retired, with its edges, sorted by
/// the text of `environment_ref`. Accepts a pool or a transaction connection;
/// takes no lock.
pub async fn unretired_environments(
    executor: impl Executor<'_>,
) -> Result<Vec<CatalogEnvironmentRow>, DbError> {
    Ok(sqlx::query_as::<_, CatalogEnvironmentRow>(sql!(
        "SELECT environment_ref, runtime, runtime_version, kernel_version,
                state_format_name, state_format_version, upgrade_edges, memory_edges
           FROM runtime_environments
          WHERE retired_at IS NULL
          ORDER BY environment_ref"
    ))
    .fetch_all(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
