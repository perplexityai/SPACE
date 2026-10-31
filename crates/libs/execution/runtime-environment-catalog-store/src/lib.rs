//! Data access for the cluster's catalog of known execution environments
//! (`runtime_environments`).
//!
//! Each row is an environment some node build has published as certified in
//! its compiled catalog. Rows aren't removed when builds stop publishing them;
//! an operator retires an environment instead, by setting `retired_at`.
//!
//! [`list_unretired`] is what every node is asked to pre-install: the
//! provisioning recipe that `GetNodeRuntimeDesiredState` serves. Installation
//! is independent of the environment policy, so an environment with no policy
//! state, or a blocked one, still appears here until it's retired.
//! [`list_unretired_entries`] adds each environment's identity and certified
//! edges, which runtime upgrades attribute recorded executions to and follow.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{DigestParseError, EnvironmentReference};

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

/// Every environment in the catalog that isn't retired, sorted by the text of
/// `environment_ref`.
/// Empty when the catalog is. Accepts a pool or a transaction connection;
/// takes no lock.
pub async fn list_unretired(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<EnvironmentReference>, ListUnretiredError> {
    let rows = sqlx::query_scalar::<_, String>(sql!(
        "SELECT environment_ref
         FROM runtime_environments
         WHERE retired_at IS NULL
         ORDER BY environment_ref"
    ))
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

#[cfg(test)]
mod tests;
