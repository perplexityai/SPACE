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
    rows.into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|source| ListUnretiredError::InvalidEnvironmentReference { value, source })
        })
        .collect()
}

#[cfg(test)]
mod tests;
