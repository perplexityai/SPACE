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
use pplx_space_common_core::types::{
    ArtifactComponentKind, DigestParseError, EnvironmentReference, RuntimeBinding,
};
use pplx_space_execution_common::environment::KnownEnvironment;

/// Recording one validated, authenticated node's certification failed.
#[derive(Debug, thiserror::Error)]
pub enum PublishKnownError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Record a compiled environment, not the node's installed/usable subset.
///
/// The caller authenticates the registered node and validates the complete
/// environment observation before calling this operation in its publication
/// transaction. Definitions are content-addressed; this is evidence of a node
/// build's certification, not execution permission or a download source.
///
/// Certifications are conservative and cumulative: later publications can
/// remove edges or disagree about ordering, never add edges to an existing
/// row. An absent/old build cannot silently re-enable an edge another build
/// refused. Expanding certifications needs a separate operator-reviewed
/// protocol, not last-writer-wins. Retirement is never changed here.
pub async fn publish_known(
    executor: impl sqlx::PgExecutor<'_>,
    environment: &KnownEnvironment,
) -> Result<(), PublishKnownError> {
    let spec = environment.definition().descriptor().spec();
    let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let sort_key = environment.runtime_version_sort_key().map(|key| {
        key.components()
            .iter()
            // VersionSortKey validates the Postgres INT range at construction.
            .map(|component| i32::try_from(*component).expect("validated version sort key"))
            .collect::<Vec<_>>()
    });
    let kernel = spec
        .boot
        .components()
        .find(|(_, component, _)| component.kind == ArtifactComponentKind::Kernel)
        .map(|(_, component, _)| component.version.to_string());
    let state_format = spec.compatibility.state_format();
    let upgrade_edges = environment
        .upgrade_edges()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let memory_edges = environment
        .memory_edges()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version,
             upgrade_edges, memory_edges
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (environment_ref) DO UPDATE SET
             version_sort_key = CASE
                 WHEN runtime_environments.version_sort_key = EXCLUDED.version_sort_key
                 THEN runtime_environments.version_sort_key
                 ELSE NULL
             END,
             upgrade_edges = ARRAY(
                 SELECT target FROM unnest(runtime_environments.upgrade_edges) AS old(target)
                 INTERSECT
                 SELECT target FROM unnest(EXCLUDED.upgrade_edges) AS new(target)
                 ORDER BY target
             ),
             memory_edges = ARRAY(
                 SELECT target FROM unnest(runtime_environments.memory_edges) AS old(target)
                 INTERSECT
                 SELECT target FROM unnest(EXCLUDED.memory_edges) AS new(target)
                 ORDER BY target
             ),
             updated_at = now()
         WHERE NOT runtime_environments.upgrade_edges <@ EXCLUDED.upgrade_edges
            OR NOT runtime_environments.memory_edges <@ EXCLUDED.memory_edges
            OR (runtime_environments.version_sort_key IS NOT NULL
                AND runtime_environments.version_sort_key IS DISTINCT FROM EXCLUDED.version_sort_key)"
    ))
    .bind(environment.reference().to_string())
    .bind(release.runtime.kind.as_str())
    .bind(release.runtime.version.as_str())
    .bind(sort_key)
    .bind(kernel)
    .bind(state_format.map(|format| format.format.as_str()))
    .bind(state_format.map(|format| format.version.as_str()))
    .bind(upgrade_edges)
    .bind(memory_edges)
    .execute(executor)
    .await?;
    Ok(())
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
