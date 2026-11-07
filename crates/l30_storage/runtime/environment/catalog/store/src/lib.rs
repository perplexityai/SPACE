//! Data access for the cluster's catalog of known execution environments.
//! Node publication registers definitions; operators retire them.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ArtifactComponentKind, DigestParseError, EnvironmentReference, RuntimeBinding,
};
use pplx_space_execution_common::environment::KnownEnvironment;

#[derive(Debug, thiserror::Error)]
pub enum RegisterKnownError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Register a validated definition if it is new. Node reports never change
/// existing metadata, certifications, or operator retirement.
pub async fn register_known(
    executor: impl sqlx::PgExecutor<'_>,
    environment: &KnownEnvironment,
) -> Result<(), RegisterKnownError> {
    let spec = environment.definition().descriptor().spec();
    let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let state_format = spec.compatibility.state_format();
    let sort_key = environment.runtime_version_sort_key().map(|key| {
        key.components()
            .iter()
            .map(|component| i32::try_from(*component).expect("validated version sort key"))
            .collect::<Vec<_>>()
    });
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version,
             upgrade_edges, memory_edges
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (environment_ref) DO NOTHING"
    ))
    .bind(environment.reference().to_string())
    .bind(release.runtime.kind.to_string())
    .bind(release.runtime.version.to_string())
    .bind(sort_key)
    .bind(
        spec.boot
            .components()
            .find(|(_, component, _)| component.kind == ArtifactComponentKind::Kernel)
            .map(|(_, component, _)| component.version.to_string()),
    )
    .bind(state_format.map(|format| format.format.to_string()))
    .bind(state_format.map(|format| format.version.to_string()))
    .bind(
        environment
            .upgrade_edges()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .bind(
        environment
            .memory_edges()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .execute(executor)
    .await?;
    Ok(())
}

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

/// Every environment in the catalog that isn't retired, sorted by reference.
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

/// Profiles supported by the legacy activation planner. Other profiles still
/// belong in provisioning recipes, but lack legacy attribution metadata.
#[derive(sqlx::FromRow)]
pub struct LegacyUpgradeCandidate {
    pub environment_ref: String,
    pub runtime: String,
    pub runtime_version: String,
    pub kernel_version: String,
    pub state_format_name: String,
    pub state_format_version: String,
    pub upgrade_edges: Vec<String>,
    pub memory_edges: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ListLegacyUpgradeCandidatesError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Skip profiles the legacy reader cannot attribute, rather than failing all
/// activations when unrelated catalog rows have optional metadata.
pub async fn list_legacy_upgrade_candidates(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<LegacyUpgradeCandidate>, ListLegacyUpgradeCandidatesError> {
    Ok(sqlx::query_as(sql!(
        "SELECT environment_ref, runtime, runtime_version, kernel_version,
                state_format_name, state_format_version, upgrade_edges, memory_edges
         FROM runtime_environments
         WHERE retired_at IS NULL
           AND kernel_version IS NOT NULL
           AND state_format_name IS NOT NULL
           AND state_format_version IS NOT NULL
         ORDER BY environment_ref"
    ))
    .fetch_all(executor)
    .await?)
}

#[cfg(test)]
mod tests;
