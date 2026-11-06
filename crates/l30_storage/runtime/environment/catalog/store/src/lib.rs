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
///
/// New rows are admitted only while the persisted unretired count is below
/// `capacity`; the result counts deferred entries, not failed inventory
/// reports. Existing certifications are still intersected at capacity.
/// The caller supplies the recipe protocol's bound. Admission is serialized
/// before reference-ordered row updates; identical existing
/// publications acquire neither the admission lock nor row update locks.
/// The transaction must use READ COMMITTED so admission sees commits from
/// earlier holders of the admission lock.
pub async fn publish_known<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    environments: impl IntoIterator<Item = &'a KnownEnvironment>,
    capacity: usize,
) -> Result<usize, PublishKnownError> {
    let mut environments = environments.into_iter().collect::<Vec<_>>();
    environments.sort_unstable_by_key(|environment| environment.reference());
    let references = environments
        .iter()
        .map(|environment| environment.reference().to_string())
        .collect::<Vec<_>>();
    let has_new: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM unnest($1::text[]) AS reported(reference)
             WHERE NOT EXISTS (
                 SELECT 1 FROM runtime_environments
                 WHERE environment_ref = reported.reference
             )
         )"
    ))
    .bind(&references)
    .fetch_one(&mut **tx)
    .await?;
    if has_new {
        // Only admission takes this lock, before any catalog row locks.
        // Serialize the count-and-insert across reports, not steady-state
        // replays. The transaction owns it through commit.
        sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock(hashtextextended('runtime-environment-catalog-admission', 0))"
        ))
        .execute(&mut **tx)
        .await?;
    }
    let mut deferred = 0;
    for environment in environments {
        if !publish_one(tx, environment, capacity).await? {
            deferred += 1;
        }
    }
    Ok(deferred)
}

async fn publish_one(
    connection: &mut sqlx::PgConnection,
    environment: &KnownEnvironment,
    capacity: usize,
) -> Result<bool, PublishKnownError> {
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
         ) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
         WHERE (SELECT count(*) FROM runtime_environments WHERE retired_at IS NULL) < $10
         ON CONFLICT (environment_ref) DO NOTHING"
    ))
    .bind(environment.reference().to_string())
    .bind(release.runtime.kind.as_str())
    .bind(release.runtime.version.as_str())
    .bind(&sort_key)
    .bind(kernel)
    .bind(state_format.map(|format| format.format.as_str()))
    .bind(state_format.map(|format| format.version.as_str()))
    .bind(&upgrade_edges)
    .bind(&memory_edges)
    .bind(i64::try_from(capacity).unwrap_or(i64::MAX))
    .execute(&mut *connection)
    .await?;
    // A separate statement sees a concurrent insert after ON CONFLICT waits.
    // Unlike an upsert, UPDATE's predicate takes no tuple lock on a replay.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET
             version_sort_key = CASE
                 WHEN version_sort_key = $2
                 THEN runtime_environments.version_sort_key
                 ELSE NULL
             END,
             upgrade_edges = ARRAY(
                 SELECT target FROM unnest(runtime_environments.upgrade_edges) AS old(target)
                 INTERSECT
                 SELECT target FROM unnest($3::text[]) AS new(target)
                 ORDER BY target
             ),
             memory_edges = ARRAY(
                 SELECT target FROM unnest(runtime_environments.memory_edges) AS old(target)
                 INTERSECT
                 SELECT target FROM unnest($4::text[]) AS new(target)
                 ORDER BY target
             ),
             updated_at = now()
         WHERE environment_ref = $1 AND (
                NOT runtime_environments.upgrade_edges <@ $3::text[]
            OR NOT runtime_environments.memory_edges <@ $4::text[]
            OR (runtime_environments.version_sort_key IS NOT NULL
                AND runtime_environments.version_sort_key IS DISTINCT FROM $2::integer[]))"
    ))
    .bind(environment.reference().to_string())
    .bind(sort_key)
    .bind(upgrade_edges)
    .bind(memory_edges)
    .execute(&mut *connection)
    .await?;
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM runtime_environments WHERE environment_ref = $1)"
    ))
    .bind(environment.reference().to_string())
    .fetch_one(&mut *connection)
    .await?)
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

/// The legacy activation planner can attribute only Linux/snapshot profiles
/// carrying all three historical metadata fields. Other profiles still belong
/// in provisioning recipes, but cannot be inferred from that legacy evidence.
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

/// Read supported legacy attribution candidates without allowing an unrelated
/// runtime-neutral row's optional metadata to fail the entire catalog read.
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
