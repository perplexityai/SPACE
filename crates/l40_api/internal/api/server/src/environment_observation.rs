//! Record the environments a node's complete inventory reports: its usable and
//! known sets on `node_status_snapshots`, and its build's certifications in
//! the environment catalog.
//!
//! Only the server's verdict on a complete inventory moves either. An
//! observation that's absent (a node build that predates it) or that fails to
//! decode withdraws both, so the node has no environment evidence rather than
//! stale evidence. The decoder rejects the whole observation when any entry
//! fails, an entry of a kind this build doesn't know included, so a new
//! entry kind must reach cluster-api before nodes publish it. An unavailable
//! or legacy report says nothing about environments and leaves both alone;
//! the arrays then age out by their own receipt.

use pplx_space_common_core::types::{ArtifactComponentKind, NodeId};
use pplx_space_execution_common::environment::{EnvironmentObservation, KnownEnvironment};
use pplx_space_execution_common::proto::NodeExecutionInventory;
use pplx_space_runtime_environment_catalog_store::{
    self as catalog_store, CatalogIdentity, CertifiedEnvironment,
};
use tonic::Status;

/// What a publication does to `usable_environment_refs`,
/// `known_environment_refs` and their receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum EnvironmentRefs {
    /// The report says nothing about environments.
    Preserve,
    /// A complete inventory's valid observation, with retired environments
    /// already left out of `usable`.
    Record {
        usable: Vec<String>,
        known: Vec<String>,
    },
    /// A complete inventory with no valid observation: no evidence.
    Clear,
}

impl EnvironmentRefs {
    /// Whether the publication writes the arrays and their receipt.
    pub(super) const fn writes(&self) -> bool {
        !matches!(self, Self::Preserve)
    }

    pub(super) fn usable(&self) -> Option<&[String]> {
        match self {
            Self::Record { usable, .. } => Some(usable),
            Self::Preserve | Self::Clear => None,
        }
    }

    pub(super) fn known(&self) -> Option<&[String]> {
        match self {
            Self::Record { known, .. } => Some(known),
            Self::Preserve | Self::Clear => None,
        }
    }
}

/// Record a complete inventory's environments. Runs in the publication's
/// transaction, under the node lock `execution_observation::prepare` takes.
pub(super) async fn publish(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: &NodeExecutionInventory,
) -> Result<EnvironmentRefs, Status> {
    let observation = match EnvironmentObservation::from_inventory(inventory) {
        Ok(observation) => observation,
        Err(error) => {
            tracing::warn!(%error, %node_id, "invalid environment observation; withdrawing environment evidence");
            None
        }
    };
    let certified: Vec<CertifiedEnvironment> = observation
        .as_ref()
        .map(|observation| observation.known().map(certified).collect())
        .unwrap_or_default();
    let outcome = catalog_store::record_node_certifications(conn, node_id, &certified)
        .await
        .map_err(|error| {
            Status::internal(format!("record node environment certifications: {error}"))
        })?;
    if outcome == catalog_store::CertificationOutcome::Deferred {
        tracing::debug!(%node_id, "environment catalog busy; recording certifications on a later publication");
    }
    let Some(observation) = observation else {
        sample_recipe(conn, None).await?;
        return Ok(EnvironmentRefs::Clear);
    };
    let known: Vec<String> = observation
        .known()
        .map(|environment| environment.reference().to_string())
        .collect();
    sample_recipe(conn, Some(&known)).await?;
    let retired = catalog_store::retired_among(&mut *conn, &known)
        .await
        .map_err(|error| Status::internal(format!("read retired environments: {error}")))?;
    let usable = observation
        .usable()
        .iter()
        .map(ToString::to_string)
        .filter(|reference| !retired.contains(reference))
        .collect();
    Ok(EnvironmentRefs::Record { usable, known })
}

/// Sample the size of the recipe a node knowing `known` would be assigned,
/// whether or not recipes are served, so the limit alerts before it's reached.
/// It's the served recipe's own rule, so the two can't drift apart.
///
/// The read runs in a savepoint, so a failure only skips this sample: a
/// failed statement would otherwise abort the publication's transaction.
async fn sample_recipe(
    conn: &mut sqlx::PgConnection,
    known: Option<&[String]>,
) -> Result<(), Status> {
    let mut savepoint = sqlx::Connection::begin(&mut *conn)
        .await
        .map_err(|error| Status::internal(format!("open recipe sample savepoint: {error}")))?;
    match catalog_store::list_unretired_within(&mut *savepoint, known).await {
        Ok(recipe) => {
            savepoint.commit().await.map_err(|error| {
                Status::internal(format!("release recipe sample savepoint: {error}"))
            })?;
            let scope = match known {
                Some(_) => pplx_space_metrics::internal_api::RecipeCatalogScope::Known,
                None => pplx_space_metrics::internal_api::RecipeCatalogScope::Full,
            };
            pplx_space_metrics::internal_api::runtime_provisioning_recipe_environments(
                recipe.len(),
                scope,
            );
        }
        Err(error) => {
            savepoint.rollback().await.map_err(|error| {
                Status::internal(format!("roll back recipe sample savepoint: {error}"))
            })?;
            tracing::warn!(%error, "read environment catalog for the recipe-size sample; skipping it");
        }
    }
    Ok(())
}

fn certified(environment: &KnownEnvironment) -> CertifiedEnvironment {
    CertifiedEnvironment {
        reference: environment.reference(),
        identity: catalog_identity(environment),
        version_sort_key: environment.runtime_version_sort_key().cloned(),
        upgrade_edges: environment.upgrade_edges().clone(),
        memory_edges: environment.memory_edges().clone(),
    }
}

/// The catalog columns, spelled as published executions record them, so the
/// upgrade decision can match an unbound record against the row: the runtime's
/// alias, the kernel component's version and the state format.
fn catalog_identity(environment: &KnownEnvironment) -> Option<CatalogIdentity> {
    let spec = environment.definition().descriptor().spec();
    let pplx_space_common_core::types::RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let kernel = spec
        .boot
        .components()
        .find(|(_, component, _)| component.kind == ArtifactComponentKind::Kernel)?
        .1;
    let state_format = spec.compatibility.state_format()?;
    Some(CatalogIdentity {
        runtime: release.runtime.kind.alias().to_owned(),
        runtime_version: release.runtime.version.to_string(),
        kernel_version: kernel.version.to_string(),
        state_format_name: state_format.format.to_string(),
        state_format_version: state_format.version.to_string(),
    })
}

#[cfg(test)]
mod tests;
